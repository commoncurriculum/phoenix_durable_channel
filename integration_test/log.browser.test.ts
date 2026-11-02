import { describe, expect, test } from "vitest"
import type { Channel } from "../assets/js/phoenix_durable_channel/channel"
import { LongPoll } from "../assets/js/phoenix_durable_channel/longpoll"
import { Serializer } from "../assets/js/phoenix_durable_channel/serializer"
import { answer, join, nextEvent, openSocket, topic, waitFor } from "./support/helpers"

const fast = { rejoinAfterMs: () => 10, reconnectAfterMs: () => 10 }

function durableTopic(name: string): string {
  return `durable:${name}:${Math.random().toString(36).slice(2)}`
}

function updates(channel: Channel): Array<string> {
  const bodies: Array<string> = []
  channel.on("update", (payload) => bodies.push((payload as { body: string }).body))
  return bodies
}

async function ledger(channel: Channel): Promise<unknown> {
  return (await answer(channel.push("ledger"))).response
}

describe.each(["memory", "mongo", "s2", "durable-streams"])("with the %s ledger", (store) => {
  const params = { ledger: store }

  test("a durable push whose handler stops the channel is answered and recorded", async () => {
    const socket = openSocket("/socket", fast)
    socket.connect()
    const name = durableTopic("stop")
    const first = socket.channel(name, params, { durable: true })
    await join(first)
    expect(await answer(first.push("action", { body: "stop" }, { durable: true }))).toEqual({
      status: "ok",
      response: { stopped: true },
    })
    await waitFor(() => first.state === "closed")

    const second = socket.channel(name, params, { durable: true })
    await join(second)
    expect(await ledger(second)).toEqual({ received: 1, sent: 0 })
    expect(
      await answer(second.push("action", { body: "stop_quietly" }, { durable: true })),
    ).toEqual({
      status: "ok",
      response: {},
    })
    await waitFor(() => second.state === "closed")

    const third = socket.channel(name, params, { durable: true })
    await join(third)
    expect(await ledger(third)).toEqual({ received: 2, sent: 0 })
    expect(await answer(third.push("action", { body: "stop_ok" }, { durable: true }))).toEqual({
      status: "ok",
      response: {},
    })
    await waitFor(() => third.state === "closed")

    const fourth = socket.channel(name, params, { durable: true })
    await join(fourth)
    expect(await ledger(fourth)).toEqual({ received: 3, sent: 0 })
  })

  test("durable messages go both ways once and in order, beside fire-and-forget ones", async () => {
    const socket = openSocket("/socket", fast)
    socket.connect()
    const channel = socket.channel(durableTopic("both"), params, { durable: true })
    const seen = updates(channel)
    channel.join()

    const first = channel.push("action", { body: "first" }, { durable: true })
    const second = channel.push("action", { body: "second" }, { durable: true })
    expect(channel.log?.status.pending).toBe(2)
    expect(await answer(first)).toEqual({
      status: "ok",
      response: { body: "first" },
    })
    expect(await answer(second)).toEqual({
      status: "ok",
      response: { body: "second" },
    })
    expect(channel.log?.status.pending).toBe(0)

    expect(await answer(channel.push("action", { body: "refuse" }, { durable: true }))).toEqual({
      status: "error",
      response: { reason: "refused" },
    })
    expect(await answer(channel.push("action", { body: "quiet" }, { durable: true }))).toEqual({
      status: "ok",
      response: {},
    })
    expect(await answer(channel.push("action", { body: "hibernate" }, { durable: true }))).toEqual({
      status: "ok",
      response: {},
    })
    expect(await answer(channel.push("action", { body: "ok" }, { durable: true }))).toEqual({
      status: "ok",
      response: {},
    })
    expect(await answer(channel.push("action", { body: "crash" }, { durable: true }))).toEqual({
      status: "error",
      response: { reason: "crashed" },
    })
    expect(channel.state).toBe("joined")
    expect(await answer(channel.push("action", { body: "number" }, { durable: true }))).toEqual({
      status: "ok",
      response: { number: 8 },
    })
    expect(await ledger(channel)).toEqual({ received: 8, sent: 0 })
    expect((await answer(channel.push("durable"))).response).toEqual({
      durable: true,
    })

    await answer(channel.push("publish", { body: "one" }))
    await answer(channel.push("publish", { body: "two" }))
    await waitFor(() => seen.length === 2)
    expect(seen).toEqual(["one", "two"])

    const fired = nextEvent(channel, "fired")
    channel.push("fire", { hello: "world" })
    expect(await fired).toEqual({ hello: "world" })

    const plain = socket.channel(durableTopic("plain"), params)
    const plainSeen = updates(plain)
    await join(plain)
    expect(() => plain.push("action", {}, { durable: true })).toThrow(
      "was not opened with { durable: true }",
    )
    await answer(plain.push("publish", { body: "plain" }))
    await waitFor(() => plainSeen.length === 1)
    expect(plain.log).toBeNull()
    expect((await answer(plain.push("durable"))).response).toEqual({
      durable: false,
    })
  })

  test("a reply lost with the connection is sent again, handled once, and answered the same", async () => {
    const socket = openSocket("/socket", fast)
    socket.connect()
    const channel = socket.channel(durableTopic("lost"), params, { durable: true })
    const seen = updates(channel)
    await join(channel)
    await answer(channel.push("publish", { body: "before" }))

    const lost = channel.push("action", { body: "lose_reply" }, { durable: true })
    await waitFor(() => channel.state !== "joined")
    const waiting = channel.push("action", { body: "while away" }, { durable: true })
    await waitFor(() => channel.state === "joined")

    expect(await answer(lost)).toEqual({ status: "ok", response: {} })
    expect(await answer(waiting)).toEqual({
      status: "ok",
      response: { body: "while away" },
    })

    const refused = channel.push("action", { body: "refuse_and_lose" }, { durable: true })
    expect(await answer(refused)).toEqual({
      status: "error",
      response: { reason: "refused" },
    })
    expect(await ledger(channel)).toEqual({ received: 3, sent: 1 })
    expect(seen).toEqual(["before"])

    expect(() => channel.push("action", new ArrayBuffer(1), { durable: true })).toThrow(
      "with a binary payload",
    )
  })

  test("a sync with no answer is tried again; a durable push and a read die with their channel", async () => {
    const socket = openSocket("/socket", { ...fast, timeout: 100 })
    socket.connect()
    const channel = socket.channel(
      durableTopic("slow_sync"),
      { ...params, busy_after_join: true },
      { durable: true },
    )
    join(channel)
    const action = channel.push("action", { body: "after a slow sync" }, { durable: true })
    expect(await answer(action)).toEqual({
      status: "ok",
      response: { body: "after a slow sync" },
    })

    const reading = socket.channel(
      durableTopic("slow_read"),
      { ...params, stream: "slow" },
      { durable: true, after: 0 },
    )
    const seen = updates(reading)
    await join(reading)
    await answer(reading.leave())
    await new Promise((resolve) => setTimeout(resolve, 400))
    expect(seen).toEqual([])
    expect(reading.state).toBe("closed")
    const afterLeave = reading.push("action", { body: "after leave" }, { durable: true })
    expect(await answer(afterLeave)).toEqual({
      status: "timeout",
      response: undefined,
    })
    expect(reading.log?.status.pending).toBe(0)

    const offline = openSocket("/socket", fast)
    const closing = offline.channel(durableTopic("closing"), params, { durable: true })
    closing.join()
    const abandoned = closing.push("action", { body: "never sent" }, { durable: true })
    closing.leave()
    expect(await answer(abandoned)).toEqual({
      status: "timeout",
      response: undefined,
    })
    expect(closing.log?.status.pending).toBe(0)
  })

  test("durable messages work over LongPoll", async () => {
    const socket = openSocket("/socket", { ...fast, transport: LongPoll })
    socket.connect()
    const channel = socket.channel(durableTopic("longpoll"), params, { durable: true })
    const seen = updates(channel)
    await join(channel)
    expect(await answer(channel.push("action", { body: "polled" }, { durable: true }))).toEqual({
      status: "ok",
      response: { body: "polled" },
    })
    await answer(channel.push("publish", { body: "over longpoll" }))
    await waitFor(() => seen.length === 1)
    expect(seen).toEqual(["over longpoll"])
  })

  test("a message the server could not take is sent again, with the ones after it, in order", async () => {
    const socket = openSocket("/socket", fast)
    socket.connect()
    const channel = socket.channel(durableTopic("busy"), params, { durable: true })
    await join(channel)

    const order: Array<string> = []
    const pushes = ["busy_once", "second", "third"].map((body) => {
      const reply = channel.push("action", { body }, { durable: true })
      reply.receive("ok", () => order.push(body))
      return reply
    })
    expect(await Promise.all(pushes.map(answer))).toEqual(
      ["busy_once", "second", "third"].map((body) => ({
        status: "ok",
        response: { body },
      })),
    )
    expect(order).toEqual(["busy_once", "second", "third"])
    expect(await ledger(channel)).toEqual({ received: 3, sent: 0 })
  })

  test("a message the old connection's process is still running when the client rejoins runs once", async () => {
    const socket = openSocket("/socket", fast)
    socket.connect()
    const channel = socket.channel(durableTopic("lingering"), params, { durable: true })
    await join(channel)
    const room = socket.channel(topic("lingering"))
    await join(room)

    const slow = channel.push("action", { body: "slow" }, { durable: true })
    await new Promise((resolve) => setTimeout(resolve, 100))
    room.push("kill_transport")
    expect(await answer(slow)).toEqual({
      status: "ok",
      response: { body: "slow" },
    })
    expect((await answer(channel.push("handled"))).response).toEqual({
      handled: 1,
    })
  })

  test("a late join from a connection the client left replaces its channel, and the client rejoins", async () => {
    const name = durableTopic("late-join")
    const socket = openSocket("/socket", fast)
    socket.connect()
    const channel = socket.channel(name, params, { durable: true })
    let errors = 0
    channel.onError(() => errors++)
    await join(channel)
    expect(await answer(channel.push("action", { body: "first" }, { durable: true }))).toEqual({
      status: "ok",
      response: { body: "first" },
    })

    const late = openSocket("/socket", fast)
    late.connect()
    await join(late.channel(name, params, { durable: true }))
    await new Promise<void>((resolve) => late.disconnect(resolve))

    await waitFor(() => errors === 1)
    const after = channel.push("action", { body: "after" }, { durable: true })
    expect(await answer(after)).toEqual({
      status: "ok",
      response: { body: "after" },
    })
    expect(channel.state).toBe("joined")
  })

  test("a durable push that reaches a replaced channel's connection after it stopped is sent again", async () => {
    const name = durableTopic("unmatched")
    let held: Array<() => void> | null = null
    const socket = openSocket("/socket", {
      ...fast,
      decode: (raw, callback) =>
        Serializer.decode(raw, (message) => {
          if (held) held.push(() => callback(message))
          else callback(message)
        }),
    })
    socket.connect()
    const channel = socket.channel(name, params, { durable: true })
    await join(channel)

    held = []
    const late = openSocket("/socket", fast)
    late.connect()
    await join(late.channel(name, params, { durable: true }))
    await new Promise<void>((resolve) => late.disconnect(resolve))

    const crossing = channel.push("action", { body: "crossing" }, { durable: true })
    await new Promise((resolve) => setTimeout(resolve, 200))
    const release = held
    held = null
    for (const deliver of release) deliver()

    expect(await answer(crossing)).toEqual({
      status: "ok",
      response: { body: "crossing" },
    })
  })

  test("at most 32 durable messages are unanswered at once; the rest wait their turn, in order", async () => {
    const socket = openSocket("/socket", fast)
    socket.connect()
    const channel = socket.channel(durableTopic("window"), params, { durable: true })
    let most = 0
    channel.log?.actor.subscribe((snapshot) => {
      const inFlight = snapshot.context.queue.filter((entry) => entry.sent !== null).length
      most = Math.max(most, inFlight)
    })
    channel.join()
    const bodies = Array.from({ length: 41 }, (_, index) => String(index + 1))
    const replies = bodies.map((body) => channel.push("action", { body }, { durable: true }))
    expect(await Promise.all(replies.map(answer))).toEqual(
      bodies.map((body) => ({ status: "ok", response: { body } })),
    )
    expect(most).toBe(32)
    expect(await ledger(channel)).toEqual({ received: 41, sent: 0 })
  })
})

describe.each(["memory", "mongo"])("reading from %s", (store) => {
  const params = { ledger: store, stream: store }

  test("a reconnecting client gets what was published while it was away", async () => {
    const socket = openSocket("/socket", fast)
    socket.connect()
    const channel = socket.channel(durableTopic("away"), params, { durable: true })
    const seen = updates(channel)
    await join(channel)
    await answer(channel.push("publish", { body: "1" }))
    await waitFor(() => seen.length === 1)

    channel.push("kill_transport")
    await waitFor(() => channel.state !== "joined")
    await waitFor(() => channel.state === "joined")
    await answer(channel.push("publish_silently", { body: "2" }))
    channel.push("kill_transport")
    await waitFor(() => channel.state !== "joined")
    await waitFor(() => seen.length === 2)
    expect(seen).toEqual(["1", "2"])
  })

  test("`after` resumes from a number the app saved, and the first read starts from now", async () => {
    const socket = openSocket("/socket", fast)
    socket.connect()
    const name = durableTopic("after")
    const earlier = socket.channel(name, params, { durable: true })
    await join(earlier)
    for (const body of ["1", "2", "3"]) await answer(earlier.push("publish_silently", { body }))
    await answer(earlier.leave())

    const fresh = socket.channel(name, { ...params, greet: true }, { durable: true })
    const freshSeen = updates(fresh)
    await join(fresh)
    await waitFor(() => freshSeen.length === 1)
    expect(freshSeen).toEqual(["welcome"])
    await answer(fresh.leave())

    const resumed = socket.channel(name, params, { durable: true, after: 1 })
    const resumedSeen = updates(resumed)
    await join(resumed)
    await waitFor(() => resumedSeen.length === 3)
    expect(resumedSeen).toEqual(["2", "3", "welcome"])
  })

  test("a read of more than a page gets every message, in order", async () => {
    const socket = openSocket("/socket", fast)
    socket.connect()
    const channel = socket.channel(durableTopic("pages"), params, { durable: true })
    const seen = updates(channel)
    await join(channel)
    await answer(channel.push("publish_many_silently", { count: 1201 }))
    await answer(channel.push("publish", { body: "last" }))
    await waitFor(() => seen.length === 1202)
    expect(seen).toEqual([...Array.from({ length: 1201 }, (_, index) => index + 1), "last"])
  })
})

describe("with the memory store, whose kept messages and replies the test app can drop", () => {
  test("a gap is fetched, and what the server no longer has is skipped, logged and told to the channel's phx_skipped", async () => {
    const logs: Array<string> = []
    const socket = openSocket("/socket", {
      ...fast,
      logger: (_kind, message) => logs.push(message),
    })
    socket.connect()
    const name = durableTopic("gap")
    const channel = socket.channel(name, {}, { durable: true })
    const seen = updates(channel)
    const skipped: Array<unknown> = []
    channel.on("phx_skipped", (payload) => skipped.push(payload))
    await join(channel)

    await answer(channel.push("publish", { body: "1" }))
    await answer(channel.push("publish_silently", { body: "2" }))
    await answer(channel.push("publish", { body: "3" }))
    await waitFor(() => seen.length === 3)
    expect(seen).toEqual(["1", "2", "3"])

    await answer(channel.push("publish_silently", { body: "4" }))
    await answer(channel.push("forget"))
    channel.push("publish", { body: "5" })
    channel.push("publish", { body: "6" })
    await waitFor(() => seen.length === 5)
    expect(seen).toEqual(["1", "2", "3", "5", "6"])
    expect(logs).toContain(`${name} skipped durable messages 4 to 4, which the store no longer has`)
    expect(skipped).toEqual([{ after: 3, next: 5 }])
    await answer(channel.push("publish", { body: "7" }))
    await waitFor(() => seen.length === 6)
    expect(seen).toEqual(["1", "2", "3", "5", "6", "7"])

    await answer(channel.push("publish_silently", { body: "8" }))
    await answer(channel.push("forget"))
    channel.push("kill_transport")
    await waitFor(() => skipped.length === 2)
    expect(skipped).toEqual([
      { after: 3, next: 5 },
      { after: 7, next: 9 },
    ])
    await answer(channel.push("publish", { body: "9" }))
    await waitFor(() => seen.length === 7)
    expect(seen).toEqual(["1", "2", "3", "5", "6", "7", "9"])
  })

  test("a ledger reset while its channel lives keeps the channel's count; the next join starts both directions from the ledger's numbers", async () => {
    const socket = openSocket("/socket", fast)
    socket.connect()
    const channel = socket.channel(durableTopic("reset"), {}, { durable: true })
    const seen = updates(channel)
    await join(channel)
    await answer(channel.push("publish", { body: "1" }))
    await answer(channel.push("publish", { body: "2" }))
    await answer(channel.push("action", { body: "1" }, { durable: true }))
    await waitFor(() => seen.length === 2)

    await answer(channel.push("reset"))
    expect(await answer(channel.push("action", { body: "2" }, { durable: true }))).toEqual({
      status: "ok",
      response: { body: "2" },
    })
    expect(await ledger(channel)).toEqual({ received: 2, sent: 0 })

    await answer(channel.push("reset"))
    channel.push("kill_transport")
    await waitFor(() => channel.state !== "joined")
    await waitFor(() => channel.state === "joined")
    expect(await answer(channel.push("action", { body: "after" }, { durable: true }))).toEqual({
      status: "ok",
      response: { body: "after" },
    })
    await answer(channel.push("publish", { body: "1 again" }))
    await waitFor(() => seen.length === 3)
    expect(seen).toEqual(["1", "2", "1 again"])
    expect(await ledger(channel)).toEqual({ received: 1, sent: 1 })
  })

  test("a reply that neither the channel nor its ledger kept is answered reply_lost", async () => {
    const socket = openSocket("/socket", fast)
    socket.connect()
    let forgetReplies = false
    const channel = socket.channel(
      durableTopic("reply_lost"),
      () => ({ forget_replies: forgetReplies }),
      { durable: true },
    )
    await join(channel)
    forgetReplies = true
    expect(await answer(channel.push("action", { body: "lose_reply" }, { durable: true }))).toEqual(
      {
        status: "error",
        response: { reason: "reply_lost" },
      },
    )
    expect(await ledger(channel)).toEqual({ received: 1, sent: 0 })
  })

  test("a store that numbers afresh is read from its start, and one read before from where the client got to", async () => {
    const socket = openSocket("/socket", fast)
    socket.connect()
    const channel = socket.channel(durableTopic("epochs"), {}, { durable: true })
    const seen = updates(channel)
    await join(channel)
    await answer(channel.push("publish", { body: "1" }))
    await answer(channel.push("publish", { body: "2" }))
    await waitFor(() => seen.length === 2)

    await answer(channel.push("reset"))
    channel.push("kill_transport")
    await waitFor(() => channel.state !== "joined")
    await waitFor(() => channel.state === "joined")
    await answer(channel.push("publish", { body: "other 1" }))
    await waitFor(() => seen.length === 3)

    await answer(channel.push("restore"))
    channel.push("kill_transport")
    await waitFor(() => channel.state !== "joined")
    await waitFor(() => channel.state === "joined")
    await answer(channel.push("publish", { body: "3" }))
    await waitFor(() => seen.length === 4)
    expect(seen).toEqual(["1", "2", "other 1", "3"])
  })

  test("a server from an earlier deploy, which joins without durable messages, is left and joined again until it logs", async () => {
    const socket = openSocket("/socket", fast)
    socket.connect()
    const room = socket.channel(topic("deploy"))
    await join(room)
    const count = async (key: string) =>
      ((await answer(room.push("count", { key }))).response as { count: number }).count
    const upgrading = `upgrading:deploy:${Math.random().toString(36).slice(2)}`
    const channel = socket.channel(upgrading, {}, { durable: true })
    const refused: Array<unknown> = []
    channel.join().receive("error", (response) => refused.push(response))
    const saved = channel.push("action", { body: "saved" }, { durable: true })

    await waitFor(() => refused.length >= 2)
    expect(refused[0]).toEqual({ reason: "nondurable" })
    expect(channel.state).not.toBe("joined")

    await answer(room.push("upgrade", { topic: upgrading }))
    expect(await answer(saved)).toEqual({
      status: "ok",
      response: { body: "saved" },
    })
    const oldJoins = await count(`old_joins:${upgrading}`)
    expect(oldJoins).toBeGreaterThanOrEqual(2)
    await expect.poll(() => count(`old_left:${upgrading}`)).toBe(oldJoins)
  })
})

test("a node reads a Durable Streams ledger from its start once while the topic's channel comes and goes, and again once the topic has had no channel for keep_offsets_for since its last leave, whatever the stream", async () => {
  const socket = openSocket("/socket", fast)
  socket.connect()
  const room = socket.channel(durableTopic("asks"))
  await join(room)
  const name = durableTopic("cold")
  const readsFromStart = async () =>
    (
      (await answer(room.push("count", { key: `durable_streams_read:${name}/replies@-1` })))
        .response as { count: number }
    ).count
  const pause = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))
  const joinAndLeave = async () => {
    const channel = socket.channel(name, { ledger: "durable-streams" }, { durable: true })
    await join(channel)
    expect(await answer(channel.push("ledger"))).toMatchObject({ response: { received: 1 } })
    await answer(channel.leave())
  }
  const first = socket.channel(name, { ledger: "durable-streams" }, { durable: true })
  await join(first)
  await answer(first.push("action", { body: "1" }, { durable: true }))
  await answer(first.leave())
  expect(await readsFromStart()).toBe(1)

  await pause(1000)
  await joinAndLeave()
  await pause(1300)
  await joinAndLeave()
  expect(await readsFromStart()).toBe(1)

  await pause(2500)
  await joinAndLeave()
  expect(await readsFromStart()).toBe(2)
})

test("a Memory ledger keeps what a joined channel received however long the channel stays idle, whatever the stream", async () => {
  const socket = openSocket("/socket", fast)
  socket.connect()
  const channel = socket.channel(durableTopic("idle"), { stream: "s2" }, { durable: true })
  await join(channel)
  await answer(channel.push("action", { body: "1" }, { durable: true }))

  await new Promise((resolve) => setTimeout(resolve, 3000))
  expect(await ledger(channel)).toMatchObject({ received: 1 })
})

test("a channel reads through itself only while joined: rejoins the server refuses ask for no read, and the join that succeeds asks for one", async () => {
  const socket = openSocket("/socket", fast)
  socket.connect()
  const room = socket.channel(durableTopic("asks"))
  await join(room)
  const name = durableTopic("refused")
  const counts = async () =>
    (await answer(room.push("counts", { topic: name }))).response as {
      reads: number
      refused: number
    }
  let refuse = false
  const channel = socket.channel(name, () => ({ stream: "slow", refuse }), { durable: true })
  const seen = updates(channel)
  await join(channel)
  await expect.poll(async () => (await counts()).reads).toBe(1)

  refuse = true
  channel.push("kill_transport")
  await expect.poll(async () => (await counts()).refused).toBeGreaterThanOrEqual(5)
  expect((await counts()).reads).toBe(1)
  refuse = false
  await waitFor(() => channel.state === "joined")
  await expect.poll(async () => (await counts()).reads).toBe(2)
  await answer(channel.push("publish", { body: "after" }))
  await waitFor(() => seen.length === 1)
  expect(seen).toEqual(["after"])
  expect((await counts()).reads).toBe(2)
})

test("an `after` that is not a whole number of 0 or more is refused when the channel is made; a read the server refuses is logged and asked again", async () => {
  const logs: Array<string> = []
  const socket = openSocket("/socket", { ...fast, logger: (_kind, message) => logs.push(message) })
  socket.connect()
  for (const after of [-1, 1.5, Number.NaN])
    expect(() => socket.channel(durableTopic("after"), {}, { durable: true, after })).toThrow(
      `after ${after}, which is not a whole number of 0 or more`,
    )

  const name = durableTopic("unreadable")
  const channel = socket.channel(name, { stream: "unreadable" }, { durable: true })
  await join(channel)
  await waitFor(() => logs.filter((line) => line === `${name} could not read after 0`).length >= 2)
  expect(channel.state).toBe("joined")
})

test("a durable request on a channel joined without durable messages, or a read the store does not serve, is invalid", async () => {
  const socket = openSocket("/socket", fast)
  socket.connect()
  const invalid = { status: "error", response: { reason: "invalid" } }
  const name = durableTopic("plain")
  const plain = socket.channel(name)
  await join(plain)
  expect(await answer(plain.push("phx_sync"))).toEqual(invalid)
  expect(await answer(plain.push("phx_read", { after: 0 }))).toEqual(invalid)
  expect(await answer(plain.push("phx_reader"))).toEqual(invalid)
  const replies: Array<unknown> = []
  socket.onMessage((message) => {
    if (message.topic === name && message.ref === "durable-frame") replies.push(message.payload)
  })
  socket.push({
    topic: name,
    event: "action",
    payload: { body: "1" },
    ref: "durable-frame",
    join_ref: plain.joinRef(),
    number: 1,
  })
  await waitFor(() => replies.length === 1)
  expect(replies).toEqual([invalid])

  const durable = socket.channel(durableTopic("durable"), {}, { durable: true })
  await join(durable)
  expect(await answer(durable.push("phx_read", { after: -1 }))).toEqual(invalid)
  const elsewhere = socket.channel(durableTopic("s2"), { stream: "s2" }, { durable: true })
  await join(elsewhere)
  expect(await answer(elsewhere.push("phx_read", { after: 0 }))).toEqual(invalid)
})
