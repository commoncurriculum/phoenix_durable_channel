import { expect, test } from "vitest"
import type { Channel } from "../assets/js/phoenix_durable_channel/channel"
import { LongPoll } from "../assets/js/phoenix_durable_channel/longpoll"
import { Serializer } from "../assets/js/phoenix_durable_channel/serializer"
import { answer, join, nextEvent, openSocket, topic, waitFor } from "./support/helpers"

const fast = { rejoinAfterMs: () => 10, reconnectAfterMs: () => 10 }

function durableTopic(name: string): string {
  return `durable:${name}:${Math.random().toString(36).slice(2)}`
}

/** Every "update" the channel's bindings see, by body. */
function updates(channel: Channel): Array<string> {
  const bodies: Array<string> = []
  channel.on("update", (payload) => bodies.push((payload as { body: string }).body))
  return bodies
}

async function ledger(channel: Channel): Promise<unknown> {
  return (await answer(channel.push("ledger"))).response
}

test("durable messages go both ways once and in order, beside fire-and-forget ones", async () => {
  const socket = openSocket("/socket", fast)
  socket.connect()
  const channel = socket.channel(durableTopic("both"), {}, { durable: true })
  const seen = updates(channel)
  channel.join()

  // Pushed before the join is answered: numbered once the first sync says where the server is.
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

  // Pushed while synced: sent at once. A refusal is an answer; a reply-less handle_in is "ok".
  expect(await answer(channel.push("action", { body: "refuse" }, { durable: true }))).toEqual({
    status: "error",
    response: { reason: "refused" },
  })
  expect(await answer(channel.push("action", { body: "quiet" }, { durable: true }))).toEqual({
    status: "ok",
    response: {},
  })
  // A crash is answered and recorded once, and the channel carries on.
  expect(await answer(channel.push("action", { body: "crash" }, { durable: true }))).toEqual({
    status: "error",
    response: { reason: "crashed" },
  })
  expect(channel.state).toBe("joined")
  // The handler gets the message's number.
  expect(await answer(channel.push("action", { body: "number" }, { durable: true }))).toEqual({
    status: "ok",
    response: { number: 6 },
  })
  expect(await ledger(channel)).toEqual({ received: 6, sent: 0 })
  expect((await answer(channel.push("durable"))).response).toEqual({
    durable: true,
  })

  await answer(channel.push("publish", { body: "one" }))
  await answer(channel.push("publish", { body: "two" }))
  await waitFor(() => seen.length === 2)
  expect(seen).toEqual(["one", "two"])

  // Fire and forget on the same channel: no number, straight to the bindings.
  const fired = nextEvent(channel, "fired")
  channel.push("fire", { hello: "world" })
  expect(await fired).toEqual({ hello: "world" })

  // A channel opened without `durable` takes numbered messages as plain ones and refuses to log.
  const plain = socket.channel(durableTopic("plain"))
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

test("a gap is fetched, and what the server no longer has is skipped", async () => {
  const socket = openSocket("/socket", fast)
  socket.connect()
  const channel = socket.channel(durableTopic("gap"), {}, { durable: true })
  const seen = updates(channel)
  await join(channel)

  await answer(channel.push("publish", { body: "1" }))
  await answer(channel.push("publish_silently", { body: "2" }))
  // 3 arrives after a gap: the read ends, and the next one gets 2 and 3 again.
  await answer(channel.push("publish", { body: "3" }))
  await waitFor(() => seen.length === 3)
  expect(seen).toEqual(["1", "2", "3"])

  // 4 is gone from the server before the client asks. 5 ends the read, and the next read
  // gets 5 and 6 again: past 4, which the server no longer has.
  await answer(channel.push("publish_silently", { body: "4" }))
  await answer(channel.push("forget"))
  channel.push("publish", { body: "5" })
  channel.push("publish", { body: "6" })
  await waitFor(() => seen.length === 5)
  expect(seen).toEqual(["1", "2", "3", "5", "6"])
  await answer(channel.push("publish", { body: "7" }))
  await waitFor(() => seen.length === 6)
  expect(seen).toEqual(["1", "2", "3", "5", "6", "7"])
})

test("a reply lost with the connection is sent again, handled once, and answered the same", async () => {
  const socket = openSocket("/socket", fast)
  socket.connect()
  const channel = socket.channel(durableTopic("lost"), {}, { durable: true })
  const seen = updates(channel)
  await join(channel)
  await answer(channel.push("publish", { body: "before" }))

  const lost = channel.push("action", { body: "lose_reply" }, { durable: true })
  await waitFor(() => channel.state !== "joined")
  // Pushed while the channel is away: waits for the rejoin, then goes after the resend.
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

test("a ledger that lost what it kept starts both directions again from its numbers", async () => {
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
  // Numbered 2 by the client, while the ledger now expects 1: refused, synced, renumbered.
  expect(await answer(channel.push("action", { body: "after" }, { durable: true }))).toEqual({
    status: "ok",
    response: { body: "after" },
  })
  // A ledger loses what it kept when its node restarts, which drops the connection. The read
  // after the rejoin finds the server behind the client, and reads all it has again, so a
  // second 1 is new whether it is sent before that read or after.
  channel.push("kill_transport")
  await waitFor(() => channel.state !== "joined")
  await waitFor(() => channel.state === "joined")
  await answer(channel.push("publish", { body: "1 again" }))
  await waitFor(() => seen.length === 3)
  expect(seen).toEqual(["1", "2", "1 again"])
  expect(await ledger(channel)).toEqual({ received: 1, sent: 1 })
})

test("a reconnecting client gets what was published while it was away", async () => {
  const socket = openSocket("/socket", fast)
  socket.connect()
  const topic = durableTopic("away")
  const channel = socket.channel(topic, {}, { durable: true })
  const seen = updates(channel)
  await join(channel)
  await answer(channel.push("publish", { body: "1" }))
  await waitFor(() => seen.length === 1)

  channel.push("kill_transport")
  await waitFor(() => channel.state !== "joined")
  await waitFor(() => channel.state === "joined")
  await answer(channel.push("publish_silently", { body: "2" }))
  // Nothing tells the client about 2 until a gap or a rejoin; a rejoin does.
  channel.push("kill_transport")
  await waitFor(() => channel.state !== "joined")
  await waitFor(() => seen.length === 2)
  expect(seen).toEqual(["1", "2"])
})

test("`after` resumes from a number the app saved, and the first read starts from now", async () => {
  const socket = openSocket("/socket", fast)
  socket.connect()
  const topic = durableTopic("after")
  const earlier = socket.channel(topic, {}, { durable: true })
  await join(earlier)
  for (const body of ["1", "2", "3"]) await answer(earlier.push("publish_silently", { body }))
  await answer(earlier.leave())

  // No `after`: starts from where the server is, so 1 to 3 are not delivered. The greeting,
  // pushed after the join reply but before the read's reply, is.
  const fresh = socket.channel(topic, { greet: true }, { durable: true })
  const freshSeen = updates(fresh)
  await join(fresh)
  await waitFor(() => freshSeen.length === 1)
  expect(freshSeen).toEqual(["welcome"])
  await answer(fresh.leave())

  const resumed = socket.channel(topic, {}, { durable: true, after: 1 })
  const resumedSeen = updates(resumed)
  await join(resumed)
  await waitFor(() => resumedSeen.length === 3)
  expect(resumedSeen).toEqual(["2", "3", "welcome"])
})

test("a sync with no answer is tried again; a durable push and a read die with their channel", async () => {
  const socket = openSocket("/socket", { ...fast, timeout: 100 })
  socket.connect()
  const channel = socket.channel(durableTopic("slow_sync"), {}, { durable: true })
  join(channel)
  const action = channel.push("action", { body: "after a slow sync" }, { durable: true })
  expect(await answer(action)).toEqual({
    status: "ok",
    response: { body: "after a slow sync" },
  })

  // Left while its read waits for an answer: the late answer finds the read gone.
  const reading = socket.channel(durableTopic("slow_read"), {}, { durable: true, after: 0 })
  const seen = updates(reading)
  await join(reading)
  await answer(reading.leave())
  await new Promise((resolve) => setTimeout(resolve, 400))
  expect(seen).toEqual([])
  expect(reading.state).toBe("closed")

  const offline = openSocket("/socket", fast)
  const closing = offline.channel(durableTopic("closing"), {}, { durable: true })
  closing.join()
  const abandoned = closing.push("action", { body: "never sent" }, { durable: true })
  closing.leave()
  expect(await answer(abandoned)).toEqual({
    status: "timeout",
    response: undefined,
  })
})

test("durable messages work over LongPoll", async () => {
  const socket = openSocket("/socket", { ...fast, transport: LongPoll })
  socket.connect()
  const channel = socket.channel(durableTopic("longpoll"), {}, { durable: true })
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
  const channel = socket.channel(durableTopic("busy"), {}, { durable: true })
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
  const channel = socket.channel(durableTopic("lingering"), {}, { durable: true })
  await join(channel)
  const room = socket.channel(topic("lingering"))
  await join(room)

  // The server's process for the old connection is still in the slow handler when the
  // client rejoins on a new one; the new join waits for it, so the sync finds it recorded.
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
  const channel = socket.channel(name, {}, { durable: true })
  let errors = 0
  channel.onError(() => errors++)
  await join(channel)
  expect(await answer(channel.push("action", { body: "first" }, { durable: true }))).toEqual({
    status: "ok",
    response: { body: "first" },
  })

  // The late join's process replaces the live one, and its connection goes away.
  const late = openSocket("/socket", fast)
  late.connect()
  await join(late.channel(name, {}, { durable: true }))
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
  // Frames to the client wait here while `held` is set, as a slow network holds them.
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
  const channel = socket.channel(name, {}, { durable: true })
  await join(channel)

  held = []
  const late = openSocket("/socket", fast)
  late.connect()
  await join(late.channel(name, {}, { durable: true }))
  await new Promise<void>((resolve) => late.disconnect(resolve))

  // Sent before the client hears of phx_error: Phoenix answers it "unmatched topic".
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
  const channel = socket.channel(durableTopic("window"), {}, { durable: true })
  let most = 0
  channel.log?.actor.subscribe((snapshot) => {
    const inFlight = snapshot.context.queue.filter((entry) => entry.ref !== null).length
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

test("a read of more than a page gets every message, in order", async () => {
  const socket = openSocket("/socket", fast)
  socket.connect()
  const channel = socket.channel(durableTopic("pages"), {}, { durable: true })
  const seen = updates(channel)
  await join(channel)
  await answer(channel.push("publish_many_silently", { count: 1201 }))
  await answer(channel.push("publish", { body: "last" }))
  await waitFor(() => seen.length === 1202)
  expect(seen).toEqual([...Array.from({ length: 1201 }, (_, index) => index + 1), "last"])
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

  // Another node's memory, numbered from 1 again: the read after the rejoin says so.
  await answer(channel.push("reset"))
  channel.push("kill_transport")
  await waitFor(() => channel.state !== "joined")
  await waitFor(() => channel.state === "joined")
  await answer(channel.push("publish", { body: "other 1" }))
  await waitFor(() => seen.length === 3)

  // Back on the first node: read on after 2, so nothing comes twice.
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
  // Each join the old server took was left, so it keeps no channel process behind.
  const oldJoins = await count(`old_joins:${upgrading}`)
  expect(oldJoins).toBeGreaterThanOrEqual(2)
  await expect.poll(() => count(`old_left:${upgrading}`)).toBe(oldJoins)
})
