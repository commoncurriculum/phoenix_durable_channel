import { expect, test } from "vitest"
import { answer, join, nextEvent, openSocket, topic, waitFor } from "./support/helpers"

const fast = { rejoinAfterMs: () => 10, reconnectAfterMs: () => 10 }

test("joins, pushes and receives over a real Phoenix server", async () => {
  const socket = openSocket("/socket", { params: { id: "alice" } })
  const opened: Array<string> = []
  socket.onOpen(() => opened.push("open"))
  socket.connect()
  const channel = socket.channel(topic("basic"), { hello: "world" })
  expect(() => channel.push("echo")).toThrow("tried to push 'echo' to")

  expect(channel.isClosed()).toBe(true)
  const joinPush = channel.join()
  expect(() => channel.join()).toThrow("tried to join multiple times")
  expect(channel.isJoining()).toBe(true)
  // Pushed while joining: these wait, then go out before the join's own "ok" callbacks.
  const order: Array<string> = []
  const early = channel.push("echo", { early: true })
  early.receive("ok", () => order.push("early reply"))
  expect(early.status.state).toBe("buffered")
  joinPush.receive("ok", () => order.push(`join ok, early push ${early.status.state}`))

  expect(await answer(joinPush)).toEqual({
    status: "ok",
    response: { joins: 1, params: { hello: "world" } },
  })
  expect(opened).toEqual(["open"])
  expect(socket.status.connectionState).toBe("open")
  expect(channel.state).toBe("joined")
  expect(channel.status.state).toBe("joined")
  expect([channel.isJoined(), channel.isErrored(), channel.isLeaving()]).toEqual([
    true,
    false,
    false,
  ])

  expect(await answer(early)).toEqual({
    status: "ok",
    response: { early: true },
  })
  expect(order).toEqual(["join ok, early push sent", "early reply"])
  // A hook added after the answer runs at once; one for another status never does.
  const late: Array<unknown> = []
  early.receive("ok", (response) => late.push(response)).receive("error", () => late.push("never"))
  joinPush.receive("ok", (response) => late.push(response))
  expect(late).toEqual([{ early: true }, { joins: 1, params: { hello: "world" } }])

  expect(await answer(channel.push("echo", { a: 1 }))).toEqual({
    status: "ok",
    response: { a: 1 },
  })
  expect(await answer(channel.push("echo"))).toEqual({
    status: "ok",
    response: {},
  })
  expect(await answer(channel.push("error", { b: 2 }))).toEqual({
    status: "error",
    response: { b: 2 },
  })
  expect((await answer(channel.push("noreply", {}, 50))).status).toBe("timeout")

  const broadcast = nextEvent(channel, "broadcasted")
  channel.push("broadcast", { c: 3 })
  expect(await broadcast).toEqual({ c: 3 })

  const pushed = nextEvent(channel, "pushed")
  channel.push("push_to_self", { d: 4 })
  expect(await pushed).toEqual({ d: 4 })

  // `off` with no ref removes every binding for the event.
  const seen: Array<unknown> = []
  channel.on("pushed", (payload) => seen.push(payload))
  channel.on("pushed", (payload) => seen.push(payload))
  channel.off("pushed")
  await answer(channel.push("push_to_self", { e: 5 }))
  expect(seen).toEqual([])

  // `onMessage` can be overridden to change payloads before bindings see them.
  channel.onMessage = (_event, payload) => ({ wrapped: payload })
  const wrapped = nextEvent(channel, "pushed")
  channel.push("push_to_self", { f: 6 })
  expect(await wrapped).toEqual({ wrapped: { f: 6 } })
})

test("sends and receives binary payloads", async () => {
  const socket = openSocket()
  socket.connect()
  const channel = socket.channel(topic("binary"))
  await join(channel)

  const { status, response } = await answer(
    channel.push("echo_binary", new Uint8Array([1, 2, 250]).buffer),
  )
  expect(status).toBe("ok")
  expect([...new Uint8Array(response as ArrayBuffer)]).toEqual([1, 2, 250])

  const pushed = nextEvent(channel, "binary")
  channel.push("push_binary")
  expect([...new Uint8Array((await pushed) as ArrayBuffer)]).toEqual([1, 2, 3])

  const broadcast = nextEvent(channel, "binary_broadcast")
  channel.push("broadcast_binary")
  expect([...new Uint8Array((await broadcast) as ArrayBuffer)]).toEqual([4, 5, 6])
})

test("a denied join is answered with the error, and the channel keeps trying until it is left", async () => {
  const socket = openSocket("/socket", fast)
  socket.connect()
  const channel = socket.channel("room:deny")
  const errors: Array<unknown> = []
  channel.onError((reason) => errors.push(reason))
  const denials: Array<unknown> = []
  channel.join().receive("error", (response) => denials.push(response))
  await waitFor(() => denials.length >= 2)
  expect(denials[0]).toEqual({ reason: "denied" })
  expect(errors).toEqual([])

  const closes: Array<unknown> = []
  channel.onClose((reason) => closes.push(reason))
  expect((await answer(channel.leave())).status).toBe("ok")
  await waitFor(() => channel.state === "closed")
  expect(closes).toEqual(["leave"])
  expect(socket.channels).not.toContain(channel)
})

test("a join that times out is left on the server, and the rejoin succeeds", async () => {
  const socket = openSocket("/socket", fast)
  socket.connect()
  let sleep = 300
  const room = topic("slow")
  const channel = socket.channel(room, () => ({ sleep }))
  const answers: Array<string> = []
  const joinPush = channel.join(100)
  for (const status of ["ok", "error", "timeout"] as const)
    joinPush.receive(status, () => answers.push(status))
  await waitFor(() => answers.length === 1)
  expect(answers).toEqual(["timeout"])
  sleep = 0
  await waitFor(() => channel.state === "joined")
  expect(answers.at(-1)).toBe("ok")
  // The late "ok" for the first join never counted as this channel's join.
  expect(answers.filter((status) => status === "ok")).toHaveLength(1)
})

test("a crashed channel errors, fails its in-flight pushes at once, and rejoins", async () => {
  const socket = openSocket("/socket", fast)
  socket.connect()
  const channel = socket.channel(topic("crash"))
  await join(channel)
  const errors: Array<unknown> = []
  channel.onError((reason) => errors.push(reason))

  const started = Date.now()
  const unanswered = channel.push("noreply", {}, 10_000)
  const crash = channel.push("crash", {}, 10_000)
  expect((await answer(crash)).status).toBe("timeout")
  expect((await answer(unanswered)).status).toBe("timeout")
  expect(Date.now() - started).toBeLessThan(1000)
  expect(errors).toHaveLength(1)

  await waitFor(() => channel.state === "joined")
  expect(await answer(channel.push("joins"))).toEqual({
    status: "ok",
    response: { joins: 2 },
  })

  // A message from the crashed join is not this join's.
  const stale = {
    topic: channel.topic,
    event: "pushed",
    payload: {},
    ref: null,
    join_ref: "0",
  }
  expect(channel.isMember(stale)).toBe(false)
  expect(channel.isMember({ ...stale, topic: "room:other" })).toBe(false)
  expect(channel.isMember({ ...stale, join_ref: null })).toBe(true)
})

test("a channel the server stops is closed for good", async () => {
  const socket = openSocket()
  socket.connect()
  const channel = socket.channel(topic("stop"))
  await join(channel)
  const closes: Array<unknown> = []
  channel.onClose((reason) => closes.push(reason))
  const pending = channel.push("noreply", {}, 10_000)
  channel.push("stop_channel")
  await waitFor(() => channel.state === "closed")
  expect(closes).toEqual([{}])
  expect((await answer(pending)).status).toBe("timeout")
  expect(socket.channels).not.toContain(channel)
  // Nothing answers a push to a closed channel.
  const after = channel.push("echo", {}, 50)
  expect((await answer(after)).status).toBe("timeout")
})

test("leaving: before joining, while joining, and when the reply does not come", async () => {
  const socket = openSocket()
  const unjoined = socket.channel(topic("unjoined"))
  expect((await answer(unjoined.leave())).status).toBe("ok")
  expect(unjoined.state).toBe("closed")

  // Waiting for the socket to open: nothing to tell the server.
  const waiting = socket.channel(topic("waiting"))
  waiting.join()
  const buffered = waiting.push("echo", {}, 10_000)
  expect(waiting.state).toBe("joining")
  expect((await answer(waiting.leave())).status).toBe("ok")
  // It never went out, and now it never will.
  expect((await answer(buffered)).status).toBe("timeout")

  socket.connect()
  const impatient = socket.channel(topic("impatient"))
  await join(impatient)
  expect((await answer(impatient.leave(0))).status).toBe("timeout")
  expect(impatient.state).toBe("closed")

  // The connection drops before the reply: the server has dropped the channel too.
  const dropped = socket.channel(topic("dropped"))
  await join(dropped)
  const leave = dropped.leave()
  // Leaving again while leaving is the same leave.
  expect(dropped.leave()).toBe(leave)
  socket.disconnect()
  expect((await answer(leave)).status).toBe("ok")
  expect(dropped.state).toBe("closed")
  // Leaving a closed channel is answered at once; joining it is a mistake.
  expect((await answer(dropped.leave())).status).toBe("ok")
  expect((await answer(unjoined.leave())).status).toBe("ok")
  expect(() => unjoined.join()).toThrow("which was left or replaced")
})

test("a buffered push's timer starts again when it is sent", async () => {
  const socket = openSocket()
  const channel = socket.channel(topic("buffered_timer"))
  channel.join()
  const buffered = channel.push("slow", { sleep: 200 }, 300)
  await new Promise((resolve) => setTimeout(resolve, 200))
  socket.connect()
  // Sent about 200 ms in, answered about 200 ms later: past 300 ms from the push, inside it from the send.
  expect(await answer(buffered)).toEqual({
    status: "ok",
    response: { sleep: 200 },
  })
})

test("a channel errored by a lost connection is left without the server", async () => {
  const socket = openSocket("/socket", { reconnectAfterMs: () => 60_000 })
  socket.connect()
  const channel = socket.channel(topic("errored"))
  await join(channel)
  channel.push("drain")
  await waitFor(() => channel.state === "errored")
  expect((await answer(channel.leave())).status).toBe("ok")
  expect(channel.state).toBe("closed")
})

test("a newer channel object for a topic closes the older one", async () => {
  const socket = openSocket()
  socket.connect()
  const room = topic("duplicate")
  const older = socket.channel(room)
  await join(older)
  const newer = socket.channel(room)
  await waitFor(() => older.state === "closed")
  await join(newer)
  expect((await answer(older.leave())).status).toBe("ok")
  expect(socket.channels).toEqual([newer])
})
