import { expect, test } from "vitest"
import type { CloseDetails } from "../assets/js/phoenix_durable_channel/actors"
import { LongPoll } from "../assets/js/phoenix_durable_channel/longpoll"
import { Serializer } from "../assets/js/phoenix_durable_channel/serializer"
import { Socket } from "../assets/js/phoenix_durable_channel/socket"
import { answer, join, openSocket, phoenixUrl, topic, waitFor } from "./support/helpers"

test("opens, reports, disconnects and connects again", async () => {
  const logs: Array<string> = []
  const socket = openSocket("/socket", {
    logger: (kind, message) => logs.push(`${kind}: ${message}`),
  })
  const events: Array<string> = []
  const refs = [
    socket.onOpen(() => events.push("open")),
    socket.onClose((event) => events.push(`close ${event.code}`)),
    socket.onError(() => events.push("error")),
    socket.onMessage((message) => events.push(`message ${message.event}`)),
  ]
  expect(socket.connectionState()).toBe("closed")
  expect(socket.hasLogger()).toBe(true)
  expect(socket.transport).toBe(WebSocket)
  socket.connect()
  expect(socket.connectionState()).toBe("connecting")
  await waitFor(() => socket.isConnected())
  expect(socket.establishedConnections).toBe(1)

  const channel = socket.channel(topic("lifecycle"))
  await join(channel)
  expect(events).toEqual(["open", "message phx_reply"])

  await new Promise<void>((resolve) => socket.disconnect(resolve, 1000, "bye"))
  expect(socket.connectionState()).toBe("closed")
  expect(socket.closeWasClean).toBe(true)
  expect(events).toEqual(["open", "message phx_reply", "close 1000"])
  await waitFor(() => channel.state === "errored")

  // Buffered while closed, sent once the socket opens again.
  expect(socket.ping(() => {})).toBe(false)
  const replies: Array<string | null> = []
  socket.onMessage((message) => replies.push(message.ref))
  socket.push({
    topic: "phoenix",
    event: "heartbeat",
    payload: {},
    ref: "x",
    join_ref: null,
  })
  expect(logs).toContain("push: buffered until the socket opens")

  socket.off(refs)
  socket.connect()
  await waitFor(() => channel.state === "joined")
  expect(replies).toContain("x")
  // Two pings at once: each answers only to its own reply.
  const first = new Promise<number>((resolve) => socket.ping(resolve))
  const latency = await new Promise<number>((resolve) => expect(socket.ping(resolve)).toBe(true))
  expect(latency).toBeGreaterThanOrEqual(0)
  expect(await first).toBeGreaterThanOrEqual(0)
  expect(await answer(channel.push("joins"))).toMatchObject({
    response: { joins: 2 },
  })
  expect(events).toHaveLength(3)
  expect(socket.establishedConnections).toBe(2)
})

test("disconnecting a socket that never connected still calls back", async () => {
  const socket = new Socket(`${phoenixUrl}/socket`)
  await new Promise<void>((resolve) => socket.disconnect(resolve))
  expect(socket.connectionState()).toBe("closed")
})

test("disconnecting while the connection is still opening reports the close", async () => {
  const socket = openSocket("/hang")
  const closes: Array<CloseDetails> = []
  socket.onClose((event) => closes.push(event))
  socket.connect()
  expect(socket.connectionState()).toBe("connecting")
  socket.disconnect(undefined, 1000, "bye")
  expect(closes).toEqual([{ code: 1000, reason: "bye", wasClean: true }])
})

test("replaceTransport closes the connection and the next connect uses the new transport", async () => {
  const socket = openSocket("/socket")
  socket.connect()
  await waitFor(() => socket.isConnected())
  socket.replaceTransport(LongPoll)
  await waitFor(() => socket.connectionState() === "closed")
  expect(socket.transport).toBe(LongPoll)
  socket.connect()
  await waitFor(() => socket.isConnected())
  expect(socket.status.transport).toBe("longpoll")
  socket.replaceTransport(WebSocket)
  socket.connect()
  await waitFor(() => socket.isConnected())
  expect(socket.status.transport).toBe("websocket")
})

test("reconnects after every close it did not ask for, whatever the code", async () => {
  const fast = { reconnectAfterMs: () => 10, rejoinAfterMs: () => 10 }
  // The server closes an idle socket with 1000, which phoenix.js 1.7 takes as final.
  const idle = openSocket("/idle_socket", fast)
  const idleCloses: Array<CloseDetails> = []
  idle.onClose((event) => idleCloses.push(event))
  idle.connect()
  const idleChannel = idle.channel(topic("idle"))
  await join(idleChannel)
  await waitFor(
    () => idleCloses.length === 1 && idle.isConnected() && idleChannel.state === "joined",
    5000,
  )
  expect(idleCloses[0]?.code).toBe(1000)

  const socket = openSocket("/socket", { ...fast, params: { id: "carol" } })
  const closes: Array<CloseDetails> = []
  socket.onClose((event) => closes.push(event))
  socket.connect()
  const channel = socket.channel(topic("reconnect"))
  await join(channel)

  // A deploy drains sockets with 1012.
  const inFlight = channel.push("noreply", {}, 10_000)
  channel.push("drain")
  expect((await answer(inFlight)).status).toBe("timeout")
  await waitFor(() => closes.length === 1 && socket.isConnected() && channel.state === "joined")
  expect(closes[0]?.code).toBe(1012)

  // Phoenix's "disconnect" broadcast to the socket's id.
  channel.push("disconnect_socket")
  await waitFor(() => closes.length === 2 && socket.isConnected() && channel.state === "joined")
  expect(await answer(channel.push("joins"))).toMatchObject({
    response: { joins: 3 },
  })
  expect(socket.establishedConnections).toBe(3)
})

test("heartbeats, and closes a connection whose heartbeat goes unanswered", async () => {
  const beating = openSocket("/socket", { heartbeatIntervalMs: 20 })
  const replies: Array<string> = []
  beating.onMessage((message) => {
    if (message.topic === "phoenix") replies.push(message.event)
  })
  beating.connect()
  await waitFor(() => replies.length >= 3)
  expect(replies.every((event) => event === "phx_reply")).toBe(true)

  const silent = openSocket("/socket", {
    heartbeatIntervalMs: 20,
    heartbeatTimeoutMs: 0,
    reconnectAfterMs: () => 10,
  })
  const closes: Array<CloseDetails> = []
  silent.onClose((event) => closes.push(event))
  silent.connect()
  await waitFor(() => closes.length >= 2)
  expect(closes[0]).toEqual({
    code: 1000,
    reason: "heartbeat timeout",
    wasClean: false,
  })
  expect(silent.closeWasClean).toBe(false)
})

test("a refused connection reports errors, keeps trying, and goes offline after a while", async () => {
  let token = "bad"
  const socket = openSocket("/socket", {
    params: () => ({ token }),
    reconnectAfterMs: () => 20,
    offlineAfterMs: 50,
  })
  const errors: Array<[unknown, unknown, number]> = []
  socket.onError((error, transport, established) => errors.push([error, transport, established]))
  socket.connect()
  expect(socket.status.online).toBe(true)
  await waitFor(() => !socket.status.online)
  expect(errors.length).toBeGreaterThanOrEqual(2)
  expect(errors[0]?.[1]).toBe(WebSocket)
  expect(errors[0]?.[2]).toBe(0)

  token = "good"
  await waitFor(() => socket.isConnected())
  expect(socket.status.online).toBe(true)
})

test("a short drop does not count as offline", async () => {
  const socket = openSocket("/socket", {
    reconnectAfterMs: () => 10,
    offlineAfterMs: 5000,
  })
  socket.connect()
  const channel = socket.channel(topic("blip"))
  await join(channel)
  channel.push("drain")
  await waitFor(() => !socket.isConnected())
  await waitFor(() => socket.isConnected())
  expect(socket.status.online).toBe(true)
})

test("builds its URL from the endpoint, params and vsn", () => {
  const relative = new Socket("/socket", {
    params: { token: "t", user: { id: 1 } },
    vsn: "1.0.0",
  })
  expect(relative.endPointURL()).toBe(
    `ws://${location.host}/socket/websocket?token=t&user%5Bid%5D=1&vsn=1.0.0`,
  )
  expect(relative.protocol()).toBe("ws")
  expect(relative.hasLogger()).toBe(false)
  expect(relative.timeout).toBe(10_000)

  const logs: Array<string> = []
  const deprecated = new Socket(`${phoenixUrl}/socket`, {
    timeout: 500,
    logger: (_kind, message) => logs.push(message),
  })
  deprecated.connect({ token: "old" })
  expect(deprecated.endPointURL()).toBe(`${phoenixUrl}/socket/websocket?token=old&vsn=2.0.0`)
  expect(logs[0]).toContain("passing params to connect is deprecated")
  deprecated.disconnect()
})

test("reconnect and rejoin delays grow, with jitter, up to a cap", () => {
  const socket = new Socket(`${phoenixUrl}/socket`)
  for (const [tries, base] of [
    [1, 10],
    [2, 20],
    [10, 5000],
  ] as const) {
    expect(socket.reconnectAfterMs(tries)).toBeGreaterThanOrEqual(base)
    expect(socket.reconnectAfterMs(tries)).toBeLessThanOrEqual(base * 1.2)
  }
  expect(socket.rejoinAfterMs(1)).toBeGreaterThanOrEqual(1000)
  expect(socket.rejoinAfterMs(20)).toBeLessThanOrEqual(12_000)
})

test("uses the encode and decode it is given", async () => {
  const encoded: Array<string> = []
  const socket = openSocket("/socket", {
    encode: (message, callback) =>
      Serializer.encode(message, (data) => {
        encoded.push(message.event)
        callback(data)
      }),
    decode: (raw, callback) => Serializer.decode(raw, callback),
  })
  socket.connect()
  const channel = socket.channel(topic("codec"))
  await join(channel)
  expect(encoded).toEqual(["phx_join"])
})
