import { expect, test } from "vitest"
import type { CloseDetails } from "../assets/js/phoenix_durable_channel/actors"
import type { Channel } from "../assets/js/phoenix_durable_channel/channel"
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

  const inFlight = channel.push("noreply", {}, 10_000)
  channel.push("drain")
  expect((await answer(inFlight)).status).toBe("timeout")
  await waitFor(() => closes.length === 1 && socket.isConnected() && channel.state === "joined")
  expect(closes[0]?.code).toBe(1012)

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

test("a channel joined for a new connection from a socket callback is not errored by the old one", async () => {
  for (const replace of [false, true]) {
    const socket = openSocket("/socket")
    socket.connect()
    await join(socket.channel(topic("old")))
    const fresh: Array<Channel> = []
    const errors: Array<unknown> = []
    const ref = socket.onMessage(() => {
      socket.off([ref])
      if (replace) socket.replaceTransport(WebSocket)
      else socket.disconnect()
      socket.connect()
      const channel = socket.channel(topic("fresh"))
      channel.onError((reason) => errors.push(reason))
      channel.join()
      fresh.push(channel)
    })
    socket.ping(() => {})
    await waitFor(() => fresh[0]?.state === "joined")
    expect(errors).toEqual([])
  }
})

test("a socket closed from its open callback leaves its channels waiting, and they join once when it opens again", async () => {
  const socket = openSocket("/socket", { reconnectAfterMs: () => 10 })
  const name = topic("closed-on-open")
  const replies: Array<string> = []
  socket.onMessage((message) => {
    if (message.topic === name && message.event === "phx_reply") replies.push(message.event)
  })
  const closing = socket.onOpen(() => socket.disconnect())
  const channel = socket.channel(name)
  const joined = answer(channel.join())
  socket.connect()
  await waitFor(() => socket.establishedConnections === 1)
  await new Promise((resolve) => setTimeout(resolve, 300))
  expect(socket.connectionState()).toBe("closed")
  expect(socket.establishedConnections).toBe(1)
  expect(channel.state).not.toBe("joined")

  socket.off([closing])
  socket.connect()
  expect((await joined).status).toBe("ok")
  await new Promise((resolve) => setTimeout(resolve, 200))
  expect(replies).toEqual(["phx_reply"])
  expect(channel.state).toBe("joined")
})

test("a channel left while its join waits in the send buffer leaves that join on the server too", async () => {
  const socket = openSocket("/socket", { reconnectAfterMs: () => 10 })
  const name = topic("left-in-buffer")
  const events: Array<string> = []
  socket.onMessage((message) => {
    if (message.topic === name) events.push(message.event)
  })
  const closing = socket.onOpen(() => socket.disconnect())
  const channel = socket.channel(name)
  channel.join()
  socket.connect()
  await waitFor(() => socket.establishedConnections === 1)
  await waitFor(() => socket.connectionState() === "closed")
  const unsent = channel.push("echo", {}, 10_000)
  expect((await answer(channel.leave())).status).toBe("ok")
  expect(channel.state).toBe("closed")
  expect((await answer(unsent)).status).toBe("timeout")

  socket.off([closing])
  socket.connect()
  await waitFor(() => events.includes("phx_close"))
  expect(events).toEqual(["phx_reply", "phx_reply", "phx_close"])
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

  const debugging = new Socket("/socket", { debug: true, sessionStorage: window.sessionStorage })
  expect(debugging.hasLogger()).toBe(true)
  debugging.log("transport", "debug logs to the console", { debug: true })

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

test("a frame that encode hands back after its connection closed is not sent on the next one", async () => {
  const sent: Array<string> = []
  class RecordedWebSocket extends WebSocket {
    override send(data: string | ArrayBufferLike | Blob | ArrayBufferView) {
      sent.push(String(data))
      super.send(data)
    }
  }
  const held: Array<() => void> = []
  const socket = openSocket("/socket", {
    transport: RecordedWebSocket,
    encode: (message, callback) =>
      Serializer.encode(message, (data) => {
        if (message.event === "late") held.push(() => callback(data))
        else callback(data)
      }),
  })
  socket.connect()
  const channel = socket.channel(topic("late"))
  await join(channel)
  channel.push("late", {})
  expect(held).toHaveLength(1)

  socket.disconnect()
  socket.connect()
  await waitFor(() => socket.isConnected() && channel.state === "joined")
  held[0]!()
  await answer(channel.push("echo", {}))
  expect(sent.filter((data) => data.includes('"late"'))).toEqual([])
})

test("uses JSON over LongPoll whatever encode and decode it is given, as phoenix.js does", async () => {
  const custom = {
    encode: (_message: unknown, callback: (encoded: string) => void) => callback("custom"),
    decode: (_raw: unknown, callback: (message: never) => void) => callback("custom" as never),
  }
  const socket = openSocket("/socket", { transport: LongPoll, ...custom })
  const message = { join_ref: "1", ref: "2", topic: "t", event: "e", payload: { a: 1 } }
  socket.encode(message, (encoded) => expect(encoded).toBe('["1","2","t","e",{"a":1}]'))
  socket.decode('["1","2","t","e",{"a":1}]', (decoded) => expect(decoded).toEqual(message))
  socket.connect()
  await join(socket.channel(topic("codec")))

  const logger = () => {}
  const websocket = openSocket("/socket", {
    ...custom,
    heartbeatIntervalMs: 60_000,
    logger,
    params: { one: "two" },
  })
  websocket.encode(message, (encoded) => expect(encoded).toBe("custom"))
  expect(websocket.heartbeatIntervalMs).toBe(60_000)
  expect(websocket.logger).toBe(logger)
  expect(websocket.params()).toEqual({ one: "two" })
})

test("sends authToken in Sec-WebSocket-Protocol, and over LongPoll in a header", async () => {
  for (const transport of [WebSocket, LongPoll]) {
    const tokens = ["first-token", "second-token"]
    const socket = openSocket("/socket", { transport, authToken: () => tokens[0]! })
    socket.connect()
    const first = socket.channel(topic("auth"))
    await join(first)
    expect(await answer(first.push("auth_token", {}))).toEqual({
      status: "ok",
      response: { auth_token: "first-token" },
    })
    tokens.shift()
    await new Promise<void>((resolve) => socket.disconnect(resolve))
    socket.connect()
    const second = socket.channel(topic("auth"))
    await join(second)
    expect(await answer(second.push("auth_token", {}))).toEqual({
      status: "ok",
      response: { auth_token: "second-token" },
    })
  }

  const plain = openSocket("/socket", { authToken: "plain", binaryType: "blob" })
  expect(plain.binaryType).toBe("blob")
  plain.connect()
  const channel = plain.channel(topic("auth"))
  await join(channel)
  expect(await answer(channel.push("auth_token", {}))).toEqual({
    status: "ok",
    response: { auth_token: "plain" },
  })

  const none = openSocket()
  none.connect()
  const anonymous = none.channel(topic("auth"))
  await join(anonymous)
  expect(await answer(anonymous.push("auth_token", {}))).toEqual({
    status: "ok",
    response: { auth_token: null },
  })
})
