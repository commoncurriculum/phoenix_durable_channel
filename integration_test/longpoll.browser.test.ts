import { expect, test } from "vitest"
import type { CloseDetails } from "../assets/js/phoenix_durable_channel/actors"
import { LongPoll } from "../assets/js/phoenix_durable_channel/longpoll"
import { answer, join, nextEvent, openSocket, phoenixUrl, topic, waitFor } from "./support/helpers"

const fast = { reconnectAfterMs: () => 10, rejoinAfterMs: () => 10 }

test("joins, pushes and receives over LongPoll", async () => {
  const socket = openSocket("/socket", { transport: LongPoll })
  expect(socket.transport).toBe(LongPoll)
  expect(socket.status.transport).toBe("longpoll")
  socket.connect()
  const channel = socket.channel(topic("longpoll"), { via: "longpoll" })
  expect(await join(channel)).toMatchObject({ params: { via: "longpoll" } })

  const pushes = [1, 2, 3].map((n) => channel.push("echo", { n }))
  expect(await Promise.all(pushes.map(answer))).toEqual(
    [1, 2, 3].map((n) => ({ status: "ok", response: { n } })),
  )

  const broadcast = nextEvent(channel, "broadcasted")
  channel.push("broadcast", { b: true })
  expect(await broadcast).toEqual({ b: true })

  expect(await answer(channel.push("binary_size", new Uint8Array([1, 2, 3, 4]).buffer))).toEqual({
    status: "ok",
    response: { size: 4 },
  })
})

test("a lost LongPoll session closes the connection, so channels rejoin on the new one", async () => {
  const socket = openSocket("/socket", { transport: LongPoll, ...fast })
  const closes: Array<CloseDetails> = []
  const errors: Array<unknown> = []
  socket.onClose((event) => closes.push(event))
  socket.onError((error) => errors.push(error))
  socket.connect()
  const channel = socket.channel(topic("session"))
  await join(channel)
  channel.push("kill_transport")
  await waitFor(() => closes.length === 1 && channel.state === "joined")
  expect(closes[0]).toEqual({
    code: 3410,
    reason: "session gone",
    wasClean: false,
  })
  expect(errors).toEqual([410])
  expect(await answer(channel.push("joins"))).toMatchObject({
    response: { joins: 2 },
  })
})

test("LongPoll reports a refused socket, a server it cannot reach, and a poll that hangs", async () => {
  const refused = openSocket("/socket", {
    transport: LongPoll,
    params: { token: "bad" },
    reconnectAfterMs: () => 60_000,
  })
  const refusedCloses: Array<CloseDetails> = []
  refused.onClose((event) => refusedCloses.push(event))
  refused.connect()
  await waitFor(() => refusedCloses.length === 1)
  expect(refusedCloses[0]).toEqual({
    code: 1008,
    reason: "forbidden",
    wasClean: false,
  })

  const unreachable = new LongPoll("ws://127.0.0.1:1/socket/websocket")
  const unreachableCloses: Array<CloseDetails> = []
  unreachable.onclose = (event) => unreachableCloses.push(event)
  await waitFor(() => unreachableCloses.length === 1)
  expect(unreachableCloses[0]).toEqual({
    code: 1011,
    reason: "internal server error",
    wasClean: false,
  })
  unreachable.close()
  expect(unreachableCloses).toHaveLength(1)

  const hanging = openSocket("/socket", {
    transport: LongPoll,
    longpollerTimeout: 300,
    reconnectAfterMs: () => 60_000,
  })
  const hangingCloses: Array<CloseDetails> = []
  hanging.onClose((event) => hangingCloses.push(event))
  hanging.connect()
  await waitFor(() => hangingCloses.length === 1)
  expect(hangingCloses[0]).toEqual({
    code: 1005,
    reason: "timeout",
    wasClean: false,
  })
})

test("LongPoll fails on a POST the server rejects or does not answer in time", async () => {
  const open = async () => {
    const connection = new LongPoll(`${phoenixUrl}/socket/websocket?vsn=2.0.0`)
    expect(connection.readyState).toBe(WebSocket.CONNECTING)
    expect(connection.timeout).toBe(20_000)
    const closes: Array<CloseDetails> = []
    connection.onclose = (event) => closes.push(event)
    await new Promise((resolve) => (connection.onopen = resolve))
    expect(connection.readyState).toBe(WebSocket.OPEN)
    return { connection, closes }
  }

  const rejected = await open()
  rejected.connection.send("[not json")
  await waitFor(() => rejected.closes.length === 1)
  expect(rejected.closes[0]?.code).toBe(1011)
  expect(rejected.connection.readyState).toBe(WebSocket.CLOSED)

  const slow = await open()
  slow.connection.timeout = 0
  expect(slow.connection.timeout).toBe(0)
  slow.connection.send('[null,"1","phoenix","heartbeat",{}]')
  await waitFor(() => slow.closes.length === 1)
  expect(slow.closes[0]).toEqual({
    code: 1005,
    reason: "timeout",
    wasClean: false,
  })
})

test("falls back to LongPoll when WebSocket never opens, and tries WebSocket again later", async () => {
  const socket = openSocket("/longpoll_only", {
    longPollFallbackMs: 2000,
    websocketRetryAfterMs: 300,
    ...fast,
  })
  socket.connect()
  const channel = socket.channel(topic("fallback"))
  await join(channel)
  expect(socket.status.transport).toBe("longpoll")
  expect(socket.transport).toBe(LongPoll)
  await waitFor(() => socket.establishedConnections >= 2 && channel.state === "joined", 8000)
  expect(socket.status.transport).toBe("longpoll")

  const slow = openSocket("/hang", { longPollFallbackMs: 200 })
  slow.connect()
  await waitFor(() => slow.isConnected())
  expect(slow.status.transport).toBe("longpoll")
})
