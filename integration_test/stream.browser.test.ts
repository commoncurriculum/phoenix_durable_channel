import { expect, inject, test } from "vitest"
import type { Channel } from "../assets/js/phoenix_durable_channel/channel"
import { DurableStream, S2Stream, type PublishedReader, type StreamMessage } from "../assets/js/phoenix_durable_channel/stream"
import { answer, join, openSocket, waitFor } from "./support/helpers"

const s2Url = inject("s2Url")
const durableStreamsUrl = inject("durableStreamsUrl")
const basin = "phoenix-durable-channel"
const fast = { rejoinAfterMs: () => 10, reconnectAfterMs: () => 10 }

function durableTopic(name: string): string {
  return `durable:${name}:${Math.random().toString(36).slice(2)}`
}

/** Writes to S2 as the server would, without it. Appending creates the stream. */
async function appendToS2(stream: string, bodies: Array<string>): Promise<void> {
  const records = bodies.map((body) => ({
    body: JSON.stringify({ event: "update", payload: { body } }),
  }))
  const response = await fetch(`${s2Url}/v1/streams/${encodeURIComponent(stream)}/records`, {
    method: "POST",
    headers: { "content-type": "application/json", "s2-basin": basin },
    body: JSON.stringify({ records }),
  })
  expect(response.ok).toBe(true)
}

async function deleteS2Stream(stream: string): Promise<void> {
  const response = await fetch(`${s2Url}/v1/streams/${encodeURIComponent(stream)}`, {
    method: "DELETE",
    headers: { "s2-basin": basin },
  })
  expect(response.ok).toBe(true)
}

function updates(channel: Channel): Array<string> {
  const bodies: Array<string> = []
  channel.on("update", (payload) => bodies.push((payload as { body: string }).body))
  return bodies
}

async function publish(channel: Channel, body: string): Promise<void> {
  expect((await answer(channel.push("publish", { body }))).status).toBe("ok")
}

async function joinedReader(channel: Channel): Promise<PublishedReader> {
  return ((await join(channel)) as { phx_reader: PublishedReader }).phx_reader
}

const pause = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

test("the server names S2 in its join reply, and the channel reads its durable messages there, in order, after a number", async () => {
  const topic = durableTopic("s2")
  await appendToS2(topic, ["one"])
  const socket = openSocket("/socket", fast)
  socket.connect()
  const channel = socket.channel(topic, { stream: "s2" }, { durable: true, after: 0 })
  const seen = updates(channel)
  expect(await joinedReader(channel)).toEqual({
    type: "s2",
    endpoint: s2Url,
    basin,
    stream: topic,
    token: null,
    last: 1,
  })

  await waitFor(() => seen.length === 1)
  await publish(channel, "two")
  await publish(channel, "three")
  await waitFor(() => seen.length === 3)
  // Phoenix does not push them as well: each arrives once.
  await pause(200)
  expect(seen).toEqual(["one", "two", "three"])

  // Leaving stops the read.
  await answer(channel.leave())
  await appendToS2(topic, ["four"])
  await pause(300)
  expect(seen).toEqual(["one", "two", "three"])
})

test("without a number, a channel reads what the store gets after the join, however late its read opens", async () => {
  const topic = durableTopic("tail")
  await appendToS2(topic, ["before"])
  const socket = openSocket("/socket", fast)
  socket.connect()
  const channel = socket.channel(topic, { stream: "s2" }, { durable: true })
  const seen = updates(channel)
  await join(channel)
  await appendToS2(topic, ["after"])

  await waitFor(() => seen.length === 1)
  await pause(200)
  expect(seen).toEqual(["after"])
})

test("a read that ends is read again, after a backoff, from the last message delivered", async () => {
  const topic = durableTopic("again")
  const socket = openSocket("/socket", fast)
  socket.connect()
  const channel = socket.channel(topic, { stream: "s2" }, { durable: true, after: 0 })
  const seen = updates(channel)
  await join(channel)
  await publish(channel, "one")
  await waitFor(() => seen.length === 1)

  // Deleting the stream writes a trim command record, which is not a message, then ends
  // the read with an error. The next read starts after "one".
  await deleteS2Stream(topic)
  await pause(300)
  await appendToS2(topic, ["one again", "two"])
  await waitFor(() => seen.length === 2)
  expect(seen).toEqual(["one", "two"])
  expect(channel.state).toBe("joined")
})

test("an app reads a store the client has no reader for with its own, and without one the channel says so", async () => {
  const logs: Array<string> = []
  const socket = openSocket("/socket", {
    ...fast,
    logger: (_kind, message) => logs.push(message),
  })
  socket.connect()

  const unread = socket.channel(durableTopic("unread"), { stream: "elsewhere" }, { durable: true })
  expect((await joinedReader(unread)).type).toBe("elsewhere")
  await waitFor(() => logs.includes('no reader for the store "elsewhere"'))

  const topic = durableTopic("elsewhere")
  const channel = socket.channel(
    topic,
    { stream: "elsewhere" },
    {
      durable: true,
      after: 0,
      readers: {
        elsewhere: (latest) =>
          new S2Stream({
            endpoint: s2Url,
            basin,
            stream: String(latest()["stream"]),
          }),
      },
    },
  )
  const seen = updates(channel)
  await join(channel)
  await publish(channel, "one")
  await waitFor(() => seen.length === 1)
  expect(seen).toEqual(["one"])
})

test("while the store cannot be reached, the channel keeps trying and stays joined", async () => {
  const socket = openSocket("/socket", fast)
  socket.connect()
  const channel = socket.channel(
    durableTopic("unreachable"),
    { stream: "s2" },
    {
      durable: true,
      readers: {
        s2: () =>
          new S2Stream({
            endpoint: "http://127.0.0.1:1",
            basin,
            stream: "unreachable",
          }),
      },
    },
  )
  await join(channel)
  await pause(100)
  expect(channel.state).toBe("joined")
})

/** Signs a JWT for the test project, as another server node holding its secret would. */
async function durableStreamsToken(scope: string, stream: string): Promise<string> {
  const encode = (bytes: Uint8Array) =>
    btoa(String.fromCharCode(...bytes))
      .replaceAll("+", "-")
      .replaceAll("/", "_")
      .replace(/=+$/, "")
  const part = (value: object) => encode(new TextEncoder().encode(JSON.stringify(value)))
  const claims = {
    sub: "phoenix-durable-channel",
    scope,
    stream_id: stream,
    exp: Date.now() / 1000 + 600,
  }
  const signed = `${part({ alg: "HS256", typ: "JWT" })}.${part(claims)}`
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode("phoenix-durable-channel-signing-secret"),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  )
  const signature = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(signed))
  return `${signed}.${encode(new Uint8Array(signature))}`
}

test("the server names a Durable Streams store, and a node whose next number another node took goes on from the store's last", async () => {
  const topic = durableTopic("durable-streams")
  const socket = openSocket("/socket", fast)
  socket.connect()
  const channel = socket.channel(topic, { stream: "durable-streams" }, { durable: true, after: 0 })
  const seen = updates(channel)
  const reader = await joinedReader(channel)
  expect(reader).toMatchObject({
    type: "durable-streams",
    url: `${durableStreamsUrl}/v1/stream/phoenix-durable-channel/${encodeURIComponent(topic)}`,
    last: 0,
  })
  expect(reader["token"]).toEqual(expect.any(String))

  await publish(channel, "one")
  await publish(channel, "two")
  await waitFor(() => seen.length === 2)

  // Another node, which read the stream after this one, appends message 3.
  const response = await fetch(String(reader["url"]), {
    method: "POST",
    headers: {
      authorization: `Bearer ${await durableStreamsToken("write", topic)}`,
      "content-type": "application/json",
      "stream-seq": "3".padStart(20, "0"),
    },
    body: JSON.stringify({
      number: 3,
      event: "update",
      payload: { body: "another node" },
    }),
  })
  expect(response.ok).toBe(true)
  await waitFor(() => seen.length === 3)

  // This node's append of 3 is refused; it reads on, and numbers the message 4.
  await publish(channel, "four")
  await waitFor(() => seen.length === 4)
  await pause(200)
  expect(seen).toEqual(["one", "two", "another node", "four"])
})

function collect(stream: DurableStream, after: number) {
  const delivered: Array<StreamMessage> = []
  const failures: Array<unknown> = []
  const stop = stream.read(
    { after, epoch: "" },
    {
      deliver: (message) => delivered.push(message),
      moved: () => {},
      failed: (error) => failures.push(error),
    },
  )
  return { delivered, failures, stop }
}

test("a Durable Streams read after the last one, or after where the join said the stream ended, starts there; any other at the beginning", async () => {
  const topic = durableTopic("offsets")
  const socket = openSocket("/socket", fast)
  socket.connect()
  const channel = socket.channel(topic, { stream: "durable-streams" }, { durable: true })
  const reader = await joinedReader(channel)
  const token = String(reader["token"])
  const stream = new DurableStream({
    url: String(reader["url"]),
    token: () => token,
  })
  await publish(channel, "one")
  await publish(channel, "two")
  const numbers = (read: { delivered: Array<StreamMessage> }) =>
    read.delivered.map((message) => message.number)

  const first = collect(stream, 0)
  await waitFor(() => first.delivered.length === 2)
  first.stop()
  expect(first.delivered).toEqual([
    { number: 1, event: "update", payload: { body: "one" } },
    { number: 2, event: "update", payload: { body: "two" } },
  ])

  const resumed = collect(stream, 2)
  await publish(channel, "three")
  await waitFor(() => resumed.delivered.length === 1)
  resumed.stop()
  expect(numbers(resumed)).toEqual([3])

  const fromTheStart = collect(
    new DurableStream({ url: String(reader["url"]), token: () => token }),
    1,
  )
  await waitFor(() => fromTheStart.delivered.length === 2)
  fromTheStart.stop()
  expect(numbers(fromTheStart)).toEqual([2, 3])

  const laterReader = (await answer(channel.push("phx_reader"))).response as PublishedReader
  expect(laterReader["last"]).toBe(3)
  const fromTheJoin = collect(
    new DurableStream({
      url: String(reader["url"]),
      token: () => token,
      resume: { number: 3, offset: String(laterReader["offset"]) },
    }),
    3,
  )
  await publish(channel, "four")
  await waitFor(() => fromTheJoin.delivered.length === 1)
  fromTheJoin.stop()
  expect(numbers(fromTheJoin)).toEqual([4])
})

test("a Durable Streams read without a token is refused, and fails", async () => {
  const socket = openSocket("/socket", fast)
  socket.connect()
  const channel = socket.channel(
    durableTopic("refused"),
    { stream: "durable-streams" },
    { durable: true },
  )
  const reader = await joinedReader(channel)
  const refused = collect(new DurableStream({ url: String(reader["url"]), token: () => null }), 0)
  await waitFor(() => refused.failures.length === 1)
  expect(refused.failures).toEqual([401])
  expect(refused.delivered).toEqual([])
})

test("a read token that expired is fetched again, and asked for again when the answer is late", async () => {
  const socket = openSocket("/socket", { ...fast, timeout: 600 })
  socket.connect()
  const channel = socket.channel(
    durableTopic("expiring"),
    { stream: "durable-streams-expiring" },
    { durable: true, after: 0 },
  )
  const seen = updates(channel)
  await join(channel)
  await publish(channel, "one")
  await waitFor(() => seen.length === 1, 10_000)
  expect(seen).toEqual(["one"])
})

test("with the server's memory as the store, a channel gets what was published while it was away", async () => {
  const topic = durableTopic("memory")
  const socket = openSocket("/socket", fast)
  socket.connect()
  const channel = socket.channel(topic, { stream: "memory" }, { durable: true, after: 0 })
  const seen = updates(channel)
  expect(await joinedReader(channel)).toEqual({
    type: "phoenix",
    last: 0,
    epoch: expect.any(String),
  })
  await publish(channel, "one")
  await waitFor(() => seen.length === 1)

  await new Promise<void>((resolve) => socket.disconnect(resolve))
  const other = openSocket("/socket", fast)
  other.connect()
  const otherChannel = other.channel(topic, { stream: "memory" })
  await join(otherChannel)
  await publish(otherChannel, "while away")

  socket.connect()
  await waitFor(() => seen.length === 2)
  expect(seen).toEqual(["one", "while away"])
})
