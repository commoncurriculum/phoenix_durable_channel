import { expect, inject, test } from "vitest"
import type { Channel } from "../assets/js/phoenix_durable_channel/channel"
import {
  builtInReaders,
  DurableStream,
  S2Stream,
  type Position,
  type PublishedReader,
  type Resume,
  type StreamMessage,
} from "../assets/js/phoenix_durable_channel/stream"
import { answer, join, openSocket, phoenixUrl, waitFor } from "./support/helpers"

const s2Url = inject("s2Url")
const durableStreamsUrl = inject("durableStreamsUrl")
const basin = "phoenix-durable-channel"
const fast = { rejoinAfterMs: () => 10, reconnectAfterMs: () => 10 }

function durableTopic(name: string): string {
  return `durable:${name}:${Math.random().toString(36).slice(2)}`
}

async function appendToS2(stream: string, bodies: Array<string>): Promise<void> {
  const response = await fetch(`${phoenixUrl.replace("ws", "http")}/s2/append`, {
    method: "POST",
    body: JSON.stringify({ stream, bodies }),
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
  expect(await join(channel)).toEqual({ phx_durable: true })
  return (await answer(channel.push("phx_reader"))).response as PublishedReader
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
  await pause(200)
  expect(seen).toEqual(["one", "two", "three"])

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

test("a read that ends is read again, after a backoff, from the last message delivered; an S2 command record is no message, and nothing is skipped", async () => {
  const logs: Array<string> = []
  const topic = durableTopic("again")
  const socket = openSocket("/socket", { ...fast, logger: (_kind, message) => logs.push(message) })
  socket.connect()
  const channel = socket.channel(topic, { stream: "s2" }, { durable: true, after: 0 })
  const seen = updates(channel)
  const skipped: Array<unknown> = []
  channel.on("phx_skipped", (payload) => skipped.push(payload))
  await join(channel)
  await publish(channel, "one")
  await waitFor(() => seen.length === 1)

  await deleteS2Stream(topic)
  await pause(300)
  await appendToS2(topic, ["one again", "the deletion's trim again", "two"])
  await waitFor(() => seen.length === 2)
  expect(seen).toEqual(["one", "two"])
  expect(channel.state).toBe("joined")
  expect(skipped).toEqual([])
  expect(logs.filter((line) => line.includes("skipped"))).toEqual([])
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
        elsewhere: (published) =>
          new S2Stream({
            endpoint: s2Url,
            basin,
            stream: String(published["stream"]),
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

test("a number a reader passes is no message, and the numbers it jumps over are skipped", async () => {
  const topic = durableTopic("passed")
  await appendToS2(topic, ["one", "two", "three", "four"])
  const socket = openSocket("/socket", fast)
  socket.connect()
  const channel = socket.channel(
    topic,
    { stream: "elsewhere" },
    {
      durable: true,
      after: 0,
      readers: {
        elsewhere: (published) => ({
          read: (from, callbacks) => {
            callbacks.passed(1)
            callbacks.passed(3)
            return new S2Stream({
              endpoint: s2Url,
              basin,
              stream: String(published["stream"]),
            }).read({ ...from, after: 3 }, callbacks)
          },
        }),
      },
    },
  )
  const seen = updates(channel)
  const skipped: Array<unknown> = []
  channel.on("phx_skipped", (payload) => skipped.push(payload))
  await join(channel)
  await waitFor(() => seen.length === 1)
  expect(seen).toEqual(["four"])
  expect(skipped).toEqual([{ after: 1, next: 3 }])
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
  await fetch(`${phoenixUrl.replace("ws", "http")}/traces/append`, {
    method: "POST",
    body: JSON.stringify({ topic, number: 3 }),
  })
  await waitFor(() => seen.length === 3)

  await publish(channel, "four")
  await waitFor(() => seen.length === 4)
  await pause(200)
  expect(seen).toEqual(["one", "two", "another node", "four"])
})

function collect(stream: DurableStream, after: number, resume: Resume | null = null) {
  const delivered: Array<StreamMessage> = []
  const failures: Array<unknown> = []
  const reached: Array<Resume> = []
  const stop = stream.read(
    { after, epoch: "", resume },
    {
      deliver: (message) => delivered.push(message),
      passed: () => {},
      reached: (position) => reached.push(position),
      failed: (error) => failures.push(error),
    },
  )
  return { delivered, failures, reached, stop }
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
    token,
  })
  await publish(channel, "one")
  await publish(channel, "two")
  const numbers = (read: { delivered: Array<StreamMessage> }) =>
    read.delivered.map((message) => message.number)

  const first = collect(stream, 0)
  await waitFor(() => first.reached.at(-1)?.number === 2)
  first.stop()
  expect(first.delivered).toEqual([
    { number: 1, event: "update", payload: { body: "one" } },
    { number: 2, event: "update", payload: { body: "two" } },
  ])

  const reachedTwo = first.reached.at(-1)
  expect(reachedTwo).toEqual({ number: 2, offset: expect.any(String) })
  const resumed = collect(stream, 2, reachedTwo)
  await publish(channel, "three")
  await waitFor(() => resumed.delivered.length === 1)
  resumed.stop()
  expect(numbers(resumed)).toEqual([3])

  const fromTheStart = collect(stream, 1, reachedTwo)
  await waitFor(() => fromTheStart.delivered.length === 3)
  fromTheStart.stop()
  expect(numbers(fromTheStart)).toEqual([1, 2, 3])

  const laterReader = (await answer(channel.push("phx_reader"))).response as PublishedReader
  expect(laterReader["last"]).toBe(3)
  const fromTheJoin = collect(
    new DurableStream({
      url: String(reader["url"]),
      token,
      resume: { number: 3, offset: String(laterReader["offset"]) },
    }),
    3,
  )
  await publish(channel, "four")
  await waitFor(() => fromTheJoin.delivered.length === 1)
  fromTheJoin.stop()
  expect(numbers(fromTheJoin)).toEqual([4])
})

test("a channel whose Durable Streams read fails reads again from where the store said it had reached", async () => {
  const topic = durableTopic("reached")
  const socket = openSocket("/socket", fast)
  socket.connect()
  const reads: Array<Position> = []
  const channel = socket.channel(
    topic,
    { stream: "durable-streams" },
    {
      durable: true,
      after: 0,
      readers: {
        "durable-streams": (published, refresh) => ({
          read: (from, callbacks) => {
            reads.push(from)
            return builtInReaders["durable-streams"]!(published, refresh).read(from, {
              ...callbacks,
              reached: (resume) => {
                callbacks.reached(resume)
                if (reads.length === 1 && resume.number === 2) callbacks.failed("cut")
              },
            })
          },
        }),
      },
    },
  )
  const seen = updates(channel)
  await join(channel)
  await publish(channel, "one")
  await publish(channel, "two")
  await waitFor(() => reads.length === 2)
  expect(reads[1]).toEqual({
    after: 2,
    epoch: "",
    resume: { number: 2, offset: expect.any(String) },
  })
  expect(reads[1]!.resume!.offset).not.toBe("-1")

  await publish(channel, "three")
  await waitFor(() => seen.length === 3)
  await pause(200)
  expect(seen).toEqual(["one", "two", "three"])
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
  const refused = collect(new DurableStream({ url: String(reader["url"]), token: null }), 0)
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

test("a fresh reader the server will not give is logged, and the read fails and is tried again", async () => {
  const logs: Array<string> = []
  const socket = openSocket("/socket", { ...fast, logger: (_kind, message) => logs.push(message) })
  socket.connect()
  const name = durableTopic("expiring")
  const channel = socket.channel(
    name,
    { stream: "durable-streams-expiring", not_durable: true },
    { durable: true, after: 0 },
  )
  await join(channel)
  await waitFor(
    () => logs.filter((line) => line === `${name} could not get a fresh reader`).length >= 2,
  )
  expect(channel.state).toBe("joined")
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
