import { context, propagation, SpanKind, trace } from "@opentelemetry/api"
import { W3CTraceContextPropagator } from "@opentelemetry/core"
import { StackContextManager, TracerProvider, type ReadableSpan } from "@opentelemetry/sdk-trace"
import { expect, test } from "vitest"
import type { Channel } from "../assets/js/phoenix_durable_channel/channel"
import { LongPoll } from "../assets/js/phoenix_durable_channel/longpoll"
import type { SocketOptions } from "../assets/js/phoenix_durable_channel/socket"
import type { TracedMessage, TracingOptions } from "../assets/js/phoenix_durable_channel/tracing"
import { answer, join, nextEvent, openSocket, topic } from "./support/helpers"

trace.setGlobalTracerProvider(new TracerProvider())
context.setGlobalContextManager(new StackContextManager().enable())
propagation.setGlobalPropagator(new W3CTraceContextPropagator())

const actionTraceId = "4bf92f3577b34da6a3ce929d0e0e4736"
const actionMeta = { traceparent: `00-${actionTraceId}-00f067aa0ba902b7-01` }

function inSpan<T>(fn: () => T): { traceId: string; result: T } {
  const span = trace.getTracer("test").startSpan("action")
  try {
    return {
      traceId: span.spanContext().traceId,
      result: context.with(trace.setSpan(context.active(), span), fn),
    }
  } finally {
    span.end()
  }
}

function traceIdOf(payload: unknown): string | undefined {
  const meta = (payload as { meta?: { traceparent?: string } }).meta
  return meta?.traceparent?.split("-")[1]
}

function activeTraceIds(channel: Channel, event: string): Array<string | undefined> {
  const seen: Array<string | undefined> = []
  channel.on(event, () => seen.push(trace.getActiveSpan()?.spanContext().traceId))
  return seen
}

function activeSpanNames(channel: Channel, event: string): Array<string | undefined> {
  const seen: Array<string | undefined> = []
  channel.on(event, () => seen.push((trace.getActiveSpan() as ReadableSpan | undefined)?.name))
  return seen
}

const traced = (tracing: Partial<TracingOptions> = {}): SocketOptions => ({
  tracing: { carrier: "meta", ...tracing },
})

const durableTopic = (): string => `durable:tracing:${Math.random().toString(36).slice(2)}`

async function joined(options = traced(), name = topic("tracing")): Promise<Channel> {
  const socket = openSocket("/socket", options)
  socket.connect()
  const channel = socket.channel(name, {}, { durable: name.startsWith("durable:") })
  await join(channel)
  return channel
}

async function broadcast(channel: Channel, payload: object): Promise<void> {
  const delivered = nextEvent(channel, "broadcasted")
  await answer(channel.push("broadcast", payload))
  await delivered
}

test("a push carries the trace it was made in, over WebSocket and LongPoll, even sent before the join", async () => {
  for (const transport of [WebSocket, LongPoll]) {
    const socket = openSocket("/socket", { ...traced(), transport })
    socket.connect()
    const channel = socket.channel(topic("tracing"))
    const joining = channel.join()
    const { traceId, result: early } = inSpan(() => channel.push("echo", { early: true }))
    expect(early.status.state).toBe("buffered")
    await answer(joining)

    const { status, response } = await answer(early)
    expect(status).toBe("ok")
    expect(response).toMatchObject({ early: true })
    expect(traceIdOf(response)).toBe(traceId)
  }
})

test("a durable push carries the trace it was made in", async () => {
  const channel = await joined(traced(), durableTopic())
  const { traceId, result: reply } = inSpan(() =>
    channel.push("action", { body: "traced" }, { durable: true }),
  )

  const { response } = await answer(reply)
  expect(response).toMatchObject({ body: "traced" })
  expect(traceIdOf(response)).toBe(traceId)
})

test("a push keeps a carrier it already has, so a retry stays in the trace it was first made in", async () => {
  const channel = await joined()
  const { result: push } = inSpan(() => channel.push("echo", { meta: actionMeta }))

  expect((await answer(push)).response).toEqual({ meta: actionMeta })
})

test("a push outside any trace, with a payload that isn't an object, or on a socket without tracing is sent as given", async () => {
  const channel = await joined()
  expect((await answer(channel.push("echo", { a: 1 }))).response).toEqual({ a: 1 })
  const { result: listed } = inSpan(() => channel.push("echo", [1, 2]))
  expect((await answer(listed)).response).toEqual([1, 2])

  const plain = await joined({})
  const { result: untraced } = inSpan(() => plain.push("echo", { a: 1 }))
  expect((await answer(untraced)).response).toEqual({ a: 1 })
})

test("by default, every incoming message's handlers run in a span continuing the trace in its carrier, or a new trace without one", async () => {
  const channel = await joined()
  const seen = activeTraceIds(channel, "broadcasted")

  await broadcast(channel, { meta: actionMeta })
  await broadcast(channel, { meta: "not a carrier" })

  expect(seen[0]).toBe(actionTraceId)
  expect(seen[1]).toBeDefined()
  expect(seen[1]).not.toBe(actionTraceId)
})

test("a durable message's handlers run in the trace in its carrier", async () => {
  const channel = await joined(traced(), durableTopic())
  const seen = activeTraceIds(channel, "traced")

  const delivered = nextEvent(channel, "traced")
  await answer(channel.push("append_unpushed", { event: "traced", payload: { meta: actionMeta } }))
  await delivered

  expect(seen).toEqual([actionTraceId])
})

test("shouldTraceMessage sees each incoming message and decides whether its handlers get a span", async () => {
  const asked: Array<TracedMessage> = []
  const channel = await joined(
    traced({
      shouldTraceMessage: (message) => {
        asked.push(message)
        return message.parent !== undefined
      },
    }),
  )
  const seen = activeTraceIds(channel, "broadcasted")

  await broadcast(channel, { body: "presence" })
  await broadcast(channel, { meta: actionMeta })

  expect(seen).toEqual([undefined, actionTraceId])
  expect(asked.map(({ topic, event, payload }) => ({ topic, event, payload }))).toEqual([
    { topic: channel.topic, event: "broadcasted", payload: { body: "presence" } },
    { topic: channel.topic, event: "broadcasted", payload: { meta: actionMeta } },
  ])
  expect(asked.map(({ parent }) => parent?.traceId)).toEqual([undefined, actionTraceId])
})

test("a span is named after the topic's pattern, room:* for room:lobby by default, or what topicPattern returns", async () => {
  const wildcard = await joined()
  const wildcardNames = activeSpanNames(wildcard, "broadcasted")
  await broadcast(wildcard, {})
  expect(wildcardNames).toEqual(["process room:*"])

  const custom = await joined(traced({ topicPattern: (name) => name.split(":")[1] ?? name }))
  const customNames = activeSpanNames(custom, "broadcasted")
  await broadcast(custom, {})
  expect(customNames).toEqual(["process tracing"])
})

test("a span is a consumer span with the messaging attributes semantic conventions name", async () => {
  const channel = await joined()
  const spans: Array<ReadableSpan | undefined> = []
  channel.on("broadcasted", () => spans.push(trace.getActiveSpan() as ReadableSpan | undefined))
  await broadcast(channel, {})
  expect(spans.map((span) => [span?.kind, span?.attributes])).toEqual([
    [
      SpanKind.CONSUMER,
      {
        "messaging.system.name": "phoenix",
        "messaging.operation.name": "process",
        "messaging.operation.type": "process",
        "messaging.destination.name": channel.topic,
        "messaging.destination.template": "room:*",
        "phoenix.channel.event": "broadcasted",
      },
    ],
  ])
})

test("the channel's own phx_error and phx_close run outside any span", async () => {
  const channel = await joined()
  const seen: Array<unknown> = []
  channel.onError(() => seen.push(trace.getActiveSpan()))
  channel.onClose(() => seen.push(trace.getActiveSpan()))

  const errored = nextEvent(channel, "phx_error")
  channel.push("crash", {})
  await errored
  const closed = nextEvent(channel, "phx_close")
  await answer(channel.leave())
  await closed

  expect(seen).toEqual([undefined, undefined])
})

test("without tracing, incoming messages get no span", async () => {
  const channel = await joined({})
  const seen = activeTraceIds(channel, "broadcasted")

  await broadcast(channel, { meta: actionMeta })

  expect(seen).toEqual([undefined])
})
