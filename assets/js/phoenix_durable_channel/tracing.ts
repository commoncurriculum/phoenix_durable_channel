import {
  context,
  propagation,
  ROOT_CONTEXT,
  SpanKind,
  trace,
  type Context,
  type SpanContext,
} from "@opentelemetry/api"

export interface TracedMessage {
  readonly topic: string
  readonly event: string
  readonly payload: unknown
  readonly parent: SpanContext | undefined
}

export interface UntracedMessage {
  readonly topic: string
  readonly pattern: string
  readonly event: string
  readonly payload: unknown
}

export interface TracingOptions {
  carrier: string
  shouldTraceMessage?: (message: TracedMessage) => boolean
  topicPattern?: (topic: string) => string
  untracedContext?: (message: UntracedMessage) => Context
}

export interface Tracing {
  inject(payload: unknown): unknown
  process(topic: string, event: string, payload: unknown, handle: () => void): void
}

const untraced: Tracing = {
  inject: (payload) => payload,
  process: (_topic, _event, _payload, handle) => handle(),
}

const isPlainObject = (payload: unknown): payload is Record<string, unknown> =>
  typeof payload === "object" &&
  payload !== null &&
  Object.getPrototypeOf(payload) === Object.prototype

const everyMessage = (): boolean => true

const wildcardTopicPattern = (topic: string): string => topic.replace(/:.*/, ":*")

const activeContext = (): Context => context.active()

export function createTracing(options: TracingOptions | undefined): Tracing {
  if (options === undefined) return untraced
  const {
    carrier,
    shouldTraceMessage = everyMessage,
    topicPattern = wildcardTopicPattern,
    untracedContext = activeContext,
  } = options
  const tracer = trace.getTracer("phoenix_durable_channel")
  return {
    inject(payload) {
      if (!isPlainObject(payload) || payload[carrier] !== undefined) return payload
      const injected: Record<string, string> = {}
      propagation.inject(context.active(), injected)
      return Object.keys(injected).length === 0 ? payload : { ...payload, [carrier]: injected }
    },
    process(topic, event, payload, handle) {
      const carried =
        isPlainObject(payload) && isPlainObject(payload[carrier]) ? payload[carrier] : {}
      const parent = propagation.extract(ROOT_CONTEXT, carried)
      const pattern = topicPattern(topic)
      if (!shouldTraceMessage({ topic, event, payload, parent: trace.getSpanContext(parent) })) {
        context.with(untracedContext({ topic, pattern, event, payload }), handle)
        return
      }
      tracer.startActiveSpan(
        `process ${pattern}`,
        {
          kind: SpanKind.CONSUMER,
          attributes: {
            "messaging.system.name": "phoenix",
            "messaging.operation.name": "process",
            "messaging.operation.type": "process",
            "messaging.destination.name": topic,
            "messaging.destination.template": pattern,
            "phoenix.channel.event": event,
          },
        },
        parent,
        (span) => {
          try {
            handle()
          } finally {
            span.end()
          }
        },
      )
    },
  }
}
