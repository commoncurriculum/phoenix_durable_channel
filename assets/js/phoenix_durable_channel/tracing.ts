import { context, propagation, ROOT_CONTEXT, SpanKind, trace } from "@opentelemetry/api"

export interface TracingOptions {
  readonly carrier: string | undefined
  readonly requireParentSpan: boolean
}

const isPlainObject = (payload: unknown): payload is Record<string, unknown> =>
  typeof payload === "object" &&
  payload !== null &&
  Object.getPrototypeOf(payload) === Object.prototype

const traced = (key: string | undefined, event: string): key is string =>
  key !== undefined && !event.startsWith("phx_")

export function injectCarrier(options: TracingOptions, event: string, payload: unknown): unknown {
  const key = options.carrier
  if (!traced(key, event) || !isPlainObject(payload) || payload[key] !== undefined) return payload
  const carrier: Record<string, string> = {}
  propagation.inject(context.active(), carrier)
  return Object.keys(carrier).length === 0 ? payload : { ...payload, [key]: carrier }
}

export function processMessage(
  options: TracingOptions,
  topic: string,
  event: string,
  payload: unknown,
  handle: () => void,
): void {
  const key = options.carrier
  if (!traced(key, event)) return handle()
  const carrier = isPlainObject(payload) && isPlainObject(payload[key]) ? payload[key] : {}
  const parent = propagation.extract(ROOT_CONTEXT, carrier)
  if (options.requireParentSpan && trace.getSpanContext(parent) === undefined) return handle()

  const template = topic.replace(/:.*/, ":*")
  trace.getTracer("phoenix_durable_channel").startActiveSpan(
    `process ${template}`,
    {
      kind: SpanKind.CONSUMER,
      attributes: {
        "messaging.system": "phoenix",
        "messaging.operation.name": "process",
        "messaging.operation.type": "process",
        "messaging.destination.name": topic,
        "messaging.destination.template": template,
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
}
