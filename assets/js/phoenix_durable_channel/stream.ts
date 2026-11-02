import { Effect, Stream as EffectStream, Fiber } from "effect"
import {
  decodeDurableStreamsControl,
  decodeDurableStreamsData,
  decodeDurableStreamsReader,
  decodeS2Batch,
  decodeS2Reader,
  decodeStreamBody,
} from "./wire"

export interface StreamMessage {
  number: number
  event: string
  payload: unknown
}

export interface MessageStream {
  read(from: Position, callbacks: ReadCallbacks): () => void
}

export interface Resume {
  number: number
  offset: string
}

export interface Position {
  after: number
  epoch: string
  resume: Resume | null
}

export interface ReadCallbacks {
  deliver(message: StreamMessage): void
  passed(number: number): void
  reached(resume: Resume): void
  failed(error: unknown): void
}

export interface PublishedReader {
  readonly type: string
  readonly last: number
  readonly epoch?: string
  readonly [key: string]: unknown
}

export type ReaderFactory = (
  published: PublishedReader,
  refresh: (signal?: AbortSignal) => Promise<void>,
) => MessageStream

export const builtInReaders: Record<string, ReaderFactory> = {
  s2: (published, refresh) => {
    const { endpoint, basin, stream, token } = decodeS2Reader(published)
    return new S2Stream({ endpoint, basin, stream, token, refused: refresh })
  },
  "durable-streams": (published, refresh) => {
    const { url, token, last, offset } = decodeDurableStreamsReader(published)
    return new DurableStream({ url, token, refused: refresh, resume: { number: last, offset } })
  },
}

export interface S2StreamOptions {
  endpoint: string
  basin: string
  stream: string
  token?: string | null
  refused?: (signal?: AbortSignal) => Promise<void>
}

export interface DurableStreamOptions {
  url: string
  token?: string | null
  refused?: (signal?: AbortSignal) => Promise<void>
  resume?: Resume
}

const unchanged = (error: unknown) => error

function readEvents(
  url: string,
  headers: Record<string, string>,
  token: string | null,
  refused: ((signal?: AbortSignal) => Promise<void>) | undefined,
  handle: (
    events: EffectStream.Stream<{ event: string; data: string }, unknown>,
  ) => Effect.Effect<void, unknown>,
  failed: (error: unknown) => void,
): () => void {
  const read = Effect.gen(function* () {
    const authorization: Record<string, string> =
      token === null ? {} : { authorization: `Bearer ${token}` }
    const response = yield* Effect.tryPromise({
      try: (signal) =>
        fetch(url, {
          headers: {
            accept: "text/event-stream",
            ...headers,
            ...authorization,
          },
          signal,
        }),
      catch: unchanged,
    })
    if ((response.status === 401 || response.status === 403) && refused !== undefined)
      yield* Effect.tryPromise({ try: refused, catch: unchanged })
    if (response.body === null || !response.ok) return yield* Effect.fail(response.status)
    yield* handle(serverSentEvents(response.body))
    return yield* Effect.fail("the read ended")
  })
  const fiber = Effect.runFork(Effect.catchAll(read, (error) => Effect.sync(() => failed(error))))
  return () => {
    Effect.runFork(Fiber.interrupt(fiber))
  }
}

function serverSentEvents(body: ReadableStream<Uint8Array>) {
  return EffectStream.fromReadableStream(() => body, unchanged).pipe(
    EffectStream.decodeText(),
    EffectStream.splitLines,
    EffectStream.mapAccum({ event: "message", data: "" }, (pending, line) => {
      if (line === "") return [{ event: "message", data: "" }, [pending]]
      if (line.startsWith("event:")) return [{ ...pending, event: line.slice(6).trim() }, []]
      if (line.startsWith("data:"))
        return [{ ...pending, data: pending.data + line.slice(5).trim() }, []]
      return [pending, []]
    }),
    EffectStream.flattenIterables,
  )
}

export class S2Stream implements MessageStream {
  constructor(readonly options: S2StreamOptions) {}

  read(from: Position, callbacks: ReadCallbacks): () => void {
    const { endpoint, stream, basin, token, refused } = this.options
    const url = `${endpoint.replace("{basin}", basin)}/v1/streams/${encodeURIComponent(stream)}/records?seq_num=${from.after}`
    return readEvents(
      url,
      { "s2-basin": basin },
      token ?? null,
      refused,
      (events) =>
        EffectStream.runForEach(events, (event) => {
          if (event.event !== "batch") return Effect.void
          return Effect.sync(() => {
            for (const record of decodeS2Batch(event.data).records) {
              if (record.headers[0]?.[0] === "") {
                callbacks.passed(record.seq_num + 1)
                continue
              }
              const { event: name, payload } = decodeStreamBody(record.body)
              callbacks.deliver({
                number: record.seq_num + 1,
                event: name,
                payload,
              })
            }
          })
        }),
      callbacks.failed,
    )
  }
}

export class DurableStream implements MessageStream {
  constructor(readonly options: DurableStreamOptions) {}

  read(from: Position, callbacks: ReadCallbacks): () => void {
    const offset =
      [from.resume, this.options.resume].find((resume) => resume?.number === from.after)?.offset ??
      "-1"
    const url = `${this.options.url}?offset=${encodeURIComponent(offset)}&live=sse`
    return readEvents(
      url,
      {},
      this.options.token ?? null,
      this.options.refused,
      (events) =>
        EffectStream.runFoldEffect(events, from.after, (last, event) =>
          Effect.sync(() => {
            if (event.event === "control") {
              const { streamNextOffset } = decodeDurableStreamsControl(event.data)
              callbacks.reached({ number: last, offset: streamNextOffset })
              return last
            }
            let latest = last
            for (const message of decodeDurableStreamsData(event.data)) {
              callbacks.deliver(message)
              latest = message.number
            }
            return latest
          }),
        ),
      callbacks.failed,
    )
  }
}
