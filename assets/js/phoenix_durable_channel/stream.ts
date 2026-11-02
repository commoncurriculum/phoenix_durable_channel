import { Effect, Stream as EffectStream, Fiber } from "effect"
import type { Actor, EventFromLogic } from "xstate"
import { ignore, startMachine } from "./actors"
import { channelReadMachine } from "./machines/channel-read"
import { readerMachine } from "./machines/reader"
import type { Push } from "./push"
import {
  decodeDurableStreamsControl,
  decodeDurableStreamsData,
  decodeDurableStreamsReader,
  decodeReadReply,
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

export interface Position {
  after: number
  epoch: string
}

export interface ReadCallbacks {
  deliver(message: StreamMessage): void
  moved(epoch: string): void
  failed(error: unknown): void
}

export type Token = () => string | null | Promise<string | null>

export interface PublishedReader {
  readonly type: string
  readonly last: number
  readonly epoch?: string
  readonly [key: string]: unknown
}

export type ReaderFactory = (
  latest: () => PublishedReader,
  refresh: () => Promise<void>,
) => MessageStream

export const builtInReaders: Record<string, ReaderFactory> = {
  s2: (latest, refresh) => {
    const { endpoint, basin, stream } = decodeS2Reader(latest())
    return new S2Stream({
      endpoint,
      basin,
      stream,
      token: () => decodeS2Reader(latest()).token,
      refused: refresh,
    })
  },
  "durable-streams": (latest, refresh) => {
    const { url, last, offset } = decodeDurableStreamsReader(latest())
    return new DurableStream({
      url,
      token: () => decodeDurableStreamsReader(latest()).token,
      refused: refresh,
      resume: { number: last, offset },
    })
  },
}

export interface S2StreamOptions {
  endpoint: string
  basin: string
  stream: string
  token?: Token
  refused?: () => Promise<void>
}

export interface DurableStreamOptions {
  url: string
  token?: Token
  refused?: () => Promise<void>
  resume?: { number: number; offset: string }
}

const unchanged = (error: unknown) => error

function readEvents(
  url: string,
  headers: Record<string, string>,
  token: Token | undefined,
  refused: (() => Promise<void>) | undefined,
  handle: (
    events: EffectStream.Stream<{ event: string; data: string }, unknown>,
  ) => Effect.Effect<void, unknown>,
  failed: (error: unknown) => void,
): () => void {
  const read = Effect.gen(function* () {
    const bearer = token === undefined ? null : yield* Effect.promise(async () => token())
    const authorization: Record<string, string> =
      bearer === null ? {} : { authorization: `Bearer ${bearer}` }
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
      token,
      refused,
      (events) =>
        EffectStream.runForEach(events, (event) => {
          if (event.event !== "batch") return Effect.void
          return Effect.sync(() => {
            for (const record of decodeS2Batch(event.data).records) {
              if (record.headers[0]?.[0] === "") continue
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
  private resume: { number: number; offset: string } | null

  constructor(readonly options: DurableStreamOptions) {
    this.resume = options.resume ?? null
  }

  read(from: Position, callbacks: ReadCallbacks): () => void {
    const offset = this.resume?.number === from.after ? this.resume.offset : "-1"
    const url = `${this.options.url}?offset=${encodeURIComponent(offset)}&live=sse`
    return readEvents(
      url,
      {},
      this.options.token,
      this.options.refused,
      (events) =>
        EffectStream.runFoldEffect(events, from.after, (last, event) =>
          Effect.sync(() => {
            if (event.event === "control") {
              const { streamNextOffset } = decodeDurableStreamsControl(event.data)
              this.resume = { number: last, offset: streamNextOffset }
              return last
            }
            let delivered = last
            for (const message of decodeDurableStreamsData(event.data)) {
              if (message.number <= delivered) continue
              callbacks.deliver(message)
              delivered = message.number
            }
            return delivered
          }),
        ),
      callbacks.failed,
    )
  }
}

export interface ChannelReads {
  read(after: number): Push
  listen(listener: ChannelListener): () => void
}

export interface ChannelListener {
  message(message: StreamMessage): void
  lost(): void
}

export class ChannelStream implements MessageStream {
  constructor(private readonly channel: ChannelReads) {}

  read(from: Position, callbacks: ReadCallbacks): () => void {
    const actor = startMachine(channelReadMachine, { from }, ignore, (event) => {
      switch (event.type) {
        case "deliver":
          return callbacks.deliver(event.message)
        case "moved":
          return callbacks.moved(event.epoch)
        case "failed":
          return callbacks.failed("ended")
      }
    })
    const send = (event: EventFromLogic<typeof channelReadMachine>) => {
      if (actor.getSnapshot().status === "active") actor.send(event)
    }
    const stopListening = this.channel.listen({
      message: (message) => send({ type: "MESSAGE", message }),
      lost: () => send({ type: "LOST" }),
    })
    this.channel
      .read(from.after)
      .receive("ok", (response) => send({ type: "READ_REPLY", ...decodeReadReply(response) }))
      .receive("timeout", () => send({ type: "LOST" }))
    return () => {
      stopListening()
      actor.stop()
    }
  }
}

export class Reader {
  readonly actor: Actor<typeof readerMachine>

  constructor(
    stream: MessageStream,
    from: Position,
    deliver: (message: StreamMessage) => void,
    backoffMs: (tries: number) => number,
  ) {
    this.actor = startMachine(readerMachine, { stream, from, backoffMs }, ignore, (event) =>
      deliver(event.message),
    )
  }

  stop(): void {
    this.actor.send({ type: "STOP" })
  }
}
