import { Effect, Stream as EffectStream, Fiber } from "effect"
import type { Actor, EventFromLogic } from "xstate"
import { startMachine } from "./actors"
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

/** A durable message from the server, numbered from 1 in the order the server sent it. */
export interface StreamMessage {
  number: number
  event: string
  payload: unknown
}

/**
 * Where a channel reads the server's durable messages, instead of from Phoenix. The server
 * writes them there; the client reads them with that system's own protocol, so they never
 * pass through the Phoenix server.
 */
export interface MessageStream {
  /**
   * Calls `deliver` with each message numbered after `from.after`, in order. Calls `failed`
   * once if the read ends. The returned function stops the read.
   */
  read(from: Position, callbacks: ReadCallbacks): () => void
}

/**
 * Where a read starts: after message `after` of `epoch`, the store's numbering. A store
 * that never numbers afresh has one epoch, `""`.
 */
export interface Position {
  after: number
  epoch: string
}

export interface ReadCallbacks {
  deliver(message: StreamMessage): void
  /** The store numbers its messages afresh, as `epoch`, so the next read reads that one. */
  moved(epoch: string): void
  failed(error: unknown): void
}

/**
 * A read token, or null to read without one. Called for every read, so it may be
 * refreshed.
 */
export type Token = () => string | null | Promise<string | null>

/**
 * What a join reply's `phx_reader` says about where to read the channel's durable
 * messages: `type` names the store, `last` is the number of its last message at the
 * join, `epoch` its numbering when it can number afresh, and the rest is that store's
 * address and token.
 */
export interface PublishedReader {
  readonly type: string
  readonly last: number
  readonly epoch?: string
  readonly [key: string]: unknown
}

/**
 * Makes the stream for one `type` of `PublishedReader`. `latest` is what the server
 * published last, so a read after a rejoin uses the newest token. `refresh` asks the
 * server for a fresh one, for a store that refused the token it has.
 */
export type ReaderFactory = (
  latest: () => PublishedReader,
  refresh: () => Promise<void>,
) => MessageStream

/** The stores the client reads without being told how, besides `"phoenix"`, the channel itself. */
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
  /** Such as `https://{basin}.b.aws.s2.dev`, or S2 Lite's `http://localhost:4243`. */
  endpoint: string
  basin: string
  stream: string
  /** A token that can read this stream; S2 Lite reads without one. */
  token?: Token
  /** Called when S2 refuses the token (it expired), before the next read asks for one. */
  refused?: () => Promise<void>
}

export interface DurableStreamOptions {
  /** The stream's URL, such as `https://streams.example.workers.dev/v1/stream/project/name`. */
  url: string
  /** A read-scoped JWT for this stream. */
  token?: Token
  /** Called when the server refuses the token (it expired), before the next read asks for one. */
  refused?: () => Promise<void>
  /** Where message `number` ends, so a read after it starts there. */
  resume?: { number: number; offset: string }
}

const unchanged = (error: unknown) => error

/**
 * Fetches `url` as server-sent events and runs `handle` on each, until the read ends or the
 * returned function stops it. `failed` is called once when it ends.
 */
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

/** Server-sent events from a response body: each event's name and data. */
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

/**
 * An S2 stream, read with S2's server-sent events. Each record's body is the JSON
 * `{ event, payload }`; record `seq_num` n is message number n + 1.
 */
export class S2Stream implements MessageStream {
  constructor(readonly options: S2StreamOptions) {}

  read(from: Position, callbacks: ReadCallbacks): () => void {
    const { endpoint, stream, basin, token, refused } = this.options
    const url = `${endpoint.replace("{basin}", basin)}/v1/streams/${encodeURIComponent(stream)}/records?seq_num=${from.after}`
    // S2 Lite reads the basin from this header; hosted S2 from the endpoint. A read ends
    // after S2 sends an error event, so the error needs no handling of its own.
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
              // A command record, such as the trim that deleting the stream writes.
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

/**
 * A JSON stream on a Durable Streams server, read with the protocol's `live=sse`. Offsets
 * are opaque, so each message carries its number: `{ number, event, payload }`. The stream
 * keeps where the last message it delivered ends, so the read after it starts there; any
 * other read starts at the beginning and skips what came before.
 */
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
            // The protocol's only other event is `data`.
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

/** What a read through the channel needs from it. */
export interface ChannelReads {
  /**
   * Sends `phx_read`, with no timeout: the server sends again what followed `after`, then
   * replies with `{ sent, epoch }`, the last number it sent and its store's numbering.
   */
  read(after: number): Push
  /** Gives `listener` the channel's durable messages, and its losing the server, until the returned function is called. */
  listen(listener: ChannelListener): () => void
}

export interface ChannelListener {
  message(message: StreamMessage): void
  lost(): void
}

/** The store `"phoenix"` names: the server keeps the messages, and sends them on the channel. */
export class ChannelStream implements MessageStream {
  constructor(private readonly channel: ChannelReads) {}

  read(from: Position, callbacks: ReadCallbacks): () => void {
    const actor = startMachine(channelReadMachine, (later) => ({
      from,
      callbacks: {
        deliver: (message) => later(() => callbacks.deliver(message)),
        moved: (epoch) => later(() => callbacks.moved(epoch)),
        failed: (error) => later(() => callbacks.failed(error)),
      },
    }))
    // The read's reply can come after the read was stopped.
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
      // PhoenixDurableChannel.Channel always answers "ok", and the read has no timer: "timeout" is a
      // lost connection or a closed channel.
      .receive("timeout", () => send({ type: "LOST" }))
    return () => {
      stopListening()
      actor.stop()
    }
  }
}

/** Reads a channel's durable messages from a stream; see `readerMachine`. */
export class Reader {
  readonly actor: Actor<typeof readerMachine>

  constructor(
    stream: MessageStream,
    from: Position,
    deliver: (message: StreamMessage) => void,
    backoffMs: (tries: number) => number,
  ) {
    this.actor = startMachine(readerMachine, (later) => ({
      stream,
      from,
      deliver: (message) => later(() => deliver(message)),
      backoffMs,
    }))
  }

  stop(): void {
    this.actor.send({ type: "STOP" })
  }
}
