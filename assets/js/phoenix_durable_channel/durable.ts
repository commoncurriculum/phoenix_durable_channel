import { Effect } from "effect"
import type { Actor } from "xstate"
import { ignore, startMachine } from "./actors"
import type { ChannelSocket } from "./channel"
import { CHANNEL_EVENTS } from "./constants"
import { Log } from "./log"
import { followMachine } from "./machines/follow"
import type { Push } from "./push"
import { Reply } from "./reply"
import {
  builtInReaders,
  ChannelStream,
  Reader,
  type ChannelListener,
  type MessageStream,
  type PublishedReader,
  type ReaderFactory,
  type StreamMessage,
} from "./stream"
import type { Params } from "./url"
import {
  decodeDurableJoinReply,
  decodePublishedReader,
  decodeSyncReply,
  hasPublishedReader,
} from "./wire"

const unread: MessageStream = { read: () => ignore }

export interface DurableChannel {
  readonly topic: string
  readonly socket: ChannelSocket
  isJoined(): boolean
  joinRef(): string | null
  push(event: string, payload?: unknown, timeout?: number): Push
}

export interface DurableOptions {
  readers?: Record<string, ReaderFactory>
  after?: number
}

export class Durable {
  readonly log: Log
  private readonly follow: Actor<typeof followMachine>
  private readonly listeners = new Set<ChannelListener>()
  private readonly readers: Record<string, ReaderFactory>

  constructor(
    private readonly channel: DurableChannel,
    private readonly options: DurableOptions,
    private readonly deliver: (message: StreamMessage) => void,
  ) {
    const { socket } = channel
    this.readers = {
      phoenix: () =>
        new ChannelStream({
          read: (after) => channel.push(CHANNEL_EVENTS.read, { after }, Infinity),
          listen: (listener) => {
            this.listeners.add(listener)
            return () => this.listeners.delete(listener)
          },
        }),
      ...builtInReaders,
      ...options.readers,
    }
    this.log = new Log(
      {
        channelJoined: () => channel.isJoined(),
        backoffMs: (tries) => socket.rejoinAfterMs(tries),
        makeRef: () => socket.makeRef(),
      },
      {
        sync: () =>
          channel
            .push(CHANNEL_EVENTS.sync)
            .receive("ok", (response) =>
              this.log.actor.send({ type: "SYNCED", ...decodeSyncReply(response) }),
            )
            .receive("timeout", () => this.log.actor.send({ type: "SYNC_FAILED" })),
        send: ({ event, payload, ref, number }) =>
          socket.push({
            topic: channel.topic,
            event,
            payload,
            ref,
            join_ref: channel.joinRef(),
            number,
          }),
      },
    )
    this.follow = startMachine(
      followMachine,
      { read: (latest) => this.read(latest) },
      ignore,
      ignore,
    )
  }

  joinParams(params: Params): Params {
    return { ...params, phx_durable: true }
  }

  refuses(response: unknown): boolean {
    return !hasPublishedReader(response)
  }

  joined(response: unknown): void {
    this.follow.send({ type: "PUBLISHED", reader: decodeDurableJoinReply(response).phx_reader })
    this.log.actor.send({ type: "CHANNEL_JOINED" })
  }

  lost(): void {
    this.log.actor.send({ type: "CHANNEL_LEFT" })
    for (const listener of this.listeners) listener.lost()
  }

  closed(): void {
    this.log.actor.send({ type: "CHANNEL_CLOSED" })
    this.follow.send({ type: "STOP" })
  }

  push(event: string, payload: unknown): Reply {
    if (payload instanceof ArrayBuffer)
      throw new Error(`tried to push a durable '${event}' with a binary payload`)
    const reply = new Reply()
    this.log.push(event, payload, reply)
    return reply
  }

  receive(message: StreamMessage): void {
    for (const listener of this.listeners) listener.message(message)
  }

  private read(latest: () => PublishedReader): () => void {
    const published = latest()
    const reader = new Reader(
      this.streamFor(published, latest),
      { after: this.options.after ?? published.last, epoch: published.epoch ?? "" },
      this.deliver,
      (tries) => this.channel.socket.rejoinAfterMs(tries),
    )
    return () => reader.stop()
  }

  private streamFor(published: PublishedReader, latest: () => PublishedReader): MessageStream {
    const factory = this.readers[published.type]
    if (factory !== undefined) return factory(latest, () => this.refresh())
    this.channel.socket.log("channel", `no reader for the store "${published.type}"`, published)
    return unread
  }

  private refresh(): Promise<void> {
    return Effect.runPromise(
      Effect.async<void, string>((resume) => {
        this.channel
          .push(CHANNEL_EVENTS.reader)
          .receive("ok", (response) => {
            this.follow.send({ type: "PUBLISHED", reader: decodePublishedReader(response) })
            resume(Effect.void)
          })
          .receive("timeout", () => resume(Effect.fail("timeout")))
      }),
    )
  }
}
