import { Effect } from "effect"
import { fromCallback, type Actor } from "xstate"
import { backoff, ignore, startMachine } from "./actors"
import type { ChannelExtension, ChannelSocket, Trigger } from "./channel"
import { CHANNEL_EVENTS } from "./constants"
import { Log } from "./log"
import { readerMachine, type ReaderEvent, type ReadingInput } from "./machines/reader"
import type { Push } from "./push"
import { Reply } from "./reply"
import type { Message } from "./serializer"
import {
  builtInReaders,
  ChannelStream,
  type ChannelListener,
  type MessageStream,
  type PublishedReader,
  type ReaderFactory,
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
  joinRef(): string | null
  push(event: string, payload?: unknown, timeout?: number): Push
}

export interface DurableOptions {
  readers?: Record<string, ReaderFactory>
  after?: number
}

export class Durable implements ChannelExtension {
  readonly log: Log
  private readonly reader: Actor<typeof readerMachine>
  private readonly listeners = new Set<ChannelListener>()
  private readonly readers: Record<string, ReaderFactory>

  constructor(
    private readonly channel: DurableChannel,
    options: DurableOptions,
    private readonly trigger: Trigger,
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
      (tries) => socket.rejoinAfterMs(tries),
      (event) => {
        switch (event.type) {
          case "sync":
            return channel
              .push(CHANNEL_EVENTS.sync)
              .receive("ok", (response) =>
                this.log.actor.send({ type: "SYNCED", ...decodeSyncReply(response) }),
              )
              .receive("timeout", () => this.log.actor.send({ type: "SYNC_FAILED" }))
          case "send":
            return socket.push({
              topic: channel.topic,
              event: event.event,
              payload: event.payload,
              ref: event.sent.ref,
              join_ref: channel.joinRef(),
              number: event.sent.number,
            })
          case "answer":
            return event.reply.answer(event.status, event.response)
        }
      },
    )
    this.reader = startMachine(
      readerMachine.provide({
        actors: {
          reading: fromCallback<ReaderEvent, ReadingInput, ReaderEvent>(({ input, sendBack }) =>
            this.streamFor(input.published).read(input.from, {
              deliver: (message) => sendBack({ type: "MESSAGE", message }),
              moved: (epoch) => sendBack({ type: "MOVED", epoch }),
              failed: () => sendBack({ type: "FAILED" }),
            }),
          ),
          waitToRead: backoff((tries) => socket.rejoinAfterMs(tries)),
        },
      }),
      { after: options.after ?? null },
      ignore,
      ({ message }) => trigger(message.event, message.payload),
    )
  }

  joinParams(params: Params): Params {
    return { ...params, phx_durable: true }
  }

  refuses(response: unknown): boolean {
    return !hasPublishedReader(response)
  }

  joined(response: unknown): void {
    this.reader.send({ type: "PUBLISHED", reader: decodeDurableJoinReply(response).phx_reader })
    this.log.actor.send({ type: "CHANNEL_JOINED" })
  }

  lost(): void {
    this.log.actor.send({ type: "CHANNEL_LEFT" })
    for (const listener of this.listeners) listener.lost()
  }

  closed(): void {
    this.log.actor.send({ type: "CHANNEL_CLOSED" })
    this.reader.send({ type: "STOP" })
  }

  push(event: string, payload: unknown): Reply {
    if (payload instanceof ArrayBuffer)
      throw new Error(`tried to push a durable '${event}' with a binary payload`)
    const reply = new Reply()
    this.log.actor.send({ type: "PUSH", event, payload, reply })
    return reply
  }

  receive(message: Message): void {
    const { event, payload, ref, join_ref, number } = message
    if (number === undefined) return this.trigger(event, payload, ref, join_ref)
    for (const listener of this.listeners) listener.message({ number, event, payload })
  }

  reply(ref: string | null, status: string, response: unknown): void {
    this.log.actor.send({ type: "REPLY", ref, status, response })
  }

  private streamFor(published: PublishedReader): MessageStream {
    const factory = this.readers[published.type]
    if (factory !== undefined)
      return factory(
        () => this.latest(),
        () => this.refresh(),
      )
    this.channel.socket.log("channel", `no reader for the store "${published.type}"`, published)
    return unread
  }

  private latest(): PublishedReader {
    return this.reader.getSnapshot().context.published!
  }

  private refresh(): Promise<void> {
    return Effect.runPromise(
      Effect.async<void, string>((resume) => {
        this.channel
          .push(CHANNEL_EVENTS.reader)
          .receive("ok", (response) => {
            this.reader.send({ type: "PUBLISHED", reader: decodePublishedReader(response) })
            resume(Effect.void)
          })
          .receive("timeout", () => resume(Effect.fail("timeout")))
      }),
    )
  }
}
