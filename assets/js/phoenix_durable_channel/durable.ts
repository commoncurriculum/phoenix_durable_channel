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
  type MessageStream,
  type PublishedReader,
  type ReaderFactory,
  type StreamMessage,
} from "./stream"
import type { Params } from "./url"
import { decodePublishedReader, decodeSyncReply, isDurableJoinReply } from "./wire"

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
  private reader: Actor<typeof readerMachine> | null = null
  private readonly replies = new Map<string, Reply>()
  private readonly listeners = new Set<(message: StreamMessage) => void>()
  private readonly readers: Record<string, ReaderFactory>

  constructor(
    private readonly channel: DurableChannel,
    private readonly options: DurableOptions,
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
          case "answer": {
            const reply = this.replies.get(event.id)
            this.replies.delete(event.id)
            return reply?.answer(event.status, event.response)
          }
        }
      },
    )
  }

  joinParams(params: Params): Params {
    return { ...params, phx_durable: true }
  }

  refuses(response: unknown): boolean {
    return !isDurableJoinReply(response)
  }

  joined(): void {
    this.log.actor.send({ type: "CHANNEL_JOINED" })
    this.reader?.send({ type: "CHANNEL_JOINED" })
  }

  lost(): void {
    this.log.actor.send({ type: "CHANNEL_LEFT" })
    this.reader?.send({ type: "CHANNEL_LEFT" })
  }

  closed(): void {
    this.log.actor.send({ type: "CHANNEL_CLOSED" })
    this.reader?.send({ type: "STOP" })
  }

  push(event: string, payload: unknown): Reply {
    if (payload instanceof ArrayBuffer)
      throw new Error(`tried to push a durable '${event}' with a binary payload`)
    const reply = new Reply()
    const id = this.channel.socket.makeRef()
    this.replies.set(id, reply)
    this.log.actor.send({ type: "PUSH", id, event, payload })
    return reply
  }

  receive(message: Message): void {
    const { event, payload, ref, join_ref, number } = message
    if (event === CHANNEL_EVENTS.reader) return this.published(decodePublishedReader(payload))
    if (number === undefined) return this.trigger(event, payload, ref, join_ref)
    for (const listener of this.listeners) listener({ number, event, payload })
  }

  reply(ref: string | null, status: string, response: unknown): void {
    this.log.actor.send({ type: "REPLY", ref, status, response })
  }

  private published(published: PublishedReader): void {
    if (this.reader !== null) return this.reader.send({ type: "PUBLISHED", reader: published })
    const { socket } = this.channel
    this.reader = startMachine(
      readerMachine.provide({
        actors: {
          reading: fromCallback<ReaderEvent, ReadingInput, ReaderEvent>(({ input, sendBack }) =>
            this.streamFor(input.published).read(input.from, {
              deliver: (message) => sendBack({ type: "MESSAGE", message }),
              moved: (epoch) => sendBack({ type: "MOVED", epoch }),
              reached: (resume) => sendBack({ type: "REACHED", resume }),
              failed: () => sendBack({ type: "FAILED" }),
            }),
          ),
          waitToRead: backoff((tries) => socket.rejoinAfterMs(tries)),
        },
      }),
      { published, after: this.options.after ?? null },
      ignore,
      (event) => {
        switch (event.type) {
          case "deliver":
            return this.trigger(event.message.event, event.message.payload)
          case "skipped":
            return socket.log(
              "channel",
              `${this.channel.topic} skipped durable messages ${event.after + 1} to ${event.next - 1}, which the store no longer has`,
            )
        }
      },
    )
  }

  private streamFor(published: PublishedReader): MessageStream {
    const factory = this.readers[published.type]
    if (factory !== undefined)
      return factory(
        () => published,
        () => this.refresh(),
      )
    this.channel.socket.log("channel", `no reader for the store "${published.type}"`, published)
    return unread
  }

  private refresh(): Promise<void> {
    return Effect.runPromise(
      Effect.async<void, string>((resume) => {
        this.channel
          .push(CHANNEL_EVENTS.reader)
          .receive("ok", (response) => {
            this.published(decodePublishedReader(response))
            resume(Effect.void)
          })
          .receive("timeout", () => resume(Effect.fail("timeout")))
      }),
    )
  }
}
