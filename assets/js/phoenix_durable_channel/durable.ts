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
  type MessageStream,
  type PublishedReader,
  type ReaderFactory,
} from "./stream"
import type { Params } from "./url"
import {
  decodeDurableReply,
  decodePublishedReader,
  decodeReadReply,
  decodeSyncReply,
  isDurableJoinReply,
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
  private readonly replies = new Map<string, Reply>()
  private readonly readers: Record<string, ReaderFactory>

  constructor(
    private readonly channel: DurableChannel,
    options: DurableOptions,
    private readonly trigger: Trigger,
  ) {
    const { socket } = channel
    const { after } = options
    if (after !== undefined && !(Number.isInteger(after) && after >= 0))
      throw new Error(
        `tried to read '${channel.topic}' after ${after}, which is not a whole number of 0 or more`,
      )
    this.readers = { ...builtInReaders, ...options.readers }
    this.log = new Log(
      (tries) => socket.rejoinAfterMs(tries),
      (event) => {
        switch (event.type) {
          case "sync":
            return channel
              .push(CHANNEL_EVENTS.sync)
              .receive("ok", (response) => {
                const { published, from } = this.reader.getSnapshot().context
                this.log.actor.send({
                  type: "SYNCED",
                  ...decodeSyncReply(response),
                  last: published.last,
                })
                this.log.actor.send({ type: "READ", after: from.after })
              })
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
    this.reader = startMachine(
      readerMachine.provide({
        actors: {
          reading: fromCallback<ReaderEvent, ReadingInput, ReaderEvent>(({ input, sendBack }) =>
            this.streamFor(input.published).read(input.from, {
              deliver: (message) => sendBack({ type: "MESSAGE", message }),
              passed: (number) => sendBack({ type: "PASSED", number }),
              reached: (resume) => sendBack({ type: "REACHED", resume }),
              failed: () => sendBack({ type: "FAILED" }),
            }),
          ),
          waitToRead: backoff((tries) => socket.rejoinAfterMs(tries)),
        },
      }),
      { after: after ?? null },
      ignore,
      (event) => {
        switch (event.type) {
          case "deliver":
            if (event.message.event === CHANNEL_EVENTS.reply)
              return this.log.actor.send({
                type: "ANSWERED",
                ...decodeDurableReply(event.message.payload),
              })
            return trigger(event.message.event, event.message.payload)
          case "readTo":
            return this.log.actor.send({ type: "READ", after: event.after })
          case "skipped":
            socket.log(
              "channel",
              `${channel.topic} skipped durable messages ${event.after + 1} to ${event.next - 1}, which the store no longer has`,
            )
            return trigger(CHANNEL_EVENTS.skipped, { after: event.after, next: event.next })
          case "read":
            return channel
              .push(CHANNEL_EVENTS.read, { after: event.after }, Infinity)
              .receive("ok", (response) =>
                this.reader.send({
                  type: "READ_REPLY",
                  read: event.read,
                  ...decodeReadReply(response),
                }),
              )
              .receive("error", (response) => {
                socket.log(
                  "channel",
                  `${channel.topic} could not read after ${event.after}`,
                  response,
                )
                this.reader.send({ type: "READ_REFUSED", read: event.read })
              })
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
    this.reader.send({ type: "CHANNEL_JOINED" })
  }

  lost(): void {
    this.log.actor.send({ type: "CHANNEL_LEFT" })
    this.reader.send({ type: "CHANNEL_LEFT" })
  }

  closed(): void {
    this.log.actor.send({ type: "CHANNEL_CLOSED" })
    this.reader.send({ type: "STOP" })
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
    const { event, payload, ref, join_ref, number, epoch } = message
    if (event === CHANNEL_EVENTS.reader)
      return this.reader.send({ type: "PUBLISHED", reader: decodePublishedReader(payload) })
    if (number === undefined) return this.trigger(event, payload, ref, join_ref)
    this.reader.send({ type: "MESSAGE", message: { number, event, payload }, epoch })
  }

  reply(ref: string | null, status: string, response: unknown): void {
    this.log.actor.send({ type: "REPLY", ref, status, response })
  }

  private streamFor(published: PublishedReader): MessageStream {
    const factory = this.readers[published.type]
    if (factory !== undefined) return factory(published, (signal) => this.refresh(signal))
    this.channel.socket.log("channel", `no reader for the store "${published.type}"`, published)
    return unread
  }

  private refresh(signal?: AbortSignal): Promise<void> {
    return Effect.runPromise(
      Effect.async<unknown, unknown>((resume) => {
        this.channel
          .push(CHANNEL_EVENTS.reader)
          .receive("ok", (response) => resume(Effect.succeed(response)))
          .receive("error", (response) => resume(Effect.fail(response)))
          .receive("timeout", () => resume(Effect.fail("timeout")))
      }).pipe(
        Effect.tapError((reason) =>
          Effect.sync(() =>
            this.channel.socket.log(
              "channel",
              `${this.channel.topic} could not get a fresh reader`,
              reason,
            ),
          ),
        ),
        Effect.map((response) =>
          this.reader.send({ type: "PUBLISHED", reader: decodePublishedReader(response) }),
        ),
      ),
      { signal },
    )
  }
}
