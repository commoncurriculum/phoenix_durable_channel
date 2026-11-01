import { createReactive } from "@supergrain/kernel"
import { Effect } from "effect"
import type { Actor, SnapshotFrom } from "xstate"
import { ignore, startMachine, type Later } from "./actors"
import { CHANNEL_EVENTS, type ChannelState } from "./constants"
import { Log } from "./log"
import { channelMachine, type ChannelHooks } from "./machines/channel"
import { Push } from "./push"
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
import { decodePublishedReader, decodeReply, decodeSyncReply } from "./wire"

// For a store the client has no reader for.
const unread: MessageStream = { read: () => ignore }

/** What a channel needs from its socket. */
export interface ChannelSocket {
  timeout: number
  isConnected(): boolean
  makeRef(): string
  push(message: Message): void
  rejoinAfterMs(tries: number): number
  remove(channel: Channel): void
  log(kind: string, message: string, data?: unknown): void
}

export interface ChannelOptions {
  /**
   * Lets this channel send and receive durable messages. The server channel must
   * `use PhoenixDurableChannel.Channel`.
   */
  durable?: boolean
  /**
   * With `durable`: how to read the server's durable messages from each store a join reply
   * can name, by its `phx_reader.type`. Added to `"phoenix"`, the channel itself, and
   * `builtInReaders` (S2 and Durable Streams).
   */
  readers?: Record<string, ReaderFactory>
  /**
   * With `durable`: the last server message the app already has, to receive what followed.
   * Without it, the channel receives what the server logs after the first join.
   */
  after?: number
}

function channelState(snapshot: SnapshotFrom<typeof channelMachine>): ChannelState {
  if (snapshot.matches({ member: "joining" })) return "joining"
  if (snapshot.matches({ member: "joined" })) return "joined"
  if (snapshot.matches({ member: "errored" })) return "errored"
  if (snapshot.matches({ member: "leaving" })) return "leaving"
  return "closed"
}

type Binding = {
  event: string
  ref: number
  callback: (payload: unknown, ref?: string | null, joinRef?: string | null) => void
}

export class Channel {
  readonly status: { state: ChannelState }
  readonly actor: Actor<typeof channelMachine>
  readonly joinPush = new Reply()
  /** This channel's durable messages to the server, when it was opened with `durable`. */
  readonly log: Log | null
  timeout: number
  private readonly params: () => Params
  private readonly bindings: Array<Binding> = []
  private bindingRef = 0
  private readonly pushes = new Set<Push>()
  private readonly listeners = new Set<ChannelListener>()
  private leavePush = new Reply()

  constructor(
    readonly topic: string,
    params: Params | (() => Params) = {},
    readonly socket: ChannelSocket,
    options: ChannelOptions,
  ) {
    this.params = typeof params === "function" ? params : () => params
    this.timeout = socket.timeout
    this.log = options.durable
      ? new Log({
          channelJoined: () => this.state === "joined",
          backoffMs: (tries) => socket.rejoinAfterMs(tries),
          makeRef: () => socket.makeRef(),
          sync: () =>
            this.push(CHANNEL_EVENTS.sync)
              .receive("ok", (response) =>
                this.log?.actor.send({
                  type: "SYNCED",
                  ...decodeSyncReply(response),
                }),
              )
              // PhoenixDurableChannel.Channel always answers "ok"; no answer means a slow server or a lost connection.
              .receive("timeout", () => this.log?.actor.send({ type: "SYNC_FAILED" })),
          // The log sends only while joined, and sends again after every join.
          send: ({ event, payload, ref, number }) =>
            socket.push({
              topic,
              event,
              payload,
              ref,
              join_ref: this.joinRef(),
              number,
            }),
        })
      : null
    this.status = createReactive<{ state: ChannelState }>({ state: "closed" })
    this.actor = startMachine(
      channelMachine,
      (later) => ({ hooks: this.hooks(options, later) }),
      (snapshot) => {
        this.status.state = channelState(snapshot)
      },
    )
  }

  get state(): ChannelState {
    return this.status.state
  }

  joinRef(): string | null {
    return this.actor.getSnapshot().context.joinRef
  }

  isJoining(): boolean {
    return this.state === "joining"
  }

  isJoined(): boolean {
    return this.state === "joined"
  }

  isErrored(): boolean {
    return this.state === "errored"
  }

  isLeaving(): boolean {
    return this.state === "leaving"
  }

  isClosed(): boolean {
    return this.state === "closed"
  }

  join(timeout = this.timeout): Reply {
    if (this.actor.getSnapshot().status === "done") {
      throw new Error(`tried to join '${this.topic}', which was left or replaced`)
    }
    if (!this.actor.getSnapshot().matches("unjoined")) {
      throw new Error(
        "tried to join multiple times. 'join' can only be called a single time per channel instance",
      )
    }
    this.timeout = timeout
    this.actor.send({ type: "JOIN" })
    return this.joinPush
  }

  leave(timeout = this.timeout): Reply {
    if (this.state === "leaving") return this.leavePush
    const leavePush = new Reply()
    if (this.actor.getSnapshot().status === "done") {
      leavePush.answer("ok", {})
      return leavePush
    }
    this.timeout = timeout
    this.leavePush = leavePush
    this.actor.send({ type: "LEAVE" })
    return leavePush
  }

  /**
   * Fire and forget, as phoenix.js: answered "ok", "error" or "timeout". With
   * `{ durable: true }`: numbered, sent again after every rejoin until the server answers,
   * and handled by the server once and in order; answered "ok" or "error", or "timeout"
   * only when the channel closes first.
   */
  push(event: string, payload?: unknown, timeout?: number): Push
  push(event: string, payload: unknown, options: { durable: true }): Reply
  push(
    event: string,
    payload: unknown = {},
    timeoutOrOptions: number | { durable: true } = this.timeout,
  ): Push | Reply {
    if (this.actor.getSnapshot().matches("unjoined")) {
      throw new Error(
        `tried to push '${event}' to '${this.topic}' before joining. Use channel.join() before pushing events`,
      )
    }
    if (typeof timeoutOrOptions === "object") {
      if (!this.log)
        throw new Error(
          `tried to push a durable '${event}' to '${this.topic}', which was not opened with { durable: true }`,
        )
      // The binary frame has no slot for the number.
      if (payload instanceof ArrayBuffer)
        throw new Error(`tried to push a durable '${event}' with a binary payload`)
      const reply = new Reply()
      this.log.push(event, payload, reply)
      return reply
    }
    const push = new Push(
      event,
      () => payload,
      timeoutOrOptions,
      (finished) => this.pushes.delete(finished),
    )
    this.pushes.add(push)
    if (this.canPush()) this.send(push)
    return push
  }

  on(event: string, callback: Binding["callback"]): number {
    const ref = this.bindingRef++
    this.bindings.push({ event, ref, callback })
    return ref
  }

  off(event: string, ref?: number): void {
    for (const binding of this.bindings.filter(
      (binding) => binding.event === event && (ref === undefined || binding.ref === ref),
    )) {
      this.bindings.splice(this.bindings.indexOf(binding), 1)
    }
  }

  onClose(callback: (reason: unknown) => void): number {
    return this.on(CHANNEL_EVENTS.close, callback)
  }

  onError(callback: (reason: unknown) => void): number {
    return this.on(CHANNEL_EVENTS.error, callback)
  }

  /** Overridable: transforms a payload before the bindings see it. */
  onMessage(
    _event: string,
    payload: unknown,
    _ref?: string | null,
    _joinRef?: string | null,
  ): unknown {
    return payload
  }

  canPush(): boolean {
    return this.socket.isConnected() && this.state === "joined"
  }

  isMember(message: Message): boolean {
    if (this.topic !== message.topic) return false
    if (message.join_ref !== null && message.join_ref !== this.joinRef()) {
      this.socket.log("channel", "dropping outdated message", message)
      return false
    }
    return true
  }

  /** Called by the socket for each message on this channel's topic and join. */
  receiveMessage(message: Message): void {
    const { event, payload, ref, join_ref, number } = message
    switch (event) {
      case CHANNEL_EVENTS.reply:
        this.receiveReply(ref, payload)
        break
      case CHANNEL_EVENTS.error:
        this.actor.send({ type: "SERVER_ERROR", reason: payload })
        break
      case CHANNEL_EVENTS.close:
        this.actor.send({ type: "SERVER_CLOSE", reason: payload })
        break
      default:
        // A durable message goes to the read in progress, which delivers it in order.
        if (this.log && number !== undefined)
          for (const listener of this.listeners) listener.message({ number, event, payload })
        else this.trigger(event, payload, ref, join_ref)
    }
  }

  /** Called by the socket when its connection opens or closes. */
  socketOpened(): void {
    this.actor.send({ type: "SOCKET_OPEN" })
  }

  socketClosed(): void {
    this.actor.send({ type: "SOCKET_CLOSED" })
  }

  replace(): void {
    this.actor.send({ type: "REPLACED" })
  }

  private receiveReply(ref: string | null, payload: unknown): void {
    const { status, response } = decodeReply(payload)
    const snapshot = this.actor.getSnapshot()
    if (
      ref === snapshot.context.joinRef &&
      snapshot.matches({ member: { joining: "awaitingReply" } })
    ) {
      return this.actor.send({ type: "JOIN_REPLY", status, response })
    }
    if (ref === snapshot.context.leaveRef) return this.actor.send({ type: "LEAVE_REPLY" })
    for (const push of this.pushes)
      if (push.ref === ref) push.actor.send({ type: "REPLY", status, response })
    this.log?.actor.send({ type: "REPLY", ref, status, response })
  }

  private trigger(
    event: string,
    payload: unknown,
    ref: string | null = null,
    joinRef: string | null = null,
  ): void {
    const handled = this.onMessage(event, payload, ref, joinRef)
    for (const binding of [...this.bindings])
      if (binding.event === event) binding.callback(handled, ref, joinRef ?? this.joinRef())
  }

  private send(push: Push): void {
    const ref = this.socket.makeRef()
    push.actor.send({ type: "SEND", ref })
    this.socket.push({
      topic: this.topic,
      event: push.event,
      payload: push.payload(),
      ref,
      join_ref: this.joinRef(),
    })
  }

  private hooks(options: ChannelOptions, later: Later): ChannelHooks {
    const channelStream = () =>
      new ChannelStream({
        read: (after) => this.push(CHANNEL_EVENTS.read, { after }, Infinity),
        listen: (listener) => {
          this.listeners.add(listener)
          return () => this.listeners.delete(listener)
        },
      })
    const readers: Record<string, ReaderFactory> = {
      phoenix: channelStream,
      ...builtInReaders,
      ...options.readers,
    }
    return {
      timeout: () => this.timeout,
      socketIsOpen: () => this.socket.isConnected(),
      makeRef: () => this.socket.makeRef(),
      rejoinAfterMs: (tries) => this.socket.rejoinAfterMs(tries),
      sendJoin: (joinRef) =>
        later(() =>
          this.socket.push({
            topic: this.topic,
            event: CHANNEL_EVENTS.join,
            // Tells PhoenixDurableChannel.Channel to send this client its durable messages.
            payload: options.durable ? { ...this.params(), phx_durable: true } : this.params(),
            ref: joinRef,
            join_ref: joinRef,
          }),
        ),
      sendLeave: (joinRef, leaveRef) =>
        later(() =>
          this.socket.push({
            topic: this.topic,
            event: CHANNEL_EVENTS.leave,
            payload: {},
            ref: leaveRef,
            join_ref: joinRef,
          }),
        ),
      answerJoin: (status, response) => later(() => this.joinPush.answer(status, response)),
      answerLeave: (status) => later(() => this.leavePush.answer(status, {})),
      joined: () =>
        later(() => {
          // Every push still here is buffered: leaving `joined` timed out the sent ones.
          for (const push of this.pushes) this.send(push)
          this.log?.actor.send({ type: "CHANNEL_JOINED" })
        }),
      durable: options.durable === true,
      streamFor: (reader, latest, refresh) => {
        const factory = readers[reader.type]
        if (factory !== undefined) return factory(latest, refresh)
        this.socket.log("channel", `no reader for the store "${reader.type}"`, reader)
        return unread
      },
      fetchReader: () =>
        Effect.runPromise(
          Effect.async<PublishedReader, string>((resume) => {
            this.push(CHANNEL_EVENTS.reader)
              .receive("ok", (response) => resume(Effect.succeed(decodePublishedReader(response))))
              // PhoenixDurableChannel.Channel always answers "ok".
              .receive("timeout", () => resume(Effect.fail("timeout")))
          }),
        ),
      after: options.after ?? null,
      // The reader defers its deliveries itself; this channel's effects run only when its own state changes.
      deliver: (message) => this.trigger(message.event, message.payload),
      connectionLost: () =>
        later(() => {
          // First, so the log and the read are done with this connection before its pushes time out.
          this.log?.actor.send({ type: "CHANNEL_LEFT" })
          for (const listener of this.listeners) listener.lost()
          for (const push of this.pushes)
            if (push.status.state === "sent") push.actor.send({ type: "CONNECTION_LOST" })
        }),
      errored: (reason) => later(() => this.trigger(CHANNEL_EVENTS.error, reason)),
      closed: (reason) =>
        later(() => {
          for (const push of this.pushes) push.actor.send({ type: "CHANNEL_CLOSED" })
          this.log?.actor.send({ type: "CHANNEL_CLOSED" })
          this.socket.remove(this)
          this.trigger(CHANNEL_EVENTS.close, reason)
        }),
    }
  }
}
