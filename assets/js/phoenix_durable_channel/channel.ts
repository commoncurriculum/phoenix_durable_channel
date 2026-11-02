import { createReactive } from "@supergrain/kernel"
import type { Actor } from "xstate"
import { backoff, startMachine, tagged } from "./actors"
import { CHANNEL_EVENTS, decodeChannelState, type ChannelState } from "./constants"
import { Durable, type DurableOptions } from "./durable"
import type { Log } from "./log"
import { channelMachine, type ChannelEmitted } from "./machines/channel"
import { Push } from "./push"
import { Reply } from "./reply"
import type { Message } from "./serializer"
import type { Params } from "./url"
import { decodeReply } from "./wire"

export interface ChannelSocket {
  timeout: number
  readonly establishedConnections: number
  isConnected(): boolean
  makeRef(): string
  push(message: Message): void
  rejoinAfterMs(tries: number): number
  remove(channel: Channel): void
  log(kind: string, message: string, data?: unknown): void
}

export interface ChannelOptions extends DurableOptions {
  durable?: boolean
}

export type Trigger = (
  event: string,
  payload: unknown,
  ref?: string | null,
  joinRef?: string | null,
) => void

export interface ChannelExtension {
  readonly log: Log | null
  joinParams(params: Params): Params
  refuses(response: unknown): boolean
  joined(response: unknown): void
  lost(): void
  closed(): void
  push(event: string, payload: unknown): Reply
  receive(message: Message): void
  reply(ref: string | null, status: string, response: unknown): void
}

const plain = (topic: string, trigger: Trigger): ChannelExtension => ({
  log: null,
  joinParams: (params) => params,
  refuses: () => false,
  joined: () => {},
  lost: () => {},
  closed: () => {},
  push: (event) => {
    throw new Error(
      `tried to push a durable '${event}' to '${topic}', which was not opened with { durable: true }`,
    )
  },
  receive: ({ event, payload, ref, join_ref }) => trigger(event, payload, ref, join_ref),
  reply: () => {},
})

type Binding = {
  event: string
  ref: number
  callback: (payload: unknown, ref?: string | null, joinRef?: string | null) => void
}

export class Channel {
  readonly status: { state: ChannelState }
  readonly actor: Actor<typeof channelMachine>
  readonly joinPush = new Reply()
  private readonly extension: ChannelExtension
  private readonly params: () => Params
  private readonly bindings: Array<Binding> = []
  private bindingRef = 0
  private readonly pushes = new Set<Push>()
  private readonly leavePush = new Reply()

  constructor(
    readonly topic: string,
    params: Params | (() => Params) = {},
    readonly socket: ChannelSocket,
    options: ChannelOptions = {},
  ) {
    this.params = typeof params === "function" ? params : () => params
    const trigger: Trigger = (event, payload, ref, joinRef) =>
      this.trigger(event, payload, ref, joinRef)
    this.extension = options.durable ? new Durable(this, options, trigger) : plain(topic, trigger)
    this.status = createReactive<{ state: ChannelState }>({ state: "closed" })
    this.actor = startMachine(
      channelMachine.provide({
        actors: { waitToRejoin: backoff((tries) => socket.rejoinAfterMs(tries)) },
      }),
      {
        timeout: socket.timeout,
        connection: socket.isConnected() ? socket.establishedConnections : null,
      },
      (snapshot) => {
        this.status.state = tagged(snapshot, decodeChannelState)
      },
      (event) => this.handle(event),
    )
  }

  get log(): Log | null {
    return this.extension.log
  }

  get state(): ChannelState {
    return this.status.state
  }

  get timeout(): number {
    return this.actor.getSnapshot().context.timeout
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
    this.actor.send({ type: "JOIN", timeout })
    return this.joinPush
  }

  leave(timeout = this.timeout): Reply {
    if (this.actor.getSnapshot().status === "done") {
      const left = new Reply()
      left.answer("ok", {})
      return left
    }
    this.actor.send({ type: "LEAVE", timeout })
    return this.leavePush
  }

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
    if (typeof timeoutOrOptions === "object") return this.extension.push(event, payload)
    const push = new Push(event, payload, timeoutOrOptions, (finished) =>
      this.pushes.delete(finished),
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

  receiveMessage(message: Message): void {
    switch (message.event) {
      case CHANNEL_EVENTS.reply:
        return this.receiveReply(message.ref, message.payload)
      case CHANNEL_EVENTS.error:
        return this.actor.send({ type: "SERVER_ERROR", reason: message.payload })
      case CHANNEL_EVENTS.close:
        return this.actor.send({ type: "SERVER_CLOSE", reason: message.payload })
      default:
        return this.extension.receive(message)
    }
  }

  socketOpened(connection: number): void {
    this.actor.send({ type: "SOCKET_OPEN", connection })
  }

  socketClosed(connection: number): void {
    this.actor.send({ type: "SOCKET_CLOSED", connection })
  }

  joinEncoded(ref: string | null, connection: number): void {
    this.actor.send({ type: "JOIN_ENCODED", ref, connection })
  }

  replace(): void {
    this.actor.send({ type: "REPLACED" })
  }

  private receiveReply(ref: string | null, payload: unknown): void {
    const { status, response } = decodeReply(payload)
    const { joinRef, leaveRef } = this.actor.getSnapshot().context
    if (ref === joinRef && status === "ok" && this.extension.refuses(response))
      return this.actor.send({ type: "JOIN_REFUSED", ref, response: { reason: "nondurable" } })
    if (ref === joinRef) return this.actor.send({ type: "JOIN_REPLY", ref, status, response })
    if (ref === leaveRef) return this.actor.send({ type: "LEAVE_REPLY", ref })
    for (const push of this.pushes)
      if (push.ref === ref) push.actor.send({ type: "REPLY", status, response })
    this.extension.reply(ref, status, response)
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
      payload: push.payload,
      ref,
      join_ref: this.joinRef(),
    })
  }

  private handle(event: ChannelEmitted): void {
    switch (event.type) {
      case "join": {
        const ref = this.socket.makeRef()
        this.actor.send({ type: "JOIN_SENT", ref })
        return this.socket.push({
          topic: this.topic,
          event: CHANNEL_EVENTS.join,
          payload: this.extension.joinParams(this.params()),
          ref,
          join_ref: ref,
        })
      }
      case "leave": {
        const ref = this.socket.makeRef()
        this.actor.send({ type: "LEAVE_SENT", ref })
        return this.socket.push({
          topic: this.topic,
          event: CHANNEL_EVENTS.leave,
          payload: {},
          ref,
          join_ref: event.joinRef,
        })
      }
      case "answerJoin":
        return this.joinPush.answer(event.status, event.response)
      case "answerLeave":
        return this.leavePush.answer(event.status, {})
      case "joined":
        for (const push of this.pushes) this.send(push)
        return this.extension.joined(event.response)
      case "lost":
        this.extension.lost()
        for (const push of this.pushes)
          if (push.status.state === "sent") push.actor.send({ type: "CONNECTION_LOST" })
        return
      case "errored":
        return this.trigger(CHANNEL_EVENTS.error, event.reason)
      case "closed":
        for (const push of this.pushes) push.actor.send({ type: "CHANNEL_CLOSED" })
        this.extension.closed()
        this.socket.remove(this)
        return this.trigger(CHANNEL_EVENTS.close, event.reason)
    }
  }
}
