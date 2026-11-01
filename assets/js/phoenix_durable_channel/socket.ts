import type { ActivityTracker } from "@supergrain/activity"
import { createReactive } from "@supergrain/kernel"
import type { Actor, SnapshotFrom } from "xstate"
import { startMachine, type CloseDetails, type Later, type TransportConstructor } from "./actors"
import { Channel, type ChannelOptions } from "./channel"
import { DEFAULT_TIMEOUT, DEFAULT_VSN, TRANSPORTS } from "./constants"
import { LongPoll } from "./longpoll"
import { socketMachine, type SocketHooks, type TransportKind } from "./machines/socket"
import { Serializer, type EncodedMessage, type Message } from "./serializer"
import { appendParams, type Params } from "./url"

export type ConnectionState = "connecting" | "open" | "closing" | "closed"

function connectionState(snapshot: SnapshotFrom<typeof socketMachine>): ConnectionState {
  if (snapshot.matches({ connection: { engaged: { live: "connecting" } } })) return "connecting"
  if (snapshot.matches({ connection: { engaged: { live: "open" } } })) return "open"
  if (snapshot.matches({ connection: { engaged: { live: "closing" } } })) return "closing"
  return "closed"
}

export interface SocketOptions {
  transport?: TransportConstructor
  longPollFallbackMs?: number
  timeout?: number
  heartbeatIntervalMs?: number
  heartbeatTimeoutMs?: number
  reconnectAfterMs?: (tries: number) => number
  rejoinAfterMs?: (tries: number) => number
  logger?: (kind: string, message: string, data?: unknown) => void
  longpollerTimeout?: number
  params?: Params | (() => Params)
  vsn?: string
  websocketRetryAfterMs?: number
  offlineAfterMs?: number
  activity?: ActivityTracker
  disconnectAfterInactiveMs?: number
  encode?: (message: Message, callback: (encoded: EncodedMessage) => void) => void
  decode?: (raw: EncodedMessage, callback: (message: Message) => void) => void
}

type ErrorCallback = (
  error: unknown,
  transportBefore: TransportConstructor,
  establishedBefore: number,
) => void

type StateChangeCallback =
  | { kind: "open"; callback: () => void }
  | { kind: "close"; callback: (event: CloseDetails) => void }
  | { kind: "error"; callback: ErrorCallback }
  | { kind: "message"; callback: (message: Message) => void }

const jitter = (ms: number) => ms + Math.floor(ms * 0.2 * Math.random())

export class Socket {
  readonly status: {
    connectionState: ConnectionState
    transport: TransportKind
    online: boolean
  }
  readonly actor: Actor<typeof socketMachine>
  channels: Array<Channel> = []
  readonly endPoint: string
  readonly timeout: number
  readonly vsn: string
  readonly longpollerTimeout: number
  readonly reconnectAfterMs: (tries: number) => number
  readonly rejoinAfterMs: (tries: number) => number
  private params: () => Params
  private ref = 0
  private readonly logger: SocketOptions["logger"]
  private webSocket: TransportConstructor
  private readonly encode: NonNullable<SocketOptions["encode"]>
  private readonly decode: NonNullable<SocketOptions["decode"]>
  private stateChangeCallbacks: Array<StateChangeCallback & { ref: string }> = []
  private disconnectCallbacks: Array<() => void> = []

  constructor(endPoint: string, options: SocketOptions = {}) {
    this.endPoint = `${endPoint}/${TRANSPORTS.websocket}`
    this.timeout = options.timeout ?? DEFAULT_TIMEOUT
    this.vsn = options.vsn ?? DEFAULT_VSN
    this.longpollerTimeout = options.longpollerTimeout ?? 20_000
    this.logger = options.logger
    this.encode = options.encode ?? ((message, callback) => Serializer.encode(message, callback))
    this.decode = options.decode ?? ((raw, callback) => Serializer.decode(raw, callback))
    const params = options.params ?? {}
    this.params = typeof params === "function" ? params : () => params
    this.reconnectAfterMs =
      options.reconnectAfterMs ?? ((tries) => jitter(Math.min(10 * 2 ** (tries - 1), 5000)))
    this.rejoinAfterMs =
      options.rejoinAfterMs ?? ((tries) => jitter(Math.min(1000 * 2 ** (tries - 1), 10_000)))
    this.webSocket = options.transport ?? WebSocket
    const kind: TransportKind = this.webSocket === LongPoll ? "longpoll" : "websocket"

    this.status = createReactive<Socket["status"]>({
      connectionState: "closed",
      transport: kind,
      online: true,
    })
    this.actor = startMachine(
      socketMachine,
      (later) => ({
        hooks: this.hooks(options.activity ?? null, later),
        transport: kind,
        settings: {
          longPollFallbackMs: options.longPollFallbackMs ?? null,
          heartbeatIntervalMs: options.heartbeatIntervalMs ?? 30_000,
          heartbeatTimeoutMs: options.heartbeatTimeoutMs ?? 10_000,
          websocketRetryAfterMs: options.websocketRetryAfterMs ?? 10 * 60_000,
          offlineAfterMs: options.offlineAfterMs ?? 25_000,
          disconnectAfterInactiveMs: options.disconnectAfterInactiveMs ?? null,
          closeTimeoutMs: 1500,
        },
      }),
      (snapshot) => {
        this.status.connectionState = connectionState(snapshot)
        this.status.transport = snapshot.context.transport
        this.status.online = !snapshot.matches({ online: "offline" })
      },
    )
  }

  get transport(): TransportConstructor {
    return this.status.transport === "longpoll" ? LongPoll : this.webSocket
  }

  get closeWasClean(): boolean {
    return this.actor.getSnapshot().context.closeWasClean
  }

  get establishedConnections(): number {
    return this.actor.getSnapshot().context.establishedConnections
  }

  connect(params?: Params): void {
    if (params) {
      this.log(
        "push",
        "passing params to connect is deprecated. Instead pass :params to the Socket constructor",
      )
      this.params = () => params
    }
    this.actor.send({ type: "CONNECT" })
  }

  disconnect(callback?: () => void, code = 1000, reason = ""): void {
    if (callback) this.disconnectCallbacks.push(callback)
    this.actor.send({ type: "DISCONNECT", code, reason })
  }

  replaceTransport(transport: TransportConstructor): void {
    this.webSocket = transport
    this.actor.send({
      type: "REPLACE_TRANSPORT",
      transport: transport === LongPoll ? "longpoll" : "websocket",
    })
  }

  ping(callback: (latency: number) => void): boolean {
    if (!this.isConnected()) return false
    const ref = this.makeRef()
    const start = Date.now()
    const listener = this.onMessage((message) => {
      if (message.ref !== ref) return
      this.off([listener])
      callback(Date.now() - start)
    })
    this.push({
      topic: "phoenix",
      event: "heartbeat",
      payload: {},
      ref,
      join_ref: null,
    })
    return true
  }

  channel(
    topic: string,
    params: Params | (() => Params) = {},
    options: ChannelOptions = {},
  ): Channel {
    const channel = new Channel(topic, params, this, options)
    for (const older of this.channels.filter((existing) => existing.topic === topic))
      older.replace()
    this.channels.push(channel)
    return channel
  }

  remove(channel: Channel): void {
    this.channels = this.channels.filter((existing) => existing !== channel)
  }

  onOpen(callback: () => void): string {
    return this.addCallback({ kind: "open", callback })
  }

  onClose(callback: (event: CloseDetails) => void): string {
    return this.addCallback({ kind: "close", callback })
  }

  onError(callback: ErrorCallback): string {
    return this.addCallback({ kind: "error", callback })
  }

  onMessage(callback: (message: Message) => void): string {
    return this.addCallback({ kind: "message", callback })
  }

  off(refs: Array<string | null>): void {
    this.stateChangeCallbacks = this.stateChangeCallbacks.filter(({ ref }) => !refs.includes(ref))
  }

  connectionState(): ConnectionState {
    return this.status.connectionState
  }

  isConnected(): boolean {
    return this.connectionState() === "open"
  }

  makeRef(): string {
    this.ref = this.ref + 1
    return `${this.ref}`
  }

  push(message: Message): void {
    this.log(
      "push",
      `${message.topic} ${message.event} (${message.join_ref}, ${message.ref})`,
      message.payload,
    )
    this.actor.send({ type: "PUSH", message })
  }

  endPointURL(): string {
    const url = new URL(
      appendParams(appendParams(this.endPoint, this.params()), {
        vsn: this.vsn,
      }),
      location.href,
    )
    url.protocol = url.protocol.replace("http", "ws")
    return url.toString()
  }

  protocol(): string {
    return location.protocol.replace("http", "ws").replace(":", "")
  }

  hasLogger(): boolean {
    return this.logger !== undefined
  }

  log(kind: string, message: string, data?: unknown): void {
    this.logger?.(kind, message, data)
  }

  private addCallback(callback: StateChangeCallback): string {
    const ref = this.makeRef()
    this.stateChangeCallbacks.push({ ...callback, ref })
    return ref
  }

  private hooks(activity: ActivityTracker | null, later: Later): SocketHooks {
    return {
      url: () => this.endPointURL(),
      transportFor: (kind) => (kind === "longpoll" ? LongPoll : this.webSocket),
      longpollerTimeout: this.longpollerTimeout,
      activity,
      makeRef: () => this.makeRef(),
      reconnectAfterMs: (tries) => this.reconnectAfterMs(tries),
      send: (message) =>
        later(() =>
          this.encode(message, (data) =>
            this.actor.getSnapshot().children.transport?.send({ type: "SEND", data }),
          ),
        ),
      buffered: (message) => this.log("push", "buffered until the socket opens", message),
      opened: () =>
        later(() => {
          this.log("transport", `connected to ${this.endPointURL()}`)
          for (const entry of this.stateChangeCallbacks) if (entry.kind === "open") entry.callback()
          for (const channel of this.channels) channel.socketOpened()
        }),
      connectionLost: () =>
        later(() => {
          for (const channel of this.channels) channel.socketClosed()
        }),
      closed: (details) =>
        later(() => {
          this.log("transport", "close", details)
          for (const entry of this.stateChangeCallbacks)
            if (entry.kind === "close") entry.callback(details)
        }),
      errored: (error) =>
        later(() => {
          this.log("transport", "error", error)
          for (const entry of this.stateChangeCallbacks)
            if (entry.kind === "error")
              entry.callback(error, this.transport, this.establishedConnections)
        }),
      message: (data) => later(() => this.decode(data, (message) => this.receive(message))),
      disconnected: () =>
        later(() => {
          const callbacks = this.disconnectCallbacks
          this.disconnectCallbacks = []
          for (const callback of callbacks) callback()
        }),
    }
  }

  private receive(message: Message): void {
    const { topic, event, payload, ref, join_ref } = message
    this.log("receive", `${topic} ${event} ${ref ?? ""} ${join_ref ?? ""}`, payload)
    if (
      topic === "phoenix" &&
      ref !== null &&
      ref === this.actor.getSnapshot().context.heartbeatRef
    ) {
      this.actor.send({ type: "HEARTBEAT_REPLY" })
    }
    for (const channel of this.channels)
      if (channel.isMember(message)) channel.receiveMessage(message)
    for (const entry of this.stateChangeCallbacks)
      if (entry.kind === "message") entry.callback(message)
  }
}
