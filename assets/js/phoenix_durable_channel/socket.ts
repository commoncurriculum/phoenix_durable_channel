import type { ActivityTracker } from "@supergrain/activity"
import { createReactive } from "@supergrain/kernel"
import { fromCallback, type Actor } from "xstate"
import {
  backoff,
  startMachine,
  tagged,
  transport,
  type CloseDetails,
  type TransportConstructor,
} from "./actors"
import { Channel, type ChannelOptions } from "./channel"
import {
  AUTH_TOKEN_PREFIX,
  DEFAULT_TIMEOUT,
  DEFAULT_VSN,
  TRANSPORTS,
  decodeConnectionState,
  type ConnectionState,
} from "./constants"
import { LongPoll } from "./longpoll"
import {
  socketMachine,
  type SocketEmitted,
  type SocketEvent,
  type TransportKind,
} from "./machines/socket"
import { Serializer, type EncodedMessage, type Message } from "./serializer"
import { appendParams, type Params } from "./url"

export type { ConnectionState }

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
  authToken?: string | (() => string)
  binaryType?: BinaryType
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
  readonly binaryType: BinaryType
  readonly heartbeatIntervalMs: number
  readonly logger: SocketOptions["logger"]
  readonly encode: NonNullable<SocketOptions["encode"]>
  readonly decode: NonNullable<SocketOptions["decode"]>
  params: () => Params
  private readonly authToken: (() => string) | null
  private ref = 0
  private webSocket: TransportConstructor
  private stateChangeCallbacks: Array<StateChangeCallback & { ref: string }> = []
  private disconnectCallbacks: Array<() => void> = []

  constructor(endPoint: string, options: SocketOptions = {}) {
    this.endPoint = `${endPoint}/${TRANSPORTS.websocket}`
    this.timeout = options.timeout ?? DEFAULT_TIMEOUT
    this.vsn = options.vsn ?? DEFAULT_VSN
    this.longpollerTimeout = options.longpollerTimeout ?? 20_000
    this.logger = options.logger
    this.heartbeatIntervalMs = options.heartbeatIntervalMs ?? 30_000
    const json = options.transport === LongPoll
    this.encode =
      (json ? undefined : options.encode) ??
      ((message, callback) => Serializer.encode(message, callback))
    this.decode =
      (json ? undefined : options.decode) ?? ((raw, callback) => Serializer.decode(raw, callback))
    const params = options.params ?? {}
    this.params = typeof params === "function" ? params : () => params
    const authToken = options.authToken
    this.authToken =
      authToken === undefined ? null : typeof authToken === "function" ? authToken : () => authToken
    this.binaryType = options.binaryType ?? "arraybuffer"
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
    const activity = options.activity ?? null
    this.actor = startMachine(
      socketMachine.provide({
        actors: {
          transport: transport((transportKind) => ({
            transport: transportKind === "longpoll" ? LongPoll : this.webSocket,
            url: this.endPointURL(),
            protocols:
              this.authToken === null
                ? undefined
                : ["phoenix", `${AUTH_TOKEN_PREFIX}${btoa(this.authToken()).replace(/=/g, "")}`],
            binaryType: this.binaryType,
            timeout: this.longpollerTimeout,
          })),
          watchActivity: fromCallback<SocketEvent, void, SocketEvent>(({ sendBack }) => {
            if (activity === null) return
            sendBack({ type: "ACTIVITY", status: activity.state.status })
            const stops = (["active", "idle", "hidden"] as const).map((status) =>
              activity.on(status, () => sendBack({ type: "ACTIVITY", status })),
            )
            return () => {
              for (const stop of stops) stop()
            }
          }),
          waitToReconnect: backoff((tries) => this.reconnectAfterMs(tries)),
        },
      }),
      {
        transport: kind,
        timing: {
          longPollFallbackMs: options.longPollFallbackMs ?? null,
          heartbeatIntervalMs: this.heartbeatIntervalMs,
          heartbeatTimeoutMs: options.heartbeatTimeoutMs ?? 10_000,
          websocketRetryAfterMs: options.websocketRetryAfterMs ?? 10 * 60_000,
          offlineAfterMs: options.offlineAfterMs ?? 25_000,
          disconnectAfterInactiveMs: options.disconnectAfterInactiveMs ?? null,
          closeTimeoutMs: 1500,
        },
      },
      (snapshot) => {
        this.status.connectionState = tagged(snapshot, decodeConnectionState)
        this.status.transport = snapshot.context.transport
        this.status.online = !snapshot.matches({ online: "offline" })
      },
      (event) => this.handle(event),
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
    this.loseConnection()
    this.actor.send({ type: "DISCONNECT", code, reason })
  }

  replaceTransport(transport: TransportConstructor): void {
    this.webSocket = transport
    this.loseConnection()
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

  private loseConnection(): void {
    if (this.isConnected()) for (const channel of this.channels) channel.socketClosed()
  }

  private addCallback(callback: StateChangeCallback): string {
    const ref = this.makeRef()
    this.stateChangeCallbacks.push({ ...callback, ref })
    return ref
  }

  private handle(event: SocketEmitted): void {
    switch (event.type) {
      case "encode":
        return this.encode(event.message, (data) => this.actor.send({ type: "ENCODED", data }))
      case "buffered":
        return this.log("push", "buffered until the socket opens", event.message)
      case "opened":
        this.log("transport", `connected to ${this.endPointURL()}`)
        for (const entry of this.stateChangeCallbacks) if (entry.kind === "open") entry.callback()
        for (const channel of this.channels) channel.socketOpened()
        return
      case "connectionLost":
        for (const channel of this.channels) channel.socketClosed()
        return
      case "closed": {
        const details = { code: event.code, reason: event.reason, wasClean: event.wasClean }
        this.log("transport", "close", details)
        for (const entry of this.stateChangeCallbacks)
          if (entry.kind === "close") entry.callback(details)
        return
      }
      case "errored":
        this.log("transport", "error", event.error)
        for (const entry of this.stateChangeCallbacks)
          if (entry.kind === "error")
            entry.callback(event.error, this.transport, this.establishedConnections)
        return
      case "message":
        return this.decode(event.data, (message) => this.receive(message))
      case "disconnected": {
        const callbacks = this.disconnectCallbacks
        this.disconnectCallbacks = []
        for (const callback of callbacks) callback()
      }
    }
  }

  private receive(message: Message): void {
    const { topic, event, payload, ref, join_ref } = message
    this.log("receive", `${topic} ${event} ${ref ?? ""} ${join_ref ?? ""}`, payload)
    if (topic === "phoenix" && ref !== null) this.actor.send({ type: "HEARTBEAT_REPLY", ref })
    for (const channel of this.channels)
      if (channel.isMember(message)) channel.receiveMessage(message)
    for (const entry of this.stateChangeCallbacks)
      if (entry.kind === "message") entry.callback(message)
  }
}
