import { Schema } from "effect"

export const DEFAULT_VSN = "2.0.0"
export const DEFAULT_TIMEOUT = 10_000
export const AUTH_TOKEN_PREFIX = "base64url.bearer.phx."

export const SOCKET_STATES = {
  connecting: 0,
  open: 1,
  closing: 2,
  closed: 3,
} as const

export type ConnectionState = keyof typeof SOCKET_STATES

export const CHANNEL_STATES = {
  closed: "closed",
  errored: "errored",
  joined: "joined",
  joining: "joining",
  leaving: "leaving",
} as const

export type ChannelState = (typeof CHANNEL_STATES)[keyof typeof CHANNEL_STATES]

export const decodeConnectionState = Schema.decodeUnknownSync(
  Schema.Literal("connecting", "open", "closing", "closed"),
)

export const decodeChannelState = Schema.decodeUnknownSync(
  Schema.Literal("closed", "errored", "joined", "joining", "leaving"),
)

export const decodePushState = Schema.decodeUnknownSync(
  Schema.Literal("buffered", "sent", "ok", "error", "timeout"),
)

export const CHANNEL_EVENTS = {
  close: "phx_close",
  error: "phx_error",
  join: "phx_join",
  reply: "phx_reply",
  leave: "phx_leave",
  sync: "phx_sync",
  read: "phx_read",
  reader: "phx_reader",
  skipped: "phx_skipped",
} as const

export const TRANSPORTS = {
  longpoll: "longpoll",
  websocket: "websocket",
} as const
