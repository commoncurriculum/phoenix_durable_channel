export const DEFAULT_VSN = "2.0.0"
export const DEFAULT_TIMEOUT = 10_000

export const SOCKET_STATES = {
  connecting: 0,
  open: 1,
  closing: 2,
  closed: 3,
} as const

export const CHANNEL_STATES = {
  closed: "closed",
  errored: "errored",
  joined: "joined",
  joining: "joining",
  leaving: "leaving",
} as const

export type ChannelState = (typeof CHANNEL_STATES)[keyof typeof CHANNEL_STATES]

export const CHANNEL_EVENTS = {
  close: "phx_close",
  error: "phx_error",
  join: "phx_join",
  reply: "phx_reply",
  leave: "phx_leave",
  // Durable messages only; answered by PhoenixDurableChannel.Channel on the server.
  sync: "phx_sync",
  read: "phx_read",
  reader: "phx_reader",
} as const

/**
 * The most durable messages a channel has unanswered at once. A `PhoenixDurableChannel.Ledger`
 * keeps at least this many replies, so a resend is always of one it kept.
 */
export const DURABLE_WINDOW = 32

export const TRANSPORTS = {
  longpoll: "longpoll",
  websocket: "websocket",
} as const
