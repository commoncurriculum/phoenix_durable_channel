import type { ActivityStatus } from "@supergrain/activity"
import { assign, emit, enqueueActions, fromCallback, raise, sendTo, setup } from "xstate"
import {
  backoff,
  ignore,
  sleep,
  type CloseDetails,
  type Data,
  type TransportCommand,
  type TransportEvent,
  type TransportKind,
} from "../actors"
import type { ConnectionState } from "../constants"
import type { EncodedMessage, Message } from "../serializer"

export type { TransportKind }

export interface SocketTiming {
  longPollFallbackMs: number | null
  heartbeatIntervalMs: number
  heartbeatTimeoutMs: number
  websocketRetryAfterMs: number
  offlineAfterMs: number
  disconnectAfterInactiveMs: number | null
  closeTimeoutMs: number
}

export type SocketEvent =
  | TransportEvent
  | { type: "CONNECT" }
  | { type: "DISCONNECT"; code: number; reason: string }
  | { type: "PUSH"; message: Message }
  | { type: "ENCODED"; data: string | ArrayBuffer; connection: number }
  | { type: "HEARTBEAT_REPLY"; ref: string }
  | { type: "ACTIVITY"; status: ActivityStatus }
  | { type: "SUSPEND" }
  | { type: "CONNECTION_OPENED" }
  | { type: "CONNECTION_LOST" }
  | { type: "REPLACE_TRANSPORT"; transport: TransportKind }

export type SocketEmitted =
  | { type: "encode"; message: Message; connection: number }
  | { type: "buffered"; message: Message }
  | { type: "opened"; connection: number }
  | { type: "connectionLost"; connection: number }
  | ({ type: "closed" } & CloseDetails)
  | { type: "errored"; error: unknown }
  | { type: "message"; data: EncodedMessage }
  | { type: "disconnected" }

type SocketContext = Data<{
  timing: SocketTiming
  transport: TransportKind
  webSocketOpened: boolean
  fellBack: boolean
  tries: number
  heartbeats: number
  heartbeatRef: string | null
  closeRequest: { code: number; reason: string }
  buffer: ReadonlyArray<Message>
  closeWasClean: boolean
  establishedConnections: number
}>

const closeDetails = ({ code, reason, wasClean }: CloseDetails): CloseDetails => ({
  code,
  reason,
  wasClean,
})

const suspended: CloseDetails = {
  code: 1000,
  reason: "inactive",
  wasClean: true,
}

const heartbeatTimedOut: CloseDetails = {
  code: 1000,
  reason: "heartbeat timeout",
  wasClean: false,
}

export const socketMachine = setup({
  types: {
    context: {} as SocketContext,
    input: {} as { timing: SocketTiming; transport: TransportKind },
    events: {} as SocketEvent,
    emitted: {} as SocketEmitted,
    tags: {} as ConnectionState,
  },
  actors: {
    sleep,
    transport: fromCallback<TransportCommand, { kind: TransportKind }, TransportEvent>(ignore),
    watchActivity: fromCallback<SocketEvent, void, SocketEvent>(ignore),
    waitToReconnect: backoff(Number),
  },
  guards: {
    isThisConnection: ({ context, event }) =>
      event.type === "ENCODED" && event.connection === context.establishedConnections,
    canFallBack: ({ context }) =>
      context.transport === "websocket" &&
      context.timing.longPollFallbackMs !== null &&
      !context.webSocketOpened,
    userIsActive: ({ event }) => event.type === "ACTIVITY" && event.status === "active",
    userIsAway: ({ event }) => event.type === "ACTIVITY" && event.status !== "active",
    answersHeartbeat: ({ context, event }) =>
      event.type === "HEARTBEAT_REPLY" && event.ref === context.heartbeatRef,
  },
  actions: {
    fallBack: assign({ transport: "longpoll", fellBack: true }),
    close: enqueueActions(({ enqueue }, details: CloseDetails) => {
      enqueue.emit({ type: "closed", ...details })
      enqueue.assign({ closeWasClean: details.wasClean })
    }),
    buffer: enqueueActions(({ context, enqueue }, message: Message) => {
      enqueue.emit({ type: "buffered", message })
      enqueue.assign({ buffer: [...context.buffer, message] })
    }),
    dropBuffer: assign({ buffer: [] }),
  },
}).createMachine({
  id: "socket",
  type: "parallel",
  context: ({ input }) => ({
    timing: input.timing,
    transport: input.transport,
    webSocketOpened: false,
    fellBack: false,
    tries: 0,
    heartbeats: 0,
    heartbeatRef: null,
    closeRequest: { code: 1000, reason: "" },
    buffer: [],
    closeWasClean: false,
    establishedConnections: 0,
  }),
  states: {
    connection: {
      initial: "disconnected",
      on: {
        DISCONNECT: { target: ".disconnected", actions: "dropBuffer" },
        CONNECT: { target: ".engaged.live" },
        SUSPEND: { target: ".engaged.suspended" },
        PUSH: {
          actions: { type: "buffer", params: ({ event }) => event.message },
        },
        REPLACE_TRANSPORT: {
          target: ".disconnected",
          actions: [
            "dropBuffer",
            assign(({ event }) => ({
              transport: event.transport,
              webSocketOpened: false,
              fellBack: false,
            })),
          ],
        },
      },
      states: {
        disconnected: {
          tags: "closed",
          entry: emit({ type: "disconnected" }),
          on: { SUSPEND: {} },
        },
        engaged: {
          invoke: { src: "watchActivity" },
          initial: "live",
          states: {
            live: {
              invoke: {
                id: "transport",
                src: "transport",
                input: ({ context }) => ({ kind: context.transport }),
              },
              initial: "connecting",
              on: {
                CONNECT: {},
                TRANSPORT_ERROR: {
                  actions: emit(({ event }) => ({ type: "errored" as const, error: event.error })),
                },
                TRANSPORT_MESSAGE: {
                  actions: emit(({ event }) => ({ type: "message" as const, data: event.data })),
                },
                TRANSPORT_CLOSE: [
                  {
                    guard: "canFallBack",
                    target: "live",
                    reenter: true,
                    actions: [
                      {
                        type: "close",
                        params: ({ event }) => closeDetails(event),
                      },
                      "fallBack",
                    ],
                  },
                  {
                    target: "waitingToReconnect",
                    actions: {
                      type: "close",
                      params: ({ event }) => closeDetails(event),
                    },
                  },
                ],
                SUSPEND: {
                  target: "suspended",
                  actions: { type: "close", params: suspended },
                },
                DISCONNECT: {
                  target: "#socket.connection.disconnected",
                  actions: [
                    "dropBuffer",
                    {
                      type: "close",
                      params: ({ event }) => ({
                        code: event.code,
                        reason: event.reason,
                        wasClean: true,
                      }),
                    },
                  ],
                },
              },
              states: {
                connecting: {
                  tags: "connecting",
                  invoke: {
                    src: "sleep",
                    input: ({ context }) => ({
                      ms:
                        context.transport === "websocket" && !context.webSocketOpened
                          ? context.timing.longPollFallbackMs
                          : null,
                    }),
                    onDone: {
                      target: "#socket.connection.engaged.live",
                      reenter: true,
                      actions: "fallBack",
                    },
                  },
                  on: { TRANSPORT_OPEN: "open" },
                },
                open: {
                  tags: "open",
                  entry: [
                    assign({
                      tries: 0,
                      webSocketOpened: ({ context }) =>
                        context.webSocketOpened || context.transport === "websocket",
                      closeWasClean: false,
                      establishedConnections: ({ context }) => context.establishedConnections + 1,
                    }),
                    enqueueActions(({ context, enqueue }) => {
                      for (const message of context.buffer)
                        enqueue.emit({
                          type: "encode",
                          message,
                          connection: context.establishedConnections,
                        })
                    }),
                    "dropBuffer",
                    emit(({ context }) => ({
                      type: "opened" as const,
                      connection: context.establishedConnections,
                    })),
                    raise({ type: "CONNECTION_OPENED" }),
                  ],
                  exit: [
                    emit(({ context }) => ({
                      type: "connectionLost" as const,
                      connection: context.establishedConnections,
                    })),
                    raise({ type: "CONNECTION_LOST" }),
                  ],
                  invoke: {
                    src: "sleep",
                    input: ({ context }) => ({
                      ms: context.fellBack ? context.timing.websocketRetryAfterMs : null,
                    }),
                    onDone: {
                      target: "#socket.connection.engaged.live",
                      reenter: true,
                      actions: assign({
                        transport: "websocket",
                        fellBack: false,
                      }),
                    },
                  },
                  on: {
                    PUSH: {
                      actions: emit(({ context, event }) => ({
                        type: "encode" as const,
                        message: event.message,
                        connection: context.establishedConnections,
                      })),
                    },
                    ENCODED: {
                      guard: "isThisConnection",
                      actions: sendTo("transport", ({ event }) => ({
                        type: "SEND" as const,
                        data: event.data,
                      })),
                    },
                    DISCONNECT: {
                      target: "closing",
                      actions: [
                        assign({
                          closeRequest: ({ event }) => ({
                            code: event.code,
                            reason: event.reason,
                          }),
                        }),
                      ],
                    },
                  },
                  initial: "beating",
                  states: {
                    beating: {
                      invoke: {
                        src: "sleep",
                        input: ({ context }) => ({
                          ms: context.timing.heartbeatIntervalMs,
                        }),
                        onDone: "awaitingHeartbeat",
                      },
                      on: {
                        ACTIVITY: {
                          guard: "userIsActive",
                          target: "awaitingHeartbeat",
                        },
                      },
                    },
                    awaitingHeartbeat: {
                      entry: [
                        assign({
                          heartbeats: ({ context }) => context.heartbeats + 1,
                          heartbeatRef: ({ context }) => `heartbeat:${context.heartbeats + 1}`,
                        }),
                        emit(({ context }) => ({
                          type: "encode" as const,
                          message: {
                            topic: "phoenix",
                            event: "heartbeat",
                            payload: {},
                            ref: context.heartbeatRef,
                            join_ref: null,
                          },
                          connection: context.establishedConnections,
                        })),
                      ],
                      invoke: {
                        src: "sleep",
                        input: ({ context }) => ({
                          ms: context.timing.heartbeatTimeoutMs,
                        }),
                        onDone: {
                          target: "#socket.connection.engaged.waitingToReconnect",
                          actions: { type: "close", params: heartbeatTimedOut },
                        },
                      },
                      on: { HEARTBEAT_REPLY: { guard: "answersHeartbeat", target: "beating" } },
                    },
                  },
                },
                closing: {
                  tags: "closing",
                  entry: [
                    "dropBuffer",
                    sendTo("transport", ({ context }) => ({
                      type: "CLOSE" as const,
                      ...context.closeRequest,
                    })),
                  ],
                  invoke: {
                    src: "sleep",
                    input: ({ context }) => ({
                      ms: context.timing.closeTimeoutMs,
                    }),
                    onDone: "#socket.connection.disconnected",
                  },
                  on: {
                    TRANSPORT_CLOSE: {
                      target: "#socket.connection.disconnected",
                      actions: {
                        type: "close",
                        params: ({ event }) => closeDetails(event),
                      },
                    },
                    CONNECT: {
                      target: "#socket.connection.engaged.live",
                      reenter: true,
                      actions: {
                        type: "close",
                        params: ({ context }) => ({
                          ...context.closeRequest,
                          wasClean: true,
                        }),
                      },
                    },
                  },
                },
              },
            },
            waitingToReconnect: {
              tags: "closed",
              entry: [
                assign({ tries: ({ context }) => context.tries + 1 }),
                raise({ type: "CONNECTION_LOST" }),
              ],
              invoke: {
                src: "waitToReconnect",
                input: ({ context }) => ({ tries: context.tries }),
                onDone: "live",
              },
              on: { ACTIVITY: { guard: "userIsActive", target: "live" } },
            },
            suspended: {
              tags: "closed",
              on: { ACTIVITY: { guard: "userIsActive", target: "live" } },
            },
          },
        },
      },
    },
    activity: {
      initial: "active",
      states: {
        active: {
          on: { ACTIVITY: { guard: "userIsAway", target: "away" } },
        },
        away: {
          invoke: {
            src: "sleep",
            input: ({ context }) => ({
              ms: context.timing.disconnectAfterInactiveMs,
            }),
            onDone: { actions: raise({ type: "SUSPEND" }) },
          },
          on: { ACTIVITY: { guard: "userIsActive", target: "active" } },
        },
      },
    },
    online: {
      initial: "online",
      states: {
        online: { on: { CONNECTION_LOST: "goingOffline" } },
        goingOffline: {
          invoke: {
            src: "sleep",
            input: ({ context }) => ({ ms: context.timing.offlineAfterMs }),
            onDone: "offline",
          },
          on: { CONNECTION_OPENED: "online" },
        },
        offline: { on: { CONNECTION_OPENED: "online" } },
      },
    },
  },
})
