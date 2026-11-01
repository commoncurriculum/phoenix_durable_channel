import type { ActivityStatus, ActivityTracker } from "@supergrain/activity"
import { assign, fromCallback, raise, sendTo, setup } from "xstate"
import {
  sleep,
  transport,
  type CloseDetails,
  type TransportConstructor,
  type TransportEvent,
} from "../actors"
import type { EncodedMessage, Message } from "../serializer"

export type TransportKind = "websocket" | "longpoll"

export interface SocketHooks {
  url(): string
  protocols(): Array<string> | undefined
  binaryType: BinaryType
  transportFor(kind: TransportKind): TransportConstructor
  longpollerTimeout: number
  activity: ActivityTracker | null
  makeRef(): string
  reconnectAfterMs(tries: number): number
  send(message: Message): void
  buffered(message: Message): void
  opened(): void
  connectionLost(): void
  closed(details: CloseDetails): void
  errored(error: unknown): void
  message(data: EncodedMessage): void
  disconnected(): void
}

export interface SocketSettings {
  longPollFallbackMs: number | null
  heartbeatIntervalMs: number
  heartbeatTimeoutMs: number
  websocketRetryAfterMs: number
  offlineAfterMs: number
  disconnectAfterInactiveMs: number | null
  closeTimeoutMs: number
}

type SocketEvent =
  | TransportEvent
  | { type: "CONNECT" }
  | { type: "DISCONNECT"; code: number; reason: string }
  | { type: "PUSH"; message: Message }
  | { type: "HEARTBEAT_REPLY" }
  | { type: "ACTIVITY"; status: ActivityStatus }
  | { type: "SUSPEND" }
  | { type: "CONNECTION_OPENED" }
  | { type: "CONNECTION_LOST" }
  | { type: "REPLACE_TRANSPORT"; transport: TransportKind }

const watchActivity = fromCallback<SocketEvent, { activity: ActivityTracker | null }, SocketEvent>(
  ({ input, sendBack }) => {
    const { activity } = input
    if (activity === null) return
    sendBack({ type: "ACTIVITY", status: activity.state.status })
    const stops = (["active", "idle", "hidden"] as const).map((status) =>
      activity.on(status, () => sendBack({ type: "ACTIVITY", status })),
    )
    return () => {
      for (const stop of stops) stop()
    }
  },
)

interface SocketContext {
  hooks: SocketHooks
  settings: SocketSettings
  transport: TransportKind
  webSocketOpened: boolean
  fellBack: boolean
  tries: number
  heartbeatRef: string | null
  closeRequest: { code: number; reason: string }
  buffer: Array<Message>
  closeWasClean: boolean
  establishedConnections: number
}

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
    input: {} as {
      hooks: SocketHooks
      settings: SocketSettings
      transport: TransportKind
    },
    events: {} as SocketEvent,
  },
  actors: { sleep, transport, watchActivity },
  guards: {
    canFallBack: ({ context }) =>
      context.transport === "websocket" &&
      context.settings.longPollFallbackMs !== null &&
      !context.webSocketOpened,
    userIsActive: ({ event }) => event.type === "ACTIVITY" && event.status === "active",
    userIsAway: ({ event }) => event.type === "ACTIVITY" && event.status !== "active",
  },
  actions: {
    fallBack: assign({ transport: "longpoll", fellBack: true }),
    close: assign(({ context }, details: CloseDetails) => {
      context.hooks.closed(details)
      return { closeWasClean: details.wasClean }
    }),
    buffer: assign(({ context }, message: Message) => {
      context.hooks.buffered(message)
      return { buffer: [...context.buffer, message] }
    }),
    dropBuffer: assign({ buffer: [] }),
  },
}).createMachine({
  id: "socket",
  type: "parallel",
  context: ({ input }) => ({
    hooks: input.hooks,
    settings: input.settings,
    transport: input.transport,
    webSocketOpened: false,
    fellBack: false,
    tries: 0,
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
            assign({
              transport: ({ event }) => event.transport,
              webSocketOpened: false,
              fellBack: false,
            }),
          ],
        },
      },
      states: {
        disconnected: {
          entry: ({ context }) => context.hooks.disconnected(),
          on: { SUSPEND: {} },
        },
        engaged: {
          invoke: {
            src: "watchActivity",
            input: ({ context }) => ({ activity: context.hooks.activity }),
          },
          initial: "live",
          states: {
            live: {
              invoke: {
                id: "transport",
                src: "transport",
                input: ({ context }) => ({
                  transport: context.hooks.transportFor(context.transport),
                  url: context.hooks.url(),
                  protocols: context.hooks.protocols(),
                  binaryType: context.hooks.binaryType,
                  timeout: context.hooks.longpollerTimeout,
                }),
              },
              initial: "connecting",
              on: {
                CONNECT: {},
                TRANSPORT_ERROR: {
                  actions: ({ context, event }) => context.hooks.errored(event.error),
                },
                TRANSPORT_MESSAGE: {
                  actions: ({ context, event }) => context.hooks.message(event.data),
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
                  invoke: {
                    src: "sleep",
                    input: ({ context }) => ({
                      ms:
                        context.transport === "websocket" && !context.webSocketOpened
                          ? context.settings.longPollFallbackMs
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
                  entry: [
                    assign({
                      tries: 0,
                      webSocketOpened: ({ context }) =>
                        context.webSocketOpened || context.transport === "websocket",
                      closeWasClean: false,
                      establishedConnections: ({ context }) => context.establishedConnections + 1,
                    }),
                    ({ context }) => {
                      for (const message of context.buffer) context.hooks.send(message)
                    },
                    "dropBuffer",
                    ({ context }) => context.hooks.opened(),
                    raise({ type: "CONNECTION_OPENED" }),
                  ],
                  exit: [
                    ({ context, event }) => {
                      if (event.type !== "DISCONNECT" && event.type !== "REPLACE_TRANSPORT")
                        context.hooks.connectionLost()
                    },
                    raise({ type: "CONNECTION_LOST" }),
                  ],
                  invoke: {
                    src: "sleep",
                    input: ({ context }) => ({
                      ms: context.fellBack ? context.settings.websocketRetryAfterMs : null,
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
                      actions: ({ context, event }) => context.hooks.send(event.message),
                    },
                    DISCONNECT: {
                      target: "closing",
                      actions: assign({
                        closeRequest: ({ event }) => ({
                          code: event.code,
                          reason: event.reason,
                        }),
                      }),
                    },
                  },
                  initial: "beating",
                  states: {
                    beating: {
                      invoke: {
                        src: "sleep",
                        input: ({ context }) => ({
                          ms: context.settings.heartbeatIntervalMs,
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
                          heartbeatRef: ({ context }) => context.hooks.makeRef(),
                        }),
                        ({ context }) =>
                          context.hooks.send({
                            topic: "phoenix",
                            event: "heartbeat",
                            payload: {},
                            ref: context.heartbeatRef,
                            join_ref: null,
                          }),
                      ],
                      invoke: {
                        src: "sleep",
                        input: ({ context }) => ({
                          ms: context.settings.heartbeatTimeoutMs,
                        }),
                        onDone: {
                          target: "#socket.connection.engaged.waitingToReconnect",
                          actions: { type: "close", params: heartbeatTimedOut },
                        },
                      },
                      on: { HEARTBEAT_REPLY: "beating" },
                    },
                  },
                },
                closing: {
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
                      ms: context.settings.closeTimeoutMs,
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
              entry: [
                assign({ tries: ({ context }) => context.tries + 1 }),
                raise({ type: "CONNECTION_LOST" }),
              ],
              invoke: {
                src: "sleep",
                input: ({ context }) => ({
                  ms: context.hooks.reconnectAfterMs(context.tries),
                }),
                onDone: "live",
              },
              on: { ACTIVITY: { guard: "userIsActive", target: "live" } },
            },
            suspended: {
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
              ms: context.settings.disconnectAfterInactiveMs,
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
            input: ({ context }) => ({ ms: context.settings.offlineAfterMs }),
            onDone: "offline",
          },
          on: { CONNECTION_OPENED: "online" },
        },
        offline: { on: { CONNECTION_OPENED: "online" } },
      },
    },
  },
})
