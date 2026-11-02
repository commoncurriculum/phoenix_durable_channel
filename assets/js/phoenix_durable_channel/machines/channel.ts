import { assign, emit, setup } from "xstate"
import { backoff, sleep, type Data } from "../actors"
import type { ChannelState } from "../constants"

type ChannelEvent =
  | { type: "JOIN"; timeout: number }
  | { type: "JOIN_SENT"; ref: string }
  | { type: "JOIN_ENCODED"; ref: string | null; connection: number }
  | { type: "LEAVE"; timeout: number }
  | { type: "LEAVE_SENT"; ref: string }
  | { type: "SOCKET_OPEN"; connection: number }
  | { type: "SOCKET_CLOSED"; connection: number }
  | { type: "JOIN_REPLY"; ref: string | null; status: string; response: unknown }
  | { type: "JOIN_REFUSED"; ref: string | null; response: unknown }
  | { type: "LEAVE_REPLY"; ref: string | null }
  | { type: "SERVER_ERROR"; reason: unknown }
  | { type: "SERVER_CLOSE"; reason: unknown }
  | { type: "REPLACED" }

export type ChannelEmitted =
  | { type: "join" }
  | { type: "leave"; joinRef: string | null }
  | { type: "answerJoin"; status: "ok" | "error" | "timeout"; response: unknown }
  | { type: "answerLeave"; status: "ok" | "timeout" }
  | { type: "joined"; response: unknown }
  | { type: "lost" }
  | { type: "errored"; reason: unknown }
  | { type: "closed"; reason: unknown }

type ChannelContext = Data<{
  timeout: number
  connection: number | null
  joinConnection: number | null
  joinRef: string | null
  leaveRef: string | null
  rejoinTries: number
  joinResponse: unknown
}>

export const channelMachine = setup({
  types: {
    context: {} as ChannelContext,
    input: {} as { timeout: number; connection: number | null },
    events: {} as ChannelEvent,
    emitted: {} as ChannelEmitted,
    tags: {} as ChannelState,
  },
  actors: { sleep, waitToRejoin: backoff(Number) },
  guards: {
    socketIsOpen: ({ context }) => context.connection !== null,
    closesSocket: ({ context, event }) =>
      event.type === "SOCKET_CLOSED" && event.connection === context.connection,
    losesJoin: ({ context, event }) =>
      event.type === "SOCKET_CLOSED" && event.connection === context.joinConnection,
    carriesJoin: ({ context, event }) =>
      event.type === "JOIN_ENCODED" && event.ref === context.joinRef,
    answersJoin: ({ context, event }) =>
      (event.type === "JOIN_REPLY" || event.type === "JOIN_REFUSED") &&
      event.ref === context.joinRef,
    acceptsJoin: ({ context, event }) =>
      event.type === "JOIN_REPLY" && event.ref === context.joinRef && event.status === "ok",
    answersLeave: ({ context, event }) =>
      event.type === "LEAVE_REPLY" && event.ref === context.leaveRef,
  },
  actions: {
    socketOpened: assign((_, connection: number) => ({ connection })),
    socketClosed: assign({ connection: null }),
    lose: emit({ type: "lost" }),
    error: emit(({ event }) => ({
      type: "errored" as const,
      reason: "reason" in event ? event.reason : undefined,
    })),
    answerJoin: emit((_, params: { status: "ok" | "error" | "timeout"; response: unknown }) => ({
      type: "answerJoin" as const,
      ...params,
    })),
    answerLeave: emit((_, status: "ok" | "timeout") => ({ type: "answerLeave" as const, status })),
    leaveJoin: emit(({ context }) => ({ type: "leave" as const, joinRef: context.joinRef })),
  },
}).createMachine({
  id: "channel",
  context: ({ input }) => ({
    timeout: input.timeout,
    connection: input.connection,
    joinConnection: null,
    joinRef: null,
    leaveRef: null,
    rejoinTries: 0,
    joinResponse: undefined,
  }),
  initial: "unjoined",
  on: {
    REPLACED: { target: ".closed" },
    SOCKET_OPEN: { actions: { type: "socketOpened", params: ({ event }) => event.connection } },
    SOCKET_CLOSED: { guard: "closesSocket", actions: "socketClosed" },
    JOIN_SENT: { actions: assign({ joinRef: ({ event }) => event.ref, joinConnection: null }) },
    JOIN_ENCODED: {
      guard: "carriesJoin",
      actions: assign({ joinConnection: ({ event }) => event.connection }),
    },
    LEAVE_SENT: { actions: assign({ leaveRef: ({ event }) => event.ref }) },
  },
  states: {
    unjoined: {
      tags: "closed",
      on: {
        JOIN: { target: "member", actions: assign({ timeout: ({ event }) => event.timeout }) },
        LEAVE: { target: "closed", actions: { type: "answerLeave", params: "ok" } },
      },
    },
    member: {
      initial: "joining",
      states: {
        joining: {
          tags: "joining",
          initial: "checkingSocket",
          on: {
            LEAVE: {
              target: "leaving",
              actions: assign({ timeout: ({ event }) => event.timeout }),
            },
            SERVER_CLOSE: "#channel.closed",
            SERVER_ERROR: { target: "errored", actions: "error" },
          },
          states: {
            checkingSocket: {
              always: [
                { guard: "socketIsOpen", target: "awaitingReply" },
                { target: "awaitingSocket" },
              ],
            },
            awaitingSocket: {
              on: {
                LEAVE: {
                  target: "#channel.closed",
                  actions: { type: "answerLeave", params: "ok" },
                },
                SOCKET_OPEN: {
                  target: "awaitingReply",
                  actions: { type: "socketOpened", params: ({ event }) => event.connection },
                },
              },
            },
            awaitingReply: {
              entry: emit({ type: "join" }),
              invoke: {
                src: "sleep",
                input: ({ context }) => ({ ms: context.timeout }),
                onDone: {
                  target: "#channel.member.errored",
                  actions: [
                    { type: "answerJoin", params: { status: "timeout", response: undefined } },
                    "leaveJoin",
                  ],
                },
              },
              on: {
                JOIN_REFUSED: {
                  guard: "answersJoin",
                  target: "#channel.member.errored",
                  actions: [
                    {
                      type: "answerJoin",
                      params: ({ event }) => ({ status: "error", response: event.response }),
                    },
                    "leaveJoin",
                  ],
                },
                JOIN_REPLY: [
                  {
                    guard: "acceptsJoin",
                    target: "#channel.member.joined",
                    actions: assign({ joinResponse: ({ event }) => event.response }),
                  },
                  {
                    guard: "answersJoin",
                    target: "#channel.member.errored",
                    actions: {
                      type: "answerJoin",
                      params: ({ event }) => ({ status: "error", response: event.response }),
                    },
                  },
                ],
                SOCKET_CLOSED: {
                  guard: "losesJoin",
                  target: "#channel.member.errored",
                  actions: ["socketClosed", "error"],
                },
              },
            },
          },
        },
        joined: {
          tags: "joined",
          entry: [
            assign({ rejoinTries: 0 }),
            emit(({ context }) => ({ type: "joined" as const, response: context.joinResponse })),
            {
              type: "answerJoin",
              params: ({ context }) => ({ status: "ok", response: context.joinResponse }),
            },
          ],
          on: {
            LEAVE: {
              target: "leaving",
              actions: assign({ timeout: ({ event }) => event.timeout }),
            },
            SOCKET_CLOSED: {
              guard: "losesJoin",
              target: "errored",
              actions: ["socketClosed", "error"],
            },
            SERVER_ERROR: { target: "errored", actions: "error" },
            SERVER_CLOSE: "#channel.closed",
          },
        },
        errored: {
          tags: "errored",
          entry: "lose",
          initial: "checkingSocket",
          on: {
            LEAVE: { target: "#channel.closed", actions: { type: "answerLeave", params: "ok" } },
            SOCKET_OPEN: {
              target: "joining",
              actions: [
                { type: "socketOpened", params: ({ event }) => event.connection },
                assign({ rejoinTries: 0 }),
              ],
            },
          },
          states: {
            checkingSocket: {
              always: [
                { guard: "socketIsOpen", target: "awaitingRejoin" },
                { target: "awaitingSocket" },
              ],
            },
            awaitingSocket: {},
            awaitingRejoin: {
              invoke: {
                src: "waitToRejoin",
                input: ({ context }) => ({ tries: context.rejoinTries + 1 }),
                onDone: {
                  target: "#channel.member.joining",
                  actions: assign({
                    rejoinTries: ({ context }) => context.rejoinTries + 1,
                  }),
                },
              },
              on: {
                SOCKET_CLOSED: {
                  guard: "closesSocket",
                  target: "awaitingSocket",
                  actions: "socketClosed",
                },
              },
            },
          },
        },
        leaving: {
          tags: "leaving",
          entry: "lose",
          initial: "checkingSocket",
          states: {
            checkingSocket: {
              always: [
                { guard: "socketIsOpen", target: "awaitingReply" },
                {
                  target: "#channel.closed",
                  actions: ["leaveJoin", { type: "answerLeave", params: "ok" }],
                },
              ],
            },
            awaitingReply: {
              entry: "leaveJoin",
              invoke: {
                src: "sleep",
                input: ({ context }) => ({ ms: context.timeout }),
                onDone: {
                  target: "#channel.closed",
                  actions: { type: "answerLeave", params: "timeout" },
                },
              },
              on: {
                LEAVE_REPLY: {
                  guard: "answersLeave",
                  target: "#channel.closed",
                  actions: { type: "answerLeave", params: "ok" },
                },
                SOCKET_CLOSED: {
                  guard: "closesSocket",
                  target: "#channel.closed",
                  actions: ["socketClosed", { type: "answerLeave", params: "ok" }],
                },
              },
            },
          },
        },
      },
    },
    closed: {
      tags: "closed",
      type: "final",
      entry: emit(({ event }) => ({
        type: "closed" as const,
        reason:
          event.type === "LEAVE" || event.type === "LEAVE_REPLY"
            ? "leave"
            : "reason" in event
              ? event.reason
              : undefined,
      })),
    },
  },
})
