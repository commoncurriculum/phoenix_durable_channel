import { assign, emit, setup } from "xstate"
import { backoff, sleep, type Data } from "../actors"
import type { ChannelState } from "../constants"

type ChannelEvent =
  | { type: "JOIN"; timeout: number }
  | { type: "JOIN_SENT"; ref: string }
  | { type: "LEAVE"; timeout: number }
  | { type: "LEAVE_SENT"; ref: string }
  | { type: "SOCKET_OPEN" }
  | { type: "SOCKET_CLOSED" }
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
  socketOpen: boolean
  joinRef: string | null
  leaveRef: string | null
  rejoinTries: number
  joinResponse: unknown
}>

export const channelMachine = setup({
  types: {
    context: {} as ChannelContext,
    input: {} as { timeout: number; socketOpen: boolean },
    events: {} as ChannelEvent,
    emitted: {} as ChannelEmitted,
    tags: {} as ChannelState,
  },
  actors: { sleep, waitToRejoin: backoff(Number) },
  guards: {
    socketIsOpen: ({ context }) => context.socketOpen,
    answersJoin: ({ context, event }) =>
      (event.type === "JOIN_REPLY" || event.type === "JOIN_REFUSED") &&
      event.ref === context.joinRef,
    acceptsJoin: ({ context, event }) =>
      event.type === "JOIN_REPLY" && event.ref === context.joinRef && event.status === "ok",
    answersLeave: ({ context, event }) =>
      event.type === "LEAVE_REPLY" && event.ref === context.leaveRef,
  },
  actions: {
    socketOpened: assign({ socketOpen: true }),
    socketClosed: assign({ socketOpen: false }),
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
    socketOpen: input.socketOpen,
    joinRef: null,
    leaveRef: null,
    rejoinTries: 0,
    joinResponse: undefined,
  }),
  initial: "unjoined",
  on: {
    REPLACED: { target: ".closed" },
    SOCKET_OPEN: { actions: "socketOpened" },
    SOCKET_CLOSED: { actions: "socketClosed" },
    JOIN_SENT: { actions: assign({ joinRef: ({ event }) => event.ref }) },
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
              on: { SOCKET_OPEN: { target: "awaitingReply", actions: "socketOpened" } },
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
            SOCKET_CLOSED: { target: "errored", actions: ["socketClosed", "error"] },
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
              actions: ["socketOpened", assign({ rejoinTries: 0 })],
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
              on: { SOCKET_CLOSED: { target: "awaitingSocket", actions: "socketClosed" } },
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
                { target: "#channel.closed", actions: { type: "answerLeave", params: "ok" } },
              ],
            },
            awaitingReply: {
              entry: emit(({ context }) => ({ type: "leave" as const, joinRef: context.joinRef })),
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
