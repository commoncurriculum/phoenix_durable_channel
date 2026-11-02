import { assign, emit, setup } from "xstate"
import { sleep } from "../actors"
import type { ChannelState } from "../constants"

export interface ChannelSettings {
  timeout(): number
  socketIsOpen(): boolean
  makeRef(): string
  rejoinAfterMs(tries: number): number
}

type ChannelEvent =
  | { type: "JOIN" }
  | { type: "LEAVE" }
  | { type: "SOCKET_OPEN" }
  | { type: "SOCKET_CLOSED" }
  | { type: "JOIN_REPLY"; status: string; response: unknown }
  | { type: "JOIN_REFUSED"; response: unknown }
  | { type: "LEAVE_REPLY" }
  | { type: "SERVER_ERROR"; reason: unknown }
  | { type: "SERVER_CLOSE"; reason: unknown }
  | { type: "REPLACED" }

export type ChannelEmitted =
  | { type: "join"; joinRef: string | null }
  | { type: "leave"; joinRef: string | null; leaveRef: string | null }
  | { type: "answerJoin"; status: "ok" | "error" | "timeout"; response: unknown }
  | { type: "answerLeave"; status: "ok" | "timeout" }
  | { type: "joined"; response: unknown }
  | { type: "lost" }
  | { type: "errored"; reason: unknown }
  | { type: "closed"; reason: unknown }

interface ChannelContext {
  settings: ChannelSettings
  joinRef: string | null
  leaveRef: string | null
  rejoinTries: number
  joinResponse: unknown
}

export const channelMachine = setup({
  types: {
    context: {} as ChannelContext,
    input: {} as { settings: ChannelSettings },
    events: {} as ChannelEvent,
    emitted: {} as ChannelEmitted,
    tags: {} as ChannelState,
  },
  actors: { sleep },
  guards: {
    socketIsOpen: ({ context }) => context.settings.socketIsOpen(),
    replyIsOk: ({ event }) => event.type === "JOIN_REPLY" && event.status === "ok",
  },
  actions: {
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
    leaveJoin: emit(({ context }) => ({
      type: "leave" as const,
      joinRef: context.joinRef,
      leaveRef: context.settings.makeRef(),
    })),
  },
}).createMachine({
  id: "channel",
  context: ({ input }) => ({
    settings: input.settings,
    joinRef: null,
    leaveRef: null,
    rejoinTries: 0,
    joinResponse: undefined,
  }),
  initial: "unjoined",
  on: {
    REPLACED: { target: ".closed" },
  },
  states: {
    unjoined: {
      tags: "closed",
      on: {
        JOIN: "member",
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
            LEAVE: "leaving",
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
              on: { SOCKET_OPEN: "awaitingReply" },
            },
            awaitingReply: {
              entry: [
                assign({ joinRef: ({ context }) => context.settings.makeRef() }),
                emit(({ context }) => ({ type: "join" as const, joinRef: context.joinRef })),
              ],
              invoke: {
                src: "sleep",
                input: ({ context }) => ({ ms: context.settings.timeout() }),
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
                    guard: "replyIsOk",
                    target: "#channel.member.joined",
                    actions: assign({ joinResponse: ({ event }) => event.response }),
                  },
                  {
                    target: "#channel.member.errored",
                    actions: {
                      type: "answerJoin",
                      params: ({ event }) => ({ status: "error", response: event.response }),
                    },
                  },
                ],
                SOCKET_CLOSED: {
                  target: "#channel.member.errored",
                  actions: "error",
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
            LEAVE: "leaving",
            SOCKET_CLOSED: { target: "errored", actions: "error" },
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
              actions: assign({ rejoinTries: 0 }),
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
                src: "sleep",
                input: ({ context }) => ({
                  ms: context.settings.rejoinAfterMs(context.rejoinTries + 1),
                }),
                onDone: {
                  target: "#channel.member.joining",
                  actions: assign({
                    rejoinTries: ({ context }) => context.rejoinTries + 1,
                  }),
                },
              },
              on: { SOCKET_CLOSED: "awaitingSocket" },
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
              entry: [
                assign({ leaveRef: ({ context }) => context.settings.makeRef() }),
                emit(({ context }) => ({
                  type: "leave" as const,
                  joinRef: context.joinRef,
                  leaveRef: context.leaveRef,
                })),
              ],
              invoke: {
                src: "sleep",
                input: ({ context }) => ({ ms: context.settings.timeout() }),
                onDone: {
                  target: "#channel.closed",
                  actions: { type: "answerLeave", params: "timeout" },
                },
              },
              on: {
                LEAVE_REPLY: {
                  target: "#channel.closed",
                  actions: { type: "answerLeave", params: "ok" },
                },
                SOCKET_CLOSED: {
                  target: "#channel.closed",
                  actions: { type: "answerLeave", params: "ok" },
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
