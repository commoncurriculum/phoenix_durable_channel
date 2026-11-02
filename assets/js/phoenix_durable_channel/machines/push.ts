import { assign, emit, setup } from "xstate"
import { sleep } from "../actors"

export type ReplyStatus = "ok" | "error" | "timeout"
export type PushState = "buffered" | "sent" | ReplyStatus

type PushEvent =
  | { type: "SEND"; ref: string }
  | { type: "REPLY"; status: string; response: unknown }
  | { type: "CONNECTION_LOST" }
  | { type: "CHANNEL_CLOSED" }

export type PushEmitted = { type: "finished"; status: ReplyStatus; response: unknown }

export const pushMachine = setup({
  types: {
    context: {} as { timeout: number; ref: string | null; response: unknown },
    input: {} as { timeout: number },
    events: {} as PushEvent,
    emitted: {} as PushEmitted,
    tags: {} as PushState,
  },
  actors: { sleep },
  actions: {
    finish: emit(({ context }, status: ReplyStatus) => ({
      type: "finished" as const,
      status,
      response: context.response,
    })),
  },
}).createMachine({
  id: "push",
  context: ({ input }) => ({
    timeout: input.timeout,
    ref: null,
    response: undefined,
  }),
  initial: "pending",
  states: {
    pending: {
      initial: "buffered",
      on: { CHANNEL_CLOSED: "timeout" },
      states: {
        buffered: {
          tags: "buffered",
          invoke: {
            src: "sleep",
            input: ({ context }) => ({ ms: context.timeout }),
            onDone: "#push.timeout",
          },
          on: {
            SEND: {
              target: "sent",
              actions: assign({ ref: ({ event }) => event.ref }),
            },
          },
        },
        sent: {
          tags: "sent",
          invoke: {
            src: "sleep",
            input: ({ context }) => ({ ms: context.timeout }),
            onDone: "#push.timeout",
          },
          on: {
            REPLY: [
              {
                guard: ({ event }) => event.status === "ok",
                target: "#push.ok",
                actions: assign({ response: ({ event }) => event.response }),
              },
              {
                target: "#push.error",
                actions: assign({ response: ({ event }) => event.response }),
              },
            ],
            CONNECTION_LOST: "#push.timeout",
          },
        },
      },
    },
    ok: { type: "final", tags: "ok", entry: { type: "finish", params: "ok" } },
    error: { type: "final", tags: "error", entry: { type: "finish", params: "error" } },
    timeout: { type: "final", tags: "timeout", entry: { type: "finish", params: "timeout" } },
  },
})
