import { assign, setup } from "xstate"
import { sleep } from "../actors"

type PushEvent =
  | { type: "SEND"; ref: string }
  | { type: "REPLY"; status: string; response: unknown }
  | { type: "CONNECTION_LOST" }
  | { type: "CHANNEL_CLOSED" }

/**
 * One push and its one answer. It waits in `buffered` until its channel can send it,
 * then in `sent` until the reply. It ends in exactly one of `ok`, `error` or `timeout`.
 * `timeout` means "no reply will come": the timer ran out, or the connection the push
 * went out on is gone. The server may or may not have handled it.
 */
export const pushMachine = setup({
  types: {
    context: {} as { timeout: number; ref: string | null; response: unknown },
    input: {} as { timeout: number },
    events: {} as PushEvent,
  },
  actors: { sleep },
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
        // The timer starts again on send, as in phoenix.js.
        sent: {
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
    ok: { type: "final" },
    error: { type: "final" },
    timeout: { type: "final" },
  },
})
