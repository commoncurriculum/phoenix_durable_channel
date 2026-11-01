import { assign, fromCallback, setup } from "xstate"
import { sleep } from "../actors"
import type { MessageStream, Position, StreamMessage } from "../stream"

type ReaderEvent =
  | { type: "MESSAGE"; message: StreamMessage }
  | { type: "MOVED"; epoch: string }
  | { type: "FAILED" }
  | { type: "STOP" }

/** One read, alive exactly as long as the state that invoked it. */
const reading = fromCallback<ReaderEvent, { stream: MessageStream; from: Position }, ReaderEvent>(
  ({ input, sendBack }) =>
    input.stream.read(input.from, {
      deliver: (message) => sendBack({ type: "MESSAGE", message }),
      moved: (epoch) => sendBack({ type: "MOVED", epoch }),
      failed: () => sendBack({ type: "FAILED" }),
    }),
)

export interface ReaderContext {
  stream: MessageStream
  deliver(message: StreamMessage): void
  backoffMs(tries: number): number
  from: Position
  // Where the reader got to in each epoch it has left, so going back to one reads on.
  left: ReadonlyMap<string, number>
  tries: number
}

/**
 * Follows a stream: delivers each message once and in order, and after a read ends
 * reads again, after a backoff, from the last message delivered. When the store's epoch
 * changes it reads the new one from where it got to in it before, or from the start.
 */
export const readerMachine = setup({
  types: {
    context: {} as ReaderContext,
    input: {} as Omit<ReaderContext, "tries" | "left">,
    events: {} as ReaderEvent,
  },
  actors: { reading, sleep },
}).createMachine({
  id: "reader",
  context: ({ input }) => ({ ...input, left: new Map(), tries: 0 }),
  initial: "reading",
  on: { STOP: ".stopped" },
  states: {
    reading: {
      invoke: {
        src: "reading",
        input: ({ context }) => ({
          stream: context.stream,
          from: context.from,
        }),
      },
      on: {
        // A read delivers in order from `from`, and a stopped read sends nothing more.
        MESSAGE: {
          actions: assign(({ context, event }) => {
            context.deliver(event.message)
            return {
              from: { ...context.from, after: event.message.number },
              tries: 0,
            }
          }),
        },
        MOVED: {
          actions: assign(({ context, event }) => ({
            left: new Map(context.left).set(context.from.epoch, context.from.after),
            from: {
              after: context.left.get(event.epoch) ?? 0,
              epoch: event.epoch,
            },
          })),
        },
        FAILED: "backingOff",
      },
    },
    backingOff: {
      entry: assign({ tries: ({ context }) => context.tries + 1 }),
      invoke: {
        src: "sleep",
        input: ({ context }) => ({ ms: context.backoffMs(context.tries) }),
        onDone: "reading",
      },
    },
    stopped: { type: "final" },
  },
})
