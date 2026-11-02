import { assign, enqueueActions, fromCallback, setup } from "xstate"
import { sleep } from "../actors"
import type { MessageStream, Position, StreamMessage } from "../stream"

type ReaderEvent =
  | { type: "MESSAGE"; message: StreamMessage }
  | { type: "MOVED"; epoch: string }
  | { type: "FAILED" }
  | { type: "STOP" }

export type ReaderEmitted = { type: "deliver"; message: StreamMessage }

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
  backoffMs(tries: number): number
  from: Position
  left: ReadonlyMap<string, number>
  tries: number
}

export const readerMachine = setup({
  types: {
    context: {} as ReaderContext,
    input: {} as Omit<ReaderContext, "tries" | "left">,
    events: {} as ReaderEvent,
    emitted: {} as ReaderEmitted,
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
        MESSAGE: {
          actions: enqueueActions(({ context, event, enqueue }) => {
            enqueue.emit({ type: "deliver", message: event.message })
            enqueue.assign({
              from: { ...context.from, after: event.message.number },
              tries: 0,
            })
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
