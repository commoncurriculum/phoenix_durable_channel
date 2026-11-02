import { assign, enqueueActions, fromCallback, setup } from "xstate"
import { backoff, ignore, type Data } from "../actors"
import type { Position, PublishedReader, StreamMessage } from "../stream"

export type ReaderEvent =
  | { type: "PUBLISHED"; reader: PublishedReader }
  | { type: "MESSAGE"; message: StreamMessage }
  | { type: "MOVED"; epoch: string }
  | { type: "FAILED" }
  | { type: "STOP" }

export type ReaderEmitted = { type: "deliver"; message: StreamMessage }

export type ReadingInput = { published: PublishedReader; from: Position }

type ReaderContext = Data<{
  after: number | null
  published: PublishedReader | null
  from: Position
  left: ReadonlyMap<string, number>
  tries: number
}>

export const readerMachine = setup({
  types: {
    context: {} as ReaderContext,
    input: {} as { after: number | null },
    events: {} as ReaderEvent,
    emitted: {} as ReaderEmitted,
  },
  actors: {
    reading: fromCallback<ReaderEvent, ReadingInput, ReaderEvent>(ignore),
    waitToRead: backoff(Number),
  },
  guards: {
    isNext: ({ context, event }) =>
      event.type === "MESSAGE" && event.message.number > context.from.after,
  },
}).createMachine({
  id: "reader",
  context: ({ input }) => ({
    after: input.after,
    published: null,
    from: { after: 0, epoch: "" },
    left: new Map(),
    tries: 0,
  }),
  initial: "waiting",
  on: {
    PUBLISHED: { actions: assign({ published: ({ event }) => event.reader }) },
    STOP: ".stopped",
  },
  states: {
    waiting: {
      on: {
        PUBLISHED: {
          target: "reading",
          actions: assign(({ context, event }) => ({
            published: event.reader,
            from: { after: context.after ?? event.reader.last, epoch: event.reader.epoch ?? "" },
          })),
        },
      },
    },
    reading: {
      invoke: {
        src: "reading",
        input: ({ context }) => ({ published: context.published!, from: context.from }),
      },
      on: {
        MESSAGE: {
          guard: "isNext",
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
        src: "waitToRead",
        input: ({ context }) => ({ tries: context.tries }),
        onDone: "reading",
      },
    },
    stopped: { type: "final" },
  },
})
