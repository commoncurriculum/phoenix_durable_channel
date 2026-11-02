import { assign, enqueueActions, fromCallback, setup } from "xstate"
import { backoff, ignore, type Data } from "../actors"
import type { Position, PublishedReader, Resume, StreamMessage } from "../stream"

export type ReaderEvent =
  | { type: "PUBLISHED"; reader: PublishedReader }
  | { type: "MESSAGE"; message: StreamMessage }
  | { type: "MOVED"; epoch: string }
  | { type: "REACHED"; resume: Resume }
  | { type: "FAILED" }
  | { type: "CHANNEL_JOINED" }
  | { type: "CHANNEL_LEFT" }
  | { type: "STOP" }

export type ReaderEmitted =
  { type: "deliver"; message: StreamMessage } | { type: "skipped"; after: number; next: number }

export type ReadingInput = { published: PublishedReader; from: Position }

type ReaderContext = Data<{
  published: PublishedReader
  from: Position
  left: ReadonlyMap<string, number>
  tries: number
}>

export const readerMachine = setup({
  types: {
    context: {} as ReaderContext,
    input: {} as { published: PublishedReader; after: number | null },
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
    readsThroughChannel: ({ context }) => context.published.type === "phoenix",
  },
}).createMachine({
  id: "reader",
  context: ({ input }) => ({
    published: input.published,
    from: {
      after: input.after ?? input.published.last,
      epoch: input.published.epoch ?? "",
      resume: null,
    },
    left: new Map(),
    tries: 0,
  }),
  initial: "joined",
  on: {
    PUBLISHED: { actions: assign({ published: ({ event }) => event.reader }) },
    STOP: ".stopped",
  },
  states: {
    joined: {
      initial: "reading",
      on: { CHANNEL_LEFT: { guard: "readsThroughChannel", target: "away" } },
      states: {
        reading: {
          invoke: {
            src: "reading",
            input: ({ context }) => ({ published: context.published, from: context.from }),
          },
          on: {
            MESSAGE: {
              guard: "isNext",
              actions: enqueueActions(({ context, event, enqueue }) => {
                if (event.message.number > context.from.after + 1)
                  enqueue.emit({
                    type: "skipped",
                    after: context.from.after,
                    next: event.message.number,
                  })
                enqueue.emit({ type: "deliver", message: event.message })
                enqueue.assign({
                  from: { ...context.from, after: event.message.number },
                  tries: 0,
                })
              }),
            },
            REACHED: {
              actions: assign({
                from: ({ context, event }) => ({ ...context.from, resume: event.resume }),
              }),
            },
            MOVED: {
              actions: assign(({ context, event }) => ({
                left: new Map(context.left).set(context.from.epoch, context.from.after),
                from: {
                  after: context.left.get(event.epoch) ?? 0,
                  epoch: event.epoch,
                  resume: null,
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
      },
    },
    away: {
      on: { CHANNEL_JOINED: "joined" },
    },
    stopped: { type: "final" },
  },
})
