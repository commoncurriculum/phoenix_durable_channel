import { and, assign, enqueueActions, fromCallback, setup } from "xstate"
import { backoff, ignore, type Data } from "../actors"
import type { Position, PublishedReader, Resume, StreamMessage } from "../stream"

export type ReaderEvent =
  | { type: "PUBLISHED"; reader: PublishedReader }
  | { type: "MESSAGE"; message: StreamMessage }
  | { type: "PASSED"; number: number }
  | { type: "REACHED"; resume: Resume }
  | { type: "FAILED" }
  | { type: "READ_REPLY"; read: number; sent: number; epoch: string }
  | { type: "READ_REFUSED"; read: number }
  | { type: "CHANNEL_JOINED" }
  | { type: "CHANNEL_LEFT" }
  | { type: "STOP" }

export type ReaderEmitted =
  | { type: "deliver"; message: StreamMessage }
  | { type: "skipped"; after: number; next: number }
  | { type: "read"; read: number; after: number }

export type ReadingInput = { published: PublishedReader; from: Position }

type ReaderContext = Data<{
  after: number | null
  published: PublishedReader
  from: Position
  left: ReadonlyMap<string, number>
  held: ReadonlyMap<number, StreamMessage>
  reads: number
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
    isNextInLine: ({ context, event }) =>
      event.type === "MESSAGE" && event.message.number === context.from.after + 1,
    isThisRead: ({ context, event }) =>
      (event.type === "READ_REPLY" || event.type === "READ_REFUSED") &&
      event.read === context.reads,
    otherEpoch: ({ context, event }) =>
      event.type === "READ_REPLY" && event.epoch !== context.from.epoch,
    readsThroughChannel: ({ context }) => context.published.type === "phoenix",
  },
  actions: {
    advance: enqueueActions(
      (
        { context, enqueue },
        { next, after, message }: { next: number; after: number; message: StreamMessage | null },
      ) => {
        if (next > context.from.after + 1)
          enqueue.emit({ type: "skipped", after: context.from.after, next })
        if (message) enqueue.emit({ type: "deliver", message })
        enqueue.assign({
          from: { ...context.from, after: Math.max(context.from.after, after) },
          tries: message ? 0 : context.tries,
        })
      },
    ),
  },
}).createMachine({
  id: "reader",
  context: ({ input }) => ({
    after: input.after,
    published: { type: "unpublished", last: 0 },
    from: { after: 0, epoch: "", resume: null },
    left: new Map(),
    held: new Map(),
    reads: 0,
    tries: 0,
  }),
  initial: "unpublished",
  on: {
    PUBLISHED: { actions: assign({ published: ({ event }) => event.reader }) },
    STOP: ".stopped",
  },
  states: {
    unpublished: {
      on: {
        PUBLISHED: {
          target: "joined",
          actions: assign(({ context, event }) => ({
            published: event.reader,
            from: {
              after: context.after ?? event.reader.last,
              epoch: event.reader.epoch ?? "",
              resume: null,
            },
          })),
        },
      },
    },
    joined: {
      initial: "reading",
      on: { CHANNEL_LEFT: { guard: "readsThroughChannel", target: "away" } },
      states: {
        reading: {
          always: [{ guard: "readsThroughChannel", target: "holding" }, { target: "streaming" }],
        },
        streaming: {
          invoke: {
            src: "reading",
            input: ({ context }) => ({ published: context.published, from: context.from }),
          },
          on: {
            MESSAGE: {
              guard: "isNext",
              actions: {
                type: "advance",
                params: ({ event }) => ({
                  next: event.message.number,
                  after: event.message.number,
                  message: event.message,
                }),
              },
            },
            PASSED: {
              actions: {
                type: "advance",
                params: ({ event }) => ({ next: event.number, after: event.number, message: null }),
              },
            },
            REACHED: {
              actions: assign({
                from: ({ context, event }) => ({ ...context.from, resume: event.resume }),
              }),
            },
            FAILED: "backingOff",
          },
        },
        holding: {
          entry: enqueueActions(({ context, enqueue }) => {
            enqueue.assign({ reads: context.reads + 1, held: new Map() })
            enqueue.emit({ type: "read", read: context.reads + 1, after: context.from.after })
          }),
          on: {
            MESSAGE: {
              actions: assign({
                held: ({ context, event }) =>
                  new Map(context.held).set(event.message.number, event.message),
              }),
            },
            READ_REPLY: [
              {
                guard: and(["isThisRead", "otherEpoch"]),
                target: "backingOff",
                actions: assign(({ context, event }) => ({
                  left: new Map(context.left).set(context.from.epoch, context.from.after),
                  from: {
                    after: context.left.get(event.epoch) ?? 0,
                    epoch: event.epoch,
                    resume: null,
                  },
                })),
              },
              {
                guard: "isThisRead",
                target: "live",
                actions: enqueueActions(({ context, event, enqueue }) => {
                  const unseen = [...context.held.values()].filter(
                    (message) => message.number > context.from.after,
                  )
                  for (const message of unseen.sort((a, b) => a.number - b.number))
                    enqueue({
                      type: "advance",
                      params: { next: message.number, after: message.number, message },
                    })
                  enqueue({
                    type: "advance",
                    params: { next: event.sent + 1, after: event.sent, message: null },
                  })
                  enqueue.assign({ held: new Map() })
                }),
              },
            ],
            READ_REFUSED: { guard: "isThisRead", target: "backingOff" },
          },
        },
        live: {
          on: {
            MESSAGE: [
              {
                guard: "isNextInLine",
                actions: {
                  type: "advance",
                  params: ({ event }) => ({
                    next: event.message.number,
                    after: event.message.number,
                    message: event.message,
                  }),
                },
              },
              { guard: "isNext", target: "backingOff" },
            ],
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
