import { assign, emit, enqueueActions, setup } from "xstate"
import type { Data } from "../actors"
import type { Position, StreamMessage } from "../stream"

type ChannelReadEvent =
  | { type: "MESSAGE"; message: StreamMessage }
  | { type: "READ_REPLY"; sent: number; epoch: string }
  | { type: "LOST" }

export type ChannelReadEmitted =
  | { type: "deliver"; message: StreamMessage }
  | { type: "moved"; epoch: string }
  | { type: "failed" }

type ChannelReadContext = Data<{
  from: Position
  held: ReadonlyMap<number, StreamMessage>
  next: number
}>

export const channelReadMachine = setup({
  types: {
    context: {} as ChannelReadContext,
    input: {} as { from: Position },
    events: {} as ChannelReadEvent,
    emitted: {} as ChannelReadEmitted,
  },
  guards: {
    isNext: ({ context, event }) =>
      event.type === "MESSAGE" && event.message.number === context.next,
    otherEpoch: ({ context }, epoch: string) => epoch !== context.from.epoch,
  },
  actions: {
    hold: assign(({ context }, message: StreamMessage) => ({
      held: new Map(context.held).set(message.number, message),
    })),
    deliverHeld: enqueueActions(({ context, enqueue }, sent: number) => {
      for (const message of [...context.held.values()].sort((a, b) => a.number - b.number))
        enqueue.emit({ type: "deliver", message })
      enqueue.assign({ held: new Map(), next: sent + 1 })
    }),
    moved: emit((_, epoch: string) => ({ type: "moved" as const, epoch })),
    deliver: enqueueActions(({ enqueue }, message: StreamMessage) => {
      enqueue.emit({ type: "deliver", message })
      enqueue.assign({ next: message.number + 1 })
    }),
  },
}).createMachine({
  id: "channelRead",
  context: ({ input }) => ({ from: input.from, held: new Map(), next: 0 }),
  initial: "waiting",
  on: { LOST: ".ended" },
  states: {
    waiting: {
      on: {
        MESSAGE: {
          actions: { type: "hold", params: ({ event }) => event.message },
        },
        READ_REPLY: [
          {
            guard: { type: "otherEpoch", params: ({ event }) => event.epoch },
            target: "ended",
            actions: { type: "moved", params: ({ event }) => event.epoch },
          },
          {
            target: "live",
            actions: { type: "deliverHeld", params: ({ event }) => event.sent },
          },
        ],
      },
    },
    live: {
      on: {
        MESSAGE: [
          {
            guard: "isNext",
            actions: { type: "deliver", params: ({ event }) => event.message },
          },
          { target: "ended" },
        ],
      },
    },
    ended: {
      type: "final",
      entry: emit({ type: "failed" }),
    },
  },
})
