import { assign, setup } from "xstate"
import type { Position, ReadCallbacks, StreamMessage } from "../stream"

type ChannelReadEvent =
  | { type: "MESSAGE"; message: StreamMessage }
  | { type: "READ_REPLY"; sent: number; epoch: string }
  | { type: "LOST" }

interface ChannelReadContext {
  from: Position
  callbacks: ReadCallbacks
  held: ReadonlyMap<number, StreamMessage>
  next: number
}

export const channelReadMachine = setup({
  types: {
    context: {} as ChannelReadContext,
    input: {} as { from: Position; callbacks: ReadCallbacks },
    events: {} as ChannelReadEvent,
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
    deliverHeld: assign(({ context }, sent: number) => {
      for (const message of [...context.held.values()].sort((a, b) => a.number - b.number))
        context.callbacks.deliver(message)
      return { held: new Map(), next: sent + 1 }
    }),
    moved: ({ context }, epoch: string) => context.callbacks.moved(epoch),
    deliver: assign(({ context }, message: StreamMessage) => {
      context.callbacks.deliver(message)
      return { next: message.number + 1 }
    }),
  },
}).createMachine({
  id: "channelRead",
  context: ({ input }) => ({ ...input, held: new Map(), next: 0 }),
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
      entry: ({ context }) => context.callbacks.failed("ended"),
    },
  },
})
