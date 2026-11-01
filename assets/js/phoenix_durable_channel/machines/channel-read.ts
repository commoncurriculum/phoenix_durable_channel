import { assign, setup } from "xstate"
import type { Position, ReadCallbacks, StreamMessage } from "../stream"

type ChannelReadEvent =
  | { type: "MESSAGE"; message: StreamMessage }
  | { type: "READ_REPLY"; sent: number; epoch: string }
  | { type: "LOST" }

interface ChannelReadContext {
  from: Position
  callbacks: ReadCallbacks
  // Messages that arrived before the read's reply, by number.
  held: ReadonlyMap<number, StreamMessage>
  next: number
}

/**
 * One read through the channel. `phx_read` asks the server to send again what followed
 * `after`; it sends those, then replies with the last number it sent and the store's
 * epoch. Until the reply, messages are held: resends and new ones can arrive in any mix. At
 * the reply every number up to `sent` has arrived or is gone from the server, so what is
 * held is delivered in order, and from then on each message must be the next one. Messages
 * that arrive before the read starts are missed, and come again as resends. One that is not
 * the next means the server's numbers moved without this read, and the read ends, as it
 * does when the channel loses the server; the reader then reads again from the last
 * message delivered. A reply in another epoch means the store numbers its messages afresh,
 * so nothing held belongs to `after`: the read ends, and the next one reads that epoch.
 * The reply has no timeout: a server that holds it is still sending.
 */
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
      // Every message held is numbered after `after`: the server resends only those, and
      // pushes only new ones.
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
