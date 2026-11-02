import { assertEvent, fromCallback, sendTo, setup } from "xstate"
import type { PublishedReader } from "../stream"

type Published = { type: "PUBLISHED"; reader: PublishedReader }

type FollowEvent = Published | { type: "STOP" }

export type Read = (latest: () => PublishedReader) => () => void

const readStream = fromCallback<Published, { read: Read; published: PublishedReader }>(
  ({ input, receive }) => {
    let latest = input.published
    receive((event) => {
      latest = event.reader
    })
    return input.read(() => latest)
  },
)

export const followMachine = setup({
  types: {
    context: {} as { read: Read },
    input: {} as { read: Read },
    events: {} as FollowEvent,
  },
  actors: { readStream },
}).createMachine({
  id: "follow",
  context: ({ input }) => ({ read: input.read }),
  initial: "waiting",
  on: { STOP: ".stopped" },
  states: {
    waiting: {
      on: { PUBLISHED: "reading" },
    },
    reading: {
      invoke: {
        id: "reader",
        src: "readStream",
        input: ({ context, event }) => {
          assertEvent(event, "PUBLISHED")
          return { read: context.read, published: event.reader }
        },
      },
      on: { PUBLISHED: { actions: sendTo("reader", ({ event }) => event) } },
    },
    stopped: { type: "final" },
  },
})
