import { assign, emit, enqueueActions, setup } from "xstate"
import { sleep } from "../actors"
import { DURABLE_WINDOW } from "../constants"
import type { Reply } from "../reply"
import { isNotTaken } from "../wire"

export interface Entry {
  event: string
  payload: unknown
  reply: Reply
  number: number | null
  ref: string | null
}

export type SentEntry = Entry & { number: number; ref: string }

type LogEvent =
  | { type: "PUSH"; entry: Entry }
  | { type: "REPLY"; ref: string | null; status: string; response: unknown }
  | { type: "CHANNEL_JOINED" }
  | { type: "CHANNEL_LEFT" }
  | { type: "CHANNEL_CLOSED" }
  | { type: "SYNCED"; received: number }
  | { type: "SYNC_FAILED" }

export type LogEmitted =
  | { type: "sync" }
  | { type: "send"; entry: SentEntry }
  | { type: "answer"; entry: Entry; status: "ok" | "error" | "timeout"; response: unknown }

export interface LogSettings {
  channelJoined(): boolean
  backoffMs(tries: number): number
  makeRef(): string
}

export interface LogContext {
  settings: LogSettings
  queue: Array<Entry>
  next: number
  tries: number
}

type ReplyEvent = Extract<LogEvent, { type: "REPLY" }>

const isOurs = (context: LogContext, event: ReplyEvent) =>
  context.queue.some((entry) => entry.ref === event.ref)

const notTaken = (event: ReplyEvent) => event.status === "error" && isNotTaken(event.response)

function numberWaiting(
  queue: Array<Entry>,
  next: number,
  makeRef: () => string,
): { queue: Array<Entry>; next: number; sending: Array<SentEntry> } {
  const inFlight = queue.findIndex((entry) => entry.ref === null)
  if (inFlight === -1 || inFlight >= DURABLE_WINDOW) return { queue, next, sending: [] }
  const sending = queue.slice(inFlight, DURABLE_WINDOW).map((entry, index) => ({
    ...entry,
    number: next + index,
    ref: makeRef(),
  }))
  return {
    queue: [...queue.slice(0, inFlight), ...sending, ...queue.slice(inFlight + sending.length)],
    next: next + sending.length,
    sending,
  }
}

export const logMachine = setup({
  types: {
    context: {} as LogContext,
    input: {} as { settings: LogSettings },
    events: {} as LogEvent,
    emitted: {} as LogEmitted,
  },
  actors: { sleep },
  guards: {
    channelJoined: ({ context }) => context.settings.channelJoined(),
    notTaken: ({ context, event }) =>
      event.type === "REPLY" && isOurs(context, event) && notTaken(event),
  },
  actions: {
    enqueue: assign(({ context }, entry: Entry) => ({
      queue: [...context.queue, entry],
    })),
    sendWaiting: enqueueActions(({ context, enqueue }) => {
      const { queue, next, sending } = numberWaiting(
        context.queue,
        context.next,
        context.settings.makeRef,
      )
      enqueue.assign({ queue, next })
      for (const entry of sending) enqueue.emit({ type: "send", entry })
    }),
    answer: enqueueActions(({ context, enqueue }, event: ReplyEvent) => {
      if (notTaken(event)) return
      for (const entry of context.queue.filter((entry) => entry.ref === event.ref))
        enqueue.emit({
          type: "answer",
          entry,
          status: event.status === "ok" ? "ok" : "error",
          response: event.response,
        })
      enqueue.assign({ queue: context.queue.filter((entry) => entry.ref !== event.ref) })
    }),
    sync: emit({ type: "sync" }),
    applySync: enqueueActions(({ context, enqueue }, received: number) => {
      const unsent = context.queue.map((entry) => ({ ...entry, number: null, ref: null }))
      const { queue, next, sending } = numberWaiting(
        unsent,
        Math.min(context.queue[0]?.number ?? received + 1, received + 1),
        context.settings.makeRef,
      )
      enqueue.assign({ queue, next, tries: 0 })
      for (const entry of sending) enqueue.emit({ type: "send", entry })
    }),
    abandon: enqueueActions(({ context, enqueue }) => {
      for (const entry of context.queue)
        enqueue.emit({ type: "answer", entry, status: "timeout", response: undefined })
    }),
  },
}).createMachine({
  id: "log",
  context: ({ input }) => ({
    settings: input.settings,
    queue: [],
    next: 0,
    tries: 0,
  }),
  initial: "away",
  on: {
    PUSH: { actions: { type: "enqueue", params: ({ event }) => event.entry } },
    REPLY: { actions: { type: "answer", params: ({ event }) => event } },
    CHANNEL_LEFT: ".away",
    CHANNEL_CLOSED: { target: ".closed", actions: "abandon" },
  },
  states: {
    away: {
      on: { CHANNEL_JOINED: "syncing" },
    },
    syncing: {
      entry: "sync",
      on: {
        SYNCED: {
          target: "synced",
          actions: { type: "applySync", params: ({ event }) => event.received },
        },
        SYNC_FAILED: [{ guard: "channelJoined", target: "backingOff" }, { target: "away" }],
      },
    },
    backingOff: {
      entry: assign({ tries: ({ context }) => context.tries + 1 }),
      invoke: {
        src: "sleep",
        input: ({ context }) => ({
          ms: context.settings.backoffMs(context.tries),
        }),
        onDone: "syncing",
      },
    },
    synced: {
      on: {
        PUSH: {
          actions: [{ type: "enqueue", params: ({ event }) => event.entry }, "sendWaiting"],
        },
        REPLY: [
          { guard: "notTaken", target: "backingOff" },
          {
            actions: [{ type: "answer", params: ({ event }) => event }, "sendWaiting"],
          },
        ],
      },
    },
    closed: { type: "final" },
  },
})
