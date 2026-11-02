import { assign, emit, enqueueActions, setup } from "xstate"
import { backoff, type Data } from "../actors"
import { DURABLE_WINDOW } from "../constants"
import { isNotTaken } from "../wire"

export interface Sent {
  number: number
  ref: string
}

export interface Entry {
  id: string
  event: string
  payload: unknown
  sent: Sent | null
}

type LogEvent =
  | { type: "PUSH"; id: string; event: string; payload: unknown }
  | { type: "REPLY"; ref: string | null; status: string; response: unknown }
  | { type: "CHANNEL_JOINED" }
  | { type: "CHANNEL_LEFT" }
  | { type: "CHANNEL_CLOSED" }
  | { type: "SYNCED"; received: number }
  | { type: "SYNC_FAILED" }

export type LogEmitted =
  | { type: "sync" }
  | { type: "send"; event: string; payload: unknown; sent: Sent }
  | { type: "answer"; id: string; status: "ok" | "error" | "timeout"; response: unknown }

type LogContext = Data<{
  queue: ReadonlyArray<Entry>
  next: number
  syncs: number
  tries: number
}>

type ReplyEvent = Extract<LogEvent, { type: "REPLY" }>

const answered = (context: LogContext, event: ReplyEvent) =>
  context.queue.filter((entry) => entry.sent !== null && entry.sent.ref === event.ref)

const notTaken = (event: ReplyEvent) => event.status === "error" && isNotTaken(event.response)

function numberWaiting(
  queue: ReadonlyArray<Entry>,
  next: number,
  syncs: number,
): { queue: ReadonlyArray<Entry>; next: number; sending: ReadonlyArray<Entry & { sent: Sent }> } {
  const firstWaiting = queue.findIndex((entry) => entry.sent === null)
  if (firstWaiting === -1 || firstWaiting >= DURABLE_WINDOW) return { queue, next, sending: [] }
  const sending = queue.slice(firstWaiting, DURABLE_WINDOW).map((entry, index) => ({
    ...entry,
    sent: { number: next + index, ref: `d${syncs}.${next + index}` },
  }))
  return {
    queue: [
      ...queue.slice(0, firstWaiting),
      ...sending,
      ...queue.slice(firstWaiting + sending.length),
    ],
    next: next + sending.length,
    sending,
  }
}

export const logMachine = setup({
  types: {
    context: {} as LogContext,
    events: {} as LogEvent,
    emitted: {} as LogEmitted,
  },
  actors: { waitToSync: backoff(Number) },
  guards: {
    notTaken: ({ context, event }) =>
      event.type === "REPLY" && answered(context, event).length > 0 && notTaken(event),
  },
  actions: {
    enqueue: assign(({ context }, entry: Entry) => ({
      queue: [...context.queue, entry],
    })),
    sendWaiting: enqueueActions(({ context, enqueue }) => {
      const { queue, next, sending } = numberWaiting(context.queue, context.next, context.syncs)
      enqueue.assign({ queue, next })
      for (const { event, payload, sent } of sending)
        enqueue.emit({ type: "send", event, payload, sent })
    }),
    answer: enqueueActions(({ context, enqueue }, event: ReplyEvent) => {
      if (notTaken(event)) return
      for (const entry of answered(context, event))
        enqueue.emit({
          type: "answer",
          id: entry.id,
          status: event.status === "ok" ? "ok" : "error",
          response: event.response,
        })
      enqueue.assign({
        queue: context.queue.filter((entry) => entry.sent === null || entry.sent.ref !== event.ref),
      })
    }),
    sync: emit({ type: "sync" }),
    applySync: enqueueActions(({ context, enqueue }, received: number) => {
      const unsent = context.queue.map((entry) => ({ ...entry, sent: null }))
      const syncs = context.syncs + 1
      const { queue, next, sending } = numberWaiting(
        unsent,
        Math.min(context.queue[0]?.sent?.number ?? received + 1, received + 1),
        syncs,
      )
      enqueue.assign({ queue, next, syncs, tries: 0 })
      for (const { event, payload, sent } of sending)
        enqueue.emit({ type: "send", event, payload, sent })
    }),
    abandon: enqueueActions(({ context, enqueue }) => {
      for (const entry of context.queue)
        enqueue.emit({ type: "answer", id: entry.id, status: "timeout", response: undefined })
      enqueue.assign({ queue: [] })
    }),
  },
}).createMachine({
  id: "log",
  context: { queue: [], next: 0, syncs: 0, tries: 0 },
  initial: "away",
  on: {
    PUSH: {
      actions: {
        type: "enqueue",
        params: ({ event }) => ({ ...event, sent: null }),
      },
    },
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
        SYNC_FAILED: "backingOff",
      },
    },
    backingOff: {
      entry: assign({ tries: ({ context }) => context.tries + 1 }),
      invoke: {
        src: "waitToSync",
        input: ({ context }) => ({ tries: context.tries }),
        onDone: "syncing",
      },
    },
    synced: {
      on: {
        PUSH: {
          actions: [
            { type: "enqueue", params: ({ event }) => ({ ...event, sent: null }) },
            "sendWaiting",
          ],
        },
        REPLY: [
          { guard: "notTaken", target: "backingOff" },
          {
            actions: [{ type: "answer", params: ({ event }) => event }, "sendWaiting"],
          },
        ],
      },
    },
    closed: {
      on: {
        PUSH: {
          actions: emit(({ event }) => ({
            type: "answer" as const,
            id: event.id,
            status: "timeout" as const,
            response: undefined,
          })),
        },
      },
    },
  },
})
