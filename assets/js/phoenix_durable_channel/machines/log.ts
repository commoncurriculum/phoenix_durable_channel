import { and, assign, emit, enqueueActions, not, setup } from "xstate"
import { backoff, type Data } from "../actors"
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
  | { type: "ANSWERED"; number: number; status: string; response: unknown }
  | { type: "CHANNEL_JOINED" }
  | { type: "CHANNEL_LEFT" }
  | { type: "CHANNEL_CLOSED" }
  | { type: "SYNCED"; received: number; last: number }
  | { type: "READ"; after: number }
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
  answeredBy: { received: number; last: number }
}>

function numberWaiting(
  queue: ReadonlyArray<Entry>,
  next: number,
  syncs: number,
): { queue: ReadonlyArray<Entry>; next: number; sending: ReadonlyArray<Entry & { sent: Sent }> } {
  const firstWaiting = queue.findIndex((entry) => entry.sent === null)
  if (firstWaiting === -1) return { queue, next, sending: [] }
  const sending = queue.slice(firstWaiting).map((entry, index) => ({
    ...entry,
    sent: { number: next + index, ref: `d${syncs}.${next + index}` },
  }))
  return {
    queue: [...queue.slice(0, firstWaiting), ...sending],
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
    isNotTaken: ({ event }) =>
      event.type === "REPLY" && event.status === "error" && isNotTaken(event.response),
    isQueued: ({ context, event }) =>
      event.type === "REPLY" && context.queue.some((entry) => entry.sent?.ref === event.ref),
    isReadPastAnswers: ({ context, event }) =>
      event.type === "READ" && event.after >= context.answeredBy.last,
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
    answer: enqueueActions(
      ({ context, enqueue }, event: Extract<LogEvent, { type: "ANSWERED" }>) => {
        for (const entry of context.queue.filter((entry) => entry.sent?.number === event.number))
          enqueue.emit({
            type: "answer",
            id: entry.id,
            status: event.status === "ok" ? "ok" : "error",
            response: event.response,
          })
        enqueue.assign({
          queue: context.queue.filter((entry) => entry.sent?.number !== event.number),
        })
      },
    ),
    lose: enqueueActions(({ context, enqueue }, below: number) => {
      const earlier = (entry: Entry) => entry.sent !== null && entry.sent.number < below
      for (const entry of context.queue.filter(earlier))
        enqueue.emit({
          type: "answer",
          id: entry.id,
          status: "error",
          response: { reason: "reply_lost" },
        })
      enqueue.assign({ queue: context.queue.filter((entry) => !earlier(entry)) })
    }),
    sync: emit({ type: "sync" }),
    applySync: enqueueActions(({ context, enqueue }, received: number) => {
      const handled = context.queue.findIndex(
        (entry) => entry.sent === null || entry.sent.number > received,
      )
      const kept = handled === -1 ? context.queue.length : handled
      const syncs = context.syncs + 1
      const { queue, next, sending } = numberWaiting(
        [
          ...context.queue.slice(0, kept),
          ...context.queue.slice(kept).map((entry) => ({ ...entry, sent: null })),
        ],
        received + 1,
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
  context: {
    queue: [],
    next: 0,
    syncs: 0,
    tries: 0,
    answeredBy: { received: 0, last: Infinity },
  },
  initial: "away",
  on: {
    PUSH: {
      actions: {
        type: "enqueue",
        params: ({ event }) => ({ ...event, sent: null }),
      },
    },
    ANSWERED: { actions: { type: "answer", params: ({ event }) => event } },
    READ: {
      guard: "isReadPastAnswers",
      actions: [
        { type: "lose", params: ({ context }) => context.answeredBy.received + 1 },
        assign({ answeredBy: { received: 0, last: Infinity } }),
      ],
    },
    REPLY: {
      guard: not("isNotTaken"),
      actions: {
        type: "answer",
        params: ({ context, event }) => ({
          type: "ANSWERED" as const,
          number: context.queue.find((entry) => entry.sent?.ref === event.ref)?.sent?.number ?? 0,
          status: event.status,
          response: event.response,
        }),
      },
    },
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
          actions: [
            { type: "applySync", params: ({ event }) => event.received },
            assign({ answeredBy: ({ event }) => ({ received: event.received, last: event.last }) }),
          ],
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
        REPLY: { guard: and(["isNotTaken", "isQueued"]), target: "backingOff" },
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
