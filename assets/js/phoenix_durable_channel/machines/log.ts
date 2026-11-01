import { assign, setup } from "xstate"
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

export interface LogHooks {
  channelJoined(): boolean
  backoffMs(tries: number): number
  makeRef(): string
  sync(): void
  send(entry: SentEntry): void
  answer(entry: Entry, status: "ok" | "error" | "timeout", response: unknown): void
}

export interface LogContext {
  hooks: LogHooks
  queue: Array<Entry>
  next: number
  tries: number
}

type ReplyEvent = Extract<LogEvent, { type: "REPLY" }>

const isOurs = (context: LogContext, event: ReplyEvent) =>
  context.queue.some((entry) => entry.ref === event.ref)

const notTaken = (event: ReplyEvent) => event.status === "error" && isNotTaken(event.response)

function sendWaiting(hooks: LogHooks, queue: Array<Entry>, next: number) {
  let number = next
  let inFlight = queue.filter((entry) => entry.ref !== null).length
  const sent = queue.map((entry) => {
    if (entry.ref !== null || inFlight >= DURABLE_WINDOW) return entry
    inFlight++
    const sending = { ...entry, number: number++, ref: hooks.makeRef() }
    hooks.send(sending)
    return sending
  })
  return { queue: sent, next: number }
}

export const logMachine = setup({
  types: {
    context: {} as LogContext,
    input: {} as { hooks: LogHooks },
    events: {} as LogEvent,
  },
  actors: { sleep },
  guards: {
    channelJoined: ({ context }) => context.hooks.channelJoined(),
    notTaken: ({ context, event }) =>
      event.type === "REPLY" && isOurs(context, event) && notTaken(event),
  },
  actions: {
    enqueue: assign(({ context }, entry: Entry) => ({
      queue: [...context.queue, entry],
    })),
    sendWaiting: assign(({ context }) => sendWaiting(context.hooks, context.queue, context.next)),
    answer: assign(({ context }, event: ReplyEvent) => {
      if (notTaken(event)) return {}
      for (const entry of context.queue.filter((entry) => entry.ref === event.ref))
        context.hooks.answer(entry, event.status === "ok" ? "ok" : "error", event.response)
      return {
        queue: context.queue.filter((entry) => entry.ref !== event.ref),
      }
    }),
    sync: ({ context }) => context.hooks.sync(),
    applySync: assign(({ context }, received: number) => {
      const next = Math.min(context.queue[0]?.number ?? received + 1, received + 1)
      const waiting = context.queue.map((entry) => ({
        ...entry,
        number: null,
        ref: null,
      }))
      return { ...sendWaiting(context.hooks, waiting, next), tries: 0 }
    }),
    abandon: ({ context }) => {
      for (const entry of context.queue) context.hooks.answer(entry, "timeout", undefined)
    },
  },
}).createMachine({
  id: "log",
  context: ({ input }) => ({
    hooks: input.hooks,
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
          ms: context.hooks.backoffMs(context.tries),
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
