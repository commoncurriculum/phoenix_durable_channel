import { Effect } from "effect"
import { assign, emit, enqueueActions, fromPromise, setup } from "xstate"
import { sleep, type CloseDetails } from "../actors"
import { appendParams } from "../url"
import { decodePollResponse } from "../wire"

interface PollResponse {
  status: number | "timeout"
  token: string | null
  messages: Array<string>
}

const timedOut: PollResponse = { status: "timeout", token: null, messages: [] }

interface RequestInput {
  endpoint: string
  token: string | null
  authToken: string | null
  method: "GET" | "POST"
  body: string | null
  timeout: number
}

const request = fromPromise<PollResponse, RequestInput>(({ input, signal }) =>
  Effect.runPromise(
    Effect.tryPromise((signal) =>
      fetch(appendParams(input.endpoint, input.token === null ? {} : { token: input.token }), {
        method: input.method,
        body: input.body,
        signal,
        headers: {
          "Content-Type": input.method === "GET" ? "application/json" : "application/x-ndjson",
          ...(input.authToken === null ? {} : { "X-Phoenix-AuthToken": input.authToken }),
        },
      }),
    ).pipe(
      Effect.flatMap((response) => Effect.tryPromise(() => response.json())),
      Effect.flatMap((body) => Effect.try(() => decodePollResponse(body))),
      Effect.timeoutTo({
        duration: input.timeout,
        onSuccess: (response) => response,
        onTimeout: () => timedOut,
      }),
      Effect.catchAll(() => Effect.succeed({ status: 0, token: null, messages: [] })),
    ),
    { signal },
  ),
)

export interface LongPollSettings {
  endpoint: string
  authToken: string | null
  timeout(): number
}

export type LongPollEmitted =
  | { type: "opened" }
  | { type: "message"; data: string }
  | { type: "errored"; error: unknown }
  | ({ type: "closed" } & CloseDetails)

interface LongPollContext {
  settings: LongPollSettings
  token: string | null
  batch: Array<string>
  sending: Array<string>
}

type Failure = { code: number; reason: string }

type LongPollEvent = { type: "SEND"; frame: string } | ({ type: "CLOSE" } & CloseDetails)

const internalServerError: Failure = {
  code: 1011,
  reason: "internal server error",
}

const poll = (context: LongPollContext): RequestInput => ({
  endpoint: context.settings.endpoint,
  token: context.token,
  authToken: context.settings.authToken,
  method: "GET",
  body: null,
  timeout: context.settings.timeout(),
})

const pollFailures: Record<string, Failure> = {
  timeout: { code: 1005, reason: "timeout" },
  410: { code: 3410, reason: "session gone" },
  403: { code: 1008, reason: "forbidden" },
}

const publishFailures: Record<string, Failure> = {
  timeout: { code: 1005, reason: "timeout" },
}

export const longPollMachine = setup({
  types: {
    context: {} as LongPollContext,
    input: {} as { settings: LongPollSettings },
    events: {} as LongPollEvent,
    emitted: {} as LongPollEmitted,
    tags: {} as "connecting" | "open" | "closed",
  },
  actors: { request, sleep },
  guards: {
    answered: (_, params: { output: PollResponse; status: PollResponse["status"] }) =>
      params.output.status === params.status,
    batched: ({ context }) => context.batch.length > 0,
  },
  actions: {
    keepToken: assign(({ context }, output: PollResponse) => ({
      token: output.token,
    })),
    deliver: enqueueActions(({ enqueue }, output: PollResponse) => {
      for (const data of output.messages) enqueue.emit({ type: "message", data })
    }),
    collect: assign(({ context }, frame: string) => ({
      batch: [...context.batch, frame],
    })),
    fail: enqueueActions(
      ({ enqueue }, params: { output: PollResponse; failures: Record<string, Failure> }) => {
        const { status } = params.output
        enqueue.emit({ type: "errored", error: status })
        enqueue.emit({
          type: "closed",
          ...(params.failures[status] ?? internalServerError),
          wasClean: false,
        })
      },
    ),
  },
}).createMachine({
  id: "longpoll",
  context: ({ input }) => ({
    settings: input.settings,
    token: null,
    batch: [],
    sending: [],
  }),
  initial: "starting",
  on: {
    SEND: { actions: { type: "collect", params: ({ event }) => event.frame } },
    CLOSE: {
      target: ".closed",
      actions: emit(({ event }) => ({
        type: "closed" as const,
        code: event.code,
        reason: event.reason,
        wasClean: event.wasClean,
      })),
    },
  },
  states: {
    starting: {
      tags: "connecting",
      invoke: { src: "sleep", input: { ms: 0 }, onDone: "running" },
    },
    running: {
      type: "parallel",
      states: {
        polling: {
          initial: "connecting",
          states: {
            connecting: {
              tags: "connecting",
              invoke: {
                src: "request",
                input: ({ context }) => poll(context),
                onDone: [
                  {
                    guard: {
                      type: "answered",
                      params: ({ event }) => ({
                        output: event.output,
                        status: 410,
                      }),
                    },
                    target: "open",
                    actions: {
                      type: "keepToken",
                      params: ({ event }) => event.output,
                    },
                  },
                  {
                    target: "#longpoll.closed",
                    actions: {
                      type: "fail",
                      params: ({ event }) => ({
                        output: event.output,
                        failures: pollFailures,
                      }),
                    },
                  },
                ],
              },
            },
            open: {
              tags: "open",
              entry: emit({ type: "opened" }),
              initial: "polling",
              states: {
                polling: {
                  invoke: {
                    src: "request",
                    input: ({ context }) => poll(context),
                    onDone: [
                      {
                        guard: {
                          type: "answered",
                          params: ({ event }) => ({
                            output: event.output,
                            status: 200,
                          }),
                        },
                        target: "polling",
                        reenter: true,
                        actions: [
                          {
                            type: "keepToken",
                            params: ({ event }) => event.output,
                          },
                          {
                            type: "deliver",
                            params: ({ event }) => event.output,
                          },
                        ],
                      },
                      {
                        guard: {
                          type: "answered",
                          params: ({ event }) => ({
                            output: event.output,
                            status: 204,
                          }),
                        },
                        target: "polling",
                        reenter: true,
                      },
                      {
                        target: "#longpoll.closed",
                        actions: {
                          type: "fail",
                          params: ({ event }) => ({
                            output: event.output,
                            failures: pollFailures,
                          }),
                        },
                      },
                    ],
                  },
                },
              },
            },
          },
        },
        publishing: {
          initial: "idle",
          states: {
            idle: {
              always: { guard: "batched", target: "gathering" },
              on: {
                SEND: {
                  target: "gathering",
                  actions: {
                    type: "collect",
                    params: ({ event }) => event.frame,
                  },
                },
              },
            },
            gathering: {
              invoke: { src: "sleep", input: { ms: 0 }, onDone: "sending" },
            },
            sending: {
              entry: assign(({ context }) => ({
                sending: context.batch,
                batch: [],
              })),
              invoke: {
                src: "request",
                input: ({ context }) => ({
                  ...poll(context),
                  method: "POST" as const,
                  body: context.sending.join("\n"),
                }),
                onDone: [
                  {
                    guard: {
                      type: "answered",
                      params: ({ event }) => ({
                        output: event.output,
                        status: 200,
                      }),
                    },
                    target: "idle",
                  },
                  {
                    target: "#longpoll.closed",
                    actions: {
                      type: "fail",
                      params: ({ event }) => ({
                        output: event.output,
                        failures: publishFailures,
                      }),
                    },
                  },
                ],
              },
            },
          },
        },
      },
    },
    closed: { type: "final", tags: "closed" },
  },
})
