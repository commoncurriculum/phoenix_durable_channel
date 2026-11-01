import { Effect } from "effect"
import { assign, fromPromise, setup } from "xstate"
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

export interface LongPollHooks {
  endpoint: string
  timeout(): number
  opened(): void
  message(data: string): void
  errored(error: unknown): void
  closed(details: CloseDetails): void
}

interface LongPollContext {
  hooks: LongPollHooks
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
  endpoint: context.hooks.endpoint,
  token: context.token,
  method: "GET",
  body: null,
  timeout: context.hooks.timeout(),
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
    input: {} as { hooks: LongPollHooks },
    events: {} as LongPollEvent,
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
    deliver: ({ context }, output: PollResponse) => {
      for (const data of output.messages) context.hooks.message(data)
    },
    collect: assign(({ context }, frame: string) => ({
      batch: [...context.batch, frame],
    })),
    fail: ({ context }, params: { output: PollResponse; failures: Record<string, Failure> }) => {
      const { status } = params.output
      context.hooks.errored(status)
      context.hooks.closed({
        ...(params.failures[status] ?? internalServerError),
        wasClean: false,
      })
    },
  },
}).createMachine({
  id: "longpoll",
  context: ({ input }) => ({
    hooks: input.hooks,
    token: null,
    batch: [],
    sending: [],
  }),
  initial: "starting",
  on: {
    SEND: { actions: { type: "collect", params: ({ event }) => event.frame } },
    CLOSE: {
      target: ".closed",
      actions: ({ context, event }) =>
        context.hooks.closed({
          code: event.code,
          reason: event.reason,
          wasClean: event.wasClean,
        }),
    },
  },
  states: {
    starting: {
      invoke: { src: "sleep", input: { ms: 0 }, onDone: "running" },
    },
    running: {
      type: "parallel",
      states: {
        polling: {
          initial: "connecting",
          states: {
            connecting: {
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
              entry: ({ context }) => context.hooks.opened(),
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
                  endpoint: context.hooks.endpoint,
                  token: context.token,
                  method: "POST" as const,
                  body: context.sending.join("\n"),
                  timeout: context.hooks.timeout(),
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
    closed: { type: "final" },
  },
})
