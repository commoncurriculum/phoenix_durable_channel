import { assign, enqueueActions, fromCallback, sendTo, setup } from "xstate"
import { sleep } from "../actors"
import { Reader, type MessageStream, type PublishedReader, type StreamMessage } from "../stream"
import { hasPublishedReader } from "../wire"

export interface ChannelHooks {
  timeout(): number
  socketIsOpen(): boolean
  makeRef(): string
  rejoinAfterMs(tries: number): number
  sendJoin(joinRef: string | null): void
  sendLeave(joinRef: string | null, leaveRef: string | null): void
  answerJoin(status: "ok" | "error" | "timeout", response: unknown): void
  answerLeave(status: "ok" | "timeout"): void
  joined(): void
  /** Whether the channel reads the server's durable messages: it was opened with `durable`. */
  durable: boolean
  /** The stream a published reader names. */
  streamFor(
    reader: PublishedReader,
    latest: () => PublishedReader,
    refresh: () => Promise<void>,
  ): MessageStream
  /** Asks the server for the reader again, with a fresh token. */
  fetchReader(): Promise<PublishedReader>
  after: number | null
  deliver(message: StreamMessage): void
  connectionLost(): void
  errored(reason: unknown): void
  closed(reason: unknown): void
}

type ChannelEvent =
  | { type: "JOIN" }
  | { type: "LEAVE" }
  | { type: "SOCKET_OPEN" }
  | { type: "SOCKET_CLOSED" }
  | { type: "JOIN_REPLY"; status: string; response: unknown }
  | { type: "LEAVE_REPLY" }
  | { type: "SERVER_ERROR"; reason: unknown }
  | { type: "SERVER_CLOSE"; reason: unknown }
  | { type: "REPLACED" }

interface ChannelContext {
  hooks: ChannelHooks
  joinRef: string | null
  leaveRef: string | null
  rejoinTries: number
  // What the server answered the join that led to `joined`.
  joinResponse: unknown
}

type FollowEvent = { type: "PUBLISHED"; reader: PublishedReader }

/**
 * Reads the store `published` names for as long as the state that invoked it, from the
 * app's `after` or else from the store's last message at the first join, in the epoch
 * the join published. Each later join reply's reader, and each one fetched after the
 * store refused a token, becomes the `latest` the stream reads its token from.
 */
const readStream = fromCallback<FollowEvent, { hooks: ChannelHooks; published: PublishedReader }>(
  ({ input, receive }) => {
    const { after, deliver, rejoinAfterMs, streamFor, fetchReader } = input.hooks
    let latest = input.published
    receive((event) => {
      latest = event.reader
    })
    const refresh = async () => {
      latest = await fetchReader()
    }
    const reader = new Reader(
      streamFor(input.published, () => latest, refresh),
      {
        after: after ?? input.published.last,
        epoch: input.published.epoch ?? "",
      },
      deliver,
      rejoinAfterMs,
    )
    return () => reader.stop()
  },
)

/**
 * Where the channel reads the server's durable messages: `waiting` until the first join
 * reply says, then `reading` that store until the channel closes.
 */
export const followMachine = setup({
  types: {
    context: {} as { hooks: ChannelHooks; published: PublishedReader },
    input: {} as { hooks: ChannelHooks },
    events: {} as FollowEvent,
  },
  actors: { readStream },
}).createMachine({
  id: "follow",
  context: ({ input }) => ({
    hooks: input.hooks,
    published: { type: "phoenix", last: 0 },
  }),
  initial: "waiting",
  states: {
    waiting: {
      on: {
        PUBLISHED: {
          target: "reading",
          actions: assign({ published: ({ event }) => event.reader }),
        },
      },
    },
    reading: {
      invoke: {
        id: "reader",
        src: "readStream",
        input: ({ context }) => ({
          hooks: context.hooks,
          published: context.published,
        }),
      },
      on: { PUBLISHED: { actions: sendTo("reader", ({ event }) => event) } },
    },
  },
})

/**
 * A channel's life: `unjoined` until `join()`, then a `member` of the topic, `joining`,
 * `joined` and `errored` (which rejoins on its own) until it is left, closed by the
 * server, or replaced by a newer channel object on the same topic. `closed` is final for
 * this object.
 */
export const channelMachine = setup({
  types: {
    context: {} as ChannelContext,
    input: {} as { hooks: ChannelHooks },
    events: {} as ChannelEvent,
  },
  actors: { sleep, follow: followMachine },
  guards: {
    socketIsOpen: ({ context }) => context.hooks.socketIsOpen(),
    replyIsOk: ({ event }) => event.type === "JOIN_REPLY" && event.status === "ok",
    // A server that does not log the channel, such as a pod still running a deploy from
    // before it did, joins it without saying where to read.
    nondurable: ({ context, event }) =>
      event.type === "JOIN_REPLY" &&
      event.status === "ok" &&
      context.hooks.durable &&
      !hasPublishedReader(event.response),
  },
  actions: {
    lose: ({ context }) => context.hooks.connectionLost(),
    error: ({ context, event }) =>
      context.hooks.errored("reason" in event ? event.reason : undefined),
    answerLeave: ({ context }) => context.hooks.answerLeave("ok"),
  },
}).createMachine({
  id: "channel",
  context: ({ input }) => ({
    hooks: input.hooks,
    joinRef: null,
    leaveRef: null,
    rejoinTries: 0,
    joinResponse: undefined,
  }),
  initial: "unjoined",
  on: {
    REPLACED: { target: ".closed" },
  },
  states: {
    unjoined: {
      on: {
        JOIN: "member",
        LEAVE: { target: "closed", actions: "answerLeave" },
      },
    },
    member: {
      invoke: {
        id: "follow",
        src: "follow",
        input: ({ context }) => ({ hooks: context.hooks }),
      },
      initial: "joining",
      states: {
        joining: {
          initial: "checkingSocket",
          on: {
            LEAVE: "leaving",
            SERVER_CLOSE: "#channel.closed",
            SERVER_ERROR: { target: "errored", actions: "error" },
          },
          states: {
            checkingSocket: {
              always: [
                { guard: "socketIsOpen", target: "awaitingReply" },
                { target: "awaitingSocket" },
              ],
            },
            awaitingSocket: {
              on: { SOCKET_OPEN: "awaitingReply" },
            },
            awaitingReply: {
              entry: [
                assign({ joinRef: ({ context }) => context.hooks.makeRef() }),
                ({ context }) => context.hooks.sendJoin(context.joinRef),
              ],
              invoke: {
                src: "sleep",
                input: ({ context }) => ({ ms: context.hooks.timeout() }),
                onDone: {
                  target: "#channel.member.errored",
                  actions: ({ context }) => {
                    context.hooks.answerJoin("timeout", undefined)
                    // The server may still finish this join; leaving it means a late join
                    // does not leave a channel process behind.
                    context.hooks.sendLeave(context.joinRef, context.hooks.makeRef())
                  },
                },
              },
              on: {
                JOIN_REPLY: [
                  {
                    // Not joined: it leaves that join, and joins again after a backoff.
                    guard: "nondurable",
                    target: "#channel.member.errored",
                    actions: ({ context }) => {
                      context.hooks.answerJoin("error", {
                        reason: "nondurable",
                      })
                      context.hooks.sendLeave(context.joinRef, context.hooks.makeRef())
                    },
                  },
                  {
                    guard: "replyIsOk",
                    target: "#channel.member.joined",
                    actions: [
                      assign({ joinResponse: ({ event }) => event.response }),
                      enqueueActions(({ event, enqueue }) => {
                        if (hasPublishedReader(event.response))
                          enqueue.sendTo("follow", {
                            type: "PUBLISHED",
                            reader: event.response.phx_reader,
                          })
                      }),
                    ],
                  },
                  {
                    target: "#channel.member.errored",
                    actions: ({ context, event }) =>
                      context.hooks.answerJoin("error", event.response),
                  },
                ],
                SOCKET_CLOSED: {
                  target: "#channel.member.errored",
                  actions: "error",
                },
              },
            },
          },
        },
        joined: {
          // Buffered pushes go out before the join's "ok" callbacks run, as in phoenix.js.
          entry: [
            assign({ rejoinTries: 0 }),
            ({ context }) => context.hooks.joined(),
            ({ context }) => context.hooks.answerJoin("ok", context.joinResponse),
          ],
          on: {
            LEAVE: "leaving",
            SOCKET_CLOSED: { target: "errored", actions: "error" },
            SERVER_ERROR: { target: "errored", actions: "error" },
            SERVER_CLOSE: "#channel.closed",
          },
        },
        errored: {
          entry: "lose",
          initial: "checkingSocket",
          on: {
            LEAVE: { target: "#channel.closed", actions: "answerLeave" },
            SOCKET_OPEN: {
              target: "joining",
              actions: assign({ rejoinTries: 0 }),
            },
          },
          states: {
            checkingSocket: {
              always: [
                { guard: "socketIsOpen", target: "awaitingRejoin" },
                { target: "awaitingSocket" },
              ],
            },
            awaitingSocket: {},
            awaitingRejoin: {
              invoke: {
                src: "sleep",
                input: ({ context }) => ({
                  ms: context.hooks.rejoinAfterMs(context.rejoinTries + 1),
                }),
                onDone: {
                  target: "#channel.member.joining",
                  actions: assign({
                    rejoinTries: ({ context }) => context.rejoinTries + 1,
                  }),
                },
              },
              on: { SOCKET_CLOSED: "awaitingSocket" },
            },
          },
        },
        leaving: {
          entry: "lose",
          initial: "checkingSocket",
          states: {
            checkingSocket: {
              always: [
                { guard: "socketIsOpen", target: "awaitingReply" },
                {
                  target: "#channel.closed",
                  actions: ({ context }) => context.hooks.answerLeave("ok"),
                },
              ],
            },
            awaitingReply: {
              entry: [
                assign({ leaveRef: ({ context }) => context.hooks.makeRef() }),
                ({ context }) => context.hooks.sendLeave(context.joinRef, context.leaveRef),
              ],
              invoke: {
                src: "sleep",
                input: ({ context }) => ({ ms: context.hooks.timeout() }),
                onDone: {
                  target: "#channel.closed",
                  actions: ({ context }) => context.hooks.answerLeave("timeout"),
                },
              },
              on: {
                LEAVE_REPLY: {
                  target: "#channel.closed",
                  actions: ({ context }) => context.hooks.answerLeave("ok"),
                },
                SOCKET_CLOSED: {
                  target: "#channel.closed",
                  actions: ({ context }) => context.hooks.answerLeave("ok"),
                },
              },
            },
          },
        },
      },
    },
    closed: {
      type: "final",
      entry: ({ context, event }) =>
        context.hooks.closed(
          event.type === "LEAVE" || event.type === "LEAVE_REPLY"
            ? "leave"
            : "reason" in event
              ? event.reason
              : undefined,
        ),
    },
  },
})
