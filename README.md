# phoenix_durable_channel

Phoenix Channels with durable messages: numbered, kept in a store you choose (MongoDB, S2 or Durable Objects), delivered once and in order both ways, across reconnects, node failures and deploys. It is two packages:

- the JavaScript client (`src/`), a drop-in replacement for the `phoenix` npm package's client (`Socket`, `Channel`, `Push`, `Presence`, `LongPoll`), built on XState, Effect and supergrain;
- the Elixir server library (`server/`), `PhoenixDurableChannel.Channel`, used beside `Phoenix.Channel`.

```sh
pnpm add "phoenix_durable_channel@git+https://github.com/commoncurriculum/phoenix_durable_channel.git#<sha>"
```

```elixir
{:phoenix_durable_channel, github: "commoncurriculum/phoenix_durable_channel", sparse: "server"}
```

```ts
import { Socket } from "phoenix_durable_channel"

const socket = new Socket("/socket", { params: { token } })
socket.connect()
const channel = socket.channel("room:lobby", { since })
channel.join().receive("ok", (reply) => {})
channel
  .push("new_msg", { body })
  .receive("ok", (reply) => {})
  .receive("timeout", () => {})
```

## What is different from phoenix.js 1.7

The calls and their arguments are the same. The behavior differs where phoenix.js loses work or gets stuck:

- Any close the app did not ask for reconnects, including code 1000 (a server idle timeout or a proxy).
- A push already sent when the connection or the channel drops is answered `"timeout"` at once, not after its full timeout.
- A join that times out sends a leave for that join, so the server does not keep a channel process behind.
- A late reply to an old join is ignored, and a newer `socket.channel()` on the same topic closes the older object.
- A lost LongPoll session (HTTP 410) closes the connection, so channels rejoin on the new session. LongPoll fallback is remembered per socket, and WebSocket is retried after `websocketRetryAfterMs`.
- Presence keys such as `"toString"` or `"__proto__"` are ordinary keys.
- `heartbeatTimeoutMs` (default 10 s) is separate from the heartbeat interval.

New options: `heartbeatTimeoutMs`, `websocketRetryAfterMs`, `offlineAfterMs`, `activity` (an `ActivityTracker` from `@supergrain/activity`) and `disconnectAfterInactiveMs`. `socket.status` and `channel.status` are reactive supergrain objects (`connectionState`, `transport`, `online`; `state`).

## Durable messages

A channel can carry two kinds of message, chosen per message. Fire-and-forget is phoenix.js as it is. A durable message has a number, kept on the server, so it is never lost and is handled in order, and the app pays for that storage only where it asks for it. It is handled once unless the server's channel process dies between running it and recording it; then the resend runs it again, so a handler must be safe to run twice.

```ts
const channel = socket.channel("session:42", params, {
  durable: true,
  after: lastSeen,
})
channel.push("cursor", payload) // fire and forget
channel.push("action", payload, { durable: true }) // a Reply: "ok" or "error", once
channel.on("update", (payload) => {}) // durable updates arrive once and in order
channel.log.status.pending // durable pushes not answered yet
```

```elixir
use PhoenixDurableChannel.Channel, ledger: PhoenixDurableChannel.Stream.S2, stream: PhoenixDurableChannel.Stream.S2
push(socket, "cursor", payload)
push_durable(socket, "update", payload)
```

On the wire, a durable message's number is a sixth element of the V2 frame, `[join_ref, ref, topic, event, payload, number]`; the endpoint reads it with `PhoenixDurableChannel.Serializer` (in `server/`). A client opened with `durable` joins with `phx_durable: true`; the server channel finds that in `socket.assigns.phx_durable`, and a durable push's number in `socket.assigns.phx_number`.

The two directions are separate:

- **The client's durable pushes.** After every join the client sends `phx_sync`, and the server replies `{received}`, the last number its ledger recorded. The client numbers its pushes on from there, sends them in order, and sends them again after every rejoin until answered; the server answers one it already handled with the reply its ledger kept. A push the server cannot take now (`{:retry, reason, socket}`) is sent again, with the ones after it, after a backoff. A durable push is answered `"timeout"` only when its channel closes first. At most 32 are unanswered at once, and the ledger keeps the last 32 replies or more, so a resend always finds its reply. A durable join replaces the topic's channel process from an earlier connection, after that process has run what it had already received, so two processes never run the client's pushes at once. A replaced process sends its client `phx_error`, so a client still on that join rejoins and sends again what crossed it (Phoenix answers those "unmatched topic"): the join that replaced it can be a late one from a connection the client already gave up on. Two clients joined on one topic at once would take it from each other in turn. The ledger is keyed by one client, such as a session id made for each page load: a reload is a new client, numbering from 1.
- **The server's durable messages.** They go to a stream (below). The join reply's `phx_reader` says where to read them and `last`, the stream's last number at the join; a client with no `after` of its own starts after `last`, so nothing durable after its join is missed however late its read opens. Only what was appended is kept: a channel that appends what it hears on PubSub appends nothing while no process of it is subscribed, so the app catches up on its own after a rejoin. A durable channel's join answered `ok` with no `phx_reader`, as a server from before durable messages answers, is not joined: the push gets `"error"` `{reason: "nondurable"}`, the client leaves that join, and it joins again after a backoff, keeping its durable pushes, until a server with durable messages answers. So during a rolling deploy a new page waits for a new pod rather than sending to an old one.

## Where durable messages are kept

Two things are kept, each behind its own behaviour:

- the **ledger**, a `PhoenixDurableChannel.Ledger`: the last number handled from the client (`received/1`) and the replies (`put_received/3`, `reply/2`);
- the **stream**, a `PhoenixDurableChannel.Stream`: the server's durable messages. `append/3` numbers and keeps one, and `reader/1` says how a client reads them. The join reply carries that as `phx_reader`, so the client needs no configuration of its own.

A channel picks them with `ledger:` and `stream:`, or per socket by defining `durable_ledger/1` and `durable_stream/1`. Every store the library ships is both, so swapping stores is changing the module name. Each is an optional dependency: add the one you use.

| Store                                         | Needs                                                                                   | `phx_reader`                                          | The client reads                                                  |
| --------------------------------------------- | --------------------------------------------------------------------------------------- | ----------------------------------------------------- | ----------------------------------------------------------------- |
| `PhoenixDurableChannel.Stream.Memory`         | nothing                                                                                 | `{type: "phoenix", last, epoch}`                      | through the channel; each node's own, lost when the node restarts |
| `PhoenixDurableChannel.Stream.Mongo`          | `mongodb_driver`                                                                        | `{type: "phoenix", last, epoch}`                      | through the channel                                               |
| `PhoenixDurableChannel.Stream.S2`             | `s2_client`                                                                             | `{type: "s2", endpoint, basin, stream, token, last}`  | from S2 with its server-sent events; Phoenix never carries them   |
| `PhoenixDurableChannel.Stream.DurableStreams` | `req`, a Durable Streams server such as `durable-streams-cloudflare` on Durable Objects | `{type: "durable-streams", url, token, last, offset}` | from the server with `live=sse`; Phoenix never carries them       |

Mongo keeps messages and replies as documents in two collections, unique on `{topic, number}` (`PhoenixDurableChannel.Stream.Mongo.create_indexes/0`). S2 and Durable Streams keep the replies in a second stream, `<topic>/replies`, which the client's read token cannot read. Memory's ledger is the node's own, so it suits one node. A store of your own implements the same behaviours.

A stream read through the channel is also a `PhoenixDurableChannel.Stream.ThroughChannel`: `push_durable/3` pushes each message with its number, and `since/3` answers the client's `phx_read {after}` a page at a time. The server sends again what it kept after `after`, then replies `{sent, epoch}`: the last number it sent, and the stream's epoch. Until the reply the client holds what arrives; after it, each message must be the next one, and one that is not ends the read, as losing the connection does, and the client reads again from the last message it delivered. The read has no timeout: a server still paging is still sending. The epoch names one numbering of the stream: Memory gives each node's store its own, and a new one when the node restarts or forgets the topic. A reply in another epoch ends the read, and the client reads that epoch from where it last got to in it, or from the start.

A store read through its own protocol is read from the number after `after`, and after a read ends, read again from the last message delivered. Each topic is one stream of the same name, and each token reads only that stream. A store that refuses the token (401 or 403) makes the client ask the channel for a fresh reader (`phx_reader`) and read again. S2 record `seq_num` n is message n + 1. Durable Streams offsets are opaque, so each message carries its number, the join reply gives the offset where `last` ends, and the server numbers on from the stream's last with `Stream-Seq`, which the store refuses to repeat, so two nodes never write the same number. An S2 or Durable Streams stream that is deleted and made again, or trimmed past where a client got to, is not noticed: delete a topic's stream only when no client will read it again.

A client reads another kind of store with a reader of its own, by `type`:

```ts
socket.channel(topic, params, {
  durable: true,
  readers: { kafka: (latest, refresh) => new KafkaStream(latest(), refresh) }, // a MessageStream
})
```

`latest()` is what the last join reply, or the last `refresh()`, published, so a fresh token is used on the next read. `refresh()` asks the channel for a fresh reader. A type the client has no reader for is logged to the console, and its messages are not read.

## Tests

```sh
pnpm install # from the repository root
pnpm test
```

The tests run in Chromium against a real Phoenix 1.7.21 server in `test/server`, S2 Lite and the `durable-streams-cloudflare` worker on wrangler, which `test/support/phoenix-server.ts` starts (it needs Elixir, `mix` and the `s2` CLI from mise). Coverage must stay at 100%.

## Jepsen

```sh
jepsen/run.sh --store mongo --transport websocket --time-limit 180
# --store mongo|s2|durable-streams  --transport websocket|longpoll  --faults partition,kill,pause,cut-off-clients
```

A [Jepsen](https://jepsen.io) test of durable messages under faults. Three Docker nodes run `jepsen/server`, a Phoenix app with one durable channel on this library and nothing else, as one Erlang cluster. One of the library's stores, shared by the nodes, is both its ledger and its stream: MongoDB, S2 Lite, or the `durable-streams-cloudflare` worker on Durable Objects under wrangler. Clients are this library in Chromium (`jepsen/driver`), over WebSocket or LongPoll, behind a proxy that sends each new connection to a random node (so a LongPoll session's requests land on any node). Each client pushes a durable `add {value, position}`, and the server appends each to that client's stream.

Faults: partitions between nodes, `kill -9`, `SIGSTOP`, and cutting a node off from the clients without closing their sockets. The checker requires, for every client:

- its stream holds its pushes in the order pushed, from the first, with every acknowledged one;
- it was delivered exactly its stream: every message once, in order, none missing;
- no push was answered with an error.

A push may be in the stream twice in a row: a push runs again when its channel process dies between running it and the ledger's record (see Durable messages). The checker counts these as `:reruns`. Results are in `jepsen/store`; it needs Docker, and Leiningen from `jepsen/mise.toml`.

## Formal verification

```sh
verification/check.sh
```

TLA+ models of the socket, channel and push lifecycle, the client's durable pushes, reading the server's durable messages through the channel and from a stream, the socket's push buffer, and numbering on Durable Streams. The XState machines are in `src/machines/`, one per file, and the classes in `src/` run them. The `verify-phoenix-durable-channel` skill in `.agents/skills` says what it checks, how to read a failure, and how to keep the model in step with the code.
