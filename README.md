# phoenix_durable_channel

Phoenix Channels where a message you mark durable is not lost while your store keeps it. Each durable message is numbered, kept in a store you choose (MongoDB, S2 or Durable Objects), and delivered once and in order, in both directions, across reconnects, node failures and deploys. Every other message is plain Phoenix.

- **Why:** Phoenix Channels deliver at most once. A push in flight when a connection drops, a node dies or a deploy restarts a pod is gone, and phoenix.js answers it `"timeout"` with no way to know whether the server ran it. Apps patch around this with their own sequence numbers, resend loops and "request the lost one" events. This library does that once, under the protocol, for the messages that need it.
- **What you change:** the client is a drop-in for phoenix.js (`import { Socket } from "phoenix_durable_channel"` instead of `"phoenix"`). The server is your existing `Phoenix.Channel` with one more `use` line and one serializer in your endpoint. Messages you don't mark durable cost nothing extra.

## The repo

It is laid out like Phoenix's own repo: the Elixir package at the root, the JavaScript client under `assets/js`.

| Path                                | What it is                                                                                                                                                  |
| ----------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `mix.exs`, `lib/`                   | The Elixir package `phoenix_durable_channel`: `PhoenixDurableChannel.Channel`, the serializer, the stores                                                   |
| `assets/js/phoenix_durable_channel` | The npm package `phoenix_durable_channel`: `Socket`, `Channel`, `Push`, `Presence`, `LongPoll`, in TypeScript on XState, Effect and supergrain              |
| `integration_test/`                 | Browser tests: the client in Chromium against `integration_test/phoenix_app`, a real Phoenix app on this library, with real MongoDB, S2 and Durable Streams |
| `test/`                             | Phoenix's own server channel and serializer tests, copied from Phoenix, run on `PhoenixDurableChannel.Channel` and its serializer                           |
| `phoenix_js_test/`                  | Phoenix's own JavaScript client tests, copied from Phoenix, run on this client                                                                              |
| `jepsen/`                           | A Jepsen test of durable messages under partitions, crashes and pauses                                                                                      |
| `verification/`                     | TLA+ models of the client and the server channel                                                                                                            |

## Install

In `mix.exs`, the library and the client for the store you pick (below):

```elixir
{:phoenix_durable_channel, github: "commoncurriculum/phoenix_durable_channel", ref: "<sha>"},
{:mongodb_driver, "~> 1.5"}
```

```sh
pnpm add "phoenix_durable_channel@git+https://github.com/commoncurriculum/phoenix_durable_channel.git#<sha>"
```

## Step 1: swap the client

Change the import. Nothing else changes, and it works against your server as it is, with no server change.

```diff
-import { Socket } from "phoenix"
+import { Socket } from "phoenix_durable_channel"
```

```ts
const socket = new Socket("/socket", { params: { token } })
socket.connect()
const channel = socket.channel("room:lobby", {})
channel.join().receive("ok", (reply) => {})
channel.push("new_msg", { body }).receive("ok", (reply) => {})
channel.on("new_msg", (payload) => {})
```

What behaves differently from phoenix.js 1.7 is listed in [Differences from phoenix.js](#differences-from-phoenixjs-17); each one is a case where phoenix.js loses work or gets stuck.

## Step 2: add durable messages to your Phoenix app

The example is a chat app with a `room:*` channel today. It keeps that channel and adds a durable one: the client sends messages durably and receives the room's messages durably.

### 2a. The endpoint: add the serializer

A durable message carries its number in a sixth slot of Phoenix's V2 JSON frame. `PhoenixDurableChannel.Serializer` reads and writes it; plain frames are unchanged. Keep Phoenix's V1 serializer for any client that still asks for it. In `lib/my_app_web/endpoint.ex`:

```diff
 socket "/socket", MyAppWeb.UserSocket,
-  websocket: true,
-  longpoll: true
+  websocket: [
+    serializer: [
+      {Phoenix.Socket.V1.JSONSerializer, "~> 1.0.0"},
+      {PhoenixDurableChannel.Serializer, "~> 2.0.0"}
+    ]
+  ],
+  longpoll: [
+    serializer: [
+      {Phoenix.Socket.V1.JSONSerializer, "~> 1.0.0"},
+      {PhoenixDurableChannel.Serializer, "~> 2.0.0"}
+    ]
+  ]
```

### 2b. Pick a store, configure it and start it

The store keeps two things: the **ledger** (the last durable message handled from the client, and its replies, so a resend gets the same reply) and the **stream** (the server's durable messages, so the client can read what it missed). Every store the library ships is both.

| Store                                         | Add to `deps`                                                                               | Start in your supervision tree                 | The client reads the server's messages              |
| --------------------------------------------- | ------------------------------------------------------------------------------------------- | ---------------------------------------------- | --------------------------------------------------- |
| `PhoenixDurableChannel.Stream.Mongo`          | `mongodb_driver`                                                                            | your `Mongo` topology, then `create_indexes/0` | through the channel                                 |
| `PhoenixDurableChannel.Stream.S2`             | `s2_client`                                                                                 | an `S2` client for your basin                  | straight from S2; Phoenix never carries them        |
| `PhoenixDurableChannel.Stream.DurableStreams` | `req`, and a Durable Streams server such as `durable-streams-cloudflare` on Durable Objects | `PhoenixDurableChannel.Stream.DurableStreams`  | straight from the Durable Streams server            |
| `PhoenixDurableChannel.Stream.Memory`         | nothing                                                                                     | `PhoenixDurableChannel.Stream.Memory`          | through the channel; one node only, lost on restart |

With MongoDB, in `config/runtime.exs`:

```elixir
config :phoenix_durable_channel, PhoenixDurableChannel.Stream.Mongo,
  name: MyApp.Mongo,
  messages: "durable_messages",
  replies: "durable_replies"
```

and in `lib/my_app/application.ex`:

```elixir
children = [
  {Mongo, name: MyApp.Mongo, url: System.fetch_env!("MONGO_URL")},
  {Task, &PhoenixDurableChannel.Stream.Mongo.create_indexes/0},
  MyAppWeb.Endpoint
]
```

With S2, in `config/runtime.exs`, where `account_endpoint` issues each client a token for its own stream:

```elixir
config :phoenix_durable_channel, PhoenixDurableChannel.Stream.S2,
  name: MyApp.S2,
  basin: "my-basin",
  basin_endpoint: "https://{basin}.b.aws.s2.dev",
  account_endpoint: "https://aws.s2.dev",
  token: System.fetch_env!("S2_TOKEN")
```

and in `lib/my_app/application.ex`:

```elixir
children = [
  {S2, name: MyApp.S2, basin: "my-basin", basin_endpoint: "https://{basin}.b.aws.s2.dev"},
  MyAppWeb.Endpoint
]
```

With Durable Streams on Durable Objects, in `config/runtime.exs`:

```elixir
config :phoenix_durable_channel, PhoenixDurableChannel.Stream.DurableStreams,
  url: "https://durable-streams.example.workers.dev",
  project: "my-project",
  signing_secret: System.fetch_env!("DURABLE_STREAMS_SECRET")
```

and in `lib/my_app/application.ex`:

```elixir
children = [PhoenixDurableChannel.Stream.DurableStreams, MyAppWeb.Endpoint]
```

Each node keeps where it read each topic's streams to, and where its recent replies are, while a channel has the topic and for `keep_offsets_for` (five minutes by default, `{PhoenixDurableChannel.Stream.DurableStreams, keep_offsets_for: :timer.minutes(5)}`) after the topic's last channel leaves. So a reconnect to the same node does not read the streams again from the start, and a push sent again is looked up from near its reply, on any node, after one read of the replies. Forgetting drops only this cache, never the ledger.

### 2c. The channel: one `use` line

A durable channel's topic names **one client**, such as one page load: the ledger counts that client's messages, and a durable join makes its process the topic's only one. So the room's messages reach each client on the client's own topic. In `lib/my_app_web/user_socket.ex`:

```elixir
channel "client:*", MyAppWeb.ClientChannel
```

and in `lib/my_app_web/channels/client_channel.ex`:

```elixir
defmodule MyAppWeb.ClientChannel do
  use Phoenix.Channel
  use PhoenixDurableChannel.Channel,
    ledger: PhoenixDurableChannel.Stream.Mongo,
    stream: PhoenixDurableChannel.Stream.Mongo

  def join("client:" <> _id, %{"room" => room}, socket) do
    MyAppWeb.Endpoint.subscribe("room:" <> room)
    {:ok, assign(socket, :room, room)}
  end

  def handle_in("new_msg", %{"body" => body}, socket) do
    case MyApp.Chat.create_message(socket.assigns.room, body) do
      {:ok, message} ->
        MyAppWeb.Endpoint.broadcast("room:" <> socket.assigns.room, "new_msg", message)
        {:reply, {:ok, %{id: message.id}}, socket}

      {:error, :database_down} ->
        {:retry, :database_down, socket}

      {:error, %Ecto.Changeset{}} ->
        {:reply, {:error, %{reason: "invalid"}}, socket}
    end
  end

  def handle_info(%Phoenix.Socket.Broadcast{event: "new_msg", payload: message}, socket) do
    push_durable(socket, "new_msg", message)
    {:noreply, socket}
  end
end
```

- `handle_in("new_msg", ...)` is the client's durable push. It runs once and in order; its reply is recorded, and a resend after a lost reply gets this same reply. `{:retry, :database_down, socket}` makes the client send it again, in order, after a backoff.
- `handle_info` makes the room's messages durable to this client: numbered, kept, and read again after a drop.
- `use PhoenixDurableChannel.Channel` comes after `use Phoenix.Channel`. It wraps your `join/3`, `handle_in/3` and `handle_info/2`; your functions stay as they are.
- `push/3` stays fire and forget. `push_durable/3` is the durable one.
- `handle_in/3` sees a durable message's plain payload, and `socket.assigns.phx_number` is its number. It answers with what Phoenix allows (`{:reply, reply, socket}`, `{:noreply, socket}` and `{:noreply, socket, timeout | :hibernate}` meaning `:ok`, `{:stop, reason, reply, socket}`, `{:stop, reason, socket}` meaning `:ok`), or `{:retry, reason, socket}` for a message it can't take now. A stop's reply is recorded before the process stops. A handler that raises answers `{:error, %{reason: "crashed"}}` and is not run again.
- **A handler must be safe to run twice.** A message runs once unless its process dies between running it and the ledger's record; then the client's resend runs it again.
- `ledger:` and `stream:` are read on every call, so they can come from runtime config. Define `durable_ledger/1` or `durable_stream/1` to pick per socket.
- Only what was appended is kept. Above, a broadcast while no process of the client's channel is subscribed (the client is offline) is not appended, so the client reloads the room after a rejoin, as it would today.

### 2d. The client: open the channel durable

The `id` is one per page load. A durable push is answered `"ok"` or `"error"` exactly once, `channel.on` gets each durable message once and in order, and `channel.log?.status.pending` counts the durable pushes not answered yet (`channel.log` is `null` on a channel opened without `durable`).

```ts
const id = crypto.randomUUID()
const channel = socket.channel(`client:${id}`, { room: "lobby" }, { durable: true })
channel.join()

channel
  .push("new_msg", { body }, { durable: true })
  .receive("ok", ({ id }) => {})
  .receive("error", ({ reason }) => {})

channel.on("new_msg", (message) => {})
channel.log?.status.pending
```

A durable push is answered `"timeout"` only when its channel is closed (`channel.leave()`); a dropped connection is not an answer. It waits, and goes again after the rejoin.

### 2e. Deploying it

- **Pages still on phoenix.js keep working.** They speak V2 frames without a sixth slot, which the serializer reads as before, and channels they join without `phx_durable` behave as plain Phoenix channels.
- **A new page that reaches an old pod waits.** An old pod answers its durable join without `phx_durable: true`; the client treats that join as not joined, keeps its durable pushes, and joins again after a backoff until a pod with this library answers.
- **Upgrading from a version of this library that answered the durable join with `phx_reader` is a breaking change.** This version answers `phx_durable: true` and pushes `phx_reader` after the join. A page loaded before the deploy treats the new reply as not joined and joins again until it is reloaded, its durable pushes waiting. Deploy it when pages can be reloaded, or reload them.

## Differences from phoenix.js 1.7

The calls and their arguments are the same. The behavior differs where phoenix.js loses work or gets stuck:

- Any close the app did not ask for reconnects, including code 1000 (a server idle timeout or a proxy).
- A push already sent when the connection or the channel drops is answered `"timeout"` at once, not after its full timeout.
- A join that times out sends a leave for that join, so the server does not keep a channel process behind.
- A late reply to an old join is ignored, and a newer `socket.channel()` on the same topic closes the older object.
- A lost LongPoll session (HTTP 410) closes the connection, so channels rejoin on the new session. LongPoll fallback is remembered per socket, and WebSocket is retried after `websocketRetryAfterMs`.
- Presence keys such as `"toString"` or `"__proto__"` are ordinary keys.
- `heartbeatTimeoutMs` (default 10 s) is separate from the heartbeat interval.

New socket options: `heartbeatTimeoutMs`, `websocketRetryAfterMs`, `offlineAfterMs`, `activity` (an `ActivityTracker` from `@supergrain/activity`) and `disconnectAfterInactiveMs`. `socket.status` and `channel.status` are reactive supergrain objects (`connectionState`, `transport`, `online`; `state`).

New channel options, the third argument of `socket.channel`: `durable`, `after` (the number of the last server message the app already has, a whole number of 0 or more, or `socket.channel` throws; without it the channel receives what is appended after its first join) and `readers` (below).

## How durable messages work

On the wire, a durable message's number is a sixth element of the V2 frame, `[join_ref, ref, topic, event, payload, number]`. A client opened with `durable` joins with `phx_durable: true`. The server channel assigns `phx_durable` and adds `phx_durable: true` to the join reply, so the client knows the server speaks durable messages. On a channel joined without it, `phx_sync`, `phx_read`, `phx_reader` and any numbered frame are answered `{:error, %{reason: "invalid"}}`, as is a `phx_read` with an `after` that is not a number of 0 or more, or on a stream that is not read through the channel.

The two directions are separate.

**The client's durable pushes:**

- After every join the client sends `phx_sync`, and the server replies `{received}`, the last number its ledger recorded.
- The server reads `received` from the ledger once, at the join, and counts on from it in the channel's assigns, with the last 32 replies: the join makes its process the only one taking the client's pushes. A ledger reset while the process runs changes nothing until the next join, which reads the ledger again.
- The client numbers its pushes on from there, sends them in order, and sends them again after every rejoin until answered. The server answers one it already handled with the reply its process or its ledger kept, and one whose reply neither kept with `{:error, %{reason: "reply_lost"}}`.
- A push the server can't take now (`{:retry, reason, socket}`) is sent again, with the ones after it, after a backoff.
- At most 32 are unanswered at once, and the ledger keeps the last 32 replies or more, so a resend finds its reply unless the ledger lost what it kept.
- A durable join replaces the topic's process from an earlier connection, after that process has run what it had already received, so two processes never run the client's pushes at once. The replaced process sends its client `phx_error`, so a client still on that join rejoins and sends again what crossed it (Phoenix answers those "unmatched topic"): the join that replaced it can be a late one from a connection the client already gave up on.
- Two clients joined on one topic at once would take it from each other in turn. A reload is a new client, numbering from 1.

**The server's durable messages:**

- They go to the stream. Right after a durable join, once the topic's earlier process is gone, the server pushes `phx_reader`: where to read them, and `last`, the stream's last number then. A client with no `after` starts after `last`, so nothing durable after its join is missed however late its read opens.
- A read through the channel pauses while the channel is not joined and starts again after the rejoin, so a read never waits in the socket's buffer. A read from the store's own protocol goes on regardless.
- A message the store no longer has is skipped: the client delivers the next one it has, logs `<topic> skipped durable messages <from> to <to>, which the store no longer has` through the socket's `logger`, and triggers `phx_skipped` on the channel with `{after, next}`, the last number it delivered and the next it has, so `channel.on("phx_skipped", ...)` can reload what the app shows.
- A durable channel's join answered `ok` without `phx_durable: true`, as a server without this library answers, is not joined: its pushes wait, the client leaves that join and joins again after a backoff.

## Stores

Two behaviours, and each store implements both:

- `PhoenixDurableChannel.Ledger`: `received/1`, the last number handled from the client; `put_received/3` and `reply/2`, the replies.
- `PhoenixDurableChannel.Stream`: `append/3` numbers and keeps a message; `reader/1` is how the client reads them, pushed after the join as `phx_reader`, so the client needs no configuration of its own.

| Store            | Where                                                                                     | `phx_reader`                                          |
| ---------------- | ----------------------------------------------------------------------------------------- | ----------------------------------------------------- |
| `Mongo`          | documents in two collections, unique on `{topic, number}`                                 | `{type: "phoenix", last, epoch}`                      |
| `S2`             | an S2 stream per topic; replies in `<topic>/replies`, which the client's token can't read | `{type: "s2", endpoint, basin, stream, token, last}`  |
| `DurableStreams` | a Durable Streams stream per topic; replies in `<topic>/replies`                          | `{type: "durable-streams", url, token, last, offset}` |
| `Memory`         | the node's memory; each node's own numbering (an epoch), lost on restart                  | `{type: "phoenix", last, epoch}`                      |

A stream that implements `PhoenixDurableChannel.Stream.ThroughChannel` (`since/3`) is read through the channel. Any other is read from the store's own protocol.

**Read through the channel** (`Mongo`, `Memory`, or your own `ThroughChannel`):

- `push_durable/3` pushes each message with its number, and `since/3` answers the client's `phx_read {after}` a page at a time.
- The server resends what it kept after `after`, then replies `{sent, epoch}`. Until that reply the client holds what arrives; after it, each message must be the next one, and one that isn't ends the read, as losing the connection does. The client reads again from the last message it delivered.
- The read has no timeout: a server still paging is still sending. A read the server answers with an error is logged through the socket's `logger` and asked again after a backoff.
- The epoch names one numbering of the stream. A reply in another epoch ends the read, and the client reads that epoch from where it last got to in it, or from the start.

**Read from the store's own protocol** (`S2`, `DurableStreams`):

- Each topic is one stream of the same name, and each token reads only that stream. A store that refuses the token (401 or 403) makes the client ask the channel for a fresh reader (`phx_reader`) and read again.
- S2 record `seq_num` n is message n + 1. A command record, such as the trim S2 appends when a stream is deleted, takes a number but is no message: the client passes it.
- Durable Streams offsets are opaque, so each message carries its number, `phx_reader` gives the offset where `last` ends, a read that fails starts again at the offset where it last got to, and the server numbers on from the stream's last with `Stream-Seq`, which the store refuses to repeat, so two nodes never write the same number.
- A stream that is deleted and made again, or trimmed past where a client got to, is not noticed: delete a topic's stream only when no client will read it again.

**Another kind of store** needs a reader on the client, by `type`, that returns a `MessageStream`:

```ts
socket.channel(topic, params, {
  durable: true,
  readers: { kafka: (published, refresh) => new KafkaStream(published, refresh) },
})
```

`published` is the last `phx_reader` when the read starts. `refresh()` asks the channel for a fresh reader: fail the read after it (`callbacks.failed`), and the channel reads again with the new one. `callbacks.passed(number)` tells the client a number is no message, as an S2 command record is. A `MessageStream` may deliver a message again: the client drops any at or before the last one it delivered. A type the client has no reader for is logged to the console, and its messages are not read.

## Tests

```sh
pnpm install
pnpm test
mix test
npx jest -c phoenix_js_test/jest.config.cjs -i
```

`mix test` runs Phoenix 1.8.16's channel tests (`test/phoenix/integration`, `test/phoenix/test/channel_test.exs`) and its V2 serializer test, as Phoenix ships them, with three changes: each test channel adds `use PhoenixDurableChannel.Channel`, each test endpoint's socket uses `PhoenixDurableChannel.Serializer` for vsn 2.0.0, and the log assertion names that serializer. When Phoenix releases, copy its files over and make the same three changes; `diff` against Phoenix's tag shows exactly them.

`phoenix_js_test/` runs Phoenix 1.8.16's `assets/test` against this client, through `phoenix_js_test/js/phoenix`, which re-exports it. Most of those tests reach into phoenix.js's private members (`socket.conn`, `channel.joinPush`, `trigger()`, its timers and transport handlers), which this client does not have. `phoenix_js_test/internals.json` lists each such test with what it reaches into. The run fails if any other test fails, or if a listed test starts passing. `PHOENIX_JS_TEST_RECORD=1` rewrites the list, keeping the reasons already written; a new entry needs its reason written by hand.

`pnpm lint` type checks and fails on any comment in the TypeScript and JavaScript, and `mix test` fails on any comment in the Elixir, outside Phoenix's vendored tests: what the code must do goes in a test.

The tests in `integration_test/` run the client in Chromium against `integration_test/phoenix_app`, a Phoenix 1.8.16 app on this library, with MongoDB, S2 Lite and the `durable-streams-cloudflare` worker on wrangler. `integration_test/support/phoenix-server.ts` starts them all; it needs Elixir and the `s2` CLI from mise. Coverage of `assets/js` must stay at 100%.

## Jepsen

```sh
jepsen/run.sh --store mongo --transport websocket --time-limit 180
```

`--store` is `mongo`, `s2` or `durable-streams`; `--transport` is `websocket` or `longpoll`; `--faults` is any of `partition,kill,pause,cut-off-clients`.

A [Jepsen](https://jepsen.io) test of durable messages under faults. Three Docker nodes run `jepsen/phoenix_app`, a Phoenix app with one durable channel on this library and nothing else, as one Erlang cluster, sharing one store. Clients are this library in Chromium (`jepsen/driver`), over WebSocket or LongPoll, behind a proxy that sends each new connection to a random node. Each client pushes a durable `add {value, position}`, and the server appends each to that client's stream.

Faults: partitions between nodes, `kill -9`, `SIGSTOP`, and cutting a node off from the clients without closing their sockets. The checker requires, for every client:

- its stream holds its pushes in the order pushed, from the first, with every acknowledged one;
- it was delivered exactly its stream: every message once, in order, none missing;
- no push was answered with an error.

A push may be in the stream twice in a row, when its channel process died between running it and the ledger's record; the checker counts these as `:reruns`. Results are in `jepsen/store`. It needs Docker, and Leiningen from `jepsen/mise.toml`.

## Formal verification

```sh
verification/check.sh
```

TLA+ models of the socket, channel and push lifecycle, the client's durable pushes, reading the server's durable messages through the channel and from a store, the socket's push buffer, and numbering on Durable Streams. The XState machines are in `assets/js/phoenix_durable_channel/machines/`, one per file. The `verify-phoenix-durable-channel` skill in `.agents/skills` lists the requirements every change is checked against, these models among them.
