---
name: verify-phoenix-durable-channel
description: Model-check the JavaScript client (assets/js/phoenix_durable_channel) and the Elixir library (lib) with TLA+. Use when a change touches socket.ts, channel.ts, push.ts, log.ts, stream.ts, machines/, the transport actor in actors.ts or lib/, or when asked whether reconnects, rejoins, push answers or durable-message ordering are still guaranteed.
---

# Verify the Phoenix client

`verification/PhoenixDurableChannel.tla` models one `Socket`, one `Channel` on it and two pushes, against one Phoenix server, in every order the network, the server, the timers and the app can produce. A server channel process outlives a connection that drops: it runs the frames it had already received, and its replies go nowhere, until it sees the socket is gone. TLC checks every reachable state, up to 3 faults (drops, crashes, refused joins, join timeouts) and 2 connect/disconnect calls.

## The rules

Each rule is a promise the client makes, and the harm if it breaks:

| Rule                               | Promise                                                                                                | Harm if broken                                                                                           |
| ---------------------------------- | ------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------- |
| `ReconnectsUnlessAsked`            | The socket stays disconnected only when the app called `disconnect()`.                                 | The app sits offline after a deploy or an idle close (phoenix.js 1.7 stops for good on close code 1000). |
| `WaitingPushCanBeAnswered`         | A push waiting for its reply is on the join and connection that can still answer it.                   | A save waits its full timeout for a reply that cannot come.                                              |
| `AnsweredOnce`                     | Each push is answered once.                                                                            | A save is reported as both failed and saved.                                                             |
| `HandledOnce`                      | The server runs each push at most once, whichever of its processes runs it.                            | A save applied twice.                                                                                    |
| `HandledInOrderOnAJoin`            | The pushes one join carries run in the order the app pushed them.                                      | A later edit applied before an earlier one.                                                              |
| `ClosedOnlyWhenLeft`               | A channel the app still wants is never closed for good.                                                | Updates stop arriving with no error, until a reload.                                                     |
| `ServerForgetsClosedChannel`       | After the app leaves, the server keeps no channel process for it.                                      | Leaked processes on the server, still receiving broadcasts.                                              |
| `JoinedIsReal`                     | A channel that says it is joined is joined on the server, or the frame saying otherwise is on its way. | The app believes it is subscribed when it is not.                                                        |
| `NoChannelWaitsOnAnOpenConnection` | Once the connection opens, no channel is still waiting for it.                                         | A channel never rejoins after a reconnect.                                                               |
| `RejoinsWhenItCan` (liveness)      | A channel the app wants, on a socket the app wants, ends up joined.                                    | Same as above, over time.                                                                                |
| `EveryPushAnswered` (liveness)     | Every push is answered: ok, error or timeout.                                                          | A caller waits forever.                                                                                  |

## Run it

```sh
verification/check.sh
```

It needs Java 11 or later. It downloads TLC 1.7.4 once (checksum pinned) to `~/.cache/`, or uses `TLA2TOOLS=/path/to/tla2tools.jar`. It checks all six modules (`PhoenixDurableChannel`, `DurableMessages`, `ChannelRead`, `SocketBuffer`, `StreamReader`, `DurableStreamsNumbering`), and for each does two things:

1. Checks every rule, plus deadlock, on the model as it is. This must print `No error has been found`.
2. For each rule, turns on the `Broken` change it guards against and confirms the rule breaks. A rule that stops breaking has stopped checking anything. The changes, all in `PhoenixDurableChannel.tla`:

| `Broken`               | The code change it stands for                                          | Breaks                                                 |
| ---------------------- | ---------------------------------------------------------------------- | ------------------------------------------------------ |
| `closeIsFinal`         | `TRANSPORT_CLOSE` goes to `disconnected` (phoenix.js 1.7 on code 1000) | `ReconnectsUnlessAsked`                                |
| `pushesWaitOnLoss`     | `lose` no longer times out sent pushes                                 | `WaitingPushCanBeAnswered`                             |
| `replyAfterTimeout`    | `pushMachine` takes `REPLY` after `timeout`                            | `AnsweredOnce`                                         |
| `staleJoinReply`       | `receiveReply` takes a join reply outside `joining.awaitingReply`      | `ClosedOnlyWhenLeft`                                   |
| `noJoinRefCheck`       | `isMember` stops dropping frames for older joins                       | `ClosedOnlyWhenLeft`                                   |
| `noLeaveOnJoinTimeout` | the join timeout no longer sends a leave (phoenix.js 1.7)              | `ServerForgetsClosedChannel`                           |
| `joinedIgnoresError`   | `joined` loses its `SERVER_ERROR` transition                           | `JoinedIsReal`                                         |
| `noRejoinOnOpen`       | `errored` loses its `SOCKET_OPEN` transition                           | `NoChannelWaitsOnAnOpenConnection`, `RejoinsWhenItCan` |
| `noPushTimer`          | `pushMachine` loses its `sleep`                                        | `EveryPushAnswered`                                    |
| `rebufferOnLoss`       | `lose` buffers sent pushes to go out again on the next join            | `HandledOnce`                                          |
| `flushReversed`        | `hooks.joined` sends the buffered pushes in another order              | `HandledInOrderOnAJoin`                                |
| `replacedCloses`       | `replaced/1` on the server sends only `phx_close`, not `phx_error` too | `ClosedOnlyWhenLeft`                                   |

Pushes on different joins have no order: the dropped connection's process can run its pushes after the new one runs later ones. Durable pushes are for that.

Read every failing trace with the pstack `/statechart` skill: it turns TLC's counterexample into the shortest path in plain words. `verification/view.ts` is the view to give it (it labels the connection and the channel in the app's words). `/statechart` caps its chart at 500 states, so for a drawing of the whole lifecycle use a small config: `MaxFaults = 1`, `MaxUserSteps = 1`, `CONSTRAINT NoPush`, invariants only.

## Does my change touch the verified code?

The model is a hand model, so it only describes the code while the two agree. Each action names the code it stands for:

| Model action                                                | Code                                                                                                                                                                                                                                                                                                                                   |
| ----------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `YouConnect`, `YouDisconnect`                               | `socket.ts` `Socket.connect`, `Socket.disconnect`; `socketMachine` `CONNECT`, `DISCONNECT`, `live.open` → `closing` → `disconnected`                                                                                                                                                                                                   |
| `ConnectionOpens`                                           | `socketMachine` `live.connecting` `TRANSPORT_OPEN`; `hooks.opened` → `Channel.socketOpened` → `channelMachine` `SOCKET_OPEN` (in `joining.awaitingSocket` and `errored`)                                                                                                                                                               |
| `ConnectionDrops`                                           | `socketMachine` `live` `TRANSPORT_CLOSE` and `awaitingHeartbeat` timeout → `waitingToReconnect`; `live.open` exit → `hooks.connectionLost` → `SOCKET_CLOSED`; the `transport` actor in `actors.ts` detaching handlers                                                                                                                  |
| `ReconnectTimerFires`                                       | `socketMachine` `waitingToReconnect` sleep                                                                                                                                                                                                                                                                                             |
| `ChannelLosesConnection`                                    | `channelMachine` `SOCKET_CLOSED` in `joining`, `joined`, `errored.awaitingRejoin`, `leaving.awaitingReply`                                                                                                                                                                                                                             |
| `YouJoin`, `JoinTimesOut`, `RejoinTimerFires`               | `Channel.join`; `channelMachine` `unjoined` `JOIN`, `joining.awaitingReply` sleep (answers timeout, sends leave), `errored.awaitingRejoin` sleep                                                                                                                                                                                       |
| `YouLeave`, `LeaveTimesOut`                                 | `Channel.leave`; `channelMachine` `LEAVE` in each state, `leaving`                                                                                                                                                                                                                                                                     |
| `YouPush`, `PushTimesOut`, `Flush`                          | `Channel.push`, `Channel.canPush`, `hooks.joined`; `push.ts` `pushMachine`                                                                                                                                                                                                                                                             |
| `LoseSent`, `ClosePending`                                  | `channelMachine` `lose` action (`errored` and `leaving` entry) → `hooks.connectionLost`; `hooks.closed` → `CHANNEL_CLOSED`                                                                                                                                                                                                             |
| `JoinReplyArrives`, `LeaveReplyArrives`, `PushReplyArrives` | `Channel.receiveReply`; `pushMachine` `REPLY`                                                                                                                                                                                                                                                                                          |
| `CloseArrives`, `ErrorArrives`, `Member`                    | `Channel.receiveMessage` (`phx_close`, `phx_error`), `Channel.isMember`                                                                                                                                                                                                                                                                |
| `Server*` actions                                           | Phoenix 1.7.21 `Phoenix.Socket.handle_in` (duplicate join closes the old channel; unmatched topic reply has no join_ref) and channel exit (`phx_error`, `phx_close`)                                                                                                                                                                   |
| `ConnectionGone`, `OldRuns`, `OldStops`                     | a dropped connection's channel process: it keeps the frames it had received (a half-open WebSocket, a LongPoll session) and runs them, until a heartbeat timeout or the end of the long-poll window stops it. A join it runs late replaces the current process (`replace/1`, `replaced/1` in `lib/phoenix_durable_channel/channel.ex`) |

Each action is one step because the code runs it as one: actions only queue effects, and `Socket`/`Channel` run the queue right after XState commits the state, before any other frame or timer can arrive.

Not modelled as their own steps: LongPoll and the WebSocket-to-LongPoll fallback (a fallback is `ConnectionDrops` then a reconnect on another transport, and a lost poll ends the connection), `@supergrain/activity` suspend and resume (the library calling `disconnect()` and `connect()`, which are `YouDisconnect` and `YouConnect`), heartbeats beyond "an unanswered heartbeat drops the connection", Presence, the serializer, several channels, more than one dropped connection's process at a time, and real time. Those are covered by the browser tests in `test`.

## Durable messages from the client

`verification/DurableMessages.tla` models the client's durable pushes on one channel: `logMachine` in `assets/js/phoenix_durable_channel/machines/log.ts` on the client, `join/3`, `replace/1`, `handle_durable/4` and `sync/2` in `lib/phoenix_durable_channel/channel.ex` and a ledger on the server. The connection and the join are one switch (`PhoenixDurableChannel.tla` checks the rejoin itself). A drop loses the frames on their way to the client, but the server's process lives on and runs what it had received; a durable join waits for it to stop, or kills it after a timeout. The server runs a push (`handle_in`) and records it (`put_received`) as two steps, and a process can die between them. The ledger keeps the replies of the last `Kept` numbers and the client has at most `Window` unanswered (64 and 32 in the code; the rules need only `Window <= Kept`). A ledger is keyed by one client, so a reload is another client and nothing here reloads. TLC checks every order, up to 4 app pushes and 2 faults (drops, crashes between running and recording, replace timeouts, a late join replacing the current process, sync timeouts, `handle_in` asking for a retry), with `Window = Kept = 2`.

| Rule                       | Promise                                                                                  | Harm if broken                                                                    |
| -------------------------- | ---------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------- |
| `HandledInOrder`           | The server's `handle_in` runs the durable pushes in the order pushed.                    | An action applied after a later one.                                              |
| `HandledOnceUnlessCrashed` | Each durable push runs once, but for a process that died between running and recording.  | An action applied twice with no crash to explain it.                              |
| `AnsweredOnce`             | Each durable push is answered once.                                                      | A save reported twice; in the code, a crash.                                      |
| `AnsweredAsHandled`        | Every answer, a resend's included, is the one `handle_in` gave that push.                | A save reported as saved that never happened, or a refused one reported as saved. |
| `OnlyTakenAreAnswered`     | A push the server did not take (a retry, or one after it) is sent again, never answered. | An action dropped as if the server refused it.                                    |
| `CountIsRecorded`          | Every number the ledger counts has its reply recorded.                                   | A resend answered with no reply, or with another number's.                        |
| `RepliesKept`              | A resend is always of a number whose reply the ledger still keeps.                       | A failed save answered as saved.                                                  |
| `CatchesUp` (liveness)     | Once faults stop, every durable push is answered.                                        | A save that never arrives.                                                        |

| `Broken`            | The code change it stands for                                                        | Breaks                     |
| ------------------- | ------------------------------------------------------------------------------------ | -------------------------- |
| `noReplace`         | a durable join no longer calls `replace/1`, so two processes run the client's pushes | `HandledOnceUnlessCrashed` |
| `noDedupe`          | `handle_durable` runs `handle_in` for a number it has already received               | `HandledOnceUnlessCrashed` |
| `noWindow`          | `sendWaiting` sends every waiting entry, not at most `DURABLE_WINDOW`                | `RepliesKept`              |
| `countBeforeRecord` | the handler moves the ledger's count itself, before `put_received` records the reply | `CountIsRecorded`          |
| `noResend`          | `applySync` sends only newly numbered entries, not those from an earlier join        | `CatchesUp`                |
| `historicIsOk`      | `handle_durable` answers a resend "ok" instead of the reply the ledger kept          | `AnsweredAsHandled`        |
| `retryIsAnswer`     | the log's `REPLY` takes a `phx_not_taken` refusal as an answer (`notTaken` guard)    | `OnlyTakenAreAnswered`     |
| `unmatchedIsAnswer` | Phoenix's "unmatched topic" reply answers a durable push                             | `OnlyTakenAreAnswered`     |

| Model action                        | Code                                                                                                                         |
| ----------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| `Joins`, `ReplaceKills`             | `Channel` hook `joined` → `CHANNEL_JOINED`, `syncing` entry `sync`; `join/3` → `replace/1` and `stop/1` (`@replace_timeout`) |
| `Drops`, `ServerCrashes`            | `connectionLost` → `CHANNEL_LEFT` → `away`; the process dying between `handle_in` and `put_received`                         |
| `AppPushes`                         | `Channel.push(..., { durable: true })` → `Log.push` → `PUSH`: queued, then `sendWaiting` in `synced`                         |
| `SyncTimesOut`                      | the `phx_sync` push's timeout → `SYNC_FAILED` → `backingOff` → `syncing`                                                     |
| `SyncReplyArrives`, `AnswerArrives` | `SYNCED {received}` (`applySync`); `REPLY` then `sendWaiting`, with the `notTaken` guard → `backingOff` in `synced`          |
| `ServerTakes`, `ServerRecords`      | `handle_durable/4`: `received/1`, `run/2` (`handle_in`); `record/4` → `put_received/3`; `sync/2`                             |
| `ServerCannotTake`                  | `handle_in/3` returning `{:retry, reason, socket}` in `handle_durable/4`                                                     |
| `OldTakes`, `OldRecords`            | the replaced process running its mailbox before it handles `:phx_replaced`                                                   |

## Durable messages from the server, through the channel

`verification/ChannelRead.tla` models a channel reading the server's durable messages through Phoenix, the store `"phoenix"`: `channelReadMachine` in `assets/js/phoenix_durable_channel/machines/channel-read.ts`, `ChannelStream` in `assets/js/phoenix_durable_channel/stream.ts`, `readerMachine` around them, `readStream` in `assets/js/phoenix_durable_channel/machines/channel.ts`, and `read/3` and `push_durable/4` in `PhoenixDurableChannel.Channel` over `PhoenixDurableChannel.Stream.Memory`. Each node keeps its own store, an epoch of its own, and a restart or a forgotten topic starts a new one; each join may reach any node, so a client can leave an epoch and come back to it. The server pushes each message to whichever read is listening; between reads a push is missed. A read sends `phx_read {after}`; the server resends what it kept after `after`, a page at a time, then replies `{sent, epoch}`. The read has no timeout. The first join's `phx_reader` says where to start: `last`, or the app's `after` of 0. Two nodes, up to 2 messages in each epoch and 2 faults (drops, the store forgetting, a store starting again), pages of 1.

| Rule                      | Promise                                                                                                                             | Harm if broken                         |
| ------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------- |
| `DeliveredOnceInOrder`    | The bindings get each message of an epoch once, in the store's order, also after the reader leaves the epoch and comes back.        | A patch applied twice or out of order. |
| `NothingLostButForgotten` | In every epoch, up to where the client got, a message it passed over is one the store no longer had, or from before its first join. | A silently missing patch.              |
| `CatchesUp` (liveness)    | Once faults stop, the read is live in the epoch of the node it is on, and the next message it expects is that store's next.         | Updates that stop with no error.       |

| `Broken`            | The code change it stands for                                                                | Breaks                    |
| ------------------- | -------------------------------------------------------------------------------------------- | ------------------------- |
| `deliverOnArrival`  | `waiting` delivers each message as it arrives instead of holding it                          | `DeliveredOnceInOrder`    |
| `replyBeforeResend` | `read/3` replies before sending the kept messages again                                      | `NothingLostButForgotten` |
| `noGapCheck`        | `live` delivers any later number instead of ending the read                                  | `NothingLostButForgotten` |
| `noFailOnLoss`      | `connectionLost` no longer tells the read (`LOST`)                                           | `CatchesUp`               |
| `noEpochCheck`      | `READ_REPLY` loses its `otherEpoch` guard, so a reply in another epoch delivers what is held | `NothingLostButForgotten` |
| `fromStartOnReturn` | `MOVED` reads every epoch from 0, not from where the reader got to in it (`left`)            | `DeliveredOnceInOrder`    |
| `replyOnFirstPage`  | `read/3` replies after its first page, not once it has sent up to the stream's latest        | `CatchesUp`               |
| `readTimeout`       | `phx_read` has a timeout again, so a read that takes long is given up and asked again        | `CatchesUp`               |

| Model action                                                  | Code                                                                                                                                       |
| ------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `Joins`                                                       | `JOIN_REPLY` → `PUBLISHED`; `readStream` starts the `Reader` at `{after: the app's after ?? phx_reader.last, epoch: phx_reader.epoch}`     |
| `Drops`                                                       | `connectionLost` → each `ChannelListener.lost()` → `LOST` → `ended`                                                                        |
| `StartsRead`, `ReadTimesOut`                                  | `readerMachine` `reading` → `ChannelStream.read`: listen, then push `phx_read {after}` with no timeout                                     |
| `MessageArrives`                                              | `Channel.receiveMessage` → listeners → `MESSAGE`: `hold` in `waiting`; `deliver` if `isNext`, else `ended`, in `live`                      |
| `ReplyArrives`                                                | `READ_REPLY`: `otherEpoch` → `moved` → `ended`, and `MOVED` saves `left` and reads the epoch from `left` or 0; else `deliverHeld` → `live` |
| `ServerPublishes`, `ServerPublishesAfterAGap`, `OtherAppends` | `push_durable/4`; another process appending first (`publish_silently` in the tests); another process appending                             |
| `ServerTakesRead`, `ServerSendsPage`                          | `read/3`: `since/3` a page at a time, pushing each, until `sent` reaches `latest`, then the reply                                          |
| `StoreForgets`, `StoreStartsAgain`                            | a store that prunes (`:kept`); `Memory` on a node restart, or forgetting a topic no channel process watches                                |

## The socket's push buffer

`verification/SocketBuffer.tla` models `context.buffer` in `socketMachine`: frames pushed while the connection is not open wait, and go out in order as it opens. Up to 3 pushes, 2 drops and 3 connect/disconnect calls.

| Rule                          | Promise                                                                                     | Harm if broken                                                   |
| ----------------------------- | ------------------------------------------------------------------------------------------- | ---------------------------------------------------------------- |
| `WrittenOnceInOrder`          | The server reads each push once, in the order the app pushed.                               | A join or a push sent twice, or a push before the join it needs. |
| `NothingVanishes`             | A push is read by the server, lost with a dropped connection, or dropped by `disconnect()`. | A push that is never sent and never answered by its own channel. |
| `EveryPushGoesOut` (liveness) | While the app wants the connection, every push is eventually read or lost with a drop.      | A buffered join that never goes out, so the channel never joins. |

| `Broken`              | The code change it stands for                                     | Breaks               |
| --------------------- | ----------------------------------------------------------------- | -------------------- |
| `keepBuffer`          | `open` entry no longer runs `dropBuffer` after sending the buffer | `WrittenOnceInOrder` |
| `lateFlush`           | the buffer goes out after `open` is entered, not in its entry     | `WrittenOnceInOrder` |
| `sendWhileConnecting` | `PUSH` sends while `connecting` instead of buffering              | `NothingVanishes`    |
| `noFlush`             | `open` entry no longer sends the buffer                           | `EveryPushGoesOut`   |

| Model action                    | Code                                                                                        |
| ------------------------------- | ------------------------------------------------------------------------------------------- |
| `AppPushes`                     | `Socket.push` → `PUSH`: `hooks.send` in `live.open`, the `buffer` action anywhere else      |
| `AppConnects`, `AppDisconnects` | `CONNECT`; `DISCONNECT` with `dropBuffer`                                                   |
| `Opens`                         | `live.connecting` `TRANSPORT_OPEN` → `open` entry: send each buffered message, `dropBuffer` |
| `Drops`                         | `TRANSPORT_CLOSE` or a missed heartbeat → `waitingToReconnect` → `connecting`               |

## Reading durable messages from a stream

`verification/StreamReader.tla` models `readerMachine` in `assets/js/phoenix_durable_channel/machines/reader.ts`, which a channel's `followMachine` invokes once a join reply names a store other than `"phoenix"`. The server appends; each read returns the store in order from where it starts; a read can end at any moment. An S2 read starts right after `last`; a Durable Streams read starts there only when it knows where `last` ends (from the read that delivered it, or from the join reply), otherwise at the beginning, and skips every number up to the last it delivered. The read token expires; a store that refuses it gets a fresh reader from the channel (`phx_reader`) before the read fails, so the next read carries a good token. Up to 3 appends and 2 faults (failed reads, an expired token), starting from the app's `after` or the join reply's `phx_reader.last`.

| Rule                   | Promise                                                                    | Harm if broken                     |
| ---------------------- | -------------------------------------------------------------------------- | ---------------------------------- |
| `DeliveredInOrder`     | The bindings get every message after `after` once, in order, none skipped. | A patch applied twice, or missing. |
| `CatchesUp` (liveness) | Once reads stop failing, the client has every message.                     | Updates that stop with no error.   |

| `Broken`    | The code change it stands for                                                 | Breaks             |
| ----------- | ----------------------------------------------------------------------------- | ------------------ |
| `staleRead` | a read that ended can still deliver (its actor is not stopped with `reading`) | `DeliveredInOrder` |
| `fromStart` | a new read skips up to the channel's `after`, not up to `last`                | `DeliveredInOrder` |
| `noSkip`    | `DurableStream.read` delivers numbers it already delivered                    | `DeliveredInOrder` |
| `noRetry`   | `FAILED` ends the reader instead of going to `backingOff`                     | `CatchesUp`        |
| `noRefresh` | `readEvents` fails a 401 or 403 without calling `refused`                     | `CatchesUp`        |

| Model action                  | Code                                                                                                                                                        |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ReadDelivers`                | the `reading` actor (`S2Stream.read`, `DurableStream.read`) → `MESSAGE` → deliver, `last` = its number; a Durable Streams read skips numbers up to its fold |
| `ReadEnds`                    | the read calls `failed` → `FAILED` → `backingOff`; XState drops `sendBack` from the stopped actor                                                           |
| `TokenExpires`, `ReadRefused` | the token's `exp`; `readEvents` on a 401 or 403 runs `refused` (`refresh` in `readStream`: `fetchReader` → `phx_reader` → `latest`), then fails             |
| `ReadsAgain`                  | `backingOff` sleep → `reading`, input `after: context.last`; `DurableStream` starts at its `resume` offset or at `-1`                                       |

## Numbering on a Durable Streams store

`verification/DurableStreamsNumbering.tla` models `PhoenixDurableChannel.Stream.DurableStreams.append/3` on two nodes with two channel processes each, appending to one topic, up to 3 messages. Each node keeps one ETS row per topic, shared by its processes. A process reads the row, posts the next number as the append's `Stream-Seq`, and on success writes the number back: three steps, so two processes can post the same number. The store refuses a `Stream-Seq` that is not past its last; a refused process reads on and appends again. A post can fail whether or not the store took it; the process then raises and the row stays as it was.

| Rule              | Promise                                              | Harm if broken                         |
| ----------------- | ---------------------------------------------------- | -------------------------------------- |
| `NumberedInOrder` | The store holds 1, 2, 3, …: each once, none skipped. | Two patches with one number; one lost. |

| `Broken`          | The code change it stands for                                    | Breaks            |
| ----------------- | ---------------------------------------------------------------- | ----------------- |
| `noStreamSeq`     | `post/4` stops sending `Stream-Seq`                              | `NumberedInOrder` |
| `givenBeforePost` | `append/3` writes the number to the row before the post succeeds | `NumberedInOrder` |

| Model action | Code                                                                                                        |
| ------------ | ----------------------------------------------------------------------------------------------------------- |
| `Reads`      | `read/1`: the stream read to its end from the ETS row's offset, the row's number reset to the stream's last |
| `Takes`      | `append/3`: `kept/1` (`:ets.lookup`), the number after the row's `given`                                    |
| `Posts`      | `post/4`: `:ok` → `:ets.insert` of the number; `:taken` (409) → `read/1` and append again                   |
| `PostFails`  | `post/4` raising on any other status or a network error, the store having taken the message or not          |

## When the code changes

The models were last confirmed against the code in the commit that last changed this file.

1. `git diff $(git log -1 --format=%h -- .agents/skills/verify-SKILL.md) -- assets/js/phoenix_durable_channel lib`. Empty: run `check.sh` and you are done.
2. Otherwise, find each changed state, transition or hook in the table above and change the matching action so it does what the code now does. Keep one model step per code step.
3. Run `check.sh`.
   - A rule breaks on the model as it is: replay the `/statechart` path against the real code as a browser test in `test` (one step per action). If it happens there, it is a bug in the change; report it with that path. If it does not, the model is wrong; fix the model, not the rule.
   - A `Broken` change no longer breaks its rule: the change moved the protection somewhere else. Update that `Broken` branch so it removes the protection where it now lives.
   - Never weaken a rule or a bound to make it pass.
4. Only after the model matches the code at the new commit, move the sync commit above to that commit.
