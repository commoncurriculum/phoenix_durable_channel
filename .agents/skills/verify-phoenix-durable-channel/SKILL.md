---
name: verify-phoenix-durable-channel
description: Check a change to phoenix_durable_channel against the owner's requirements. Use before every commit that touches assets/js, lib, test, integration_test, phoenix_js_test, verification or jepsen, and before calling any work done.
---

# Verify phoenix_durable_channel

Each requirement below is the owner's. Check every one, every time. A requirement that fails is a failure of the change, not a note for later. What the code does belongs in the tests; this list is only what the code must be built from and must pass.

## How it is built

1. **TypeScript.** Every client source under `assets/js/phoenix_durable_channel/` is `.ts`. `pnpm lint` passes.
2. **XState holds every state.** Each thing with a lifecycle (socket, channel, push, log, read, reader, follow, long poll) is a machine in `machines/`. No mutable flags or `let` that outlive one function decide behavior; a variable that does is a state the machine is missing. A machine reaches the outside only by `emit`; its actions do no side effects, and `assign` only computes context. Public states (`connectionState`, `channel.status.state`, a push's state) are the machine's tags, one on every leaf (`integration_test/machines.browser.test.ts`).
3. **Effect does the concurrency.** Timers, fetches, streams, retries and cancellation are Effect fibers, started and interrupted by the machine state that owns them. No bare `setTimeout`, `setInterval`, `new Promise` or `.then` chains.
   `grep -rnE 'setTimeout|setInterval|new Promise|\.then\(' assets/js` finds nothing.
4. **supergrain is the state the app reads.** `socket.status`, `channel.status` and `channel.log.status` are supergrain reactives, written only from machine snapshots. Activity (here, away) comes from `@supergrain/activity`'s statechart.
5. **No setState.** Nothing sets state from outside a machine: no `setState`, no assigning to a reactive outside the snapshot observer.
6. **No comments.** None in any file this repo owns. `pnpm lint` (`scripts/no-comments.ts`) fails on one in TypeScript or JavaScript, and `mix test` (`test/no_comments_test.exs`) on one in Elixir under `lib/`. Phoenix's vendored tests (`test/phoenix/`, `test/support/*_client.exs`, `phoenix_js_test/test/`) stay byte-for-byte as Phoenix ships them.
7. **General-purpose.** The library knows Phoenix sockets, channels and durable messages. No app's names or concepts anywhere: code, tests, README, Jepsen.
8. **Stores: Mongo, S2 and Durable Streams on Cloudflare Durable Objects (via Wrangler). Never Postgres.** Each is a ledger and a stream behind `PhoenixDurableChannel.Ledger` and `PhoenixDurableChannel.Stream`. A hard store is never swapped for an easier one.

## What it must pass

9. **Phoenix's own tests, from the latest Phoenix release.** `mix test` runs Phoenix's server channel tests (`test/phoenix/`) with their channels on `PhoenixDurableChannel.Channel` and their sockets on `PhoenixDurableChannel.Serializer`, over WebSocket and LongPoll. `npx jest -c phoenix_js_test/jest.config.cjs -i` runs Phoenix's JS tests against this client; the only failures allowed are those in `phoenix_js_test/internals.json`, each naming the phoenix.js private member it reaches into. A listed test that touches no private member is a gap in this client: fix the client, not the list. When Phoenix releases, copy its files over again unchanged.
10. **The phoenix.js API.** An app swaps `import { Socket } from "phoenix"` for this package and changes nothing else. A change to that API needs the owner's yes first.
11. **Old clients keep working.** A server running this library still serves phoenix.js 1.7 clients (V1 and V2 serializers) without a reload.
12. **100% coverage from real tests.** `pnpm test` runs in real Chromium against the real Phoenix app and real stores, and fails under 100% lines, branches, functions and statements. No mocks, stubs or fakes; Phoenix's own vendored tests are the only exception.
13. **TLA+.** `verification/check.sh` passes: every model holds on the code as it is, and every `Broken` variant breaks the rule it names. A change to a machine or to `lib/` changes the model of it in the same commit.
14. **Jepsen.** `jepsen/run.sh` with real Jepsen, every store and both transports, finds the history valid: each durable message handled once and in order, none lost.

## Before saying done

Run 1, 3, 9, 12 and 13, and 14 when the change touches delivery, the stores or `lib/`. Report each by number with its result.
