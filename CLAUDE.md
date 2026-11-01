# AGENTS.md

The README is the source of truth: the layout, setup, tests, Jepsen and formal verification are there.

- Type check: `pnpm lint`. Tests with 100% coverage: `pnpm test`. TLA+: `verification/check.sh`.
- Any change to `assets/js/phoenix_durable_channel/` or `lib/` must keep the TLA+ models in step: see `.agents/skills/verify-phoenix-durable-channel`.
- No mocks, stubs or fakes in tests: they run in real Chromium against a real Phoenix app and real stores.
- This library knows Phoenix channels and durable messages only, never an app's concepts.
