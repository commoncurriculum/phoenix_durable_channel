# AGENTS.md

The README is the source of truth: the layout, setup, tests, Jepsen and formal verification are there.

- Before every commit, check the change against `.agents/skills/verify-phoenix-durable-channel`: the owner's requirements, every one, every time.
- Type check: `pnpm lint`. Browser tests with 100% coverage: `pnpm test`. Phoenix's server tests: `mix test`. Phoenix's JS tests: `npx jest -c phoenix_js_test/jest.config.cjs -i`. TLA+: `verification/check.sh`.
- No comments in code. What the code must do goes in a test.
- No mocks, stubs or fakes in tests: they run in real Chromium against a real Phoenix app and real stores.
- This library knows Phoenix channels and durable messages only, never an app's concepts.
