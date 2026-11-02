---
name: verify-phoenix-durable-channel
description: Check a change to phoenix_durable_channel against the owner's requirements, which are quoted here and nothing else. Use before every commit and before calling any work done.
---

# Verify phoenix_durable_channel

Every requirement below is quoted from the owner. Nothing else is a requirement: anything a reviewer or an agent proposes beyond this list is a suggestion for the owner to accept or reject, never a rule to check against. Add a requirement here only in the owner's own words, after the owner says it.

1. **supergrain.** "you use the supergrain library, read the docs, understand how it works." "an activity module in Supergrain, which is a state chart around activity ... you should also build on that as well."
2. **No setState.** "you may not use set state, that is not allowed."
3. **Effect.** "you must use effect TS. That is the way to handle concurrency."
4. **XState.** "you must use X state as the state chart library."
5. **TypeScript.** "It must be written in TypeScript."
6. **Formally verified.** "it must be formally verified", with "the pstack formal verification skill, and their state chart skill." Check: `verification/check.sh`.
7. **Same API.** "the API should not change unless it absolutely has to ... the goal should be same API, only it works ... easy to swap out what we have for this new thing."
8. **100% coverage.** "it needs to be fully tested with 100% test coverage." Check: `pnpm test`.
9. **Jepsen.** The owner chose "Real Jepsen (Clojure)". Check: `jepsen/run.sh` on every store and both transports.
10. **General-purpose.** "It simply knows about messages from Phoenix channels. I should be able to publicize this as a general purpose library, not something that's tied to our app." "Could you verify there isn't anything about v4 or our app in it? No codes, no concepts".
11. **Stores.** "Our data store options for any sort of ledger are Mongo ... S2, or durable objects, which is Cloudflare. I should not see Postgres. I should definitely see durable objects in on Wrangler ... all of those stores should be pluggable and have the same interface."
12. **Phoenix's own tests.** "does this pass all the existing phoenix tests? If not, it needs to and those need to be part of the test suite in a separate folder so we can copy/paste them over". "do you have the server phoenix tests running for durable_channel? Those are the ones I'm most concerned about." Check: `mix test`, and `npx jest -c phoenix_js_test/jest.config.cjs -i`.
13. **No comments.** "remove ALL the comments in your code. ALL of them." Check: `pnpm lint`.

The repo's `CLAUDE.md` also holds the owner's rule of no mocks, stubs or fakes in tests.

Before saying done, run the checks above and report each by number with its result.
