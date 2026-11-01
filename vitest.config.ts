import { playwright } from "@vitest/browser-playwright"
import { defineConfig } from "vitest/config"

// Every test runs in real Chromium against a real Phoenix server (test/server, the same
// Phoenix, Cowboy and WebSockAdapter versions as the backend), which globalSetup starts.
export default defineConfig({
  test: {
    globalSetup: ["test/support/phoenix-server.ts"],
    include: ["test/**/*.browser.test.ts"],
    testTimeout: 20_000,
    browser: {
      enabled: true,
      headless: true,
      provider: playwright(),
      instances: [{ browser: "chromium" }],
      screenshotFailures: false,
    },
    coverage: {
      provider: "v8",
      include: ["src/**/*.ts"],
      thresholds: { 100: true },
    },
  },
})
