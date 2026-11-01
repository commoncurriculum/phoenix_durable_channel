import { playwright } from "@vitest/browser-playwright"
import { defineConfig } from "vitest/config"

// Every test runs in real Chromium against a real Phoenix app (integration_test/phoenix_app),
// which globalSetup starts.
export default defineConfig({
  test: {
    globalSetup: ["integration_test/support/phoenix-server.ts"],
    include: ["integration_test/**/*.browser.test.ts"],
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
      include: ["assets/js/**/*.ts"],
      thresholds: { 100: true },
    },
  },
})
