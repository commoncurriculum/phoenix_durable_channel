import { playwright } from "@vitest/browser-playwright"
import { defineConfig } from "vitest/config"

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
