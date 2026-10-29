import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "tests",
  timeout: 30_000,
  use: { baseURL: "http://localhost:4173", viewport: { width: 390, height: 844 } },
  webServer: { command: "node scripts/test-server.mjs", port: 4173, reuseExistingServer: false },
});
