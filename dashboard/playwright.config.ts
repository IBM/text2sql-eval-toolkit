import { defineConfig, devices } from "@playwright/test";

/**
 * End-to-end tests for the one goal unit tests cannot prove: that a URL, opened
 * cold in a browser that has never seen this session, reproduces the view the
 * sender was looking at.
 *
 * Everything else about shareable links is testable in isolation -- the URL
 * scheme has unit tests, the views have component tests. What none of them
 * exercise is the real path: a real router, a real server, a real page load
 * from an address bar.
 *
 * The server runs against a synthetic data root built by
 * `scripts/ci/make_e2e_fixture.py`, because the real results snapshot is ~4 GB
 * and lives on the Hub. The fixture is deterministic, which matters: these
 * tests copy a link in one context and open it in another, so the data behind
 * it must not move.
 */

const PORT = 8123;
const DATA_ROOT = process.env.E2E_DATA_ROOT ?? "../.e2e-data";

export default defineConfig({
  testDir: "./e2e",
  // A link that only works on a second try has not proven anything; retries
  // would hide exactly the flakiness worth knowing about.
  retries: 0,
  fullyParallel: true,
  reporter: process.env.CI ? [["github"], ["list"]] : [["list"]],
  use: {
    baseURL: `http://127.0.0.1:${PORT}`,
    trace: "retain-on-failure",
    // Clipboard access, for the copy-link controls.
    permissions: ["clipboard-read", "clipboard-write"],
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
  webServer: {
    // Serves dashboard/dist, so the build under test is the one that ships.
    command: [
      `python scripts/ci/make_e2e_fixture.py ${DATA_ROOT}`,
      `TEXT2SQL_DATA_ROOT=${DATA_ROOT} text2sql-eval-dashboard --port ${PORT} --no-watch-dashboard`,
    ].join(" && "),
    cwd: "..",
    url: `http://127.0.0.1:${PORT}/api/benchmarks`,
    reuseExistingServer: !process.env.CI,
    timeout: 120_000,
  },
});
