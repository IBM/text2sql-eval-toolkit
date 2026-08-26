import { describe, expect, it } from "vitest";

import { clampToAvailable } from "./metricInsightsSelect";

/**
 * The rule three views rely on to keep a metric selection valid. It replaced an
 * effect that corrected the selection after the fact, so these tests are what
 * make that rewrite safe.
 */
describe("clampToAvailable", () => {
  const AVAILABLE = ["execution_accuracy", "subset_non_empty_execution_accuracy", "llm_score"];

  it("keeps a choice that is still offered", () => {
    expect(clampToAvailable("llm_score", AVAILABLE)).toBe("llm_score");
  });

  it("falls back when the choice is no longer offered", () => {
    expect(clampToAvailable("gone", AVAILABLE)).toBe("execution_accuracy");
  });

  it("uses the requested fallback position", () => {
    expect(clampToAvailable("gone", AVAILABLE, 1)).toBe(
      "subset_non_empty_execution_accuracy"
    );
  });

  it("falls back to the first entry when the requested position is past the end", () => {
    expect(clampToAvailable("gone", ["only_one"], 5)).toBe("only_one");
  });

  it("leaves the choice alone while nothing is available yet", () => {
    // A slow definitions fetch must not reset what the user picked.
    expect(clampToAvailable("llm_score", [])).toBe("llm_score");
  });

  it("is stable: clamping an already-clamped value changes nothing", () => {
    const once = clampToAvailable("gone", AVAILABLE);
    expect(clampToAvailable(once, AVAILABLE)).toBe(once);
  });
});
