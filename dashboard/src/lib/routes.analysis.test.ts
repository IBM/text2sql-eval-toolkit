import { describe, expect, it } from "vitest";

import { parseLocation, routes } from "./routes";

/**
 * The analysis views, addressed with and without a benchmark.
 *
 * `/errors` used to redirect to `/benchmark/<whichever loaded first>/errors`,
 * so opening error analysis showed numbers for a benchmark the reader had not
 * asked about. Both forms are real addresses now: the bare one asks.
 */
const BENCHMARK = "spider_dev";

describe("building analysis addresses", () => {
  it("names the benchmark when there is one", () => {
    expect(routes.insights(BENCHMARK)).toBe("/benchmark/spider_dev/insights");
    expect(routes.compare(BENCHMARK)).toBe("/benchmark/spider_dev/compare");
    expect(routes.errors(BENCHMARK)).toBe("/benchmark/spider_dev/errors");
    expect(routes.profileCompare(BENCHMARK)).toBe(
      "/benchmark/spider_dev/compare/profile",
    );
  });

  it("omits it when there is not", () => {
    for (const build of [routes.insights, routes.compare, routes.errors]) {
      expect(build()).not.toContain("/benchmark/");
      expect(build(null)).toBe(build());
      expect(build(undefined)).toBe(build());
    }
    expect(routes.insights()).toBe("/insights");
    expect(routes.compare()).toBe("/compare");
    expect(routes.errors()).toBe("/errors");
    expect(routes.profileCompare()).toBe("/compare/profile");
  });

  it("keeps error filters on the benchmark-less form", () => {
    expect(routes.errors(null, { pipeline: "p", value: "0" })).toBe(
      "/errors?pipeline=p&value=0",
    );
  });
});

describe("resolving analysis addresses", () => {
  it("reads the benchmark-scoped forms", () => {
    expect(parseLocation("/benchmark/spider_dev/insights")).toMatchObject({
      view: "toolkitInsights",
      benchmarkId: BENCHMARK,
    });
    expect(parseLocation("/benchmark/spider_dev/compare/profile")).toMatchObject(
      { view: "profileCompare", benchmarkId: BENCHMARK },
    );
  });

  it("reads the benchmark-less forms with no benchmark", () => {
    for (const [path, view] of [
      ["/insights", "toolkitInsights"],
      ["/compare", "pipelineCompare"],
      ["/compare/profile", "profileCompare"],
      ["/errors", "errorAnalysis"],
    ] as const) {
      const match = parseLocation(path);
      expect(match.view).toBe(view);
      expect(match.benchmarkId).toBeNull();
      expect(match.notFound).toBe(false);
    }
  });

  it("does not let /compare swallow /compare/profile", () => {
    // The one-segment rule would match the two-segment path and lose its
    // second half, sending profile compare to pipeline compare.
    expect(parseLocation("/compare/profile").view).toBe("profileCompare");
  });

  it("round-trips every form through the builder and the parser", () => {
    for (const build of [routes.insights, routes.compare, routes.errors]) {
      expect(parseLocation(build()).benchmarkId).toBeNull();
      expect(parseLocation(build(BENCHMARK)).benchmarkId).toBe(BENCHMARK);
    }
  });

  it("still refuses a mangled path under these prefixes", () => {
    expect(parseLocation("/compare/nonsense").notFound).toBe(true);
    expect(parseLocation("/insights/extra").notFound).toBe(true);
  });
});
