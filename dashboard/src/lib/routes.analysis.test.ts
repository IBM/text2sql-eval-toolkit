import { describe, expect, it } from "vitest";

import { parseBenchmarkList, parseLocation, routes } from "./routes";

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
    // Profile compare is the exception: it pools several benchmarks, so its
    // address carries a list rather than a path segment. See below.
    expect(routes.profileCompare(BENCHMARK)).toBe(
      "/compare/profile?benchmarks=spider_dev",
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

describe("profile compare pools several benchmarks", () => {
  it("carries the whole selection, not just the last one added", () => {
    // The bug this replaced: the address named whichever benchmark was chosen
    // most recently while the view was pooling several.
    expect(routes.profileCompare(["a", "b", "c"])).toBe(
      "/compare/profile?benchmarks=a,b,c",
    );
  });

  it("accepts a single id, for the tab strip and for old callers", () => {
    expect(routes.profileCompare("spider_dev")).toBe(
      "/compare/profile?benchmarks=spider_dev",
    );
  });

  it("names none when nothing is selected", () => {
    expect(routes.profileCompare()).toBe("/compare/profile");
    expect(routes.profileCompare([])).toBe("/compare/profile");
    expect(routes.profileCompare(null)).toBe("/compare/profile");
  });

  it("round-trips a selection through the query string", () => {
    for (const ids of [["a"], ["a", "b"], ["bird_mini_dev_postgres", "beaver"]]) {
      const url = routes.profileCompare(ids);
      expect(parseBenchmarkList(url.split("?")[1] ?? "")).toEqual(ids);
    }
  });

  it("encodes an id so a comma in one cannot split the list", () => {
    const url = routes.profileCompare(["a,b", "c"]);
    expect(url).toBe("/compare/profile?benchmarks=a%2Cb,c");
    expect(parseBenchmarkList(url.split("?")[1])).toEqual(["a,b", "c"]);
  });

  it("reads an absent or empty parameter as no selection", () => {
    expect(parseBenchmarkList("")).toEqual([]);
    expect(parseBenchmarkList("other=1")).toEqual([]);
    expect(parseBenchmarkList("benchmarks=")).toEqual([]);
  });

  it("drops a malformed entry rather than throwing", () => {
    // A bad escape must cost that one id, not blank the whole view.
    expect(parseBenchmarkList("benchmarks=good,%E0%A4%A")).toEqual(["good"]);
  });

  it("still resolves the older benchmark-scoped address", () => {
    // Links to it exist; it seeds the selection with that one benchmark.
    expect(parseLocation("/benchmark/spider_dev/compare/profile")).toMatchObject(
      { view: "profileCompare", benchmarkId: "spider_dev" },
    );
  });
});
