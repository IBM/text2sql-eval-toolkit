import { describe, expect, it } from "vitest";

import { parseLocation, routes } from "./routes";

/**
 * The Eval Playground is where someone lands on a specific record to argue
 * about a specific score, so it is the view most worth being able to link to.
 */
describe("eval playground routes", () => {
  it("builds the bare view", () => {
    expect(routes.run()).toBe("/run");
  });

  it("builds a benchmark address", () => {
    expect(routes.run("spider_dev")).toBe("/run/spider_dev");
  });

  it("builds a record address", () => {
    expect(routes.run("spider_dev", "1490")).toBe(
      "/run/spider_dev/record/1490",
    );
  });

  it("carries the pipeline in the query string", () => {
    expect(routes.run("spider_dev", "1490", "modelA-greedy")).toBe(
      "/run/spider_dev/record/1490?pipeline=modelA-greedy",
    );
  });

  it("omits the record when no benchmark is named", () => {
    // A record id means nothing without the benchmark it belongs to.
    expect(routes.run(null, "1490")).toBe("/run");
  });

  it("round-trips a record address", () => {
    const match = parseLocation(routes.run("spider_dev", "1490"));
    expect(match.view).toBe("runEvaluation");
    expect(match.benchmarkId).toBe("spider_dev");
    expect(match.recordId).toBe("1490");
  });

  it("round-trips a benchmark address", () => {
    const match = parseLocation(routes.run("archer_en_dev"));
    expect(match.benchmarkId).toBe("archer_en_dev");
    expect(match.recordId).toBeNull();
  });

  it("survives ids that need encoding", () => {
    const recordId = "6627652a/7532 x";
    const match = parseLocation(routes.run("spider_dev", recordId));
    expect(match.recordId).toBe(recordId);
  });

  it("survives a pipeline id with a colon and slash", () => {
    // Pipeline ids embed the model name: wxai:openai/gpt-oss-120b-...
    const pipeline = "wxai:openai/gpt-oss-120b-greedy-zero-shot-chatapi";
    const url = routes.run("spider_dev", "1", pipeline);
    const query = new URLSearchParams(url.split("?")[1]);
    expect(query.get("pipeline")).toBe(pipeline);
  });

  it("flags a mangled address rather than opening a different record", () => {
    const match = parseLocation("/run/spider_dev/rec/1490");
    expect(match.view).toBe("runEvaluation");
    expect(match.notFound).toBe(true);
  });

  it("still parses the bare view", () => {
    expect(parseLocation("/run").view).toBe("runEvaluation");
    expect(parseLocation("/run").benchmarkId).toBeNull();
  });
});

describe("benchmarks page", () => {
  it("has an address of its own", () => {
    // It was a slide-out panel, so there was nothing to link to or open in a
    // new tab.
    expect(routes.benchmarks()).toBe("/benchmarks");
    expect(parseLocation("/benchmarks").view).toBe("benchmarks");
  });

  it("does not collide with a single benchmark", () => {
    expect(parseLocation("/benchmark/spider_dev").view).toBe("benchmark");
    expect(parseLocation("/benchmarks").benchmarkId).toBeNull();
  });
});

describe("judge config in the playground address", () => {
  it("is absent until a verdict is showing", () => {
    expect(routes.run("archer_en_dev", "6627652a7532", "wxai:m-greedy")).toBe(
      "/run/archer_en_dev/record/6627652a7532?pipeline=wxai%3Am-greedy",
    );
  });

  it("rides alongside the pipeline once one is", () => {
    const url = routes.run(
      "archer_en_dev",
      "6627652a7532",
      "wxai:m-greedy",
      "llm_judge_claude",
    );
    expect(url).toContain("pipeline=wxai%3Am-greedy");
    expect(url).toContain("judge=llm_judge_claude");
    expect(new URL(url, "https://x.test").searchParams.get("judge")).toBe(
      "llm_judge_claude",
    );
  });

  it("survives a config name that needs encoding", () => {
    const url = routes.run("b", "r", null, "judge config/v2");
    expect(new URL(url, "https://x.test").searchParams.get("judge")).toBe(
      "judge config/v2",
    );
  });

  it("can name a judge with no pipeline chosen", () => {
    expect(routes.run("b", "r", null, "cfg")).toBe("/run/b/record/r?judge=cfg");
  });
});
