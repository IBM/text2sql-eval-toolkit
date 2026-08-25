import { describe, expect, it } from "vitest";

import {
  FILTER_DEFAULTS,
  buildQuery,
  parseLocation,
  parseQuery,
  routes,
} from "./routes";

// Real pipeline ids contain both ':' and '/', which is the whole reason
// encoding lives in one place.
const PIPELINE = "wxai:openai/gpt-oss-120b-agentic-baseline1-3attempts";
const BENCHMARK = "bird_mini_dev_sqlite";

describe("path builders", () => {
  it("encodes pipeline ids so slashes do not create extra path segments", () => {
    const path = routes.pipeline(BENCHMARK, PIPELINE);
    expect(path).toBe(
      `/b/${BENCHMARK}/pipeline/${encodeURIComponent(PIPELINE)}`
    );
    // One segment after /pipeline/, not three.
    expect(path.split("/pipeline/")[1].includes("/")).toBe(false);
  });

  it("round-trips a pipeline id through encode and decode", () => {
    const encoded = routes.pipeline(BENCHMARK, PIPELINE).split("/pipeline/")[1];
    expect(decodeURIComponent(encoded)).toBe(PIPELINE);
  });

  it.each([
    ["with spaces", "model name with spaces"],
    ["with unicode", "モデル-日本語"],
    ["with percent", "model%20already"],
    ["with hash", "model#tag"],
    ["with question mark", "model?x=1"],
    ["with ampersand", "a&b"],
  ])("survives a pipeline id %s", (_label, pipeline) => {
    const encoded = routes.pipeline(BENCHMARK, pipeline).split("/pipeline/")[1];
    expect(decodeURIComponent(encoded)).toBe(pipeline);
  });

  it("builds the documented paths", () => {
    expect(routes.home()).toBe("/");
    expect(routes.benchmark(BENCHMARK)).toBe(`/b/${BENCHMARK}`);
    expect(routes.errors(BENCHMARK)).toBe(`/b/${BENCHMARK}/errors`);
    expect(routes.insights(BENCHMARK)).toBe(`/b/${BENCHMARK}/insights`);
    expect(routes.compare(BENCHMARK)).toBe(`/b/${BENCHMARK}/compare`);
    expect(routes.profileCompare(BENCHMARK)).toBe(`/b/${BENCHMARK}/compare/profile`);
    expect(routes.llmJudge()).toBe("/llm-judge");
    expect(routes.llmJudge("default")).toBe("/llm-judge/default");
    expect(routes.run()).toBe("/run");
  });
});

describe("query serialization", () => {
  it("omits defaults so a pristine view has a clean URL", () => {
    expect(
      buildQuery({
        metric: FILTER_DEFAULTS.metric,
        op: FILTER_DEFAULTS.op,
        disagree: false,
        page: 1,
        pageSize: 25,
      })
    ).toBe("");
  });

  it("omits null and empty values", () => {
    expect(buildQuery({ pipeline: null, q: "", value: null })).toBe("");
  });

  it("includes non-default values", () => {
    const query = buildQuery({ pipeline: PIPELINE, value: "0", page: 3 });
    const params = new URLSearchParams(query.slice(1));
    expect(params.get("pipeline")).toBe(PIPELINE);
    expect(params.get("value")).toBe("0");
    expect(params.get("page")).toBe("3");
  });

  it("round-trips every filter", () => {
    const filters = {
      pipeline: PIPELINE,
      metric: "llm_score",
      value: "0.5",
      op: "ge",
      pipeline2: "other:model/x-greedy-zero-shot-chatapi",
      metric2: "execution_accuracy",
      disagree: true,
      q: "how many customers?",
      page: 4,
      pageSize: 50,
      record: "rec-001",
    };
    const parsed = parseQuery(buildQuery(filters).slice(1));
    expect(parsed).toMatchObject(filters);
  });

  it("keeps search text with characters that need escaping", () => {
    const q = "a&b=c?d #e";
    const parsed = parseQuery(buildQuery({ q }).slice(1));
    expect(parsed.q).toBe(q);
  });

  it("treats disagree as present-only", () => {
    expect(buildQuery({ disagree: false })).toBe("");
    expect(buildQuery({ disagree: true })).toBe("?disagree=true");
    expect(parseQuery("disagree=true").disagree).toBe(true);
    expect(parseQuery("").disagree).toBe(false);
  });
});

describe("query parsing", () => {
  it("applies defaults for absent values", () => {
    const parsed = parseQuery("");
    expect(parsed.metric).toBe(FILTER_DEFAULTS.metric);
    expect(parsed.op).toBe(FILTER_DEFAULTS.op);
    expect(parsed.page).toBe(1);
    expect(parsed.pageSize).toBe(25);
  });

  it.each([
    ["page=0", 1],
    ["page=-3", 1],
    ["page=abc", 1],
    ["page=", 1],
    ["page=2.9", 2],
  ])("clamps a hostile page value (%s)", (search, expected) => {
    expect(parseQuery(search).page).toBe(expected);
  });

  it("accepts a URLSearchParams instance as well as a string", () => {
    expect(parseQuery(new URLSearchParams("q=hello")).q).toBe("hello");
  });
});

describe("parseLocation", () => {
  it("resolves the documented paths", () => {
    expect(parseLocation("/")).toMatchObject({ view: "home", notFound: false });
    expect(parseLocation(`/b/${BENCHMARK}`)).toMatchObject({
      view: "benchmark",
      benchmarkId: BENCHMARK,
    });
    expect(parseLocation(`/b/${BENCHMARK}/errors`)).toMatchObject({
      view: "errorAnalysis",
      benchmarkId: BENCHMARK,
    });
    expect(parseLocation(`/b/${BENCHMARK}/insights`)).toMatchObject({
      view: "toolkitInsights",
    });
    expect(parseLocation(`/b/${BENCHMARK}/compare`)).toMatchObject({
      view: "pipelineCompare",
    });
    expect(parseLocation(`/b/${BENCHMARK}/compare/profile`)).toMatchObject({
      view: "profileCompare",
    });
    expect(parseLocation("/llm-judge")).toMatchObject({ view: "llmJudge", configName: null });
    expect(parseLocation("/llm-judge/default")).toMatchObject({
      view: "llmJudge",
      configName: "default",
    });
    expect(parseLocation("/run")).toMatchObject({ view: "runEvaluation" });
  });

  it("decodes an encoded pipeline id back to its original form", () => {
    const match = parseLocation(routes.pipeline(BENCHMARK, PIPELINE));
    expect(match).toMatchObject({
      view: "pipeline",
      benchmarkId: BENCHMARK,
      pipelineId: PIPELINE,
    });
  });

  it("round-trips every builder through the parser", () => {
    expect(parseLocation(routes.benchmark(BENCHMARK)).benchmarkId).toBe(BENCHMARK);
    expect(parseLocation(routes.errors(BENCHMARK)).benchmarkId).toBe(BENCHMARK);
    expect(parseLocation(routes.pipeline(BENCHMARK, PIPELINE)).pipelineId).toBe(PIPELINE);
  });

  it("flags unknown paths rather than silently rendering home", () => {
    expect(parseLocation("/nope")).toMatchObject({ notFound: true });
    expect(parseLocation(`/b/${BENCHMARK}/bogus`)).toMatchObject({
      notFound: true,
      benchmarkId: BENCHMARK,
    });
  });

  it("tolerates trailing slashes and repeated separators", () => {
    expect(parseLocation(`/b/${BENCHMARK}/`)).toMatchObject({ view: "benchmark" });
    expect(parseLocation(`//b//${BENCHMARK}//`)).toMatchObject({ view: "benchmark" });
  });

  it("does not throw on a malformed percent escape", () => {
    expect(() => parseLocation("/b/%E0%A4%A")).not.toThrow();
  });
});
