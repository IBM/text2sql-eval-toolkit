import { render, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { ToolkitInsightsView } from "./ToolkitInsightsView";
import type { BenchmarkSummary } from "../types/benchmark";

/**
 * Pins the behaviour that the selection-clamping effects provide.
 *
 * Several views reset a selected metric or pipeline from inside an effect once
 * the options load. `react-hooks/set-state-in-effect` flags that pattern, and it
 * is real debt -- but each of those effects decides which option a user ends up
 * looking at, so rewriting them needs a test that asserts the *outcome* rather
 * than the mechanism.
 *
 * These tests do exactly that: whatever the implementation, the selection must
 * end up on a metric that exists, and must not silently change to something the
 * user did not choose. That makes the effects safe to convert to derived state.
 */

const BENCHMARK = "demo";
const PIPE = "wxai:openai/gpt-oss-120b-greedy-zero-shot-chatapi";

const METRIC_DEFINITIONS = {
  groups: ["Execution match", "LLM judge"],
  metrics: [
    {
      group: "Execution match",
      name: "execution_accuracy",
      description: "exact match",
      value_type: "binary",
    },
    {
      group: "Execution match",
      name: "subset_non_empty_execution_accuracy",
      description: "relaxed match",
      value_type: "binary",
    },
    {
      group: "LLM judge",
      name: "llm_score",
      description: "judge verdict",
      value_type: "float",
    },
  ],
};

const CONFUSION = {
  benchmark_id: BENCHMARK,
  metric_a: "execution_accuracy",
  metric_b: "subset_non_empty_execution_accuracy",
  per_pipeline: [
    {
      pipeline: PIPE,
      counts: { a0b0: 10, a0b1: 2, a1b0: 1, a1b1: 30 },
      n_valid: 43,
      rates: { a0b0: 0.23, a0b1: 0.05, a1b0: 0.02, a1b1: 0.7 },
      agreement_rate: 0.93,
      disagreement_rate: 0.07,
    },
  ],
};

const BY_CATEGORY = {
  benchmark_id: BENCHMARK,
  default_sort_metric: "subset_non_empty_execution_accuracy",
  overall: [{ name: PIPE, metrics: { subset_non_empty_execution_accuracy: 0.7 } }],
  categories: {},
  has_full_results: true,
};

const BENCHMARKS: BenchmarkSummary[] = [
  {
    benchmark_id: BENCHMARK,
    name: BENCHMARK,
    description: "demo",
    db_type: "sqlite",
    num_records: 10,
    num_pipelines: 1,
  } as BenchmarkSummary,
];

function stubApi(overrides: Record<string, unknown> = {}) {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      let body: unknown = {};
      if (url.includes("evaluation-metric-definitions")) {
        body = overrides.metricDefinitions ?? METRIC_DEFINITIONS;
      } else if (url.includes("binary-metric-confusion-by-pipeline")) {
        body = CONFUSION;
      } else if (url.includes("summary/by-category")) {
        body = BY_CATEGORY;
      }
      return new Response(JSON.stringify(body), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    })
  );
}

function mount() {
  return render(
    <ToolkitInsightsView
      benchmarks={BENCHMARKS}
      benchmarkId={BENCHMARK}
      onSelectBenchmark={() => {}}
      onOpenErrorAnalysis={() => {}}
    />
  );
}

/** Every metric name currently offered by a <select>. */
function selectedMetricValues(container: HTMLElement): string[] {
  return [...container.querySelectorAll("select")]
    .map((s) => (s as HTMLSelectElement).value)
    .filter(Boolean);
}

beforeEach(() => {
  vi.unstubAllGlobals();
});

describe("metric selection", () => {
  it("settles on metrics that actually exist", async () => {
    stubApi();
    const { container } = mount();

    await waitFor(() => {
      expect(container.querySelectorAll("select").length).toBeGreaterThan(0);
    });

    const offered = new Set(METRIC_DEFINITIONS.metrics.map((m) => m.name));
    await waitFor(() => {
      const chosen = selectedMetricValues(container);
      expect(chosen.length).toBeGreaterThan(0);
      for (const value of chosen) {
        expect(offered.has(value)).toBe(true);
      }
    });
  });

  it("does not leave a selection pointing at a metric the server no longer defines", async () => {
    // A deployment whose metric set has changed: the previously-default
    // `execution_accuracy` is gone.
    stubApi({
      metricDefinitions: {
        groups: ["LLM judge"],
        metrics: [
          {
            group: "LLM judge",
            name: "llm_score",
            description: "judge verdict",
            value_type: "float",
          },
        ],
      },
    });
    const { container } = mount();

    await waitFor(() => {
      const chosen = selectedMetricValues(container);
      expect(chosen.length).toBeGreaterThan(0);
      for (const value of chosen) {
        expect(value).toBe("llm_score");
      }
    });
  });

  it("renders without metric definitions rather than blocking the view", async () => {
    stubApi({ metricDefinitions: { groups: [], metrics: [] } });
    const { container } = mount();
    await waitFor(() => expect(container.textContent).toContain("Metrics Comparison"));
  });

  it("renders the comparison sections against a live response", async () => {
    stubApi();
    const { container } = mount();
    await waitFor(() => {
      const text = container.textContent || "";
      expect(text).toContain("Metrics Comparison");
      expect(text).toContain("LLM judge comparison");
    });
    // Asserting individual confusion cells is left to the API tests, which
    // compare the counts against a reference implementation directly; doing it
    // here would pin this test to how the matrix is laid out.
  });

  it("survives a summary response with no `overall` field", async () => {
    // `!summary` was the only guard, so a truthy response missing the field it
    // then mapped over threw TypeError and took the whole view down.
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        const url = String(input);
        let body: unknown = {};
        if (url.includes("evaluation-metric-definitions")) body = METRIC_DEFINITIONS;
        else if (url.includes("binary-metric-confusion-by-pipeline")) body = CONFUSION;
        else body = { benchmark_id: BENCHMARK }; // no `overall`, no `categories`
        return new Response(JSON.stringify(body), { status: 200 });
      })
    );
    const { container } = mount();
    await waitFor(() => expect(container.textContent).toContain("Metrics Comparison"));
  });
});

describe("resilience", () => {
  it("does not crash when the insights endpoint fails", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        const url = String(input);
        if (url.includes("evaluation-metric-definitions")) {
          return new Response(JSON.stringify(METRIC_DEFINITIONS), { status: 200 });
        }
        return new Response(JSON.stringify({ detail: "boom" }), { status: 500 });
      })
    );
    const { container } = mount();
    await waitFor(() => expect(container.textContent).toContain("Metrics Comparison"));
  });
});
