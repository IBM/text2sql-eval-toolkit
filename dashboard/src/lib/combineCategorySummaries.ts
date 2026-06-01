/** Pipeline-level metrics from `/api/benchmarks/{id}/summary/by-category`. */
export interface PipelineMetrics {
  name: string;
  metrics: Record<string, { average?: number; n?: number; stddev?: number } | number>;
}

export interface CategorySummaryResponse {
  benchmark_id: string;
  default_sort_metric: string;
  overall: PipelineMetrics[];
  categories: Record<string, PipelineMetrics[]>;
  has_full_results?: boolean;
}

export function getMetricAgg(
  pipeline: PipelineMetrics | null | undefined,
  metricKey: string
): { average: number; n: number } | null {
  if (!pipeline) return null;
  const raw = pipeline.metrics?.[metricKey];
  if (typeof raw === "number") {
    return { average: raw, n: 1 };
  }
  if (raw && typeof raw === "object" && typeof raw.average === "number") {
    const n = typeof raw.n === "number" && raw.n > 0 ? raw.n : 1;
    return { average: raw.average, n };
  }
  return null;
}

export function mergeMetricAggs(
  aggs: ({ average: number; n: number } | null)[]
): { average: number; n: number } | null {
  const valid = aggs.filter((a): a is { average: number; n: number } => a != null && a.n > 0);
  if (valid.length === 0) return null;
  const n = valid.reduce((sum, a) => sum + a.n, 0);
  const average = valid.reduce((sum, a) => sum + a.average * a.n, 0) / n;
  return { average, n };
}

function collectMetricKeys(
  summaries: CategorySummaryResponse[],
  profileKey: string | "overall"
): Set<string> {
  const keys = new Set<string>();
  for (const summary of summaries) {
    const pipelines =
      profileKey === "overall"
        ? summary.overall
        : summary.categories[profileKey] ?? [];
    for (const pipeline of pipelines) {
      for (const key of Object.keys(pipeline.metrics ?? {})) {
        if (key !== "num_records" && key !== "num_evaluated") {
          keys.add(key);
        }
      }
    }
  }
  return keys;
}

function buildMergedPipelines(
  summaries: CategorySummaryResponse[],
  profileKey: string | "overall",
  pipelineNames: string[]
): PipelineMetrics[] {
  const metricKeys = collectMetricKeys(summaries, profileKey);
  const result: PipelineMetrics[] = [];

  for (const name of pipelineNames) {
    const metrics: PipelineMetrics["metrics"] = {};
    for (const metricKey of metricKeys) {
      const aggs = summaries.map((summary) => {
        const pipelines =
          profileKey === "overall"
            ? summary.overall
            : summary.categories[profileKey] ?? [];
        const pipeline = pipelines.find((p) => p.name === name);
        return getMetricAgg(pipeline, metricKey);
      });
      const merged = mergeMetricAggs(aggs);
      if (merged) {
        metrics[metricKey] = merged;
      }
    }
    if (Object.keys(metrics).length > 0) {
      result.push({ name, metrics });
    }
  }

  return result;
}

/** Weighted-average merge of per-benchmark category summaries (pools profile subsets). */
export function combineCategorySummaries(
  summaries: CategorySummaryResponse[]
): CategorySummaryResponse | null {
  if (summaries.length === 0) return null;
  if (summaries.length === 1) return summaries[0];

  const profileKeys = new Set<string>();
  const pipelineNames = new Set<string>();

  for (const summary of summaries) {
    Object.keys(summary.categories ?? {}).forEach((k) => profileKeys.add(k));
    summary.overall.forEach((p) => pipelineNames.add(p.name));
    Object.values(summary.categories ?? {}).forEach((list) =>
      list.forEach((p) => pipelineNames.add(p.name))
    );
  }

  const pipelineList = [...pipelineNames].sort();
  const categories: Record<string, PipelineMetrics[]> = {};
  for (const profile of profileKeys) {
    categories[profile] = buildMergedPipelines(summaries, profile, pipelineList);
  }

  return {
    benchmark_id: summaries.map((s) => s.benchmark_id).join(" + "),
    default_sort_metric: summaries[0].default_sort_metric,
    overall: buildMergedPipelines(summaries, "overall", pipelineList),
    categories,
    has_full_results: summaries.every((s) => s.has_full_results !== false),
  };
}

/** Pipelines present in every selected benchmark (preferred for cross-benchmark comparison). */
export function intersectionPipelineNames(summaries: CategorySummaryResponse[]): string[] {
  if (summaries.length === 0) return [];
  const sets = summaries.map(
    (s) => new Set(s.overall.map((p) => p.name))
  );
  return [...sets[0]].filter((name) => sets.every((set) => set.has(name))).sort();
}

/** All pipeline names across selected benchmarks. */
export function unionPipelineNames(summaries: CategorySummaryResponse[]): string[] {
  const names = new Set<string>();
  for (const summary of summaries) {
    summary.overall.forEach((p) => names.add(p.name));
  }
  return [...names].sort();
}
