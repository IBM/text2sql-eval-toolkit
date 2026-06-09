import React, { useEffect, useMemo, useState } from "react";
import {
  Button,
  ComboBox,
  DataTableSkeleton,
  InlineLoading,
  InlineNotification,
  Select,
  SelectItem,
  SelectItemGroup,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
  Tag,
} from "@carbon/react";
import type { BenchmarkSummary } from "../types/benchmark";
import { apiFetch, apiUrl } from "../lib/api";
import {
  type CategorySummaryResponse,
  combineCategorySummaries,
  getMetricAgg,
  intersectionPipelineNames,
  unionPipelineNames,
} from "../lib/combineCategorySummaries";
import {
  type MetricDefinitionsResponse,
  buildMetricInsightsSelectGroups,
  flattenMetricInsightsSelectNames,
} from "../lib/metricInsightsSelect";
import {
  LARGE_BENCHMARK_WARNING,
  formatEvalResultsSize,
  isLargeBenchmark,
} from "../lib/largeBenchmark";

interface Props {
  benchmarks: BenchmarkSummary[];
  benchmarkId: string | null;
  onSelectBenchmark?: (id: string) => void;
}

type PipelineMetrics = CategorySummaryResponse["overall"][number];

const PROFILE_ORDER = [
  "single_source_basic",
  "multi_table_simple",
  "single_source_advanced",
  "has_join",
  "has_nested_query",
  "has_aggregation",
  "has_group_by",
  "has_having",
  "has_sorting",
  "has_window_function",
  "has_distinct",
  "has_limit",
  "has_set_operation",
  "has_case_expression",
  "has_cte",
  "has_cast",
  "has_like",
  "has_between",
  "has_in_predicate",
  "question_brief",
  "question_moderate",
  "question_verbose",
  "question_counting",
  "question_superlative",
  "question_comparison",
  "question_temporal",
  "question_aggregation_intent",
  "question_listing",
  "question_existence",
  "question_negation",
  "question_grouping_intent",
];

const PROFILE_DESCRIPTIONS: Record<string, string> = {
  single_source_basic: "Single table; no joins, subqueries, or window functions.",
  multi_table_simple: "Multiple tables with joins; no subqueries or window functions.",
  single_source_advanced: "Single table with subqueries and/or window functions.",
  has_join: "Contains at least one JOIN.",
  has_nested_query: "Contains a nested SELECT (or similar).",
  has_aggregation: "Uses aggregate functions.",
  has_group_by: "Uses GROUP BY.",
  has_having: "Uses HAVING.",
  has_sorting: "Uses ORDER BY.",
  has_window_function: "Uses window functions.",
  has_distinct: "Uses SELECT DISTINCT.",
  has_limit: "Uses LIMIT (or TOP).",
  has_set_operation: "Uses UNION, INTERSECT, or EXCEPT.",
  has_case_expression: "Uses a CASE expression.",
  has_cte: "Uses a WITH (CTE) clause.",
  has_cast: "Uses CAST (or similar type conversion).",
  has_like: "Uses LIKE pattern matching.",
  has_between: "Uses BETWEEN.",
  has_in_predicate: "Uses an IN predicate.",
  question_brief: "Short question (8 words or fewer).",
  question_moderate: "Medium-length question (9–15 words).",
  question_verbose: "Long question (more than 15 words).",
  question_counting: "Asks for a count (e.g. how many, number of).",
  question_superlative: "Asks for a superlative (most, least, highest, etc.).",
  question_comparison: "Compares values (more than, between, etc.).",
  question_temporal: "References time or dates.",
  question_aggregation_intent: "Mentions average, ratio, percentage, or similar.",
  question_listing: "Asks to list or enumerate (which, what are, etc.).",
  question_existence: "Asks whether something exists.",
  question_negation: "Contains negation (not, without, never, etc.).",
  question_grouping_intent: "Suggests per-group breakdown (for each, per, by).",
};

function sortProfiles(profiles: string[]): string[] {
  const rank = new Map(PROFILE_ORDER.map((p, i) => [p, i]));
  return [...profiles].sort((a, b) => {
    const ra = rank.has(a) ? rank.get(a)! : PROFILE_ORDER.length;
    const rb = rank.has(b) ? rank.get(b)! : PROFILE_ORDER.length;
    if (ra !== rb) return ra - rb;
    return a.localeCompare(b);
  });
}

function getMetricAverage(p: PipelineMetrics | null | undefined, metricKey: string): number | null {
  return getMetricAgg(p ?? null, metricKey)?.average ?? null;
}

function getMetricCount(p: PipelineMetrics | null | undefined, metricKey: string): number | null {
  return getMetricAgg(p ?? null, metricKey)?.n ?? null;
}

function getSubsetScore(p: PipelineMetrics): number {
  return getMetricAverage(p, "subset_non_empty_execution_accuracy") ?? -1;
}

function findPipeline(pipelines: PipelineMetrics[], name: string): PipelineMetrics | null {
  return pipelines.find((p) => p.name === name) ?? null;
}

export const ProfileCompareView: React.FC<Props> = ({
  benchmarks,
  benchmarkId,
  onSelectBenchmark,
}) => {
  const [selectedBenchmarkIds, setSelectedBenchmarkIds] = useState<string[]>([]);
  const [summariesById, setSummariesById] = useState<Record<string, CategorySummaryResponse>>({});
  const [loadErrorsById, setLoadErrorsById] = useState<Record<string, string>>({});
  const [loading, setLoading] = useState(false);

  const [selectedPipeline, setSelectedPipeline] = useState("");
  const [metricA, setMetricA] = useState("execution_accuracy");
  const [metricB, setMetricB] = useState("subset_non_empty_execution_accuracy");
  const [pipelineScope, setPipelineScope] = useState<"common" | "all">("common");

  const [metricDefinitions, setMetricDefinitions] = useState<MetricDefinitionsResponse | null>(null);
  const [metricDefinitionsError, setMetricDefinitionsError] = useState<string | null>(null);

  const metricSelectGroups = useMemo(
    () => buildMetricInsightsSelectGroups(metricDefinitions?.metrics ?? []),
    [metricDefinitions]
  );

  const benchmarksAvailableToAdd = useMemo(
    () => benchmarks.filter((b) => !selectedBenchmarkIds.includes(b.benchmark_id)),
    [benchmarks, selectedBenchmarkIds]
  );

  const benchmarkById = useMemo(
    () => new Map(benchmarks.map((b) => [b.benchmark_id, b])),
    [benchmarks]
  );

  const selectedLargeBenchmarks = useMemo(
    () =>
      selectedBenchmarkIds
        .map((id) => benchmarkById.get(id))
        .filter((b): b is BenchmarkSummary => b != null && isLargeBenchmark(b)),
    [selectedBenchmarkIds, benchmarkById]
  );

  useEffect(() => {
    if (!benchmarkId) return;
    setSelectedBenchmarkIds((prev) =>
      prev.includes(benchmarkId) ? prev : [...prev, benchmarkId]
    );
  }, [benchmarkId]);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        setMetricDefinitionsError(null);
        const res = await apiFetch(apiUrl("/api/evaluation-metric-definitions"));
        const json = (await res.json()) as MetricDefinitionsResponse;
        if (!cancelled) setMetricDefinitions(json);
      } catch (e: unknown) {
        if (!cancelled) {
          setMetricDefinitions(null);
          setMetricDefinitionsError(
            e instanceof Error ? e.message : "Failed to load metric definitions"
          );
        }
      }
    };
    void load();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!metricDefinitions?.metrics?.length) return;
    const groups = buildMetricInsightsSelectGroups(metricDefinitions.metrics);
    const names = flattenMetricInsightsSelectNames(groups);
    if (names.length === 0) return;
    const allowed = new Set(names);
    setMetricA((a) => (allowed.has(a) ? a : names[0]));
    setMetricB((b) => (allowed.has(b) ? b : names[1] ?? names[0]));
  }, [metricDefinitions]);

  useEffect(() => {
    if (selectedBenchmarkIds.length === 0) {
      setSummariesById({});
      setLoadErrorsById({});
      return;
    }

    let cancelled = false;
    const loadAll = async () => {
      setLoading(true);
      const errors: Record<string, string> = {};
      const loaded: Record<string, CategorySummaryResponse> = {};

      await Promise.all(
        selectedBenchmarkIds.map(async (id) => {
          try {
            const res = await apiFetch(apiUrl(`/api/benchmarks/${id}/summary/by-category`));
            const json = (await res.json()) as CategorySummaryResponse;
            loaded[id] = json;
          } catch (e: unknown) {
            errors[id] = e instanceof Error ? e.message : "Failed to load summary";
          }
        })
      );

      if (cancelled) return;
      setSummariesById(loaded);
      setLoadErrorsById(errors);
      setLoading(false);
    };

    void loadAll();
    return () => {
      cancelled = true;
    };
  }, [selectedBenchmarkIds]);

  const loadedSummaries = useMemo(
    () =>
      selectedBenchmarkIds
        .map((id) => summariesById[id])
        .filter((s): s is CategorySummaryResponse => s != null),
    [selectedBenchmarkIds, summariesById]
  );

  const combinedSummary = useMemo(
    () => combineCategorySummaries(loadedSummaries),
    [loadedSummaries]
  );

  const commonPipelines = useMemo(
    () => intersectionPipelineNames(loadedSummaries),
    [loadedSummaries]
  );

  const allPipelines = useMemo(() => unionPipelineNames(loadedSummaries), [loadedSummaries]);

  const pipelines = pipelineScope === "common" ? commonPipelines : allPipelines;

  useEffect(() => {
    if (!combinedSummary) return;
    const ranked = [...(combinedSummary.overall ?? [])].sort(
      (a, b) => getSubsetScore(b) - getSubsetScore(a)
    );
    const namesInScope = new Set(pipelines);
    const rankedInScope = ranked.filter((p) => namesInScope.has(p.name));
    const defaultPipeline = rankedInScope[0]?.name ?? pipelines[0] ?? "";
    const available = new Set(pipelines);
    setSelectedPipeline((p) => (p && available.has(p) ? p : defaultPipeline));
  }, [combinedSummary, pipelines]);

  const addBenchmark = (item: BenchmarkSummary | null) => {
    if (!item) return;
    const id = item.benchmark_id;
    if (selectedBenchmarkIds.includes(id)) return;
    setSelectedBenchmarkIds((prev) => [...prev, id]);
    onSelectBenchmark?.(id);
  };

  const removeBenchmark = (id: string) => {
    setSelectedBenchmarkIds((prev) => {
      const next = prev.filter((x) => x !== id);
      if (next.length > 0) onSelectBenchmark?.(next[next.length - 1]);
      return next;
    });
    setSummariesById((prev) => {
      const copy = { ...prev };
      delete copy[id];
      return copy;
    });
    setLoadErrorsById((prev) => {
      const copy = { ...prev };
      delete copy[id];
      return copy;
    });
  };

  const profileRows = useMemo(() => {
    if (!combinedSummary || !selectedPipeline) return [];

    const buildRow = (
      profile: string,
      pipelineList: PipelineMetrics[],
      description: string
    ) => {
      const pipelineObj = findPipeline(pipelineList, selectedPipeline);
      const valueA = getMetricAverage(pipelineObj, metricA);
      const valueB = getMetricAverage(pipelineObj, metricB);
      const diff = valueA != null && valueB != null ? valueB - valueA : null;
      return {
        profile,
        valueA,
        valueB,
        diff,
        nA: getMetricCount(pipelineObj, metricA),
        nB: getMetricCount(pipelineObj, metricB),
        description,
      };
    };

    const profiles = sortProfiles(Object.keys(combinedSummary.categories ?? {}));
    const rows = profiles.map((profile) =>
      buildRow(
        profile,
        combinedSummary.categories[profile] ?? [],
        PROFILE_DESCRIPTIONS[profile] ?? ""
      )
    );

    const overallRow = buildRow(
      "overall",
      combinedSummary.overall,
      loadedSummaries.length > 1
        ? `Pooled across ${loadedSummaries.length} benchmarks (weighted by record count per profile).`
        : "All questions in the benchmark (unfiltered)."
    );

    return [overallRow, ...rows];
  }, [combinedSummary, selectedPipeline, metricA, metricB, loadedSummaries.length]);

  const maxAbsDiff = useMemo(() => {
    return profileRows.reduce((m, r) => {
      if (r.diff == null) return m;
      return Math.max(m, Math.abs(r.diff));
    }, 0.01);
  }, [profileRows]);

  const combinedLabel =
    selectedBenchmarkIds.length > 1
      ? `${selectedBenchmarkIds.length} benchmarks combined`
      : selectedBenchmarkIds[0] ?? "";

  const noProfiles =
    combinedSummary != null &&
    combinedSummary.has_full_results !== false &&
    Object.keys(combinedSummary.categories).length === 0;

  if (benchmarks.length === 0) {
    return (
      <InlineNotification
        kind="info"
        title="No benchmarks"
        subtitle="Load benchmarks before using profile compare."
        lowContrast
      />
    );
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }}>
      <h3 style={{ margin: 0 }}>Profile compare</h3>
      <p style={{ margin: 0, opacity: 0.88, fontSize: "0.9rem", lineHeight: 1.45, maxWidth: "52rem" }}>
        Select one or more benchmarks to pool profile metrics (weighted by sample size), pick a pipeline,
        then compare two metrics on each SQL query profile.
      </p>

      <section
        style={{
          border: "1px solid rgba(15,98,254,0.2)",
          borderRadius: "6px",
          padding: "0.75rem",
          display: "flex",
          flexDirection: "column",
          gap: "0.5rem",
        }}
      >
        <h4 style={{ margin: 0, color: "#0f62fe", fontSize: "0.95rem" }}>Benchmarks</h4>
        <div style={{ display: "flex", flexWrap: "wrap", gap: "0.35rem", alignItems: "center" }}>
          {selectedBenchmarkIds.length === 0 ? (
            <span style={{ fontSize: "0.875rem", opacity: 0.75 }}>Add at least one benchmark below.</span>
          ) : (
            selectedBenchmarkIds.map((id) => {
              const meta = benchmarkById.get(id);
              const large = meta != null && isLargeBenchmark(meta);
              const sizeLabel = formatEvalResultsSize(meta?.eval_results_bytes);
              const largeTitle = large
                ? `${LARGE_BENCHMARK_WARNING}${sizeLabel ? ` (${sizeLabel} on disk)` : ""}`
                : undefined;
              return (
                <span
                  key={id}
                  style={{ display: "inline-flex", gap: "0.25rem", alignItems: "center" }}
                >
                  <Tag
                    type={loadErrorsById[id] ? "red" : summariesById[id] ? "blue" : "gray"}
                    filter
                    onClose={() => removeBenchmark(id)}
                    title={loadErrorsById[id] ?? largeTitle}
                  >
                    {id}
                  </Tag>
                  {large ? (
                    <Tag type="magenta" size="sm" title={largeTitle}>
                      Large
                    </Tag>
                  ) : null}
                </span>
              );
            })
          )}
        </div>
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "minmax(260px, 1fr) auto",
            gap: "0.5rem",
            alignItems: "end",
          }}
        >
          <ComboBox
            id="profile-compare-add-benchmark"
            titleText="Add benchmark"
            items={benchmarksAvailableToAdd}
            itemToString={(item) => (item ? item.benchmark_id : "")}
            itemToElement={(item) =>
              item ? (
                <span style={{ display: "inline-flex", gap: "0.35rem", alignItems: "center" }}>
                  <span>{item.benchmark_id}</span>
                  {isLargeBenchmark(item) ? (
                    <Tag type="magenta" size="sm" title={LARGE_BENCHMARK_WARNING}>
                      Large
                    </Tag>
                  ) : null}
                </span>
              ) : null
            }
            selectedItem={null}
            onChange={(e) => addBenchmark(e.selectedItem as BenchmarkSummary | null)}
            placeholder={
              benchmarksAvailableToAdd.length === 0
                ? "All benchmarks already selected"
                : "Choose benchmark to add…"
            }
            disabled={benchmarksAvailableToAdd.length === 0}
          />
          {selectedBenchmarkIds.length > 1 && (
            <Button
              kind="ghost"
              size="sm"
              onClick={() => {
                setSelectedBenchmarkIds(benchmarkId ? [benchmarkId] : [selectedBenchmarkIds[0]]);
              }}
            >
              Reset to one
            </Button>
          )}
        </div>
        {Object.entries(loadErrorsById).map(([id, msg]) => (
          <InlineNotification
            key={id}
            kind="error"
            title={`Failed to load ${id}`}
            subtitle={msg}
            lowContrast
          />
        ))}
      </section>

      {selectedBenchmarkIds.length === 0 ? (
        <InlineNotification
          kind="info"
          title="Select benchmarks"
          subtitle="Add one or more benchmarks to compare metrics by SQL profile."
          lowContrast
        />
      ) : null}

      {selectedLargeBenchmarks.length > 0 && (
        <InlineNotification
          kind="warning"
          title="Large benchmarks selected"
          subtitle={`${selectedLargeBenchmarks.map((b) => b.benchmark_id).join(", ")} — ${LARGE_BENCHMARK_WARNING}`}
          lowContrast
        />
      )}

      {metricDefinitionsError && (
        <InlineNotification
          kind="warning"
          title="Metric list unavailable"
          subtitle={metricDefinitionsError}
          lowContrast
        />
      )}

      {loadedSummaries.some((s) => s.has_full_results === false) && (
        <InlineNotification
          kind="info"
          title="Partial profile data"
          subtitle="Some selected benchmarks lack full eval JSON; their profile breakdown may be missing or incomplete."
          lowContrast
        />
      )}

      {noProfiles && (
        <InlineNotification
          kind="warning"
          title="No profiles found"
          subtitle="Records have no meta.categories tags. Run scripts/profiling/run_profiling.py on eval JSON files."
          lowContrast
        />
      )}

      {pipelineScope === "common" &&
        selectedBenchmarkIds.length > 1 &&
        commonPipelines.length === 0 &&
        loadedSummaries.length > 1 && (
          <InlineNotification
            kind="warning"
            title="No common pipelines"
            subtitle='None of the selected benchmarks share a pipeline name. Switch pipeline list to "All pipelines" or adjust your selection.'
            lowContrast
          />
        )}

      {selectedBenchmarkIds.length > 0 && (
        <>
          {!loading && combinedSummary && (
            <>
              <p style={{ margin: 0, fontSize: "0.85rem", opacity: 0.8 }}>
                Viewing: <strong>{combinedLabel}</strong>
                {selectedBenchmarkIds.length > 1
                  ? " — metrics are weighted averages pooled per profile."
                  : null}
              </p>

              <div
                style={{
                  display: "grid",
                  gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))",
                  gap: "0.5rem",
                  alignItems: "end",
                }}
              >
                <Select
                  id="profile-compare-pipeline-scope"
                  labelText="Pipeline list"
                  value={pipelineScope}
                  onChange={(e) => setPipelineScope(e.target.value as "common" | "all")}
                >
                  <SelectItem value="common" text="Common across benchmarks" />
                  <SelectItem value="all" text="All pipelines" />
                </Select>
                <ComboBox
                  id="profile-compare-pipeline"
                  titleText="Pipeline"
                  items={pipelines}
                  itemToString={(item) => item ?? ""}
                  selectedItem={selectedPipeline || null}
                  onChange={(e) => setSelectedPipeline((e.selectedItem as string) ?? "")}
                  placeholder="Select pipeline"
                  disabled={pipelines.length === 0}
                />
                <Select
                  id="profile-compare-metric-a"
                  labelText="Metric A"
                  value={metricA}
                  onChange={(e) => setMetricA(e.target.value)}
                  disabled={metricSelectGroups.length === 0}
                >
                  {metricSelectGroups.map((g) => (
                    <SelectItemGroup key={g.label} label={g.label}>
                      {g.metrics.map((m) => (
                        <SelectItem
                          key={m.name}
                          value={m.name}
                          text={m.name}
                          title={m.description}
                        />
                      ))}
                    </SelectItemGroup>
                  ))}
                </Select>
                <Select
                  id="profile-compare-metric-b"
                  labelText="Metric B"
                  value={metricB}
                  onChange={(e) => setMetricB(e.target.value)}
                  disabled={metricSelectGroups.length === 0}
                >
                  {metricSelectGroups.map((g) => (
                    <SelectItemGroup key={`${g.label}-b`} label={g.label}>
                      {g.metrics.map((m) => (
                        <SelectItem
                          key={`${m.name}-b`}
                          value={m.name}
                          text={m.name}
                          title={m.description}
                        />
                      ))}
                    </SelectItemGroup>
                  ))}
                </Select>
              </div>
            </>
          )}

          {loading ? (
            <div
              style={{
                display: "flex",
                flexDirection: "column",
                gap: "0.75rem",
                border: "1px solid rgba(15,98,254,0.2)",
                borderRadius: "6px",
                padding: "0.75rem",
              }}
            >
              <InlineLoading
                description={
                  selectedBenchmarkIds.length > 1
                    ? `Loading profile summaries for ${selectedBenchmarkIds.length} benchmarks…`
                    : `Loading profile summary for ${selectedBenchmarkIds[0] ?? "benchmark"}…`
                }
                status="active"
              />
              <DataTableSkeleton role="progressbar" columnCount={6} rowCount={10} />
            </div>
          ) : combinedSummary ? (
            <section
              style={{
                border: "1px solid rgba(15,98,254,0.2)",
                borderRadius: "6px",
                padding: "0.75rem",
              }}
            >
              <h4 style={{ margin: "0 0 0.5rem 0", color: "#0f62fe" }}>
                {selectedPipeline ? `${selectedPipeline} · ` : ""}
                {metricA} vs {metricB} by profile
              </h4>
              <div style={{ overflow: "auto", maxHeight: "520px" }}>
                <Table size="sm" aria-label="Profile comparison table">
                  <TableHead>
                    <TableRow>
                      <TableHeader>Profile</TableHeader>
                      <TableHeader>{metricA}</TableHeader>
                      <TableHeader>{metricB}</TableHeader>
                      <TableHeader>Δ (B − A)</TableHeader>
                      <TableHeader>n (A / B)</TableHeader>
                      <TableHeader>Δ visual</TableHeader>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {profileRows.map((row) => {
                      const diff = row.diff;
                      const barPct =
                        diff != null ? Math.min(100, (Math.abs(diff) / maxAbsDiff) * 100) : 0;
                      const barColor = diff == null ? "#8d8d8d" : diff >= 0 ? "#24a148" : "#da1e28";
                      return (
                        <TableRow key={row.profile}>
                          <TableCell style={{ verticalAlign: "top", maxWidth: "220px" }}>
                            <div style={{ fontWeight: row.profile === "overall" ? 600 : 400 }}>
                              {row.profile}
                            </div>
                            {row.description ? (
                              <div
                                style={{ fontSize: "0.75rem", opacity: 0.75, marginTop: "0.2rem" }}
                              >
                                {row.description}
                              </div>
                            ) : null}
                          </TableCell>
                          <TableCell
                            style={{ fontVariantNumeric: "tabular-nums", verticalAlign: "top" }}
                          >
                            {row.valueA == null ? "N/A" : row.valueA.toFixed(3)}
                          </TableCell>
                          <TableCell
                            style={{ fontVariantNumeric: "tabular-nums", verticalAlign: "top" }}
                          >
                            {row.valueB == null ? "N/A" : row.valueB.toFixed(3)}
                          </TableCell>
                          <TableCell
                            style={{ fontVariantNumeric: "tabular-nums", verticalAlign: "top" }}
                          >
                            {diff == null ? "N/A" : diff.toFixed(3)}
                          </TableCell>
                          <TableCell
                            style={{
                              fontVariantNumeric: "tabular-nums",
                              verticalAlign: "top",
                              fontSize: "0.8125rem",
                            }}
                          >
                            {row.nA != null || row.nB != null
                              ? `${row.nA ?? "–"} / ${row.nB ?? "–"}`
                              : "–"}
                          </TableCell>
                          <TableCell style={{ verticalAlign: "top", minWidth: "120px" }}>
                            {diff != null ? (
                              <div
                                style={{
                                  height: "10px",
                                  background: "rgba(0,0,0,0.08)",
                                  borderRadius: "999px",
                                  overflow: "hidden",
                                }}
                              >
                                <div
                                  style={{
                                    width: `${barPct}%`,
                                    height: "100%",
                                    background: barColor,
                                    borderRadius: "999px",
                                  }}
                                />
                              </div>
                            ) : (
                              "–"
                            )}
                          </TableCell>
                        </TableRow>
                      );
                    })}
                  </TableBody>
                </Table>
              </div>
            </section>
          ) : null}
        </>
      )}
    </div>
  );
};
