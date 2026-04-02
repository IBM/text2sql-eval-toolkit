import React, { useEffect, useMemo, useState } from "react";
import {
  Button,
  ComboBox,
  InlineNotification,
  Select,
  SelectItem,
  SelectItemGroup,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
} from "@carbon/react";
import type { BenchmarkSummary } from "../types/benchmark";
import { apiUrl } from "../lib/api";
import {
  type MetricDefinitionsResponse,
  buildMetricInsightsSelectGroups,
  flattenMetricInsightsSelectNames,
} from "../lib/metricInsightsSelect";

type ErrorAnalysisFilters = {
  pipeline: string;
  metric: string;
  value: string;
  op: string;
  pipeline2: string;
  metric2: string;
  disagree: boolean;
};

interface Props {
  benchmarks: BenchmarkSummary[];
  benchmarkId: string | null;
  onSelectBenchmark: (id: string) => void;
  onOpenErrorAnalysis: (filters: Partial<ErrorAnalysisFilters>) => void;
}

interface ConfusionByPipelineRow {
  pipeline: string;
  counts: {
    a0b0: number;
    a0b1: number;
    a1b0: number;
    a1b1: number;
  };
  n_valid: number;
  rates: {
    a0b0: number;
    a0b1: number;
    a1b0: number;
    a1b1: number;
  };
  agreement_rate: number;
  disagreement_rate: number;
}

interface ConfusionByPipelineResponse {
  benchmark_id: string;
  metric_a: string;
  metric_b: string;
  per_pipeline: ConfusionByPipelineRow[];
}

interface SummaryResponse {
  benchmark_id: string;
  default_sort_metric: string;
  overall: { name: string; metrics: Record<string, any> }[];
  categories: Record<string, { name: string; metrics: Record<string, any> }[]>;
}

function metricDelta(p: any, metricA: string, metricB: string): number {
  const a = p?.metrics?.[metricA]?.average;
  const b = p?.metrics?.[metricB]?.average;
  if (typeof a !== "number" || typeof b !== "number") return 0;
  return b - a;
}

export const ToolkitInsightsView: React.FC<Props> = ({
  benchmarks,
  benchmarkId,
  onSelectBenchmark,
  onOpenErrorAnalysis,
}) => {
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const [metricAKey, setMetricAKey] = useState<string>("execution_accuracy");
  const [metricBKey, setMetricBKey] = useState<string>(
    "subset_non_empty_execution_accuracy"
  );
  const [llmMetricAKey, setLlmMetricAKey] = useState<string>("execution_accuracy");

  const [execVsSubset, setExecVsSubset] = useState<ConfusionByPipelineResponse | null>(null);
  const [execVsLLM, setExecVsLLM] = useState<ConfusionByPipelineResponse | null>(null);
  const [summary, setSummary] = useState<SummaryResponse | null>(null);
  const [selectedPipeline, setSelectedPipeline] = useState<string>("");

  const [metricDefinitions, setMetricDefinitions] = useState<MetricDefinitionsResponse | null>(null);
  const [metricDefinitionsError, setMetricDefinitionsError] = useState<string | null>(null);

  const metricInsightsGroups = useMemo(
    () => buildMetricInsightsSelectGroups(metricDefinitions?.metrics ?? []),
    [metricDefinitions]
  );

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        setMetricDefinitionsError(null);
        const res = await fetch(apiUrl("/api/evaluation-metric-definitions"));
        if (!res.ok) throw new Error(`HTTP ${res.status} (metric definitions)`);
        const json = (await res.json()) as MetricDefinitionsResponse;
        if (!cancelled) setMetricDefinitions(json);
      } catch (e: any) {
        if (!cancelled) {
          setMetricDefinitions(null);
          setMetricDefinitionsError(e?.message || "Failed to load metric definitions");
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
    setMetricAKey((a) => (allowed.has(a) ? a : names[0]));
    setMetricBKey((b) => (allowed.has(b) ? b : names[1] ?? names[0]));
    setLlmMetricAKey((x) => (allowed.has(x) ? x : names[0]));
  }, [metricDefinitions]);

  useEffect(() => {
    if (!benchmarkId) return;
    const load = async () => {
      try {
        setLoading(true);
        setError(null);

        const paramsSubset = new URLSearchParams({
          metric_a: metricAKey,
          metric_b: metricBKey,
        });
        const paramsLLM = new URLSearchParams({
          metric_a: llmMetricAKey,
          metric_b: "llm_score",
        });

        const [subsetRes, llmRes, summaryRes] = await Promise.all([
          fetch(
            apiUrl(
              `/api/benchmarks/${benchmarkId}/insights/binary-metric-confusion-by-pipeline?${paramsSubset.toString()}`
            )
          ),
          fetch(
            apiUrl(
              `/api/benchmarks/${benchmarkId}/insights/binary-metric-confusion-by-pipeline?${paramsLLM.toString()}`
            )
          ),
          fetch(apiUrl(`/api/benchmarks/${benchmarkId}/summary/by-category`)),
        ]);

        if (!subsetRes.ok) throw new Error(`HTTP ${subsetRes.status} (exec vs subset)`);
        if (!llmRes.ok) throw new Error(`HTTP ${llmRes.status} (exec vs llm)`);
        if (!summaryRes.ok) throw new Error(`HTTP ${summaryRes.status} (summary)`);

        const subsetJson = (await subsetRes.json()) as ConfusionByPipelineResponse;
        const llmJson = (await llmRes.json()) as ConfusionByPipelineResponse;
        const summaryJson = (await summaryRes.json()) as SummaryResponse;

        setExecVsSubset(subsetJson);
        setExecVsLLM(llmJson);
        setSummary(summaryJson);
      } catch (e: any) {
        setError(e.message || "Failed to load toolkit insights");
      } finally {
        setLoading(false);
      }
    };

    void load();
  }, [benchmarkId, metricAKey, metricBKey, llmMetricAKey]);

  useEffect(() => {
    if (!summary?.overall?.length) {
      setSelectedPipeline("");
      return;
    }
    const bySubset = [...summary.overall].sort((a, b) => {
      const av = Number(a.metrics?.subset_non_empty_execution_accuracy?.average ?? -1);
      const bv = Number(b.metrics?.subset_non_empty_execution_accuracy?.average ?? -1);
      return bv - av;
    });
    const bestPipeline = bySubset[0]?.name ?? "";
    const exists = summary.overall.some((p) => p.name === selectedPipeline);
    if (!selectedPipeline || !exists) {
      setSelectedPipeline(bestPipeline);
    }
  }, [summary, selectedPipeline]);

  const pipelineDeltaRows = useMemo(() => {
    if (!summary) return [];
    const rows = summary.overall
      .map((p) => {
        const delta = metricDelta(p, "execution_accuracy", "subset_non_empty_execution_accuracy");
        const deltaLLM = metricDelta(p, "subset_non_empty_execution_accuracy", "llm_score");
        return { pipeline: p.name, deltaExecSubset: delta, deltaLLM };
      })
      .sort((a, b) => b.deltaExecSubset - a.deltaExecSubset);
    return rows;
  }, [summary]);

  const availablePipelines = useMemo(
    () => summary?.overall.map((p) => p.name) ?? [],
    [summary]
  );

  const selectedExecVsSubset = useMemo(
    () => execVsSubset?.per_pipeline.find((p) => p.pipeline === selectedPipeline) ?? null,
    [execVsSubset, selectedPipeline]
  );

  const selectedExecVsLLM = useMemo(
    () => execVsLLM?.per_pipeline.find((p) => p.pipeline === selectedPipeline) ?? null,
    [execVsLLM, selectedPipeline]
  );

  const renderConfusionMatrix = (
    counts: { a0b0: number; a0b1: number; a1b0: number; a1b1: number },
    nValid: number,
    metricAName: string,
    metricBName: string
  ) => {
    const cellStyleBase: React.CSSProperties = {
      border: "1px solid rgba(141,192,219,0.6)",
      borderRadius: "6px",
      padding: "0.35rem 0.45rem",
      textAlign: "center",
      minWidth: "84px",
    };
    const regularCell: React.CSSProperties = {
      ...cellStyleBase,
      background: "rgba(255,255,255,0.04)",
    };
    const highlightCell: React.CSSProperties = {
      ...cellStyleBase,
      background: "rgba(15,98,254,0.18)",
      border: "1px solid rgba(15,98,254,0.8)",
      fontWeight: 700,
    };

    return (
      <div
        style={{
          display: "flex",
          flexDirection: "column",
          gap: "0.2rem",
          minWidth: "250px",
          maxWidth: "760px",
          margin: "0 auto",
        }}
      >
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "max-content minmax(130px, 1fr) minmax(130px, 1fr)",
            gap: "0.15rem",
            alignItems: "center",
            justifyContent: "center",
          }}
        >
          <div />
          <div
            style={{
              fontSize: "0.74rem",
              textAlign: "center",
              opacity: 0.85,
              lineHeight: 1.1,
              overflowWrap: "anywhere",
              padding: "0 0.2rem",
            }}
          >
            {metricBName}=0
          </div>
          <div
            style={{
              fontSize: "0.74rem",
              textAlign: "center",
              opacity: 0.85,
              lineHeight: 1.1,
              overflowWrap: "anywhere",
              padding: "0 0.2rem",
            }}
          >
            {metricBName}=1
          </div>

          <div
            style={{
              fontSize: "0.74rem",
              textAlign: "right",
              opacity: 0.85,
              paddingRight: "0.25rem",
              overflowWrap: "anywhere",
              lineHeight: 1.1,
            }}
          >
            {metricAName}=0
          </div>
          <div style={regularCell} title={`A=0,B=0 rate=${nValid > 0 ? counts.a0b0 / nValid : 0}`}>
            {counts.a0b0}
          </div>
          <div style={highlightCell} title={`A=0,B=1 rate=${nValid > 0 ? counts.a0b1 / nValid : 0}`}>
            {counts.a0b1}
          </div>

          <div
            style={{
              fontSize: "0.74rem",
              textAlign: "right",
              opacity: 0.85,
              paddingRight: "0.25rem",
              overflowWrap: "anywhere",
              lineHeight: 1.1,
            }}
          >
            {metricAName}=1
          </div>
          <div style={regularCell} title={`A=1,B=0 rate=${nValid > 0 ? counts.a1b0 / nValid : 0}`}>
            {counts.a1b0}
          </div>
          <div style={regularCell} title={`A=1,B=1 rate=${nValid > 0 ? counts.a1b1 / nValid : 0}`}>
            {counts.a1b1}
          </div>
        </div>
      </div>
    );
  };

  if (!benchmarkId) {
    return (
      <InlineNotification
        kind="info"
        title="Select a benchmark"
        subtitle="Choose a benchmark to see toolkit insights."
        lowContrast
      />
    );
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }}>
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "minmax(260px, 1fr)",
          gap: "0.75rem",
          alignItems: "end",
        }}
      >
        <div>
          <ComboBox
            id="insights-benchmark-select"
            titleText="Benchmark"
            items={benchmarks}
            itemToString={(item) => (item ? item.benchmark_id : "")}
            selectedItem={benchmarks.find((b) => b.benchmark_id === benchmarkId) ?? null}
            onChange={(e) => {
              const selected = e.selectedItem as BenchmarkSummary | null;
              if (selected) onSelectBenchmark(selected.benchmark_id);
            }}
            placeholder="Select benchmark"
          />
        </div>
        <div>
          <ComboBox
            id="insights-pipeline-select"
            titleText="Pipeline"
            items={availablePipelines}
            itemToString={(item) => item ?? ""}
            selectedItem={selectedPipeline || null}
            onChange={(e) => {
              const selected = (e.selectedItem as string | null) ?? "";
              setSelectedPipeline(selected);
            }}
            placeholder="Select pipeline"
            disabled={!availablePipelines.length}
          />
        </div>
      </div>

      {metricDefinitionsError && (
        <InlineNotification
          kind="warning"
          title="Metric list unavailable"
          subtitle={metricDefinitionsError}
          lowContrast
        />
      )}

      {error && (
        <InlineNotification
          kind="error"
          title="Failed to load insights"
          subtitle={error}
          lowContrast
        />
      )}

      {loading && (
        <InlineNotification
          kind="info"
          title="Loading evidence…"
          subtitle="Computing metric disagreements and pulling relevant statistics."
          lowContrast
        />
      )}

      <section style={{ border: "1px solid rgba(15,98,254,0.2)", borderRadius: "6px", padding: "0.75rem" }}>
        <h4 style={{ margin: "0 0 0.25rem 0", color: "#0f62fe" }}>Metrics Comparison</h4>
        <div style={{ opacity: 0.88, fontSize: "0.9rem", marginBottom: "0.5rem" }}>
          Compare two binary metrics (A and B) and inspect where they disagree for each pipeline.
        </div>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))", gap: "0.75rem", marginBottom: "0.5rem" }}>
          <Select
            id="metricA-select"
            labelText="Metric A"
            value={metricAKey}
            onChange={(e) => setMetricAKey(e.target.value)}
            disabled={metricInsightsGroups.length === 0}
          >
            {metricInsightsGroups.map((g) => (
              <SelectItemGroup key={g.label} label={g.label}>
                {g.metrics.map((m) => (
                  <SelectItem key={m.name} value={m.name} text={m.name} title={m.description} />
                ))}
              </SelectItemGroup>
            ))}
          </Select>
          <Select
            id="metricB-select"
            labelText="Metric B"
            value={metricBKey}
            onChange={(e) => setMetricBKey(e.target.value)}
            disabled={metricInsightsGroups.length === 0}
          >
            {metricInsightsGroups.map((g) => (
              <SelectItemGroup key={`${g.label}-b`} label={g.label}>
                {g.metrics.map((m) => (
                  <SelectItem key={`${m.name}-b`} value={m.name} text={m.name} title={m.description} />
                ))}
              </SelectItemGroup>
            ))}
          </Select>
        </div>
        {selectedExecVsSubset ? (
          <div
            style={{
              border: "1px solid rgba(15,98,254,0.2)",
              borderRadius: "6px",
              padding: "0.75rem",
              display: "flex",
              flexDirection: "column",
              gap: "0.6rem",
            }}
          >
            {(() => {
              const row = selectedExecVsSubset;
              const n = row.n_valid;
              const { a0b0, a0b1, a1b0, a1b1 } = row.counts;
              const rate = n > 0 ? a0b1 / n : 0;
              return (
                <>
                  <div style={{ fontSize: "0.85rem", opacity: 0.9 }}>
                    Pipeline: <strong>{row.pipeline}</strong>
                  </div>
                  {renderConfusionMatrix(
                    { a0b0, a0b1, a1b0, a1b1 },
                    n,
                    metricAKey,
                    metricBKey
                  )}
                  <div style={{ fontSize: "0.9rem" }}>
                    Recovery cases ({metricAKey} missed, {metricBKey} matched):{" "}
                    <strong>{a0b1}</strong>{" "}
                    <span style={{ opacity: 0.85 }}>({rate.toFixed(3)})</span>
                  </div>
                  <div>
                    <Button
                      size="sm"
                      kind="secondary"
                      disabled={n === 0 || a0b1 === 0}
                      onClick={() =>
                        onOpenErrorAnalysis({
                          pipeline: row.pipeline,
                          metric: metricAKey,
                          value: "0",
                          op: "eq",
                          pipeline2: row.pipeline,
                          metric2: metricBKey,
                          disagree: true,
                        })
                      }
                    >
                      View examples
                    </Button>
                  </div>
                </>
              );
            })()}
          </div>
        ) : (
          <InlineNotification
            kind="info"
            title="No evidence available"
            subtitle="Check that evaluation artifacts exist under your `TEXT2SQL_DATA_ROOT/results/`."
            lowContrast
          />
        )}
      </section>

      <section style={{ border: "1px solid rgba(15,98,254,0.2)", borderRadius: "6px", padding: "0.75rem" }}>
        <h4 style={{ margin: "0 0 0.25rem 0", color: "#0f62fe" }}>LLM judge comparison</h4>
        <div style={{ opacity: 0.88, fontSize: "0.9rem", marginBottom: "0.5rem" }}>
          Compare a selectable execution-like metric (Metric A) against `llm_score` (Metric B). The highlighted cell shows records where Metric A is 0 but LLM judge is 1.
        </div>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(240px, 1fr))", gap: "0.75rem", marginBottom: "0.5rem" }}>
          <Select
            id="llm-metricA-select"
            labelText="Metric A"
            value={llmMetricAKey}
            onChange={(e) => setLlmMetricAKey(e.target.value)}
            disabled={metricInsightsGroups.length === 0}
          >
            {metricInsightsGroups.map((g) => (
              <SelectItemGroup key={`${g.label}-llm`} label={g.label}>
                {g.metrics.map((m) => (
                  <SelectItem
                    key={`${m.name}-llm`}
                    value={m.name}
                    text={m.name}
                    title={m.description}
                  />
                ))}
              </SelectItemGroup>
            ))}
          </Select>
          <Select id="llm-metricB-select" labelText="Metric B (fixed)" value={"llm_score"} disabled>
            <SelectItem value="llm_score" text="llm_score" />
          </Select>
        </div>
        {selectedExecVsLLM ? (
          <div
            style={{
              border: "1px solid rgba(15,98,254,0.2)",
              borderRadius: "6px",
              padding: "0.75rem",
              display: "flex",
              flexDirection: "column",
              gap: "0.6rem",
            }}
          >
            {(() => {
              const row = selectedExecVsLLM;
              const n = row.n_valid;
              const { a0b0, a0b1, a1b0, a1b1 } = row.counts;
              const rate = n > 0 ? a0b1 / n : 0;
              return (
                <>
                  <div style={{ fontSize: "0.85rem", opacity: 0.9 }}>
                    Pipeline: <strong>{row.pipeline}</strong>
                  </div>
                  {renderConfusionMatrix(
                    { a0b0, a0b1, a1b0, a1b1 },
                    n,
                    llmMetricAKey,
                    "llm_score"
                  )}
                  <div style={{ fontSize: "0.9rem" }}>
                    Recovery cases ({llmMetricAKey} missed, llm_score matched):{" "}
                    <strong>{a0b1}</strong>{" "}
                    <span style={{ opacity: 0.85 }}>({rate.toFixed(3)})</span>
                  </div>
                  <div>
                    <Button
                      size="sm"
                      kind="secondary"
                      disabled={n === 0 || a0b1 === 0}
                      onClick={() =>
                        onOpenErrorAnalysis({
                          pipeline: row.pipeline,
                          metric: llmMetricAKey,
                          value: "0",
                          op: "eq",
                          pipeline2: row.pipeline,
                          metric2: "llm_score",
                          disagree: true,
                        })
                      }
                    >
                      View examples
                    </Button>
                  </div>
                </>
              );
            })()}
          </div>
        ) : (
          <InlineNotification
            kind="info"
            title="No evidence available"
            subtitle="Check that evaluation artifacts exist under your `TEXT2SQL_DATA_ROOT/results/`."
            lowContrast
          />
        )}
      </section>

      <section style={{ border: "1px solid rgba(15,98,254,0.2)", borderRadius: "6px", padding: "0.75rem" }}>
        <h4 style={{ margin: "0 0 0.25rem 0", color: "#0f62fe" }}>How metric choice changes ranking</h4>
        <div style={{ opacity: 0.88, fontSize: "0.9rem", marginBottom: "0.5rem" }}>
          If you rank solely by `execution_accuracy`, you may overlook pipelines that do better under alternative execution metrics (subset/superset non-empty). This view shows the top pipelines by `subset_non_empty_execution_accuracy - execution_accuracy` (and how `llm_score` compares to the subset metric).
        </div>
        {summary && pipelineDeltaRows.length > 0 ? (
          <div style={{ overflow: "auto", maxHeight: "260px" }}>
            <Table size="sm" aria-label="Ranking deltas">
              <TableHead>
                <TableRow>
                  <TableCell>Pipeline</TableCell>
                  <TableCell>Δ(subset - exec)</TableCell>
                  <TableCell>Δ(llm - subset)</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {pipelineDeltaRows.slice(0, 10).map((r) => (
                  <TableRow
                    key={r.pipeline}
                    style={
                      r.pipeline === selectedPipeline
                        ? { background: "rgba(15,98,254,0.08)" }
                        : undefined
                    }
                  >
                    <TableCell style={{ maxWidth: 360 }}>
                      {r.pipeline}
                      {r.pipeline === selectedPipeline ? " (selected)" : ""}
                    </TableCell>
                    <TableCell>{r.deltaExecSubset.toFixed(3)}</TableCell>
                    <TableCell>{r.deltaLLM.toFixed(3)}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        ) : (
          <InlineNotification kind="info" title="No summary loaded" subtitle="Switch benchmarks to load evidence." lowContrast />
        )}
      </section>
    </div>
  );
};

