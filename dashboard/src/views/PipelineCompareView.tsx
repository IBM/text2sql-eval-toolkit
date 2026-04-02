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
  TableHeader,
  TableRow,
  Tag,
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
  benchmarkId: string | null;
  onSelectBenchmark?: (id: string) => void;
  benchmarks?: BenchmarkSummary[];
  onOpenErrorAnalysis: (filters: Partial<ErrorAnalysisFilters>) => void;
}

interface SummaryResponse {
  benchmark_id: string;
  default_sort_metric: string;
  overall: { name: string; metrics: Record<string, any> }[];
  categories: Record<string, { name: string; metrics: Record<string, any> }[]>;
}

interface CrossPipelineConfusionResponse {
  benchmark_id: string;
  left_id: string;
  right_id: string;
  metric_left: string;
  metric_right: string;
  n_valid: number;
  counts: {
    left0right0: number;
    left0right1: number;
    left1right0: number;
    left1right1: number;
  };
  rates: {
    left0right0: number;
    left0right1: number;
    left1right0: number;
    left1right1: number;
  };
  agreement_rate: number;
  disagreement_rate: number;
}

function getMetricAverage(p: any, metricKey: string): number | null {
  const raw = p?.metrics?.[metricKey];
  const avg = raw?.average;
  return typeof avg === "number" ? avg : null;
}

function getSubsetScore(p: { metrics: Record<string, any> }): number {
  const v = p?.metrics?.subset_non_empty_execution_accuracy?.average;
  return typeof v === "number" ? v : -1;
}

/** Aligns with Eval Playground metric group intro (toolkit metric_definitions). */
const TIMING_AND_TOKENS_INTRO =
  "Token counts and timings copied from the prediction record when available.";

function valueTypeLabel(t: string): string {
  switch (t) {
    case "binary":
      return "0 / 1";
    case "float":
      return "Number";
    case "int":
      return "Integer";
    case "text":
      return "Text";
    default:
      return t;
  }
}

function formatTimingSummaryAverage(valueType: string, avg: number | null): string {
  if (avg == null) return "N/A";
  if (valueType === "int") return Number.isFinite(avg) ? String(Math.round(avg)) : "N/A";
  if (valueType === "float") return avg.toFixed(2);
  return avg.toFixed(3);
}

function formatTimingDelta(valueType: string, left: number | null, right: number | null): string {
  if (left == null || right == null) return "N/A";
  const d = right - left;
  if (valueType === "int") return Number.isFinite(d) ? String(Math.round(d)) : "N/A";
  if (valueType === "float") return d.toFixed(2);
  return d.toFixed(3);
}

export const PipelineCompareView: React.FC<Props> = ({
  benchmarkId,
  onOpenErrorAnalysis,
}) => {
  const [summary, setSummary] = useState<SummaryResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const [leftPipeline, setLeftPipeline] = useState<string>("");
  const [rightPipeline, setRightPipeline] = useState<string>("");

  const [metricA, setMetricA] = useState<string>("execution_accuracy");
  const [metricB, setMetricB] = useState<string>("subset_non_empty_execution_accuracy");

  const [metricDefinitions, setMetricDefinitions] = useState<MetricDefinitionsResponse | null>(null);
  const [metricDefinitionsError, setMetricDefinitionsError] = useState<string | null>(null);

  const [confusionA, setConfusionA] = useState<CrossPipelineConfusionResponse | null>(null);
  const [confusionB, setConfusionB] = useState<CrossPipelineConfusionResponse | null>(null);

  const pipelines = useMemo(() => summary?.overall.map((p) => p.name) ?? [], [summary]);

  const pipelineCompareMetricGroups = useMemo(
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
    setMetricA((a) => (allowed.has(a) ? a : names[0]));
    setMetricB((b) => (allowed.has(b) ? b : names[1] ?? names[0]));
  }, [metricDefinitions]);

  useEffect(() => {
    if (!benchmarkId) return;

    const loadSummary = async () => {
      try {
        setError(null);
        setLoading(true);
        const res = await fetch(apiUrl(`/api/benchmarks/${benchmarkId}/summary/by-category`));
        if (!res.ok) throw new Error(`HTTP ${res.status} (summary)`);
        const json = (await res.json()) as SummaryResponse;
        setSummary(json);

        const ranked = [...(json.overall ?? [])].sort(
          (a, b) => getSubsetScore(b) - getSubsetScore(a)
        );
        const leftDefault = ranked[0]?.name ?? "";
        const rightDefault = ranked[1]?.name ?? leftDefault;
        const available = new Set(ranked.map((p) => p.name));
        setLeftPipeline((p) => (p && available.has(p) ? p : leftDefault));
        setRightPipeline((p) => (p && available.has(p) ? p : rightDefault));
      } catch (e: any) {
        setError(e.message || "Failed to load summary");
      } finally {
        setLoading(false);
      }
    };

    void loadSummary();
  }, [benchmarkId]);

  const leftPipelineObj = useMemo(
    () => (summary ? summary.overall.find((p) => p.name === leftPipeline) ?? null : null),
    [summary, leftPipeline]
  );
  const rightPipelineObj = useMemo(
    () => (summary ? summary.overall.find((p) => p.name === rightPipeline) ?? null : null),
    [summary, rightPipeline]
  );

  const avgA = useMemo(() => {
    if (!leftPipelineObj || !rightPipelineObj) return { left: null as number | null, right: null as number | null };
    return {
      left: getMetricAverage(leftPipelineObj, metricA),
      right: getMetricAverage(rightPipelineObj, metricA),
    };
  }, [leftPipelineObj, rightPipelineObj, metricA]);

  const avgB = useMemo(() => {
    if (!leftPipelineObj || !rightPipelineObj) return { left: null as number | null, right: null as number | null };
    return {
      left: getMetricAverage(leftPipelineObj, metricB),
      right: getMetricAverage(rightPipelineObj, metricB),
    };
  }, [leftPipelineObj, rightPipelineObj, metricB]);

  /** Same set and order as Eval Playground "Timing and tokens" (from `/api/evaluation-metric-definitions`). */
  const timingMetricsDef = useMemo(
    () => (metricDefinitions?.metrics ?? []).filter((m) => m.group === "Timing and tokens"),
    [metricDefinitions]
  );

  useEffect(() => {
    if (!benchmarkId) return;
    if (!leftPipeline || !rightPipeline) return;

    const loadConfusion = async (metricKey: string) => {
      const params = new URLSearchParams({
        pipeline_left: leftPipeline,
        pipeline_right: rightPipeline,
        metric_left: metricKey,
        metric_right: metricKey,
      });
      const res = await fetch(
        apiUrl(`/api/benchmarks/${benchmarkId}/insights/cross-pipeline-binary-metric-confusion?${params.toString()}`)
      );
      if (!res.ok) {
        throw new Error(`HTTP ${res.status} (confusion for ${metricKey})`);
      }
      return (await res.json()) as CrossPipelineConfusionResponse;
    };

    const load = async () => {
      try {
        setError(null);
        setLoading(true);
        const [a, b] = await Promise.all([loadConfusion(metricA), loadConfusion(metricB)]);
        setConfusionA(a);
        setConfusionB(b);
      } catch (e: any) {
        setError(e.message || "Failed to load comparison evidence");
        setConfusionA(null);
        setConfusionB(null);
      } finally {
        setLoading(false);
      }
    };

    void load();
  }, [benchmarkId, leftPipeline, rightPipeline, metricA, metricB]);

  const renderDisagreementBlock = (metricKey: string, c: CrossPipelineConfusionResponse | null) => {
    if (!c) {
      return (
        <InlineNotification kind="info" title={`No evidence for ${metricKey}`} subtitle="Switch pipelines/metrics or ensure results artifacts exist." lowContrast />
      );
    }
    const n = c.n_valid;
    const left0right1 = c.counts.left0right1;
    const left1right0 = c.counts.left1right0;
    const rate = n > 0 ? (left0right1 + left1right0) / n : 0;

    return (
      <section style={{ border: "1px solid rgba(15,98,254,0.2)", borderRadius: "6px", padding: "0.75rem" }}>
        <h4 style={{ margin: "0 0 0.25rem 0", color: "#0f62fe" }}>{`Disagreement evidence for ${metricKey}`}</h4>
        <div style={{ opacity: 0.88, fontSize: "0.9rem", marginBottom: "0.5rem" }}>
          Disagreement rate: <strong>{rate.toFixed(3)}</strong> (valid samples: {n})
        </div>
        <div style={{ overflow: "auto", maxHeight: "240px" }}>
          <Table size="sm" aria-label={`Cross-pipeline confusion for ${metricKey}`}>
            <TableHead>
              <TableRow>
                <TableCell>Case</TableCell>
                <TableCell>Count</TableCell>
                <TableCell>Rate</TableCell>
                <TableCell />
              </TableRow>
            </TableHead>
            <TableBody>
              <TableRow>
                <TableCell>{`Left=0, Right=1`}</TableCell>
                <TableCell>{left0right1}</TableCell>
                <TableCell>{n > 0 ? (left0right1 / n).toFixed(3) : "0.000"}</TableCell>
                <TableCell>
                  <Button
                    size="sm"
                    kind="secondary"
                    disabled={left0right1 === 0}
                    onClick={() =>
                      onOpenErrorAnalysis({
                        pipeline: leftPipeline,
                        metric: metricKey,
                        value: "0",
                        op: "eq",
                        pipeline2: rightPipeline,
                        metric2: metricKey,
                        disagree: true,
                      })
                    }
                  >
                    View examples
                  </Button>
                </TableCell>
              </TableRow>
              <TableRow>
                <TableCell>{`Left=1, Right=0`}</TableCell>
                <TableCell>{left1right0}</TableCell>
                <TableCell>{n > 0 ? (left1right0 / n).toFixed(3) : "0.000"}</TableCell>
                <TableCell>
                  <Button
                    size="sm"
                    kind="secondary"
                    disabled={left1right0 === 0}
                    onClick={() =>
                      onOpenErrorAnalysis({
                        pipeline: leftPipeline,
                        metric: metricKey,
                        value: "1",
                        op: "eq",
                        pipeline2: rightPipeline,
                        metric2: metricKey,
                        disagree: true,
                      })
                    }
                  >
                    View examples
                  </Button>
                </TableCell>
              </TableRow>
            </TableBody>
          </Table>
        </div>
      </section>
    );
  };

  if (!benchmarkId) {
    return (
      <InlineNotification kind="info" title="Select a benchmark" subtitle="Choose a benchmark to compare pipelines." lowContrast />
    );
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }}>
      <h3 style={{ margin: 0 }}>Pipeline compare – {benchmarkId}</h3>

      {metricDefinitionsError && (
        <InlineNotification
          kind="warning"
          title="Metric list unavailable"
          subtitle={metricDefinitionsError}
          lowContrast
        />
      )}

      {error && (
        <InlineNotification kind="error" title="Comparison error" subtitle={error} lowContrast />
      )}

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))",
          gap: "0.5rem",
          alignItems: "end",
        }}
      >
        <ComboBox
          id="compare-left-pipeline"
          titleText="Left pipeline"
          items={pipelines}
          itemToString={(item) => item ?? ""}
          selectedItem={leftPipeline || null}
          onChange={(e) => setLeftPipeline(e.selectedItem as string)}
          placeholder="Select left pipeline"
        />
        <ComboBox
          id="compare-right-pipeline"
          titleText="Right pipeline"
          items={pipelines}
          itemToString={(item) => item ?? ""}
          selectedItem={rightPipeline || null}
          onChange={(e) => setRightPipeline(e.selectedItem as string)}
          placeholder="Select right pipeline"
        />
        <Select
          id="metricA-select"
          labelText="Metric A"
          value={metricA}
          onChange={(e) => setMetricA(e.target.value)}
          disabled={pipelineCompareMetricGroups.length === 0}
        >
          {pipelineCompareMetricGroups.map((g) => (
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
          value={metricB}
          onChange={(e) => setMetricB(e.target.value)}
          disabled={pipelineCompareMetricGroups.length === 0}
        >
          {pipelineCompareMetricGroups.map((g) => (
            <SelectItemGroup key={`${g.label}-b`} label={g.label}>
              {g.metrics.map((m) => (
                <SelectItem key={`${m.name}-b`} value={m.name} text={m.name} title={m.description} />
              ))}
            </SelectItemGroup>
          ))}
        </Select>
      </div>

      <section style={{ border: "1px solid rgba(15,98,254,0.2)", borderRadius: "6px", padding: "0.75rem" }}>
        <h4 style={{ margin: "0 0 0.25rem 0", color: "#0f62fe" }}>Metric averages and deltas</h4>
        <div style={{ overflow: "auto", maxHeight: "220px" }}>
          <Table size="sm" aria-label="Metric averages and deltas">
            <TableHead>
              <TableRow>
                <TableCell>Metric</TableCell>
                <TableCell>{`Left (${leftPipeline})`}</TableCell>
                <TableCell>{`Right (${rightPipeline})`}</TableCell>
                <TableCell>Δ (right - left)</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              <TableRow>
                <TableCell>{metricA}</TableCell>
                <TableCell>{avgA.left == null ? "N/A" : avgA.left.toFixed(3)}</TableCell>
                <TableCell>{avgA.right == null ? "N/A" : avgA.right.toFixed(3)}</TableCell>
                <TableCell>
                  {avgA.left != null && avgA.right != null ? (avgA.right - avgA.left).toFixed(3) : "N/A"}
                </TableCell>
              </TableRow>
              <TableRow>
                <TableCell>{metricB}</TableCell>
                <TableCell>{avgB.left == null ? "N/A" : avgB.left.toFixed(3)}</TableCell>
                <TableCell>{avgB.right == null ? "N/A" : avgB.right.toFixed(3)}</TableCell>
                <TableCell>
                  {avgB.left != null && avgB.right != null ? (avgB.right - avgB.left).toFixed(3) : "N/A"}
                </TableCell>
              </TableRow>
            </TableBody>
          </Table>
        </div>
      </section>

      {loading ? (
        <InlineNotification kind="info" title="Loading evidence…" subtitle="Computing disagreements across pipelines." lowContrast />
      ) : (
        <>
          {renderDisagreementBlock(metricA, confusionA)}
          {renderDisagreementBlock(metricB, confusionB)}
        </>
      )}

      <section style={{ border: "1px solid rgba(15,98,254,0.2)", borderRadius: "6px", padding: "0.75rem" }}>
        <h4 style={{ margin: "0 0 0.25rem 0", color: "#0f62fe" }}>Timing and tokens</h4>
        <p style={{ margin: "0 0 0.5rem 0", fontSize: "0.8125rem", lineHeight: 1.45, opacity: 0.88, maxWidth: "52rem" }}>
          {TIMING_AND_TOKENS_INTRO} Averages are benchmark-wide aggregates from the summary artifact (same fields as Eval
          Playground per-record metrics).
        </p>
        {timingMetricsDef.length === 0 ? (
          <InlineNotification
            kind="info"
            title="No timing metrics in definitions"
            subtitle="Load metric definitions or ensure the toolkit lists Timing and tokens metrics."
            lowContrast
          />
        ) : (
          <div style={{ overflow: "auto", maxHeight: "320px" }}>
            <Table size="sm" aria-label="Timing and tokens comparison">
              <TableHead>
                <TableRow>
                  <TableHeader>Metric</TableHeader>
                  <TableHeader>Type</TableHeader>
                  <TableHeader>{`Left (${leftPipeline})`}</TableHeader>
                  <TableHeader>{`Right (${rightPipeline})`}</TableHeader>
                  <TableHeader>Δ (right − left)</TableHeader>
                  <TableHeader>What it means</TableHeader>
                </TableRow>
              </TableHead>
              <TableBody>
                {timingMetricsDef.map((m) => {
                  const left = leftPipelineObj ? getMetricAverage(leftPipelineObj, m.name) : null;
                  const right = rightPipelineObj ? getMetricAverage(rightPipelineObj, m.name) : null;
                  return (
                    <TableRow key={m.name}>
                      <TableCell style={{ fontFamily: "monospace", fontSize: "0.8125rem", verticalAlign: "top" }}>
                        {m.name}
                      </TableCell>
                      <TableCell style={{ verticalAlign: "top" }}>
                        <Tag type="gray" size="sm">
                          {valueTypeLabel(m.value_type)}
                        </Tag>
                      </TableCell>
                      <TableCell style={{ verticalAlign: "top", fontVariantNumeric: "tabular-nums" }}>
                        {formatTimingSummaryAverage(m.value_type, left)}
                      </TableCell>
                      <TableCell style={{ verticalAlign: "top", fontVariantNumeric: "tabular-nums" }}>
                        {formatTimingSummaryAverage(m.value_type, right)}
                      </TableCell>
                      <TableCell style={{ verticalAlign: "top", fontVariantNumeric: "tabular-nums" }}>
                        {formatTimingDelta(m.value_type, left, right)}
                      </TableCell>
                      <TableCell style={{ fontSize: "0.8125rem", lineHeight: 1.45, verticalAlign: "top" }}>
                        {m.description}
                      </TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          </div>
        )}
      </section>
    </div>
  );
};

