import React, { useEffect, useMemo, useState } from "react";
import {
  Button,
  ComboBox,
  InlineNotification,
  Select,
  SelectItem,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
} from "@carbon/react";
import type { BenchmarkSummary } from "../pages/App";
import { apiUrl } from "../lib/api";

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

const metricOptions = [
  "execution_accuracy",
  "non_empty_execution_accuracy",
  "subset_non_empty_execution_accuracy",
  "llm_score",
];

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

  const [confusionA, setConfusionA] = useState<CrossPipelineConfusionResponse | null>(null);
  const [confusionB, setConfusionB] = useState<CrossPipelineConfusionResponse | null>(null);

  const pipelines = useMemo(() => summary?.overall.map((p) => p.name) ?? [], [summary]);

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

        // Default pipelines if empty
        const leftDefault = json.overall?.[0]?.name ?? "";
        const rightDefault = json.overall?.[1]?.name ?? leftDefault;
        setLeftPipeline((p) => (p ? p : leftDefault));
        setRightPipeline((p) => (p ? p : rightDefault));
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
          itemToString={(item) => item}
          selectedItem={leftPipeline || null}
          onChange={(e) => setLeftPipeline(e.selectedItem as string)}
          placeholder="Select left pipeline"
        />
        <ComboBox
          id="compare-right-pipeline"
          titleText="Right pipeline"
          items={pipelines}
          itemToString={(item) => item}
          selectedItem={rightPipeline || null}
          onChange={(e) => setRightPipeline(e.selectedItem as string)}
          placeholder="Select right pipeline"
        />
        <Select id="metricA-select" labelText="Metric A" value={metricA} onChange={(e) => setMetricA(e.target.value)}>
          {metricOptions.map((m) => (
            <SelectItem key={m} value={m} text={m} />
          ))}
        </Select>
        <Select id="metricB-select" labelText="Metric B" value={metricB} onChange={(e) => setMetricB(e.target.value)}>
          {metricOptions.map((m) => (
            <SelectItem key={m} value={m} text={m} />
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
    </div>
  );
};

