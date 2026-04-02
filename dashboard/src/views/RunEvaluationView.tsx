import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  Button,
  Checkbox,
  ComboBox,
  InlineNotification,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableHeader,
  TableRow,
  Tag,
  TextArea,
  TextInput,
} from "@carbon/react";
import type { BenchmarkSummary } from "../types/benchmark";
import { apiUrl } from "../lib/api";

interface Props {
  benchmarks: BenchmarkSummary[];
}

interface MetricDefinition {
  group: string;
  name: string;
  description: string;
  value_type: string;
}

interface MetricDefinitionsResponse {
  groups: string[];
  metrics: MetricDefinition[];
}

interface RecordIdItem {
  record_id: string;
  question: string;
}

interface LLMJudgeConfigItem {
  name: string;
  path: string;
}

interface LLMJudgeConfigListResponse {
  items: LLMJudgeConfigItem[];
}

interface PipelinePlaygroundInfo {
  name: string;
  predicted_sql?: string | null;
  has_prompt: boolean;
  has_agent_trace: boolean;
  evaluation?: Record<string, unknown> | null;
  prediction_error?: string | null;
  prediction_row_count?: number | null;
  prediction_column_count?: number | null;
  predicted_df?: string | null;
}

interface PlaygroundInitResponse {
  benchmark_id: string;
  record_id: string;
  question: string;
  db_id?: string | null;
  ground_truth_sqls: string[];
  pipelines: PipelinePlaygroundInfo[];
  ground_truth_row_counts?: number[];
  ground_truth_dfs?: string[];
}

interface PlaygroundEvaluateResponse {
  benchmark_id: string;
  record_id: string;
  evaluation: Record<string, unknown>;
  ground_truth_row_counts: number[];
  ground_truth_dfs?: string[];
  predicted_df?: string | null;
  prediction_error?: string | null;
  prediction_row_count?: number | null;
  prediction_column_count?: number | null;
}

type PipelinePickItem = { name: string };

const TEXT_DETAIL_KEYS = new Set([
  "df_error_message",
  "eval_error_message",
  "llm_judge_error",
  "gt_sql",
  "gt_df",
]);

/** Shown under the LLM judge section instead of the metrics table or text-details table. */
const LLM_EXPLANATION_KEY = "llm_explanation";

/** Eval Playground defaults: record and pipeline to load and auto-run on first open. */
const DEFAULT_PLAYGROUND_RECORD_ID = "1490";
const DEFAULT_PLAYGROUND_PIPELINE = "wxai:openai/gpt-oss-120b-greedy-zero-shot-chatapi";

/** One-line context for each metric group (aligned with toolkit metric_definitions). */
const METRIC_GROUP_INTRO: Record<string, string> = {
  "Execution match":
    "Compares executed result tables from predicted vs ground-truth SQL (exact, subset, BIRD-style, etc.).",
  "SQL equivalence":
    "String and parser-based checks that predicted SQL is equivalent to ground truth without executing.",
  Parsing: "Whether the predicted SQL parses cleanly with SQLGlot or sqlparse.",
  "LLM judge": "Scores and notes from the optional LLM-as-judge when enabled.",
  "Timing and tokens": "Token counts and timings copied from the prediction record when available.",
  Errors: "Flags and messages when execution, dataframe handling, or evaluation fails.",
  "Ground truth (when matched)":
    "Which ground-truth SQL and result snapshot were used when a subset-style match succeeded.",
  Other: "Metrics not listed in the toolkit definitions (e.g. newer fields).",
};

function hasPlaygroundCachedEvaluation(pl: PipelinePlaygroundInfo | null | undefined): boolean {
  const ev = pl?.evaluation;
  return ev != null && typeof ev === "object" && Object.keys(ev).length > 0;
}

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

function formatCellValue(v: unknown): string {
  if (v === null || v === undefined) return "";
  if (typeof v === "object") {
    try {
      const s = JSON.stringify(v);
      return s.length > 2000 ? `${s.slice(0, 2000)}…` : s;
    } catch {
      return String(v);
    }
  }
  return String(v);
}

/** Pandas `orient="split"` JSON (columns + data rows). */
function parseSplitDfJson(dfJson: string | null | undefined): { columns: string[]; data: unknown[][] } | null {
  if (!dfJson || !String(dfJson).trim()) return null;
  try {
    const o = JSON.parse(dfJson) as unknown;
    if (typeof o !== "object" || o === null) return null;
    const obj = o as Record<string, unknown>;
    const cols = obj.columns;
    const data = obj.data;
    if (!Array.isArray(cols) || !Array.isArray(data)) return null;
    return { columns: cols.map((c) => String(c)), data: data as unknown[][] };
  } catch {
    return null;
  }
}

const DF_PREVIEW_SCROLL_STYLE: React.CSSProperties = {
  maxHeight: "18rem",
  overflow: "auto",
  border: "1px solid var(--cds-border-subtle-01)",
  borderRadius: 4,
  backgroundColor: "var(--cds-layer-02)",
};

function DataFramePreview({
  title,
  dfJson,
  emptyMessage,
}: {
  title: string;
  dfJson: string | null | undefined;
  emptyMessage: string;
}) {
  const parsed = useMemo(() => parseSplitDfJson(dfJson ?? null), [dfJson]);
  if (!parsed) {
    return (
      <div style={{ display: "flex", flexDirection: "column", gap: "0.35rem" }}>
        <p style={{ margin: 0, fontSize: "0.8125rem", fontWeight: 600 }}>{title}</p>
        <p style={{ margin: 0, fontSize: "0.8125rem", color: "var(--cds-text-secondary)" }}>{emptyMessage}</p>
      </div>
    );
  }
  const { columns, data } = parsed;
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "0.35rem", minWidth: 0 }}>
      <p style={{ margin: 0, fontSize: "0.8125rem", fontWeight: 600 }}>{title}</p>
      <TableContainer style={DF_PREVIEW_SCROLL_STYLE}>
        <Table size="sm">
          <TableHead>
            <TableRow>
              {columns.map((c, ci) => (
                <TableHeader key={ci}>{c}</TableHeader>
              ))}
            </TableRow>
          </TableHead>
          <TableBody>
            {data.map((row, ri) => (
              <TableRow key={ri}>
                {columns.map((_, ci) => (
                  <TableCell
                    key={ci}
                    style={{
                      fontSize: "0.8125rem",
                      whiteSpace: "pre-wrap",
                      wordBreak: "break-word",
                      maxWidth: "14rem",
                    }}
                  >
                    {formatCellValue(Array.isArray(row) ? row[ci] : undefined)}
                  </TableCell>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </div>
  );
}

export const RunEvaluationView: React.FC<Props> = ({ benchmarks }) => {
  const [selectedBenchmark, setSelectedBenchmark] = useState<BenchmarkSummary | null>(
    benchmarks[0] ?? null
  );

  useEffect(() => {
    if (benchmarks.length === 0) return;
    setSelectedBenchmark((prev) => {
      if (prev) {
        const match = benchmarks.find((b) => b.benchmark_id === prev.benchmark_id);
        return match ?? prev;
      }
      return benchmarks[0];
    });
  }, [benchmarks]);

  const [metricDefs, setMetricDefs] = useState<MetricDefinition[]>([]);
  /** Order of groups from the API (stable, matches toolkit sections). */
  const [metricGroupsOrder, setMetricGroupsOrder] = useState<string[]>([]);
  const [recordItems, setRecordItems] = useState<RecordIdItem[]>([]);
  /** Stable id for Carbon ComboBox — selected item must be referentially in `items` */
  const [selectedRecordId, setSelectedRecordId] = useState<string | null>(null);
  const [recordIdManual, setRecordIdManual] = useState("");
  const [questionText, setQuestionText] = useState("");
  const [dbIdText, setDbIdText] = useState("");
  const [groundTruthSqls, setGroundTruthSqls] = useState<string[]>([""]);
  const [predictedSql, setPredictedSql] = useState("");
  const [pipelines, setPipelines] = useState<PipelinePlaygroundInfo[]>([]);
  const [selectedPipelineName, setSelectedPipelineName] = useState<string | null>(null);
  const [benchPipelineNames, setBenchPipelineNames] = useState<string[]>([]);
  const [loadedRecordId, setLoadedRecordId] = useState("");
  /** GT row counts from eval file (init) or last successful playground evaluate. */
  const [savedGroundTruthRowCounts, setSavedGroundTruthRowCounts] = useState<number[]>([]);
  /** GT dataframe JSON strings (split) from eval file (init) or last evaluate. */
  const [savedGroundTruthDfs, setSavedGroundTruthDfs] = useState<string[]>([]);

  const [playgroundResult, setPlaygroundResult] = useState<PlaygroundEvaluateResponse | null>(null);
  const [playgroundError, setPlaygroundError] = useState<string | null>(null);
  const [recordIdsError, setRecordIdsError] = useState<string | null>(null);
  const [playgroundLoading, setPlaygroundLoading] = useState(false);
  const [initLoading, setInitLoading] = useState(false);

  const [useLLMPlayground, setUseLLMPlayground] = useState(true);
  const [llmConfigPathPlayground, setLlmConfigPathPlayground] = useState("");
  const [forceRerunLLMPlayground, setForceRerunLLMPlayground] = useState(false);
  const [llmJudgeConfigs, setLlmJudgeConfigs] = useState<LLMJudgeConfigItem[]>([]);

  const llmConfigDefaultItem = useMemo(() => ({ name: "Default (built-in)", path: "" }), []);

  const llmJudgeComboItems = useMemo(() => {
    const items: LLMJudgeConfigItem[] = [llmConfigDefaultItem, ...llmJudgeConfigs];
    const p = llmConfigPathPlayground.trim();
    if (p && !items.some((i) => i.path === p)) {
      const tail = p.split("/").pop() || p;
      items.push({ name: `${tail} (custom path)`, path: p });
    }
    return items;
  }, [llmConfigDefaultItem, llmJudgeConfigs, llmConfigPathPlayground]);

  const selectedLlmConfigItem = useMemo(
    () =>
      llmJudgeComboItems.find((i) => i.path === llmConfigPathPlayground) ?? llmConfigDefaultItem,
    [llmJudgeComboItems, llmConfigPathPlayground, llmConfigDefaultItem]
  );

  const autoLoadedForBenchRef = useRef<string | null>(null);

  useEffect(() => {
    autoLoadedForBenchRef.current = null;
  }, [selectedBenchmark?.benchmark_id]);

  const selectedRecordItem = useMemo(() => {
    if (!selectedRecordId) return null;
    return recordItems.find((r) => String(r.record_id) === String(selectedRecordId)) ?? null;
  }, [recordItems, selectedRecordId]);

  const effectiveRecordId = (recordIdManual.trim() || selectedRecordId || "").trim();

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const res = await fetch(apiUrl("/api/evaluation-metric-definitions"));
        if (!res.ok) return;
        const json: MetricDefinitionsResponse = await res.json();
        if (!cancelled) {
          setMetricDefs(json.metrics || []);
          setMetricGroupsOrder(json.groups || []);
        }
      } catch {
        /* ignore */
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const res = await fetch(apiUrl("/api/llm-judge/configs"));
        if (!res.ok) return;
        const json: LLMJudgeConfigListResponse = await res.json();
        if (!cancelled) setLlmJudgeConfigs(json.items || []);
      } catch {
        /* ignore */
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  const fetchRecordIds = useCallback(async () => {
    const bid = selectedBenchmark?.benchmark_id;
    if (!bid) {
      setRecordItems([]);
      setRecordIdsError(null);
      return;
    }
    setRecordIdsError(null);
    try {
      const res = await fetch(apiUrl(`/api/benchmarks/${encodeURIComponent(bid)}/record-ids`));
      if (!res.ok) {
        const txt = await res.text();
        setRecordIdsError(txt || `HTTP ${res.status}`);
        setRecordItems([]);
        return;
      }
      const json = await res.json();
      setRecordItems(json.items || []);
    } catch (e: unknown) {
      setRecordIdsError(e instanceof Error ? e.message : "Failed to load record IDs");
      setRecordItems([]);
    }
  }, [selectedBenchmark?.benchmark_id]);

  useEffect(() => {
    void fetchRecordIds();
  }, [fetchRecordIds]);

  useEffect(() => {
    const bid = selectedBenchmark?.benchmark_id;
    if (!bid) {
      setBenchPipelineNames([]);
      setSelectedPipelineName(null);
      return;
    }
    let cancelled = false;
    (async () => {
      try {
        const res = await fetch(apiUrl(`/api/benchmarks/${encodeURIComponent(bid)}/summary`));
        if (!res.ok) {
          if (!cancelled) {
            setBenchPipelineNames([]);
            setSelectedPipelineName(null);
          }
          return;
        }
        const j = await res.json();
        const names: string[] = (j.pipelines || [])
          .map((p: { name: string }) => p.name)
          .sort((a: string, b: string) => a.localeCompare(b));
        if (!cancelled) {
          setBenchPipelineNames(names);
          const preferred =
            names.includes(DEFAULT_PLAYGROUND_PIPELINE) ? DEFAULT_PLAYGROUND_PIPELINE : (names[0] ?? null);
          setSelectedPipelineName(preferred);
        }
      } catch {
        if (!cancelled) {
          setBenchPipelineNames([]);
          setSelectedPipelineName(null);
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [selectedBenchmark?.benchmark_id]);

  const pipelineComboItems: PipelinePickItem[] = useMemo(() => {
    if (benchPipelineNames.length > 0) {
      return benchPipelineNames.map((name) => ({ name }));
    }
    return pipelines.map((p) => ({ name: p.name }));
  }, [benchPipelineNames, pipelines]);

  const selectedPipelineItem: PipelinePickItem | null = useMemo(() => {
    if (!selectedPipelineName) return null;
    return pipelineComboItems.find((p) => p.name === selectedPipelineName) ?? null;
  }, [pipelineComboItems, selectedPipelineName]);

  const defByName = useMemo(() => {
    const m = new Map<string, MetricDefinition>();
    metricDefs.forEach((d) => m.set(d.name, d));
    return m;
  }, [metricDefs]);

  const executePlaygroundEvaluate = useCallback(
    async (payload: {
      record_id: string;
      ground_truth_sqls: string[];
      predicted_sql: string;
      merge_pipeline: string | null;
    }) => {
      if (!selectedBenchmark?.benchmark_id) return;
      try {
        setPlaygroundLoading(true);
        setPlaygroundError(null);
        const body = {
          ...payload,
          timeout_s: 90,
          use_llm: useLLMPlayground,
          llm_judge_config_path: llmConfigPathPlayground || null,
          force_rerun_llm_judge: forceRerunLLMPlayground,
        };
        const res = await fetch(
          apiUrl(`/api/benchmarks/${encodeURIComponent(selectedBenchmark.benchmark_id)}/playground/evaluate`),
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body),
          }
        );
        if (!res.ok) {
          const txt = await res.text();
          throw new Error(txt || `HTTP ${res.status}`);
        }
        const json: PlaygroundEvaluateResponse = await res.json();
        setPlaygroundResult(json);
        setSavedGroundTruthRowCounts(json.ground_truth_row_counts ?? []);
        setSavedGroundTruthDfs(json.ground_truth_dfs ?? []);
      } catch (e: unknown) {
        setPlaygroundError(e instanceof Error ? e.message : "Evaluation failed");
      } finally {
        setPlaygroundLoading(false);
      }
    },
    [
      selectedBenchmark?.benchmark_id,
      useLLMPlayground,
      llmConfigPathPlayground,
      forceRerunLLMPlayground,
    ]
  );

  const loadPlaygroundForRecordId = useCallback(
    async (
      recordId: string,
      options?: { preferredPipelineName?: string | null; autoEvaluate?: boolean }
    ) => {
      if (!selectedBenchmark?.benchmark_id || !recordId.trim()) return;
      try {
        setInitLoading(true);
        setPlaygroundError(null);
        const res = await fetch(
          apiUrl(
            `/api/benchmarks/${encodeURIComponent(selectedBenchmark.benchmark_id)}/playground/${encodeURIComponent(recordId.trim())}`
          )
        );
        if (!res.ok) {
          const txt = await res.text();
          throw new Error(txt || `HTTP ${res.status}`);
        }
        const json: PlaygroundInitResponse = await res.json();
        setLoadedRecordId(json.record_id);
        setQuestionText(json.question || "");
        setDbIdText(json.db_id != null ? String(json.db_id) : "");
        const gts = json.ground_truth_sqls?.length ? json.ground_truth_sqls : [""];
        setGroundTruthSqls(gts);

        const pls = json.pipelines || [];
        setPipelines(pls);
        setSavedGroundTruthRowCounts(json.ground_truth_row_counts ?? []);
        setSavedGroundTruthDfs(json.ground_truth_dfs ?? []);

        const pref =
          options?.preferredPipelineName !== undefined
            ? options.preferredPipelineName
            : selectedPipelineName;
        const match =
          (pref ? pls.find((p) => p.name === pref) : undefined) || pls[0] || null;
        setSelectedPipelineName(match?.name ?? null);
        setPredictedSql(match?.predicted_sql ?? "");

        const benchId = selectedBenchmark.benchmark_id;
        const gtCounts = json.ground_truth_row_counts ?? [];

        if (match && hasPlaygroundCachedEvaluation(match)) {
          setPlaygroundResult({
            benchmark_id: benchId,
            record_id: json.record_id,
            evaluation: match.evaluation as Record<string, unknown>,
            ground_truth_row_counts: gtCounts,
            ground_truth_dfs: json.ground_truth_dfs ?? [],
            predicted_df: match.predicted_df ?? null,
            prediction_error: match.prediction_error ?? null,
            prediction_row_count: match.prediction_row_count ?? null,
            prediction_column_count: match.prediction_column_count ?? null,
          });
        } else {
          setPlaygroundResult(null);
          if (options?.autoEvaluate) {
            const gtsTrim = gts.map((s) => s.trim()).filter(Boolean);
            const pred = (match?.predicted_sql ?? "").trim();
            if (gtsTrim.length > 0 && pred) {
              await executePlaygroundEvaluate({
                record_id: json.record_id,
                ground_truth_sqls: gtsTrim,
                predicted_sql: pred,
                merge_pipeline: match?.name ?? null,
              });
            }
          }
        }
      } catch (e: unknown) {
        setPlaygroundError(e instanceof Error ? e.message : "Failed to load record");
      } finally {
        setInitLoading(false);
      }
    },
    [selectedBenchmark, selectedPipelineName, executePlaygroundEvaluate]
  );

  const loadPlayground = useCallback(() => {
    void loadPlaygroundForRecordId(effectiveRecordId);
  }, [loadPlaygroundForRecordId, effectiveRecordId]);

  const loadPlaygroundRef = useRef(loadPlaygroundForRecordId);
  loadPlaygroundRef.current = loadPlaygroundForRecordId;

  useEffect(() => {
    const bid = selectedBenchmark?.benchmark_id;
    if (!bid || recordItems.length === 0) return;
    if (autoLoadedForBenchRef.current === bid) return;
    autoLoadedForBenchRef.current = bid;
    const hasDefault = recordItems.some((r) => String(r.record_id) === DEFAULT_PLAYGROUND_RECORD_ID);
    if (hasDefault) {
      setSelectedRecordId(DEFAULT_PLAYGROUND_RECORD_ID);
      setRecordIdManual("");
    } else {
      setSelectedRecordId(null);
      setRecordIdManual(DEFAULT_PLAYGROUND_RECORD_ID);
    }
    void loadPlaygroundRef.current(DEFAULT_PLAYGROUND_RECORD_ID, {
      preferredPipelineName: DEFAULT_PLAYGROUND_PIPELINE,
      autoEvaluate: true,
    });
  }, [selectedBenchmark?.benchmark_id, recordItems]);

  const pickRandomRecord = () => {
    if (recordItems.length === 0) return;
    const pick = recordItems[Math.floor(Math.random() * recordItems.length)];
    setSelectedRecordId(String(pick.record_id));
    setRecordIdManual("");
    void loadPlaygroundForRecordId(String(pick.record_id));
  };

  const onPipelinePick = (item: PipelinePickItem | null) => {
    const name = item?.name ?? null;
    setSelectedPipelineName(name);
    if (name && pipelines.length > 0) {
      const pl = pipelines.find((p) => p.name === name);
      if (pl?.predicted_sql != null) setPredictedSql(pl.predicted_sql);
      if (pl && hasPlaygroundCachedEvaluation(pl) && selectedBenchmark && loadedRecordId) {
        setPlaygroundResult({
          benchmark_id: selectedBenchmark.benchmark_id,
          record_id: loadedRecordId,
          evaluation: pl.evaluation as Record<string, unknown>,
          ground_truth_row_counts: savedGroundTruthRowCounts,
          ground_truth_dfs: savedGroundTruthDfs,
          predicted_df: pl.predicted_df ?? null,
          prediction_error: pl.prediction_error ?? null,
          prediction_row_count: pl.prediction_row_count ?? null,
          prediction_column_count: pl.prediction_column_count ?? null,
        });
      } else {
        setPlaygroundResult(null);
      }
    }
  };

  const runPlaygroundEvaluate = async () => {
    if (!selectedBenchmark || !loadedRecordId) {
      setPlaygroundError("Load a record first (use Load record).");
      return;
    }
    const gts = groundTruthSqls.map((s) => s.trim()).filter(Boolean);
    if (!gts.length) {
      setPlaygroundError("Enter at least one ground truth SQL.");
      return;
    }
    const pred = predictedSql.trim();
    if (!pred) {
      setPlaygroundError("Enter predicted SQL.");
      return;
    }
    await executePlaygroundEvaluate({
      record_id: loadedRecordId,
      ground_truth_sqls: gts,
      predicted_sql: pred,
      merge_pipeline: selectedPipelineName,
    });
  };

  const metricRows = useMemo(() => {
    if (!playgroundResult?.evaluation) return [];
    const ev = playgroundResult.evaluation as Record<string, unknown>;
    const orderedNames: string[] = [];
    metricDefs.forEach((d) => {
      if (d.name in ev && !TEXT_DETAIL_KEYS.has(d.name) && d.name !== LLM_EXPLANATION_KEY) {
        orderedNames.push(d.name);
      }
    });
    Object.keys(ev).forEach((k) => {
      if (!TEXT_DETAIL_KEYS.has(k) && k !== LLM_EXPLANATION_KEY && !orderedNames.includes(k)) {
        orderedNames.push(k);
      }
    });
    return orderedNames.map((name) => ({
      id: name,
      name,
      value: formatCellValue(ev[name]),
      description: defByName.get(name)?.description ?? "—",
      group: defByName.get(name)?.group ?? "Other",
      value_type: defByName.get(name)?.value_type ?? "text",
    }));
  }, [playgroundResult, metricDefs, defByName]);

  const metricGroupSections = useMemo(() => {
    if (metricRows.length === 0) return [];
    const byGroup = new Map<string, typeof metricRows>();
    for (const row of metricRows) {
      const g = row.group;
      if (!byGroup.has(g)) byGroup.set(g, []);
      byGroup.get(g)!.push(row);
    }
    const inOrder = metricGroupsOrder.filter((g) => byGroup.has(g));
    const rest = [...byGroup.keys()]
      .filter((g) => !metricGroupsOrder.includes(g))
      .sort((a, b) => {
        if (a === "Other") return 1;
        if (b === "Other") return -1;
        return a.localeCompare(b);
      });
    const fullOrder = [...inOrder, ...rest];
    return fullOrder.map((group) => ({
      group,
      intro: METRIC_GROUP_INTRO[group] ?? null,
      rows: byGroup.get(group)!,
    }));
  }, [metricRows, metricGroupsOrder]);

  const llmExplanationText = useMemo(() => {
    if (!playgroundResult?.evaluation) return "";
    const ev = playgroundResult.evaluation as Record<string, unknown>;
    const v = ev[LLM_EXPLANATION_KEY];
    if (v === undefined || v === null || v === "") return "";
    return formatCellValue(v);
  }, [playgroundResult]);

  const textDetails = useMemo(() => {
    if (!playgroundResult?.evaluation) return [];
    const ev = playgroundResult.evaluation as Record<string, unknown>;
    const out: { key: string; value: string }[] = [];
    TEXT_DETAIL_KEYS.forEach((k) => {
      if (k in ev && ev[k] !== undefined && ev[k] !== null && ev[k] !== "") {
        out.push({ key: k, value: formatCellValue(ev[k]) });
      }
    });
    return out;
  }, [playgroundResult]);

  if (benchmarks.length === 0) {
    return (
      <div style={{ display: "flex", flexDirection: "column", gap: "1rem" }}>
        <h3 style={{ margin: 0 }}>Eval Playground</h3>
        <p style={{ margin: 0 }}>Loading benchmarks…</p>
      </div>
    );
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "1rem" }}>
      <h3 style={{ margin: 0 }}>Eval Playground</h3>
      <p style={{ margin: 0, maxWidth: "52rem", lineHeight: 1.45 }}>
        Pick a benchmark and pipeline, then load a record (a random one is loaded automatically when possible). Edit
        ground-truth and predicted SQL and run per-record metrics. Metrics below include short descriptions from the
        toolkit. <strong>logic_execution_accuracy</strong> only uses <code>logic_df</code> when that field is present
        (e.g. from merged pipeline context); otherwise it follows subset match.
      </p>

      {recordIdsError && (
        <InlineNotification
          kind="error"
          title="Could not load record list"
          subtitle={recordIdsError}
          lowContrast
        />
      )}
      {playgroundError && (
        <InlineNotification kind="error" title="Eval Playground" subtitle={playgroundError} lowContrast />
      )}

      <section
        style={{
          display: "grid",
          gridTemplateColumns: "minmax(0, 1fr)",
          gap: "0.75rem",
        }}
      >
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))",
            gap: "0.5rem",
            alignItems: "end",
          }}
        >
          <ComboBox
            id="pg-benchmark-select"
            titleText="Benchmark"
            items={benchmarks}
            itemToString={(item) => (item ? item.benchmark_id : "")}
            selectedItem={
              selectedBenchmark
                ? benchmarks.find((b) => b.benchmark_id === selectedBenchmark.benchmark_id) ?? selectedBenchmark
                : null
            }
            onChange={(e) => {
              autoLoadedForBenchRef.current = null;
              setSelectedBenchmark(e.selectedItem as BenchmarkSummary);
              setRecordItems([]);
              setSelectedRecordId(null);
              setRecordIdManual("");
              setLoadedRecordId("");
              setPlaygroundResult(null);
              setPipelines([]);
            }}
            placeholder="Select benchmark"
          />
          <ComboBox
            id="pg-pipeline-bench"
            titleText="Pipeline"
            items={pipelineComboItems}
            itemToString={(item) => (item ? item.name : "")}
            selectedItem={selectedPipelineItem}
            onChange={(e) => onPipelinePick(e.selectedItem as PipelinePickItem | null)}
            placeholder={
              benchPipelineNames.length === 0 && pipelines.length === 0
                ? "Load a record for pipelines"
                : "Select pipeline"
            }
            disabled={pipelineComboItems.length === 0}
          />
          <ComboBox
            id="pg-record-combo"
            titleText="Record (search)"
            items={recordItems}
            itemToString={(item) => (item ? String(item.record_id) : "")}
            selectedItem={selectedRecordItem}
            onChange={(e) => {
              const item = e.selectedItem as RecordIdItem | null;
              setSelectedRecordId(item != null ? String(item.record_id) : null);
            }}
            placeholder={recordItems.length === 0 ? "No records (check benchmark data)" : "Pick a record"}
            disabled={recordItems.length === 0}
          />
          <TextInput
            id="pg-record-manual"
            labelText="Record ID (optional override)"
            placeholder="e.g. 1471"
            value={recordIdManual}
            onChange={(e) => setRecordIdManual(e.target.value)}
          />
          <Button
            kind="primary"
            onClick={() => void loadPlayground()}
            disabled={initLoading || !selectedBenchmark || !effectiveRecordId}
          >
            {initLoading ? "Loading…" : "Load record"}
          </Button>
          <Button kind="secondary" onClick={() => void pickRandomRecord()} disabled={initLoading || recordItems.length === 0}>
            Random record
          </Button>
        </div>

        {benchPipelineNames.length === 0 && selectedBenchmark && (
          <p style={{ margin: 0, fontSize: "0.8125rem", color: "var(--cds-text-secondary)" }}>
            No summary file found for this benchmark yet; pipeline names appear after you have evaluation results, or
            from the loaded record below.
          </p>
        )}

        {questionText && (
          <p style={{ margin: 0, fontSize: "0.875rem", color: "var(--cds-text-secondary)" }}>
            <strong>Question:</strong> {questionText}
            {dbIdText ? (
              <>
                {" "}
                <strong>db_id:</strong> {dbIdText}
              </>
            ) : null}
          </p>
        )}

        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fit, minmax(280px, 1fr))",
            gap: "0.75rem",
            alignItems: "stretch",
          }}
        >
          <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem", minWidth: 0 }}>
            {groundTruthSqls.map((sql, idx) => (
              <TextArea
                key={`gt-${idx}`}
                labelText={
                  groundTruthSqls.length > 1 ? `Ground truth SQL (${idx + 1})` : "Ground truth SQL"
                }
                value={sql}
                onChange={(e) => {
                  const next = [...groundTruthSqls];
                  next[idx] = e.target.value;
                  setGroundTruthSqls(next);
                }}
                rows={8}
                style={{ resize: "vertical", minHeight: "8rem", fontFamily: "monospace" }}
              />
            ))}
          </div>
          <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem", minWidth: 0 }}>
            <TextArea
              id="pg-pred-sql"
              labelText="Predicted SQL"
              value={predictedSql}
              onChange={(e) => setPredictedSql(e.target.value)}
              rows={8}
              style={{ resize: "vertical", minHeight: "8rem", fontFamily: "monospace" }}
            />
          </div>
        </div>

        <div
          style={{
            border: "1px solid var(--cds-border-subtle-01)",
            borderRadius: 4,
            padding: "1rem",
            backgroundColor: "var(--cds-layer-01)",
            display: "flex",
            flexDirection: "column",
            gap: "0.75rem",
          }}
        >
          <p style={{ margin: 0, fontSize: "0.875rem", fontWeight: 600 }}>LLM judge</p>
          <div
            style={{
              display: "flex",
              flexWrap: "wrap",
              justifyContent: "space-between",
              alignItems: "flex-end",
              gap: "1rem",
            }}
          >
            <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}>
              <Checkbox
                id="pg-use-llm"
                labelText="Use LLM-as-judge"
                checked={useLLMPlayground}
                onChange={(_, { checked }) => setUseLLMPlayground(checked)}
              />
              <Checkbox
                id="pg-force-llm"
                labelText="Force rerun LLM judge"
                checked={forceRerunLLMPlayground}
                disabled={!useLLMPlayground}
                onChange={(_, { checked }) => setForceRerunLLMPlayground(checked)}
              />
            </div>
            <div style={{ flex: "1 1 16rem", maxWidth: "28rem", minWidth: "min(100%, 14rem)" }}>
              <ComboBox
                id="pg-llm-config"
                titleText="LLM judge config (optional)"
                items={llmJudgeComboItems}
                itemToString={(item) => (item ? item.name : "")}
                selectedItem={selectedLlmConfigItem}
                onChange={(e) => {
                  const item = e.selectedItem as LLMJudgeConfigItem | null;
                  setLlmConfigPathPlayground(item?.path ?? "");
                }}
                disabled={!useLLMPlayground}
                placeholder={llmJudgeConfigs.length === 0 ? "Default or load configs…" : "Select config"}
              />
            </div>
          </div>
        </div>

        <div style={{ display: "flex", gap: "0.5rem", flexWrap: "wrap" }}>
          <Button
            kind="primary"
            onClick={() => void runPlaygroundEvaluate()}
            disabled={playgroundLoading || !loadedRecordId}
          >
            {playgroundLoading ? "Evaluating…" : "Run evaluation"}
          </Button>
        </div>

        {playgroundResult && (
          <div style={{ display: "flex", flexDirection: "column", gap: "1.25rem" }}>
            {playgroundResult.prediction_error != null && (
              <InlineNotification
                kind="warning"
                title="Predicted SQL did not execute"
                subtitle={playgroundResult.prediction_error}
                lowContrast
              />
            )}
            {playgroundResult.ground_truth_row_counts?.length ? (
              <p style={{ margin: 0, fontSize: "0.875rem" }}>
                <strong>GT result row counts:</strong> {playgroundResult.ground_truth_row_counts.join(", ")}
                {playgroundResult.prediction_row_count != null && (
                  <>
                    {" "}
                    <strong>Predicted rows/cols:</strong> {playgroundResult.prediction_row_count} /{" "}
                    {playgroundResult.prediction_column_count ?? "—"}
                  </>
                )}
              </p>
            ) : null}

            {(playgroundResult.ground_truth_dfs?.length ?? 0) > 0 || playgroundResult.predicted_df != null ? (
              <div
                style={{
                  border: "1px solid var(--cds-border-subtle-01)",
                  borderRadius: 4,
                  padding: "1rem",
                  backgroundColor: "var(--cds-layer-01)",
                  display: "flex",
                  flexDirection: "column",
                  gap: "0.75rem",
                }}
              >
                <div>
                  <h4 style={{ margin: 0, fontSize: "1rem", fontWeight: 600 }}>Result dataframes</h4>
                  <p
                    style={{
                      margin: "0.35rem 0 0",
                      fontSize: "0.8125rem",
                      lineHeight: 1.45,
                      color: "var(--cds-text-secondary)",
                      maxWidth: "52rem",
                    }}
                  >
                    Ground-truth and predicted result tables (pandas orient=&quot;split&quot;). Scroll when large.
                  </p>
                </div>
                <div
                  style={{
                    display: "grid",
                    gridTemplateColumns: "repeat(auto-fit, minmax(min(100%, 22rem), 1fr))",
                    gap: "1.25rem",
                    alignItems: "start",
                  }}
                >
                  <div style={{ display: "flex", flexDirection: "column", gap: "1rem", minWidth: 0 }}>
                    <p style={{ margin: 0, fontSize: "0.875rem", fontWeight: 600 }}>Ground truth dataframe(s)</p>
                    {(playgroundResult.ground_truth_dfs ?? []).length === 0 ? (
                      <p style={{ margin: 0, fontSize: "0.8125rem", color: "var(--cds-text-secondary)" }}>
                        No serialized ground-truth dataframe in this response (run evaluation to compute from SQL).
                      </p>
                    ) : (
                      (playgroundResult.ground_truth_dfs ?? []).map((dfJson, i) => (
                        <DataFramePreview
                          key={`gt-df-${i}`}
                          title={groundTruthSqls.length > 1 ? `Ground truth ${i + 1}` : "Ground truth"}
                          dfJson={dfJson}
                          emptyMessage="Could not parse dataframe JSON."
                        />
                      ))
                    )}
                  </div>
                  <div style={{ minWidth: 0 }}>
                    <p style={{ margin: "0 0 1rem", fontSize: "0.875rem", fontWeight: 600 }}>Prediction dataframe</p>
                    <DataFramePreview
                      title="Predicted SQL result"
                      dfJson={playgroundResult.predicted_df}
                      emptyMessage={
                        playgroundResult.prediction_error != null
                          ? "Predicted SQL did not execute; no dataframe."
                          : "No prediction dataframe in this response."
                      }
                    />
                  </div>
                </div>
              </div>
            ) : null}

            <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}>
              <h4 style={{ margin: 0, fontSize: "1rem", fontWeight: 600 }}>Evaluation metrics</h4>
              <p style={{ margin: 0, fontSize: "0.8125rem", color: "var(--cds-text-secondary)", maxWidth: "48rem" }}>
                Each section groups related scores. The toolkit name is shown in monospace; the description explains how to
                read the value.
              </p>
            </div>

            {metricGroupSections.map(({ group, intro, rows }) => (
              <div
                key={group}
                style={{
                  border: "1px solid var(--cds-border-subtle-01)",
                  borderRadius: 4,
                  padding: "1rem",
                  backgroundColor: "var(--cds-layer-01)",
                  display: "flex",
                  flexDirection: "column",
                  gap: "0.75rem",
                }}
              >
                <div>
                  <h5 style={{ margin: 0, fontSize: "0.9375rem", fontWeight: 600 }}>{group}</h5>
                  {intro ? (
                    <p
                      style={{
                        margin: "0.35rem 0 0",
                        fontSize: "0.8125rem",
                        lineHeight: 1.45,
                        color: "var(--cds-text-secondary)",
                        maxWidth: "52rem",
                      }}
                    >
                      {intro}
                    </p>
                  ) : null}
                </div>
                <TableContainer>
                  <Table size="sm">
                    <TableHead>
                      <TableRow>
                        <TableHeader style={{ width: "26%" }}>Metric</TableHeader>
                        <TableHeader style={{ width: "10%" }}>Type</TableHeader>
                        <TableHeader style={{ width: "14%" }}>Value</TableHeader>
                        <TableHeader>What it means</TableHeader>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {rows.map((row) => (
                        <TableRow key={row.id}>
                          <TableCell
                            style={{
                              fontFamily: "monospace",
                              fontSize: "0.8125rem",
                              verticalAlign: "top",
                            }}
                          >
                            {row.name}
                          </TableCell>
                          <TableCell style={{ verticalAlign: "top" }}>
                            <Tag type="gray" size="sm">
                              {valueTypeLabel(row.value_type)}
                            </Tag>
                          </TableCell>
                          <TableCell
                            style={{
                              whiteSpace: "pre-wrap",
                              wordBreak: "break-word",
                              verticalAlign: "top",
                              fontVariantNumeric: "tabular-nums",
                            }}
                          >
                            {row.value}
                          </TableCell>
                          <TableCell
                            style={{
                              fontSize: "0.8125rem",
                              lineHeight: 1.45,
                              verticalAlign: "top",
                              color: "var(--cds-text-primary)",
                            }}
                          >
                            {row.description}
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </TableContainer>
                {group === "LLM judge" && llmExplanationText ? (
                  <div style={{ display: "flex", flexDirection: "column", gap: "0.35rem" }}>
                    <p style={{ margin: 0, fontSize: "0.8125rem", fontWeight: 600 }}>LLM explanation</p>
                    <p
                      style={{
                        margin: 0,
                        fontSize: "0.8125rem",
                        lineHeight: 1.45,
                        color: "var(--cds-text-secondary)",
                      }}
                    >
                      {defByName.get(LLM_EXPLANATION_KEY)?.description ??
                        "Short explanation from the LLM judge when invoked."}
                    </p>
                    <div
                      style={{
                        maxHeight: "14rem",
                        overflow: "auto",
                        padding: "0.75rem",
                        borderRadius: 4,
                        border: "1px solid var(--cds-border-subtle-01)",
                        backgroundColor: "var(--cds-layer-02)",
                        fontSize: "0.8125rem",
                        lineHeight: 1.45,
                        whiteSpace: "pre-wrap",
                        wordBreak: "break-word",
                        fontFamily: "var(--cds-code-01-font-family, monospace)",
                      }}
                    >
                      {llmExplanationText}
                    </div>
                  </div>
                ) : null}
              </div>
            ))}

            {textDetails.length > 0 && (
              <div
                style={{
                  border: "1px solid var(--cds-border-subtle-01)",
                  borderRadius: 4,
                  padding: "1rem",
                  backgroundColor: "var(--cds-layer-01)",
                  display: "flex",
                  flexDirection: "column",
                  gap: "0.75rem",
                }}
              >
                <div>
                  <h5 style={{ margin: 0, fontSize: "0.9375rem", fontWeight: 600 }}>Text and debug fields</h5>
                  <p
                    style={{
                      margin: "0.35rem 0 0",
                      fontSize: "0.8125rem",
                      lineHeight: 1.45,
                      color: "var(--cds-text-secondary)",
                      maxWidth: "52rem",
                    }}
                  >
                    Long-form strings from evaluation (errors, serialized ground-truth data). The LLM explanation
                    appears under the LLM judge section above when present.
                  </p>
                </div>
                <TableContainer>
                  <Table size="sm">
                    <TableHead>
                      <TableRow>
                        <TableHeader style={{ width: "22%" }}>Field</TableHeader>
                        <TableHeader>Content</TableHeader>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {textDetails.map((t) => (
                        <TableRow key={t.key}>
                          <TableCell
                            style={{
                              fontFamily: "monospace",
                              fontSize: "0.8125rem",
                              verticalAlign: "top",
                            }}
                          >
                            {t.key}
                          </TableCell>
                          <TableCell style={{ whiteSpace: "pre-wrap", wordBreak: "break-word", verticalAlign: "top" }}>
                            {t.value}
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </TableContainer>
              </div>
            )}
          </div>
        )}
      </section>
    </div>
  );
};
