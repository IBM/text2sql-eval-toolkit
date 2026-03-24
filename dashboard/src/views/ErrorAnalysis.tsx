import React, { useEffect, useMemo, useState } from "react";
import {
  Button,
  DataTable,
  DataTableHeader,
  InlineNotification,
  Pagination,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableHeader,
  TableRow,
  TextInput,
  Select,
  SelectItem,
  SelectItemGroup,
  TextArea,
} from "@carbon/react";
import { apiUrl } from "../lib/api";
import {
  type MetricDefinitionsResponse,
  buildMetricInsightsSelectGroups,
  flattenMetricInsightsSelectNames,
} from "../lib/metricInsightsSelect";

interface Props {
  benchmarkId: string;
  onBack?: () => void;
  initialFilters?: Partial<ErrorAnalysisFilters>;
}

interface ErrorRecordSummary {
  record_id: string;
  question: string;
  predictions: Record<string, Record<string, any>>;
}

type ErrorAnalysisFilters = {
  pipeline: string;
  metric: string;
  value: string;
  op: string;
  pipeline2: string;
  metric2: string;
  disagree: boolean;
};

interface PaginatedErrorResponse {
  items: ErrorRecordSummary[];
  total: number;
  page: number;
  page_size: number;
}

type LoadOverrides = Partial<
  ErrorAnalysisFilters & { page: number; pageSize: number; search: string }
>;

interface ErrorRecordDetail {
  record_id: string;
  pipeline: string;
  question: string;
  db_id?: string;
  ground_truth_sql: string[];
  predicted_sql?: string;
  evaluation_metrics: Record<string, any>;
  ground_truth_results: any[];
  predicted_result: any;
  prompt?: string;
  llm_judge_score?: number;
  llm_judge_explanation?: string;
  sql_execution_error?: string;
  inference_error?: string;
}

interface ExecuteSqlResponse {
  benchmark_id: string;
  db_type: string;
  sql: string;
  db_id?: string;
  execution_time_ms: number;
  row_count: number;
  column_count: number;
  result: any;
}

interface AddGroundTruthSqlResponse {
  benchmark_id: string;
  record_id: string;
  added: boolean;
  message: string;
  ground_truth_count: number;
}

function escapeHtml(text: string): string {
  return text.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
}

function highlightSql(sql: string): string {
  const escaped = escapeHtml(sql);
  const keywords = [
    "SELECT","FROM","WHERE","GROUP BY","ORDER BY","HAVING","LIMIT","JOIN","LEFT JOIN",
    "RIGHT JOIN","INNER JOIN","OUTER JOIN","ON","AS","AND","OR","NOT","IN","EXISTS",
    "COUNT","SUM","AVG","MIN","MAX","DISTINCT","CASE","WHEN","THEN","ELSE","END",
  ];
  const sorted = keywords.sort((a, b) => b.length - a.length);
  let html = escaped;
  sorted.forEach((kw) => {
    const token = kw.replace(/\s+/g, "\\s+");
    const re = new RegExp(`\\b${token}\\b`, "gi");
    html = html.replace(
      re,
      (m) => `<span style="color:#0f62fe;font-weight:600;">${m.toUpperCase()}</span>`
    );
  });
  return html;
}

function normalizeTableData(raw: any): { columns: string[]; rows: any[] } {
  let value = raw;
  if (typeof value === "string") {
    try {
      value = JSON.parse(value);
    } catch {
      return { columns: ["value"], rows: [{ value }] };
    }
  }
  if (
    value &&
    typeof value === "object" &&
    Array.isArray(value.columns) &&
    Array.isArray(value.data)
  ) {
    const columns = value.columns.map((c: any) => String(c));
    const rows = value.data.map((row: any[], idx: number) => {
      const out: Record<string, any> = { id: `r-${idx}` };
      columns.forEach((c, i) => {
        out[c] = row?.[i];
      });
      return out;
    });
    return { columns, rows };
  }
  if (Array.isArray(value)) {
    if (value.length === 0) return { columns: [], rows: [] };
    if (typeof value[0] === "object" && value[0] !== null && !Array.isArray(value[0])) {
      const columnSet = new Set<string>();
      value.forEach((v) => Object.keys(v).forEach((k) => columnSet.add(k)));
      const columns = Array.from(columnSet);
      const rows = value.map((v, idx) => ({ id: `r-${idx}`, ...v }));
      return { columns, rows };
    }
    const rows = value.map((v, idx) => ({ id: `r-${idx}`, value: v }));
    return { columns: ["value"], rows };
  }
  if (value && typeof value === "object") {
    return { columns: Object.keys(value), rows: [{ id: "r-0", ...value }] };
  }
  return { columns: ["value"], rows: [{ id: "r-0", value: String(value) }] };
}

const ResultTableView: React.FC<{ title: string; rawData: any }> = ({ title, rawData }) => {
  const normalized = useMemo(() => normalizeTableData(rawData), [rawData]);
  const headers: DataTableHeader[] = normalized.columns.map((c) => ({ key: c, header: c }));
  return (
    <section
      style={{
        border: "1px solid rgba(15,98,254,0.2)",
        borderRadius: "6px",
        padding: "0.6rem",
        background: "#ffffff",
      }}
    >
      <h4 style={{ margin: "0 0 0.5rem 0", color: "#0f62fe" }}>{title}</h4>
      {headers.length === 0 ? (
        <div style={{ opacity: 0.8 }}>No rows</div>
      ) : (
        <div style={{ maxHeight: "240px", overflow: "auto" }}>
          <DataTable rows={normalized.rows} headers={headers} size="sm">
            {({ rows, headers, getHeaderProps }) => (
              <TableContainer>
                <Table aria-label={title}>
                  <TableHead>
                    <TableRow>
                      {headers.map((header) => (
                        <TableHeader key={header.key} {...getHeaderProps({ header })}>
                          {header.header}
                        </TableHeader>
                      ))}
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {rows.map((row) => (
                      <TableRow key={row.id}>
                        {row.cells.map((cell) => (
                          <TableCell key={cell.id}>
                            {cell.value == null ? "NULL" : String(cell.value)}
                          </TableCell>
                        ))}
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>
            )}
          </DataTable>
        </div>
      )}
    </section>
  );
};

export const ErrorAnalysis: React.FC<Props> = ({ benchmarkId, onBack, initialFilters }) => {
  const [items, setItems] = useState<ErrorRecordSummary[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(25);
  const [search, setSearch] = useState("");
  const [pipeline, setPipeline] = useState(() => initialFilters?.pipeline ?? "");
  const [metric, setMetric] = useState(() => initialFilters?.metric ?? "execution_accuracy");
  const [value, setValue] = useState(() => initialFilters?.value ?? "0");
  const [op, setOp] = useState(() => initialFilters?.op ?? "eq");
  const [pipeline2, setPipeline2] = useState(() => initialFilters?.pipeline2 ?? "");
  const [metric2, setMetric2] = useState(
    () => initialFilters?.metric2 ?? "subset_non_empty_execution_accuracy"
  );
  const [disagree, setDisagree] = useState(() => initialFilters?.disagree ?? false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedRecordId, setSelectedRecordId] = useState<string | null>(null);
  const [selectedRecordPipeline, setSelectedRecordPipeline] = useState<string | null>(null);
  const [detail, setDetail] = useState<ErrorRecordDetail | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState<string | null>(null);
  const [detailViewMode, setDetailViewMode] = useState<"detail" | "raw" | "modify">("detail");
  const [rawJsonRecord, setRawJsonRecord] = useState<Record<string, any> | null>(null);
  const [rawJsonLoading, setRawJsonLoading] = useState(false);
  const [rawJsonError, setRawJsonError] = useState<string | null>(null);
  const [modifySourceLabel, setModifySourceLabel] = useState<string>("");
  const [modifySql, setModifySql] = useState<string>("");
  const [modifyLoading, setModifyLoading] = useState(false);
  const [modifyError, setModifyError] = useState<string | null>(null);
  const [modifyResponse, setModifyResponse] = useState<ExecuteSqlResponse | null>(null);
  const [addGroundTruthLoading, setAddGroundTruthLoading] = useState(false);
  const [addGroundTruthError, setAddGroundTruthError] = useState<string | null>(null);
  const [addGroundTruthSuccess, setAddGroundTruthSuccess] = useState<string | null>(null);

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
    setMetric((m) => (allowed.has(m) ? m : names[0]));
    setMetric2((m2) => (allowed.has(m2) ? m2 : names[1] ?? names[0]));
  }, [metricDefinitions]);

  useEffect(() => {
    const loadDefaultPipeline = async () => {
      try {
        const res = await fetch(
          apiUrl(`/api/benchmarks/${benchmarkId}/summary/by-category`)
        );
        if (!res.ok) return;
        const json = (await res.json()) as {
          overall?: { name: string; metrics: Record<string, any> }[];
        };
        const ranked = [...(json.overall ?? [])].sort((a, b) => {
          const av = Number(a.metrics?.subset_non_empty_execution_accuracy?.average ?? -1);
          const bv = Number(b.metrics?.subset_non_empty_execution_accuracy?.average ?? -1);
          return bv - av;
        });
        const bestPipeline = ranked[0]?.name ?? "";
        if (!bestPipeline) return;
        setPipeline((p) => p || bestPipeline);
        setPipeline2((p) => p || bestPipeline);
      } catch {
        // Keep UX resilient; defaults are best-effort.
      }
    };
    void loadDefaultPipeline();
  }, [benchmarkId]);

  const load = async (overrides?: LoadOverrides) => {
    const effectivePage = overrides?.page ?? page;
    const effectivePageSize = overrides?.pageSize ?? pageSize;
    const effectiveSearch = overrides?.search ?? search;
    const effectivePipeline = overrides?.pipeline ?? pipeline;
    const effectiveMetric = overrides?.metric ?? metric;
    const effectiveValue = overrides?.value ?? value;
    const effectiveOp = overrides?.op ?? op;
    const effectivePipeline2 = overrides?.pipeline2 ?? pipeline2;
    const effectiveMetric2 = overrides?.metric2 ?? metric2;
    const effectiveDisagree = overrides?.disagree ?? disagree;

    try {
      setLoading(true);
      setError(null);
      const params = new URLSearchParams();
      params.set("page", String(effectivePage));
      params.set("page_size", String(effectivePageSize));
      if (effectiveSearch) params.set("q", effectiveSearch);
      if (effectivePipeline) {
        params.set("pipeline", effectivePipeline);
        if (effectiveMetric) params.set("metric", effectiveMetric);
        if (effectiveValue) params.set("value", effectiveValue);
        if (effectiveOp) params.set("op", effectiveOp);
      }
      if (effectivePipeline && effectivePipeline2 && effectiveDisagree) {
        params.set("pipeline2", effectivePipeline2);
        if (effectiveMetric2) params.set("metric2", effectiveMetric2);
        params.set("disagree", "true");
      }
      const res = await fetch(
        apiUrl(`/api/benchmarks/${benchmarkId}/errors?${params.toString()}`)
      );
      if (!res.ok) {
        throw new Error(`HTTP ${res.status}`);
      }
      const json: PaginatedErrorResponse = await res.json();
      setItems(json.items);
      setTotal(json.total);
    } catch (e: any) {
      setError(e.message || "Failed to load error records");
    } finally {
      setLoading(false);
    }
  };

  /** Same fetch as "Apply filters" — quick presets call `load({...})` with explicit params. */
  const applyFilters = () => {
    setPage(1);
    void load({ page: 1 });
  };

  useEffect(() => {
    void load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [benchmarkId, page, pageSize]);

  const headers: DataTableHeader[] = [
    { key: "record_id", header: "Record ID" },
    { key: "question", header: "Question" },
    { key: "summary", header: "Summary (first pipeline metrics)" },
  ];

  const rows = useMemo(
    () =>
      items.map((item) => {
        const firstPipeline = Object.entries(item.predictions)[0];
        const summary =
          firstPipeline && firstPipeline[1]
            ? `${firstPipeline[0]}: exec=${firstPipeline[1].execution_accuracy}, subset=${firstPipeline[1].subset_non_empty_execution_accuracy}`
            : "";
        return {
          id: item.record_id,
          record_id: item.record_id,
          question: item.question,
          summary,
        };
      }),
    [items]
  );

  useEffect(() => {
    if (!selectedRecordId || !selectedRecordPipeline) return;
    const loadDetail = async () => {
      try {
        setDetailLoading(true);
        setDetailError(null);
        const params = new URLSearchParams();
        params.set("pipeline", selectedRecordPipeline);
        const res = await fetch(
          apiUrl(
            `/api/benchmarks/${benchmarkId}/errors/${selectedRecordId}/detail?${params.toString()}`
          )
        );
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        setDetail((await res.json()) as ErrorRecordDetail);
      } catch (e: any) {
        setDetailError(e.message || "Failed to load record details");
      } finally {
        setDetailLoading(false);
      }
    };
    void loadDetail();
  }, [benchmarkId, selectedRecordId, selectedRecordPipeline]);

  const closeDetail = () => {
    setSelectedRecordId(null);
    setSelectedRecordPipeline(null);
    setDetail(null);
    setDetailError(null);
    setDetailLoading(false);
    setDetailViewMode("detail");
    setRawJsonRecord(null);
    setRawJsonError(null);
    setRawJsonLoading(false);
    setModifySourceLabel("");
    setModifySql("");
    setModifyLoading(false);
    setModifyError(null);
    setModifyResponse(null);
    setAddGroundTruthLoading(false);
    setAddGroundTruthError(null);
    setAddGroundTruthSuccess(null);
  };

  useEffect(() => {
    setDetailViewMode("detail");
    setRawJsonRecord(null);
    setRawJsonError(null);
    setRawJsonLoading(false);
    setModifySourceLabel("");
    setModifySql("");
    setModifyLoading(false);
    setModifyError(null);
    setModifyResponse(null);
    setAddGroundTruthLoading(false);
    setAddGroundTruthError(null);
    setAddGroundTruthSuccess(null);
  }, [selectedRecordId]);

  const openRawJsonView = async () => {
    if (!selectedRecordId) return;
    setDetailViewMode("raw");
    if (rawJsonRecord) return;
    try {
      setRawJsonLoading(true);
      setRawJsonError(null);
      const res = await fetch(
        apiUrl(`/api/benchmarks/${benchmarkId}/errors/${selectedRecordId}`)
      );
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      setRawJsonRecord((await res.json()) as Record<string, any>);
    } catch (e: any) {
      setRawJsonError(e.message || "Failed to load raw JSON");
    } finally {
      setRawJsonLoading(false);
    }
  };

  const openModifyQueryView = (sql: string, sourceLabel: string) => {
    setModifySourceLabel(sourceLabel);
    setModifySql(sql);
    setModifyLoading(false);
    setModifyError(null);
    setModifyResponse(null);
    setAddGroundTruthLoading(false);
    setAddGroundTruthError(null);
    setAddGroundTruthSuccess(null);
    setDetailViewMode("modify");
  };

  const executeModifiedQuery = async () => {
    if (!selectedRecordId || !modifySql.trim()) return;
    try {
      setModifyLoading(true);
      setModifyError(null);
      const res = await fetch(apiUrl(`/api/benchmarks/${benchmarkId}/execute`), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          sql: modifySql,
          record_id: selectedRecordId,
          db_id: detail?.db_id,
        }),
      });
      const payload = await res.json();
      if (!res.ok) {
        throw new Error(payload?.detail || `HTTP ${res.status}`);
      }
      setModifyResponse(payload as ExecuteSqlResponse);
      setAddGroundTruthError(null);
      setAddGroundTruthSuccess(null);
    } catch (e: any) {
      setModifyError(e.message || "Failed to execute SQL");
      setModifyResponse(null);
    } finally {
      setModifyLoading(false);
    }
  };

  const addToBenchmarkGroundTruth = async () => {
    if (!selectedRecordId || !modifySql.trim()) return;
    const confirmed = window.confirm(
      "Are you confident this query should be added to benchmark ground truth SQLs?"
    );
    if (!confirmed) return;
    try {
      setAddGroundTruthLoading(true);
      setAddGroundTruthError(null);
      setAddGroundTruthSuccess(null);
      const res = await fetch(apiUrl(`/api/benchmarks/${benchmarkId}/ground-truth-sql`), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          record_id: selectedRecordId,
          sql: modifySql,
        }),
      });
      const payload = (await res.json()) as AddGroundTruthSqlResponse | { detail?: string };
      if (!res.ok) {
        throw new Error((payload as { detail?: string })?.detail || `HTTP ${res.status}`);
      }
      setAddGroundTruthSuccess((payload as AddGroundTruthSqlResponse).message);
    } catch (e: any) {
      setAddGroundTruthError(e.message || "Failed to add query to benchmark ground truth");
    } finally {
      setAddGroundTruthLoading(false);
    }
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: "0.75rem" }}>
        <h3 style={{ margin: 0 }}>Error analysis – {benchmarkId}</h3>
        {onBack && (
          <Button kind="ghost" size="sm" onClick={onBack}>
            Back
          </Button>
        )}
      </div>
      {metricDefinitionsError && (
        <InlineNotification
          kind="warning"
          title="Metric list unavailable"
          subtitle={metricDefinitionsError}
          lowContrast
        />
      )}
      <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}>
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))",
            gap: "0.5rem",
            alignItems: "end",
          }}
        >
          <TextInput
            id="error-search"
            labelText="Search"
            placeholder="Question text or record id"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
          />
          <TextInput
            id="pipeline-1"
            labelText="Pipeline 1 (optional)"
            placeholder="e.g. wxai:openai/gpt-oss-120b-greedy-zero-shot-chatapi"
            value={pipeline}
            onChange={(e) => setPipeline(e.target.value)}
          />
          <Select
            id="metric-select"
            labelText="Metric"
            value={metric}
            onChange={(e) => setMetric(e.target.value)}
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
            id="metric2-select"
            labelText="Metric 2 (for disagreement)"
            value={metric2}
            onChange={(e) => setMetric2(e.target.value)}
            disabled={!disagree || metricInsightsGroups.length === 0}
          >
            {metricInsightsGroups.map((g) => (
              <SelectItemGroup key={`${g.label}-m2`} label={g.label}>
                {g.metrics.map((m) => (
                  <SelectItem
                    key={`${m.name}-m2`}
                    value={m.name}
                    text={m.name}
                    title={m.description}
                  />
                ))}
              </SelectItemGroup>
            ))}
          </Select>
          <Select
            id="op-select"
            labelText="Operator"
            value={op}
            onChange={(e) => setOp(e.target.value)}
          >
            <SelectItem value="eq" text="=" />
            <SelectItem value="ne" text="≠" />
            <SelectItem value="lt" text="<" />
            <SelectItem value="gt" text=">" />
            <SelectItem value="le" text="≤" />
            <SelectItem value="ge" text="≥" />
          </Select>
        </div>
        <div
          style={{
            display: "flex",
            flexWrap: "wrap",
            alignItems: "flex-end",
            gap: "0.5rem",
            width: "100%",
          }}
        >
          <div
            style={{
              flex: "1 1 400px",
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))",
              gap: "0.5rem",
              alignItems: "end",
              minWidth: 0,
            }}
          >
            <TextInput
              id="metric-value"
              labelText="Metric value"
              value={value}
              onChange={(e) => setValue(e.target.value)}
            />
            <TextInput
              id="pipeline-2"
              labelText="Pipeline 2 (for disagreement)"
              placeholder="Second pipeline id"
              value={pipeline2}
              onChange={(e) => setPipeline2(e.target.value)}
            />
            <Select
              id="disagree-select"
              labelText="P1 vs P2 disagree?"
              value={disagree ? "true" : "false"}
              onChange={(e) => setDisagree(e.target.value === "true")}
            >
              <SelectItem value="false" text="No" />
              <SelectItem value="true" text="Yes" />
            </Select>
          </div>
          <Button
            kind="primary"
            size="sm"
            onClick={applyFilters}
            disabled={loading}
            style={{ flex: "0 0 auto", marginLeft: "auto" }}
          >
            Apply filters
          </Button>
        </div>
        <div
          style={{
            display: "flex",
            flexWrap: "wrap",
            alignItems: "center",
            gap: "0.5rem",
            padding: "0.65rem 0.75rem",
            borderRadius: "6px",
            border: "1px solid rgba(15, 98, 254, 0.15)",
            background: "rgba(15, 98, 254, 0.03)",
          }}
        >
          <span
            style={{
              fontSize: "0.75rem",
              fontWeight: 600,
              letterSpacing: "0.02em",
              color: "var(--cds-text-secondary, #525252)",
              marginRight: "0.25rem",
            }}
          >
            Quick presets
          </span>
          <Button
            kind="secondary"
            size="sm"
            disabled={!pipeline}
            onClick={() => {
              const p = pipeline;
              if (!p) return;
              setPipeline(p);
              setPipeline2(p);
              setMetric("execution_accuracy");
              setMetric2("subset_non_empty_execution_accuracy");
              setValue("0");
              setOp("eq");
              setDisagree(true);
              setPage(1);
              void load({
                page: 1,
                pipeline: p,
                pipeline2: p,
                metric: "execution_accuracy",
                metric2: "subset_non_empty_execution_accuracy",
                value: "0",
                op: "eq",
                disagree: true,
              });
            }}
          >
            Exec=0 & subset=1
          </Button>
          <Button
            kind="secondary"
            size="sm"
            disabled={!pipeline}
            onClick={() => {
              const p = pipeline;
              if (!p) return;
              setPipeline(p);
              setPipeline2(p);
              setMetric("subset_non_empty_execution_accuracy");
              setMetric2("llm_score");
              setValue("0");
              setOp("eq");
              setDisagree(true);
              setPage(1);
              void load({
                page: 1,
                pipeline: p,
                pipeline2: p,
                metric: "subset_non_empty_execution_accuracy",
                metric2: "llm_score",
                value: "0",
                op: "eq",
                disagree: true,
              });
            }}
          >
            Subset=0 & llm=1
          </Button>
        </div>
      </div>
      {error && (
        <InlineNotification
          kind="error"
          title="Error loading error records"
          subtitle={error}
          lowContrast
        />
      )}
      <div style={{ maxHeight: "420px", overflow: "auto" }}>
        <DataTable rows={rows} headers={headers} size="sm">
          {({ rows, headers, getHeaderProps }) => (
            <TableContainer>
              <Table aria-label="Error records">
                <TableHead>
                  <TableRow>
                    {headers.map((header) => (
                      <TableHeader key={header.key} {...getHeaderProps({ header })}>
                        {header.header}
                      </TableHeader>
                    ))}
                  </TableRow>
                </TableHead>
                <TableBody>
                  {rows.map((row) => (
                    <TableRow
                      key={row.id}
                      style={{ cursor: "pointer" }}
                      onClick={() => {
                        const recordId = String(row.id);
                        const source = items.find((x) => x.record_id === recordId);
                        const availablePipelines = Object.keys(source?.predictions ?? {});
                        const detailPipeline =
                          (pipeline && availablePipelines.includes(pipeline) ? pipeline : null) ||
                          availablePipelines[0] ||
                          null;
                        if (!detailPipeline) return;
                        setSelectedRecordId(recordId);
                        setSelectedRecordPipeline(detailPipeline);
                      }}
                    >
                      {row.cells.map((cell) => (
                        <TableCell key={cell.id}>{cell.value}</TableCell>
                      ))}
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          )}
        </DataTable>
      </div>
      <Pagination
        page={page}
        pageSize={pageSize}
        pageSizes={[10, 25, 50, 100]}
        totalItems={total}
        onChange={({ page, pageSize }) => {
          setPage(page);
          setPageSize(pageSize);
        }}
      />
      {selectedRecordId && (
        <>
          <div
            onClick={closeDetail}
            style={{
              position: "fixed",
              inset: 0,
              background: "rgba(0,0,0,0.35)",
              zIndex: 7400,
            }}
          />
          <div
            style={{
              position: "fixed",
              top: "3rem",
              right: 0,
              bottom: 0,
              width: "min(900px, 92vw)",
              zIndex: 7500,
              background: "#ffffff",
              color: "#161616",
              borderLeft: "1px solid rgba(0,0,0,0.12)",
              padding: "0.85rem",
              overflow: "auto",
              display: "flex",
              flexDirection: "column",
              gap: "0.75rem",
            }}
          >
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <h3 style={{ margin: 0 }}>
                {detailViewMode === "raw"
                  ? `Raw JSON – ${selectedRecordId}`
                  : detailViewMode === "modify"
                  ? `Modify Query – ${selectedRecordId}`
                  : `Record detail – ${selectedRecordId}${selectedRecordPipeline ? ` (${selectedRecordPipeline})` : ""}`}
              </h3>
              <div style={{ display: "flex", gap: "0.35rem" }}>
                {detailViewMode !== "detail" && (
                  <Button kind="ghost" size="sm" onClick={() => setDetailViewMode("detail")}>
                    Back to detail
                  </Button>
                )}
                <Button kind="ghost" size="sm" onClick={closeDetail}>
                  X
                </Button>
              </div>
            </div>

            {detailViewMode === "raw" ? (
              <>
                {rawJsonLoading && (
                  <InlineNotification
                    kind="info"
                    title="Loading raw JSON..."
                    subtitle="Fetching full record payload from predictions_eval"
                    lowContrast
                  />
                )}
                {rawJsonError && (
                  <InlineNotification
                    kind="error"
                    title="Failed to load raw JSON"
                    subtitle={rawJsonError}
                    lowContrast
                  />
                )}
                {rawJsonRecord && (
                  <section>
                    <pre
                      style={{
                        margin: "0.3rem 0",
                        padding: "0.6rem",
                        background: "#f4f4f4",
                        borderRadius: "4px",
                        whiteSpace: "pre-wrap",
                        border: "1px solid rgba(15,98,254,0.2)",
                        color: "#161616",
                      }}
                    >
                      {JSON.stringify(rawJsonRecord, null, 2)}
                    </pre>
                  </section>
                )}
              </>
            ) : detailViewMode === "modify" ? (
              <>
                <section>
                  <h4 style={{ margin: "0.25rem 0", color: "#0f62fe" }}>Source SQL</h4>
                  <div style={{ marginBottom: "0.35rem" }}>
                    {modifySourceLabel || "Custom query"}
                  </div>
                </section>
                <TextArea
                  id="error-analysis-modify-query-sql"
                  labelText="Editable SQL"
                  rows={14}
                  value={modifySql}
                  onChange={(e) => setModifySql(e.target.value)}
                />
                <div style={{ display: "flex", justifyContent: "flex-end" }}>
                  <Button
                    kind="secondary"
                    size="sm"
                    disabled={modifyLoading || !modifySql.trim()}
                    onClick={() => void executeModifiedQuery()}
                  >
                    Execute
                  </Button>
                </div>
                {modifyLoading && (
                  <InlineNotification
                    kind="info"
                    title="Executing SQL..."
                    subtitle="Running query against the benchmark backend"
                    lowContrast
                  />
                )}
                {modifyError && (
                  <InlineNotification
                    kind="error"
                    title="SQL execution failed"
                    subtitle={modifyError}
                    lowContrast
                  />
                )}
                {addGroundTruthError && (
                  <InlineNotification
                    kind="error"
                    title="Failed to update ground truth"
                    subtitle={addGroundTruthError}
                    lowContrast
                  />
                )}
                {addGroundTruthSuccess && (
                  <InlineNotification
                    kind="success"
                    title="Ground truth updated"
                    subtitle={addGroundTruthSuccess}
                    lowContrast
                  />
                )}
                {modifyResponse && (
                  <>
                    <section>
                      <h4 style={{ margin: "0.25rem 0", color: "#0f62fe" }}>
                        Execution summary
                      </h4>
                      <pre
                        style={{
                          margin: "0.3rem 0",
                          padding: "0.6rem",
                          background: "#f4f4f4",
                          borderRadius: "4px",
                          whiteSpace: "pre-wrap",
                          color: "#161616",
                        }}
                      >
                        {JSON.stringify(
                          {
                            db_type: modifyResponse.db_type,
                            db_id: modifyResponse.db_id,
                            execution_time_ms: modifyResponse.execution_time_ms,
                            row_count: modifyResponse.row_count,
                            column_count: modifyResponse.column_count,
                          },
                          null,
                          2
                        )}
                      </pre>
                    </section>
                    <ResultTableView title="Execution result" rawData={modifyResponse.result} />
                    <div style={{ display: "flex", justifyContent: "flex-end" }}>
                      <Button
                        kind="primary"
                        size="sm"
                        disabled={addGroundTruthLoading || !modifySql.trim()}
                        onClick={() => void addToBenchmarkGroundTruth()}
                      >
                        Add to benchmark ground truth
                      </Button>
                    </div>
                  </>
                )}
              </>
            ) : (
              <>
                {detailLoading && (
                  <InlineNotification kind="info" title="Loading details..." subtitle="Fetching full record detail" lowContrast />
                )}
                {detailError && (
                  <InlineNotification kind="error" title="Failed to load details" subtitle={detailError} lowContrast />
                )}
                {detail && (
                  <>
                    <section>
                      <h4 style={{ margin: "0.25rem 0", color: "#0f62fe" }}>Question</h4>
                      <div style={{ whiteSpace: "pre-wrap" }}>{detail.question || "N/A"}</div>
                    </section>
                    <section>
                      <h4 style={{ margin: "0.25rem 0", color: "#0f62fe" }}>Ground truth SQL</h4>
                      {(detail.ground_truth_sql || []).map((sql, idx) => (
                        <div key={`gt-sql-${idx}`} style={{ marginBottom: "0.55rem" }}>
                          <pre
                            style={{
                              margin: "0.3rem 0",
                              padding: "0.6rem",
                              background: "#f4f4f4",
                              borderRadius: "4px",
                              whiteSpace: "pre-wrap",
                              border: "1px solid rgba(15,98,254,0.2)",
                              color: "#161616",
                            }}
                          >
                            <code dangerouslySetInnerHTML={{ __html: highlightSql(sql) }} />
                          </pre>
                          <div style={{ display: "flex", justifyContent: "flex-end" }}>
                            <Button
                              kind="ghost"
                              size="sm"
                              onClick={() =>
                                openModifyQueryView(sql, `Ground truth SQL ${idx + 1}`)
                              }
                            >
                              Modify Query
                            </Button>
                          </div>
                        </div>
                      ))}
                    </section>
                    <section>
                      <h4 style={{ margin: "0.25rem 0", color: "#0f62fe" }}>Predicted SQL</h4>
                      <pre
                        style={{
                          margin: "0.3rem 0",
                          padding: "0.6rem",
                          background: "#f4f4f4",
                          borderRadius: "4px",
                          whiteSpace: "pre-wrap",
                          border: "1px solid rgba(15,98,254,0.2)",
                          color: "#161616",
                        }}
                      >
                        <code dangerouslySetInnerHTML={{ __html: highlightSql(detail.predicted_sql || "N/A") }} />
                      </pre>
                      <div style={{ display: "flex", justifyContent: "flex-end" }}>
                        <Button
                          kind="ghost"
                          size="sm"
                          disabled={!detail.predicted_sql}
                          onClick={() =>
                            openModifyQueryView(
                              detail.predicted_sql || "",
                              "Predicted SQL"
                            )
                          }
                        >
                          Modify Query
                        </Button>
                      </div>
                    </section>
                    <section>
                      <h4 style={{ margin: "0.25rem 0", color: "#0f62fe" }}>Evaluation metrics</h4>
                      <pre style={{ margin: "0.3rem 0", padding: "0.6rem", background: "#f4f4f4", borderRadius: "4px", whiteSpace: "pre-wrap", color: "#161616" }}>
                        {JSON.stringify(detail.evaluation_metrics ?? {}, null, 2)}
                      </pre>
                    </section>
                    {(detail.ground_truth_results || []).map((r, idx) => (
                      <ResultTableView key={`gt-result-table-${idx}`} title={`Ground truth result ${idx + 1}`} rawData={r} />
                    ))}
                    <ResultTableView title="Predicted result" rawData={detail.predicted_result} />
                    <section>
                      <h4 style={{ margin: "0.25rem 0", color: "#0f62fe" }}>Prompt</h4>
                      <pre style={{ margin: "0.3rem 0", padding: "0.6rem", background: "#f4f4f4", borderRadius: "4px", whiteSpace: "pre-wrap", color: "#161616" }}>
                        {detail.prompt || "N/A"}
                      </pre>
                    </section>
                    <section>
                      <h4 style={{ margin: "0.25rem 0", color: "#0f62fe" }}>LLM judge</h4>
                      <div style={{ marginBottom: "0.25rem" }}>Score: {detail.llm_judge_score ?? "N/A"}</div>
                      <pre style={{ margin: "0.3rem 0", padding: "0.6rem", background: "#f4f4f4", borderRadius: "4px", whiteSpace: "pre-wrap", color: "#161616" }}>
                        {detail.llm_judge_explanation || "N/A"}
                      </pre>
                    </section>
                    {(detail.sql_execution_error || detail.inference_error) && (
                      <section>
                        <h4 style={{ margin: "0.25rem 0", color: "#0f62fe" }}>Errors</h4>
                        <pre style={{ margin: "0.3rem 0", padding: "0.6rem", background: "#f4f4f4", borderRadius: "4px", whiteSpace: "pre-wrap", color: "#161616" }}>
                          {JSON.stringify(
                            {
                              sql_execution_error: detail.sql_execution_error,
                              inference_error: detail.inference_error,
                            },
                            null,
                            2
                          )}
                        </pre>
                      </section>
                    )}
                    <div style={{ marginTop: "0.25rem", display: "flex", justifyContent: "flex-end" }}>
                      <Button kind="secondary" size="sm" onClick={() => void openRawJsonView()}>
                        View Raw JSON
                      </Button>
                    </div>
                  </>
                )}
              </>
            )}
          </div>
        </>
      )}
    </div>
  );
};

