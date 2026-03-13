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
} from "@carbon/react";
import { apiUrl } from "../lib/api";

interface Props {
  benchmarkId: string;
  pipelineName: string;
  onBack: () => void;
}

interface PipelineMetrics {
  name: string;
  metrics: Record<string, any>;
}

interface SummaryResponse {
  benchmark_id: string;
  default_sort_metric: string;
  overall: PipelineMetrics[];
  categories: Record<string, PipelineMetrics[]>;
}

interface ErrorRecordSummary {
  record_id: string;
  question: string;
  predictions: Record<string, Record<string, any>>;
}

interface PaginatedErrorResponse {
  items: ErrorRecordSummary[];
  total: number;
  page: number;
  page_size: number;
}

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
  token_usage?: Record<string, any>;
  inference_time_ms?: number;
  execution_time_ms?: number;
  llm_judge_score?: number;
  llm_judge_explanation?: string;
  sql_execution_error?: string;
  inference_error?: string;
}

function escapeHtml(text: string): string {
  return text
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function highlightSql(sql: string): string {
  const escaped = escapeHtml(sql);
  const keywords = [
    "SELECT",
    "FROM",
    "WHERE",
    "GROUP BY",
    "ORDER BY",
    "HAVING",
    "LIMIT",
    "JOIN",
    "LEFT JOIN",
    "RIGHT JOIN",
    "INNER JOIN",
    "OUTER JOIN",
    "ON",
    "AS",
    "AND",
    "OR",
    "NOT",
    "IN",
    "EXISTS",
    "COUNT",
    "SUM",
    "AVG",
    "MIN",
    "MAX",
    "DISTINCT",
    "CASE",
    "WHEN",
    "THEN",
    "ELSE",
    "END",
  ];
  const sorted = keywords.sort((a, b) => b.length - a.length);
  let html = escaped;
  sorted.forEach((kw) => {
    const token = kw.replace(/\s+/g, "\\s+");
    const re = new RegExp(`\\b${token}\\b`, "gi");
    html = html.replace(
      re,
      (m) =>
        `<span style="color:#0f62fe;font-weight:600;">${m.toUpperCase()}</span>`
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

  // pandas orient='split'
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
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(10);

  const normalized = useMemo(() => normalizeTableData(rawData), [rawData]);
  const headers: DataTableHeader[] = normalized.columns.map((c) => ({ key: c, header: c }));

  const total = normalized.rows.length;
  const start = (page - 1) * pageSize;
  const end = start + pageSize;
  const pageRows = normalized.rows.slice(start, end);

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
        <>
          <div style={{ maxHeight: "280px", overflow: "auto" }}>
            <DataTable rows={pageRows} headers={headers} size="sm">
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
          <Pagination
            page={page}
            pageSize={pageSize}
            pageSizes={[10, 25, 50]}
            totalItems={total}
            onChange={({ page, pageSize }) => {
              setPage(page);
              setPageSize(pageSize);
            }}
          />
        </>
      )}
    </section>
  );
};

export const PipelineDetailView: React.FC<Props> = ({
  benchmarkId,
  pipelineName,
  onBack,
}) => {
  const [summary, setSummary] = useState<SummaryResponse | null>(null);
  const [errors, setErrors] = useState<ErrorRecordSummary[]>([]);
  const [totalErrors, setTotalErrors] = useState(0);
  const [search, setSearch] = useState("");
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(10);
  const [error, setError] = useState<string | null>(null);
  const [selectedRecordId, setSelectedRecordId] = useState<string | null>(null);
  const [detail, setDetail] = useState<ErrorRecordDetail | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState<string | null>(null);

  useEffect(() => {
    const loadSummary = async () => {
      try {
        setError(null);
        const res = await fetch(
          apiUrl(`/api/benchmarks/${benchmarkId}/summary/by-category`)
        );
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        setSummary(await res.json());
      } catch (e: any) {
        setError(e.message || "Failed to load pipeline summary");
      }
    };
    void loadSummary();
  }, [benchmarkId]);

  useEffect(() => {
    const loadErrors = async () => {
      try {
        setError(null);
        const params = new URLSearchParams();
        params.set("pipeline", pipelineName);
        params.set("failed_only", "true");
        params.set("page", String(page));
        params.set("page_size", String(pageSize));
        if (search) params.set("q", search);

        const res = await fetch(
          apiUrl(`/api/benchmarks/${benchmarkId}/errors?${params.toString()}`)
        );
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const json: PaginatedErrorResponse = await res.json();
        setErrors(json.items);
        setTotalErrors(json.total);
      } catch (e: any) {
        setError(e.message || "Failed to load failed examples");
      }
    };
    void loadErrors();
  }, [benchmarkId, pipelineName, page, pageSize, search]);

  const pipelineSummary = useMemo(() => {
    if (!summary) return null;
    const overall = summary.overall.find((p) => p.name === pipelineName)?.metrics ?? {};
    const perCategory: Record<string, Record<string, any>> = {};
    for (const [cat, rows] of Object.entries(summary.categories)) {
      const row = rows.find((p) => p.name === pipelineName);
      if (row) perCategory[cat] = row.metrics;
    }
    return { overall, perCategory };
  }, [summary, pipelineName]);

  const metricHeaders: DataTableHeader[] = [
    { key: "scope", header: "Scope" },
    { key: "execution_accuracy", header: "execution_accuracy" },
    { key: "subset_non_empty_execution_accuracy", header: "subset_non_empty_execution_accuracy" },
    { key: "llm_score", header: "llm_score" },
    { key: "bird_execution_accuracy", header: "bird_execution_accuracy" },
  ];

  const metricRows = useMemo(() => {
    if (!pipelineSummary) return [];
    const formatMetric = (metrics: Record<string, any>, key: string) => {
      const v = metrics?.[key];
      if (v && typeof v === "object" && "average" in v) {
        return Number(v.average).toFixed(3);
      }
      if (typeof v === "number") return v.toFixed(3);
      return "";
    };

    const rows = [
      {
        id: "overall",
        scope: "overall",
        execution_accuracy: formatMetric(pipelineSummary.overall, "execution_accuracy"),
        subset_non_empty_execution_accuracy: formatMetric(
          pipelineSummary.overall,
          "subset_non_empty_execution_accuracy"
        ),
        llm_score: formatMetric(pipelineSummary.overall, "llm_score"),
        bird_execution_accuracy: formatMetric(
          pipelineSummary.overall,
          "bird_execution_accuracy"
        ),
      },
    ];

    Object.entries(pipelineSummary.perCategory).forEach(([cat, metrics]) => {
      rows.push({
        id: cat,
        scope: cat,
        execution_accuracy: formatMetric(metrics, "execution_accuracy"),
        subset_non_empty_execution_accuracy: formatMetric(
          metrics,
          "subset_non_empty_execution_accuracy"
        ),
        llm_score: formatMetric(metrics, "llm_score"),
        bird_execution_accuracy: formatMetric(metrics, "bird_execution_accuracy"),
      });
    });
    return rows;
  }, [pipelineSummary]);

  const errorHeaders: DataTableHeader[] = [
    { key: "record_id", header: "Record ID" },
    { key: "question", header: "Question" },
    { key: "metrics", header: "Pipeline metrics" },
  ];

  const errorRows = useMemo(
    () =>
      errors.map((r) => {
        const m = r.predictions?.[pipelineName] ?? {};
        return {
          id: r.record_id,
          record_id: r.record_id,
          question: r.question,
          metrics: `exec=${m.execution_accuracy ?? "?"}, subset=${m.subset_non_empty_execution_accuracy ?? "?"}, llm=${m.llm_score ?? "?"}`,
        };
      }),
    [errors, pipelineName]
  );

  useEffect(() => {
    if (!selectedRecordId) return;
    const loadDetail = async () => {
      try {
        setDetailLoading(true);
        setDetailError(null);
        const params = new URLSearchParams();
        params.set("pipeline", pipelineName);
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
  }, [benchmarkId, pipelineName, selectedRecordId]);

  const closeDetail = () => {
    setSelectedRecordId(null);
    setDetail(null);
    setDetailError(null);
    setDetailLoading(false);
  };

  const formatRaw = (value: any) => {
    if (value === null || value === undefined) return "N/A";
    if (typeof value === "string") {
      // Try to pretty-print JSON strings if possible
      try {
        const parsed = JSON.parse(value);
        return JSON.stringify(parsed, null, 2);
      } catch {
        return value;
      }
    }
    try {
      return JSON.stringify(value, null, 2);
    } catch {
      return String(value);
    }
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }}>
      <div style={{ display: "flex", alignItems: "center", gap: "0.75rem" }}>
        <Button kind="ghost" size="sm" onClick={onBack}>
          Back to summary
        </Button>
        <h3 style={{ margin: 0 }}>
          {benchmarkId} – {pipelineName}
        </h3>
      </div>

      {error && (
        <InlineNotification
          kind="error"
          title="Error loading pipeline details"
          subtitle={error}
          lowContrast
        />
      )}

      <h4 style={{ margin: "0.25rem 0 0 0" }}>Performance overview</h4>
      <div style={{ maxHeight: "260px", overflow: "auto" }}>
        <DataTable rows={metricRows} headers={metricHeaders} size="sm">
          {({ rows, headers, getHeaderProps }) => (
            <TableContainer>
              <Table aria-label="Pipeline metrics by scope">
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

      <h4 style={{ margin: "0.25rem 0 0 0" }}>Failed examples (execution_accuracy = 0)</h4>
      <TextInput
        id="pipeline-errors-search"
        labelText="Search failed examples"
        placeholder="Question text or record id"
        value={search}
        onChange={(e) => {
          setSearch(e.target.value);
          setPage(1);
        }}
      />
      <div style={{ maxHeight: "360px", overflow: "auto" }}>
        <DataTable rows={errorRows} headers={errorHeaders} size="sm">
          {({ rows, headers, getHeaderProps }) => (
            <TableContainer>
              <Table aria-label="Failed examples">
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
                        const recordCell = row.cells.find(
                          (c) => c.info.header === "record_id"
                        );
                        const recordId =
                          recordCell?.value != null
                            ? String(recordCell.value)
                            : String(row.id);
                        setSelectedRecordId(recordId);
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
        pageSizes={[10, 25, 50]}
        totalItems={totalErrors}
        onChange={({ page, pageSize }) => {
          setPage(page);
          setPageSize(pageSize);
        }}
      />
      <div style={{ fontSize: "0.82rem", opacity: 0.85 }}>
        Click a failed question row for full details (ground truth SQL/results, prediction, metrics, prompt, and LLM-judge explanation).
      </div>

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
                Record detail – {selectedRecordId}
              </h3>
              <Button kind="ghost" size="sm" onClick={closeDetail}>
                X
              </Button>
            </div>

            {detailLoading && <InlineNotification kind="info" title="Loading details..." subtitle="Fetching full record detail" lowContrast />}
            {detailError && <InlineNotification kind="error" title="Failed to load details" subtitle={detailError} lowContrast />}

            {detail && (
              <>
                <section>
                  <h4 style={{ margin: "0.25rem 0", color: "#0f62fe" }}>Question</h4>
                  <div style={{ whiteSpace: "pre-wrap" }}>{detail.question || "N/A"}</div>
                </section>

                <section>
                  <h4 style={{ margin: "0.25rem 0", color: "#0f62fe" }}>Ground truth SQL</h4>
                  {(detail.ground_truth_sql || []).map((sql, idx) => (
                    <pre
                      key={`gt-sql-${idx}`}
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
                </section>

                <section>
                  <h4 style={{ margin: "0.25rem 0", color: "#0f62fe" }}>Evaluation metrics</h4>
                  <pre style={{ margin: "0.3rem 0", padding: "0.6rem", background: "#f4f4f4", borderRadius: "4px", whiteSpace: "pre-wrap", color: "#161616" }}>
{formatRaw(detail.evaluation_metrics)}
                  </pre>
                </section>

                {(detail.ground_truth_results || []).map((r, idx) => (
                  <ResultTableView
                    key={`gt-result-table-${idx}`}
                    title={`Ground truth result ${idx + 1}`}
                    rawData={r}
                  />
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
                  <div style={{ marginBottom: "0.25rem" }}>
                    Score: {detail.llm_judge_score ?? "N/A"}
                  </div>
                  <pre style={{ margin: "0.3rem 0", padding: "0.6rem", background: "#f4f4f4", borderRadius: "4px", whiteSpace: "pre-wrap", color: "#161616" }}>
{detail.llm_judge_explanation || "N/A"}
                  </pre>
                </section>

                {(detail.sql_execution_error || detail.inference_error) && (
                  <section>
                    <h4 style={{ margin: "0.25rem 0", color: "#0f62fe" }}>Errors</h4>
                    <pre style={{ margin: "0.3rem 0", padding: "0.6rem", background: "#f4f4f4", borderRadius: "4px", whiteSpace: "pre-wrap", color: "#161616" }}>
{formatRaw({ sql_execution_error: detail.sql_execution_error, inference_error: detail.inference_error })}
                    </pre>
                  </section>
                )}
              </>
            )}
          </div>
        </>
      )}
    </div>
  );
};

