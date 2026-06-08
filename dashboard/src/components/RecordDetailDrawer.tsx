import React, { useEffect, useMemo, useState } from "react";
import {
  Button,
  DataTable,
  DataTableHeader,
  InlineNotification,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableHeader,
  TableRow,
  TextArea,
} from "@carbon/react";
import { apiFetch, apiUrl } from "../lib/api";

export interface ErrorRecordDetail {
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

interface Props {
  benchmarkId: string;
  recordId: string | null;
  pipeline: string | null;
  onClose: () => void;
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

export const RecordDetailDrawer: React.FC<Props> = ({
  benchmarkId,
  recordId,
  pipeline,
  onClose,
}) => {
  const [detail, setDetail] = useState<ErrorRecordDetail | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState<string | null>(null);
  const [detailViewMode, setDetailViewMode] = useState<"detail" | "raw" | "modify">("detail");
  const [rawJsonRecord, setRawJsonRecord] = useState<Record<string, any> | null>(null);
  const [rawJsonLoading, setRawJsonLoading] = useState(false);
  const [rawJsonError, setRawJsonError] = useState<string | null>(null);
  const [modifySourceLabel, setModifySourceLabel] = useState<string>("");
  const [modifySql, setModifySql] = useState("");
  const [modifyLoading, setModifyLoading] = useState(false);
  const [modifyError, setModifyError] = useState<string | null>(null);
  const [modifyResponse, setModifyResponse] = useState<ExecuteSqlResponse | null>(null);
  const [addGroundTruthLoading, setAddGroundTruthLoading] = useState(false);
  const [addGroundTruthError, setAddGroundTruthError] = useState<string | null>(null);
  const [addGroundTruthSuccess, setAddGroundTruthSuccess] = useState<string | null>(null);

  useEffect(() => {
    if (!recordId || !pipeline) return;
    const loadDetail = async () => {
      try {
        setDetailLoading(true);
        setDetailError(null);
        const params = new URLSearchParams();
        params.set("pipeline", pipeline);
        const res = await apiFetch(
          apiUrl(`/api/benchmarks/${benchmarkId}/errors/${recordId}/detail?${params.toString()}`)
        );
        setDetail((await res.json()) as ErrorRecordDetail);
      } catch (e: any) {
        setDetailError(e.message || "Failed to load record details");
      } finally {
        setDetailLoading(false);
      }
    };
    void loadDetail();
  }, [benchmarkId, recordId, pipeline]);

  useEffect(() => {
    setDetailViewMode("detail");
    setDetail(null);
    setDetailError(null);
    setDetailLoading(false);
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
  }, [recordId]);

  const openRawJsonView = async () => {
    if (!recordId) return;
    setDetailViewMode("raw");
    if (rawJsonRecord) return;
    try {
      setRawJsonLoading(true);
      setRawJsonError(null);
      const res = await apiFetch(
        apiUrl(`/api/benchmarks/${benchmarkId}/errors/${recordId}`)
      );
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
    if (!recordId || !modifySql.trim()) return;
    try {
      setModifyLoading(true);
      setModifyError(null);
      const res = await fetch(apiUrl(`/api/benchmarks/${benchmarkId}/execute`), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          sql: modifySql,
          record_id: recordId,
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
    if (!recordId || !modifySql.trim()) return;
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
          record_id: recordId,
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

  if (!recordId) return null;

  return (
    <>
      <div
        onClick={onClose}
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
              ? `Raw JSON – ${recordId}`
              : detailViewMode === "modify"
                ? `Modify Query – ${recordId}`
                : `Record detail – ${recordId}${pipeline ? ` (${pipeline})` : ""}`}
          </h3>
          <div style={{ display: "flex", gap: "0.35rem" }}>
            {detailViewMode !== "detail" && (
              <Button kind="ghost" size="sm" onClick={() => setDetailViewMode("detail")}>
                Back to detail
              </Button>
            )}
            <Button kind="ghost" size="sm" onClick={onClose}>
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
              id="record-detail-modify-query-sql"
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
                  <h4 style={{ margin: "0.25rem 0", color: "#0f62fe" }}>Execution summary</h4>
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
              <InlineNotification
                kind="info"
                title="Loading details..."
                subtitle="Fetching full record detail"
                lowContrast
              />
            )}
            {detailError && (
              <InlineNotification
                kind="error"
                title="Failed to load details"
                subtitle={detailError}
                lowContrast
              />
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
                    <code
                      dangerouslySetInnerHTML={{
                        __html: highlightSql(detail.predicted_sql || "N/A"),
                      }}
                    />
                  </pre>
                  <div style={{ display: "flex", justifyContent: "flex-end" }}>
                    <Button
                      kind="ghost"
                      size="sm"
                      disabled={!detail.predicted_sql}
                      onClick={() =>
                        openModifyQueryView(detail.predicted_sql || "", "Predicted SQL")
                      }
                    >
                      Modify Query
                    </Button>
                  </div>
                </section>
                <section>
                  <h4 style={{ margin: "0.25rem 0", color: "#0f62fe" }}>Evaluation metrics</h4>
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
                    {JSON.stringify(detail.evaluation_metrics ?? {}, null, 2)}
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
                    {detail.prompt || "N/A"}
                  </pre>
                </section>
                <section>
                  <h4 style={{ margin: "0.25rem 0", color: "#0f62fe" }}>LLM judge</h4>
                  <div style={{ marginBottom: "0.25rem" }}>
                    Score: {detail.llm_judge_score ?? "N/A"}
                  </div>
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
                    {detail.llm_judge_explanation || "N/A"}
                  </pre>
                </section>
                {(detail.sql_execution_error || detail.inference_error) && (
                  <section>
                    <h4 style={{ margin: "0.25rem 0", color: "#0f62fe" }}>Errors</h4>
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
  );
};
