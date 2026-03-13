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
} from "@carbon/react";
import { apiUrl } from "../lib/api";

interface Props {
  benchmarkId: string;
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

export const ErrorAnalysis: React.FC<Props> = ({ benchmarkId }) => {
  const [items, setItems] = useState<ErrorRecordSummary[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(25);
  const [search, setSearch] = useState("");
  const [pipeline, setPipeline] = useState("");
  const [metric, setMetric] = useState("execution_accuracy");
  const [value, setValue] = useState("0");
  const [op, setOp] = useState("eq");
  const [pipeline2, setPipeline2] = useState("");
  const [disagree, setDisagree] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const load = async () => {
    try {
      setLoading(true);
      setError(null);
      const params = new URLSearchParams();
      params.set("page", String(page));
      params.set("page_size", String(pageSize));
      if (search) params.set("q", search);
      if (pipeline) {
        params.set("pipeline", pipeline);
        if (metric) params.set("metric", metric);
        if (value) params.set("value", value);
        if (op) params.set("op", op);
      }
      if (pipeline && pipeline2 && disagree) {
        params.set("pipeline2", pipeline2);
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

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}>
      <h3 style={{ margin: 0 }}>Error analysis – {benchmarkId}</h3>
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))",
          gap: "0.5rem",
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
        >
          <SelectItem value="execution_accuracy" text="execution_accuracy" />
          <SelectItem
            value="non_empty_execution_accuracy"
            text="non_empty_execution_accuracy"
          />
          <SelectItem
            value="subset_non_empty_execution_accuracy"
            text="subset_non_empty_execution_accuracy"
          />
          <SelectItem value="llm_score" text="llm_score" />
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
        <div style={{ display: "flex", alignItems: "flex-end" }}>
          <Button
            kind="primary"
            onClick={() => {
              setPage(1);
              void load();
            }}
            disabled={loading}
          >
            Apply filters
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
    </div>
  );
};

