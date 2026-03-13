import React, { useEffect, useMemo, useState } from "react";
import {
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
  TextInput,
  Button,
} from "@carbon/react";
import { apiUrl } from "../lib/api";

interface Props {
  benchmarkId: string;
}

interface CompareRow {
  pipeline: string;
  metric: string;
  left: number | null;
  right: number | null;
  diff: number | null;
}

interface CompareResponse {
  benchmark_id: string;
  left_id: string;
  right_id: string;
  rows: CompareRow[];
}

export const CompareView: React.FC<Props> = ({ benchmarkId }) => {
  const [leftId, setLeftId] = useState(benchmarkId);
  const [rightId, setRightId] = useState(benchmarkId);
  const [data, setData] = useState<CompareResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const headers: DataTableHeader[] = [
    { key: "pipeline", header: "Pipeline" },
    { key: "metric", header: "Metric" },
    { key: "left", header: "Left" },
    { key: "right", header: "Right" },
    { key: "diff", header: "Δ (right - left)" },
  ];

  const load = async () => {
    try {
      setLoading(true);
      setError(null);
      const params = new URLSearchParams();
      params.set("benchmark_id", benchmarkId);
      params.set("left_id", leftId);
      params.set("right_id", rightId);
      const res = await fetch(apiUrl(`/api/compare?${params.toString()}`));
      if (!res.ok) {
        throw new Error(`HTTP ${res.status}`);
      }
      const json: CompareResponse = await res.json();
      setData(json);
    } catch (e: any) {
      setError(e.message || "Failed to load comparison");
      setData(null);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [benchmarkId]);

  const rows = useMemo(
    () =>
      data?.rows.map((r, idx) => ({
        id: `${r.pipeline}-${r.metric}-${idx}`,
        pipeline: r.pipeline,
        metric: r.metric,
        left: r.left != null ? r.left.toFixed(3) : "",
        right: r.right != null ? r.right.toFixed(3) : "",
        diff: r.diff != null ? r.diff.toFixed(3) : "",
      })) ?? [],
    [data]
  );

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}>
      <h3 style={{ margin: 0 }}>Compare results – {benchmarkId}</h3>
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))",
          gap: "0.5rem",
        }}
      >
        <TextInput
          id="left-id"
          labelText="Left result id"
          value={leftId}
          onChange={(e) => setLeftId(e.target.value)}
        />
        <TextInput
          id="right-id"
          labelText="Right result id"
          value={rightId}
          onChange={(e) => setRightId(e.target.value)}
        />
        <div style={{ display: "flex", alignItems: "flex-end" }}>
          <Button kind="primary" onClick={() => void load()} disabled={loading}>
            Compare
          </Button>
        </div>
      </div>
      {error && (
        <InlineNotification
          kind="error"
          title="Error loading comparison"
          subtitle={error}
          lowContrast
        />
      )}
      <div style={{ maxHeight: "420px", overflow: "auto" }}>
        <DataTable rows={rows} headers={headers} size="sm">
          {({ rows, headers, getHeaderProps }) => (
            <TableContainer>
              <Table aria-label="Comparison table">
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
    </div>
  );
};

