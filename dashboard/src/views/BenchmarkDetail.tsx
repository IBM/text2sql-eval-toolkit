import React, { CSSProperties, useEffect, useState } from "react";
import {
  DataTable,
  DataTableHeader,
  Pagination,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableHeader,
  TableRow,
  InlineNotification,
  Dropdown,
} from "@carbon/react";
import { apiUrl } from "../lib/api";

interface Props {
  benchmarkId: string;
  style?: CSSProperties;
  onSelectPipeline?: (pipelineName: string) => void;
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

export const BenchmarkDetail: React.FC<Props> = ({
  benchmarkId,
  style,
  onSelectPipeline,
}) => {
  const [data, setData] = useState<SummaryResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(10);
  const [sortMetric, setSortMetric] = useState<string | null>(null);
  const [selectedCategory, setSelectedCategory] = useState<string>("overall");

  useEffect(() => {
    const fetchSummary = async () => {
      try {
        setError(null);
        const res = await fetch(
          apiUrl(`/api/benchmarks/${benchmarkId}/summary/by-category`)
        );
        if (!res.ok) {
          throw new Error(`HTTP ${res.status}`);
        }
        const json: SummaryResponse = await res.json();
        setData(json);
        setSortMetric(json.default_sort_metric);
        setSelectedCategory("overall");
        setPage(1);
      } catch (e: any) {
        setError(e.message || "Failed to load benchmark summary");
      }
    };
    fetchSummary();
  }, [benchmarkId]);

  if (error) {
    return (
      <InlineNotification
        kind="error"
        title={`Error loading summary for ${benchmarkId}`}
        subtitle={error}
        lowContrast
      />
    );
  }

  if (!data) {
    return null;
  }

  const activePipelines =
    selectedCategory === "overall"
      ? data.overall
      : data.categories[selectedCategory] || [];

  const metricKeys = new Set<string>();
  activePipelines.forEach((p) => {
    Object.keys(p.metrics).forEach((k) => {
      if (k !== "num_records" && k !== "num_evaluated") {
        metricKeys.add(k);
      }
    });
  });

  const headers: DataTableHeader[] = [
    { key: "name", header: "Pipeline" },
    ...Array.from(metricKeys).map((m) => ({ key: m, header: m })),
  ];

  const sortedPipelines = [...activePipelines].sort((a, b) => {
    if (!sortMetric) return 0;
    const av = a.metrics?.[sortMetric]?.average ?? 0;
    const bv = b.metrics?.[sortMetric]?.average ?? 0;
    return bv - av;
  });

  const metricValue = (pipeline: PipelineMetrics) => {
    if (!sortMetric) return 0;
    const raw = pipeline.metrics?.[sortMetric];
    if (typeof raw === "number") return raw;
    if (raw && typeof raw === "object" && typeof raw.average === "number") {
      return raw.average;
    }
    return 0;
  };

  const metricStddev = (pipeline: PipelineMetrics) => {
    if (!sortMetric) return 0;
    const raw = pipeline.metrics?.[sortMetric];
    if (raw && typeof raw === "object" && typeof raw.stddev === "number") {
      return Math.max(0, raw.stddev);
    }
    return 0;
  };

  const metricCI = (pipeline: PipelineMetrics) => {
    if (!sortMetric) return { low: 0, high: 0, hasCI: false };
    const raw = pipeline.metrics?.[sortMetric];
    if (
      raw &&
      typeof raw === "object" &&
      typeof raw.ci95_low === "number" &&
      typeof raw.ci95_high === "number"
    ) {
      return { low: raw.ci95_low, high: raw.ci95_high, hasCI: true };
    }
    const mean = metricValue(pipeline);
    const sd = metricStddev(pipeline);
    return { low: Math.max(0, mean - sd), high: mean + sd, hasCI: false };
  };

  const chartData = sortedPipelines.map((p) => ({
    name: p.name,
    value: metricValue(p),
    stddev: metricStddev(p),
    ci: metricCI(p),
  }));
  const maxValue =
    chartData.reduce((m, x) => Math.max(m, x.value, x.ci.high), 0) || 1;

  const total = sortedPipelines.length;
  const start = (page - 1) * pageSize;
  const end = start + pageSize;
  const pageItems = sortedPipelines.slice(start, end);

  const metricsDropdownItems = Array.from(metricKeys);
  const categoryItems = ["overall", ...Object.keys(data.categories)];

  return (
    <div style={{ ...style, display: "flex", flexDirection: "column", gap: "0.5rem" }}>
      <div style={{ display: "flex", gap: "1rem", alignItems: "center" }}>
        <h3 style={{ margin: 0 }}>{benchmarkId} – Summary</h3>
        <div style={{ marginLeft: "auto", display: "flex", gap: "0.75rem", alignItems: "center" }}>
          <div style={{ minWidth: "220px" }}>
            <Dropdown
              id="category-dropdown"
              titleText="Query Category"
              label={selectedCategory}
              items={categoryItems}
              selectedItem={selectedCategory}
              onChange={(e) => {
                setSelectedCategory(e.selectedItem as string);
                setPage(1);
              }}
            />
          </div>
        {metricsDropdownItems.length > 0 && (
            <div style={{ minWidth: "260px" }}>
              <Dropdown
                id="sort-metric-dropdown"
                titleText="Sort by metric"
                label={sortMetric || "Select metric"}
                items={metricsDropdownItems}
                selectedItem={sortMetric}
                onChange={(e) => {
                  setSortMetric(e.selectedItem as string);
                  setPage(1);
                }}
              />
            </div>
        )}
        </div>
      </div>
      <div
        style={{
          border: "1px solid rgba(255,255,255,0.12)",
          borderRadius: "6px",
          padding: "0.75rem",
          background: "rgba(255,255,255,0.02)",
        }}
      >
        <div style={{ marginBottom: "0.5rem", fontWeight: 600 }}>
          Pipeline performance by {sortMetric || "selected metric"}
        </div>
        <div style={{ maxHeight: "260px", overflow: "auto", display: "flex", flexDirection: "column", gap: "0.45rem" }}>
          {chartData.map((item) => {
            const barWidthPct = Math.max(2, (item.value / maxValue) * 100);
            const low = Math.max(0, item.ci.low);
            const high = Math.min(maxValue, item.ci.high);
            const lowPct = (low / maxValue) * 100;
            const highPct = (high / maxValue) * 100;
            const whiskerWidthPct = Math.max(0, highPct - lowPct);
            const rightCapAtEdge = highPct >= 99.9;
            return (
            <div
              key={`chart-${item.name}`}
              title={
                item.ci.hasCI
                  ? `${item.name}\nMean: ${item.value.toFixed(3)}\n95% CI: [${item.ci.low.toFixed(3)}, ${item.ci.high.toFixed(3)}]`
                  : `${item.name}\nMean: ${item.value.toFixed(3)}`
              }
              style={{ display: "grid", gridTemplateColumns: "260px 1fr 56px", gap: "0.6rem", alignItems: "center" }}
            >
              <div
                title={item.name}
                style={{
                  whiteSpace: "nowrap",
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                  fontSize: "0.82rem",
                  opacity: 0.9,
                }}
              >
                {item.name}
              </div>
              <div style={{ height: "12px", background: "rgba(255,255,255,0.1)", borderRadius: "999px", overflow: "hidden", position: "relative" }}>
                <div
                  style={{
                    width: `${barWidthPct}%`,
                    height: "100%",
                    background: "linear-gradient(90deg, #0f62fe, #78a9ff)",
                    borderRadius: "999px",
                  }}
                />
                {high > low && (
                  <>
                    <div
                      style={{
                        position: "absolute",
                        left: `${lowPct}%`,
                        width: `${whiskerWidthPct}%`,
                        top: "5.5px",
                        height: "1px",
                        background: "rgba(255,255,255,0.7)",
                      }}
                    />
                    <div
                      style={{
                        position: "absolute",
                        left: `${lowPct}%`,
                        top: "3px",
                        width: "1px",
                        height: "6px",
                        background: "rgba(255,255,255,0.7)",
                      }}
                    />
                    <div
                      style={{
                        position: "absolute",
                        ...(rightCapAtEdge ? { right: 0 } : { left: `${highPct}%` }),
                        top: "3px",
                        width: "1px",
                        height: "6px",
                        background: "rgba(255,255,255,0.7)",
                      }}
                    />
                  </>
                )}
              </div>
              <div style={{ textAlign: "right", fontSize: "0.82rem" }}>
                {item.value.toFixed(3)}
              </div>
            </div>
          );
          })}
        </div>
      </div>
      <div style={{ maxHeight: "360px", overflow: "auto" }}>
        <div
          style={{
            fontSize: "0.82rem",
            opacity: 0.85,
            marginBottom: "0.45rem",
          }}
        >
          Tip: click a pipeline row in the table below for detailed pipeline analysis.
        </div>
        <DataTable
          rows={pageItems.map((p) => ({
            id: p.name,
            name: p.name,
            ...Object.fromEntries(
              Array.from(metricKeys).map((k) => {
                const v = p.metrics[k];
                if (v && typeof v === "object" && "average" in v) {
                  return [k, (v.average as number).toFixed(3)];
                }
                return [k, v ?? ""];
              })
            ),
          }))}
          headers={headers}
          size="sm"
        >
          {({ rows, headers, getHeaderProps }) => (
            <TableContainer>
              <Table aria-label="Pipeline summary">
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
                      style={{ cursor: onSelectPipeline ? "pointer" : "default" }}
                      onClick={() => onSelectPipeline?.(row.id)}
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
        totalItems={total}
        onChange={({ page, pageSize }) => {
          setPage(page);
          setPageSize(pageSize);
        }}
      />
    </div>
  );
};

