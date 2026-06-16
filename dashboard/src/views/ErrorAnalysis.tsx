import React, { useEffect, useMemo, useState } from "react";
import {
  Button,
  ComboBox,
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
  DataTableSkeleton,
  InlineLoading,
} from "@carbon/react";
import { apiFetch, apiUrl } from "../lib/api";
import { appendLlmJudgeConfigId, withLlmJudgeConfigId } from "../lib/llmJudgeQuery";
import { RecordDetailDrawer } from "../components/RecordDetailDrawer";
import { LlmJudgeSelector } from "../components/LlmJudgeSelector";
import type { LlmJudgeFilterProps } from "../hooks/useLlmJudgeConfigs";
import {
  type MetricDefinitionsResponse,
  buildMetricInsightsSelectGroups,
  flattenMetricInsightsSelectNames,
} from "../lib/metricInsightsSelect";

interface Props extends LlmJudgeFilterProps {
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
  agree?: boolean;
  category?: string;
  metricCompare?: boolean;
};

interface PaginatedErrorResponse {
  items: ErrorRecordSummary[];
  total: number;
  page: number;
  page_size: number;
}

type LoadOverrides = Partial<
  ErrorAnalysisFilters & { page: number; pageSize: number; search: string; metricCompare: boolean }
>;

function formatMetricValue(value: any): string {
  if (value == null) return "N/A";
  if (typeof value === "number") return Number.isFinite(value) ? String(value) : "N/A";
  if (typeof value === "boolean") return value ? "true" : "false";
  if (typeof value === "string") return value;
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function formatMetricHeader(metricName: string, fallback: string): string {
  const trimmed = metricName.trim();
  if (!trimmed) return fallback;
  return trimmed.replaceAll("_", " ");
}

export const ErrorAnalysis: React.FC<Props> = ({
  benchmarkId,
  onBack,
  initialFilters,
  llmJudgeConfigs,
  llmJudgeConfigId,
  onLlmJudgeConfigIdChange,
}) => {
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
  const [agree, setAgree] = useState(() => initialFilters?.agree ?? false);
  const [category, setCategory] = useState(() => initialFilters?.category ?? "");
  const [metricCompare, setMetricCompare] = useState(
    () => initialFilters?.metricCompare ?? false
  );
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedRecordId, setSelectedRecordId] = useState<string | null>(null);
  const [selectedRecordPipeline, setSelectedRecordPipeline] = useState<string | null>(null);

  const [availablePipelines, setAvailablePipelines] = useState<string[]>([]);

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
        const res = await apiFetch(apiUrl("/api/evaluation-metric-definitions"));
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
          apiUrl(
            withLlmJudgeConfigId(
              `/api/benchmarks/${benchmarkId}/summary/by-category`,
              llmJudgeConfigId
            )
          )
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
        setAvailablePipelines(ranked.map((p) => p.name));
        const bestPipeline = ranked[0]?.name ?? "";
        if (!bestPipeline) return;
        setPipeline((p) => p || bestPipeline);
        setPipeline2((p) => p || bestPipeline);
      } catch {
        // Keep UX resilient; defaults are best-effort.
      }
    };
    void loadDefaultPipeline();
  }, [benchmarkId, llmJudgeConfigId]);

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
    const effectiveAgree = overrides?.agree ?? agree;
    const effectiveCategory = overrides?.category ?? category;
    const effectiveMetricCompare = overrides?.metricCompare ?? metricCompare;

    try {
      setLoading(true);
      setError(null);
      setItems([]);
      const params = new URLSearchParams();
      params.set("page", String(effectivePage));
      params.set("page_size", String(effectivePageSize));
      if (effectiveSearch) params.set("q", effectiveSearch);
      if (effectivePipeline) {
        params.set("pipeline", effectivePipeline);
        if (effectiveMetric) params.set("metric", effectiveMetric);
        const useMetricCompare =
          Boolean(effectiveMetric2) &&
          (effectiveDisagree ||
            effectiveAgree ||
            effectiveMetricCompare ||
            Boolean(effectivePipeline2));
        if (!useMetricCompare && effectiveValue !== "") {
          params.set("value", effectiveValue);
          if (effectiveOp) params.set("op", effectiveOp);
        }
      }
      if (effectiveCategory) params.set("category", effectiveCategory);
      const metricCompareActive =
        Boolean(effectiveMetric2) &&
        (effectiveDisagree ||
          effectiveAgree ||
          effectiveMetricCompare ||
          Boolean(effectivePipeline2));
      if (effectivePipeline && metricCompareActive) {
        params.set("pipeline2", effectivePipeline2 || effectivePipeline);
        params.set("metric2", effectiveMetric2);
        if (effectiveDisagree) params.set("disagree", "true");
        if (effectiveAgree) params.set("agree", "true");
      } else if (effectivePipeline && effectivePipeline2 && effectiveDisagree) {
        params.set("pipeline2", effectivePipeline2);
        if (effectiveMetric2) params.set("metric2", effectiveMetric2);
        params.set("disagree", "true");
      }
      appendLlmJudgeConfigId(params, llmJudgeConfigId);
      const res = await apiFetch(
        apiUrl(`/api/benchmarks/${benchmarkId}/errors?${params.toString()}`)
      );
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
  }, [benchmarkId, page, pageSize, llmJudgeConfigId]);

  const headers: DataTableHeader[] = [
    { key: "record_id", header: "Record ID" },
    { key: "question", header: "Question" },
    { key: "metric1_pipeline", header: "Pipeline 1" },
    { key: "metric1_score", header: formatMetricHeader(metric, "Metric 1") },
    { key: "metric2_pipeline", header: "Pipeline 2" },
    { key: "metric2_score", header: formatMetricHeader(metric2, "Metric 2") },
  ];

  const rows = useMemo(
    () =>
      items.map((item) => {
        const pipeline1Prediction = pipeline ? item.predictions?.[pipeline] : undefined;
        const pipeline2Prediction = pipeline2 ? item.predictions?.[pipeline2] : undefined;
        return {
          id: item.record_id,
          record_id: item.record_id,
          question: item.question,
          metric1_pipeline: pipeline || "N/A",
          metric1_score: formatMetricValue(pipeline1Prediction?.[metric]),
          metric2_pipeline: pipeline2 || "N/A",
          metric2_score: formatMetricValue(pipeline2Prediction?.[metric2]),
        };
      }),
    [items, metric, metric2, pipeline, pipeline2]
  );

  const closeDetail = () => {
    setSelectedRecordId(null);
    setSelectedRecordPipeline(null);
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
          <LlmJudgeSelector
            id="error-analysis-llm-judge-select"
            configs={llmJudgeConfigs}
            selectedId={llmJudgeConfigId}
            onChange={onLlmJudgeConfigIdChange}
          />
          <TextInput
            id="error-search"
            labelText="Search"
            placeholder="Question text or record id"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
          />
          <ComboBox
            id="pipeline-1"
            titleText="Pipeline 1 (optional)"
            placeholder="e.g. wxai:openai/gpt-oss-120b-greedy-zero-shot-chatapi"
            items={availablePipelines}
            itemToString={(item) => item ?? ""}
            selectedItem={pipeline || null}
            onChange={({ selectedItem }) => setPipeline(selectedItem ?? "")}
            onInputChange={(text) => setPipeline(text)}
            allowCustomValue
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
            <ComboBox
              id="pipeline-2"
              titleText="Pipeline 2 (for disagreement)"
              placeholder="Second pipeline id"
              items={availablePipelines}
              itemToString={(item) => item ?? ""}
              selectedItem={pipeline2 || null}
              onChange={({ selectedItem }) => setPipeline2(selectedItem ?? "")}
              onInputChange={(text) => setPipeline2(text)}
              allowCustomValue
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
      {loading ? (
        <div style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }}>
          <InlineLoading
            description={`Loading error records for ${benchmarkId}…`}
            status="active"
          />
          <DataTableSkeleton role="progressbar" columnCount={6} rowCount={10} />
        </div>
      ) : (
        <>
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
        </>
      )}
      {selectedRecordId && (
        <RecordDetailDrawer
          benchmarkId={benchmarkId}
          recordId={selectedRecordId}
          pipeline={selectedRecordPipeline}
          llmJudgeConfigId={llmJudgeConfigId}
          onClose={closeDetail}
        />
      )}
    </div>
  );
};

