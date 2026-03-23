import React, { useEffect, useState } from "react";
import {
  Button,
  Content,
  Header,
  HeaderName,
  InlineNotification,
  Theme,
} from "@carbon/react";
import { DataTableSkeleton } from "@carbon/react";
import { BenchmarkList } from "../views/BenchmarkList";
import { BenchmarkTiles } from "../views/BenchmarkTiles";
import { BenchmarkConfigModal } from "../views/BenchmarkConfigModal";
import { BenchmarkDetail } from "../views/BenchmarkDetail";
import { ErrorAnalysis } from "../views/ErrorAnalysis";
import { PipelineDetailView } from "../views/PipelineDetailView";
import { LLMJudgeConfigView } from "../views/LLMJudgeConfigView";
import { RunEvaluationView } from "../views/RunEvaluationView";
import { ToolkitInsightsView } from "../views/ToolkitInsightsView";
import { PipelineCompareView } from "../views/PipelineCompareView";
import {
  createBenchmark,
  fetchBenchmarkConfig,
  fetchBenchmarks,
  updateBenchmark,
  uploadBenchmarkLogo,
} from "../services/benchmarks";
import toolkitLogo from "../assets/text2sql-eval-toolkit-logo.png";
import githubLogo from "../assets/github.png";
import type { BenchmarkConfigInput, BenchmarkSummary } from "../types/benchmark";

type BenchmarkModalMode = "create" | "edit";
const DEFAULT_BENCHMARK_ID = "bird_mini_dev_sqlite";

export const App: React.FC = () => {
  const [benchmarks, setBenchmarks] = useState<BenchmarkSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [feedback, setFeedback] = useState<{ kind: "success" | "error"; message: string } | null>(null);
  const [selectedBenchmark, setSelectedBenchmark] = useState<string | null>(null);
  const [selectedPipeline, setSelectedPipeline] = useState<string | null>(null);
  const [showBenchmarkPanel, setShowBenchmarkPanel] = useState(false);
  const [showBenchmarkModal, setShowBenchmarkModal] = useState(false);
  const [benchmarkModalMode, setBenchmarkModalMode] = useState<BenchmarkModalMode>("create");
  const [editingBenchmarkId, setEditingBenchmarkId] = useState<string | null>(null);
  const [editingBenchmarkConfig, setEditingBenchmarkConfig] = useState<BenchmarkConfigInput | null>(null);
  const [savingBenchmark, setSavingBenchmark] = useState(false);
  const [activeView, setActiveView] = useState<
    "home" | "benchmark" | "pipeline" | "toolkitInsights" | "pipelineCompare" | "errorAnalysis" | "llmJudge" | "runEvaluation"
  >("home");
  const [errorAnalysisInitialFilters, setErrorAnalysisInitialFilters] = useState<Record<string, any> | null>(null);

  const loadBenchmarks = async () => {
    try {
      setLoading(true);
      setError(null);
      const items = await fetchBenchmarks();
      setBenchmarks(items);
    } catch (e) {
      const message = e instanceof Error ? e.message : "Failed to load benchmarks";
      setError(message);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void loadBenchmarks();
  }, []);

  const fallbackBenchmarkId =
    benchmarks.find((b) => b.benchmark_id === DEFAULT_BENCHMARK_ID)?.benchmark_id ??
    benchmarks[0]?.benchmark_id ??
    null;

  useEffect(() => {
    if (!selectedBenchmark) {
      if (
        (activeView === "toolkitInsights" ||
          activeView === "pipelineCompare" ||
          activeView === "errorAnalysis") &&
        fallbackBenchmarkId
      ) {
        setSelectedBenchmark(fallbackBenchmarkId);
      }
      return;
    }
    const exists = benchmarks.some((b) => b.benchmark_id === selectedBenchmark);
    if (!exists && fallbackBenchmarkId) {
      setSelectedBenchmark(fallbackBenchmarkId);
    }
  }, [activeView, benchmarks, fallbackBenchmarkId, selectedBenchmark]);

  const resetBenchmarkModal = () => {
    setShowBenchmarkModal(false);
    setEditingBenchmarkId(null);
    setEditingBenchmarkConfig(null);
    setSavingBenchmark(false);
  };

  const openCreateBenchmarkModal = () => {
    setBenchmarkModalMode("create");
    setEditingBenchmarkId(null);
    setEditingBenchmarkConfig(null);
    setShowBenchmarkModal(true);
  };

  const openEditBenchmarkModal = async (benchmarkId: string) => {
    try {
      setBenchmarkModalMode("edit");
      setShowBenchmarkModal(true);
      setSavingBenchmark(true);
      setEditingBenchmarkId(benchmarkId);
      const response = await fetchBenchmarkConfig(benchmarkId);
      setEditingBenchmarkConfig(response.config);
    } catch (e) {
      const message = e instanceof Error ? e.message : "Failed to load benchmark config";
      setFeedback({ kind: "error", message });
      resetBenchmarkModal();
    } finally {
      setSavingBenchmark(false);
    }
  };

  const saveBenchmarkConfig = async (payload: {
    benchmark_id?: string;
    config: BenchmarkConfigInput;
  }) => {
    setSavingBenchmark(true);
    try {
      if (benchmarkModalMode === "create") {
        if (!payload.benchmark_id) {
          throw new Error("benchmark_id is required");
        }
        await createBenchmark({
          benchmark_id: payload.benchmark_id,
          ...payload.config,
        });
        setFeedback({ kind: "success", message: `Created benchmark '${payload.benchmark_id}'.` });
      } else {
        if (!editingBenchmarkId) {
          throw new Error("No benchmark selected for edit");
        }
        await updateBenchmark(editingBenchmarkId, payload.config);
        setFeedback({ kind: "success", message: `Updated benchmark '${editingBenchmarkId}'.` });
      }
      await loadBenchmarks();
      resetBenchmarkModal();
    } finally {
      setSavingBenchmark(false);
    }
  };

  const body = () => {
    if (loading) {
      return <DataTableSkeleton role="progressbar" />;
    }
    if (error) {
      return (
        <InlineNotification
          kind="error"
          title="Error loading benchmarks"
          subtitle={error}
          lowContrast
        />
      );
    }

    if (activeView === "home") {
      if (!selectedBenchmark) {
        return (
        <div
          style={{
            maxWidth: "1100px",
            margin: "0 auto",
            padding: "0 1.25rem 1.25rem 1.25rem",
            display: "flex",
            flexDirection: "column",
            gap: "1rem",
          }}
        >
          <div
            style={{
              border: "1px solid rgba(120,169,255,0.22)",
              borderRadius: "10px",
              padding: "1.1rem 1.2rem",
              background:
                "linear-gradient(180deg, rgba(255,255,255,0.04), rgba(255,255,255,0.015))",
              boxShadow: "0 8px 24px rgba(0,0,0,0.22)",
            }}
          >
            <div style={{ display: "flex", justifyContent: "center", marginBottom: "0.85rem" }}>
              <img
                src={toolkitLogo}
                alt="Text2SQL Evaluation Toolkit logo"
                style={{
                  width: "140px",
                  maxWidth: "100%",
                  borderRadius: "6px",
                }}
              />
            </div>
            <h3 style={{ margin: "0 0 0.45rem 0", textAlign: "center" }}>
              Welcome to the Text2SQL Evaluation Dashboard
            </h3>
            <p
              style={{
                margin: "0 0 0.45rem 0",
                opacity: 0.9,
                textAlign: "center",
                lineHeight: 1.45,
              }}
            >
              Explore benchmark-level performance, compare pipelines, and drill down into
              failed examples for targeted error analysis.
            </p>
            <p
              style={{
                margin: 0,
                opacity: 0.9,
                textAlign: "center",
                lineHeight: 1.4,
              }}
            >
              Start by selecting a benchmark tile below, or use the
              <strong> Benchmarks </strong>
              button in the top-right corner at any time.
            </p>
          </div>

          <div
            style={{
              border: "1px solid rgba(255,255,255,0.12)",
              borderRadius: "8px",
              padding: "0.75rem",
              background: "rgba(255,255,255,0.015)",
            }}
          >
            <BenchmarkTiles
              items={benchmarks}
              onSelect={(benchmarkId) => {
                setSelectedBenchmark(benchmarkId);
                setSelectedPipeline(null);
                setActiveView("benchmark");
              }}
              onEdit={(benchmarkId) => {
                void openEditBenchmarkModal(benchmarkId);
              }}
              onAddNew={openCreateBenchmarkModal}
            />
          </div>
        </div>
      );
      }

      // If user came back to "home" but has a selected benchmark, re-enter its detail view.
      return (
        <InlineNotification
          kind="info"
          title="Resuming benchmark view"
          subtitle="Showing the selected benchmark summary."
          lowContrast
        />
      );
    }

    if (activeView === "benchmark") {
      return (
        <BenchmarkDetail
          benchmarkId={selectedBenchmark}
          onSelectPipeline={(pipeline) => {
            setSelectedPipeline(pipeline);
            setActiveView("pipeline");
          }}
          onOpenToolkitInsights={() => {
            setSelectedPipeline(null);
            setActiveView("toolkitInsights");
          }}
          onOpenPipelineCompare={() => {
            setSelectedPipeline(null);
            setActiveView("pipelineCompare");
          }}
          onOpenErrorAnalysis={() => {
            setErrorAnalysisInitialFilters(null);
            setActiveView("errorAnalysis");
          }}
        />
      );
    }

    if (activeView === "pipeline") {
      return (
        <PipelineDetailView
          benchmarkId={selectedBenchmark}
          pipelineName={selectedPipeline}
          onBack={() => {
            setSelectedPipeline(null);
            setActiveView("benchmark");
          }}
          onOpenErrorAnalysis={(filters) => {
            setErrorAnalysisInitialFilters(filters);
            setActiveView("errorAnalysis");
          }}
        />
      );
    }

    if (activeView === "errorAnalysis") {
      const effectiveBenchmarkId = selectedBenchmark ?? fallbackBenchmarkId;
      if (!effectiveBenchmarkId) {
        return (
          <InlineNotification
            kind="info"
            title="Select a benchmark"
            subtitle="Choose a benchmark before running error analysis."
            lowContrast
          />
        );
      }
      return (
        <ErrorAnalysis
          benchmarkId={effectiveBenchmarkId}
          onBack={() => setActiveView(selectedPipeline ? "pipeline" : "benchmark")}
          initialFilters={errorAnalysisInitialFilters ?? undefined}
        />
      );
    }

    if (activeView === "llmJudge") {
      return <LLMJudgeConfigView />;
    }

    if (activeView === "runEvaluation") {
      return <RunEvaluationView benchmarks={benchmarks} />;
    }

    if (activeView === "toolkitInsights") {
      const effectiveBenchmarkId = selectedBenchmark ?? fallbackBenchmarkId;
      if (!effectiveBenchmarkId) {
        return (
          <InlineNotification
            kind="info"
            title="Loading benchmarks…"
            subtitle="Fetching available evaluation artifacts."
            lowContrast
          />
        );
      }
      return (
        <ToolkitInsightsView
          benchmarks={benchmarks}
          benchmarkId={effectiveBenchmarkId}
          onSelectBenchmark={(id) => {
            setSelectedBenchmark(id);
            setSelectedPipeline(null);
          }}
          onOpenErrorAnalysis={(filters) => {
            setErrorAnalysisInitialFilters(filters);
            setActiveView("errorAnalysis");
          }}
        />
      );
    }

    if (activeView === "pipelineCompare") {
      const effectiveBenchmarkId = selectedBenchmark ?? fallbackBenchmarkId;
      if (!effectiveBenchmarkId) {
        return (
          <InlineNotification
            kind="info"
            title="Select a benchmark"
            subtitle="Choose a benchmark to compare pipelines."
            lowContrast
          />
        );
      }
      return (
        <PipelineCompareView
          benchmarkId={effectiveBenchmarkId}
          onOpenErrorAnalysis={(filters) => {
            setErrorAnalysisInitialFilters(filters);
            setActiveView("errorAnalysis");
          }}
        />
      );
    }

    return null;
  };

  return (
    <Theme theme="g100">
      <Header aria-label="Text2SQL Evaluation Dashboard">
        <HeaderName
          prefix="Text2SQL"
          href="#"
          onClick={(e) => {
            e.preventDefault();
            setSelectedBenchmark(null);
            setSelectedPipeline(null);
            setActiveView("home");
            setShowBenchmarkPanel(false);
            setErrorAnalysisInitialFilters(null);
          }}
          style={{ cursor: "pointer" }}
        >
          Evaluation Dashboard
        </HeaderName>
        <Button
          kind="ghost"
          size="sm"
          onClick={() => {
            setShowBenchmarkPanel(false);
            setSelectedPipeline(null);
            setActiveView("toolkitInsights");
          }}
          style={{ marginRight: "0.5rem" }}
        >
          Metric Insights
        </Button>
        <Button
          kind="ghost"
          size="sm"
          onClick={() => {
            setShowBenchmarkPanel(false);
            setSelectedPipeline(null);
            setActiveView("pipelineCompare");
          }}
          style={{ marginRight: "0.5rem" }}
        >
          Pipeline Compare
        </Button>
        <Button
          kind="ghost"
          size="sm"
          onClick={() => {
            setShowBenchmarkPanel(false);
            setErrorAnalysisInitialFilters(null);
            setActiveView("errorAnalysis");
          }}
          style={{ marginRight: "0.5rem" }}
        >
          Error Analysis
        </Button>
        <Button
          kind="ghost"
          size="sm"
          onClick={() => {
            setShowBenchmarkPanel(false);
            setActiveView("llmJudge");
          }}
          style={{ marginRight: "0.5rem" }}
        >
          LLM Judge
        </Button>
        <Button
          kind="ghost"
          size="sm"
          onClick={() => {
            setShowBenchmarkPanel(false);
            setActiveView("runEvaluation");
          }}
          style={{ marginRight: "0.5rem" }}
        >
          Run evaluation
        </Button>
        <Button
          kind="ghost"
          size="sm"
          onClick={() => setShowBenchmarkPanel(true)}
          style={{ marginLeft: "auto", marginRight: "0.5rem" }}
        >
          Benchmarks
        </Button>
      </Header>
      {showBenchmarkPanel && (
        <>
          <div
            onClick={() => setShowBenchmarkPanel(false)}
            style={{
              position: "fixed",
              inset: 0,
              background: "rgba(0, 0, 0, 0.25)",
              zIndex: 7000,
            }}
          />
          <div
            style={{
              position: "fixed",
              top: "3rem",
              right: 0,
              bottom: 0,
              width: "420px",
              zIndex: 7100,
              background: "#161616",
              borderLeft: "1px solid rgba(255,255,255,0.12)",
              padding: "0.75rem",
              overflow: "auto",
            }}
          >
            <div
              style={{
                display: "flex",
                alignItems: "center",
                justifyContent: "space-between",
                marginBottom: "0.5rem",
              }}
            >
              <strong>Benchmarks</strong>
              <Button
                kind="ghost"
                size="sm"
                onClick={() => setShowBenchmarkPanel(false)}
              >
                X
              </Button>
            </div>
            <BenchmarkList
              items={benchmarks}
              selectedId={selectedBenchmark}
              onSelect={(benchmarkId) => {
                setSelectedBenchmark(benchmarkId);
                setSelectedPipeline(null);
                setActiveView("benchmark");
                setShowBenchmarkPanel(false);
              }}
            />
          </div>
        </>
      )}
      <BenchmarkConfigModal
        open={showBenchmarkModal}
        mode={benchmarkModalMode}
        benchmarkId={editingBenchmarkId}
        initialConfig={editingBenchmarkConfig}
        submitting={savingBenchmark}
        onClose={resetBenchmarkModal}
        onSubmit={saveBenchmarkConfig}
        onUploadLogo={uploadBenchmarkLogo}
      />
      <Theme theme="g10">
        <Content
          id="main-content"
          style={{
            padding: "1rem",
            paddingTop: "4rem",
            minHeight: "calc(100vh - 3rem)",
            background: "#ffffff",
            display: "flex",
            flexDirection: "column",
          }}
        >
          <div style={{ flex: 1, display: "flex", flexDirection: "column", gap: "0.75rem" }}>
            {feedback ? (
              <InlineNotification
                kind={feedback.kind}
                title={feedback.kind === "success" ? "Success" : "Error"}
                subtitle={feedback.message}
                lowContrast
                onCloseButtonClick={() => setFeedback(null)}
              />
            ) : null}
            {body()}
          </div>
          <footer
            style={{
              marginTop: "1rem",
              paddingTop: "0.75rem",
              paddingBottom: "0.75rem",
              marginLeft: "-1rem",
              marginRight: "-1rem",
              marginBottom: "-1rem",
              borderTop: "1px solid rgba(255,255,255,0.16)",
              display: "flex",
              justifyContent: "center",
              background: "#161616",
            }}
          >
            <a
              href="https://github.com/IBM/text2sql-eval-toolkit"
              target="_blank"
              rel="noreferrer"
              style={{
                display: "inline-flex",
                alignItems: "center",
                gap: "0.45rem",
                color: "#f4f4f4",
                textDecoration: "none",
                fontSize: "0.9rem",
                fontWeight: 500,
              }}
            >
              <img
                src={githubLogo}
                alt="GitHub"
                style={{ width: "18px", height: "18px" }}
              />
              IBM/text2sql-eval-toolkit
            </a>
          </footer>
        </Content>
      </Theme>
    </Theme>
  );
};

