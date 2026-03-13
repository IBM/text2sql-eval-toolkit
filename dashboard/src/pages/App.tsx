import React, { useEffect, useState } from "react";
import {
  Button,
  Content,
  Header,
  HeaderName,
  Theme,
} from "@carbon/react";
import { DataTableSkeleton, InlineNotification } from "@carbon/react";
import { BenchmarkList } from "../views/BenchmarkList";
import { BenchmarkDetail } from "../views/BenchmarkDetail";
import { PipelineDetailView } from "../views/PipelineDetailView";
import { apiUrl } from "../lib/api";
import toolkitLogo from "../assets/text2sql-eval-toolkit-logo.png";
import githubLogo from "../assets/github.png";

export interface BenchmarkSummary {
  benchmark_id: string;
  description: string;
  db_type: string;
  num_records: number;
  num_pipelines: number;
}

interface BenchmarksResponse {
  items: BenchmarkSummary[];
}

export const App: React.FC = () => {
  const [benchmarks, setBenchmarks] = useState<BenchmarkSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedBenchmark, setSelectedBenchmark] = useState<string | null>(null);
  const [selectedPipeline, setSelectedPipeline] = useState<string | null>(null);
  const [showBenchmarkPanel, setShowBenchmarkPanel] = useState(false);

  useEffect(() => {
    const fetchBenchmarks = async () => {
      try {
        setLoading(true);
        const res = await fetch(apiUrl("/api/benchmarks"));
        if (!res.ok) {
          throw new Error(`HTTP ${res.status}`);
        }
        const data: BenchmarksResponse = await res.json();
        setBenchmarks(data.items);
      } catch (e: any) {
        setError(e.message || "Failed to load benchmarks");
      } finally {
        setLoading(false);
      }
    };
    fetchBenchmarks();
  }, []);

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
              Start by selecting a benchmark from the table below, or use the
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
            <BenchmarkList
              items={benchmarks}
              selectedId={null}
              onSelect={(benchmarkId) => {
                setSelectedBenchmark(benchmarkId);
                setSelectedPipeline(null);
              }}
            />
          </div>
        </div>
      );
    }

    if (selectedBenchmark && !selectedPipeline) {
      return (
        <BenchmarkDetail
          benchmarkId={selectedBenchmark}
          onSelectPipeline={(pipeline) => setSelectedPipeline(pipeline)}
        />
      );
    }

    if (selectedBenchmark && selectedPipeline) {
      return (
        <PipelineDetailView
          benchmarkId={selectedBenchmark}
          pipelineName={selectedPipeline}
          onBack={() => setSelectedPipeline(null)}
        />
      );
    }

    return (
      <InlineNotification
        kind="info"
        title="Select a benchmark"
        subtitle="Choose a benchmark from the Benchmarks view first."
        lowContrast
      />
    );
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
            setShowBenchmarkPanel(false);
          }}
          style={{ cursor: "pointer" }}
        >
          Evaluation Dashboard
        </HeaderName>
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
                setShowBenchmarkPanel(false);
              }}
            />
          </div>
        </>
      )}
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
          <div style={{ flex: 1 }}>{body()}</div>
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

