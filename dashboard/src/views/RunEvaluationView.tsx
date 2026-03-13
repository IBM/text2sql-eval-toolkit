import React, { useState } from "react";
import {
  Button,
  Checkbox,
  ComboBox,
  InlineNotification,
  TextInput,
} from "@carbon/react";
import type { BenchmarkSummary } from "../pages/App";
import { apiUrl } from "../lib/api";

interface Props {
  benchmarks: BenchmarkSummary[];
}

interface EvaluateRequest {
  use_llm: boolean;
  llm_judge_config_path?: string | null;
  force_rerun_llm_judge: boolean;
  force_rerun: boolean;
}

interface JobStatus {
  job_id: string;
  benchmark_id: string;
  status: string;
  error?: string | null;
}

export const RunEvaluationView: React.FC<Props> = ({ benchmarks }) => {
  const [selectedBenchmark, setSelectedBenchmark] = useState<BenchmarkSummary | null>(
    benchmarks[0] ?? null
  );
  const [useLLM, setUseLLM] = useState(false);
  const [llmConfigPath, setLlmConfigPath] = useState("");
  const [forceRerunLLMJudge, setForceRerunLLMJudge] = useState(false);
  const [forceRerun, setForceRerun] = useState(false);
  const [job, setJob] = useState<JobStatus | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const submit = async () => {
    if (!selectedBenchmark) return;
    try {
      setLoading(true);
      setError(null);
      setJob(null);
      const body: EvaluateRequest = {
        use_llm: useLLM,
        llm_judge_config_path: llmConfigPath || null,
        force_rerun_llm_judge: forceRerunLLMJudge,
        force_rerun: forceRerun,
      };
      const res = await fetch(
        apiUrl(`/api/benchmarks/${selectedBenchmark.benchmark_id}/evaluate`),
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
      const json: JobStatus = await res.json();
      setJob(json);
    } catch (e: any) {
      setError(e.message || "Failed to start evaluation");
    } finally {
      setLoading(false);
    }
  };

  const refreshStatus = async () => {
    if (!job) return;
    try {
      setLoading(true);
      setError(null);
      const res = await fetch(apiUrl(`/api/jobs/${job.job_id}`));
      if (!res.ok) {
        const txt = await res.text();
        throw new Error(txt || `HTTP ${res.status}`);
      }
      const json: JobStatus = await res.json();
      setJob(json);
    } catch (e: any) {
      setError(e.message || "Failed to refresh job status");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}>
      <h3 style={{ margin: 0 }}>Run evaluation</h3>
      {error && (
        <InlineNotification
          kind="error"
          title="Error"
          subtitle={error}
          lowContrast
        />
      )}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))",
          gap: "0.5rem",
        }}
      >
        <ComboBox
          id="benchmark-select"
          titleText="Benchmark"
          items={benchmarks}
          itemToString={(item) => (item ? item.benchmark_id : "")}
          selectedItem={selectedBenchmark}
          onChange={(e) => setSelectedBenchmark(e.selectedItem as BenchmarkSummary)}
          placeholder="Select benchmark"
        />
        <Checkbox
          id="use-llm"
          labelText="Use LLM-as-judge"
          checked={useLLM}
          onChange={(_, { checked }) => setUseLLM(checked)}
        />
        <TextInput
          id="llm-config-path"
          labelText="LLM judge config path (optional)"
          placeholder="Path to YAML config or leave empty for default"
          value={llmConfigPath}
          onChange={(e) => setLlmConfigPath(e.target.value)}
        />
        <Checkbox
          id="force-rerun-llm"
          labelText="Force rerun LLM judge"
          checked={forceRerunLLMJudge}
          onChange={(_, { checked }) => setForceRerunLLMJudge(checked)}
        />
        <Checkbox
          id="force-rerun-all"
          labelText="Force rerun full evaluation"
          checked={forceRerun}
          onChange={(_, { checked }) => setForceRerun(checked)}
        />
      </div>
      <div style={{ display: "flex", gap: "0.5rem" }}>
        <Button kind="primary" onClick={() => void submit()} disabled={loading || !selectedBenchmark}>
          Start evaluation
        </Button>
        {job && (
          <Button kind="secondary" onClick={() => void refreshStatus()} disabled={loading}>
            Refresh status
          </Button>
        )}
      </div>
      {job && (
        <InlineNotification
          kind={job.status === "failed" ? "error" : job.status === "completed" ? "success" : "info"}
          title={`Job ${job.status}`}
          subtitle={
            job.error
              ? job.error
              : `Job id: ${job.job_id}, benchmark: ${job.benchmark_id}`
          }
          lowContrast
        />
      )}
    </div>
  );
};

