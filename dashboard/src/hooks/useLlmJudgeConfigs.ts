import { useEffect, useState } from "react";
import { apiFetch, apiUrl } from "../lib/api";

export interface BenchmarkLlmJudgeConfig {
  id: number;
  name: string;
  model_id?: string | null;
}

interface LlmJudgeConfigListResponse {
  items: BenchmarkLlmJudgeConfig[];
  default_id?: number | null;
}

export function useLlmJudgeConfigs(benchmarkId: string | null) {
  const [configs, setConfigs] = useState<BenchmarkLlmJudgeConfig[]>([]);
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    setSelectedId(null);
    setConfigs([]);
    if (!benchmarkId) {
      return;
    }

    let cancelled = false;
    const load = async () => {
      setLoading(true);
      try {
        const res = await apiFetch(
          apiUrl(`/api/benchmarks/${benchmarkId}/llm-judge-configs`)
        );
        const json = (await res.json()) as LlmJudgeConfigListResponse;
        if (cancelled) {
          return;
        }
        const items = json.items ?? [];
        setConfigs(items);
        const defaultId =
          json.default_id ??
          (items.length > 0 ? items[items.length - 1].id : null);
        setSelectedId(defaultId);
      } catch {
        if (!cancelled) {
          setConfigs([]);
          setSelectedId(null);
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    };

    void load();
    return () => {
      cancelled = true;
    };
  }, [benchmarkId]);

  return {
    configs,
    selectedId,
    setSelectedId,
    loading,
  };
}

export interface LlmJudgeFilterProps {
  llmJudgeConfigs: BenchmarkLlmJudgeConfig[];
  llmJudgeConfigId: number | null;
  onLlmJudgeConfigIdChange: (id: number | null) => void;
}
