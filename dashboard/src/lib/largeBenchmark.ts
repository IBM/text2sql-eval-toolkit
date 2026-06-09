import type { BenchmarkSummary } from "../types/benchmark";

/** On-disk size of {id}-predictions_eval.json above which the dashboard may OOM on low RAM. */
export const LARGE_EVAL_RESULTS_BYTES = 100 * 1024 * 1024; // 100 MiB

export function isLargeBenchmark(benchmark: BenchmarkSummary): boolean {
  const bytes = benchmark.eval_results_bytes;
  return bytes != null && bytes >= LARGE_EVAL_RESULTS_BYTES;
}

export function formatEvalResultsSize(bytes: number | null | undefined): string | null {
  if (bytes == null || bytes <= 0) return null;
  if (bytes >= 1024 * 1024 * 1024) {
    return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)} GB`;
  }
  if (bytes >= 1024 * 1024) {
    return `${(bytes / (1024 * 1024)).toFixed(0)} MB`;
  }
  if (bytes >= 1024) {
    return `${(bytes / 1024).toFixed(0)} KB`;
  }
  return `${bytes} B`;
}

export const LARGE_BENCHMARK_WARNING =
  "Large evaluation file — loading profile data uses a lot of memory and may crash the server on machines with limited RAM.";
