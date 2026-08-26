import type { BenchmarkSummary } from "../types/benchmark";

/**
 * Human-readable size of a benchmark's evaluation artifact.
 *
 * This used to sit alongside an `isLargeBenchmark` guard and a warning that
 * loading profile data "may crash the server on machines with limited RAM".
 * That was true while every request parsed the whole artifact; endpoints now
 * read through the SQLite index, so memory no longer scales with file size and
 * the warning has been removed. The size is still shown as plain context.
 */
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

export type { BenchmarkSummary };
