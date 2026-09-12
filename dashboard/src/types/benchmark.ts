export interface BenchmarkSummary {
  benchmark_id: string;
  name: string;
  description: string;
  db_type: string;
  num_records: number;
  num_pipelines: number;
  logo?: string | null;
  /** Size of data/results/{id}-predictions_eval.json on disk, if present. */
  eval_results_bytes?: number | null;
  /**
   * Its questions, SQL and per-record results require sign-in; its tile and
   * overall scores are public.
   */
  requires_sign_in?: boolean;
  /** Whether this caller is locked out of those details right now. */
  details_locked?: boolean;
}

export interface BenchmarksResponse {
  items: BenchmarkSummary[];
}

export interface BenchmarkConfigInput {
  name: string;
  description: string;
  data: string;
  schema: string;
  predictions: string;
  logo?: string;
  db_engine: Record<string, string>;
}

export interface CreateBenchmarkRequest extends BenchmarkConfigInput {
  benchmark_id: string;
}

export interface BenchmarkConfigResponse {
  benchmark_id: string;
  config: BenchmarkConfigInput;
}
