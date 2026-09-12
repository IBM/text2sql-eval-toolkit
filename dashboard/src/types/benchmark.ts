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
   * Visible only to signed-in users. Only ever true in a signed-in caller's
   * listing: nobody else is sent such a benchmark at all.
   */
  requires_sign_in?: boolean;
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
