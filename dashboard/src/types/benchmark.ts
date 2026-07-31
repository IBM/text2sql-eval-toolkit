export interface BenchmarkSummary {
  benchmark_id: string;
  name: string;
  description: string;
  db_type: string;
  num_records: number;
  num_pipelines: number;
  logo?: string | null;
  /** Estimated evaluation payload size in the database, if available. */
  eval_results_bytes?: number | null;
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
