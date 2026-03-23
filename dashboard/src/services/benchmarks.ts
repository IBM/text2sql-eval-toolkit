import { apiUrl } from "../lib/api";
import type {
  BenchmarkConfigInput,
  BenchmarkConfigResponse,
  BenchmarksResponse,
  BenchmarkSummary,
  CreateBenchmarkRequest,
} from "../types/benchmark";

async function parseJsonResponse<T>(res: Response): Promise<T> {
  if (!res.ok) {
    let detail = `HTTP ${res.status}`;
    try {
      const body = await res.json();
      if (typeof body?.detail === "string") {
        detail = body.detail;
      }
    } catch {
      // No-op: fall back to HTTP status code.
    }
    throw new Error(detail);
  }
  return (await res.json()) as T;
}

export async function fetchBenchmarks(): Promise<BenchmarkSummary[]> {
  const res = await fetch(apiUrl("/api/benchmarks"));
  const data = await parseJsonResponse<BenchmarksResponse>(res);
  return data.items;
}

export async function fetchBenchmarkConfig(
  benchmarkId: string
): Promise<BenchmarkConfigResponse> {
  const res = await fetch(
    apiUrl(`/api/benchmarks/${encodeURIComponent(benchmarkId)}/config`)
  );
  return parseJsonResponse<BenchmarkConfigResponse>(res);
}

export async function createBenchmark(
  payload: CreateBenchmarkRequest
): Promise<BenchmarkConfigResponse> {
  const res = await fetch(apiUrl("/api/benchmarks"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  return parseJsonResponse<BenchmarkConfigResponse>(res);
}

export async function updateBenchmark(
  benchmarkId: string,
  payload: BenchmarkConfigInput
): Promise<BenchmarkConfigResponse> {
  const res = await fetch(
    apiUrl(`/api/benchmarks/${encodeURIComponent(benchmarkId)}`),
    {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    }
  );
  return parseJsonResponse<BenchmarkConfigResponse>(res);
}

export async function uploadBenchmarkLogo(
  file: File,
  benchmarkId: string
): Promise<string> {
  const base64 = await fileToBase64(file);
  const res = await fetch(apiUrl("/api/benchmarks/logo-upload"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      benchmark_id: benchmarkId,
      filename: file.name,
      mime_type: file.type,
      content_base64: base64,
    }),
  });
  const data = await parseJsonResponse<{ logo: string }>(res);
  return data.logo;
}

function fileToBase64(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => {
      const result = String(reader.result || "");
      const base64 = result.includes(",") ? result.split(",", 2)[1] : result;
      resolve(base64);
    };
    reader.onerror = () => {
      reject(new Error("Failed to read image file"));
    };
    reader.readAsDataURL(file);
  });
}
