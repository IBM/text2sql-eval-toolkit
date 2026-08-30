/**
 * URL scheme for the dashboard.
 *
 * Every artifact the UI can show is addressable, so a link can be pasted into
 * an issue or a paper and reopen the same view. Path segments identify *what*
 * is being shown; query parameters carry *how* it is filtered.
 *
 * The subtlety is `pipelineId`: it is derived from the model name and looks
 * like `wxai:openai/gpt-oss-120b-agentic-baseline1-3attempts`, so it contains
 * both `/` and `:` and must be percent-encoded in path position. All encoding
 * lives here rather than being hand-rolled at each link site, because a single
 * missed `encodeURIComponent` produces a route that silently 404s.
 */

export type ViewName =
  | "home"
  | "benchmark"
  | "pipeline"
  | "errorAnalysis"
  | "toolkitInsights"
  | "pipelineCompare"
  | "profileCompare"
  | "llmJudge"
  | "benchmarks"
  | "users"
  | "myKeys"
  | "runEvaluation";

/** Filter state for the error-analysis view, all optional. */
export interface ErrorFilters {
  pipeline?: string | null;
  metric?: string | null;
  value?: string | null;
  op?: string | null;
  pipeline2?: string | null;
  metric2?: string | null;
  disagree?: boolean | null;
  q?: string | null;
  page?: number | null;
  pageSize?: number | null;
  record?: string | null;
}

/**
 * Defaults are omitted from the URL so shared links stay short and a pristine
 * view has a clean address.
 */
export const FILTER_DEFAULTS: Required<
  Pick<ErrorFilters, "metric" | "op" | "disagree" | "page" | "pageSize">
> = {
  metric: "execution_accuracy",
  op: "eq",
  disagree: false,
  page: 1,
  pageSize: 25,
};

const encode = (segment: string): string => encodeURIComponent(segment);

/**
 * The path prefix for a benchmark. Spelled out rather than abbreviated: these
 * addresses get pasted into issues and papers, where `/benchmark/spider_dev`
 * says what it points at and `/b/spider_dev` needs the reader to already know.
 */
const BENCHMARK_SEGMENT = "benchmark";

export const routes = {
  home: (): string => "/",
  benchmark: (benchmarkId: string): string =>
    `/benchmark/${encode(benchmarkId)}`,
  pipeline: (benchmarkId: string, pipelineId: string): string =>
    `/benchmark/${encode(benchmarkId)}/pipeline/${encode(pipelineId)}`,
  /**
   * One record, within one pipeline's detail view.
   *
   * A path segment rather than a query parameter, unlike error analysis: there
   * are no filters here for it to sit beside, and an address that reads as a
   * page is what makes it obviously shareable.
   */
  pipelineRecord: (
    benchmarkId: string,
    pipelineId: string,
    recordId: string,
  ): string =>
    `/benchmark/${encode(benchmarkId)}/pipeline/${encode(
      pipelineId,
    )}/record/${encode(recordId)}`,
  errors: (benchmarkId: string, filters?: ErrorFilters): string =>
    `/benchmark/${encode(benchmarkId)}/errors${buildQuery(filters)}`,
  insights: (benchmarkId: string): string =>
    `/benchmark/${encode(benchmarkId)}/insights`,
  compare: (benchmarkId: string): string =>
    `/benchmark/${encode(benchmarkId)}/compare`,
  profileCompare: (benchmarkId: string): string =>
    `/benchmark/${encode(benchmarkId)}/compare/profile`,
  llmJudge: (configName?: string): string =>
    configName ? `/llm-judge/${encode(configName)}` : "/llm-judge",
  /**
   * The Eval Playground, optionally at one benchmark and record.
   *
   * Benchmark and record are path segments because they say which thing is open,
   * the way `/benchmark/{id}/pipeline/{p}/record/{r}` does. The pipeline is a
   * query parameter: it chooses which prediction to look at within an
   * already-identified record, which is a choice about the view rather than
   * about what the view is showing. `judge` is the same kind of choice -- which
   * judge config's verdict is being shown.
   */
  run: (
    benchmarkId?: string | null,
    recordId?: string | null,
    pipeline?: string | null,
    judgeConfig?: string | null,
  ): string => {
    let path = "/run";
    if (benchmarkId) {
      path += `/${encode(benchmarkId)}`;
      if (recordId) path += `/record/${encode(recordId)}`;
    }
    const params = new URLSearchParams();
    if (pipeline) params.set("pipeline", pipeline);
    // Present only once a verdict is actually on screen. It names the config
    // rather than carrying the verdict itself: the verdict is cached against
    // the record, pipeline and config contents, so the name is enough to bring
    // the same answer back, and a URL cannot be edited into claiming a verdict
    // the judge never gave.
    if (judgeConfig) params.set("judge", judgeConfig);
    const query = params.toString();
    return query ? `${path}?${query}` : path;
  },
  /** Every benchmark, as a page rather than a slide-out panel. */
  benchmarks: (): string => "/benchmarks",
  users: (): string => "/users",
  myKeys: (): string => "/my-keys",
};

/**
 * Serialize filters to a query string, dropping empty values and anything
 * equal to its default.
 */
export function buildQuery(filters?: ErrorFilters): string {
  if (!filters) return "";
  const params = new URLSearchParams();

  const put = (key: string, value: unknown, fallback?: unknown) => {
    if (value == null || value === "") return;
    if (fallback !== undefined && value === fallback) return;
    params.set(key, String(value));
  };

  put("pipeline", filters.pipeline);
  put("metric", filters.metric, FILTER_DEFAULTS.metric);
  put("value", filters.value);
  put("op", filters.op, FILTER_DEFAULTS.op);
  put("pipeline2", filters.pipeline2);
  put("metric2", filters.metric2);
  if (filters.disagree) params.set("disagree", "true");
  put("q", filters.q);
  put("page", filters.page, FILTER_DEFAULTS.page);
  put("pageSize", filters.pageSize, FILTER_DEFAULTS.pageSize);
  put("record", filters.record);

  const query = params.toString();
  return query ? `?${query}` : "";
}

/** Read filters back out of a query string, applying defaults. */
export function parseQuery(search: string | URLSearchParams): ErrorFilters {
  const params =
    typeof search === "string" ? new URLSearchParams(search) : search;

  const num = (key: string, fallback: number): number => {
    const raw = params.get(key);
    if (raw == null || raw === "") return fallback;
    const parsed = Number(raw);
    return Number.isFinite(parsed) && parsed > 0
      ? Math.floor(parsed)
      : fallback;
  };

  return {
    pipeline: params.get("pipeline"),
    metric: params.get("metric") ?? FILTER_DEFAULTS.metric,
    value: params.get("value"),
    op: params.get("op") ?? FILTER_DEFAULTS.op,
    pipeline2: params.get("pipeline2"),
    metric2: params.get("metric2"),
    disagree: params.get("disagree") === "true",
    q: params.get("q"),
    page: num("page", FILTER_DEFAULTS.page ?? 1),
    pageSize: num("pageSize", FILTER_DEFAULTS.pageSize ?? 25),
    record: params.get("record"),
  };
}

/** What a URL path resolves to. */
export interface RouteMatch {
  view: ViewName;
  benchmarkId: string | null;
  pipelineId: string | null;
  configName: string | null;
  /** Record open within a pipeline detail view, if the path names one. */
  recordId: string | null;
  /** True when the path matched no known route. */
  notFound: boolean;
}

const EMPTY: RouteMatch = {
  view: "home",
  benchmarkId: null,
  pipelineId: null,
  configName: null,
  recordId: null,
  notFound: false,
};

/**
 * Resolve a pathname to a view plus its identifiers.
 *
 * Kept as a pure function so the URL scheme is testable without mounting the
 * app, and so every consumer agrees on what a path means.
 */
export function parseLocation(pathname: string): RouteMatch {
  const segments = pathname
    .split("/")
    .filter(Boolean)
    .map((segment) => {
      try {
        return decodeURIComponent(segment);
      } catch {
        // A malformed escape should render "not found", not throw.
        return segment;
      }
    });

  if (segments.length === 0) return { ...EMPTY };

  if (segments[0] === "run") {
    // /run
    if (segments.length === 1) {
      return { ...EMPTY, view: "runEvaluation" };
    }
    // /run/{benchmarkId}
    if (segments.length === 2) {
      return { ...EMPTY, view: "runEvaluation", benchmarkId: segments[1] };
    }
    // /run/{benchmarkId}/record/{recordId}
    if (segments.length === 4 && segments[2] === "record") {
      return {
        ...EMPTY,
        view: "runEvaluation",
        benchmarkId: segments[1],
        recordId: segments[3],
      };
    }
    // Anything else under /run is a link that has been mangled; say so rather
    // than silently opening the playground on a different record.
    return { ...EMPTY, view: "runEvaluation", notFound: true };
  }

  if (segments[0] === "benchmarks" && segments.length === 1) {
    return { ...EMPTY, view: "benchmarks" };
  }

  if (segments[0] === "users" && segments.length === 1) {
    return { ...EMPTY, view: "users" };
  }

  if (segments[0] === "my-keys" && segments.length === 1) {
    return { ...EMPTY, view: "myKeys" };
  }

  if (segments[0] === "llm-judge" && segments.length <= 2) {
    return { ...EMPTY, view: "llmJudge", configName: segments[1] ?? null };
  }

  if (segments[0] === BENCHMARK_SEGMENT && segments.length >= 2) {
    const benchmarkId = segments[1];
    const rest = segments.slice(2);

    if (rest.length === 0) return { ...EMPTY, view: "benchmark", benchmarkId };

    if (rest[0] === "pipeline" && rest.length === 2) {
      return { ...EMPTY, view: "pipeline", benchmarkId, pipelineId: rest[1] };
    }
    if (rest[0] === "pipeline" && rest[2] === "record" && rest.length === 4) {
      return {
        ...EMPTY,
        view: "pipeline",
        benchmarkId,
        pipelineId: rest[1],
        recordId: rest[3],
      };
    }
    if (rest[0] === "errors" && rest.length === 1) {
      return { ...EMPTY, view: "errorAnalysis", benchmarkId };
    }
    if (rest[0] === "insights" && rest.length === 1) {
      return { ...EMPTY, view: "toolkitInsights", benchmarkId };
    }
    if (rest[0] === "compare" && rest.length === 1) {
      return { ...EMPTY, view: "pipelineCompare", benchmarkId };
    }
    if (rest[0] === "compare" && rest[1] === "profile" && rest.length === 2) {
      return { ...EMPTY, view: "profileCompare", benchmarkId };
    }
    return { ...EMPTY, view: "benchmark", benchmarkId, notFound: true };
  }

  return { ...EMPTY, notFound: true };
}
