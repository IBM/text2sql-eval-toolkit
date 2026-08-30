import React, {
  Suspense,
  lazy,
  useCallback,
  useEffect,
  useMemo,
  useState,
} from "react";
import { useLocation, useNavigate } from "react-router-dom";
import {
  Button,
  Content,
  Header,
  HeaderName,
  InlineNotification,
  Theme,
} from "@carbon/react";
import { DataTableSkeleton } from "@carbon/react";
import { BenchmarkList } from "../views/BenchmarkList";
import { BenchmarkTiles } from "../views/BenchmarkTiles";
import { BenchmarkConfigModal } from "../views/BenchmarkConfigModal";

// Heavy views load on demand. The initial bundle previously carried all eleven
// views whether or not they were opened; error analysis and run-evaluation alone
// are ~2.4k lines. Each is reachable only via its own route, so splitting on the
// route boundary costs nothing in navigation terms.
const BenchmarkDetail = lazy(() =>
  import("../views/BenchmarkDetail").then((m) => ({
    default: m.BenchmarkDetail,
  })),
);
const ErrorAnalysis = lazy(() =>
  import("../views/ErrorAnalysis").then((m) => ({ default: m.ErrorAnalysis })),
);
const PipelineDetailView = lazy(() =>
  import("../views/PipelineDetailView").then((m) => ({
    default: m.PipelineDetailView,
  })),
);
const LLMJudgeConfigView = lazy(() =>
  import("../views/LLMJudgeConfigView").then((m) => ({
    default: m.LLMJudgeConfigView,
  })),
);
const UsersView = lazy(() =>
  import("../views/UsersView").then((m) => ({
    default: m.UsersView,
  })),
);
const MyKeysView = lazy(() =>
  import("../views/MyKeysView").then((m) => ({
    default: m.MyKeysView,
  })),
);
const RunEvaluationView = lazy(() =>
  import("../views/RunEvaluationView").then((m) => ({
    default: m.RunEvaluationView,
  })),
);
const ToolkitInsightsView = lazy(() =>
  import("../views/ToolkitInsightsView").then((m) => ({
    default: m.ToolkitInsightsView,
  })),
);
const PipelineCompareView = lazy(() =>
  import("../views/PipelineCompareView").then((m) => ({
    default: m.PipelineCompareView,
  })),
);
const ProfileCompareView = lazy(() =>
  import("../views/ProfileCompareView").then((m) => ({
    default: m.ProfileCompareView,
  })),
);
import { FetchResultsBanner } from "../views/FetchResultsBanner";
import { CopyShortLinkButton } from "../views/CopyShortLinkButton";
import { DataStampBar, SessionBar } from "../views/SessionBar";
import { AboutPanel } from "../views/AboutPanel";
import { NavLink } from "../views/NavLink";
import { fetchSession } from "../lib/session";
import {
  createBenchmark,
  fetchBenchmarkConfig,
  fetchBenchmarks,
  updateBenchmark,
  uploadBenchmarkLogo,
} from "../services/benchmarks";
import toolkitLogo from "../assets/text2sql-eval-toolkit-logo.png";
import githubLogo from "../assets/github.png";
import type {
  BenchmarkConfigInput,
  BenchmarkSummary,
} from "../types/benchmark";
import {
  FILTER_DEFAULTS,
  parseLocation,
  parseQuery,
  routes,
} from "../lib/routes";
import {
  expandUrl,
  looksLikeAlias,
  usePipelineAliases,
} from "../lib/pipelineAlias";

type BenchmarkModalMode = "create" | "edit";
const DEFAULT_BENCHMARK_ID = "bird_mini_dev_sqlite";
/** Left nav width when open; main content shifts right by this amount (no overlay). */
const NAV_PANEL_WIDTH_PX = 200;

/** IBM Cloud–style quad-line menu icon (four horizontal bars). */
const HamburgerMenuIcon: React.FC = () => (
  <svg
    width={18}
    height={18}
    viewBox="0 0 16 16"
    aria-hidden
    style={{ display: "block", paddingLeft: 0, paddingRight: 0 }}
  >
    {[0, 1, 2, 3].map((i) => (
      <rect
        key={i}
        x="1"
        y={2 + i * 3.25}
        width="14"
        height="1.5"
        fill="currentColor"
        rx="0.5"
      />
    ))}
  </svg>
);

/** Shown when a shared link points at something this server does not have. */
const NotFound: React.FC<{ message: string }> = ({ message }) => (
  <InlineNotification
    kind="info"
    title="Not found"
    subtitle={message}
    lowContrast
  />
);

export const App: React.FC = () => {
  const [benchmarks, setBenchmarks] = useState<BenchmarkSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [feedback, setFeedback] = useState<{
    kind: "success" | "error";
    message: string;
  } | null>(null);
  // The URL is the source of truth for navigation, so every view is linkable and
  // survives a reload. `navigate` replaces what used to be setActiveView.
  const location = useLocation();
  const navigate = useNavigate();
  const match = useMemo(
    () => parseLocation(location.pathname),
    [location.pathname],
  );

  const urlFilters = useMemo(
    () => parseQuery(location.search.replace(/^\?/, "")),
    [location.search],
  );

  // Read straight from the query string rather than through parseQuery: that
  // parser is the error-analysis filter set, and the judge config is not one of
  // its filters.
  const judgeConfigFromUrl = useMemo(
    () => new URLSearchParams(location.search).get("judge"),
    [location.search],
  );

  // A shared link may name a pipeline by its short alias rather than its full
  // id. The readable form stays canonical, so an alias is expanded on arrival
  // and the address rewritten -- which means every view below this point only
  // ever sees full pipeline ids, and nothing else has to know aliases exist.
  const aliasRefs = useMemo(
    () =>
      [match.pipelineId, urlFilters.pipeline, urlFilters.pipeline2].filter(
        (ref): ref is string => !!ref && looksLikeAlias(ref),
      ),
    [match.pipelineId, urlFilters.pipeline, urlFilters.pipeline2],
  );
  const { table: aliasTable, ready: aliasesReady } = usePipelineAliases(
    match.benchmarkId,
    aliasRefs.length > 0,
  );
  const unknownAlias =
    aliasesReady && aliasRefs.some((ref) => !aliasTable.aliases[ref]);
  const pendingAlias = aliasRefs.length > 0 && !unknownAlias;

  useEffect(() => {
    if (!pendingAlias || !aliasesReady) return;
    const current = `${location.pathname}${location.search}`;
    const expanded = expandUrl(current, aliasTable);
    if (expanded !== current) navigate(expanded, { replace: true });
  }, [
    pendingAlias,
    aliasesReady,
    aliasTable,
    location.pathname,
    location.search,
    navigate,
  ]);

  // ErrorAnalysis takes non-null strings; the URL layer uses null for "absent".
  const errorAnalysisFilters = useMemo(() => {
    const out: Record<string, string | boolean> = {};
    for (const [key, value] of Object.entries(urlFilters)) {
      if (value == null || value === "") continue;
      if (key === "page" || key === "pageSize" || key === "record") continue;
      out[key] = typeof value === "boolean" ? value : String(value);
    }
    return out;
  }, [urlFilters]);

  // Every filter, page and record change is written to the URL, so a shared
  // link reproduces the exact view. Whether it also becomes a history entry
  // depends on what changed.
  //
  // Typing in a filter must not: a search term would otherwise leave one entry
  // per keystroke, and the back button becomes a way to delete characters.
  // Turning a page or opening a record must: those are the deliberate steps a
  // reader expects to walk back through, and replacing them meant "back" left
  // the view entirely from page 2.
  const onPlaygroundStateChange = useCallback(
    (state: {
      benchmarkId: string | null;
      recordId: string | null;
      pipeline: string | null;
      judgeConfig: string | null;
    }) => {
      const next = routes.run(
        state.benchmarkId,
        state.recordId,
        state.pipeline,
        state.judgeConfig,
      );
      const current = `${location.pathname}${location.search}`;
      if (next === current) return;
      // Replace while the same record is being re-examined, push when the record
      // changes: the back button should step between records rather than undo
      // every pipeline toggle.
      const stepped = (state.recordId ?? null) !== (match.recordId ?? null);
      navigate(next, { replace: !stepped });
    },
    [location.pathname, location.search, match.recordId, navigate],
  );

  const onErrorAnalysisStateChange = useCallback(
    (state: {
      filters: Record<string, unknown>;
      page: number;
      pageSize: number;
      record: string | null;
    }) => {
      const benchmark = match.benchmarkId;
      if (!benchmark) return;
      const next = routes.errors(benchmark, {
        ...(state.filters as Record<string, string | boolean>),
        page: state.page,
        pageSize: state.pageSize,
        record: state.record,
      });
      const current = `${location.pathname}${location.search}`;
      if (next === current) return;

      const stepped =
        state.page !== (urlFilters.page ?? FILTER_DEFAULTS.page) ||
        (state.record ?? null) !== (urlFilters.record ?? null);
      navigate(next, { replace: !stepped });
    },
    [
      match.benchmarkId,
      location.pathname,
      location.search,
      navigate,
      urlFilters,
    ],
  );

  const selectedBenchmark = match.benchmarkId;
  const selectedPipeline = match.pipelineId;
  const activeView = match.view;
  const [showBenchmarkModal, setShowBenchmarkModal] = useState(false);
  const [benchmarkModalMode, setBenchmarkModalMode] =
    useState<BenchmarkModalMode>("create");
  const [editingBenchmarkId, setEditingBenchmarkId] = useState<string | null>(
    null,
  );
  const [editingBenchmarkConfig, setEditingBenchmarkConfig] =
    useState<BenchmarkConfigInput | null>(null);
  const [savingBenchmark, setSavingBenchmark] = useState(false);
  const [showNavMenu, setShowNavMenu] = useState(false);

  const loadBenchmarks = async () => {
    try {
      setLoading(true);
      setError(null);
      const items = await fetchBenchmarks();
      setBenchmarks(items);
    } catch (e) {
      const message =
        e instanceof Error ? e.message : "Failed to load benchmarks";
      setError(message);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void loadBenchmarks();
  }, []);

  useEffect(() => {
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        setShowNavMenu(false);
      }
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, []);

  const fallbackBenchmarkId =
    benchmarks.find((b) => b.benchmark_id === DEFAULT_BENCHMARK_ID)
      ?.benchmark_id ??
    benchmarks[0]?.benchmark_id ??
    null;

  // A view that needs a benchmark and was opened without one redirects to a
  // default, so the address bar always reflects what is on screen.
  //
  // A benchmark the URL *names* is a different case and is not redirected. This
  // is the situation shared links are most likely to hit -- the recipient's
  // server has a different set of benchmarks -- and silently swapping in
  // another one shows them numbers for something they did not ask about, with
  // nothing to say the link failed.
  useEffect(() => {
    if (!fallbackBenchmarkId || benchmarks.length === 0) return;
    if (selectedBenchmark) return;
    const needsBenchmark =
      activeView === "toolkitInsights" ||
      activeView === "pipelineCompare" ||
      activeView === "profileCompare" ||
      activeView === "errorAnalysis";
    if (needsBenchmark) {
      navigate(routes.benchmark(fallbackBenchmarkId), { replace: true });
    }
  }, [
    activeView,
    benchmarks,
    fallbackBenchmarkId,
    selectedBenchmark,
    navigate,
  ]);

  // Named, but not here.
  const unknownBenchmark =
    !!selectedBenchmark &&
    benchmarks.length > 0 &&
    !benchmarks.some((b) => b.benchmark_id === selectedBenchmark);

  const resetBenchmarkModal = () => {
    setShowBenchmarkModal(false);
    setEditingBenchmarkId(null);
    setEditingBenchmarkConfig(null);
    setSavingBenchmark(false);
  };

  const openCreateBenchmarkModal = () => {
    setBenchmarkModalMode("create");
    setEditingBenchmarkId(null);
    setEditingBenchmarkConfig(null);
    setShowBenchmarkModal(true);
  };

  const openEditBenchmarkModal = async (benchmarkId: string) => {
    try {
      setBenchmarkModalMode("edit");
      setShowBenchmarkModal(true);
      setSavingBenchmark(true);
      setEditingBenchmarkId(benchmarkId);
      const response = await fetchBenchmarkConfig(benchmarkId);
      setEditingBenchmarkConfig(response.config);
    } catch (e) {
      const message =
        e instanceof Error ? e.message : "Failed to load benchmark config";
      setFeedback({ kind: "error", message });
      resetBenchmarkModal();
    } finally {
      setSavingBenchmark(false);
    }
  };

  const saveBenchmarkConfig = async (payload: {
    benchmark_id?: string;
    config: BenchmarkConfigInput;
  }) => {
    setSavingBenchmark(true);
    try {
      if (benchmarkModalMode === "create") {
        if (!payload.benchmark_id) {
          throw new Error("benchmark_id is required");
        }
        await createBenchmark({
          benchmark_id: payload.benchmark_id,
          ...payload.config,
        });
        setFeedback({
          kind: "success",
          message: `Created benchmark '${payload.benchmark_id}'.`,
        });
      } else {
        if (!editingBenchmarkId) {
          throw new Error("No benchmark selected for edit");
        }
        await updateBenchmark(editingBenchmarkId, payload.config);
        setFeedback({
          kind: "success",
          message: `Updated benchmark '${editingBenchmarkId}'.`,
        });
      }
      await loadBenchmarks();
      resetBenchmarkModal();
    } finally {
      setSavingBenchmark(false);
    }
  };

  const goto = useCallback(
    (path: string) => {
      navigate(path);
    },
    [navigate],
  );

  /** Target benchmark for views that require one, falling back when none is in the URL. */
  const benchmarkForNav = selectedBenchmark ?? fallbackBenchmarkId;

  // Whether to offer the user console at all. Showing a control that 403s is
  // the failure this whole area is trying to avoid.
  const [canManageUsers, setCanManageUsers] = useState(false);
  const [signedIn, setSignedIn] = useState(false);
  useEffect(() => {
    let cancelled = false;
    void fetchSession()
      .then((info) => {
        if (cancelled) return;
        setCanManageUsers(Boolean(info.can_manage_users));
        setSignedIn(Boolean(info.signed_in));
      })
      .catch(() => {
        /* the nav simply omits the entry */
      });
    return () => {
      cancelled = true;
    };
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

    // An alias in the address is resolved before anything renders: showing a
    // view built from an unresolved alias would fetch under the wrong name.
    if (unknownAlias) {
      return (
        <div style={{ maxWidth: "760px", margin: "0 auto", padding: "1rem" }}>
          <NotFound message="That short link does not name a pipeline this server has. It may be from a different results snapshot." />
          <Button
            kind="tertiary"
            size="sm"
            onClick={() => navigate(routes.home())}
          >
            Go to benchmarks
          </Button>
        </div>
      );
    }
    if (pendingAlias) {
      return <DataTableSkeleton role="progressbar" />;
    }

    if (unknownBenchmark) {
      return (
        <div style={{ maxWidth: "760px", margin: "0 auto", padding: "1rem" }}>
          <NotFound
            message={`This server has no benchmark called "${selectedBenchmark}". It may be from a deployment with a different results snapshot.`}
          />
          <Button
            kind="tertiary"
            size="sm"
            onClick={() => navigate(routes.home())}
          >
            Go to benchmarks
          </Button>
        </div>
      );
    }

    if (match.notFound) {
      return (
        <div style={{ maxWidth: "760px", margin: "0 auto", padding: "1rem" }}>
          <NotFound message={`No such page: ${location.pathname}`} />
          <Button
            kind="tertiary"
            size="sm"
            onClick={() => navigate(routes.home())}
          >
            Go to benchmarks
          </Button>
        </div>
      );
    }

    if (activeView === "home") {
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
              <div
                style={{
                  display: "flex",
                  justifyContent: "center",
                  marginBottom: "0.85rem",
                }}
              >
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
                Explore benchmark-level performance, compare pipelines, and
                drill down into failed examples for targeted error analysis.
              </p>
              <p
                style={{
                  margin: 0,
                  opacity: 0.9,
                  textAlign: "center",
                  lineHeight: 1.4,
                }}
              >
                Start by selecting a benchmark tile below, or use the
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
              <BenchmarkTiles
                items={benchmarks}
                onSelect={(benchmarkId) => {
                  navigate(routes.benchmark(benchmarkId));
                }}
                onEdit={(benchmarkId) => {
                  void openEditBenchmarkModal(benchmarkId);
                }}
                onAddNew={openCreateBenchmarkModal}
              />
            </div>

            <AboutPanel />
          </div>
        );
      }
    }

    if (activeView === "benchmark") {
      if (!selectedBenchmark) {
        return <NotFound message="No benchmark in the URL." />;
      }
      return (
        <BenchmarkDetail
          benchmarkId={selectedBenchmark}
          onSelectPipeline={(pipeline) =>
            selectedBenchmark &&
            navigate(routes.pipeline(selectedBenchmark, pipeline))
          }
          onOpenToolkitInsights={() =>
            selectedBenchmark && navigate(routes.insights(selectedBenchmark))
          }
          onOpenPipelineCompare={() =>
            selectedBenchmark && navigate(routes.compare(selectedBenchmark))
          }
          onOpenProfileCompare={() =>
            selectedBenchmark &&
            navigate(routes.profileCompare(selectedBenchmark))
          }
          onOpenErrorAnalysis={() =>
            selectedBenchmark && navigate(routes.errors(selectedBenchmark))
          }
        />
      );
    }

    if (activeView === "pipeline") {
      if (!selectedBenchmark || !selectedPipeline) {
        return (
          <NotFound message="That pipeline link is missing a benchmark or pipeline id." />
        );
      }
      return (
        <PipelineDetailView
          benchmarkId={selectedBenchmark}
          pipelineName={selectedPipeline}
          recordId={match.recordId}
          // Opening or closing a record is a step a reader walks back through,
          // so it pushes a history entry rather than replacing one.
          onSelectRecord={(recordId) =>
            navigate(
              recordId
                ? routes.pipelineRecord(
                    selectedBenchmark,
                    selectedPipeline,
                    recordId,
                  )
                : routes.pipeline(selectedBenchmark, selectedPipeline),
            )
          }
          onBack={() =>
            selectedBenchmark && navigate(routes.benchmark(selectedBenchmark))
          }
          onOpenErrorAnalysis={(filters) =>
            selectedBenchmark &&
            navigate(routes.errors(selectedBenchmark, filters))
          }
        />
      );
    }

    if (activeView === "errorAnalysis") {
      const effectiveBenchmarkId = selectedBenchmark ?? fallbackBenchmarkId;
      if (!effectiveBenchmarkId) {
        return (
          <InlineNotification
            kind="info"
            title="Select a benchmark"
            subtitle="Choose a benchmark before running error analysis."
            lowContrast
          />
        );
      }
      return (
        <ErrorAnalysis
          key={effectiveBenchmarkId}
          benchmarkId={effectiveBenchmarkId}
          onBack={() => navigate(routes.benchmark(effectiveBenchmarkId))}
          initialFilters={errorAnalysisFilters}
          initialPage={urlFilters.page ?? undefined}
          initialPageSize={urlFilters.pageSize ?? undefined}
          initialRecordId={urlFilters.record ?? undefined}
          onStateChange={onErrorAnalysisStateChange}
        />
      );
    }

    if (activeView === "llmJudge") {
      return <LLMJudgeConfigView />;
    }

    if (activeView === "benchmarks") {
      return (
        <div style={{ display: "flex", flexDirection: "column", gap: "1rem" }}>
          <div>
            <h3 style={{ margin: 0 }}>Benchmarks</h3>
            <p
              style={{
                margin: "0.35rem 0 0",
                maxWidth: "52rem",
                lineHeight: 1.45,
                color: "var(--cds-text-secondary)",
              }}
            >
              Every benchmark with results on this deployment. This was a
              slide-out panel, which meant it had no address of its own and
              could not be linked to or opened in a new tab.
            </p>
          </div>
          <BenchmarkList
            items={benchmarks}
            selectedId={selectedBenchmark}
            onSelect={(benchmarkId) => navigate(routes.benchmark(benchmarkId))}
          />
        </div>
      );
    }

    if (activeView === "users") {
      return <UsersView />;
    }

    if (activeView === "myKeys") {
      return <MyKeysView />;
    }

    if (activeView === "runEvaluation") {
      return (
        <RunEvaluationView
          benchmarks={benchmarks}
          initialBenchmarkId={match.benchmarkId}
          initialRecordId={match.recordId}
          initialPipeline={urlFilters.pipeline ?? null}
          initialJudgeConfig={judgeConfigFromUrl}
          onStateChange={onPlaygroundStateChange}
        />
      );
    }

    if (activeView === "toolkitInsights") {
      const effectiveBenchmarkId = selectedBenchmark ?? fallbackBenchmarkId;
      if (!effectiveBenchmarkId) {
        return (
          <InlineNotification
            kind="info"
            title="Loading benchmarks…"
            subtitle="Fetching available evaluation artifacts."
            lowContrast
          />
        );
      }
      return (
        <ToolkitInsightsView
          benchmarks={benchmarks}
          benchmarkId={effectiveBenchmarkId}
          onSelectBenchmark={(id) => navigate(routes.insights(id))}
          onOpenErrorAnalysis={(filters) =>
            navigate(routes.errors(effectiveBenchmarkId, filters))
          }
        />
      );
    }

    if (activeView === "pipelineCompare") {
      const effectiveBenchmarkId = selectedBenchmark ?? fallbackBenchmarkId;
      if (!effectiveBenchmarkId) {
        return (
          <InlineNotification
            kind="info"
            title="Select a benchmark"
            subtitle="Choose a benchmark to compare pipelines."
            lowContrast
          />
        );
      }
      return (
        <PipelineCompareView
          benchmarkId={effectiveBenchmarkId}
          onOpenErrorAnalysis={(filters) =>
            navigate(routes.errors(effectiveBenchmarkId, filters))
          }
        />
      );
    }

    if (activeView === "profileCompare") {
      return (
        <ProfileCompareView
          benchmarks={benchmarks}
          benchmarkId={selectedBenchmark ?? fallbackBenchmarkId}
          onSelectBenchmark={(id) => navigate(routes.profileCompare(id))}
        />
      );
    }

    return null;
  };

  return (
    <Theme theme="g100">
      <Header aria-label="Text2SQL Evaluation Dashboard">
        <Button
          kind="ghost"
          size="sm"
          onClick={() => {
            setShowNavMenu((prev) => !prev);
          }}
          aria-label="Toggle navigation menu"
          aria-expanded={showNavMenu}
          title="Toggle menu"
          style={{
            marginRight: 0,
            minWidth: "2rem",
            /* Align icon with nav ghost buttons: nav pad 0.35rem + ~1rem button inset */
            paddingLeft: "1.35rem",
            paddingRight: "1px",
          }}
        >
          <HamburgerMenuIcon />
        </Button>
        <HeaderName
          prefix="Text2SQL"
          href="#"
          onClick={(e) => {
            e.preventDefault();
            navigate(routes.home());
          }}
          style={{
            cursor: "pointer",
            whiteSpace: "nowrap",
            flexShrink: 0,
            marginLeft: "1px",
            marginRight: "0.75rem",
            paddingLeft: "6px",
            paddingRight: "6px",
          }}
        >
          Evaluation Dashboard
        </HeaderName>
        <div
          style={{ marginLeft: "auto", display: "flex", alignItems: "center" }}
        >
          <SessionBar />
          {/* The playground has its own Export menu, which offers this and
              more. Two controls doing the same thing on one page is worse
              than either. */}
          {activeView !== "runEvaluation" && <CopyShortLinkButton />}
        </div>
      </Header>
      <div style={{ marginTop: "3rem" }}>
        <DataStampBar />
      </div>
      <div
        style={{
          display: "flex",
          flexDirection: "row",
          width: "100%",
          minHeight: "calc(100vh - 3rem)",
          alignItems: "stretch",
        }}
      >
        <aside
          id="app-nav-panel"
          aria-hidden={!showNavMenu}
          style={{
            width: showNavMenu ? NAV_PANEL_WIDTH_PX : 0,
            minWidth: 0,
            flexShrink: 0,
            transition: "width 0.22s cubic-bezier(0.2, 0, 0, 1)",
            overflow: "hidden",
            background: "#161616",
            borderRight: showNavMenu
              ? "1px solid rgba(255,255,255,0.12)"
              : "none",
            display: "flex",
            flexDirection: "column",
          }}
        >
          <div
            style={{
              width: NAV_PANEL_WIDTH_PX,
              minHeight: "calc(100vh - 3rem)",
              display: "flex",
              flexDirection: "column",
              flexShrink: 0,
            }}
          >
            <nav
              aria-label="Main navigation"
              style={{
                padding: "0.5rem 0.35rem",
                overflowY: "auto",
                flex: 1,
                display: "flex",
                flexDirection: "column",
                gap: "0.15rem",
              }}
            >
              <NavLink href={routes.benchmarks()} onNavigate={goto}>
                Benchmarks
              </NavLink>
              <NavLink
                href={benchmarkForNav ? routes.insights(benchmarkForNav) : null}
                onNavigate={goto}
              >
                Metric Insights
              </NavLink>
              <NavLink
                href={benchmarkForNav ? routes.compare(benchmarkForNav) : null}
                onNavigate={goto}
              >
                Pipeline Compare
              </NavLink>
              <NavLink
                href={
                  benchmarkForNav
                    ? routes.profileCompare(benchmarkForNav)
                    : null
                }
                onNavigate={goto}
              >
                Profile Compare
              </NavLink>
              <NavLink
                href={benchmarkForNav ? routes.errors(benchmarkForNav) : null}
                onNavigate={goto}
              >
                Error Analysis
              </NavLink>
              <div
                style={{
                  height: "1px",
                  background: "rgba(255,255,255,0.12)",
                  margin: "0.5rem 0",
                }}
              />
              <NavLink href={routes.llmJudge()} onNavigate={goto}>
                LLM Judge
              </NavLink>
              {signedIn && (
                <NavLink href={routes.myKeys()} onNavigate={goto}>
                  My API keys
                </NavLink>
              )}
              {canManageUsers && (
                <NavLink href={routes.users()} onNavigate={goto}>
                  Users
                </NavLink>
              )}
              <NavLink href={routes.run()} onNavigate={goto}>
                Eval Playground
              </NavLink>
            </nav>
          </div>
        </aside>
        <div
          style={{
            flex: 1,
            minWidth: 0,
            display: "flex",
            flexDirection: "column",
          }}
        >
          <BenchmarkConfigModal
            open={showBenchmarkModal}
            mode={benchmarkModalMode}
            benchmarkId={editingBenchmarkId}
            initialConfig={editingBenchmarkConfig}
            submitting={savingBenchmark}
            onClose={resetBenchmarkModal}
            onSubmit={saveBenchmarkConfig}
            onUploadLogo={uploadBenchmarkLogo}
          />
          <Theme theme="g10">
            <Content
              id="main-content"
              style={{
                padding: "1rem",
                paddingTop: "1rem",
                flex: 1,
                minHeight: 0,
                background: "#ffffff",
                display: "flex",
                flexDirection: "column",
              }}
            >
              <div
                style={{
                  flex: 1,
                  display: "flex",
                  flexDirection: "column",
                  gap: "0.75rem",
                }}
              >
                <FetchResultsBanner
                  onResultsFetched={() => void loadBenchmarks()}
                />
                {feedback ? (
                  <InlineNotification
                    kind={feedback.kind}
                    title={feedback.kind === "success" ? "Success" : "Error"}
                    subtitle={feedback.message}
                    lowContrast
                    onCloseButtonClick={() => setFeedback(null)}
                  />
                ) : null}
                <Suspense fallback={<DataTableSkeleton role="progressbar" />}>
                  {body()}
                </Suspense>
              </div>
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
        </div>
      </div>
    </Theme>
  );
};
