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
// Lazy for the usual reason and one more: it pulls in a Markdown renderer and a
// sanitiser, which do not fit the entry bundle's 460 KB budget. Splitting on the
// route boundary puts both in this view's own chunk.
const DocsView = lazy(() =>
  import("../views/DocsView").then((m) => ({ default: m.DocsView })),
);
import { FetchResultsBanner } from "../views/FetchResultsBanner";
import { CopyShortLinkButton } from "../views/CopyShortLinkButton";
import { DataStampBar, SessionBar } from "../views/SessionBar";
import { AboutPanel } from "../views/AboutPanel";
import { NavLink } from "../views/NavLink";
import { LinkTile, TileGrid } from "../views/LinkTile";
import { BenchmarkViewTabs } from "../views/BenchmarkViewTabs";
import { BenchmarkSelect, NoBenchmarkYet } from "../views/BenchmarkSelect";
import { fetchSession } from "../lib/session";
import { REFERENCE_URL, fetchDocs, type DocInfo } from "../services/docs";
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
  parseBenchmark,
  parseBenchmarkList,
  parseQuery,
  routes,
  type ViewName,
} from "../lib/routes";
import {
  expandUrl,
  looksLikeAlias,
  usePipelineAliases,
} from "../lib/pipelineAlias";

type BenchmarkModalMode = "create" | "edit";

/**
 * One of the five views of a benchmark, under its tab strip.
 *
 * `benchmarkId` may be null, which happens on profile compare -- its canonical
 * address names no benchmark. There is nothing for the tabs to point at then,
 * so they are omitted rather than rendered pointing nowhere.
 */
const BenchmarkView: React.FC<{
  benchmarkId: string | null;
  active: ViewName;
  onNavigate: (href: string) => void;
  children: React.ReactNode;
}> = ({ benchmarkId, active, onNavigate, children }) => (
  <>
    {benchmarkId && (
      <BenchmarkViewTabs
        benchmarkId={benchmarkId}
        active={active}
        onNavigate={onNavigate}
      />
    )}
    {children}
  </>
);

/**
 * An analysis view before a benchmark is chosen.
 *
 * The same dropdown the view itself carries, over an otherwise empty page --
 * so choosing the first benchmark and changing it later are the same gesture,
 * in the same place. It was a grid of benchmark tiles, which looked like a
 * different page rather than like this view waiting for an input.
 */
const ChooseBenchmark: React.FC<{
  title: string;
  what: string;
  benchmarks: BenchmarkSummary[];
  selectId: string;
  onChoose: (benchmarkId: string) => void;
}> = ({ title, what, benchmarks, selectId, onChoose }) => (
  <div style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }}>
    <h3 style={{ margin: 0 }}>{title}</h3>
    <BenchmarkSelect
      id={selectId}
      benchmarks={benchmarks}
      selected={null}
      onSelect={onChoose}
    />
    <NoBenchmarkYet what={what} />
  </div>
);

/**
 * A titled band of tiles on the home page.
 *
 * The home page is the way in to everything the dashboard does, so it is a
 * list of places rather than a single grid; the headings are what stop three
 * rows of tiles reading as one long undifferentiated one.
 */
/**
 * Whether a home section keeps a border around its tiles.
 *
 * Flipped once, compared side by side, and settled -- see the banner comment
 * below. Left as a named constant rather than deleted branches because the
 * question ("does the box still earn its place?") is the kind that gets asked
 * again the next time the page changes.
 */
const HOME_SECTION_BOX = false;

/**
 * The band banners' colour: Carbon Gray 30 with Gray 90 text.
 *
 * Several candidates were rendered against the real page. Blue 60 -- the
 * interactive blue used for links and primary buttons -- read as three calls to
 * action rather than as structure. Blue 80 fixed the brightness and still spent
 * the brand colour on decoration. Gray 100 matches the header, rail and footer
 * exactly, which is the tidiest argument on paper and makes the page top-heavy
 * in practice. Gray 10 was quiet enough to stop separating the bands at all.
 *
 * Gray 30 is the neutral that still reads as a band: darker than the tile
 * borders, so the eye stops at it, without the weight of a filled colour.
 * Carbon's own section headings are neutral rather than filled, which is what
 * makes this the least surprising thing the page can do.
 */
const HOME_BANNER_BG = "#c6c6c6";
const HOME_BANNER_FG = "#262626";

const HomeSection: React.FC<{
  title: string;
  children: React.ReactNode;
}> = ({ title, children }) => (
  <section style={{ display: "flex", flexDirection: "column" }}>
    {/*
      A solid banner, white and centred -- see HOME_BANNER_BG for the colour.

      Carbon's own section heading is typography and whitespace rather than a
      filled bar, so this is a deliberate departure: the home page is a landing
      page carrying three unrelated bands of tiles, and a heading that competes
      with sixteen bordered tiles for attention loses.

      Square, because Carbon does not round anything.
    */}
    <h4
      style={{
        margin: 0,
        padding: "0.55rem 1rem",
        background: HOME_BANNER_BG,
        color: HOME_BANNER_FG,
        fontSize: "0.875rem",
        fontWeight: 600,
        letterSpacing: "0.02em",
        textAlign: "center",
      }}
    >
      {title}
    </h4>
    <div
      style={{
        padding: HOME_SECTION_BOX ? "0.75rem" : "0.75rem 0 0",
        ...(HOME_SECTION_BOX
          ? { border: "1px solid var(--cds-border-subtle)", borderTop: "none" }
          : null),
      }}
    >
      {children}
    </div>
  </section>
);
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

  /** The benchmarks profile compare is pooling, from `?benchmarks=a,b`. */
  const profileBenchmarkIds = useMemo(
    () => parseBenchmarkList(location.search.replace(/^\?/, "")),
    [location.search],
  );

  // Read straight from the query string rather than through parseQuery: that
  // parser is the error-analysis filter set, and the judge config is not one of
  // its filters.
  const judgeConfigFromUrl = useMemo(
    () => new URLSearchParams(location.search).get("judge"),
    [location.search],
  );

  /**
   * The benchmark an analysis view is looking at.
   *
   * `?benchmark=` is where it lives now; a path segment is kept working for
   * the older `/benchmark/{id}/insights` addresses, which is why both are
   * consulted.
   *
   * Computed here rather than beside the other view state because the alias
   * lookup below needs it: fetching the table with the path benchmark alone
   * meant `/errors?benchmark=x&pipeline=<alias>` had nothing to look the alias
   * up in, so it read as unknown and the link died as a not-found.
   */
  const analysisBenchmark = useMemo(
    () => match.benchmarkId ?? parseBenchmark(location.search),
    [match.benchmarkId, location.search],
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
    analysisBenchmark,
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
      // The path form for older links, the query form for current ones. Reading
      // only the path made this a no-op on `/errors?benchmark=…`, so the
      // address stopped following the page number and the open record.
      const benchmark = match.benchmarkId ?? parseBenchmark(location.search);
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

  // A view opened without a benchmark used to redirect to whichever one came
  // first, so `/insights` silently became `/benchmark/bird_mini_dev_sqlite/
  // insights` and the reader was shown numbers for something they had not
  // asked about. It asks now -- see `ChooseBenchmark` below -- which is the
  // same reasoning that already applied to a benchmark the URL *names* and
  // this server does not have: guessing is worse than saying.

  // Named, but not here.
  //
  // `analysisBenchmark`, not the path segment: `/errors?benchmark=missing`
  // named a benchmark this server does not have and rendered the view anyway,
  // which then issued API calls that could only fail. Naming one that is not
  // here reads the same whichever half of the address it came from.
  const unknownBenchmark =
    !!analysisBenchmark &&
    benchmarks.length > 0 &&
    !benchmarks.some((b) => b.benchmark_id === analysisBenchmark);

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

  /**
   * The analysis views, for the home page's second band of tiles.
   *
   * None of them names a benchmark: the view asks when it needs one, rather
   * than the link choosing on the reader's behalf.
   */
  const analysisTiles = useMemo(
    () => [
      {
        title: "Metric Insights",
        summary:
          "Confusion matrices between two binary metrics, per pipeline and across pipelines — where execution match and the judge disagree.",
        href: routes.insights(),
      },
      {
        title: "Pipeline Compare",
        summary:
          "Two pipelines side by side, with the count of records each gets right where the other does not.",
        href: routes.compare(),
      },
      {
        title: "Profile Compare",
        summary:
          "Metrics broken down by SQL feature, pooled across one or more benchmarks and weighted by sample size.",
        href: routes.profileCompare(),
      },
      {
        title: "Error Analysis",
        summary:
          "Search and filter records, then open one to see both queries, both result tables and every metric.",
        href: routes.errors(),
      },
      {
        title: "LLM Judge",
        summary:
          "The judge's model, generation parameters and prompt, as editable YAML.",
        href: routes.llmJudge(),
      },
      {
        title: "Eval Playground",
        summary:
          "Load a record, edit the SQL, run it, and evaluate the result — optionally asking the judge.",
        href: routes.run(),
      },
    ],
    [],
  );

  // The home page lists the documentation alongside everything else, so it
  // needs the same list the docs index uses. Cheap -- filenames and first
  // paragraphs -- and it is the only fetch on this page that is not benchmarks.
  const [docs, setDocs] = useState<DocInfo[]>([]);
  useEffect(() => {
    const controller = new AbortController();
    fetchDocs(controller.signal)
      .then((list) => setDocs(list.items))
      // An empty list is the pip-install case and renders as no note tiles,
      // which is the truth; the reference tile stands on its own.
      .catch(() => setDocs([]));
    return () => controller.abort();
  }, []);

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
            message={`This server has no benchmark called "${analysisBenchmark}". It may be from a deployment with a different results snapshot.`}
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
                Everything below is a starting point: a benchmark to open, a
                view to analyse it with, or a document to read.
              </p>
            </div>

            <HomeSection title="Benchmarks">
              {/* Neither `onAddNew` nor `onEdit`: the home page is for picking
                  a benchmark. Adding and editing are managing them, and that
                  is the Benchmarks page. */}
              <BenchmarkTiles
                items={benchmarks}
                onSelect={(benchmarkId) => {
                  navigate(routes.benchmark(benchmarkId));
                }}
              />
            </HomeSection>

            <HomeSection title="Analysis">
              <TileGrid>
                {analysisTiles.map((tile) => (
                  <LinkTile
                    key={tile.title}
                    eyebrow="View"
                    title={tile.title}
                    summary={tile.summary}
                    href={tile.href}
                    onNavigate={goto}
                    unavailableReason="Needs a benchmark with results on this deployment."
                  />
                ))}
              </TileGrid>
            </HomeSection>

            <HomeSection title="Documentation">
              <TileGrid>
                <LinkTile
                  eyebrow="Reference"
                  title="API reference"
                  summary="Every exported symbol, generated from the docstrings. Opens on Read the Docs."
                  href={REFERENCE_URL}
                  external
                />
                {docs.map((doc) => (
                  <LinkTile
                    key={doc.name}
                    eyebrow="Note"
                    title={doc.title}
                    summary={doc.summary}
                    href={routes.docs(doc.name)}
                    onNavigate={goto}
                  />
                ))}
              </TileGrid>
            </HomeSection>

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
        <BenchmarkView
          benchmarkId={selectedBenchmark}
          active="benchmark"
          onNavigate={goto}
        >
          <BenchmarkDetail
            benchmarkId={selectedBenchmark}
            onSelectPipeline={(pipeline) =>
              navigate(routes.pipeline(selectedBenchmark, pipeline))
            }
          />
        </BenchmarkView>
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
          onNavigate={goto}
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
      const effectiveBenchmarkId = analysisBenchmark;
      if (!effectiveBenchmarkId) {
        return (
          <ChooseBenchmark
            title="Error analysis"
            what="search and filter its records"
            benchmarks={benchmarks}
            selectId="error-analysis-choose-benchmark"
            onChoose={(id) => navigate(routes.errors(id))}
          />
        );
      }
      return (
        <BenchmarkView
          benchmarkId={effectiveBenchmarkId}
          active="errorAnalysis"
          onNavigate={goto}
        >
        <ErrorAnalysis
          key={effectiveBenchmarkId}
          benchmarkId={effectiveBenchmarkId}
          benchmarks={benchmarks}
          onNavigate={goto}
          onSelectBenchmark={(id) => navigate(routes.errors(id))}
          onBack={() => navigate(routes.benchmark(effectiveBenchmarkId))}
          initialFilters={errorAnalysisFilters}
          initialPage={urlFilters.page ?? undefined}
          initialPageSize={urlFilters.pageSize ?? undefined}
          initialRecordId={urlFilters.record ?? undefined}
          onStateChange={onErrorAnalysisStateChange}
        />
        </BenchmarkView>
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
              Every benchmark with results on this deployment.
            </p>
          </div>
          <BenchmarkTiles
            items={benchmarks}
            onSelect={(benchmarkId) => navigate(routes.benchmark(benchmarkId))}
            onEdit={(benchmarkId) => {
              void openEditBenchmarkModal(benchmarkId);
            }}
            onAddNew={openCreateBenchmarkModal}
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

    if (activeView === "docs") {
      // `configName` is the route's "which named thing" slot; here it is the
      // document stem, and null means /docs -- the embedded API reference.
      return <DocsView name={match.configName} onNavigate={goto} />;
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
      const effectiveBenchmarkId = analysisBenchmark;
      if (!effectiveBenchmarkId) {
        return (
          <ChooseBenchmark
            title="Metric insights"
            what="compare two metrics across its pipelines"
            benchmarks={benchmarks}
            selectId="insights-choose-benchmark"
            onChoose={(id) => navigate(routes.insights(id))}
          />
        );
      }
      return (
        <BenchmarkView
          benchmarkId={effectiveBenchmarkId}
          active="toolkitInsights"
          onNavigate={goto}
        >
          <ToolkitInsightsView
            benchmarks={benchmarks}
            benchmarkId={effectiveBenchmarkId}
            onSelectBenchmark={(id) => navigate(routes.insights(id))}
            onOpenErrorAnalysis={(filters) =>
              navigate(routes.errors(effectiveBenchmarkId, filters))
            }
          />
        </BenchmarkView>
      );
    }

    if (activeView === "pipelineCompare") {
      const effectiveBenchmarkId = analysisBenchmark;
      if (!effectiveBenchmarkId) {
        return (
          <ChooseBenchmark
            title="Pipeline compare"
            what="compare two of its pipelines"
            benchmarks={benchmarks}
            selectId="pipeline-compare-choose-benchmark"
            onChoose={(id) => navigate(routes.compare(id))}
          />
        );
      }
      return (
        <BenchmarkView
          benchmarkId={effectiveBenchmarkId}
          active="pipelineCompare"
          onNavigate={goto}
        >
          <PipelineCompareView
            benchmarkId={effectiveBenchmarkId}
            benchmarks={benchmarks}
            onSelectBenchmark={(id) => navigate(routes.compare(id))}
            onOpenErrorAnalysis={(filters) =>
              navigate(routes.errors(effectiveBenchmarkId, filters))
            }
          />
        </BenchmarkView>
      );
    }

    if (activeView === "profileCompare") {
      // No chooser and no fallback: this view selects benchmarks itself, and
      // several at once, so a benchmark in the address only seeds that
      // selection. Its own address names none.
      // The tab strip anchors to the first benchmark in the selection, which is
      // the one you arrived from when you came through a benchmark's tabs.
      // With none selected there is nothing to anchor to and no strip.
      return (
        <BenchmarkView
          benchmarkId={profileBenchmarkIds[0] ?? analysisBenchmark}
          active="profileCompare"
          onNavigate={goto}
        >
          <ProfileCompareView
            benchmarks={benchmarks}
            benchmarkId={analysisBenchmark}
            selectedIds={profileBenchmarkIds}
            // `replace`, not push: adding and removing benchmarks adjusts one
            // view, and a history entry per change would bury whatever the
            // reader was looking at before it.
            onSelectionChange={(ids) =>
              navigate(routes.profileCompare(ids), { replace: true })
            }
          />
        </BenchmarkView>
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
            // Pinned below the fixed 3rem header, so the navigation is still
            // there after scrolling down a long page -- the survey runs to
            // 27,000 pixels, and the rail used to be gone within one screen.
            //
            // Sticky has to be on this element rather than on anything inside
            // it: `overflow: hidden` above (which the width animation needs)
            // makes this a scroll container, and a sticky descendant would
            // position against *it* and never move. `alignSelf` matters for
            // the same reason -- the row is `align-items: stretch`, and a flex
            // item stretched to the full height of the page has no room left
            // to stick within.
            position: "sticky",
            top: "3rem",
            alignSelf: "flex-start",
            height: "calc(100vh - 3rem)",
          }}
        >
          <div
            style={{
              width: NAV_PANEL_WIDTH_PX,
              height: "100%",
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
              {/* Carries the benchmark you are already looking at, and asks
                  when there is not one -- rather than being disabled, or
                  picking whichever benchmark happened to load first.
                  `analysisBenchmark`, so it is carried from the query form of
                  the address as well as the path: reading only the path meant
                  these links dropped it and sent you to an empty picker. */}
              <NavLink
                href={routes.insights(analysisBenchmark)}
                onNavigate={goto}
              >
                Metric Insights
              </NavLink>
              <NavLink
                href={routes.compare(analysisBenchmark)}
                onNavigate={goto}
              >
                Pipeline Compare
              </NavLink>
              <NavLink href={routes.profileCompare()} onNavigate={goto}>
                Profile Compare
              </NavLink>
              <NavLink
                href={routes.errors(analysisBenchmark)}
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
              {/* Its own section, immediately below the rest rather than at the
                  foot of the rail: everything above acts on the results loaded
                  here, and this one is reading material. A divider says that;
                  a gap the height of the viewport just looked like a mistake. */}
              <div
                style={{
                  height: "1px",
                  background: "rgba(255,255,255,0.12)",
                  margin: "0.5rem 0",
                }}
              />
              <NavLink href={routes.docs()} onNavigate={goto}>
                Docs
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
