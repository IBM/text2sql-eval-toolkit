import React from "react";
import { routes, type ViewName } from "../lib/routes";
import "./BenchmarkViewTabs.css";

/**
 * The five ways to look at one benchmark.
 *
 * Shown on all five, not only the summary, so the strip is a way *between*
 * them rather than a one-way exit. The summary used to offer the other four as
 * ghost buttons in its header, beside the category dropdown -- which meant
 * they existed in one direction only, were not links, and competed with a form
 * control for the same row.
 *
 * Real anchors, so "open in a new tab", middle-click and copy-address work.
 * The plain left click is intercepted for single-page navigation and every
 * other gesture is handed back to the browser -- the bargain `NavLink` strikes
 * in the rail.
 *
 * Profile compare is addressed with the benchmark here even though its
 * canonical address names none: inside a benchmark's own tab strip, the useful
 * thing is that view seeded with this benchmark.
 */
const TABS: ReadonlyArray<{
  view: ViewName;
  label: string;
  href: (benchmarkId: string) => string;
}> = [
  { view: "benchmark", label: "Summary", href: (b) => routes.benchmark(b) },
  { view: "toolkitInsights", label: "Metric Insights", href: (b) => routes.insights(b) },
  { view: "pipelineCompare", label: "Pipeline Compare", href: (b) => routes.compare(b) },
  { view: "profileCompare", label: "Profile Compare", href: (b) => routes.profileCompare(b) },
  { view: "errorAnalysis", label: "Error Analysis", href: (b) => routes.errors(b) },
];

interface Props {
  benchmarkId: string;
  /** Which of the five is on screen. */
  active: ViewName;
  onNavigate: (href: string) => void;
}

export const BenchmarkViewTabs: React.FC<Props> = ({
  benchmarkId,
  active,
  onNavigate,
}) => (
  <nav className="t2s-benchmark-tabs" aria-label={`Views of ${benchmarkId}`}>
    {TABS.map((tab) => {
      const href = tab.href(benchmarkId);
      const current = tab.view === active;
      return (
        <a
          key={tab.view}
          className={
            current
              ? "t2s-benchmark-tabs__tab t2s-benchmark-tabs__tab--current"
              : "t2s-benchmark-tabs__tab"
          }
          href={href}
          aria-current={current ? "page" : undefined}
          onClick={(event) => {
            if (
              event.defaultPrevented ||
              event.button !== 0 ||
              event.metaKey ||
              event.ctrlKey ||
              event.shiftKey ||
              event.altKey
            ) {
              return;
            }
            event.preventDefault();
            // Following the tab you are already on would remount the view and
            // throw away its filters and scroll position for no gain.
            if (!current) onNavigate(href);
          }}
        >
          {tab.label}
        </a>
      );
    })}
  </nav>
);
