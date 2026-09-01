import React from "react";
import { ComboBox } from "@carbon/react";
import type { BenchmarkSummary } from "../types/benchmark";

/**
 * The benchmark an analysis view is looking at.
 *
 * The benchmark used to be a path segment, and a view opened without one
 * showed a grid of tiles to pick from. It is a control in the view now, at the
 * top, present whether or not one is chosen -- which means the way to change
 * benchmark is the same as the way to choose the first one, and a view with
 * none selected looks like itself rather than like a different page.
 *
 * Shared so the three views that need it cannot drift apart; metric insights
 * had one already and the other two did not.
 */
interface Props {
  benchmarks: BenchmarkSummary[];
  selected: string | null;
  onSelect: (benchmarkId: string) => void;
  /** Distinct per view, since two may be mounted in a page's lifetime. */
  id: string;
}

export const BenchmarkSelect: React.FC<Props> = ({
  benchmarks,
  selected,
  onSelect,
  id,
}) => (
  <div style={{ minWidth: "260px", maxWidth: "420px" }}>
    <ComboBox
      id={id}
      titleText="Benchmark"
      items={benchmarks}
      itemToString={(item) => (item ? item.benchmark_id : "")}
      selectedItem={
        benchmarks.find((b) => b.benchmark_id === selected) ?? null
      }
      onChange={(e) => {
        const item = e.selectedItem as BenchmarkSummary | null;
        if (item) onSelect(item.benchmark_id);
      }}
      placeholder="Select benchmark"
    />
  </div>
);

/**
 * What an analysis view shows before a benchmark is chosen.
 *
 * A sentence, not an error: nothing has gone wrong, the view is simply waiting
 * for the one input it cannot guess.
 */
export const NoBenchmarkYet: React.FC<{ what: string }> = ({ what }) => (
  <p
    style={{
      margin: "1.5rem 0 0",
      maxWidth: "42rem",
      lineHeight: 1.5,
      color: "var(--cds-text-secondary)",
    }}
  >
    Select a benchmark above to {what}.
  </p>
);
