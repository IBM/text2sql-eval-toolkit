import { render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { BenchmarkDetail } from "./BenchmarkDetail";

/**
 * A benchmark that publishes only its overall scores.
 *
 * The breakdown by category is refused to a caller who is not signed in, and
 * the summary page must show the public overall table instead of an error --
 * otherwise the one page such a benchmark offers anonymously would be broken.
 */

function stubApi(byCategory: { status: number; body: unknown }) {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      const { status, body } = url.endsWith("/summary/by-category")
        ? byCategory
        : {
            status: 200,
            body: {
              benchmark_id: "beaver",
              default_sort_metric: "execution_accuracy",
              pipelines: [
                {
                  name: "pipeline-one",
                  metrics: { execution_accuracy: { average: 0.25 } },
                },
              ],
            },
          };
      return new Response(JSON.stringify(body), {
        status,
        headers: { "Content-Type": "application/json" },
      });
    }),
  );
}

beforeEach(() => {
  vi.unstubAllGlobals();
});

describe("BenchmarkDetail for a benchmark whose details need sign-in", () => {
  it("falls back to the public overall scores", async () => {
    stubApi({
      status: 401,
      body: { detail: "Sign in to view them.", sign_in_required: true },
    });
    render(<BenchmarkDetail benchmarkId="beaver" />);

    expect(await screen.findByText("Overall scores only")).toBeTruthy();
    expect(screen.getAllByText("pipeline-one").length).toBeGreaterThan(0);
    expect(screen.queryByText(/Error loading summary/)).toBeNull();
  });

  it("still reports any other failure as an error", async () => {
    stubApi({ status: 500, body: { detail: "boom" } });
    render(<BenchmarkDetail benchmarkId="beaver" />);

    expect(
      await screen.findByText("Error loading summary for beaver"),
    ).toBeTruthy();
    expect(screen.queryByText("Overall scores only")).toBeNull();
  });
});
