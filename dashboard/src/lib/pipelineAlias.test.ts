import { beforeEach, describe, expect, it, vi } from "vitest";

import {
  expandUrl,
  fetchPipelineAliases,
  looksLikeAlias,
  resetPipelineAliasCache,
  shortenUrl,
  type PipelineAliases,
} from "./pipelineAlias";

/**
 * The alias layer only matters for links that leave the app: it is what makes a
 * pasted URL survive a mail client's line wrapping. So the properties worth
 * pinning are that shortening and expanding are inverses, and that neither one
 * touches anything in the URL that is not a pipeline reference -- rewriting a
 * search term would silently change what the link shows.
 */

const PIPE_A = "wxai:openai/gpt-oss-120b-greedy-zero-shot-chatapi";
const PIPE_B = "wxai:ibm/granite-4-h-small-agentic-baseline1-3attempts";
const ALIAS_A = "aaaaaaaaaa";
const ALIAS_B = "bbbbbbbbbb";

const TABLE: PipelineAliases = {
  aliases: { [ALIAS_A]: PIPE_A, [ALIAS_B]: PIPE_B },
  byPipeline: { [PIPE_A]: ALIAS_A, [PIPE_B]: ALIAS_B },
};

const EMPTY: PipelineAliases = { aliases: {}, byPipeline: {} };

describe("alias shape", () => {
  it("recognises an alias", () => {
    expect(looksLikeAlias(ALIAS_A)).toBe(true);
  });

  it("does not mistake a pipeline id for one", () => {
    // Resolution has no marker in the URL and relies on this being true.
    expect(looksLikeAlias(PIPE_A)).toBe(false);
    expect(looksLikeAlias(PIPE_B)).toBe(false);
  });

  it("rejects near-misses", () => {
    for (const ref of ["", "aaaaaaaaa", "aaaaaaaaaaa", "AAAAAAAAAA", "aaaaaaaaa-"]) {
      expect(looksLikeAlias(ref)).toBe(false);
    }
  });
});

describe("shortening a link", () => {
  it("replaces the pipeline in the path", () => {
    const url = `/b/demo/pipeline/${encodeURIComponent(PIPE_A)}`;
    expect(shortenUrl(url, TABLE)).toBe(`/b/demo/pipeline/${ALIAS_A}`);
  });

  it("replaces both pipelines in a comparison link", () => {
    const url = `/b/demo/errors?pipeline=${encodeURIComponent(
      PIPE_A
    )}&pipeline2=${encodeURIComponent(PIPE_B)}`;
    const short = shortenUrl(url, TABLE);
    expect(short).toContain(`pipeline=${ALIAS_A}`);
    expect(short).toContain(`pipeline2=${ALIAS_B}`);
  });

  it("actually makes the link shorter", () => {
    // The entire justification for the feature.
    const url = `/b/demo/errors?pipeline=${encodeURIComponent(
      PIPE_A
    )}&pipeline2=${encodeURIComponent(PIPE_B)}`;
    expect(shortenUrl(url, TABLE).length).toBeLessThan(url.length / 2);
  });

  it("leaves a search term alone even when it names a model", () => {
    // A blanket string replacement would rewrite this and change what the
    // shared link searches for.
    const url = `/b/demo/errors?q=${encodeURIComponent(PIPE_A)}`;
    expect(expandUrl(shortenUrl(url, TABLE), TABLE)).toBe(url);
    expect(new URLSearchParams(shortenUrl(url, TABLE).split("?")[1]).get("q")).toBe(
      PIPE_A
    );
  });

  it("leaves other filters untouched", () => {
    const url = `/b/demo/errors?pipeline=${encodeURIComponent(
      PIPE_A
    )}&metric=llm_score&page=3`;
    const short = shortenUrl(url, TABLE);
    expect(short).toContain("metric=llm_score");
    expect(short).toContain("page=3");
  });

  it("is a no-op with no alias table", () => {
    const url = `/b/demo/pipeline/${encodeURIComponent(PIPE_A)}`;
    expect(shortenUrl(url, EMPTY)).toBe(url);
  });

  it("is a no-op for a pipeline the table does not list", () => {
    const url = `/b/demo/pipeline/${encodeURIComponent("some-other-pipeline")}`;
    expect(shortenUrl(url, TABLE)).toBe(url);
  });

  it("preserves an absolute URL as absolute", () => {
    const url = `http://localhost:8000/b/demo/pipeline/${encodeURIComponent(PIPE_A)}`;
    expect(shortenUrl(url, TABLE)).toBe(
      `http://localhost:8000/b/demo/pipeline/${ALIAS_A}`
    );
  });
});

describe("expanding a link", () => {
  it("restores the readable id in the path", () => {
    expect(expandUrl(`/b/demo/pipeline/${ALIAS_A}`, TABLE)).toBe(
      `/b/demo/pipeline/${encodeURIComponent(PIPE_A)}`
    );
  });

  it("restores both query parameters", () => {
    const expanded = expandUrl(
      `/b/demo/errors?pipeline=${ALIAS_A}&pipeline2=${ALIAS_B}`,
      TABLE
    );
    const params = new URLSearchParams(expanded.split("?")[1]);
    expect(params.get("pipeline")).toBe(PIPE_A);
    expect(params.get("pipeline2")).toBe(PIPE_B);
  });

  it("round-trips", () => {
    const url = `/b/demo/errors?pipeline=${encodeURIComponent(
      PIPE_A
    )}&pipeline2=${encodeURIComponent(PIPE_B)}&metric=llm_score`;
    expect(expandUrl(shortenUrl(url, TABLE), TABLE)).toBe(url);
  });

  it("leaves an unknown alias in place for the caller to reject", () => {
    // The app renders "not found" from this; quietly dropping the reference
    // would open a different view than the link named.
    const url = "/b/demo/pipeline/cccccccccc";
    expect(expandUrl(url, TABLE)).toBe(url);
  });

  it("leaves a full id untouched", () => {
    const url = `/b/demo/pipeline/${encodeURIComponent(PIPE_A)}`;
    expect(expandUrl(url, TABLE)).toBe(url);
  });

  it("does not throw on a malformed escape in the path", () => {
    expect(() => expandUrl("/b/demo/pipeline/%E0%A4%A", TABLE)).not.toThrow();
  });
});

describe("fetching the table", () => {
  beforeEach(() => {
    resetPipelineAliasCache();
    vi.unstubAllGlobals();
  });

  it("maps the server's snake_case field", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () =>
          new Response(
            JSON.stringify({
              benchmark_id: "demo",
              aliases: { [ALIAS_A]: PIPE_A },
              by_pipeline: { [PIPE_A]: ALIAS_A },
            }),
            { status: 200 }
          )
      )
    );
    const table = await fetchPipelineAliases("demo");
    expect(table.byPipeline[PIPE_A]).toBe(ALIAS_A);
  });

  it("fetches once per benchmark", async () => {
    const spy = vi.fn(
      async () =>
        new Response(JSON.stringify({ aliases: {}, by_pipeline: {} }), {
          status: 200,
        })
    );
    vi.stubGlobal("fetch", spy);
    await Promise.all([fetchPipelineAliases("demo"), fetchPipelineAliases("demo")]);
    await fetchPipelineAliases("demo");
    expect(spy).toHaveBeenCalledTimes(1);
  });

  it("returns an empty table rather than failing the page", async () => {
    // A benchmark with no summary 404s here. That must not break a view whose
    // links are all readable ids anyway.
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response(JSON.stringify({ detail: "nope" }), { status: 404 }))
    );
    await expect(fetchPipelineAliases("demo")).resolves.toEqual({
      aliases: {},
      byPipeline: {},
    });
  });
});
