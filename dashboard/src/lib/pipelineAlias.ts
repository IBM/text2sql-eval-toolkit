/**
 * Short aliases for pipeline ids in URLs.
 *
 * A pipeline id such as `wxai:openai/gpt-oss-120b-agentic-baseline1-3attempts`
 * is fine in a path segment on its own, but the comparison and error-analysis
 * views can carry two of them plus a search term, and that address gets wrapped
 * by mail clients and truncated by chat apps. An alias is a ten-character
 * stand-in accepted anywhere an id is.
 *
 * The mapping is not computed here. The server derives it (see
 * `ui/aliases.py`), so there is one implementation of the hash rather than two
 * that can drift; this module only fetches and caches the table. That also
 * means an unknown alias is genuinely unknown rather than a hashing
 * disagreement.
 *
 * The alias does *not* survive a model rename — it is a hash of the id, so it
 * changes when the id does. It shortens links; it does not stabilise them.
 */

import { useEffect, useState } from "react";

import { apiFetch, apiUrl } from "./api";

export interface PipelineAliases {
  /** alias → pipeline id */
  aliases: Record<string, string>;
  /** pipeline id → alias */
  byPipeline: Record<string, string>;
}

const EMPTY: PipelineAliases = { aliases: {}, byPipeline: {} };

/** Same shape test as the server's: ten lowercase hex characters. */
const ALIAS_PATTERN = /^[0-9a-f]{10}$/;

export function looksLikeAlias(ref: string): boolean {
  return ALIAS_PATTERN.test(ref);
}

// One table per benchmark, kept for the life of the page. It is derived from
// the pipeline list, which does not change while a visitor is reading, and it
// is a few hundred bytes.
const cache = new Map<string, Promise<PipelineAliases>>();

export function fetchPipelineAliases(
  benchmarkId: string
): Promise<PipelineAliases> {
  const cached = cache.get(benchmarkId);
  if (cached) return cached;

  const pending = apiFetch(
    apiUrl(`/api/benchmarks/${encodeURIComponent(benchmarkId)}/pipeline-aliases`)
  )
    .then((res) => res.json())
    .then((body) => ({
      aliases: (body?.aliases ?? {}) as Record<string, string>,
      byPipeline: (body?.by_pipeline ?? {}) as Record<string, string>,
    }))
    .catch(() => {
      // A benchmark with no results has no aliases. Failing here would turn a
      // missing-summary 404 into a broken page, so callers get an empty table
      // and fall back to treating the reference as a literal id.
      cache.delete(benchmarkId);
      return EMPTY;
    });

  cache.set(benchmarkId, pending);
  return pending;
}

/** Clear the cache. Exposed for tests; nothing in the app calls it. */
export function resetPipelineAliasCache(): void {
  cache.clear();
}

/**
 * Rewrite the pipeline references in a URL through `translate`.
 *
 * Only the places that *hold* a reference are touched -- the `/pipeline/:ref`
 * path segment and the `pipeline` / `pipeline2` parameters. A blanket string
 * replacement would also rewrite a search term that happens to contain a model
 * name, silently changing what the link searches for.
 */
function rewriteRefs(
  url: string,
  translate: (ref: string) => string | undefined
): string {
  const base =
    typeof window === "undefined" ? "http://localhost" : window.location.origin;
  let parsed: URL;
  try {
    parsed = new URL(url, base);
  } catch {
    return url;
  }

  const segments = parsed.pathname.split("/");
  const marker = segments.indexOf("pipeline");
  if (marker !== -1 && marker + 1 < segments.length) {
    let ref = segments[marker + 1];
    try {
      ref = decodeURIComponent(ref);
    } catch {
      // A malformed escape is not a pipeline reference; leave it alone.
    }
    const replacement = translate(ref);
    if (replacement) {
      segments[marker + 1] = encodeURIComponent(replacement);
      parsed.pathname = segments.join("/");
    }
  }

  for (const key of ["pipeline", "pipeline2"]) {
    const value = parsed.searchParams.get(key);
    const replacement = value ? translate(value) : undefined;
    if (replacement) parsed.searchParams.set(key, replacement);
  }

  return url.startsWith("http")
    ? parsed.toString()
    : `${parsed.pathname}${parsed.search}`;
}

/** Replace pipeline ids with their aliases, for a short shareable link. */
export function shortenUrl(url: string, table: PipelineAliases): string {
  if (Object.keys(table.byPipeline).length === 0) return url;
  return rewriteRefs(url, (ref) => table.byPipeline[ref]);
}

/**
 * Replace aliases with the pipeline ids they name.
 *
 * The readable form stays canonical, so an alias link is expanded on arrival
 * rather than carried through the session.
 */
export function expandUrl(url: string, table: PipelineAliases): string {
  return rewriteRefs(url, (ref) =>
    looksLikeAlias(ref) ? table.aliases[ref] : undefined
  );
}


/**
 * Load the alias table for a benchmark, but only when something needs it.
 *
 * `enabled` is false on an ordinary visit, so a page that links pipelines by
 * their readable id costs no extra request. It turns true when the address
 * itself contains an alias -- which has to be resolved before anything can
 * render -- or when the reader asks for a short link.
 *
 * `ready` is derived by comparing what is loaded against what is wanted, not
 * set from inside the effect. Otherwise the first render after the address
 * changes reports "loaded" while still holding the previous benchmark's table,
 * and an alias link flashes "not found" before resolving.
 */
export function usePipelineAliases(
  benchmarkId: string | null,
  enabled: boolean
): { table: PipelineAliases; ready: boolean } {
  const wanted = enabled && benchmarkId ? benchmarkId : null;
  const [loaded, setLoaded] = useState<{
    key: string | null;
    table: PipelineAliases;
  }>({ key: null, table: EMPTY });

  useEffect(() => {
    if (wanted === null) return;
    let cancelled = false;
    void fetchPipelineAliases(wanted).then((table) => {
      if (!cancelled) setLoaded({ key: wanted, table });
    });
    return () => {
      cancelled = true;
    };
  }, [wanted]);

  return {
    table: loaded.key === wanted ? loaded.table : EMPTY,
    ready: loaded.key === wanted,
  };
}
