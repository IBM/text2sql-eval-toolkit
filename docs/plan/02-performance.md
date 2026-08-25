# Goal 2 — Resource Efficiency and Responsiveness

Make the dashboard fast and bounded in memory, so it stays responsive on large benchmarks
and can survive on a modest public host.

## Starting point (v1.1.0, before this work)

*Kept as the record of what this work was responding to: the codebase at `main` @
`60dd451`, version 1.1.0. It is **not** a description of the branch today — for that, see
[`README.md#where-things-stand`](README.md#where-things-stand).*

**The backend re-parsed whole evaluation files per request.** Two loading paths coexisted in
`ui/server.py`:

- `load_eval_records()` (`:585`) — caches parsed records in a process-global dict. Used by
  3 endpoints.
- `load_json(eval_path)` — no caching. Used by 8 endpoints, including the two hottest:
  `list_errors` (`:994`) and `get_error_detail` (`:1149`).

So paging through error analysis re-parses the entire file per page, and opening one
record's detail parses the entire file and linearly scans it for a single id. The
frontend already treats this as a known hazard: `lib/largeBenchmark.ts:4` marks eval files
≥100 MB as likely to OOM the server and shows a warning instead of the data. The published
result set is ~7 GB across six benchmarks.

The cache that does exist has no bound, no eviction, and no invalidation — it grows until
the process dies and serves stale data after a re-run.

Separately, `list_benchmarks` (`:630`) re-reads and re-parses every benchmark data file on
every call, just to count records, on the landing page's request path.

## Target architecture

**Stop treating a multi-hundred-megabyte JSON file as a request-time data structure.**
Build a derived, queryable index next to each artifact and serve requests from it; touch
the big JSON only to fetch one record's detail, by byte offset.

```
data/results/
  {benchmark}-predictions_eval.json        ← unchanged, still the source of truth
  .index/{benchmark}.sqlite                ← derived: one row per (record, pipeline)
                                              metrics, question text, byte offset+length
```

The index is: built by a CLI command, rebuilt automatically when the source file's
mtime/size changes, and fully disposable — deleting it costs a rebuild, never data. The
JSON contract is untouched, so the library, notebooks, and Hub snapshots are unaffected.

SQLite is the right fit here: zero-ops, single file, ships with Python, and turns the
filter/paginate/aggregate queries the dashboard actually performs into indexed lookups.
A columnar store (DuckDB/Parquet) is a reasonable alternative if aggregate scans over
millions of rows dominate — worth revisiting after 2.1 produces numbers.

## Work items

### 2.1 Measurement harness first
Before changing anything, capture a baseline: cold and warm latency for
`/api/benchmarks`, `/api/benchmarks/{id}/errors` (page 1 and page 40),
`/api/benchmarks/{id}/errors/{rid}`, and the profile endpoints; plus peak RSS while
serving them. Run against the largest benchmark available. Record results in
`docs/project-log.md`.

*Acceptance:* a repeatable script and a committed baseline table. Every later item is
judged against these numbers.

### 2.2 Index builder
`text2sql-eval-toolkit index build [--benchmarks ...]`, streaming the eval JSON with an
incremental parser (`ijson`) so building the index never loads the whole file into memory.
Record per `(record_id, pipeline_id)`: all evaluation metrics, question text, db_id, and
the byte offset/length of the record in the source file. Auto-rebuild on staleness;
`--force` to rebuild unconditionally.

*Acceptance:* index builds for the largest benchmark within bounded RSS (target <500 MB);
row counts match a full parse; stale indices rebuild automatically.

### 2.3 Serve list/filter/aggregate endpoints from the index
Rewrite `list_errors`, the profile endpoints, and the metric-insight aggregations as SQL
queries. Filtering, sorting, counting, and pagination move into the database. Delete the
in-process record cache and the `load_json(eval_path)` call sites.

*Acceptance:* responses are byte-identical to the current implementation (assert this with
a differential test over a full benchmark before deleting the old path); page-N latency is
flat in N; steady-state RSS is independent of artifact size.

### 2.4 Range-read record detail
`get_error_detail` and `get_error_detail_for_pipeline` seek to the indexed byte offset and
parse one record.

*Acceptance:* detail latency is independent of file size; response matches the current
implementation for every record in a benchmark.

### 2.5 Retire the large-benchmark warning
With 2.2–2.4 landed, `isLargeBenchmark()` and `LARGE_BENCHMARK_WARNING`
(`lib/largeBenchmark.ts`) describe a constraint that no longer exists. Remove them and the
UI paths that gate on them.

*Acceptance:* every benchmark opens fully regardless of artifact size; no size warnings
remain.

### 2.6 Cache the benchmark listing
Cache `list_benchmarks` results keyed on registry mtime; take record counts from the index
rather than re-parsing data files.

*Acceptance:* `/api/benchmarks` served from memory after first call; invalidates on
registry edit.

### 2.7 HTTP-level efficiency
`ETag`/`If-None-Match` on artifact-derived responses (artifacts are immutable between
runs, so this is nearly free), `Cache-Control` on hashed static assets, and gzip/brotli
via the reverse proxy. Note that `serve_dashboard_asset` (`:2330`) currently sets
`no-cache, no-store` on *every* asset — correct for a hot-reloading dev tool, wasteful for
a public site; make it conditional on the capability tier.

*Acceptance:* repeat navigation issues 304s; static assets cache with far-future headers.

### 2.8 Frontend responsiveness
- **Data fetching:** adopt TanStack Query (or an equivalent) for caching, deduplication,
  and cancellation of in-flight requests on rapid navigation. Today each view refetches on
  mount with no shared cache.
- **Code splitting:** route-level `React.lazy` so the initial bundle does not carry all
  eleven views. Add a bundle-size budget to CI.
- **Virtualized lists:** for long record and metric tables, render only visible rows.
- **Render hygiene:** audit the four largest views (`RunEvaluationView` 1,219 lines,
  `ErrorAnalysis` 1,188, `PipelineDetailView` 1,054, `ProfileCompareView` 765) for
  unmemoized derived work in render.

*Acceptance:* initial JS under an agreed budget; no visible jank paging through 1,000+
records; navigating away cancels in-flight requests.

**Status — code splitting done, the rest deliberately not.** Route-level `React.lazy`
across all eleven views took the entry bundle from 556 KB to 419 KB, and each view now
loads on its own route. Render hygiene improved as a side effect of 4.13: three views had
their selection logic moved from effects into derived values.

The data-fetching library and list virtualisation were both *reassessed and dropped for
now*, not forgotten. Both were sized against a backend that took 921 ms to serve a record
detail. After Goal 2 the same request is 0.3 ms and a page of records is 3–11 ms, which is
below the threshold where request deduplication or windowed rendering is perceptible.
Adding TanStack Query would also be the natural fix for 5 of the 17 remaining eslint
findings (4.13), so it is worth revisiting — but as a considered change with a measured
justification, not as a leftover checkbox.

### 2.3 addendum — streaming bounded memory, not time
`by-category` was converted from `json.load` to `iter_records()` and marked done. Memory
became independent of artifact size, which is what a public host needs, but the endpoint
still read every byte: 880 MB of Beaver to collect 36,839 floats, taking 14 seconds. It
now reads from the index (which stores `meta.categories` as of schema 2), at 0.05s.

The lesson for anything still marked "streamed": bounded memory and bounded time are
different claims, and only one of them was measured.

### 2.9 Async correctness on the server
Several handlers are `def` (sync) and perform blocking file I/O, which occupies a
threadpool worker per request; a few are `async def` doing genuinely async DB work. Make
this deliberate: blocking I/O either moves off the event loop explicitly or the handler
stays sync — but not by accident.

*Acceptance:* a documented convention plus a load test showing concurrent requests do not
serialize behind one slow file read.

## Testing

- Differential tests pinning old-vs-new endpoint responses over a whole benchmark (2.3,
  2.4). These are the safety net for the rewrite and should be written first.
- Memory-ceiling test asserting RSS stays bounded while serving the largest artifact.
- Index correctness: metrics from the index equal metrics from a full parse, for every
  record.
- Staleness: touching the source file triggers a rebuild.

## Risks

- **Index drift.** An index that silently disagrees with its source produces plausible but
  wrong analysis — worse than a slow dashboard. Mitigate with mtime+size validation, a
  cheap row-count check on open, and a `--force` rebuild escape hatch.
- **Rewrite regressions.** Eight endpoints change data sources at once. The differential
  tests in 2.3/2.4 are not optional.
- **Build-time cost moves, not disappears.** Indexing ~7 GB takes real time on first run.
  It must be incremental, resumable, and clearly reported — and ideally shipped
  pre-built alongside the Hub snapshot.
