# Project Log

A running record of where the codebase stands and why. Newest entry first.

- **Current status at a glance**, item by item:
  [`plan/README.md#where-things-stand`](plan/README.md#where-things-stand)
- **Plans** for the remaining work: [`plan/`](plan/)
- **Running the deployment**: [`deployment-runbook.md`](deployment-runbook.md)

This file explains *why* each change was made and what it cost; the plan index is the
place to look for what is finished and what is not.

---

## 2026-08-25 — Phase E: coverage on the published numbers, docs, and module size

**Coverage where a bug is silent (4.10, partial).** Two modules produce artifacts that get
committed, uploaded, and cited, and both were effectively untested.

`evaluate_prediction` (4% → 35%, 26 tests) produces every metric the toolkit publishes.
Testing it found a defect with a wide blast radius: a record whose ground-truth SQL had no
dataframe returned `{"df_error": 0}` — no metrics, no error flag — and `compute_summary`
then subscripted a missing key and **aborted the summary for the entire benchmark**. One
bad record took out 500 good ones, failing far from its cause. Now flagged as an evaluation
error, and the summary counts missing metrics as 0, which is what the comment already there
said it did. Verified against the published artifacts: recomputing all 11 `archer_en_dev`
pipeline summaries reproduces the shipped numbers exactly.

`report_tools` (0% → 52%, 20 tests) writes `data/results/README.md`. Same shape of
fragility: both the table and the chart read metrics as `.get(key, {}).get("average")`,
which raises `AttributeError` if a metric is a bare number instead of the
`{average, stddev}` shape — aborting the whole report. Both paths now share a tolerant
reader.

Overall coverage 29% → 35%. `error_analysis.py` (5%) and the inference pipelines remain
the thin spots, and no floor is enforced yet.

**Deferred lint findings cleared (4.12).** All four Ruff rule groups, one commit each.
`B905` was the one that could not be swept: `strict=True` changes behaviour, so each of
seven sites was decided separately — and checking the artifacts first showed that 2 of
2,855 records would have made `strict=True` raise. `E722` mattered more than it looked:
a bare `except:` was swallowing Ctrl-C during long agentic runs. Ruff now runs with only
`F841` and `B008` ignored, both with stated reasons.

**Frontend types (4.13, partial).** All 17 `tsc` errors fixed and the check made blocking.
Two were mine: `ProfileCompareView` still referenced a variable I had deleted, so that view
threw `ReferenceError` on render — it built cleanly, and I had not opened that view. Eight
were duplicate-`key` errors where a Carbon prop getter spreads its own `key` over the
explicit one. The browser console also showed my CSP had no `font-src`, so all 120 IBM Plex
references were blocked and the UI had silently dropped to system fonts.

The 21 react-hooks effect findings stay off, with an honest reason rather than the stale
one: 5 are the fetch-on-mount pattern the rule cannot distinguish, and 15 are real debt
that decides which option a user sees — not worth rewriting blind without component tests.

**One source of truth (4.6, 4.7).** The packaged benchmark registry had lost every
benchmark's `logo`, invisible in development because the checkout copy shadows it.
`requirements.txt` was worse than redundant: 55 of its 73 entries were not project
dependencies while `openai`, which is one, was missing. Both are now generated and checked
in CI, along with `uv lock --check` — an export can agree with a lockfile that has itself
fallen behind.

**Documentation (4.11) and module size (4.9, partial).** README gains a documentation
index and sections on shareable links, the query index, and running the dashboard for other
people; the snapshot size is corrected from ~7 GB to the ~4 GB the manifest actually
reports. 41 Pydantic models moved out of `server.py` into `ui/models.py`, taking it from
3,446 to 3,149 lines. Splitting the 37 routes into routers is the remaining half and
interacts with the tier middleware, which walks `app.router.routes`.

**Component tests (4.5, partial).** Vitest gains Testing Library, a setup file (jsdom
implements neither `matchMedia` nor `ResizeObserver`, and Carbon reaches for both), and
component tests that mount real views against stubbed APIs. 34 → 51 frontend tests.

The point of these was to unblock 4.13: the 15 synchronous selection-reset effects each
decide which option a user ends up looking at, so rewriting them needed a test asserting
the *outcome* rather than the mechanism. `ToolkitInsightsView.test.tsx` now pins exactly
that — whatever the implementation, the selection must settle on a metric that exists, and
must move off one the server no longer defines.

Mounting the views immediately found a bug class none of the type or lint checks catch:
`a?.b.c()` guards only `a`. If `a` is present but `a.b` is undefined, the call still
throws. Five instances across four views, including
`summary?.overall.map(...)` — so a `/summary/by-category` response missing `overall`
took down the whole insights view with `TypeError: Cannot read properties of undefined`.
Optional chaining looks like a guard, which is what makes it easy to miss.

490 backend and 51 frontend tests passing.

---

## 2026-08-25 — Known trap: `make_summary_report.py` clobbers a hand-written file

`scripts/analysis/make_summary_report.py` writes the generated benchmark dashboard to
`data/results/README.md`, and `README.md` documents exactly that. But commit `59a2b99`
deliberately replaced that file with a 36-line guide to downloading results from the
Hugging Face Hub.

So running the documented command silently overwrites a file someone wrote on purpose. I
hit this while checking that a change to `report_tools` did not alter the published report:
the regenerated output differed completely, which looked like a regression until the git
history explained it.

Not fixed here, because the right fix is a product decision:

- point the generator at a different filename (say `data/results/SUMMARY.md`) and leave
  `README.md` as the download guide, or
- restore the generated dashboard as `README.md` and move the download guide elsewhere.

Either way the docs and the script should agree. Recorded rather than silently changed.

---

## 2026-08-25 — Phase D begins: capability tiers and Google sign-in (3.1–3.2)

**Capability tiers (3.1).** Authorization is now resolved once per request from the
deployment mode and the caller's identity, and enforced in a single middleware rather than
across twenty-eight handlers.

- `public` — read-only. Anonymous, or signed in without an allowlist entry.
- `judge` — adds on-demand LLM-as-judge for allowlisted signed-in users.
- `full` — everything, as before; still the local default.

Two properties make it hard to get wrong. A mutating route with no declared tier requires
`full`, so forgetting to classify a new endpoint fails closed. And the startup mode is a
*ceiling* — signing in can never raise a public deployment to `full`. `--mode full`
refuses a non-loopback bind without `--allow-remote-full`, so the dangerous configuration
takes deliberate effort.

The classification test earned its keep immediately: adding `/api/auth/logout` in the very
next step made it fail until the route was explicitly classified.

**Google sign-in (3.2).** Direct OIDC via Authlib — for an allowlist this small, an
identity service would add cost and a dependency for nothing.

The load-bearing check is `email_verified`. Google returns an `email` claim for unverified
addresses too, so matching the allowlist on `email` alone would make the allowlist
meaningless: anyone could create an account claiming `oktieh@gmail.com`. Sessions carry the
verified address and nothing else, so there is no user database to secure, and logs carry a
hash rather than the address.

`safe_redirect_target()` restricts the post-sign-in redirect to same-site paths, so a
crafted sign-in link cannot bounce a freshly authenticated user to another origin.

**Three bugs found by running it rather than reasoning about it**

1. HTTP middleware executes *before* routing, so `scope["route"]` is unset. Matching the
   concrete path would have sent every parameterised route to the fail-closed default —
   safe, but it would have made the judge tier unreachable. The template is now resolved
   against the router.
2. `request.session` *raises* when `SessionMiddleware` is absent, which it is in local
   mode, so `getattr(..., None)` did not help. Reads go through `request.scope` now.
3. The first tier test asserted every mutating route is refused in `public`, which was
   wrong as soon as a deliberately-public mutating route existed. It now checks each route
   against its *declared* tier, with a separate explicit list asserting the genuinely
   dangerous endpoints stay `full` — otherwise a relaxed tier would simply be rubber-
   stamped by the parametrised test.

**Scoped judge endpoint and budget (3.3–3.4).** Landed together, because a personal
watsonx key behind a public site without a ceiling is the failure mode the plan warns
about.

`POST /api/benchmarks/{id}/judge` judges one (record, pipeline) pair. The existing
`/evaluate` endpoint was the wrong shape for a shared deployment: it re-evaluates a whole
benchmark and rewrites the shared artifacts, so one user's re-run would change what every
visitor sees. Verdicts now go to a separate store attributed to the caller, and a test
asserts the canonical artifact's bytes are unchanged afterwards. Responses carry
`source="on-demand"` so they are never confused with the snapshot's `llm_score`.

Cost controls:

- Metered from **reported tokens, not call count** — judge prompts embed both result
  dataframes and vary by orders of magnitude.
- Counters **persist to SQLite on the data volume**. In-memory counters reset on restart,
  which is exactly how a monthly ceiling stops being one.
- A semaphore serialises calls and the ceiling is re-checked *inside* it, since a burst
  could otherwise each pass the check before any recorded spend.
- Kill switch disables the tier without a redeploy; `/api/me` then reports
  `can_run_judge=false` so the UI stops offering an action that would 503.
- Remaining budget is on `/api/me`, so the ceiling is visible before it is met.

Rates are configuration rather than source — the defaults are an estimate needing
calibration against a real invoice, and a provider that reports no usage is logged as
*unmetered* rather than silently counted as free.

`llm_as_judge` now returns `token_usage`, handling both the legacy `generate` shape
(`results[0].input_token_count`) and the Chat API shape (`usage.prompt_tokens`).

**Hardening (3.5).**

- **Judge config names** are validated and contained. Both the read *and write* endpoints
  interpolated a URL segment straight into `base_dir / f"{name}.yaml"`, so the write
  endpoint could place YAML outside the config directory.
- **CORS** allowed credentialed requests from a localhost origin list. Harmless while no
  session existed; adding sign-in made it real. Withdrawn outside full mode, where the UI
  is same-origin anyway.
- **Security headers** on every response: a restrictive CSP (no third-party scripts or
  frames), nosniff, DENY framing, same-origin referrer.
- **Rate limiting** per client outside full mode, with a tighter bucket for `/api/auth/*`.
  Local mode is exempt — throttling a single-operator interactive tool would be a
  regression for nothing.
- **Error detail** is tier-dependent. 404s named the exact file under `data/results/` and
  the commands to fix it; a public visitor can act on none of that and it discloses the
  filesystem layout.

**One real exposure found and fixed, created by two safe things combining.**
`/api/static/{path}` served any file beneath the data root, and being a GET it runs at the
public tier. The judge spend store was then placed *under* the data root. Neither is a
problem alone; together, `GET /api/static/judge/usage.sqlite` returned the full SQLite
database — spend ledger, cached verdicts, per-user hashes — to any anonymous visitor.
Confirmed against a running server, not inferred. The derived indices were equally
readable. The route is now scoped to `benchmarks/logos/` and to image extensions, which
is its only actual use; the URL contract is unchanged.

Verified end-to-end in public mode against the real 3.6 GB corpus: reads work (including
the 880 MB Beaver benchmark), `/execute`, `/evaluate`, `/judge` and the static store are
all 403, and 404s disclose nothing.

**Deployment stack, operations, and polish (3.6–3.9).**

- `deploy/Dockerfile` builds the frontend from source rather than trusting the committed
  `dashboard/dist`, pins an exact interpreter, runs non-root, and bakes in no credentials.
- `deploy/docker-compose.yml`: caddy, app, postgres, mysql. Only caddy publishes a port;
  the databases sit behind a profile so a browse-only deployment never starts them.
- Read-only database roles, with Postgres also getting `default_transaction_read_only`, a
  statement timeout, and revoked `CREATE` — a write is refused by the database, not by
  application logic.
- `deploy/provision.sh` fetches the pinned snapshot, indexes, verifies, and marks. It
  refuses to run without an explicit revision, since the default falls back to `main` and
  a floating revision would let the public dataset change under shared links.
- `docs/deployment-runbook.md` — first deploy, health checks (each corresponding to
  something that actually went wrong), routine operations, troubleshooting, and an honest
  list of what is *not* covered.
- `/api/deployment` plus a header strip and About panel: a "read-only" tag, a sign-in
  control that appears only when OAuth is configured, and a full-width stamp naming the
  snapshot on screen. A link shared today is only interpretable months later if the
  recipient can see which data it shows and that nothing is evaluated live.

Docker is unavailable in this environment, so a CI job now builds the image, validates the
compose config, and smoke-tests the running container — asserting `/api/me` reports the
public tier and `/execute` is refused — rather than the VM being the first thing to try it.

**The security review found three HIGH defects, all of which I had introduced.** Each was
confirmed by driving the real ASGI app before and after the fix:

1. **Authorization skipped entirely under a non-empty `root_path`.** The tier middleware
   gated on `scope["path"]`, but Starlette routes on `get_route_path()`, which strips
   `root_path`. Served under a sub-path, the middleware returned early while the router
   still dispatched — anonymous arbitrary SQL against the configured database credentials.
   Rate limiting was disabled by the same bug, on exactly those deployments. My earlier
   bypass probes all used an empty `root_path`, so a clean run there proved less than it
   appeared to.
2. **`TEXT2SQL_DASHBOARD_MODE` was only read inside `main()`**, so serving the ASGI app
   directly left the ceiling at `FULL` while the operator believed it was public. The
   allowlist *was* read at import, which made the configuration look like it worked.
3. **Enabling Google sign-in crashed startup.** `configure_cors()` eagerly rebuilt the
   middleware stack, after which Starlette refuses `add_middleware`. The entire auth path
   was unreachable as shipped — and therefore never exercised end to end, which is why the
   middleware ordering in (1) went unnoticed.

Also fixed from the same review: concurrent index builds shared one temp filename and
could promote a half-written database as authoritative results; `X-Forwarded-For` was
trusted from any peer using its forgeable leftmost value; the session cookie's `Secure`
flag came from the bind address and so dropped behind a TLS proxy; a 500 re-leaked the
absolute index path; session secrets had no length floor; and the verdict cache key was a
non-injective space-join.

Separately found while reviewing the composed surface: `/api/static` served the entire
data root at public tier, and the judge spend store lives under it —
`GET /api/static/judge/usage.sqlite` returned the full database to any visitor. Neither
piece was a problem alone. Now scoped to `benchmarks/logos/` and image types.

**Database services (3.10, 3.12).**

*SQLite (3.10).* `run_sqlite_query()` opened databases read-write. Benchmark databases are
immutable reference data and the execute endpoint runs arbitrary caller-supplied SQL, so a
write should be refused by SQLite rather than by anything upstream. Now opened with a
`mode=ro` URI, with `SQLITE_LIMIT_ATTACHED` set to 0. `ATTACH` was not exploitable through
this function — each call opens a fresh connection and sqlite3 runs one statement per
`execute` — but refusing it outright removes the dependency on that driver detail. Two
incidental fixes: a result set with no rows raised `TypeError` because `cursor.description`
is `None` for such statements, and the URI form needs the path percent-encoded or a
directory containing a space would fail to open.

*Beaver/MySQL (3.12).* The dumps arrived, and the databases are loaded and verified.

The dumps are named after the source systems (`dw`, `nova`, `neutron`) but the benchmark
addresses two of them by a prefixed name (`csail_stata_nova`, `csail_stata_neutron`),
because `db_id` is substituted into the connection string per record. Loading the dumps
unchanged produces databases the benchmark cannot find, so `deploy/load-beaver.sh` rewrites
the `CREATE DATABASE` / `USE` statements as it streams — anchored to line starts and
matched against the specific source names, with a table-count check afterwards as the
actual proof that nothing was mangled.

Loaded and verified against a real MySQL 8:

| Database | Tables (schema / loaded) | Gold queries |
|---|---|---|
| `dw` | 97 / 97 | 121 / 121 pass |
| `csail_stata_nova` | 109 / 110 | 43 / 43 pass |
| `csail_stata_neutron` | 175 / 175 | 30 / 30 pass |

**All 194 executable gold queries run successfully, in 9.5 s.** (`nova` has one table more
than the schema records, which is harmless.)

The remaining 15 questions reference `keystone` (8), `csail_stata_glance` (5), and
`csail_stata_cinder` (2), for which **no dumps are published upstream**. They fail with a
clear unknown-database error. That is a data gap rather than a configuration mistake, and
it is now documented in `data/benchmarks/dbs/README.md` alongside the load procedure — which
previously just pointed at the upstream project.

**BIRD Mini-Dev, SQLite and PostgreSQL (3.11).** The Mini-Dev download arrived, and both
variants now execute.

*A path bug meant the documented SQLite setup could never have worked.*
`sqlite_run_execution_async` built database paths from `Path(BENCHMARKS_FILE).parent`, the
copy packaged inside the installed wheel — while the registry, and
`data/benchmarks/dbs/README.md`, both use `data/benchmarks/dbs/`. Following the
instructions put the databases somewhere the code never looked. `resolve_sqlite_db_path()`
now resolves against the registry actually in use (`$TEXT2SQL_DATA_ROOT`, else the
repository's `data/`), falling back to the packaged location so an installed-only layout
still works, and reports the documented path in the error when a database is absent.
**All 500 BIRD SQLite gold queries pass** across the 11 databases.

*PostgreSQL.* `deploy/load-bird-postgres.sh` loads the 1 GB dump with `ON_ERROR_STOP`, so a
partial load fails loudly rather than leaving a database that looks fine until a query hits
a missing table. The first attempt failed on `role "xiaolongli" does not exist` — the dump
carries `OWNER TO` for whoever produced it. The script now creates any such role as
`NOLOGIN` before loading, rather than rewriting a gigabyte of SQL to strip ownership: a
login-less role grants nobody anything, and editing the dump risks mangling a data line.
**All 500 gold queries pass, in 3.4 s.**

Worth recording the shape difference, since it explains the execution code: the Postgres
dump merges all eleven BIRD databases into a single `public` schema (75 tables), which is
why `postgres_run_execution_async` sets `search_path` once and never switches on `db_id`,
while the SQLite and MySQL paths do switch per record.

437 tests passing. **Phase D is complete.** Four of six benchmarks are fully executable
locally (BIRD SQLite 500/500, BIRD Postgres 500/500, Beaver 194/194 loadable); Spider and
Archer need their own downloads, and 15 Beaver questions await three unpublished dumps.

---

## 2026-08-25 — Local setup fixed: full result set fetched, index validated on real data

The dashboard was 404ing on five of six benchmarks because only `archer_en_dev` had ever
been downloaded. Fetched the published snapshot (3.6 GB) and validated the Phase B work
against real artifacts rather than a synthetic file.

**Two defects in the results distribution, found on the way**

1. **The published manifest is stale.** `results list` reported `bak`, `charts`, and
   `logs` as the available benchmarks — those are `results/` sub-directories, not
   benchmarks. The manifest was generated 2026-05-13, before commit `4a04755` fixed
   manifest generation, so `upload_results_to_hub.py` is already correct and the snapshot
   simply needs regenerating. Downloads are unaffected: an unfiltered fetch ignores the
   manifest, and a filtered one already falls back to direct path patterns. `results list`
   now says so instead of presenting directories as data.
2. **The 2.0.0 release is blocked on republishing the snapshot.** `_validate_manifest()`
   *raises* when the installed version falls outside `toolkit_version_compat`, and the
   published manifest declares `>=1.1.0,<2.0.0`. A 2.0.0 install would fail
   `results fetch` outright. The upload script derives that string from the toolkit
   version at upload time, so re-uploading from a 2.0.0 install fixes it — but it has to
   happen *after* the bump. Recorded on the release checklist (plan item 4.8).

**Index validated on the real corpus**

| Benchmark | Artifact | Index | Ratio | Build |
|---|---|---|---|---|
| beaver | 880 MB | 13.7 MB | 2% | 8.4 s |
| bird_mini_dev_postgres | 385 MB | 22.7 MB | 6% | 2.8 s |
| bird_mini_dev_sqlite | 334 MB | 23.1 MB | 7% | 2.3 s |
| spider_dev | 195 MB | 37.7 MB | 19% | 2.4 s |
| spider_realistic | 108 MB | 18.4 MB | 17% | 1.5 s |
| archer_en_dev | 15 MB | 1.9 MB | 13% | 0.2 s |
| **total** | **1,915 MB** | **117 MB** | **6%** | ~18 s |

Better than the 14% estimated from the synthetic file. **Five of the six artifacts exceed
the 100 MB threshold** at which the old UI warned that loading might crash the server, so
the index is what makes this data usable at all, not merely faster.

Serving latency on real data — page 1 3–11 ms, page 20 1–6 ms, record detail 1–13 ms —
flat in page number and independent of artifact size. All 65 pipeline drill-downs across
the six benchmarks return correctly.

**Build memory is dominated by the largest single record, not by batching.** Beaver
contains one 108 MB record whose parsed form costs ~324 MB transiently, which is why that
build peaks near 1 GB while the others sit at 288–537 MB. Flushing is now size-aware
rather than every-500-records (record sizes span two orders of magnitude: 4.2 MB average
in Beaver against 0.14 MB in Archer), and outsized records are logged. The deployment
implication is on record: **provision indices before starting the server**, so a rebuild
never spikes while the app and both databases are live on a 4 GB VM.

---

## 2026-08-25 — Phase C: shareable URLs (plan items 1.1–1.5, 1.7)

The dashboard had no URL state at all: navigation was nine `useState` values, no router
dependency, and nothing under `dashboard/src` ever touched `window.location`. Every view
rendered at `/`.

**Landed**

- `lib/routes.ts` — the whole URL scheme as pure functions: path builders, query
  serialization with defaults omitted, and a parser. Pipeline ids contain both `:` and `/`
  (`wxai:openai/gpt-oss-120b-...`), so they must be percent-encoded in path position;
  keeping that in one place is what stops a missed `encodeURIComponent` from silently
  producing a 404.
- `App.tsx` derives view, benchmark, and pipeline from the URL and navigates instead of
  setting state. Error-analysis filters, page, page size, and the selected record all
  travel in the query string.
- Unknown paths render an explicit not-found state instead of silently showing the landing
  page — a shared link is exactly where the target may not exist.
- `SPAStaticFiles` serves `index.html` for unknown non-API paths. Without it every deep
  link 404s on refresh and the feature is useless in practice. `/api/*` and paths with a
  file suffix still return real 404s, so a typo'd bundle path does not come back as HTML.
- A **Copy link** control in the header, with a fallback for browsers that withhold
  `navigator.clipboard` over plain http.
- Vitest added and wired into CI; 34 frontend tests.

**Two bugs found by actually opening the app in a browser**

Both passed every automated check first:

1. **Pagination did not round-trip.** The URL carried `page=2` but the view rendered page 1
   — `page`, `pageSize`, and `record` were being filtered out before reaching
   `ErrorAnalysis`. Fixed by widening its props and reporting state changes back so the
   address bar follows.
2. **A restored record opened an empty panel.** Clicking a row resolves which pipeline's
   detail to show; restoring from a URL set only the record id, so the detail fetch — which
   requires both — never ran. The resolution is now a shared pure function
   (`lib/detailPipeline.ts`) used by both paths, with tests asserting they agree.

Verified in a browser: a link carrying benchmark, pipeline filter, metric value, page, and
record id reopens with page 2 showing `26–50 of 62` and the record detail panel populated
with the question, both SQL statements, and evaluation metrics.

**Then (plan items 2.7–2.9)** — the rest of Phase B:

- **Code splitting.** Route views load on demand; the entry chunk carried all eleven
  whether or not they were opened. 556 KB → 401 KB (gzip 167 → 127 KB) across 12 chunks.
  The CI budget was changed to measure the *entry* chunk rather than the sum of all JS —
  summing every chunk would have gone *up* after splitting, hiding exactly what the budget
  exists to guard.
- **Asset revalidation.** Data-root assets (benchmark logos) were served `no-store`, so
  every page view re-downloaded every logo. They now carry an ETag from size and mtime.
- **Async audit.** Both `async def` endpoints reached `get_index()` through sync helpers
  (`execute_sql_for_record → _resolve_record_db_id → get_index`, and
  `playground_evaluate → _find_eval_record_optional → get_index`). `get_index()` builds
  the index when it is missing or stale — 4.6 s for a 415 MB artifact — so an unlucky
  first request would have stalled the event loop and every other in-flight request with
  it. Both now warm the index via `asyncio.to_thread` first. A structural test walks the
  call graph and fails if a new async endpoint reintroduces the path.

Also corrected a test that was asserting nothing: the path-traversal check on
`/api/static/` used a plain `../`, which HTTP clients normalise away before sending, so
the request never reached the handler. It now uses encoded traversal, which does reach it;
the containment check holds.

**Not yet done in Phase C:** the stable pipeline-hash alias (1.6). Phase B's data-fetching
work (TanStack Query, list virtualisation) is also outstanding.

---

## 2026-08-24 — Phase B: artifact index landed (plan items 2.1–2.2)

The backend half of the performance work. Endpoints are not rewired yet; this entry
covers the index itself, which everything else in Phase B depends on.

**What was built**

- `indexing/scanner.py` — streams an evaluation artifact and reports the exact byte range
  of every top-level record, with bounded memory. Brace counting is only valid outside
  string literals, so it tracks string state and backslash escapes including across read
  boundaries. Measured at 144 MB/s.
- `indexing/builder.py` — walks the artifact once and writes a SQLite index holding
  per-record identity and byte range, the full `evaluation` block per (record, pipeline),
  and numeric metrics in a tall indexed table. Atomic (temp file + rename), self-
  invalidating on source size/mtime/schema change, and disposable.
- `indexing/store.py` — the read API endpoints will use: filtered/paginated listing,
  aggregates, cross-pipeline confusion, and single-record reads by byte range.
- CLI: `text2sql-eval-toolkit index build` and `index status`.

**Measured on a 415 MB artifact** (the real 15 MB file scaled ×30; the largest artifact
available locally, since `data/results/` holds only one benchmark)

| Operation | Before (full parse) | Index | Change |
|---|---|---|---|
| List, page 1 | 1,087 ms | 1.49 ms | 730× |
| List, page 40 | 951 ms | 2.09 ms | 455× |
| Record detail | 921 ms | 0.33 ms | 2,818× |
| Peak RSS | **2,151 MB** | **170 MB** | independent of artifact size |
| Index size | — | 57 MB | 14% of source |

The memory column is the one that matters for deployment: parsing a 415 MB artifact once
cost 2.1 GB of RSS, so the frontend's 100 MB "may crash the server" warning was
well-founded. Index-backed serving is flat regardless of artifact size, which is what
makes a 4 GB VM viable. Extrapolating 14% to the full ~7 GB result set gives roughly
1 GB of indices — comfortable on the 40 GB Hetzner disk.

Build cost is 4.6 s for 415 MB, so the whole published set indexes in a few minutes as a
one-time provisioning step.

**Correctness**

125 new tests. The important ones are differential: `test_indexing_differential.py`
reimplements the previous endpoint filtering verbatim and asserts the index returns
identical record ids, ordering, totals, and evaluation payloads across 80+ filter
combinations, plus pagination stability and confusion matrices. The scanner is tested
against braces-in-strings, escaped quotes and backslashes, and non-ASCII input, each
re-scanned at every chunk boundary from 1 byte upward.

Two divergences were found and closed while writing those tests:

- **Booleans.** The first builder excluded `bool` from numeric metrics, but the endpoints
  test `isinstance(v, (int, float))`, which `bool` passes. A boolean metric would have
  been silently dropped, making a filter return nothing where the old code returned
  matches. Now indexed as 1.0/0.0.
- **Index size.** The first schema stored 55-character pipeline ids and 19-character
  metric names on every metric row: 8.5 MB of index for a 15 MB source (57%). Interning
  both to integers cut it to 13%, which is what makes the 7 GB set tractable.

**Then (plan items 2.3–2.6)** — endpoints rewired onto the index:

- `list_errors` filters, counts, and paginates in SQL.
- `get_error_detail`, `get_error_detail_for_pipeline`, `_resolve_record_db_id`, and
  `_find_eval_record_optional` read one record by stored byte range.
- Both insight confusion endpoints aggregate in SQL.
- `get_benchmark_summary_by_category` still needs a whole-corpus pass but now streams one
  record at a time instead of materialising the artifact.
- `EVAL_RECORDS_CACHE` (unbounded, never invalidated) is gone; index handles are cached
  instead and a changed source file invalidates its handle, so an evaluation re-run is
  picked up without a restart.
- `count_records` is cached on file size and mtime — the landing page was re-parsing every
  benchmark data file on every request.
- The large-benchmark OOM warning is retired: `isLargeBenchmark`, the "Large" tag, and the
  banner are gone, since memory no longer scales with artifact size.

**Three toolchain defects surfaced by actually running things**

The Phase A CI workflow would have failed on its first run, which is worth recording since
the branch has still never been pushed:

1. **`npm ci` failed** — `package-lock.json` was out of sync with `package.json`. Regenerated.
2. **`npm run lint` was declared but ESLint was never a dependency**, so the script errored.
   Added ESLint with a conservative flat config.
3. **`vite build` does not type-check**, so the ~7.2k lines of TypeScript had never been
   checked. Added a `typecheck` script; it currently reports 18 pre-existing errors,
   including 7 duplicate-`key` warnings where a Carbon `getHeaderProps()` spread overwrites
   an explicit `key`.

Two real frontend defects were fixed along the way: a ref assigned during render in
`RunEvaluationView`, and an unused catch binding in `api.ts`.

**Deferred with documented switches** (plan item 4.13): 20 ESLint findings and the 18 type
errors. All need effects restructured, which the routing work will largely redo — fixing
them now would be immediately rewritten.

**Not yet done in Phase B:** HTTP-level caching (2.7), frontend data-fetching/code-splitting
/virtualisation (2.8), and the async-handler audit (2.9).

---

## 2026-08-24 — Release strategy set: `dashboard-v2`, shipping as 2.0.0

All remaining phases land on a single branch, `dashboard-v2`, branched from the Phase A
work. Nothing is pushed until the whole programme is complete and comprehensively tested;
CI therefore runs for the first time on a finished branch.

The release is **2.0.0** — a major bump is the honest number given the URL scheme,
capability tiers, artifact index, and deployment model, and it supersedes the 1.1.0 /
1.2.0 skew rather than requiring it to be adjudicated. The version change is the *last*
commit, after the test pass.

One consequence to carry forward: `DEFAULT_REVISION` is derived as `v{version}`
(`results/_hub.py:39`), so 2.0.0 will request a `v2.0.0` tag on the Hugging Face results
repo and fall back to `main` with a warning if it does not exist. Publishing that Hub tag
is on the release checklist (plan item 4.8).

`phase-a-foundations` is kept as a local marker for where Phase A ended.

---

## 2026-08-24 — Phase A started: tooling and CI foundations

Branch `phase-a-foundations`. First implementation step of
[`docs/plan/`](plan/), addressing observation 4 of the baseline snapshot below.

**Landed**

- `pyproject.toml`: added `[tool.black]`, `[tool.ruff]`, `[tool.pytest.ini_options]`,
  `[tool.mypy]`, `[tool.coverage]`, and a `dev` optional-dependency group. Tool config now
  lives in one file, so local runs match CI.
- `.github/workflows/ci.yml`: the repo's first CI. Jobs — lint (ruff + black), typecheck
  (mypy, narrow scope), tests on Python 3.11/3.12/3.13, an advisory 3.14 job, and a
  dashboard job running lint, build, a bundle-size budget, and a staleness warning for the
  committed `dashboard/dist`.
- `tests/conftest.py`: registers the `integration` marker and auto-marks anything under an
  `*integration*` path, so a new file cannot silently start requiring credentials in the
  default run. Adds a `require_env` skip helper.
- `tests/test_run_experiment_integration.py`: marked `integration`. The default `pytest`
  run is now hermetic and can gate CI.
- `CONTRIBUTING.md`: reconciled with what is actually enforced.

**Deliberately not done**

- **The 1.1.0 / 1.2.0 version skew was left alone.** Bumping it changes `DEFAULT_REVISION`
  in `results/_hub.py`, which selects the Hugging Face snapshot tag — a user-visible
  behaviour change that does not belong in a tooling commit. Only `v1.0.0` is tagged in
  git, so whether 1.1.0 or 1.2.0 was actually released is ambiguous and needs the
  maintainer's answer. Stays in Phase E (plan item 4.8).
- No repo-wide formatting pass. Ruff's initial rule set is deliberately conservative
  (`E4`, `E7`, `E9`, `F`, `B`); import sorting and pyupgrade rewrite many files and belong
  in their own commit.

**Toolchain**

Installed `uv` (Homebrew) and CPython 3.13.15, created `.venv`, and installed
`-e ".[dev,dashboard]"`. The full toolchain now runs locally, so the baseline below is
measured rather than assumed.

**Measured baseline**

| Check | Result |
|---|---|
| `ruff check src tests scripts` | 83 findings → **0** (40 auto-fixed, 39 deferred with documented ignores) |
| `black --check` | 44 of 64 files reformatted → **clean** |
| `mypy` | **clean** on the configured scope (5 files) |
| `pytest` | 2 failures → **104 passed, 5 deselected** |

**The two test failures were a real pre-existing bug, not a tooling artifact.**
`sqlglot >=28` made `Rank`, `DenseRank`, `Lag`, `Lead`, `PercentRank`, `CumeDist`,
`FirstValue`, `LastValue`, and `NthValue` subclasses of `exp.AggFunc`.
`analyze_sql_query()` counts `exp.AggFunc` nodes directly, so `SELECT RANK() OVER (...)`
began reporting `query_aggregate_count=1` and picking up the `has_aggregation` tag. Since
that field drives the category breakdowns in the dashboard and the generated summary
reports, **any profiling run on a recent sqlglot silently mis-classified pure ranking
queries as aggregating** — the declared constraint is `sqlglot>=27.0.0` and it resolved to
30.17.0. Fixed in `a8f2c96`; windowed true aggregates such as `AVG(x) OVER (...)` are
still counted, matching the existing test expectations.

Found by running the test suite locally once the toolchain existed — not by CI, which has
not run yet (see status below). Still the first concrete return on the Phase A work: an
unpinned-dependency regression that nothing in the repo was positioned to notice.

**Follow-ups opened**

- 39 deferred Ruff findings (`B904` ×19, `B007` ×11, `B905` ×5, `E722` ×4) — plan item
  4.12. Each changes runtime behaviour, so they need a reviewed commit rather than being
  folded into tooling setup.
- Dependency pinning: the sqlglot drift suggests the unbounded `>=` constraints deserve
  upper bounds or a tested lockfile — folded into plan item 4.7.

**CI status: written, validated, never executed.** `.github/workflows/ci.yml` is committed
on a local branch with no upstream, so no workflow run exists. The file parses as YAML and
passes `actionlint` clean, but that is static validation — whether the jobs actually
succeed (dependency install on three interpreters, `npm ci`, the bundle-size budget)
is unknown until the branch is pushed. Treat every job as unproven until then.

**Commits** (branch `phase-a-foundations`, not pushed)

```
7f68087 build: add lint, type, and test tooling with CI
deb75a8 refactor: remove dead imports and empty f-string prefixes
a8f2c96 fix: stop counting ranking window functions as aggregations
a7a3aa9 style: apply Black formatting repo-wide
9d61dc7 chore: ignore the Black reformat in git blame
```

---

## 2026-08-24 — Baseline snapshot before the dashboard/quality refactor

Snapshot taken at `main` @ `60dd451` ahead of a four-goal refactor (shareable URLs,
performance, public deployment, code quality). This entry records what exists today and
which observations the plan is built on.

### Where the project is

`text2sql-eval-toolkit` v1.1.0 — a Python library, CLI, and local web dashboard for
evaluating text-to-SQL systems. Five stages: **inference → execution → evaluation →
profiling → analysis**. Published on PyPI; pre-computed results (~7 GB) distributed via
the Hugging Face Hub. Functionally complete and in active use; the gaps below are about
sharing, scale, and hygiene rather than missing features.

**Shape of the code**

| Area | Location | Size |
|---|---|---|
| Python package | `src/text2sql_eval_toolkit/` | ~13.6k lines |
| Dashboard backend | `ui/server.py` | 2,549 lines, 28 endpoints (19 GET / 9 mutating) |
| Agentic pipeline | `inference/agentic_pipeline.py` | 2,338 lines, 6 versions (v0–v5) |
| Execution engine | `execution/execution_tools.py` | 1,448 lines, 5 DB backends |
| Dashboard frontend | `dashboard/src/` | ~7.2k lines, React 18 + Carbon + Vite |
| Tests | `tests/` | 10 files |

**Data model.** One JSON file per benchmark accumulates through the stages
(`{benchmark}-predictions.json` → `_eval.json` → `_eval_summary.{json,csv,md}`). Records
carry `predictions: {pipeline_id: {...}}`; result dataframes are inlined as pandas
`orient='split'` JSON. `pipeline_id` is derived from model + pipeline variant and is the
unit of comparison everywhere.

### Observations behind the plan

Each item below was verified against the code at this commit.

**1. The dashboard has no URL state at all.**
Navigation is nine `useState` values in `dashboard/src/pages/App.tsx:61-70`
(`activeView`, `selectedBenchmark`, `selectedPipeline`, plus filter state). There is no
router dependency in `dashboard/package.json`, and no `window.location`, `pushState`, or
`useSearchParams` anywhere under `dashboard/src/`. Every view renders at `/`. Nothing is
linkable, bookmarkable, or restorable on reload; the back button leaves the app.
`ErrorAnalysis.tsx` alone holds ~15 pieces of shareable state (pipeline, metric, value,
op, pipeline2, metric2, disagree, page, pageSize, search, selected record, view mode).

**2. Large evaluation artifacts are re-parsed per request.**
Two loading paths coexist. `load_eval_records()` (`ui/server.py:585`) caches parsed
records in a process-global dict — used by 3 endpoints. The other 8 call
`load_json(eval_path)` directly with no caching, including the two hottest ones:
`list_errors` (`:994`) and `get_error_detail` (`:1149`). Fetching a *single record's*
detail therefore parses the entire eval JSON and linearly scans it. The frontend already
concedes the problem: `dashboard/src/lib/largeBenchmark.ts:4` flags eval files ≥100 MB as
liable to OOM the server, and the UI shows a warning instead of the data. The cache that
does exist is unbounded, never evicted, and never invalidated on file change.

**3. The dashboard is not safe to expose publicly in its current form.**
Nine mutating endpoints are unauthenticated. Two of them execute arbitrary
caller-supplied SQL against whatever database credentials the server holds:
`POST /api/benchmarks/{id}/execute` (`:1555`) and
`POST /api/benchmarks/{id}/playground/evaluate` (`:1792`). Others spend the operator's
LLM credits (`evaluate_benchmark` `:2200`), mutate benchmark data files
(`add_ground_truth_sql` `:1606`), rewrite the benchmark registry (`create_benchmark`
`:703`, `update_benchmark` `:728`), upload files (`upload_benchmark_logo` `:744`), and
overwrite YAML inside the installed package directory (`update_llm_judge_config`
`:2174` — the `{name}` path segment is also unsanitized and needs a traversal check).
There is no auth layer, no read-only mode, and no deployment artifact (no Dockerfile, no
compose file, no hosting config anywhere in the repo).

**4. No CI, and no tooling configuration to enforce quality.**
`.github/` contains only `dco.yml` — there are no workflows. `pyproject.toml` has no
`[tool.ruff]`, `[tool.black]`, `[tool.pytest.ini_options]`, or `dev` extra; CONTRIBUTING
tells contributors to `pip install pytest black ruff mypy` by hand. Nothing runs tests,
linting, or formatting on push or PR.

**5. Smaller correctness and hygiene issues.**
- Version skew: `pyproject.toml` and `__init__.py` say `1.1.0`, while `CHANGELOG.md`
  documents a `[1.2.0] - 2026-05-13` release whose features (the `results fetch` CLI) are
  present in the code. The bump was missed.
- The benchmark registry is duplicated at `data/benchmarks.json` and
  `src/text2sql_eval_toolkit/data/benchmarks.json`, and the copies **have already
  drifted** — the packaged one lacks the `logo` fields and several `db_engine` keys. The
  repo copy shadows the packaged one whenever cwd is the checkout, so the divergence is
  invisible in development and only affects pip-installed users.
- `requirements.txt` is a pinned freeze that overlaps and disagrees with the
  `dependencies` list in `pyproject.toml` (it carries `datasets`, `evaluate`, `aiohttp`,
  which are not project dependencies). Two sources of truth.
- `list_benchmarks` (`:630`) re-reads and re-parses every benchmark data file on every
  call just to count records — uncached, on the landing-page request path.

### Environment note

The working checkout has no `uv`, no virtualenv, and only the system Python 3 from Command
Line Tools. Lint/type-check tooling could not be executed for this snapshot, so item 4
reflects *configuration* that is verifiably absent rather than a measured violation count.
Establishing that baseline is the first task in the code-quality plan.
