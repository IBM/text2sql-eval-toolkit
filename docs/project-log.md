# Project Log

A running record of where the codebase stands and why. Newest entry first.
Plans for upcoming work live in [`docs/plan/`](plan/).

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

**Not yet done in Phase B:** rewiring the endpoints (2.3–2.4), retiring the large-benchmark
warning (2.5), listing cache (2.6), HTTP caching (2.7), and all frontend work (2.8).

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
