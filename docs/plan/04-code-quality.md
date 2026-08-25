# Goal 4 — Code Quality and Reliability

Get the codebase to a state where changes are safe to make, regressions are caught
automatically, and there is one source of truth for each thing.

This goal brackets the others: **Phase A** (foundations) comes first because the other
refactors are large and currently unprotected; **Phase E** (cleanup) follows once the
structural work has settled.

## Current state

- **No CI.** `.github/` contains only `dco.yml` — no workflows. Nothing runs tests, lint,
  or type checks on push or PR.
- **No tooling configuration.** `pyproject.toml` has no `[tool.ruff]`, `[tool.black]`,
  `[tool.pytest.ini_options]`, and no `dev` extra. CONTRIBUTING asks contributors to
  `pip install pytest black ruff mypy` by hand and to follow Black/Ruff conventions that
  nothing enforces.
- **Two dependency sources.** `requirements.txt` is a pinned freeze that overlaps and
  disagrees with `pyproject.toml` `dependencies` (it carries `datasets`, `evaluate`,
  `aiohttp`, which are not project dependencies).
- **Duplicated, already-drifted config.** `data/benchmarks.json` and
  `src/text2sql_eval_toolkit/data/benchmarks.json` differ: the packaged copy lacks `logo`
  fields and several `db_engine` keys. The repo copy shadows the packaged one in a
  checkout, so the drift is invisible during development and only reaches pip users.
- **Version skew.** `pyproject.toml` and `__init__.py` say `1.1.0`; `CHANGELOG.md`
  documents `[1.2.0] - 2026-05-13` whose features are present in the code.
- **Thin test coverage** relative to ~13.6k lines of Python: 10 test files, no coverage
  measurement, and `tests/test_run_experiment_integration.py` requires live LLM and DB
  credentials (fails offline, so it cannot gate CI as written).
- **No frontend testing at all.** ~7.2k lines of TypeScript with no unit, component, or
  E2E tests, and no lint run in CI despite an `eslint` script existing in
  `dashboard/package.json`.

## Phase A — Foundations (before other refactors)

### 4.1 Tooling configuration
Add `[tool.ruff]`, `[tool.black]`, `[tool.pytest.ini_options]`, `[tool.mypy]`, and a
`dev` optional-dependency group to `pyproject.toml`, matching the conventions CONTRIBUTING
already states (Black defaults, 88 columns). Add `[tool.coverage]`.

*Acceptance:* `pip install -e ".[dev]"` provides the whole toolchain; config lives in one
file.

### 4.2 Establish the lint/type baseline
The snapshot could not run these tools (no `uv`/venv in the working checkout), so the
violation count is unknown. Run them, commit the output as a baseline, and fix or
explicitly ignore per rule. Do not attempt a repo-wide autofix in one commit — separate
mechanical formatting from semantic changes so review stays possible.

*Acceptance:* `ruff check` and `black --check` pass; `mypy` passes on an agreed initial
scope (suggest `results/`, `evaluation/`, `utils.py` first, widening over time).

### 4.3 Test markers and offline-safe suite
Mark credential-requiring tests (`@pytest.mark.integration`) and make the default `pytest`
run hermetic. `tests/results/test_integration.py` already models this with its
`RUN_NETWORK_TESTS` gate — generalize that pattern.

*Acceptance:* `pytest` passes with no network and no credentials; `pytest -m integration`
runs the rest.

### 4.4 CI
GitHub Actions: lint, format check, type check, `pytest` with coverage across the Python
matrix below, plus `npm ci && npm run lint && npm run build` and a bundle-size budget for
the frontend. Add DCO and (once 3.6 exists) an image build.

**Python versions.** Three separate decisions, often conflated:

| Decision | Value | Why |
|---|---|---|
| Library floor (`requires-python`) | **`>=3.11`** — unchanged | 3.10 reaches end of life around Oct 2026, so 3.11 is the oldest version still worth supporting. The code uses no syntax newer than 3.9, so raising the floor would exclude users for no benefit — and this ships on PyPI to researchers on whatever their cluster provides. |
| CI test matrix | **3.11, 3.12, 3.13** | Test the floor (or it silently rots), the version the README recommends, and the current mature release. Add **3.14 as a non-blocking job** first; promote it to blocking once the dependency wheels are confirmed — `psycopg2-binary` and `ibm_watsonx_ai` are the ones most likely to lag on a new interpreter. |
| Deployment pin (container) | **3.13** | Pin one exact version in the image, not a range. 3.13 is mature, supported to ~Oct 2029, and faster than 3.11. Move to 3.14 once it is blocking-green in CI. |

Keeping the floor at 3.11 while deploying on 3.13 is deliberate: the library stays broadly
installable, and the one environment we control runs something modern and fast.

*Acceptance:* every PR gets a status check; a red check blocks merge.

### 4.5 Frontend test harness
Vitest + Testing Library for units and components; Playwright for E2E. Seed with the
round-trip test that Goal 1 needs (navigate → copy URL → reopen → identical state).

*Acceptance:* harness runs in CI; the URL round-trip test exists and passes.

## Phase E — Cleanup (after the structural work)

### 4.6 Single source of truth for the benchmark registry
Pick one canonical location and generate or symlink the other, with a CI check that fails
on divergence. Then reconcile the existing drift. **Deciding which copy wins is a
behavioural change** — the repo copy currently shadows the packaged one — so verify that
pip-installed users get the right registry either way.

*Acceptance:* CI fails if the copies diverge; installed and checkout behaviour documented
and tested.

### 4.7 One dependency source
Make `pyproject.toml` authoritative. Either delete `requirements.txt` or generate it as a
lockfile (`uv pip compile`) with a CI freshness check. `uv.lock` is already committed.

Also revisit the unbounded `>=` constraints. Phase A hit this concretely: `sqlglot>=27.0.0`
resolved to 30.17.0, whose reclassification of ranking functions silently corrupted
profiling categories (see the project log). Upper bounds or a tested lockfile would have
caught it at install time rather than in published results.

*Acceptance:* one authoritative source; CI catches staleness.

### 4.8 Version hygiene → the 2.0.0 release
**Decided: release as `2.0.0`**, as the final commit on `dashboard-v2` after the
comprehensive test pass. This supersedes the 1.1.0 / 1.2.0 skew rather than adjudicating
it; the existing CHANGELOG entries remain as historical record.

Release checklist:

1. Comprehensive test pass green (all phases, `pytest` + `pytest -m integration`).
2. Bump `pyproject.toml` to `2.0.0`; single-source `__init__.__version__` from package
   metadata so the number lives in one place.
3. CHANGELOG entry for 2.0.0 covering the URL scheme, capability tiers, artifact index,
   and deployment.
4. **Publish a `v2.0.0` tag on the Hugging Face results repo.** `DEFAULT_REVISION` is
   derived as `v{version}` (`results/_hub.py:39`), so without it every fetch falls back
   to `main` with a warning and shared links stop being reproducible.
5. Tag `v2.0.0` in git — note only `v1.0.0` is currently tagged, so the tag history has
   its own gap.
6. Add a CI check that version, changelog, and git tag agree, so this cannot recur.

*Acceptance:* one version string; CI fails on a changelog entry with no matching bump.

### 4.9 Reduce the large modules
`ui/server.py` (2,549 lines, 28 endpoints) becomes routers by domain — benchmarks,
errors, compare, judge, jobs, results — with Pydantic models extracted. The Goal 2 and 3
work already rewrites much of it; splitting during that work is cheaper than a separate
pass. `agentic_pipeline.py` (2,338 lines) carries six versions (v0–v5) with substantial
shared logic; consolidate around a strategy interface **only after** confirming which
versions are still in use — published results reference these pipeline ids, so removing a
version invalidates existing artifacts and shared links.

*Acceptance:* no module over ~800 lines without justification; public API and pipeline ids
unchanged.

### 4.10 Coverage targets
Set a floor and ratchet it. Prioritize by risk: `evaluation/evaluation_tools.py` and
`metrics/text2sql_utils.py` (correctness of the published numbers), `execution_tools.py`
(five backends, largely untested), and the new index layer from Goal 2.

*Acceptance:* an agreed floor enforced in CI; the four risk areas above meaningfully
covered.

### 4.11 Documentation refresh
`README.md` is comprehensive but predates several features. Reconcile it with the CLI,
add `docs/` for architecture and the deployment runbook, document the artifact JSON
schema (currently only discoverable by reading the code), and update CONTRIBUTING to
reference the now-enforced tooling.

*Acceptance:* a new contributor can set up, test, and run the dashboard from the docs
alone.

### 4.12 Clear the deferred Ruff findings
Phase A silenced 39 real findings in `[tool.ruff.lint].ignore` so the tooling commit
stayed mechanical. Each needs a reviewed change, then its `ignore` entry removed:

| Rule | Count | Change required |
|---|---|---|
| `B904` | 19 | `raise ... from err` inside `except` — restores exception chaining |
| `B007` | 11 | Rename unused loop variables to `_` — cosmetic, zero risk |
| `B905` | 5 | Explicit `strict=` on `zip()` — `strict=False` preserves behaviour; audit whether `True` is actually correct at each site |
| `E722` | 4 | Bare `except:` → `except Exception:` — stops swallowing `KeyboardInterrupt`/`SystemExit`, a genuine behaviour change |

Do these as separate commits by rule, not one sweep.

*Acceptance:* the four entries are gone from `ignore` and CI is green.

## Known bugs to fix along the way

| Issue | Location | Notes |
|---|---|---|
| Uncached full-file parse on hot endpoints | `ui/server.py:994`, `:1149` | Addressed by Goal 2 |
| Unbounded, never-invalidated record cache | `ui/server.py:576` | Serves stale data after a re-run |
| Unsanitized `{name}` in judge config path | `ui/server.py:2174` | Verify traversal; add containment check |
| Registry copies drifted | `data/benchmarks.json` vs packaged | 4.6 |
| Version skew | `pyproject.toml` / `CHANGELOG.md` | 4.8 |
| Record counting re-parses data files per request | `ui/server.py:630` | Goal 2, item 2.6 |
| ~~Ranking window functions counted as aggregations~~ | `profiling/profiling_tools.py` | **Fixed** in `a8f2c96`; caused by unpinned sqlglot |

## Risks

- **Refactor without a net.** Phase A must genuinely land before Phases B–D; doing the
  large rewrites first and the tests afterwards is how regressions reach published
  results.
- **Churn in review.** Repo-wide formatting mixed with logic changes makes review
  impossible. Keep them in separate commits (4.2).
- **Breaking published artifacts.** Pipeline ids and the JSON schema are referenced by
  Hub snapshots, notebooks, and (after Goal 1) shared links. Treat both as public API.
