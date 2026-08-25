# Refactor Plan

Plan for four goals, written against the baseline recorded in
[`../project-log.md`](../project-log.md) (`main` @ `60dd451`).

| # | Goal | Document |
|---|---|---|
| 1 | Shareable URLs for every dashboard artifact | [`01-shareable-urls.md`](01-shareable-urls.md) |
| 2 | Resource-efficient, responsive UI and backend | [`02-performance.md`](02-performance.md) |
| 3 | Public web deployment | [`03-public-deployment.md`](03-public-deployment.md) |
| 4 | Clean, reliable, up-to-date codebase | [`04-code-quality.md`](04-code-quality.md) |

## Where things stand

Branch `dashboard-v2`, **not yet pushed**. 444 backend tests and 34 frontend tests pass;
ruff, black, mypy and eslint are clean. CI is written and passes `actionlint`, but has
**never executed** — the first push is when it runs.

| Goal | Done | Remaining |
|---|---|---|
| 1 — Shareable URLs | 6 / 7 | Stable pipeline alias (1.6) |
| 2 — Performance | 8.5 / 9 | Data-fetching library and list virtualisation (part of 2.8) |
| 3 — Public deployment | 12 / 12 | — (container unbuilt locally; sign-in unexercised against Google) |
| 4 — Code quality | 8 / 13 | Effect findings, module size, coverage, 2.0.0 |

### Goal 1 — Shareable URLs

| Item | Status | Note |
|---|---|---|
| 1.1 Introduce routing | Done | `react-router-dom`, `lib/routes.ts` |
| 1.2 Replace `activeView` with route state | Done | URL is the source of truth |
| 1.3 Sync filter state to the query string | Done | Filters, page, page size, selected record |
| 1.4 Copy-link affordance | Done | Header control, with a clipboard fallback |
| 1.5 Server-side SPA fallback | Done | `SPAStaticFiles`; `/api/*` still 404s properly |
| **1.6 Stable identifiers** | **Not started** | Hash alias for pipeline ids. Retrofitting after links circulate means broken links, so this is the one Goal 1 item worth doing before launch |
| 1.7 Not-found and permission states | Done | Explicit not-found; capability surfaced via `/api/me` |

### Goal 2 — Performance

| Item | Status | Note |
|---|---|---|
| 2.1 Measurement harness | Done | Baselines recorded in the project log |
| 2.2 Index builder | Done | 1,915 MB of artifacts → 117 MB of indices (6%) |
| 2.3 Serve endpoints from the index | Done | 8 full-parse call sites removed |
| 2.4 Range-read record detail | Done | 921 ms → 0.3 ms |
| 2.5 Retire the large-benchmark warning | Done | Memory no longer scales with artifact size |
| 2.6 Cache the benchmark listing | Done | Keyed on file size and mtime |
| 2.7 HTTP-level efficiency | Done | ETag revalidation on data-root assets |
| **2.8 Frontend responsiveness** | **Partial** | Code splitting done (entry 556 → 401 KB). Data-fetching library and list virtualisation not done — worth re-assessing now that pages serve in ~3 ms |
| 2.9 Async correctness | Done | Index builds moved off the event loop; structural test guards it |

### Goal 3 — Public deployment

| Item | Status | Note |
|---|---|---|
| 3.1 Capability tiers | Done | Central enforcement, fails closed, mode is a ceiling |
| 3.2 Google sign-in | Done (code) | Verified by unit tests; **never exercised against real Google** |
| 3.3 Scoped judge endpoint | Done | Per-record; canonical artifacts untouched |
| 3.4 Cost controls | Done | $50/month, metered from tokens, persists across restarts |
| 3.5 Security hardening | Done | Plus three HIGH findings from review, all fixed |
| 3.6 Container image | Done (unbuilt) | No Docker locally; CI builds and smoke-tests it |
| 3.7 Provisioning automation | Done | Verified against the real 3.6 GB corpus |
| 3.8 Operations | Done | `docs/deployment-runbook.md` |
| 3.9 Public-facing polish | Done | Read-only tag, sign-in control, data stamp, About panel |
| 3.10 SQLite execution | Done | Read-only, `ATTACH` disabled |
| 3.11 PostgreSQL | Done | BIRD loaded; 500/500 gold queries |
| 3.12 MySQL (Beaver) | Done | 194/194 loadable; 15 await unpublished dumps |

### Goal 4 — Code quality

| Item | Status | Note |
|---|---|---|
| 4.1 Tooling configuration | Done | All config in `pyproject.toml` |
| 4.2 Lint/type baseline | Done | 83 ruff findings → 0; 44 files reformatted |
| 4.3 Test markers | Done | Default run is hermetic |
| 4.4 CI | Done (unrun) | 6 jobs; `actionlint` clean |
| **4.5 Frontend test harness** | **Partial** | Vitest done (34 tests). Playwright E2E not done — the URL round-trip is currently proven by hand, not by a test |
| 4.6 Registry single source of truth | Done | Checkout copy canonical; sync script, test, and CI check |
| 4.7 One dependency source | Done | `requirements.txt` now generated from `uv.lock`; CI checks both it and lock freshness |
| **4.8 Version hygiene → 2.0.0** | **Not started** | The final commit. Gated on a matching Hugging Face snapshot |
| **4.9 Reduce the large modules** | **Not started** | `ui/server.py` is now ~3,100 lines |
| **4.10 Coverage targets** | **Not started** | Currently **29%**. No floor enforced |
| **4.11 Documentation refresh** | **Partial** | Runbook and benchmark docs done; `README.md` still predates all of this |
| 4.12 Clear deferred Ruff findings | Done | All 4 rules re-enabled; only `F841` and `B008` remain ignored, both with stated reasons |
| 4.13 Clear deferred frontend findings | Partial | **All 17 `tsc` errors fixed and the check is now blocking.** 21 eslint effect findings remain off with a stated reason: 5 are the fetch-on-mount pattern the rule cannot distinguish, 15 are real debt that needs component tests (4.5) before being rewritten |

### Known limitations, stated plainly

- **CI has never run.** Everything is verified locally against the real toolchain, and the
  workflow passes `actionlint`, but no job has executed. `npm ci` failing was found this
  way once already.
- **The container has never been built** — no Docker in this environment.
- **Google sign-in has never completed a real round trip.** The verified-email rule, the
  redirect sanitiser, and the session wiring are unit-tested; no actual Google account has
  signed in.
- **Spider and Archer databases are not downloaded**, so those two benchmarks cannot
  execute locally. Their results still browse normally.
- **15 Beaver questions cannot run**: `keystone`, `csail_stata_glance` and
  `csail_stata_cinder` have no published dumps.
- **Coverage is 29%.** The suite is strong where it was written deliberately (indexing,
  tiers, auth, judge) and thin elsewhere.

---

## The decision that shapes everything else

**The dashboard is currently a local operator tool, and the plan splits it into two
modes rather than trying to make one server serve both purposes.**

Today's server assumes a trusted single user on `127.0.0.1`: it will execute
caller-supplied SQL against production database credentials, spend LLM budget, rewrite
the benchmark registry, and overwrite YAML inside its own installed package — all
unauthenticated (project log, observation 3). None of that can go on the public internet.

Rather than bolting auth onto 28 endpoints, the plan introduces **capability tiers**
resolved per request from the deployment profile and the caller's identity:

- **`public`** — anonymous, or signed in without an allowlist entry. GET endpoints only,
  serving pre-computed artifacts. This is the default for every visitor.
- **`judge`** — signed in via Google *and* on the allowlist. Adds on-demand
  LLM-as-judge using a server-held watsonx.ai key, under a $50/month spend ceiling.
- **`full`** — today's complete behaviour (SQL execution, inference, registry writes),
  local and loopback-bound only. Never runs on the public host.

Tiers are enforced centrally at the router level, not per-handler, so a new endpoint is
safe by default. The public host carries **no database credentials at all**, so even a
tier-enforcement bug cannot reach a database. This is detailed in
[`03-public-deployment.md`](03-public-deployment.md) and is a prerequisite for the
deployment, not an add-on to it.

## Sequencing

The goals are not independent. Suggested order, with the reasoning:

```
Phase A ── Goal 4 (foundations)      CI, lint/format config, test baseline
              │                       Nothing else is safely refactorable without this.
              ▼
Phase B ── Goal 2 (backend perf)     Indexed artifact access, bounded memory
              │                       A public server cannot hold GB-sized JSON in RAM.
              ▼
Phase C ── Goal 1 (URL routing)      Router + URL-encoded view state
              │                       "Share a link" is meaningless until links exist.
              ▼
Phase D ── Goal 3 (deployment)       Tiers, Google auth, container, Fly.io hosting
              │                       Depends on B (memory) and C (links to share).
              ▼
Phase E ── Goal 4 (cleanup)          Dedup, version hygiene, dead code, docs
```

Phase A and the frontend half of Phase C can proceed in parallel with Phase B, since they
touch disjoint files. Phase D must come last: deploying before B means an OOM on first
public traffic, and deploying before C means shipping a site where nothing is linkable.

## Branch and release strategy

**All phases land on one branch: `dashboard-v2`.** Nothing is pushed until the whole
programme is complete and comprehensively tested, so CI runs for the first time on a
finished branch rather than incrementally.

**The release is `2.0.0`.** A major bump is the honest number: the URL scheme, the
capability tiers, the artifact index, and the deployment model are all new or breaking.
It also resolves the 1.1.0 / 1.2.0 skew recorded in the project log — rather than
adjudicating whether 1.2.0 shipped, `2.0.0` supersedes both and the existing CHANGELOG
entries stay as historical record.

Consequences to handle at release time, not before:

- **`DEFAULT_REVISION` follows the package version.** `results/_hub.py` derives it as
  `v{version}`, so 2.0.0 will request a `v2.0.0` tag on the Hugging Face results repo and
  silently fall back to `main` if it is missing. A matching Hub tag must be published as
  part of the release, and the public deployment pins that tag explicitly.
- **Push and CI are deferred to the end.** The tradeoff is accepted deliberately: CI is
  validated statically (`actionlint`) and every check is run locally against the real
  toolchain as each phase lands, so the branch is not flying blind — but no job is proven
  green until the first push.
- **Testing gate before the bump.** The version change is the last commit, after the
  comprehensive test pass, not before it.

## Cross-cutting principles

- **Backward compatibility of artifacts.** The on-disk JSON contract
  (`{benchmark}-predictions_eval.json` and friends) is consumed by the library, the CLI,
  the notebooks, and published Hub snapshots. Performance work adds *derived indices
  alongside* these files; it does not change or replace them.
- **The local dashboard stays fully functional.** The public app adds a capability to
  the toolkit; it removes none. A default loopback launch keeps every feature it has
  today — SQL execution, inference, evaluation, registry editing — with no sign-in and no
  quotas. Enforced by a non-regression test, not by good intentions.
- **The public deployment is a consumer of the same artifacts.** It never runs inference
  or execution; it reads a pinned Hugging Face Hub snapshot fetched once onto disk. No
  separate data pipeline, no bespoke export format — the deployed site reads what
  `text2sql-eval-toolkit results fetch` produces.
- **Every phase lands behind passing CI.** Phase A exists so that the later, larger
  changes have a safety net.
- **Measure before optimizing.** Phase B opens with a benchmark harness so the
  improvements are demonstrable rather than assumed.

## Decisions and open questions

No open questions outstanding — everything needed to start Phase A is decided.

Decisions on record:

- **Hosting: a single Hetzner CX22 VM** (~$4–5/month) running Docker Compose — app,
  Postgres, MySQL, and Caddy for TLS, with the databases on an internal-only network.
  Live read-only API, not a static export. Roughly **$54–55/month all-in** with the judge
  budget.
- **Data: the pinned Hugging Face Hub snapshot**, fetched once onto a persistent volume.
- **Judge allowlist: `oktieh@gmail.com` only**, extendable later via env var.
- **Judge budget: $50/month** for LLM spend, metered from token usage.
- **Python: floor stays `>=3.11`**, CI matrix 3.11–3.13 (3.14 non-blocking), container
  pinned to 3.13. Rationale in [`04-code-quality.md`](04-code-quality.md).
- **Benchmark databases: all six in scope, self-hosted.** SQLite (4 benchmarks) as files
  on the volume, then PostgreSQL (BIRD), then MySQL (Beaver). Steps 1–2 are unblocked;
  Beaver waits only on load instructions from the maintainer. Licensing check gates
  exposure.

See [`03-public-deployment.md`](03-public-deployment.md) for the detail behind each.
