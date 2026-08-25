# Refactor Plan

Plan for four goals, written against the baseline recorded in
[`../project-log.md`](../project-log.md) (`main` @ `60dd451`).

| # | Goal | Document |
|---|---|---|
| 1 | Shareable URLs for every dashboard artifact | [`01-shareable-urls.md`](01-shareable-urls.md) |
| 2 | Resource-efficient, responsive UI and backend | [`02-performance.md`](02-performance.md) |
| 3 | Public web deployment | [`03-public-deployment.md`](03-public-deployment.md) |
| 4 | Clean, reliable, up-to-date codebase | [`04-code-quality.md`](04-code-quality.md) |

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
