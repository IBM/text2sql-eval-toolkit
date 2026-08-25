# Goal 3 — Public Deployment

Host the dashboard on the public internet so anyone can browse results and shared links
resolve for the recipient, with a small allowlist of signed-in users able to run
LLM-as-judge against a server-held watsonx.ai key.

> **Blocking prerequisite.** The server as written cannot be exposed to the internet. Read
> "Why v1.1.0 could not ship as-is" first. This goal depends on Goal 2 (bounded memory) and
> Goal 1 (links worth sharing).

**The public app is an addition, not a replacement.** The local dashboard keeps every
capability it has today — full error analysis, SQL execution against local databases,
inference, evaluation runs, registry editing — with no sign-in and no quotas. That is the
`full` tier, and it stays the primary tool for anyone doing real work. The public
deployment adds one new thing to the toolkit: a URL that other people can open.

This is a hard constraint on the design, not an aspiration. Tier enforcement must default
to `full` for a loopback bind, so a developer running `text2sql-eval-dashboard` after these
changes sees exactly what they see today. A regression test asserting that is part of
item 3.1.

## Why v1.1.0 could not ship as-is

*Kept as the record of what this work was responding to: the codebase at `main` @
`60dd451`, version 1.1.0. It is **not** a description of the branch today — for that, see
[`README.md#where-things-stand`](README.md#where-things-stand).*

Nine unauthenticated mutating endpoints existed. The severe ones:

| Endpoint | Location | If it had been exposed publicly |
|---|---|---|
| `POST /api/benchmarks/{id}/execute` | `:1555` | Runs **arbitrary caller-supplied SQL** against the server's configured DB credentials |
| `POST /api/benchmarks/{id}/playground/evaluate` | `:1792` | Same, for multiple statements per call |
| `POST /api/benchmarks/{id}/evaluate` | `:2200` | Spends the operator's LLM budget; re-evaluates a whole benchmark and **overwrites shared artifacts** |
| `PUT /api/llm-judge/configs/{name}` | `:2174` | Overwrites YAML **inside the installed package dir**; `{name}` is unsanitized |
| `POST /api/benchmarks/{id}/ground-truth-sql` | `:1606` | Mutates benchmark data files |
| `POST /api/benchmarks`, `PUT /api/benchmarks/{id}` | `:703`, `:728` | Rewrites the benchmark registry |
| `POST /api/benchmarks/logo-upload` | `:744` | Unauthenticated file upload |

There was no auth layer, no rate limiting, and no deployment artifact in the repo.
All of the above is addressed by items 3.1-3.5; the line numbers are those of `60dd451`
and no longer resolve.

## Capability tiers

Capability is a function of **(deployment profile, request identity)**, enforced centrally
as a router dependency — never a per-handler `if`, so a route added later is safe by
default.

| Tier | Who | Can do |
|---|---|---|
| `public` | Anonymous, **or** signed in but not allowlisted | All GET endpoints: browse benchmarks, summaries, error analysis, record detail |
| `judge` | Signed in **and** allowlisted | `public` + on-demand LLM-as-judge on a single record, using the server-held watsonx key |
| `full` | Local operator only, bound to loopback | Today's complete behaviour: SQL execution, inference, registry writes |

**`full` never runs on the public host.** The public deployment holds **no database
credentials of any kind** — so even a total failure of tier enforcement cannot reach a
database, because there is nothing to reach.

This works because **LLM-as-judge needs no database.**
`evaluate_sql_prediction_with_llm()` (`evaluation/llm_as_judge.py:28`) takes only
question, ground-truth SQL, ground-truth dataframe, predicted SQL, predicted dataframe,
and the generation prompt — all already inside the evaluation artifacts. It builds a
`WXAIClient`, which reads `WATSONX_APIKEY` / `WATSONX_API_BASE` / `WATSONX_PROJECTID` from
the environment at call time (`inference/inference_tools.py:214`), so injecting the key as
a container secret needs no code change. The judge tier and the credential-free rule are
compatible by construction.

## Hosting

**Decided: a single Hetzner CX22 VM running Docker Compose — ~$4–5/month**, hosting the
app, both database engines, the results set, and the SQLite database files together.

This is the "full" configuration: every benchmark queryable, nothing deferred for
infrastructure reasons. It is chosen over managed platforms because co-locating Postgres
and MySQL with the app is both cheaper and simpler than stitching together separate
managed instances — see
[Self-hosting the databases](#self-hosting-the-databases). Specs: 2 vCPU / 4 GB RAM /
40 GB disk, 20 TB traffic included.

Alternatives considered, kept for the record:

| Option | Est. monthly | Why / why not |
|---|---|---|
| **Hetzner CX22 + Compose + Caddy** (chosen) | **~$4–5** | 2 vCPU / 4 GB / 40 GB and 20 TB traffic included — fits app + Postgres + MySQL + data with room to spare. Cheapest predictable cost *and* the only option that makes all six benchmarks queryable without extra services. Trade-off: you own OS patching, though the databases have no public listener. |
| Fly.io | ~$6–9 without databases, more with | Managed TLS, secrets, no OS to patch — the better pick for a database-free deployment. With databases it needs a larger machine plus separate database apps or volumes, ending up more expensive and more complex than the VM. |
| Railway / Render | ~$7–12 | Comparable ergonomics to Fly; persistent disks cost more and free tiers spin down (a spun-down service makes shared links look broken). |
| Google Cloud Run | ~$3–15, variable | Scales to zero, but has no persistent disk. The GB-scale artifacts would need GCS + FUSE, which defeats the byte-offset range reads Goal 2 depends on. Poor fit. |
| Oracle Cloud always-free | $0 | Genuinely free ARM capacity, but capacity availability and account-reclamation risk make it unsuitable for something meant to serve citable links. |

**Why not Fly.io:** it would be the right answer for a browse-only deployment, and the
managed TLS and secret storage are genuinely worth the small premium. It loses once
databases are in scope — Fly has no good story for two co-located database engines that
does not cost more than the whole VM.

Topology on the VM:

```
                    ┌──────────────── Hetzner CX22 ────────────────┐
  internet ──443──▶ │  caddy ──▶ app (FastAPI + built dashboard)   │
                    │              │                               │
                    │              ├──▶ postgres   (internal only) │
                    │              ├──▶ mysql      (internal only) │
                    │              └──▶ /data volume:              │
                    │                     results/ + .index/       │
                    │                     benchmarks/dbs/*.sqlite  │
                    └──────────────────────────────────────────────┘
```

Only Caddy publishes a port. Both database engines are reachable solely over the internal
Compose network.

Setup notes:
- **Caddy for TLS** — automatic Let's Encrypt issuance and renewal, which removes the main
  ongoing chore of self-managed hosting.
- **Region** close to the primary audience; single region is fine at this scale.
- **Unattended security upgrades** on the host, and pinned image digests for the services.
- **Disk headroom:** ~7 GB results + indices + SQLite database files must fit in 40 GB.
  Measure the SQLite set before provisioning (see the sizing note below); a Hetzner volume
  can be attached cheaply if it turns out tight.
- Egress is effectively free at this scale given the included 20 TB.

## Data: load once from the Hugging Face Hub

The public app is a **consumer of the published Hub snapshot**, not a producer. It never
runs inference or execution.

Provisioning, run once on first boot and idempotent thereafter:

1. `text2sql-eval-toolkit results fetch --revision <pinned-tag> --data-root /data`
   pulls the ~7 GB result set from `text2sql-eval-toolkit/text2sql-eval-results`
   (`results/_hub.py:35`) onto the persistent volume.
2. Build the Goal 2 indices over the fetched artifacts (`index build`).
3. Write a marker file recording the resolved revision, snapshot date, and toolkit
   version. Subsequent boots see the marker and skip straight to serving.

**Pin the revision explicitly.** `DEFAULT_REVISION` is derived from the installed toolkit
version (`v1.1.0`), and `_effective_revision()` (`results/_hub.py:91`) silently falls back
to `main` when that tag is missing — which it may be, given the version skew noted in the
project log. A floating `main` means the public dataset can change under shared links,
which defeats the point of citable URLs.

Refresh is a deliberate operation: bump the pinned revision, redeploy, re-index. The
marker file feeds the "data as of" stamp in the UI (item 3.9).

**Optimization to evaluate, not baseline:** the Hub serves files over HTTP with range
request support, so record detail could stream directly from the CDN and the volume would
only need to hold the indices — cutting storage to a fraction of 7 GB. Measure the added
latency before adopting; the local-volume path is the safe default.

## Benchmark databases

Only the "modify SQL and re-run" workflow in error analysis needs a live database.
Browsing, comparison, error analysis, and LLM-as-judge all read from the artifacts and
need none — so this is a *bonus* capability, not a prerequisite for the public app.

**Four of the six benchmarks are file-based and cost nothing to host:**

| Benchmark | Engine | Shape | Public hosting effort |
|---|---|---|---|
| `bird_mini_dev_sqlite` | SQLite | 11 DBs, 79 tables | **Trivial** — copy files to the volume |
| `spider_dev`, `spider_realistic` | SQLite | 166 DBs, 876 tables | **Trivial** — same volume |
| `archer_en_dev` | SQLite | file DBs | **Trivial** — same volume |
| `bird_mini_dev_postgres` | PostgreSQL | **one** DB, one `public` schema, 75 tables | **Easy** — one small managed instance |
| `beaver` | MySQL | **6** DBs, 463 tables | **Hardest** — see below |

Two facts from the execution code make this tractable:

- **Postgres does not switch databases per record.** `postgres_run_execution_async()`
  sets `search_path` to a single schema (`execution_tools.py:562`) and ignores `db_id`
  entirely — the upstream BIRD dump loads everything into one namespace. So the Postgres
  benchmark needs exactly one small database, not eleven. Load it with the documented
  `psql -d bird < MINIDEV_postgresql/BIRD_dev.sql` from the upstream package.
- **MySQL does switch databases per record.** `db_id` is substituted into the connection
  string (`execution_tools.py:238`), so Beaver needs all six databases present on one
  server. The connection code already handles SSL and managed-provider connection strings
  (`normalize_mysql_connection_string`, `:101`), so managed MySQL is compatible.

**Beaver is the one to defer.** It is the largest schema by far (463 tables), the repo has
**no provisioning tooling for it** — `data/benchmarks/dbs/README.md` punts to the upstream
project rather than giving a dump or load command, unlike every other benchmark — and
cheap managed MySQL is scarcer than managed Postgres. Self-hosting a MySQL container
beside the app is the realistic option if it is ever wanted.

### Self-hosting the databases

Postgres and MySQL run on the same VM as the app, as Compose services on named volumes.
This is cheaper than managed instances and, for this workload, also simpler and safer.

Three reasons it fits this workload unusually well:

- **The usual reason to pay for managed databases does not apply.** Backups, HA, and
  point-in-time recovery exist to protect data you cannot recreate. These are immutable
  read-only reference databases, rebuildable from upstream dumps by re-running the loader.
  Losing one costs a restore script, not data.
- **The databases never need to be internet-reachable.** Bind them to the internal
  container network only — no published ports, no public endpoint. That is a materially
  *smaller* attack surface than a managed instance with a public hostname and
  credentials in transit. Self-hosted-and-unexposed beats managed-and-exposed here.
- **Both engines on one box is a modest footprint.** Roughly: app ~500 MB (after the
  Goal 2 index work bounds it), Postgres ~256 MB, MySQL ~512 MB tuned down from defaults.
  Comfortable on 4 GB; workable but tight on 2 GB.

This is what tips the hosting choice. A Hetzner CX22 (2 vCPU / 4 GB / 40 GB, ~$4–5/month,
20 TB traffic included) holds the app, both databases, the results set, and the SQLite
files with room to spare — for less than Fly.io costs *without* databases, because Fly
would need a larger machine plus either separate database apps or additional volumes.

Practical notes:

- **One VM, Docker Compose** — not one container running all three under a process
  supervisor. Separate containers, healthchecks, and `depends_on` so the app waits for the
  databases to accept connections.
- **Idempotent init**, guarded by a marker on the volume, in the same spirit as the Hub
  fetch: create the read-only role, load the dump, mark done, skip on subsequent boots.
- **Pin engine major versions** (`postgres:17`, `mysql:8.4` or equivalent) so a rebuild
  cannot silently change dialect behaviour — which would quietly alter evaluation results.
- **Patching still lands on you.** It is a real cost, but a bounded one for two services
  with no public listener.

**What self-hosting does not solve:** Beaver still has no dump or load procedure in this
repo. Running MySQL locally removes the *hosting* obstacle entirely; obtaining and loading
the Beaver data remains the actual blocker, and is unchanged by where the server runs. So
self-hosting upgrades Beaver from "two blockers" to one — worth knowing before assuming
MySQL support is now easy.

### Licensing

Hosting these databases is redistribution of third-party academic datasets (BIRD, Spider,
Archer, Beaver), each with its own terms. **Verify each license before exposing any
database**, and treat Beaver with particular care as it derives from institutional data.

One distinction materially reduces the exposure: with the allowlist at a single user, a
hosted database is not *published* — it is remote access for one person who already holds
the data locally. That is a much weaker claim than public redistribution. If the allowlist
ever grows, or read-only browsing is opened to anonymous users, revisit this.

### If databases are hosted, non-negotiables

The execute endpoint runs arbitrary caller-supplied SQL, so even for one trusted user:

- **Read-only credentials.** A `SELECT`-only role, plus
  `default_transaction_read_only = on` for Postgres. Never the owner role.
- **A dedicated instance** holding only benchmark data — nothing else on that server.
- **No public listener.** Databases bind to the internal container network only. Nothing
  reaches them except the app; no published ports, no exposed hostname.
- **Statement timeouts and row caps** enforced server-side, not just via the API's
  `timeout_s` parameter.
- **SQLite opened read-only.** `run_sqlite_query()` currently calls
  `sqlite3.connect(db_path)` in read-write mode (`execution_tools.py:728`); a public
  deployment must use `sqlite3.connect(f"file:{path}?mode=ro", uri=True)`. Small change,
  and worth making unconditionally.
- **Execution stays gated at the `judge` tier** (rename it `authorized` if it grows past
  the judge use case). Never `public`.

### Rollout order

All six benchmarks are in scope. They ship in this order because each is independently
useful, not because the later ones are optional:

| Step | Engine | Benchmarks | Gate |
|---|---|---|---|
| 1 | SQLite | `bird_mini_dev_sqlite`, `spider_dev`, `spider_realistic`, `archer_en_dev` | None — files on the volume |
| 2 | PostgreSQL | `bird_mini_dev_postgres` | Upstream BIRD dump (documented) |
| 3 | MySQL | `beaver` | **Load instructions from the maintainer** |

Steps 1 and 2 are unblocked today. **Step 3 is blocked only on the Beaver load
procedure**, which the maintainer will supply; the `mysql` service, read-only role, and
execution path are built in step 3's prep so that loading the data is the only remaining
work when the instructions arrive. Nothing about steps 1–2 depends on step 3.

Volume sizing note: the SQLite database files are additional to the ~7 GB of results.
Measure them before provisioning, since `data/benchmarks/dbs/` is empty in a fresh
checkout and the sizes come from third-party downloads.

## Work items

### 3.1 Capability tiers
Implement the three tiers, audit all 28 endpoints into a tier, and enforce centrally.

*Acceptance:* a test asserts every mutating endpoint is rejected at `public`; a test fails
if a newly added route is unclassified; `full` refuses to bind a non-loopback interface
without an explicit override flag; **and a non-regression test asserts a default loopback
launch resolves to `full` with every endpoint reachable and no sign-in required.**

### 3.2 Google sign-in
OIDC against Google directly, via Authlib on FastAPI. No third-party identity service —
for an allowlist this small, Auth0/Clerk add cost and a dependency for nothing. Google
OAuth itself is free.

**Initial allowlist: `operator@example.com` only.** Everyone else — signed in or not — is
`public`.

- Authorization-code flow **with PKCE and a `state` parameter**.
- **Require the `email_verified` claim to be true** before matching the allowlist; the
  `email` claim alone is not trustworthy. Verify `iss` and `aud` on the ID token.
- Session as a signed, `httpOnly`, `Secure`, `SameSite=Lax` cookie with a short TTL.
  Store no user records — the session cookie is the whole state, which keeps the privacy
  story simple and means no user database to secure.
- Allowlist from a secret env var — `TEXT2SQL_JUDGE_ALLOWLIST=operator@example.com` —
  parsed as a comma-separated list and matched case-insensitively against the verified
  email. Adding a user later is an env var change and a restart, no redeploy and no code
  change. Keep it list-valued from day one even though it holds one entry.
- Signed-in-but-not-allowlisted resolves to `public` — with a clear message saying so,
  rather than a silent no-op.
- `GET /api/me` returns tier and email so the frontend can render the right affordances
  (ties into work item 1.7).
- Log only a hashed identifier, never the raw email.

*Acceptance:* allowlisted account reaches `judge`; non-allowlisted Google account is
`public`; forged/unverified-email tokens are rejected; sign-out invalidates the session.

### 3.3 Scoped LLM-judge endpoint
The existing `POST /api/benchmarks/{id}/evaluate` (`:2200`) is the wrong shape for this —
it re-evaluates an entire benchmark and writes back to the shared artifacts. Add a narrow
endpoint instead: judge **one (record, pipeline) pair** on demand.

- Results are written to a **per-user scratch store, never the canonical artifacts.** One
  user's re-run must not change what every other visitor sees, and the published numbers
  must stay reproducible against the pinned Hub snapshot.
- Verdicts are cached by `(record, pipeline, judge-config, model)` so repeat views cost
  nothing.
- Responses clearly label the verdict as on-demand and attributable to the requesting
  user, distinct from the pre-computed `llm_score` in the snapshot.

*Acceptance:* an allowlisted user can judge a record and see the verdict; the canonical
eval JSON on disk is byte-identical afterwards; a `public` user gets 403.

### 3.4 Cost controls on the judge path
The watsonx key is a personal credential and every call costs money.

**Ceiling: $50/month for LLM spend**, configurable as
`TEXT2SQL_JUDGE_MONTHLY_BUDGET_USD=50`. Independent of the ~$6–9/month hosting cost, so
plan on roughly **$56–59/month all-in**.

Mechanism:

- **Meter tokens, not calls.** Every judge response already carries token usage; record
  prompt/completion tokens per call and convert to dollars through a configurable
  per-model rate table. A call quota alone is a poor proxy — judge prompts embed both
  result dataframes and vary enormously in size.
- **Calibrate the rate table against real watsonx billing after the first week**, and
  treat the configured rates as an estimate until then. Do not hardcode rates in source.
- **Enforce at 100%, warn at 80%.** On reaching the ceiling the judge tier degrades to
  `public` for the rest of the billing month, with an explicit message rather than an
  opaque error.
- **Kill switch** env var disabling the judge tier immediately, without a redeploy.
- **Concurrency cap of 1–2 in-flight judge calls**, which also bounds the worst case if a
  loop or a refresh storm starts issuing requests.
- **Usage visible to the operator** — spend to date, remaining budget, and reset date on
  `/api/me` and in the UI, so the ceiling is never a surprise.
- Counters persist on the volume and survive restarts; a restart must not reset the
  month's accumulated spend.

With exactly one allowlisted user, the global ceiling *is* the per-user quota — so build
the global ceiling and the kill switch now, and keep the counter keyed by user so adding
people later needs no rework. Do not build per-user quota configuration UI for a
one-person allowlist.

*Acceptance:* spend is metered from real token counts and persists across restarts;
crossing $50 degrades `judge` to `public` with a clear message; the kill switch takes
effect without a redeploy; remaining budget is visible before it runs out.

### 3.5 Security hardening
Independent of tier, and worth fixing for local users too:
- Sanitize `{name}` in the LLM-judge config endpoints; add a containment assertion under
  `base_dir` regardless of whether encoded separators prove exploitable.
- Tighten CORS (`:63`) — currently `allow_credentials=True` with localhost origins. The
  public deployment is same-origin and needs neither; with cookie sessions in play, a
  loose CORS policy becomes a real risk rather than a theoretical one.
- Security headers (CSP, `X-Content-Type-Options`, `Referrer-Policy`) and rate limiting,
  applied to the auth endpoints as well as the API.
- Make error detail tier-dependent. Several 404s embed local filesystem paths and CLI
  hints (`_eval_not_found_detail`, `:93`) — helpful locally, leaky publicly.
- Secrets via the platform's secret store; never in the image, never in the repo.

*Acceptance:* `security-review` clean on the diff; no filesystem paths in `public`
responses.

### 3.6 Container image
Multi-stage: Node stage builds `dashboard/dist`, Python stage installs the package with
the `dashboard` extra. **Pin an exact interpreter — `python:3.13-slim`, not `3.13` or
`3`** — so a base-image refresh cannot change the runtime under a running deployment.
Non-root user, digest-pinned base image, healthcheck, no credentials baked in. **Build the frontend from source in the image** — `dashboard/dist` is committed
to the repo, and trusting it would let the deployed site silently drift from
`dashboard/src/`.

The full stack is a Compose file: `caddy`, `app`, `postgres`, `mysql`. Only `caddy`
publishes ports. Pin image digests for every service.

*Acceptance:* `docker compose up` serves the dashboard at `public` tier over TLS against a
mounted volume; the app image builds reproducibly in CI; neither database is reachable
from outside the Compose network.

### 3.7 Provisioning automation
Boot script implementing the fetch → index → marker sequence above, idempotent and safe to
re-run. Fetch failures must fail loudly at startup rather than yielding a site that
renders empty benchmarks.

*Acceptance:* a cold deploy reaches a serving state unattended; measured cold-start time
recorded in the project log; a second boot skips the fetch.

### 3.8 Operations
Structured logging, uptime check, error tracking, and a documented runbook for deploy,
data refresh, allowlist change, and rollback. Decide and disclose any analytics.

*Acceptance:* runbook in `docs/` that someone other than the author can follow.

### 3.9 Public-facing polish
Landing page explaining what the site is, linking to the paper, repo, and PyPI package;
citation guidance; a **"data as of" stamp** showing the pinned Hub revision, snapshot
date, and toolkit version, so a link opened months later is interpretable; a clear
statement that results are pre-computed, not live; and a visible sign-in affordance that
explains the judge tier exists without implying everyone can use it.

*Acceptance:* a first-time visitor can tell what they are looking at, how current it is,
and how to cite it.

### 3.10 SQLite-backed execution
Copy the SQLite database files onto the volume during provisioning. Change
`run_sqlite_query()` to open read-only (`sqlite3.connect(f"file:{path}?mode=ro",
uri=True)`) — worth doing unconditionally, local included. Gate execution at the `judge`
tier.

*Acceptance:* an allowlisted user re-runs a modified statement against all four SQLite
benchmarks; a `public` user gets 403; a write statement fails at the SQLite layer, not in
application logic.

### 3.11 PostgreSQL service
A pinned `postgres` Compose service on an internal-only network, loaded once from the
upstream BIRD dump via idempotent init, with a `SELECT`-only role and
`default_transaction_read_only = on`. Recall that the Postgres path ignores `db_id` and
uses a single `search_path` schema, so this is one database, not eleven.

*Acceptance:* `bird_mini_dev_postgres` is queryable at the `judge` tier; the app's role
cannot write; statement timeout enforced server-side; a rebuild reproduces the database
from the dump.

### 3.12 MySQL service (Beaver)
A pinned `mysql` Compose service, internal-only, with a `SELECT`-only grant. Beaver needs
**six databases on one server** because `db_id` is substituted into the connection string.
Build the service, role, and execution path now; **the load step lands when the maintainer
supplies the Beaver dump and load procedure**, which is the only outstanding dependency.

*Acceptance:* once loaded, `beaver` is queryable at the `judge` tier across all six
databases; the app's grant is `SELECT`-only; until then the service is present and the
benchmark reports a clear "database not provisioned" state rather than an opaque error.

## Risks

- **Credential leakage.** Highest severity. Mitigated in depth: no DB credentials on the
  host at all, the watsonx key reachable only through the judge path, tier enforcement
  centralized, and secrets in the platform store.
- **Runaway LLM spend.** A personal key behind a public site is an open tap without item
  3.4. The $50 ceiling and the kill switch are not optional, and must land in the same
  change as 3.3 rather than after it. The residual risk is rate-table drift: if the
  configured per-token rates understate actual watsonx pricing, the meter will permit
  more than $50 of real spend — hence the week-one calibration and an independent budget
  alert on the watsonx account itself.
- **Allowlist drift.** Env-var allowlists get stale and are easy to fat-finger. Log
  allowlist size at startup and make `/api/me` show the resolved tier so misconfiguration
  is visible immediately rather than as a confusing 403.
- **Stale public data.** Without the "as of" stamp, a shared link showing old numbers is
  actively misleading. Hence 3.9.
- **Cost creep.** Fixed at ~$4–5/month for the VM; the real variable is the $50 judge
  budget. Disk is the constraint to watch — 40 GB holds results, indices, and SQLite
  databases, but measure before assuming Beaver's MySQL data also fits.
- **Self-managed host maintenance.** OS patching and Compose upkeep are now yours.
  Bounded by unattended upgrades, pinned digests, and databases with no public listener —
  but it is a real recurring obligation that a managed platform would have absorbed.
- **Deployment drift.** The committed `dashboard/dist` makes it easy to ship a build that
  does not match source. Building in the image (3.6) closes this.
