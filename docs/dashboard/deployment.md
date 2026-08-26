# Deployment Runbook

Operating the public dashboard. Companion to
[`plan/03-public-deployment.md`](../attic/plan/03-public-deployment.md), which explains
*why* the design is what it is; this file is what you follow when something
needs doing.

**Topology.** One VM running Docker Compose: `caddy`, `app`, and optionally
`postgres` and `mysql`. Only `caddy` publishes ports — both databases are
reachable solely over the internal Compose network.

---

## Capability modes

The mode is a **ceiling** set at startup. Signing in can never raise it.

| Mode | Who gets it | Can do |
|---|---|---|
| `public` | Anyone, signed in or not | Browse everything, read-only |
| `judge` | Signed in **and** on the allowlist | Adds on-demand LLM-as-judge |
| `full` | Local operator on loopback | Everything: SQL execution, evaluation runs, registry writes |

`full` refuses a non-loopback bind without `--allow-remote-full`. **Never use
that flag for a shared deployment** — it exposes SQL execution against whatever
database credentials the server holds.

Confirm the running mode at any time:

```bash
curl -s https://<domain>/api/me | jq '{tier, mode, can_mutate}'
```

---

## First deploy

1. **Provision the VM.** Docker Engine + Compose plugin, unattended security
   upgrades, ports 80/443 open, everything else closed. The default memory
   limits assume roughly 4 GB; see *Sizing and failure behaviour* below.

2. **Get a hostname.** A hostname is not optional in the way a *purchased*
   domain is. TLS needs one, and so does Google: OAuth redirect URIs must be
   `https`, and Google accepts `http` only for `localhost`. So without a
   hostname there is no sign-in, and therefore no judge tier — the site still
   works, read-only.

   Three ways, in order of preference:

   - **A domain you own.** Point an `A` record at the VM. Cleanest.
   - **A free DuckDNS subdomain** (`<name>.duckdns.org`). No purchase. It is on
     the [Public Suffix List](https://publicsuffix.org/), which matters more
     than it sounds: Let's Encrypt applies its "certificates per registered
     domain" limit per PSL entry, so your subdomain gets its own budget instead
     of sharing one with every other user of the service.
   - **Wildcard-IP DNS** (`<ip>.sslip.io`, `nip.io`). Zero setup and resolves
     correctly, but these are *not* on the Public Suffix List, so every user in
     the world shares one certificate rate-limit bucket. Issuance can fail for
     reasons that have nothing to do with you. Fine for a throwaway test, poor
     for something you want to stay up.

   Whichever you choose becomes `DASHBOARD_DOMAIN`, and its
   `https://<host>/api/auth/callback` must be registered as an authorised
   redirect URI on the Google OAuth client.

   **Browsing without any hostname** is possible: set `DASHBOARD_DOMAIN` to
   `:80` so Caddy serves plain HTTP on the IP, and leave `GOOGLE_CLIENT_ID`
   unset. Every read-only view works. Do not set `TEXT2SQL_COOKIE_SECURE=false`
   to force sign-in over plain HTTP — that sends the session cookie, which is
   the whole of the authorisation, in clear.

3. **Configure.**
   ```bash
   cp deploy/env.deploy.example deploy/.env
   ```
   Fill it in. Three entries are easy to get wrong:
   - `TEXT2SQL_RESULTS_REVISION` — **pin it**. Left unset, the toolkit falls
     back to `main`, and the public dataset would change under shared links.
   - `TEXT2SQL_SESSION_SECRET` — at least 32 characters, or startup refuses it.
     Generate with `python -c "import secrets; print(secrets.token_urlsafe(48))"`.
   - `TEXT2SQL_JUDGE_ALLOWLIST` — the addresses allowed to spend LLM budget.
     Everyone else is read-only, signed in or not.

   Leave `TEXT2SQL_TRUSTED_PROXIES` **empty**. It was once needed to name the
   Caddy container so `X-Forwarded-For` was believed; the app now trusts the
   proxy at the ASGI layer instead (`TEXT2SQL_FORWARDED_ALLOW_IPS`, already set
   by the compose file), so the client address is corrected before any handler
   sees it. Setting both means the second one re-reads a header that has already
   been applied.

4. **Register the Google OAuth client.** Authorised redirect URI must be exactly
   `https://<domain>/api/auth/callback`. Without `GOOGLE_CLIENT_ID` and
   `GOOGLE_CLIENT_SECRET` nobody can reach the judge tier — the site still works,
   read-only.

5. **Skip the databases unless you know you need them.** `docker compose up -d`
   without `--profile databases` starts only Caddy and the app. Every route
   that queries a database requires `full` tier, which a public deployment can
   never grant — so on this host those containers would be unreachable by
   anyone. Browse-only also means the box holds no database credentials, so a
   tier-enforcement bug has nothing to reach.

6. **Provision the data — before starting the app.**
   ```bash
   docker compose -f deploy/docker-compose.yml run --rm \
     --entrypoint text2sql-provision app
   ```
   `--entrypoint` is required: the image's entrypoint is the dashboard itself,
   so passing a script as an argument would append it to the dashboard's argv
   rather than run it.
   Fetches the pinned snapshot (~4 GB), builds the indices, verifies none are
   stale, and writes `/data/.provisioned`. It is idempotent.

   This runs first deliberately. Index building peaks on the largest single
   record — Beaver holds one of 108 MB whose parsed form costs several hundred
   more — and doing that while the app and both databases are serving is how a
   4 GB box runs out of memory. On a shared deployment the server will not build
   indices on demand at all; an unprovisioned benchmark returns 503.

7. **Start.**
   ```bash
   docker compose -f deploy/docker-compose.yml up -d
   ```

8. **Verify.** See [Health checks](#health-checks).

---

## Health checks

Run these after any deploy. Each corresponds to something that has actually gone
wrong at least once.

```bash
DOMAIN=https://<domain>

# Serving, and at the intended tier.
curl -s $DOMAIN/api/me | jq '{tier, mode, can_mutate}'
# expect: tier="public", can_mutate=false

# Privileged endpoints refused for anonymous callers.
curl -s -o /dev/null -w '%{http_code}\n' -X POST \
  -H 'Content-Type: application/json' -d '{"sql":"SELECT 1"}' \
  $DOMAIN/api/benchmarks/spider_dev/execute
# expect: 403

# Authorization is not skipped under a sub-path (this was a real bug).
curl -s -o /dev/null -w '%{http_code}\n' -X POST \
  $DOMAIN/dashboard/api/benchmarks/spider_dev/execute
# expect: 403 or 404 -- never 422 (which means the handler ran)

# Internal files are not served.
curl -s -o /dev/null -w '%{http_code}\n' $DOMAIN/api/static/judge/usage.sqlite
# expect: 403

# Sign-in builds an https redirect_uri. If the app cannot see that the request
# arrived over TLS it sends Google an http one, and Google refuses the whole
# flow with redirect_uri_mismatch -- so this fails before anyone can sign in.
curl -s -o /dev/null -w '%{redirect_url}\n' "$DOMAIN/api/auth/login"
# expect: an accounts.google.com URL whose redirect_uri= parameter is https://

# The data stamp matches the revision you pinned.
curl -s $DOMAIN/api/deployment | jq '{data_revision, data_provisioned_at}'

# Deep links survive a refresh (SPA fallback).
curl -s -o /dev/null -w '%{http_code}\n' $DOMAIN/benchmark/spider_dev/errors
# expect: 200

# Short links resolve: the alias table is what makes a shared /pipeline/<alias>
# link open the right pipeline, and it is empty if the summary file is missing.
curl -s $DOMAIN/api/benchmarks/spider_dev/pipeline-aliases | jq '.aliases | length'
# expect: the pipeline count for that benchmark, never 0
```

---

## Routine operations

### Publish a new results snapshot

1. Regenerate and upload from a checkout at the **release version**:
   ```bash
   python scripts/curation/upload_results_to_hub.py
   ```
   The manifest's `toolkit_version_compat` is derived from the toolkit version
   at upload time, so uploading from the wrong checkout produces a snapshot the
   deployment will refuse.
2. Tag it on the Hub (`vX.Y.Z`).
3. Update `TEXT2SQL_RESULTS_REVISION` in `deploy/.env`.
4. Re-provision and restart:
   ```bash
   rm /var/lib/docker/volumes/text2sql-dashboard_data/_data/.provisioned
   docker compose run --rm app deploy/provision.sh
   docker compose up -d --force-recreate app
   ```
5. Confirm the new stamp shows in `/api/deployment` and in the UI strip.

### Change the judge allowlist

Edit `TEXT2SQL_JUDGE_ALLOWLIST` in `deploy/.env`, then
`docker compose up -d --force-recreate app`. No rebuild. Startup logs the
allowlist **size** (never the addresses); an unexpected count means the variable
did not parse.

### Stop LLM-judge spending immediately

```bash
docker compose exec app sh -c 'echo "kill switch"'   # confirm you have a shell
# then set TEXT2SQL_JUDGE_DISABLED=true in deploy/.env and:
docker compose up -d --force-recreate app
```

The tier degrades to `public` and `/api/me` reports `can_run_judge=false`, so
the UI stops offering the action. Spend counters are untouched.

### Check judge spend

```bash
curl -s $DOMAIN/api/me | jq .judge_usage    # as an allowlisted user
```

Counters live in `/data/judge/usage.sqlite` and survive restarts. **Rates are an
estimate until calibrated.** Compare the reported spend against a real watsonx
invoice after the first month and set `TEXT2SQL_JUDGE_RATES` accordingly — an
understated rate lets the meter permit more than the intended budget. Keep an
independent budget alert on the watsonx account regardless.

### Deploy a code change

```bash
git pull
docker compose build app
docker compose up -d app
```
The image builds the frontend from source, so `dashboard/dist` in the repo is
irrelevant to what gets served.

**If the release changed the index schema, rebuild before serving.** A shared
deployment refuses to build an index on demand — that is provisioning's job — so
a server that starts against indices from an older schema answers 503 for every
benchmark until they are rebuilt:

```bash
docker compose run --rm app text2sql-eval-toolkit index build --data-root /data
docker compose run --rm app text2sql-eval-toolkit index status --data-root /data
```

`index status` naming anything `stale` means the rebuild did not finish. Budget
roughly ten seconds per gigabyte of artifacts; Beaver's 880 MB takes about nine.

### Roll back

```bash
docker compose down app
git checkout <previous-tag>
docker compose build app && docker compose up -d app
```
Data is untouched — it lives in the `data` volume, and the artifacts are
immutable within a snapshot.

---

## Databases

Only needed for the SQL-execution feature, which is not required for browsing.
Started with a profile so a browse-only deployment never runs them:

```bash
docker compose --profile databases up -d
```

Non-negotiables, all enforced in `deploy/sql/*-init/`:

- The app connects with a **`SELECT`-only role**. Postgres additionally gets
  `default_transaction_read_only`, a statement timeout, and revoked `CREATE`, so
  a write is refused by the database rather than by application logic — the
  execute endpoint runs arbitrary caller-supplied SQL and that guarantee does
  not belong in the app.
- **No published ports.** Reachable only over the Compose network.
- Execution stays gated at the `judge` tier, never `public`.

The init scripts run **once**, on first initialisation of the volume. Changing
`POSTGRES_READONLY_PASSWORD` afterwards means altering the role by hand or
recreating the volume.

**Beaver/MySQL is not loadable yet.** The repo has no dump or load procedure for
it — `data/benchmarks/dbs/README.md` points at the upstream project. The `mysql`
service, the read-only grant, and the execution path are in place; the data is
the outstanding dependency. The grant is scoped by database name prefix, so the
six Beaver databases are covered once created.

---

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| Benchmark returns 503 "not ready" | Index missing. Run `provision.sh`; shared deployments never build on demand. |
| `results fetch` fails with a version error | Snapshot's `toolkit_version_compat` excludes the installed version. Publish a snapshot from the matching checkout. |
| Everyone shares one rate-limit bucket | `TEXT2SQL_TRUSTED_PROXIES` unset, so `X-Forwarded-For` is ignored (deliberately — the header is forgeable from untrusted peers). |
| Sign-in loops or fails on callback | Redirect URI mismatch, or the session cookie was dropped. Check `TEXT2SQL_COOKIE_SECURE` and that the domain is served over TLS. |
| Google says `redirect_uri_mismatch` | The app is building an `http://` redirect because it cannot see that TLS terminated at the proxy. `TEXT2SQL_FORWARDED_ALLOW_IPS` must include the proxy's address; uvicorn believes `X-Forwarded-*` only from `127.0.0.1` by default, and the proxy is a different container. |
| Rate limiting throttles everyone at once | Same cause: without `TEXT2SQL_FORWARDED_ALLOW_IPS`, every request is attributed to the proxy, so all visitors share one bucket. |
| Sign-in rejected for a valid account | Google reports `email_verified=false`. Verify the address with Google; the allowlist deliberately does not match unverified addresses. |
| Startup fails: session secret | Shorter than 32 characters. Regenerate. |
| Startup fails: `--mode full` refuses to bind | Correct behaviour on a non-loopback interface. Use `--mode public` or `judge`. |

Logs:
```bash
docker compose logs -f app
docker compose logs caddy | grep -i error
```
Auth logs carry a truncated hash of the identity, never the address.

---

## Measured on the first deploy

A Hetzner CX22 (2 vCPU / 4 GB / 38 GB), browse-only:

| | |
|---|---|
| First image build | ~12 min (no cache; npm install, then ~90 Python packages) |
| Provisioning | ~2 min — 3.7 GB fetched, six indices built |
| Disk after provisioning | 8.3 GB of 38 GB |
| TLS issuance | seconds, `tls-alpn-01`, first attempt |
| Peak app memory | well inside the 1.5 GB limit, including Beaver's 108 MB record |

Rebuilds are cached and take well under a minute.

## Sizing and failure behaviour

Each container has a memory ceiling (`MEM_LIMIT_*` in `deploy/.env`, defaults
sized for a 4 GB VM). The point is containment: one runaway container is
OOM-killed and restarted by `restart: unless-stopped`, instead of the host
thrashing until sshd stops answering.

The app's limit is also what **provisioning** runs under, since it runs in that
service. Index building peaks on the largest single record — Beaver holds one of
108 MB whose parsed form costs several hundred more — so do not take
`MEM_LIMIT_APP` below about 1 GB, and provision before starting the databases.

`caddy` depends on `app` with `service_started`, not `service_healthy`. That is
deliberate: `depends_on` orders startup only, so the sole thing it decides is
whether a failing app can stop the edge coming up at all. It should not be able
to — a domain that resolves to nothing has no error page to read and no ACME
endpoint, which is how a certificate quietly fails to renew while you are
debugging something else. Caddy up with a 502 behind it is recoverable; Caddy
down is not.

So a broken app looks like **502 from a valid certificate**, not a dead domain.
Check `docker compose logs app`.

## HSTS

The default `Strict-Transport-Security` commits this host only. `HSTS_VALUE`
takes the whole header if you want more.

Add `; includeSubDomains` only if this domain has no other subdomains served by
anything else. Browsers cache it for the full `max-age`, so it applies to
subdomains that do not exist yet and cannot be walked back by changing the
header — a promise this deployment is in no position to make on a shared apex
domain.

## What is not covered yet

- **Backups.** Deliberately minimal: results and indices are rebuildable from
  the pinned snapshot, and the databases from upstream dumps. The one piece of
  genuinely unrecoverable state is `/data/judge/usage.sqlite` (the spend
  ledger). Back that single file up if spend history matters.
- **Multi-replica.** Rate-limit buckets and the index-build lock are per
  process, so more than one replica would need shared state.
- **Uptime monitoring and error tracking** are not wired up; pick a provider and
  point it at `/api/me`.
- **Analytics.** None is collected. If any is added, say so on the page.
