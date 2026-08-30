# Capability tiers

The dashboard began as a local operator tool for a trusted user on `127.0.0.1`:
it will execute caller-supplied SQL against configured database credentials,
spend LLM budget, rewrite the benchmark registry, and overwrite YAML inside its
own installed package. None of that can go on the public internet.

Rather than adding auth checks to individual handlers, capability is resolved
once per request from **(deployment mode, request identity)** and enforced
centrally in middleware.

| Tier | Who | Can do |
|---|---|---|
| `public` | Anonymous, or signed in with the `read_only` role (the default) | Every GET endpoint: browse benchmarks, summaries, error analysis, record detail |
| `judge` | Signed in and granted the `judge` role | `public`, plus on-demand LLM-as-judge on a single record using the server-held watsonx key |
| `full` | Local operator, loopback only | Everything: SQL execution, evaluation runs, registry writes |

Two rules keep this honest:

- **Deny by default.** A mutating route with no declared tier requires `full`, so
  forgetting to classify a new endpoint fails closed.
- **Nothing is classified implicitly.** A test enumerates every mutating route and
  fails on any missing from the table.

## Roles

Tiers say what a deployment can grant. **Roles** say what a person is granted,
and they live in a small SQLite table an admin edits from the dashboard rather
than in an environment variable that needed a redeploy to change.

| Role | Asks for | Also |
|---|---|---|
| `read_only` | `public` | The default for anyone with no row |
| `judge` | `judge` | |
| `full` | `full` | Only active where the operator started with `--allow-remote-full` |
| `admin` | `full` | May grant and revoke roles |

Admin is deliberately **not** a tier. If it were, the mode ceiling would deny it
on a `judge` deployment — exactly where the console is needed. It is a separate
gate, so user management works whatever the ceiling is, while everything else
about that admin stays capped by it.

`TEXT2SQL_ADMIN_EMAILS` always grants admin, is read at every startup, and is
never overridden by a stored row. It is the way back into a deployment whose
table is wrong, and since 1.4.0 removed `TEXT2SQL_JUDGE_ALLOWLIST` it is the only
one — so a shared deployment refuses to start when it is empty.

## The mode is a ceiling

`TEXT2SQL_DASHBOARD_MODE` sets the highest tier a deployment can grant. Signing
in cannot raise it.

This has a consequence worth stating plainly, because it is easy to get wrong:
**a deployment in `public` mode grants `public` to everyone, whatever role
they hold.** The judge control simply never appears. To let a granted role take
effect, the mode must be at least `judge`. Startup warns when the ceiling makes
every role inert, and the console shows such a grant as inactive rather than
letting it look effective.

`full` refuses to bind a non-loopback interface unless `--allow-remote-full` is
passed, because it exposes SQL execution against whatever credentials the server
holds.

## Why the judge tier is safe without a database

`evaluate_sql_prediction_with_llm()` takes only the question, ground-truth SQL
and dataframe, predicted SQL and dataframe, and the generation prompt — all of
which are already inside the evaluation artifacts. It needs no database
connection at all.

So a public deployment can offer the judge tier while holding **no database
credentials of any kind**. Even a total failure of tier enforcement could not
reach a database, because there is nothing to reach. The judge tier and the
credential-free rule are compatible by construction, not by care.

It also means the `databases` compose profile has no role in a public
deployment: every route that queries a database requires `full`, which such a
host can never grant. Those containers would be unreachable by anyone. They are
for a private team deployment running `--allow-remote-full`, which is a
different thing.

## Identity

Google sign-in exists to decide which role a caller holds. The session itself
still carries nothing but a verified email address.

**This deployment does now hold per-user state**, and the documentation used to
say otherwise. Until 1.4.0 there was no user database and a public host held no
credentials at all, so even a total failure of tier enforcement reached nothing.
Two tables ended that: roles, and — where `TEXT2SQL_SECRET_KEY` is configured —
users' own provider API keys. The second is the one that matters: it is other
people's billable credentials, which is a different class of system from serving
pre-computed results. See [Per-user API keys](#per-user-api-keys).

Two details that are easy to get wrong and are enforced:

- **The `email` claim alone is not trusted.** Google also returns
  `email_verified`; an unverified address must never match a role, or roles mean
  nothing.
- **The session cookie is `SameSite=Lax`, not `Strict`.** The OAuth callback is a
  cross-site redirect back to the app, and `Strict` would withhold the cookie and
  break the `state` check.

Logs carry a hash of the address, never the address.

## Spend

The judge path is metered from reported token usage against a monthly ceiling
(`TEXT2SQL_JUDGE_MONTHLY_BUDGET_USD`, default 50) held in a SQLite ledger that
survives restarts — an in-memory counter would reset and the ceiling would not
bind. Verdicts are cached, so a repeated request costs nothing.

`TEXT2SQL_JUDGE_DISABLED=true` is the kill switch.

## Per-user API keys

A signed-in user may store their own provider key, so a request bills their
account rather than the server's. Optional in both directions: a deployment
without `TEXT2SQL_SECRET_KEY` stores none, and a user without one falls back to
the server credential.

The rule that keeps this coherent: **tier governs who may start a workload; the
key governs whose account pays.** Storing a key grants no capability.

What the design commits to, and what is tested:

- **Encrypted at rest, master key outside the database.** From
  `TEXT2SQL_SECRET_KEY`, so the SQLite file alone is worthless. Rotating it means
  users re-enter their keys, not a broken server.
- **Write-only.** No endpoint returns a stored key — not masked, not truncated,
  not the last four characters, not to the user who saved it. There is no handler
  that reads one, which a test enforces by reading the source.
- **Never logged.** Identities are hashed and a test greps the log for a canary.
- **No user-supplied base URL.** LiteLLM accepts one, and a caller-chosen
  endpoint would make the server an open outbound proxy. Custom endpoints belong
  in server configuration.
- **Per-user caps, set by an admin.** Reserved before the call and reconciled
  after, because evaluation runs sixteen coroutines against one semaphore and
  check-then-spend would overshoot a cap by up to fifteen calls.

A cap bounds spend **through this server only**. The key keeps working
everywhere else, and the UI says so rather than implying otherwise.
