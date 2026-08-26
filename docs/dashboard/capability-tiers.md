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
| `public` | Anonymous, **or** signed in but not allowlisted | Every GET endpoint: browse benchmarks, summaries, error analysis, record detail |
| `judge` | Signed in **and** allowlisted | `public`, plus on-demand LLM-as-judge on a single record using the server-held watsonx key |
| `full` | Local operator, loopback only | Everything: SQL execution, evaluation runs, registry writes |

Two rules keep this honest:

- **Deny by default.** A mutating route with no declared tier requires `full`, so
  forgetting to classify a new endpoint fails closed.
- **Nothing is classified implicitly.** A test enumerates every mutating route and
  fails on any missing from the table.

## The mode is a ceiling

`TEXT2SQL_DASHBOARD_MODE` sets the highest tier a deployment can grant. Signing
in cannot raise it.

This has a consequence worth stating plainly, because it is easy to get wrong:
**a deployment in `public` mode grants `public` to everyone, allowlisted or
not.** The judge control simply never appears. To let allowlisted users reach the
judge tier, the mode must be `judge`. Startup warns when a non-empty allowlist
cannot grant anything.

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

Google sign-in exists for exactly one purpose: deciding whether a caller is on
the judge allowlist. Nothing else is per-user and no profile is stored — the
session holds a verified email address and nothing more, so signing out is
clearing a cookie and there is no user database to secure.

Two details that are easy to get wrong and are enforced:

- **The `email` claim alone is not trusted.** Google also returns
  `email_verified`; an unverified address must never match the allowlist, or the
  allowlist means nothing.
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
