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
| `public` | Anonymous, or signed in with the `read_only` role (the default) | Every GET endpoint: browse benchmarks, summaries, error analysis, record detail — except a [benchmark that requires sign-in](#benchmarks-that-require-sign-in), which an anonymous caller is not shown |
| `judge` | Signed in and granted the `judge` role | `public`, plus on-demand LLM-as-judge on a single record, billed to the user's own stored provider key when they have one and to the server's otherwise |
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

## Benchmarks that require sign-in

A benchmark can be hidden from anyone who is not signed in. Beaver and its
10-question test subset are, from 1.6.0.

This is a check on **identity, not a tier**. An anonymous visitor and a signed-in
`read_only` user are the same `public` tier, so no tier setting can tell them
apart. Any signed-in caller passes, whatever their role — and anyone with a
verified Google account can sign in, so this keeps a benchmark out of public view
and out of search engines, not away from particular people. Limiting a benchmark
to named people would need a role check, which does not exist.

| Caller | Sees it |
|---|---|
| Anonymous, on any shared deployment | No |
| Signed in, any role | Yes |
| The local operator (`full` on loopback) | Yes — they control the process and have the files |

**Marking one.** Set `"requires_sign_in": true` on its registry entry, or list its
id in `TEXT2SQL_SIGN_IN_BENCHMARKS` (comma-separated). The flag is honoured in
*every* registry copy the server can see: the data root's `benchmarks.json` and
`test-benchmarks.json`, and the copies packaged with the toolkit. That is
deliberate. `provision.sh` seeds `benchmarks.json` into the data root once and
never overwrites it, so a deployment provisioned before a flag was added would
otherwise keep serving the benchmark to anyone. The other side of that rule is
that lifting a restriction means removing the flag from every copy, packaged ones
included. Editing a benchmark in the dashboard keeps its flag.

**What an anonymous caller gets.**

- `/api/benchmarks` leaves the benchmark out, so the home page does not show it.
- Every route that names it — summary, error analysis, record detail, playground,
  insights, compare, config, the judge, and its logo — answers `401` with
  `"sign_in_required": true`.
- A shared link to it shows a prompt to sign in, which returns to the same
  address. Not "not found", which would make a colleague's link look dead.

**How that is enforced.** In the same middleware as the tiers, before them. It
reads the *matched route's parameters*, not the URL text, so
`/api/compare?left_id=beaver` is refused and a search for the word "beaver" in
another benchmark is not. Which parameter names identify a benchmark is a table
in `ui/benchmark_access.py`, and a test fails on any route parameter missing from
it. Without that test, a new route taking a benchmark under a new name would slip
past every other test and serve the data. The refusal tests are parametrized over
the live route table, so a new route is covered without editing them.

Two spellings reach a benchmark's files without being its id, and both are
refused. On a case-insensitive filesystem, `BEAVER` opens Beaver's files, so ids
are compared casefolded. And an id is also a filename fragment, so
`charts/../beaver` reads Beaver's summary through a directory that exists. For a
caller the check applies to, an id outside `[A-Za-z0-9_-]` is therefore refused
with `400`.

**Crawlers.** Every response for an address that mentions such a benchmark
carries `X-Robots-Tag: noindex, nofollow`, whoever asks. That covers the API and
the app shell for `/benchmark/beaver`, `/run/beaver/…` and
`/errors?benchmark=beaver`: without the header, a crawler served a sign-in
prompt could still index the address and the page title. There is no
`robots.txt` entry. It is advisory, and it would publish the very paths it asks
crawlers to skip.

**What this does not cover.** The dashboard is not the only place the data is
published. The repository tracks Beaver's question files and a results report
that quotes questions and SQL, and the public Hugging Face results dataset carries
its results. Hiding the page hides the page.

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
