# Configuration

Everything is configured by environment variable. `env_loader.load_env()` runs
on import and searches upward from the working directory, then the checkout
root, then `~/.env`. **Existing environment variables are never overridden.**

`env.example` is the annotated template; copy it to `.env`.

## Data roots

Two variables, easily confused, deliberately distinct:

| Variable | Meaning |
|---|---|
| `TEXT2SQL_DATA_ROOT` | Where the dashboard and the benchmark registry are read from |
| `TEXT2SQL_EVAL_TOOLKIT_DATA_ROOT` | Where the library *writes* — predictions, evaluations, summaries |

Inputs may be read-only and shared; outputs must be writable. When
`TEXT2SQL_EVAL_TOOLKIT_DATA_ROOT` is unset the writable root falls back to the
nearest ancestor directory holding both `pyproject.toml` and `data/`, then to
`./data`.

## Model providers

See [Models and providers](models.md) for the full table. In short:
`WATSONX_APIKEY` / `WATSONX_API_BASE` / `WATSONX_PROJECTID` (each with an
alternative spelling), `ANTHROPIC_API_KEY`, `ANTHROPIC_WORKSPACE_ID`,
`OPENAI_API_KEY`, `GEMINI_API_KEY`, `VLLM_API_BASE`, `OLLAMA_BASE_URL`,
`RITS_API_KEY`.

## Databases

Connection strings are named **by the benchmark registry**, in each entry's
`db_engine.connection_string_env_var` — not fixed by the toolkit. The
conventional names are `POSTGRES_CONNECTION_STRING`,
`MYSQL_CONNECTION_STRING`, `DB2_CONNECTION_STRING` and
`PRESTO_CONNECTION_STRING`, but two benchmarks on different servers can and
should name different variables.

Execution uses SQLAlchemy's asyncio engine, so a connection string must name an
**async** driver — `postgresql+asyncpg://`, `mysql+aiomysql://`. A sync driver
in the string is translated where the toolkit can do so unambiguously, but
naming the async driver is clearer.

SQLite benchmarks read a local folder instead, given by `db_engine.db_folder`.

## Dashboard

| Variable | Effect |
|---|---|
| `TEXT2SQL_DASHBOARD_MODE` | Capability ceiling: `public`, `judge` or `full` |
| `TEXT2SQL_ADMIN_EMAILS` | Addresses granted admin every startup — the recovery path |
| `TEXT2SQL_SESSION_SECRET` | Signs session cookies |
| `TEXT2SQL_SECRET_KEY` | Master key encrypting stored per-user provider keys |
| `TEXT2SQL_COOKIE_SECURE` | Force the `Secure` flag on cookies |
| `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET` | Google OIDC sign-in |
| `TEXT2SQL_FORWARDED_ALLOW_IPS`, `TEXT2SQL_TRUSTED_PROXIES` | Which proxies' forwarding headers to believe |
| `TEXT2SQL_RATE_LIMIT_RPS`, `TEXT2SQL_RATE_LIMIT_BURST` | Request rate limiting |
| `TEXT2SQL_AUTH_RATE_LIMIT_RPS`, `TEXT2SQL_AUTH_RATE_LIMIT_BURST` | A tighter bucket for sign-in |
| `TEXT2SQL_JUDGE_DISABLED` | Kill switch for LLM-as-judge |
| `TEXT2SQL_JUDGE_MONTHLY_BUDGET_USD` | Monthly ceiling on judge spend |
| `TEXT2SQL_JUDGE_RATES` | Per-model token rates used to estimate spend |
| `TEXT2SQL_LOG_FILE` | Write logs to a file as well as stderr |

!!! note "`TEXT2SQL_JUDGE_ALLOWLIST` was removed in 1.4.0"
    Roles now live in a database and are granted from the Users console.
    `TEXT2SQL_ADMIN_EMAILS` is read every startup and always wins over the
    table, which makes it the standing recovery path rather than a one-time
    seed — and, since the allowlist is gone, the only one.

Deployment specifics, including TLS and the compose file, are in
[Deployment](../dashboard/deployment.md).
