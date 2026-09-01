# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.5.0] - 2026-08-30

### Changed — breaking

- **`langgraph` and `langchain-core` are no longer dependencies.** Nothing in
  the package imported them: `AgenticSQLGenerationPipeline` names LangGraph in
  its docstring but runs a hand-written state machine, and the only
  `from langgraph...` line in the tree has been commented out since the module
  was written. They pulled thirteen further packages into every install,
  including six that carried open security advisories.

  **Upgrading:** nothing changes for callers of this package. If your own code
  imported `langgraph` and relied on getting it transitively, declare it.
- **`requirements.txt` is removed.** It was a generated export of `uv.lock`,
  and a second manifest that security scanning read as a separate project — so
  every advisory was reported twice. `uv.lock` and `pyproject.toml` are the
  dependency sources of truth. Run
  `uv export --format requirements-txt --no-hashes --no-dev --no-emit-project`
  for a pinned file.

### Changed

- **FastAPI's interactive documentation pages are off, and its schema moved to
  `/api/openapi.json`.** `/docs` is the dashboard's documentation view now, and
  Swagger UI held that path. Moving it surfaced the reason not to keep it: both
  it and ReDoc load their assets from a CDN that this app's own
  `script-src 'self'` blocks, so both have rendered blank since the CSP was
  added. The schema needs no CDN and is the half that was ever usable.

### Security

- **Every package in the base install with an open advisory is floored at its
  patched version**, and the lockfile moved to match: `cryptography` 46 → 50
  (it encrypts stored per-user provider keys), `sqlparse` 0.5.5 → 0.6.0,
  `requests`, `urllib3`, `idna`, `pillow`, `pyasn1`, `python-dotenv` and
  `setuptools`. Nine of these are named in `pyproject.toml` despite not being
  imported directly: a lockfile protects this repository and the container, but
  `pip install text2sql-eval-toolkit` resolves fresh, and the intermediate
  packages' own floors are years behind.
- **The dashboard's HTTP stack moved off Starlette 0.x** (FastAPI ≥ 0.141.1,
  Starlette ≥ 1.3.1), which fixes Host-header poisoning of `request.url.path`
  and `request.form()` size limits being silently ignored — both of which
  matter for a deployment that faces the internet.
- **The `notebook` extra resolves a clean Jupyter stack.** The one *critical*
  advisory outstanding at 1.4.0 was a stored XSS in Jupyter Server's nbconvert
  handlers, reachable only through this extra; the metapackage's own constraints
  were wide enough to resolve onto it, so the floors are named here.
- **`vite`, `postcss`, `nanoid` and `immutable` updated** in the dashboard
  lockfile; `npm audit` reports no vulnerabilities.
- **Dependabot version updates are on** for `uv.lock`, `dashboard/` and the
  workflow actions, grouped so the pull requests are readable. Security alerts
  were already enabled; what was missing is the half that opens a pull request
  rather than waiting for someone to notice.

### Added

- **A docs view in the dashboard.** `/docs` is an index of tiles — the
  published API reference, which opens on Read the Docs, and the long-form
  notes now kept in `docs/notes/`: a survey of how text-to-SQL evaluation is
  done and where each metric misleads, a catalogue of the cases where two
  metrics disagree, and a tour of the dashboard itself — screenshots and links
  into the real views, so a first-time reader is shown what the tool does
  rather than told. `/docs/{name}` opens one, full width.
  Each note has its own address, so a link opens the one being discussed.
  Read-only, public on every deployment mode, and adding a note needs no code
  change: the title comes out of the file.

  The notes are not packaged. `docs/` ships in neither the wheel nor the sdist,
  and CI now checks that it stays that way — so a pip install gets the reference
  and an explanation of where the notes are, rather than a blank page. The
  deployment image copies them in.

  A note may reference screenshots from `docs/notes/assets/`, written as
  relative paths so the Markdown also renders on GitHub.

  Diagrams are scaled to fit the column rather than scrolling sideways, down to
  a floor below which they would stop being readable, and each carries a **View
  full size** control that opens it at its natural size over the page. The
  index is in reading order rather than alphabetical — the tour, then the
  worked examples, then the survey.

  Beyond ordinary Markdown the view renders **Mermaid diagrams**, **LaTeX** —
  inline as `\(x\)` and display as `\[ ... \]` — and gives wide tables their
  own horizontal scroller instead of squeezing them into the prose measure.
  Both renderers are fetched only by documents that use them, so a note of
  plain prose costs neither, and the entry bundle is unchanged.
- **A judge-config editor that is not painful.** Syntax highlighting, bracket
  matching, line numbers, a **Format** action that reflows the document, and an
  error marked at its line and column as you type instead of a "not valid"
  message on save. Save stays refused when `model.id` or `prompt_template` is
  missing — highlighting makes malformed input visible and says nothing about a
  config that parses cleanly and describes a useless judge.

  **The editor now edits YAML, which is what the file is.** It presented JSON,
  so `prompt_template` — the bulk of every config, and always multi-line — was
  one line of roughly fifteen hundred characters with `\n` escapes through it.
  Highlighting does not make that editable; a different notation does. The
  endpoint is unchanged and still takes JSON.
- **The GitHub Release is created by the tag.** Pushing `vX.Y.Z` now builds,
  publishes to PyPI and creates the Release page with that version's changelog
  notes and the built wheel and sdist attached. The page used to be written by
  hand afterwards, and was forgotten on both 1.3.0 and 1.4.0 — nothing failed
  when it was missed. A tag whose version has no `CHANGELOG.md` section fails
  the workflow *before* anything is published, rather than producing a release
  with an empty page.

### Changed

- **The benchmark moved out of the analysis views' paths and into their query.**
  `/insights?benchmark=bird_mini_dev_sqlite`, and likewise for `/compare` and
  `/errors`. `/benchmark/{id}` is the summary *of* a benchmark and keeps the id
  in its path; the other four take a benchmark as an input, and one takes
  several, so it belongs in a parameter. Each of those views now carries a
  benchmark dropdown at the top — Pipeline Compare and Error Analysis had no
  way to change benchmark at all — and with none chosen shows that dropdown
  over an empty page rather than a grid of tiles. The older path forms still
  resolve.
- **Profile Compare's address names every benchmark it is pooling.**
  `/compare/profile?benchmarks=bird_mini_dev_postgres,beaver`. It named a
  single one in a path segment, so adding a second changed the address to
  whichever was chosen last and a shared link reopened the wrong view. Removing
  one and *Reset to one* keep it in step as well.
- **A benchmark's five views share a tab strip.** Summary, Metric Insights,
  Pipeline Compare, Profile Compare and Error Analysis, across the top of all
  five, with the current one marked. The summary offered the other four as
  ghost buttons in its header — one-way, not links, and sharing a row with a
  form control — so moving between two of them meant going back to the summary
  first.
- **An analysis view asks which benchmark, instead of guessing.** Opening
  Metric Insights, Pipeline Compare or Error Analysis without one redirected to
  whichever benchmark loaded first — in practice always `bird_mini_dev_sqlite`
  — so the reader was shown numbers for something they had not asked about.
  `/insights`, `/compare` and `/errors` are addresses in their own right now
  and show a benchmark picker; choosing one moves to the benchmark-scoped
  address, so the view you end up on can still be linked to. Profile Compare
  selects benchmarks itself, several at a time, so `/compare/profile` is its
  canonical address and names none.
- **The home page is the way in to everything.** Three bands of tiles under solid banners:
  benchmarks, the six analysis views (Metric Insights, Pipeline Compare,
  Profile Compare, Error Analysis, LLM Judge, Eval Playground), and the four
  documents. Each tile says what the view is for. The analysis views that need
  a benchmark say so when there is not one, rather than offering a link that
  cannot resolve. Administrative routes — Users, signing in and out — stay out
  of it and remain in the header and the navigation rail.
- **Adding and editing a benchmark live on the Benchmarks page.** The home page
  is where you pick one; the Benchmarks page shows the same tiles plus the
  controls to add and edit. It was a table before, and both controls were on
  the home page.

### Fixed

- **Storing a per-user API key no longer fails on an existing deployment.**
  `ciphertext2` was added to the key table in 1.4.0 for watsonx's project id,
  but `CREATE TABLE IF NOT EXISTS` creates nothing when the table already
  exists — so a deployment whose table predated the column never received it,
  and since every INSERT names that column, storing a key for *any* provider
  answered HTTP 500. The store now adds missing columns on open. Existing rows
  are untouched.
- **The Eval Playground opens the record its address names.** A link to
  `/run/{benchmark}/record/1480` loaded a different record and rewrote itself
  to say so. On mount the view reported "nothing open", which erased the record
  from the address before the view had read it; the record then arrived as
  null, a default was loaded, and the address was rewritten to name that. It
  had been true of every playground record link.
- **Development history no longer leaks into the interface.** The Benchmarks
  page explained that it "was a slide-out panel, which meant it had no address
  of its own" — a changelog entry rendered as product copy. Every page's prose
  was read through for others; that was the only one.
- **A document's tables, diagrams and screenshots no longer overhang its
  prose.** They were given the full article width while paragraphs kept a
  narrower measure, so on a wide window everything wide stuck out by some 400
  pixels past the text above it. One column now, a little wider than prose
  alone would want; a table still scrolls inside its own block and a diagram
  still has **View full size** when it needs more room.
- **The dashboard's welcome text pointed at a control that is not there.** It
  told the reader to use a *Benchmarks* button in the top-right corner; the
  navigation moved to a menu at the top left, and the sentence did not.
- **Saving a judge config no longer mangles its formatting.** `yaml.safe_dump`
  renders a long multi-line string as a single-quoted folded scalar — every
  line break becomes a blank line and the prose is rewrapped at 80 columns — so
  every save through the dashboard turned a file that opened with
  `prompt_template: |` into one that did not. The value always round-tripped
  correctly; the file was simply unreadable afterwards.

## [1.4.0] - 2026-08-30

### Changed — breaking

- **`TEXT2SQL_JUDGE_ALLOWLIST` is removed.** Roles now live in a database an
  administrator edits from the dashboard, so changing who may reach the judge no
  longer needs an edit to `deploy/.env` and a container recreate.

  **Upgrading:** set `TEXT2SQL_ADMIN_EMAILS` to one or more verified addresses
  before restarting. A shared deployment refuses to start without it, because
  nobody could grant a role and there would be no other way in. Existing judge
  users must then be granted the `judge` role from the dashboard — the old
  variable is ignored, and startup warns while it is still set.

### Added

- **Per-user API keys.** A signed-in user may store their own provider key, so
  requests they run bill their account rather than the server's. Encrypted with
  `TEXT2SQL_SECRET_KEY`, write-only (no endpoint returns a stored key), never
  logged, and optional in both directions. Admins can set per-user monthly
  spending caps, reserved before a call and reconciled after so concurrent
  evaluation cannot overshoot them.

  This means the deployment now holds per-user state, including other people's
  billable credentials. `docs/dashboard/capability-tiers.md` no longer claims
  otherwise.
- **One dispatch table for models.** Baseline inference, agentic inference and
  the LLM judge accept the same `provider:model` strings; the judge was
  previously watsonx-only. `litellm` is an optional extra for anything the
  built-in prefixes do not cover.
- **User management.** `admin`, `full`, `judge` and `read_only` roles, granted
  and revoked from the dashboard by an administrator. `TEXT2SQL_ADMIN_EMAILS`
  always holds admin and is read at every startup, so it is the recovery path if
  the role table is wrong.
- A grant above the deployment's mode is recorded and shown as **inactive** with
  the reason, rather than looking effective and being refused.
- **Documentation site.** A written guide — installation, the five stages, the
  data model, benchmarks, models and providers, LLM-as-judge, the command line
  and configuration — alongside the generated API reference, which now covers
  all 43 exported symbols. Hosted on Read the Docs and linked from PyPI.
- **Judge Playground.** Run LLM-as-judge on the record open in the Eval
  Playground and see the verdict, with the config selectable. A verdict is
  cached against the record, pipeline, config name and a digest of the config's
  contents, so re-running an unchanged judge costs nothing.
- **Shareable judge verdicts.** The playground address carries `?judge=<config>`
  once a verdict is showing, and opening such a link restores it. Restoring
  reads the cache only and never starts an inference: sharing a result does not
  authorise the reader to spend against the budget, or against their own key.
- **Export a playground record** as Markdown or HTML, including both the stored
  judge explanation and any on-demand verdict.
- **The Eval Playground is addressable** by benchmark, record and pipeline, and
  every navigation item is a real link — so "open in new window" works.

### Fixed

- **The LLM judge could not run on any chat-API provider.** Unifying the
  dispatch tables routed the judge through a chat client, which rejects the bare
  string prompt the judge builds. Every run failed with "Incorrect prompt type".
- **The judge was told it was a SQL expert on every request.** The Anthropic
  client sent a SQL-generation system message unconditionally, including when
  judging — a contradiction the model had to resolve, and a quiet bias on
  verdicts. It is now sent only when the caller wants SQL.
- **The Anthropic client printed its whole request payload to stdout**, prompt
  and ground truth included, on every call.
- **Judge configs saved from the dashboard were written into the installed
  package**, which fails outright where the package tree is not writable by the
  server, and is discarded by a `pip install --upgrade` where it is not. They
  now go to `<data root>/llm_judge_config` and shadow the packaged config of the
  same name; deleting the copy restores the original. The editor can also create
  a config rather than only overwrite the selected one.
- **Both spellings of the watsonx variables are accepted** — `WATSONX_APIKEY` /
  `WATSONX_API_KEY`, `WATSONX_API_BASE` / `WATSONX_URL`, `WATSONX_PROJECTID` /
  `WATSONX_PROJECT_ID` — and a missing-credential error names every accepted
  spelling.
- **A sync driver in a database connection string is translated** rather than
  failing with "the asyncio extension requires an async driver".
- **The stored LLM-judge explanation now appears in exports.** It is rendered as
  prose rather than as a metric row, and the export only walked metric rows, so
  it had been absent from every export since exports existed.
- The dashboard no longer hides the session bar — and with it sign-out — on a
  remote deployment running in `full` mode.

### Security

- **`full` mode no longer grants full capability to anonymous callers.** Tier
  resolution short-circuited on the deployment mode before checking identity, so
  enabling `full` on a reachable host would have granted it to everyone.
  Anonymous callers now resolve to `public` regardless of mode.

## [1.3.0] - 2026-08-26

A dashboard release: every view is now addressable by URL, reads are served from
a derived index instead of by re-parsing artifacts, and the server can be run as
a read-only public deployment.

The library's public API is unchanged, the CLI is unchanged, and the on-disk
artifact format is unchanged — hence a minor bump rather than a major one. A
default `text2sql-eval-dashboard` on loopback keeps every capability it had,
which is enforced by a test rather than by intention.

### Packaging

- The wheel now contains the LLM-judge prompt configs. Every release to date
  shipped the judge code without them, so `load_llm_judge_config()` raised
  `FileNotFoundError` on any pip install.
- The wheel now contains the dashboard frontend, so `pip install
  "text2sql-eval-toolkit[dashboard]"` serves the UI instead of returning 404 at
  `/`. The Vite build is copied into the package at build time by `setup.py`.

### Added
- **Shareable URLs.** Every view has its own address — benchmark, pipeline
  detail, filtered error analysis, an individual record, and a record within a
  pipeline (`/benchmark/{id}/pipeline/{pipeline}/record/{record}`) — and
  reopening one restores the same view.
- **Short pipeline links.** `GET /api/benchmarks/{id}/pipeline-aliases` returns
  a derived alias per pipeline; the dashboard accepts an alias anywhere it
  accepts an id and expands it on arrival. A two-pipeline comparison link goes
  from 247 characters to 158. Aliases shorten links; they do not survive a model
  being renamed.
- **Query index.** A SQLite index built alongside each evaluation artifact
  (`text2sql-eval-toolkit index build` / `index status`). 1,915 MB of artifacts
  become 117 MB of indices; a record detail goes from 921 ms to 0.3 ms and peak
  memory from 2,151 MB to 170 MB.
- **Capability tiers** (`public` / `judge` / `full`), resolved per request from
  the deployment mode and the caller's identity and enforced centrally, so a new
  endpoint is safe by default. `full` is the default for a loopback bind.
- **Google sign-in** and a **scoped LLM-as-judge endpoint** for allowlisted
  users, metered against a monthly budget that persists across restarts.
- **Deployment artifacts**: container image, compose file with internal-only
  database networking, provisioning script, and an operations runbook
  (`docs/dashboard/deployment.md`).
- **CI**: lint, format, type check, tests across Python 3.11–3.13, frontend
  build and lint, per-module coverage floors, and end-to-end tests.
- **Tests**: 619 backend, 82 frontend, and 13 Playwright end-to-end tests that
  copy a link and reopen it in a fresh browser context.

### Changed
- Dashboard reads are served from the index rather than by parsing whole
  evaluation files per request.
- SQLite execution opens the database read-only with `ATTACH` disabled.
- Security headers, per-client rate limiting, and CORS narrowed outside `full`
  mode.
- `data/benchmarks.json` in the checkout is now canonical; the packaged copy is
  generated from it and CI fails on divergence.
- `requirements.txt` is generated from `uv.lock`.
- The version is resolved in one place from the installed package metadata,
  rather than being repeated in `pyproject.toml` and `__init__.py`.

### Fixed
- Ranking window functions (`RANK`, `DENSE_RANK`, `ROW_NUMBER`) were counted as
  aggregations under sqlglot ≥ 28, corrupting profiling categories.
- `compute_summary` aborted a whole benchmark's summary on one record whose
  metrics were incomplete.
- `report_tools` aborted a whole report on a metric stored as a bare number.
- A ground-truth SQL with no executed dataframe silently produced a record with
  no metrics and no error flag.
- `sqlite_run_execution_async` resolved database paths against the packaged
  registry, so the documented SQLite setup could not work from a checkout.
- Error-analysis reports counted a record the pipeline never answered but could
  not say which one, and inlined an entire record — dataframes included — into
  the error note.
- The dashboard's back button did nothing in error analysis; a link to a
  benchmark the server does not have silently opened a different one; and a cold
  load wrote a default filter into the address without applying it to the
  results.

## [1.2.0] - 2026-05-13

### Added
- `text2sql-eval-toolkit results fetch` command to download pre-computed
  evaluation results from the Hugging Face Hub
  (`text2sql-eval-toolkit/text2sql-eval-results`).
- `text2sql_eval_toolkit.results` public API: `fetch_results`,
  `list_available_results`, `clear_cache`, `DEFAULT_REPO_ID`,
  `DEFAULT_REVISION`.
- `text2sql-eval-toolkit results list` — print the available results
  manifest as a table.
- `text2sql-eval-toolkit results clear` — remove downloaded results.
- Dashboard detects missing results on startup and logs an actionable hint.
- Dashboard `--enable-fetch` flag exposes `/api/results/fetch` endpoints
  and an in-UI "Fetch results" button (off by default).
- `scripts/curation/upload_results_to_hub.py` — maintainer script for
  pushing new result snapshots to the HF Hub.

## [1.1.0] - 2026-03-23

### Added
- Expanded dashboard navigation with dedicated views for Metric Insights, Pipeline Compare, Error Analysis, LLM Judge config management, and Run Evaluation.
- New dashboard views: `ToolkitInsightsView` and `PipelineCompareView` for confusion-matrix-driven metric analysis and cross-pipeline disagreement exploration.
- Richer error-analysis and pipeline-detail UX with contextual deep links, side-panel record details, SQL/result inspection, and raw JSON record viewing.
- New API endpoints for insight workflows:
  - per-pipeline binary metric confusion
  - cross-pipeline binary metric confusion
- Toolkit-owned metrics module (`text2sql_eval_toolkit.metrics`) with public exports for SQL parsing/equivalence helpers, execution utilities, connectors, and cache helpers.
- New tests for numeric-normalized subset comparison behavior in SQL result matching.

### Changed
- Internal evaluation imports now use toolkit-native metrics utilities instead of `unitxt.text2sql_utils`.
- Summary scripts/docs now recommend `pip install -e .` setup for local toolkit usage.
- Dashboard benchmark and pipeline pages now include direct actions into insights/compare/error analysis workflows.

### Fixed
- Improved subset matching robustness for mixed numeric representations (for example `4` vs `4.0`) in non-empty subset execution comparisons.

### Removed
- Direct `unitxt` dependency from project dependency manifests.

## [1.0.0] - 2026-03-11

### Added
- Pip-installable `text2sql-eval-toolkit` library with packaged benchmark metadata.
- Curated top-level Python API for evaluation (`evaluate_prediction`, `evaluate_predictions`, `run_evaluation`).
- Execution orchestration helper (`run_execution`) and benchmark discovery utilities (`get_available_benchmarks`, `get_benchmarks_info`, `get_benchmark_info`).
- Public inference pipelines (`LLMSQLGenerationPipeline`, `AgenticSQLGenerationPipeline`) for reproducing baseline and agentic experiments.
- Re-exported low-level SQL comparison and parsing helpers (`compare_result_dfs`, `sql_exact_match`, etc.) from toolkit-owned metrics utilities.
- Library-focused README examples showing record-level, file-level, and benchmark-level usage.

[1.5.0]: https://github.com/IBM/text2sql-eval-toolkit/releases/tag/v1.5.0
[1.4.0]: https://github.com/IBM/text2sql-eval-toolkit/releases/tag/v1.4.0
[1.3.0]: https://github.com/IBM/text2sql-eval-toolkit/releases/tag/v1.3.0
[1.2.0]: https://github.com/IBM/text2sql-eval-toolkit/releases/tag/v1.2.0
[1.1.0]: https://github.com/IBM/text2sql-eval-toolkit/releases/tag/v1.1.0
[1.0.0]: https://github.com/IBM/text2sql-eval-toolkit/releases/tag/v1.0.0
