# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **A benchmark can keep its details behind sign-in, and Beaver does.** Beaver's
  questions, SQL and schema are distributed under gated access, and only its
  overall scores may be published. An anonymous visitor to a shared deployment
  sees Beaver's tile and each pipeline's overall scores. Its breakdown by query
  category, error analysis, record detail, playground, insights and judge answer
  401 with `sign_in_required`, and the dashboard asks the reader to sign in and
  returns them to the same address. Any signed-in user sees the details,
  whatever their role, and the local operator tool is unaffected. The routes that
  stay open are an allowlist, so a route added later is refused until it is added
  on purpose, and a test fails on any route parameter not classified as naming a
  benchmark or not. Mark a benchmark with `"requires_sign_in": true`, or list it
  in `TEXT2SQL_SIGN_IN_BENCHMARKS`. The flag is honoured in every registry copy,
  the packaged ones included, because provisioning never overwrites a data root's
  registry.
- **Addresses that mention such a benchmark carry `X-Robots-Tag: noindex,
  nofollow`**, the dashboard's own pages as well as the API.
- **A release can be rehearsed end to end.** A `workflow_dispatch` of
  `release.yml` now runs the GitHub Release job as well, through the same
  `gh release create` step a tag uses, as a draft that the run deletes again. On
  1.5.0 that job was skipped on dispatch, so it first ran on the real tag and
  failed there, after PyPI had published. A rehearsal's TestPyPI upload skips a
  version already there, so it can be re-run. `CONTRIBUTING.md` says how to
  rehearse and what a rehearsal does not cover.
- **Archer has LLM-judge scores**, for 1,093 of its 1,144 predictions across all 11
  pipelines, so its Metric Insights page shows the judge comparison instead of
  *No evidence available*. Published in the `v1.6.0` results snapshot.

- **The chosen judge's config is shown in the playground.** Picking a config by
  name said nothing about what it would ask, and the only way to find out was to
  leave for the config editor and come back. The box names the model and shows
  the YAML behind a **Show prompt** toggle — collapsed by default, because the
  prompt template runs to forty-odd lines and would otherwise push the run
  controls and the verdict off the screen.
- **Open in Eval Playground**, from a record's detail panel in both Error
  Analysis and a pipeline's detail view. The panels show a record read-only;
  the playground is where the same record can be edited and re-run, and getting
  between them meant reading the record id off the address bar and assembling a
  `/run/...` URL by hand. The link carries the benchmark, the record and the
  pipeline you were looking at. It is an anchor rather than a button with a
  click handler, so the address can be copied and the playground opened in a new
  tab.

### Removed

- **Beaver's questions, SQL, schema and per-record results, from the repository,
  its history and the public results dataset.** They are distributed under gated
  access and should not have been published here. They are gone from every branch
  and tag on GitHub, whose history was rewritten on 2026-09-12, so every commit id
  changed and existing clones must be re-cloned. They are also gone from every
  revision of `text2sql-eval-toolkit/text2sql-eval-results`, whose tags were
  rebuilt; Beaver's overall summary and overall chart remain. The ten-question
  `beaver_test_10` subset went with them, and so did `deploy/load-beaver.sh` and
  the curation script that listed Beaver's tables. The MySQL read-only grant now
  takes its databases from `MYSQL_READONLY_DATABASES` instead of naming them.

### Fixed

- **The dashboard left SQLite connections open until it could open nothing.**
  Checking whether an index was stale opened it with `with sqlite3.connect(...)`,
  which commits or rolls back on exit but does not close — and a connection
  refers to itself through its statement cache, so it stayed open until the
  cyclic garbage collector ran. That check runs on every cached index lookup,
  six on each landing-page load, and the judge's spend ledger did the same on
  every `/api/me` from a signed-in caller. On the public deployment this reached
  Docker's default limit of 1024 open files: the landing page listed every
  benchmark with no pipelines, `/api/benchmarks` alternated between 200 and 500,
  and `/api/me` — the healthcheck — kept answering 200. Both now close what they
  open.
- **An index no longer keeps the connection of a worker thread that has
  exited.** It held every connection it opened so that closing the index could
  reach them all, and the server retires idle worker threads after a burst of
  requests.
- **The deployment's app container no longer runs with Docker's default
  open-file limit.** `deploy/docker-compose.yml` raises it to 65536, and CI fails
  a compose file that drops it.
- **The results upload script no longer publishes what it should not.** It sent
  everything under `results/` except logs, so run from a maintainer's checkout it
  would have published the query indices in `results/.index/`, which carry every
  record's raw bytes, along with backups and local copies of Beaver's gated
  per-record files. It now uploads an explicit list of files, and a benchmark whose
  details require sign-in contributes only its overall summary and overall chart.
  The manifest lists only what is uploaded.
- **No published summary lists a pipeline its own evaluation file does not
  contain.** The summaries for `bird_mini_dev_postgres`, `bird_mini_dev_sqlite`,
  `spider_dev` and `spider_realistic` listed a Gemini pipeline that none of those
  benchmarks' results contain.
- **"Judge again ignores the cache" had no space in it.** JSX drops a newline
  between an element and the text after it, so the sentence rendered as
  "Judge againignores the cache".
- **Evaluating with a different judge config no longer keeps the old judge's
  scores.** The batch judge reused any stored `llm_score`, whichever config had
  given it, so evaluating Llama-judged results with another config kept every
  Llama score and recorded the new config in the summary. Each verdict now
  carries `llm_judge_config_digest` and is reused only under the same config;
  verdicts stored before 1.6.0 carry none and are judged again.
  `rerun_metrics.py --preserve-llm-judge` still calls no judge for a stored
  verdict: it passes the new `llm_judge_reuse="any"`, and the verdicts it keeps
  keep their own digest. The dashboard's verdict cache keys on the same digest.
- **A prediction with several ground-truth queries is judged once.** The judge
  was asked about each query in turn and every answer but the last discarded;
  it is now asked once, about the query that decided the result, which is the
  answer that was kept.
- **The LLM judge was shown an agentic prediction without its schema or hints.**
  The judge's context for an agentic pipeline is the agent's trace, and every
  message in it was cut to 500 characters — including the first, which carries
  the task: a 3,600- to 11,400-character prompt of schema, hints and question
  reduced to its opening lines. The task the agent was given is now kept whole;
  later messages and responses are still cut, and the messages each step
  re-sends are shown once. Every batch verdict on an agentic prediction before
  this was reached without the schema or the hints.
- **A reasoning model that ran out of tokens no longer produces a verdict.**
  gpt-oss-120b reasons before it answers, and at the judge configs' old budget of
  512 tokens it often returned reasoning and no answer. The watsonx client fell
  back to the reasoning text, as it does when extracting SQL, so the judge read
  a verdict out of a fragment of thought — usually none, scored `N/A`, the same
  score as a rejection. Text generation now raises, naming the finish reason and
  saying to raise `max_new_tokens`; SQL generation still falls back.

### Changed

- **The packaged LLM judges use gpt-oss-120b, and there are two of them.**
  `llm_judge_default_config` compares a prediction with the ground truth and
  `llm_judge_no_gt` judges without it. They replace four configs built on Llama
  3.3 70B and Llama 4 Maverick, whose names still load: `llm_judge_alt_config`
  loads the default, and `llm_judge_no_gt_v1` and `llm_judge_no_gt_v2` load
  `llm_judge_no_gt`, unless a config of your own has that name. On 52 labelled
  execution mismatches drawn after the prompts were written, the new default
  accepts 2 of the 28 wrong predictions where the Llama config it replaces
  accepts 7, with a mean absolute error of 0.18 against 0.27; it rejects 3 of the
  14 correct ones against 2. Without the ground truth, gpt-oss-120b accepts 7 of
  the 28 where Llama accepted 15. The labelled set and the script that scores a
  config against it are in `data/judge_calibration/` and
  `scripts/analysis/judge_calibration.py`.

  **Published scores are unchanged.** Every `llm_score` in the published results
  still comes from the Llama 3.3 70B judge, and the batch judge reuses a stored
  score whichever config produced it: evaluating with the new default without
  `force_rerun_llm_judge` keeps the Llama scores while recording the new config
  in the summary.
- **The Eval Playground's question and database are legible.** They were set
  smaller and dimmer than the body copy around them, with the database run onto
  the end of the question's line, which made the subject of the whole view the
  hardest thing on it to find. The question now has its own tinted block at
  heading scale and the database is a labelled tag. Ground-truth and predicted
  SQL each get a box, so two bare text areas side by side no longer read as one
  undivided region.

## [1.5.0] - 2026-08-31

### Changed — breaking

- **`langgraph` and `langchain-core` are no longer dependencies.**
  `AgenticSQLGenerationPipeline` is a hand-written state machine and does not
  use them. Between them they brought thirteen further packages into every
  install, six of which carried open security advisories.

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

  **Duplicate and Rename.** A new config started from an empty editor, so
  writing a variant of an existing judge meant retyping or pasting a
  fifteen-hundred-character prompt — and a config named while experimenting
  kept that name for good. **Duplicate** keeps the open document and offers
  `<name>_copy` to save it under; **Rename** moves a config to a new name in
  place. A packaged config cannot be renamed — there is no file of yours to
  move — and the button says to duplicate it instead. Renaming onto a name
  already in use is refused rather than silently overwriting it.
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

- **Judge again.** A stored verdict could not be overridden. The cache key
  covers the config's contents, so an edited config re-judges by itself — but
  with the inputs identical there was no way to discard a verdict you did not
  trust, and a bad one was permanent. The button appears next to **Run judge**
  once there is a result, and the endpoint takes `refresh` alongside
  `cached_only` (setting both is a 400). A re-run costs an inference like any
  other, so the monthly ceiling applies.
- **A judge verdict is read through the formatting the model wrapped it in.**
  The prompt asks for a reply beginning "Yes", "No" or "Maybe", and the parser
  required those to be the literal first characters — so `**Yes**`, which is
  what Gemini 3 returns, scored `N/A`, which scores 0. A judge run against any
  model that writes Markdown marked every prediction wrong. Markdown emphasis,
  a heading, a block quote, a list bullet, surrounding quotes and a `Verdict:`
  label are now all read through. Only the start of the reply is examined, so
  an explanation that mentions "no" further down still cannot flip a verdict.
- **An unreadable judge reply is kept instead of being thrown away.** The
  `explanation` was replaced with `"N/A"` in exactly the case where somebody
  needs to read it, which made an unrecognised answer indistinguishable from a
  model that refused to answer. The reply is now returned verbatim whatever the
  verdict, and the server logs its first 200 characters.
- **An `N/A` verdict is no longer cached.** It records that nobody could read
  the reply, not a judgement — but it was stored, so the next run answered from
  the cache without calling the model and **Run judge** had no way to try
  again. The spend is still metered, because the tokens were still spent.
- **The CSP states `frame-src 'none'` rather than leaving it to the fallback.**
  With the directive omitted, CSP falls back to `default-src 'self'`, which
  still permits a same-origin frame — so "the dashboard frames nothing" was a
  claim in a comment that the policy did not actually enforce.
- **Renaming a config that does not exist is a 404.** It answered that the name
  "ships with the toolkit and cannot be renamed", inventing a config that was
  never there and sending the caller to duplicate it.
- **The navigation carries a query-form benchmark between analysis views.** The
  rail's links build their addresses from the benchmark you are looking at, and
  read the path segment only — so from `/insights?benchmark=x`, clicking *Error
  Analysis* dropped it and offered an empty picker rather than that benchmark's
  errors.
- **An unknown benchmark in the query is a not-found.** The guard read the
  benchmark from the path only, so `/errors?benchmark=missing` rendered the view
  and let it issue API calls that could only fail, instead of saying the server
  has no such benchmark.
- **The docs list no longer follows a symlinked document.** Fetching one was
  already a 404, but the listing reads every file to build its title and
  summary, so the target's first heading and opening paragraph came back through
  the list.
- **Starting a new judge config leaves rename mode.** Clicking **New config**
  with a rename in progress left both name fields on screen, hid the create
  action, and offered to rename a config that had just been deselected.
- **Renaming a judge config works on Windows.** The name is claimed with a
  placeholder before the move, and Windows' `rename()` refuses an existing
  destination — so every rename would have been a 500 there. It is `replace()`,
  which is defined to overwrite on every platform.
- **A short pipeline link works on the query-based analysis addresses.** The
  alias table was fetched with the benchmark from the *path*, so
  `/errors?benchmark=x&pipeline=<alias>` had nothing to look the alias up in:
  the table came back empty, every alias read as unknown, and the link died as
  a not-found. The address moved to a query parameter this release; the lookup
  had not moved with it.
- **A forced re-judge that cannot be read clears the verdict it replaces.** An
  `N/A` is not stored, so **Judge again** followed by an unreadable reply
  answered `N/A` while the cache still held the verdict you had just asked to
  discard — and the next request handed it back.
- **Renaming a judge config cannot be raced into deleting one.** The
  no-overwrite check and the move were separate, and POSIX `rename()` replaces
  its target silently, so two renames onto one name could both pass the check
  and the second would destroy the first's config. The name is claimed with
  `O_CREAT|O_EXCL` before the move, so the filesystem decides the winner and
  the loser gets the same 409.
- **The docs view refuses a symlinked notes or assets directory.** Resolving
  follows symlinks, so `docs/notes -> elsewhere` would have made that directory
  the docs root and published it at the public tier. Containment is asserted on
  the directories, not only on the names served out of them.
- **The docs view serves only this project's documents.** The directory was
  resolved by walking up for the nearest `pyproject.toml` of *any* project, so a
  pip-installed dashboard started inside an unrelated checkout that happened to
  have a `docs/notes/` would publish that project's files — at the public tier,
  with no sign-in. The ancestor must now declare `text2sql-eval-toolkit`; one
  that does not is skipped rather than ending the walk, so a checkout nested
  inside another project still resolves.
- **The Eval Playground follows the address between two records.** The auto-load
  guard was keyed on the benchmark alone, so moving from one record to another
  within the same benchmark — browser Back, or any in-app link — returned early
  and left the first record on screen while the URL named the second.
- **Tab stays inside the full-size diagram dialog.** Focus moved in on open and
  back out on close, but Tab walked into the page behind an `aria-modal`
  dialog — the one thing `aria-modal` says will not happen.
- **`openai:` models no longer require `OPENAI_BASE_URL`.** The client demanded
  it, so an OpenAI key alone was not enough and a judge run answered "Missing
  OPENAI_BASE_URL environment variable". The same client serves Ollama and any
  OpenAI-compatible server, which genuinely need an address; OpenAI itself has
  exactly one, and it is now the default. Set the variable only to point
  elsewhere — an empty value means the same as unset.
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
