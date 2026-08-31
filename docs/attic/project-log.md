# Project Log

A chronological record of the v2 rework: what changed, what it broke, and what that
cost. Newest entry first.

This is history, not reference. It explains *why* the code is the way it is; how it
works is documented in [`../dashboard/`](../dashboard/). Entries reference plan
documents and a status table that were removed once the work they tracked was
finished.

---

## 2026-08-31 — the survey arrives, and "renders properly" turns out to mean four things

The generated `state-of-the-art.md` is replaced by a real 667-line survey with
30 references. It uses three things the renderer did not have, and finding them
took a browser rather than a test suite.

**Markdown ate the maths before anything could render it.** The survey writes
inline maths as `\(q\)`, and `(` and `)` are escapable punctuation in
CommonMark -- so by the time the renderer sees them the delimiters are gone and
`\(q\)` is the literal text `(q)`. Nothing errors; the equation simply is not
there. It needs a tokenizer that runs before the escape rule.

**The sanitiser was removing the only thing that identified a diagram.** The
allow-list did not include `class`, so `<code class="language-mermaid">` came
through as an anonymous code block and there was no way to tell a diagram from
any other fence. `class` is now allowed: it cannot execute, and the worst a
document can do with it is apply a style the page already has.

**Then the diagrams drew as empty boxes.** Mermaid puts its labels in HTML
inside a `<foreignObject>`, and sanitising the result as SVG strips the HTML --
so ten diagrams rendered with every node correctly placed and entirely blank.
Two fixes work; `htmlLabels: false` is the one taken, because it means the SVG
is genuinely SVG and there is no HTML in it to have an opinion about. I first
attributed a tall gap in the timeline to that setting and wrote it in a comment;
rendering it both ways showed the timeline is identical either way, and the gap
is just Mermaid reserving height for its longest column. The comment was wrong
and is fixed. A confident explanation in a comment is worth about as much as
the check that was not run behind it.

**The "wide content escapes the prose measure" rule did nothing.** The measure
was on the article, and tables and figures were then given a larger
`max-width` -- which cannot work, because a child is not allowed to be wider
than its parent. It looked like it worked because the tables were legible
anyway. The measure now sits on the prose elements and the article is as wide
as its column, which is the arrangement that was intended all along. Checked at
1400, 1024 and 768 pixels: the page never scrolls sideways, and wide diagrams
scroll inside their own figure rather than shrinking -- one of the survey's
flowcharts is 2082 pixels wide naturally and had been rendering 32 pixels tall.

**None of this was visible from the tests, and the browser pane's screenshots
stopped working halfway through.** Playwright is already a dev dependency for
the e2e suite; driving it directly gave real screenshots and, more usefully, a
way to measure layout at several widths. Four of the five defects above were
found by looking at pixels, and the fifth by measuring `scrollWidth`.

Two smaller things, both reported rather than found: the welcome text still
told the reader to use a *Benchmarks* button in the top-right corner, which
moved to a menu at the top left some releases ago; and **Docs** sat in the
nav among the analysis pages, where it read as another one of them. It has its
own section now, below a divider -- everything above it acts on the loaded
results, and this one is reading material.

The first attempt at that put it at the foot of the rail behind a full-height
spacer, which left a link stranded in the bottom-left corner under a screen of
nothing and read as a mistake rather than as a section. The divider alone says
what the spacer was for.

The same report pointed out that the rail scrolled away with the page, which
the survey makes obvious at 27,000 pixels long. It is sticky now, and the
sticky has to be on the `<aside>` itself: the `overflow: hidden` that the width
animation needs makes that element a scroll container, so a sticky descendant
would position against it and never move.

---

## 2026-08-31 — 1.5.0 deployed, and the iframe was not finished

The branch is live at `text2sql-eval-toolkit.oaklayer.dev`, built from the
checkout on the box as usual. Starlette 1.x and FastAPI 0.141 came up clean --
that was the one upgrade in item 1 with real risk in it, since the suite drives
the API through a test client rather than a socket, and it was fine. Every
health check in `docs/dashboard/deployment.md` passes unchanged, `/docs` serves
the dashboard rather than Swagger, and the container serves its three notes.

The habit of tagging the running image before rebuilding is worth keeping:
`docker image tag text2sql-dashboard-app text2sql-dashboard-app:rollback-1.4.0`
turns a rollback from a five-minute rebuild into a container restart. It was not
needed. It cost nothing.

**The embedded reference was blank for the first several seconds and I had
called that done.** Locally the docs site is warm and paints immediately;
through Cloudflare from the deployment it took long enough that the frame is a
blank white box with nothing to say it is working. In a demo that reads as
broken. Only deploying it showed this, which is the same lesson as 1.4.0's --
three defects there needed a real API call to see.

**Then the fix for it was wrong, twice, and deploying it again is what showed
that.**

The placeholder went *behind* the frame, on the reasoning that the docs site
would simply cover it and there would be no flash. An iframe paints its own
background, white, for `about:blank` as much as for the loaded page -- so a
placeholder behind one is a placeholder nobody sees.

And it was cleared by the wrong event. A fresh iframe fires `load` **twice**:
once for the `about:blank` the browser puts in it, and again when the real
document arrives. Instrumented against the deployment, 3 ms and 480 ms on a warm
cache. So the placeholder was removed almost immediately and the view showed the
same blank box as before -- now with code in it that looked like a fix.

There is no cross-origin way to ask a frame what it is showing. There is a
same-origin way to ask whether it is still showing `about:blank`, which is the
only document in that sequence that *is* same-origin: once the frame navigates
away, reading its location throws, and the throw is the signal. That is
`lib/iframeLoad.ts`, and it has tests, because the reasoning is the kind that
looks obvious once written down and was not obvious before.

Worth naming plainly: the first attempt looked right, passed lint, types and 170
tests, and did nothing. What caught it was measuring the actual load events in
the actual browser against the actual deployment, rather than reasoning about
what iframes probably do.

## 2026-08-30 — 1.5.0, item 4: the editor, and the defect on the other side of it

The plan left one judgement inside this item and asked for it to be taken
deliberately: JSON or YAML in the editor. **YAML.** Opening the packaged config
settles it in about ten seconds — `prompt_template` is a block scalar running to
forty lines of prose, and it is most of the file. As JSON it is a single line of
roughly fifteen hundred characters with `\n` escapes through it. Syntax
highlighting does not make that editable. A different notation does, and it
happens to be the notation the file is already written in.

The endpoint is untouched: the editor converts, and nothing is lost by that
which was not lost already, since the server has always parsed and re-dumped.

**CodeMirror is assembled from its parts rather than from `basicSetup`,** which
pulls in autocompletion, search and folding for a forty-line config. Even so the
judge view's chunk is ~416 KB, which is the deal that keeps the entry bundle at
424 KB against its 460 KB budget. The CI budget step now also caps every
non-entry chunk at 520 KB — not to constrain this one, but so the next thing
that lands in the wrong place is visible rather than merely lazy.

**Three bugs in the position arithmetic, all found by tests rather than by
using it.** js-yaml reports an unterminated construct at the phantom position
one past the end of the document — "line 4" of a three-line file — so the
message named a line that does not exist and the marker had nothing to
underline. Clamping fixed the marker and left the message wrong, because the
two were computed separately; they are now derived from one value, which is the
only way they cannot disagree. Then the clamped offset could land on a newline,
putting the marker on the line break instead of on the text, and the underline
could come out zero-width, which draws nothing — on exactly the errors reported
at the end of a document, which are the ones hardest to find by eye. A
"show me where it is wrong" feature that shows nothing is worse than not having
it.

Separately, js-yaml v5 raises on an empty document where v4 returned undefined.
The editor is empty whenever nothing is selected and just after a delete, so
without a guard a red error sat on a box nobody had touched.

**The real find was on the write side, and only became visible because the read
side got better.** Saving a config through the dashboard wrote it with
`yaml.safe_dump`, which renders a long multi-line string as a *single-quoted
folded* scalar: every line break becomes a blank line and the prose is rewrapped
at 80 columns. So the editor showed `prompt_template: |`, the save wrote a
quoted blob, and the next load showed `|` again — the disagreement was invisible
until you opened the file on disk, which is what this item's whole complaint was
about, one step further along. The value round-tripped correctly the entire
time; the file was simply unreadable afterwards.

Fixed with a block-style representer on a `SafeDumper` *subclass*. Registering
it on `yaml.SafeDumper` would have been one line shorter and would have changed
every other `yaml.safe_dump` in the process, which is a strange way to fix a
formatting bug in one endpoint.

## 2026-08-30 — 1.5.0, item 3: the docs view, and what `/docs` was already for

The view is what the plan asked for: `/docs` embeds the published reference,
`/docs/{name}` opens a note from `docs/notes/`, the list is built from the files
so adding one is writing one. Three notes are written — the survey, the
catalogue of metric disagreements, and the demo script.

**The route was taken.** `/docs` is where FastAPI mounts Swagger UI, and a real
route beats the SPA fallback, so the new view was unreachable at exactly the
address the plan specified — while `/docs/state-of-the-art` worked perfectly,
because nothing else claimed *that*. A deep link working and the index not is a
confusing enough failure that it is worth writing down.

**Moving it found the second thing.** Swagger UI and ReDoc load their assets
from jsdelivr, and this app sets `script-src 'self'`. Both pages have therefore
rendered blank since the CSP was added — an interactive API browser that never
browsed anything, on every deployment, unnoticed. Serving it from a new path
would have kept a dead page alive, so it is off. `/api/openapi.json` stays,
needs no CDN, and is recorded in the route-table snapshot, which is where an
"interactive browser over every endpoint" should have been visible in the first
place rather than arriving as a framework default.

**The bundle decision, taken once for items 3 and 4 as the plan asked.**
Client-side rendering in the view's own lazy chunk. `marked` plus `dompurify`
come to 79 KB there and nothing in the entry bundle, which stands at 424 KB
against a 460 KB budget. Rendering server-side was the alternative and it is
worse twice over: it would add a Markdown dependency to the Python package in
the same release that spent an item removing dependency surface, and it would
mean shipping HTML over the wire and trusting it in the browser, which is a
weaker position than shipping Markdown and rendering it there.

**Sanitising our own files is not paranoia about today.** The notes are authored
and reviewed, so the HTML is trusted now. The tests are about the day a document
is generated, pasted or contributed — at which point a renderer that emits raw
HTML is a script-injection point, and the fix would have to be found rather than
already being there. `iframe`, `form` and `style` are stripped along with
scripts: a note should display text and nothing else.

**A cross-origin iframe cannot be restyled, so the docs site is themed instead.**
The plan said this and it is worth confirming: the same-origin policy is
absolute here. `mkdocs.yml` now carries Carbon's palette and IBM Plex, which
also fixes the appearance for people who go to Read the Docs directly — the
better half of the trade. Our own CSP was the only thing blocking the embed;
Read the Docs sends neither `X-Frame-Options` nor `frame-ancestors`.
`frame-src` names that one origin, never a wildcard, and `frame-ancestors
'none'` is untouched — the two are easy to conflate and there is now a test
asserting that adding the first did not relax the second.

**Two consequences of "the notes are not packaged" needed handling, not just
noting.** The plan verified `docs/` is absent from both distributions and
concluded no packaging change was needed. True, and it leaves the view empty
wherever the repository is not — including the deployment, which runs the
container image. So `deploy/Dockerfile` copies `docs/notes/` beside the
`pyproject.toml` it already has at `/app`, which is what the resolver walks up
to find, and the container smoke test asserts the endpoint actually serves
documents. Finding an empty docs view during the demo it was built for is the
failure this avoids. In the other direction, CI now checks `docs/` stays out of
both distributions: it was a decision, and a stray `package-data` entry would
reverse it silently.

## 2026-08-30 — 1.5.0, item 2: the release page, and where the check belongs

Small item, one decision in it. The plan said to order the Release job after
`publish`, and it is right about that: a Release page pointing at a version PyPI
rejected is worse than a missing one. But it also said the changelog extraction
should live in that job, and *that* would mean a tag with no changelog section
publishes to PyPI -- irreversibly -- and only then fails.

So the two halves are split. `build` extracts the notes and fails there, before
anything leaves the machine; the notes travel to `github-release` as an
artifact, so both jobs read the same bytes rather than re-deriving them. The
ordering the plan asked for is kept for the part that creates the page.

`scripts/ci/extract_changelog.py` has twelve tests, which is more than it looks
like it needs until you notice what they cover: the newest section running on
past the next heading, the oldest section swallowing the link-reference block at
the foot of the file, and a heading with nothing under it -- which produces an
empty page, the exact failure the item exists to prevent. One of them asserts
the *current* declared version has notes, so the repository cannot reach a tag
in the state 1.3.0 and 1.4.0 were in.

The changelog's link-reference definitions were missing for 1.2.0, 1.4.0 and
1.5.0, so those headings rendered as literal `[1.4.0]`. Added, now that the
URLs they point at will exist.

## 2026-08-30 — 1.5.0, item 1: the alerts were mostly bookkeeping, and one real finding

158 open Dependabot alerts, 28 distinct packages, three manifests. The count was
inflated three ways and the plan predicted two of them: the same package is
counted once per manifest it appears in, and one package often carries several
advisories. Deleting `requirements.txt` — a generated export of `uv.lock` that
scanning read as a separate project — removed about a third of the alerts
without changing a single dependency, exactly as the plan said it would.

**The third inflation was not predicted, and it is the finding worth keeping.**
The plan's "watch for" paragraph warned that `langgraph` and `langchain-core`
are "the agentic pipeline's spine" and should be bumped deliberately. They are
not the spine. They are not anything. `grep -rn "langgraph\|langchain" src/
scripts/ tests/` returns two lines, both of them commented-out imports, above a
comment reading "we're using a simpler state machine approach". The dependency
has been declared and unimported since the module was written.

Between them the two packages pulled thirteen more into every install —
`langgraph-checkpoint`, `langgraph-sdk`, `langgraph-prebuilt`, `langsmith`,
`orjson`, `ormsgpack`, `xxhash`, `zstandard` and the rest — and **six of the
fifteen base-install packages with an open advisory were in that subtree**.
Flooring them would have worked and would have been wrong: the correct fix for a
vulnerable dependency you do not use is to stop declaring it. Removed. The
docstring claiming a LangGraph agent, and `scripts/inference/README.md`'s
troubleshooting entry telling people to `pip install langgraph`, were corrected
at the same time — they had been describing a design that never shipped.

**Nine packages are now named in `pyproject.toml` that this project does not
import.** `pillow` via matplotlib, `urllib3` and `idna` via requests,
`cryptography` and `pyasn1` via google-auth. This looks wrong and is not: a
lockfile protects this repository and the container, and does nothing for
`pip install text2sql-eval-toolkit`, which resolves fresh against the
intermediate package's own floor — and those floors are years behind. Naming the
patched version is the only mechanism that reaches that install. They are floors
rather than pins, and the comment above them says when to delete each one.

**`uv lock --upgrade` was the wrong tool and it took one run to find out.** It
moved 60-odd packages including pandas 2.2 → 3.0, sqlglot 28 → 30, numpy 2.4 →
2.5 and starlette 0.52 → 1.6. None of those belonged in a security fix, and
pandas 3 under a project whose core data model is serialised dataframes is a
week of its own. Reverted, and replaced with: raise the floors, plain `uv lock`,
then `--upgrade-package` by name for the handful the floors could not reach.
22 packages moved instead of 60.

**Starlette 1.x was taken deliberately, and it is the one upgrade with risk in
it.** The 0.x advisories are Host-header poisoning of `request.url.path` and
`request.form()` limits being silently ignored; both are about a server facing
the internet, which this one is. It needed FastAPI ≥ 0.141.1 to come with it.
921 tests pass, but the suite exercises the API through the test client rather
than a socket, so this is the item to watch on the deployment.

The one *critical* alert — stored XSS in Jupyter Server's nbconvert handlers —
was reachable only through the `notebook` extra. The plan offered dropping the
extra or pinning it forward; a clean stack existed, so it is pinned forward and
the notebooks still install with one command.

## 2026-08-30 — 1.4.0 built. The features were the easy half.

All five planned items shipped and are running live at
`text2sql-eval-toolkit.oaklayer.dev`: documentation, test coverage, one dispatch
table for every model call site, a user-management console, and per-user
provider keys. The plan document has been deleted now the release is out, as the
attic's own rule says it should be; what survived it is below, and the standing
rule about the published surface moved to `CONTRIBUTING.md`, which is where
someone will actually meet it.

What is worth recording here is the other half of the branch, none of which was
in the plan, and almost all of which was found by deploying the thing and then
using it.

**The unification worked in the library and changed nothing for the deployment.**
`deploy/docker-compose.yml` passed only the watsonx variables into the container,
so a judge config naming `anthropic:` would resolve, build a client, and fail on
a credential the container had never been given. The dispatch table was correct
and the deployment could still only ever have used watsonx.

**Three defects appeared the first time a judge actually ran.** Routing the judge
through a chat client was new, and the test suite had never called a real
provider:

1. The judge's prompt is a bare string; the chat clients accept only a message
   list. Every run failed with "Incorrect prompt type" — introduced by the
   unification itself.
2. The Claude client printed its entire request payload to stdout, into the
   container logs, on every call.
3. The judge was being told to write SQL. `ClaudeClientChatAPI` sent "You are a
   SQL expert… convert natural language questions into accurate SQL queries" as
   the system message on *every* request, judging included — a contradiction the
   model has to resolve, and a quiet bias on verdicts.

None of the three is subtle. All three needed a real API call to see.

**`WATSONX_PROJECTID` has no underscore, and IBM's documentation mostly writes
`WATSONX_PROJECT_ID`.** Setting the sensible-looking one produced `Missing
WATSONX.AI credentials … WATSONX_PROJECTID` while a near-identically named
variable sat unread in the same file. Both spellings are now accepted for all
three watsonx variables, and the error names every accepted spelling — naming
only the one the reader has already set is how a five-minute fix becomes an hour.

**Judge configs were written into `site-packages`.** The write endpoint targeted
the installed package directory: root-owned on the deployment, with the server
running unprivileged, so every save was a bare 500. The location was wrong even
where the permissions allow it, since a pip upgrade discards the edit. Writes now
land in the data root and shadow the packaged config of the same name; deleting
the copy restores the original.

The same report surfaced a second defect behind the first: the editor could only
*overwrite* the selected config, never create one — so the attempt to add a
Claude judge was on course to overwrite the shipped default rather than fail.

**Sharing a verdict needed a new endpoint flag, not a query parameter.** The
playground address now carries `?judge=<config>` once a verdict is showing, and
opening such a link restores it — strictly cached-only, returning 204 when
nothing is stored. Opening a link someone sent must not start an inference: the
sender is sharing an answer, not authorising the reader to spend against the
budget, or against their own provider key. A consequence worth keeping: an
exhausted budget still serves a shared link rather than turning it into a 429.

The URL names the config rather than embedding the verdict, so a URL cannot be
edited into claiming a verdict the judge never gave.

**One export bug predates all of this.** `llm_explanation` from the published
evaluation is rendered as prose rather than as a metric row, and the export only
ever walked the metric rows — so it had been absent from every export since
exports existed.

**Two security findings.**

- `resolve_tier` returned `FULL` whenever the server was in full mode, *before*
  checking identity. Enabling full mode remotely would have granted it to
  anonymous internet users. Caught before that happened; the fix adds a `remote`
  flag, and anonymous callers on the full-mode host now resolve to `public`.
- `SessionBar` hid the whole strip on a remote-full deployment, leaving a
  signed-in user with no sign-out. The first fix was too broad and three existing
  tests caught it — the coverage item earning its place ahead of the features.

**What the ordering bought.** Documentation and tests were sequenced first on the
argument that they are what make the rest safe to attempt. That held twice in
ways that are easy to measure: writing docstrings surfaced five real defects
while describing what functions did, and the characterisation tests caught two
regressions in dashboard work that would otherwise have reached the deployment.

### The decisions, and what they cost

Taken 2026-08-26, before any of it was built. Each closed off an alternative
that looked attractive again mid-implementation, which is why they were written
down.

| Question | Decision |
|---|---|
| Full over the web | The console may grant it; it takes effect only where the operator started with `--allow-remote-full`. Inert grants are shown as inert. |
| Whose budget | Per-user caps set by an admin, alongside the global ceiling on the server-held key. |
| Key lifetime | Persist until explicitly deleted. |
| Legacy allowlist | `TEXT2SQL_JUDGE_ALLOWLIST` removed; the database is the only authority. |
| Admin bootstrap | `TEXT2SQL_ADMIN_EMAILS`: addresses, not domains, matched against the verified login email. Read every startup, always grants admin — a standing recovery path, not a one-time seed. |
| User-key scope | Any workload. Tier governs who may start one; the key only decides who pays. |
| Library stability | The pip-installable surface does not change because of dashboard work. Optional parameters defaulting to today's behaviour are the only permitted addition. |
| Where the quota lives | UI only. Library, CLI and notebook paths never touch it. |

Two of these carried consequences worth restating, because both came true:

- **Per-user caps are a quota subsystem, not a setting.** They needed per-model
  costs, which is why they were sequenced after the dispatch-table work. They
  were the largest single piece of the branch, as the plan predicted.
- **Removing the allowlist deleted the escape hatch.** `TEXT2SQL_ADMIN_EMAILS`
  is now the sole recovery path. It has not bitten, but it is one typo away from
  locking an operator out of their own console — a failure mode that used to
  have a shell-level answer and no longer does.

The plan also predicted that documentation and tests first would make the rest
safe to attempt, and asked to be judged on it. Writing docstrings surfaced five
real defects; the characterisation tests caught two dashboard regressions before
they reached the deployment. Neither would have been caught by the features'
own tests, because both were regressions in code nobody was touching.

**Still outstanding at the point the PR opened.** OpenAI, Gemini and the
`litellm:` prefix share the dispatch table and are covered by tests, but nothing
has called them against a real provider — only watsonx and Anthropic have been
exercised end to end. And no packaged judge config names a non-watsonx model, so
Claude is not selectable from the Judge Playground without creating a config
first.

And one that would ship a visible defect: the documentation site is not
published. GitHub Pages turned out to be unavailable on this organisation, so
the Pages workflow is gone and `[project.urls]` points at Read the Docs
instead — configured and verified against a clean build, but the RTD project
does not exist yet, so the URL still 404s. Releasing before it does puts a dead
link in PyPI's sidebar, on the page that is the project's front door.

Pointing PyPI at the repository instead is not the escape it looks like:
`docs/reference/*.md` are mkdocstrings directives, so GitHub renders the API
reference as literal `::: text2sql_eval_toolkit.foo` lines. The guide would read
fine and the reference would be unusable.

---

## 2026-08-26 — Deployed. Everything that had never run, ran.

Live on a Hetzner CX22 — browse-only,
`judge` mode, pinned to the `v1.1.0` results snapshot.

**What worked on the first attempt**, all of it previously unexercised:

- **TLS.** Let's Encrypt issued via `tls-alpn-01` within seconds of `up -d`. CI structurally
  cannot rehearse this — it has no domain to be challenged on.
- **Provisioning end to end.** 3.7 GB fetched, six indices built, all reported `current`, no
  OOM inside the 1.5 GB limit — including Beaver's 108 MB record, the part I was least sure
  of. Total ~14 minutes, most of it the first image build.
- **The proxy-header fix.** `redirect_uri` came back `https://`. Without that change Google
  would have refused every sign-in, and it would have looked like an OAuth configuration
  problem rather than a scheme problem.
- **Authorization.** Anonymous `/execute` 403; the sub-path bypass probe never reaches a
  handler; `judge/usage.sqlite` 403; deep links survive refresh. Sign-in confirmed by the
  user on the live site.

**Three bugs the deployment found, and they share one cause.** `results fetch` downloads
`results/**` and nothing else, so *anything else the app reads from the data root has to
arrive some other way*. Three places assumed a source checkout's layout:

1. **Compose would not render at all.** `:?` on the database superuser passwords applied
   even to a browse-only deployment that never starts those services, because compose
   interpolates the whole file before filtering by profile. It asked for passwords for
   containers it will never run. I had flagged exactly this friction when adding the
   read-only passwords and chose to match the existing pattern instead of changing it —
   the pattern was the bug, and only deploying proved it.
2. **Every benchmark showed "0 records"** while its pipeline count was right. The listing
   counted records from `benchmarks/*.json`, which no deployment has. It now asks the
   index, which is the better answer regardless: it counts what a visitor can actually
   browse, it is already built, and it is one query.
3. **Every tile rendered blank.** The default logos are tracked in the repository under
   `data/benchmarks/logos/`, but the snapshot does not carry them and the image copied no
   `data/` — so not even the `generic.png` fallback existed. The image now ships them and
   provisioning seeds any the data root lacks, before the already-provisioned early exit,
   because an existing data root has exactly this problem.

All three are now asserted in CI, which is the only reason to think there is not a fourth.

**One operational note.** `caddy` depends on `app` with `service_started` rather than
`service_healthy`, so the certificate was obtained while the app was still coming up. That
was the intended consequence of the change: the edge must not be hostage to app health, or
a broken app takes the ACME endpoint down with it.

---

## 2026-08-26 — A record detail is a page now, not a panel with no address

Reported directly: clicking a row in a pipeline detail view opened a panel over a view
whose URL had not changed, so the one thing a reader most wants to send — *look at this
record for this pipeline* — could not be shared at all.

    /benchmark/{id}/pipeline/{pipeline}/record/{record}

A path segment rather than a query parameter, unlike error analysis: there are no filters
here for it to sit beside, and an address that reads as a page is what makes it obviously
shareable. The record comes from the URL and the view reports changes upward — the same
shape the error-analysis position already uses — so opening and closing push history
entries and back closes the record rather than leaving the pipeline.

Three end-to-end tests, of which the middle one is the actual claim and could not have
been written before: the address reopens the identical record in a fresh browser context.

**Three of my own mistakes, since each cost time and two produced false confidence.** The
route-builder edit silently did nothing — it targeted a template literal that an earlier
`/b/` → `/benchmark/` sweep had turned into plain text, and I had not asserted on that
particular replacement. The first tests clicked `table tbody tr` first, which on that page
is the metrics summary rather than a record row. And the first live verification ran
against a stale `localhost:8010` tab and reported success for the wrong site; I caught it
only because the benchmark id in the output did not match what I had asked for. That last
one nearly became "it works in production" on the strength of a local tab.

607 backend, 82 frontend and 13 end-to-end tests passing.

---

## 2026-08-26 — Pushed, and CI immediately found what "never run" was hiding

The branch is on GitHub with [PR #12](https://github.com/IBM/text2sql-eval-toolkit/pull/12)
open as a draft. Two corrections to what I had said before pushing.

**"CI is running for the first time" was wrong.** The workflow triggers on `push` to
`main` and on `pull_request`. Pushing a branch matched neither, so nothing ran — zero runs,
checked against the API rather than assumed. `workflow_dispatch` was no help either:
GitHub only offers it for workflows that exist on the default branch, and this one does
not yet. A PR was the only way, which is what opened it.

**Its first run failed two jobs, both real.**

*The container exited on startup.* `get_logger` derived a default log path as
`Path(__file__).parents[2] / "data/results/bak/log.txt"` — the repository root in a
checkout, the *interpreter's library directory* once pip-installed. So it tried to create
`data/results/bak` inside site-packages and raised during `import text2sql_eval_toolkit`,
because `get_logger` is called at module scope. **The published package could not be
imported anywhere site-packages is read-only** — every container, every system-wide
install — and where it was writable it littered the library directory instead.
Pre-existing on `main`; it survived because a source checkout is the one layout where that
path is right, and nothing had ever built the container.

*The advisory 3.14 job failed nine evaluation tests.* `sort_df` wrote canonicalised values
back with `.iloc[:, i] = ...`, asking pandas to coerce a string into an int64 column.
Pandas warned about that for years and now raises, so **every execution-match metric
returned `eval_error` instead of a score** on any current pandas. Verified the fix by
comparing old and new over 2,550 real result frames across four benchmarks — all
identical.

---

## 2026-08-26 — Sign-in verified for real, and three things that would have broken it

Item 3.2 had been "done (code), never exercised against real Google" since it was written.
Exercising it turned up three defects, two of which would have made sign-in impossible on
the deployment rather than merely awkward.

**The app would have sent Google an `http` redirect_uri.** uvicorn believes
`X-Forwarded-*` only from peers in its trust list, which defaults to `127.0.0.1`. Caddy
runs in its own container, so it is not that — meaning the app would have seen every
request as plain http despite TLS terminating at the edge. `url_for("auth_callback")`
builds the redirect from the request scheme, and Google refuses a mismatch outright. Sign-in
could not have completed once. The same setting keys the rate limiter, so the whole
internet would also have shared one bucket. Demonstrated by driving uvicorn's
`ProxyHeadersMiddleware` directly rather than reasoning about it, and now covered by a test
that also pins uvicorn's *default* behaviour so the extra configuration can be dropped if
that ever changes.

**The judge allowlist granted nothing in `public` mode.** The mode is a ceiling, so
`resolve_tier(PUBLIC, allowlisted_user)` returns `PUBLIC`. `env.deploy.example` says
`judge` correctly, but compose falls back to `public` when the line is absent — and the
failure is silent: the judge control simply never appears. Startup now warns when a
non-empty allowlist cannot grant anything.

**`/api/auth/login` returned 500 rather than saying what was wrong.** SessionMiddleware is
installed by `main()`, so serving the ASGI app directly with Google credentials set gives a
server that advertises sign-in and then raises deep inside Starlette. Both auth routes now
return 503 naming the cause and the fix. That is the **third** bug of the shape "works only
if you go through `main()`", after the deployment-mode one in the security review and the
proxy headers above. Worth treating as a pattern.

**And the test suite was only hermetic on a machine with no `.env`.** `env_loader` runs on
import, so importing `ui.server` pulls in the developer's real credentials.
`pyproject.toml` and `CLAUDE.md` both claimed otherwise. It passed in CI and passed here
until sign-in was configured for this very test, at which point a rate-limit test silently
began exercising a different branch. A conftest fixture now strips credential-shaped
variables for non-integration tests; verified by running the suite with them set.

**What the round trip actually proved.** An allowlisted user resolves to `judge`,
`can_run_judge` true, `can_mutate` false; `/execute`, `/playground/evaluate`, the registry
write and the judge-config write all return 403; the session cookie is HttpOnly; and the
log records `identity 2426d5148397` with the address appearing zero times in clear. That is
the design's central claim, checked against a real session rather than inferred.

---

## 2026-08-26 — Reviewing the deployment found that it could not have worked

None of `deploy/` had ever executed. Reviewing it before standing up a server, rather than
after, found three faults — two of them silent.

**The documented first step failed three ways at once.**
`docker compose run --rm app deploy/provision.sh`: the image never copied `deploy/`; the
entrypoint is the dashboard, so the path would have been appended to its argv; and
`TEXT2SQL_RESULTS_REVISION`, which the script checks first, was absent from the app service
— compose uses `.env` for interpolation, and variables do not reach a container unless
named in `environment:`. The script is now `text2sql-provision` on PATH, the revision is
passed, and the runbook explains why `--entrypoint` is needed.

**The read-only database roles were never created.** Neither readonly password was passed
by compose nor mentioned in the example env, so both init scripts took their
"unset, skipping" branch and exited 0. The app would then have connected as superuser to
run caller-supplied SQL — which is exactly what those files exist to prevent, their own
comments saying application-layer checks are the wrong place for that guarantee. Both now
refuse to initialise rather than skip, because initialisation happens once and a warning on
stderr is gone by the time anyone looks.

**The MySQL grant matched nothing.** It granted on `beaver%`.*, but `load-beaver.sh`
creates `dw`, `csail_stata_neutron` and `csail_stata_nova`. The read-only user could not
have read a single table, and the example connection string named a `beaver` database that
is never created. Written before the real dumps arrived and never reconciled.

**Three more about failure behaviour rather than function.** `caddy` depended on `app`
being *healthy*, so a failing app kept the edge down entirely — no error page and, worse, no
ACME endpoint, which is how a certificate quietly fails to renew. No container had a memory
ceiling, so a runaway one takes the host down rather than itself. And HSTS asserted
`includeSubDomains`, a browser-cached year-long promise about subdomains this deployment
does not control.

All of it is now asserted in CI against the rendered compose config, so it fails a build
rather than a deployment.

606 backend, 77 frontend and 10 end-to-end tests passing; all ten CI jobs green.

---

## 2026-08-25 — Sweeping every view for responsiveness found a data-corruption bug

Prompted by "mini-dev postgres is somewhat slow too — check all the views". Timed every
endpoint the dashboard calls, on all six benchmarks. Server-side, nothing was slow any
more: the worst was 112 ms. The problems were elsewhere.

**Result tables rendered every row.** A record carries the full result set of every query
it ran. One Beaver record holds an 86,502-row ground truth beside a 55,817-row prediction,
and the panel showing them is a 240-pixel scroll box. Opening it built **854,563 DOM nodes
and 858 MB of JS heap** to display about eight visible rows. Measured, not estimated.

The cause was duplication that had drifted. There were two copies of the result table, one
per detail view; the pipeline-detail copy had gained pagination and the error-analysis copy
never did. The same result was 10 rows in one panel and 86,502 in the other. One copy now.

The server previews too — 200 rows plus the true count. The count is the part that matters:
a table showing 200 of 86,502 rows without saying so misrepresents what the query returned.

    DOM nodes  854,563 -> 894
    JS heap    858 MB  -> 10 MB
    reflow     103 ms  -> 1 ms

One ordering constraint is now written into the code: in the playground, the trim happens
*after* `evaluate_prediction` has run, on copies. Truncating any earlier would compute the
metrics against a fraction of the result set.

**Then the end-to-end suite started failing about one run in six.** I spent a while
treating that as test flakiness — added a wait for the view to settle, then a
row-stability poll, then weakened two strict assertions to polls. The failure rate barely
moved. That should have been the signal sooner: waits that do not help are usually waiting
for the wrong thing.

Reading the captured server output instead of the test diff found it:

```
sqlite3.InterfaceError: bad parameter or other API misuse
  at EvalIndex.list_records
```

`EvalIndex` opened **one** sqlite3 connection with `check_same_thread=False` and shared it
across threads. That flag silences sqlite3's ownership check; it does not make a connection
safe to use concurrently. The server caches one `EvalIndex` per benchmark and runs sync
endpoints in a threadpool, so concurrent access is the ordinary case, not an edge case.

Reproduced off the server: twelve threads over one handle, **9 of 12 failing**. And the
failures were not all exceptions — some returned rows as `None`, meaning a page of results
could come back short or empty **with no error at all**. Wrong answers, not crashes. On a
public deployment with real traffic this would have been silently serving incorrect result
pages.

Each thread now opens its own read-only connection. SQLite allows any number of concurrent
readers and read-only connections are cheap, so this costs essentially nothing.

With the fix in, twelve consecutive end-to-end runs are clean — and the two assertions I
had weakened to polls are back to strict, because the weakening was never the problem. The
one test-timing change kept is real: the error-analysis view chooses its default pipeline
after the first paint, so a snapshot taken before that captures a state no recipient of a
shared link ever sees.

**Two lessons worth writing down.** Every sequential measurement I took said the dashboard
was fine; the bug needed ten browsers at once. And a passing test suite that fails
intermittently is reporting something — I treated it as noise for several rounds before
reading the server log.

Also: the same view no longer prints "N/A" in its metric columns before a pipeline is
chosen. "N/A" says the record has no value for that metric, which is a different claim and
not a true one.

580 backend, 77 frontend and 10 end-to-end tests passing.

---

## 2026-08-25 — The Beaver page took 14 seconds; one endpoint was doing all of it

Reported as "the beaver page loads slowly". Timing every request the page makes found
one:

```
/api/benchmarks/beaver/summary               0.003s
/api/benchmarks/beaver/pipeline-aliases      0.090s
/api/benchmarks/beaver/summary/by-category  13.904s   <-- cold; 8.3s warm
```

**Goal 2 fixed memory here and left time alone.** `by-category` was converted from
`json.load` to streaming `iter_records()`, which made peak memory independent of artifact
size — and that is genuinely what a public host needs. But it still reads every byte.
Beaver is 880 MB across **209 records**: 4.2 MB each, nearly all of it serialized result
dataframes. The endpoint parsed all of it to collect 36,839 floats.

The index already held those floats. What it lacked was `meta.categories` — seven short
strings per record. So the fix was to index the categories and read the summary from the
index:

| benchmark | before | after |
|---|---|---|
| archer_en_dev | 0.18s | 0.03s |
| spider_realistic | 1.34s | 0.05s |
| **beaver** | **8.32s** | **0.05s** |
| spider_dev | 2.17s | 0.10s |
| bird_mini_dev_sqlite | 2.22s | 0.12s |

**The aggregation is untouched.** It was split into "gather the values" and "summarize
them"; the index path substitutes for the first and both paths run the same second, so
there are not two copies of a statistics routine to drift apart.

**Where this nearly went wrong.** My first version read from the interned `metrics` table
and the differential test passed — the summaries compared equal. Then the live responses
differed by 6 bytes out of 216,298. Dict equality ignores key order, and ordering by
`metric_ref` sorts metrics by *global* first-appearance rather than by each record's own
key order, so the metric keys in the JSON response were shuffled. Fixed by reading each
prediction's stored evaluation block (which preserves the record's key order) and by
storing each prediction's position within its record (which preserves the pipeline order).
The test now compares serialized output, not just parsed values.

Worth naming the near-miss: a passing differential test convinced me the rewrite was
equivalent when it was not. What caught it was diffing the running servers, old code
against new, on all five real benchmarks — they are byte-identical now.

**Schema version bumped**, so indices rebuild: automatically on a local run, and via
provisioning on a shared one. The runbook now says to rebuild *before* serving after such
a release, because a shared deployment refuses on-demand builds and would otherwise answer
503 for every benchmark. Rebuilds cost roughly ten seconds per gigabyte.

**One bug found in passing.** The large-record warning in the index builder used
loguru-style `{}` placeholders with a stdlib logger, so it raised inside the logging
handler and had never printed — not once, including every time Beaver's 108 MB record was
indexed. An AST sweep confirmed it was the only such call in the codebase. It prints now.

556 backend, 77 frontend and 9 end-to-end tests passing.

---

## 2026-08-25 — Released as 1.3.0, not 2.0.0, plus two cosmetic decisions

**The major bump was the wrong number, and the plan's argument for it did not survive
being checked.** 4.8 claimed "the URL scheme, capability tiers, artifact index, and
deployment model are all new or breaking". I wrote that before any of the work existed.
Against the finished branch: the curated public API has no removals *or* additions, the
CLI subcommands are identical, the artifact format is unchanged by design, and a loopback
dashboard keeps every capability it had — enforced by a test. The URL scheme is *new*, not
breaking; there were no URLs before, so no link broke. The one real churn is that
`ui/server.py` lost ~85 module-level names to the router split, and CLAUDE.md states the
public API is curated in `__init__.py`.

So 1.3.0. It also follows 1.2.0 naturally, resolving the changelog skew by moving past it
rather than adjudicating it — which was the plan's other argument for 2.0.0 and did not
require 2.0.0 either.

**It also dissolves the release blocker I had been treating as real.** The published
snapshot's manifest declares `>=1.1.0,<2.0.0` and `_validate_manifest` *raises* outside
that range, so a 2.0.0 install could not `results fetch` at all until a new snapshot was
uploaded. 1.3.0 is inside the range. No token, no 4 GB re-upload, and no loosening a
safety check under release pressure.

What that dodges rather than fixes: the manifest carries `schema_version: 1` — the actual
data-format contract — and **nothing in the codebase ever reads its value**, while the
enforced gate is a toolkit-version range that `upload_results_to_hub.py` generates
mechanically as `<{major+1}.0.0`. Nobody decided a 2.x toolkit cannot read this data; a
script assumed it. The trap is still there for whenever a 2.0.0 happens, and it is
recorded rather than quietly stepped over.

**The version now lives in one place.** `pyproject.toml`, read back through `_version.py`
from the installed distribution metadata, imported by both `__init__` and `results/_hub`.
It was previously written out in three, which is exactly how the package came to report
1.1.0 while the changelog documented a 1.2.0 release whose features were already in the
code. `scripts/ci/check_version.py` now fails CI if pyproject, the package, the changelog
and the tag disagree.

**Not tagged, not pushed.** A tag pointing at a commit that may still change is worse than
no tag, and the user has cosmetic review pending. Both wait.

### Two cosmetic changes, both worth their reasoning

**`/b/spider_dev` → `/benchmark/spider_dev`.** These addresses are meant to be pasted into
issues and papers. `/benchmark/spider_dev` says what it points at; `/b/spider_dev` needs
the reader to already know. Nothing is in circulation — the branch has never been pushed —
so it is a clean break with no legacy alias to carry, and an old-style path renders the
not-found state naming the path rather than a blank page.

**The "Copy link" button is gone.** The address bar already holds the address and every
browser already offers a way to copy it. A button that duplicates a browser affordance is
chrome with a cost and no purpose, and I had added it without asking what it was for.

"Copy short link" stays, because what it produces genuinely cannot be obtained from the
address bar. It now renders *only* on an address that names a pipeline, so it is absent
wherever it would change nothing — with an end-to-end test asserting that absence, since a
control that quietly reappears everywhere is how this becomes chrome again.

548 backend, 77 frontend and 9 end-to-end tests passing at 1.3.0.

---

## 2026-08-25 — End-to-end tests, and three ways shared links were quietly broken (4.5)

Nine Playwright tests, against a real server and a real cold page load. They never
navigate twice: each sets up a view, takes the address out of the browser, opens it in a
**fresh context** — no storage, no history, no shared router — and compares what renders.
That is the recipient of a pasted link, and it is the only thing that actually tests Goal
1.

Three defects on the first run. Every one of them had passed unit tests, component tests,
lint, type checking, and my own manual checking in a browser.

**A link to a benchmark this server does not have opened a different one.** The redirect
that gives benchmark-less views a default also fired when the URL *named* a benchmark that
was absent, silently substituting the fallback. That is exactly the case shared links hit
most often — the recipient's server has a different results snapshot — and the recipient
was shown numbers for a benchmark they never asked about, with nothing to indicate the
link had failed. Item 1.7 was written to cover precisely this and had been marked done.

**The back button did nothing.** `ErrorAnalysis` read page, page size and the open record
as *initial* values. Back changed the props; the view ignored them, re-emitted its own
stale page, and pushed the address straight back where it came from — so from page 2, back
was a no-op. Those three are now read from the URL, with the view reporting changes
upward, which makes the address bar the single source of truth for them rather than merely
its first writer. Page turns and opening a record push a history entry; filter edits still
replace, because a search term would otherwise leave one entry per keystroke and back would
become a way to delete characters.

**A cold load filtered the address but not the results.** Fixing the back button surfaced
this. On entry with no pipeline in the URL, the view picks a default, writes it into the
address — and never refetched. So the table showed all 60 records while the address said
it was filtered to 30. Copy that link, send it, and the recipient sees a different set of
records than the sender was looking at. That is the precise failure the entire goal exists
to prevent, and it had been shipping.

Worth being blunt about what this says. I had verified shareable links in a browser
several times across this branch, and reported them working. They were working for the
cases I thought to try. The back button is not a case anyone thinks to try, and the
filter/URL disagreement only shows up if you compare the number on screen against the
number the address implies. A test that mechanically opens the link in a clean context
does not have that blind spot.

**The fixture is synthetic and deterministic** (`scripts/ci/make_e2e_fixture.py`) because
the real snapshot is ~4 GB and lives on the Hub, and because a test that copies a link in
one context and opens it in another needs the data behind it to hold still.

CI runs Chromium only: these are about routing and history, not cross-browser rendering,
and three engines would triple the download for no additional signal. Retries are off — a
link that works on the second try has not proven anything. Verified stable across repeated
local runs before committing.

548 backend, 77 frontend and 9 end-to-end tests passing.

---

## 2026-08-25 — Coverage floors, and two more defects in the reports nobody tests (4.10)

`error_analysis.py` went 9% → 70%. It writes the per-pipeline failure reports that ship
beside `data/results/README.md` and render inline in the analysis notebooks. The risk in a
module like that is never a crash — it is a report that looks fine and says something
untrue — and writing the tests turned up two of those.

**A question the pipeline never answered lost its identity.** A record with no prediction
for a pipeline was appended to the failed list as the bare string
`"No predictions for {pipeline}"`. It still counted towards the failure total, which is
right: a pipeline that produced nothing for a question did fail on it. But the record was
gone, so the report could not say *which* question — and the formatter, handed a string
where it expected a mapping, fell into its error branch and rendered
`⚠️ Error reading prediction in record: {…}` instead of an example. Those stay records
now, and format as an explicit "No Prediction" entry naming the question.

**That error note inlined the whole record.** A record carries serialized result
dataframes, so one malformed record put hundreds of kilobytes of them into a published
markdown file, and the same again into the log line beside it. It names the record now.

**On the floors themselves.** A single project-wide percentage would have been close to
useless here. 39% is carried by the modules that were written with tests; it would sit
perfectly flat while `evaluation_tools` or `capabilities` lost half their coverage. So
`scripts/ci/check_coverage.py` enforces 16 per-module minimums, grouped by what a
regression would actually cost — the published numbers, the artifact index, authorization,
data access — and every entry carries its reason in the file.

Two details that make it a guard rather than decoration:

- It **fails on a module named in the table that the report has never heard of.** A typo or
  a rename would otherwise enforce nothing at all, silently, which is precisely the failure
  mode the file exists to prevent.
- It **reports when a module has drifted 10 points clear of its floor.** A ratchet nobody
  tightens stops being a ratchet.

Floors sit a few points under what is reached, because branch coverage differs slightly
across the 3.11/3.12/3.13 matrix and a floor that fails on noise gets deleted by the next
person. Verified the check fails on a regression rather than merely passing today.

`fail_under = 38` in `pyproject.toml` covers everything else, and is low on purpose: the
remaining bulk is inference and execution code that needs live LLM and DB endpoints, and
raising it would mean either weak tests or a suite that cannot run offline. Saying that
out loud is better than a number that implies more assurance than exists.

548 backend and 77 frontend tests passing.

---

## 2026-08-25 — Splitting server.py found a hole in the authorization layer (4.9)

`ui/server.py` is 3,184 lines down to **343**, across 18 modules with nothing over 800.
The interesting part is not the arithmetic.

**The first `include_router` call silently disabled half the authorization machinery.**
Since Starlette 1.6, `include_router` does not splice a router's routes into
`app.routes` — it leaves a wrapper object there that holds them. Both places that decide
authorization walked `app.routes` flat, saw an object with no `path` and no `methods`, and
skipped past every route inside it:

- `unclassified_routes()` stopped auditing them, so a mutating route in a router could
  never be reported as missing from `ROUTE_TIERS`. The test whose entire job is to catch
  an unclassified endpoint would have passed while being blind to nine of them.
- `_route_template()` could not resolve a template, so the gate fell back to the concrete
  path. That still fails *closed* for mutating methods — no route became reachable that
  should not be — but it means the `ROUTE_TIERS` entry is never consulted. A route
  deliberately placed below `full`, which is exactly what the judge endpoint is, would
  have become unreachable for the allowlisted user it exists for, with nothing saying so.

`capabilities.iter_routes()` now descends into included routers and mounts, unwrapping by
attribute rather than by type so it survives the private class being renamed again. Three
tests cover it and all three fail if the walk is reverted.

Worth stating plainly: this was found by the existing classification test failing on the
first split, not by review. The test was written for a different reason — to catch an
unclassified route — and caught a change in framework behaviour instead. That is the
argument for having written it.

**Grouping is by capability, not by URL.** `routers_execution` holds everything that runs
caller-supplied SQL; `routers_judge` holds the one mutating capability a public visitor
can reach. Both fit in one sitting, which is what makes "what can this deployment
actually do?" answerable by reading rather than by grep.

**Evidence the split changed nothing.** The route table is byte-identical before and
after — 35 `/api` routes, same methods, same templates. `tests/test_route_table.py` now
records that set, because routes are public API here (shared links resolve against them)
and a dropped `include_router` deletes a whole group at once without any single endpoint
test necessarily failing. Verified against a running server too, in both local and public
mode: every router answers, the tier gate still returns 403 for router-hosted mutating
routes, the sub-path bypass probe never reaches a handler, and the error listing pages
104 records.

**Two test-seam changes were forced and are improvements.** The rate-limit knobs and the
judge's LLM call are now patched in the module that reads them. Patching
`evaluate_sql_prediction_with_llm` on `server` would have silently stopped taking effect
— meaning a test suite that starts calling watsonx for real. Separately, six fixtures were
replacing `get_data_root` with a lambda, bypassing the resolver under test; they now set
`TEXT2SQL_DATA_ROOT`, which is what a deployment does, and the seam was checked to be
load-bearing rather than incidentally passing.

521 backend and 77 frontend tests passing. Coverage 36% → 38%.

---

## 2026-08-25 — Status audit: three plan items rested on premises that turned out false

Went through every plan item against the code rather than against the previous status
line. Most matched. Three did not, and in each case the item's *reasoning* had expired
even though its status was still defensible.

**2.8 assumed a slow backend.** The data-fetching library and list virtualisation were
sized against a dashboard where a record detail took 921 ms and a page of records forced a
full-artifact parse. After Goal 2 the same requests are 0.3 ms and 3–11 ms. Request
deduplication and windowed rendering solve problems that no longer exist at that latency,
so both are now recorded as *deliberately dropped pending a measured reason*, not as
outstanding work. Leaving them on the list as "remaining" would have implied the dashboard
is missing something it needs.

**4.13 assumed routing would delete the effects.** The item deferred 21 eslint findings on
the grounds that Goal 1 would replace much of that state with route params. Routing
landed; the findings did not move. Worth naming, because the deferral was reasonable when
written and would have quietly stayed reasonable-looking forever. Of the 17 that remain, 5
are the fetch-on-mount pattern, which is the sanctioned use of an effect — those need a
rule exception or a data-fetching library, not a rewrite, so the honest count of *debt* is
12, not 17.

**4.9's target moved away from it.** `server.py` was 2,549 lines when the item was
written. Extracting 41 Pydantic models removed 297. It is now 3,184 lines across 38
endpoints, because Phase D added tiers, auth, the judge endpoint and the deployment
surface faster than extraction removed anything. "Partial" was true both before and after,
which is exactly why the number needed writing down.

Also corrected figures that had drifted: frontend tests 51 → 77, coverage 35% → 36%,
`error_analysis` 5% → 9% (it was misreported), entry bundle 401 → 419 KB. The bundle
growing back 18 KB is the kind of thing a budget in CI would catch and nothing currently
does.

Added a health check to the runbook for the alias table, since a short link that resolves
to nothing is indistinguishable from a broken link to whoever was sent it.

---

## 2026-08-25 — Short pipeline links (1.6), and a goal that had to be narrowed

**The item as written could not be built.** 1.6 was "stable identifiers": a hash alias so
a link survives the model string changing. That does not follow. The alias is a hash *of*
the id, so a rename changes the alias with it and the link breaks exactly as before.
Nothing cheap fixes that — the artifacts are keyed by the id itself, so a rename-proof
alias needs a persisted mapping from a stable key to whatever the id is called today, and
there is nowhere to keep one. The rename half is not solved, and the plan now says so
rather than claiming the feature covers it.

**The half that was worth building is length.** Pipeline ids are derived from the model
name, and the comparison views carry two of them. A real filtered comparison link measured
247 characters; mail clients wrap at less. `GET /api/benchmarks/{id}/pipeline-aliases`
returns a derived `{alias: pipeline_id}` table read from the summary file — small, always
present, and never triggering an index build over a multi-GB artifact. The same link is
now 158 characters.

**One implementation of the hash, not two.** The frontend does not compute aliases; it
fetches the table. Two implementations of a truncated SHA-256 would eventually disagree
about one string, and the symptom would be a link that resolves on one machine and 404s on
another. Fetching also means an unknown alias is genuinely unknown rather than a hashing
disagreement.

**A collision resolves to neither pipeline.** At forty bits and fewer than fifty pipelines
per benchmark a collision is not going to happen, but the handling still matters: a link
that says "not found" is recoverable by asking the sender, and a link that quietly opens
the wrong pipeline shows the reader different numbers than the sender saw, with nothing to
signal it. `alias_map` drops a colliding alias rather than letting the last writer win.

**Only pipeline references are rewritten.** Shortening by string-replacing the id
throughout the URL would also rewrite a search term containing a model name — silently
changing what the shared link searches for. Both directions parse the URL and touch the
`/pipeline/:ref` segment and the `pipeline` / `pipeline2` parameters only; a test pins it.

**The readable form stays canonical.** An alias is expanded on arrival and the address
rewritten in place, so nothing downstream of `App` knows aliases exist and there is one
form in the address bar. Verified in a browser: `/b/archer_en_dev/pipeline/ec64b733f4`
expands and renders the same view as the full id, an unknown alias renders "not found"
naming the likely cause, and a two-alias error-analysis link restores both pipelines with
`page=2` and `disagree` intact.

Also widened the mypy scope to `indexing/` and `ui/aliases.py` (three missing container
annotations in the builder). 514 backend and 77 frontend tests passing.

---

## 2026-08-25 — Phase E: coverage on the published numbers, docs, and module size

**Coverage where a bug is silent (4.10, partial).** Two modules produce artifacts that get
committed, uploaded, and cited, and both were effectively untested.

`evaluate_prediction` (4% → 35%, 26 tests) produces every metric the toolkit publishes.
Testing it found a defect with a wide blast radius: a record whose ground-truth SQL had no
dataframe returned `{"df_error": 0}` — no metrics, no error flag — and `compute_summary`
then subscripted a missing key and **aborted the summary for the entire benchmark**. One
bad record took out 500 good ones, failing far from its cause. Now flagged as an evaluation
error, and the summary counts missing metrics as 0, which is what the comment already there
said it did. Verified against the published artifacts: recomputing all 11 `archer_en_dev`
pipeline summaries reproduces the shipped numbers exactly.

`report_tools` (0% → 52%, 20 tests) writes `data/results/README.md`. Same shape of
fragility: both the table and the chart read metrics as `.get(key, {}).get("average")`,
which raises `AttributeError` if a metric is a bare number instead of the
`{average, stddev}` shape — aborting the whole report. Both paths now share a tolerant
reader.

Overall coverage 29% → 35%. `error_analysis.py` (5%) and the inference pipelines remain
the thin spots, and no floor is enforced yet.

**Deferred lint findings cleared (4.12).** All four Ruff rule groups, one commit each.
`B905` was the one that could not be swept: `strict=True` changes behaviour, so each of
seven sites was decided separately — and checking the artifacts first showed that 2 of
2,855 records would have made `strict=True` raise. `E722` mattered more than it looked:
a bare `except:` was swallowing Ctrl-C during long agentic runs. Ruff now runs with only
`F841` and `B008` ignored, both with stated reasons.

**Frontend types (4.13, partial).** All 17 `tsc` errors fixed and the check made blocking.
Two were mine: `ProfileCompareView` still referenced a variable I had deleted, so that view
threw `ReferenceError` on render — it built cleanly, and I had not opened that view. Eight
were duplicate-`key` errors where a Carbon prop getter spreads its own `key` over the
explicit one. The browser console also showed my CSP had no `font-src`, so all 120 IBM Plex
references were blocked and the UI had silently dropped to system fonts.

The 21 react-hooks effect findings stay off, with an honest reason rather than the stale
one: 5 are the fetch-on-mount pattern the rule cannot distinguish, and 15 are real debt
that decides which option a user sees — not worth rewriting blind without component tests.

**One source of truth (4.6, 4.7).** The packaged benchmark registry had lost every
benchmark's `logo`, invisible in development because the checkout copy shadows it.
`requirements.txt` was worse than redundant: 55 of its 73 entries were not project
dependencies while `openai`, which is one, was missing. Both are now generated and checked
in CI, along with `uv lock --check` — an export can agree with a lockfile that has itself
fallen behind.

**Documentation (4.11) and module size (4.9, partial).** README gains a documentation
index and sections on shareable links, the query index, and running the dashboard for other
people; the snapshot size is corrected from ~7 GB to the ~4 GB the manifest actually
reports. 41 Pydantic models moved out of `server.py` into `ui/models.py`, taking it from
3,446 to 3,149 lines. Splitting the 37 routes into routers is the remaining half and
interacts with the tier middleware, which walks `app.router.routes`.

**Component tests (4.5, partial).** Vitest gains Testing Library, a setup file (jsdom
implements neither `matchMedia` nor `ResizeObserver`, and Carbon reaches for both), and
component tests that mount real views against stubbed APIs. 34 → 51 frontend tests.

The point of these was to unblock 4.13: the 15 synchronous selection-reset effects each
decide which option a user ends up looking at, so rewriting them needed a test asserting
the *outcome* rather than the mechanism. `ToolkitInsightsView.test.tsx` now pins exactly
that — whatever the implementation, the selection must settle on a metric that exists, and
must move off one the server no longer defines.

Mounting the views immediately found a bug class none of the type or lint checks catch:
`a?.b.c()` guards only `a`. If `a` is present but `a.b` is undefined, the call still
throws. Five instances across four views, including
`summary?.overall.map(...)` — so a `/summary/by-category` response missing `overall`
took down the whole insights view with `TypeError: Cannot read properties of undefined`.
Optional chaining looks like a guard, which is what makes it easy to miss.

**Effects converted to derived state (4.13, continued).** With the outcome pinned, the
metric- and pipeline-selection effects in `ToolkitInsightsView`, `PipelineCompareView` and
`ProfileCompareView` are now derived values rather than corrections applied after the fact.
Findings 21 → 17.

The clamping rule moved to `lib/metricInsightsSelect.clampToAvailable` and is unit-tested,
so the three views cannot drift apart — they had three copies of the same logic. One detail
the rewrite fixed beyond the lint finding: the pipeline-selection effect depended on
`selectedPipeline` *and* set it, so it re-ran on its own output; deriving removes that loop
along with the render in which the selection was briefly invalid.

Verified in a browser that the insights view is unchanged — same confusion matrix
(62/0/1/41), same auto-selected pipeline and metrics — and that all three views load with
no failing requests.

490 backend and 57 frontend tests passing.

---

## 2026-08-25 — Known trap: `make_summary_report.py` clobbers a hand-written file

`scripts/analysis/make_summary_report.py` writes the generated benchmark dashboard to
`data/results/README.md`, and `README.md` documents exactly that. But commit `59a2b99`
deliberately replaced that file with a 36-line guide to downloading results from the
Hugging Face Hub.

So running the documented command silently overwrites a file someone wrote on purpose. I
hit this while checking that a change to `report_tools` did not alter the published report:
the regenerated output differed completely, which looked like a regression until the git
history explained it.

Not fixed here, because the right fix is a product decision:

- point the generator at a different filename (say `data/results/SUMMARY.md`) and leave
  `README.md` as the download guide, or
- restore the generated dashboard as `README.md` and move the download guide elsewhere.

Either way the docs and the script should agree. Recorded rather than silently changed.

---

## 2026-08-25 — Phase D begins: capability tiers and Google sign-in (3.1–3.2)

**Capability tiers (3.1).** Authorization is now resolved once per request from the
deployment mode and the caller's identity, and enforced in a single middleware rather than
across twenty-eight handlers.

- `public` — read-only. Anonymous, or signed in without an allowlist entry.
- `judge` — adds on-demand LLM-as-judge for allowlisted signed-in users.
- `full` — everything, as before; still the local default.

Two properties make it hard to get wrong. A mutating route with no declared tier requires
`full`, so forgetting to classify a new endpoint fails closed. And the startup mode is a
*ceiling* — signing in can never raise a public deployment to `full`. `--mode full`
refuses a non-loopback bind without `--allow-remote-full`, so the dangerous configuration
takes deliberate effort.

The classification test earned its keep immediately: adding `/api/auth/logout` in the very
next step made it fail until the route was explicitly classified.

**Google sign-in (3.2).** Direct OIDC via Authlib — for an allowlist this small, an
identity service would add cost and a dependency for nothing.

The load-bearing check is `email_verified`. Google returns an `email` claim for unverified
addresses too, so matching the allowlist on `email` alone would make the allowlist
meaningless: anyone could create an account claiming `operator@example.com`. Sessions carry the
verified address and nothing else, so there is no user database to secure, and logs carry a
hash rather than the address.

`safe_redirect_target()` restricts the post-sign-in redirect to same-site paths, so a
crafted sign-in link cannot bounce a freshly authenticated user to another origin.

**Three bugs found by running it rather than reasoning about it**

1. HTTP middleware executes *before* routing, so `scope["route"]` is unset. Matching the
   concrete path would have sent every parameterised route to the fail-closed default —
   safe, but it would have made the judge tier unreachable. The template is now resolved
   against the router.
2. `request.session` *raises* when `SessionMiddleware` is absent, which it is in local
   mode, so `getattr(..., None)` did not help. Reads go through `request.scope` now.
3. The first tier test asserted every mutating route is refused in `public`, which was
   wrong as soon as a deliberately-public mutating route existed. It now checks each route
   against its *declared* tier, with a separate explicit list asserting the genuinely
   dangerous endpoints stay `full` — otherwise a relaxed tier would simply be rubber-
   stamped by the parametrised test.

**Scoped judge endpoint and budget (3.3–3.4).** Landed together, because a personal
watsonx key behind a public site without a ceiling is the failure mode the plan warns
about.

`POST /api/benchmarks/{id}/judge` judges one (record, pipeline) pair. The existing
`/evaluate` endpoint was the wrong shape for a shared deployment: it re-evaluates a whole
benchmark and rewrites the shared artifacts, so one user's re-run would change what every
visitor sees. Verdicts now go to a separate store attributed to the caller, and a test
asserts the canonical artifact's bytes are unchanged afterwards. Responses carry
`source="on-demand"` so they are never confused with the snapshot's `llm_score`.

Cost controls:

- Metered from **reported tokens, not call count** — judge prompts embed both result
  dataframes and vary by orders of magnitude.
- Counters **persist to SQLite on the data volume**. In-memory counters reset on restart,
  which is exactly how a monthly ceiling stops being one.
- A semaphore serialises calls and the ceiling is re-checked *inside* it, since a burst
  could otherwise each pass the check before any recorded spend.
- Kill switch disables the tier without a redeploy; `/api/me` then reports
  `can_run_judge=false` so the UI stops offering an action that would 503.
- Remaining budget is on `/api/me`, so the ceiling is visible before it is met.

Rates are configuration rather than source — the defaults are an estimate needing
calibration against a real invoice, and a provider that reports no usage is logged as
*unmetered* rather than silently counted as free.

`llm_as_judge` now returns `token_usage`, handling both the legacy `generate` shape
(`results[0].input_token_count`) and the Chat API shape (`usage.prompt_tokens`).

**Hardening (3.5).**

- **Judge config names** are validated and contained. Both the read *and write* endpoints
  interpolated a URL segment straight into `base_dir / f"{name}.yaml"`, so the write
  endpoint could place YAML outside the config directory.
- **CORS** allowed credentialed requests from a localhost origin list. Harmless while no
  session existed; adding sign-in made it real. Withdrawn outside full mode, where the UI
  is same-origin anyway.
- **Security headers** on every response: a restrictive CSP (no third-party scripts or
  frames), nosniff, DENY framing, same-origin referrer.
- **Rate limiting** per client outside full mode, with a tighter bucket for `/api/auth/*`.
  Local mode is exempt — throttling a single-operator interactive tool would be a
  regression for nothing.
- **Error detail** is tier-dependent. 404s named the exact file under `data/results/` and
  the commands to fix it; a public visitor can act on none of that and it discloses the
  filesystem layout.

**One real exposure found and fixed, created by two safe things combining.**
`/api/static/{path}` served any file beneath the data root, and being a GET it runs at the
public tier. The judge spend store was then placed *under* the data root. Neither is a
problem alone; together, `GET /api/static/judge/usage.sqlite` returned the full SQLite
database — spend ledger, cached verdicts, per-user hashes — to any anonymous visitor.
Confirmed against a running server, not inferred. The derived indices were equally
readable. The route is now scoped to `benchmarks/logos/` and to image extensions, which
is its only actual use; the URL contract is unchanged.

Verified end-to-end in public mode against the real 3.6 GB corpus: reads work (including
the 880 MB Beaver benchmark), `/execute`, `/evaluate`, `/judge` and the static store are
all 403, and 404s disclose nothing.

**Deployment stack, operations, and polish (3.6–3.9).**

- `deploy/Dockerfile` builds the frontend from source rather than trusting the committed
  `dashboard/dist`, pins an exact interpreter, runs non-root, and bakes in no credentials.
- `deploy/docker-compose.yml`: caddy, app, postgres, mysql. Only caddy publishes a port;
  the databases sit behind a profile so a browse-only deployment never starts them.
- Read-only database roles, with Postgres also getting `default_transaction_read_only`, a
  statement timeout, and revoked `CREATE` — a write is refused by the database, not by
  application logic.
- `deploy/provision.sh` fetches the pinned snapshot, indexes, verifies, and marks. It
  refuses to run without an explicit revision, since the default falls back to `main` and
  a floating revision would let the public dataset change under shared links.
- `docs/deployment-runbook.md` — first deploy, health checks (each corresponding to
  something that actually went wrong), routine operations, troubleshooting, and an honest
  list of what is *not* covered.
- `/api/deployment` plus a header strip and About panel: a "read-only" tag, a sign-in
  control that appears only when OAuth is configured, and a full-width stamp naming the
  snapshot on screen. A link shared today is only interpretable months later if the
  recipient can see which data it shows and that nothing is evaluated live.

Docker is unavailable in this environment, so a CI job now builds the image, validates the
compose config, and smoke-tests the running container — asserting `/api/me` reports the
public tier and `/execute` is refused — rather than the VM being the first thing to try it.

**The security review found three HIGH defects, all of which I had introduced.** Each was
confirmed by driving the real ASGI app before and after the fix:

1. **Authorization skipped entirely under a non-empty `root_path`.** The tier middleware
   gated on `scope["path"]`, but Starlette routes on `get_route_path()`, which strips
   `root_path`. Served under a sub-path, the middleware returned early while the router
   still dispatched — anonymous arbitrary SQL against the configured database credentials.
   Rate limiting was disabled by the same bug, on exactly those deployments. My earlier
   bypass probes all used an empty `root_path`, so a clean run there proved less than it
   appeared to.
2. **`TEXT2SQL_DASHBOARD_MODE` was only read inside `main()`**, so serving the ASGI app
   directly left the ceiling at `FULL` while the operator believed it was public. The
   allowlist *was* read at import, which made the configuration look like it worked.
3. **Enabling Google sign-in crashed startup.** `configure_cors()` eagerly rebuilt the
   middleware stack, after which Starlette refuses `add_middleware`. The entire auth path
   was unreachable as shipped — and therefore never exercised end to end, which is why the
   middleware ordering in (1) went unnoticed.

Also fixed from the same review: concurrent index builds shared one temp filename and
could promote a half-written database as authoritative results; `X-Forwarded-For` was
trusted from any peer using its forgeable leftmost value; the session cookie's `Secure`
flag came from the bind address and so dropped behind a TLS proxy; a 500 re-leaked the
absolute index path; session secrets had no length floor; and the verdict cache key was a
non-injective space-join.

Separately found while reviewing the composed surface: `/api/static` served the entire
data root at public tier, and the judge spend store lives under it —
`GET /api/static/judge/usage.sqlite` returned the full database to any visitor. Neither
piece was a problem alone. Now scoped to `benchmarks/logos/` and image types.

**Database services (3.10, 3.12).**

*SQLite (3.10).* `run_sqlite_query()` opened databases read-write. Benchmark databases are
immutable reference data and the execute endpoint runs arbitrary caller-supplied SQL, so a
write should be refused by SQLite rather than by anything upstream. Now opened with a
`mode=ro` URI, with `SQLITE_LIMIT_ATTACHED` set to 0. `ATTACH` was not exploitable through
this function — each call opens a fresh connection and sqlite3 runs one statement per
`execute` — but refusing it outright removes the dependency on that driver detail. Two
incidental fixes: a result set with no rows raised `TypeError` because `cursor.description`
is `None` for such statements, and the URI form needs the path percent-encoded or a
directory containing a space would fail to open.

*Beaver/MySQL (3.12).* The dumps arrived, and the databases are loaded and verified.

The dumps are named after the source systems (`dw`, `nova`, `neutron`) but the benchmark
addresses two of them by a prefixed name (`csail_stata_nova`, `csail_stata_neutron`),
because `db_id` is substituted into the connection string per record. Loading the dumps
unchanged produces databases the benchmark cannot find, so `deploy/load-beaver.sh` rewrites
the `CREATE DATABASE` / `USE` statements as it streams — anchored to line starts and
matched against the specific source names, with a table-count check afterwards as the
actual proof that nothing was mangled.

Loaded and verified against a real MySQL 8:

| Database | Tables (schema / loaded) | Gold queries |
|---|---|---|
| `dw` | 97 / 97 | 121 / 121 pass |
| `csail_stata_nova` | 109 / 110 | 43 / 43 pass |
| `csail_stata_neutron` | 175 / 175 | 30 / 30 pass |

**All 194 executable gold queries run successfully, in 9.5 s.** (`nova` has one table more
than the schema records, which is harmless.)

The remaining 15 questions reference `keystone` (8), `csail_stata_glance` (5), and
`csail_stata_cinder` (2), for which **no dumps are published upstream**. They fail with a
clear unknown-database error. That is a data gap rather than a configuration mistake, and
it is now documented in `data/benchmarks/dbs/README.md` alongside the load procedure — which
previously just pointed at the upstream project.

**BIRD Mini-Dev, SQLite and PostgreSQL (3.11).** The Mini-Dev download arrived, and both
variants now execute.

*A path bug meant the documented SQLite setup could never have worked.*
`sqlite_run_execution_async` built database paths from `Path(BENCHMARKS_FILE).parent`, the
copy packaged inside the installed wheel — while the registry, and
`data/benchmarks/dbs/README.md`, both use `data/benchmarks/dbs/`. Following the
instructions put the databases somewhere the code never looked. `resolve_sqlite_db_path()`
now resolves against the registry actually in use (`$TEXT2SQL_DATA_ROOT`, else the
repository's `data/`), falling back to the packaged location so an installed-only layout
still works, and reports the documented path in the error when a database is absent.
**All 500 BIRD SQLite gold queries pass** across the 11 databases.

*PostgreSQL.* `deploy/load-bird-postgres.sh` loads the 1 GB dump with `ON_ERROR_STOP`, so a
partial load fails loudly rather than leaving a database that looks fine until a query hits
a missing table. The first attempt failed on `role "xiaolongli" does not exist` — the dump
carries `OWNER TO` for whoever produced it. The script now creates any such role as
`NOLOGIN` before loading, rather than rewriting a gigabyte of SQL to strip ownership: a
login-less role grants nobody anything, and editing the dump risks mangling a data line.
**All 500 gold queries pass, in 3.4 s.**

Worth recording the shape difference, since it explains the execution code: the Postgres
dump merges all eleven BIRD databases into a single `public` schema (75 tables), which is
why `postgres_run_execution_async` sets `search_path` once and never switches on `db_id`,
while the SQLite and MySQL paths do switch per record.

437 tests passing. **Phase D is complete.** Four of six benchmarks are fully executable
locally (BIRD SQLite 500/500, BIRD Postgres 500/500, Beaver 194/194 loadable); Spider and
Archer need their own downloads, and 15 Beaver questions await three unpublished dumps.

---

## 2026-08-25 — Local setup fixed: full result set fetched, index validated on real data

The dashboard was 404ing on five of six benchmarks because only `archer_en_dev` had ever
been downloaded. Fetched the published snapshot (3.6 GB) and validated the Phase B work
against real artifacts rather than a synthetic file.

**Two defects in the results distribution, found on the way**

1. **The published manifest is stale.** `results list` reported `bak`, `charts`, and
   `logs` as the available benchmarks — those are `results/` sub-directories, not
   benchmarks. The manifest was generated 2026-05-13, before commit `4a04755` fixed
   manifest generation, so `upload_results_to_hub.py` is already correct and the snapshot
   simply needs regenerating. Downloads are unaffected: an unfiltered fetch ignores the
   manifest, and a filtered one already falls back to direct path patterns. `results list`
   now says so instead of presenting directories as data.
2. **The 2.0.0 release is blocked on republishing the snapshot.** `_validate_manifest()`
   *raises* when the installed version falls outside `toolkit_version_compat`, and the
   published manifest declares `>=1.1.0,<2.0.0`. A 2.0.0 install would fail
   `results fetch` outright. The upload script derives that string from the toolkit
   version at upload time, so re-uploading from a 2.0.0 install fixes it — but it has to
   happen *after* the bump. Recorded on the release checklist (plan item 4.8).

**Index validated on the real corpus**

| Benchmark | Artifact | Index | Ratio | Build |
|---|---|---|---|---|
| beaver | 880 MB | 13.7 MB | 2% | 8.4 s |
| bird_mini_dev_postgres | 385 MB | 22.7 MB | 6% | 2.8 s |
| bird_mini_dev_sqlite | 334 MB | 23.1 MB | 7% | 2.3 s |
| spider_dev | 195 MB | 37.7 MB | 19% | 2.4 s |
| spider_realistic | 108 MB | 18.4 MB | 17% | 1.5 s |
| archer_en_dev | 15 MB | 1.9 MB | 13% | 0.2 s |
| **total** | **1,915 MB** | **117 MB** | **6%** | ~18 s |

Better than the 14% estimated from the synthetic file. **Five of the six artifacts exceed
the 100 MB threshold** at which the old UI warned that loading might crash the server, so
the index is what makes this data usable at all, not merely faster.

Serving latency on real data — page 1 3–11 ms, page 20 1–6 ms, record detail 1–13 ms —
flat in page number and independent of artifact size. All 65 pipeline drill-downs across
the six benchmarks return correctly.

**Build memory is dominated by the largest single record, not by batching.** Beaver
contains one 108 MB record whose parsed form costs ~324 MB transiently, which is why that
build peaks near 1 GB while the others sit at 288–537 MB. Flushing is now size-aware
rather than every-500-records (record sizes span two orders of magnitude: 4.2 MB average
in Beaver against 0.14 MB in Archer), and outsized records are logged. The deployment
implication is on record: **provision indices before starting the server**, so a rebuild
never spikes while the app and both databases are live on a 4 GB VM.

---

## 2026-08-25 — Phase C: shareable URLs (plan items 1.1–1.5, 1.7)

The dashboard had no URL state at all: navigation was nine `useState` values, no router
dependency, and nothing under `dashboard/src` ever touched `window.location`. Every view
rendered at `/`.

**Landed**

- `lib/routes.ts` — the whole URL scheme as pure functions: path builders, query
  serialization with defaults omitted, and a parser. Pipeline ids contain both `:` and `/`
  (`wxai:openai/gpt-oss-120b-...`), so they must be percent-encoded in path position;
  keeping that in one place is what stops a missed `encodeURIComponent` from silently
  producing a 404.
- `App.tsx` derives view, benchmark, and pipeline from the URL and navigates instead of
  setting state. Error-analysis filters, page, page size, and the selected record all
  travel in the query string.
- Unknown paths render an explicit not-found state instead of silently showing the landing
  page — a shared link is exactly where the target may not exist.
- `SPAStaticFiles` serves `index.html` for unknown non-API paths. Without it every deep
  link 404s on refresh and the feature is useless in practice. `/api/*` and paths with a
  file suffix still return real 404s, so a typo'd bundle path does not come back as HTML.
- A **Copy link** control in the header, with a fallback for browsers that withhold
  `navigator.clipboard` over plain http.
- Vitest added and wired into CI; 34 frontend tests.

**Two bugs found by actually opening the app in a browser**

Both passed every automated check first:

1. **Pagination did not round-trip.** The URL carried `page=2` but the view rendered page 1
   — `page`, `pageSize`, and `record` were being filtered out before reaching
   `ErrorAnalysis`. Fixed by widening its props and reporting state changes back so the
   address bar follows.
2. **A restored record opened an empty panel.** Clicking a row resolves which pipeline's
   detail to show; restoring from a URL set only the record id, so the detail fetch — which
   requires both — never ran. The resolution is now a shared pure function
   (`lib/detailPipeline.ts`) used by both paths, with tests asserting they agree.

Verified in a browser: a link carrying benchmark, pipeline filter, metric value, page, and
record id reopens with page 2 showing `26–50 of 62` and the record detail panel populated
with the question, both SQL statements, and evaluation metrics.

**Then (plan items 2.7–2.9)** — the rest of Phase B:

- **Code splitting.** Route views load on demand; the entry chunk carried all eleven
  whether or not they were opened. 556 KB → 401 KB (gzip 167 → 127 KB) across 12 chunks.
  The CI budget was changed to measure the *entry* chunk rather than the sum of all JS —
  summing every chunk would have gone *up* after splitting, hiding exactly what the budget
  exists to guard.
- **Asset revalidation.** Data-root assets (benchmark logos) were served `no-store`, so
  every page view re-downloaded every logo. They now carry an ETag from size and mtime.
- **Async audit.** Both `async def` endpoints reached `get_index()` through sync helpers
  (`execute_sql_for_record → _resolve_record_db_id → get_index`, and
  `playground_evaluate → _find_eval_record_optional → get_index`). `get_index()` builds
  the index when it is missing or stale — 4.6 s for a 415 MB artifact — so an unlucky
  first request would have stalled the event loop and every other in-flight request with
  it. Both now warm the index via `asyncio.to_thread` first. A structural test walks the
  call graph and fails if a new async endpoint reintroduces the path.

Also corrected a test that was asserting nothing: the path-traversal check on
`/api/static/` used a plain `../`, which HTTP clients normalise away before sending, so
the request never reached the handler. It now uses encoded traversal, which does reach it;
the containment check holds.

**Not yet done in Phase C:** the stable pipeline-hash alias (1.6). Phase B's data-fetching
work (TanStack Query, list virtualisation) is also outstanding.

---

## 2026-08-24 — Phase B: artifact index landed (plan items 2.1–2.2)

The backend half of the performance work. Endpoints are not rewired yet; this entry
covers the index itself, which everything else in Phase B depends on.

**What was built**

- `indexing/scanner.py` — streams an evaluation artifact and reports the exact byte range
  of every top-level record, with bounded memory. Brace counting is only valid outside
  string literals, so it tracks string state and backslash escapes including across read
  boundaries. Measured at 144 MB/s.
- `indexing/builder.py` — walks the artifact once and writes a SQLite index holding
  per-record identity and byte range, the full `evaluation` block per (record, pipeline),
  and numeric metrics in a tall indexed table. Atomic (temp file + rename), self-
  invalidating on source size/mtime/schema change, and disposable.
- `indexing/store.py` — the read API endpoints will use: filtered/paginated listing,
  aggregates, cross-pipeline confusion, and single-record reads by byte range.
- CLI: `text2sql-eval-toolkit index build` and `index status`.

**Measured on a 415 MB artifact** (the real 15 MB file scaled ×30; the largest artifact
available locally, since `data/results/` holds only one benchmark)

| Operation | Before (full parse) | Index | Change |
|---|---|---|---|
| List, page 1 | 1,087 ms | 1.49 ms | 730× |
| List, page 40 | 951 ms | 2.09 ms | 455× |
| Record detail | 921 ms | 0.33 ms | 2,818× |
| Peak RSS | **2,151 MB** | **170 MB** | independent of artifact size |
| Index size | — | 57 MB | 14% of source |

The memory column is the one that matters for deployment: parsing a 415 MB artifact once
cost 2.1 GB of RSS, so the frontend's 100 MB "may crash the server" warning was
well-founded. Index-backed serving is flat regardless of artifact size, which is what
makes a 4 GB VM viable. Extrapolating 14% to the full ~7 GB result set gives roughly
1 GB of indices — comfortable on the 40 GB Hetzner disk.

Build cost is 4.6 s for 415 MB, so the whole published set indexes in a few minutes as a
one-time provisioning step.

**Correctness**

125 new tests. The important ones are differential: `test_indexing_differential.py`
reimplements the previous endpoint filtering verbatim and asserts the index returns
identical record ids, ordering, totals, and evaluation payloads across 80+ filter
combinations, plus pagination stability and confusion matrices. The scanner is tested
against braces-in-strings, escaped quotes and backslashes, and non-ASCII input, each
re-scanned at every chunk boundary from 1 byte upward.

Two divergences were found and closed while writing those tests:

- **Booleans.** The first builder excluded `bool` from numeric metrics, but the endpoints
  test `isinstance(v, (int, float))`, which `bool` passes. A boolean metric would have
  been silently dropped, making a filter return nothing where the old code returned
  matches. Now indexed as 1.0/0.0.
- **Index size.** The first schema stored 55-character pipeline ids and 19-character
  metric names on every metric row: 8.5 MB of index for a 15 MB source (57%). Interning
  both to integers cut it to 13%, which is what makes the 7 GB set tractable.

**Then (plan items 2.3–2.6)** — endpoints rewired onto the index:

- `list_errors` filters, counts, and paginates in SQL.
- `get_error_detail`, `get_error_detail_for_pipeline`, `_resolve_record_db_id`, and
  `_find_eval_record_optional` read one record by stored byte range.
- Both insight confusion endpoints aggregate in SQL.
- `get_benchmark_summary_by_category` still needs a whole-corpus pass but now streams one
  record at a time instead of materialising the artifact.
- `EVAL_RECORDS_CACHE` (unbounded, never invalidated) is gone; index handles are cached
  instead and a changed source file invalidates its handle, so an evaluation re-run is
  picked up without a restart.
- `count_records` is cached on file size and mtime — the landing page was re-parsing every
  benchmark data file on every request.
- The large-benchmark OOM warning is retired: `isLargeBenchmark`, the "Large" tag, and the
  banner are gone, since memory no longer scales with artifact size.

**Three toolchain defects surfaced by actually running things**

The Phase A CI workflow would have failed on its first run, which is worth recording since
the branch has still never been pushed:

1. **`npm ci` failed** — `package-lock.json` was out of sync with `package.json`. Regenerated.
2. **`npm run lint` was declared but ESLint was never a dependency**, so the script errored.
   Added ESLint with a conservative flat config.
3. **`vite build` does not type-check**, so the ~7.2k lines of TypeScript had never been
   checked. Added a `typecheck` script; it currently reports 18 pre-existing errors,
   including 7 duplicate-`key` warnings where a Carbon `getHeaderProps()` spread overwrites
   an explicit `key`.

Two real frontend defects were fixed along the way: a ref assigned during render in
`RunEvaluationView`, and an unused catch binding in `api.ts`.

**Deferred with documented switches** (plan item 4.13): 20 ESLint findings and the 18 type
errors. All need effects restructured, which the routing work will largely redo — fixing
them now would be immediately rewritten.

**Not yet done in Phase B:** HTTP-level caching (2.7), frontend data-fetching/code-splitting
/virtualisation (2.8), and the async-handler audit (2.9).

---

## 2026-08-24 — Release strategy set: `dashboard-v2`, shipping as 2.0.0

All remaining phases land on a single branch, `dashboard-v2`, branched from the Phase A
work. Nothing is pushed until the whole programme is complete and comprehensively tested;
CI therefore runs for the first time on a finished branch.

The release is **2.0.0** — a major bump is the honest number given the URL scheme,
capability tiers, artifact index, and deployment model, and it supersedes the 1.1.0 /
1.2.0 skew rather than requiring it to be adjudicated. The version change is the *last*
commit, after the test pass.

One consequence to carry forward: `DEFAULT_REVISION` is derived as `v{version}`
(`results/_hub.py:39`), so 2.0.0 will request a `v2.0.0` tag on the Hugging Face results
repo and fall back to `main` with a warning if it does not exist. Publishing that Hub tag
is on the release checklist (plan item 4.8).

`phase-a-foundations` is kept as a local marker for where Phase A ended.

---

## 2026-08-24 — Phase A started: tooling and CI foundations

Branch `phase-a-foundations`. First implementation step of
a set of plan documents (since removed), addressing observation 4 of the baseline snapshot below.

**Landed**

- `pyproject.toml`: added `[tool.black]`, `[tool.ruff]`, `[tool.pytest.ini_options]`,
  `[tool.mypy]`, `[tool.coverage]`, and a `dev` optional-dependency group. Tool config now
  lives in one file, so local runs match CI.
- `.github/workflows/ci.yml`: the repo's first CI. Jobs — lint (ruff + black), typecheck
  (mypy, narrow scope), tests on Python 3.11/3.12/3.13, an advisory 3.14 job, and a
  dashboard job running lint, build, a bundle-size budget, and a staleness warning for the
  committed `dashboard/dist`.
- `tests/conftest.py`: registers the `integration` marker and auto-marks anything under an
  `*integration*` path, so a new file cannot silently start requiring credentials in the
  default run. Adds a `require_env` skip helper.
- `tests/test_run_experiment_integration.py`: marked `integration`. The default `pytest`
  run is now hermetic and can gate CI.
- `CONTRIBUTING.md`: reconciled with what is actually enforced.

**Deliberately not done**

- **The 1.1.0 / 1.2.0 version skew was left alone.** Bumping it changes `DEFAULT_REVISION`
  in `results/_hub.py`, which selects the Hugging Face snapshot tag — a user-visible
  behaviour change that does not belong in a tooling commit. Only `v1.0.0` is tagged in
  git, so whether 1.1.0 or 1.2.0 was actually released is ambiguous and needs the
  maintainer's answer. Stays in Phase E (plan item 4.8).
- No repo-wide formatting pass. Ruff's initial rule set is deliberately conservative
  (`E4`, `E7`, `E9`, `F`, `B`); import sorting and pyupgrade rewrite many files and belong
  in their own commit.

**Toolchain**

Installed `uv` (Homebrew) and CPython 3.13.15, created `.venv`, and installed
`-e ".[dev,dashboard]"`. The full toolchain now runs locally, so the baseline below is
measured rather than assumed.

**Measured baseline**

| Check | Result |
|---|---|
| `ruff check src tests scripts` | 83 findings → **0** (40 auto-fixed, 39 deferred with documented ignores) |
| `black --check` | 44 of 64 files reformatted → **clean** |
| `mypy` | **clean** on the configured scope (5 files) |
| `pytest` | 2 failures → **104 passed, 5 deselected** |

**The two test failures were a real pre-existing bug, not a tooling artifact.**
`sqlglot >=28` made `Rank`, `DenseRank`, `Lag`, `Lead`, `PercentRank`, `CumeDist`,
`FirstValue`, `LastValue`, and `NthValue` subclasses of `exp.AggFunc`.
`analyze_sql_query()` counts `exp.AggFunc` nodes directly, so `SELECT RANK() OVER (...)`
began reporting `query_aggregate_count=1` and picking up the `has_aggregation` tag. Since
that field drives the category breakdowns in the dashboard and the generated summary
reports, **any profiling run on a recent sqlglot silently mis-classified pure ranking
queries as aggregating** — the declared constraint is `sqlglot>=27.0.0` and it resolved to
30.17.0. Fixed in `a8f2c96`; windowed true aggregates such as `AVG(x) OVER (...)` are
still counted, matching the existing test expectations.

Found by running the test suite locally once the toolchain existed — not by CI, which has
not run yet (see status below). Still the first concrete return on the Phase A work: an
unpinned-dependency regression that nothing in the repo was positioned to notice.

**Follow-ups opened**

- 39 deferred Ruff findings (`B904` ×19, `B007` ×11, `B905` ×5, `E722` ×4) — plan item
  4.12. Each changes runtime behaviour, so they need a reviewed commit rather than being
  folded into tooling setup.
- Dependency pinning: the sqlglot drift suggests the unbounded `>=` constraints deserve
  upper bounds or a tested lockfile — folded into plan item 4.7.

**CI status: written, validated, never executed.** `.github/workflows/ci.yml` is committed
on a local branch with no upstream, so no workflow run exists. The file parses as YAML and
passes `actionlint` clean, but that is static validation — whether the jobs actually
succeed (dependency install on three interpreters, `npm ci`, the bundle-size budget)
is unknown until the branch is pushed. Treat every job as unproven until then.

**Commits** (branch `phase-a-foundations`, not pushed)

```
7f68087 build: add lint, type, and test tooling with CI
deb75a8 refactor: remove dead imports and empty f-string prefixes
a8f2c96 fix: stop counting ranking window functions as aggregations
a7a3aa9 style: apply Black formatting repo-wide
9d61dc7 chore: ignore the Black reformat in git blame
```

---

## 2026-08-24 — Baseline snapshot before the dashboard/quality refactor

Snapshot taken at `main` @ `60dd451` ahead of a four-goal refactor (shareable URLs,
performance, public deployment, code quality). This entry records what exists today and
which observations the plan is built on.

### Where the project is

`text2sql-eval-toolkit` v1.1.0 — a Python library, CLI, and local web dashboard for
evaluating text-to-SQL systems. Five stages: **inference → execution → evaluation →
profiling → analysis**. Published on PyPI; pre-computed results (~7 GB) distributed via
the Hugging Face Hub. Functionally complete and in active use; the gaps below are about
sharing, scale, and hygiene rather than missing features.

**Shape of the code**

| Area | Location | Size |
|---|---|---|
| Python package | `src/text2sql_eval_toolkit/` | ~13.6k lines |
| Dashboard backend | `ui/server.py` | 2,549 lines, 28 endpoints (19 GET / 9 mutating) |
| Agentic pipeline | `inference/agentic_pipeline.py` | 2,338 lines, 6 versions (v0–v5) |
| Execution engine | `execution/execution_tools.py` | 1,448 lines, 5 DB backends |
| Dashboard frontend | `dashboard/src/` | ~7.2k lines, React 18 + Carbon + Vite |
| Tests | `tests/` | 10 files |

**Data model.** One JSON file per benchmark accumulates through the stages
(`{benchmark}-predictions.json` → `_eval.json` → `_eval_summary.{json,csv,md}`). Records
carry `predictions: {pipeline_id: {...}}`; result dataframes are inlined as pandas
`orient='split'` JSON. `pipeline_id` is derived from model + pipeline variant and is the
unit of comparison everywhere.

### Observations behind the plan

Each item below was verified against the code at this commit.

**1. The dashboard has no URL state at all.**
Navigation is nine `useState` values in `dashboard/src/pages/App.tsx:61-70`
(`activeView`, `selectedBenchmark`, `selectedPipeline`, plus filter state). There is no
router dependency in `dashboard/package.json`, and no `window.location`, `pushState`, or
`useSearchParams` anywhere under `dashboard/src/`. Every view renders at `/`. Nothing is
linkable, bookmarkable, or restorable on reload; the back button leaves the app.
`ErrorAnalysis.tsx` alone holds ~15 pieces of shareable state (pipeline, metric, value,
op, pipeline2, metric2, disagree, page, pageSize, search, selected record, view mode).

**2. Large evaluation artifacts are re-parsed per request.**
Two loading paths coexist. `load_eval_records()` (`ui/server.py:585`) caches parsed
records in a process-global dict — used by 3 endpoints. The other 8 call
`load_json(eval_path)` directly with no caching, including the two hottest ones:
`list_errors` (`:994`) and `get_error_detail` (`:1149`). Fetching a *single record's*
detail therefore parses the entire eval JSON and linearly scans it. The frontend already
concedes the problem: `dashboard/src/lib/largeBenchmark.ts:4` flags eval files ≥100 MB as
liable to OOM the server, and the UI shows a warning instead of the data. The cache that
does exist is unbounded, never evicted, and never invalidated on file change.

**3. The dashboard is not safe to expose publicly in its current form.**
Nine mutating endpoints are unauthenticated. Two of them execute arbitrary
caller-supplied SQL against whatever database credentials the server holds:
`POST /api/benchmarks/{id}/execute` (`:1555`) and
`POST /api/benchmarks/{id}/playground/evaluate` (`:1792`). Others spend the operator's
LLM credits (`evaluate_benchmark` `:2200`), mutate benchmark data files
(`add_ground_truth_sql` `:1606`), rewrite the benchmark registry (`create_benchmark`
`:703`, `update_benchmark` `:728`), upload files (`upload_benchmark_logo` `:744`), and
overwrite YAML inside the installed package directory (`update_llm_judge_config`
`:2174` — the `{name}` path segment is also unsanitized and needs a traversal check).
There is no auth layer, no read-only mode, and no deployment artifact (no Dockerfile, no
compose file, no hosting config anywhere in the repo).

**4. No CI, and no tooling configuration to enforce quality.**
`.github/` contains only `dco.yml` — there are no workflows. `pyproject.toml` has no
`[tool.ruff]`, `[tool.black]`, `[tool.pytest.ini_options]`, or `dev` extra; CONTRIBUTING
tells contributors to `pip install pytest black ruff mypy` by hand. Nothing runs tests,
linting, or formatting on push or PR.

**5. Smaller correctness and hygiene issues.**
- Version skew: `pyproject.toml` and `__init__.py` say `1.1.0`, while `CHANGELOG.md`
  documents a `[1.2.0] - 2026-05-13` release whose features (the `results fetch` CLI) are
  present in the code. The bump was missed.
- The benchmark registry is duplicated at `data/benchmarks.json` and
  `src/text2sql_eval_toolkit/data/benchmarks.json`, and the copies **have already
  drifted** — the packaged one lacks the `logo` fields and several `db_engine` keys. The
  repo copy shadows the packaged one whenever cwd is the checkout, so the divergence is
  invisible in development and only affects pip-installed users.
- `requirements.txt` is a pinned freeze that overlaps and disagrees with the
  `dependencies` list in `pyproject.toml` (it carries `datasets`, `evaluate`, `aiohttp`,
  which are not project dependencies). Two sources of truth.
- `list_benchmarks` (`:630`) re-reads and re-parses every benchmark data file on every
  call just to count records — uncached, on the landing-page request path.

### Environment note

The working checkout has no `uv`, no virtualenv, and only the system Python 3 from Command
Line Tools. Lint/type-check tooling could not be executed for this snapshot, so item 4
reflects *configuration* that is verifiably absent rather than a measured violation count.
Establishing that baseline is the first task in the code-quality plan.
