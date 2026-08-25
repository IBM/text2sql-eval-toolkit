# Goal 1 — Shareable URLs

Make every artifact the dashboard can display reachable by a URL that can be pasted into
Slack, a paper, or an issue, and that restores the exact same view.

## Current state

The dashboard is a single-route app. All navigation is React state in
`dashboard/src/pages/App.tsx:61-70`; there is no router dependency and no code anywhere
under `dashboard/src/` that reads or writes `window.location`. Consequences:

- No view except the home page can be linked to.
- Reload always returns to the benchmark list; deep state is lost.
- The browser back button exits the app instead of stepping back a view.
- `ErrorAnalysis.tsx` builds up ~15 filter/selection values that vanish on navigation —
  the most valuable state in the app to share, and the least recoverable.

## Target URL scheme

Path segments identify *what* you are looking at; query parameters carry *how* it is
filtered. Human-readable, stable, and diffable.

```
/                                                     benchmark list (home)
/b/:benchmarkId                                       benchmark summary
/b/:benchmarkId/pipeline/:pipelineId                  pipeline detail
/b/:benchmarkId/errors                                error analysis (filters in query)
/b/:benchmarkId/errors/:recordId                      record detail
/b/:benchmarkId/insights                              metric insights
/b/:benchmarkId/compare                               pipeline comparison
/b/:benchmarkId/compare/profile                       profile-based comparison
/llm-judge/:configName?                               judge config viewer
/run                                                  run evaluation (full mode only)
```

Error-analysis filter state, all optional with documented defaults:

```
/b/bird_mini_dev_sqlite/errors
  ?pipeline=wxai:openai/gpt-oss-120b-greedy-zero-shot-chatapi
  &metric=execution_accuracy&op=eq&value=0
  &pipeline2=...&metric2=...&disagree=true
  &q=customers&page=3&pageSize=25
```

`pipelineId` contains `/` and `:` (e.g. `wxai:openai/gpt-oss-120b-...`), so it **must** be
percent-encoded in path position. Encode/decode belongs in one helper module, used by
every link site — hand-rolled `encodeURIComponent` calls scattered through views is how
this breaks.

## Work items

### 1.1 Introduce routing
Add `react-router-dom` to `dashboard/package.json`. Use `createBrowserRouter` with a
route tree mirroring the scheme above. Requires an SPA fallback on the server so deep
links survive a hard refresh — see [1.5](#15-server-side-spa-fallback).

*Acceptance:* every view is reachable by typing its URL; reload preserves the view.

### 1.2 Replace `activeView` with route state
Delete the `activeView` union and the nine navigation `useState` values in `App.tsx`.
Views receive `benchmarkId` / `pipelineId` from `useParams()`. The existing
`openErrorAnalysis`-style callbacks become `navigate()` calls. The fallback-benchmark
effect (`App.tsx:108-124`) becomes a route loader or redirect rather than an effect that
races with render.

*Acceptance:* `App.tsx` holds no view-selection state; back/forward step through views.

### 1.3 Sync filter state to the query string
A `useUrlState` hook binding a typed filter object to `useSearchParams`, with defaults
omitted from the URL so shared links stay short. Filter changes use `replace` (no history
entry per keystroke); view changes use `push`. Debounce the free-text search before it
reaches the URL.

*Acceptance:* setting any error-analysis filter updates the URL; pasting that URL in a new
tab reproduces the filtered view exactly; typing in search does not flood history.

### 1.4 Copy-link affordance
A "Copy link" control on error analysis, record detail, pipeline detail, and comparison
views, copying the current absolute URL. Cheap, and it makes the feature discoverable.

*Acceptance:* control present on the four views and copies a URL that round-trips.

### 1.5 Server-side SPA fallback
`mount_static()` in `ui/server.py:2438` currently mounts `dashboard/dist`. Unknown
non-`/api` paths must return `index.html` (200) rather than 404, or deep links break on
refresh. Do not blanket-catch `/api/*` — those must keep returning real 404s.

*Acceptance:* `curl -I http://host/b/spider_dev/errors` returns 200 HTML; a bad API path
still returns 404 JSON.

### 1.6 Stable identifiers
`pipelineId` embeds the model name, so results published under one name are unlinkable if
the model string later changes. Add a short stable hash alias
(`/p/:pipelineHash`) resolving server-side to the full id, and keep the readable form as
the canonical URL. Worth doing now — retrofitting alias support after links are in
circulation means broken links.

*Acceptance:* both readable and hashed forms resolve to the same view; unknown ids render
a clear "not found" state rather than a blank page.

### 1.7 Not-found and permission states
Every route needs an explicit state for "benchmark/record/pipeline does not exist here"
and, once Goal 3 lands, "this view is unavailable at your capability tier". A shared link is
the one case where the target reliably might not exist on the recipient's server.

*Acceptance:* each route renders an actionable message, never a blank page or a crash.

## Testing

- Unit: URL encode/decode round-trip for pipeline ids containing `/`, `:`, spaces, and
  non-ASCII; filter serialization with defaults omitted.
- Component: render each route from a URL and assert the correct view mounts.
- E2E (Playwright, introduced in Goal 4): navigate → copy URL → open in a fresh context →
  assert identical rendered state. This is the test that actually proves the goal.

## Risks

- **Scope creep into a rewrite.** `ErrorAnalysis.tsx` is 1,188 lines and will be tempting
  to restructure while routing is added. Land routing first; refactor separately.
- **URL length.** Long pipeline ids in both `pipeline` and `pipeline2` plus a search term
  can approach practical limits. The hash alias (1.6) mitigates this.
- **Back-button churn.** Getting push-vs-replace wrong makes the back button useless.
  Covered explicitly in 1.3.
