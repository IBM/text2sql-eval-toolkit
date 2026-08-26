# Dashboard development

The dashboard is a React app (`dashboard/`) served by a FastAPI backend
(`src/text2sql_eval_toolkit/ui/`). The production build in `dashboard/dist/` is
**committed**, so running the dashboard needs no Node.js — but changing the
frontend does.

## Two ways to work on it

**Vite dev server** — hot reload, best for UI work:

```bash
# backend
TEXT2SQL_DATA_ROOT="$(pwd)/data" uvicorn text2sql_eval_toolkit.ui.server:app --reload

# frontend
cd dashboard && npm install && npm run dev
```

Vite proxies `/api` to `http://127.0.0.1:8000`.

One caveat: `uvicorn …:app` serves the ASGI app directly, which skips `main()`.
Session middleware, the deployment mode and proxy-header handling are all
configured there, so sign-in does not work under this setup — it returns a 503
saying so. Use `text2sql-eval-dashboard` when testing anything auth-related.

**`text2sql-eval-dashboard`** — serves the production build from
`dashboard/dist/`. In a source checkout it runs `vite build --watch` in the
background, so edits rebuild automatically; refresh the browser after each one.

```bash
text2sql-eval-dashboard --no-watch-dashboard   # serve the committed build only
text2sql-eval-dashboard --watch-dashboard      # force the watcher
```

The watcher needs `npm install` to have been run in `dashboard/` once.

## Before committing frontend changes

`dashboard/dist/` is checked in, so a source change without a rebuild ships a
stale UI:

```bash
cd dashboard
npm run build
npx tsc --noEmit -p .   # vite build does NOT type-check
npm run lint
npm test
```

`vite build` succeeding is not evidence the code type-checks — that has hidden
real errors before.

## Tests

- **Vitest** (`npm test`) — the URL scheme, the alias layer, metric clamping, plus
  component tests that mount real views against stubbed APIs.
- **Playwright** (`npm run e2e`) — end-to-end tests that copy a link and reopen it
  in a fresh browser context. They run against a synthetic data root built by
  `scripts/ci/make_e2e_fixture.py`, so they need no results snapshot.

The end-to-end suite is the only thing that tests what shareable links actually
claim, and it has caught defects nothing else did — a back button that did
nothing, a link to an absent benchmark silently opening a different one, and a
cold load applying a filter to the address but not to the results.

## Conventions

- Carbon Design System components; the theme is `g10`.
- The URL is the source of truth for view state. A view reads position from props
  and reports changes upward rather than owning it — see `ErrorAnalysis` and
  `PipelineDetailView`.
- Result tables are paginated and server-previewed. Query results are unbounded;
  rendering one whole cost 854,563 DOM nodes and 858 MB of heap before this was
  fixed.
