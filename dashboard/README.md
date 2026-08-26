# dashboard/

React frontend for the evaluation dashboard. The FastAPI backend lives in
`src/text2sql_eval_toolkit/ui/`.

Documentation has moved to **[`docs/dashboard/`](../docs/dashboard/)**:

- [Overview and features](../docs/dashboard/README.md)
- [Development](../docs/dashboard/development.md) — Vite dev server, rebuilds
- [Deployment](../docs/dashboard/deployment.md)

The production build in `dist/` is committed, so running the dashboard needs no
Node.js. Rebuild it with `npm run build` after changing anything under `src/`.
