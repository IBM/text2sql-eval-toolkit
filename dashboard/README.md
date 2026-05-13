## Text2SQL Evaluation Dashboard

The dashboard is a lightweight web UI for browsing evaluation results, comparing model performance, and performing error analysis for text-to-SQL experiments.

It is built with a **FastAPI** backend and a **React** frontend with a professional, accessible interface.

### Features

- **Benchmark overview**: Fixed-height, paginated tables listing all benchmarks with description, DB type, number of records, and number of pipelines.
- **Per-benchmark results**: Pipeline-level metrics (execution accuracy variants, LLM-as-judge score, token and latency stats) similar to `data/results/README.md`, with sortable metrics and pagination.
- **Error analysis**:
  - Search by question text or record id.
  - Filter records where a given pipeline/metric matches conditions (e.g. `execution_accuracy = 0 AND llm_score = 1`).
  - Cross-pipeline disagreement filters (e.g. pipeline 1 metric = 0 while pipeline 2 = 1).
  - Fixed-height, paginated lists for efficient browsing.
- **Compare result sets**: Side‑by‑side comparison of two summary files for a benchmark, showing left/right metric values and deltas for each pipeline.
- **LLM judge configuration**: View and edit LLM‑as‑judge YAML configs through a simple editor UI.
- **Run evaluations**: Trigger new evaluations for a benchmark and monitor job status from the UI.

### First run — fetch pre-computed results

Before starting the dashboard for the first time, download the pre-computed
evaluation results from the Hugging Face Hub (~7 GB):

```bash
text2sql-eval-toolkit results fetch
```

This populates `${TEXT2SQL_DATA_ROOT:-./data}/results/` with all benchmark
artefacts. You only need to do this once. To fetch only a single benchmark:

```bash
text2sql-eval-toolkit results fetch --benchmarks bird_mini_dev_sqlite
```

See `text2sql-eval-toolkit results --help` for all options.

### Running the dashboard

After installing the toolkit from source with the dashboard extras:

```bash
uv pip install -e ".[dashboard]"
```

or with pip:

```bash
pip install -e ".[dashboard]"
```

you can start the dashboard with a single command:

```bash
text2sql-eval-dashboard --open-browser
```

By default the server listens on `http://127.0.0.1:8000` and, if `--open-browser` is used, opens your default browser to the dashboard.

#### Data location

The backend expects evaluation artifacts under a configurable data root:

- Set `TEXT2SQL_DATA_ROOT` to point to the directory that contains a `results/` folder with files like:
  - `{benchmark}-predictions_eval.json`
  - `{benchmark}-predictions_eval_summary.json`
- If `TEXT2SQL_DATA_ROOT` is not set, the server defaults to `./data`.

For example, in a cloned repo where results live under `data/results/`, you can run:

```bash
export TEXT2SQL_DATA_ROOT="$(pwd)/data"
text2sql-eval-dashboard --open-browser
```

### Development

For UI development, you can run the backend and frontend separately:

1. Start the FastAPI backend:

```bash
TEXT2SQL_DATA_ROOT="$(pwd)/data" uvicorn text2sql_eval_toolkit.ui.server:app --reload
```

2. Install frontend dependencies and start the Vite dev server:

```bash
cd dashboard
npm install
npm run dev
```

The Vite dev server proxies `/api` calls to `http://127.0.0.1:8000`, so you can iterate on the React UI with hot reload while using the Python backend.

#### Rebuilding for `text2sql-eval-dashboard` (port 8000)

The `text2sql-eval-dashboard` command mounts the **production build** from `dashboard/dist/` (see `mount_static` in the Python server).

**Auto-rebuild (default in a dev checkout):** When `dashboard/package.json` is found (next to your cwd or the repo root), the server starts **`vite build --watch`** in the background so edits under `dashboard/src/` rebuild into `dashboard/dist/` without running `npm run build` manually. Refresh the browser after each rebuild (Vite prints a completion line in the terminal).

- Disable watch (serve existing `dist` only): `text2sql-eval-dashboard --no-watch-dashboard`
- Force watch even if auto-detection changes in the future: `text2sql-eval-dashboard --watch-dashboard`

Requires **Node.js/npm** and `cd dashboard && npm install` once so `node_modules` exists.

**Manual rebuild** (if you disabled watch or need a one-off build):

```bash
cd dashboard
npm install   # if needed
npm run build
```

