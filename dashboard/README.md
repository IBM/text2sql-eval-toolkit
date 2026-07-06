## Text2SQL Evaluation Dashboard

The dashboard is a lightweight web UI for browsing evaluation results, comparing model performance, and performing error analysis for text-to-SQL experiments.

It is built with a **FastAPI** backend and a **React** frontend with a professional, accessible interface.

### Features

- **Benchmark overview**: Fixed-height, paginated tables listing all benchmarks with description, DB type, number of records, and number of pipelines.
- **Per-benchmark results**: Pipeline-level metrics (execution accuracy variants, LLM-as-judge score, token and latency stats) with sortable metrics and pagination.
- **LLM judge selection**: When multiple judge configurations have stored results for a benchmark, choose which judge's `llm_score` to display (same pattern as pipeline selection). Selection persists across views for the current benchmark.
- **Error analysis**:
  - Search by question text or record id.
  - Filter records where a given pipeline/metric matches conditions (e.g. `execution_accuracy = 0 AND llm_score = 1`).
  - Cross-pipeline disagreement filters (e.g. pipeline 1 metric = 0 while pipeline 2 = 1).
  - Fixed-height, paginated lists for efficient browsing.
- **Compare result sets**: Side‑by‑side comparison of pipeline metrics, showing left/right values and deltas.
- **Profile compare**: Cross-benchmark profile analysis with the same LLM judge filter.
- **LLM judge configuration**: View and edit LLM‑as‑judge YAML config templates (filesystem configs used when *running* new evaluations).
- **Run evaluations**: Trigger new evaluations for a benchmark and monitor job status from the UI (`GET /api/jobs/{job_id}`).

### Data storage

The dashboard reads evaluation data from **SQLite** (`TEXT2SQL_DATABASE_URL`, default `data/text2sql_eval.db`), not from `*-predictions_eval.json` files.

| What | Where |
|------|--------|
| Predictions, execution, evaluations | `text2sql_eval.db` via `BenchmarkStore` |
| Benchmark catalog & gold questions | `data/benchmarks.json`, `data/benchmarks/*.json` |
| Legacy result JSON | Import only — [`scripts/migration/import_json_to_db.py`](../scripts/migration/README.md) |

### First run — seed the database

**Option A — migrate legacy JSON results** (if you have `data/results/*.json`):

```bash
python3 scripts/migration/import_json_to_db.py --init
```

**Option B — fetch from Hugging Face Hub, then import:**

```bash
text2sql-eval-toolkit results fetch
python3 scripts/migration/import_json_to_db.py --init
```

**Option C — run the pipeline** (writes directly to SQLite):

```bash
python scripts/run_experiment.py bird_mini_dev_sqlite
```

### Running the dashboard

After installing the toolkit from source with the dashboard extras:

```bash
uv pip install -e ".[dashboard]"
```

or with pip:

```bash
pip install -e ".[dashboard]"
```

Start the dashboard:

```bash
text2sql-eval-dashboard --open-browser
```

By default the server listens on `http://127.0.0.1:8000`.

#### Environment

```bash
export TEXT2SQL_DATA_ROOT="$(pwd)/data"          # benchmark catalog + db file location
export TEXT2SQL_DATABASE_URL="sqlite:///$(pwd)/data/text2sql_eval.db"
text2sql-eval-dashboard --open-browser
```

### Jobs API

Background evaluation from the UI creates rows in the `jobs` table:

| Endpoint | Description |
|----------|-------------|
| `POST /api/benchmarks/{id}/evaluate` | Start eval job; returns `job_id` |
| `GET /api/jobs/{job_id}` | Poll status (`pending`, `running`, `completed`, `failed`) |
| `GET /api/benchmarks/{id}/jobs` | List recent jobs for a benchmark |

Pipeline scripts also record jobs for `inference`, `execution`, `eval`, and `llm_judge` stages.

### LLM judge API

| Endpoint | Description |
|----------|-------------|
| `GET /api/benchmarks/{id}/llm-judge-configs` | Judge configs with stored results for this benchmark |
| `GET /api/benchmarks/{id}/summary?llm_judge_config_id=N` | Summary filtered to judge *N* |
| `GET /api/benchmarks/{id}/errors?llm_judge_config_id=N` | Error list with judge *N* scores |

The dashboard **LLM judge** dropdown calls the listing endpoint and passes `llm_judge_config_id` to summary, error, and insight requests.

### Development

For UI development, run the backend and frontend separately:

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

The Vite dev server proxies `/api` calls to `http://127.0.0.1:8000`.

#### Rebuilding for `text2sql-eval-dashboard` (port 8000)

The `text2sql-eval-dashboard` command mounts the **production build** from `dashboard/dist/`.

**Auto-rebuild (default in a dev checkout):** When `dashboard/package.json` is found, the server starts **`vite build --watch`** in the background. Refresh the browser after each rebuild.

- Disable watch: `text2sql-eval-dashboard --no-watch-dashboard`

**Manual rebuild:**

```bash
cd dashboard
npm install   # if needed
npm run build
```

