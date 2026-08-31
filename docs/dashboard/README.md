# Evaluation dashboard

A web UI for browsing evaluation results, comparing pipelines, and doing error
analysis. FastAPI backend, React frontend.

Two ways it gets used, and they are configured differently:

- **A local operator tool** — every capability, including running SQL and launching
  evaluations. This is the default when it binds loopback.
- **A shared read-only site** — browsing published results, with optional Google
  sign-in for a small allowlist. See [deployment](deployment.md).

| Guide | Covers |
|---|---|
| [Shareable links](shareable-links.md) | The URL scheme, short pipeline aliases, and what happens when a link does not resolve |
| [Query index](query-index.md) | How GB-scale artifacts are served in milliseconds, and how to build the index |
| [Capability tiers](capability-tiers.md) | `public` / `judge` / `full`, sign-in, and why the judge tier needs no database |
| [Deployment](deployment.md) | Running it for other people: container stack, TLS, provisioning, health checks |
| [Development](development.md) | Vite dev server, rebuilds, and how the frontend is served |

## Quick start

```bash
uv pip install -e ".[dashboard]"
text2sql-eval-toolkit results fetch      # ~4 GB, once
text2sql-eval-dashboard --open-browser
```

Listens on `http://127.0.0.1:8000`. `TEXT2SQL_DATA_ROOT` points at the directory
containing `results/` (defaults to `./data`).

The pre-built frontend is committed to the repository, so running the dashboard
needs no Node.js.

## Features

- **Benchmark overview** — every benchmark with its description, database type,
  record count and pipeline count.
- **Five views of a benchmark**, under a shared tab strip: the summary, metric
  insights, pipeline compare, profile compare and error analysis. Each is its
  own address, so any of the five can be linked to.
- **Per-benchmark results** — pipeline-level metrics (execution-accuracy variants,
  LLM-judge score, token and latency statistics), sortable, and broken down by
  SQL feature category.
- **Error analysis** — search by question or record id; filter on a pipeline and
  metric (`execution_accuracy = 0`); find cross-pipeline disagreement (pipeline 1
  scores 0 where pipeline 2 scores 1); open any record to see both SQL statements,
  both result sets, the prompt, and the judge's reasoning.
- **Metric insights** — confusion matrices between two binary metrics, per pipeline
  and across pipelines, for questions like "where does execution match disagree
  with the judge?"
- **Comparison** — two summary files side by side with per-pipeline deltas.
- **LLM-judge configuration** — view and edit the judge prompt YAML.
- **Run evaluations** — trigger a run and follow job status. Local mode only.
- **Docs** — an index of tiles: the published API reference, and the long-form notes in
  [`docs/notes/`](https://github.com/IBM/text2sql-eval-toolkit/tree/main/docs/notes) — a survey of how text-to-SQL evaluation is done,
  a catalogue of the cases where the metrics disagree, and a demo script. Each
  note has its own address, so a link opens the one being discussed.

  These are read from the repository. `docs/` ships in neither the wheel nor the
  sdist, so a pip install has the reference tile and no notes, and the index
  says so rather than showing an unexplained gap. The deployment image copies
  them in.

Result tables are previewed rather than rendered whole: a query can return
86,502 rows, and the panel says how many of them it is showing.
