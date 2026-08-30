# Guide

The [API reference](../reference/index.md) documents every exported function
signature by signature. This section is the other half: what the toolkit is made
of, how the pieces fit, and which one you want.

| Page | Read it when |
|---|---|
| [Getting started](getting-started.md) | Installing, and getting something on screen |
| [The five stages](stages.md) | Deciding which stage you actually need |
| [Data model](data-model.md) | Reading or writing the predictions file yourself |
| [Benchmarks](benchmarks.md) | Adding a benchmark, or resolving a path that will not resolve |
| [Models and providers](models.md) | Pointing the toolkit at an LLM |
| [LLM-as-judge](llm-judge.md) | Scoring predictions no execution check can settle |
| [Command line](cli.md) | Driving the toolkit without writing Python |
| [Configuration](configuration.md) | Working out which environment variable is missing |

## What is in the box

Three things, in one distribution:

- **A Python library.** 43 exported functions and classes covering all five
  stages. This is the stable surface — it is what other people import, and it
  does not change because the dashboard needs something.
- **Two command-line tools.** `text2sql-eval-toolkit` manages pre-computed
  results and the query index; `text2sql-eval-dashboard` serves the web UI.
- **A web dashboard.** React and Carbon over a FastAPI backend, for browsing
  results, comparing pipelines and doing error analysis. It ships built inside
  the wheel, so `pip install` is all it takes.

Plus the benchmark registry and about 4 GB of pre-computed results, which are
fetched on demand rather than shipped.

## The shape of the thing

```
question ──► inference ──► execution ──► evaluation ──► profiling ──► analysis
             (LLM writes    (run both     (score the     (tag SQL     (reports,
              the SQL)       queries)      prediction)    features)    dashboard)
```

Each stage reads and writes **one JSON file per benchmark**, which accumulates
as it goes. That single file is why the stages compose: any of them can be run
on its own against a file another stage produced, by another person, on another
machine. See [Data model](data-model.md).

Every stage is **resumable**. Existing predictions and evaluations are reused
unless you pass `force_rerun=True`, so a run that dies partway through 2,000
questions picks up where it stopped instead of paying for the first 1,900 again.
