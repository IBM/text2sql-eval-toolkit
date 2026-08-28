# Text-to-SQL Evaluation Toolkit

A Python library, CLI and web dashboard for evaluating text-to-SQL systems.

```bash
pip install "text2sql-eval-toolkit[dashboard]"
text2sql-eval-toolkit results fetch      # pre-computed results, ~4 GB
text2sql-eval-dashboard --open-browser
```

## Why this exists

Judging a generated query is harder than comparing strings. The same question
often has several correct formulations; execution-based checks disagree on
whether the column set must match exactly; and knowing *why* a model failed
matters as much as knowing that it did. The toolkit takes positions on all
three — multiple ground truths, column-name-insensitive comparison, and error
analysis as a first-class stage.

## Five stages

Each is usable standalone or chained, and each is resumable — existing results
are reused unless you force a re-run.

| Stage | What it does |
|---|---|
| **Inference** | An LLM generates SQL for each question |
| **Execution** | Ground-truth and predicted SQL are run; result sets stored |
| **Evaluation** | Execution match, SQL equivalence, LLM-as-judge |
| **Profiling** | SQL feature tags, for slicing results |
| **Analysis** | Markdown and chart reports, and the dashboard |

## Where to go next

- **[API reference](reference/index.md)** — every exported function and class.
- **[Dashboard](dashboard/README.md)** — browsing results, comparing pipelines,
  error analysis, and running it for other people.
- **[Repository](https://github.com/IBM/text2sql-eval-toolkit)** — installation
  from source, benchmark setup, and contributing.

## Citation

```bibtex
@article{HassanzadehPPKZVGSPR26,
  title   = {Text-to-{SQL} Evaluation Toolkit},
  volume  = {19},
  url     = {https://doi.org/10.14778/3827998.3828071},
  doi     = {10.14778/3827998.3828071},
  number  = {12},
  journal = {Proc. VLDB Endow.},
  author  = {Hassanzadeh, Oktie and Perlitz, Yotam and Pham, Nhan and Kaple, Tanvi and \.{Z}r\'{o}bek, Karolina and Vu, Long and Glass, Michael and Subramanian, Dharmashankar and Pourreza, Mohammadreza and Rafiei, Davood},
  year    = {2026},
  pages   = {4582--4585},
}
```
