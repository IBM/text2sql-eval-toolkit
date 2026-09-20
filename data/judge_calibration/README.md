# LLM-judge calibration set

202 predictions of the kind the batch LLM judge is actually asked about —
execution mismatches, where the prediction's result did not match the
reference — from BIRD Mini-Dev (SQLite and PostgreSQL), Spider Dev, Spider
Realistic and Archer, each labelled Yes, Maybe or No with a one-line reason.
It is how the packaged judge configs were chosen in 1.6.0, and how to check a
change to them.

- `calibration_set.jsonl` — one item per line: the benchmark, record and
  pipeline; the split; the label and its reason; `stored_llm_score`, the verdict
  the published results carry, from the Llama 3.3 70B judge; and `inputs`, the
  exact text the batch judge is sent. Scoring a config needs no results
  download and no database.
- `runs/` — verdicts per config, named `{config}-{config digest}-{set digest}`,
  for the two packaged configs and the two retired ones they are compared with.
- `configs/` — those retired Llama configs, kept as baselines.

The stored `inputs` were built before an ellipsis fix: at the time, every later
message in an agentic trace ended in `...` whether or not it had been cut. A
`build` today produces those messages without the marker, which changes the set's
digest and so starts the runs afresh; the runs in `runs/` are the ones made on
the inputs recorded here.

The labels were written by Claude Code, the assistant that also developed the
prompts, and have not been independently reviewed. Reviewing them — above all
the Maybes — is the most useful thing a person can do with this set.

## Splits

No question appears in more than one split.

| Split | Items | Used for |
| --- | --- | --- |
| `tune` | 90 | Developing the prompts. |
| `test` | 60 | Held out at first, then used to diagnose a prompt that did worse there than on `tune`. No longer unseen. |
| `holdout` | 52 | Drawn and labelled after the prompts were written, before any judge had seen it, and never used to change one. |

The holdout is the number to believe. It is also small, and a future prompt
change should be measured on a fresh holdout of its own rather than this one.

## Results on the holdout

28 items labelled No, 14 Yes, 10 Maybe.

| Config | Mean abs. error | Decisive accuracy | Wrong accepted | Correct rejected |
| --- | --- | --- | --- | --- |
| `llm_judge_default_config` — gpt-oss-120b, with ground truth | 0.183 | 88.1% | 2 of 28 | 3 of 14 |
| retired `llm_judge_default_config` — Llama 3.3 70B, same inputs | 0.269 | 78.6% | 7 of 28 | 2 of 14 |
| verdicts in the published results — Llama 3.3 70B, pre-1.6.0 inputs | 0.365 | 64.3% | 9 of 28 | 4 of 14 |
| `llm_judge_no_gt` — gpt-oss-120b, no ground truth | 0.250 | 81.0% | 7 of 28 | 1 of 14 |
| retired `llm_judge_no_gt_v1` — Llama 3.3 70B, no ground truth | 0.423 | 59.5% | 15 of 28 | 2 of 14 |

- **Mean abs. error** — the mean distance between the judge's score and the
  label's, with Yes 1, Maybe 0.5 and No 0.
- **Decisive accuracy** — of the items labelled Yes or No, the share the judge
  scored the same.
- **Wrong accepted** — items labelled No that the judge scored Yes.
  **Correct rejected** — items labelled Yes that it scored No.

A gpt-oss-120b call spends about 500 completion tokens, most of them reasoning,
and takes about 6 seconds; a Llama call about 220 tokens and 5 seconds.

## Reproducing

```bash
python scripts/analysis/judge_calibration.py report --split holdout
```

scores every run in `runs/` against the labels. To measure another config:

```bash
python scripts/analysis/judge_calibration.py run path/to/config.yaml --split holdout
```

Runs are resumable, and a run file is keyed by the config's content and the
set's, so an edited prompt or a rebuilt set starts afresh instead of mixing
verdicts. `build` rebuilds the inputs after a change to how the judge's inputs
are made, and needs the benchmarks' evaluation files locally
(`text2sql-eval-toolkit results fetch`).
