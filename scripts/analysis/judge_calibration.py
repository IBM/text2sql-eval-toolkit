#!/usr/bin/env python
#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Measure an LLM-judge config against a labelled set of predictions.

The judge only runs where execution match could not decide -- a prediction whose
result matched the reference is scored 1 without it -- so the set is drawn from
exactly those predictions: execution mismatches from BIRD (SQLite and
PostgreSQL), Spider, Spider Realistic and Archer, balanced across the verdicts
the previous judge gave and spread across pipelines. Each item carries a label
(Yes / Maybe / No, the judge's own scale) with a one-line reason, and the exact
inputs the batch judge would send, so a run needs no results download and no
database. The labels were written by Claude Code, which also developed the
prompts, and have not been independently reviewed.

The set has three splits, and no question appears in more than one:

- ``tune`` (90 items): prompts and settings were developed against these.
- ``test`` (60): held out at first, then used to diagnose the tuned prompt when
  it did worse there than on ``tune`` -- so it is no longer unseen.
- ``holdout`` (52): drawn and labelled afterwards, before any judge was run on
  it, and never used to change a prompt. This is the number to believe; a
  future prompt change should get a fresh holdout of its own.

Usage::

    # Rebuild the judge inputs after changing how they are built. Needs the
    # benchmarks' evaluation files locally (text2sql-eval-toolkit results fetch).
    python scripts/analysis/judge_calibration.py build

    # Run a config over a split; resumable, and keyed by the config's content
    # and the set's, so a changed prompt or rebuilt set starts afresh.
    python scripts/analysis/judge_calibration.py run path/to/config.yaml --split tune

    # Score every run against the labels, with the stored verdicts as a baseline.
    python scripts/analysis/judge_calibration.py report --split test
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import yaml

ROOT = Path(__file__).resolve().parents[2]
SET_DIR = ROOT / "data" / "judge_calibration"
SET_PATH = SET_DIR / "calibration_set.jsonl"
RUNS_DIR = SET_DIR / "runs"

SCORES = {"Yes": 1.0, "Maybe": 0.5, "No": 0.0}
SPLITS = ["tune", "test", "holdout", "all"]
INPUT_KEYS = (
    "question",
    "ground_truth_sql",
    "ground_truth_df",
    "predicted_sql",
    "predicted_df",
    "generation_prompt",
)


# --------------------------------------------------------------------- build


def _items_from_sample(sample_path: Path, labels_path: Path) -> List[Dict[str, Any]]:
    """The items of a first build: a sample, and labels numbered from 1."""
    sample = json.loads(sample_path.read_text(encoding="utf-8"))
    labels = {
        int(row["item"]): row
        for row in map(json.loads, labels_path.read_text(encoding="utf-8").splitlines())
    }
    if sorted(labels) != list(range(1, len(sample) + 1)):
        raise SystemExit("labels do not cover every sampled item exactly once")
    return [
        {**item, "label": labels[i]["label"], "label_note": labels[i]["note"]}
        for i, item in enumerate(sample, 1)
    ]


def build(
    sample_path: Optional[Path], labels_path: Optional[Path], results_dir: Path
) -> None:
    """
    Assemble the self-contained set from its items and the eval files.

    With a sample and labels, the items come from them. Without, they come from
    the set already on disk -- its items, splits and labels kept, only the
    judge's inputs rebuilt -- which is what a change to how those inputs are
    built needs.
    """
    from text2sql_eval_toolkit.evaluation.judge_inputs import build_llm_judge_inputs
    from text2sql_eval_toolkit.utils import get_gt_sqls, parse_dataframe

    if sample_path is not None and labels_path is not None:
        items = _items_from_sample(sample_path, labels_path)
    elif sample_path is None and labels_path is None:
        items = load_set()
    else:
        raise SystemExit("pass both --sample and --labels, or neither")

    records: Dict[str, Dict[str, Any]] = {}
    out: List[Dict[str, Any]] = []
    for index, item in enumerate(items, 1):
        benchmark = item["benchmark"]
        if benchmark not in records:
            path = results_dir / f"{benchmark}-predictions_eval.json"
            records[benchmark] = {
                str(r.get("id") or r.get("question_id")): r
                for r in json.loads(path.read_text(encoding="utf-8"))
            }
        record = records[benchmark][item["record_id"]]
        prediction = record["predictions"][item["pipeline"]]
        gold_sqls = get_gt_sqls(record)
        gold_dfs = (
            record["gt_df"] if isinstance(record["gt_df"], list) else [record["gt_df"]]
        )
        if len(gold_sqls) != 1:
            raise SystemExit(f"item {index} has {len(gold_sqls)} reference queries")

        inputs = build_llm_judge_inputs(
            record,
            prediction,
            gold_sqls[0],
            parse_dataframe(gold_dfs[0]),
            parse_dataframe(prediction["predicted_df"]),
        )
        evaluation = prediction.get("evaluation") or {}
        out.append(
            {
                "id": f"{benchmark}:{item['record_id']}:{item['pipeline']}",
                "benchmark": benchmark,
                "record_id": item["record_id"],
                "pipeline": item["pipeline"],
                "split": item["split"],
                "label": item["label"],
                "label_note": item["label_note"],
                # The verdict the published results carry, from the judge this
                # set exists to replace. A baseline that costs nothing to score.
                "stored_llm_score": evaluation.get("llm_score"),
                # Stringified exactly as the prompt template's str.format() would.
                "inputs": {key: str(inputs[key]) for key in INPUT_KEYS},
            }
        )

    SET_DIR.mkdir(parents=True, exist_ok=True)
    with SET_PATH.open("w", encoding="utf-8") as f:
        for row in out:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    print(f"wrote {len(out)} items to {SET_PATH.relative_to(ROOT)}")


# ----------------------------------------------------------------------- run


def load_set(split: str = "all") -> List[Dict[str, Any]]:
    rows = [
        json.loads(line) for line in SET_PATH.read_text(encoding="utf-8").splitlines()
    ]
    return [r for r in rows if split == "all" or r["split"] == split]


def config_identity(config_path: Path) -> tuple[str, Dict[str, Any]]:
    """A run file name that changes whenever the config's content does."""
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    digest = hashlib.sha256(
        json.dumps(config, sort_keys=True).encode("utf-8")
    ).hexdigest()[:8]
    return f"{config_path.stem}-{digest}", config


def set_identity() -> str:
    """
    A tag that changes whenever the set's content does.

    Part of every run file's name, so a rebuilt set -- different inputs for the
    same items -- can neither resume from nor be reported alongside runs made
    on the old one.
    """
    return "set" + hashlib.sha256(SET_PATH.read_bytes()).hexdigest()[:8]


def run(config_path: Path, split: str, concurrency: int, limit: Optional[int]) -> Path:
    from text2sql_eval_toolkit.env_loader import load_env
    from text2sql_eval_toolkit.evaluation.llm_as_judge import (
        evaluate_sql_prediction_with_llm,
    )

    # Nothing on the judge's import path reads .env, so the provider clients
    # would find no credentials and every item would record an error.
    load_env()

    name, config = config_identity(config_path)
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = RUNS_DIR / f"{name}-{set_identity()}.jsonl"

    done = set()
    if out_path.exists():
        for line in out_path.read_text(encoding="utf-8").splitlines():
            row = json.loads(line)
            if not row.get("error"):
                done.add(row["id"])
    items = [r for r in load_set(split) if r["id"] not in done]
    if limit is not None:
        items = items[:limit]
    print(f"{name}: {len(items)} to judge ({len(done)} already done)", flush=True)

    lock = threading.Lock()

    def judge(item: Dict[str, Any]) -> Dict[str, Any]:
        started = time.monotonic()
        try:
            result = evaluate_sql_prediction_with_llm(
                llm_judge_config=config, **item["inputs"]
            )
            return {
                "id": item["id"],
                "verdict": result["verdict"],
                "score": result["score"],
                "explanation": result["explanation"],
                "token_usage": result["token_usage"],
                "seconds": round(time.monotonic() - started, 2),
            }
        except Exception as exc:  # recorded, not raised: a run keeps going
            return {
                "id": item["id"],
                "error": f"{type(exc).__name__}: {exc}"[:500],
                "seconds": round(time.monotonic() - started, 2),
            }

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(judge, item) for item in items]
        for n, future in enumerate(as_completed(futures), 1):
            row = future.result()
            with lock, out_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
            if n % 10 == 0 or n == len(futures):
                print(f"  {n}/{len(futures)}", flush=True)
    return out_path


# -------------------------------------------------------------------- report


def _latest_rows(path: Path) -> Dict[str, Dict[str, Any]]:
    """The last row per item: a retried item's success replaces its error."""
    rows: Dict[str, Dict[str, Any]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        row = json.loads(line)
        if row["id"] not in rows or not row.get("error"):
            rows[row["id"]] = row
    return rows


def metrics(
    pairs: Iterable[tuple[str, Optional[float], Dict[str, Any]]],
) -> Dict[str, Any]:
    """
    Agreement of judge scores with labels.

    ``pairs`` is (label, judge score or None for an error, raw row). Errors are
    reported separately and scored as 0 in ``mae_with_errors``, which is what a
    batch run would record.
    """
    pairs = list(pairs)
    n = len(pairs)
    answered = [(label, score, row) for label, score, row in pairs if score is not None]
    errors = n - len(answered)
    unparsed = sum(1 for _, _, row in answered if row.get("verdict") == "N/A")
    mae = sum(abs(SCORES[lab] - s) for lab, s, _ in answered) / max(1, len(answered))
    mae_all = sum(
        abs(SCORES[lab] - (s if s is not None else 0.0)) for lab, s, _ in pairs
    ) / max(1, n)
    exact = sum(1 for lab, s, _ in answered if SCORES[lab] == s) / max(1, len(answered))
    decisive = [(lab, s) for lab, s, _ in answered if lab != "Maybe"]
    decisive_acc = sum(1 for lab, s in decisive if SCORES[lab] == s) / max(
        1, len(decisive)
    )
    labelled_no = [s for lab, s in decisive if lab == "No"]
    labelled_yes = [s for lab, s in decisive if lab == "Yes"]
    false_accept = sum(1 for s in labelled_no if s == 1.0) / max(1, len(labelled_no))
    false_reject = sum(1 for s in labelled_yes if s == 0.0) / max(1, len(labelled_yes))
    completion = [
        (row.get("token_usage") or {}).get("completion_tokens")
        for _, _, row in answered
        if (row.get("token_usage") or {}).get("completion_tokens") is not None
    ]
    seconds = [row["seconds"] for _, _, row in pairs if "seconds" in row]
    return {
        "n": n,
        "errors": errors,
        "unparsed": unparsed,
        "mae": mae,
        "mae_with_errors": mae_all,
        "exact": exact,
        "decisive_accuracy": decisive_acc,
        "false_accept": false_accept,
        "false_reject": false_reject,
        "mean_completion_tokens": (
            sum(completion) / len(completion) if completion else None
        ),
        "mean_seconds": sum(seconds) / len(seconds) if seconds else None,
    }


def report(split: str, runs: Optional[List[Path]]) -> None:
    items = {r["id"]: r for r in load_set(split)}
    tables = [
        (
            "stored verdicts (previous judge)",
            metrics(
                (item["label"], item["stored_llm_score"], {}) for item in items.values()
            ),
        )
    ]
    for path in runs or sorted(RUNS_DIR.glob(f"*-{set_identity()}.jsonl")):
        rows = _latest_rows(path)
        covered = [i for i in items if i in rows]
        if len(covered) < len(items):
            name = f"{path.stem} (partial: {len(covered)}/{len(items)})"
        else:
            name = path.stem
        tables.append(
            (
                name,
                metrics(
                    (
                        items[i]["label"],
                        None if rows[i].get("error") else rows[i]["score"],
                        rows[i],
                    )
                    for i in covered
                ),
            )
        )

    header = (
        "| config | n | errors | N/A | MAE | MAE incl. errors | exact | "
        "decisive acc. | false accept | false reject | completion tok. | seconds |"
    )
    print(f"### split: {split}\n")
    print(header)
    print("|" + "---|" * 12)
    for name, m in tables:
        tok = (
            ""
            if m["mean_completion_tokens"] is None
            else f"{m['mean_completion_tokens']:.0f}"
        )
        sec = "" if m["mean_seconds"] is None else f"{m['mean_seconds']:.1f}"
        print(
            f"| {name} | {m['n']} | {m['errors']} | {m['unparsed']} | {m['mae']:.3f} | "
            f"{m['mae_with_errors']:.3f} | {m['exact']:.1%} | {m['decisive_accuracy']:.1%} | "
            f"{m['false_accept']:.1%} | {m['false_reject']:.1%} | {tok} | {sec} |"
        )


# ---------------------------------------------------------------------- main


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    sub = parser.add_subparsers(dest="command", required=True)

    b = sub.add_parser(
        "build", help="rebuild the judge inputs, or assemble a new set from a sample"
    )
    b.add_argument("--sample", type=Path, default=None)
    b.add_argument("--labels", type=Path, default=None)
    b.add_argument("--results-dir", type=Path, default=ROOT / "data" / "results")

    r = sub.add_parser("run", help="judge the set with a config")
    r.add_argument("config", type=Path)
    r.add_argument("--split", choices=SPLITS, default="tune")
    r.add_argument("--concurrency", type=int, default=16)
    r.add_argument("--limit", type=int, default=None)

    p = sub.add_parser("report", help="score runs against the labels")
    p.add_argument("--split", choices=SPLITS, default="tune")
    p.add_argument("runs", nargs="*", type=Path)

    args = parser.parse_args(argv)
    if args.command == "build":
        build(args.sample, args.labels, args.results_dir)
    elif args.command == "run":
        run(args.config, args.split, args.concurrency, args.limit)
    else:
        report(args.split, args.runs or None)
    return 0


if __name__ == "__main__":
    sys.exit(main())
