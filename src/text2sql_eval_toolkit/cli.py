#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#
"""
Command-line interface for the text2sql-eval-toolkit.

Entry point: ``text2sql-eval-toolkit`` (wired in pyproject.toml).

Subcommands
-----------
results fetch   Download pre-computed results from the Hugging Face Hub.
results list    Print the available results manifest as a table.
results clear   Remove previously downloaded results.
"""

import argparse
import sys
from pathlib import Path
from typing import List, Optional


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _normalise_list(values: Optional[List[str]]) -> Optional[List[str]]:
    """
    Accept both ``--flag a,b`` (comma-separated) and ``--flag a --flag b``
    (repeated) forms and return a flat list, or ``None`` if the input is empty.
    """
    if not values:
        return None
    out: List[str] = []
    for v in values:
        out.extend(x.strip() for x in v.split(",") if x.strip())
    return out or None


# ---------------------------------------------------------------------------
# Sub-command handlers
# ---------------------------------------------------------------------------


def _cmd_fetch(args: argparse.Namespace) -> int:
    from text2sql_eval_toolkit.results import fetch_results

    benchmarks = _normalise_list(args.benchmarks)
    pipelines = _normalise_list(args.pipelines)
    models = _normalise_list(args.models)
    data_root = Path(args.data_root) if args.data_root else None
    revision: Optional[str] = args.revision or None

    try:
        results_dir = fetch_results(
            benchmarks=benchmarks,
            pipelines=pipelines,
            models=models,
            revision=revision,
            data_root=data_root,
            force=args.force,
            show_progress=True,
        )
        print(f"Results fetched to: {results_dir}")
        return 0
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


def _cmd_list(args: argparse.Namespace) -> int:
    from text2sql_eval_toolkit.results import list_available_results

    revision: Optional[str] = getattr(args, "revision", None) or None

    try:
        manifest = list_available_results(revision=revision)
    except Exception as exc:
        print(f"Error fetching manifest: {exc}", file=sys.stderr)
        return 1

    generated_at = manifest.get("generated_at", "N/A")
    compat = manifest.get("toolkit_version_compat", "N/A")
    total_gb = manifest.get("total_size_bytes", 0) / 1e9

    print(f"Generated:  {generated_at}")
    print(f"Compat:     {compat}")
    print(f"Total size: {total_gb:.2f} GB")
    print()

    benchmarks = manifest.get("benchmarks", {})
    col_b, col_p, col_m = 30, 20, 60
    header = (
        f"{'Benchmark':<{col_b}}"
        f"{'Pipeline':<{col_p}}"
        f"{'Models':<{col_m}}"
    )
    print(header)
    print("-" * (col_b + col_p + col_m))
    for b_name, b_info in benchmarks.items():
        for p_name, p_info in b_info.get("pipelines", {}).items():
            models_str = ", ".join(p_info.get("models", []))
            # Wrap long model lists for readability.
            if len(models_str) > col_m:
                models_str = models_str[: col_m - 3] + "..."
            print(
                f"{b_name:<{col_b}}{p_name:<{col_p}}{models_str:<{col_m}}"
            )
    return 0


def _cmd_clear(args: argparse.Namespace) -> int:
    from text2sql_eval_toolkit.results import clear_cache

    data_root = Path(args.data_root) if args.data_root else None
    confirm = not args.yes

    try:
        clear_cache(data_root=data_root, confirm=confirm)
        return 0
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="text2sql-eval-toolkit",
        description="Text2SQL Evaluation Toolkit CLI",
    )
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")

    # ── results ──────────────────────────────────────────────────────────────
    results_parser = sub.add_parser(
        "results",
        help="Manage pre-computed evaluation results.",
    )
    results_sub = results_parser.add_subparsers(
        dest="results_command", metavar="ACTION"
    )

    # results fetch
    fetch_p = results_sub.add_parser(
        "fetch",
        help="Download results from the Hugging Face Hub.",
    )
    fetch_p.add_argument(
        "--benchmarks",
        action="append",
        default=[],
        metavar="BENCHMARK",
        help=(
            "Benchmark(s) to fetch.  Accepts repeated flags "
            "(--benchmarks a --benchmarks b) or comma-separated values "
            "(--benchmarks a,b).  Omit to fetch all."
        ),
    )
    fetch_p.add_argument(
        "--pipelines",
        action="append",
        default=[],
        metavar="PIPELINE",
        help="Pipeline(s) to fetch (repeated or comma-separated).",
    )
    fetch_p.add_argument(
        "--models",
        action="append",
        default=[],
        metavar="MODEL",
        help="Model(s) to fetch (repeated or comma-separated).",
    )
    fetch_p.add_argument(
        "--revision",
        default=None,
        metavar="REVISION",
        help=(
            "HF repo revision / git tag to download from "
            "(default: tag matching the installed toolkit version)."
        ),
    )
    fetch_p.add_argument(
        "--data-root",
        default=None,
        metavar="PATH",
        help=(
            "Root data directory.  "
            "Falls back to $TEXT2SQL_DATA_ROOT or ./data."
        ),
    )
    fetch_p.add_argument(
        "--force",
        action="store_true",
        help="Force re-download even if files are already present.",
    )

    # results list
    list_p = results_sub.add_parser(
        "list",
        help="List available results from the Hub manifest.",
    )
    list_p.add_argument(
        "--revision",
        default=None,
        metavar="REVISION",
        help="HF repo revision / tag (default: installed toolkit version tag).",
    )
    list_p.add_argument("--data-root", default=None, metavar="PATH")

    # results clear
    clear_p = results_sub.add_parser(
        "clear",
        help="Remove downloaded results from the local data root.",
    )
    clear_p.add_argument(
        "--yes",
        action="store_true",
        help="Skip confirmation prompt.",
    )
    clear_p.add_argument("--data-root", default=None, metavar="PATH")

    return parser


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main(argv: Optional[List[str]] = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "results":
        if args.results_command == "fetch":
            sys.exit(_cmd_fetch(args))
        elif args.results_command == "list":
            sys.exit(_cmd_list(args))
        elif args.results_command == "clear":
            sys.exit(_cmd_clear(args))
        else:
            parser.parse_args(["results", "--help"])
            sys.exit(1)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
