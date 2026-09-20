#!/usr/bin/env python3
#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#
"""
Maintainer-only script: push a new snapshot of data/results/ to the
Hugging Face Hub dataset repo.

End users should never run this script.  Run it after each release to
upload the result artefacts that match the new toolkit tag.

What is uploaded is an explicit list of files, not the folder.  Two things in a
maintainer's results folder must never reach the public repo:

* **The derived query indices** under ``results/.index/``, which hold the raw
  bytes of every record -- and any other hidden directory, backups (``bak/``)
  and logs.
* **The details of a benchmark whose details require sign-in** (a registry entry
  with ``"requires_sign_in": true``, such as Beaver).  Its questions, SQL and
  per-record results are distributed under gated access; only its overall
  summary and overall chart are published.  A local copy of its predictions,
  evaluation file or errors report is left where it is and skipped, and an
  upload whose summary report still breaks results down by query category is
  refused.

Usage
-----
    export HF_TOKEN=<your_token>

    # Dry run first (generates manifest.json locally, prints what would happen)
    python scripts/curation/upload_results_to_hub.py --dry-run

    # Real upload
    python scripts/curation/upload_results_to_hub.py

    # Specify revision tag explicitly
    python scripts/curation/upload_results_to_hub.py --revision-tag v1.1.0

CLI flags
---------
    --data-root PATH     Root for evaluation artefacts.
                         Default: $TEXT2SQL_DATA_ROOT or ./data.
    --repo-id REPO_ID    HF dataset repo.
                         Default: text2sql-eval-toolkit/text2sql-eval-results.
    --revision-tag TAG   Git tag to create on the HF repo.
                         Default: v{toolkit.__version__}.
    --num-workers N      Parallel upload workers (default: 8).
    --dry-run            Simulate upload; write manifest.json but skip
                         actual HF API calls (steps 5-7).
    --no-tag             Skip creating the git tag on the HF repo.
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional, Set

# Import only the version from the toolkit to avoid pulling in heavy deps.
from text2sql_eval_toolkit import __version__ as _TOOLKIT_VERSION

DEFAULT_REPO_ID = "text2sql-eval-toolkit/text2sql-eval-results"

#: Top-level directories under results/ that are never published.
_NEVER_PUBLISHED_DIRS = {"bak", "logs"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _resolve_data_root(data_root: Optional[str]) -> Path:
    if data_root:
        return Path(data_root).expanduser().resolve()
    env = os.getenv("TEXT2SQL_DATA_ROOT")
    if env:
        return Path(env).expanduser().resolve()
    return (Path.cwd() / "data").resolve()


def _format_bytes(n: int) -> str:
    if n >= 1e12:
        return f"{n / 1e12:.1f} TB"
    if n >= 1e9:
        return f"{n / 1e9:.1f} GB"
    if n >= 1e6:
        return f"{n / 1e6:.1f} MB"
    return f"{n / 1e3:.1f} KB"


#: Benchmark ids the dashboard restricts without a registry flag
#: (``ui/benchmark_access.py``). Named here rather than imported, because that
#: module needs the dashboard extra and this script does not.
SIGN_IN_BENCHMARKS_ENV = "TEXT2SQL_SIGN_IN_BENCHMARKS"


def _restricted_benchmarks(data_root: Path) -> Set[str]:
    """
    Benchmarks whose details may not be published.

    Read from every registry copy -- the data root's and the ones packaged with
    the toolkit -- for the reason the dashboard does the same: a data root's
    registry can predate the flag, and a flag anywhere must hold. The
    environment variable is read for the same reason: it is the other way a
    deployment marks a benchmark's details restricted, and a benchmark marked
    only that way would otherwise be published in full from that same host.
    """
    import importlib.resources as resources

    candidates = [data_root / "benchmarks.json", data_root / "test-benchmarks.json"]
    for name in ("benchmarks.json", "test-benchmarks.json"):
        candidates.append(
            Path(str(resources.files("text2sql_eval_toolkit.data").joinpath(name)))
        )
    restricted: Set[str] = set()
    for path in candidates:
        try:
            entries = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if not isinstance(entries, dict):
            continue
        for benchmark_id, entry in entries.items():
            flag = entry.get("requires_sign_in") if isinstance(entry, dict) else None
            if flag is True or str(flag).strip().lower() in {"true", "1", "yes"}:
                restricted.add(benchmark_id)
    restricted |= {
        part.strip()
        for part in os.environ.get(SIGN_IN_BENCHMARKS_ENV, "").split(",")
        if part.strip()
    }
    return restricted


def _summary_files(benchmark_id: str) -> Set[str]:
    """What a restricted benchmark publishes: overall summary and overall chart."""
    stem = f"{benchmark_id}-predictions_eval_summary"
    return {
        f"{stem}.json",
        f"{stem}.csv",
        f"{stem}.md",
        f"charts/{stem}.png",
    }


def _is_publishable(relative: str, restricted: Iterable[str]) -> bool:
    """
    Whether ``relative`` -- a path under results/, POSIX-style -- may be uploaded.
    """
    parts = relative.split("/")
    if any(part.startswith(".") for part in parts):
        return False  # .index/ and any other hidden directory or file
    if parts[0] in _NEVER_PUBLISHED_DIRS or "logs" in parts[:-1]:
        return False
    if relative.endswith(".log"):
        return False
    name = parts[-1]
    for benchmark_id in restricted:
        # Compared without case, as the dashboard compares them: on a
        # case-insensitive filesystem `BEAVER-…` opens Beaver's files, so an id
        # given in another case must restrict them too.
        folded = benchmark_id.casefold()
        if parts[0].casefold() == folded:
            return False  # nested layout: results/<benchmark>/...
        if name.casefold().startswith(f"{folded}-"):
            # Allowed against the file's own spelling, so the four summary
            # files are still published whatever case the id was given in.
            return relative in _summary_files(name[: len(folded)])
    return True


def _publishable_files(results_dir: Path, restricted: Iterable[str]) -> List[str]:
    """Every file under ``results_dir`` that may be uploaded, as relative paths."""
    restricted = set(restricted)
    out = []
    for path in sorted(results_dir.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(results_dir).as_posix()
        if _is_publishable(relative, restricted):
            out.append(relative)
    return out


def _check_restricted_summaries(results_dir: Path, restricted: Iterable[str]) -> None:
    """
    Refuse to publish a restricted benchmark's summary report with its breakdown
    by query category, which is derived from the ground-truth SQL.
    """
    for benchmark_id in sorted(restricted):
        report = results_dir / f"{benchmark_id}-predictions_eval_summary.md"
        if report.is_file() and "## Category:" in report.read_text(encoding="utf-8"):
            raise SystemExit(
                f"ERROR: {report} breaks results down by query category, and "
                f"{benchmark_id!r} publishes overall scores only. Cut the report "
                "at its first '## Category:' heading before uploading."
            )


def _generate_manifest(results_dir: Path, restricted: Iterable[str] = ()) -> dict:
    """
    Walk results_dir and build a manifest.json describing every result file.

    Only files that will actually be uploaded are listed -- a manifest naming a
    file the repo does not have makes ``results fetch`` fail on it.

    Supported layouts
    -----------------
    Top-level flat (current default):
        results/
            bird_mini_dev_sqlite-predictions_eval.json
            bird_mini_dev_sqlite-predictions.json
            ...

        The benchmark name is the prefix before the first ``-predictions``
        occurrence in the filename.

    Nested (future / multi-pipeline):
        results/<benchmark>/<pipeline>/<model>/
            predictions.json  evaluation.json  summary.csv
    """
    restricted = set(restricted)
    ver = _TOOLKIT_VERSION
    major, minor = ver.split(".")[:2]
    compat = f">={major}.{minor}.0,<{int(major) + 1}.0.0"

    benchmarks: dict = {}
    total_bytes = 0

    # ── Pass 1: top-level flat files ──────────────────────────────────────────
    # Group files by benchmark name extracted from their filename prefix.
    # e.g. "bird_mini_dev_sqlite-predictions_eval.json" → bench "bird_mini_dev_sqlite"
    flat_groups: dict = {}
    for f in sorted(results_dir.iterdir()):
        if not f.is_file() or not _is_publishable(f.name, restricted):
            continue
        m = re.match(r"^(.+?)-predictions", f.name)
        if not m:
            continue  # skip non-result files (README.md, etc.)
        bench = m.group(1)
        flat_groups.setdefault(bench, []).append(f)

    for bench_name, files in sorted(flat_groups.items()):
        bench_bytes = sum(f.stat().st_size for f in files)
        file_names = sorted(f.name for f in files)
        benchmarks[bench_name] = {
            "pipelines": {
                "default": {
                    "models": ["default"],
                    "files": file_names,
                    "size_bytes": bench_bytes,
                }
            }
        }
        total_bytes += bench_bytes

    # ── Pass 2: nested benchmark directories ──────────────────────────────────
    # Only processes actual directories; skips non-benchmark dirs (charts, bak, logs).
    known_non_bench = {"bak", "logs", "charts"}
    for bench_dir in sorted(results_dir.iterdir()):
        if not bench_dir.is_dir():
            continue
        if bench_dir.name in known_non_bench or bench_dir.name.startswith("."):
            continue
        bench_name = bench_dir.name
        if bench_name in benchmarks or bench_name in restricted:
            continue  # already recorded as flat files above, or not published

        pipelines: dict = {}
        pipe_dirs = [p for p in bench_dir.iterdir() if p.is_dir()]

        if not pipe_dirs:
            bench_bytes = sum(
                f.stat().st_size for f in bench_dir.iterdir() if f.is_file()
            )
            file_names = sorted(f.name for f in bench_dir.iterdir() if f.is_file())
            if file_names:
                pipelines["default"] = {
                    "models": ["default"],
                    "files": file_names,
                    "size_bytes": bench_bytes,
                }
                total_bytes += bench_bytes
        else:
            for pipe_dir in sorted(pipe_dirs):
                pipe_name = pipe_dir.name
                models: List[str] = []
                pipe_bytes = 0
                for model_dir in sorted(m for m in pipe_dir.iterdir() if m.is_dir()):
                    raw = model_dir.name
                    model_id = raw.replace("__", "/", 1).replace("__", ":", 1)
                    models.append(model_id)
                    for ff in model_dir.rglob("*"):
                        if ff.is_file():
                            pipe_bytes += ff.stat().st_size
                if models:
                    pipelines[pipe_name] = {
                        "models": models,
                        "files": ["predictions.json", "evaluation.json", "summary.csv"],
                        "size_bytes": pipe_bytes,
                    }
                    total_bytes += pipe_bytes

        if pipelines:
            benchmarks[bench_name] = {"pipelines": pipelines}

    return {
        "schema_version": 1,
        "toolkit_version_compat": compat,
        "generated_at": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "total_size_bytes": total_bytes,
        "benchmarks": benchmarks,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Upload evaluation results to the Hugging Face Hub.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--data-root",
        default=None,
        metavar="PATH",
        help="Root data directory (default: $TEXT2SQL_DATA_ROOT or ./data).",
    )
    parser.add_argument(
        "--repo-id",
        default=DEFAULT_REPO_ID,
        metavar="REPO_ID",
        help=f"HF dataset repository ID (default: {DEFAULT_REPO_ID}).",
    )
    parser.add_argument(
        "--revision-tag",
        default=f"v{_TOOLKIT_VERSION}",
        metavar="TAG",
        help="Git tag to create on the HF repo (default: v{toolkit_version}).",
    )
    parser.add_argument(
        "--num-workers",
        type=int,
        default=8,
        metavar="N",
        help="Parallel upload workers (default: 8).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Generate manifest.json locally and print what would be uploaded, "
            "but do not make any HF API calls."
        ),
    )
    parser.add_argument(
        "--no-tag",
        action="store_true",
        help="Skip creating the git tag on the HF repo after upload.",
    )
    args = parser.parse_args(argv)

    # ── Step 1: Resolve data root ─────────────────────────────────────────────
    data_root = _resolve_data_root(args.data_root)
    results_dir = data_root / "results"

    if not results_dir.is_dir():
        print(
            f"ERROR: results directory does not exist: {results_dir}",
            file=sys.stderr,
        )
        sys.exit(1)

    if not any(results_dir.iterdir()):
        print(
            f"ERROR: results directory is empty: {results_dir}",
            file=sys.stderr,
        )
        sys.exit(1)

    restricted = _restricted_benchmarks(data_root)
    _check_restricted_summaries(results_dir, restricted)
    publishable = _publishable_files(results_dir, restricted)
    if restricted:
        print(
            "Details not published for: "
            + ", ".join(sorted(restricted))
            + " (overall summary and overall chart only)"
        )

    # ── Step 2: Auth check ────────────────────────────────────────────────────
    if not args.dry_run:
        hf_token = os.getenv("HF_TOKEN")
        if not hf_token:
            print(
                "HF_TOKEN is not set. "
                "Export your Hugging Face token before running this script.",
                file=sys.stderr,
            )
            sys.exit(1)

    # ── Step 3: Ensure repo exists ────────────────────────────────────────────
    if not args.dry_run:
        from huggingface_hub import HfApi
        from huggingface_hub.utils import RepositoryNotFoundError

        api = HfApi()
        try:
            api.repo_info(repo_id=args.repo_id, repo_type="dataset")
        except RepositoryNotFoundError:
            print(f"Repo {args.repo_id!r} not found; creating it as a public dataset.")
            api.create_repo(
                repo_id=args.repo_id,
                repo_type="dataset",
                private=False,
            )
            print(f"Created: https://huggingface.co/datasets/{args.repo_id}")

    # ── Step 4: Generate manifest.json ────────────────────────────────────────
    print(f"Generating manifest.json from {results_dir} …")
    manifest = _generate_manifest(results_dir, restricted)
    manifest_path = data_root / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    n_benchmarks = len(manifest["benchmarks"])
    n_pipelines = sum(len(b["pipelines"]) for b in manifest["benchmarks"].values())
    print(
        f"Manifest written to {manifest_path}\n"
        f"  Benchmarks: {n_benchmarks}   Pipelines: {n_pipelines}   "
        f"Total size: {_format_bytes(manifest['total_size_bytes'])}"
    )

    if args.dry_run:
        print()
        print("DRY RUN — the following steps would have been executed:")
        print(f"  upload_file  manifest.json → {args.repo_id}:manifest.json")
        print(
            f"  upload_large_folder  {len(publishable)} files under {results_dir}/ → "
            f"{args.repo_id}:results/  (workers={args.num_workers})"
        )
        for relative in publishable:
            print(f"    results/{relative}")
        if not args.no_tag:
            print(f"  create_tag  {args.revision_tag} on " f"{args.repo_id} (dataset)")
        print()
        print("Re-run without --dry-run to perform the upload.")
        return

    # ── Step 5: Upload manifest.json first ────────────────────────────────────
    print("Uploading manifest.json …")
    try:
        api.upload_file(
            path_or_fileobj=str(manifest_path),
            path_in_repo="manifest.json",
            repo_id=args.repo_id,
            repo_type="dataset",
            commit_message="Update manifest.json",
        )
    except Exception as exc:
        print(f"ERROR uploading manifest.json: {exc}", file=sys.stderr)
        raise

    # ── Step 6: Upload results/ ───────────────────────────────────────────────
    # upload_large_folder has no path_in_repo parameter — it maps folder_path/
    # directly to the HF repo root.  By passing folder_path=data_root, files at
    # data_root/results/... land at results/... in the repo.  The allow list is
    # the exact set of publishable files, so nothing else in the folder -- the
    # indices, backups, a gated benchmark's details -- can be swept up with it.
    print(f"Uploading {len(publishable)} files → {args.repo_id}:results/ …")
    print("This may take a while for large result sets.")
    try:
        api.upload_large_folder(
            folder_path=str(data_root),
            repo_id=args.repo_id,
            repo_type="dataset",
            num_workers=args.num_workers,
            allow_patterns=[f"results/{relative}" for relative in publishable],
        )
    except Exception as exc:
        print(f"ERROR uploading results: {exc}", file=sys.stderr)
        raise

    # ── Step 7: Tag the commit ────────────────────────────────────────────────
    if not args.no_tag:
        print(f"Creating tag {args.revision_tag!r} …")
        try:
            api.create_tag(
                repo_id=args.repo_id,
                repo_type="dataset",
                tag=args.revision_tag,
                tag_message=f"Results snapshot for toolkit {args.revision_tag}",
            )
        except Exception as exc:
            if "already exists" in str(exc).lower() or "409" in str(exc):
                print(
                    f"WARNING: tag {args.revision_tag!r} already exists on "
                    f"{args.repo_id}; skipping."
                )
            else:
                print(f"ERROR creating tag: {exc}", file=sys.stderr)
                raise

    # ── Summary ───────────────────────────────────────────────────────────────
    print()
    print("Upload complete.")
    print(f"  Repo:  {args.repo_id}")
    print(f"  Tag:   {args.revision_tag}")
    print(f"  Size:  {_format_bytes(manifest['total_size_bytes'])}")
    print(f"  URL:   https://huggingface.co/datasets/{args.repo_id}")


if __name__ == "__main__":
    main()
