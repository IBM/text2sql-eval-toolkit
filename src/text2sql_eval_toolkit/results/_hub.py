#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#
"""
Core loader for pre-computed evaluation results hosted on the Hugging Face Hub.

Public API is re-exported from ``text2sql_eval_toolkit.results``.
"""

import functools
import json
import os
import shutil
import sys
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

from huggingface_hub import HfApi, hf_hub_download, snapshot_download
from huggingface_hub.utils import RepositoryNotFoundError, RevisionNotFoundError
from loguru import logger

try:
    from importlib.metadata import version as _pkg_version

    _TOOLKIT_VERSION: str = _pkg_version("text2sql-eval-toolkit")
except Exception:  # pragma: no cover
    _TOOLKIT_VERSION = "0.0.0"

# ---------------------------------------------------------------------------
# Public constants
# ---------------------------------------------------------------------------

DEFAULT_REPO_ID: str = "text2sql-eval-toolkit/text2sql-eval-results"

# Derived at import time from the installed toolkit version so downloads are
# reproducible by default.  Override at call-time with revision= / --revision.
DEFAULT_REVISION: str = f"v{_TOOLKIT_VERSION}"

# Guard so the "revision not found" warning is only printed once per process.
_revision_warn_once: threading.Event = threading.Event()

# Cache resolved effective revisions (repo+requested → actual).
_revision_cache: Dict[str, str] = {}
_revision_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _resolve_data_root(data_root: Optional[Path]) -> Path:
    """Resolve: explicit arg → TEXT2SQL_DATA_ROOT env → ./data."""
    if data_root is not None:
        return Path(data_root).expanduser().resolve()
    env = os.getenv("TEXT2SQL_DATA_ROOT")
    if env:
        return Path(env).expanduser().resolve()
    return (Path.cwd() / "data").resolve()


def _validate_manifest(manifest: Dict[str, Any]) -> None:
    """Raise ValueError if manifest is malformed or version-incompatible."""
    required = {"schema_version", "toolkit_version_compat", "benchmarks"}
    missing = required - manifest.keys()
    if missing:
        raise ValueError(f"manifest.json is missing required fields: {missing}")

    compat: str = manifest["toolkit_version_compat"]
    try:
        from packaging.specifiers import SpecifierSet

        spec = SpecifierSet(compat)
        if _TOOLKIT_VERSION != "0.0.0" and not spec.contains(_TOOLKIT_VERSION):
            raise ValueError(
                f"The results snapshot requires toolkit {compat}, "
                f"but the installed version is {_TOOLKIT_VERSION}. "
                f"Either upgrade/downgrade the toolkit to a compatible version "
                f"or pass --revision to select a results snapshot that matches."
            )
    except ValueError:
        raise
    except Exception as exc:  # pragma: no cover — packaging parse error
        logger.warning(
            "Could not parse manifest toolkit_version_compat '{}': {}", compat, exc
        )


def _effective_revision(repo_id: str, requested: str) -> str:
    """
    Return *requested* if the tag exists on the Hub, otherwise warn once and
    return ``"main"``.  Results are cached per-process to avoid repeated
    network calls.
    """
    cache_key = f"{repo_id}@{requested}"
    with _revision_lock:
        if cache_key in _revision_cache:
            return _revision_cache[cache_key]

    resolved = requested
    try:
        HfApi().repo_info(repo_id=repo_id, repo_type="dataset", revision=requested)
    except RevisionNotFoundError:
        if not _revision_warn_once.is_set():
            _revision_warn_once.set()
            logger.warning(
                "Revision '{}' was not found on {}; falling back to 'main'. "
                "Results may not exactly match toolkit v{}. "
                "Pin a specific tag with --revision to suppress this warning.",
                requested,
                repo_id,
                _TOOLKIT_VERSION,
            )
        resolved = "main"
    except Exception:
        # Network unavailable or other transient error — let snapshot_download
        # surface a clear message instead of failing silently here.
        pass

    with _revision_lock:
        _revision_cache[cache_key] = resolved
    return resolved


def _fetch_manifest_raw(repo_id: str, revision: str) -> Dict[str, Any]:
    """Download and parse manifest.json from the HF repo (uncached)."""
    path = hf_hub_download(
        repo_id=repo_id,
        repo_type="dataset",
        filename="manifest.json",
        revision=revision,
    )
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


@functools.lru_cache(maxsize=16)
def _cached_manifest(repo_id: str, revision: str) -> Dict[str, Any]:
    """Fetch, parse, and validate manifest.json; cached per (repo_id, revision)."""
    manifest = _fetch_manifest_raw(repo_id, revision)
    _validate_manifest(manifest)
    return manifest


def _build_allow_patterns(
    manifest: Dict[str, Any],
    benchmarks: Optional[List[str]],
    pipelines: Optional[List[str]],
    models: Optional[List[str]],
) -> List[str]:
    """
    Build the ``allow_patterns`` list for ``snapshot_download``.

    If no filters are specified, download the entire results tree.
    Otherwise, expand explicit leaf-level globs from the manifest so that
    ``snapshot_download`` fetches only what was requested — no full directory
    listing is required.
    """
    always = ["manifest.json", "README.md"]

    # No filters → download everything.
    if not benchmarks and not pipelines and not models:
        return always + ["results/**"]

    patterns: List[str] = list(always)
    bench_items: Dict[str, Any] = manifest.get("benchmarks", {})
    bench_filter = set(benchmarks) if benchmarks else None
    pipe_filter = set(pipelines) if pipelines else None
    model_filter = set(models) if models else None

    for b_name, b_info in bench_items.items():
        if bench_filter and b_name not in bench_filter:
            continue
        pipe_items: Dict[str, Any] = b_info.get("pipelines", {})
        for p_name, p_info in pipe_items.items():
            if pipe_filter and p_name not in pipe_filter:
                continue
            model_list: List[str] = p_info.get("models", [])
            for m_name in model_list:
                if model_filter and m_name not in model_filter:
                    continue

                m_safe = m_name.replace(":", "__").replace("/", "__")

                if p_name == "default" or m_safe == "default":
                    # Flat layout: files sit directly under results/ using the
                    # benchmark name as a filename prefix (e.g.
                    # results/bird_mini_dev_sqlite-predictions_eval.json).
                    # Match both the flat prefix form and any nested sub-dir.
                    patterns.append(f"results/{b_name}*")
                    patterns.append(f"results/{b_name}/**")
                else:
                    # Nested layout: results/<bench>/<pipeline>/<model>/
                    patterns.append(f"results/{b_name}/{p_name}/{m_safe}/**")

    return patterns


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def list_available_results(
    repo_id: str = DEFAULT_REPO_ID,
    revision: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Return the parsed and validated ``manifest.json`` as a Python dict.

    The result is cached per ``(repo_id, revision)`` for the lifetime of the
    process so repeated calls are free.
    """
    rev = _effective_revision(repo_id, revision or DEFAULT_REVISION)
    return _cached_manifest(repo_id, rev)


def fetch_results(
    *,
    benchmarks: Optional[List[str]] = None,
    pipelines: Optional[List[str]] = None,
    models: Optional[List[str]] = None,
    repo_id: str = DEFAULT_REPO_ID,
    revision: Optional[str] = None,
    data_root: Optional[Path] = None,
    force: bool = False,
    show_progress: bool = True,
) -> Path:
    """
    Download pre-computed evaluation results from the Hugging Face Hub.

    Parameters
    ----------
    benchmarks:
        Restrict to the listed benchmark IDs (``None`` → all).
    pipelines:
        Restrict to the listed pipeline IDs (``None`` → all).
    models:
        Restrict to the listed model identifiers (``None`` → all).
    repo_id:
        Hugging Face dataset repo to download from.
    revision:
        Git tag or branch on the HF repo.  Defaults to ``DEFAULT_REVISION``
        (i.e. the tag matching the installed toolkit version).
    data_root:
        Root directory for evaluation artefacts.  Resolved via
        ``TEXT2SQL_DATA_ROOT`` env var or ``./data`` when ``None``.
    force:
        Re-download even if files are already cached locally.
    show_progress:
        Forward ``huggingface_hub``'s built-in tqdm progress bars.

    Returns
    -------
    Path
        Absolute path to the populated ``results/`` directory.
    """
    data_root_path = _resolve_data_root(data_root)
    rev = _effective_revision(repo_id, revision or DEFAULT_REVISION)

    # Fetch and validate the manifest first to build precise allow_patterns.
    # This avoids a full directory listing on the Hub and supports
    # revision-pinned partial downloads.
    manifest = _cached_manifest(repo_id, rev)
    allow_patterns = _build_allow_patterns(manifest, benchmarks, pipelines, models)

    logger.info(
        "Fetching results from {} @ {} → {}",
        repo_id,
        rev,
        data_root_path,
    )
    if benchmarks or pipelines or models:
        logger.info(
            "Filters — benchmarks: {}  pipelines: {}  models: {}",
            benchmarks,
            pipelines,
            models,
        )

    # IMPORTANT: local_dir=data_root_path (NOT data_root_path / "results").
    # The HF repo already contains a "results/" directory at its root, so the
    # downloaded files will land at data_root_path/results/... — exactly the
    # layout expected by the rest of the toolkit.
    # Passing data_root_path / "results" would produce the double-nested path
    # data_root_path/results/results/... which would break all downstream code.
    snapshot_download(
        repo_id=repo_id,
        repo_type="dataset",
        revision=rev,
        local_dir=str(data_root_path),
        allow_patterns=allow_patterns,
        force_download=force,
    )

    results_dir = data_root_path / "results"
    logger.info("Results available at {}", results_dir)
    return results_dir


def clear_cache(
    data_root: Optional[Path] = None,
    *,
    confirm: bool = True,
) -> None:
    """
    Remove the downloaded results from ``{data_root}/results/``.

    Parameters
    ----------
    data_root:
        Root directory.  Resolved the same way as in :func:`fetch_results`.
    confirm:
        When ``True`` *and* stdin is a TTY, prompt the user before deleting.
        Pass ``confirm=False`` for non-interactive / scripted use.
    """
    data_root_path = _resolve_data_root(data_root)
    results_dir = data_root_path / "results"

    if not results_dir.exists():
        logger.info("Nothing to clear: {} does not exist.", results_dir)
        return

    if confirm and sys.stdin.isatty():
        answer = input(
            f"Delete {results_dir} and all its contents? [y/N] "
        ).strip().lower()
        if answer not in ("y", "yes"):
            logger.info("Aborted.")
            return

    shutil.rmtree(results_dir)
    logger.info("Removed {}", results_dir)
