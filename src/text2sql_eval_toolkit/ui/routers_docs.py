#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Serving the project's long-form documents to the dashboard's docs view.

These are the notes under ``docs/notes/`` -- a survey of how text-to-SQL
evaluation is done and where it goes wrong, worked examples, the things worth
showing in a demo. They are versioned with the code and deliberately *not*
packaged: ``docs/`` ships in neither the wheel nor the sdist, so a pip install
finds no documents and the view says so.  That is the intended behaviour, not a
bug to work around -- the content is public on GitHub, and the deployment builds
its image from the repository, so the two places that matter both have it.

Read-only. There is no write endpoint; the view is not an editor.

Names arrive from a URL segment and are validated the same way judge-config
names are -- a plain stem, with containment asserted on the resolved path. That
is not defensive habit: the first version of the judge-config endpoint
interpolated a URL segment straight into a path, and this is the same shape of
bug in the same codebase.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import List, Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

from text2sql_eval_toolkit.logging import get_logger
from text2sql_eval_toolkit.ui.models import DocInfo, DocListResponse, DocResponse

logger = get_logger(__name__)

router = APIRouter()

#: Documents live here, relative to the repository root.
DOCS_SUBDIR = ("docs", "notes")

#: A plain stem: no traversal, no dotfiles, no empty name. Mirrors
#: ``_validate_config_name`` in ``routers_judge``.
_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")

#: Images a note may reference, kept in ``docs/notes/assets/``.
#:
#: Raster only. An SVG loaded through ``<img>`` cannot run script, but it is a
#: document rather than a bitmap and the exception is not worth the paragraph
#: it would need; screenshots are PNGs.
_ASSET_TYPES = {
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".webp": "image/webp",
    ".gif": "image/gif",
}

#: Not listed as documents. ``README.md`` is the directory's index for someone
#: browsing the repository on GitHub -- it describes the other files rather
#: than being one of them, and in the view it would read as a document about
#: how to add documents. ``mkdocs.yml`` excludes the repository's other
#: READMEs for exactly this reason. Still fetchable by name, so a link to it
#: is not broken.
_NOT_LISTED = frozenset({"README", "index"})

#: ``# Title`` on its own line -- an ATX heading, which is how every document
#: here opens. Used for the list's display title so adding a document needs no
#: registration anywhere.
_H1 = re.compile(r"^#\s+(?P<title>.+?)\s*$", re.MULTILINE)


def docs_dir() -> Optional[Path]:
    """
    The directory holding the documents, or ``None`` if there is not one.

    Resolved by walking up from the working directory for a checkout -- the
    nearest ancestor carrying ``pyproject.toml`` -- which is the same idiom
    ``get_writable_data_root()`` uses.  The deployment image copies the
    documents to the same relative place beside its own ``pyproject.toml``, so
    one rule covers both.

    Returns ``None`` rather than a non-existent path so callers cannot
    accidentally report a filesystem layout that is not there.
    """
    cwd = Path.cwd().resolve()
    for directory in [cwd, *cwd.parents]:
        if (directory / "pyproject.toml").is_file():
            candidate = directory.joinpath(*DOCS_SUBDIR)
            return candidate.resolve() if candidate.is_dir() else None
    return None


def _resolve_doc_path(name: str) -> Path:
    """
    ``<docs>/<name>.md``, asserted to still be directly inside the docs
    directory.

    Args:
        name: The document's stem, straight from a URL segment.

    Returns:
        The resolved path.  It is not guaranteed to exist; the caller reports
        the miss, so a valid-but-absent name is a 404 rather than a 400.

    Raises:
        FileNotFoundError: The name is not a plain stem, the resolved path
            escapes the directory, or there is no documents directory at all.
    """
    if not _NAME.fullmatch(name or ""):
        raise FileNotFoundError(name)
    base = docs_dir()
    if base is None:
        raise FileNotFoundError(name)
    candidate = (base / f"{name}.md").resolve()
    # FastAPI will not match a raw `/` into a single path parameter, but
    # percent-encoded separators and `..` are decoded before they reach here.
    if candidate.parent != base:
        raise FileNotFoundError(name)
    return candidate


def _title_of(text: str, fallback: str) -> str:
    """The document's first H1, or a readable form of its filename."""
    match = _H1.search(text)
    if match:
        return match.group("title")
    return fallback.replace("-", " ").replace("_", " ").capitalize()


def _summary_of(text: str) -> str:
    """
    The first ordinary paragraph, for the list.

    Headings, blockquotes, code fences, list items and link-reference lines are
    all skipped: a one-line summary that reads "# State of the art" tells the
    reader nothing they cannot already see in the title beside it.
    """
    paragraph: List[str] = []
    in_fence = False
    for raw in text.splitlines():
        line = raw.strip()
        if line.startswith("```"):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        if not line:
            if paragraph:
                break
            continue
        if line.startswith(("#", ">", "-", "*", "|", "[", "!")) or line[0].isdigit():
            if paragraph:
                break
            continue
        paragraph.append(line)
    summary = " ".join(paragraph)
    return summary if len(summary) <= 240 else summary[:237].rstrip() + "…"


@router.get("/api/docs", response_model=DocListResponse)
def list_docs() -> DocListResponse:
    """
    List the documents available to the docs view.

    An empty list is a normal answer, not an error: it is what a pip install
    sees, because ``docs/`` is not packaged. The view renders an explanation
    rather than a blank page or a 500.
    """
    base = docs_dir()
    if base is None:
        return DocListResponse(items=[], available=False)

    items: List[DocInfo] = []
    for path in sorted(base.glob("*.md")):
        if path.stem in _NOT_LISTED:
            continue
        if not _NAME.fullmatch(path.stem):
            # Not addressable through this API, so listing it would offer a
            # link that 404s.
            logger.warning("skipping doc with unaddressable name: %s", path.name)
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            logger.warning("could not read doc %s", path.name, exc_info=True)
            continue
        items.append(
            DocInfo(
                name=path.stem,
                title=_title_of(text, path.stem),
                summary=_summary_of(text),
            )
        )
    items.sort(key=lambda item: item.title.lower())
    return DocListResponse(items=items, available=True)


@router.get("/api/docs/assets/{filename}")
def get_doc_asset(filename: str) -> FileResponse:
    """
    Serve one image referenced by a note.

    Registered before ``/api/docs/{name}`` matters not at all -- the paths have
    different segment counts -- but the validation does: ``filename`` arrives
    from a URL and is checked the same way a document name is, stem and
    extension separately, with containment asserted on the resolved path.

    Notes reference these relatively, as ``assets/foo.png``, so the Markdown
    also renders on GitHub; the renderer rewrites the relative path to this
    endpoint. See ``dashboard/src/lib/markdown.ts``.
    """
    suffix = Path(filename).suffix.lower()
    media_type = _ASSET_TYPES.get(suffix)
    if media_type is None or not _NAME.fullmatch(Path(filename).stem):
        raise HTTPException(status_code=404, detail="Asset not found")

    base = docs_dir()
    if base is None:
        raise HTTPException(status_code=404, detail="Asset not found")
    assets = (base / "assets").resolve()
    candidate = (assets / filename).resolve()
    if candidate.parent != assets or not candidate.is_file():
        raise HTTPException(status_code=404, detail="Asset not found")

    return FileResponse(
        candidate,
        media_type=media_type,
        # Screenshots change only when the notes do, and the notes ship with
        # the image; a week is short enough that a redeploy is not haunted by
        # a stale one and long enough to be worth setting.
        headers={"Cache-Control": "public, max-age=604800"},
    )


@router.get("/api/docs/{name}", response_model=DocResponse)
def get_doc(name: str) -> DocResponse:
    """
    Return one document's Markdown source.

    The Markdown is returned unrendered and the view renders it. Rendering
    server-side would mean shipping HTML to the browser and trusting it there,
    which is a sink worth not creating for content that is only trusted today.
    """
    try:
        path = _resolve_doc_path(name)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Document not found") from None
    if not path.is_file():
        raise HTTPException(status_code=404, detail="Document not found")

    text = path.read_text(encoding="utf-8")
    return DocResponse(
        name=name,
        title=_title_of(text, name),
        markdown=text,
    )
