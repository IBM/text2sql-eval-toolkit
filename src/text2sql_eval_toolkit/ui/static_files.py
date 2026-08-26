#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Serving the built frontend, and the data-root assets it references.

Two things that look unrelated and are not: ``/api/static/{path}`` serves files
out of the data root, and the SPA fallback serves ``index.html`` for any path
the client router owns.  Both decide what of the filesystem reaches a visitor,
so they are read together.

``serve_dashboard_asset`` is scoped to ``benchmarks/logos/`` deliberately.  It
once served the whole data root, which put the judge spend ledger --
``judge/usage.sqlite`` -- one URL away from any anonymous caller.
"""

import shutil
import subprocess
from pathlib import Path
from typing import Any, Optional

from fastapi import (
    APIRouter,
    FastAPI,
    HTTPException,
    Request,
    Response,
)
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from starlette.exceptions import HTTPException as StarletteHTTPException

import text2sql_eval_toolkit.env_loader  # noqa: F401 — load .env (WATSONX_*, etc.) before eval/inference

from text2sql_eval_toolkit.ui.paths import get_data_root
from text2sql_eval_toolkit.ui.registry import (
    ALLOWED_LOGO_EXTENSIONS,
    STATIC_ASSET_SUBDIR,
)
from text2sql_eval_toolkit.logging import get_logger

logger = get_logger(__name__)

router = APIRouter()


@router.get("/api/static/{file_path:path}")
def serve_dashboard_asset(file_path: str, request: Request):
    """
    Serve a benchmark logo.

    Deliberately scoped to one directory and to image types. This used to serve
    anything beneath the data root, which on a shared deployment would have
    exposed internal files that happen to live there -- notably
    ``judge/usage.sqlite`` -- to any anonymous visitor, because this is a GET
    and so runs at the public tier.

    Responses are validated with an ETag from the file's size and mtime, so an
    unchanged logo costs a 304 rather than a full body.
    """
    # The URL carries the full relative path ("benchmarks/logos/beaver.png"), so
    # resolve against the data root and then require the result to sit inside
    # the logos directory.
    data_root = get_data_root().resolve()
    allowed_root = (data_root / STATIC_ASSET_SUBDIR).resolve()
    candidate = (data_root / file_path).resolve()

    # Containment, then type: a logo directory should only yield images.
    if allowed_root not in candidate.parents:
        raise HTTPException(status_code=403, detail="Forbidden path")
    if candidate.suffix.lower() not in ALLOWED_LOGO_EXTENSIONS:
        raise HTTPException(status_code=403, detail="Forbidden asset type")
    if not candidate.is_file():
        raise HTTPException(status_code=404, detail="Asset not found")

    stat = candidate.stat()
    etag = f'W/"{stat.st_size:x}-{stat.st_mtime_ns:x}"'
    headers = {"ETag": etag, "Cache-Control": "no-cache, must-revalidate"}

    if request.headers.get("if-none-match") == etag:
        return Response(status_code=304, headers=headers)

    return FileResponse(str(candidate), headers=headers)


def _resolve_dashboard_source_dir() -> Optional[Path]:
    """
    Location of the Vite project (directory containing package.json), if present.
    Prefer cwd (repo checkout) then package-relative path for editable installs.
    """
    candidates = [
        Path.cwd() / "dashboard",
        Path(__file__).resolve().parents[3] / "dashboard",
    ]
    for p in candidates:
        pkg = p / "package.json"
        if pkg.is_file():
            return p.resolve()
    return None


def _ensure_dashboard_dist(dashboard_dir: Path) -> None:
    """Ensure dist/index.html exists so StaticFiles can mount before the first watch rebuild."""
    dist_index = dashboard_dir / "dist" / "index.html"
    if dist_index.is_file():
        return
    npm = shutil.which("npm")
    if not npm:
        logger.warning(
            "npm not found on PATH; cannot build dashboard. Install Node.js/npm or run "
            "`cd dashboard && npm install && npm run build`."
        )
        return
    if not (dashboard_dir / "node_modules").is_dir():
        logger.info(
            "dashboard/node_modules missing; running `npm install` in %s", dashboard_dir
        )
        install = subprocess.run(
            [npm, "install"],
            cwd=str(dashboard_dir),
        )
        if install.returncode != 0:
            logger.warning(
                "npm install failed (exit %s). Run `cd dashboard && npm install && npm run build` manually.",
                install.returncode,
            )
            return
    logger.info(
        "No dashboard dist found; running one-time `npm run build` in %s", dashboard_dir
    )
    r = subprocess.run(
        [npm, "run", "build"],
        cwd=str(dashboard_dir),
    )
    if r.returncode != 0:
        logger.warning(
            "Dashboard build failed (exit %s). The UI may not load until you build successfully.",
            r.returncode,
        )


def _spawn_dashboard_watch(dashboard_dir: Path) -> Optional[subprocess.Popen]:
    """Run `vite build --watch` so dashboard/dist updates when sources change."""
    npm = shutil.which("npm")
    if not npm:
        logger.warning(
            "npm not found on PATH; skipping dashboard watch. Run `cd dashboard && npm run build` after edits."
        )
        return None
    if not (dashboard_dir / "node_modules").is_dir():
        logger.warning(
            "dashboard/node_modules missing; skipping dashboard watch. Run `cd dashboard && npm install`."
        )
        return None
    try:
        proc = subprocess.Popen(
            [npm, "run", "watch-build"],
            cwd=str(dashboard_dir),
        )
        logger.info(
            "Dashboard watch started (%s): Vite will rebuild dashboard/dist when sources change",
            dashboard_dir,
        )
        return proc
    except OSError as exc:
        logger.warning("Could not start dashboard watch: %s", exc)
        return None


def _terminate_dashboard_watch(
    proc: Optional[subprocess.Popen], *, timeout: float = 12.0
) -> None:
    if proc is None or proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()


class SPAStaticFiles(StaticFiles):
    """
    Static files with a single-page-app fallback.

    The dashboard uses real paths (``/b/{benchmark}/errors``) so links can be
    shared, but those paths exist only in the client router -- there is no such
    file on disk.  Without a fallback, opening a shared link or refreshing any
    view returns 404.  Unknown non-API paths therefore serve ``index.html`` and
    let the client resolve them.

    ``/api/*`` is deliberately excluded: an unknown API path must stay a real
    404 rather than silently returning an HTML page, which would turn a typo
    into a confusing parse error in the caller.
    """

    async def get_response(self, path: str, scope: Any) -> Any:
        try:
            return await super().get_response(path, scope)
        except StarletteHTTPException as exc:
            if exc.status_code != 404:
                raise
            request_path = scope.get("path", "")
            if request_path.startswith("/api/"):
                raise
            # Anything that looks like a missing asset should stay a 404 rather
            # than returning HTML with a JS or CSS content type.
            if Path(path).suffix:
                raise
            return await super().get_response("index.html", scope)


def mount_static(app: FastAPI) -> None:
    """
    Mount built frontend assets if available.

    Two layouts, in priority order.

    A source checkout builds to `dashboard/dist`, and that wins: it is what the
    `vite build --watch` process rewrites, so a developer sees their edits.

    An installed wheel has no such directory. setup.py copies the build into the
    package as `ui/static/` at build time, which is the fallback here.

    If neither exists the API still serves and `/` returns 404, which on its own
    tells an operator nothing -- so that case warns with the paths it tried.
    """
    candidate_dirs = [
        Path.cwd() / "dashboard" / "dist",
        Path(__file__).resolve().parents[3] / "dashboard" / "dist",
        # Installed wheel: placed here by setup.py. Last, so a source checkout
        # always prefers its own live build over a stale packaged one.
        Path(__file__).resolve().parent / "static",
    ]
    for static_dir in candidate_dirs:
        if static_dir.exists():
            app.mount(
                "/",
                SPAStaticFiles(directory=str(static_dir), html=True),
                name="dashboard",
            )
            logger.info(f"Mounted dashboard static files from {static_dir}")
            return
    logger.warning(
        "No built dashboard frontend found -- the API is serving, but '/' will "
        "return 404. Looked in: %s. Build it with `cd dashboard && npm run "
        "build`; an installed wheel should carry one already.",
        ", ".join(str(d) for d in candidate_dirs),
    )
