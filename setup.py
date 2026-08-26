#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#
"""
Build hook that places the dashboard frontend inside the package.

Everything else about this project is configured in pyproject.toml; this file
exists for one reason. The Vite build lands in ``dashboard/dist`` at the repo
root, which is outside the package, and ``package-data`` cannot reach outside a
package directory. Without this the wheel carries the ``text2sql-eval-dashboard``
console script and no frontend, so a pip install starts a server that answers
``/api/*`` normally and returns a bare 404 at ``/``.

The build is copied rather than committed to ``ui/static/`` so there is exactly
one copy of it in the tree. Two copies of a build artifact drift, and this
repository has already paid for that once.
"""

import shutil
from pathlib import Path

from setuptools import setup
from setuptools.command.build_py import build_py

ROOT = Path(__file__).parent.resolve()
FRONTEND_BUILD = ROOT / "dashboard" / "dist"
PACKAGE_STATIC = ROOT / "src" / "text2sql_eval_toolkit" / "ui" / "static"


class BuildPyWithFrontend(build_py):
    """Copy dashboard/dist into the package before package data is collected."""

    def run(self) -> None:
        if (FRONTEND_BUILD / "index.html").is_file():
            # Replace rather than merge: a stale asset from a previous build
            # would otherwise be shipped alongside the current one, and the
            # hashed filenames mean it would never be overwritten.
            shutil.rmtree(PACKAGE_STATIC, ignore_errors=True)
            shutil.copytree(FRONTEND_BUILD, PACKAGE_STATIC)
        else:
            # Not fatal: the library and CLI are useful without the dashboard,
            # and this is the normal state of a checkout that has never run npm.
            self.announce(
                f"warning: no frontend build at {FRONTEND_BUILD}; the wheel will "
                "have no dashboard UI. Run `cd dashboard && npm run build` first.",
                level=3,
            )
        super().run()


setup(cmdclass={"build_py": BuildPyWithFrontend})
