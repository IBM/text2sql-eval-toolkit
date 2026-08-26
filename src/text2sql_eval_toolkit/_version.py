#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
The one place the toolkit's version is resolved.

``pyproject.toml`` is the single source: this reads it back from the installed
distribution's metadata rather than repeating the literal.  Before, the number
was written out in ``pyproject.toml`` and again in ``__init__.py``, and
``results/_hub`` resolved it a third way -- which is how the package came to
claim 1.1.0 while the changelog documented a 1.2.0 release whose features were
already in the code.

Falls back to ``0.0.0`` when the package is not installed at all, which happens
when the source tree is on ``sys.path`` directly.  ``_hub`` treats that value as
"unknown" and skips its compatibility gate rather than refusing to run.
"""

from __future__ import annotations

#: Sentinel for "running from a source tree that was never installed".
UNKNOWN_VERSION = "0.0.0"

try:
    from importlib.metadata import PackageNotFoundError, version as _pkg_version

    try:
        __version__: str = _pkg_version("text2sql-eval-toolkit")
    except PackageNotFoundError:  # pragma: no cover - uninstalled source tree
        __version__ = UNKNOWN_VERSION
except ImportError:  # pragma: no cover - importlib.metadata is stdlib on 3.11+
    __version__ = UNKNOWN_VERSION

__all__ = ["UNKNOWN_VERSION", "__version__"]
