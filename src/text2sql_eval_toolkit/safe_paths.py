#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Building file paths from names that come from outside the program.

Benchmark ids, judge config names and database ids arrive in URLs and request
bodies, and each of them ends up as part of a file name. `safe_join` is the one
place that turns such a name into a path.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Union


def safe_join(base: Union[str, os.PathLike], *names: str) -> Path:
    """
    Join plain file or folder names onto *base*, refusing to leave it.

    Each name must be a single path component: no separators, not empty, and not
    ``.`` or ``..``. The joined path is then normalized and checked to start
    with *base*. The name check already rules out an escape; the prefix check is
    kept because it is the form CodeQL recognizes as a path-injection guard.

    Symlinks are not resolved, so a data folder that is a symlink to another
    disk keeps working.

    Args:
        base: The folder the result must stay inside.
        *names: The names to join, outermost first.

    Returns:
        Path: The absolute, normalized path.

    Raises:
        ValueError: If a name is not a plain name, or the result is not
            inside *base*.

    Example:
        ```python
        >>> safe_join("/data/results", "spider-predictions_eval.json")
        PosixPath('/data/results/spider-predictions_eval.json')
        >>> safe_join("/data/results", "../secrets.json")
        Traceback (most recent call last):
        ...
        ValueError: not a plain name: '../secrets.json'
        ```
    """
    if not names:
        raise ValueError("no name given")
    for name in names:
        if (
            not isinstance(name, str)
            or name in ("", ".", "..")
            or "/" in name
            or "\\" in name
            or "\0" in name
        ):
            raise ValueError(f"not a plain name: {name!r}")
    root = os.path.abspath(base)
    full = os.path.abspath(os.path.join(root, *names))
    prefix = root if root.endswith(os.sep) else root + os.sep
    if not full.startswith(prefix):
        raise ValueError(f"not a plain name: {'/'.join(names)!r}")
    return Path(full)
