#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

import logging
import os
from pathlib import Path
from typing import Optional

from tqdm import tqdm


class TqdmLoggingHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.write(msg)
            self.flush()
        except Exception:
            self.handleError(record)


def get_logger(
    name: str = "text2sql_eval_toolkit", level=logging.DEBUG, log_file: str = None
):
    logger = logging.getLogger(name)

    if not logger.handlers:
        logger.setLevel(level)

        console_handler = TqdmLoggingHandler()
        console_formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s", datefmt="%H:%M:%S"
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

        resolved = Path(log_file) if log_file is not None else default_log_file()
        if resolved is not None:
            _attach_file_handler(logger, resolved)

    return logger


def default_log_file() -> Optional[Path]:
    """
    Where to write a log by default, or ``None`` for console only.

    Only a source checkout gets a file. The previous version derived the path as
    ``Path(__file__).parents[2] / "data/results/bak/log.txt"``, which is the
    repository root in a checkout but the *interpreter's library directory* once
    the package is pip-installed. There it tried to create
    ``/usr/local/lib/python3.13/data/results/bak`` -- raising at import time
    wherever that directory is read-only, which is every container, and
    littering it where it is not.

    ``TEXT2SQL_LOG_FILE`` overrides, for anyone who wants a log from an
    installed copy.
    """
    override = os.getenv("TEXT2SQL_LOG_FILE")
    if override:
        return Path(override).expanduser()

    root = Path(__file__).resolve().parents[2]
    # `pyproject.toml` marks the checkout root; site-packages has no such file.
    # This is the same signal `get_writable_data_root()` uses.
    if (root / "pyproject.toml").is_file():
        return root / "data" / "results" / "bak" / "log.txt"
    return None


def _attach_file_handler(logger: logging.Logger, path: Path) -> None:
    """
    Add a file handler, or carry on without one.

    Logging is a diagnostic aid; being unable to write it is not a reason to
    stop the program, and `get_logger` is called at module scope across the
    package -- so anything raised here happens during import and takes the whole
    process down before it can report why.
    """
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        # Append, not truncate. Two dashboards on different ports is an
        # ordinary thing to do, and with "w" the second one truncates the
        # first's file while the first keeps writing at its old offset -- which
        # produces a log full of half-lines in the wrong order. A diagnostic
        # that misleads while you are debugging is worse than no diagnostic.
        handler = logging.FileHandler(path, mode="a")
    except OSError as exc:
        logger.warning("Logging to file is disabled (%s): %s", path, exc)
        return

    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
    )
    logger.addHandler(handler)
