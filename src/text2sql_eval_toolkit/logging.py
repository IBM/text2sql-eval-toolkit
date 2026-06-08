#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

import logging
import os
import sys
from pathlib import Path

from tqdm import tqdm

_RESET = "\033[0m"
_DIM = "\033[2m"

_LEVEL_COLORS = {
    logging.DEBUG: "\033[36m",  # cyan
    logging.INFO: "\033[32m",  # green
    logging.WARNING: "\033[33m",  # yellow
    logging.ERROR: "\033[31m",  # red
    logging.CRITICAL: "\033[1;31m",  # bold red
}


def _color_enabled() -> bool:
    if os.environ.get("NO_COLOR"):
        return False
    if os.environ.get("FORCE_COLOR"):
        return True
    return hasattr(sys.stderr, "isatty") and sys.stderr.isatty()


class ColoredFormatter(logging.Formatter):
    """ANSI-colored console formatter; level name is tinted by severity."""

    def __init__(self, *args, use_color: bool | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.use_color = _color_enabled() if use_color is None else use_color

    def format(self, record: logging.LogRecord) -> str:
        if self.use_color:
            color = _LEVEL_COLORS.get(record.levelno, "")
            record.levelname = f"{color}{record.levelname:<8}{_RESET}"
            record.asctime = (
                f"{_DIM}{self.formatTime(record, self.datefmt)}{_RESET}"
            )
        return super().format(record)


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
        console_formatter = ColoredFormatter(
            "%(asctime)s - %(levelname)s - %(message)s", datefmt="%H:%M:%S"
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

        # Default log file path relative to the project root
        if log_file is None:
            project_root = (
                Path(__file__).resolve().parents[2]
            )  # Go up from src/text2sql_eval_toolkit
            log_file = project_root / "data" / "results" / "bak" / "log.txt"

        log_file = Path(log_file)
        log_file.parent.mkdir(parents=True, exist_ok=True)  # Ensure directory exists

        # File handler (plain text, no ANSI codes)
        file_handler = logging.FileHandler(log_file, mode="w")
        file_formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

    return logger
