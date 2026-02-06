#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

import logging
from tqdm import tqdm
from pathlib import Path


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

        # Default log file path relative to the project root
        if log_file is None:
            project_root = (
                Path(__file__).resolve().parents[2]
            )  # Go up from src/text2sql_eval_toolkit
            log_file = project_root / "data" / "results" / "bak" / "log.txt"

        log_file = Path(log_file)
        log_file.parent.mkdir(parents=True, exist_ok=True)  # Ensure directory exists

        # File handler
        file_handler = logging.FileHandler(log_file, mode="w")
        file_formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

    return logger
