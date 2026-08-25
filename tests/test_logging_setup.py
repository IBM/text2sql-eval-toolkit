#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
``get_logger`` is called at module scope throughout the package, so anything it
raises happens during ``import text2sql_eval_toolkit`` and takes the process
down before it can say why.

It used to derive a default log path as ``Path(__file__).parents[2] /
"data/results/bak/log.txt"``. That is the repository root in a checkout and the
*interpreter's library directory* once pip-installed -- so an installed copy
tried to create ``/usr/local/lib/python3.13/data/results/bak``, crashing on
import wherever that is read-only (every container) and littering it where it is
not. It was found by the container image failing to start in CI.
"""

import logging

import pytest

from text2sql_eval_toolkit import logging as toolkit_logging
from text2sql_eval_toolkit.logging import (
    _attach_file_handler,
    default_log_file,
    get_logger,
)


@pytest.fixture(autouse=True)
def no_inherited_override(monkeypatch):
    monkeypatch.delenv("TEXT2SQL_LOG_FILE", raising=False)


def _pretend_installed_at(monkeypatch, tmp_path):
    """Point the module at a location with no pyproject.toml above it."""
    fake = tmp_path / "site-packages" / "text2sql_eval_toolkit" / "logging.py"
    fake.parent.mkdir(parents=True)
    fake.write_text("", encoding="utf-8")
    monkeypatch.setattr(toolkit_logging, "__file__", str(fake))


# --- choosing a default ---------------------------------------------------


def test_a_checkout_still_gets_its_log_file():
    """The behaviour a local run had before; nobody should notice a change."""
    path = default_log_file()
    assert path is not None
    assert path.name == "log.txt"
    # log.txt -> bak -> results -> data -> repository root
    assert (path.parents[3] / "pyproject.toml").is_file()


def test_an_installed_copy_gets_no_file(monkeypatch, tmp_path):
    """
    This is the fix. Console-only beats writing into site-packages, and beats
    crashing when site-packages is read-only.
    """
    _pretend_installed_at(monkeypatch, tmp_path)
    assert default_log_file() is None


def test_an_installed_copy_writes_nothing_into_the_library_directory(
    monkeypatch, tmp_path
):
    _pretend_installed_at(monkeypatch, tmp_path)
    default_log_file()
    assert not (tmp_path / "site-packages" / "data").exists()


def test_the_environment_can_ask_for_a_log_anywhere(monkeypatch, tmp_path):
    target = tmp_path / "logs" / "toolkit.log"
    monkeypatch.setenv("TEXT2SQL_LOG_FILE", str(target))
    assert default_log_file() == target


def test_the_override_wins_even_in_a_checkout(monkeypatch, tmp_path):
    target = tmp_path / "elsewhere.log"
    monkeypatch.setenv("TEXT2SQL_LOG_FILE", str(target))
    assert default_log_file() == target


def test_a_tilde_in_the_override_is_expanded(monkeypatch):
    monkeypatch.setenv("TEXT2SQL_LOG_FILE", "~/toolkit.log")
    resolved = default_log_file()
    assert resolved is not None
    assert "~" not in str(resolved)


# --- failing to write must not be fatal -----------------------------------


def test_an_unwritable_path_warns_and_carries_on(tmp_path, caplog):
    """
    A diagnostic that cannot be written is not a reason to stop the program --
    and raising here would raise during import.
    """
    blocked = tmp_path / "a-file-not-a-directory"
    blocked.write_text("", encoding="utf-8")
    logger = logging.getLogger("test-unwritable")
    logger.handlers.clear()

    with caplog.at_level(logging.WARNING):
        _attach_file_handler(logger, blocked / "nested" / "log.txt")

    assert not any(isinstance(h, logging.FileHandler) for h in logger.handlers)
    assert "Logging to file is disabled" in caplog.text


def test_a_writable_path_does_get_a_handler(tmp_path):
    logger = logging.getLogger("test-writable")
    logger.handlers.clear()
    target = tmp_path / "nested" / "log.txt"

    _attach_file_handler(logger, target)

    assert any(isinstance(h, logging.FileHandler) for h in logger.handlers)
    assert target.parent.is_dir()
    for handler in logger.handlers:
        handler.close()


# --- the logger itself ----------------------------------------------------


def test_get_logger_works_with_no_file_at_all(monkeypatch, tmp_path):
    _pretend_installed_at(monkeypatch, tmp_path)
    logger = get_logger("test-no-file")
    logger.handlers.clear()
    logger = get_logger("test-no-file")

    logger.info("this must not raise")
    assert not any(isinstance(h, logging.FileHandler) for h in logger.handlers)


def test_an_explicit_path_is_honoured(tmp_path):
    target = tmp_path / "explicit.log"
    logger = get_logger("test-explicit", log_file=str(target))
    logger.info("recorded")
    for handler in logger.handlers:
        handler.flush()

    assert target.is_file()
    assert "recorded" in target.read_text(encoding="utf-8")


def test_handlers_are_not_added_twice(tmp_path):
    """`get_logger` is called from many modules with the same name."""
    first = get_logger("test-once", log_file=str(tmp_path / "once.log"))
    count = len(first.handlers)
    second = get_logger("test-once", log_file=str(tmp_path / "once.log"))
    assert second is first
    assert len(second.handlers) == count
    for handler in first.handlers:
        handler.close()


def test_importing_the_package_never_depends_on_a_writable_install(
    monkeypatch, tmp_path
):
    """
    The property that actually matters, stated directly: with the module living
    somewhere unwritable and no override, building a logger must still work.
    """
    _pretend_installed_at(monkeypatch, tmp_path)
    logger = get_logger("test-import-safety")
    logger.handlers.clear()
    assert get_logger("test-import-safety") is logger
