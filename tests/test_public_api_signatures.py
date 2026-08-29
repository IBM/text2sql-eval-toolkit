#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#
"""
The public API is a published contract.

These functions are on PyPI and other people import them. 1.4.0 adds a dashboard
that needs things from the library -- per-user credentials, usage callbacks --
and the standing requirement is that none of that changes what an existing caller
sees. A requirement nothing checks is a hope, so this checks it.

The snapshot in ``tests/data/public_api_snapshot.json`` records every exported
symbol's parameters, their order, their kind, and their defaults. Any change
fails here with a diff naming the symbol.

**When this test fails**, decide which happened:

- An accidental change. Restore the signature.
- A deliberate, additive change -- a new optional parameter with a default that
  preserves today's behaviour. Regenerate the snapshot:
  ``python -m tests.regen_api_snapshot``.
- A breaking change. It needs a major version and a changelog entry, not a
  snapshot refresh.
"""

import inspect
import json
from pathlib import Path

import pytest

import text2sql_eval_toolkit as toolkit

SNAPSHOT_PATH = Path(__file__).parent / "data" / "public_api_snapshot.json"


def _describe(name):
    """Describe one exported symbol the same way the snapshot records it."""
    obj = getattr(toolkit, name)
    if inspect.isclass(obj):
        target, kind = obj.__init__, "class"
    elif callable(obj):
        target, kind = obj, "function"
    else:
        return {"kind": "value"}
    try:
        signature = inspect.signature(target)
    except (TypeError, ValueError):  # pragma: no cover - builtins
        return {"kind": kind}
    params = []
    for param_name, param in signature.parameters.items():
        if param_name == "self":
            continue
        entry = {"name": param_name, "kind": str(param.kind)}
        if param.default is not inspect.Parameter.empty:
            entry["default"] = repr(param.default)
        params.append(entry)
    return {"kind": kind, "params": params}


@pytest.fixture(scope="module")
def snapshot():
    return json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))


def test_no_exported_symbol_disappeared(snapshot):
    """Removing an export breaks every caller that imports it."""
    missing = sorted(set(snapshot) - set(toolkit.__all__))
    assert not missing, f"Exported symbols removed from __all__: {missing}"


def test_new_exports_are_recorded(snapshot):
    """
    Additions are fine, but the snapshot has to know about them -- otherwise it
    silently stops covering the part of the API that is changing.
    """
    unrecorded = sorted(set(toolkit.__all__) - set(snapshot))
    assert not unrecorded, (
        f"New exports not in the snapshot: {unrecorded}. "
        "Run `python -m tests.regen_api_snapshot` if the addition is intended."
    )


@pytest.mark.parametrize("name", sorted(json.loads(SNAPSHOT_PATH.read_text())))
def test_signature_is_unchanged(name, snapshot):
    """Each symbol keeps its parameters, their order, and their defaults."""
    if name not in toolkit.__all__:
        pytest.skip("covered by test_no_exported_symbol_disappeared")

    expected = snapshot[name]
    actual = _describe(name)

    assert (
        actual["kind"] == expected["kind"]
    ), f"{name} changed from {expected['kind']} to {actual['kind']}"
    if "params" not in expected:
        return

    old = {p["name"]: p for p in expected["params"]}
    new = {p["name"]: p for p in actual["params"]}

    removed = [p for p in old if p not in new]
    assert not removed, f"{name}: parameters removed: {removed}"

    for param, spec in old.items():
        assert new[param]["kind"] == spec["kind"], (
            f"{name}: parameter '{param}' changed kind "
            f"({spec['kind']} -> {new[param]['kind']})"
        )
        assert new[param].get("default") == spec.get("default"), (
            f"{name}: default for '{param}' changed "
            f"({spec.get('default')} -> {new[param].get('default')})"
        )

    old_order = [p["name"] for p in expected["params"]]
    new_order = [p["name"] for p in actual["params"] if p["name"] in old]
    assert (
        new_order == old_order
    ), f"{name}: parameter order changed {old_order} -> {new_order}"

    added = [p for p in actual["params"] if p["name"] not in old]
    for param in added:
        assert "default" in param, (
            f"{name}: new parameter '{param['name']}' has no default, so it "
            "breaks every existing caller. Give it one."
        )
