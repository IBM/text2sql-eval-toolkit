import importlib


def test_top_level_import_and_symbols():
    mod = importlib.import_module("text2sql_eval_toolkit")

    # Core evaluation API
    for name in [
        "evaluate_prediction",
        "evaluate_predictions",
        "run_evaluation",
    ]:
        assert hasattr(mod, name), f"Missing expected API symbol: {name}"

    # Execution and benchmark utilities
    for name in [
        "run_execution",
        "get_available_benchmarks",
        "get_benchmarks_info",
        "get_benchmark_info",
    ]:
        assert hasattr(mod, name), f"Missing expected API symbol: {name}"

    # Low-level SQL comparison helpers (re-exported from toolkit metrics)
    for name in [
        "compare_result_dfs",
        "sql_exact_match",
    ]:
        assert hasattr(mod, name), f"Missing expected helper symbol: {name}"


def test_get_available_benchmarks_non_empty():
    mod = importlib.import_module("text2sql_eval_toolkit")
    get_available_benchmarks = mod.get_available_benchmarks

    benchmarks = get_available_benchmarks()
    assert isinstance(benchmarks, list)
    # The packaged benchmark metadata should define at least one benchmark id
    assert len(benchmarks) > 0


def test_every_exported_symbol_is_documented():
    """
    The README points readers at these docstrings for the public API, and PyPI
    renders that README as the project's front door. When this was first
    measured, 25 of the 41 exported symbols had no docstring at all -- so the
    promise was broken for most of the surface it described.

    A generated API reference is only as good as what it has to render, which is
    why this is enforced rather than reviewed.
    """
    import inspect

    mod = importlib.import_module("text2sql_eval_toolkit")

    undocumented = []
    thin = []
    for name in mod.__all__:
        obj = getattr(mod, name, None)
        assert obj is not None, f"{name} is exported in __all__ but not importable"
        doc = inspect.getdoc(obj) or ""
        if not doc.strip():
            undocumented.append(name)
        elif len(doc.splitlines()) < 3:
            # A one-liner does not carry arguments, return shape or failure
            # modes, which is what a caller actually needs.
            thin.append(name)

    assert not undocumented, f"Exported symbols with no docstring: {undocumented}"
    assert not thin, f"Exported symbols with a one-line docstring: {thin}"


def test_documented_arguments_match_real_signatures():
    """
    A docstring that names arguments the function does not take is worse than
    none: it is confidently wrong, and it survives refactors that rename
    parameters. This catches the drift for Google-style ``Args:`` blocks.
    """
    import inspect
    import re

    mod = importlib.import_module("text2sql_eval_toolkit")

    wrong = []
    for name in mod.__all__:
        obj = getattr(mod, name)
        if not (inspect.isfunction(obj) or inspect.ismethod(obj)):
            continue
        doc = inspect.getdoc(obj) or ""
        if "Args:" not in doc:
            continue
        block = doc.split("Args:", 1)[1]
        for section in ("Returns:", "Raises:", "Example:", "Note:", "See Also:"):
            block = block.split(section, 1)[0]
        documented = set(re.findall(r"^\s{0,4}(\w+):", block, re.M))
        actual = set(inspect.signature(obj).parameters)
        for arg in documented - actual:
            wrong.append(f"{name}: documents '{arg}', which is not a parameter")

    assert not wrong, "Docstrings disagree with signatures:\n  " + "\n  ".join(wrong)
