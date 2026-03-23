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
    get_available_benchmarks = getattr(mod, "get_available_benchmarks")

    benchmarks = get_available_benchmarks()
    assert isinstance(benchmarks, list)
    # The packaged benchmark metadata should define at least one benchmark id
    assert len(benchmarks) > 0

