#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#
"""
Characterisation tests over the public API.

These record what the library does *today*, so that the dashboard work planned
for 1.4.0 -- which reaches into the library for per-user credentials and usage
callbacks -- cannot change it unnoticed. Written before the refactors on purpose:
written after, they would encode whatever the refactor happened to produce.

Where today's behaviour is surprising, the test says so rather than asserting
what would be nicer. Changing it is a decision, and these are the places that
decision would show up.

Everything here is hermetic: no credentials, no database, no network.
"""

import asyncio
import json

import pandas as pd
import pytest

import text2sql_eval_toolkit as toolkit
from text2sql_eval_toolkit import (
    DEFAULT_REPO_ID,
    DEFAULT_REVISION,
    AgenticSQLGenerationPipeline,
    LLMSQLGenerationPipeline,
    LLMSQLGenerationPipelineSimple,
    add_summary_csv_suffix,
    add_summary_json_suffix,
    async_evaluate_predictions,
    compare_dfs_bird_eval_logic,
    evaluate_sql_prediction_with_llm,
    get_available_benchmarks,
    get_benchmark_info,
    get_benchmarks_info,
    get_default_eval_filename,
    get_question,
    is_sqlglot_parsable,
    is_sqlparse_parsable,
    load_llm_judge_config,
    parse_dataframe,
    print_summary,
    run_with_timeout,
    run_with_timeout_async,
    sqlglot_optimized_equivalence,
    sqlparse_queries_equivalent,
    summary_to_df_csv,
    truncate_dataframe,
)


class TestPackageMetadata:
    def test_version_is_a_release_string(self):
        assert isinstance(toolkit.__version__, str)
        assert toolkit.__version__.split(".")[0].isdigit()

    def test_hub_defaults(self):
        assert "/" in DEFAULT_REPO_ID
        assert isinstance(DEFAULT_REVISION, str) and DEFAULT_REVISION


class TestPathHelpers:
    """Pure string transforms the whole pipeline relies on for artifact naming."""

    def test_eval_filename_inserts_the_suffix_before_the_extension(self):
        assert (
            get_default_eval_filename("data/results/spider_dev-predictions.json")
            == "data/results/spider_dev-predictions_eval.json"
        )

    def test_summary_suffixes_replace_the_extension(self):
        base = "spider_dev-predictions_eval.json"
        assert (
            add_summary_json_suffix(base) == "spider_dev-predictions_eval_summary.json"
        )
        assert add_summary_csv_suffix(base) == "spider_dev-predictions_eval_summary.csv"

    def test_a_path_without_an_extension_still_works(self):
        assert add_summary_json_suffix("results") == "results_summary.json"


class TestRecordReading:
    def test_get_question_prefers_page_content(self):
        assert (
            get_question({"page_content": "a", "question": "b", "utterance": "c"})
            == "a"
        )

    def test_get_question_falls_back_in_order(self):
        assert get_question({"question": "b", "utterance": "c"}) == "b"
        assert get_question({"utterance": "c"}) == "c"

    def test_get_question_raises_key_error_not_value_error(self):
        # Differs from get_utterance, which raises ValueError. Both read the same
        # content; callers switching between them must expect different failures.
        with pytest.raises(KeyError):
            get_question({})


class TestDataframeSerialisation:
    def test_round_trip_through_the_stored_format(self):
        df = pd.DataFrame({"n": [1, 2], "s": ["a", "b"]})
        restored = parse_dataframe(df.to_json(orient="split"))
        pd.testing.assert_frame_equal(restored, df)

    def test_malformed_json_raises_value_error_naming_the_input(self):
        with pytest.raises(ValueError) as excinfo:
            parse_dataframe("{not json")
        assert "{not json" in str(excinfo.value)

    def test_truncate_keeps_head_and_tail_with_an_ellipsis_row(self):
        df = pd.DataFrame({"n": range(100)})
        out = truncate_dataframe(df, head=3, tail=2)
        assert len(out) == 6  # 3 + ellipsis + 2
        assert "..." in out.index

    def test_a_short_frame_is_returned_unchanged(self):
        df = pd.DataFrame({"n": [1, 2]})
        pd.testing.assert_frame_equal(truncate_dataframe(df, head=10, tail=10), df)


class TestBenchmarkRegistry:
    def test_available_benchmarks_are_listed(self):
        assert len(get_available_benchmarks()) > 0

    def test_benchmark_info_resolves_every_artifact_path(self):
        info = get_benchmark_info(get_available_benchmarks()[0])
        for key in (
            "benchmark_json_path",
            "schema_json_path",
            "predictions_path",
            "eval_results_path",
            "eval_summary_path",
        ):
            assert key in info, f"{key} missing from benchmark info"

    def test_eval_paths_are_derived_from_the_predictions_path(self):
        info = get_benchmark_info(get_available_benchmarks()[0])
        assert info["eval_results_path"].name.endswith("_eval.json")
        assert info["eval_summary_path"].name.endswith("_eval_summary.json")

    def test_unknown_benchmark_raises_value_error(self):
        with pytest.raises(ValueError):
            get_benchmark_info("no-such-benchmark-exists")

    def test_benchmarks_info_returns_every_registered_benchmark(self):
        info = get_benchmarks_info()
        assert isinstance(info, dict) and len(info) > 0


class TestSqlParsability:
    @pytest.mark.parametrize("sql", ["SELECT 1", "SELECT a FROM t WHERE b = 1"])
    def test_valid_sql_parses(self, sql):
        assert is_sqlglot_parsable(sql)
        assert is_sqlparse_parsable(sql)

    @pytest.mark.parametrize("sql", ["", "   "])
    def test_empty_input_is_not_parsable(self, sql):
        assert not is_sqlglot_parsable(sql)
        assert not is_sqlparse_parsable(sql)

    def test_db2_is_read_as_postgres(self):
        # sqlglot has no Db2 dialect; the toolkit substitutes Postgres.
        assert is_sqlglot_parsable("SELECT a FROM t", db_type="db2")

    def test_parsers_never_raise_on_junk(self):
        assert is_sqlglot_parsable("!!! not sql (((") in (True, False)
        assert is_sqlparse_parsable("!!! not sql (((") in (True, False)


class TestSqlEquivalence:
    def test_optimized_equivalence_returns_int_not_bool(self):
        # Documented inconsistency with its siblings; kept for compatibility.
        result = sqlglot_optimized_equivalence("SELECT a FROM t", "SELECT a FROM t")
        assert result == 1 and isinstance(result, int)

    def test_optimized_equivalence_sees_through_formatting(self):
        assert (
            sqlglot_optimized_equivalence("SELECT a FROM t", "select  a  from  t") == 1
        )

    def test_optimized_equivalence_returns_zero_when_unparseable(self):
        # Zero means "not shown to be equivalent", not "shown to differ".
        assert sqlglot_optimized_equivalence("((( bad", "SELECT a FROM t") == 0

    def test_sqlparse_equivalence_ignores_whitespace_and_case(self):
        assert sqlparse_queries_equivalent(
            "SELECT a FROM t WHERE b = 1", "select a\nfrom t\nwhere b = 1"
        )

    def test_sqlparse_equivalence_rejects_a_different_clause(self):
        assert not sqlparse_queries_equivalent(
            "SELECT a FROM t WHERE b = 1", "SELECT a FROM t WHERE b = 2"
        )


class TestBirdComparison:
    def test_row_order_is_ignored(self):
        a = pd.DataFrame({"x": [1, 2]})
        b = pd.DataFrame({"x": [2, 1]})
        assert compare_dfs_bird_eval_logic(a, b) == 1

    def test_duplicate_rows_collapse(self):
        # Inherited from BIRD: a set of tuples, so multiplicity is lost.
        one = pd.DataFrame({"x": [1]})
        three = pd.DataFrame({"x": [1, 1, 1]})
        assert compare_dfs_bird_eval_logic(one, three) == 1

    def test_values_are_compared_as_text(self):
        assert (
            compare_dfs_bird_eval_logic(
                pd.DataFrame({"x": [1]}), pd.DataFrame({"x": ["1"]})
            )
            == 1
        )
        assert (
            compare_dfs_bird_eval_logic(
                pd.DataFrame({"x": [1]}), pd.DataFrame({"x": [1.0]})
            )
            == 0
        )

    def test_different_contents_do_not_match(self):
        assert (
            compare_dfs_bird_eval_logic(
                pd.DataFrame({"x": [1]}), pd.DataFrame({"x": [2]})
            )
            == 0
        )


class TestTimeouts:
    def test_returns_the_result_when_it_completes(self):
        assert run_with_timeout(lambda: 42, timeout=5) == 42

    def test_arguments_are_passed_through(self):
        assert run_with_timeout(lambda a, b=0: a + b, 5, 2, 0, 1, b=2) is not None

    def test_async_variant_returns_the_result(self):
        async def task():
            return "done"

        assert asyncio.run(run_with_timeout_async(task, base_timeout=5)) == "done"

    def test_async_variant_gives_up_after_the_retries(self):
        async def never():
            await asyncio.sleep(10)

        with pytest.raises(asyncio.TimeoutError):
            asyncio.run(
                run_with_timeout_async(never, base_timeout=0.01, retries=1, wait=0)
            )


class TestJudgeConfig:
    def test_the_packaged_default_loads(self):
        config = load_llm_judge_config()
        assert config["model"]["id"].startswith("wxai:")
        assert config.get("prompt_template")

    def test_a_missing_path_raises_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            load_llm_judge_config("/nonexistent/judge.yaml")

    def test_any_supported_prefix_may_judge(self, monkeypatch):
        """
        The judge accepted only ``wxai:`` until the dispatch tables were merged.
        It now reaches whatever the pipelines reach, so a judge config can name
        an Anthropic or OpenAI model.
        """
        seen = {}

        class FakeClient:
            def __init__(self, model_name, model_parameters):
                seen["model"] = model_name

            def generate_sql(self, prompt, postprocess=True):
                return "Yes, correct", {"prompt_tokens": 1, "completion_tokens": 1}

        monkeypatch.setattr(
            "text2sql_eval_toolkit.inference.model_clients.ClaudeClientChatAPI",
            FakeClient,
        )
        result = evaluate_sql_prediction_with_llm(
            question="q",
            ground_truth_sql="SELECT 1",
            ground_truth_df="{}",
            predicted_sql="SELECT 1",
            predicted_df="{}",
            generation_prompt="p",
            llm_judge_config={
                "model": {"id": "anthropic:claude-sonnet-4-5"},
                "prompt_template": "{question}",
            },
        )
        assert result["verdict"] == "Yes"
        assert seen["model"] == "claude-sonnet-4-5"  # prefix stripped for the provider

    def test_an_unknown_prefix_is_still_refused(self):
        config = {"model": {"id": "nosuchprovider:x"}, "prompt_template": "{question}"}
        with pytest.raises(NotImplementedError):
            evaluate_sql_prediction_with_llm(
                question="q",
                ground_truth_sql="SELECT 1",
                ground_truth_df="{}",
                predicted_sql="SELECT 1",
                predicted_df="{}",
                generation_prompt="p",
                llm_judge_config=config,
            )

    @pytest.mark.parametrize(
        "reply,verdict,score",
        [
            ("Yes, the query is correct", "Yes", 1.0),
            ("No, it selects the wrong column", "No", 0.0),
            ("Maybe, it depends on the schema", "Maybe", 0.5),
            ("something unparseable", "N/A", 0.0),
        ],
    )
    def test_verdicts_are_read_from_the_reply(self, monkeypatch, reply, verdict, score):
        """The judge maps a free-text reply onto a verdict and a score."""

        class FakeClient:
            def __init__(self, model_name, model_parameters):
                pass

            def generate_sql(self, prompt, postprocess=True):
                # postprocess must be False here: the judge asks for text, and
                # SQL post-processing would edit prose.
                assert postprocess is False
                return reply, {"prompt_tokens": 10, "completion_tokens": 5}

        monkeypatch.setattr(
            "text2sql_eval_toolkit.inference.model_clients.WXAIClientChatAPI",
            FakeClient,
        )
        result = evaluate_sql_prediction_with_llm(
            question="q",
            ground_truth_sql="SELECT 1",
            ground_truth_df="{}",
            predicted_sql="SELECT 1",
            predicted_df="{}",
            generation_prompt="p",
            llm_judge_config={
                "model": {"id": "wxai:x"},
                "prompt_template": "{question}",
            },
        )
        assert result["verdict"] == verdict
        assert result["score"] == score
        assert "token_usage" in result


class TestEvaluationEntryPoints:
    def test_async_variant_writes_the_three_artifacts(self, tmp_path):
        predictions = tmp_path / "b-predictions.json"
        predictions.write_text(json.dumps([]), encoding="utf-8")

        data, summary_df = asyncio.run(async_evaluate_predictions(str(predictions)))

        assert data == []
        assert (tmp_path / "b-predictions_eval.json").exists()
        assert (tmp_path / "b-predictions_eval_summary.json").exists()
        assert (tmp_path / "b-predictions_eval_summary.csv").exists()

    def test_output_paths_can_be_overridden(self, tmp_path):
        predictions = tmp_path / "b-predictions.json"
        predictions.write_text(json.dumps([]), encoding="utf-8")
        out = tmp_path / "custom_eval.json"

        asyncio.run(async_evaluate_predictions(str(predictions), output_file=str(out)))
        assert out.exists()

    def test_summary_to_csv_returns_the_frame_it_wrote(self, tmp_path):
        summary = {
            "pipeA": {"num_records": 2, "num_evaluated": 2},
            "llm_judge_config": {},
        }
        out = tmp_path / "s.csv"
        df = summary_to_df_csv(summary, str(out), use_llm=False)

        assert out.exists()
        assert len(df) == 1  # the judge config is not a pipeline
        assert df.iloc[0]["Model"] == "pipeA"

    def test_judge_columns_are_present_but_not_scored_without_the_judge(self, tmp_path):
        """
        The column set is stable whether or not the judge ran, so a CSV from one
        run lines up with a CSV from another. The score column reads "N/A"; the
        error count is a real 0, not a placeholder -- so a reader cannot use
        "is it a string" to tell whether the judge ran.
        """
        summary = {"pipeA": {"num_records": 1}}
        df = summary_to_df_csv(summary, str(tmp_path / "s.csv"), use_llm=False)

        assert df.iloc[0]["Number of Correct Results According to LLM Judge"] == "N/A"
        assert df.iloc[0]["LLM Judge Errors"] == 0

    def test_print_summary_writes_to_stdout(self, capsys):
        print_summary({"pipeA": {"num_records": 1, "num_evaluated": 1}}, use_llm=False)
        assert "pipeA" in capsys.readouterr().out


class TestPipelineConstruction:
    """The pipelines need endpoints to run; construction and guards do not."""

    @pytest.mark.parametrize(
        "cls",
        [
            LLMSQLGenerationPipeline,
            LLMSQLGenerationPipelineSimple,
            AgenticSQLGenerationPipeline,
        ],
    )
    def test_pipelines_construct_without_credentials(self, cls):
        assert cls() is not None

    @pytest.mark.parametrize(
        "cls", [LLMSQLGenerationPipeline, LLMSQLGenerationPipelineSimple]
    )
    def test_run_pipeline_is_callable(self, cls):
        assert callable(cls().run_pipeline)
