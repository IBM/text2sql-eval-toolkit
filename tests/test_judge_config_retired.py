#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
The packaged judge configs, and the names they replaced.

1.6.0 replaced four Llama-based configs with two gpt-oss-120b ones: the default,
which compares a prediction with the ground truth, and ``llm_judge_no_gt``,
which judges without it. Both were tuned against a labelled set (see
``scripts/analysis/judge_calibration.py``). The retired names still load --
each resolves to its replacement -- so a script or saved request that names one
keeps working, but only where nothing else claims the name: a user's own config
of that name is theirs, and is used as written.
"""

import string

import pytest
import yaml

from text2sql_eval_toolkit.evaluation.llm_as_judge import (
    PACKAGED_JUDGE_CONFIG_DIR,
    RETIRED_JUDGE_CONFIGS,
    load_llm_judge_config,
)

ALL_FIELDS = {
    "question",
    "generation_prompt",
    "ground_truth_sql",
    "ground_truth_df",
    "predicted_sql",
    "predicted_df",
}
GROUND_TRUTH_FIELDS = {"ground_truth_sql", "ground_truth_df"}


def _packaged(name):
    return load_llm_judge_config(str(PACKAGED_JUDGE_CONFIG_DIR / f"{name}.yaml"))


def _fields(template):
    return {field for _, field, _, _ in string.Formatter().parse(template) if field}


def test_the_packaged_configs_are_the_two_gpt_oss_judges():
    names = sorted(p.stem for p in PACKAGED_JUDGE_CONFIG_DIR.glob("*.yaml"))
    assert names == ["llm_judge_default_config", "llm_judge_no_gt"]
    for name in names:
        model = _packaged(name)["model"]
        assert model["id"] == "wxai:openai/gpt-oss-120b"
        # The model reasons before it answers and the reasoning counts against
        # this budget; the Llama-era 512 left no room for the verdict.
        assert model["max_new_tokens"] >= 4096


def test_only_the_default_is_shown_the_ground_truth():
    default = load_llm_judge_config()["prompt_template"]
    no_gt = _packaged("llm_judge_no_gt")["prompt_template"]

    assert GROUND_TRUTH_FIELDS <= _fields(default) <= ALL_FIELDS
    assert {"question", "predicted_sql", "predicted_df"} <= _fields(no_gt)
    assert not _fields(no_gt) & GROUND_TRUTH_FIELDS
    assert _fields(no_gt) <= ALL_FIELDS

    # The judge fills these with str.format; a stray brace would raise at the
    # first call rather than here.
    for template in (default, no_gt):
        template.format(**{field: "x" for field in ALL_FIELDS})


@pytest.mark.parametrize("retired,replacement", sorted(RETIRED_JUDGE_CONFIGS.items()))
def test_a_retired_packaged_path_loads_its_replacement(retired, replacement):
    retired_path = PACKAGED_JUDGE_CONFIG_DIR / f"{retired}.yaml"
    assert not retired_path.exists()
    assert load_llm_judge_config(str(retired_path)) == _packaged(replacement)


def test_a_retired_name_outside_the_package_is_not_redirected(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_llm_judge_config(str(tmp_path / "llm_judge_alt_config.yaml"))


# --- the dashboard's by-name lookup -----------------------------------------


@pytest.fixture
def routers_judge(tmp_path, monkeypatch):
    pytest.importorskip("fastapi")
    monkeypatch.setenv("TEXT2SQL_DATA_ROOT", str(tmp_path))
    from text2sql_eval_toolkit.ui import routers_judge

    return routers_judge


@pytest.mark.parametrize("retired,replacement", sorted(RETIRED_JUDGE_CONFIGS.items()))
def test_the_dashboard_resolves_a_retired_name_to_its_replacement(
    routers_judge, retired, replacement
):
    assert routers_judge._resolve_judge_config_path(retired).name == (
        f"{replacement}.yaml"
    )
    assert routers_judge._load_judge_config_by_name(retired) == _packaged(replacement)


def test_a_user_config_with_a_retired_name_is_used_as_written(routers_judge, tmp_path):
    body = {
        "model": {"id": "anthropic:claude-sonnet-4-5"},
        "prompt_template": "mine: {question}",
    }
    user_dir = tmp_path / "llm_judge_config"
    user_dir.mkdir()
    (user_dir / "llm_judge_alt_config.yaml").write_text(
        yaml.safe_dump(body), encoding="utf-8"
    )
    assert routers_judge._load_judge_config_by_name("llm_judge_alt_config") == body
