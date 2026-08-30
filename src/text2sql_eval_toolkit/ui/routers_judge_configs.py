#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Reading and writing the LLM-judge prompt configurations.

Writes land in the data root, not in the installed package: a config written
through the dashboard shadows the packaged one of the same name, and deleting
it restores the original. Writing into the package instead -- which this did
until it met a container whose package tree is root-owned -- fails outright for
an unprivileged server process, and is discarded by the next pip upgrade even
where the permissions allow it.

The write endpoint is ``full`` tier. Names are validated as a plain stem with
containment asserted on the resolved path -- belt and braces, because the first
version of this interpolated a URL segment straight into a path.
"""

from typing import Any, Dict, List

from fastapi import (
    APIRouter,
    Body,
    HTTPException,
)

import text2sql_eval_toolkit.env_loader  # noqa: F401 — load .env (WATSONX_*, etc.) before eval/inference

from text2sql_eval_toolkit.ui import runtime
from text2sql_eval_toolkit.ui.capabilities import Tier
from text2sql_eval_toolkit.ui.models import (
    LLMJudgeConfigInfo,
    LLMJudgeConfigListResponse,
)

from text2sql_eval_toolkit.logging import get_logger

logger = get_logger(__name__)

# The path resolution and its containment check live with the judge itself, so
# there is one implementation of "which file does this name mean".
from text2sql_eval_toolkit.ui.routers_judge import (  # noqa: E402
    _judge_config_dir,
    _judge_config_write_path,
    _resolve_judge_config_path,
    _user_judge_config_dir,
)

router = APIRouter()


@router.get("/api/llm-judge/configs", response_model=LLMJudgeConfigListResponse)
def list_llm_judge_configs() -> LLMJudgeConfigListResponse:
    """
    List available LLM-judge YAML config files.
    """
    # Clients address a config by name; only the full-mode editor has any use for
    # a filesystem path, and it is a path *inside the installed package*, which
    # is exactly the layout detail shared modes withhold everywhere else.
    reveal_path = runtime.get_mode() is Tier.FULL

    # A user copy shadows the packaged config it shares a name with, so the two
    # directories are merged by name rather than concatenated -- otherwise an
    # edited config appears twice, and the picker offers a stale copy that
    # nothing will ever read.
    user_names = {p.stem for p in _user_judge_config_dir().glob("*.yaml")}
    packaged_names = {p.stem for p in _judge_config_dir().glob("*.yaml")}

    items: List[LLMJudgeConfigInfo] = []
    for name in sorted(user_names | packaged_names):
        path = _resolve_judge_config_path(name)
        items.append(
            LLMJudgeConfigInfo(
                name=name,
                path=str(path) if reveal_path else "",
                user_defined=name in user_names,
            )
        )
    return LLMJudgeConfigListResponse(items=items)


@router.get("/api/llm-judge/configs/{name}", response_model=Dict[str, Any])
def get_llm_judge_config(name: str):
    """
    Return the parsed YAML config by name (stem).
    """

    try:
        path = _resolve_judge_config_path(name)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Config not found") from None
    if not path.exists():
        raise HTTPException(status_code=404, detail="Config not found")

    import yaml

    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


@router.put("/api/llm-judge/configs/{name}", response_model=Dict[str, Any])
def update_llm_judge_config(name: str, body: Dict[str, Any] = Body(...)):
    """
    Overwrite a YAML config file with the provided structure.
    Performs minimal validation (must have model.id and prompt_template).
    """
    model_cfg = body.get("model") or {}
    if "id" not in model_cfg:
        raise HTTPException(status_code=400, detail="model.id is required")
    if "prompt_template" not in body:
        raise HTTPException(status_code=400, detail="prompt_template is required")

    try:
        path = _judge_config_write_path(name)
    except FileNotFoundError:
        raise HTTPException(status_code=400, detail="Invalid config name") from None
    path.parent.mkdir(parents=True, exist_ok=True)

    import yaml

    try:
        with path.open("w", encoding="utf-8") as f:
            yaml.safe_dump(body, f, sort_keys=False, allow_unicode=True)
    except OSError as exc:
        # The data root is meant to be writable; if it is not, say so plainly.
        # This used to surface as a bare 500 with the traceback in the log and
        # nothing actionable in the browser.
        logger.error("Failed to write judge config %s: %s", name, exc)
        raise HTTPException(
            status_code=500,
            detail=f"Could not write the config: {exc.strerror or exc}",
        ) from None

    return body


@router.delete("/api/llm-judge/configs/{name}", response_model=Dict[str, Any])
def delete_llm_judge_config(name: str):
    """
    Delete a config written through the dashboard.

    Only the user copy is removed. Where it was shadowing a packaged config the
    packaged one becomes visible again, which is the way back from an edit that
    broke a shipped config; the packaged files themselves are never deleted.
    """
    try:
        path = _judge_config_write_path(name)
    except FileNotFoundError:
        raise HTTPException(status_code=400, detail="Invalid config name") from None
    if not path.is_file():
        raise HTTPException(
            status_code=404, detail="No user-defined config by that name"
        )

    try:
        path.unlink()
    except OSError as exc:
        logger.error("Failed to delete judge config %s: %s", name, exc)
        raise HTTPException(
            status_code=500,
            detail=f"Could not delete the config: {exc.strerror or exc}",
        ) from None

    # Say whether a packaged config has just been uncovered, so the caller knows
    # if the name still resolves to something.
    return {
        "deleted": name,
        "reverted_to_packaged": _resolve_judge_config_path(name).is_file(),
    }
