#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Reading and writing the LLM-judge prompt configurations.

The write endpoint is ``full`` tier and lands YAML inside the installed package
directory, so the name is validated as a plain stem and containment is asserted
on the resolved path -- belt and braces, because the first version of this
interpolated a URL segment straight into a path.
"""

from pathlib import Path
from typing import Any, Dict, List

from fastapi import (
    APIRouter,
    Body,
    HTTPException,
)

import text2sql_eval_toolkit.env_loader  # noqa: F401 — load .env (WATSONX_*, etc.) before eval/inference

from text2sql_eval_toolkit.ui.models import (
    LLMJudgeConfigInfo,
    LLMJudgeConfigListResponse,
)

# Runtime state (deployment ceiling, judge allowlist, request identity) and the
# middleware stack live in their own modules.  Their names are re-exported here
# because ``server.set_mode`` / ``server.reset_rate_limits`` are the surface the
# CLI and the tests already use -- and re-exporting is only safe because they are
# accessors over module state rather than the state itself.
from text2sql_eval_toolkit.ui.indexes import (  # noqa: F401
    EVAL_INDEX_CACHE,
    get_index,
    invalidate_index_cache,
)
from text2sql_eval_toolkit.ui.paths import (  # noqa: F401
    _eval_not_found_detail,
    _summary_not_found_detail,
    count_records,
    get_data_root,
    get_results_dir,
    load_json,
)
from text2sql_eval_toolkit.ui.registry import (  # noqa: F401
    ALLOWED_DB_TYPES,
    ALLOWED_LOGO_EXTENSIONS,
    MAX_LOGO_UPLOAD_BYTES,
    STATIC_ASSET_SUBDIR,
    get_benchmark_registry_path,
    load_benchmark_registry,
    normalize_benchmark_config,
    normalize_benchmark_id,
    write_json_atomic,
)
from text2sql_eval_toolkit.ui.middleware import reset_rate_limits  # noqa: F401
from text2sql_eval_toolkit.ui.runtime import (  # noqa: F401
    _cookie_secure,
    current_user_email,
    get_judge_allowlist,
    get_mode,
    set_judge_allowlist,
    set_mode,
)
from text2sql_eval_toolkit.logging import get_logger

logger = get_logger(__name__)

# The path resolution and its containment check live with the judge itself, so
# there is one implementation of "which file does this name mean".
from text2sql_eval_toolkit.ui.routers_judge import (  # noqa: E402
    _resolve_judge_config_path,
)

router = APIRouter()


@router.get("/api/llm-judge/configs", response_model=LLMJudgeConfigListResponse)
def list_llm_judge_configs() -> LLMJudgeConfigListResponse:
    """
    List available LLM-judge YAML config files.
    """
    from text2sql_eval_toolkit.evaluation import llm_as_judge

    base_dir = Path(llm_as_judge.__file__).parent / "llm_judge_config"
    items: List[LLMJudgeConfigInfo] = []
    if base_dir.exists():
        for path in sorted(base_dir.glob("*.yaml")):
            items.append(LLMJudgeConfigInfo(name=path.stem, path=str(path.resolve())))
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
        path = _resolve_judge_config_path(name)
    except FileNotFoundError:
        raise HTTPException(status_code=400, detail="Invalid config name") from None
    path.parent.mkdir(parents=True, exist_ok=True)

    import yaml

    with path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(body, f, sort_keys=False, allow_unicode=True)

    return body
