#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

from pathlib import Path
import yaml
from text2sql_eval_toolkit.logging import get_logger
from typing import Dict, Any, Optional
from text2sql_eval_toolkit.inference.inference_tools import WXAIClient

logger = get_logger(__name__)


def load_llm_judge_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    if config_path is None:
        config_path = (
            Path(__file__).parent / "llm_judge_config" / "llm_judge_default_config.yaml"
        )
    else:
        config_path = Path(config_path)
    if not config_path.exists():
        raise FileNotFoundError(f"LLM judge config file not found: {config_path}")
    with config_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def evaluate_sql_prediction_with_llm(
    question: str,
    ground_truth_sql: str,
    ground_truth_df: Any,
    predicted_sql: str,
    predicted_df: Any,
    generation_prompt: str,
    llm_judge_config: dict,
) -> Dict[str, Any]:
    # Extract model config
    model_config = llm_judge_config.get("model", {})
    evaluator_model = model_config.get("id", "")

    # Extract all other model parameters except "id"
    model_parameters = {k: v for k, v in model_config.items() if k != "id"}

    # Initialize client
    if evaluator_model.startswith("wxai:"):
        client = WXAIClient(
            model_name=evaluator_model[5:],  # Strip "wxai:"
            model_parameters=model_parameters,
        )
    else:
        raise NotImplementedError(
            f"Model '{evaluator_model}' is not supported. Only 'wxai:' models are currently implemented."
        )

    # Format prompt
    prompt_template = llm_judge_config.get("prompt_template", "")
    prompt = prompt_template.format(
        question=question,
        generation_prompt=generation_prompt,
        ground_truth_sql=ground_truth_sql,
        ground_truth_df=ground_truth_df,
        predicted_sql=predicted_sql,
        predicted_df=predicted_df,
    )

    verdict = "N/A"
    score = 0.0
    explanation = "N/A"

    # Run inference
    logger.debug("Running LLM-as-a-judge inference...")
    response = client.model.generate(prompt)
    answer = response.get("results", [{}])[0].get("generated_text", "").strip()
    if not answer:
        logger.error(f"LLM judge inference failed with response: {response}")
        raise ValueError(f"LLM judge inference failed with response: {response}")
    elif answer.lower().startswith("yes"):
        verdict = "Yes"
        score = 1.0
        explanation = answer
    elif answer.lower().startswith("no"):
        verdict = "No"
        score = 0.0
        explanation = answer
    elif answer.lower().startswith("maybe"):
        verdict = "Maybe"
        score = 0.5
        explanation = answer

    return {"verdict": verdict, "score": score, "explanation": explanation}
