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
    """
    Load an LLM-judge configuration from YAML.

    The config carries the model id under ``model.id`` -- in ``provider:name``
    form -- with any remaining keys under ``model`` passed through as generation
    parameters, plus the prompt template the judge uses.

    Args:
        config_path: Path to a judge YAML. ``None`` loads the packaged
            ``llm_judge_default_config.yaml``. Other packaged configs sit
            alongside it in ``evaluation/llm_judge_config/``.

    Returns:
        dict: The parsed configuration.

    Raises:
        FileNotFoundError: If *config_path* does not exist.

    Example:
        ```python
        >>> config = load_llm_judge_config()
        >>> config["model"]["id"]
        'wxai:meta-llama/llama-3-3-70b-instruct'
        ```
    """
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


def extract_token_usage(response: Any) -> Optional[Dict[str, int]]:
    """
    Pull token counts out of a watsonx response.

    Two shapes are in play: the legacy ``generate`` API reports
    ``input_token_count`` / ``generated_token_count`` inside ``results[0]``,
    while the Chat API reports a ``usage`` object. Metering spend depends on
    these, so both are handled, and an unrecognised shape returns None rather
    than a misleading zero.
    """
    if not isinstance(response, dict):
        return None

    usage = response.get("usage")
    if isinstance(usage, dict) and usage:
        prompt_tokens = int(usage.get("prompt_tokens") or 0)
        completion_tokens = int(usage.get("completion_tokens") or 0)
        if prompt_tokens or completion_tokens:
            return {
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "total_tokens": int(
                    usage.get("total_tokens") or prompt_tokens + completion_tokens
                ),
            }

    results = response.get("results")
    if isinstance(results, list) and results and isinstance(results[0], dict):
        first = results[0]
        prompt_tokens = int(first.get("input_token_count") or 0)
        completion_tokens = int(first.get("generated_token_count") or 0)
        if prompt_tokens or completion_tokens:
            return {
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "total_tokens": prompt_tokens + completion_tokens,
            }
    return None


def evaluate_sql_prediction_with_llm(
    question: str,
    ground_truth_sql: str,
    ground_truth_df: Any,
    predicted_sql: str,
    predicted_df: Any,
    generation_prompt: str,
    llm_judge_config: dict,
) -> Dict[str, Any]:
    """
    Ask an LLM whether a predicted query answers the question.

    Complements the execution metrics rather than replacing them: it catches
    predictions that are defensible but do not match the ground truth exactly,
    and questions where more than one query is reasonable.

    Everything it needs is already in the evaluation artifacts, so **this
    requires no database connection** -- only credentials for the judge model.

    Args:
        question: The natural-language question.
        ground_truth_sql: The reference statement.
        ground_truth_df: The reference result, as a DataFrame or its serialised
            form.
        predicted_sql: The model's statement.
        predicted_df: The model's result, in the same form as *ground_truth_df*.
        generation_prompt: The prompt the prediction was generated from. Shown
            to the judge as context.
        llm_judge_config: A config from :func:`load_llm_judge_config`.

    Returns:
        dict: With keys

        - ``verdict``: ``"Yes"``, ``"No"``, ``"Maybe"``, or ``"N/A"`` when the
          reply could not be interpreted.
        - ``score``: ``1.0``, ``0.0``, ``0.5`` and ``0.0`` respectively. Note
          that an uninterpretable reply scores the same as a rejection, so
          ``verdict`` is the field to check when telling the two apart matters.
        - ``explanation``: The judge's reasoning.
        - ``token_usage``: Prompt and completion counts, or ``None`` when the
          provider reported none. Callers metering spend must handle ``None``
          rather than treating it as zero.

    Raises:
        NotImplementedError: If the configured model is not a ``wxai:`` model.
            Only watsonx is wired up on this path today.

    Example:
        ```python
        >>> config = load_llm_judge_config()
        >>> result = evaluate_sql_prediction_with_llm(
        ...     question="How many customers are there?",
        ...     ground_truth_sql="SELECT COUNT(*) FROM customers",
        ...     ground_truth_df=gt_df,
        ...     predicted_sql="SELECT COUNT(id) FROM customers",
        ...     predicted_df=pred_df,
        ...     generation_prompt=prompt,
        ...     llm_judge_config=config,
        ... )
        >>> result["verdict"], result["score"]
        ('Yes', 1.0)
        ```
    """
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
    token_usage = extract_token_usage(response)
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

    return {
        "verdict": verdict,
        "score": score,
        "explanation": explanation,
        # Present so callers can meter spend. None when the provider did not
        # report usage; callers must handle that rather than assume zero.
        "token_usage": token_usage,
    }
