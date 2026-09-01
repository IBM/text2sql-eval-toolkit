#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

from pathlib import Path
import re
import yaml
from text2sql_eval_toolkit.logging import get_logger
from typing import Callable, Dict, Any, Optional
from text2sql_eval_toolkit.inference.model_clients import Credential, resolve_client

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


#: Decoration a model may put before its verdict: Markdown emphasis, a heading,
#: a block quote, a list bullet, an opening quote or bracket.
_LEADING_DECORATION = re.compile(r"""^[\s>#*_`~\-\u2022"'\[(]+""")

#: A label some models write before answering, e.g. "Verdict: Yes".
_VERDICT_LABEL = re.compile(
    r"^(?:verdict|answer|judgment|judgement|assessment)\s*[:\-\u2014]\s*",
    re.IGNORECASE,
)

#: The verdict itself, which must be the first word of what is left. The
#: lookahead rather than ``\b`` because ``_`` is a word character to ``re``, so
#: ``\b`` does not fire between the "s" and the underscore of ``__Yes__``.
_VERDICT_WORD = re.compile(r"(yes|no|maybe)(?![A-Za-z0-9])", re.IGNORECASE)

#: Verdict -> score. An unreadable reply scores the same as a rejection, so the
#: verdict is the field to check when telling the two apart matters.
_VERDICT_SCORES = {"Yes": 1.0, "Maybe": 0.5, "No": 0.0}


def _read_verdict(answer: str) -> Optional[str]:
    """
    Find the verdict at the start of a reply, ignoring how it is dressed.

    The prompt asks the model to lead with "Yes", "No" or "Maybe", and models
    comply -- but they comply in their own house style. Gemini 3 answers
    ``**Yes**``; others write ``### Yes``, ``> Yes``, ``"Yes"`` or
    ``Verdict: Yes``. Matching the bare word against the raw first characters
    scored every one of those N/A, which is to say zero, so a judge run against
    a Markdown-formatting model marked every prediction wrong.

    Only the head of the reply is examined. Scanning the whole text would find
    the "No" in "No ground-truth SQL was available" and read a rejection out of
    an explanation.

    Args:
        answer: The reply, already stripped.

    Returns:
        ``"Yes"``, ``"No"``, ``"Maybe"``, or ``None`` if the head of the reply
        is not one of them.
    """
    head = _LEADING_DECORATION.sub("", answer)
    head = _VERDICT_LABEL.sub("", head)
    head = _LEADING_DECORATION.sub("", head)
    match = _VERDICT_WORD.match(head)
    if match is None:
        return None
    return match.group(1).capitalize()


def evaluate_sql_prediction_with_llm(
    question: str,
    ground_truth_sql: str,
    ground_truth_df: Any,
    predicted_sql: str,
    predicted_df: Any,
    generation_prompt: str,
    llm_judge_config: dict,
    api_key: Optional[Credential] = None,
    on_usage: Optional[Callable] = None,
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
        llm_judge_config: A config from [`load_llm_judge_config`][text2sql_eval_toolkit.load_llm_judge_config].
        api_key: Use this credential instead of the provider's environment
            variable. Omitted -- as every library, CLI and notebook call does --
            the client reads the environment exactly as before. This is what lets
            the dashboard bill a request to the signed-in user's own key.
        on_usage: Called as ``on_usage(model_name, usage)`` after the request.
            The library never interprets it; it is how the dashboard meters spend
            without quota logic living here.

    Returns:
        dict: With keys

        - ``verdict``: ``"Yes"``, ``"No"``, ``"Maybe"``, or ``"N/A"`` when the
          reply could not be interpreted. The verdict is read from the start of
          the reply through any Markdown emphasis, heading, quoting or
          ``Verdict:`` label the model wrapped it in.
        - ``score``: ``1.0``, ``0.0``, ``0.5`` and ``0.0`` respectively. Note
          that an uninterpretable reply scores the same as a rejection, so
          ``verdict`` is the field to check when telling the two apart matters.
        - ``explanation``: The judge's reasoning -- the reply verbatim, kept
          even when the verdict is ``"N/A"`` so that an unrecognised answer can
          be read rather than guessed at.
        - ``token_usage``: Prompt and completion counts, or ``None`` when the
          provider reported none. Callers metering spend must handle ``None``
          rather than treating it as zero.

    Raises:
        UnsupportedModel: If no provider prefix matches the configured model and
            LiteLLM is not installed to route it. A subclass of
            ``NotImplementedError``. Any prefix the installation supports works
            here -- ``wxai:``, ``anthropic:``, ``openai:``, ``gemini:``,
            ``vllm:``, ``ollama:``, and ``litellm:`` with that extra installed.
            The judge is no longer watsonx-only.

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

    # One dispatch table, shared with both inference pipelines: a model string
    # that works there works here. This used to accept only "wxai:".
    client = resolve_client(
        evaluator_model, model_parameters, api_key=api_key, on_usage=on_usage
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

    # Run inference
    logger.debug("Running LLM-as-a-judge inference...")
    # generate_text, not generate_sql: the reply is a verdict and an explanation,
    # and SQL post-processing would strip fenced blocks and trailing punctuation
    # out of prose.
    answer, token_usage = client.generate_text(prompt)
    answer = (answer or "").strip()
    if not answer:
        logger.error(f"LLM judge returned no text for model {evaluator_model!r}")
        raise ValueError(f"LLM judge returned no text for model {evaluator_model!r}")

    read = _read_verdict(answer)
    if read is None:
        # The reply is kept regardless. It used to be replaced with "N/A", which
        # discarded the evidence in the one case where somebody needs to read it
        # -- there was no way to tell a model that refused to answer from one
        # whose answer was simply not recognised.
        # The reply itself goes to DEBUG, not here. It can carry the question,
        # the SQL and rows of result data, and a warning is a level production
        # keeps on -- so the fact is logged where an operator will see it and
        # the content where the rest of this pipeline already puts content. The
        # caller gets the full text back as `explanation` either way.
        logger.warning(
            "LLM judge reply from %r did not begin with a verdict; scoring N/A.",
            evaluator_model,
        )
        logger.debug("Unrecognised LLM judge reply: %s", answer)
    verdict = read or "N/A"
    score = _VERDICT_SCORES.get(verdict, 0.0)

    return {
        "verdict": verdict,
        "score": score,
        "explanation": answer,
        # Present so callers can meter spend. None when the provider did not
        # report usage; callers must handle that rather than assume zero.
        "token_usage": token_usage,
    }
