#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
This module provides a pipeline for generating SQL queries from natural language utterances using IBM watsonx.ai foundation models.

Classes:
    LLMApiClient:
        A client for interacting with IBM watsonx.ai to generate SQL queries based on provided database schemas and user utterances.
        - Initializes with model credentials and parameters.
        - Verbalizes database schemas for prompt construction.
        - Generates SQL queries from natural language questions.

    LLMSQLGenerationPipeline (inherits from BasePipeline):
        Orchestrates the process of loading benchmark data, schemas, and generating SQL predictions using LLMApiClient.
        - Loads benchmark metadata, schema, and data.
        - Manages prediction storage and avoids duplicate predictions for the same model and parameters.
        - Saves generated SQL predictions to a specified file.

Usage:
    - Set required IBM watsonx.ai credentials in environment variables: WATSONX_APIKEY, WATSONX_API_BASE, WATSONX_PROJECTID.
    - Use LLMSQLGenerationPipeline to run the SQL generation pipeline for a specified benchmark, model, and parameters.
"""

import asyncio
import os
import json
import time
from text2sql_eval_toolkit.logging import get_logger
from text2sql_eval_toolkit.inference.base_pipeline import BasePipeline
from text2sql_eval_toolkit.inference.inference_tools import (
    Text2SQLPrompt,
    WXAIClientChatAPI,
    VLLMClientChatAPI,
    ClaudeClientChatAPI,
    OpenAIClientChatAPI,
)
from text2sql_eval_toolkit.utils import (
    get_benchmark_info,
    get_question_id,
    get_utterance,
)


logger = get_logger(__name__)


class LLMSQLGenerationPipelineSimple(BasePipeline):
    def __init__(self):
        super().__init__()

    def run_pipeline(
        self,
        benchmark_id: str,
        pipeline_id: str,
        model_name: str,
        model_parameters: dict,
    ):
        benchmark_info = get_benchmark_info(benchmark_id)
        db_type = benchmark_info["db_engine"]["db_type"]
        # Load schema
        with open(benchmark_info["schema_json_path"], "r") as f:
            schema = json.load(f)

        # Load benchmark data
        with open(benchmark_info["benchmark_json_path"], "r") as fin:
            data = json.load(fin)

        # Load or initialize predictions file
        if os.path.exists(benchmark_info["predictions_path"]):
            with open(benchmark_info["predictions_path"], "r") as pf:
                predictions_data = json.load(pf)
        else:
            predictions_data = []

        if model_name.startswith("wxai:"):
            client = WXAIClientChatAPI(model_name[5:], model_parameters)
        elif model_name.startswith("vllm"):
            client = VLLMClientChatAPI(model_name[5:], model_parameters)
        else:
            raise NotImplementedError(
                f"Model {model_name} is not supported. Only 'wxai:' models are currently implemented."
            )

        # Extend predictions_data with new predictions
        for idx, record in enumerate(data):
            question_id = get_question_id(record)
            utterance = get_utterance(record)
            db_schema = None
            if "tables" not in schema:
                # If schema does not have 'tables', assume it's a multi-schema benchmark
                db_id = record.get("db_id", None)
                if db_id is None:
                    raise ValueError(
                        f"Object id={question_id} (line {idx}) does not contain 'db_id' for multi-schema benchmarks."
                    )
                db_schema = schema.get(db_id)
                logger.debug(f"Using schema for db_id={db_id} from the benchmark.")
                logger.debug(f"Schema: {db_schema}")
            else:
                db_schema = schema
            prompt = record.get("chat_prompt", None)
            generation_prompt = prompt
            if prompt is None:
                prompt = Text2SQLPrompt(utterance, db_schema, db_type)
                generation_prompt = prompt.prompt
            # Find existing record with the same id
            existing = next(
                (p for p in predictions_data if p.get("id") == question_id), None
            )
            if existing:
                # Add or update predictions for this model_name and model_parameters
                if "predictions" not in existing:
                    existing["predictions"] = {}
                # Check if this exact model_name and model_parameters already exist
                pred = existing["predictions"].get(pipeline_id)
                if pred:
                    logger.info(
                        f"Prediction for id={question_id}, pipeline={pipeline_id} prompt already exists. Skipping."
                    )
                    continue
                # Add/update the prediction for this model_name
                sql = client.generate_sql(prompt)
                existing["predictions"][pipeline_id] = {
                    "predicted_sql": sql,
                    "prompt": generation_prompt,
                    "model_name": model_name,
                    "model_parameters": model_parameters,
                }
            else:
                # New object, add predictions field
                sql = client.generate_sql(prompt)
                record["predictions"] = {
                    pipeline_id: {
                        "predicted_sql": sql,
                        "prompt": generation_prompt,
                        "model_name": model_name,
                        "model_parameters": model_parameters,
                    }
                }
                predictions_data.append(record)

        # Save updated predictions
        with open(benchmark_info["predictions_path"], "w") as fout:
            json.dump(predictions_data, fout, ensure_ascii=False, indent=2)

        logger.info(f"Predictions written to {benchmark_info['predictions_path']}")


class LLMSQLGenerationPipeline(BasePipeline):
    def __init__(self):
        super().__init__()

    async def generate_sql(
        self,
        idx,
        record,
        schema,
        db_type,
        pipeline_id,
        model_name,
        model_parameters,
        client,
        predictions_data,
        semaphore,
        timeout=1200,  # timeout in seconds
        force_rerun=False,
        skip_inference_error_retries=False,
    ):
        async with semaphore:
            question_id = get_question_id(record)
            try:
                utterance = get_utterance(record)
                evidence = record.get("evidence", None)
                db_schema = (
                    schema.get(record.get("db_id"))
                    if "tables" not in schema
                    else schema
                )
                prompt = record.get("chat_prompt", None)
                generation_prompt = prompt
                if prompt is None:
                    prompt = Text2SQLPrompt(utterance, db_schema, db_type, evidence)
                    generation_prompt = prompt.prompt

                existing = next(
                    (p for p in predictions_data if p.get("id") == question_id), None
                )
                if existing:
                    if "predictions" not in existing:
                        existing["predictions"] = {}
                    pred = existing["predictions"].get(pipeline_id)
                    if pred:
                        # Always retry if there was an inference error (unless skip flag is set)
                        if "inference_error" in pred and not skip_inference_error_retries:
                            logger.info(
                                f"Retrying failed inference for id={question_id}, pipeline={pipeline_id}"
                            )
                            # Continue with inference (don't return)
                        elif not force_rerun:
                            logger.info(
                                f"Prediction for id={question_id}, pipeline={pipeline_id} already exists. Skipping..."
                            )
                            return
                    logger.debug(f"Starting inference for record #{idx}")
                    inference_start = time.perf_counter()
                    sql, token_usage = await asyncio.wait_for(
                        asyncio.to_thread(client.generate_sql, prompt),
                        timeout=timeout,
                    )
                    inference_end = time.perf_counter()
                    inference_time_ms = (inference_end - inference_start) * 1000
                    logger.debug(f"Finished generating SQL for record #{idx}")
                    existing["predictions"][pipeline_id] = {
                        "predicted_sql": sql,
                        "prompt": generation_prompt,
                        "model_name": model_name,
                        "model_parameters": model_parameters,
                        "token_usage": token_usage,
                        "inference_time_ms": round(inference_time_ms, 2),
                    }
                else:
                    logger.debug(f"Starting inference for record #{idx}")
                    inference_start = time.perf_counter()
                    sql, token_usage = await asyncio.wait_for(
                        asyncio.to_thread(client.generate_sql, prompt),
                        timeout=timeout,
                    )
                    inference_end = time.perf_counter()
                    inference_time_ms = (inference_end - inference_start) * 1000
                    logger.debug(f"Finished generating SQL for record #{idx}")
                    record["predictions"] = {
                        pipeline_id: {
                            "predicted_sql": sql,
                            "prompt": generation_prompt,
                            "model_name": model_name,
                            "model_parameters": model_parameters,
                            "token_usage": token_usage,
                            "inference_time_ms": round(inference_time_ms, 2),
                        }
                    }
                    predictions_data.append(record)

            except TimeoutError as e:
                logger.error(
                    f"Timeout in record {idx} (question id={question_id}): {e}"
                )
                # Create prediction record with timeout error
                error_record = {
                    "predicted_sql": None,
                    "prompt": generation_prompt,
                    "model_name": model_name,
                    "model_parameters": model_parameters,
                    "inference_error": f"TimeoutError: {str(e)}",
                    "inference_time_ms": timeout * 1000,  # Max time reached
                }
                if existing:
                    existing["predictions"][pipeline_id] = error_record
                else:
                    record["predictions"] = {pipeline_id: error_record}
                    predictions_data.append(record)
                    
            except Exception as e:
                logger.error(f"Record {idx} (question id={question_id}) failed: {e}")
                # Create prediction record with inference error
                error_record = {
                    "predicted_sql": None,
                    "prompt": generation_prompt,
                    "model_name": model_name,
                    "model_parameters": model_parameters,
                    "inference_error": str(e),
                }
                # Try to capture serializable response info if available
                if hasattr(e, 'response'):
                    try:
                        response = e.response
                        error_record["response_info"] = {
                            "status_code": getattr(response, 'status_code', None),
                            "reason": getattr(response, 'reason', None),
                            "text": getattr(response, 'text', None)[:1000] if hasattr(response, 'text') else None,  # Limit text length
                        }
                    except Exception:
                        # If we can't serialize the response, just skip it
                        pass
                if existing:
                    existing["predictions"][pipeline_id] = error_record
                else:
                    record["predictions"] = {pipeline_id: error_record}
                    predictions_data.append(record)

    def run_pipeline(
        self,
        benchmark_id: str,
        model_name: str,
        model_parameters: dict,
        max_num_threads: int = 16,
        force_rerun: bool = False,
        skip_inference_error_retries: bool = False,
    ):
        pipeline_id = model_name + "-greedy-zero-shot-chatapi"
        logger.debug(
            f"🚀 Running inference for benchmark {benchmark_id},  pipeline: {pipeline_id}"
        )
        benchmark_info = get_benchmark_info(benchmark_id)
        db_type = benchmark_info["db_engine"]["db_type"]

        with open(benchmark_info["schema_json_path"], "r") as f:
            schema = json.load(f)
        with open(benchmark_info["benchmark_json_path"], "r") as fin:
            data = json.load(fin)
        if os.path.exists(benchmark_info["predictions_path"]):
            with open(benchmark_info["predictions_path"], "r") as pf:
                predictions_data = json.load(pf)
        else:
            predictions_data = []

        if model_name.startswith("wxai:"):
            client = WXAIClientChatAPI(model_name[5:], model_parameters)
        elif model_name.startswith("anthropic:"):
            client = ClaudeClientChatAPI(model_name[10:], model_parameters)
        elif model_name.startswith("vllm:"):
            client = VLLMClientChatAPI(model_name[5:], model_parameters)
        elif model_name.startswith("ollama:"):
            # Ollama uses OpenAI-compatible API with custom base URL
            client = OpenAIClientChatAPI(model_name[7:], model_parameters)
        elif model_name.startswith("openai:"):
            client = OpenAIClientChatAPI(model_name[7:], model_parameters)
        elif model_name.startswith("rits"):
            logger.info(f"Getting RITS model endpoint for {model_name}")
            model_id = model_name.split("/")[-1].replace(".", "-").lower()
            rits_api_key = os.environ.get("RITS_API_KEY")
            if rits_api_key is None:
                raise ValueError("Missing RITS_API_KEY environment variable")
            os.environ["VLLM_API_BASE"] = (
                f"https://inference-3scale-apicast-production.apps.rits.fmaas.res.ibm.com/{model_id}/v1"
            )
            client = VLLMClientChatAPI(model_name[5:], model_parameters)
        else:
            raise NotImplementedError(f"Model {model_name} is not supported.")

        async def run_all():
            semaphore = asyncio.Semaphore(max_num_threads)
            tasks = [
                self.generate_sql(
                    idx,
                    obj,
                    schema,
                    db_type,
                    pipeline_id,
                    model_name,
                    model_parameters,
                    client,
                    predictions_data,
                    semaphore,
                    force_rerun=force_rerun,
                    skip_inference_error_retries=skip_inference_error_retries,
                )
                for idx, obj in enumerate(data)
            ]
            await asyncio.gather(*tasks)

        asyncio.run(run_all())

        with open(benchmark_info["predictions_path"], "w") as fout:
            json.dump(predictions_data, fout, ensure_ascii=False, indent=2)

        logger.debug(
            f"✅ Inference completed for benchmark '{benchmark_id}',  pipeline: {pipeline_id}."
        )
        logger.info(f"Predictions written to {benchmark_info['predictions_path']}")
