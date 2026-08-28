# Inference

Generating SQL from a natural-language question. Both pipelines write their
predictions into the benchmark's predictions file, keyed by `pipeline_id`, so
several models can accumulate side by side and be compared afterwards.

::: text2sql_eval_toolkit.LLMSQLGenerationPipeline
::: text2sql_eval_toolkit.AgenticSQLGenerationPipeline
::: text2sql_eval_toolkit.LLMSQLGenerationPipelineSimple
