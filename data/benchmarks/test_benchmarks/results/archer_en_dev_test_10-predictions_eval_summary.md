# Summary Results

## Overall Average Accuracy Results

| Rank | Pipeline | Records # | Predictions # | Exec Acc | Non-Empty Exec Acc | Subset Non-Empty Exec Acc | BIRD Exec Acc | Parsable SQL | Syntactic Equivalence Score | LLM Score |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | wxai:openai/gpt-oss-120b-agentic-baseline4-3attempts | 10 | 0 | 0.40 | 0.40 | 0.40 | 0.40 | 1.00 | 0.00 | 0.80 |
| 2 | wxai:ibm/granite-4-h-small-greedy-zero-shot-chatapi | 10 | 0 | 0.30 | 0.30 | 0.30 | 0.30 | 1.00 | 0.00 | 0.50 |
| 3 | wxai:meta-llama/llama-4-maverick-17b-128e-instruct-fp8-greedy-zero-shot-chatapi | 10 | 0 | 0.40 | 0.30 | 0.30 | 0.40 | 1.00 | 0.00 | 0.60 |
| 4 | wxai:openai/gpt-oss-120b-greedy-zero-shot-chatapi | 10 | 0 | 0.40 | 0.30 | 0.30 | 0.40 | 1.00 | 0.00 | 0.80 |
| 5 | wxai:openai/gpt-oss-120b-agentic-baseline0-3attempts | 10 | 0 | 0.30 | 0.30 | 0.30 | 0.30 | 1.00 | 0.00 | 0.50 |
| 6 | wxai:openai/gpt-oss-120b-agentic-baseline1-3attempts | 10 | 0 | 0.20 | 0.20 | 0.20 | 0.20 | 1.00 | 0.00 | 0.60 |
| 7 | wxai:openai/gpt-oss-120b-agentic-baseline2-3attempts | 10 | 0 | 0.30 | 0.20 | 0.20 | 0.30 | 1.00 | 0.00 | 0.60 |
| 8 | wxai:openai/gpt-oss-120b-agentic-baseline5-3attempts | 10 | 0 | 0.14 | 0.14 | 0.14 | 0.14 | 1.00 | 0.00 | 0.57 |
| 9 | wxai:meta-llama/llama-3-3-70b-instruct-greedy-zero-shot-chatapi | 10 | 0 | 0.20 | 0.10 | 0.10 | 0.20 | 1.00 | 0.00 | 0.70 |
| 10 | wxai:openai/gpt-oss-120b-agentic-baseline3-3attempts | 10 | 0 | 0.00 | 0.00 | 0.00 | 0.00 | 1.00 | 0.00 | 0.20 |


![Overall Chart](charts/archer_en_dev_test_10-predictions_eval_summary.png)


## Category: `has_aggregation`

| Rank | Pipeline | Records # | Predictions # | Exec Acc | Non-Empty Exec Acc | Subset Non-Empty Exec Acc | BIRD Exec Acc | Parsable SQL | Syntactic Equivalence Score | LLM Score |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | wxai:openai/gpt-oss-120b-agentic-baseline4-3attempts | 7 | 0 | 0.67 | 0.67 | 0.67 | 0.67 | 1.00 | 0.00 | 0.67 |
| 2 | wxai:ibm/granite-4-h-small-greedy-zero-shot-chatapi | 7 | 0 | 0.29 | 0.29 | 0.29 | 0.29 | 1.00 | 0.00 | 0.43 |
| 3 | wxai:meta-llama/llama-4-maverick-17b-128e-instruct-fp8-greedy-zero-shot-chatapi | 7 | 0 | 0.43 | 0.29 | 0.29 | 0.43 | 1.00 | 0.00 | 0.43 |
| 4 | wxai:openai/gpt-oss-120b-greedy-zero-shot-chatapi | 7 | 0 | 0.43 | 0.29 | 0.29 | 0.43 | 1.00 | 0.00 | 0.71 |
| 5 | wxai:openai/gpt-oss-120b-agentic-baseline0-3attempts | 7 | 0 | 0.29 | 0.29 | 0.29 | 0.29 | 1.00 | 0.00 | 0.57 |
| 6 | wxai:openai/gpt-oss-120b-agentic-baseline1-3attempts | 7 | 0 | 0.29 | 0.29 | 0.29 | 0.29 | 1.00 | 0.00 | 0.86 |
| 7 | wxai:openai/gpt-oss-120b-agentic-baseline2-3attempts | 7 | 0 | 0.43 | 0.29 | 0.29 | 0.43 | 1.00 | 0.00 | 0.86 |
| 8 | wxai:openai/gpt-oss-120b-agentic-baseline5-3attempts | 7 | 0 | 0.20 | 0.20 | 0.20 | 0.20 | 1.00 | 0.00 | 0.60 |
| 9 | wxai:meta-llama/llama-3-3-70b-instruct-greedy-zero-shot-chatapi | 7 | 0 | 0.29 | 0.14 | 0.14 | 0.29 | 1.00 | 0.00 | 0.71 |
| 10 | wxai:openai/gpt-oss-120b-agentic-baseline3-3attempts | 7 | 0 | 0.00 | 0.00 | 0.00 | 0.00 | 1.00 | 0.00 | 0.29 |


![Chart for has_aggregation](charts/archer_en_dev_test_10-predictions_eval_summary-has_aggregation.png)


## Category: `has_join`

| Rank | Pipeline | Records # | Predictions # | Exec Acc | Non-Empty Exec Acc | Subset Non-Empty Exec Acc | BIRD Exec Acc | Parsable SQL | Syntactic Equivalence Score | LLM Score |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | wxai:ibm/granite-4-h-small-greedy-zero-shot-chatapi | 5 | 0 | 0.40 | 0.40 | 0.40 | 0.40 | 1.00 | 0.00 | 0.80 |
| 2 | wxai:meta-llama/llama-4-maverick-17b-128e-instruct-fp8-greedy-zero-shot-chatapi | 5 | 0 | 0.40 | 0.40 | 0.40 | 0.40 | 1.00 | 0.00 | 0.60 |
| 3 | wxai:openai/gpt-oss-120b-greedy-zero-shot-chatapi | 5 | 0 | 0.40 | 0.40 | 0.40 | 0.40 | 1.00 | 0.00 | 0.60 |
| 4 | wxai:openai/gpt-oss-120b-agentic-baseline0-3attempts | 5 | 0 | 0.40 | 0.40 | 0.40 | 0.40 | 1.00 | 0.00 | 0.40 |
| 5 | wxai:openai/gpt-oss-120b-agentic-baseline4-3attempts | 5 | 0 | 0.33 | 0.33 | 0.33 | 0.33 | 1.00 | 0.00 | 0.67 |
| 6 | wxai:meta-llama/llama-3-3-70b-instruct-greedy-zero-shot-chatapi | 5 | 0 | 0.20 | 0.20 | 0.20 | 0.20 | 1.00 | 0.00 | 0.80 |
| 7 | wxai:openai/gpt-oss-120b-agentic-baseline1-3attempts | 5 | 0 | 0.20 | 0.20 | 0.20 | 0.20 | 1.00 | 0.00 | 0.40 |
| 8 | wxai:openai/gpt-oss-120b-agentic-baseline2-3attempts | 5 | 0 | 0.20 | 0.20 | 0.20 | 0.20 | 1.00 | 0.00 | 0.40 |
| 9 | wxai:openai/gpt-oss-120b-agentic-baseline3-3attempts | 5 | 0 | 0.00 | 0.00 | 0.00 | 0.00 | 1.00 | 0.00 | 0.00 |
| 10 | wxai:openai/gpt-oss-120b-agentic-baseline5-3attempts | 5 | 0 | 0.00 | 0.00 | 0.00 | 0.00 | 1.00 | 0.00 | 0.25 |


![Chart for has_join](charts/archer_en_dev_test_10-predictions_eval_summary-has_join.png)


## Category: `has_nested_query`

| Rank | Pipeline | Records # | Predictions # | Exec Acc | Non-Empty Exec Acc | Subset Non-Empty Exec Acc | BIRD Exec Acc | Parsable SQL | Syntactic Equivalence Score | LLM Score |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | wxai:openai/gpt-oss-120b-agentic-baseline4-3attempts | 8 | 0 | 0.67 | 0.67 | 0.67 | 0.67 | 1.00 | 0.00 | 0.67 |
| 2 | wxai:ibm/granite-4-h-small-greedy-zero-shot-chatapi | 8 | 0 | 0.38 | 0.38 | 0.38 | 0.38 | 1.00 | 0.00 | 0.50 |
| 3 | wxai:meta-llama/llama-4-maverick-17b-128e-instruct-fp8-greedy-zero-shot-chatapi | 8 | 0 | 0.50 | 0.38 | 0.38 | 0.50 | 1.00 | 0.00 | 0.50 |
| 4 | wxai:openai/gpt-oss-120b-greedy-zero-shot-chatapi | 8 | 0 | 0.50 | 0.38 | 0.38 | 0.50 | 1.00 | 0.00 | 0.75 |
| 5 | wxai:openai/gpt-oss-120b-agentic-baseline0-3attempts | 8 | 0 | 0.38 | 0.38 | 0.38 | 0.38 | 1.00 | 0.00 | 0.62 |
| 6 | wxai:openai/gpt-oss-120b-agentic-baseline1-3attempts | 8 | 0 | 0.25 | 0.25 | 0.25 | 0.25 | 1.00 | 0.00 | 0.75 |
| 7 | wxai:openai/gpt-oss-120b-agentic-baseline2-3attempts | 8 | 0 | 0.38 | 0.25 | 0.25 | 0.38 | 1.00 | 0.00 | 0.75 |
| 8 | wxai:openai/gpt-oss-120b-agentic-baseline5-3attempts | 8 | 0 | 0.17 | 0.17 | 0.17 | 0.17 | 1.00 | 0.00 | 0.50 |
| 9 | wxai:meta-llama/llama-3-3-70b-instruct-greedy-zero-shot-chatapi | 8 | 0 | 0.25 | 0.12 | 0.12 | 0.25 | 1.00 | 0.00 | 0.62 |
| 10 | wxai:openai/gpt-oss-120b-agentic-baseline3-3attempts | 8 | 0 | 0.00 | 0.00 | 0.00 | 0.00 | 1.00 | 0.00 | 0.25 |


![Chart for has_nested_query](charts/archer_en_dev_test_10-predictions_eval_summary-has_nested_query.png)


## Category: `has_sorting`

| Rank | Pipeline | Records # | Predictions # | Exec Acc | Non-Empty Exec Acc | Subset Non-Empty Exec Acc | BIRD Exec Acc | Parsable SQL | Syntactic Equivalence Score | LLM Score |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | wxai:openai/gpt-oss-120b-agentic-baseline4-3attempts | 3 | 0 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 | 0.00 | 1.00 |
| 2 | wxai:ibm/granite-4-h-small-greedy-zero-shot-chatapi | 3 | 0 | 0.67 | 0.67 | 0.67 | 0.67 | 1.00 | 0.00 | 0.67 |
| 3 | wxai:meta-llama/llama-4-maverick-17b-128e-instruct-fp8-greedy-zero-shot-chatapi | 3 | 0 | 0.67 | 0.67 | 0.67 | 0.67 | 1.00 | 0.00 | 0.67 |
| 4 | wxai:openai/gpt-oss-120b-greedy-zero-shot-chatapi | 3 | 0 | 0.67 | 0.67 | 0.67 | 0.67 | 1.00 | 0.00 | 0.67 |
| 5 | wxai:openai/gpt-oss-120b-agentic-baseline0-3attempts | 3 | 0 | 0.67 | 0.67 | 0.67 | 0.67 | 1.00 | 0.00 | 0.67 |
| 6 | wxai:openai/gpt-oss-120b-agentic-baseline1-3attempts | 3 | 0 | 0.33 | 0.33 | 0.33 | 0.33 | 1.00 | 0.00 | 0.33 |
| 7 | wxai:openai/gpt-oss-120b-agentic-baseline2-3attempts | 3 | 0 | 0.33 | 0.33 | 0.33 | 0.33 | 1.00 | 0.00 | 0.33 |
| 8 | wxai:openai/gpt-oss-120b-agentic-baseline5-3attempts | 3 | 0 | 0.33 | 0.33 | 0.33 | 0.33 | 1.00 | 0.00 | 0.67 |
| 9 | wxai:meta-llama/llama-3-3-70b-instruct-greedy-zero-shot-chatapi | 3 | 0 | 0.00 | 0.00 | 0.00 | 0.00 | 1.00 | 0.00 | 0.33 |
| 10 | wxai:openai/gpt-oss-120b-agentic-baseline3-3attempts | 3 | 0 | 0.00 | 0.00 | 0.00 | 0.00 | 1.00 | 0.00 | 0.00 |


![Chart for has_sorting](charts/archer_en_dev_test_10-predictions_eval_summary-has_sorting.png)


## Category: `multi_table_simple`

| Rank | Pipeline | Records # | Predictions # | Exec Acc | Non-Empty Exec Acc | Subset Non-Empty Exec Acc | BIRD Exec Acc | Parsable SQL | Syntactic Equivalence Score | LLM Score |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | wxai:meta-llama/llama-3-3-70b-instruct-greedy-zero-shot-chatapi | 1 | 0 | 0.00 | 0.00 | 0.00 | 0.00 | 1.00 | 0.00 | 1.00 |
| 2 | wxai:ibm/granite-4-h-small-greedy-zero-shot-chatapi | 1 | 0 | 0.00 | 0.00 | 0.00 | 0.00 | 1.00 | 0.00 | 1.00 |
| 3 | wxai:meta-llama/llama-4-maverick-17b-128e-instruct-fp8-greedy-zero-shot-chatapi | 1 | 0 | 0.00 | 0.00 | 0.00 | 0.00 | 1.00 | 0.00 | 1.00 |
| 4 | wxai:openai/gpt-oss-120b-greedy-zero-shot-chatapi | 1 | 0 | 0.00 | 0.00 | 0.00 | 0.00 | 1.00 | 0.00 | 1.00 |
| 5 | wxai:openai/gpt-oss-120b-agentic-baseline0-3attempts | 1 | 0 | 0.00 | 0.00 | 0.00 | 0.00 | 1.00 | 0.00 | 0.00 |
| 6 | wxai:openai/gpt-oss-120b-agentic-baseline1-3attempts | 1 | 0 | 0.00 | 0.00 | 0.00 | 0.00 | 1.00 | 0.00 | 0.00 |
| 7 | wxai:openai/gpt-oss-120b-agentic-baseline2-3attempts | 1 | 0 | 0.00 | 0.00 | 0.00 | 0.00 | 1.00 | 0.00 | 0.00 |
| 8 | wxai:openai/gpt-oss-120b-agentic-baseline3-3attempts | 1 | 0 | 0.00 | 0.00 | 0.00 | 0.00 | 1.00 | 0.00 | 0.00 |
| 9 | wxai:openai/gpt-oss-120b-agentic-baseline4-3attempts | 1 | 0 | 0.00 | 0.00 | 0.00 | 0.00 | 1.00 | 0.00 | 1.00 |
| 10 | wxai:openai/gpt-oss-120b-agentic-baseline5-3attempts | 1 | 0 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |


![Chart for multi_table_simple](charts/archer_en_dev_test_10-predictions_eval_summary-multi_table_simple.png)


## Category: `single_source_basic`

| Rank | Pipeline | Records # | Predictions # | Exec Acc | Non-Empty Exec Acc | Subset Non-Empty Exec Acc | BIRD Exec Acc | Parsable SQL | Syntactic Equivalence Score | LLM Score |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | wxai:meta-llama/llama-3-3-70b-instruct-greedy-zero-shot-chatapi | 1 | 0 | 0.00 | 0.00 | 0.00 | 0.00 | 1.00 | 0.00 | 1.00 |
| 2 | wxai:ibm/granite-4-h-small-greedy-zero-shot-chatapi | 1 | 0 | 0.00 | 0.00 | 0.00 | 0.00 | 1.00 | 0.00 | 0.00 |
| 3 | wxai:meta-llama/llama-4-maverick-17b-128e-instruct-fp8-greedy-zero-shot-chatapi | 1 | 0 | 0.00 | 0.00 | 0.00 | 0.00 | 1.00 | 0.00 | 1.00 |
| 4 | wxai:openai/gpt-oss-120b-greedy-zero-shot-chatapi | 1 | 0 | 0.00 | 0.00 | 0.00 | 0.00 | 1.00 | 0.00 | 1.00 |
| 5 | wxai:openai/gpt-oss-120b-agentic-baseline0-3attempts | 1 | 0 | 0.00 | 0.00 | 0.00 | 0.00 | 1.00 | 0.00 | 0.00 |
| 6 | wxai:openai/gpt-oss-120b-agentic-baseline1-3attempts | 1 | 0 | 0.00 | 0.00 | 0.00 | 0.00 | 1.00 | 0.00 | 0.00 |
| 7 | wxai:openai/gpt-oss-120b-agentic-baseline2-3attempts | 1 | 0 | 0.00 | 0.00 | 0.00 | 0.00 | 1.00 | 0.00 | 0.00 |
| 8 | wxai:openai/gpt-oss-120b-agentic-baseline3-3attempts | 1 | 0 | 0.00 | 0.00 | 0.00 | 0.00 | 1.00 | 0.00 | 0.00 |
| 9 | wxai:openai/gpt-oss-120b-agentic-baseline4-3attempts | 1 | 0 | 0.00 | 0.00 | 0.00 | 0.00 | 1.00 | 0.00 | 1.00 |
| 10 | wxai:openai/gpt-oss-120b-agentic-baseline5-3attempts | 1 | 0 | 0.00 | 0.00 | 0.00 | 0.00 | 1.00 | 0.00 | 1.00 |


![Chart for single_source_basic](charts/archer_en_dev_test_10-predictions_eval_summary-single_source_basic.png)


# Per-Pipeline Comparison Across Categories


### Pipeline: `wxai:ibm/granite-4-h-small-greedy-zero-shot-chatapi`
| Category | # Records | # Predictions | Exec Acc | Non-Empty Exec Acc | Subset Non-Empty Exec Acc | BIRD Exec Acc | LLM Score |
|---|---|---|---|---|---|---|---|
| All Categories | 10 | 0 | 0.300 | 0.300 | 0.300 | 0.300 | 0.500 |
| has_aggregation | 7 | 0 | 0.286 | 0.286 | 0.286 | 0.286 | 0.429 |
| has_join | 5 | 0 | 0.400 | 0.400 | 0.400 | 0.400 | 0.800 |
| has_nested_query | 8 | 0 | 0.375 | 0.375 | 0.375 | 0.375 | 0.500 |
| has_sorting | 3 | 0 | 0.667 | 0.667 | 0.667 | 0.667 | 0.667 |
| multi_table_simple | 1 | 0 | 0.000 | 0.000 | 0.000 | 0.000 | 1.000 |
| single_source_basic | 1 | 0 | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 |

### Pipeline: `wxai:meta-llama/llama-3-3-70b-instruct-greedy-zero-shot-chatapi`
| Category | # Records | # Predictions | Exec Acc | Non-Empty Exec Acc | Subset Non-Empty Exec Acc | BIRD Exec Acc | LLM Score |
|---|---|---|---|---|---|---|---|
| All Categories | 10 | 0 | 0.200 | 0.100 | 0.100 | 0.200 | 0.700 |
| has_aggregation | 7 | 0 | 0.286 | 0.143 | 0.143 | 0.286 | 0.714 |
| has_join | 5 | 0 | 0.200 | 0.200 | 0.200 | 0.200 | 0.800 |
| has_nested_query | 8 | 0 | 0.250 | 0.125 | 0.125 | 0.250 | 0.625 |
| has_sorting | 3 | 0 | 0.000 | 0.000 | 0.000 | 0.000 | 0.333 |
| multi_table_simple | 1 | 0 | 0.000 | 0.000 | 0.000 | 0.000 | 1.000 |
| single_source_basic | 1 | 0 | 0.000 | 0.000 | 0.000 | 0.000 | 1.000 |

### Pipeline: `wxai:meta-llama/llama-4-maverick-17b-128e-instruct-fp8-greedy-zero-shot-chatapi`
| Category | # Records | # Predictions | Exec Acc | Non-Empty Exec Acc | Subset Non-Empty Exec Acc | BIRD Exec Acc | LLM Score |
|---|---|---|---|---|---|---|---|
| All Categories | 10 | 0 | 0.400 | 0.300 | 0.300 | 0.400 | 0.600 |
| has_aggregation | 7 | 0 | 0.429 | 0.286 | 0.286 | 0.429 | 0.429 |
| has_join | 5 | 0 | 0.400 | 0.400 | 0.400 | 0.400 | 0.600 |
| has_nested_query | 8 | 0 | 0.500 | 0.375 | 0.375 | 0.500 | 0.500 |
| has_sorting | 3 | 0 | 0.667 | 0.667 | 0.667 | 0.667 | 0.667 |
| multi_table_simple | 1 | 0 | 0.000 | 0.000 | 0.000 | 0.000 | 1.000 |
| single_source_basic | 1 | 0 | 0.000 | 0.000 | 0.000 | 0.000 | 1.000 |

### Pipeline: `wxai:openai/gpt-oss-120b-agentic-baseline0-3attempts`
| Category | # Records | # Predictions | Exec Acc | Non-Empty Exec Acc | Subset Non-Empty Exec Acc | BIRD Exec Acc | LLM Score |
|---|---|---|---|---|---|---|---|
| All Categories | 10 | 0 | 0.300 | 0.300 | 0.300 | 0.300 | 0.500 |
| has_aggregation | 7 | 0 | 0.286 | 0.286 | 0.286 | 0.286 | 0.571 |
| has_join | 5 | 0 | 0.400 | 0.400 | 0.400 | 0.400 | 0.400 |
| has_nested_query | 8 | 0 | 0.375 | 0.375 | 0.375 | 0.375 | 0.625 |
| has_sorting | 3 | 0 | 0.667 | 0.667 | 0.667 | 0.667 | 0.667 |
| multi_table_simple | 1 | 0 | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 |
| single_source_basic | 1 | 0 | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 |

### Pipeline: `wxai:openai/gpt-oss-120b-agentic-baseline1-3attempts`
| Category | # Records | # Predictions | Exec Acc | Non-Empty Exec Acc | Subset Non-Empty Exec Acc | BIRD Exec Acc | LLM Score |
|---|---|---|---|---|---|---|---|
| All Categories | 10 | 0 | 0.200 | 0.200 | 0.200 | 0.200 | 0.600 |
| has_aggregation | 7 | 0 | 0.286 | 0.286 | 0.286 | 0.286 | 0.857 |
| has_join | 5 | 0 | 0.200 | 0.200 | 0.200 | 0.200 | 0.400 |
| has_nested_query | 8 | 0 | 0.250 | 0.250 | 0.250 | 0.250 | 0.750 |
| has_sorting | 3 | 0 | 0.333 | 0.333 | 0.333 | 0.333 | 0.333 |
| multi_table_simple | 1 | 0 | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 |
| single_source_basic | 1 | 0 | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 |

### Pipeline: `wxai:openai/gpt-oss-120b-agentic-baseline2-3attempts`
| Category | # Records | # Predictions | Exec Acc | Non-Empty Exec Acc | Subset Non-Empty Exec Acc | BIRD Exec Acc | LLM Score |
|---|---|---|---|---|---|---|---|
| All Categories | 10 | 0 | 0.300 | 0.200 | 0.200 | 0.300 | 0.600 |
| has_aggregation | 7 | 0 | 0.429 | 0.286 | 0.286 | 0.429 | 0.857 |
| has_join | 5 | 0 | 0.200 | 0.200 | 0.200 | 0.200 | 0.400 |
| has_nested_query | 8 | 0 | 0.375 | 0.250 | 0.250 | 0.375 | 0.750 |
| has_sorting | 3 | 0 | 0.333 | 0.333 | 0.333 | 0.333 | 0.333 |
| multi_table_simple | 1 | 0 | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 |
| single_source_basic | 1 | 0 | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 |

### Pipeline: `wxai:openai/gpt-oss-120b-agentic-baseline3-3attempts`
| Category | # Records | # Predictions | Exec Acc | Non-Empty Exec Acc | Subset Non-Empty Exec Acc | BIRD Exec Acc | LLM Score |
|---|---|---|---|---|---|---|---|
| All Categories | 10 | 0 | 0.000 | 0.000 | 0.000 | 0.000 | 0.200 |
| has_aggregation | 7 | 0 | 0.000 | 0.000 | 0.000 | 0.000 | 0.286 |
| has_join | 5 | 0 | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 |
| has_nested_query | 8 | 0 | 0.000 | 0.000 | 0.000 | 0.000 | 0.250 |
| has_sorting | 3 | 0 | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 |
| multi_table_simple | 1 | 0 | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 |
| single_source_basic | 1 | 0 | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 |

### Pipeline: `wxai:openai/gpt-oss-120b-agentic-baseline4-3attempts`
| Category | # Records | # Predictions | Exec Acc | Non-Empty Exec Acc | Subset Non-Empty Exec Acc | BIRD Exec Acc | LLM Score |
|---|---|---|---|---|---|---|---|
| All Categories | 10 | 0 | 0.400 | 0.400 | 0.400 | 0.400 | 0.800 |
| has_aggregation | 7 | 0 | 0.667 | 0.667 | 0.667 | 0.667 | 0.667 |
| has_join | 5 | 0 | 0.333 | 0.333 | 0.333 | 0.333 | 0.667 |
| has_nested_query | 8 | 0 | 0.667 | 0.667 | 0.667 | 0.667 | 0.667 |
| has_sorting | 3 | 0 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 |
| multi_table_simple | 1 | 0 | 0.000 | 0.000 | 0.000 | 0.000 | 1.000 |
| single_source_basic | 1 | 0 | 0.000 | 0.000 | 0.000 | 0.000 | 1.000 |

### Pipeline: `wxai:openai/gpt-oss-120b-agentic-baseline5-3attempts`
| Category | # Records | # Predictions | Exec Acc | Non-Empty Exec Acc | Subset Non-Empty Exec Acc | BIRD Exec Acc | LLM Score |
|---|---|---|---|---|---|---|---|
| All Categories | 10 | 0 | 0.143 | 0.143 | 0.143 | 0.143 | 0.571 |
| has_aggregation | 7 | 0 | 0.200 | 0.200 | 0.200 | 0.200 | 0.600 |
| has_join | 5 | 0 | 0.000 | 0.000 | 0.000 | 0.000 | 0.250 |
| has_nested_query | 8 | 0 | 0.167 | 0.167 | 0.167 | 0.167 | 0.500 |
| has_sorting | 3 | 0 | 0.333 | 0.333 | 0.333 | 0.333 | 0.667 |
| multi_table_simple | 1 | 0 | - | - | - | - | - |
| single_source_basic | 1 | 0 | 0.000 | 0.000 | 0.000 | 0.000 | 1.000 |

### Pipeline: `wxai:openai/gpt-oss-120b-greedy-zero-shot-chatapi`
| Category | # Records | # Predictions | Exec Acc | Non-Empty Exec Acc | Subset Non-Empty Exec Acc | BIRD Exec Acc | LLM Score |
|---|---|---|---|---|---|---|---|
| All Categories | 10 | 0 | 0.400 | 0.300 | 0.300 | 0.400 | 0.800 |
| has_aggregation | 7 | 0 | 0.429 | 0.286 | 0.286 | 0.429 | 0.714 |
| has_join | 5 | 0 | 0.400 | 0.400 | 0.400 | 0.400 | 0.600 |
| has_nested_query | 8 | 0 | 0.500 | 0.375 | 0.375 | 0.500 | 0.750 |
| has_sorting | 3 | 0 | 0.667 | 0.667 | 0.667 | 0.667 | 0.667 |
| multi_table_simple | 1 | 0 | 0.000 | 0.000 | 0.000 | 0.000 | 1.000 |
| single_source_basic | 1 | 0 | 0.000 | 0.000 | 0.000 | 0.000 | 1.000 |