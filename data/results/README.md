# Text-to-SQL Evaluation Results Dashboard

| Benchmark | Description | DB Type | Records | Pipelines |
|-----------|-------------|---------|---------|-----------|
| [bird_mini_dev_sqlite](#benchmark-bird_mini_dev_sqlite) | BIRD-SQL Mini-Dev in SQLite https://github.com/bird-bench/mini_dev | sqlite | 500 | 10 |
| [bird_mini_dev_postgres](#benchmark-bird_mini_dev_postgres) | BIRD-SQL Mini-Dev in PostgreSQL https://github.com/bird-bench/mini_dev | postgres | 500 | 10 |
| [beaver](#benchmark-beaver) | Beaver benchmark https://peterbaile.github.io/beaver/ | mysql | 209 | 10 |
| [archer_en_dev](#benchmark-archer_en_dev) | Archer English Dev Set https://sig4kg.github.io/archer-bench/ | sqlite | 104 | 10 |
| [spider_dev](#benchmark-spider_dev) | Spider Dev Set - Full 1,034 questions https://yale-lily.github.io/spider | sqlite | 1034 | 10 |
| [spider_realistic](#benchmark-spider_realistic) | Spider Realistic Dataset In Structure-Grounded Pretraining for Text-to-SQL https://zenodo.org/records/5205322 | sqlite | 508 | 10 |

---
### Benchmark: bird_mini_dev_sqlite

_Results sorted by default on `subset_non_empty_execution_accuracy` (higher is better)_

📄 [View In-Depth Summary Results Across Categories](bird_mini_dev_sqlite-predictions_eval_summary.md) - [View Examples of Errors for Error Analysis](bird_mini_dev_sqlite-predictions_eval_errors.md)

| Rank | Model / Pipeline | Execution Acc | Non-Empty Exec Acc | Subset Non-Empty Exec Acc | BIRD Exec Acc | LLM Judge Score | Parsable SQL | SQL Syntactic Match | Eval Err | DF Err | Avg Tokens/Q | Avg Inference (ms) | Avg Execution (ms) | Total Tokens | Total Inference (ms) | Total Execution (ms) | #Records | #Predictions | #Evaluated | #Correct Non-Empty Exec Acc | #Correct Subset Non-Empty Exec Acc | #Correct As Per LLM Judge |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | wxai:openai/gpt-oss-120b-greedy-zero-shot-chatapi | 0.51 | 0.51 | 0.57 | 0.53 | 0.90 | 1.00 | 0.05 | 0.00 | 0.01 | 8597.60 | 5374.99 | 155.27 | 4298799 | 2687494.35 | 77635.25 | 500 | 500 | 498 | 255 | 284 | 450 |
| 2 | wxai:meta-llama/llama-4-maverick-17b-128e-instruct-fp8-greedy-zero-shot-chatapi | 0.53 | 0.53 | 0.56 | 0.56 | 0.85 | 1.00 | 0.13 | 0.00 | 0.04 | 8261.04 | 2462.63 | 166.81 | 4130522 | 1231312.75 | 83407.49 | 500 | 500 | 498 | 265 | 281 | 424 |
| 3 | wxai:meta-llama/llama-3-3-70b-instruct-greedy-zero-shot-chatapi | 0.50 | 0.50 | 0.54 | 0.55 | 0.85 | 1.00 | 0.11 | 0.00 | 0.06 | 8130.33 | 7976.21 | 168.95 | 4065163 | 3988106.65 | 84474.2 | 500 | 500 | 498 | 252 | 270 | 423 |
| 4 | wxai:openai/gpt-oss-120b-agentic-baseline1-3attempts | 0.40 | 0.40 | 0.44 | 0.42 | 0.81 | 1.00 | 0.04 | 0.00 | 0.01 | 8816.23 | 6142.69 | 90.35 | 4408113 | 3071344.92 | 45176.67 | 500 | 500 | 498 | 198 | 221 | 403 |
| 5 | wxai:openai/gpt-oss-120b-agentic-baseline2-3attempts | 0.38 | 0.38 | 0.44 | 0.41 | 0.79 | 1.00 | 0.04 | 0.00 | 0.00 | 8809.55 | 5758.73 | 106.47 | 4404773 | 2879362.51 | 53236.95 | 500 | 500 | 498 | 192 | 220 | 396 |
| 6 | wxai:ibm/granite-4-h-small-greedy-zero-shot-chatapi | 0.41 | 0.41 | 0.43 | 0.42 | 0.68 | 1.00 | 0.05 | 0.00 | 0.20 | 8112.76 | 4618.98 | 132.98 | 4056380 | 2309490.68 | 66487.83 | 500 | 500 | 498 | 203 | 217 | 338 |
| 7 | wxai:openai/gpt-oss-120b-agentic-baseline0-3attempts | 0.32 | 0.32 | 0.42 | 0.35 | 0.83 | 0.99 | 0.03 | 0.00 | 0.01 | 9165.13 | 6716.09 | 109.42 | 4582564 | 3358044.83 | 54710.29 | 500 | 500 | 498 | 158 | 208 | 413 |
| 8 | wxai:openai/gpt-oss-120b-agentic-baseline4-3attempts | 0.35 | 0.35 | 0.41 | 0.36 | 0.84 | 0.96 | 0.03 | 0.00 | 0.03 | 23282.17 | 84675.12 | 26977.05 | 11641084 | 42337559.84 | 13488523.79 | 500 | 500 | 478 | 173 | 204 | 421 |
| 9 | wxai:openai/gpt-oss-120b-agentic-baseline5-3attempts | 0.27 | 0.27 | 0.29 | 0.29 | 0.52 | 0.86 | 0.03 | 0.00 | 0.23 | 41658.35 | 157580.62 | 7869.64 | 20829175 | 78790311.41 | 3934818.07 | 500 | 500 | 429 | 136 | 144 | 261 |
| 10 | wxai:openai/gpt-oss-120b-agentic-baseline3-3attempts | 0.02 | 0.02 | 0.03 | 0.02 | 0.12 | 0.99 | 0.04 | 0.00 | 0.83 | 25100.44 | 40682.63 | 146.72 | 12550218 | 20341316.17 | 73362.39 | 500 | 500 | 498 | 10 | 15 | 60 |

![Chart for bird_mini_dev_sqlite](charts/bird_mini_dev_sqlite-predictions_eval_summary.png)

### Benchmark: bird_mini_dev_postgres

_Results sorted by default on `subset_non_empty_execution_accuracy` (higher is better)_

📄 [View In-Depth Summary Results Across Categories](bird_mini_dev_postgres-predictions_eval_summary.md) - [View Examples of Errors for Error Analysis](bird_mini_dev_postgres-predictions_eval_errors.md)

| Rank | Model / Pipeline | Execution Acc | Non-Empty Exec Acc | Subset Non-Empty Exec Acc | BIRD Exec Acc | LLM Judge Score | Parsable SQL | SQL Syntactic Match | Eval Err | DF Err | Avg Tokens/Q | Avg Inference (ms) | Avg Execution (ms) | Total Tokens | Total Inference (ms) | Total Execution (ms) | #Records | #Predictions | #Evaluated | #Correct Non-Empty Exec Acc | #Correct Subset Non-Empty Exec Acc | #Correct As Per LLM Judge |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | wxai:openai/gpt-oss-120b-greedy-zero-shot-chatapi | 0.45 | 0.45 | 0.51 | 0.47 | 0.86 | 1.00 | 0.04 | 0.00 | 0.03 | 8760.12 | 5758.15 | 356.71 | 4380060 | 2879074.43 | 178353.89 | 500 | 500 | 499 | 225 | 254 | 432 |
| 2 | wxai:meta-llama/llama-3-3-70b-instruct-greedy-zero-shot-chatapi | 0.41 | 0.41 | 0.44 | 0.43 | 0.72 | 1.00 | 0.06 | 0.00 | 0.21 | 8288.53 | 8061.44 | 256.69 | 4144266 | 4030721.52 | 128347.37 | 500 | 500 | 500 | 204 | 218 | 360 |
| 3 | wxai:openai/gpt-oss-120b-agentic-baseline1-3attempts | 0.35 | 0.35 | 0.41 | 0.38 | 0.80 | 1.00 | 0.04 | 0.00 | 0.01 | 9165.86 | 6702.31 | 240.04 | 4582932 | 3351156.83 | 120019.02 | 500 | 500 | 499 | 173 | 207 | 401 |
| 4 | wxai:openai/gpt-oss-120b-agentic-baseline2-3attempts | 0.35 | 0.34 | 0.39 | 0.38 | 0.82 | 1.00 | 0.02 | 0.00 | 0.00 | 9087.37 | 14168.63 | 280.03 | 4543686 | 7084316.59 | 140013.41 | 500 | 500 | 499 | 171 | 196 | 410 |
| 5 | wxai:openai/gpt-oss-120b-agentic-baseline0-3attempts | 0.29 | 0.29 | 0.37 | 0.32 | 0.79 | 1.00 | 0.02 | 0.00 | 0.01 | 9787.48 | 7724.49 | 239.90 | 4893741 | 3862244.17 | 119951.02 | 500 | 500 | 498 | 144 | 185 | 395 |
| 6 | wxai:openai/gpt-oss-120b-agentic-baseline4-3attempts | 0.29 | 0.29 | 0.37 | 0.32 | 0.82 | 0.95 | 0.04 | 0.00 | 0.03 | 23740.99 | 90984.40 | 10991.48 | 11870494 | 45492200.2 | 5495740.41 | 500 | 500 | 475 | 145 | 185 | 409 |
| 7 | wxai:meta-llama/llama-4-maverick-17b-128e-instruct-fp8-greedy-zero-shot-chatapi | 0.33 | 0.32 | 0.37 | 0.35 | 0.62 | 1.00 | 0.08 | 0.00 | 0.30 | 8417.81 | 2613.22 | 227.46 | 4208906 | 1306609.41 | 113728.01 | 500 | 500 | 500 | 162 | 183 | 308 |
| 8 | wxai:ibm/granite-4-h-small-greedy-zero-shot-chatapi | 0.32 | 0.32 | 0.34 | 0.34 | 0.62 | 1.00 | 0.05 | 0.00 | 0.25 | 8273.99 | 5565.69 | 233.18 | 4136996 | 2782844.04 | 116590.23 | 500 | 500 | 500 | 159 | 169 | 309 |
| 9 | wxai:openai/gpt-oss-120b-agentic-baseline5-3attempts | 0.24 | 0.24 | 0.25 | 0.26 | 0.49 | 0.75 | 0.03 | 0.00 | 0.15 | 38646.11 | 209082.93 | 986.41 | 19323055 | 104541463.27 | 493206.98 | 500 | 500 | 376 | 119 | 126 | 247 |
| 10 | wxai:openai/gpt-oss-120b-agentic-baseline3-3attempts | 0.02 | 0.02 | 0.03 | 0.04 | 0.15 | 0.98 | 0.03 | 0.00 | 0.79 | 25779.08 | 74387.21 | 232.61 | 12889541 | 37193607.12 | 116306.44 | 500 | 500 | 499 | 11 | 15 | 73 |

![Chart for bird_mini_dev_postgres](charts/bird_mini_dev_postgres-predictions_eval_summary.png)

### Benchmark: beaver

_Results sorted by default on `subset_non_empty_execution_accuracy` (higher is better)_

📄 [View In-Depth Summary Results Across Categories](beaver-predictions_eval_summary.md) - [View Examples of Errors for Error Analysis](beaver-predictions_eval_errors.md)

| Rank | Model / Pipeline | Execution Acc | Non-Empty Exec Acc | Subset Non-Empty Exec Acc | BIRD Exec Acc | LLM Judge Score | Parsable SQL | SQL Syntactic Match | Eval Err | DF Err | Avg Tokens/Q | Avg Inference (ms) | Avg Execution (ms) | Total Tokens | Total Inference (ms) | Total Execution (ms) | #Records | #Predictions | #Evaluated | #Correct Non-Empty Exec Acc | #Correct Subset Non-Empty Exec Acc | #Correct As Per LLM Judge |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | wxai:openai/gpt-oss-120b-agentic-baseline1-3attempts | 0.04 | 0.03 | 0.14 | 0.06 | 0.66 | 0.99 | 0.01 | 0.00 | 0.19 | 75441.56 | 49124.30 | 2573.12 | 15767287 | 10266979.45 | 537781.55 | 209 | 209 | 209 | 7 | 30 | 137 |
| 2 | wxai:openai/gpt-oss-120b-greedy-zero-shot-chatapi | 0.03 | 0.03 | 0.14 | 0.06 | 0.51 | 0.98 | 0.00 | 0.00 | 0.35 | 48814.07 | 17852.59 | 1168.76 | 10202141 | 3731191.68 | 244270.04 | 209 | 209 | 209 | 6 | 29 | 106 |
| 3 | wxai:openai/gpt-oss-120b-agentic-baseline2-3attempts | 0.05 | 0.04 | 0.14 | 0.07 | 0.69 | 0.98 | 0.00 | 0.00 | 0.12 | 76396.56 | 48405.14 | 2375.63 | 15966882 | 10116673.94 | 496506.36 | 209 | 209 | 209 | 9 | 29 | 145 |
| 4 | wxai:openai/gpt-oss-120b-agentic-baseline0-3attempts | 0.03 | 0.03 | 0.12 | 0.06 | 0.69 | 1.00 | 0.00 | 0.00 | 0.16 | 90844.61 | 74566.41 | 11309.07 | 18986523 | 15584378.99 | 2363595.7 | 209 | 209 | 209 | 6 | 25 | 144 |
| 5 | wxai:meta-llama/llama-4-maverick-17b-128e-instruct-fp8-greedy-zero-shot-chatapi | 0.02 | 0.01 | 0.11 | 0.04 | 0.41 | 1.00 | 0.00 | 0.00 | 0.44 | 48177.68 | 8351.90 | 1092.14 | 10069135 | 1745546.08 | 228257.4 | 209 | 209 | 209 | 3 | 22 | 86 |
| 6 | wxai:openai/gpt-oss-120b-agentic-baseline4-3attempts | 0.01 | 0.01 | 0.11 | 0.01 | 0.30 | 0.49 | 0.00 | 0.00 | 0.17 | 75392.29 | 137697.09 | 10225.30 | 15756989 | 28778691.33 | 2137088.56 | 209 | 209 | 103 | 2 | 22 | 62 |
| 7 | wxai:meta-llama/llama-3-3-70b-instruct-greedy-zero-shot-chatapi | 0.02 | 0.01 | 0.09 | 0.05 | 0.31 | 1.00 | 0.00 | 0.00 | 0.49 | 48250.76 | 38699.56 | 824.87 | 10084409 | 8088208.88 | 172397.08 | 209 | 209 | 209 | 3 | 18 | 64 |
| 8 | wxai:openai/gpt-oss-120b-agentic-baseline5-3attempts | 0.01 | 0.01 | 0.08 | 0.01 | 0.28 | 0.35 | 0.00 | 0.00 | 0.06 | 74590.01 | 196307.30 | 1251.79 | 15589313 | 41028225.56 | 261624.32 | 209 | 209 | 74 | 2 | 17 | 59 |
| 9 | wxai:ibm/granite-4-h-small-greedy-zero-shot-chatapi | 0.01 | 0.01 | 0.05 | 0.02 | 0.18 | 1.00 | 0.00 | 0.00 | 0.71 | 48261.02 | 21215.98 | 412.62 | 10086554 | 4434140.71 | 86238.13 | 209 | 209 | 209 | 2 | 11 | 37 |
| 10 | wxai:openai/gpt-oss-120b-agentic-baseline3-3attempts | 0.02 | 0.02 | 0.03 | 0.03 | 0.31 | 0.95 | 0.00 | 0.00 | 0.64 | 115903.98 | 91929.20 | 1559.71 | 24223931 | 19213203.4 | 325978.81 | 209 | 209 | 208 | 4 | 6 | 65 |

![Chart for beaver](charts/beaver-predictions_eval_summary.png)

### Benchmark: archer_en_dev

_Results sorted by default on `subset_non_empty_execution_accuracy` (higher is better)_

📄 [View In-Depth Summary Results Across Categories](archer_en_dev-predictions_eval_summary.md) - [View Examples of Errors for Error Analysis](archer_en_dev-predictions_eval_errors.md) - [View Full Results JSON](archer_en_dev-predictions_eval.json)

| Rank | Model / Pipeline | Execution Acc | Non-Empty Exec Acc | Subset Non-Empty Exec Acc | BIRD Exec Acc | LLM Judge Score | Parsable SQL | SQL Syntactic Match | Eval Err | DF Err | Avg Tokens/Q | Avg Inference (ms) | Avg Execution (ms) | Total Tokens | Total Inference (ms) | Total Execution (ms) | #Records | #Predictions | #Evaluated | #Correct Non-Empty Exec Acc | #Correct Subset Non-Empty Exec Acc | #Correct As Per LLM Judge |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | wxai:openai/gpt-oss-120b-agentic-baseline1-3attempts | 0.33 | 0.33 | 0.35 | 0.34 | 0.70 | 1.00 | 0.00 | 0.00 | 0.04 | 2033.98 | 10503.69 | 6.31 | 211534 | 1092383.76 | 656.04 | 104 | 104 | 104 | 34 | 36 | 73 |
| 2 | wxai:openai/gpt-oss-120b-agentic-baseline0-3attempts | 0.18 | 0.17 | 0.33 | 0.19 | 0.63 | 1.00 | 0.00 | 0.00 | 0.06 | 2245.66 | 11781.76 | 6.48 | 233549 | 1225303.3 | 673.55 | 104 | 104 | 104 | 18 | 34 | 66 |
| 3 | wxai:openai/gpt-oss-120b-greedy-zero-shot-chatapi | 0.30 | 0.28 | 0.31 | 0.31 | 0.74 | 1.00 | 0.00 | 0.00 | 0.13 | 1629.68 | 7191.54 | 37.32 | 169487 | 747919.72 | 3880.78 | 104 | 104 | 104 | 29 | 32 | 77 |
| 4 | wxai:openai/gpt-oss-120b-agentic-baseline2-3attempts | 0.33 | 0.29 | 0.31 | 0.35 | 0.71 | 1.00 | 0.00 | 0.00 | 0.01 | 1808.19 | 8777.84 | 6.69 | 188052 | 912895.08 | 696.0 | 104 | 104 | 104 | 30 | 32 | 74 |
| 5 | wxai:openai/gpt-oss-120b-agentic-baseline4-3attempts | 0.24 | 0.24 | 0.31 | 0.24 | 0.69 | 0.81 | 0.00 | 0.00 | 0.00 | 3566.39 | 97801.63 | 34912.71 | 370905 | 10171369.23 | 3630922.1 | 104 | 104 | 84 | 25 | 32 | 72 |
| 6 | wxai:meta-llama/llama-4-maverick-17b-128e-instruct-fp8-greedy-zero-shot-chatapi | 0.23 | 0.21 | 0.22 | 0.24 | 0.52 | 1.00 | 0.00 | 0.00 | 0.06 | 1134.86 | 3664.11 | 42.27 | 118025 | 381067.68 | 4396.29 | 104 | 104 | 104 | 22 | 23 | 54 |
| 7 | wxai:meta-llama/llama-3-3-70b-instruct-greedy-zero-shot-chatapi | 0.20 | 0.20 | 0.20 | 0.20 | 0.58 | 1.00 | 0.00 | 0.00 | 0.03 | 1087.05 | 5366.80 | 34.19 | 113053 | 558147.12 | 3555.27 | 104 | 104 | 104 | 21 | 21 | 60 |
| 8 | wxai:openai/gpt-oss-120b-agentic-baseline5-3attempts | 0.21 | 0.20 | 0.20 | 0.21 | 0.51 | 0.70 | 0.00 | 0.00 | 0.04 | 8109.88 | 135190.70 | 12944.26 | 843427 | 14059833.29 | 1346203.08 | 104 | 104 | 73 | 21 | 21 | 53 |
| 9 | wxai:ibm/granite-4-h-small-greedy-zero-shot-chatapi | 0.17 | 0.17 | 0.17 | 0.17 | 0.49 | 1.00 | 0.00 | 0.00 | 0.08 | 1063.48 | 5281.20 | 40.68 | 110602 | 549244.82 | 4230.48 | 104 | 104 | 104 | 18 | 18 | 51 |
| 10 | wxai:openai/gpt-oss-120b-agentic-baseline3-3attempts | 0.08 | 0.07 | 0.08 | 0.08 | 0.25 | 0.98 | 0.00 | 0.00 | 0.70 | 4570.28 | 46927.94 | 5.95 | 475309 | 4880505.96 | 618.41 | 104 | 104 | 104 | 7 | 8 | 26 |

![Chart for archer_en_dev](charts/archer_en_dev-predictions_eval_summary.png)

### Benchmark: spider_dev

_Results sorted by default on `subset_non_empty_execution_accuracy` (higher is better)_

📄 [View In-Depth Summary Results Across Categories](spider_dev-predictions_eval_summary.md) - [View Examples of Errors for Error Analysis](spider_dev-predictions_eval_errors.md)

| Rank | Model / Pipeline | Execution Acc | Non-Empty Exec Acc | Subset Non-Empty Exec Acc | BIRD Exec Acc | LLM Judge Score | Parsable SQL | SQL Syntactic Match | Eval Err | DF Err | Avg Tokens/Q | Avg Inference (ms) | Avg Execution (ms) | Total Tokens | Total Inference (ms) | Total Execution (ms) | #Records | #Predictions | #Evaluated | #Correct Non-Empty Exec Acc | #Correct Subset Non-Empty Exec Acc | #Correct As Per LLM Judge |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | wxai:meta-llama/llama-3-3-70b-instruct-greedy-zero-shot-chatapi | 0.82 | 0.78 | 0.79 | 0.79 | 0.97 | 1.00 | 0.27 | 0.00 | 0.01 | 1060.21 | 1699.00 | 53.02 | 1096253 | 1756763.6 | 54824.28 | 1034 | 1034 | 1034 | 805 | 820 | 1003 |
| 2 | wxai:meta-llama/llama-4-maverick-17b-128e-instruct-fp8-greedy-zero-shot-chatapi | 0.82 | 0.78 | 0.79 | 0.79 | 0.97 | 1.00 | 0.29 | 0.00 | 0.00 | 1054.71 | 1358.10 | 52.66 | 1090565 | 1404280.11 | 54455.43 | 1034 | 1034 | 1034 | 803 | 814 | 1008 |
| 3 | wxai:openai/gpt-oss-120b-greedy-zero-shot-chatapi | 0.80 | 0.76 | 0.78 | 0.77 | 0.98 | 1.00 | 0.31 | 0.00 | 0.00 | 1237.59 | 2248.10 | 57.08 | 1279670 | 2324531.4 | 59025.86 | 1034 | 1034 | 1034 | 791 | 808 | 1017 |
| 4 | wxai:openai/gpt-oss-120b-agentic-baseline4-3attempts | 0.73 | 0.72 | 0.78 | 0.69 | 0.97 | 1.00 | 0.28 | 0.00 | 0.01 | 3679.25 | 48985.67 | 21396.45 | 3804346 | 50651186.67 | 22123930.73 | 1034 | 1034 | 1031 | 744 | 808 | 1004 |
| 5 | wxai:openai/gpt-oss-120b-agentic-baseline1-3attempts | 0.79 | 0.76 | 0.78 | 0.76 | 0.98 | 1.00 | 0.30 | 0.00 | 0.00 | 1238.58 | 2365.03 | 6.65 | 1280691 | 2445442.6 | 6871.91 | 1034 | 1034 | 1034 | 783 | 802 | 1011 |
| 6 | wxai:openai/gpt-oss-120b-agentic-baseline2-3attempts | 0.79 | 0.76 | 0.77 | 0.76 | 0.98 | 1.00 | 0.30 | 0.00 | 0.00 | 1240.12 | 2443.20 | 6.25 | 1282283 | 2526264.48 | 6461.18 | 1034 | 1034 | 1034 | 784 | 801 | 1011 |
| 7 | wxai:openai/gpt-oss-120b-agentic-baseline0-3attempts | 0.74 | 0.72 | 0.77 | 0.71 | 0.97 | 1.00 | 0.23 | 0.00 | 0.00 | 1311.64 | 2593.13 | 6.96 | 1356238 | 2681300.95 | 7199.96 | 1034 | 1034 | 1034 | 741 | 795 | 1006 |
| 8 | wxai:ibm/granite-4-h-small-greedy-zero-shot-chatapi | 0.77 | 0.73 | 0.74 | 0.74 | 0.94 | 1.00 | 0.31 | 0.00 | 0.02 | 1039.17 | 2784.63 | 55.64 | 1074502 | 2879307.84 | 57531.82 | 1034 | 1034 | 1034 | 756 | 766 | 971 |
| 9 | wxai:openai/gpt-oss-120b-agentic-baseline5-3attempts | 0.72 | 0.71 | 0.72 | 0.69 | 0.89 | 0.96 | 0.27 | 0.00 | 0.02 | 9705.32 | 88533.30 | 10158.31 | 10035304 | 91543429.76 | 10503695.47 | 1034 | 1034 | 989 | 736 | 747 | 921 |
| 10 | wxai:openai/gpt-oss-120b-agentic-baseline3-3attempts | 0.01 | 0.01 | 0.01 | 0.01 | 0.02 | 1.00 | 0.24 | 0.00 | 0.98 | 4322.56 | 26326.43 | 8.00 | 4469525 | 27221528.69 | 8271.72 | 1034 | 1034 | 1034 | 9 | 10 | 20 |

![Chart for spider_dev](charts/spider_dev-predictions_eval_summary.png)

### Benchmark: spider_realistic

_Results sorted by default on `subset_non_empty_execution_accuracy` (higher is better)_

📄 [View In-Depth Summary Results Across Categories](spider_realistic-predictions_eval_summary.md) - [View Examples of Errors for Error Analysis](spider_realistic-predictions_eval_errors.md)

| Rank | Model / Pipeline | Execution Acc | Non-Empty Exec Acc | Subset Non-Empty Exec Acc | BIRD Exec Acc | LLM Judge Score | Parsable SQL | SQL Syntactic Match | Eval Err | DF Err | Avg Tokens/Q | Avg Inference (ms) | Avg Execution (ms) | Total Tokens | Total Inference (ms) | Total Execution (ms) | #Records | #Predictions | #Evaluated | #Correct Non-Empty Exec Acc | #Correct Subset Non-Empty Exec Acc | #Correct As Per LLM Judge |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | wxai:openai/gpt-oss-120b-greedy-zero-shot-chatapi | 0.81 | 0.76 | 0.77 | 0.80 | 0.99 | 1.00 | 0.20 | 0.00 | 0.00 | 1189.76 | 2590.36 | 47.42 | 604397 | 1315902.42 | 24091.46 | 508 | 508 | 508 | 388 | 392 | 502 |
| 2 | wxai:openai/gpt-oss-120b-agentic-baseline2-3attempts | 0.81 | 0.76 | 0.77 | 0.79 | 0.97 | 1.00 | 0.21 | 0.00 | 0.00 | 1200.01 | 2441.16 | 7.62 | 609607 | 1240108.69 | 3873.02 | 508 | 508 | 508 | 384 | 390 | 493 |
| 3 | wxai:openai/gpt-oss-120b-agentic-baseline1-3attempts | 0.79 | 0.74 | 0.76 | 0.79 | 0.96 | 1.00 | 0.21 | 0.00 | 0.00 | 1193.89 | 2282.72 | 7.86 | 606495 | 1159623.6 | 3991.3 | 508 | 508 | 508 | 378 | 384 | 489 |
| 4 | wxai:meta-llama/llama-4-maverick-17b-128e-instruct-fp8-greedy-zero-shot-chatapi | 0.80 | 0.74 | 0.75 | 0.79 | 0.96 | 1.00 | 0.21 | 0.00 | 0.01 | 1000.40 | 1686.43 | 48.16 | 508202 | 856708.72 | 24465.47 | 508 | 508 | 508 | 378 | 383 | 489 |
| 5 | wxai:meta-llama/llama-3-3-70b-instruct-greedy-zero-shot-chatapi | 0.80 | 0.74 | 0.75 | 0.78 | 0.96 | 1.00 | 0.24 | 0.00 | 0.01 | 1001.18 | 2035.21 | 44.32 | 508598 | 1033885.48 | 22516.39 | 508 | 508 | 508 | 376 | 380 | 489 |
| 6 | wxai:openai/gpt-oss-120b-agentic-baseline4-3attempts | 0.73 | 0.71 | 0.75 | 0.70 | 0.96 | 1.00 | 0.19 | 0.00 | 0.01 | 3612.94 | 47224.40 | 19251.26 | 1835371 | 23989995.71 | 9779637.87 | 508 | 508 | 507 | 362 | 380 | 489 |
| 7 | wxai:openai/gpt-oss-120b-agentic-baseline0-3attempts | 0.75 | 0.70 | 0.74 | 0.74 | 0.97 | 1.00 | 0.15 | 0.00 | 0.00 | 1259.59 | 2374.40 | 7.99 | 639872 | 1206196.4 | 4060.69 | 508 | 508 | 508 | 355 | 376 | 493 |
| 8 | wxai:ibm/granite-4-h-small-greedy-zero-shot-chatapi | 0.75 | 0.70 | 0.71 | 0.74 | 0.93 | 1.00 | 0.21 | 0.00 | 0.03 | 978.02 | 2924.83 | 46.05 | 496833 | 1485813.85 | 23394.52 | 508 | 508 | 508 | 357 | 359 | 472 |
| 9 | wxai:openai/gpt-oss-120b-agentic-baseline5-3attempts | 0.68 | 0.66 | 0.67 | 0.66 | 0.84 | 0.95 | 0.18 | 0.00 | 0.04 | 9532.59 | 91403.42 | 10628.44 | 4842557 | 46432939.15 | 5399245.33 | 508 | 508 | 482 | 335 | 338 | 428 |
| 10 | wxai:openai/gpt-oss-120b-agentic-baseline3-3attempts | 0.02 | 0.02 | 0.02 | 0.02 | 0.03 | 1.00 | 0.18 | 0.00 | 0.97 | 4177.23 | 25118.63 | 21.17 | 2122034 | 12760265.9 | 10752.05 | 508 | 508 | 508 | 8 | 8 | 13 |

![Chart for spider_realistic](charts/spider_realistic-predictions_eval_summary.png)

