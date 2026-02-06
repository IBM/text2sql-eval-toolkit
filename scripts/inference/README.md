# Text-to-SQL Inference Pipelines

This directory contains inference pipelines for generating SQL queries from natural language questions. The toolkit supports both **standard (non-agentic)** and **agentic** baseline approaches.

## Table of Contents

- [Quick Start](#quick-start)
- [Pipeline Types](#pipeline-types)
  - [Standard Baseline (Non-Agentic)](#standard-baseline-non-agentic)
  - [Agentic Baselines](#agentic-baselines)
- [Usage Examples](#usage-examples)
- [Configuration Options](#configuration-options)
- [Implementation Details](#implementation-details)

## Quick Start

```bash
# Standard baseline (single-shot generation)
python scripts/inference/run_inference.py bird_mini_dev_sqlite_sample_50 \
    --model_names wxai:openai/gpt-oss-120b

# Agentic baseline1 (baseline prompt + retry logic)
python scripts/inference/run_inference.py bird_mini_dev_sqlite_sample_50 \
    --model_names wxai:openai/gpt-oss-120b \
    --pipeline_type agentic \
    --use_baseline_prompt \
    --max_attempts 5

# Agentic baseline2 (smarter retry logic)
python scripts/inference/run_inference.py bird_mini_dev_sqlite_sample_50 \
    --model_names wxai:openai/gpt-oss-120b \
    --pipeline_type agentic \
    --agentic_version v2 \
    --max_attempts 5

# Agentic baseline3 (LLM judge validation)
python scripts/inference/run_inference.py bird_mini_dev_sqlite_sample_50 \
    --model_names wxai:openai/gpt-oss-120b \
    --pipeline_type agentic \
    --agentic_version v3 \
    --max_attempts 5

# Agentic baseline4 (truly agentic - LLM controls workflow)
python scripts/inference/run_inference.py bird_mini_dev_sqlite_sample_50 \
    --model_names wxai:openai/gpt-oss-120b \
    --pipeline_type agentic \
    --agentic_version v4 \
    --max_attempts 5

# Agentic baseline5 (truly agentic with improved prompting)
python scripts/inference/run_inference.py bird_mini_dev_sqlite_sample_50 \
    --model_names wxai:openai/gpt-oss-120b \
    --pipeline_type agentic \
    --agentic_version v5 \
    --max_attempts 5
```

## Pipeline Types

### Standard Baseline (Non-Agentic)

The standard baseline performs **single-shot SQL generation** using an LLM with a zero-shot prompt.

**How it works:**
1. Constructs a prompt with the question, database schema, and instructions
2. Calls the LLM once to generate SQL
3. Returns the generated SQL (no validation or retry)

**Pipeline ID format:** `{model_name}-greedy-zero-shot-chatapi`

**Pros:**
- Fast (single LLM call)
- Simple and predictable
- Minimal latency and cost

**Cons:**
- No error recovery
- Can't fix syntax errors
- No schema probing

**Implementation:** [`src/text2sql_eval_toolkit/inference/baseline_llm_pipeline.py`](../../src/text2sql_eval_toolkit/inference/baseline_llm_pipeline.py)

### Agentic Baselines

Agentic baselines use **LangGraph** to create an agent that can:
- Execute generated SQL queries to check for errors
- Retry with error feedback when queries fail
- Probe the database schema for additional information
- Self-correct over multiple attempts

We provide **six agentic variants**:

#### agentic-baseline0 (Original Agent-Aware)

The first implementation with agent-specific prompts from the start.

**Pipeline ID:** `{model_name}-agentic-baseline0-{N}attempts`

**How it works:**
1. Uses verbose agent-aware prompts that explain the multi-step process
2. Executes SQL and checks for errors
3. Retries on any execution error
4. Can probe database schema when needed

**Command:**
```bash
python scripts/inference/run_inference.py <benchmark> \
    --model_names <model> \
    --pipeline_type agentic \
    --max_attempts 5
```

#### agentic-baseline1 (Baseline + Agentic Retry Logic)

Uses the **exact same prompt as the standard baseline** for the first attempt, then adds minimal retry logic.

**Pipeline ID:** `{model_name}-agentic-baseline1-{N}attempts`

**How it works:**
1. **First attempt:** Uses identical prompt to standard baseline
2. **Subsequent attempts:** Adds minimal error context to the prompt
3. Executes SQL and validates execution
4. Retries only on execution errors

**Command:**
```bash
python scripts/inference/run_inference.py <benchmark> \
    --model_names <model> \
    --pipeline_type agentic \
    --use_baseline_prompt \
    --max_attempts 5
```

#### agentic-baseline2 (Smart Retries)

Enhanced version with **smart error classification** and targeted retry decisions.

**Pipeline ID:** `{model_name}-agentic-baseline2-{N}attempts`

**How it works:**
1. **First attempt:** Same as baseline
2. **Error classification:** Analyzes error type and fixability
   - High-confidence fixable: column errors, table errors, syntax errors
   - Medium-confidence: type errors, aggregation errors
   - Unfixable: timeouts, permissions
3. **Smart retry logic:**
   - Skips retry if error is not fixable
   - Stops early if confidence is low and already tried once
   - Provides targeted fix instructions based on error category
4. **Minimal changes:** Instructs model to make minimal fixes

**Command:**
```bash
python scripts/inference/run_inference.py <benchmark> \
    --model_names <model> \
    --pipeline_type agentic \
    --agentic_version v2 \
    --max_attempts 5
```

#### agentic-baseline3 (LLM Judge Validation)

Uses an **LLM as a judge** to validate whether the generated SQL and results correctly answer the question.

**Pipeline ID:** `{model_name}-agentic-baseline3-{N}attempts`

**How it works:**
1. **First attempt:** Uses baseline prompt to generate SQL
2. **Execute SQL** and get dataframe results
3. **LLM Judge Validation:** Asks LLM to evaluate if the SQL and dataframe correctly answer the question
   - Provides: question, schema, generated SQL, execution results
   - LLM responds with: ACCEPT/RETRY verdict, confidence level, reasoning
4. **If LLM says ACCEPT:** Return the SQL and results
5. **If LLM says RETRY:** Generate new SQL incorporating LLM's feedback
6. Repeats until LLM accepts or max attempts reached

**Key Features:**
- **Semantic validation without ground truth:** The LLM judges correctness based on question intent
- **Comprehensive validation prompt:** Checks query correctness, result validity, and completeness
- **Confidence levels:** LLM provides HIGH/MEDIUM/LOW confidence with reasoning
- **Feedback loop:** LLM's critique guides retry attempts

**LLM Judge Criteria:**
The judge evaluates:
- Does the SQL target the right tables and columns?
- Are JOINs, filters, and aggregations appropriate?
- Do the results make sense for the question?
- Is the row count reasonable?
- Are there any obvious errors or missing information?

**Command:**
```bash
python scripts/inference/run_inference.py <benchmark> \
    --model_names <model> \
    --pipeline_type agentic \
    --agentic_version v3 \
    --max_attempts 5
```

#### agentic-baseline4 (Truly Agentic - LLM Controls Workflow)

The first **truly agentic** implementation where the **LLM decides the control flow** via tool calling and ReAct pattern.

**Pipeline ID:** `{model_name}-agentic-baseline4-{N}attempts`

**How it works:**
1. **ReAct Pattern:** LLM reasons and acts in a loop
   - **Thought:** LLM explains its reasoning
   - **Action:** LLM chooses which tool to use
   - **Observation:** System provides feedback
2. **Available Tools (LLM-controlled):**
   - `generate_sql(sql, reasoning)` - Generate and execute SQL
   - `probe_schema(table_name, reason)` - Get detailed schema info
   - `analyze_error(analysis, fixable)` - Analyze execution errors
   - `submit_final_answer(sql, confidence, explanation)` - Submit final result
3. **LLM Decision Making:**
   - LLM chooses when to probe schema vs. generate SQL
   - LLM decides what to do after errors (retry, probe, or give up)
   - LLM controls the entire workflow based on observations
4. **JSON-based Communication:**
   - LLM responds with structured JSON: `{"thought": "...", "action": "tool_name", "action_input": {...}}`
   - System executes the chosen action and returns results
   - LLM sees results and chooses next action

**Key Differences from Other Baselines:**
- ❌ **Others:** Python code decides control flow (hardcoded retry loops)
- ✅ **v4:** LLM decides control flow (chooses actions dynamically)
- ❌ **Others:** Fixed sequence of steps
- ✅ **v4:** Flexible workflow adapted by LLM based on context
- ❌ **Others:** Rule-based error handling
- ✅ **v4:** LLM-driven error analysis and recovery

**Example Workflow:**
```
User Question: "Find average salary by department"

LLM Thought: "I need to check if there's a salary column first"
LLM Action: probe_schema(table_name="employees")
System Observation: "Columns: id, name, department_id, monthly_pay"

LLM Thought: "It's called monthly_pay not salary, let me generate SQL"
LLM Action: generate_sql(sql="SELECT department_id, AVG(monthly_pay)...")
System Observation: "❌ Error: must appear in GROUP BY clause"

LLM Thought: "I forgot GROUP BY, let me fix it"
LLM Action: generate_sql(sql="SELECT department_id, AVG(monthly_pay) ... GROUP BY department_id")
System Observation: "✅ Success! 5 rows returned"

LLM Action: submit_final_answer(sql="...", confidence="high", explanation="...")
```

**Command:**
```bash
python scripts/inference/run_inference.py <benchmark> \
    --model_names <model> \
    --pipeline_type agentic \
    --agentic_version v4 \
    --max_attempts 5
```

#### agentic-baseline5 (Truly Agentic with Improved Prompting)

An enhanced version of agentic-baseline4 with **systematic multi-step reasoning** and **better prompting strategies** to encourage thorough exploration.

**Pipeline ID:** `{model_name}-agentic-baseline5-{N}attempts`

**How it works:**
1. **Systematic 6-Step Process:** Uses structured workflow embedded in system prompt
   - **ANALYZE:** Understand what the question is asking
   - **EXPLORE:** Use probe_schema to understand the database
   - **PLAN:** Think through SQL logic
   - **GENERATE:** Create the SQL query
   - **VALIDATE:** Check correctness before submitting
   - **SUBMIT:** Only when confident
2. **Same Tools as v4:** Uses identical ReAct pattern and tool-calling interface
   - `generate_sql`, `probe_schema`, `analyze_error`, `submit_final_answer`
3. **Encouraged Schema Probing:** System prompt strongly encourages probing tables before generating SQL
4. **Column Selection Guidelines:** Explicit rules for what columns to return
   - "If question asks 'which month?', return ONLY the month column"
   - "Match the question's specificity exactly"
5. **Validation Checklist:** 5-point checklist before submitting
   - Correct columns only (no extras)
   - Proper granularity (e.g., month vs full date)
   - Correct aggregation level
   - Proper ordering if needed
6. **Discouraged Rushing:** Prompt explicitly tells LLM to use multiple attempts
   - "Submitting on first try is rarely optimal"
7. **Few-Shot Example:** Includes complete 3-step example workflow showing proper schema probing and validation

**Key Differences from v4:**
- ❌ **v4:** Basic system prompt, minimal guidance
- ✅ **v5:** Comprehensive system prompt with step-by-step process
- ❌ **v4:** No explicit encouragement to probe schema
- ✅ **v5:** Strongly encourages schema probing as first step
- ❌ **v4:** No column selection guidelines
- ✅ **v5:** Explicit rules for what to return
- ❌ **v4:** No validation checklist
- ✅ **v5:** 5-point checklist before submission
- ❌ **v4:** No few-shot examples
- ✅ **v5:** Complete example workflow included

**Command:**
```bash
python scripts/inference/run_inference.py <benchmark> \
    --model_names <model> \
    --pipeline_type agentic \
    --agentic_version v5 \
    --max_attempts 5
```

**Implementation:** All agentic variants are in [`src/text2sql_eval_toolkit/inference/agentic_pipeline.py`](../../src/text2sql_eval_toolkit/inference/agentic_pipeline.py)

## Usage Examples

### Running Inference Only

```bash
# Standard baseline
python scripts/inference/run_inference.py bird_mini_dev_sqlite_sample_50 \
    --model_names wxai:openai/gpt-oss-120b \
    --decoding_method greedy \
    --max_new_tokens 512

# Agentic baseline1 (baseline prompt + agentic retry)
python scripts/inference/run_inference.py bird_mini_dev_sqlite_sample_50 \
    --model_names wxai:openai/gpt-oss-120b \
    --pipeline_type agentic \
    --use_baseline_prompt \
    --max_attempts 3

# Agentic baseline2 (agentic with smarter retries)
python scripts/inference/run_inference.py bird_mini_dev_sqlite_sample_50 \
    --model_names wxai:openai/gpt-oss-120b \
    --pipeline_type agentic \
    --agentic_version v2 \
    --max_attempts 5

# Agentic baseline3 (LLM judge validation)
python scripts/inference/run_inference.py bird_mini_dev_sqlite_sample_50 \
    --model_names wxai:openai/gpt-oss-120b \
    --pipeline_type agentic \
    --agentic_version v3 \
    --max_attempts 5

# Agentic baseline4 (truly agentic - LLM controls workflow)
python scripts/inference/run_inference.py bird_mini_dev_sqlite_sample_50 \
    --model_names wxai:openai/gpt-oss-120b \
    --pipeline_type agentic \
    --agentic_version v4 \
    --max_attempts 5

# Agentic baseline5 (truly agentic with improved prompting)
python scripts/inference/run_inference.py bird_mini_dev_sqlite_sample_50 \
    --model_names wxai:openai/gpt-oss-120b \
    --pipeline_type agentic \
    --agentic_version v5 \
    --max_attempts 5

# Multiple models
python scripts/inference/run_inference.py bird_mini_dev_sqlite_sample_50 \
    --model_names wxai:openai/gpt-oss-120b wxai:ibm/granite-4-h-small
```

### Running Full Experiments

The `run_experiment.py` script combines inference, execution, and evaluation:

```bash
# Standard baseline experiment
python scripts/run_experiment.py bird_mini_dev_sqlite_sample_50 \
    --model_names wxai:openai/gpt-oss-120b

# Agentic baseline1 experiment
python scripts/run_experiment.py bird_mini_dev_sqlite_sample_50 \
    --model_names wxai:openai/gpt-oss-120b \
    --pipeline_type agentic \
    --use_baseline_prompt \
    --max_attempts 5
```

## Configuration Options

### Common Arguments

- `benchmark_id`: Name of the benchmark (defined in `data/benchmarks.json`)
- `--model_names`: One or more model names (e.g., `wxai:openai/gpt-oss-120b`)
- `--decoding_method`: Decoding method (default: `greedy`)
- `--max_new_tokens`: Maximum tokens to generate (default: `512`)
- `--stop_sequences`: Stop sequences for generation (default: `["```"]`)

### Agentic Pipeline Arguments

- `--pipeline_type`: Choose `baseline` (default) or `agentic`
- `--max_attempts`: Maximum retry attempts for agentic (default: `3`)
- `--use_baseline_prompt`: Use baseline-compatible prompts (for agentic-baseline1)
- `--agentic_version`: Version of agentic logic - `v0`, `v1`, `v2`, `v3`, `v4`, or `v5` (default: `v1`)
  - `v0`: Agent-aware prompts (agentic-baseline0)
  - `v1`: Baseline-compatible prompts (agentic-baseline1)
  - `v2`: Smart retry logic with error classification (agentic-baseline2)
  - `v3`: LLM judge validation (agentic-baseline3)
  - `v4`: Truly agentic with LLM-controlled workflow (agentic-baseline4)
  - `v5`: Truly agentic with improved prompting and systematic reasoning (agentic-baseline5)

### LLM Provider Configuration

Set environment variables (or use a `.env` file in the project root):

**WatsonX.AI:**
```bash
WATSONX_APIKEY=your_api_key
WATSONX_API_BASE=https://your-instance.cloud.ibm.com
WATSONX_PROJECTID=your_project_id
```

**vLLM:**
```bash
VLLM_API_BASE=http://your-vllm-server:8000/v1
```

**Anthropic Claude:**
```bash
ANTHROPIC_API_KEY=your_api_key
```

See [`env.example`](../../env.example) for a complete template.

## Implementation Details

### Standard Baseline Architecture

```
Question + Schema
       ↓
  [LLM Inference]
       ↓
   SQL Output
```

**Files:**
- `src/text2sql_eval_toolkit/inference/baseline_llm_pipeline.py` - Main pipeline
- `src/text2sql_eval_toolkit/inference/inference_tools.py` - LLM clients and prompts

### Agentic Baseline Architecture

```
Question + Schema
       ↓
  [Agent State Init]
       ↓
  ┌──────────────────┐
  │ Generate SQL     │ ← LLM Call
  └────────┬─────────┘
           ↓
  ┌──────────────────┐
  │ Execute SQL      │ ← Database
  └────────┬─────────┘
           ↓
    ┌─────✓─────┐
    │ Success?  │
    └─────┬─────┘
      No  │  Yes
          │  ↓
          │  ┌──────────────────────┐
          │  │ LLM Judge Validate   │ ← (v3 only)
          │  │ (Question + SQL +    │ ← LLM Call
          │  │  Results) → Verdict  │
          │  └──────────┬───────────┘
          │             │
          │       ┌─────✓─────┐
          │       │  ACCEPT?  │ (v3 only)
          │       └─────┬─────┘
          │      RETRY  │  ACCEPT
          │             │  ↓
          │             │ Return SQL + Result
          │             ↓
          ↓  ┌──────────────────┐
          └─>│ More attempts?   │
             └────────┬─────────┘
                 Yes  │  No
                      │  ↓
                      │ Return best SQL
                      ↓
             ┌──────────────────┐
             │ Classify Error   │ (v2, v3)
             └────────┬─────────┘
                      ↓
             ┌──────────────────┐
             │ Probe Schema?    │
             └────────┬─────────┘
                      ↓
             [Retry with feedback] ───┘
```

**Key differences by version:**
- **v0/v1**: Basic retry on execution errors only
- **v2**: Adds smart error classification to decide if retry is worthwhile
- **v3**: Adds LLM judge validation - even successful executions are validated for correctness

**Built with LangGraph:**
- State management for multi-step reasoning
- Node-based execution (generate → execute → validate)
- Conditional routing based on errors and attempts
- Schema probing for additional context

### Truly Agentic Architecture (v4/v5)

For v4 and v5, the **LLM controls the workflow** using a ReAct (Reasoning + Acting) pattern with tool calling:

```
Question + Schema
       ↓
  [Agent State Init]
       ↓
  ┌─────────────────────────────────────────┐
  │         LLM DECISION LOOP               │
  │  (LLM chooses actions based on state)   │
  └─────────────────┬───────────────────────┘
                    ↓
              ┌─────────┐
              │   LLM   │ ← "What should I do next?"
              │ Reasons │    Think → Choose Tool
              └────┬────┘
                   ↓
        ╔══════════════════════════╗
        ║   LLM Chooses Action:    ║
        ╚══════════════════════════╝
                   │
        ┌──────────┼──────────┬──────────┐
        ↓          ↓          ↓          ↓
   [probe_   [generate_ [analyze_ [submit_final_
    schema]    sql]      error]    answer]
        │          │          │          │
        ↓          ↓          ↓          ↓
   Get table   Execute    Analyze    Submit &
   columns/    SQL query  error      return
   types       & get      message    result
              results
        │          │          │          │
        └──────────┴──────────┴──────────┘
                   ↓
         System Returns Observation
         ("✅ Success: 5 rows"
          "❌ Error: no such column"
          "Schema: table has columns X, Y, Z")
                   │
                   └───────┐
                           ↓
                    ┌─────────────┐
                    │ Reached     │
                    │ max actions │  No ──┐
                    │ or final    │       │
                    │ answer?     │       │
                    └─────────────┘       │
                           │ Yes          │
                           ↓              │
                    Return SQL + DF  ←────┘
```

**v4 vs v5 Differences:**

The control flow is identical, but **v5 has improved prompting**:

| Aspect | v4 | v5 |
|--------|----|----|
| **System Prompt** | Basic instructions | 6-step systematic process (ANALYZE → EXPLORE → PLAN → GENERATE → VALIDATE → SUBMIT) |
| **Schema Probing** | Optional, rarely used | Strongly encouraged as first step |
| **Column Validation** | Not mentioned | Explicit rules: "return ONLY what's asked" |
| **Validation Checklist** | None | 5-point checklist before submission |
| **Few-Shot Examples** | None | Complete 3-step example workflow |
| **Typical Behavior** | 1-2 actions, rushes to answer | 4-7 actions, explores thoroughly |

**Example v5 Workflow:**

```
Question: "Which month had peak SME consumption in 2013?"

Action 1: probe_schema("yearmonth")
  → Observation: "Date column is YYYYMM string format"

Action 2: probe_schema("customers") 
  → Observation: "Segment column has values: SME, LAM, KAM"

Action 3: generate_sql("SELECT SUBSTR(Date, 5, 2)...")
  → Observation: "✅ Success! 1 row returned: ['07']"

Action 4: submit_final_answer(sql="...", confidence="high")
  → Returns: SQL + Dataframe
```

**Files:**
- `src/text2sql_eval_toolkit/inference/agentic_pipeline.py` - All agentic variants (v0-v5)
- Uses `DatabaseExecutor` for SQL execution
- Supports SQLite, PostgreSQL, MySQL
- v4/v5 use JSON-based tool calling for LLM communication

### Prompt Design

**Standard Baseline Prompt:**
```
Your task is to convert a natural language question into an 
accurate SQL query using the given {db_type} database schema.

**Question:**
{question}

**Database Engine / Dialect:**
{db_type}

**Schema:**
{schema_verbalization}

**Instructions:**
- Only use columns listed in the schema
- Ensure the SQL query is valid and executable
- Use proper SQL syntax and conventions
- Your output must start with ```sql and end with ```
```

**Agentic-baseline1 First Attempt:** (Identical to standard baseline)

**Agentic-baseline1 Retry:**
```
{baseline_prompt}

**Previous attempt failed with error:**
{error_message}

Please generate a corrected SQL query.
```

**Agentic-baseline2 Retry:**
```
{baseline_prompt}

**Previous SQL (failed):**
{previous_sql}

**Error:**
{error_message}

**Fix Instructions (based on error type):**
{targeted_instructions}

- Make MINIMAL changes to fix the error
- Preserve the query logic
- Only change what's necessary
```

### Database Execution

The `DatabaseExecutor` class handles:
- SQL execution with error handling
- Schema probing (list tables, describe columns)
- Support for multiple database types
- Connection pooling and cleanup

### Error Classification (baseline2)

Errors are classified into categories:

| Category | Examples | Fixable | Confidence |
|----------|----------|---------|------------|
| **column_error** | "no such column", "unknown column" | ✅ Yes | 80% |
| **table_error** | "no such table", "unknown table" | ✅ Yes | 80% |
| **syntax_error** | "syntax error near", "unexpected" | ✅ Yes | 60% |
| **ambiguous_reference** | "ambiguous column" | ✅ Yes | 70% |
| **type_error** | "type mismatch", "cannot cast" | ⚠️ Maybe | 50% |
| **aggregation_error** | "must appear in GROUP BY" | ⚠️ Maybe | 60% |
| **timeout** | "timed out", "deadlock" | ❌ No | 10% |
| **permission** | "permission denied" | ❌ No | 0% |

The agent uses this classification to decide:
- Whether to retry at all
- Whether to probe schema for more info
- How many more attempts to use

## Extending the Pipelines

### Adding a New LLM Provider

1. Create a new client class in `inference_tools.py`:

```python
class MyLLMClient:
    def __init__(self, model_name: str, model_parameters: dict):
        # Initialize your client
        pass
    
    def generate_sql(self, messages: list[dict]) -> str:
        # Call your LLM API
        # Return the generated SQL
        pass
```

2. Register it in the pipeline's `_create_llm_client` method

### Adding a New Agentic Variant

1. Add a new version parameter in `AgenticSQLGenerationPipeline.__init__`:

```python
def __init__(self, ..., version: str = "v1"):
    self.version = version  # "v1", "v2", "v3", etc.
```

2. Implement version-specific logic in `_build_agent_prompt` or create a new method

3. Update the pipeline ID generation in `run_pipeline`

### Customizing Prompts

Edit the prompt construction in:
- **Standard:** `Text2SQLPrompt` class in `inference_tools.py`
- **Agentic:** `_build_*_prompt` methods in `agentic_pipeline.py`

### Adding New Database Types

1. Add database executor logic in `DatabaseExecutor` class
2. Update schema probing methods for your database
3. Add connection string handling
4. Update `benchmarks.json` with your database configuration

## Troubleshooting

**Issue: "No module named 'langgraph'"**
- Solution: `pip install langgraph langchain-core`

**Issue: "Missing WATSONX.AI credentials"**
- Solution: Set environment variables or create `.env` file (see Configuration)

**Issue: Agentic pipeline is slow**
- This is expected - multiple LLM calls and SQL executions per question
- Reduce `--max_attempts` to speed up (try 3 instead of 5)
- Use `--max_num_threads` to parallelize (default: 16)

**Issue: Different results on reruns**
- LLM generation can be non-deterministic
- Set `--decoding_method greedy` and `--temperature 0` (if supported)
- Use `--seed` for reproducibility (if your LLM provider supports it)

**Issue: Permission errors when reading .env**
- The evaluation toolkit auto-loads `.env` from project root
- Ensure the file exists and is readable
- Verify that `.env` IS in `.gitignore` (to prevent accidental commits of credentials)

## References

- **LangGraph Documentation:** https://langchain-ai.github.io/langgraph/
- **BIRD Benchmark:** https://bird-bench.github.io/
- **Text2SQL Evaluation Metrics:** See `scripts/evaluation/README.md`

## Contributing

To contribute a new pipeline or improvement:

1. Extend `BasePipeline` class
2. Implement `run_pipeline` and `get_results` methods
3. Add command-line arguments in `run_inference.py`
4. Update this README with usage examples
5. Add tests and documentation

