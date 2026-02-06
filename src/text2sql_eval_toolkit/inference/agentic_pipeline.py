#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Agentic pipeline for text-to-SQL generation using LangGraph.

This module provides an agentic pipeline that:
- Generates SQL queries from natural language questions
- Executes queries to check for errors
- Probes database schema when needed
- Fixes errors and retries up to a maximum number of attempts
- Validates results for correctness
"""

import asyncio
import json
import os
import time
from typing import Any, Dict, List, Optional, TypedDict, Annotated
from pathlib import Path
import pandas as pd

# Note: LangGraph imports are available but we're using a simpler state machine approach
# from langgraph.graph import StateGraph, END
# from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from text2sql_eval_toolkit.logging import get_logger
from text2sql_eval_toolkit.inference.base_pipeline import BasePipeline
from text2sql_eval_toolkit.inference.inference_tools import (
    Text2SQLPrompt,
    WXAIClientChatAPI,
    VLLMClientChatAPI,
    ClaudeClientChatAPI,
    OpenAIClientChatAPI,
    postprocess_sql,
)
from text2sql_eval_toolkit.utils import (
    get_benchmark_info,
    get_question_id,
    get_utterance,
)
from text2sql_eval_toolkit.execution.execution_tools import (
    run_sql_and_get_dataframe_async,
    run_sqlite_query_with_timeout,
    run_sql_and_get_dataframe_mysql_async,
    normalize_mysql_connection_string,
    quote_mixed_case_columns,
    quote_mysql_identifiers,
)
import asyncpg
import sqlite3
from func_timeout import func_timeout, FunctionTimedOut

logger = get_logger(__name__)


class AgentState(TypedDict):
    """State for the agentic SQL generation pipeline."""

    question: str
    schema: dict
    db_type: str
    db_id: Optional[str]
    db_connection_info: dict
    attempt: int
    max_attempts: int
    sql_history: List[str]
    error_history: List[str]
    current_sql: Optional[str]
    execution_result: Optional[dict]
    execution_error: Optional[str]
    schema_probes: List[str]
    reasoning: List[str]
    final_sql: Optional[str]
    final_df: Optional[str]
    messages: Annotated[List[Any], "messages"]
    llm_judge_verdict: Optional[str]  # For v3: ACCEPT or RETRY
    llm_judge_confidence: Optional[str]  # For v3: HIGH, MEDIUM, or LOW
    llm_judge_reasoning: Optional[str]  # For v3: LLM judge explanation
    agent_trace: List[dict]  # Full trace of all LLM interactions
    token_usage_per_attempt: List[dict]  # Token usage for each attempt
    total_token_usage: dict  # Aggregated token usage


class DatabaseExecutor:
    """Handles database query execution for different database types."""

    def __init__(
        self, db_type: str, db_connection_info: dict, db_id: Optional[str] = None
    ):
        self.db_type = db_type
        self.db_connection_info = db_connection_info
        self.db_id = db_id
        self._pool = None
        self._normalized_conn_str = None
        self._connect_args = None

    async def initialize(self):
        """Initialize database connections if needed."""
        try:
            if self.db_type == "postgres":
                connection_string = os.getenv(
                    self.db_connection_info.get("connection_string_env_var")
                )
                schema_name = self.db_connection_info.get("schema_name")
                if connection_string:
                    self._pool = await asyncpg.create_pool(
                        dsn=connection_string, min_size=1, max_size=1
                    )
                    self._schema_name = schema_name
                else:
                    logger.warning(
                        "PostgreSQL connection string not found in environment variables"
                    )
            elif self.db_type == "mysql":
                connection_string = os.getenv(
                    self.db_connection_info.get("connection_string_env_var")
                )
                if connection_string:
                    self._normalized_conn_str, self._connect_args = (
                        normalize_mysql_connection_string(connection_string, self.db_id)
                    )
                else:
                    logger.warning(
                        "MySQL connection string not found in environment variables"
                    )
            elif self.db_type == "sqlite":
                db_folder = self.db_connection_info.get("db_folder")
                if db_folder and self.db_id:
                    db_filename = self.db_id + ".sqlite"
                    from text2sql_eval_toolkit.utils import BENCHMARKS_FILE

                    self._db_path = (
                        Path(BENCHMARKS_FILE).parent
                        / Path(db_folder)
                        / self.db_id
                        / db_filename
                    )
                    if not self._db_path.exists():
                        logger.warning(f"SQLite database not found at {self._db_path}")
                else:
                    logger.warning("SQLite db_folder or db_id not provided")
        except Exception as e:
            logger.error(f"Error initializing database connection: {e}")
            raise

    async def execute_query(self, sql: str) -> dict:
        """
        Execute a SQL query and return results or error.

        Returns:
            dict with keys:
                - success: bool
                - df: pandas DataFrame (if success)
                - error: str (if not success)
                - row_count: int (if success)
                - execution_time_ms: float (time taken to execute query)
        """
        execution_start = time.perf_counter()
        try:
            if self.db_type == "postgres":
                if not self._pool:
                    await self.initialize()
                async with self._pool.acquire() as conn:
                    await conn.execute(f"SET search_path TO {self._schema_name}")
                    rows = await conn.fetch(sql)
                    if rows:
                        columns = rows[0].keys()
                    else:
                        columns = []
                    data = [dict(row) for row in rows]
                    df = await asyncio.to_thread(pd.DataFrame, data, columns=columns)
                    execution_end = time.perf_counter()
                    execution_time_ms = (execution_end - execution_start) * 1000
                    return {
                        "success": True,
                        "df": df,
                        "row_count": len(df),
                        "error": None,
                        "execution_time_ms": round(execution_time_ms, 2),
                    }
            elif self.db_type == "mysql":
                if not self._normalized_conn_str:
                    await self.initialize()
                sql = quote_mysql_identifiers(sql)
                df = await run_sql_and_get_dataframe_mysql_async(
                    self._normalized_conn_str, self._connect_args, self.db_id, sql
                )
                execution_end = time.perf_counter()
                execution_time_ms = (execution_end - execution_start) * 1000
                return {
                    "success": True,
                    "df": df,
                    "row_count": len(df),
                    "error": None,
                    "execution_time_ms": round(execution_time_ms, 2),
                }
            elif self.db_type == "sqlite":
                if not hasattr(self, "_db_path"):
                    await self.initialize()
                df = await run_sqlite_query_with_timeout(self._db_path, sql, timeout=30)
                execution_end = time.perf_counter()
                execution_time_ms = (execution_end - execution_start) * 1000
                return {
                    "success": True,
                    "df": df,
                    "row_count": len(df),
                    "error": None,
                    "execution_time_ms": round(execution_time_ms, 2),
                }
            else:
                return {
                    "success": False,
                    "df": None,
                    "row_count": 0,
                    "error": f"Unsupported database type: {self.db_type}",
                    "execution_time_ms": None,
                }
        except Exception as e:
            error_msg = str(e)
            logger.debug(f"SQL execution error: {error_msg}")
            execution_end = time.perf_counter()
            execution_time_ms = (execution_end - execution_start) * 1000
            return {
                "success": False,
                "df": None,
                "row_count": 0,
                "error": error_msg,
                "execution_time_ms": round(execution_time_ms, 2),
            }

    async def probe_schema(self, query_type: str = "tables") -> dict:
        """
        Probe the database schema.

        Args:
            query_type: Type of probe - "tables", "columns", "sample"

        Returns:
            dict with schema information
        """
        try:
            if query_type == "tables":
                if self.db_type == "postgres":
                    sql = """
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = current_schema()
                    ORDER BY table_name;
                    """
                elif self.db_type == "mysql":
                    sql = "SHOW TABLES;"
                elif self.db_type == "sqlite":
                    sql = "SELECT name FROM sqlite_master WHERE type='table';"
                else:
                    return {
                        "success": False,
                        "error": f"Unsupported db_type: {self.db_type}",
                    }

                result = await self.execute_query(sql)
                if result["success"]:
                    return {"success": True, "tables": result["df"].to_dict("records")}
                return result
            elif query_type == "columns":
                # Get columns for all tables
                tables_result = await self.probe_schema("tables")
                if not tables_result["success"]:
                    return tables_result

                all_columns = {}
                for table_info in tables_result["tables"]:
                    # Extract table name based on database type
                    if isinstance(table_info, dict):
                        table_name = table_info.get("table_name") or table_info.get(
                            "name"
                        )
                        # MySQL SHOW TABLES returns dict with key like "Tables_in_database"
                        if not table_name:
                            for key in table_info.keys():
                                if key.startswith("Tables_in"):
                                    table_name = table_info[key]
                                    break
                    else:
                        table_name = str(table_info)

                    if not table_name:
                        continue

                    if self.db_type == "postgres":
                        sql = f"""
                        SELECT column_name, data_type, is_nullable
                        FROM information_schema.columns
                        WHERE table_schema = current_schema() AND table_name = '{table_name}'
                        ORDER BY ordinal_position;
                        """
                    elif self.db_type == "mysql":
                        sql = f"DESCRIBE `{table_name}`;"
                    elif self.db_type == "sqlite":
                        sql = f"PRAGMA table_info({table_name});"
                    else:
                        continue

                    result = await self.execute_query(sql)
                    if result["success"]:
                        all_columns[table_name] = result["df"].to_dict("records")

                return {"success": True, "columns": all_columns}
            else:
                return {"success": False, "error": f"Unknown query_type: {query_type}"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def close(self):
        """Close database connections."""
        if self._pool:
            await self._pool.close()


class AgenticSQLGenerationPipeline(BasePipeline):
    """
    Agentic pipeline for SQL generation with error recovery and schema probing.

    Supports multiple versions (v0-v5):
    - v0: Agent-aware prompts with basic retry logic (agentic-baseline0)
    - v1: Baseline-compatible prompts with retry logic (agentic-baseline1)
    - v2: Smart retry logic with error classification (agentic-baseline2)
    - v3: LLM judge validation for semantic correctness (agentic-baseline3)
    - v4: Truly agentic with LLM-controlled workflow (agentic-baseline4)
    - v5: Truly agentic with improved prompting and systematic reasoning (agentic-baseline5)
    """

    def __init__(
        self,
        max_attempts: int = 3,
        use_baseline_prompt: bool = False,
        version: str = "v1",
    ):
        """
        Initialize the agentic pipeline.

        Args:
            max_attempts: Maximum number of attempts to generate SQL
            use_baseline_prompt: If True, use baseline-compatible prompt for first attempt.
                                If False, use agent-aware prompt from the start.
            version: Pipeline version:
                - "v0": Agent-aware prompts (agentic-baseline0)
                - "v1": Baseline-compatible prompts (agentic-baseline1)
                - "v2": Smart retry logic with error classification (agentic-baseline2)
                - "v3": LLM judge validation (agentic-baseline3)
                - "v4": Truly agentic with LLM-controlled tools (agentic-baseline4)
                - "v5": Truly agentic with improved prompting (agentic-baseline5)
        """
        super().__init__()
        self.max_attempts = max_attempts
        self.use_baseline_prompt = use_baseline_prompt
        self.version = version

    def _create_llm_client(self, model_name: str, model_parameters: dict):
        """Create an LLM client based on model name."""
        if model_name.startswith("wxai:"):
            return WXAIClientChatAPI(model_name[5:], model_parameters)
        elif model_name.startswith("anthropic:"):
            return ClaudeClientChatAPI(model_name[10:], model_parameters)
        elif model_name.startswith("vllm:"):
            return VLLMClientChatAPI(model_name[5:], model_parameters)
        elif model_name.startswith("openai:"):
            return OpenAIClientChatAPI(model_name[7:], model_parameters)
        elif model_name.startswith("rits"):
            logger.info(f"Getting RITS model endpoint for {model_name}")
            model_id = model_name.split("/")[-1].replace(".", "-").lower()
            rits_api_key = os.environ.get("RITS_API_KEY")
            if rits_api_key is None:
                raise ValueError("Missing RITS_API_KEY environment variable")
            os.environ["VLLM_API_BASE"] = (
                f"https://inference-3scale-apicast-production.apps.rits.fmaas.res.ibm.com/{model_id}/v1"
            )
            return VLLMClientChatAPI(model_name[5:], model_parameters)
        else:
            raise NotImplementedError(f"Model {model_name} is not supported.")

    def _build_v4_system_prompt(self, state: AgentState) -> str:
        """
        Build system prompt for v4 (truly agentic) pipeline.

        This prompt instructs the LLM on how to use tools to solve the task.
        """
        schema_str = self._verbalize_schema(state["schema"])

        system_prompt = f"""You are an expert SQL agent tasked with converting natural language questions into SQL queries.

You have access to the following tools that let you interact with the database:

1. **probe_schema**(table_name: str, reason: str)
   - Query the database to get detailed schema information about a specific table
   - Use when you need to know exact column names, data types, or sample data
   
2. **generate_sql**(sql: str, reasoning: str)
   - Generate and execute a SQL query
   - The query will be automatically executed and you'll get results or errors
   
3. **analyze_error**(analysis: str, fixable: bool)
   - Analyze an error from a failed SQL query
   - Determine what went wrong and if it can be fixed
   
4. **submit_final_answer**(sql: str, confidence: "high"|"medium"|"low", explanation: str)
   - Submit your final SQL query
   - Use when you have a working query or have exhausted attempts

**Database Schema:**
{schema_str}

**Instructions:**
1. Think step-by-step using a ReAct (Reasoning + Acting) approach
2. You must respond with ONLY a JSON object in this exact format:
   {{
     "thought": "Your reasoning about what to do next",
     "action": "tool_name",
     "action_input": {{...tool parameters...}}
   }}

3. After each action, you'll receive the result and can choose your next action
4. You have {state["max_attempts"]} total attempts - use them wisely
5. Always start by generating SQL unless you're missing critical schema information

**Example Response Format:**
{{
  "thought": "I need to find the average salary by department. The schema shows a salary column in the employees table and a department_id. I'll write a GROUP BY query.",
  "action": "generate_sql",
  "action_input": {{
    "sql": "SELECT department_id, AVG(salary) FROM employees GROUP BY department_id",
    "reasoning": "Grouping by department and calculating average salary"
  }}
}}

Remember: Respond ONLY with valid JSON. No extra text before or after."""

        return system_prompt

    def _build_v5_system_prompt(self, state: AgentState) -> str:
        """
        Build system prompt for v5 (improved agentic) pipeline.

        V5 improvements:
        - Emphasizes multi-step reasoning
        - Mandates schema probing
        - Adds column selection validation
        - Discourages rushing
        - Includes process checklist
        """
        schema_str = self._verbalize_schema(state["schema"])

        system_prompt = f"""You are an expert SQL agent tasked with converting natural language questions into SQL queries.

⚠️ IMPORTANT: Take a systematic, multi-step approach. Submitting on your first try is rarely optimal - you have {state["max_attempts"]} attempts, use them to explore, validate, and refine!

**SYSTEMATIC PROCESS (Follow These Steps):**

1. **ANALYZE** - Understand exactly what the question is asking
   - What specific data is requested?
   - What is the expected format of the answer?
   - Are there any implicit requirements (e.g., "which month" means return ONLY month, not full date)?

2. **EXPLORE** - Use probe_schema to understand the database
   - ALWAYS probe relevant tables before generating SQL
   - Understand column formats (e.g., is Date stored as YYYYMM string or timestamp?)
   - Verify table/column names and data types
   - Check for sample data if unsure

3. **PLAN** - Think through your SQL logic
   - What tables do I need?
   - What joins are required?
   - What filters/aggregations?
   - **CRITICAL:** What columns should the SELECT return? (No extras!)

4. **GENERATE** - Create the SQL query
   - Write clean, correct SQL
   - Match the database dialect

5. **VALIDATE** - Before submitting, check:
   ☐ Does SELECT return ONLY what the question asks for?
   ☐ No extra columns (like intermediate calculations)?
   ☐ Correct granularity (e.g., month vs full date)?
   ☐ Proper aggregation level?
   ☐ Correct ordering if question asks for "first", "highest", "least", etc.?

6. **SUBMIT** - Only when confident

**Available Tools:**

1. **probe_schema**(table_name: str, reason: str)
   - Query database for detailed schema information about a table
   - **USE THIS FIRST** before generating SQL
   - Helps understand column formats, types, and sample data

2. **generate_sql**(sql: str, reasoning: str)
   - Generate and execute a SQL query
   - Will be automatically executed and you'll get results or errors

3. **analyze_error**(analysis: str, fixable: bool)
   - Analyze errors from failed SQL queries
   - Determine what went wrong and if it's fixable

4. **submit_final_answer**(sql: str, confidence: "high"|"medium"|"low", explanation: str)
   - Submit your final SQL query
   - Only use after validation or when attempts exhausted

**Database Schema (High-Level Overview):**
{schema_str}

**Column Selection Rules (CRITICAL):**
- If question asks "which month?", return ONLY the month column (not month + count/consumption)
- If question asks "who?", return ONLY the identifier (CustomerID, not CustomerID + aggregate)
- If question asks "how many?", return ONLY the count (not count + other columns)
- Match the question's specificity exactly
- Use SUBSTR, DATE functions as needed to extract specific parts (e.g., month from YYYYMM)

**Response Format:**
You MUST respond with ONLY a JSON object:
{{
  "thought": "Before taking action, I need to [what I'm trying to learn/accomplish]. This will help me [why this is the right step].",
  "action": "tool_name",
  "action_input": {{...tool parameters...}}
}}

**Example Good Flow:**
Step 1:
{{
  "thought": "The question asks about 'peak month for SME customers in 2013'. Before generating SQL, I need to understand how the Date column is formatted in the yearmonth table - is it YYYYMM string, full timestamp, or separate year/month columns? This will help me write the correct date extraction logic.",
  "action": "probe_schema",
  "action_input": {{
    "table_name": "yearmonth",
    "reason": "Need to understand Date column format to extract month correctly"
  }}
}}

Step 2 (after seeing Date is YYYYMM string):
{{
  "thought": "Now I know Date is stored as YYYYMM string like '201301'. The question asks 'what was the peak month' so I need to return ONLY the month (MM), not the full date or consumption amount. I'll use SUBSTR(Date, 5, 2) to extract just the month digits, filter for SME and 2013, group by month, order by total consumption DESC, and LIMIT 1.",
  "action": "generate_sql",
  "action_input": {{
    "sql": "SELECT SUBSTR(Date, 5, 2) AS month FROM yearmonth ym JOIN customers c ON ym.CustomerID = c.CustomerID WHERE c.Segment = 'SME' AND SUBSTR(Date, 1, 4) = '2013' GROUP BY SUBSTR(Date, 5, 2) ORDER BY SUM(ym.Consumption) DESC LIMIT 1",
    "reasoning": "Extract only month (MM) from YYYYMM date string, filter for SME in 2013, find month with highest total consumption. Returns only month as requested."
  }}
}}

Step 3 (after SQL executes successfully):
{{
  "thought": "The SQL executed successfully and returned the month. Let me verify: (1) Does it return only the month column? Yes. (2) Is the logic correct for finding the peak? Yes, I'm grouping by month and ordering by SUM(Consumption) DESC with LIMIT 1. (3) Are filters correct? Yes, SME segment and year 2013. I'm confident this is correct.",
  "action": "submit_final_answer",
  "action_input": {{
    "sql": "SELECT SUBSTR(Date, 5, 2) AS month FROM yearmonth ym JOIN customers c ON ym.CustomerID = c.CustomerID WHERE c.Segment = 'SME' AND SUBSTR(Date, 1, 4) = '2013' GROUP BY SUBSTR(Date, 5, 2) ORDER BY SUM(ym.Consumption) DESC LIMIT 1",
    "confidence": "high",
    "explanation": "Returns only the month (MM) with highest SME consumption in 2013, correctly handling YYYYMM date format"
  }}
}}

⚠️ Remember: 
- ALWAYS probe schema first
- Think carefully about what columns to return
- Don't rush - validate before submitting
- Respond ONLY with valid JSON, no extra text"""

        return system_prompt

    def _get_v4_tools(self) -> List[dict]:
        """
        Define tools available to the LLM for v4 (truly agentic) pipeline.

        These tools give the LLM control over the workflow.
        """
        return [
            {
                "type": "function",
                "function": {
                    "name": "generate_sql",
                    "description": "Generate a SQL query to answer the user's question. Use this when you have enough information about the database schema and are ready to write SQL.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "sql": {
                                "type": "string",
                                "description": "The SQL query to execute. Must be valid SQL for the target database.",
                            },
                            "reasoning": {
                                "type": "string",
                                "description": "Explain your reasoning for this SQL query and what it's trying to accomplish.",
                            },
                        },
                        "required": ["sql", "reasoning"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "probe_schema",
                    "description": "Query the database to get detailed schema information about a specific table. Use this when you need to know the exact columns, data types, or sample data from a table.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "table_name": {
                                "type": "string",
                                "description": "The name of the table to probe for more information.",
                            },
                            "reason": {
                                "type": "string",
                                "description": "Why you need to probe this table (e.g., 'Need to know exact column names', 'Not sure about data types').",
                            },
                        },
                        "required": ["table_name", "reason"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "analyze_error",
                    "description": "Analyze an error from a failed SQL query to understand what went wrong. Use this after a SQL execution fails to get insights on how to fix it.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "analysis": {
                                "type": "string",
                                "description": "Your analysis of what went wrong and what needs to be fixed.",
                            },
                            "fixable": {
                                "type": "boolean",
                                "description": "Whether you believe this error can be fixed with another attempt.",
                            },
                        },
                        "required": ["analysis", "fixable"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "submit_final_answer",
                    "description": "Submit your final SQL query and result. Use this when you're confident in your SQL and have a successful execution, or when you've exhausted attempts and want to submit your best effort.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "sql": {
                                "type": "string",
                                "description": "The final SQL query to submit.",
                            },
                            "confidence": {
                                "type": "string",
                                "enum": ["high", "medium", "low"],
                                "description": "Your confidence level in this answer.",
                            },
                            "explanation": {
                                "type": "string",
                                "description": "Brief explanation of your final answer.",
                            },
                        },
                        "required": ["sql", "confidence", "explanation"],
                    },
                },
            },
        ]

    def _build_v2_prompt(
        self, state: AgentState, error_classification: dict = None
    ) -> List[dict]:
        """
        Build improved v2 prompt with targeted fix instructions.

        For first attempt: Uses baseline prompt.
        For retries: Provides specific guidance based on error type.
        """
        messages = []

        db_type = state["db_type"]
        schema_text = self._verbalize_schema(state["schema"])

        # System message
        system_prompt = (
            "You are a SQL expert. Your task is to convert natural language questions "
            "into accurate SQL queries using the given database schema and instructions."
        )
        messages.append({"role": "system", "content": system_prompt})

        if state["attempt"] == 1:
            # FIRST ATTEMPT: Exact baseline prompt
            user_content = (
                f"Your task is to convert a natural language question into an accurate SQL query "
                f"using the given {db_type} database schema.\n\n"
                f"**Question:**:\n{state['question']}\n\n"
                f"**Database Engine / Dialect:**:\n{db_type}\n\n"
                f"**Schema:**\n{schema_text}\n\n"
                "**Instructions:**\n"
                "- Only use columns listed in the schema.\n"
                "- Do not use any other columns or tables not mentioned in the schema.\n"
                "- Ensure the SQL query is valid and executable.\n"
                "- Use proper SQL syntax and conventions.\n"
                "- Generate a complete SQL query that answers the question.\n"
                f"- Use the correct SQL dialect for the database, i.e., {db_type}.\n"
                "- Do not include any explanations or comments in the SQL output.\n"
                "- Your output must start with ```sql and end with ```.\n\n"
                f"Question: {state['question']}"
            )
        else:
            # RETRY: Targeted fix based on error classification
            user_content = (
                f"**Question:** {state['question']}\n\n"
                f"**Database Type:** {db_type}\n\n"
                f"**Schema:**\n{schema_text}\n\n"
            )

            # Add previous SQL for reference
            if state.get("sql_history") and len(state["sql_history"]) > 0:
                user_content += f"\n**Previous SQL (failed):**\n```sql\n{state['sql_history'][-1]}\n```\n\n"

            # Add error
            if state.get("error_history"):
                user_content += f"**Error:**\n{state['error_history'][-1]}\n\n"

            # Add targeted fix instructions based on error category
            if error_classification:
                category = error_classification.get("category", "unknown")

                if category == "column_error":
                    user_content += (
                        "**Fix Instructions:**\n"
                        "- The column name is incorrect or doesn't exist\n"
                        "- Check the schema carefully for the correct column name\n"
                        "- Look for similar column names or aliases\n"
                        "- Ensure you're using the exact column name from the schema\n\n"
                    )
                elif category == "table_error":
                    user_content += (
                        "**Fix Instructions:**\n"
                        "- The table name is incorrect or doesn't exist\n"
                        "- Check the schema carefully for the correct table name\n"
                        "- Ensure you're using the exact table name from the schema\n\n"
                    )
                elif category == "syntax_error":
                    user_content += (
                        "**Fix Instructions:**\n"
                        "- There is a SQL syntax error\n"
                        "- Review SQL syntax rules carefully\n"
                        "- Check for missing commas, parentheses, or keywords\n"
                        f"- Ensure the query follows {db_type} syntax\n\n"
                    )
                elif category == "ambiguous_reference":
                    user_content += (
                        "**Fix Instructions:**\n"
                        "- A column reference is ambiguous (appears in multiple tables)\n"
                        "- Use table aliases to qualify the column (e.g., t1.column_name)\n"
                        "- Ensure all columns in JOINs are properly qualified\n\n"
                    )
                elif category == "aggregation_error":
                    user_content += (
                        "**Fix Instructions:**\n"
                        "- There is an issue with GROUP BY or aggregate functions\n"
                        "- All non-aggregated columns must appear in GROUP BY\n"
                        "- Check if aggregate functions are used correctly\n\n"
                    )
                else:
                    user_content += (
                        "**Fix Instructions:**\n"
                        "- Analyze the error message carefully\n"
                        "- Make a minimal, targeted fix to the previous SQL\n"
                        "- Only change what's necessary to fix the error\n\n"
                    )

            user_content += (
                "**Instructions:**\n"
                "- Make a minimal fix to the previous SQL\n"
                "- Only change what's needed to fix the error\n"
                "- Keep the rest of the query structure the same\n"
                "- Only use columns and tables from the schema\n"
                f"- Use {db_type} syntax\n"
                "- Your output must start with ```sql and end with ```.\n"
            )

        messages.append({"role": "user", "content": user_content})
        return messages

    def _build_v3_validation_prompt(self, state: AgentState) -> List[dict]:
        """
        Build v3 LLM judge validation prompt.

        This prompt asks the LLM to validate if the generated SQL and resulting
        dataframe correctly and accurately answer the original question.
        """
        messages = []

        db_type = state["db_type"]

        schema_text = self._verbalize_schema(state["schema"])

        # System message
        system_prompt = (
            "You are an expert SQL validator and data analyst. Your task is to assess "
            "whether a generated SQL query and its results correctly and accurately answer "
            "a given natural language question."
        )
        messages.append({"role": "system", "content": system_prompt})

        # Get the current execution result
        result_df_json = state.get("execution_result", {}).get("df")
        row_count = state.get("execution_result", {}).get("row_count", 0)

        # Convert dataframe JSON to readable format
        df_preview = "No results"
        if result_df_json:
            try:
                df = pd.read_json(result_df_json, orient="split")
                # Show first 10 rows
                df_preview = df.head(10).to_string(index=False)
                if len(df) > 10:
                    df_preview += f"\n... ({len(df) - 10} more rows)"
            except:
                df_preview = (
                    f"({row_count} rows returned, but could not parse dataframe)"
                )

        # Build comprehensive validation prompt
        user_content = f"""You are validating a text-to-SQL system's output. Your task is to determine if the generated SQL query and its results correctly and accurately answer the user's question.

**Original Question:**
{state["question"]}

**Database Type:** {db_type}

**Database Schema:**
{schema_text}

**Generated SQL Query:**
```sql
{state["current_sql"]}
```

**Query Execution Results:**
{df_preview}

**Total Rows Returned:** {row_count}

**Your Task:**
Carefully analyze whether the SQL query and its results correctly and accurately answer the original question. Consider:

1. **Query Correctness:**
   - Does the SQL query target the right tables and columns?
   - Are the JOINs, WHERE clauses, and filters appropriate for the question?
   - Does the query logic match what the question is asking for?
   - Are aggregations (COUNT, SUM, AVG, etc.) used correctly if needed?
   - Is the GROUP BY clause correct if aggregation is used?

2. **Result Validation:**
   - Do the returned results make sense for the question?
   - Is the number of rows reasonable? (e.g., if asking for "the top 5", are there 5 or fewer rows?)
   - Are the column names in the result relevant to what was asked?
   - Do the data values look appropriate for the question?

3. **Completeness:**
   - Does the query return all the information requested in the question?
   - Are there any missing columns or filters that should be included?

4. **Common Issues to Check:**
   - Missing or incorrect filters (WHERE clauses)
   - Wrong aggregation level (GROUP BY issues)
   - Incorrect JOINs or missing tables
   - Wrong sorting (ORDER BY) or limits
   - Overly broad results (too many rows when specific answer expected)
   - Empty results when data should exist

**Response Format:**
Provide your assessment in the following format:

VERDICT: [ACCEPT or RETRY]

CONFIDENCE: [HIGH, MEDIUM, or LOW]

REASONING:
[Provide detailed reasoning for your decision. If RETRY, explain what seems wrong and what should be fixed.]

**Guidelines:**
- Use ACCEPT if the SQL and results correctly answer the question, even if the format could be improved.
- Use RETRY if there are clear errors, missing information, or the results don't match what was asked.
- Be strict but fair - minor formatting differences are acceptable, but logical errors require RETRY.
- If results are empty but the question suggests data should exist, consider RETRY.
- If you're uncertain, provide MEDIUM or LOW confidence and explain your concerns.
"""

        messages.append({"role": "user", "content": user_content})
        return messages

    async def _validate_with_llm_judge(self, state: AgentState, client) -> dict:
        """
        Use LLM as judge to validate if SQL and results are correct.

        Returns:
            dict with keys: verdict (ACCEPT/RETRY), confidence (HIGH/MEDIUM/LOW), reasoning (str)
        """
        messages = self._build_v3_validation_prompt(state)

        try:
            response = await asyncio.to_thread(client.generate_sql, messages)

            # Parse the response
            verdict = "RETRY"  # Default to retry if we can't parse
            confidence = "LOW"
            reasoning = response

            # Extract VERDICT
            if "VERDICT:" in response:
                verdict_line = [
                    line for line in response.split("\n") if "VERDICT:" in line
                ]
                if verdict_line:
                    verdict_text = verdict_line[0].split("VERDICT:")[1].strip().upper()
                    if "ACCEPT" in verdict_text:
                        verdict = "ACCEPT"
                    elif "RETRY" in verdict_text:
                        verdict = "RETRY"

            # Extract CONFIDENCE
            if "CONFIDENCE:" in response:
                conf_line = [
                    line for line in response.split("\n") if "CONFIDENCE:" in line
                ]
                if conf_line:
                    conf_text = conf_line[0].split("CONFIDENCE:")[1].strip().upper()
                    if "HIGH" in conf_text:
                        confidence = "HIGH"
                    elif "MEDIUM" in conf_text:
                        confidence = "MEDIUM"
                    elif "LOW" in conf_text:
                        confidence = "LOW"

            # Extract REASONING
            if "REASONING:" in response:
                reasoning_start = response.find("REASONING:")
                reasoning = response[reasoning_start + len("REASONING:") :].strip()

            logger.debug(f"LLM Judge Verdict: {verdict} (Confidence: {confidence})")
            logger.debug(f"LLM Judge Reasoning: {reasoning[:200]}...")

            result = {
                "verdict": verdict,
                "confidence": confidence,
                "reasoning": reasoning,
                "messages": messages,  # Include messages for trace
                "response": response,  # Include full response for trace
            }

            return result

        except Exception as e:
            logger.error(f"Error in LLM judge validation: {e}")
            return {
                "verdict": "ACCEPT",  # Default to accepting on error
                "confidence": "LOW",
                "reasoning": f"Validation error: {str(e)}",
            }

    def _build_baseline_compatible_prompt(
        self, state: AgentState, is_retry: bool = False
    ) -> List[dict]:
        """
        Build a baseline-compatible prompt.

        For first attempt: Uses EXACT same prompt as baseline.
        For retries: Adds minimal error context while staying close to baseline format.
        """
        messages = []

        # Convert db_type for display
        db_type = state["db_type"]

        schema_text = self._verbalize_schema(state["schema"])

        # Use baseline system message
        system_prompt = (
            "You are a SQL expert. Your task is to convert natural language questions "
            "into accurate SQL queries using the given database schema and instructions."
        )
        messages.append({"role": "system", "content": system_prompt})

        if not is_retry:
            # FIRST ATTEMPT: Exact baseline prompt
            user_content = (
                f"Your task is to convert a natural language question into an accurate SQL query "
                f"using the given {db_type} database schema.\n\n"
                f"**Question:**:\n{state['question']}\n\n"
                f"**Database Engine / Dialect:**:\n{db_type}\n\n"
                f"**Schema:**\n{schema_text}\n\n"
                "**Instructions:**\n"
                "- Only use columns listed in the schema.\n"
                "- Do not use any other columns or tables not mentioned in the schema.\n"
                "- Ensure the SQL query is valid and executable.\n"
                "- Use proper SQL syntax and conventions.\n"
                "- Generate a complete SQL query that answers the question.\n"
                f"- Use the correct SQL dialect for the database, i.e., {db_type}.\n"
                "- Do not include any explanations or comments in the SQL output.\n"
                "- Your output must start with ```sql and end with ```.\n\n"
                f"Question: {state['question']}"
            )
        else:
            # RETRY: Add minimal error context
            user_content = (
                f"Your task is to convert a natural language question into an accurate SQL query "
                f"using the given {db_type} database schema.\n\n"
                f"**Question:**:\n{state['question']}\n\n"
                f"**Database Engine / Dialect:**:\n{db_type}\n\n"
                f"**Schema:**\n{schema_text}\n\n"
            )

            # Add error from previous attempt
            if state.get("error_history"):
                user_content += f"\n**Previous attempt failed with error:**\n{state['error_history'][-1]}\n\n"

            user_content += (
                "**Instructions:**\n"
                "- Only use columns listed in the schema.\n"
                "- Do not use any other columns or tables not mentioned in the schema.\n"
                "- Fix the error from the previous attempt.\n"
                "- Ensure the SQL query is valid and executable.\n"
                "- Use proper SQL syntax and conventions.\n"
                f"- Use the correct SQL dialect for the database, i.e., {db_type}.\n"
                "- Do not include any explanations or comments in the SQL output.\n"
                "- Your output must start with ```sql and end with ```.\n\n"
                f"Question: {state['question']}"
            )

        messages.append({"role": "user", "content": user_content})
        return messages

    def _build_agent_prompt(self, state: AgentState) -> List[dict]:
        """Build the prompt for the agent based on current state (original agent-aware version)."""
        messages = []

        system_prompt = """You are an expert SQL assistant that helps convert natural language questions into accurate SQL queries.

Your capabilities:
1. Generate SQL queries from natural language questions
2. Analyze SQL errors and fix them
3. Probe database schema when information is missing
4. Validate query results for correctness

You have access to:
- The database schema
- The ability to execute SQL queries
- The ability to probe the database for additional schema information
- Previous attempts and errors (if any)

When you encounter an error:
1. Analyze the error message carefully
2. Check if the error is due to missing schema information
3. If needed, probe the database for more information
4. Fix the SQL query based on the error and try again

When you generate SQL:
- Use only columns and tables mentioned in the schema
- Follow the correct SQL dialect for the database
- Ensure the query is syntactically correct
- Make sure the query answers the question accurately"""

        messages.append({"role": "system", "content": system_prompt})

        # Add reasoning history
        if state.get("reasoning"):
            messages.append(
                {
                    "role": "assistant",
                    "content": f"Previous reasoning:\n" + "\n".join(state["reasoning"]),
                }
            )

        # Add schema information
        schema_text = self._verbalize_schema(state["schema"])
        messages.append(
            {
                "role": "user",
                "content": f"""**Question:** {state["question"]}

**Database Type:** {state["db_type"]}

**Schema:**
{schema_text}

**Attempt:** {state["attempt"]} of {state["max_attempts"]}""",
            }
        )

        # Add error history if any
        if state.get("error_history"):
            messages.append(
                {
                    "role": "user",
                    "content": f"""**Previous Errors:**
{chr(10).join(f"Attempt {i + 1}: {err}" for i, err in enumerate(state["error_history"]))}""",
                }
            )

        # Add schema probes if any
        if state.get("schema_probes"):
            messages.append(
                {
                    "role": "user",
                    "content": f"""**Additional Schema Information from Probes:**
{chr(10).join(state["schema_probes"])}""",
                }
            )

        # Add instruction based on state
        if state["attempt"] == 1:
            instruction = "Generate a SQL query to answer the question."
        elif state.get("execution_error"):
            instruction = f"""The previous SQL query failed with error: {state["execution_error"]}

Analyze the error and generate a corrected SQL query. If you need more schema information, indicate what you need to probe."""
        else:
            instruction = (
                "Review the previous attempt and generate an improved SQL query."
            )

        messages.append({"role": "user", "content": instruction})

        return messages

    def _verbalize_schema(self, schema: dict) -> str:
        """Convert schema dict to text format."""
        lines = []
        db_desc = schema.get("description", "")
        if db_desc:
            lines.append(f"Database description: {db_desc}\n")

        tables = []
        if not isinstance(schema.get("tables"), list) and isinstance(
            schema.get("tables"), dict
        ):
            for table_name, table_obj in schema.get("tables").items():
                tables.append(table_obj)
        else:
            tables = schema.get("tables", [])

        for table in tables:
            table_name = table.get("name")
            table_desc = table.get("description", "")
            lines.append(f"Table: {table_name}")
            if table_desc:
                lines.append(f"  Description: {table_desc}")
            lines.append("  Columns:")
            for col in table.get("columns", []):
                col_name = col.get("name")
                col_type = col.get("type")
                col_desc = col.get("description", "")
                pk = " (Primary Key)" if col.get("primary_key", False) else ""
                samples = col.get("samples") or col.get("value_samples")
                sample_str = ""
                if samples and isinstance(samples, list):
                    shown = samples[:5]
                    shown_str = ", ".join(str(s) for s in shown)
                    sample_str = f" # Example values: {shown_str}"
                elif samples and isinstance(samples, (str, int, float)):
                    sample_str = f" # Example value: {samples}"
                if col_desc:
                    lines.append(
                        f"    - {col_name} ({col_type}){pk}: {col_desc}{sample_str}"
                    )
                else:
                    lines.append(f"    - {col_name} ({col_type}){pk}{sample_str}")
            lines.append("")

        return "\n".join(lines)

    async def _generate_sql_node(self, state: AgentState, client) -> AgentState:
        """Node: Generate SQL query."""
        logger.debug(f"Generating SQL (attempt {state['attempt']})")

        # Choose prompt strategy based on version and configuration
        if self.version == "v3":
            # V3: Use baseline prompt + LLM judge feedback on retries
            if state["attempt"] == 1:
                # First attempt: use baseline prompt
                messages = self._build_v2_prompt(state, error_classification=None)
            else:
                # Retry: incorporate LLM judge feedback if available
                llm_feedback = state.get("llm_judge_reasoning", "")
                error_classification = None
                if state.get("execution_error"):
                    error_classification = self._classify_error(
                        state["execution_error"]
                    )
                elif llm_feedback:
                    # Use LLM feedback as error context
                    state["error_history"].append(f"LLM Judge Feedback: {llm_feedback}")
                messages = self._build_v2_prompt(state, error_classification)
        elif self.version == "v2":
            # V2: Use improved prompting with error classification
            error_classification = None
            if state.get("execution_error"):
                error_classification = self._classify_error(state["execution_error"])
            messages = self._build_v2_prompt(state, error_classification)
        elif self.version == "v1":
            # V1: baseline-compatible prompts (baseline1)
            is_retry = state["attempt"] > 1
            messages = self._build_baseline_compatible_prompt(state, is_retry=is_retry)
        elif self.version == "v0" or self.use_baseline_prompt is False:
            # V0: agent-aware prompts (baseline0)
            messages = self._build_agent_prompt(state)
        else:
            # Fallback: use baseline-compatible if use_baseline_prompt is True
            is_retry = state["attempt"] > 1
            messages = self._build_baseline_compatible_prompt(state, is_retry=is_retry)

        # Generate SQL using the client
        try:
            sql, token_usage = await asyncio.to_thread(client.generate_sql, messages)
            sql = postprocess_sql(sql)

            state["current_sql"] = sql
            state["sql_history"].append(sql)
            state["reasoning"].append(
                f"Generated SQL (attempt {state['attempt']}): {sql}"
            )

            # Track token usage for this attempt
            if token_usage:
                state["token_usage_per_attempt"].append(token_usage)
                # Update total token usage
                for key in ["prompt_tokens", "completion_tokens", "total_tokens"]:
                    state["total_token_usage"][key] = (
                        state["total_token_usage"].get(key, 0) + token_usage.get(key, 0)
                    )

            # Save full trace of this interaction
            state["agent_trace"].append(
                {
                    "step": f"generate_sql_attempt_{state['attempt']}",
                    "messages": messages,
                    "response": sql,
                    "parsed_sql": sql,
                    "token_usage": token_usage,
                }
            )

            logger.debug(f"Generated SQL: {sql}")
        except Exception as e:
            logger.error(f"Error generating SQL: {e}")
            state["execution_error"] = f"Error generating SQL: {str(e)}"
            state["error_history"].append(state["execution_error"])

            # Save error in trace
            state["agent_trace"].append(
                {
                    "step": f"generate_sql_attempt_{state['attempt']}",
                    "messages": messages,
                    "error": str(e),
                }
            )

        return state

    async def _execute_sql_node(
        self, state: AgentState, db_executor: DatabaseExecutor
    ) -> AgentState:
        """Node: Execute SQL query."""
        if not state.get("current_sql"):
            state["execution_error"] = "No SQL to execute"
            return state

        logger.debug(f"Executing SQL: {state['current_sql']}")

        result = await db_executor.execute_query(state["current_sql"])

        if result["success"]:
            state["execution_result"] = {
                "success": True,
                "row_count": result["row_count"],
                "df": result["df"].to_json(orient="split")
                if result["df"] is not None
                else None,
                "execution_time_ms": result.get("execution_time_ms"),
            }
            state["execution_error"] = None
            logger.debug(
                f"SQL executed successfully, returned {result['row_count']} rows"
            )
        else:
            state["execution_result"] = None
            state["execution_error"] = result["error"]
            state["error_history"].append(result["error"])
            logger.debug(f"SQL execution failed: {result['error']}")

        return state

    async def _probe_schema_node(
        self, state: AgentState, db_executor: DatabaseExecutor
    ) -> AgentState:
        """Node: Probe database schema for additional information."""
        logger.debug("Probing database schema")

        # Probe for tables and columns
        columns_result = await db_executor.probe_schema("columns")

        if columns_result["success"]:
            probe_text = "Additional schema information from database:\n"
            for table_name, columns in columns_result["columns"].items():
                probe_text += f"\nTable: {table_name}\n"
                for col in columns:
                    if isinstance(col, dict):
                        col_name = (
                            col.get("column_name")
                            or col.get("Field")
                            or col.get("name")
                        )
                        col_type = (
                            col.get("data_type") or col.get("Type") or col.get("type")
                        )
                        probe_text += f"  - {col_name} ({col_type})\n"

            state["schema_probes"].append(probe_text)
            state["reasoning"].append(
                "Probed database for additional schema information"
            )

        return state

    async def _validate_result_node(self, state: AgentState, client=None) -> AgentState:
        """Node: Validate the execution result."""
        if (
            not state.get("execution_result")
            or not state["execution_result"]["success"]
        ):
            return state

        # Basic validation: check if result makes sense
        row_count = state["execution_result"]["row_count"]

        if row_count == 0:
            state["reasoning"].append(
                "Query executed but returned 0 rows. This might be correct or might indicate an issue."
            )
        else:
            state["reasoning"].append(
                f"Query executed successfully and returned {row_count} rows."
            )

        # V3: Use LLM judge to validate if SQL and results are correct
        if self.version == "v3" and client is not None:
            logger.debug("Running LLM judge validation (v3)")
            validation_result = await self._validate_with_llm_judge(state, client)

            state["llm_judge_verdict"] = validation_result["verdict"]
            state["llm_judge_confidence"] = validation_result["confidence"]
            state["llm_judge_reasoning"] = validation_result["reasoning"]

            state["reasoning"].append(
                f"LLM Judge: {validation_result['verdict']} (Confidence: {validation_result['confidence']})"
            )

            # Save LLM judge interaction to trace
            state["agent_trace"].append(
                {
                    "step": f"llm_judge_validation_attempt_{state['attempt']}",
                    "messages": validation_result.get("messages", []),
                    "response": validation_result.get("response", ""),
                    "verdict": validation_result["verdict"],
                    "confidence": validation_result["confidence"],
                    "reasoning": validation_result["reasoning"],
                }
            )

            # If LLM judge says ACCEPT, mark as final
            if validation_result["verdict"] == "ACCEPT":
                state["final_sql"] = state["current_sql"]
                if state["execution_result"].get("df"):
                    state["final_df"] = state["execution_result"]["df"]
            # If RETRY, clear final markers so we try again
            else:
                state["final_sql"] = None
                state["final_df"] = None
        else:
            # For non-v3 versions: Mark as final if successful execution
            state["final_sql"] = state["current_sql"]
            if state["execution_result"].get("df"):
                state["final_df"] = state["execution_result"]["df"]

        return state

    def _classify_error(self, error_message: str) -> dict:
        """
        Classify SQL error to determine if retry is worthwhile (v2 improvement).

        Returns dict with:
            - fixable: bool - whether this error is likely fixable
            - category: str - error category
            - confidence: float - confidence that retry will help
        """
        error_lower = error_message.lower()

        # High-confidence fixable errors (syntax, typos, schema issues)
        if any(
            term in error_lower
            for term in ["no such column", "unknown column", "column", "does not exist"]
        ):
            return {"fixable": True, "category": "column_error", "confidence": 0.8}

        if any(
            term in error_lower
            for term in ["no such table", "unknown table", "table", "relation"]
        ):
            return {"fixable": True, "category": "table_error", "confidence": 0.8}

        if any(term in error_lower for term in ["syntax error", "near", "unexpected"]):
            return {"fixable": True, "category": "syntax_error", "confidence": 0.6}

        if any(term in error_lower for term in ["ambiguous", "ambiguous column"]):
            return {
                "fixable": True,
                "category": "ambiguous_reference",
                "confidence": 0.7,
            }

        # Medium-confidence fixable errors
        if any(term in error_lower for term in ["type", "datatype", "cast", "convert"]):
            return {"fixable": True, "category": "type_error", "confidence": 0.5}

        if any(
            term in error_lower for term in ["group by", "aggregate", "must appear"]
        ):
            return {"fixable": True, "category": "aggregation_error", "confidence": 0.6}

        # Low-confidence or unfixable errors
        if any(
            term in error_lower for term in ["timeout", "timed out", "lock", "deadlock"]
        ):
            return {"fixable": False, "category": "timeout", "confidence": 0.1}

        if any(term in error_lower for term in ["permission", "denied", "access"]):
            return {"fixable": False, "category": "permission", "confidence": 0.0}

        # Unknown error - low confidence
        return {"fixable": True, "category": "unknown", "confidence": 0.3}

    def _should_retry(self, state: AgentState) -> str:
        """Determine next step based on state (improved for v2)."""
        # If we have a successful result, we're done
        if state.get("execution_result") and state["execution_result"].get("success"):
            return "end"

        # If we've exceeded max attempts, stop
        if state["attempt"] >= state["max_attempts"]:
            return "end"

        # V2: Smarter retry decisions based on error classification
        if self.version == "v2" and state.get("execution_error"):
            error_classification = self._classify_error(state["execution_error"])

            # Don't retry if error is not fixable
            if not error_classification["fixable"]:
                logger.debug(
                    f"Error not fixable ({error_classification['category']}), stopping"
                )
                return "end"

            # Don't retry if confidence is too low and we've tried once
            if error_classification["confidence"] < 0.4 and state["attempt"] >= 2:
                logger.debug(
                    f"Low confidence ({error_classification['confidence']}) and already tried, stopping"
                )
                return "end"

            # Decide whether to probe schema or just retry
            if (
                error_classification["category"] in ["column_error", "table_error"]
                and len(state["schema_probes"]) == 0
            ):
                return "probe_schema"

            return "generate_sql"

        # V1: Original simple logic
        if state.get("execution_error"):
            error_lower = state["execution_error"].lower()
            schema_related_errors = [
                "column",
                "table",
                "relation",
                "does not exist",
                "unknown",
                "invalid",
            ]
            if any(term in error_lower for term in schema_related_errors):
                return "probe_schema"
            return "generate_sql"

        return "end"

    async def _run_agent(
        self,
        question: str,
        schema: dict,
        db_type: str,
        db_connection_info: dict,
        db_id: Optional[str],
        client,
        max_attempts: int,
    ) -> dict:
        """Run the agentic pipeline for a single question."""

        # Initialize state
        state: AgentState = {
            "question": question,
            "schema": schema,
            "db_type": db_type,
            "db_id": db_id,
            "db_connection_info": db_connection_info,
            "attempt": 0,
            "max_attempts": max_attempts,
            "sql_history": [],
            "error_history": [],
            "current_sql": None,
            "execution_result": None,
            "execution_error": None,
            "schema_probes": [],
            "reasoning": [],
            "final_sql": None,
            "final_df": None,
            "messages": [],
            "llm_judge_verdict": None,
            "llm_judge_confidence": None,
            "llm_judge_reasoning": None,
            "agent_trace": [],  # Full trace of all LLM interactions
            "token_usage_per_attempt": [],  # Token usage for each attempt
            "total_token_usage": {},  # Aggregated token usage
        }

        # Initialize database executor
        db_executor = DatabaseExecutor(db_type, db_connection_info, db_id)
        await db_executor.initialize()

        try:
            # Main loop
            while state["attempt"] < max_attempts:
                state["attempt"] += 1
                logger.debug(f"Agent attempt {state['attempt']}/{max_attempts}")

                # Generate SQL
                state = await self._generate_sql_node(state, client)

                if not state.get("current_sql"):
                    # Failed to generate, try again if we have attempts left
                    continue

                # Execute SQL
                state = await self._execute_sql_node(state, db_executor)

                # Check if successful
                if state.get("execution_result") and state["execution_result"].get(
                    "success"
                ):
                    # Validate result
                    state = await self._validate_result_node(state, client=client)

                    # For v3: Check LLM judge verdict
                    if self.version == "v3":
                        if state.get("llm_judge_verdict") == "ACCEPT":
                            logger.debug("LLM judge accepted the result, stopping")
                            break
                        elif state.get("llm_judge_verdict") == "RETRY":
                            logger.debug(
                                f"LLM judge requested retry: {state.get('llm_judge_reasoning', '')[:100]}"
                            )
                            # Continue to next attempt
                            continue
                    else:
                        # For non-v3: successful execution means we're done
                        break

                # If error suggests schema issue, probe schema
                if state.get("execution_error"):
                    error_lower = state["execution_error"].lower()
                    schema_related = any(
                        term in error_lower
                        for term in [
                            "column",
                            "table",
                            "relation",
                            "does not exist",
                            "unknown",
                        ]
                    )
                    if schema_related and len(state["schema_probes"]) == 0:
                        state = await self._probe_schema_node(state, db_executor)

            # Finalize: use last SQL if we have one, even if it failed
            if not state.get("final_sql") and state.get("current_sql"):
                state["final_sql"] = state["current_sql"]

        finally:
            await db_executor.close()

        return {
            "predicted_sql": state.get("final_sql"),
            "predicted_df": state.get("final_df"),
            "sql_execution_error": state.get("execution_error")
            if not state.get("execution_result")
            or not state["execution_result"].get("success")
            else None,
            "execution_time_ms": state.get("execution_result", {}).get("execution_time_ms") if state.get("execution_result") else None,
            "attempts": state["attempt"],
            "reasoning": state.get("reasoning", []),
            "agent_trace": state.get("agent_trace", []),  # Full LLM interaction history
            "token_usage": state.get("total_token_usage"),  # Aggregated token usage
            "token_usage_per_attempt": state.get("token_usage_per_attempt", []),  # Per-attempt breakdown
        }

    async def _run_agent_v4(
        self,
        question: str,
        schema: dict,
        db_type: str,
        db_connection_info: dict,
        db_id: Optional[str],
        client,
        max_attempts: int,
    ) -> dict:
        """
        Run the truly agentic v4 pipeline where LLM controls the workflow.

        Uses ReAct pattern: LLM reasons and chooses actions via JSON responses.
        """
        # Initialize state
        state: AgentState = {
            "question": question,
            "schema": schema,
            "db_type": db_type,
            "db_id": db_id,
            "db_connection_info": db_connection_info,
            "attempt": 0,
            "max_attempts": max_attempts,
            "sql_history": [],
            "error_history": [],
            "current_sql": None,
            "execution_result": None,
            "execution_error": None,
            "schema_probes": [],
            "reasoning": [],
            "final_sql": None,
            "final_df": None,
            "messages": [],
            "llm_judge_verdict": None,
            "llm_judge_confidence": None,
            "llm_judge_reasoning": None,
            "agent_trace": [],
            "token_usage_per_attempt": [],  # Token usage for each attempt
            "total_token_usage": {},  # Aggregated token usage
        }

        # Initialize database executor
        db_executor = DatabaseExecutor(db_type, db_connection_info, db_id)
        await db_executor.initialize()

        # Build system prompt - use v5 if version is v5, otherwise v4
        if self.version == "v5":
            system_prompt = self._build_v5_system_prompt(state)
        else:
            system_prompt = self._build_v4_system_prompt(state)

        # Initialize conversation
        messages = [
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": f"Question: {question}\n\nPlease help me write the SQL query to answer this question.",
            },
        ]

        try:
            # Main agent loop - LLM decides what to do
            action_count = 0
            # v5 needs more actions for thorough exploration (4-7 actions typical)
            # v4 is more direct (1-2 actions typical)
            action_multiplier = 5 if self.version == "v5" else 3
            max_actions = (
                max_attempts * action_multiplier
            )  # Allow multiple actions per attempt

            while action_count < max_actions and state["attempt"] < max_attempts:
                action_count += 1
                logger.debug(f"V4 Agent action {action_count}/{max_actions}")

                # Get LLM's next action
                llm_response = None
                token_usage = None
                try:
                    if isinstance(client, WXAIClientChatAPI):
                        response = client.model.chat(messages=messages)
                        # Handle both response formats
                        if "choices" in response and len(response["choices"]) > 0:
                            message = response["choices"][0].get("message", {})
                            llm_response = message.get(
                                "content", message.get("text", "")
                            ).strip()
                        else:
                            raise ValueError(f"Unexpected response format: {response}")
                        # Extract token usage
                        usage = response.get("usage", {})
                        if usage:
                            token_usage = {
                                "prompt_tokens": usage.get("prompt_tokens", 0),
                                "completion_tokens": usage.get("completion_tokens", 0),
                                "total_tokens": usage.get("total_tokens", 0),
                            }
                    elif isinstance(client, ClaudeClientChatAPI):
                        # Claude has system separate
                        claude_messages = [
                            msg for msg in messages if msg["role"] != "system"
                        ]
                        claude_system = next(
                            (
                                msg["content"]
                                for msg in messages
                                if msg["role"] == "system"
                            ),
                            "",
                        )
                        response = client.client.messages.create(
                            model=client.model_name,
                            max_tokens=client.model_parameters.get("max_tokens", 1024),
                            system=claude_system,
                            messages=claude_messages,
                        )
                        llm_response = response.content[0].text.strip()
                        # Extract token usage from Claude
                        if hasattr(response, 'usage'):
                            token_usage = {
                                "prompt_tokens": getattr(response.usage, 'input_tokens', 0),
                                "completion_tokens": getattr(response.usage, 'output_tokens', 0),
                                "total_tokens": getattr(response.usage, 'input_tokens', 0) + getattr(response.usage, 'output_tokens', 0),
                            }
                    elif isinstance(client, VLLMClientChatAPI):
                        response = client._make_chat_request(messages)
                        llm_response = response["choices"][0]["message"][
                            "content"
                        ].strip()
                        # Extract token usage
                        usage = response.get("usage", {})
                        if usage:
                            token_usage = {
                                "prompt_tokens": usage.get("prompt_tokens", 0),
                                "completion_tokens": usage.get("completion_tokens", 0),
                                "total_tokens": usage.get("total_tokens", 0),
                            }
                    elif isinstance(client, OpenAIClientChatAPI):
                        # OpenAI client uses the standard chat completions API
                        response = client.client.chat.completions.create(
                            model=client.model_name,
                            messages=messages,
                            **client.model_parameters,
                        )
                        llm_response = response.choices[0].message.content.strip()
                        # Extract token usage from OpenAI
                        if hasattr(response, 'usage') and response.usage:
                            token_usage = {
                                "prompt_tokens": response.usage.prompt_tokens,
                                "completion_tokens": response.usage.completion_tokens,
                                "total_tokens": response.usage.total_tokens,
                            }
                    else:
                        raise ValueError(f"Unsupported client type: {type(client)}")

                    if not llm_response:
                        raise ValueError("Empty response from LLM")

                    # Track token usage for this attempt
                    if token_usage:
                        state["token_usage_per_attempt"].append(token_usage)
                        # Update total token usage
                        for key in ["prompt_tokens", "completion_tokens", "total_tokens"]:
                            state["total_token_usage"][key] = (
                                state["total_token_usage"].get(key, 0) + token_usage.get(key, 0)
                            )

                    # Record trace
                    state["agent_trace"].append(
                        {
                            "action_num": action_count,
                            "messages": messages.copy(),
                            "response": llm_response,
                            "token_usage": token_usage,
                        }
                    )

                    # Parse JSON response
                    llm_response_clean = llm_response
                    if "```json" in llm_response:
                        llm_response_clean = (
                            llm_response.split("```json")[1].split("```")[0].strip()
                        )
                    elif "```" in llm_response:
                        llm_response_clean = (
                            llm_response.split("```")[1].split("```")[0].strip()
                        )

                    action_data = json.loads(llm_response_clean)
                    thought = action_data.get("thought", "")
                    action = action_data.get("action", "")
                    action_input = action_data.get("action_input", {})

                    state["reasoning"].append(f"[Action {action_count}] {thought}")
                    logger.debug(
                        f"LLM chose action: {action} with input: {action_input}"
                    )

                except (json.JSONDecodeError, KeyError, IndexError, ValueError) as e:
                    response_preview = (
                        llm_response[:200] if llm_response else "No response received"
                    )
                    error_msg = f"Failed to parse LLM response: {e}\nResponse: {response_preview}"
                    logger.error(error_msg)

                    if llm_response:
                        messages.append({"role": "assistant", "content": llm_response})
                        messages.append(
                            {
                                "role": "user",
                                "content": f"Error: Your response must be valid JSON with 'thought', 'action', and 'action_input' fields. Please try again.",
                            }
                        )
                    else:
                        # If we couldn't get a response at all, log and break
                        logger.error(
                            "Failed to get response from LLM, stopping agent loop"
                        )
                        break
                    continue

                # Execute the chosen action
                if action == "generate_sql":
                    state["attempt"] += 1
                    sql = action_input.get("sql", "").strip()
                    reasoning = action_input.get("reasoning", "")

                    if not sql:
                        observation = "Error: No SQL provided"
                    else:
                        state["current_sql"] = sql
                        state["sql_history"].append(sql)

                        # Execute SQL
                        state = await self._execute_sql_node(state, db_executor)

                        if state.get("execution_result") and state[
                            "execution_result"
                        ].get("success"):
                            observation = f"✅ Success! Query executed successfully.\nRows returned: {state['execution_result'].get('row_count', 0)}\nResult preview: {str(state['execution_result'].get('preview', ''))[:200]}"
                            # Note: Don't set final_sql/final_df here - only set them when agent explicitly calls submit_final_answer
                            # This allows v5 to use generate_sql for exploration without triggering early stop
                        else:
                            observation = f"❌ SQL execution failed.\nError: {state.get('execution_error', 'Unknown error')}"
                            state["error_history"].append(
                                state.get("execution_error", "")
                            )

                    messages.append({"role": "assistant", "content": llm_response})
                    messages.append(
                        {
                            "role": "user",
                            "content": f"Observation: {observation}\n\nWhat's your next action?",
                        }
                    )

                elif action == "probe_schema":
                    table_name = action_input.get("table_name", "")
                    reason = action_input.get("reason", "")

                    if not table_name:
                        observation = "Error: No table name provided"
                    else:
                        # Probe schema
                        state = await self._probe_schema_node(state, db_executor)
                        schema_info = (
                            state.get("schema_probes", [])[-1]
                            if state.get("schema_probes")
                            else "No schema information retrieved"
                        )
                        observation = f"Schema information for '{table_name}':\n{schema_info[:500]}"

                    messages.append({"role": "assistant", "content": llm_response})
                    messages.append(
                        {
                            "role": "user",
                            "content": f"Observation: {observation}\n\nWhat's your next action?",
                        }
                    )

                elif action == "analyze_error":
                    analysis = action_input.get("analysis", "")
                    fixable = action_input.get("fixable", True)

                    state["reasoning"].append(
                        f"Error analysis: {analysis} (Fixable: {fixable})"
                    )

                    if not fixable:
                        observation = "Based on your analysis, this error is not fixable. Consider submitting your best attempt."
                    else:
                        observation = (
                            "Error analyzed. What's your next action to fix it?"
                        )

                    messages.append({"role": "assistant", "content": llm_response})
                    messages.append(
                        {
                            "role": "user",
                            "content": f"Observation: {observation}\n\nWhat's your next action?",
                        }
                    )

                elif action == "submit_final_answer":
                    sql = action_input.get("sql", "")
                    confidence = action_input.get("confidence", "medium")
                    explanation = action_input.get("explanation", "")

                    # Use the submitted SQL if provided, otherwise use current_sql
                    submitted_sql = sql if sql else state.get("current_sql")

                    # If SQL was submitted but not executed yet, execute it now
                    if submitted_sql and not state.get("final_df"):
                        logger.debug(
                            "SQL submitted in final_answer but not executed yet - executing now"
                        )
                        state["current_sql"] = submitted_sql
                        state["sql_history"].append(submitted_sql)
                        state["attempt"] += 1

                        # Execute the SQL
                        state = await self._execute_sql_node(state, db_executor)

                        # Check if execution was successful
                        if state.get("execution_result") and state[
                            "execution_result"
                        ].get("success"):
                            state["final_sql"] = submitted_sql
                            state["final_df"] = state.get("execution_result", {}).get(
                                "df"
                            )
                            logger.debug(
                                "SQL executed successfully in submit_final_answer"
                            )
                        else:
                            # Execution failed - still set final_sql but no dataframe
                            state["final_sql"] = submitted_sql
                            logger.warning(
                                f"SQL execution failed in submit_final_answer: {state.get('execution_error')}"
                            )
                    elif (
                        state.get("final_sql") and sql and sql != state.get("final_sql")
                    ):
                        # LLM submitted different SQL than what was executed
                        logger.warning(
                            f"LLM submitted different SQL in final_answer than what was executed. "
                            f"Using the executed SQL and dataframe."
                        )
                        # Keep the existing final_sql and final_df from execution
                    elif not state.get("final_sql"):
                        # No SQL executed at all - just set it (shouldn't happen but handle it)
                        state["final_sql"] = submitted_sql

                    state["reasoning"].append(
                        f"Final answer submitted with {confidence} confidence: {explanation}"
                    )
                    logger.debug(
                        f"Agent submitted final answer with {confidence} confidence"
                    )
                    break

                else:
                    observation = f"Unknown action: {action}. Please use one of: generate_sql, probe_schema, analyze_error, submit_final_answer"
                    messages.append({"role": "assistant", "content": llm_response})
                    messages.append(
                        {
                            "role": "user",
                            "content": f"Error: {observation}\n\nWhat's your next action?",
                        }
                    )

                # Check if we should stop
                if state.get("final_sql") and state.get("final_df"):
                    logger.debug("Agent has successful result, stopping")
                    break

                if state["attempt"] >= max_attempts:
                    logger.debug("Max attempts reached, stopping")
                    break

            # Finalize - if agent didn't call submit_final_answer, try to find best SQL
            if not state.get("final_sql"):
                # Look through SQL history for the best candidate (avoid exploratory queries)
                best_sql = None
                best_df = None

                def is_exploratory(sql: str) -> bool:
                    """Check if a SQL query is exploratory (not a real answer)."""
                    sql_lower = sql.lower().strip()

                    # Skip schema probing queries
                    if any(
                        pattern in sql_lower
                        for pattern in [
                            "information_schema",
                            "pragma_table_info",
                            "pragma table_info",
                            "show columns",
                            "describe ",
                            "desc ",
                            "pg_tables",
                            "pg_catalog",
                        ]
                    ):
                        return True

                    # Skip queries with DISTINCT + LIMIT (usually exploratory)
                    if "distinct" in sql_lower and "limit" in sql_lower:
                        return True

                    # Skip small SELECT * or SELECT [1-2 columns] with LIMIT (exploratory)
                    if "limit" in sql_lower:
                        # Count SELECT items (rough heuristic)
                        if sql_lower.count("select") == 1:  # Single SELECT
                            select_to_from = sql_lower.split("from")[0].split("select")[
                                1
                            ]
                            # If selecting * or very few columns
                            if "*" in select_to_from or select_to_from.count(",") <= 1:
                                return True

                    return False

                # Go through sql_history in reverse (most recent first)
                for sql in reversed(state.get("sql_history", [])):
                    if not is_exploratory(sql):
                        best_sql = sql
                        break

                # If we found a good SQL, use it
                if best_sql:
                    state["final_sql"] = best_sql
                    logger.debug(
                        f"Using best SQL from history as fallback: {best_sql[:100]}..."
                    )
                    # Try to find the corresponding dataframe from execution_result if available
                    if state.get("execution_result") and state["execution_result"].get(
                        "success"
                    ):
                        if state.get("current_sql") == best_sql:
                            # The best SQL is the last one executed
                            state["final_df"] = state["execution_result"].get("df")
                elif state.get("current_sql"):
                    # No good SQL found, use current as last resort (or don't return anything)
                    if not is_exploratory(state["current_sql"]):
                        state["final_sql"] = state["current_sql"]
                        logger.warning(
                            "No good SQL found in history, using current_sql as fallback"
                        )
                    else:
                        logger.warning(
                            "Agent exhausted attempts without finding valid SQL"
                        )

        finally:
            await db_executor.close()

        return {
            "predicted_sql": state.get("final_sql"),
            "predicted_df": state.get("final_df"),
            "sql_execution_error": state.get("execution_error")
            if not state.get("execution_result")
            or not state["execution_result"].get("success")
            else None,
            "execution_time_ms": state.get("execution_result", {}).get("execution_time_ms") if state.get("execution_result") else None,
            "attempts": state["attempt"],
            "reasoning": state.get("reasoning", []),
            "agent_trace": state.get("agent_trace", []),
            "token_usage": state.get("total_token_usage"),  # Aggregated token usage
            "token_usage_per_attempt": state.get("token_usage_per_attempt", []),  # Per-attempt breakdown
        }

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
        benchmark_id: str,
        db_connection_info: dict,
        max_attempts: int = 3,
        timeout=1200,
        force_rerun: bool = False,
        skip_inference_error_retries: bool = False,
    ):
        """Generate SQL for a single record using the agentic pipeline."""
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
                db_id = record.get("db_id", None)

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

                logger.debug(f"Starting agentic inference for record #{idx}")

                # Track inference time
                inference_start = time.perf_counter()

                # Run agentic pipeline - use v4/v5 for truly agentic, otherwise use standard
                if self.version in ["v4", "v5"]:
                    logger.debug(
                        f"Using {self.version} (truly agentic) pipeline with LLM-controlled workflow"
                    )
                    result = await asyncio.wait_for(
                        self._run_agent_v4(
                            utterance,
                            db_schema,
                            db_type,
                            db_connection_info,
                            db_id,
                            client,
                            max_attempts,
                        ),
                        timeout=timeout,
                    )
                else:
                    result = await asyncio.wait_for(
                        self._run_agent(
                            utterance,
                            db_schema,
                            db_type,
                            db_connection_info,
                            db_id,
                            client,
                            max_attempts,
                        ),
                        timeout=timeout,
                    )

                inference_end = time.perf_counter()
                inference_time_ms = (inference_end - inference_start) * 1000

                logger.debug(f"Finished agentic inference for record #{idx}")

                if existing:
                    existing["predictions"][pipeline_id] = {
                        "predicted_sql": result["predicted_sql"],
                        "predicted_df": result.get("predicted_df"),
                        "sql_execution_error": result.get("sql_execution_error"),
                        "model_name": model_name,
                        "model_parameters": model_parameters,
                        "agent_attempts": result["attempts"],
                        "agent_reasoning": result.get("reasoning", []),
                        "inference_time_ms": round(inference_time_ms, 2),
                        "execution_time_ms": result.get("execution_time_ms"),
                        "agent_trace": result.get(
                            "agent_trace", []
                        ),  # Full LLM interaction history
                        "token_usage": result.get("token_usage"),  # Aggregated token usage
                        "token_usage_per_attempt": result.get("token_usage_per_attempt", []),  # Per-attempt breakdown
                    }
                else:
                    record["predictions"] = {
                        pipeline_id: {
                            "predicted_sql": result["predicted_sql"],
                            "predicted_df": result.get("predicted_df"),
                            "sql_execution_error": result.get("sql_execution_error"),
                            "model_name": model_name,
                            "model_parameters": model_parameters,
                            "agent_attempts": result["attempts"],
                            "agent_reasoning": result.get("reasoning", []),
                            "inference_time_ms": round(inference_time_ms, 2),
                            "execution_time_ms": result.get("execution_time_ms"),
                            "agent_trace": result.get(
                                "agent_trace", []
                            ),  # Full LLM interaction history
                            "token_usage": result.get("token_usage"),  # Aggregated token usage
                            "token_usage_per_attempt": result.get("token_usage_per_attempt", []),  # Per-attempt breakdown
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
                    "model_name": model_name,
                    "model_parameters": model_parameters,
                    "inference_error": str(e),
                    "raw_response": getattr(e, 'response', None),  # Capture raw response if available
                }
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
        max_attempts: int = 3,
        force_rerun: bool = False,
        skip_inference_error_retries: bool = False,
    ):
        """Run the agentic pipeline for a benchmark."""
        # Follow the same naming convention as baseline: model_name + method descriptor
        # Naming: agentic-baseline0 (original), agentic-baseline1, agentic-baseline2, agentic-baseline3, agentic-baseline4 (truly agentic), agentic-baseline5 (improved agentic)
        if self.version == "v5":
            pipeline_id = f"{model_name}-agentic-baseline5-{max_attempts}attempts"
        elif self.version == "v4":
            pipeline_id = f"{model_name}-agentic-baseline4-{max_attempts}attempts"
        elif self.version == "v3":
            pipeline_id = f"{model_name}-agentic-baseline3-{max_attempts}attempts"
        elif self.version == "v2":
            pipeline_id = f"{model_name}-agentic-baseline2-{max_attempts}attempts"
        elif self.version == "v1":
            pipeline_id = f"{model_name}-agentic-baseline1-{max_attempts}attempts"
        elif self.version == "v0":
            pipeline_id = f"{model_name}-agentic-baseline0-{max_attempts}attempts"
        else:
            # Fallback for backward compatibility
            if self.use_baseline_prompt:
                pipeline_id = f"{model_name}-agentic-baseline1-{max_attempts}attempts"
            else:
                pipeline_id = f"{model_name}-agentic-baseline0-{max_attempts}attempts"
        logger.debug(
            f"🚀 Running agentic inference for benchmark {benchmark_id}, pipeline: {pipeline_id}"
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

        client = self._create_llm_client(model_name, model_parameters)
        db_connection_info = benchmark_info["db_engine"]

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
                    benchmark_id,
                    db_connection_info,
                    max_attempts,
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
            f"✅ Agentic inference completed for benchmark '{benchmark_id}', pipeline: {pipeline_id}."
        )
        logger.info(f"Predictions written to {benchmark_info['predictions_path']}")
