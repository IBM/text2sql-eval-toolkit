# SQL Query Profiling

Profiling annotates benchmark and evaluation records with **profile categories** that describe SQL structure and natural-language question intent. These tags are stored on each record as `meta.categories` and are used by the dashboard (Profile Compare view) and evaluation summaries to break down accuracy by query type.

Categories are assigned by [`profiling_tools.py`](../../src/text2sql_eval_toolkit/profiling/profiling_tools.py):

- **`analyze_sql_query()`** — parses ground-truth SQL with sqlglot and assigns SQL complexity and structural tags.
- **`analyze_question()`** — applies benchmark-agnostic regex patterns to the question text for intent and length tags.
- **`analyze_record()`** — merges SQL and question tags for a single record.

## Scripts

* **`run_profiling.py`** — profile a single JSON file (benchmark, predictions, or eval output).

```bash
python scripts/profiling/run_profiling.py <input_file.json> [--dialect postgres]
```

* **`profile_all_benchmarks.py`** — profile all configured benchmarks (and their predictions/eval files when present).

```bash
python scripts/profiling/profile_all_benchmarks.py [--test]
```

Use `--test` to profile the smaller test benchmark set instead of the full benchmarks.

## Output format

After profiling, each record includes (or updates) a `meta` object:

```json
{
  "meta": {
    "features": {
      "query_table_count": 2,
      "query_join_count": 1,
      "query_aggregate_count": 0
    },
    "categories": ["has_join", "multi_table_simple", "question_brief"]
  }
}
```

A record may carry **multiple** categories. Complexity buckets (`single_source_basic`, `multi_table_simple`, `single_source_advanced`) are mutually exclusive; `has_*` and `question_*` tags are additive overlays.

## Profile categories

### SQL complexity (one bucket per query)

| Category | Description |
|----------|-------------|
| `single_source_basic` | Single table; no joins, subqueries, or window functions. |
| `multi_table_simple` | Multiple tables with joins; no subqueries or window functions. |
| `single_source_advanced` | Single table with subqueries and/or window functions. |

### SQL structural features

| Category | Description |
|----------|-------------|
| `has_join` | Contains at least one JOIN. |
| `has_nested_query` | Contains a nested SELECT (or similar). |
| `has_aggregation` | Uses aggregate functions. |
| `has_group_by` | Uses GROUP BY. |
| `has_having` | Uses HAVING. |
| `has_sorting` | Uses ORDER BY. |
| `has_window_function` | Uses window functions. |
| `has_distinct` | Uses SELECT DISTINCT. |
| `has_limit` | Uses LIMIT (or TOP). |
| `has_set_operation` | Uses UNION, INTERSECT, or EXCEPT. |
| `has_case_expression` | Uses a CASE expression. |
| `has_cte` | Uses a WITH (CTE) clause. |
| `has_cast` | Uses CAST (or similar type conversion). |
| `has_like` | Uses LIKE pattern matching. |
| `has_between` | Uses BETWEEN. |
| `has_in_predicate` | Uses an IN predicate. |
| `has_null` | References NULL via IS NULL or IS NOT NULL. |
| `has_negation` | Uses SQL negation (`NOT IN`, `NOT EXISTS`, or `EXCEPT`). |

### Question length

| Category | Description |
|----------|-------------|
| `question_brief` | Short question (8 words or fewer). |
| `question_moderate` | Medium-length question (9–15 words). |
| `question_verbose` | Long question (more than 15 words). |

### Question intent (English, regex-based)

| Category | Description |
|----------|-------------|
| `question_counting` | Asks for a count (e.g. how many, number of). |
| `question_superlative` | Asks for a superlative (most, least, highest, etc.). |
| `question_comparison` | Compares values (more than, between, etc.). |
| `question_temporal` | References time or dates. |
| `question_aggregation_intent` | Mentions average, ratio, percentage, or similar. |
| `question_listing` | Asks to list or enumerate (which, what are, etc.). |
| `question_existence` | Asks whether something exists. |
| `question_negation` | Contains negation (not, without, never, etc.). |
| `question_grouping_intent` | Suggests per-group breakdown (for each, per, by). |

## Tests

Examples of expected tags per query and question are in:

- [`tests/test_sql_analysis.py`](../../tests/test_sql_analysis.py)
- [`tests/test_question_analysis.py`](../../tests/test_question_analysis.py)
