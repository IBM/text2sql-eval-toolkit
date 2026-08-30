# Metrics

The comparisons underneath the scores.

## Result sets

Column names are ignored throughout: a question asking for "the customers" has no
single correct column labelling, and marking a prediction wrong for choosing a
different one measures the wrong thing.

::: text2sql_eval_toolkit.compare_result_dfs
::: text2sql_eval_toolkit.compare_dfs_bird_eval_logic

## SQL equivalence

A ladder from cheapest and strictest to most tolerant: text comparison, AST
equality, clause-by-clause comparison, and comparison after optimisation. None
is semantic — two queries that always return the same rows may still compare
unequal.

::: text2sql_eval_toolkit.sql_exact_match
::: text2sql_eval_toolkit.sqlglot_parsed_queries_equivalent
::: text2sql_eval_toolkit.sqlparse_queries_equivalent
::: text2sql_eval_toolkit.sqlglot_optimized_equivalence

## Parsability

Separating "not SQL at all" from "SQL that is wrong" — different failure modes,
and worth distinguishing in error analysis.

::: text2sql_eval_toolkit.is_sqlglot_parsable
::: text2sql_eval_toolkit.is_sqlparse_parsable
