import pandas as pd

from text2sql_eval_toolkit.metrics.text2sql_utils import (
    compare_dfs_ignore_colnames_subset,
    compare_result_dfs,
)


def test_subset_numeric_normalization_ordered_rows():
    gold_df = pd.DataFrame({"SUBSTR(T2.Date, 5, 2)": [4]})
    pred_df = pd.DataFrame({"Month": [4], "TotalConsumption": [126047776.92]})
    gold_sql = (
        "SELECT SUBSTR(T2.Date, 5, 2) "
        "FROM customers AS T1 "
        "INNER JOIN yearmonth AS T2 ON T1.CustomerID = T2.CustomerID "
        "WHERE SUBSTR(T2.Date, 1, 4) = '2013' "
        "AND T1.Segment = 'SME' "
        "GROUP BY SUBSTR(T2.Date, 5, 2) "
        "ORDER BY SUM(T2.Consumption) DESC LIMIT 1"
    )

    match, non_empty_match, subset_match = compare_result_dfs(gold_df, pred_df, gold_sql)

    assert match == 0
    assert non_empty_match == 0
    assert subset_match == 1


def test_subset_comparison_treats_4_and_4_point_0_as_equal():
    gold_df = pd.DataFrame({"col": [4]})
    pred_df = pd.DataFrame({"col": [4.0], "extra": [10]})

    assert compare_dfs_ignore_colnames_subset(
        gold_df,
        pred_df,
        ignore_row_order=False,
    )
