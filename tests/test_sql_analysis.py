#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from typing import Set
from text2sql_eval_toolkit.profiling.profiling_tools import analyze_sql_query

# List of queries and their expected tags
queries_with_expected_features = [
    ("SELECT col1 ,col2,col3 FROM db1.table1", {"single_source_basic"}),
    ("SELECT * FROM Products ORDER BY Price;", {"has_sorting", "single_source_basic"}),
    (
        "SELECT * FROM Customers ORDER BY Country ASC, CustomerName DESC;",
        {"has_sorting", "single_source_basic"},
    ),
    (
        "SELECT COUNT(CustomerID), Country FROM Customers GROUP BY Country ORDER BY COUNT(CustomerID) DESC;",
        {"has_aggregation", "has_sorting", "single_source_basic"},
    ),
    (
        "SELECT sales_person_id, name FROM sales_performance ORDER BY 2;",
        {"has_sorting", "single_source_basic"},
    ),
    (
        "SELECT * FROM sales_performance ORDER BY total_sales_value DESC;",
        {"has_sorting", "single_source_basic"},
    ),
    (
        "SELECT * FROM sales_performance ORDER BY CONCAT(territory, name);",
        {"has_sorting", "single_source_basic"},
    ),
    (
        "SELECT CAST(date AS DATE), SUM(amount) AS total_amount FROM table WHERE date BETWEEN '2019-01-01 00:00:00' AND '2019-12-31 00:00:00' GROUP BY CAST(date AS DATE)",
        {"has_aggregation", "single_source_basic"},
    ),
    (
        "SELECT department_name, ROUND(AVG(salary), 0) avg_salary FROM employees INNER JOIN departments USING (department_id) GROUP BY department_name ORDER BY department_name;",
        {"has_aggregation", "has_sorting", "has_join", "multi_table_simple"},
    ),
    (
        "SELECT department_name, MIN(salary) min_salary FROM employees INNER JOIN departments USING (department_id) GROUP BY department_name ORDER BY department_name;",
        {"has_aggregation", "has_sorting", "has_join", "multi_table_simple"},
    ),
    (
        "SELECT department_name, MAX(salary) highest_salary FROM employees INNER JOIN departments USING (department_id) GROUP BY department_name ORDER BY department_name;",
        {"has_aggregation", "has_sorting", "has_join", "multi_table_simple"},
    ),
    (
        "SELECT department_name, COUNT(*) headcount FROM employees INNER JOIN departments USING (department_id) GROUP BY department_name ORDER BY department_name;",
        {"has_aggregation", "has_sorting", "has_join", "multi_table_simple"},
    ),
    (
        "SELECT department_id, SUM(salary) FROM employees GROUP BY department_id;",
        {"has_aggregation", "single_source_basic"},
    ),
    (
        "select min(dec) as min_dec, max(dec) as max_dec, avg(dec) as avg_dec from photoObj where run = 1458",
        {"has_aggregation", "single_source_basic"},
    ),
    (
        "select count(z) as num_redshift from specObj where z BETWEEN 0.5 AND 1",
        {"has_aggregation", "single_source_basic"},
    ),
    (
        "SELECT COUNT(*) AS TotalEmployees FROM Employee;",
        {"has_aggregation", "single_source_basic"},
    ),
    (
        "SELECT S_ID FROM STUDENT_COURSE WHERE C_ID IN (SELECT C_ID FROM COURSE WHERE C_NAME IN ('DSA', 'DBMS'));",
        {"has_nested_query"},
    ),
    (
        "SELECT S_NAME FROM STUDENT WHERE S_AGE > ALL (SELECT S_AGE FROM STUDENT WHERE S_ADDRESS = 'DELHI');",
        {"has_nested_query"},
    ),
    (
        "SELECT S_NAME FROM STUDENT WHERE S_ID IN (SELECT S_ID FROM STUDENT_COURSE WHERE C_ID IN (SELECT C_ID FROM COURSE WHERE C_NAME IN ('DSA', 'DBMS')));",
        {"has_nested_query"},
    ),
    (
        "SELECT C_ID FROM COURSE WHERE C_NAME IN ('DSA', 'DBMS');",
        {"single_source_basic"},
    ),
    (
        "SELECT S_NAME FROM STUDENT S WHERE EXISTS ( SELECT 1 FROM STUDENT_COURSE SC WHERE S.S_ID = SC.S_ID AND SC.C_ID = 'C1' );",
        {"has_nested_query"},
    ),
    (
        "SELECT S_NAME FROM STUDENT WHERE S_ID IN ( SELECT S_ID FROM STUDENT_COURSE WHERE C_ID IN ( SELECT C_ID FROM COURSE WHERE C_NAME IN ('DSA', 'DBMS') ) );",
        {"has_nested_query"},
    ),
    (
        "SELECT S_NAME FROM STUDENT WHERE S_ID NOT IN ( SELECT S_ID FROM STUDENT_COURSE WHERE C_ID NOT IN ( SELECT C_ID FROM COURSE WHERE C_NAME IN ('DSA', 'DBMS') ) );",
        {"has_nested_query"},
    ),
    (
        "SELECT * FROM students WHERE class_id = ( SELECT id FROM classes WHERE number_of_students = ( SELECT MAX(number_of_students) FROM classes));",
        {"has_nested_query", "has_aggregation"},
    ),
    (
        "SELECT subject, MAX(salary_by_subject.avg_salary) AS max_salary FROM ( SELECT subject, AVG(monthly_salary) AS avg_salary FROM teachers GROUP BY subject) salary_by_subject;",
        {"has_nested_query", "has_aggregation", "single_source_advanced"},
    ),
    (
        "SELECT Name, Department, Salary, RANK() OVER(PARTITION BY Department ORDER BY Salary DESC) AS emp_rank FROM employee;",
        {"has_window_function", "has_sorting", "single_source_advanced"},
    ),
    (
        "SELECT Name, Department, Salary, DENSE_RANK() OVER(PARTITION BY Department ORDER BY Salary DESC) AS emp_dense_rank FROM employee;",
        {"has_window_function", "has_sorting", "single_source_advanced"},
    ),
    (
        "SELECT Name, Department, Salary, ROW_NUMBER() OVER(PARTITION BY Department ORDER BY Salary DESC) AS emp_row_no FROM employee;",
        {"has_window_function", "has_sorting", "single_source_advanced"},
    ),
    (
        "SELECT Name, Age, Department, Salary, AVG(Salary) OVER( PARTITION BY Department) AS Avg_Salary FROM employee",
        {"has_aggregation", "has_window_function", "single_source_advanced"},
    ),
    (
        "SELECT Shippers.ShipperName, COUNT(Orders.OrderID) AS NumberOfOrders FROM Orders LEFT JOIN Shippers ON Orders.ShipperID = Shippers.ShipperID GROUP BY ShipperName;",
        {"has_aggregation", "has_join", "multi_table_simple"},
    ),
    (
        "SELECT b.id, b.title, a.first_name, a.last_name FROM books b INNER JOIN authors a ON b.author_id = a.id ORDER BY b.id;",
        {"has_join", "has_sorting", "multi_table_simple"},
    ),
    (
        "SELECT b.id, b.title, b.type, t.last_name AS translator FROM books b JOIN translators t ON b.translator_id = t.id ORDER BY b.id;",
        {"has_join", "has_sorting", "multi_table_simple"},
    ),
    (
        "SELECT b.id, b.title, b.type, a.last_name AS author, t.last_name AS translator FROM books b LEFT JOIN authors a ON b.author_id = a.id LEFT JOIN translators t ON b.translator_id = t.id ORDER BY b.id;",
        {"has_join", "has_sorting", "multi_table_simple"},
    ),
    (
        "SELECT b.id, b.title, e.last_name AS editor FROM books b RIGHT JOIN editors e ON b.editor_id = e.id ORDER BY b.id;",
        {"has_join", "has_sorting", "multi_table_simple"},
    ),
    (
        "SELECT b.id, b.title, e.last_name AS editor FROM books b FULL JOIN editors e ON b.editor_id = e.id ORDER BY b.id;",
        {"has_join", "has_sorting", "multi_table_simple"},
    ),
    (
        "SELECT b.id, b.title, a.last_name AS author, e.last_name AS editor, t.last_name AS translator FROM books b FULL JOIN authors a ON b.author_id = a.id FULL JOIN editors e ON b.editor_id = e.id FULL JOIN translators t ON b.translator_id = t.id ORDER BY b.id;",
        {"has_join", "has_sorting", "multi_table_simple"},
    ),
]


@pytest.mark.parametrize("query,expected_tags", queries_with_expected_features)
def test_analyze_sql_query_tags(query: str, expected_tags: Set[str]):
    result = analyze_sql_query(query)
    assert "features" in result
    assert isinstance(result["features"], dict)
    assert "categories" in result
    assert isinstance(result["categories"], list)
    assert expected_tags == set(result["categories"])
