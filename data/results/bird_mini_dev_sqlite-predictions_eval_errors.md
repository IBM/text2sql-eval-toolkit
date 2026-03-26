# ❌ Failed Examples by Pipeline

## 🔍 Pipeline/Model ID: `gemini:gemini-3-flash-preview-greedy-zero-shot-chatapi`

20 failed predictions shown (out of 188)

### ❓ Failed Question #1 (of 20 examples) - Question ID: `1498`


**Question**: What is the highest monthly consumption in the year 2012?


### ✅ Ground Truth SQL(s)

```sql
SELECT SUM(Consumption) FROM yearmonth WHERE SUBSTR(Date, 1, 4) = '2012' GROUP BY SUBSTR(Date, 5, 2) ORDER BY SUM(Consumption) DESC LIMIT 1
```

### ❌ Predicted SQL

```sql
SELECT
  MAX(Consumption)
FROM yearmonth
WHERE
  Date LIKE '2012%'
```

### 📊 Evaluation Metrics

|   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |   execution_time_ms |
|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|--------------------:|
|                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          0 |            1088 |                  28 |           8754 |             43433.2 |               75.42 |

### 📘 Ground Truth Result(s)

**Result 1:**

|   SUM(Consumption) |
|-------------------:|
|        5.17872e+07 |

### 📕 Predicted Result

|   MAX(Consumption) |
|-------------------:|
|             445280 |

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given sqlite database schema.

**Question:**:
What is the highest monthly consumption in the year 2012?

**Database Engine / Dialect:**:
sqlite

**Schema:**
Table: customers
  Columns:
    - CustomerID (INTEGER) (Primary Key) # Example values: 3, 5, 6, 7, 9
    - Segment (TEXT) # Example values: SME, LAM, KAM
    - Currency (TEXT) # Example values: EUR, CZK

Table: gasstations
  Columns:
    - GasStationID (INTEGER) (Primary Key) # Example values: 44, 45, 46, 47, 48
    - ChainID (INTEGER) # Example values: 13, 6, 23, 33, 4
    - Country (TEXT) # Example values: CZE, SVK
    - Segment (TEXT) # Example values: Value for money, Premium, Other, Noname, Discount

Table: products
  Columns:
    - ProductID (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Description (TEXT) # Example values: Rucní zadání, Nafta, Special, Super, Natural

Table: transactions_1k
  Columns:
    - TransactionID (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Date (DATE) # Example values: 2012-08-24, 2012-08-23, 2012-08-25, 2012-08-26
    - Time (TEXT) # Example values: 09:41:00, 10:03:00, 13:53:00, 08:49:00, 08:53:00
    - CustomerID (INTEGER) # Example values: 31543, 46707, 7654, 17373, 7881
    - CardID (INTEGER) # Example values: 486621, 550134, 684220, 536109, 99745
    - GasStationID (INTEGER) # Example values: 3704, 656, 741, 1152, 636
    - ProductID (INTEGER) # Example values: 2, 23, 5, 11, 7
    - Amount (INTEGER) # Example values: 28, 18, 1, 5, 4
    - Price (REAL) # Example values: 672.64, 430.72, 121.99, 120.74, 645.05

Table: sqlite_sequence
  Columns:
    - name (TEXT) # Example values: transactions_1k
    - seq (TEXT) # Example values: 1000

Table: yearmonth
  Columns:
    - CustomerID (INTEGER) (Primary Key) # Example values: 39, 63, 172, 603, 1492
    - Date (TEXT) (Primary Key) # Example values: 201112, 201201, 201202, 201203, 201204
    - Consumption (REAL) # Example values: 528.3, 1598.28, 1931.36, 1497.14, 51.06


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., sqlite.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

***Hints***
- The first 4 strings of the Date values in the yearmonth table can represent year
- The 5th and 6th string of the date can refer to month.

Question: What is the highest monthly consumption in the year 2012?</pre>

---

### ❓ Failed Question #2 (of 20 examples) - Question ID: `1482`


**Question**: Which of the three segments—SME, LAM and KAM—has the biggest and lowest percentage increases in consumption paid in EUR between 2012 and 2013?


### ✅ Ground Truth SQL(s)

```sql
SELECT CAST((SUM(IIF(T1.Segment = 'SME' AND T2.Date LIKE '2013%', T2.Consumption, 0)) - SUM(IIF(T1.Segment = 'SME' AND T2.Date LIKE '2012%', T2.Consumption, 0))) AS FLOAT) * 100 / SUM(IIF(T1.Segment = 'SME' AND T2.Date LIKE '2012%', T2.Consumption, 0)), CAST(SUM(IIF(T1.Segment = 'LAM' AND T2.Date LIKE '2013%', T2.Consumption, 0)) - SUM(IIF(T1.Segment = 'LAM' AND T2.Date LIKE '2012%', T2.Consumption, 0)) AS FLOAT) * 100 / SUM(IIF(T1.Segment = 'LAM' AND T2.Date LIKE '2012%', T2.Consumption, 0)) , CAST(SUM(IIF(T1.Segment = 'KAM' AND T2.Date LIKE '2013%', T2.Consumption, 0)) - SUM(IIF(T1.Segment = 'KAM' AND T2.Date LIKE '2012%', T2.Consumption, 0)) AS FLOAT) * 100 / SUM(IIF(T1.Segment = 'KAM' AND T2.Date LIKE '2012%', T2.Consumption, 0)) FROM customers AS T1 INNER JOIN yearmonth AS T2 ON T1.CustomerID = T2.CustomerID
```

### ❌ Predicted SQL

```sql
'EUR'
      AND c.Segment IN ('SME', 'LAM', 'KAM')
      AND SUBSTR(y.Date, 1, 4) IN ('2012', '2013')
    ```
    However, if I use the `SUM(CASE ...)` approach, having the `WHERE` filter for only 2012/2013 is better.

    Actually, let's look at the `SUM(CASE ...)` again. If a segment has consumption in 2011, it will be ignored by `CASE`, which is correct. If I filter in `WHERE`, it's just an optimization.

    Wait, if a segment has 2012 but NO 2013 data, `SUM(CASE 2013)` is 0.
    `(0 - 50) / 0` is `NULL`.
    If a segment has 2013 but NO 2012 data, `SUM(CASE 2012)` is 0.
    `(50 - 0) / 50` is `100%`.
    This seems logically sound.

    Final check on prompt: "biggest and lowest percentage increases in consumption paid in EUR".
    The phrase "consumption paid in EUR" is slightly ambiguous—does it mean `Consumption * Price`?
    Table `yearmonth` only has `Consumption`.
    Table `transactions_1
```

### 📊 Evaluation Metrics

| df_error_message                                                   |   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |
|:-------------------------------------------------------------------|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|
| Error running SQL: Error running query: near "'EUR'": syntax error |                    0 |                              0 |                                     0 |                          0 |                         0 |                     0 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          1 |            1161 |                 321 |           9181 |             39581.9 |

### 📘 Ground Truth Result(s)

**Result 1:**

|   CAST((SUM(IIF(T1.Segment = 'SME' AND T2.Date LIKE '2013%', T2.Consumption, 0)) - SUM(IIF(T1.Segment = 'SME' AND T2.Date LIKE '2012%', T2.Consumption, 0))) AS FLOAT) * 100 / SUM(IIF(T1.Segment = 'SME' AND T2.Date LIKE '2012%', T2.Consumption, 0)) |   CAST(SUM(IIF(T1.Segment = 'LAM' AND T2.Date LIKE '2013%', T2.Consumption, 0)) - SUM(IIF(T1.Segment = 'LAM' AND T2.Date LIKE '2012%', T2.Consumption, 0)) AS FLOAT) * 100 / SUM(IIF(T1.Segment = 'LAM' AND T2.Date LIKE '2012%', T2.Consumption, 0)) |   CAST(SUM(IIF(T1.Segment = 'KAM' AND T2.Date LIKE '2013%', T2.Consumption, 0)) - SUM(IIF(T1.Segment = 'KAM' AND T2.Date LIKE '2012%', T2.Consumption, 0)) AS FLOAT) * 100 / SUM(IIF(T1.Segment = 'KAM' AND T2.Date LIKE '2012%', T2.Consumption, 0)) |
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|
|                                                                                                                                                                                                                                                 545.402 |                                                                                                                                                                                                                                               681.582 |                                                                                                                                                                                                                                               708.112 |

### 📕 Predicted Result

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given sqlite database schema.

**Question:**:
Which of the three segments—SME, LAM and KAM—has the biggest and lowest percentage increases in consumption paid in EUR between 2012 and 2013?

**Database Engine / Dialect:**:
sqlite

**Schema:**
Table: customers
  Columns:
    - CustomerID (INTEGER) (Primary Key) # Example values: 3, 5, 6, 7, 9
    - Segment (TEXT) # Example values: SME, LAM, KAM
    - Currency (TEXT) # Example values: EUR, CZK

Table: gasstations
  Columns:
    - GasStationID (INTEGER) (Primary Key) # Example values: 44, 45, 46, 47, 48
    - ChainID (INTEGER) # Example values: 13, 6, 23, 33, 4
    - Country (TEXT) # Example values: CZE, SVK
    - Segment (TEXT) # Example values: Value for money, Premium, Other, Noname, Discount

Table: products
  Columns:
    - ProductID (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Description (TEXT) # Example values: Rucní zadání, Nafta, Special, Super, Natural

Table: transactions_1k
  Columns:
    - TransactionID (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Date (DATE) # Example values: 2012-08-24, 2012-08-23, 2012-08-25, 2012-08-26
    - Time (TEXT) # Example values: 09:41:00, 10:03:00, 13:53:00, 08:49:00, 08:53:00
    - CustomerID (INTEGER) # Example values: 31543, 46707, 7654, 17373, 7881
    - CardID (INTEGER) # Example values: 486621, 550134, 684220, 536109, 99745
    - GasStationID (INTEGER) # Example values: 3704, 656, 741, 1152, 636
    - ProductID (INTEGER) # Example values: 2, 23, 5, 11, 7
    - Amount (INTEGER) # Example values: 28, 18, 1, 5, 4
    - Price (REAL) # Example values: 672.64, 430.72, 121.99, 120.74, 645.05

Table: sqlite_sequence
  Columns:
    - name (TEXT) # Example values: transactions_1k
    - seq (TEXT) # Example values: 1000

Table: yearmonth
  Columns:
    - CustomerID (INTEGER) (Primary Key) # Example values: 39, 63, 172, 603, 1492
    - Date (TEXT) (Primary Key) # Example values: 201112, 201201, 201202, 201203, 201204
    - Consumption (REAL) # Example values: 528.3, 1598.28, 1931.36, 1497.14, 51.06


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., sqlite.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

***Hints***
- Increase or Decrease = consumption for 2013 - consumption for 2012
- Percentage of Increase = (Increase or Decrease / consumption for 2013) * 100%
- The first 4 strings of the Date values in the yearmonth table can represent year

Question: Which of the three segments—SME, LAM and KAM—has the biggest and lowest percentage increases in consumption paid in EUR between 2012 and 2013?</pre>

---

### ❓ Failed Question #3 (of 20 examples) - Question ID: `1500`


**Question**: Please list the product description of the products consumed in September, 2013.


### ✅ Ground Truth SQL(s)

```sql
SELECT T3.Description FROM transactions_1k AS T1 INNER JOIN yearmonth AS T2 ON T1.CustomerID = T2.CustomerID INNER JOIN products AS T3 ON T1.ProductID = T3.ProductID WHERE T2.Date = '201309'
```

### ❌ Predicted SQL

```sql
SELECT DISTINCT
  T2.Description
FROM transactions_1k AS T1
JOIN products AS T2
  ON T1.ProductID = T2.ProductID
WHERE
  strftime('%Y%m', T1.Date) = '201309'
```

### 📊 Evaluation Metrics

|   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |   execution_time_ms |
|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|--------------------:|
|                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          0 |            1111 |                  64 |           8877 |             40972.5 |               21.46 |

### 📘 Ground Truth Result(s)

**Result 1:**

| Description     |
|:----------------|
| Nafta           |
| Nafta           |
| Provoz.nápl.    |
| Natural         |
| Nafta           |
| Natural         |
| Natural         |
| Nemrz.kapal.    |
| Nafta           |
| Nafta           |
| Nafta           |
| Nafta           |
| Natural         |
| Natural         |
| Oleje,tuky      |
| Nafta           |
| Nafta           |
| Nafta           |
| Nafta           |
| Nafta           |
| ...             |
| ... (truncated) |
| ...             |
| Nafta           |
| Nafta           |
| Diesel +        |
| Natural         |
| Natural         |
| Natural         |
| Nafta           |
| Nafta           |
| Nafta           |
| Natural         |
| Natural         |
| Natural         |
| Nafta           |
| Nafta           |
| Nafta           |
| Nafta           |
| Nafta           |
| Nafta           |
| Nafta           |
| Nafta           |

### 📕 Predicted Result

| Description   |
|---------------|

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given sqlite database schema.

**Question:**:
Please list the product description of the products consumed in September, 2013.

**Database Engine / Dialect:**:
sqlite

**Schema:**
Table: customers
  Columns:
    - CustomerID (INTEGER) (Primary Key) # Example values: 3, 5, 6, 7, 9
    - Segment (TEXT) # Example values: SME, LAM, KAM
    - Currency (TEXT) # Example values: EUR, CZK

Table: gasstations
  Columns:
    - GasStationID (INTEGER) (Primary Key) # Example values: 44, 45, 46, 47, 48
    - ChainID (INTEGER) # Example values: 13, 6, 23, 33, 4
    - Country (TEXT) # Example values: CZE, SVK
    - Segment (TEXT) # Example values: Value for money, Premium, Other, Noname, Discount

Table: products
  Columns:
    - ProductID (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Description (TEXT) # Example values: Rucní zadání, Nafta, Special, Super, Natural

Table: transactions_1k
  Columns:
    - TransactionID (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Date (DATE) # Example values: 2012-08-24, 2012-08-23, 2012-08-25, 2012-08-26
    - Time (TEXT) # Example values: 09:41:00, 10:03:00, 13:53:00, 08:49:00, 08:53:00
    - CustomerID (INTEGER) # Example values: 31543, 46707, 7654, 17373, 7881
    - CardID (INTEGER) # Example values: 486621, 550134, 684220, 536109, 99745
    - GasStationID (INTEGER) # Example values: 3704, 656, 741, 1152, 636
    - ProductID (INTEGER) # Example values: 2, 23, 5, 11, 7
    - Amount (INTEGER) # Example values: 28, 18, 1, 5, 4
    - Price (REAL) # Example values: 672.64, 430.72, 121.99, 120.74, 645.05

Table: sqlite_sequence
  Columns:
    - name (TEXT) # Example values: transactions_1k
    - seq (TEXT) # Example values: 1000

Table: yearmonth
  Columns:
    - CustomerID (INTEGER) (Primary Key) # Example values: 39, 63, 172, 603, 1492
    - Date (TEXT) (Primary Key) # Example values: 201112, 201201, 201202, 201203, 201204
    - Consumption (REAL) # Example values: 528.3, 1598.28, 1931.36, 1497.14, 51.06


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., sqlite.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

***Hints***
- September 2013 refers to 201309
- The first 4 strings of the Date values in the yearmonth table can represent year
- The 5th and 6th string of the date can refer to month.

Question: Please list the product description of the products consumed in September, 2013.</pre>

---

### ❓ Failed Question #4 (of 20 examples) - Question ID: `1481`


**Question**: What is the difference in the annual average consumption of the customers with the least amount of consumption paid in CZK for 2013 between SME and LAM, LAM and KAM, and KAM and SME?


### ✅ Ground Truth SQL(s)

```sql
SELECT CAST(SUM(IIF(T1.Segment = 'SME', T2.Consumption, 0)) AS REAL) / COUNT(T1.CustomerID) - CAST(SUM(IIF(T1.Segment = 'LAM', T2.Consumption, 0)) AS REAL) / COUNT(T1.CustomerID) , CAST(SUM(IIF(T1.Segment = 'LAM', T2.Consumption, 0)) AS REAL) / COUNT(T1.CustomerID) - CAST(SUM(IIF(T1.Segment = 'KAM', T2.Consumption, 0)) AS REAL) / COUNT(T1.CustomerID) , CAST(SUM(IIF(T1.Segment = 'KAM', T2.Consumption, 0)) AS REAL) / COUNT(T1.CustomerID) - CAST(SUM(IIF(T1.Segment = 'SME', T2.Consumption, 0)) AS REAL) / COUNT(T1.CustomerID) FROM customers AS T1 INNER JOIN yearmonth AS T2 ON T1.CustomerID = T2.CustomerID WHERE T1.Currency = 'CZK' AND T2.Consumption = ( SELECT MIN(Consumption) FROM yearmonth ) AND T2.Date BETWEEN 201301 AND 201312
```

### ❌ Predicted SQL

```sql
WITH YearlyConsumption AS (
  SELECT
    T2.Segment,
    T1.CustomerID,
    SUM(T1.Consumption) AS TotalCons
  FROM yearmonth AS T1
  JOIN customers AS T2 ON T1.CustomerID = T2.CustomerID
  WHERE T2.Currency = 'CZK' AND T1.Date BETWEEN '201301' AND '201312'
  GROUP BY T2.Segment, T1.CustomerID
),
MinConsumption AS (
  SELECT
    Segment,
    MIN(TotalCons) AS MinCons
  FROM YearlyConsumption
  GROUP BY Segment
)
SELECT
  (SELECT MinCons FROM MinConsumption WHERE Segment = 'SME') - (SELECT MinCons FROM MinConsumption WHERE Segment = 'LAM'),
  (SELECT MinCons FROM MinConsumption WHERE Segment = 'LAM') - (SELECT MinCons FROM MinConsumption WHERE Segment = 'KAM'),
  (SELECT MinCons FROM MinConsumption WHERE Segment = 'KAM') - (SELECT MinCons FROM MinConsumption WHERE Segment = 'SME')
```

### 📊 Evaluation Metrics

|   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |   execution_time_ms |
|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|--------------------:|
|                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          0 |            1218 |                 239 |           9157 |             39699.6 |              380.17 |

### 📘 Ground Truth Result(s)

**Result 1:**

|   CAST(SUM(IIF(T1.Segment = 'SME', T2.Consumption, 0)) AS REAL) / COUNT(T1.CustomerID) - CAST(SUM(IIF(T1.Segment = 'LAM', T2.Consumption, 0)) AS REAL) / COUNT(T1.CustomerID) |   CAST(SUM(IIF(T1.Segment = 'LAM', T2.Consumption, 0)) AS REAL) / COUNT(T1.CustomerID) - CAST(SUM(IIF(T1.Segment = 'KAM', T2.Consumption, 0)) AS REAL) / COUNT(T1.CustomerID) |   CAST(SUM(IIF(T1.Segment = 'KAM', T2.Consumption, 0)) AS REAL) / COUNT(T1.CustomerID) - CAST(SUM(IIF(T1.Segment = 'SME', T2.Consumption, 0)) AS REAL) / COUNT(T1.CustomerID) |
|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|
|                                                                                                                                                                             0 |                                                                                                                                                                        582093 |                                                                                                                                                                       -582093 |

### 📕 Predicted Result

|   (SELECT MinCons FROM MinConsumption WHERE Segment = 'SME') - (SELECT MinCons FROM MinConsumption WHERE Segment = 'LAM') |   (SELECT MinCons FROM MinConsumption WHERE Segment = 'LAM') - (SELECT MinCons FROM MinConsumption WHERE Segment = 'KAM') |   (SELECT MinCons FROM MinConsumption WHERE Segment = 'KAM') - (SELECT MinCons FROM MinConsumption WHERE Segment = 'SME') |
|--------------------------------------------------------------------------------------------------------------------------:|--------------------------------------------------------------------------------------------------------------------------:|--------------------------------------------------------------------------------------------------------------------------:|
|                                                                                                                  -14009.3 |                                                                                                                   6046.62 |                                                                                                                   7962.72 |

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given sqlite database schema.

**Question:**:
What is the difference in the annual average consumption of the customers with the least amount of consumption paid in CZK for 2013 between SME and LAM, LAM and KAM, and KAM and SME?

**Database Engine / Dialect:**:
sqlite

**Schema:**
Table: customers
  Columns:
    - CustomerID (INTEGER) (Primary Key) # Example values: 3, 5, 6, 7, 9
    - Segment (TEXT) # Example values: SME, LAM, KAM
    - Currency (TEXT) # Example values: EUR, CZK

Table: gasstations
  Columns:
    - GasStationID (INTEGER) (Primary Key) # Example values: 44, 45, 46, 47, 48
    - ChainID (INTEGER) # Example values: 13, 6, 23, 33, 4
    - Country (TEXT) # Example values: CZE, SVK
    - Segment (TEXT) # Example values: Value for money, Premium, Other, Noname, Discount

Table: products
  Columns:
    - ProductID (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Description (TEXT) # Example values: Rucní zadání, Nafta, Special, Super, Natural

Table: transactions_1k
  Columns:
    - TransactionID (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Date (DATE) # Example values: 2012-08-24, 2012-08-23, 2012-08-25, 2012-08-26
    - Time (TEXT) # Example values: 09:41:00, 10:03:00, 13:53:00, 08:49:00, 08:53:00
    - CustomerID (INTEGER) # Example values: 31543, 46707, 7654, 17373, 7881
    - CardID (INTEGER) # Example values: 486621, 550134, 684220, 536109, 99745
    - GasStationID (INTEGER) # Example values: 3704, 656, 741, 1152, 636
    - ProductID (INTEGER) # Example values: 2, 23, 5, 11, 7
    - Amount (INTEGER) # Example values: 28, 18, 1, 5, 4
    - Price (REAL) # Example values: 672.64, 430.72, 121.99, 120.74, 645.05

Table: sqlite_sequence
  Columns:
    - name (TEXT) # Example values: transactions_1k
    - seq (TEXT) # Example values: 1000

Table: yearmonth
  Columns:
    - CustomerID (INTEGER) (Primary Key) # Example values: 39, 63, 172, 603, 1492
    - Date (TEXT) (Primary Key) # Example values: 201112, 201201, 201202, 201203, 201204
    - Consumption (REAL) # Example values: 528.3, 1598.28, 1931.36, 1497.14, 51.06


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., sqlite.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

***Hints***
- annual average consumption of customer with the lowest consumption in each segment = total consumption per year / the number of customer with lowest consumption in each segment
- Difference in annual average = SME&#x27;s annual average - LAM&#x27;s annual average
- Difference in annual average = LAM&#x27;s annual average - KAM&#x27;s annual average
- Year 2013 can be presented as Between 201301 And 201312
- The first 4 strings of the Date values in the yearmonth table can represent year.

Question: What is the difference in the annual average consumption of the customers with the least amount of consumption paid in CZK for 2013 between SME and LAM, LAM and KAM, and KAM and SME?</pre>

---

### ❓ Failed Question #5 (of 20 examples) - Question ID: `1501`


**Question**: Please list the countries of the gas stations with transactions taken place in June, 2013.


### ✅ Ground Truth SQL(s)

```sql
SELECT DISTINCT T2.Country FROM transactions_1k AS T1 INNER JOIN gasstations AS T2 ON T1.GasStationID = T2.GasStationID INNER JOIN yearmonth AS T3 ON T1.CustomerID = T3.CustomerID WHERE T3.Date = '201306'
```

### ❌ Predicted SQL

```sql
SELECT DISTINCT
  T2.Country
FROM transactions_1k AS T1
JOIN gasstations AS T2
  ON T1.GasStationID = T2.GasStationID
WHERE
  T1.Date LIKE '2013-06%'
```

### 📊 Evaluation Metrics

|   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |   execution_time_ms |
|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|--------------------:|
|                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          0 |            1118 |                  63 |           8880 |             40880.4 |               45.65 |

### 📘 Ground Truth Result(s)

**Result 1:**

| Country   |
|:----------|
| CZE       |
| SVK       |

### 📕 Predicted Result

| Country   |
|-----------|

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given sqlite database schema.

**Question:**:
Please list the countries of the gas stations with transactions taken place in June, 2013.

**Database Engine / Dialect:**:
sqlite

**Schema:**
Table: customers
  Columns:
    - CustomerID (INTEGER) (Primary Key) # Example values: 3, 5, 6, 7, 9
    - Segment (TEXT) # Example values: SME, LAM, KAM
    - Currency (TEXT) # Example values: EUR, CZK

Table: gasstations
  Columns:
    - GasStationID (INTEGER) (Primary Key) # Example values: 44, 45, 46, 47, 48
    - ChainID (INTEGER) # Example values: 13, 6, 23, 33, 4
    - Country (TEXT) # Example values: CZE, SVK
    - Segment (TEXT) # Example values: Value for money, Premium, Other, Noname, Discount

Table: products
  Columns:
    - ProductID (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Description (TEXT) # Example values: Rucní zadání, Nafta, Special, Super, Natural

Table: transactions_1k
  Columns:
    - TransactionID (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Date (DATE) # Example values: 2012-08-24, 2012-08-23, 2012-08-25, 2012-08-26
    - Time (TEXT) # Example values: 09:41:00, 10:03:00, 13:53:00, 08:49:00, 08:53:00
    - CustomerID (INTEGER) # Example values: 31543, 46707, 7654, 17373, 7881
    - CardID (INTEGER) # Example values: 486621, 550134, 684220, 536109, 99745
    - GasStationID (INTEGER) # Example values: 3704, 656, 741, 1152, 636
    - ProductID (INTEGER) # Example values: 2, 23, 5, 11, 7
    - Amount (INTEGER) # Example values: 28, 18, 1, 5, 4
    - Price (REAL) # Example values: 672.64, 430.72, 121.99, 120.74, 645.05

Table: sqlite_sequence
  Columns:
    - name (TEXT) # Example values: transactions_1k
    - seq (TEXT) # Example values: 1000

Table: yearmonth
  Columns:
    - CustomerID (INTEGER) (Primary Key) # Example values: 39, 63, 172, 603, 1492
    - Date (TEXT) (Primary Key) # Example values: 201112, 201201, 201202, 201203, 201204
    - Consumption (REAL) # Example values: 528.3, 1598.28, 1931.36, 1497.14, 51.06


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., sqlite.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

***Hints***
- June 2013 refers to &#x27;201306&#x27;
- The first 4 strings of the Date values in the yearmonth table can represent year
- The 5th and 6th string of the date can refer to month;

Question: Please list the countries of the gas stations with transactions taken place in June, 2013.</pre>

---

### ❓ Failed Question #6 (of 20 examples) - Question ID: `1490`


**Question**: How many percent of LAM customer consumed more than 46.73?


### ✅ Ground Truth SQL(s)

```sql
SELECT CAST(SUM(IIF(T2.Consumption > 46.73, 1, 0)) AS FLOAT) * 100 / COUNT(T1.CustomerID) FROM customers AS T1 INNER JOIN yearmonth AS T2 ON T1.CustomerID = T2.CustomerID WHERE T1.Segment = 'LAM'
```

### ❌ Predicted SQL

```sql
SELECT
  COUNT(DISTINCT T1.CustomerID) * 100.0 / (
    SELECT
      COUNT(*)
    FROM customers
    WHERE
      Segment = 'LAM'
  )
FROM yearmonth AS T1
JOIN customers AS T2
  ON T1.CustomerID = T2.CustomerID
WHERE
  T1.Consumption > 46.73 AND T2.Segment = 'LAM'
```

### 📊 Evaluation Metrics

|   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |   execution_time_ms |
|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|--------------------:|
|                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          0 |            1099 |                  99 |           7594 |             34739.4 |               78.81 |

### 📘 Ground Truth Result(s)

**Result 1:**

|   CAST(SUM(IIF(T2.Consumption > 46.73, 1, 0)) AS FLOAT) * 100 / COUNT(T1.CustomerID) |
|-------------------------------------------------------------------------------------:|
|                                                                              98.5268 |

### 📕 Predicted Result

|   COUNT(DISTINCT T1.CustomerID) * 100.0 / ( |
|                                      SELECT |
|                                    COUNT(*) |
|                              FROM customers |
|                                       WHERE |
|                             Segment = 'LAM' |
|                                           ) |
|--------------------------------------------:|
|                                     98.2504 |

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given sqlite database schema.

**Question:**:
How many percent of LAM customer consumed more than 46.73?

**Database Engine / Dialect:**:
sqlite

**Schema:**
Table: customers
  Columns:
    - CustomerID (INTEGER) (Primary Key) # Example values: 3, 5, 6, 7, 9
    - Segment (TEXT) # Example values: SME, LAM, KAM
    - Currency (TEXT) # Example values: EUR, CZK

Table: gasstations
  Columns:
    - GasStationID (INTEGER) (Primary Key) # Example values: 44, 45, 46, 47, 48
    - ChainID (INTEGER) # Example values: 13, 6, 23, 33, 4
    - Country (TEXT) # Example values: CZE, SVK
    - Segment (TEXT) # Example values: Value for money, Premium, Other, Noname, Discount

Table: products
  Columns:
    - ProductID (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Description (TEXT) # Example values: Rucní zadání, Nafta, Special, Super, Natural

Table: transactions_1k
  Columns:
    - TransactionID (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Date (DATE) # Example values: 2012-08-24, 2012-08-23, 2012-08-25, 2012-08-26
    - Time (TEXT) # Example values: 09:41:00, 10:03:00, 13:53:00, 08:49:00, 08:53:00
    - CustomerID (INTEGER) # Example values: 31543, 46707, 7654, 17373, 7881
    - CardID (INTEGER) # Example values: 486621, 550134, 684220, 536109, 99745
    - GasStationID (INTEGER) # Example values: 3704, 656, 741, 1152, 636
    - ProductID (INTEGER) # Example values: 2, 23, 5, 11, 7
    - Amount (INTEGER) # Example values: 28, 18, 1, 5, 4
    - Price (REAL) # Example values: 672.64, 430.72, 121.99, 120.74, 645.05

Table: sqlite_sequence
  Columns:
    - name (TEXT) # Example values: transactions_1k
    - seq (TEXT) # Example values: 1000

Table: yearmonth
  Columns:
    - CustomerID (INTEGER) (Primary Key) # Example values: 39, 63, 172, 603, 1492
    - Date (TEXT) (Primary Key) # Example values: 201112, 201201, 201202, 201203, 201204
    - Consumption (REAL) # Example values: 528.3, 1598.28, 1931.36, 1497.14, 51.06


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., sqlite.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

***Hints***
- Percentage of LAM customer consumed more than 46.73 = (Total no. of LAM customers who consumed more than 46.73 / Total no. of LAM customers) * 100.

Question: How many percent of LAM customer consumed more than 46.73?</pre>

---

### ❓ Failed Question #7 (of 20 examples) - Question ID: `1505`


**Question**: Among the customers who paid in euro, how many of them have a monthly consumption of over 1000?


### ✅ Ground Truth SQL(s)

```sql
SELECT COUNT(*) FROM yearmonth AS T1 INNER JOIN customers AS T2 ON T1.CustomerID = T2.CustomerID WHERE T2.Currency = 'EUR' AND T1.Consumption > 1000.00
```

### ❌ Predicted SQL

```sql
SELECT
  COUNT(DISTINCT T1.CustomerID)
FROM customers AS T1
JOIN yearmonth AS T2
  ON T1.CustomerID = T2.CustomerID
WHERE
  T1.Currency = 'EUR' AND T2.Consumption > 1000
```

### 📊 Evaluation Metrics

|   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |   execution_time_ms |
|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|--------------------:|
|                    0 |                              0 |                                     0 |                          1 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          0 |            1079 |                  66 |           1640 |             3522.56 |               74.25 |

### 📘 Ground Truth Result(s)

**Result 1:**

|   COUNT(*) |
|-----------:|
|       2730 |

### 📕 Predicted Result

|   COUNT(DISTINCT T1.CustomerID) |
|--------------------------------:|
|                             391 |

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given sqlite database schema.

**Question:**:
Among the customers who paid in euro, how many of them have a monthly consumption of over 1000?

**Database Engine / Dialect:**:
sqlite

**Schema:**
Table: customers
  Columns:
    - CustomerID (INTEGER) (Primary Key) # Example values: 3, 5, 6, 7, 9
    - Segment (TEXT) # Example values: SME, LAM, KAM
    - Currency (TEXT) # Example values: EUR, CZK

Table: gasstations
  Columns:
    - GasStationID (INTEGER) (Primary Key) # Example values: 44, 45, 46, 47, 48
    - ChainID (INTEGER) # Example values: 13, 6, 23, 33, 4
    - Country (TEXT) # Example values: CZE, SVK
    - Segment (TEXT) # Example values: Value for money, Premium, Other, Noname, Discount

Table: products
  Columns:
    - ProductID (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Description (TEXT) # Example values: Rucní zadání, Nafta, Special, Super, Natural

Table: transactions_1k
  Columns:
    - TransactionID (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Date (DATE) # Example values: 2012-08-24, 2012-08-23, 2012-08-25, 2012-08-26
    - Time (TEXT) # Example values: 09:41:00, 10:03:00, 13:53:00, 08:49:00, 08:53:00
    - CustomerID (INTEGER) # Example values: 31543, 46707, 7654, 17373, 7881
    - CardID (INTEGER) # Example values: 486621, 550134, 684220, 536109, 99745
    - GasStationID (INTEGER) # Example values: 3704, 656, 741, 1152, 636
    - ProductID (INTEGER) # Example values: 2, 23, 5, 11, 7
    - Amount (INTEGER) # Example values: 28, 18, 1, 5, 4
    - Price (REAL) # Example values: 672.64, 430.72, 121.99, 120.74, 645.05

Table: sqlite_sequence
  Columns:
    - name (TEXT) # Example values: transactions_1k
    - seq (TEXT) # Example values: 1000

Table: yearmonth
  Columns:
    - CustomerID (INTEGER) (Primary Key) # Example values: 39, 63, 172, 603, 1492
    - Date (TEXT) (Primary Key) # Example values: 201112, 201201, 201202, 201203, 201204
    - Consumption (REAL) # Example values: 528.3, 1598.28, 1931.36, 1497.14, 51.06


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., sqlite.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

***Hints***
- Pays in euro = Currency = &#x27;EUR&#x27;.

Question: Among the customers who paid in euro, how many of them have a monthly consumption of over 1000?</pre>

---

### ❓ Failed Question #8 (of 20 examples) - Question ID: `1514`


**Question**: What kind of currency did the customer paid at 16:25:00 in 2012/8/24?


### ✅ Ground Truth SQL(s)

```sql
SELECT DISTINCT T3.Currency FROM transactions_1k AS T1 INNER JOIN gasstations AS T2 ON T1.GasStationID = T2.GasStationID INNER JOIN customers AS T3 ON T1.CustomerID = T3.CustomerID WHERE T1.Date = '2012-08-24' AND T1.Time = '16:25:00'
```

### ❌ Predicted SQL

```sql
SELECT
  T2.Currency
FROM transactions_1k AS T1
JOIN customers AS T2
  ON T1.CustomerID = T2.CustomerID
WHERE
  T1.Time = '16:25:00' AND T1.Date = '2012-08-24'
```

### 📊 Evaluation Metrics

|   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |   execution_time_ms |
|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|--------------------:|
|                    0 |                              0 |                                     0 |                          0 |                         1 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          0 |            1112 |                  76 |           1663 |             3336.41 |               51.64 |

### 📘 Ground Truth Result(s)

**Result 1:**

| Currency   |
|:-----------|
| CZK        |

### 📕 Predicted Result

| Currency   |
|:-----------|
| CZK        |
| CZK        |
| CZK        |

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given sqlite database schema.

**Question:**:
What kind of currency did the customer paid at 16:25:00 in 2012/8/24?

**Database Engine / Dialect:**:
sqlite

**Schema:**
Table: customers
  Columns:
    - CustomerID (INTEGER) (Primary Key) # Example values: 3, 5, 6, 7, 9
    - Segment (TEXT) # Example values: SME, LAM, KAM
    - Currency (TEXT) # Example values: EUR, CZK

Table: gasstations
  Columns:
    - GasStationID (INTEGER) (Primary Key) # Example values: 44, 45, 46, 47, 48
    - ChainID (INTEGER) # Example values: 13, 6, 23, 33, 4
    - Country (TEXT) # Example values: CZE, SVK
    - Segment (TEXT) # Example values: Value for money, Premium, Other, Noname, Discount

Table: products
  Columns:
    - ProductID (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Description (TEXT) # Example values: Rucní zadání, Nafta, Special, Super, Natural

Table: transactions_1k
  Columns:
    - TransactionID (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Date (DATE) # Example values: 2012-08-24, 2012-08-23, 2012-08-25, 2012-08-26
    - Time (TEXT) # Example values: 09:41:00, 10:03:00, 13:53:00, 08:49:00, 08:53:00
    - CustomerID (INTEGER) # Example values: 31543, 46707, 7654, 17373, 7881
    - CardID (INTEGER) # Example values: 486621, 550134, 684220, 536109, 99745
    - GasStationID (INTEGER) # Example values: 3704, 656, 741, 1152, 636
    - ProductID (INTEGER) # Example values: 2, 23, 5, 11, 7
    - Amount (INTEGER) # Example values: 28, 18, 1, 5, 4
    - Price (REAL) # Example values: 672.64, 430.72, 121.99, 120.74, 645.05

Table: sqlite_sequence
  Columns:
    - name (TEXT) # Example values: transactions_1k
    - seq (TEXT) # Example values: 1000

Table: yearmonth
  Columns:
    - CustomerID (INTEGER) (Primary Key) # Example values: 39, 63, 172, 603, 1492
    - Date (TEXT) (Primary Key) # Example values: 201112, 201201, 201202, 201203, 201204
    - Consumption (REAL) # Example values: 528.3, 1598.28, 1931.36, 1497.14, 51.06


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., sqlite.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

***Hints***
- &#x27;2012/8/24&#x27; can be represented by &#x27;2012-08-24&#x27;
- 

Question: What kind of currency did the customer paid at 16:25:00 in 2012/8/24?</pre>

---

### ⚠️  Inference Failed - Question #9 (of 20 examples) - Question ID: `1521`

**Question**: For all the transactions happened during 8:00-9:00 in 2012/8/26, how many happened in CZE?

### ❌ Inference Error
```
Failed to get response from Gemini API: 503 UNAVAILABLE. {'error': {'code': 503, 'message': 'The service is currently unavailable.', 'status': 'UNAVAILABLE', 'details': [{'@type': 'type.googleapis.com/google.rpc.DebugInfo', 'detail': '[ORIGINAL ERROR] generic::unavailable: [original: throttling::THROTTLED_TASK_LIMIT] Request \'/GenerativeService.GenerateContent\' from role \'genai-api\' throttled: Task is overloaded (cpu-protection) go/tr-t. [google.rpc.error_details_ext] { message: "The service is currently unavailable." details { type_url: "type.googleapis.com/language_labs.genai.debug.GeminiApiDebugInfo" value: "\\222\\001\\275\\001\\nzRequest \\\'/GenerativeService.GenerateContent\\\' from role \\\'genai-api\\\' throttled: Task is overloaded (cpu-protection) go/tr-t.\\022?net/loadshedding/serverthrottler/core/server_throttler.cc:590:0" } }'}]}}
```
### 📝 Prompt Used
```
Your task is to convert a natural language question into an accurate SQL query using the given sqlite database schema.

**Question:**:
For all the transactions happened during 8:00-9:00 in 2012/8/26, how many happened in CZE?

**Database Engine / Dialect:**:
sqlite

**Schema:**
Table: customers
  Columns:
    - CustomerID (INTEGER) (Primary Key) # Example values: 3, 5, 6, 7, 9
    - Segment (TEXT) # Example values: SME, LAM, KAM
    - Currency (TEXT) # Example values: EUR, CZK

Table: gasstation
…
t SQL dialect for the database, i.e., sqlite.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

***Hints***
- Czech Republic can be represented as the Country value in the gasstations table is 'CZE'
- '2012/8/26' can be represented by '2012-08-26'
- during 8:00-9:00 can be represented as Time BETWEEN '08:00:00' AND '09:00:00'

Question: For all the transactions happened during 8:00-9:00 in 2012/8/26, how many happened in CZE?
```
---


---

### ❓ Failed Question #10 (of 20 examples) - Question ID: `1525`


**Question**: What is the percentage of the customers who used EUR in 2012/8/25?


### ✅ Ground Truth SQL(s)

```sql
SELECT CAST(SUM(IIF(T2.Currency = 'EUR', 1, 0)) AS FLOAT) * 100 / COUNT(T1.CustomerID) FROM transactions_1k AS T1 INNER JOIN customers AS T2 ON T1.CustomerID = T2.CustomerID WHERE T1.Date = '2012-08-25'
```

### ❌ Predicted SQL

```sql
SELECT
  100.0 * COUNT(DISTINCT CASE WHEN T2.Currency = 'EUR' THEN T1.CustomerID END) / COUNT(DISTINCT T1.CustomerID)
FROM transactions_1k AS T1
JOIN customers AS T2
  ON T1.CustomerID = T2.CustomerID
WHERE
  T1.Date = '2012-08-25'
```

### 📊 Evaluation Metrics

|   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |   execution_time_ms |
|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|--------------------:|
|                    0 |                              0 |                                     0 |                          1 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          0 |            1093 |                  93 |           8887 |             41374.4 |               46.81 |

### 📘 Ground Truth Result(s)

**Result 1:**

|   CAST(SUM(IIF(T2.Currency = 'EUR', 1, 0)) AS FLOAT) * 100 / COUNT(T1.CustomerID) |
|----------------------------------------------------------------------------------:|
|                                                                           1.64706 |

### 📕 Predicted Result

|   100.0 * COUNT(DISTINCT CASE WHEN T2.Currency = 'EUR' THEN T1.CustomerID END) / COUNT(DISTINCT T1.CustomerID) |
|---------------------------------------------------------------------------------------------------------------:|
|                                                                                                         2.7027 |

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given sqlite database schema.

**Question:**:
What is the percentage of the customers who used EUR in 2012/8/25?

**Database Engine / Dialect:**:
sqlite

**Schema:**
Table: customers
  Columns:
    - CustomerID (INTEGER) (Primary Key) # Example values: 3, 5, 6, 7, 9
    - Segment (TEXT) # Example values: SME, LAM, KAM
    - Currency (TEXT) # Example values: EUR, CZK

Table: gasstations
  Columns:
    - GasStationID (INTEGER) (Primary Key) # Example values: 44, 45, 46, 47, 48
    - ChainID (INTEGER) # Example values: 13, 6, 23, 33, 4
    - Country (TEXT) # Example values: CZE, SVK
    - Segment (TEXT) # Example values: Value for money, Premium, Other, Noname, Discount

Table: products
  Columns:
    - ProductID (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Description (TEXT) # Example values: Rucní zadání, Nafta, Special, Super, Natural

Table: transactions_1k
  Columns:
    - TransactionID (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Date (DATE) # Example values: 2012-08-24, 2012-08-23, 2012-08-25, 2012-08-26
    - Time (TEXT) # Example values: 09:41:00, 10:03:00, 13:53:00, 08:49:00, 08:53:00
    - CustomerID (INTEGER) # Example values: 31543, 46707, 7654, 17373, 7881
    - CardID (INTEGER) # Example values: 486621, 550134, 684220, 536109, 99745
    - GasStationID (INTEGER) # Example values: 3704, 656, 741, 1152, 636
    - ProductID (INTEGER) # Example values: 2, 23, 5, 11, 7
    - Amount (INTEGER) # Example values: 28, 18, 1, 5, 4
    - Price (REAL) # Example values: 672.64, 430.72, 121.99, 120.74, 645.05

Table: sqlite_sequence
  Columns:
    - name (TEXT) # Example values: transactions_1k
    - seq (TEXT) # Example values: 1000

Table: yearmonth
  Columns:
    - CustomerID (INTEGER) (Primary Key) # Example values: 39, 63, 172, 603, 1492
    - Date (TEXT) (Primary Key) # Example values: 201112, 201201, 201202, 201203, 201204
    - Consumption (REAL) # Example values: 528.3, 1598.28, 1931.36, 1497.14, 51.06


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., sqlite.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

***Hints***
- &#x27;2012/8/25&#x27; can be represented by &#x27;2012-08-25&#x27;

Question: What is the percentage of the customers who used EUR in 2012/8/25?</pre>

---

### ❓ Failed Question #11 (of 20 examples) - Question ID: `1529`


**Question**: What is the amount spent by customer "38508" at the gas stations? How much had the customer spent in January 2012?


### ✅ Ground Truth SQL(s)

```sql
SELECT SUM(T1.Price ) , SUM(IIF(T3.Date = '201201', T1.Price, 0)) FROM transactions_1k AS T1 INNER JOIN gasstations AS T2 ON T1.GasStationID = T2.GasStationID INNER JOIN yearmonth AS T3 ON T1.CustomerID = T3.CustomerID WHERE T1.CustomerID = '38508'
```

### ❌ Predicted SQL

```sql
SELECT SUM(Consumption) FROM yearmonth WHERE CustomerID = 38508;
SELECT Consumption FROM yearmonth WHERE CustomerID = 38508 AND Date = '201201'
```

### 📊 Evaluation Metrics

| df_error_message                                                                      |   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |
|:--------------------------------------------------------------------------------------|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|
| Error running SQL: Error running query: You can only execute one statement at a time. |                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          1 |            1108 |                  51 |           8858 |             43777.2 |

### 📘 Ground Truth Result(s)

**Result 1:**

|   SUM(T1.Price ) |   SUM(IIF(T3.Date = '201201', T1.Price, 0)) |
|-----------------:|--------------------------------------------:|
|          68740.2 |                                     3437.01 |

### 📕 Predicted Result

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given sqlite database schema.

**Question:**:
What is the amount spent by customer &quot;38508&quot; at the gas stations? How much had the customer spent in January 2012?

**Database Engine / Dialect:**:
sqlite

**Schema:**
Table: customers
  Columns:
    - CustomerID (INTEGER) (Primary Key) # Example values: 3, 5, 6, 7, 9
    - Segment (TEXT) # Example values: SME, LAM, KAM
    - Currency (TEXT) # Example values: EUR, CZK

Table: gasstations
  Columns:
    - GasStationID (INTEGER) (Primary Key) # Example values: 44, 45, 46, 47, 48
    - ChainID (INTEGER) # Example values: 13, 6, 23, 33, 4
    - Country (TEXT) # Example values: CZE, SVK
    - Segment (TEXT) # Example values: Value for money, Premium, Other, Noname, Discount

Table: products
  Columns:
    - ProductID (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Description (TEXT) # Example values: Rucní zadání, Nafta, Special, Super, Natural

Table: transactions_1k
  Columns:
    - TransactionID (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Date (DATE) # Example values: 2012-08-24, 2012-08-23, 2012-08-25, 2012-08-26
    - Time (TEXT) # Example values: 09:41:00, 10:03:00, 13:53:00, 08:49:00, 08:53:00
    - CustomerID (INTEGER) # Example values: 31543, 46707, 7654, 17373, 7881
    - CardID (INTEGER) # Example values: 486621, 550134, 684220, 536109, 99745
    - GasStationID (INTEGER) # Example values: 3704, 656, 741, 1152, 636
    - ProductID (INTEGER) # Example values: 2, 23, 5, 11, 7
    - Amount (INTEGER) # Example values: 28, 18, 1, 5, 4
    - Price (REAL) # Example values: 672.64, 430.72, 121.99, 120.74, 645.05

Table: sqlite_sequence
  Columns:
    - name (TEXT) # Example values: transactions_1k
    - seq (TEXT) # Example values: 1000

Table: yearmonth
  Columns:
    - CustomerID (INTEGER) (Primary Key) # Example values: 39, 63, 172, 603, 1492
    - Date (TEXT) (Primary Key) # Example values: 201112, 201201, 201202, 201203, 201204
    - Consumption (REAL) # Example values: 528.3, 1598.28, 1931.36, 1497.14, 51.06


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., sqlite.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

***Hints***
- January 2012 refers to the Date value = &#x27;201201&#x27;

Question: What is the amount spent by customer &quot;38508&quot; at the gas stations? How much had the customer spent in January 2012?</pre>

---

### ❓ Failed Question #12 (of 20 examples) - Question ID: `1526`


**Question**: For the customer who paid 634.8 in 2012/8/25, what was the consumption decrease rate from Year 2012 to 2013?


### ✅ Ground Truth SQL(s)

```sql
SELECT CAST(SUM(IIF(SUBSTRING(Date, 1, 4) = '2012', Consumption, 0)) - SUM(IIF(SUBSTRING(Date, 1, 4) = '2013', Consumption, 0)) AS FLOAT) / SUM(IIF(SUBSTRING(Date, 1, 4) = '2012', Consumption, 0)) FROM yearmonth WHERE CustomerID = ( SELECT T1.CustomerID FROM transactions_1k AS T1 INNER JOIN gasstations AS T2 ON T1.GasStationID = T2.GasStationID WHERE T1.Date = '2012-08-25' AND T1.Price  = 1513.12 )
```

### ❌ Predicted SQL

```sql
SELECT
  (
    SUM(
      CASE
        WHEN Date LIKE '2012%' THEN Consumption
        ELSE 0
      END
    ) - SUM(
      CASE
        WHEN Date LIKE '2013%' THEN Consumption
        ELSE 0
      END
    )
  ) / SUM(
    CASE
      WHEN Date LIKE '2012%' THEN Consumption
      ELSE 0
    END
  )
FROM yearmonth
WHERE
  CustomerID IN (
    SELECT
      CustomerID
    FROM transactions_1k
    WHERE
      Price = 634.8
      AND Date = '2012-08-25'
  )
```

### 📊 Evaluation Metrics

|   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |   execution_time_ms |
|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|--------------------:|
|                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          0 |            1163 |                 160 |           9022 |             40149.1 |              139.02 |

### 📘 Ground Truth Result(s)

**Result 1:**

| CAST(SUM(IIF(SUBSTRING(Date, 1, 4) = '2012', Consumption, 0)) - SUM(IIF(SUBSTRING(Date, 1, 4) = '2013', Consumption, 0)) AS FLOAT) / SUM(IIF(SUBSTRING(Date, 1, 4) = '2012', Consumption, 0))   |
|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|                                                                                                                                                                                                 |

### 📕 Predicted Result

|                                                 ( |
|                                              SUM( |
|                                              CASE |
|           WHEN Date LIKE '2012%' THEN Consumption |
|                                            ELSE 0 |
|                                               END |
|                                          ) - SUM( |
|                                              CASE |
|           WHEN Date LIKE '2013%' THEN Consumption |
|                                            ELSE 0 |
|                                               END |
|                                                 ) |
|                                          ) / SUM( |
|                                              CASE |
|           WHEN Date LIKE '2012%' THEN Consumption |
|                                            ELSE 0 |
|                                               END |
|                                                 ) |
|--------------------------------------------------:|
|                                          -5.81524 |

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given sqlite database schema.

**Question:**:
For the customer who paid 634.8 in 2012/8/25, what was the consumption decrease rate from Year 2012 to 2013?

**Database Engine / Dialect:**:
sqlite

**Schema:**
Table: customers
  Columns:
    - CustomerID (INTEGER) (Primary Key) # Example values: 3, 5, 6, 7, 9
    - Segment (TEXT) # Example values: SME, LAM, KAM
    - Currency (TEXT) # Example values: EUR, CZK

Table: gasstations
  Columns:
    - GasStationID (INTEGER) (Primary Key) # Example values: 44, 45, 46, 47, 48
    - ChainID (INTEGER) # Example values: 13, 6, 23, 33, 4
    - Country (TEXT) # Example values: CZE, SVK
    - Segment (TEXT) # Example values: Value for money, Premium, Other, Noname, Discount

Table: products
  Columns:
    - ProductID (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Description (TEXT) # Example values: Rucní zadání, Nafta, Special, Super, Natural

Table: transactions_1k
  Columns:
    - TransactionID (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Date (DATE) # Example values: 2012-08-24, 2012-08-23, 2012-08-25, 2012-08-26
    - Time (TEXT) # Example values: 09:41:00, 10:03:00, 13:53:00, 08:49:00, 08:53:00
    - CustomerID (INTEGER) # Example values: 31543, 46707, 7654, 17373, 7881
    - CardID (INTEGER) # Example values: 486621, 550134, 684220, 536109, 99745
    - GasStationID (INTEGER) # Example values: 3704, 656, 741, 1152, 636
    - ProductID (INTEGER) # Example values: 2, 23, 5, 11, 7
    - Amount (INTEGER) # Example values: 28, 18, 1, 5, 4
    - Price (REAL) # Example values: 672.64, 430.72, 121.99, 120.74, 645.05

Table: sqlite_sequence
  Columns:
    - name (TEXT) # Example values: transactions_1k
    - seq (TEXT) # Example values: 1000

Table: yearmonth
  Columns:
    - CustomerID (INTEGER) (Primary Key) # Example values: 39, 63, 172, 603, 1492
    - Date (TEXT) (Primary Key) # Example values: 201112, 201201, 201202, 201203, 201204
    - Consumption (REAL) # Example values: 528.3, 1598.28, 1931.36, 1497.14, 51.06


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., sqlite.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

***Hints***
- &#x27;2012/8/24&#x27; can be represented by &#x27;2012-08-24&#x27;
- Consumption decrease rate = (consumption_2012 - consumption_2013) / consumption_2012

Question: For the customer who paid 634.8 in 2012/8/25, what was the consumption decrease rate from Year 2012 to 2013?</pre>

---

### ❓ Failed Question #13 (of 20 examples) - Question ID: `1531`


**Question**: Who is the top spending customer and how much is the average price per single item purchased by this customer? What currency was being used?


### ✅ Ground Truth SQL(s)

```sql
SELECT T2.CustomerID, SUM(T2.Price / T2.Amount), T1.Currency FROM customers AS T1 INNER JOIN transactions_1k AS T2 ON T1.CustomerID = T2.CustomerID WHERE T2.CustomerID = ( SELECT CustomerID FROM yearmonth ORDER BY Consumption DESC LIMIT 1 ) GROUP BY T2.CustomerID, T1.Currency
```

### ❌ Predicted SQL

```sql
SELECT
  T.CustomerID,
  SUM(T.Price) / SUM(T.Amount),
  C.Currency
FROM transactions_1k AS T
JOIN customers AS C
  ON T.CustomerID = C.CustomerID
GROUP BY
  T.CustomerID
ORDER BY
  SUM(T.Price) DESC
LIMIT 1
```

### 📊 Evaluation Metrics

|   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |   execution_time_ms |
|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|--------------------:|
|                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          0 |            1091 |                  80 |           1829 |             4409.43 |               93.35 |

### 📘 Ground Truth Result(s)

**Result 1:**

|   CustomerID |   SUM(T2.Price / T2.Amount) | Currency   |
|-------------:|----------------------------:|:-----------|
|        12459 |                     203.856 | CZK        |

### 📕 Predicted Result

|   CustomerID |   SUM(T.Price) / SUM(T.Amount) | Currency   |
|-------------:|-------------------------------:|:-----------|
|        19182 |                        24.2776 | CZK        |

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given sqlite database schema.

**Question:**:
Who is the top spending customer and how much is the average price per single item purchased by this customer? What currency was being used?

**Database Engine / Dialect:**:
sqlite

**Schema:**
Table: customers
  Columns:
    - CustomerID (INTEGER) (Primary Key) # Example values: 3, 5, 6, 7, 9
    - Segment (TEXT) # Example values: SME, LAM, KAM
    - Currency (TEXT) # Example values: EUR, CZK

Table: gasstations
  Columns:
    - GasStationID (INTEGER) (Primary Key) # Example values: 44, 45, 46, 47, 48
    - ChainID (INTEGER) # Example values: 13, 6, 23, 33, 4
    - Country (TEXT) # Example values: CZE, SVK
    - Segment (TEXT) # Example values: Value for money, Premium, Other, Noname, Discount

Table: products
  Columns:
    - ProductID (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Description (TEXT) # Example values: Rucní zadání, Nafta, Special, Super, Natural

Table: transactions_1k
  Columns:
    - TransactionID (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Date (DATE) # Example values: 2012-08-24, 2012-08-23, 2012-08-25, 2012-08-26
    - Time (TEXT) # Example values: 09:41:00, 10:03:00, 13:53:00, 08:49:00, 08:53:00
    - CustomerID (INTEGER) # Example values: 31543, 46707, 7654, 17373, 7881
    - CardID (INTEGER) # Example values: 486621, 550134, 684220, 536109, 99745
    - GasStationID (INTEGER) # Example values: 3704, 656, 741, 1152, 636
    - ProductID (INTEGER) # Example values: 2, 23, 5, 11, 7
    - Amount (INTEGER) # Example values: 28, 18, 1, 5, 4
    - Price (REAL) # Example values: 672.64, 430.72, 121.99, 120.74, 645.05

Table: sqlite_sequence
  Columns:
    - name (TEXT) # Example values: transactions_1k
    - seq (TEXT) # Example values: 1000

Table: yearmonth
  Columns:
    - CustomerID (INTEGER) (Primary Key) # Example values: 39, 63, 172, 603, 1492
    - Date (TEXT) (Primary Key) # Example values: 201112, 201201, 201202, 201203, 201204
    - Consumption (REAL) # Example values: 528.3, 1598.28, 1931.36, 1497.14, 51.06


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., sqlite.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

***Hints***
- average price per single item = Total(price) / Total(amount)

Question: Who is the top spending customer and how much is the average price per single item purchased by this customer? What currency was being used?</pre>

---

### ❓ Failed Question #14 (of 20 examples) - Question ID: `1533`


**Question**: For all the people who paid more than 29.00 per unit of product id No.5. Give their consumption status in the August of 2012.


### ✅ Ground Truth SQL(s)

```sql
SELECT T2.Consumption FROM transactions_1k AS T1 INNER JOIN yearmonth AS T2 ON T1.CustomerID = T2.CustomerID WHERE T1.Price / T1.Amount > 29.00 AND T1.ProductID = 5 AND T2.Date = '201208'
```

### ❌ Predicted SQL

```sql
SELECT
  T1.Consumption
FROM yearmonth AS T1
WHERE
  T1.Date = '201208'
  AND T1.CustomerID IN (
    SELECT
      T2.CustomerID
    FROM transactions_1k AS T2
    WHERE
      T2.ProductID = 5 AND T2.Price / T2.Amount > 29
  )
```

### 📊 Evaluation Metrics

|   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |   execution_time_ms |
|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|--------------------:|
|                    0 |                              0 |                                     0 |                          0 |                         1 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          0 |            1130 |                  93 |           2820 |             10372.5 |               92.51 |

### 📘 Ground Truth Result(s)

**Result 1:**

|   Consumption |
|--------------:|
|       1903.2  |
|      88265.4  |
|       1129.2  |
|     126158    |
|     126158    |
|         58.19 |
|       1142.95 |
|       8878.07 |
|      69331.7  |
|      45937.2  |

### 📕 Predicted Result

|   Consumption |
|--------------:|
|      88265.4  |
|     126158    |
|      45937.2  |
|       1903.2  |
|         58.19 |
|       8878.07 |
|      69331.7  |
|       1142.95 |
|       1129.2  |

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given sqlite database schema.

**Question:**:
For all the people who paid more than 29.00 per unit of product id No.5. Give their consumption status in the August of 2012.

**Database Engine / Dialect:**:
sqlite

**Schema:**
Table: customers
  Columns:
    - CustomerID (INTEGER) (Primary Key) # Example values: 3, 5, 6, 7, 9
    - Segment (TEXT) # Example values: SME, LAM, KAM
    - Currency (TEXT) # Example values: EUR, CZK

Table: gasstations
  Columns:
    - GasStationID (INTEGER) (Primary Key) # Example values: 44, 45, 46, 47, 48
    - ChainID (INTEGER) # Example values: 13, 6, 23, 33, 4
    - Country (TEXT) # Example values: CZE, SVK
    - Segment (TEXT) # Example values: Value for money, Premium, Other, Noname, Discount

Table: products
  Columns:
    - ProductID (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Description (TEXT) # Example values: Rucní zadání, Nafta, Special, Super, Natural

Table: transactions_1k
  Columns:
    - TransactionID (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Date (DATE) # Example values: 2012-08-24, 2012-08-23, 2012-08-25, 2012-08-26
    - Time (TEXT) # Example values: 09:41:00, 10:03:00, 13:53:00, 08:49:00, 08:53:00
    - CustomerID (INTEGER) # Example values: 31543, 46707, 7654, 17373, 7881
    - CardID (INTEGER) # Example values: 486621, 550134, 684220, 536109, 99745
    - GasStationID (INTEGER) # Example values: 3704, 656, 741, 1152, 636
    - ProductID (INTEGER) # Example values: 2, 23, 5, 11, 7
    - Amount (INTEGER) # Example values: 28, 18, 1, 5, 4
    - Price (REAL) # Example values: 672.64, 430.72, 121.99, 120.74, 645.05

Table: sqlite_sequence
  Columns:
    - name (TEXT) # Example values: transactions_1k
    - seq (TEXT) # Example values: 1000

Table: yearmonth
  Columns:
    - CustomerID (INTEGER) (Primary Key) # Example values: 39, 63, 172, 603, 1492
    - Date (TEXT) (Primary Key) # Example values: 201112, 201201, 201202, 201203, 201204
    - Consumption (REAL) # Example values: 528.3, 1598.28, 1931.36, 1497.14, 51.06


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., sqlite.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

***Hints***
- August of 2012 refers to the Date value = &#x27;201208&#x27; 
- Price per unit of product = Price / Amount;

Question: For all the people who paid more than 29.00 per unit of product id No.5. Give their consumption status in the August of 2012.</pre>

---

### ❓ Failed Question #15 (of 20 examples) - Question ID: `1322`


**Question**: Among the events attended by more than 10 members of the Student_Club, how many of them are meetings?


### ✅ Ground Truth SQL(s)

```sql
SELECT COUNT(DISTINCT T1.event_id) FROM event AS T1 INNER JOIN attendance AS T2 ON T1.event_id = T2.link_to_event WHERE T1.type = 'Meeting' GROUP BY T1.event_id HAVING COUNT(T2.link_to_event) > 10
```

### ❌ Predicted SQL

```sql
SELECT
  COUNT(*)
FROM (
  SELECT
    T1.event_id
  FROM event AS T1
  JOIN attendance AS T2
    ON T1.event_id = T2.link_to_event
  WHERE
    T1.type = 'Meeting'
  GROUP BY
    T1.event_id
  HAVING
    COUNT(T1.event_id) > 10
)
```

### 📊 Evaluation Metrics

|   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |   execution_time_ms |
|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|--------------------:|
|                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          0 |            2614 |                 102 |           4932 |             12227.6 |               58.37 |

### 📘 Ground Truth Result(s)

**Result 1:**

|   COUNT(DISTINCT T1.event_id) |
|------------------------------:|
|                             1 |
|                             1 |
|                             1 |
|                             1 |

### 📕 Predicted Result

|   COUNT(*) |
|-----------:|
|          4 |

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given sqlite database schema.

**Question:**:
Among the events attended by more than 10 members of the Student_Club, how many of them are meetings?

**Database Engine / Dialect:**:
sqlite

**Schema:**
Table: event
  Columns:
    - event_id (TEXT) (Primary Key) # Example values: rec0Si5cQ4rJRVzd6, rec0akZnLLpGUloLH, rec0dZPcWXF0QjNnE, rec180D2MI4EpckHy, rec2N69DMcrqN9PJC
    - event_name (TEXT) # Example values: March Meeting, Officers meeting - January, Spring Elections, Officers meeting - March, Women&#x27;s Soccer
    - event_date (TEXT) # Example values: 2020-03-10T12:00:00, 2020-01-14T09:30:00, 2019-11-24T09:00:00, 2020-03-10T09:30:00, 2019-10-05T12:00:00
    - type (TEXT) # Example values: Meeting, Election, Game, Guest Speaker, Social
    - notes (TEXT) # Example values: All active members can vote for new officers between 4pm-8pm., Attend Women&#x27;s soccer game as a group., Semester social event. Optional attendance., Attend school football game as a group., Students can stop by the table to get information on the club and register.
    - location (TEXT) # Example values: MU 215, Campus Soccer/Lacrosse stadium, 900 E. Washington St., Campus Football stadium, Campus Common
    - status (TEXT) # Example values: Open, Planning, Closed

Table: major
  Columns:
    - major_id (TEXT) (Primary Key) # Example values: rec06DF6vZ1CyPKpc, rec09LedkREyskCNv, rec0Eanv576RhQllI, rec0xRZtkzxrg8kj2, rec1N0upiVLy5esTO
    - major_name (TEXT) # Example values: Outdoor Product Design and Development, Agricultural Communication, Fisheries and Aquatic Sciences, Finance, Forest Ecology and Management
    - department (TEXT) # Example values: School of Applied Sciences, Technology and Education, Watershed Sciences Department, Economics and Finance Department, Wildland Resources Department, Biological Engineering Department
    - college (TEXT) # Example values: College of Agriculture and Applied Sciences, College of Natural Resources, School of Business, College of Engineering, College of Humanities and Social Sciences

Table: zip_code
  Columns:
    - zip_code (INTEGER) (Primary Key) # Example values: 501, 544, 601, 602, 603
    - type (TEXT) # Example values: Unique, Standard, PO Box
    - city (TEXT) # Example values: Holtsville, Adjuntas, Aguada, Aguadilla, Maricao
    - county (TEXT) # Example values: Suffolk County, Adjuntas Municipio, Aguada Municipio, Aguadilla Municipio, Maricao Municipio
    - state (TEXT) # Example values: New York, Puerto Rico, Massachusetts, Rhode Island, New Hampshire
    - short_state (TEXT) # Example values: NY, PR, MA, RI, NH

Table: attendance
  Columns:
    - link_to_event (TEXT) (Primary Key) # Example values: rec2N69DMcrqN9PJC, rec5XDvJLyxDsGZWc, recEVTik3MlqbvLFi, recGxVCwaLW3mDIa3, recI43CzsZ0Q625ma
    - link_to_member (TEXT) (Primary Key) # Example values: recD078PnS3x2doBe, recP6DJPyi5donvXL, rec28ORZgcm1dtqBZ, recTjHY5xXhvkCdVT, recZ4PkGERzl9ziHO

Table: budget
  Columns:
    - budget_id (TEXT) (Primary Key) # Example values: rec0QmEc3cSQFQ6V2, rec1bG6HSft7XIvTP, rec1z6ISJU2HdIsVm, rec33PFqxLtnp80RJ, rec4DYUKBHMPZXWB2
    - category (TEXT) # Example values: Advertisement, Food, Speaker Gifts, Parking, Club T-Shirts
    - spent (REAL) # Example values: 67.81, 121.14, 20.2, 0.0, 173.06
    - remaining (REAL) # Example values: 7.19, 28.86, -0.199999999999999, 25.0, 150.0
    - amount (INTEGER) # Example values: 75, 150, 20, 25, 10
    - event_status (TEXT) # Example values: Closed, Open, Planning
    - link_to_event (TEXT) # Example values: recI43CzsZ0Q625ma, recggMW2eyCYceNcy, recJ4Witp9tpjaugn, recHaMmaKyfktt5fW, recs4x1BYWAsU2SKg

Table: expense
  Columns:
    - expense_id (TEXT) (Primary Key) # Example values: rec017x6R3hQqkLAo, rec1nIjoZKTYayqZ6, rec1oMgNFt7Y0G40x, rec4Zg7WEmfiHXcnC, rec7gUiykKKW4RaJS
    - expense_description (TEXT) # Example values: Post Cards, Posters, Water, Cookies, Pizza, Posters, Parking
    - expense_date (TEXT) # Example values: 2019-08-20, 2019-10-08, 2019-09-10, 2019-10-10, 2019-11-19
    - cost (REAL) # Example values: 122.06, 20.2, 51.81, 67.81, 6.0
    - approved (TEXT) # Example values: true
    - link_to_member (TEXT) # Example values: rec4BLdZHS2Blfp4v, recro8T1MPMwRadVH, recD078PnS3x2doBe
    - link_to_budget (TEXT) # Example values: recvKTAWAFKkVNnXQ, recy8KY5bUdzF81vv, recwXIiKoBMjXJsGZ, recsI0IzpUuxl2bPh, recTUGXxhTaFZ2qkg

Table: income
  Columns:
    - income_id (TEXT) (Primary Key) # Example values: rec0s9ZrO15zhzUeE, rec7f5XMQZexgtQJo, rec8BUJa8GXUjiglg, rec8V9BPNIoewWt2z, recCRWMfFqifuKMc6
    - date_received (TEXT) # Example values: 2019-10-17, 2019-09-04, 2019-10-08, 2019-10-02, 2019-09-18
    - amount (INTEGER) # Example values: 50, 200, 3000, 1000
    - source (TEXT) # Example values: Dues, Fundraising, School Appropration, Sponsorship
    - notes (TEXT) # Example values: Secured donations to help pay for speaker gifts., Annual funding from Student Government., Ad revenue for use on flyers used to advertise upcoming events.
    - link_to_member (TEXT) # Example values: reccW7q1KkhSKZsea, recTjHY5xXhvkCdVT, recUdRhbhcEO1Hk5r, rec3pH4DxMcWHMRB7, rec28ORZgcm1dtqBZ

Table: member
  Columns:
    - member_id (TEXT) (Primary Key) # Example values: rec1x5zBFIqoOuPW8, rec280Sk7o31iG0Tx, rec28ORZgcm1dtqBZ, rec2a03QXbFQAUZ7X, rec3pH4DxMcWHMRB7
    - first_name (TEXT) # Example values: Angela, Grant, Luisa, Randy, Connor
    - last_name (TEXT) # Example values: Sanders, Gilmour, Guidi, Woodard, Hilton
    - email (TEXT) # Example values: angela.sanders@lpu.edu, grant.gilmour@lpu.edu, luisa.guidi@lpu.edu, randy.woodard@lpu.edu, connor.hilton@lpu.edu
    - position (TEXT) # Example values: Member, Inactive, President, Vice President, Treasurer
    - t_shirt_size (TEXT) # Example values: Medium, X-Large, Large, Small
    - phone (TEXT) # Example values: (651) 928-4507, 403-555-1310, 442-555-5882, 490-555-8460, 454-555-7970
    - zip (INTEGER) # Example values: 55108, 29440, 10002, 8021, 48236
    - link_to_major (TEXT) # Example values: recxK3MHQFbR9J5uO, rec7BxKpjJ7bNph3O, recdIBgeU38UbV2sy, recaJdSK83k6ekRJL, recCk8lCDOTRp6rKN


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., sqlite.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

***Hints***
- meetings events refers to type = &#x27;Meeting&#x27;
- attended by more than 10 members refers to COUNT(event_id) &gt; 10

Question: Among the events attended by more than 10 members of the Student_Club, how many of them are meetings?</pre>

---

### ❓ Failed Question #16 (of 20 examples) - Question ID: `1387`


**Question**: Which student has been entrusted to manage the budget for the Yearly Kickoff?


### ✅ Ground Truth SQL(s)

```sql
SELECT T4.first_name, T4.last_name FROM event AS T1 INNER JOIN budget AS T2 ON T1.event_id = T2.link_to_event INNER JOIN expense AS T3 ON T2.budget_id = T3.link_to_budget INNER JOIN member AS T4 ON T3.link_to_member = T4.member_id WHERE T1.event_name = 'Yearly Kickoff'
```

### ❌ Predicted SQL

```sql
SELECT DISTINCT
  T1.first_name,
  T1.last_name
FROM member AS T1
JOIN expense AS T2
  ON T1.member_id = T2.link_to_member
JOIN budget AS T3
  ON T2.link_to_budget = T3.budget_id
JOIN event AS T4
  ON T3.link_to_event = T4.event_id
WHERE
  T4.event_name = 'Yearly Kickoff'
```

### 📊 Evaluation Metrics

|   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |   execution_time_ms |
|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|--------------------:|
|                    0 |                              0 |                                     0 |                          0 |                         1 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          0 |            2596 |                 119 |          10288 |             40625.1 |               55.68 |

### 📘 Ground Truth Result(s)

**Result 1:**

| first_name   | last_name   |
|:-------------|:------------|
| Sacha        | Harrison    |
| Elijah       | Allen       |
| Elijah       | Allen       |
| Sacha        | Harrison    |

### 📕 Predicted Result

| first_name   | last_name   |
|:-------------|:------------|
| Sacha        | Harrison    |
| Elijah       | Allen       |

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given sqlite database schema.

**Question:**:
Which student has been entrusted to manage the budget for the Yearly Kickoff?

**Database Engine / Dialect:**:
sqlite

**Schema:**
Table: event
  Columns:
    - event_id (TEXT) (Primary Key) # Example values: rec0Si5cQ4rJRVzd6, rec0akZnLLpGUloLH, rec0dZPcWXF0QjNnE, rec180D2MI4EpckHy, rec2N69DMcrqN9PJC
    - event_name (TEXT) # Example values: March Meeting, Officers meeting - January, Spring Elections, Officers meeting - March, Women&#x27;s Soccer
    - event_date (TEXT) # Example values: 2020-03-10T12:00:00, 2020-01-14T09:30:00, 2019-11-24T09:00:00, 2020-03-10T09:30:00, 2019-10-05T12:00:00
    - type (TEXT) # Example values: Meeting, Election, Game, Guest Speaker, Social
    - notes (TEXT) # Example values: All active members can vote for new officers between 4pm-8pm., Attend Women&#x27;s soccer game as a group., Semester social event. Optional attendance., Attend school football game as a group., Students can stop by the table to get information on the club and register.
    - location (TEXT) # Example values: MU 215, Campus Soccer/Lacrosse stadium, 900 E. Washington St., Campus Football stadium, Campus Common
    - status (TEXT) # Example values: Open, Planning, Closed

Table: major
  Columns:
    - major_id (TEXT) (Primary Key) # Example values: rec06DF6vZ1CyPKpc, rec09LedkREyskCNv, rec0Eanv576RhQllI, rec0xRZtkzxrg8kj2, rec1N0upiVLy5esTO
    - major_name (TEXT) # Example values: Outdoor Product Design and Development, Agricultural Communication, Fisheries and Aquatic Sciences, Finance, Forest Ecology and Management
    - department (TEXT) # Example values: School of Applied Sciences, Technology and Education, Watershed Sciences Department, Economics and Finance Department, Wildland Resources Department, Biological Engineering Department
    - college (TEXT) # Example values: College of Agriculture and Applied Sciences, College of Natural Resources, School of Business, College of Engineering, College of Humanities and Social Sciences

Table: zip_code
  Columns:
    - zip_code (INTEGER) (Primary Key) # Example values: 501, 544, 601, 602, 603
    - type (TEXT) # Example values: Unique, Standard, PO Box
    - city (TEXT) # Example values: Holtsville, Adjuntas, Aguada, Aguadilla, Maricao
    - county (TEXT) # Example values: Suffolk County, Adjuntas Municipio, Aguada Municipio, Aguadilla Municipio, Maricao Municipio
    - state (TEXT) # Example values: New York, Puerto Rico, Massachusetts, Rhode Island, New Hampshire
    - short_state (TEXT) # Example values: NY, PR, MA, RI, NH

Table: attendance
  Columns:
    - link_to_event (TEXT) (Primary Key) # Example values: rec2N69DMcrqN9PJC, rec5XDvJLyxDsGZWc, recEVTik3MlqbvLFi, recGxVCwaLW3mDIa3, recI43CzsZ0Q625ma
    - link_to_member (TEXT) (Primary Key) # Example values: recD078PnS3x2doBe, recP6DJPyi5donvXL, rec28ORZgcm1dtqBZ, recTjHY5xXhvkCdVT, recZ4PkGERzl9ziHO

Table: budget
  Columns:
    - budget_id (TEXT) (Primary Key) # Example values: rec0QmEc3cSQFQ6V2, rec1bG6HSft7XIvTP, rec1z6ISJU2HdIsVm, rec33PFqxLtnp80RJ, rec4DYUKBHMPZXWB2
    - category (TEXT) # Example values: Advertisement, Food, Speaker Gifts, Parking, Club T-Shirts
    - spent (REAL) # Example values: 67.81, 121.14, 20.2, 0.0, 173.06
    - remaining (REAL) # Example values: 7.19, 28.86, -0.199999999999999, 25.0, 150.0
    - amount (INTEGER) # Example values: 75, 150, 20, 25, 10
    - event_status (TEXT) # Example values: Closed, Open, Planning
    - link_to_event (TEXT) # Example values: recI43CzsZ0Q625ma, recggMW2eyCYceNcy, recJ4Witp9tpjaugn, recHaMmaKyfktt5fW, recs4x1BYWAsU2SKg

Table: expense
  Columns:
    - expense_id (TEXT) (Primary Key) # Example values: rec017x6R3hQqkLAo, rec1nIjoZKTYayqZ6, rec1oMgNFt7Y0G40x, rec4Zg7WEmfiHXcnC, rec7gUiykKKW4RaJS
    - expense_description (TEXT) # Example values: Post Cards, Posters, Water, Cookies, Pizza, Posters, Parking
    - expense_date (TEXT) # Example values: 2019-08-20, 2019-10-08, 2019-09-10, 2019-10-10, 2019-11-19
    - cost (REAL) # Example values: 122.06, 20.2, 51.81, 67.81, 6.0
    - approved (TEXT) # Example values: true
    - link_to_member (TEXT) # Example values: rec4BLdZHS2Blfp4v, recro8T1MPMwRadVH, recD078PnS3x2doBe
    - link_to_budget (TEXT) # Example values: recvKTAWAFKkVNnXQ, recy8KY5bUdzF81vv, recwXIiKoBMjXJsGZ, recsI0IzpUuxl2bPh, recTUGXxhTaFZ2qkg

Table: income
  Columns:
    - income_id (TEXT) (Primary Key) # Example values: rec0s9ZrO15zhzUeE, rec7f5XMQZexgtQJo, rec8BUJa8GXUjiglg, rec8V9BPNIoewWt2z, recCRWMfFqifuKMc6
    - date_received (TEXT) # Example values: 2019-10-17, 2019-09-04, 2019-10-08, 2019-10-02, 2019-09-18
    - amount (INTEGER) # Example values: 50, 200, 3000, 1000
    - source (TEXT) # Example values: Dues, Fundraising, School Appropration, Sponsorship
    - notes (TEXT) # Example values: Secured donations to help pay for speaker gifts., Annual funding from Student Government., Ad revenue for use on flyers used to advertise upcoming events.
    - link_to_member (TEXT) # Example values: reccW7q1KkhSKZsea, recTjHY5xXhvkCdVT, recUdRhbhcEO1Hk5r, rec3pH4DxMcWHMRB7, rec28ORZgcm1dtqBZ

Table: member
  Columns:
    - member_id (TEXT) (Primary Key) # Example values: rec1x5zBFIqoOuPW8, rec280Sk7o31iG0Tx, rec28ORZgcm1dtqBZ, rec2a03QXbFQAUZ7X, rec3pH4DxMcWHMRB7
    - first_name (TEXT) # Example values: Angela, Grant, Luisa, Randy, Connor
    - last_name (TEXT) # Example values: Sanders, Gilmour, Guidi, Woodard, Hilton
    - email (TEXT) # Example values: angela.sanders@lpu.edu, grant.gilmour@lpu.edu, luisa.guidi@lpu.edu, randy.woodard@lpu.edu, connor.hilton@lpu.edu
    - position (TEXT) # Example values: Member, Inactive, President, Vice President, Treasurer
    - t_shirt_size (TEXT) # Example values: Medium, X-Large, Large, Small
    - phone (TEXT) # Example values: (651) 928-4507, 403-555-1310, 442-555-5882, 490-555-8460, 454-555-7970
    - zip (INTEGER) # Example values: 55108, 29440, 10002, 8021, 48236
    - link_to_major (TEXT) # Example values: recxK3MHQFbR9J5uO, rec7BxKpjJ7bNph3O, recdIBgeU38UbV2sy, recaJdSK83k6ekRJL, recCk8lCDOTRp6rKN


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., sqlite.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

***Hints***
- name of students means the full name
- full name refers to first_name, last_name;&#x27;Yearly Kickoff&#x27; is an event name;

Question: Which student has been entrusted to manage the budget for the Yearly Kickoff?</pre>

---

### ❓ Failed Question #17 (of 20 examples) - Question ID: `1399`


**Question**: Did Maya Mclean attend the 'Women's Soccer' event?


### ✅ Ground Truth SQL(s)

```sql
SELECT CASE WHEN T3.event_name = 'Women''s Soccer' THEN 'YES' END AS result FROM member AS T1 INNER JOIN attendance AS T2 ON T1.member_id = T2.link_to_member INNER JOIN event AS T3 ON T2.link_to_event = T3.event_id WHERE T1.first_name = 'Maya' AND T1.last_name = 'Mclean'
```

### ❌ Predicted SQL

```sql
SELECT
  T1.link_to_member
FROM attendance AS T1
JOIN member AS T2
  ON T1.link_to_member = T2.member_id
JOIN event AS T3
  ON T1.link_to_event = T3.event_id
WHERE
  T2.first_name = 'Maya' AND T2.last_name = 'Mclean' AND T3.event_name = 'Women''s Soccer'
```

### 📊 Evaluation Metrics

|   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |   execution_time_ms |
|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|--------------------:|
|                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          0 |            2596 |                 110 |           4196 |             8952.46 |               58.31 |

### 📘 Ground Truth Result(s)

**Result 1:**

| result   |
|:---------|
| YES      |
|          |
|          |
|          |
|          |
|          |
|          |
|          |
|          |
|          |
|          |
|          |
|          |
|          |

### 📕 Predicted Result

| link_to_member    |
|:------------------|
| recZ4PkGERzl9ziHO |

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given sqlite database schema.

**Question:**:
Did Maya Mclean attend the &#x27;Women&#x27;s Soccer&#x27; event?

**Database Engine / Dialect:**:
sqlite

**Schema:**
Table: event
  Columns:
    - event_id (TEXT) (Primary Key) # Example values: rec0Si5cQ4rJRVzd6, rec0akZnLLpGUloLH, rec0dZPcWXF0QjNnE, rec180D2MI4EpckHy, rec2N69DMcrqN9PJC
    - event_name (TEXT) # Example values: March Meeting, Officers meeting - January, Spring Elections, Officers meeting - March, Women&#x27;s Soccer
    - event_date (TEXT) # Example values: 2020-03-10T12:00:00, 2020-01-14T09:30:00, 2019-11-24T09:00:00, 2020-03-10T09:30:00, 2019-10-05T12:00:00
    - type (TEXT) # Example values: Meeting, Election, Game, Guest Speaker, Social
    - notes (TEXT) # Example values: All active members can vote for new officers between 4pm-8pm., Attend Women&#x27;s soccer game as a group., Semester social event. Optional attendance., Attend school football game as a group., Students can stop by the table to get information on the club and register.
    - location (TEXT) # Example values: MU 215, Campus Soccer/Lacrosse stadium, 900 E. Washington St., Campus Football stadium, Campus Common
    - status (TEXT) # Example values: Open, Planning, Closed

Table: major
  Columns:
    - major_id (TEXT) (Primary Key) # Example values: rec06DF6vZ1CyPKpc, rec09LedkREyskCNv, rec0Eanv576RhQllI, rec0xRZtkzxrg8kj2, rec1N0upiVLy5esTO
    - major_name (TEXT) # Example values: Outdoor Product Design and Development, Agricultural Communication, Fisheries and Aquatic Sciences, Finance, Forest Ecology and Management
    - department (TEXT) # Example values: School of Applied Sciences, Technology and Education, Watershed Sciences Department, Economics and Finance Department, Wildland Resources Department, Biological Engineering Department
    - college (TEXT) # Example values: College of Agriculture and Applied Sciences, College of Natural Resources, School of Business, College of Engineering, College of Humanities and Social Sciences

Table: zip_code
  Columns:
    - zip_code (INTEGER) (Primary Key) # Example values: 501, 544, 601, 602, 603
    - type (TEXT) # Example values: Unique, Standard, PO Box
    - city (TEXT) # Example values: Holtsville, Adjuntas, Aguada, Aguadilla, Maricao
    - county (TEXT) # Example values: Suffolk County, Adjuntas Municipio, Aguada Municipio, Aguadilla Municipio, Maricao Municipio
    - state (TEXT) # Example values: New York, Puerto Rico, Massachusetts, Rhode Island, New Hampshire
    - short_state (TEXT) # Example values: NY, PR, MA, RI, NH

Table: attendance
  Columns:
    - link_to_event (TEXT) (Primary Key) # Example values: rec2N69DMcrqN9PJC, rec5XDvJLyxDsGZWc, recEVTik3MlqbvLFi, recGxVCwaLW3mDIa3, recI43CzsZ0Q625ma
    - link_to_member (TEXT) (Primary Key) # Example values: recD078PnS3x2doBe, recP6DJPyi5donvXL, rec28ORZgcm1dtqBZ, recTjHY5xXhvkCdVT, recZ4PkGERzl9ziHO

Table: budget
  Columns:
    - budget_id (TEXT) (Primary Key) # Example values: rec0QmEc3cSQFQ6V2, rec1bG6HSft7XIvTP, rec1z6ISJU2HdIsVm, rec33PFqxLtnp80RJ, rec4DYUKBHMPZXWB2
    - category (TEXT) # Example values: Advertisement, Food, Speaker Gifts, Parking, Club T-Shirts
    - spent (REAL) # Example values: 67.81, 121.14, 20.2, 0.0, 173.06
    - remaining (REAL) # Example values: 7.19, 28.86, -0.199999999999999, 25.0, 150.0
    - amount (INTEGER) # Example values: 75, 150, 20, 25, 10
    - event_status (TEXT) # Example values: Closed, Open, Planning
    - link_to_event (TEXT) # Example values: recI43CzsZ0Q625ma, recggMW2eyCYceNcy, recJ4Witp9tpjaugn, recHaMmaKyfktt5fW, recs4x1BYWAsU2SKg

Table: expense
  Columns:
    - expense_id (TEXT) (Primary Key) # Example values: rec017x6R3hQqkLAo, rec1nIjoZKTYayqZ6, rec1oMgNFt7Y0G40x, rec4Zg7WEmfiHXcnC, rec7gUiykKKW4RaJS
    - expense_description (TEXT) # Example values: Post Cards, Posters, Water, Cookies, Pizza, Posters, Parking
    - expense_date (TEXT) # Example values: 2019-08-20, 2019-10-08, 2019-09-10, 2019-10-10, 2019-11-19
    - cost (REAL) # Example values: 122.06, 20.2, 51.81, 67.81, 6.0
    - approved (TEXT) # Example values: true
    - link_to_member (TEXT) # Example values: rec4BLdZHS2Blfp4v, recro8T1MPMwRadVH, recD078PnS3x2doBe
    - link_to_budget (TEXT) # Example values: recvKTAWAFKkVNnXQ, recy8KY5bUdzF81vv, recwXIiKoBMjXJsGZ, recsI0IzpUuxl2bPh, recTUGXxhTaFZ2qkg

Table: income
  Columns:
    - income_id (TEXT) (Primary Key) # Example values: rec0s9ZrO15zhzUeE, rec7f5XMQZexgtQJo, rec8BUJa8GXUjiglg, rec8V9BPNIoewWt2z, recCRWMfFqifuKMc6
    - date_received (TEXT) # Example values: 2019-10-17, 2019-09-04, 2019-10-08, 2019-10-02, 2019-09-18
    - amount (INTEGER) # Example values: 50, 200, 3000, 1000
    - source (TEXT) # Example values: Dues, Fundraising, School Appropration, Sponsorship
    - notes (TEXT) # Example values: Secured donations to help pay for speaker gifts., Annual funding from Student Government., Ad revenue for use on flyers used to advertise upcoming events.
    - link_to_member (TEXT) # Example values: reccW7q1KkhSKZsea, recTjHY5xXhvkCdVT, recUdRhbhcEO1Hk5r, rec3pH4DxMcWHMRB7, rec28ORZgcm1dtqBZ

Table: member
  Columns:
    - member_id (TEXT) (Primary Key) # Example values: rec1x5zBFIqoOuPW8, rec280Sk7o31iG0Tx, rec28ORZgcm1dtqBZ, rec2a03QXbFQAUZ7X, rec3pH4DxMcWHMRB7
    - first_name (TEXT) # Example values: Angela, Grant, Luisa, Randy, Connor
    - last_name (TEXT) # Example values: Sanders, Gilmour, Guidi, Woodard, Hilton
    - email (TEXT) # Example values: angela.sanders@lpu.edu, grant.gilmour@lpu.edu, luisa.guidi@lpu.edu, randy.woodard@lpu.edu, connor.hilton@lpu.edu
    - position (TEXT) # Example values: Member, Inactive, President, Vice President, Treasurer
    - t_shirt_size (TEXT) # Example values: Medium, X-Large, Large, Small
    - phone (TEXT) # Example values: (651) 928-4507, 403-555-1310, 442-555-5882, 490-555-8460, 454-555-7970
    - zip (INTEGER) # Example values: 55108, 29440, 10002, 8021, 48236
    - link_to_major (TEXT) # Example values: recxK3MHQFbR9J5uO, rec7BxKpjJ7bNph3O, recdIBgeU38UbV2sy, recaJdSK83k6ekRJL, recCk8lCDOTRp6rKN


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., sqlite.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

***Hints***
- Maya Mclean is the full name
- full name refers to first_name, last_name
- &#x27;Women&#x27;s Soccer&#x27; is an event_name

Question: Did Maya Mclean attend the &#x27;Women&#x27;s Soccer&#x27; event?</pre>

---

### ⚠️  Inference Failed - Question #18 (of 20 examples) - Question ID: `1403`

**Question**: Indicate the name of the closed event whose cost has exceeded the budget the most.

### ❌ Inference Error
```
Failed to get response from Gemini API: 503 UNAVAILABLE. {'error': {'code': 503, 'message': 'This model is currently experiencing high demand. Spikes in demand are usually temporary. Please try again later.', 'status': 'UNAVAILABLE', 'details': [{'@type': 'type.googleapis.com/google.rpc.DebugInfo', 'detail': '[ORIGINAL ERROR] generic::unavailable: Preempted out of decode queue by a higher priority request.;  Failed to run inference for model: go/debugproto   \nname: "prod-common-global__/aistudio/gemini-v4s-rev23-fiercefalcon-tc__main__/aistudio/gemini-v4s-rev23-fiercefalcon-tc__2026030500__decode__variant__b20b7bb1-fff7-4a18-8f15-b307ba2b1544"\nversion {\n  value: 1\n}\nsignature_name: "serving_default"\n; RPC from prefill servable to decode servable failed; Failed to close the streaming context; status = UNAVAILABLE: Preempted out of decode queue by a higher priority request.;  Failed to run inference for model: go/debugproto   \nname: "prod-common-global__/aistudio/gemini-v4s-rev23-fiercefalcon-tc__main__/aistudio/gemini-v4s-rev23-fiercefalcon-tc__2026030500__decode__variant__b20b7bb1-fff7-4a18-8f15-b307ba2b1544"\nversion {\n  value: 1\n}\nsignature_name: "serving_default"\n; RPC from prefill servable to decode servable failed [type.googleapis.com/util.MessageSetPayload=\'[jax.wiz.servo.ServoErrorDetail] { error_code: DECODE_PREEMPTED }\']\n=== Source Location Trace: === \nnet/rpc/common/stream/stream-context.cc:1470\nlearning/serving/servables/wiz/remote_wiz_servable.cc:227\nlearning/serving/servables/wiz/prefill_remote_wiz_servable.cc:221\nlearning/serving/servables/wiz/wiz_servable.cc:3185\n;  Failed to run inference for model: go/debugonly   \nname: "prod-common-global__/aistudio/gemini-v4s-rev23-fiercefalcon-tc__main__/aistudio/gemini-v4s-rev23-fiercefalcon-tc__2026030500__prefill__variant__225e3315-9c9a-4e33-b259-9899676b5941"\nversion {\n  value: 1\n}\nsignature_name: "serving_default"\n [google.rpc.error_details_ext] { message: "This model is currently experiencing high demand. Spikes in demand are usually temporary. Please try again later." details { type_url: "type.googleapis.com/language_labs.genai.debug.GeminiApiDebugInfo" value: "\\212\\001\\310\\014\\n\\222\\014Preempted out of decode queue by a higher priority request.;  Failed to run inference for model: go/debugproto   \\nname: \\"prod-common-global__/aistudio/gemini-v4s-rev23-fiercefalcon-tc__main__/aistudio/gemini-v4s-rev23-fiercefalcon-tc__2026030500__decode__variant__b20b7bb1-fff7-4a18-8f15-b307ba2b1544\\"\\nversion {\\n  value: 1\\n}\\nsignature_name: \\"serving_default\\"\\n; RPC from prefill servable to decode servable failed; Failed to close the streaming context; status = UNAVAILABLE: Preempted out of decode queue by a higher priority request.;  Failed to run inference for model: go/debugproto   \\nname: \\"prod-common-global__/aistudio/gemini-v4s-rev23-fiercefalcon-tc__main__/aistudio/gemini-v4s-rev23-fiercefalcon-tc__2026030500__decode__variant__b20b7bb1-fff7-4a18-8f15-b307ba2b1544\\"\\nversion {\\n  value: 1\\n}\\nsignature_name: \\"serving_default\\"\\n; RPC from prefill servable to decode servable failed [type.googleapis.com/util.MessageSetPayload=\\\'[jax.wiz.servo.ServoErrorDetail] { error_code: DECODE_PREEMPTED }\\\']\\n=== Source Location Trace: === \\nnet/rpc/common/stream/stream-context.cc:1470\\nlearning/serving/servables/wiz/remote_wiz_servable.cc:227\\nlearning/serving/servables/wiz/prefill_remote_wiz_servable.cc:221\\nlearning/serving/servables/wiz/wiz_servable.cc:3185\\n;  Failed to run inference for model: go/debugonly   \\nname: \\"prod-common-global__/aistudio/gemini-v4s-rev23-fiercefalcon-tc__main__/aistudio/gemini-v4s-rev23-fiercefalcon-tc__2026030500__prefill__variant__225e3315-9c9a-4e33-b259-9899676b5941\\"\\nversion {\\n  value: 1\\n}\\nsignature_name: \\"serving_default\\"\\n\\0221labs/language/genai/common/error_handler.cc:291:0" } } 525005957 { 1: 13 }'}]}}
```
### 📝 Prompt Used
```
Your task is to convert a natural language question into an accurate SQL query using the given sqlite database schema.

**Question:**:
Indicate the name of the closed event whose cost has exceeded the budget the most.

**Database Engine / Dialect:**:
sqlite

**Schema:**
Table: event
  Columns:
    - event_id (TEXT) (Primary Key) # Example values: rec0Si5cQ4rJRVzd6, rec0akZnLLpGUloLH, rec0dZPcWXF0QjNnE, rec180D2MI4EpckHy, rec2N69DMcrqN9PJC
    - event_name (TEXT) # Example values: March Meeting, 
…
 and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., sqlite.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

***Hints***
- closed events refers to event_name where status = 'Closed'
- exceed the budget the most refers to MIN(remaining) where remaining < 0

Question: Indicate the name of the closed event whose cost has exceeded the budget the most.
```
---


---

### ❓ Failed Question #19 (of 20 examples) - Question ID: `1404`


**Question**: Identify the type of expenses and their total value approved for 'October Meeting' event.


### ✅ Ground Truth SQL(s)

```sql
SELECT T1.type, SUM(T3.cost) FROM event AS T1 INNER JOIN budget AS T2 ON T1.event_id = T2.link_to_event INNER JOIN expense AS T3 ON T2.budget_id = T3.link_to_budget WHERE T1.event_name = 'October Meeting'
```

### ❌ Predicted SQL

```sql
SELECT
  T2.category,
  SUM(T3.cost)
FROM event AS T1
JOIN budget AS T2
  ON T1.event_id = T2.link_to_event
JOIN expense AS T3
  ON T2.budget_id = T3.link_to_budget
WHERE
  T1.event_name = 'October Meeting' AND T3.approved = 'true'
GROUP BY
  T2.category
```

### 📊 Evaluation Metrics

|   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |   execution_time_ms |
|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|--------------------:|
|                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          0 |            2588 |                 110 |           3483 |             5528.22 |               41.78 |

### 📘 Ground Truth Result(s)

**Result 1:**

| type    |   SUM(T3.cost) |
|:--------|---------------:|
| Meeting |         175.39 |

### 📕 Predicted Result

| category      |   SUM(T3.cost) |
|:--------------|---------------:|
| Advertisement |          54.25 |
| Food          |         121.14 |

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given sqlite database schema.

**Question:**:
Identify the type of expenses and their total value approved for &#x27;October Meeting&#x27; event.

**Database Engine / Dialect:**:
sqlite

**Schema:**
Table: event
  Columns:
    - event_id (TEXT) (Primary Key) # Example values: rec0Si5cQ4rJRVzd6, rec0akZnLLpGUloLH, rec0dZPcWXF0QjNnE, rec180D2MI4EpckHy, rec2N69DMcrqN9PJC
    - event_name (TEXT) # Example values: March Meeting, Officers meeting - January, Spring Elections, Officers meeting - March, Women&#x27;s Soccer
    - event_date (TEXT) # Example values: 2020-03-10T12:00:00, 2020-01-14T09:30:00, 2019-11-24T09:00:00, 2020-03-10T09:30:00, 2019-10-05T12:00:00
    - type (TEXT) # Example values: Meeting, Election, Game, Guest Speaker, Social
    - notes (TEXT) # Example values: All active members can vote for new officers between 4pm-8pm., Attend Women&#x27;s soccer game as a group., Semester social event. Optional attendance., Attend school football game as a group., Students can stop by the table to get information on the club and register.
    - location (TEXT) # Example values: MU 215, Campus Soccer/Lacrosse stadium, 900 E. Washington St., Campus Football stadium, Campus Common
    - status (TEXT) # Example values: Open, Planning, Closed

Table: major
  Columns:
    - major_id (TEXT) (Primary Key) # Example values: rec06DF6vZ1CyPKpc, rec09LedkREyskCNv, rec0Eanv576RhQllI, rec0xRZtkzxrg8kj2, rec1N0upiVLy5esTO
    - major_name (TEXT) # Example values: Outdoor Product Design and Development, Agricultural Communication, Fisheries and Aquatic Sciences, Finance, Forest Ecology and Management
    - department (TEXT) # Example values: School of Applied Sciences, Technology and Education, Watershed Sciences Department, Economics and Finance Department, Wildland Resources Department, Biological Engineering Department
    - college (TEXT) # Example values: College of Agriculture and Applied Sciences, College of Natural Resources, School of Business, College of Engineering, College of Humanities and Social Sciences

Table: zip_code
  Columns:
    - zip_code (INTEGER) (Primary Key) # Example values: 501, 544, 601, 602, 603
    - type (TEXT) # Example values: Unique, Standard, PO Box
    - city (TEXT) # Example values: Holtsville, Adjuntas, Aguada, Aguadilla, Maricao
    - county (TEXT) # Example values: Suffolk County, Adjuntas Municipio, Aguada Municipio, Aguadilla Municipio, Maricao Municipio
    - state (TEXT) # Example values: New York, Puerto Rico, Massachusetts, Rhode Island, New Hampshire
    - short_state (TEXT) # Example values: NY, PR, MA, RI, NH

Table: attendance
  Columns:
    - link_to_event (TEXT) (Primary Key) # Example values: rec2N69DMcrqN9PJC, rec5XDvJLyxDsGZWc, recEVTik3MlqbvLFi, recGxVCwaLW3mDIa3, recI43CzsZ0Q625ma
    - link_to_member (TEXT) (Primary Key) # Example values: recD078PnS3x2doBe, recP6DJPyi5donvXL, rec28ORZgcm1dtqBZ, recTjHY5xXhvkCdVT, recZ4PkGERzl9ziHO

Table: budget
  Columns:
    - budget_id (TEXT) (Primary Key) # Example values: rec0QmEc3cSQFQ6V2, rec1bG6HSft7XIvTP, rec1z6ISJU2HdIsVm, rec33PFqxLtnp80RJ, rec4DYUKBHMPZXWB2
    - category (TEXT) # Example values: Advertisement, Food, Speaker Gifts, Parking, Club T-Shirts
    - spent (REAL) # Example values: 67.81, 121.14, 20.2, 0.0, 173.06
    - remaining (REAL) # Example values: 7.19, 28.86, -0.199999999999999, 25.0, 150.0
    - amount (INTEGER) # Example values: 75, 150, 20, 25, 10
    - event_status (TEXT) # Example values: Closed, Open, Planning
    - link_to_event (TEXT) # Example values: recI43CzsZ0Q625ma, recggMW2eyCYceNcy, recJ4Witp9tpjaugn, recHaMmaKyfktt5fW, recs4x1BYWAsU2SKg

Table: expense
  Columns:
    - expense_id (TEXT) (Primary Key) # Example values: rec017x6R3hQqkLAo, rec1nIjoZKTYayqZ6, rec1oMgNFt7Y0G40x, rec4Zg7WEmfiHXcnC, rec7gUiykKKW4RaJS
    - expense_description (TEXT) # Example values: Post Cards, Posters, Water, Cookies, Pizza, Posters, Parking
    - expense_date (TEXT) # Example values: 2019-08-20, 2019-10-08, 2019-09-10, 2019-10-10, 2019-11-19
    - cost (REAL) # Example values: 122.06, 20.2, 51.81, 67.81, 6.0
    - approved (TEXT) # Example values: true
    - link_to_member (TEXT) # Example values: rec4BLdZHS2Blfp4v, recro8T1MPMwRadVH, recD078PnS3x2doBe
    - link_to_budget (TEXT) # Example values: recvKTAWAFKkVNnXQ, recy8KY5bUdzF81vv, recwXIiKoBMjXJsGZ, recsI0IzpUuxl2bPh, recTUGXxhTaFZ2qkg

Table: income
  Columns:
    - income_id (TEXT) (Primary Key) # Example values: rec0s9ZrO15zhzUeE, rec7f5XMQZexgtQJo, rec8BUJa8GXUjiglg, rec8V9BPNIoewWt2z, recCRWMfFqifuKMc6
    - date_received (TEXT) # Example values: 2019-10-17, 2019-09-04, 2019-10-08, 2019-10-02, 2019-09-18
    - amount (INTEGER) # Example values: 50, 200, 3000, 1000
    - source (TEXT) # Example values: Dues, Fundraising, School Appropration, Sponsorship
    - notes (TEXT) # Example values: Secured donations to help pay for speaker gifts., Annual funding from Student Government., Ad revenue for use on flyers used to advertise upcoming events.
    - link_to_member (TEXT) # Example values: reccW7q1KkhSKZsea, recTjHY5xXhvkCdVT, recUdRhbhcEO1Hk5r, rec3pH4DxMcWHMRB7, rec28ORZgcm1dtqBZ

Table: member
  Columns:
    - member_id (TEXT) (Primary Key) # Example values: rec1x5zBFIqoOuPW8, rec280Sk7o31iG0Tx, rec28ORZgcm1dtqBZ, rec2a03QXbFQAUZ7X, rec3pH4DxMcWHMRB7
    - first_name (TEXT) # Example values: Angela, Grant, Luisa, Randy, Connor
    - last_name (TEXT) # Example values: Sanders, Gilmour, Guidi, Woodard, Hilton
    - email (TEXT) # Example values: angela.sanders@lpu.edu, grant.gilmour@lpu.edu, luisa.guidi@lpu.edu, randy.woodard@lpu.edu, connor.hilton@lpu.edu
    - position (TEXT) # Example values: Member, Inactive, President, Vice President, Treasurer
    - t_shirt_size (TEXT) # Example values: Medium, X-Large, Large, Small
    - phone (TEXT) # Example values: (651) 928-4507, 403-555-1310, 442-555-5882, 490-555-8460, 454-555-7970
    - zip (INTEGER) # Example values: 55108, 29440, 10002, 8021, 48236
    - link_to_major (TEXT) # Example values: recxK3MHQFbR9J5uO, rec7BxKpjJ7bNph3O, recdIBgeU38UbV2sy, recaJdSK83k6ekRJL, recCk8lCDOTRp6rKN


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., sqlite.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

***Hints***
- total value refers to SUM(cost)
- &#x27;October Meeting&#x27; is an event name;

Question: Identify the type of expenses and their total value approved for &#x27;October Meeting&#x27; event.</pre>

---

### ❓ Failed Question #20 (of 20 examples) - Question ID: `1422`


**Question**: State the category of events were held at MU 215.


### ✅ Ground Truth SQL(s)

```sql
SELECT DISTINCT T2.category FROM event AS T1 INNER JOIN budget AS T2 ON T1.event_id = T2.link_to_event WHERE T1.location = 'MU 215'
```

### ❌ Predicted SQL

```sql
SELECT DISTINCT type FROM event WHERE location = 'MU 215'
```

### 📊 Evaluation Metrics

|   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |   execution_time_ms |
|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|--------------------:|
|                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          0 |            2578 |                  20 |           3770 |             7273.07 |               75.48 |

### 📘 Ground Truth Result(s)

**Result 1:**

| category      |
|:--------------|
| Advertisement |
| Food          |
| Speaker Gifts |
| Parking       |

### 📕 Predicted Result

| type          |
|:--------------|
| Meeting       |
| Election      |
| Guest Speaker |

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given sqlite database schema.

**Question:**:
State the category of events were held at MU 215.

**Database Engine / Dialect:**:
sqlite

**Schema:**
Table: event
  Columns:
    - event_id (TEXT) (Primary Key) # Example values: rec0Si5cQ4rJRVzd6, rec0akZnLLpGUloLH, rec0dZPcWXF0QjNnE, rec180D2MI4EpckHy, rec2N69DMcrqN9PJC
    - event_name (TEXT) # Example values: March Meeting, Officers meeting - January, Spring Elections, Officers meeting - March, Women&#x27;s Soccer
    - event_date (TEXT) # Example values: 2020-03-10T12:00:00, 2020-01-14T09:30:00, 2019-11-24T09:00:00, 2020-03-10T09:30:00, 2019-10-05T12:00:00
    - type (TEXT) # Example values: Meeting, Election, Game, Guest Speaker, Social
    - notes (TEXT) # Example values: All active members can vote for new officers between 4pm-8pm., Attend Women&#x27;s soccer game as a group., Semester social event. Optional attendance., Attend school football game as a group., Students can stop by the table to get information on the club and register.
    - location (TEXT) # Example values: MU 215, Campus Soccer/Lacrosse stadium, 900 E. Washington St., Campus Football stadium, Campus Common
    - status (TEXT) # Example values: Open, Planning, Closed

Table: major
  Columns:
    - major_id (TEXT) (Primary Key) # Example values: rec06DF6vZ1CyPKpc, rec09LedkREyskCNv, rec0Eanv576RhQllI, rec0xRZtkzxrg8kj2, rec1N0upiVLy5esTO
    - major_name (TEXT) # Example values: Outdoor Product Design and Development, Agricultural Communication, Fisheries and Aquatic Sciences, Finance, Forest Ecology and Management
    - department (TEXT) # Example values: School of Applied Sciences, Technology and Education, Watershed Sciences Department, Economics and Finance Department, Wildland Resources Department, Biological Engineering Department
    - college (TEXT) # Example values: College of Agriculture and Applied Sciences, College of Natural Resources, School of Business, College of Engineering, College of Humanities and Social Sciences

Table: zip_code
  Columns:
    - zip_code (INTEGER) (Primary Key) # Example values: 501, 544, 601, 602, 603
    - type (TEXT) # Example values: Unique, Standard, PO Box
    - city (TEXT) # Example values: Holtsville, Adjuntas, Aguada, Aguadilla, Maricao
    - county (TEXT) # Example values: Suffolk County, Adjuntas Municipio, Aguada Municipio, Aguadilla Municipio, Maricao Municipio
    - state (TEXT) # Example values: New York, Puerto Rico, Massachusetts, Rhode Island, New Hampshire
    - short_state (TEXT) # Example values: NY, PR, MA, RI, NH

Table: attendance
  Columns:
    - link_to_event (TEXT) (Primary Key) # Example values: rec2N69DMcrqN9PJC, rec5XDvJLyxDsGZWc, recEVTik3MlqbvLFi, recGxVCwaLW3mDIa3, recI43CzsZ0Q625ma
    - link_to_member (TEXT) (Primary Key) # Example values: recD078PnS3x2doBe, recP6DJPyi5donvXL, rec28ORZgcm1dtqBZ, recTjHY5xXhvkCdVT, recZ4PkGERzl9ziHO

Table: budget
  Columns:
    - budget_id (TEXT) (Primary Key) # Example values: rec0QmEc3cSQFQ6V2, rec1bG6HSft7XIvTP, rec1z6ISJU2HdIsVm, rec33PFqxLtnp80RJ, rec4DYUKBHMPZXWB2
    - category (TEXT) # Example values: Advertisement, Food, Speaker Gifts, Parking, Club T-Shirts
    - spent (REAL) # Example values: 67.81, 121.14, 20.2, 0.0, 173.06
    - remaining (REAL) # Example values: 7.19, 28.86, -0.199999999999999, 25.0, 150.0
    - amount (INTEGER) # Example values: 75, 150, 20, 25, 10
    - event_status (TEXT) # Example values: Closed, Open, Planning
    - link_to_event (TEXT) # Example values: recI43CzsZ0Q625ma, recggMW2eyCYceNcy, recJ4Witp9tpjaugn, recHaMmaKyfktt5fW, recs4x1BYWAsU2SKg

Table: expense
  Columns:
    - expense_id (TEXT) (Primary Key) # Example values: rec017x6R3hQqkLAo, rec1nIjoZKTYayqZ6, rec1oMgNFt7Y0G40x, rec4Zg7WEmfiHXcnC, rec7gUiykKKW4RaJS
    - expense_description (TEXT) # Example values: Post Cards, Posters, Water, Cookies, Pizza, Posters, Parking
    - expense_date (TEXT) # Example values: 2019-08-20, 2019-10-08, 2019-09-10, 2019-10-10, 2019-11-19
    - cost (REAL) # Example values: 122.06, 20.2, 51.81, 67.81, 6.0
    - approved (TEXT) # Example values: true
    - link_to_member (TEXT) # Example values: rec4BLdZHS2Blfp4v, recro8T1MPMwRadVH, recD078PnS3x2doBe
    - link_to_budget (TEXT) # Example values: recvKTAWAFKkVNnXQ, recy8KY5bUdzF81vv, recwXIiKoBMjXJsGZ, recsI0IzpUuxl2bPh, recTUGXxhTaFZ2qkg

Table: income
  Columns:
    - income_id (TEXT) (Primary Key) # Example values: rec0s9ZrO15zhzUeE, rec7f5XMQZexgtQJo, rec8BUJa8GXUjiglg, rec8V9BPNIoewWt2z, recCRWMfFqifuKMc6
    - date_received (TEXT) # Example values: 2019-10-17, 2019-09-04, 2019-10-08, 2019-10-02, 2019-09-18
    - amount (INTEGER) # Example values: 50, 200, 3000, 1000
    - source (TEXT) # Example values: Dues, Fundraising, School Appropration, Sponsorship
    - notes (TEXT) # Example values: Secured donations to help pay for speaker gifts., Annual funding from Student Government., Ad revenue for use on flyers used to advertise upcoming events.
    - link_to_member (TEXT) # Example values: reccW7q1KkhSKZsea, recTjHY5xXhvkCdVT, recUdRhbhcEO1Hk5r, rec3pH4DxMcWHMRB7, rec28ORZgcm1dtqBZ

Table: member
  Columns:
    - member_id (TEXT) (Primary Key) # Example values: rec1x5zBFIqoOuPW8, rec280Sk7o31iG0Tx, rec28ORZgcm1dtqBZ, rec2a03QXbFQAUZ7X, rec3pH4DxMcWHMRB7
    - first_name (TEXT) # Example values: Angela, Grant, Luisa, Randy, Connor
    - last_name (TEXT) # Example values: Sanders, Gilmour, Guidi, Woodard, Hilton
    - email (TEXT) # Example values: angela.sanders@lpu.edu, grant.gilmour@lpu.edu, luisa.guidi@lpu.edu, randy.woodard@lpu.edu, connor.hilton@lpu.edu
    - position (TEXT) # Example values: Member, Inactive, President, Vice President, Treasurer
    - t_shirt_size (TEXT) # Example values: Medium, X-Large, Large, Small
    - phone (TEXT) # Example values: (651) 928-4507, 403-555-1310, 442-555-5882, 490-555-8460, 454-555-7970
    - zip (INTEGER) # Example values: 55108, 29440, 10002, 8021, 48236
    - link_to_major (TEXT) # Example values: recxK3MHQFbR9J5uO, rec7BxKpjJ7bNph3O, recdIBgeU38UbV2sy, recaJdSK83k6ekRJL, recCk8lCDOTRp6rKN


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., sqlite.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

***Hints***
- &#x27;MU 215&#x27; is the location of event
- 

Question: State the category of events were held at MU 215.</pre>

---

