# ❌ Failed Examples by Pipeline

## 🔍 Pipeline/Model ID: `gemini:gemini-3-flash-preview-greedy-zero-shot-chatapi`

20 failed predictions shown (out of 370)

### ❓ Failed Question #1 (of 20 examples) - Question ID: `1472`


**Question**: In 2012, who had the least consumption in LAM?


### ✅ Ground Truth SQL(s)

```sql
SELECT T1.CustomerID FROM customers AS T1 INNER JOIN yearmonth AS T2 ON T1.CustomerID = T2.CustomerID WHERE T1.Segment = 'LAM' AND SUBSTR(T2.Date, 1, 4) = '2012' GROUP BY T1.CustomerID ORDER BY SUM(T2.Consumption) ASC NULLS FIRST LIMIT 1
```

### ❌ Predicted SQL

```sql
SELECT "T1".customerid FROM customers AS T1 JOIN yearmonth AS T2 ON "T1".customerid = "T2".customerid WHERE "T1".segment = 'LAM' AND "T2".date BETWEEN '201201' AND '201212' GROUP BY "T1".customerid ORDER BY SUM("T2".consumption) ASC NULLS FIRST LIMIT 1
```

### 📊 Evaluation Metrics

| df_error_message                                            |   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |
|:------------------------------------------------------------|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|
| Error running SQL: missing FROM-clause entry for table "T1" |                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          1 |            1083 |                 103 |           1875 |             4455.17 |

### 📘 Ground Truth Result(s)

**Result 1:**

|   customerid |
|-------------:|
|        47273 |

### 📕 Predicted Result

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given postgres database schema.

**Question:**:
In 2012, who had the least consumption in LAM?

**Database Engine / Dialect:**:
postgres

**Schema:**
Table: customers
  Columns:
    - customerid (BIGINT) (Primary Key) # Example values: 3, 5, 6, 7, 9
    - segment (TEXT) # Example values: SME, LAM, SME, LAM, SME
    - currency (TEXT) # Example values: EUR, EUR, EUR, EUR, EUR

Table: gasstations
  Columns:
    - gasstationid (BIGINT) (Primary Key) # Example values: 44, 45, 46, 47, 48
    - chainid (BIGINT) # Example values: 13, 6, 23, 33, 4
    - country (TEXT) # Example values: CZE, CZE, CZE, CZE, CZE
    - segment (TEXT) # Example values: Value for money, Premium, Other, Premium, Premium

Table: products
  Columns:
    - productid (BIGINT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - description (TEXT) # Example values: Rucní zadání, Nafta, Special, Super, Natural

Table: transactions_1k
  Columns:
    - transactionid (BIGINT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - date (DATE) # Example values: 2012-08-24, 2012-08-24, 2012-08-24, 2012-08-24, 2012-08-24
    - time (TEXT) # Example values: 09:41:00, 10:03:00, 10:03:00, 13:53:00, 08:49:00
    - customerid (BIGINT) # Example values: 31543, 46707, 46707, 7654, 17373
    - cardid (BIGINT) # Example values: 486621, 550134, 550134, 684220, 536109
    - gasstationid (BIGINT) # Example values: 3704, 3704, 3704, 656, 741
    - productid (BIGINT) # Example values: 2, 2, 23, 5, 2
    - amount (BIGINT) # Example values: 28, 18, 1, 5, 28
    - price (REAL) # Example values: 672.64, 430.72, 121.99, 120.74, 645.05

Table: yearmonth
  Columns:
    - customerid (BIGINT) # Example values: 5, 5, 5, 5, 6
    - date (TEXT) # Example values: 201207, 201302, 201303, 201304, 201203
    - consumption (REAL) # Example values: 528.3, 1598.28, 1931.36, 1497.14, 51.06


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., postgres.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

***Hints***
- Year 2012 can be presented as Between 201201 And 201212
- The first 4 strings of the Date values in the yearmonth table can represent year.

Question: In 2012, who had the least consumption in LAM?</pre>

---

### ❓ Failed Question #2 (of 20 examples) - Question ID: `1476`


**Question**: What was the difference in gas consumption between CZK-paying customers and EUR-paying customers in 2012?


### ✅ Ground Truth SQL(s)

```sql
SELECT SUM(CASE WHEN T1.Currency = 'CZK' THEN CAST(T2.Consumption AS float) ELSE 0 END) -SUM(CASE WHEN T1.Currency = 'EUR' THEN CAST(T2.Consumption AS float) ELSE 0 END) FROM customers AS T1 INNER JOIN yearmonth AS T2 ON T1.CustomerID = T2.CustomerID WHERE substring(T2.Date, 1, 4) = '2012'
```

### ❌ Predicted SQL

```sql
SELECT SUM(CASE WHEN "T1".currency = 'CZK' THEN CAST("T2".consumption AS REAL) ELSE 0 END) - SUM(CASE WHEN "T1".currency = 'EUR' THEN CAST("T2".consumption AS REAL) ELSE 0 END) FROM customers AS T1 JOIN yearmonth AS T2 ON "T1".customerid = "T2".customerid WHERE "T2".date BETWEEN '201201' AND '201212'
```

### 📊 Evaluation Metrics

| df_error_message                                            |   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |
|:------------------------------------------------------------|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|
| Error running SQL: missing FROM-clause entry for table "T1" |                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          1 |            1138 |                 117 |           2428 |             7127.93 |

### 📘 Ground Truth Result(s)

**Result 1:**

|    ?column? |
|------------:|
| 4.02525e+08 |

### 📕 Predicted Result

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given postgres database schema.

**Question:**:
What was the difference in gas consumption between CZK-paying customers and EUR-paying customers in 2012?

**Database Engine / Dialect:**:
postgres

**Schema:**
Table: customers
  Columns:
    - customerid (BIGINT) (Primary Key) # Example values: 3, 5, 6, 7, 9
    - segment (TEXT) # Example values: SME, LAM, SME, LAM, SME
    - currency (TEXT) # Example values: EUR, EUR, EUR, EUR, EUR

Table: gasstations
  Columns:
    - gasstationid (BIGINT) (Primary Key) # Example values: 44, 45, 46, 47, 48
    - chainid (BIGINT) # Example values: 13, 6, 23, 33, 4
    - country (TEXT) # Example values: CZE, CZE, CZE, CZE, CZE
    - segment (TEXT) # Example values: Value for money, Premium, Other, Premium, Premium

Table: products
  Columns:
    - productid (BIGINT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - description (TEXT) # Example values: Rucní zadání, Nafta, Special, Super, Natural

Table: transactions_1k
  Columns:
    - transactionid (BIGINT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - date (DATE) # Example values: 2012-08-24, 2012-08-24, 2012-08-24, 2012-08-24, 2012-08-24
    - time (TEXT) # Example values: 09:41:00, 10:03:00, 10:03:00, 13:53:00, 08:49:00
    - customerid (BIGINT) # Example values: 31543, 46707, 46707, 7654, 17373
    - cardid (BIGINT) # Example values: 486621, 550134, 550134, 684220, 536109
    - gasstationid (BIGINT) # Example values: 3704, 3704, 3704, 656, 741
    - productid (BIGINT) # Example values: 2, 2, 23, 5, 2
    - amount (BIGINT) # Example values: 28, 18, 1, 5, 28
    - price (REAL) # Example values: 672.64, 430.72, 121.99, 120.74, 645.05

Table: yearmonth
  Columns:
    - customerid (BIGINT) # Example values: 5, 5, 5, 5, 6
    - date (TEXT) # Example values: 201207, 201302, 201303, 201304, 201203
    - consumption (REAL) # Example values: 528.3, 1598.28, 1931.36, 1497.14, 51.06


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., postgres.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

***Hints***
- cast the consumption into float when perform calculation
- Year 2012 can be presented as Between 201201 And 201212
- The first 4 strings of the Date values in the yearmonth table can represent year
- Difference in Consumption = CZK customers consumption in 2012 - EUR customers consumption in 2012

Question: What was the difference in gas consumption between CZK-paying customers and EUR-paying customers in 2012?</pre>

---

### ❓ Failed Question #3 (of 20 examples) - Question ID: `1480`


**Question**: What was the gas consumption peak month for SME customers in 2013?


### ✅ Ground Truth SQL(s)

```sql
SELECT SUBSTR(T2.Date, 5, 2) FROM customers AS T1 INNER JOIN yearmonth AS T2 ON T1.CustomerID = T2.CustomerID WHERE SUBSTR(T2.Date, 1, 4) = '2013' AND T1.Segment = 'SME' GROUP BY SUBSTR(T2.Date, 5, 2) ORDER BY SUM(T2.Consumption) DESC NULLS LAST LIMIT 1
```

### ❌ Predicted SQL

```sql
SELECT SUBSTRING("T2".date FROM 5 FOR 2) FROM customers AS T1 JOIN yearmonth AS T2 ON "T1".customerid = "T2".customerid WHERE "T1".segment = 'SME' AND "T2".date BETWEEN '201301' AND '201312' GROUP BY SUBSTRING("T2".date FROM 5 FOR 2) ORDER BY SUM("T2".consumption) DESC NULLS LAST LIMIT 1
```

### 📊 Evaluation Metrics

| df_error_message                                            |   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |
|:------------------------------------------------------------|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|
| Error running SQL: missing FROM-clause entry for table "T1" |                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          1 |            1105 |                 124 |           4112 |             15956.4 |

### 📘 Ground Truth Result(s)

**Result 1:**

|   substr |
|---------:|
|       04 |

### 📕 Predicted Result

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given postgres database schema.

**Question:**:
What was the gas consumption peak month for SME customers in 2013?

**Database Engine / Dialect:**:
postgres

**Schema:**
Table: customers
  Columns:
    - customerid (BIGINT) (Primary Key) # Example values: 3, 5, 6, 7, 9
    - segment (TEXT) # Example values: SME, LAM, SME, LAM, SME
    - currency (TEXT) # Example values: EUR, EUR, EUR, EUR, EUR

Table: gasstations
  Columns:
    - gasstationid (BIGINT) (Primary Key) # Example values: 44, 45, 46, 47, 48
    - chainid (BIGINT) # Example values: 13, 6, 23, 33, 4
    - country (TEXT) # Example values: CZE, CZE, CZE, CZE, CZE
    - segment (TEXT) # Example values: Value for money, Premium, Other, Premium, Premium

Table: products
  Columns:
    - productid (BIGINT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - description (TEXT) # Example values: Rucní zadání, Nafta, Special, Super, Natural

Table: transactions_1k
  Columns:
    - transactionid (BIGINT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - date (DATE) # Example values: 2012-08-24, 2012-08-24, 2012-08-24, 2012-08-24, 2012-08-24
    - time (TEXT) # Example values: 09:41:00, 10:03:00, 10:03:00, 13:53:00, 08:49:00
    - customerid (BIGINT) # Example values: 31543, 46707, 46707, 7654, 17373
    - cardid (BIGINT) # Example values: 486621, 550134, 550134, 684220, 536109
    - gasstationid (BIGINT) # Example values: 3704, 3704, 3704, 656, 741
    - productid (BIGINT) # Example values: 2, 2, 23, 5, 2
    - amount (BIGINT) # Example values: 28, 18, 1, 5, 28
    - price (REAL) # Example values: 672.64, 430.72, 121.99, 120.74, 645.05

Table: yearmonth
  Columns:
    - customerid (BIGINT) # Example values: 5, 5, 5, 5, 6
    - date (TEXT) # Example values: 201207, 201302, 201303, 201304, 201203
    - consumption (REAL) # Example values: 528.3, 1598.28, 1931.36, 1497.14, 51.06


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., postgres.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

***Hints***
- Year 2013 can be presented as Between 201301 And 201312
- The first 4 strings of the Date values in the yearmonth table can represent year
- The 5th and 6th string of the date can refer to month.

Question: What was the gas consumption peak month for SME customers in 2013?</pre>

---

### ❓ Failed Question #4 (of 20 examples) - Question ID: `1473`


**Question**: What was the average monthly consumption of customers in SME for the year 2013?


### ✅ Ground Truth SQL(s)

```sql
SELECT AVG(T2.Consumption) / NULLIF(12, 0) FROM customers AS T1 INNER JOIN yearmonth AS T2 ON T1.CustomerID = T2.CustomerID WHERE SUBSTR(T2.Date, 1, 4) = '2013' AND T1.Segment = 'SME'
```

### ❌ Predicted SQL

```sql
SELECT CAST(AVG("T1".consumption) AS DOUBLE PRECISION) / 12 FROM yearmonth AS T1 JOIN customers AS T2 ON "T1".customerid = "T2".customerid WHERE "T2".segment = 'SME' AND "T1".date BETWEEN '201301' AND '201312'
```

### 📊 Evaluation Metrics

| df_error_message                                            |   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |
|:------------------------------------------------------------|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|
| Error running SQL: missing FROM-clause entry for table "T1" |                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          1 |            1105 |                  82 |           8889 |             43262.7 |

### 📘 Ground Truth Result(s)

**Result 1:**

|   ?column? |
|-----------:|
|    459.956 |

### 📕 Predicted Result

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given postgres database schema.

**Question:**:
What was the average monthly consumption of customers in SME for the year 2013?

**Database Engine / Dialect:**:
postgres

**Schema:**
Table: customers
  Columns:
    - customerid (BIGINT) (Primary Key) # Example values: 3, 5, 6, 7, 9
    - segment (TEXT) # Example values: SME, LAM, SME, LAM, SME
    - currency (TEXT) # Example values: EUR, EUR, EUR, EUR, EUR

Table: gasstations
  Columns:
    - gasstationid (BIGINT) (Primary Key) # Example values: 44, 45, 46, 47, 48
    - chainid (BIGINT) # Example values: 13, 6, 23, 33, 4
    - country (TEXT) # Example values: CZE, CZE, CZE, CZE, CZE
    - segment (TEXT) # Example values: Value for money, Premium, Other, Premium, Premium

Table: products
  Columns:
    - productid (BIGINT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - description (TEXT) # Example values: Rucní zadání, Nafta, Special, Super, Natural

Table: transactions_1k
  Columns:
    - transactionid (BIGINT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - date (DATE) # Example values: 2012-08-24, 2012-08-24, 2012-08-24, 2012-08-24, 2012-08-24
    - time (TEXT) # Example values: 09:41:00, 10:03:00, 10:03:00, 13:53:00, 08:49:00
    - customerid (BIGINT) # Example values: 31543, 46707, 46707, 7654, 17373
    - cardid (BIGINT) # Example values: 486621, 550134, 550134, 684220, 536109
    - gasstationid (BIGINT) # Example values: 3704, 3704, 3704, 656, 741
    - productid (BIGINT) # Example values: 2, 2, 23, 5, 2
    - amount (BIGINT) # Example values: 28, 18, 1, 5, 28
    - price (REAL) # Example values: 672.64, 430.72, 121.99, 120.74, 645.05

Table: yearmonth
  Columns:
    - customerid (BIGINT) # Example values: 5, 5, 5, 5, 6
    - date (TEXT) # Example values: 201207, 201302, 201303, 201304, 201203
    - consumption (REAL) # Example values: 528.3, 1598.28, 1931.36, 1497.14, 51.06


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., postgres.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

***Hints***
- Average Monthly consumption = AVG(Consumption) / 12
- Year 2013 can be presented as Between 201301 And 201312
- The first 4 strings of the Date values in the yearmonth table can represent year.

Question: What was the average monthly consumption of customers in SME for the year 2013?</pre>

---

### ❓ Failed Question #5 (of 20 examples) - Question ID: `1481`


**Question**: What is the difference in the annual average consumption of the customers with the least amount of consumption paid in CZK for 2013 between SME and LAM, LAM and KAM, and KAM and SME?


### ✅ Ground Truth SQL(s)

```sql
SELECT CAST(SUM(CASE WHEN T1.Segment = 'SME' THEN T2.Consumption ELSE 0 END) AS REAL) / NULLIF(COUNT(T1.CustomerID), 0) - CAST(SUM(CASE WHEN T1.Segment = 'LAM' THEN T2.Consumption ELSE 0 END) AS REAL) / NULLIF(COUNT(T1.CustomerID), 0), CAST(SUM(CASE WHEN T1.Segment = 'LAM' THEN T2.Consumption ELSE 0 END) AS REAL) / NULLIF(COUNT(T1.CustomerID), 0) - CAST(SUM(CASE WHEN T1.Segment = 'KAM' THEN T2.Consumption ELSE 0 END) AS REAL) / NULLIF(COUNT(T1.CustomerID), 0), CAST(SUM(CASE WHEN T1.Segment = 'KAM' THEN T2.Consumption ELSE 0 END) AS REAL) / NULLIF(COUNT(T1.CustomerID), 0) - CAST(SUM(CASE WHEN T1.Segment = 'SME' THEN T2.Consumption ELSE 0 END) AS REAL) / NULLIF(COUNT(T1.CustomerID), 0) FROM customers AS T1 INNER JOIN yearmonth AS T2 ON T1.CustomerID = T2.CustomerID WHERE T1.Currency = 'CZK' AND T2.Consumption = (SELECT MIN(Consumption) FROM yearmonth) AND T2.Date::text BETWEEN '201301' AND '201312'
```

### ❌ Predicted SQL

```sql
WITH customer_annual_totals AS (SELECT c.customerid, c.segment, SUM(y.consumption) AS annual_consumption FROM customers AS c JOIN yearmonth AS y ON c.customerid = y.customerid WHERE c.currency = 'CZK' AND y.date BETWEEN '201301' AND '201312' GROUP BY c.customerid, c.segment), segment_min_consumption AS (SELECT segment, MIN(annual_consumption) AS min_cons FROM customer_annual_totals WHERE segment IN ('SME', 'LAM', 'KAM') GROUP BY segment) SELECT (SELECT min_cons FROM segment_min_consumption WHERE segment = 'SME') - (SELECT min_cons FROM segment_min_consumption WHERE segment = 'LAM') AS diff_sme_lam, (SELECT min_cons FROM segment_min_consumption WHERE segment = 'LAM') - (SELECT min_cons FROM segment_min_consumption WHERE segment = 'KAM') AS diff_lam_kam, (SELECT min_cons FROM segment_min_consumption WHERE segment = 'KAM') - (SELECT min_cons FROM segment_min_consumption WHERE segment = 'SME') AS diff_kam_sme
```

### 📊 Evaluation Metrics

|   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |   execution_time_ms |
|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|--------------------:|
|                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          0 |            1203 |                 315 |           9217 |             41268.2 |              261.63 |

### 📘 Ground Truth Result(s)

**Result 1:**

|   ?column? |   ?column? |   ?column? |
|-----------:|-----------:|-----------:|
|    -582093 |    -582093 |    -582093 |

### 📕 Predicted Result

|   diff_sme_lam |   diff_lam_kam |   diff_kam_sme |
|---------------:|---------------:|---------------:|
|       -14009.3 |        6046.62 |        7962.72 |

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given postgres database schema.

**Question:**:
What is the difference in the annual average consumption of the customers with the least amount of consumption paid in CZK for 2013 between SME and LAM, LAM and KAM, and KAM and SME?

**Database Engine / Dialect:**:
postgres

**Schema:**
Table: customers
  Columns:
    - customerid (BIGINT) (Primary Key) # Example values: 3, 5, 6, 7, 9
    - segment (TEXT) # Example values: SME, LAM, SME, LAM, SME
    - currency (TEXT) # Example values: EUR, EUR, EUR, EUR, EUR

Table: gasstations
  Columns:
    - gasstationid (BIGINT) (Primary Key) # Example values: 44, 45, 46, 47, 48
    - chainid (BIGINT) # Example values: 13, 6, 23, 33, 4
    - country (TEXT) # Example values: CZE, CZE, CZE, CZE, CZE
    - segment (TEXT) # Example values: Value for money, Premium, Other, Premium, Premium

Table: products
  Columns:
    - productid (BIGINT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - description (TEXT) # Example values: Rucní zadání, Nafta, Special, Super, Natural

Table: transactions_1k
  Columns:
    - transactionid (BIGINT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - date (DATE) # Example values: 2012-08-24, 2012-08-24, 2012-08-24, 2012-08-24, 2012-08-24
    - time (TEXT) # Example values: 09:41:00, 10:03:00, 10:03:00, 13:53:00, 08:49:00
    - customerid (BIGINT) # Example values: 31543, 46707, 46707, 7654, 17373
    - cardid (BIGINT) # Example values: 486621, 550134, 550134, 684220, 536109
    - gasstationid (BIGINT) # Example values: 3704, 3704, 3704, 656, 741
    - productid (BIGINT) # Example values: 2, 2, 23, 5, 2
    - amount (BIGINT) # Example values: 28, 18, 1, 5, 28
    - price (REAL) # Example values: 672.64, 430.72, 121.99, 120.74, 645.05

Table: yearmonth
  Columns:
    - customerid (BIGINT) # Example values: 5, 5, 5, 5, 6
    - date (TEXT) # Example values: 201207, 201302, 201303, 201304, 201203
    - consumption (REAL) # Example values: 528.3, 1598.28, 1931.36, 1497.14, 51.06


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., postgres.
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

### ❓ Failed Question #6 (of 20 examples) - Question ID: `1482`


**Question**: Which of the three segments—SME, LAM and KAM—has the biggest and lowest percentage increases in consumption paid in EUR between 2012 and 2013?


### ✅ Ground Truth SQL(s)

```sql
SELECT CAST((SUM(CASE WHEN T1.Segment = 'SME' AND T2.Date LIKE '2013%' THEN T2.Consumption ELSE 0 END) - SUM(CASE WHEN T1.Segment = 'SME' AND T2.Date LIKE '2012%' THEN T2.Consumption ELSE 0 END)) AS REAL) * 100 / NULLIF(SUM(CASE WHEN T1.Segment = 'SME' AND T2.Date LIKE '2012%' THEN T2.Consumption ELSE 0 END), 0), CAST(SUM(CASE WHEN T1.Segment = 'LAM' AND T2.Date LIKE '2013%' THEN T2.Consumption ELSE 0 END) - SUM(CASE WHEN T1.Segment = 'LAM' AND T2.Date LIKE '2012%' THEN T2.Consumption ELSE 0 END) AS REAL) * 100 / NULLIF(SUM(CASE WHEN T1.Segment = 'LAM' AND T2.Date LIKE '2012%' THEN T2.Consumption ELSE 0 END), 0), CAST(SUM(CASE WHEN T1.Segment = 'KAM' AND T2.Date LIKE '2013%' THEN T2.Consumption ELSE 0 END) - SUM(CASE WHEN T1.Segment = 'KAM' AND T2.Date LIKE '2012%' THEN T2.Consumption ELSE 0 END) AS REAL) * 100 / NULLIF(SUM(CASE WHEN T1.Segment = 'KAM' AND T2.Date LIKE '2012%' THEN T2.Consumption ELSE 0 END), 0) FROM customers AS T1 INNER JOIN yearmonth AS T2 ON T1.CustomerID = T2.CustomerID
```

### ❌ Predicted SQL

```sql
date`?
    The question asks about consumption. The `yearmonth` table explicitly has a `consumption` column. `transactions_1k` has `amount` and `price`. Consumption usually refers to the total amount or value consumed. Looking at the schema, `yearmonth.consumption` is the intended metric.

    The hint says: `Increase or Decrease = consumption for 2013 - consumption for 2012`. This confirms using the `yearmonth` table.

    Wait, the hint also says: `The first 4 strings of the Date values in the yearmonth table can represent year`. This confirms `LEFT(date, 4)` or `LIKE '2012%'`.

    One last check: `segment` column is in `customers`. `segment` column is also in `gasstations`. The question asks about segments SME, LAM, KAM, which are examples given for `customers.segment`. `gasstations.segment` examples are 'Premium', 'Other', etc. So we definitely use `customers.segment`.

    Let's refine the query slightly to be more compact.

    ```sql
    WITH IncreaseData AS (
      SELECT 
        c.segment,
        (SUM(CASE WHEN y.date LIKE '2013%' THEN y.consumption ELSE 0 END) - SUM(CASE WHEN y.date LIKE '2012%' THEN y.consumption ELSE 0 END)) / SUM(CASE WHEN y.date LIKE '20
```

### 📊 Evaluation Metrics

| df_error_message                                  |   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |
|:--------------------------------------------------|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|
| Error running SQL: syntax error at or near "date" |                    0 |                              0 |                                     0 |                          0 |                         0 |                     0 |                      0 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          1 |            1146 |                 320 |           9166 |             40557.7 |

### 📘 Ground Truth Result(s)

**Result 1:**

|   ?column? |   ?column? |   ?column? |
|-----------:|-----------:|-----------:|
|    708.113 |    708.113 |    708.113 |

### 📕 Predicted Result

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given postgres database schema.

**Question:**:
Which of the three segments—SME, LAM and KAM—has the biggest and lowest percentage increases in consumption paid in EUR between 2012 and 2013?

**Database Engine / Dialect:**:
postgres

**Schema:**
Table: customers
  Columns:
    - customerid (BIGINT) (Primary Key) # Example values: 3, 5, 6, 7, 9
    - segment (TEXT) # Example values: SME, LAM, SME, LAM, SME
    - currency (TEXT) # Example values: EUR, EUR, EUR, EUR, EUR

Table: gasstations
  Columns:
    - gasstationid (BIGINT) (Primary Key) # Example values: 44, 45, 46, 47, 48
    - chainid (BIGINT) # Example values: 13, 6, 23, 33, 4
    - country (TEXT) # Example values: CZE, CZE, CZE, CZE, CZE
    - segment (TEXT) # Example values: Value for money, Premium, Other, Premium, Premium

Table: products
  Columns:
    - productid (BIGINT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - description (TEXT) # Example values: Rucní zadání, Nafta, Special, Super, Natural

Table: transactions_1k
  Columns:
    - transactionid (BIGINT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - date (DATE) # Example values: 2012-08-24, 2012-08-24, 2012-08-24, 2012-08-24, 2012-08-24
    - time (TEXT) # Example values: 09:41:00, 10:03:00, 10:03:00, 13:53:00, 08:49:00
    - customerid (BIGINT) # Example values: 31543, 46707, 46707, 7654, 17373
    - cardid (BIGINT) # Example values: 486621, 550134, 550134, 684220, 536109
    - gasstationid (BIGINT) # Example values: 3704, 3704, 3704, 656, 741
    - productid (BIGINT) # Example values: 2, 2, 23, 5, 2
    - amount (BIGINT) # Example values: 28, 18, 1, 5, 28
    - price (REAL) # Example values: 672.64, 430.72, 121.99, 120.74, 645.05

Table: yearmonth
  Columns:
    - customerid (BIGINT) # Example values: 5, 5, 5, 5, 6
    - date (TEXT) # Example values: 201207, 201302, 201303, 201304, 201203
    - consumption (REAL) # Example values: 528.3, 1598.28, 1931.36, 1497.14, 51.06


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., postgres.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

***Hints***
- Increase or Decrease = consumption for 2013 - consumption for 2012
- Percentage of Increase = (Increase or Decrease / consumption for 2013) * 100%
- The first 4 strings of the Date values in the yearmonth table can represent year

Question: Which of the three segments—SME, LAM and KAM—has the biggest and lowest percentage increases in consumption paid in EUR between 2012 and 2013?</pre>

---

### ❓ Failed Question #7 (of 20 examples) - Question ID: `1498`


**Question**: What is the highest monthly consumption in the year 2012?


### ✅ Ground Truth SQL(s)

```sql
SELECT SUM(CAST(Consumption AS float)) FROM yearmonth WHERE SUBSTR(Date, 1, 4) = '2012' GROUP BY SUBSTR(Date, 5, 2) ORDER BY SUM(Consumption) DESC NULLS LAST LIMIT 1
```

### ❌ Predicted SQL

```sql
SELECT MAX(CAST(consumption AS REAL)) FROM yearmonth WHERE SUBSTRING(date FROM 1 FOR 4) = '2012'
```

### 📊 Evaluation Metrics

|   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |   execution_time_ms |
|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|--------------------:|
|                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          0 |            1083 |                  34 |           3462 |             14049.6 |               64.76 |

### 📘 Ground Truth Result(s)

**Result 1:**

|         sum |
|------------:|
| 5.17872e+07 |

### 📕 Predicted Result

|    max |
|-------:|
| 445280 |

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given postgres database schema.

**Question:**:
What is the highest monthly consumption in the year 2012?

**Database Engine / Dialect:**:
postgres

**Schema:**
Table: customers
  Columns:
    - customerid (BIGINT) (Primary Key) # Example values: 3, 5, 6, 7, 9
    - segment (TEXT) # Example values: SME, LAM, SME, LAM, SME
    - currency (TEXT) # Example values: EUR, EUR, EUR, EUR, EUR

Table: gasstations
  Columns:
    - gasstationid (BIGINT) (Primary Key) # Example values: 44, 45, 46, 47, 48
    - chainid (BIGINT) # Example values: 13, 6, 23, 33, 4
    - country (TEXT) # Example values: CZE, CZE, CZE, CZE, CZE
    - segment (TEXT) # Example values: Value for money, Premium, Other, Premium, Premium

Table: products
  Columns:
    - productid (BIGINT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - description (TEXT) # Example values: Rucní zadání, Nafta, Special, Super, Natural

Table: transactions_1k
  Columns:
    - transactionid (BIGINT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - date (DATE) # Example values: 2012-08-24, 2012-08-24, 2012-08-24, 2012-08-24, 2012-08-24
    - time (TEXT) # Example values: 09:41:00, 10:03:00, 10:03:00, 13:53:00, 08:49:00
    - customerid (BIGINT) # Example values: 31543, 46707, 46707, 7654, 17373
    - cardid (BIGINT) # Example values: 486621, 550134, 550134, 684220, 536109
    - gasstationid (BIGINT) # Example values: 3704, 3704, 3704, 656, 741
    - productid (BIGINT) # Example values: 2, 2, 23, 5, 2
    - amount (BIGINT) # Example values: 28, 18, 1, 5, 28
    - price (REAL) # Example values: 672.64, 430.72, 121.99, 120.74, 645.05

Table: yearmonth
  Columns:
    - customerid (BIGINT) # Example values: 5, 5, 5, 5, 6
    - date (TEXT) # Example values: 201207, 201302, 201303, 201304, 201203
    - consumption (REAL) # Example values: 528.3, 1598.28, 1931.36, 1497.14, 51.06


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., postgres.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

***Hints***
- cast the consumption into float when perform calculation
- The first 4 strings of the Date values in the yearmonth table can represent year
- The 5th and 6th string of the date can refer to month.

Question: What is the highest monthly consumption in the year 2012?</pre>

---

### ❓ Failed Question #8 (of 20 examples) - Question ID: `1490`


**Question**: How many percent of LAM customer consumed more than 46.73?


### ✅ Ground Truth SQL(s)

```sql
SELECT CAST(SUM(CASE WHEN T2.Consumption > 46.73 THEN 1 ELSE 0 END) AS REAL) * 100 / NULLIF(COUNT(T1.CustomerID), 0) FROM customers AS T1 INNER JOIN yearmonth AS T2 ON T1.CustomerID = T2.CustomerID WHERE T1.Segment = 'LAM'
```

### ❌ Predicted SQL

```sql
SELECT CAST(COUNT(CASE WHEN sub.total_consumption > 46.73 THEN 1 END) * 100.0 AS DOUBLE PRECISION) / COUNT(*) FROM (SELECT t1.customerid, SUM(t2.consumption) AS total_consumption FROM customers AS t1 LEFT JOIN yearmonth AS t2 ON t1.customerid = t2.customerid WHERE t1.segment = 'LAM' GROUP BY t1.customerid) AS sub
```

### 📊 Evaluation Metrics

|   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |   execution_time_ms |
|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|--------------------:|
|                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          0 |            1084 |                 125 |           7527 |             34578.7 |               86.88 |

### 📘 Ground Truth Result(s)

**Result 1:**

|   ?column? |
|-----------:|
|    98.5268 |

### 📕 Predicted Result

|   ?column? |
|-----------:|
|    98.3871 |

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given postgres database schema.

**Question:**:
How many percent of LAM customer consumed more than 46.73?

**Database Engine / Dialect:**:
postgres

**Schema:**
Table: customers
  Columns:
    - customerid (BIGINT) (Primary Key) # Example values: 3, 5, 6, 7, 9
    - segment (TEXT) # Example values: SME, LAM, SME, LAM, SME
    - currency (TEXT) # Example values: EUR, EUR, EUR, EUR, EUR

Table: gasstations
  Columns:
    - gasstationid (BIGINT) (Primary Key) # Example values: 44, 45, 46, 47, 48
    - chainid (BIGINT) # Example values: 13, 6, 23, 33, 4
    - country (TEXT) # Example values: CZE, CZE, CZE, CZE, CZE
    - segment (TEXT) # Example values: Value for money, Premium, Other, Premium, Premium

Table: products
  Columns:
    - productid (BIGINT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - description (TEXT) # Example values: Rucní zadání, Nafta, Special, Super, Natural

Table: transactions_1k
  Columns:
    - transactionid (BIGINT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - date (DATE) # Example values: 2012-08-24, 2012-08-24, 2012-08-24, 2012-08-24, 2012-08-24
    - time (TEXT) # Example values: 09:41:00, 10:03:00, 10:03:00, 13:53:00, 08:49:00
    - customerid (BIGINT) # Example values: 31543, 46707, 46707, 7654, 17373
    - cardid (BIGINT) # Example values: 486621, 550134, 550134, 684220, 536109
    - gasstationid (BIGINT) # Example values: 3704, 3704, 3704, 656, 741
    - productid (BIGINT) # Example values: 2, 2, 23, 5, 2
    - amount (BIGINT) # Example values: 28, 18, 1, 5, 28
    - price (REAL) # Example values: 672.64, 430.72, 121.99, 120.74, 645.05

Table: yearmonth
  Columns:
    - customerid (BIGINT) # Example values: 5, 5, 5, 5, 6
    - date (TEXT) # Example values: 201207, 201302, 201303, 201304, 201203
    - consumption (REAL) # Example values: 528.3, 1598.28, 1931.36, 1497.14, 51.06


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., postgres.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

***Hints***
- Percentage of LAM customer consumed more than 46.73 = (Total no. of LAM customers who consumed more than 46.73 / Total no. of LAM customers) * 100.

Question: How many percent of LAM customer consumed more than 46.73?</pre>

---

### ❓ Failed Question #9 (of 20 examples) - Question ID: `1505`


**Question**: Among the customers who paid in euro, how many of them have a monthly consumption of over 1000?


### ✅ Ground Truth SQL(s)

```sql
SELECT COUNT(*) FROM yearmonth AS T1 INNER JOIN customers AS T2 ON T1.CustomerID = T2.CustomerID WHERE T2.Currency = 'EUR' AND T1.Consumption > 1000.00
```

### ❌ Predicted SQL

```sql
SELECT COUNT(DISTINCT "T1".customerid) FROM customers AS T1 JOIN yearmonth AS T2 ON "T1".customerid = "T2".customerid WHERE "T1".currency = 'EUR' AND "T2".consumption > 1000
```

### 📊 Evaluation Metrics

| df_error_message                                            |   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |
|:------------------------------------------------------------|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|
| Error running SQL: missing FROM-clause entry for table "T1" |                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          1 |            1064 |                  69 |           2394 |             7372.79 |

### 📘 Ground Truth Result(s)

**Result 1:**

|   count |
|--------:|
|    2730 |

### 📕 Predicted Result

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given postgres database schema.

**Question:**:
Among the customers who paid in euro, how many of them have a monthly consumption of over 1000?

**Database Engine / Dialect:**:
postgres

**Schema:**
Table: customers
  Columns:
    - customerid (BIGINT) (Primary Key) # Example values: 3, 5, 6, 7, 9
    - segment (TEXT) # Example values: SME, LAM, SME, LAM, SME
    - currency (TEXT) # Example values: EUR, EUR, EUR, EUR, EUR

Table: gasstations
  Columns:
    - gasstationid (BIGINT) (Primary Key) # Example values: 44, 45, 46, 47, 48
    - chainid (BIGINT) # Example values: 13, 6, 23, 33, 4
    - country (TEXT) # Example values: CZE, CZE, CZE, CZE, CZE
    - segment (TEXT) # Example values: Value for money, Premium, Other, Premium, Premium

Table: products
  Columns:
    - productid (BIGINT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - description (TEXT) # Example values: Rucní zadání, Nafta, Special, Super, Natural

Table: transactions_1k
  Columns:
    - transactionid (BIGINT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - date (DATE) # Example values: 2012-08-24, 2012-08-24, 2012-08-24, 2012-08-24, 2012-08-24
    - time (TEXT) # Example values: 09:41:00, 10:03:00, 10:03:00, 13:53:00, 08:49:00
    - customerid (BIGINT) # Example values: 31543, 46707, 46707, 7654, 17373
    - cardid (BIGINT) # Example values: 486621, 550134, 550134, 684220, 536109
    - gasstationid (BIGINT) # Example values: 3704, 3704, 3704, 656, 741
    - productid (BIGINT) # Example values: 2, 2, 23, 5, 2
    - amount (BIGINT) # Example values: 28, 18, 1, 5, 28
    - price (REAL) # Example values: 672.64, 430.72, 121.99, 120.74, 645.05

Table: yearmonth
  Columns:
    - customerid (BIGINT) # Example values: 5, 5, 5, 5, 6
    - date (TEXT) # Example values: 201207, 201302, 201303, 201304, 201203
    - consumption (REAL) # Example values: 528.3, 1598.28, 1931.36, 1497.14, 51.06


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., postgres.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

***Hints***
- Pays in euro = Currency = &#x27;EUR&#x27;.

Question: Among the customers who paid in euro, how many of them have a monthly consumption of over 1000?</pre>

---

### ❓ Failed Question #10 (of 20 examples) - Question ID: `1507`


**Question**: Please list the disparate time of the transactions taken place in the gas stations from chain no. 11.


### ✅ Ground Truth SQL(s)

```sql
SELECT DISTINCT T1.Time FROM transactions_1k AS T1 INNER JOIN gasstations AS T2 ON T1.GasStationID = T2.GasStationID WHERE T2.ChainID = 11
```

### ❌ Predicted SQL

```sql
SELECT DISTINCT "T1".time FROM transactions_1k AS T1 JOIN gasstations AS T2 ON "T1".gasstationid = "T2".gasstationid WHERE "T2".chainid = 11
```

### 📊 Evaluation Metrics

| df_error_message                                            |   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |
|:------------------------------------------------------------|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|
| Error running SQL: missing FROM-clause entry for table "T1" |                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               1 |                      0 |                 0 |                           1 |            0 |          1 |            1045 |                  59 |           1586 |             3458.18 |

### 📘 Ground Truth Result(s)

**Result 1:**

| time     |
|:---------|
| 11:55:00 |
| 14:29:00 |

### 📕 Predicted Result

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given postgres database schema.

**Question:**:
Please list the disparate time of the transactions taken place in the gas stations from chain no. 11.

**Database Engine / Dialect:**:
postgres

**Schema:**
Table: customers
  Columns:
    - customerid (BIGINT) (Primary Key) # Example values: 3, 5, 6, 7, 9
    - segment (TEXT) # Example values: SME, LAM, SME, LAM, SME
    - currency (TEXT) # Example values: EUR, EUR, EUR, EUR, EUR

Table: gasstations
  Columns:
    - gasstationid (BIGINT) (Primary Key) # Example values: 44, 45, 46, 47, 48
    - chainid (BIGINT) # Example values: 13, 6, 23, 33, 4
    - country (TEXT) # Example values: CZE, CZE, CZE, CZE, CZE
    - segment (TEXT) # Example values: Value for money, Premium, Other, Premium, Premium

Table: products
  Columns:
    - productid (BIGINT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - description (TEXT) # Example values: Rucní zadání, Nafta, Special, Super, Natural

Table: transactions_1k
  Columns:
    - transactionid (BIGINT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - date (DATE) # Example values: 2012-08-24, 2012-08-24, 2012-08-24, 2012-08-24, 2012-08-24
    - time (TEXT) # Example values: 09:41:00, 10:03:00, 10:03:00, 13:53:00, 08:49:00
    - customerid (BIGINT) # Example values: 31543, 46707, 46707, 7654, 17373
    - cardid (BIGINT) # Example values: 486621, 550134, 550134, 684220, 536109
    - gasstationid (BIGINT) # Example values: 3704, 3704, 3704, 656, 741
    - productid (BIGINT) # Example values: 2, 2, 23, 5, 2
    - amount (BIGINT) # Example values: 28, 18, 1, 5, 28
    - price (REAL) # Example values: 672.64, 430.72, 121.99, 120.74, 645.05

Table: yearmonth
  Columns:
    - customerid (BIGINT) # Example values: 5, 5, 5, 5, 6
    - date (TEXT) # Example values: 201207, 201302, 201303, 201304, 201203
    - consumption (REAL) # Example values: 528.3, 1598.28, 1931.36, 1497.14, 51.06


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., postgres.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

Question: Please list the disparate time of the transactions taken place in the gas stations from chain no. 11.</pre>

---

### ❓ Failed Question #11 (of 20 examples) - Question ID: `1506`


**Question**: Please list the product descriptions of the transactions taken place in the gas stations in the Czech Republic.


### ✅ Ground Truth SQL(s)

```sql
SELECT DISTINCT T3.Description FROM transactions_1k AS T1 INNER JOIN gasstations AS T2 ON T1.GasStationID = T2.GasStationID INNER JOIN products AS T3 ON T1.ProductID = T3.ProductID WHERE T2.Country = 'CZE'
```

### ❌ Predicted SQL

```sql
SELECT "T3".description FROM transactions_1k AS T1 JOIN gasstations AS T2 ON "T1".gasstationid = "T2".gasstationid JOIN products AS T3 ON "T1".productid = "T3".productid WHERE "T2".country = 'CZE'
```

### 📊 Evaluation Metrics

| df_error_message                                            |   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |
|:------------------------------------------------------------|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|
| Error running SQL: missing FROM-clause entry for table "T1" |                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          1 |            1067 |                  77 |           3007 |             10557.1 |

### 📘 Ground Truth Result(s)

**Result 1:**

| description   |
|:--------------|
| Dalnic.popl.  |
| Zpr.nakupu    |
| Autoprísluš.  |
| Zboží nesp.   |
| Diesel +      |
| Obcerstvení   |
| Nemrz.kapal.  |
| Nafta         |
| Natural Plus  |
| Autokosmet.   |
| Natural       |
| Natural +     |
| Knihy,nov.    |
| Provoz.nápl.  |
| Oleje,tuky    |
| Potraviny     |
| Mytí vozidel  |
| LPG           |
| Additivum     |
| Natural Spec  |
| Nafta Plus    |

### 📕 Predicted Result

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given postgres database schema.

**Question:**:
Please list the product descriptions of the transactions taken place in the gas stations in the Czech Republic.

**Database Engine / Dialect:**:
postgres

**Schema:**
Table: customers
  Columns:
    - customerid (BIGINT) (Primary Key) # Example values: 3, 5, 6, 7, 9
    - segment (TEXT) # Example values: SME, LAM, SME, LAM, SME
    - currency (TEXT) # Example values: EUR, EUR, EUR, EUR, EUR

Table: gasstations
  Columns:
    - gasstationid (BIGINT) (Primary Key) # Example values: 44, 45, 46, 47, 48
    - chainid (BIGINT) # Example values: 13, 6, 23, 33, 4
    - country (TEXT) # Example values: CZE, CZE, CZE, CZE, CZE
    - segment (TEXT) # Example values: Value for money, Premium, Other, Premium, Premium

Table: products
  Columns:
    - productid (BIGINT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - description (TEXT) # Example values: Rucní zadání, Nafta, Special, Super, Natural

Table: transactions_1k
  Columns:
    - transactionid (BIGINT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - date (DATE) # Example values: 2012-08-24, 2012-08-24, 2012-08-24, 2012-08-24, 2012-08-24
    - time (TEXT) # Example values: 09:41:00, 10:03:00, 10:03:00, 13:53:00, 08:49:00
    - customerid (BIGINT) # Example values: 31543, 46707, 46707, 7654, 17373
    - cardid (BIGINT) # Example values: 486621, 550134, 550134, 684220, 536109
    - gasstationid (BIGINT) # Example values: 3704, 3704, 3704, 656, 741
    - productid (BIGINT) # Example values: 2, 2, 23, 5, 2
    - amount (BIGINT) # Example values: 28, 18, 1, 5, 28
    - price (REAL) # Example values: 672.64, 430.72, 121.99, 120.74, 645.05

Table: yearmonth
  Columns:
    - customerid (BIGINT) # Example values: 5, 5, 5, 5, 6
    - date (TEXT) # Example values: 201207, 201302, 201303, 201304, 201203
    - consumption (REAL) # Example values: 528.3, 1598.28, 1931.36, 1497.14, 51.06


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., postgres.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

***Hints***
- Czech Republic can be represented as the Country value in the gasstations table is &#x27;CZE&#x27;
- 

Question: Please list the product descriptions of the transactions taken place in the gas stations in the Czech Republic.</pre>

---

### ❓ Failed Question #12 (of 20 examples) - Question ID: `1509`


**Question**: Among the transactions made in the gas stations in the Czech Republic, how many of them are taken place after 2012/1/1?


### ✅ Ground Truth SQL(s)

```sql
SELECT COUNT(T1.TransactionID) FROM transactions_1k AS T1 INNER JOIN gasstations AS T2 ON T1.GasStationID = T2.GasStationID WHERE T2.Country = 'CZE' AND TO_CHAR(CAST(T1.Date AS TIMESTAMP), 'YYYY') >= '2012'
```

### ❌ Predicted SQL

```sql
SELECT COUNT(*) FROM transactions_1k AS T1 JOIN gasstations AS T2 ON "T1".gasstationid = "T2".gasstationid WHERE "T2".country = 'CZE' AND "T1".date > '2012-01-01'
```

### 📊 Evaluation Metrics

| df_error_message                                            |   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |
|:------------------------------------------------------------|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|
| Error running SQL: missing FROM-clause entry for table "T1" |                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          1 |            1088 |                  73 |           2471 |             7704.56 |

### 📘 Ground Truth Result(s)

**Result 1:**

|   count |
|--------:|
|     933 |

### 📕 Predicted Result

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given postgres database schema.

**Question:**:
Among the transactions made in the gas stations in the Czech Republic, how many of them are taken place after 2012/1/1?

**Database Engine / Dialect:**:
postgres

**Schema:**
Table: customers
  Columns:
    - customerid (BIGINT) (Primary Key) # Example values: 3, 5, 6, 7, 9
    - segment (TEXT) # Example values: SME, LAM, SME, LAM, SME
    - currency (TEXT) # Example values: EUR, EUR, EUR, EUR, EUR

Table: gasstations
  Columns:
    - gasstationid (BIGINT) (Primary Key) # Example values: 44, 45, 46, 47, 48
    - chainid (BIGINT) # Example values: 13, 6, 23, 33, 4
    - country (TEXT) # Example values: CZE, CZE, CZE, CZE, CZE
    - segment (TEXT) # Example values: Value for money, Premium, Other, Premium, Premium

Table: products
  Columns:
    - productid (BIGINT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - description (TEXT) # Example values: Rucní zadání, Nafta, Special, Super, Natural

Table: transactions_1k
  Columns:
    - transactionid (BIGINT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - date (DATE) # Example values: 2012-08-24, 2012-08-24, 2012-08-24, 2012-08-24, 2012-08-24
    - time (TEXT) # Example values: 09:41:00, 10:03:00, 10:03:00, 13:53:00, 08:49:00
    - customerid (BIGINT) # Example values: 31543, 46707, 46707, 7654, 17373
    - cardid (BIGINT) # Example values: 486621, 550134, 550134, 684220, 536109
    - gasstationid (BIGINT) # Example values: 3704, 3704, 3704, 656, 741
    - productid (BIGINT) # Example values: 2, 2, 23, 5, 2
    - amount (BIGINT) # Example values: 28, 18, 1, 5, 28
    - price (REAL) # Example values: 672.64, 430.72, 121.99, 120.74, 645.05

Table: yearmonth
  Columns:
    - customerid (BIGINT) # Example values: 5, 5, 5, 5, 6
    - date (TEXT) # Example values: 201207, 201302, 201303, 201304, 201203
    - consumption (REAL) # Example values: 528.3, 1598.28, 1931.36, 1497.14, 51.06


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., postgres.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

***Hints***
- Czech Republic can be represented as the Country value in the gasstations table is &#x27;CZE&#x27;

Question: Among the transactions made in the gas stations in the Czech Republic, how many of them are taken place after 2012/1/1?</pre>

---

### ❓ Failed Question #13 (of 20 examples) - Question ID: `1514`


**Question**: What kind of currency did the customer paid at 16:25:00 in 2012/8/24?


### ✅ Ground Truth SQL(s)

```sql
SELECT DISTINCT T3.Currency FROM transactions_1k AS T1 INNER JOIN gasstations AS T2 ON T1.GasStationID = T2.GasStationID INNER JOIN customers AS T3 ON T1.CustomerID = T3.CustomerID WHERE T1.Date = '2012-08-24' AND T1.Time = '16:25:00'
```

### ❌ Predicted SQL

```sql
SELECT "T2".currency FROM transactions_1k AS T1 JOIN customers AS T2 ON "T1".customerid = "T2".customerid WHERE "T1".date = '2012-08-24' AND "T1".time = '16:25:00'
```

### 📊 Evaluation Metrics

| df_error_message                                            |   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |
|:------------------------------------------------------------|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|
| Error running SQL: missing FROM-clause entry for table "T1" |                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          1 |            1097 |                  78 |           1582 |             3312.71 |

### 📘 Ground Truth Result(s)

**Result 1:**

| currency   |
|:-----------|
| CZK        |

### 📕 Predicted Result

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given postgres database schema.

**Question:**:
What kind of currency did the customer paid at 16:25:00 in 2012/8/24?

**Database Engine / Dialect:**:
postgres

**Schema:**
Table: customers
  Columns:
    - customerid (BIGINT) (Primary Key) # Example values: 3, 5, 6, 7, 9
    - segment (TEXT) # Example values: SME, LAM, SME, LAM, SME
    - currency (TEXT) # Example values: EUR, EUR, EUR, EUR, EUR

Table: gasstations
  Columns:
    - gasstationid (BIGINT) (Primary Key) # Example values: 44, 45, 46, 47, 48
    - chainid (BIGINT) # Example values: 13, 6, 23, 33, 4
    - country (TEXT) # Example values: CZE, CZE, CZE, CZE, CZE
    - segment (TEXT) # Example values: Value for money, Premium, Other, Premium, Premium

Table: products
  Columns:
    - productid (BIGINT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - description (TEXT) # Example values: Rucní zadání, Nafta, Special, Super, Natural

Table: transactions_1k
  Columns:
    - transactionid (BIGINT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - date (DATE) # Example values: 2012-08-24, 2012-08-24, 2012-08-24, 2012-08-24, 2012-08-24
    - time (TEXT) # Example values: 09:41:00, 10:03:00, 10:03:00, 13:53:00, 08:49:00
    - customerid (BIGINT) # Example values: 31543, 46707, 46707, 7654, 17373
    - cardid (BIGINT) # Example values: 486621, 550134, 550134, 684220, 536109
    - gasstationid (BIGINT) # Example values: 3704, 3704, 3704, 656, 741
    - productid (BIGINT) # Example values: 2, 2, 23, 5, 2
    - amount (BIGINT) # Example values: 28, 18, 1, 5, 28
    - price (REAL) # Example values: 672.64, 430.72, 121.99, 120.74, 645.05

Table: yearmonth
  Columns:
    - customerid (BIGINT) # Example values: 5, 5, 5, 5, 6
    - date (TEXT) # Example values: 201207, 201302, 201303, 201304, 201203
    - consumption (REAL) # Example values: 528.3, 1598.28, 1931.36, 1497.14, 51.06


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., postgres.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

***Hints***
- &#x27;2012/8/24&#x27; can be represented by &#x27;2012-08-24&#x27;
- 

Question: What kind of currency did the customer paid at 16:25:00 in 2012/8/24?</pre>

---

### ❓ Failed Question #14 (of 20 examples) - Question ID: `1515`


**Question**: What segment did the customer have at 2012/8/23 21:20:00?


### ✅ Ground Truth SQL(s)

```sql
SELECT T2.Segment FROM transactions_1k AS T1 INNER JOIN customers AS T2 ON T1.CustomerID = T2.CustomerID WHERE T1.date = '2012-08-23' AND T1.time = '21:20:00'
```

### ❌ Predicted SQL

```sql
SELECT "T2".segment FROM transactions_1k AS T1 JOIN customers AS T2 ON "T1".customerid = "T2".customerid WHERE "T1".date = '2012-08-23' AND "T1".time = '21:20:00'
```

### 📊 Evaluation Metrics

| df_error_message                                            |   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |
|:------------------------------------------------------------|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|
| Error running SQL: missing FROM-clause entry for table "T1" |                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               1 |                      0 |                 0 |                           1 |            0 |          1 |            1088 |                  78 |           2084 |             5376.84 |

### 📘 Ground Truth Result(s)

**Result 1:**

| segment   |
|:----------|
| KAM       |

### 📕 Predicted Result

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given postgres database schema.

**Question:**:
What segment did the customer have at 2012/8/23 21:20:00?

**Database Engine / Dialect:**:
postgres

**Schema:**
Table: customers
  Columns:
    - customerid (BIGINT) (Primary Key) # Example values: 3, 5, 6, 7, 9
    - segment (TEXT) # Example values: SME, LAM, SME, LAM, SME
    - currency (TEXT) # Example values: EUR, EUR, EUR, EUR, EUR

Table: gasstations
  Columns:
    - gasstationid (BIGINT) (Primary Key) # Example values: 44, 45, 46, 47, 48
    - chainid (BIGINT) # Example values: 13, 6, 23, 33, 4
    - country (TEXT) # Example values: CZE, CZE, CZE, CZE, CZE
    - segment (TEXT) # Example values: Value for money, Premium, Other, Premium, Premium

Table: products
  Columns:
    - productid (BIGINT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - description (TEXT) # Example values: Rucní zadání, Nafta, Special, Super, Natural

Table: transactions_1k
  Columns:
    - transactionid (BIGINT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - date (DATE) # Example values: 2012-08-24, 2012-08-24, 2012-08-24, 2012-08-24, 2012-08-24
    - time (TEXT) # Example values: 09:41:00, 10:03:00, 10:03:00, 13:53:00, 08:49:00
    - customerid (BIGINT) # Example values: 31543, 46707, 46707, 7654, 17373
    - cardid (BIGINT) # Example values: 486621, 550134, 550134, 684220, 536109
    - gasstationid (BIGINT) # Example values: 3704, 3704, 3704, 656, 741
    - productid (BIGINT) # Example values: 2, 2, 23, 5, 2
    - amount (BIGINT) # Example values: 28, 18, 1, 5, 28
    - price (REAL) # Example values: 672.64, 430.72, 121.99, 120.74, 645.05

Table: yearmonth
  Columns:
    - customerid (BIGINT) # Example values: 5, 5, 5, 5, 6
    - date (TEXT) # Example values: 201207, 201302, 201303, 201304, 201203
    - consumption (REAL) # Example values: 528.3, 1598.28, 1931.36, 1497.14, 51.06


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., postgres.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

***Hints***
- &#x27;2012/8/23&#x27; can be represented by &#x27;2012-08-23&#x27;

Question: What segment did the customer have at 2012/8/23 21:20:00?</pre>

---

### ❓ Failed Question #15 (of 20 examples) - Question ID: `1500`


**Question**: Please list the product description of the products consumed in September, 2013.


### ✅ Ground Truth SQL(s)

```sql
SELECT T3.Description FROM transactions_1k AS T1 INNER JOIN yearmonth AS T2 ON T1.CustomerID = T2.CustomerID INNER JOIN products AS T3 ON T1.ProductID = T3.ProductID WHERE T2.Date = '201309'
```

### ❌ Predicted SQL

```sql
SELECT DISTINCT "T2".description FROM transactions_1k AS T1 JOIN products AS T2 ON "T1".productid = "T2".productid WHERE TO_CHAR("T1".date, 'YYYYMM') = '201309'
```

### 📊 Evaluation Metrics

| df_error_message                                            |   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |
|:------------------------------------------------------------|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|
| Error running SQL: missing FROM-clause entry for table "T1" |                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          1 |            1096 |                  68 |           8865 |               41131 |

### 📘 Ground Truth Result(s)

**Result 1:**

| description     |
|:----------------|
| Diesel          |
| Nat.Super       |
| Diesel          |
| Diesel          |
| Nat.Super       |
| Nafta           |
| Nafta           |
| Autoprísluš.    |
| Provoz.nápl.    |
| Nafta           |
| Nafta           |
| Nafta           |
| Mytí vozidel    |
| Diesel          |
| Diesel          |
| Diesel aditiv   |
| Nafta           |
| Diesel          |
| Diesel          |
| Diesel          |
| ...             |
| ... (truncated) |
| ...             |
| Diesel          |
| Nafta           |
| Zboží nesp.     |
| Obcerstvení     |
| Natural         |
| Diesel +        |
| Natural         |
| Natural         |
| Nafta           |
| LPG             |
| Diesel          |
| Diesel          |
| Diesel aditiv   |
| Umývanie voz    |
| Nafta           |
| Natural         |
| Provoz.nápl.    |
| Nafta           |
| Nafta           |
| Nafta           |

### 📕 Predicted Result

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given postgres database schema.

**Question:**:
Please list the product description of the products consumed in September, 2013.

**Database Engine / Dialect:**:
postgres

**Schema:**
Table: customers
  Columns:
    - customerid (BIGINT) (Primary Key) # Example values: 3, 5, 6, 7, 9
    - segment (TEXT) # Example values: SME, LAM, SME, LAM, SME
    - currency (TEXT) # Example values: EUR, EUR, EUR, EUR, EUR

Table: gasstations
  Columns:
    - gasstationid (BIGINT) (Primary Key) # Example values: 44, 45, 46, 47, 48
    - chainid (BIGINT) # Example values: 13, 6, 23, 33, 4
    - country (TEXT) # Example values: CZE, CZE, CZE, CZE, CZE
    - segment (TEXT) # Example values: Value for money, Premium, Other, Premium, Premium

Table: products
  Columns:
    - productid (BIGINT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - description (TEXT) # Example values: Rucní zadání, Nafta, Special, Super, Natural

Table: transactions_1k
  Columns:
    - transactionid (BIGINT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - date (DATE) # Example values: 2012-08-24, 2012-08-24, 2012-08-24, 2012-08-24, 2012-08-24
    - time (TEXT) # Example values: 09:41:00, 10:03:00, 10:03:00, 13:53:00, 08:49:00
    - customerid (BIGINT) # Example values: 31543, 46707, 46707, 7654, 17373
    - cardid (BIGINT) # Example values: 486621, 550134, 550134, 684220, 536109
    - gasstationid (BIGINT) # Example values: 3704, 3704, 3704, 656, 741
    - productid (BIGINT) # Example values: 2, 2, 23, 5, 2
    - amount (BIGINT) # Example values: 28, 18, 1, 5, 28
    - price (REAL) # Example values: 672.64, 430.72, 121.99, 120.74, 645.05

Table: yearmonth
  Columns:
    - customerid (BIGINT) # Example values: 5, 5, 5, 5, 6
    - date (TEXT) # Example values: 201207, 201302, 201303, 201304, 201203
    - consumption (REAL) # Example values: 528.3, 1598.28, 1931.36, 1497.14, 51.06


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., postgres.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

***Hints***
- September 2013 refers to 201309
- The first 4 strings of the Date values in the yearmonth table can represent year
- The 5th and 6th string of the date can refer to month.

Question: Please list the product description of the products consumed in September, 2013.</pre>

---

### ❓ Failed Question #16 (of 20 examples) - Question ID: `1501`


**Question**: Please list the countries of the gas stations with transactions taken place in June, 2013.


### ✅ Ground Truth SQL(s)

```sql
SELECT DISTINCT T2.Country FROM transactions_1k AS T1 INNER JOIN gasstations AS T2 ON T1.GasStationID = T2.GasStationID INNER JOIN yearmonth AS T3 ON T1.CustomerID = T3.CustomerID WHERE T3.Date = '201306'
```

### ❌ Predicted SQL

```sql
SELECT DISTINCT "T2".country FROM transactions_1k AS T1 JOIN gasstations AS T2 ON "T1".gasstationid = "T2".gasstationid WHERE TO_CHAR("T1".date, 'YYYYMM') = '201306'
```

### 📊 Evaluation Metrics

| df_error_message                                            |   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |
|:------------------------------------------------------------|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|
| Error running SQL: missing FROM-clause entry for table "T1" |                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          1 |            1103 |                  71 |           6627 |             29459.3 |

### 📘 Ground Truth Result(s)

**Result 1:**

| country   |
|:----------|
| SVK       |
| CZE       |

### 📕 Predicted Result

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given postgres database schema.

**Question:**:
Please list the countries of the gas stations with transactions taken place in June, 2013.

**Database Engine / Dialect:**:
postgres

**Schema:**
Table: customers
  Columns:
    - customerid (BIGINT) (Primary Key) # Example values: 3, 5, 6, 7, 9
    - segment (TEXT) # Example values: SME, LAM, SME, LAM, SME
    - currency (TEXT) # Example values: EUR, EUR, EUR, EUR, EUR

Table: gasstations
  Columns:
    - gasstationid (BIGINT) (Primary Key) # Example values: 44, 45, 46, 47, 48
    - chainid (BIGINT) # Example values: 13, 6, 23, 33, 4
    - country (TEXT) # Example values: CZE, CZE, CZE, CZE, CZE
    - segment (TEXT) # Example values: Value for money, Premium, Other, Premium, Premium

Table: products
  Columns:
    - productid (BIGINT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - description (TEXT) # Example values: Rucní zadání, Nafta, Special, Super, Natural

Table: transactions_1k
  Columns:
    - transactionid (BIGINT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - date (DATE) # Example values: 2012-08-24, 2012-08-24, 2012-08-24, 2012-08-24, 2012-08-24
    - time (TEXT) # Example values: 09:41:00, 10:03:00, 10:03:00, 13:53:00, 08:49:00
    - customerid (BIGINT) # Example values: 31543, 46707, 46707, 7654, 17373
    - cardid (BIGINT) # Example values: 486621, 550134, 550134, 684220, 536109
    - gasstationid (BIGINT) # Example values: 3704, 3704, 3704, 656, 741
    - productid (BIGINT) # Example values: 2, 2, 23, 5, 2
    - amount (BIGINT) # Example values: 28, 18, 1, 5, 28
    - price (REAL) # Example values: 672.64, 430.72, 121.99, 120.74, 645.05

Table: yearmonth
  Columns:
    - customerid (BIGINT) # Example values: 5, 5, 5, 5, 6
    - date (TEXT) # Example values: 201207, 201302, 201303, 201304, 201203
    - consumption (REAL) # Example values: 528.3, 1598.28, 1931.36, 1497.14, 51.06


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., postgres.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

***Hints***
- June 2013 refers to &#x27;201306&#x27;
- The first 4 strings of the Date values in the yearmonth table can represent year
- The 5th and 6th string of the date can refer to month;

Question: Please list the countries of the gas stations with transactions taken place in June, 2013.</pre>

---

### ❓ Failed Question #17 (of 20 examples) - Question ID: `1524`


**Question**: What's the nationality of the customer who spent 548.4 in 2012/8/24?


### ✅ Ground Truth SQL(s)

```sql
SELECT T2.Country FROM transactions_1k AS T1 INNER JOIN gasstations AS T2 ON T1.GasStationID = T2.GasStationID WHERE T1.Date = '2012-08-24' AND T1.Price = '548.4'
```

### ❌ Predicted SQL

```sql
SELECT "T2".country FROM transactions_1k AS T1 JOIN gasstations AS T2 ON "T1".gasstationid = "T2".gasstationid WHERE "T1".price = 548.4 AND "T1".date = '2012-08-24'
```

### 📊 Evaluation Metrics

| df_error_message                                            |   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |
|:------------------------------------------------------------|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|
| Error running SQL: missing FROM-clause entry for table "T1" |                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          1 |            1111 |                  77 |           2418 |             7404.42 |

### 📘 Ground Truth Result(s)

**Result 1:**

| country   |
|:----------|
| CZE       |

### 📕 Predicted Result

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given postgres database schema.

**Question:**:
What&#x27;s the nationality of the customer who spent 548.4 in 2012/8/24?

**Database Engine / Dialect:**:
postgres

**Schema:**
Table: customers
  Columns:
    - customerid (BIGINT) (Primary Key) # Example values: 3, 5, 6, 7, 9
    - segment (TEXT) # Example values: SME, LAM, SME, LAM, SME
    - currency (TEXT) # Example values: EUR, EUR, EUR, EUR, EUR

Table: gasstations
  Columns:
    - gasstationid (BIGINT) (Primary Key) # Example values: 44, 45, 46, 47, 48
    - chainid (BIGINT) # Example values: 13, 6, 23, 33, 4
    - country (TEXT) # Example values: CZE, CZE, CZE, CZE, CZE
    - segment (TEXT) # Example values: Value for money, Premium, Other, Premium, Premium

Table: products
  Columns:
    - productid (BIGINT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - description (TEXT) # Example values: Rucní zadání, Nafta, Special, Super, Natural

Table: transactions_1k
  Columns:
    - transactionid (BIGINT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - date (DATE) # Example values: 2012-08-24, 2012-08-24, 2012-08-24, 2012-08-24, 2012-08-24
    - time (TEXT) # Example values: 09:41:00, 10:03:00, 10:03:00, 13:53:00, 08:49:00
    - customerid (BIGINT) # Example values: 31543, 46707, 46707, 7654, 17373
    - cardid (BIGINT) # Example values: 486621, 550134, 550134, 684220, 536109
    - gasstationid (BIGINT) # Example values: 3704, 3704, 3704, 656, 741
    - productid (BIGINT) # Example values: 2, 2, 23, 5, 2
    - amount (BIGINT) # Example values: 28, 18, 1, 5, 28
    - price (REAL) # Example values: 672.64, 430.72, 121.99, 120.74, 645.05

Table: yearmonth
  Columns:
    - customerid (BIGINT) # Example values: 5, 5, 5, 5, 6
    - date (TEXT) # Example values: 201207, 201302, 201303, 201304, 201203
    - consumption (REAL) # Example values: 528.3, 1598.28, 1931.36, 1497.14, 51.06


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., postgres.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

***Hints***
- &#x27;2012/8/24&#x27; can be represented by &#x27;2012-08-24&#x27;
- spent 548.4 refers to price = &#x27;548.4&#x27; 

Question: What&#x27;s the nationality of the customer who spent 548.4 in 2012/8/24?</pre>

---

### ❓ Failed Question #18 (of 20 examples) - Question ID: `1531`


**Question**: Who is the top spending customer and how much is the average price per single item purchased by this customer? What currency was being used?


### ✅ Ground Truth SQL(s)

```sql
SELECT T2.CustomerID, SUM(T2.Price / NULLIF(T2.Amount, 0)), T1.Currency FROM customers AS T1 INNER JOIN transactions_1k AS T2 ON T1.CustomerID = T2.CustomerID WHERE T2.CustomerID = (SELECT CustomerID FROM yearmonth ORDER BY Consumption DESC NULLS LAST LIMIT 1) GROUP BY T2.CustomerID, T1.Currency
```

### ❌ Predicted SQL

```sql
SELECT t1.customerid, CAST(SUM(t1.price) AS DOUBLE PRECISION) / SUM(t1.amount) AS avg_price_per_item, t2.currency FROM transactions_1k AS t1 JOIN customers AS t2 ON t1.customerid = t2.customerid GROUP BY t1.customerid, t2.currency ORDER BY SUM(t1.price) DESC NULLS LAST LIMIT 1
```

### 📊 Evaluation Metrics

| df_error_message                    |   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |
|:------------------------------------|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|
| Error running SQL: division by zero |                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          1 |            1076 |                 110 |           1880 |                4390 |

### 📘 Ground Truth Result(s)

**Result 1:**

|   customerid |     sum | currency   |
|-------------:|--------:|:-----------|
|        12459 | 203.856 | CZK        |

### 📕 Predicted Result

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given postgres database schema.

**Question:**:
Who is the top spending customer and how much is the average price per single item purchased by this customer? What currency was being used?

**Database Engine / Dialect:**:
postgres

**Schema:**
Table: customers
  Columns:
    - customerid (BIGINT) (Primary Key) # Example values: 3, 5, 6, 7, 9
    - segment (TEXT) # Example values: SME, LAM, SME, LAM, SME
    - currency (TEXT) # Example values: EUR, EUR, EUR, EUR, EUR

Table: gasstations
  Columns:
    - gasstationid (BIGINT) (Primary Key) # Example values: 44, 45, 46, 47, 48
    - chainid (BIGINT) # Example values: 13, 6, 23, 33, 4
    - country (TEXT) # Example values: CZE, CZE, CZE, CZE, CZE
    - segment (TEXT) # Example values: Value for money, Premium, Other, Premium, Premium

Table: products
  Columns:
    - productid (BIGINT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - description (TEXT) # Example values: Rucní zadání, Nafta, Special, Super, Natural

Table: transactions_1k
  Columns:
    - transactionid (BIGINT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - date (DATE) # Example values: 2012-08-24, 2012-08-24, 2012-08-24, 2012-08-24, 2012-08-24
    - time (TEXT) # Example values: 09:41:00, 10:03:00, 10:03:00, 13:53:00, 08:49:00
    - customerid (BIGINT) # Example values: 31543, 46707, 46707, 7654, 17373
    - cardid (BIGINT) # Example values: 486621, 550134, 550134, 684220, 536109
    - gasstationid (BIGINT) # Example values: 3704, 3704, 3704, 656, 741
    - productid (BIGINT) # Example values: 2, 2, 23, 5, 2
    - amount (BIGINT) # Example values: 28, 18, 1, 5, 28
    - price (REAL) # Example values: 672.64, 430.72, 121.99, 120.74, 645.05

Table: yearmonth
  Columns:
    - customerid (BIGINT) # Example values: 5, 5, 5, 5, 6
    - date (TEXT) # Example values: 201207, 201302, 201303, 201304, 201203
    - consumption (REAL) # Example values: 528.3, 1598.28, 1931.36, 1497.14, 51.06


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., postgres.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

***Hints***
- average price per single item = Total(price) / Total(amount)

Question: Who is the top spending customer and how much is the average price per single item purchased by this customer? What currency was being used?</pre>

---

### ❓ Failed Question #19 (of 20 examples) - Question ID: `1312`


**Question**: What's Angela Sanders's major?


### ✅ Ground Truth SQL(s)

```sql
SELECT T2.major_name FROM member AS T1 INNER JOIN major AS T2 ON T1.link_to_major = T2.major_id WHERE T1.first_name = 'Angela' AND T1.last_name = 'Sanders'
```

### ❌ Predicted SQL

```sql
SELECT "T2".major_name FROM member AS T1 JOIN major AS T2 ON "T1".link_to_major = "T2".major_id WHERE "T1".first_name = 'Angela' AND "T1".last_name = 'Sanders'
```

### 📊 Evaluation Metrics

| df_error_message                                            |   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |
|:------------------------------------------------------------|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|
| Error running SQL: missing FROM-clause entry for table "T1" |                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               1 |                      0 |                 0 |                           1 |            0 |          1 |            2622 |                  70 |           3130 |             3142.69 |

### 📘 Ground Truth Result(s)

**Result 1:**

| major_name   |
|:-------------|
| Business     |

### 📕 Predicted Result

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given postgres database schema.

**Question:**:
What&#x27;s Angela Sanders&#x27;s major?

**Database Engine / Dialect:**:
postgres

**Schema:**
Table: event
  Columns:
    - event_id (TEXT) (Primary Key) # Example values: rec0Si5cQ4rJRVzd6, rec0akZnLLpGUloLH, rec0dZPcWXF0QjNnE, rec180D2MI4EpckHy, rec2N69DMcrqN9PJC
    - event_name (TEXT) # Example values: March Meeting, Officers meeting - January, Spring Elections, Officers meeting - March, Women&#x27;s Soccer
    - event_date (TEXT) # Example values: 2020-03-10T12:00:00, 2020-01-14T09:30:00, 2019-11-24T09:00:00, 2020-03-10T09:30:00, 2019-10-05T12:00:00
    - type (TEXT) # Example values: Meeting, Meeting, Election, Meeting, Game
    - notes (TEXT) # Example values: All active members can vote for new officers between 4pm-8pm., Attend Women&#x27;s soccer game as a group., Semester social event. Optional attendance., Attend school football game as a group., Attend school football game as a group.
    - location (TEXT) # Example values: MU 215, MU 215, Campus Soccer/Lacrosse stadium, MU 215, 900 E. Washington St.
    - status (TEXT) # Example values: Open, Open, Open, Planning, Closed

Table: major
  Columns:
    - major_id (TEXT) (Primary Key) # Example values: rec06DF6vZ1CyPKpc, rec09LedkREyskCNv, rec0Eanv576RhQllI, rec0xRZtkzxrg8kj2, rec1N0upiVLy5esTO
    - major_name (TEXT) # Example values: Outdoor Product Design and Development, Agricultural Communication, Fisheries and Aquatic Sciences, Finance, Forest Ecology and Management
    - department (TEXT) # Example values: School of Applied Sciences, Technology and Education, School of Applied Sciences, Technology and Education, Watershed Sciences Department, Economics and Finance Department, Wildland Resources Department
    - college (TEXT) # Example values: College of Agriculture and Applied Sciences, College of Agriculture and Applied Sciences, College of Natural Resources, School of Business, College of Natural Resources

Table: zip_code
  Columns:
    - zip_code (BIGINT) (Primary Key) # Example values: 501, 544, 601, 602, 603
    - type (TEXT) # Example values: Unique, Unique, Standard, Standard, Standard
    - city (TEXT) # Example values: Holtsville, Holtsville, Adjuntas, Aguada, Aguadilla
    - county (TEXT) # Example values: Suffolk County, Suffolk County, Adjuntas Municipio, Aguada Municipio, Aguadilla Municipio
    - state (TEXT) # Example values: New York, New York, Puerto Rico, Puerto Rico, Puerto Rico
    - short_state (TEXT) # Example values: NY, NY, PR, PR, PR

Table: attendance
  Columns:
    - link_to_event (TEXT) (Primary Key) # Example values: rec2N69DMcrqN9PJC, rec2N69DMcrqN9PJC, rec2N69DMcrqN9PJC, rec2N69DMcrqN9PJC, rec2N69DMcrqN9PJC
    - link_to_member (TEXT) (Primary Key) # Example values: recD078PnS3x2doBe, recP6DJPyi5donvXL, rec28ORZgcm1dtqBZ, recTjHY5xXhvkCdVT, recZ4PkGERzl9ziHO

Table: budget
  Columns:
    - budget_id (TEXT) (Primary Key) # Example values: rec0QmEc3cSQFQ6V2, rec1bG6HSft7XIvTP, rec1z6ISJU2HdIsVm, rec33PFqxLtnp80RJ, rec4DYUKBHMPZXWB2
    - category (TEXT) # Example values: Advertisement, Food, Food, Speaker Gifts, Food
    - spent (REAL) # Example values: 67.81, 121.14, 20.2, 0.0, 0.0
    - remaining (REAL) # Example values: 7.19, 28.86, -0.2, 25.0, 150.0
    - amount (BIGINT) # Example values: 75, 150, 20, 25, 150
    - event_status (TEXT) # Example values: Closed, Closed, Closed, Open, Open
    - link_to_event (TEXT) # Example values: recI43CzsZ0Q625ma, recggMW2eyCYceNcy, recJ4Witp9tpjaugn, recHaMmaKyfktt5fW, recHaMmaKyfktt5fW

Table: expense
  Columns:
    - expense_id (TEXT) (Primary Key) # Example values: rec017x6R3hQqkLAo, rec1nIjoZKTYayqZ6, rec1oMgNFt7Y0G40x, rec4Zg7WEmfiHXcnC, rec7gUiykKKW4RaJS
    - expense_description (TEXT) # Example values: Post Cards, Posters, Water, Cookies, Pizza, Posters, Parking
    - expense_date (TEXT) # Example values: 2019-08-20, 2019-10-08, 2019-09-10, 2019-10-10, 2019-11-19
    - cost (REAL) # Example values: 122.06, 20.2, 51.81, 67.81, 6.0
    - approved (TEXT) # Example values: true, true, true, true, true
    - link_to_member (TEXT) # Example values: rec4BLdZHS2Blfp4v, recro8T1MPMwRadVH, recD078PnS3x2doBe, rec4BLdZHS2Blfp4v, recro8T1MPMwRadVH
    - link_to_budget (TEXT) # Example values: recvKTAWAFKkVNnXQ, recy8KY5bUdzF81vv, recwXIiKoBMjXJsGZ, recsI0IzpUuxl2bPh, recTUGXxhTaFZ2qkg

Table: income
  Columns:
    - income_id (TEXT) (Primary Key) # Example values: rec0s9ZrO15zhzUeE, rec7f5XMQZexgtQJo, rec8BUJa8GXUjiglg, rec8V9BPNIoewWt2z, recCRWMfFqifuKMc6
    - date_received (TEXT) # Example values: 2019-10-17, 2019-09-04, 2019-10-08, 2019-10-02, 2019-09-18
    - amount (BIGINT) # Example values: 50, 50, 50, 50, 50
    - source (TEXT) # Example values: Dues, Dues, Dues, Dues, Dues
    - notes (TEXT) # Example values: Secured donations to help pay for speaker gifts., Annual funding from Student Government., Ad revenue for use on flyers used to advertise upcoming events.
    - link_to_member (TEXT) # Example values: reccW7q1KkhSKZsea, recTjHY5xXhvkCdVT, recUdRhbhcEO1Hk5r, rec3pH4DxMcWHMRB7, rec28ORZgcm1dtqBZ

Table: member
  Columns:
    - member_id (TEXT) (Primary Key) # Example values: rec1x5zBFIqoOuPW8, rec280Sk7o31iG0Tx, rec28ORZgcm1dtqBZ, rec2a03QXbFQAUZ7X, rec3pH4DxMcWHMRB7
    - first_name (TEXT) # Example values: Angela, Grant, Luisa, Randy, Connor
    - last_name (TEXT) # Example values: Sanders, Gilmour, Guidi, Woodard, Hilton
    - email (TEXT) # Example values: angela.sanders@lpu.edu, grant.gilmour@lpu.edu, luisa.guidi@lpu.edu, randy.woodard@lpu.edu, connor.hilton@lpu.edu
    - position (TEXT) # Example values: Member, Member, Member, Inactive, Member
    - t_shirt_size (TEXT) # Example values: Medium, X-Large, Medium, X-Large, X-Large
    - phone (TEXT) # Example values: (651) 928-4507, 403-555-1310, 442-555-5882, 490-555-8460, 454-555-7970
    - zip (BIGINT) # Example values: 55108, 29440, 10002, 8021, 48236
    - link_to_major (TEXT) # Example values: recxK3MHQFbR9J5uO, rec7BxKpjJ7bNph3O, recdIBgeU38UbV2sy, recaJdSK83k6ekRJL, recCk8lCDOTRp6rKN


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., postgres.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

***Hints***
- Angela Sanders is the full name
- full name refers to first_name, last_name
- major refers to major_name.

Question: What&#x27;s Angela Sanders&#x27;s major?</pre>

---

### ❓ Failed Question #20 (of 20 examples) - Question ID: `1525`


**Question**: What is the percentage of the customers who used EUR in 2012/8/25?


### ✅ Ground Truth SQL(s)

```sql
SELECT CAST(SUM(CASE WHEN T2.Currency = 'EUR' THEN 1 ELSE 0 END) AS REAL) * 100 / NULLIF(COUNT(T1.CustomerID), 0) FROM transactions_1k AS T1 INNER JOIN customers AS T2 ON T1.CustomerID = T2.CustomerID WHERE T1.Date = '2012-08-25'
```

### ❌ Predicted SQL

```sql
SELECT CAST(COUNT(DISTINCT CASE WHEN "T2".currency = 'EUR' THEN "T1".customerid END) * 100.0 AS DOUBLE PRECISION) / COUNT(DISTINCT "T1".customerid) FROM transactions_1k AS T1 JOIN customers AS T2 ON "T1".customerid = "T2".customerid WHERE "T1".date = '2012-08-25'
```

### 📊 Evaluation Metrics

| df_error_message                                            |   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |
|:------------------------------------------------------------|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|
| Error running SQL: missing FROM-clause entry for table "T1" |                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          1 |            1078 |                  97 |           4017 |               15698 |

### 📘 Ground Truth Result(s)

**Result 1:**

|   ?column? |
|-----------:|
|    1.64706 |

### 📕 Predicted Result

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given postgres database schema.

**Question:**:
What is the percentage of the customers who used EUR in 2012/8/25?

**Database Engine / Dialect:**:
postgres

**Schema:**
Table: customers
  Columns:
    - customerid (BIGINT) (Primary Key) # Example values: 3, 5, 6, 7, 9
    - segment (TEXT) # Example values: SME, LAM, SME, LAM, SME
    - currency (TEXT) # Example values: EUR, EUR, EUR, EUR, EUR

Table: gasstations
  Columns:
    - gasstationid (BIGINT) (Primary Key) # Example values: 44, 45, 46, 47, 48
    - chainid (BIGINT) # Example values: 13, 6, 23, 33, 4
    - country (TEXT) # Example values: CZE, CZE, CZE, CZE, CZE
    - segment (TEXT) # Example values: Value for money, Premium, Other, Premium, Premium

Table: products
  Columns:
    - productid (BIGINT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - description (TEXT) # Example values: Rucní zadání, Nafta, Special, Super, Natural

Table: transactions_1k
  Columns:
    - transactionid (BIGINT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - date (DATE) # Example values: 2012-08-24, 2012-08-24, 2012-08-24, 2012-08-24, 2012-08-24
    - time (TEXT) # Example values: 09:41:00, 10:03:00, 10:03:00, 13:53:00, 08:49:00
    - customerid (BIGINT) # Example values: 31543, 46707, 46707, 7654, 17373
    - cardid (BIGINT) # Example values: 486621, 550134, 550134, 684220, 536109
    - gasstationid (BIGINT) # Example values: 3704, 3704, 3704, 656, 741
    - productid (BIGINT) # Example values: 2, 2, 23, 5, 2
    - amount (BIGINT) # Example values: 28, 18, 1, 5, 28
    - price (REAL) # Example values: 672.64, 430.72, 121.99, 120.74, 645.05

Table: yearmonth
  Columns:
    - customerid (BIGINT) # Example values: 5, 5, 5, 5, 6
    - date (TEXT) # Example values: 201207, 201302, 201303, 201304, 201203
    - consumption (REAL) # Example values: 528.3, 1598.28, 1931.36, 1497.14, 51.06


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., postgres.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

***Hints***
- &#x27;2012/8/25&#x27; can be represented by &#x27;2012-08-25&#x27;

Question: What is the percentage of the customers who used EUR in 2012/8/25?</pre>

---

