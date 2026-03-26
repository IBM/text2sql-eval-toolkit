# ❌ Failed Examples by Pipeline

## 🔍 Pipeline/Model ID: `gemini:gemini-3-flash-preview-greedy-zero-shot-chatapi`

20 failed predictions shown (out of 114)

### ❓ Failed Question #1 (of 20 examples) - Question ID: `16`


**Question**: What is the maximum capacity and the average of all stadiums ?


### ✅ Ground Truth SQL(s)

```sql
select max(capacity), average from stadium
```

### ❌ Predicted SQL

```sql
SELECT MAX(Capacity), AVG(Average) FROM stadium
```

### 📊 Evaluation Metrics

|   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |   execution_time_ms |
|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|--------------------:|
|                    0 |                              0 |                                     0 |                          1 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          0 |             875 |                  17 |           2009 |             6833.07 |              140.33 |

### 📘 Ground Truth Result(s)

**Result 1:**

|   max(capacity) |   Average |
|----------------:|----------:|
|           52500 |       730 |

### 📕 Predicted Result

|   MAX(Capacity) |   AVG(Average) |
|----------------:|---------------:|
|           52500 |        917.889 |

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given sqlite database schema.

**Question:**:
What is the maximum capacity and the average of all stadiums ?

**Database Engine / Dialect:**:
sqlite

**Schema:**
Table: stadium
  Columns:
    - Stadium_ID (INT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Location (TEXT) # Example values: Raith Rovers, Ayr United, East Fife, Queen&#x27;s Park, Stirling Albion
    - Name (TEXT) # Example values: Stark&#x27;s Park, Somerset Park, Bayview Stadium, Hampden Park, Forthbank Stadium
    - Capacity (INT) # Example values: 10104, 11998, 2000, 52500, 3808
    - Highest (INT) # Example values: 4812, 2363, 1980, 1763, 1125
    - Lowest (INT) # Example values: 1294, 1057, 533, 466, 404
    - Average (INT) # Example values: 2106, 1477, 864, 730, 642

Table: singer
  Columns:
    - Singer_ID (INT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Name (TEXT) # Example values: Joe Sharp, Timbaland, Justin Brown, Rose White, John Nizinik
    - Country (TEXT) # Example values: Netherlands, United States, France
    - Song_Name (TEXT) # Example values: You, Dangerous, Hey Oh, Sun, Gentleman
    - Song_release_year (TEXT) # Example values: 1992, 2008, 2013, 2003, 2014
    - Age (INT) # Example values: 52, 32, 29, 41, 43
    - Is_male (BOOL) # Example values: F, T

Table: concert
  Columns:
    - concert_ID (INT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - concert_Name (TEXT) # Example values: Auditions, Super bootcamp, Home Visits, Week 1, Week 2
    - Theme (TEXT) # Example values: Free choice, Free choice 2, Bleeding Love, Wide Awake, Happy Tonight
    - Stadium_ID (TEXT) # Example values: 1, 2, 10, 9, 7
    - Year (TEXT) # Example values: 2014, 2015

Table: singer_in_concert
  Columns:
    - concert_ID (INT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Singer_ID (TEXT) (Primary Key) # Example values: 2, 3, 5, 6, 4


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., sqlite.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

Question: What is the maximum capacity and the average of all stadiums ?</pre>

---

### ❓ Failed Question #2 (of 20 examples) - Question ID: `23`


**Question**: For each stadium, how many concerts play there?


### ✅ Ground Truth SQL(s)

```sql
SELECT T2.name ,  count(*) FROM concert AS T1 JOIN stadium AS T2 ON T1.stadium_id  =  T2.stadium_id GROUP BY T1.stadium_id
```

### ❌ Predicted SQL

```sql
SELECT count(*), Stadium_ID FROM concert GROUP BY Stadium_ID
```

### 📊 Evaluation Metrics

|   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |   execution_time_ms |
|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|--------------------:|
|                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          0 |             871 |                  19 |           3524 |             14942.9 |                 187 |

### 📘 Ground Truth Result(s)

**Result 1:**

| Name            |   count(*) |
|:----------------|-----------:|
| Stark's Park    |          1 |
| Glebe Park      |          1 |
| Somerset Park   |          2 |
| Recreation Park |          1 |
| Balmoor         |          1 |

### 📕 Predicted Result

|   count(*) |   Stadium_ID |
|-----------:|-------------:|
|          1 |            1 |
|          1 |           10 |
|          2 |            2 |
|          1 |            7 |
|          1 |            9 |

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given sqlite database schema.

**Question:**:
For each stadium, how many concerts play there?

**Database Engine / Dialect:**:
sqlite

**Schema:**
Table: stadium
  Columns:
    - Stadium_ID (INT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Location (TEXT) # Example values: Raith Rovers, Ayr United, East Fife, Queen&#x27;s Park, Stirling Albion
    - Name (TEXT) # Example values: Stark&#x27;s Park, Somerset Park, Bayview Stadium, Hampden Park, Forthbank Stadium
    - Capacity (INT) # Example values: 10104, 11998, 2000, 52500, 3808
    - Highest (INT) # Example values: 4812, 2363, 1980, 1763, 1125
    - Lowest (INT) # Example values: 1294, 1057, 533, 466, 404
    - Average (INT) # Example values: 2106, 1477, 864, 730, 642

Table: singer
  Columns:
    - Singer_ID (INT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Name (TEXT) # Example values: Joe Sharp, Timbaland, Justin Brown, Rose White, John Nizinik
    - Country (TEXT) # Example values: Netherlands, United States, France
    - Song_Name (TEXT) # Example values: You, Dangerous, Hey Oh, Sun, Gentleman
    - Song_release_year (TEXT) # Example values: 1992, 2008, 2013, 2003, 2014
    - Age (INT) # Example values: 52, 32, 29, 41, 43
    - Is_male (BOOL) # Example values: F, T

Table: concert
  Columns:
    - concert_ID (INT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - concert_Name (TEXT) # Example values: Auditions, Super bootcamp, Home Visits, Week 1, Week 2
    - Theme (TEXT) # Example values: Free choice, Free choice 2, Bleeding Love, Wide Awake, Happy Tonight
    - Stadium_ID (TEXT) # Example values: 1, 2, 10, 9, 7
    - Year (TEXT) # Example values: 2014, 2015

Table: singer_in_concert
  Columns:
    - concert_ID (INT) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Singer_ID (TEXT) (Primary Key) # Example values: 2, 3, 5, 6, 4


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., sqlite.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

Question: For each stadium, how many concerts play there?</pre>

---

### ❓ Failed Question #3 (of 20 examples) - Question ID: `57`


**Question**: Find the first name of students who have cat or dog pet.


### ✅ Ground Truth SQL(s)

```sql
SELECT DISTINCT T1.Fname FROM student AS T1 JOIN has_pet AS T2 ON T1.stuid  =  T2.stuid JOIN pets AS T3 ON T3.petid  =  T2.petid WHERE T3.pettype  =  'cat' OR T3.pettype  =  'dog'
```

### ❌ Predicted SQL

```sql
SELECT
  T1.Fname
FROM Student AS T1
JOIN Has_Pet AS T2
  ON T1.StuID = T2.StuID
JOIN Pets AS T3
  ON T2.PetID = T3.PetID
WHERE
  T3.PetType = 'cat' OR T3.PetType = 'dog'
```

### 📊 Evaluation Metrics

|   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |   execution_time_ms |
|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|--------------------:|
|                    0 |                              0 |                                     0 |                          0 |                         1 |                     1 |                      1 |                     1 |                               0 |                      0 |                 0 |                           1 |            0 |          0 |             625 |                  84 |           1282 |              4926.9 |              139.03 |

### 📘 Ground Truth Result(s)

**Result 1:**

| Fname   |
|:--------|
| Linda   |
| Tracy   |

### 📕 Predicted Result

| Fname   |
|:--------|
| Linda   |
| Tracy   |
| Tracy   |

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given sqlite database schema.

**Question:**:
Find the first name of students who have cat or dog pet.

**Database Engine / Dialect:**:
sqlite

**Schema:**
Table: Student
  Columns:
    - StuID (INTEGER) (Primary Key) # Example values: 1001, 1002, 1003, 1004, 1005
    - LName (VARCHAR(12)) # Example values: Smith, Kim, Jones, Kumar, Gompers
    - Fname (VARCHAR(12)) # Example values: Linda, Tracy, Shiela, Dinesh, Paul
    - Age (INTEGER) # Example values: 18, 19, 21, 20, 26
    - Sex (VARCHAR(1)) # Example values: F, M
    - Major (INTEGER) # Example values: 600, 520, 540, 550, 100
    - Advisor (INTEGER) # Example values: 1121, 7712, 7792, 8423, 1148
    - city_code (VARCHAR(3)) # Example values: BAL, HKG, WAS, CHI, YYZ

Table: Has_Pet
  Columns:
    - StuID (INTEGER) # Example values: 1001, 1002
    - PetID (INTEGER) # Example values: 2001, 2002, 2003

Table: Pets
  Columns:
    - PetID (INTEGER) (Primary Key) # Example values: 2001, 2002, 2003
    - PetType (VARCHAR(20)) # Example values: cat, dog
    - pet_age (INTEGER) # Example values: 3, 2, 1
    - weight (REAL) # Example values: 12.0, 13.4, 9.3


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., sqlite.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

Question: Find the first name of students who have cat or dog pet.</pre>

---

### ❓ Failed Question #4 (of 20 examples) - Question ID: `58`


**Question**: What are the first names of every student who has a cat or dog as a pet?


### ✅ Ground Truth SQL(s)

```sql
SELECT DISTINCT T1.Fname FROM student AS T1 JOIN has_pet AS T2 ON T1.stuid  =  T2.stuid JOIN pets AS T3 ON T3.petid  =  T2.petid WHERE T3.pettype  =  'cat' OR T3.pettype  =  'dog'
```

### ❌ Predicted SQL

```sql
SELECT
  T1.Fname
FROM Student AS T1
JOIN Has_Pet AS T2
  ON T1.StuID = T2.StuID
JOIN Pets AS T3
  ON T2.PetID = T3.PetID
WHERE
  T3.PetType = 'cat' OR T3.PetType = 'dog'
```

### 📊 Evaluation Metrics

|   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |   execution_time_ms |
|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|--------------------:|
|                    0 |                              0 |                                     0 |                          0 |                         1 |                     1 |                      1 |                     1 |                               0 |                      0 |                 0 |                           1 |            0 |          0 |             635 |                  84 |           2138 |             8673.25 |              159.17 |

### 📘 Ground Truth Result(s)

**Result 1:**

| Fname   |
|:--------|
| Linda   |
| Tracy   |

### 📕 Predicted Result

| Fname   |
|:--------|
| Linda   |
| Tracy   |
| Tracy   |

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given sqlite database schema.

**Question:**:
What are the first names of every student who has a cat or dog as a pet?

**Database Engine / Dialect:**:
sqlite

**Schema:**
Table: Student
  Columns:
    - StuID (INTEGER) (Primary Key) # Example values: 1001, 1002, 1003, 1004, 1005
    - LName (VARCHAR(12)) # Example values: Smith, Kim, Jones, Kumar, Gompers
    - Fname (VARCHAR(12)) # Example values: Linda, Tracy, Shiela, Dinesh, Paul
    - Age (INTEGER) # Example values: 18, 19, 21, 20, 26
    - Sex (VARCHAR(1)) # Example values: F, M
    - Major (INTEGER) # Example values: 600, 520, 540, 550, 100
    - Advisor (INTEGER) # Example values: 1121, 7712, 7792, 8423, 1148
    - city_code (VARCHAR(3)) # Example values: BAL, HKG, WAS, CHI, YYZ

Table: Has_Pet
  Columns:
    - StuID (INTEGER) # Example values: 1001, 1002
    - PetID (INTEGER) # Example values: 2001, 2002, 2003

Table: Pets
  Columns:
    - PetID (INTEGER) (Primary Key) # Example values: 2001, 2002, 2003
    - PetType (VARCHAR(20)) # Example values: cat, dog
    - pet_age (INTEGER) # Example values: 3, 2, 1
    - weight (REAL) # Example values: 12.0, 13.4, 9.3


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., sqlite.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

Question: What are the first names of every student who has a cat or dog as a pet?</pre>

---

### ❓ Failed Question #5 (of 20 examples) - Question ID: `65`


**Question**: Find the first name and age of students who have a dog but do not have a cat as a pet.


### ✅ Ground Truth SQL(s)

```sql
SELECT T1.fname ,  T1.age FROM student AS T1 JOIN has_pet AS T2 ON T1.stuid  =  T2.stuid JOIN pets AS T3 ON T3.petid  =  T2.petid WHERE T3.pettype  =  'dog' AND T1.stuid NOT IN (SELECT T1.stuid FROM student AS T1 JOIN has_pet AS T2 ON T1.stuid  =  T2.stuid JOIN pets AS T3 ON T3.petid  =  T2.petid WHERE T3.pettype  =  'cat')
```

### ❌ Predicted SQL

```sql
SELECT
  Fname,
  Age
FROM Student
WHERE
  StuID IN (
    SELECT
      T1.StuID
    FROM Has_Pet AS T1
    JOIN Pets AS T2
      ON T1.PetID = T2.PetID
    WHERE
      T2.PetType = 'dog'
  )
  AND StuID NOT IN (
    SELECT
      T1.StuID
    FROM Has_Pet AS T1
    JOIN Pets AS T2
      ON T1.PetID = T2.PetID
    WHERE
      T2.PetType = 'cat'
  )
```

### 📊 Evaluation Metrics

|   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |   execution_time_ms |
|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|--------------------:|
|                    0 |                              0 |                                     0 |                          0 |                         1 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          0 |             643 |                 147 |           3154 |               13161 |              105.44 |

### 📘 Ground Truth Result(s)

**Result 1:**

| Fname   |   Age |
|:--------|------:|
| Tracy   |    19 |
| Tracy   |    19 |

### 📕 Predicted Result

| Fname   |   Age |
|:--------|------:|
| Tracy   |    19 |

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given sqlite database schema.

**Question:**:
Find the first name and age of students who have a dog but do not have a cat as a pet.

**Database Engine / Dialect:**:
sqlite

**Schema:**
Table: Student
  Columns:
    - StuID (INTEGER) (Primary Key) # Example values: 1001, 1002, 1003, 1004, 1005
    - LName (VARCHAR(12)) # Example values: Smith, Kim, Jones, Kumar, Gompers
    - Fname (VARCHAR(12)) # Example values: Linda, Tracy, Shiela, Dinesh, Paul
    - Age (INTEGER) # Example values: 18, 19, 21, 20, 26
    - Sex (VARCHAR(1)) # Example values: F, M
    - Major (INTEGER) # Example values: 600, 520, 540, 550, 100
    - Advisor (INTEGER) # Example values: 1121, 7712, 7792, 8423, 1148
    - city_code (VARCHAR(3)) # Example values: BAL, HKG, WAS, CHI, YYZ

Table: Has_Pet
  Columns:
    - StuID (INTEGER) # Example values: 1001, 1002
    - PetID (INTEGER) # Example values: 2001, 2002, 2003

Table: Pets
  Columns:
    - PetID (INTEGER) (Primary Key) # Example values: 2001, 2002, 2003
    - PetType (VARCHAR(20)) # Example values: cat, dog
    - pet_age (INTEGER) # Example values: 3, 2, 1
    - weight (REAL) # Example values: 12.0, 13.4, 9.3


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., sqlite.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

Question: Find the first name and age of students who have a dog but do not have a cat as a pet.</pre>

---

### ❓ Failed Question #6 (of 20 examples) - Question ID: `75`


**Question**: Find the first name and age of students who have a pet.


### ✅ Ground Truth SQL(s)

```sql
SELECT DISTINCT T1.fname ,  T1.age FROM student AS T1 JOIN has_pet AS T2 ON T1.stuid  =  T2.stuid
```

### ❌ Predicted SQL

```sql
SELECT T1.Fname, T1.Age FROM Student AS T1 JOIN Has_Pet AS T2 ON T1.StuID = T2.StuID
```

### 📊 Evaluation Metrics

|   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |   execution_time_ms |
|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|--------------------:|
|                    0 |                              0 |                                     0 |                          0 |                         1 |                     1 |                      1 |                     1 |                               0 |                      0 |                 0 |                           1 |            0 |          0 |             625 |                  40 |           1185 |              5724.6 |              179.28 |

### 📘 Ground Truth Result(s)

**Result 1:**

| Fname   |   Age |
|:--------|------:|
| Linda   |    18 |
| Tracy   |    19 |

### 📕 Predicted Result

| Fname   |   Age |
|:--------|------:|
| Linda   |    18 |
| Tracy   |    19 |
| Tracy   |    19 |

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given sqlite database schema.

**Question:**:
Find the first name and age of students who have a pet.

**Database Engine / Dialect:**:
sqlite

**Schema:**
Table: Student
  Columns:
    - StuID (INTEGER) (Primary Key) # Example values: 1001, 1002, 1003, 1004, 1005
    - LName (VARCHAR(12)) # Example values: Smith, Kim, Jones, Kumar, Gompers
    - Fname (VARCHAR(12)) # Example values: Linda, Tracy, Shiela, Dinesh, Paul
    - Age (INTEGER) # Example values: 18, 19, 21, 20, 26
    - Sex (VARCHAR(1)) # Example values: F, M
    - Major (INTEGER) # Example values: 600, 520, 540, 550, 100
    - Advisor (INTEGER) # Example values: 1121, 7712, 7792, 8423, 1148
    - city_code (VARCHAR(3)) # Example values: BAL, HKG, WAS, CHI, YYZ

Table: Has_Pet
  Columns:
    - StuID (INTEGER) # Example values: 1001, 1002
    - PetID (INTEGER) # Example values: 2001, 2002, 2003

Table: Pets
  Columns:
    - PetID (INTEGER) (Primary Key) # Example values: 2001, 2002, 2003
    - PetType (VARCHAR(20)) # Example values: cat, dog
    - pet_age (INTEGER) # Example values: 3, 2, 1
    - weight (REAL) # Example values: 12.0, 13.4, 9.3


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., sqlite.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

Question: Find the first name and age of students who have a pet.</pre>

---

### ❓ Failed Question #7 (of 20 examples) - Question ID: `66`


**Question**: What is the first name of every student who has a dog but does not have a cat?


### ✅ Ground Truth SQL(s)

```sql
SELECT T1.fname ,  T1.age FROM student AS T1 JOIN has_pet AS T2 ON T1.stuid  =  T2.stuid JOIN pets AS T3 ON T3.petid  =  T2.petid WHERE T3.pettype  =  'dog' AND T1.stuid NOT IN (SELECT T1.stuid FROM student AS T1 JOIN has_pet AS T2 ON T1.stuid  =  T2.stuid JOIN pets AS T3 ON T3.petid  =  T2.petid WHERE T3.pettype  =  'cat')
```

### ❌ Predicted SQL

```sql
SELECT T1.Fname FROM Student AS T1 JOIN Has_Pet AS T2 ON T1.StuID = T2.StuID JOIN Pets AS T3 ON T2.PetID = T3.PetID WHERE T3.PetType = 'dog' EXCEPT SELECT T1.Fname FROM Student AS T1 JOIN Has_Pet AS T2 ON T1.StuID = T2.StuID JOIN Pets AS T3 ON T2.PetID = T3.PetID WHERE T3.PetType = 'cat'
```

### 📊 Evaluation Metrics

|   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |   execution_time_ms |
|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|--------------------:|
|                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          0 |             637 |                 120 |           4005 |             17894.6 |               127.5 |

### 📘 Ground Truth Result(s)

**Result 1:**

| Fname   |   Age |
|:--------|------:|
| Tracy   |    19 |
| Tracy   |    19 |

### 📕 Predicted Result

| Fname   |
|:--------|
| Tracy   |

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given sqlite database schema.

**Question:**:
What is the first name of every student who has a dog but does not have a cat?

**Database Engine / Dialect:**:
sqlite

**Schema:**
Table: Student
  Columns:
    - StuID (INTEGER) (Primary Key) # Example values: 1001, 1002, 1003, 1004, 1005
    - LName (VARCHAR(12)) # Example values: Smith, Kim, Jones, Kumar, Gompers
    - Fname (VARCHAR(12)) # Example values: Linda, Tracy, Shiela, Dinesh, Paul
    - Age (INTEGER) # Example values: 18, 19, 21, 20, 26
    - Sex (VARCHAR(1)) # Example values: F, M
    - Major (INTEGER) # Example values: 600, 520, 540, 550, 100
    - Advisor (INTEGER) # Example values: 1121, 7712, 7792, 8423, 1148
    - city_code (VARCHAR(3)) # Example values: BAL, HKG, WAS, CHI, YYZ

Table: Has_Pet
  Columns:
    - StuID (INTEGER) # Example values: 1001, 1002
    - PetID (INTEGER) # Example values: 2001, 2002, 2003

Table: Pets
  Columns:
    - PetID (INTEGER) (Primary Key) # Example values: 2001, 2002, 2003
    - PetType (VARCHAR(20)) # Example values: cat, dog
    - pet_age (INTEGER) # Example values: 3, 2, 1
    - weight (REAL) # Example values: 12.0, 13.4, 9.3


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., sqlite.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

Question: What is the first name of every student who has a dog but does not have a cat?</pre>

---

### ❓ Failed Question #8 (of 20 examples) - Question ID: `102`


**Question**: What is the maker of the carr produced in the earliest year and what year was it?


### ✅ Ground Truth SQL(s)

```sql
SELECT T2.Make ,  T1.Year FROM CARS_DATA AS T1 JOIN CAR_NAMES AS T2 ON T1.Id  =  T2.MakeId WHERE T1.Year  =  (SELECT min(YEAR) FROM CARS_DATA);
```

### ❌ Predicted SQL

```sql
SELECT
  T1.Maker,
  T2.Year
FROM car_makers AS T1
JOIN model_list AS T3
  ON T1.Id = T3.Maker
JOIN car_names AS T4
  ON T3.Model = T4.Model
JOIN cars_data AS T2
  ON T4.MakeId = T2.Id
ORDER BY
  T2.Year ASC
LIMIT 1
```

### 📊 Evaluation Metrics

|   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |   execution_time_ms |
|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|--------------------:|
|                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          0 |             961 |                 103 |           2829 |             9987.54 |               55.46 |

### 📘 Ground Truth Result(s)

**Result 1:**

| Make                             |   Year |
|:---------------------------------|-------:|
| chevrolet chevelle malibu        |   1970 |
| buick skylark 320                |   1970 |
| plymouth satellite               |   1970 |
| amc rebel sst                    |   1970 |
| ford torino                      |   1970 |
| ford galaxie 500                 |   1970 |
| chevrolet impala                 |   1970 |
| plymouth fury iii                |   1970 |
| pontiac catalina                 |   1970 |
| amc ambassador dpl               |   1970 |
| citroen ds-21 pallas             |   1970 |
| chevrolet chevelle concours (sw) |   1970 |
| ford torino (sw)                 |   1970 |
| plymouth satellite (sw)          |   1970 |
| amc rebel sst (sw)               |   1970 |
| dodge challenger se              |   1970 |
| plymouth cuda 340                |   1970 |
| ford mustang boss 302            |   1970 |
| chevrolet monte carlo            |   1970 |
| buick estate wagon (sw)          |   1970 |
| toyota corona mark ii            |   1970 |
| plymouth duster                  |   1970 |
| amc hornet                       |   1970 |
| ford maverick                    |   1970 |
| datsun pl510                     |   1970 |
| volkswagen 1131 deluxe sedan     |   1970 |
| peugeot 504                      |   1970 |
| audi 100 ls                      |   1970 |
| saab 99e                         |   1970 |
| bmw 2002                         |   1970 |
| amc gremlin                      |   1970 |
| ford f250                        |   1970 |
| chevy c20                        |   1970 |
| dodge d200                       |   1970 |
| hi 1200d                         |   1970 |

### 📕 Predicted Result

| Maker   |   Year |
|:--------|-------:|
| gm      |   1970 |

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given sqlite database schema.

**Question:**:
What is the maker of the carr produced in the earliest year and what year was it?

**Database Engine / Dialect:**:
sqlite

**Schema:**
Table: continents
  Columns:
    - ContId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Continent (TEXT) # Example values: america, europe, asia, africa, australia

Table: countries
  Columns:
    - CountryId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - CountryName (TEXT) # Example values: usa, germany, france, japan, italy
    - Continent (INTEGER) # Example values: 1, 2, 3, 4, 5

Table: car_makers
  Columns:
    - Id (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Maker (TEXT) # Example values: amc, volkswagen, bmw, gm, ford
    - FullName (TEXT) # Example values: American Motor Company, Volkswagen, BMW, General Motors, Ford Motor Company
    - Country (TEXT) # Example values: 1, 2, 3, 4, 5

Table: model_list
  Columns:
    - ModelId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Maker (INTEGER) # Example values: 1, 2, 3, 4, 5
    - Model (TEXT) # Example values: amc, audi, bmw, buick, cadillac

Table: car_names
  Columns:
    - MakeId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Model (TEXT) # Example values: chevrolet, buick, plymouth, amc, ford
    - Make (TEXT) # Example values: chevrolet chevelle malibu, buick skylark 320, plymouth satellite, amc rebel sst, ford torino

Table: cars_data
  Columns:
    - Id (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - MPG (TEXT) # Example values: 18, 15, 16, 17, 14
    - Cylinders (INTEGER) # Example values: 8, 4, 6, 3, 5
    - Edispl (REAL) # Example values: 307.0, 350.0, 318.0, 304.0, 302.0
    - Horsepower (TEXT) # Example values: 130, 165, 150, 140, 198
    - Weight (INTEGER) # Example values: 3504, 3693, 3436, 3433, 3449
    - Accelerate (REAL) # Example values: 12.0, 11.5, 11.0, 10.5, 10.0
    - Year (INTEGER) # Example values: 1970, 1971, 1972, 1973, 1974


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., sqlite.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

Question: What is the maker of the carr produced in the earliest year and what year was it?</pre>

---

### ❓ Failed Question #9 (of 20 examples) - Question ID: `121`


**Question**: What are all the makers and models?


### ✅ Ground Truth SQL(s)

```sql
SELECT Maker ,  Model FROM MODEL_LIST;
```

### ❌ Predicted SQL

```sql
SELECT T1.Maker, T2.Model FROM car_makers AS T1 JOIN model_list AS T2 ON T1.Id = T2.Maker
```

### 📊 Evaluation Metrics

|   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |   execution_time_ms |
|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|--------------------:|
|                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          0 |             941 |                  40 |           2434 |             9644.54 |              347.55 |

### 📘 Ground Truth Result(s)

**Result 1:**

|   Maker | Model         |
|--------:|:--------------|
|       1 | amc           |
|       2 | audi          |
|       3 | bmw           |
|       4 | buick         |
|       4 | cadillac      |
|       5 | capri         |
|       4 | chevrolet     |
|       6 | chrysler      |
|       7 | citroen       |
|       8 | datsun        |
|       6 | dodge         |
|       9 | fiat          |
|       5 | ford          |
|      10 | hi            |
|      11 | honda         |
|      12 | mazda         |
|      13 | mercedes      |
|      13 | mercedes-benz |
|       5 | mercury       |
|       8 | nissan        |
|       4 | oldsmobile    |
|      14 | opel          |
|      15 | peugeot       |
|       6 | plymouth      |
|       4 | pontiac       |
|      16 | renault       |
|      17 | saab          |
|      18 | subaru        |
|      19 | toyota        |
|      20 | triumph       |
|       2 | volkswagen    |
|      21 | volvo         |
|      22 | kia           |
|      23 | hyundai       |
|       6 | jeep          |
|      19 | scion         |

### 📕 Predicted Result

| Maker        | Model         |
|:-------------|:--------------|
| amc          | amc           |
| volkswagen   | audi          |
| bmw          | bmw           |
| gm           | buick         |
| gm           | cadillac      |
| ford         | capri         |
| gm           | chevrolet     |
| chrysler     | chrysler      |
| citroen      | citroen       |
| nissan       | datsun        |
| chrysler     | dodge         |
| fiat         | fiat          |
| ford         | ford          |
| honda        | honda         |
| mazda        | mazda         |
| daimler benz | mercedes      |
| daimler benz | mercedes-benz |
| ford         | mercury       |
| nissan       | nissan        |
| gm           | oldsmobile    |
| opel         | opel          |
| peugeaut     | peugeot       |
| chrysler     | plymouth      |
| gm           | pontiac       |
| renault      | renault       |
| saab         | saab          |
| subaru       | subaru        |
| toyota       | toyota        |
| triumph      | triumph       |
| volkswagen   | volkswagen    |
| volvo        | volvo         |
| kia          | kia           |
| hyundai      | hyundai       |
| chrysler     | jeep          |
| toyota       | scion         |

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given sqlite database schema.

**Question:**:
What are all the makers and models?

**Database Engine / Dialect:**:
sqlite

**Schema:**
Table: continents
  Columns:
    - ContId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Continent (TEXT) # Example values: america, europe, asia, africa, australia

Table: countries
  Columns:
    - CountryId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - CountryName (TEXT) # Example values: usa, germany, france, japan, italy
    - Continent (INTEGER) # Example values: 1, 2, 3, 4, 5

Table: car_makers
  Columns:
    - Id (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Maker (TEXT) # Example values: amc, volkswagen, bmw, gm, ford
    - FullName (TEXT) # Example values: American Motor Company, Volkswagen, BMW, General Motors, Ford Motor Company
    - Country (TEXT) # Example values: 1, 2, 3, 4, 5

Table: model_list
  Columns:
    - ModelId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Maker (INTEGER) # Example values: 1, 2, 3, 4, 5
    - Model (TEXT) # Example values: amc, audi, bmw, buick, cadillac

Table: car_names
  Columns:
    - MakeId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Model (TEXT) # Example values: chevrolet, buick, plymouth, amc, ford
    - Make (TEXT) # Example values: chevrolet chevelle malibu, buick skylark 320, plymouth satellite, amc rebel sst, ford torino

Table: cars_data
  Columns:
    - Id (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - MPG (TEXT) # Example values: 18, 15, 16, 17, 14
    - Cylinders (INTEGER) # Example values: 8, 4, 6, 3, 5
    - Edispl (REAL) # Example values: 307.0, 350.0, 318.0, 304.0, 302.0
    - Horsepower (TEXT) # Example values: 130, 165, 150, 140, 198
    - Weight (INTEGER) # Example values: 3504, 3693, 3436, 3433, 3449
    - Accelerate (REAL) # Example values: 12.0, 11.5, 11.0, 10.5, 10.0
    - Year (INTEGER) # Example values: 1970, 1971, 1972, 1973, 1974


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., sqlite.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

Question: What are all the makers and models?</pre>

---

### ❓ Failed Question #10 (of 20 examples) - Question ID: `122`


**Question**: What are the makers and models?


### ✅ Ground Truth SQL(s)

```sql
SELECT Maker ,  Model FROM MODEL_LIST;
```

### ❌ Predicted SQL

```sql
SELECT T1.Maker, T2.Model FROM car_makers AS T1 JOIN model_list AS T2 ON T1.Id = T2.Maker
```

### 📊 Evaluation Metrics

|   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |   execution_time_ms |
|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|--------------------:|
|                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          0 |             939 |                  40 |           2545 |              9625.9 |              332.96 |

### 📘 Ground Truth Result(s)

**Result 1:**

|   Maker | Model         |
|--------:|:--------------|
|       1 | amc           |
|       2 | audi          |
|       3 | bmw           |
|       4 | buick         |
|       4 | cadillac      |
|       5 | capri         |
|       4 | chevrolet     |
|       6 | chrysler      |
|       7 | citroen       |
|       8 | datsun        |
|       6 | dodge         |
|       9 | fiat          |
|       5 | ford          |
|      10 | hi            |
|      11 | honda         |
|      12 | mazda         |
|      13 | mercedes      |
|      13 | mercedes-benz |
|       5 | mercury       |
|       8 | nissan        |
|       4 | oldsmobile    |
|      14 | opel          |
|      15 | peugeot       |
|       6 | plymouth      |
|       4 | pontiac       |
|      16 | renault       |
|      17 | saab          |
|      18 | subaru        |
|      19 | toyota        |
|      20 | triumph       |
|       2 | volkswagen    |
|      21 | volvo         |
|      22 | kia           |
|      23 | hyundai       |
|       6 | jeep          |
|      19 | scion         |

### 📕 Predicted Result

| Maker        | Model         |
|:-------------|:--------------|
| amc          | amc           |
| volkswagen   | audi          |
| bmw          | bmw           |
| gm           | buick         |
| gm           | cadillac      |
| ford         | capri         |
| gm           | chevrolet     |
| chrysler     | chrysler      |
| citroen      | citroen       |
| nissan       | datsun        |
| chrysler     | dodge         |
| fiat         | fiat          |
| ford         | ford          |
| honda        | honda         |
| mazda        | mazda         |
| daimler benz | mercedes      |
| daimler benz | mercedes-benz |
| ford         | mercury       |
| nissan       | nissan        |
| gm           | oldsmobile    |
| opel         | opel          |
| peugeaut     | peugeot       |
| chrysler     | plymouth      |
| gm           | pontiac       |
| renault      | renault       |
| saab         | saab          |
| subaru       | subaru        |
| toyota       | toyota        |
| triumph      | triumph       |
| volkswagen   | volkswagen    |
| volvo        | volvo         |
| kia          | kia           |
| hyundai      | hyundai       |
| chrysler     | jeep          |
| toyota       | scion         |

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given sqlite database schema.

**Question:**:
What are the makers and models?

**Database Engine / Dialect:**:
sqlite

**Schema:**
Table: continents
  Columns:
    - ContId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Continent (TEXT) # Example values: america, europe, asia, africa, australia

Table: countries
  Columns:
    - CountryId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - CountryName (TEXT) # Example values: usa, germany, france, japan, italy
    - Continent (INTEGER) # Example values: 1, 2, 3, 4, 5

Table: car_makers
  Columns:
    - Id (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Maker (TEXT) # Example values: amc, volkswagen, bmw, gm, ford
    - FullName (TEXT) # Example values: American Motor Company, Volkswagen, BMW, General Motors, Ford Motor Company
    - Country (TEXT) # Example values: 1, 2, 3, 4, 5

Table: model_list
  Columns:
    - ModelId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Maker (INTEGER) # Example values: 1, 2, 3, 4, 5
    - Model (TEXT) # Example values: amc, audi, bmw, buick, cadillac

Table: car_names
  Columns:
    - MakeId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Model (TEXT) # Example values: chevrolet, buick, plymouth, amc, ford
    - Make (TEXT) # Example values: chevrolet chevelle malibu, buick skylark 320, plymouth satellite, amc rebel sst, ford torino

Table: cars_data
  Columns:
    - Id (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - MPG (TEXT) # Example values: 18, 15, 16, 17, 14
    - Cylinders (INTEGER) # Example values: 8, 4, 6, 3, 5
    - Edispl (REAL) # Example values: 307.0, 350.0, 318.0, 304.0, 302.0
    - Horsepower (TEXT) # Example values: 130, 165, 150, 140, 198
    - Weight (INTEGER) # Example values: 3504, 3693, 3436, 3433, 3449
    - Accelerate (REAL) # Example values: 12.0, 11.5, 11.0, 10.5, 10.0
    - Year (INTEGER) # Example values: 1970, 1971, 1972, 1973, 1974


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., sqlite.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

Question: What are the makers and models?</pre>

---

### ❓ Failed Question #11 (of 20 examples) - Question ID: `124`


**Question**: What are the names and ids of all countries with at least one car maker?


### ✅ Ground Truth SQL(s)

```sql
SELECT T1.CountryName ,  T1.CountryId FROM COUNTRIES AS T1 JOIN CAR_MAKERS AS T2 ON T1.CountryId  =  T2.Country GROUP BY T1.CountryId HAVING count(*)  >=  1;
```

### ❌ Predicted SQL

```sql
SELECT T1.CountryName, T1.CountryId FROM countries AS T1 JOIN car_makers AS T2 ON T1.CountryId = T2.Country
```

### 📊 Evaluation Metrics

|   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |   execution_time_ms |
|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|--------------------:|
|                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          0 |             957 |                  41 |           2741 |             10765.6 |              256.23 |

### 📘 Ground Truth Result(s)

**Result 1:**

| CountryName   |   CountryId |
|:--------------|------------:|
| usa           |           1 |
| germany       |           2 |
| france        |           3 |
| japan         |           4 |
| italy         |           5 |
| sweden        |           6 |
| uk            |           7 |
| korea         |           8 |

### 📕 Predicted Result

| CountryName   |   CountryId |
|:--------------|------------:|
| usa           |           1 |
| germany       |           2 |
| germany       |           2 |
| usa           |           1 |
| usa           |           1 |
| usa           |           1 |
| france        |           3 |
| japan         |           4 |
| italy         |           5 |
| japan         |           4 |
| japan         |           4 |
| germany       |           2 |
| germany       |           2 |
| france        |           3 |
| france        |           3 |
| sweden        |           6 |
| japan         |           4 |

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given sqlite database schema.

**Question:**:
What are the names and ids of all countries with at least one car maker?

**Database Engine / Dialect:**:
sqlite

**Schema:**
Table: continents
  Columns:
    - ContId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Continent (TEXT) # Example values: america, europe, asia, africa, australia

Table: countries
  Columns:
    - CountryId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - CountryName (TEXT) # Example values: usa, germany, france, japan, italy
    - Continent (INTEGER) # Example values: 1, 2, 3, 4, 5

Table: car_makers
  Columns:
    - Id (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Maker (TEXT) # Example values: amc, volkswagen, bmw, gm, ford
    - FullName (TEXT) # Example values: American Motor Company, Volkswagen, BMW, General Motors, Ford Motor Company
    - Country (TEXT) # Example values: 1, 2, 3, 4, 5

Table: model_list
  Columns:
    - ModelId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Maker (INTEGER) # Example values: 1, 2, 3, 4, 5
    - Model (TEXT) # Example values: amc, audi, bmw, buick, cadillac

Table: car_names
  Columns:
    - MakeId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Model (TEXT) # Example values: chevrolet, buick, plymouth, amc, ford
    - Make (TEXT) # Example values: chevrolet chevelle malibu, buick skylark 320, plymouth satellite, amc rebel sst, ford torino

Table: cars_data
  Columns:
    - Id (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - MPG (TEXT) # Example values: 18, 15, 16, 17, 14
    - Cylinders (INTEGER) # Example values: 8, 4, 6, 3, 5
    - Edispl (REAL) # Example values: 307.0, 350.0, 318.0, 304.0, 302.0
    - Horsepower (TEXT) # Example values: 130, 165, 150, 140, 198
    - Weight (INTEGER) # Example values: 3504, 3693, 3436, 3433, 3449
    - Accelerate (REAL) # Example values: 12.0, 11.5, 11.0, 10.5, 10.0
    - Year (INTEGER) # Example values: 1970, 1971, 1972, 1973, 1974


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., sqlite.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

Question: What are the names and ids of all countries with at least one car maker?</pre>

---

### ❓ Failed Question #12 (of 20 examples) - Question ID: `150`


**Question**: What are the names and ids of all makers with more than 3 models?


### ✅ Ground Truth SQL(s)

```sql
SELECT T1.FullName ,  T1.Id FROM CAR_MAKERS AS T1 JOIN MODEL_LIST AS T2 ON T1.Id  =  T2.Maker GROUP BY T1.Id HAVING count(*)  >  3;
```

### ❌ Predicted SQL

```sql
SELECT
  T1.Maker,
  T1.Id
FROM car_makers AS T1
JOIN model_list AS T2
  ON T1.Id = T2.Maker
GROUP BY
  T1.Id
HAVING
  COUNT(*) > 3
```

### 📊 Evaluation Metrics

|   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |   execution_time_ms |
|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|--------------------:|
|                    0 |                              0 |                                     0 |                          1 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          0 |             957 |                  67 |           1879 |             5612.81 |              149.86 |

### 📘 Ground Truth Result(s)

**Result 1:**

| FullName       |   Id |
|:---------------|-----:|
| General Motors |    4 |
| Chrysler       |    6 |

### 📕 Predicted Result

| Maker    |   Id |
|:---------|-----:|
| gm       |    4 |
| chrysler |    6 |

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given sqlite database schema.

**Question:**:
What are the names and ids of all makers with more than 3 models?

**Database Engine / Dialect:**:
sqlite

**Schema:**
Table: continents
  Columns:
    - ContId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Continent (TEXT) # Example values: america, europe, asia, africa, australia

Table: countries
  Columns:
    - CountryId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - CountryName (TEXT) # Example values: usa, germany, france, japan, italy
    - Continent (INTEGER) # Example values: 1, 2, 3, 4, 5

Table: car_makers
  Columns:
    - Id (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Maker (TEXT) # Example values: amc, volkswagen, bmw, gm, ford
    - FullName (TEXT) # Example values: American Motor Company, Volkswagen, BMW, General Motors, Ford Motor Company
    - Country (TEXT) # Example values: 1, 2, 3, 4, 5

Table: model_list
  Columns:
    - ModelId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Maker (INTEGER) # Example values: 1, 2, 3, 4, 5
    - Model (TEXT) # Example values: amc, audi, bmw, buick, cadillac

Table: car_names
  Columns:
    - MakeId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Model (TEXT) # Example values: chevrolet, buick, plymouth, amc, ford
    - Make (TEXT) # Example values: chevrolet chevelle malibu, buick skylark 320, plymouth satellite, amc rebel sst, ford torino

Table: cars_data
  Columns:
    - Id (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - MPG (TEXT) # Example values: 18, 15, 16, 17, 14
    - Cylinders (INTEGER) # Example values: 8, 4, 6, 3, 5
    - Edispl (REAL) # Example values: 307.0, 350.0, 318.0, 304.0, 302.0
    - Horsepower (TEXT) # Example values: 130, 165, 150, 140, 198
    - Weight (INTEGER) # Example values: 3504, 3693, 3436, 3433, 3449
    - Accelerate (REAL) # Example values: 12.0, 11.5, 11.0, 10.5, 10.0
    - Year (INTEGER) # Example values: 1970, 1971, 1972, 1973, 1974


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., sqlite.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

Question: What are the names and ids of all makers with more than 3 models?</pre>

---

### ❓ Failed Question #13 (of 20 examples) - Question ID: `151`


**Question**: Which distinctive models are produced by maker with the full name General Motors or weighing more than 3500?


### ✅ Ground Truth SQL(s)

```sql
SELECT DISTINCT T2.Model FROM CAR_NAMES AS T1 JOIN MODEL_LIST AS T2 ON T1.Model  =  T2.Model JOIN CAR_MAKERS AS T3 ON T2.Maker  =  T3.Id JOIN CARS_DATA AS T4 ON T1.MakeId  =  T4.Id WHERE T3.FullName  =  'General Motors' OR T4.weight  >  3500;
```

### ❌ Predicted SQL

```sql
SELECT
  T1.Model
FROM model_list AS T1
JOIN car_makers AS T2
  ON T1.Maker = T2.Id
WHERE
  T2.FullName = 'General Motors'
UNION
SELECT
  T1.Model
FROM car_names AS T1
JOIN cars_data AS T2
  ON T1.MakeId = T2.Id
WHERE
  T2.Weight > 3500
```

### 📊 Evaluation Metrics

|   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |   execution_time_ms |
|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|--------------------:|
|                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          0 |             971 |                 107 |           4266 |             17741.7 |              212.27 |

### 📘 Ground Truth Result(s)

**Result 1:**

| Model         |
|:--------------|
| chevrolet     |
| buick         |
| ford          |
| plymouth      |
| pontiac       |
| amc           |
| dodge         |
| mercury       |
| oldsmobile    |
| chrysler      |
| mercedes-benz |
| cadillac      |
| mercedes      |

### 📕 Predicted Result

| Model         |
|:--------------|
| amc           |
| buick         |
| cadillac      |
| chevrolet     |
| chrysler      |
| dodge         |
| ford          |
| hi            |
| mercedes      |
| mercedes-benz |
| mercury       |
| oldsmobile    |
| plymouth      |
| pontiac       |

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given sqlite database schema.

**Question:**:
Which distinctive models are produced by maker with the full name General Motors or weighing more than 3500?

**Database Engine / Dialect:**:
sqlite

**Schema:**
Table: continents
  Columns:
    - ContId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Continent (TEXT) # Example values: america, europe, asia, africa, australia

Table: countries
  Columns:
    - CountryId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - CountryName (TEXT) # Example values: usa, germany, france, japan, italy
    - Continent (INTEGER) # Example values: 1, 2, 3, 4, 5

Table: car_makers
  Columns:
    - Id (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Maker (TEXT) # Example values: amc, volkswagen, bmw, gm, ford
    - FullName (TEXT) # Example values: American Motor Company, Volkswagen, BMW, General Motors, Ford Motor Company
    - Country (TEXT) # Example values: 1, 2, 3, 4, 5

Table: model_list
  Columns:
    - ModelId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Maker (INTEGER) # Example values: 1, 2, 3, 4, 5
    - Model (TEXT) # Example values: amc, audi, bmw, buick, cadillac

Table: car_names
  Columns:
    - MakeId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Model (TEXT) # Example values: chevrolet, buick, plymouth, amc, ford
    - Make (TEXT) # Example values: chevrolet chevelle malibu, buick skylark 320, plymouth satellite, amc rebel sst, ford torino

Table: cars_data
  Columns:
    - Id (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - MPG (TEXT) # Example values: 18, 15, 16, 17, 14
    - Cylinders (INTEGER) # Example values: 8, 4, 6, 3, 5
    - Edispl (REAL) # Example values: 307.0, 350.0, 318.0, 304.0, 302.0
    - Horsepower (TEXT) # Example values: 130, 165, 150, 140, 198
    - Weight (INTEGER) # Example values: 3504, 3693, 3436, 3433, 3449
    - Accelerate (REAL) # Example values: 12.0, 11.5, 11.0, 10.5, 10.0
    - Year (INTEGER) # Example values: 1970, 1971, 1972, 1973, 1974


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., sqlite.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

Question: Which distinctive models are produced by maker with the full name General Motors or weighing more than 3500?</pre>

---

### ❓ Failed Question #14 (of 20 examples) - Question ID: `152`


**Question**: What are the different models created by either the car maker General Motors or weighed more than 3500?


### ✅ Ground Truth SQL(s)

```sql
SELECT DISTINCT T2.Model FROM CAR_NAMES AS T1 JOIN MODEL_LIST AS T2 ON T1.Model  =  T2.Model JOIN CAR_MAKERS AS T3 ON T2.Maker  =  T3.Id JOIN CARS_DATA AS T4 ON T1.MakeId  =  T4.Id WHERE T3.FullName  =  'General Motors' OR T4.weight  >  3500;
```

### ❌ Predicted SQL

```sql
SELECT T1.Model FROM model_list AS T1 JOIN car_makers AS T2 ON T1.Maker = T2.Id WHERE T2.FullName = 'General Motors' UNION SELECT T1.Model FROM car_names AS T1 JOIN cars_data AS T2 ON T1.MakeId = T2.Id WHERE T2.Weight > 3500
```

### 📊 Evaluation Metrics

|   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |   execution_time_ms |
|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|--------------------:|
|                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          0 |             971 |                  87 |           6860 |             31655.3 |               235.3 |

### 📘 Ground Truth Result(s)

**Result 1:**

| Model         |
|:--------------|
| chevrolet     |
| buick         |
| ford          |
| plymouth      |
| pontiac       |
| amc           |
| dodge         |
| mercury       |
| oldsmobile    |
| chrysler      |
| mercedes-benz |
| cadillac      |
| mercedes      |

### 📕 Predicted Result

| Model         |
|:--------------|
| amc           |
| buick         |
| cadillac      |
| chevrolet     |
| chrysler      |
| dodge         |
| ford          |
| hi            |
| mercedes      |
| mercedes-benz |
| mercury       |
| oldsmobile    |
| plymouth      |
| pontiac       |

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given sqlite database schema.

**Question:**:
What are the different models created by either the car maker General Motors or weighed more than 3500?

**Database Engine / Dialect:**:
sqlite

**Schema:**
Table: continents
  Columns:
    - ContId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Continent (TEXT) # Example values: america, europe, asia, africa, australia

Table: countries
  Columns:
    - CountryId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - CountryName (TEXT) # Example values: usa, germany, france, japan, italy
    - Continent (INTEGER) # Example values: 1, 2, 3, 4, 5

Table: car_makers
  Columns:
    - Id (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Maker (TEXT) # Example values: amc, volkswagen, bmw, gm, ford
    - FullName (TEXT) # Example values: American Motor Company, Volkswagen, BMW, General Motors, Ford Motor Company
    - Country (TEXT) # Example values: 1, 2, 3, 4, 5

Table: model_list
  Columns:
    - ModelId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Maker (INTEGER) # Example values: 1, 2, 3, 4, 5
    - Model (TEXT) # Example values: amc, audi, bmw, buick, cadillac

Table: car_names
  Columns:
    - MakeId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Model (TEXT) # Example values: chevrolet, buick, plymouth, amc, ford
    - Make (TEXT) # Example values: chevrolet chevelle malibu, buick skylark 320, plymouth satellite, amc rebel sst, ford torino

Table: cars_data
  Columns:
    - Id (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - MPG (TEXT) # Example values: 18, 15, 16, 17, 14
    - Cylinders (INTEGER) # Example values: 8, 4, 6, 3, 5
    - Edispl (REAL) # Example values: 307.0, 350.0, 318.0, 304.0, 302.0
    - Horsepower (TEXT) # Example values: 130, 165, 150, 140, 198
    - Weight (INTEGER) # Example values: 3504, 3693, 3436, 3433, 3449
    - Accelerate (REAL) # Example values: 12.0, 11.5, 11.0, 10.5, 10.0
    - Year (INTEGER) # Example values: 1970, 1971, 1972, 1973, 1974


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., sqlite.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

Question: What are the different models created by either the car maker General Motors or weighed more than 3500?</pre>

---

### ❓ Failed Question #15 (of 20 examples) - Question ID: `161`


**Question**: How many countries has more than 2 car makers ?


### ✅ Ground Truth SQL(s)

```sql
select count(*) from countries as t1 join car_makers as t2 on t1.countryid  =  t2.country group by t1.countryid having count(*)  >  2
```

### ❌ Predicted SQL

```sql
SELECT count(*) FROM (SELECT Country FROM car_makers GROUP BY Country HAVING count(*) > 2)
```

### 📊 Evaluation Metrics

|   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |   execution_time_ms |
|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|--------------------:|
|                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          0 |             947 |                  26 |           2053 |             7225.91 |              112.49 |

### 📘 Ground Truth Result(s)

**Result 1:**

|   count(*) |
|-----------:|
|          4 |
|          4 |
|          3 |
|          5 |

### 📕 Predicted Result

|   count(*) |
|-----------:|
|          4 |

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given sqlite database schema.

**Question:**:
How many countries has more than 2 car makers ?

**Database Engine / Dialect:**:
sqlite

**Schema:**
Table: continents
  Columns:
    - ContId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Continent (TEXT) # Example values: america, europe, asia, africa, australia

Table: countries
  Columns:
    - CountryId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - CountryName (TEXT) # Example values: usa, germany, france, japan, italy
    - Continent (INTEGER) # Example values: 1, 2, 3, 4, 5

Table: car_makers
  Columns:
    - Id (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Maker (TEXT) # Example values: amc, volkswagen, bmw, gm, ford
    - FullName (TEXT) # Example values: American Motor Company, Volkswagen, BMW, General Motors, Ford Motor Company
    - Country (TEXT) # Example values: 1, 2, 3, 4, 5

Table: model_list
  Columns:
    - ModelId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Maker (INTEGER) # Example values: 1, 2, 3, 4, 5
    - Model (TEXT) # Example values: amc, audi, bmw, buick, cadillac

Table: car_names
  Columns:
    - MakeId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Model (TEXT) # Example values: chevrolet, buick, plymouth, amc, ford
    - Make (TEXT) # Example values: chevrolet chevelle malibu, buick skylark 320, plymouth satellite, amc rebel sst, ford torino

Table: cars_data
  Columns:
    - Id (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - MPG (TEXT) # Example values: 18, 15, 16, 17, 14
    - Cylinders (INTEGER) # Example values: 8, 4, 6, 3, 5
    - Edispl (REAL) # Example values: 307.0, 350.0, 318.0, 304.0, 302.0
    - Horsepower (TEXT) # Example values: 130, 165, 150, 140, 198
    - Weight (INTEGER) # Example values: 3504, 3693, 3436, 3433, 3449
    - Accelerate (REAL) # Example values: 12.0, 11.5, 11.0, 10.5, 10.0
    - Year (INTEGER) # Example values: 1970, 1971, 1972, 1973, 1974


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., sqlite.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

Question: How many countries has more than 2 car makers ?</pre>

---

### ❓ Failed Question #16 (of 20 examples) - Question ID: `162`


**Question**: What is the number of countries with more than 2 car makers ?


### ✅ Ground Truth SQL(s)

```sql
select count(*) from countries as t1 join car_makers as t2 on t1.countryid  =  t2.country group by t1.countryid having count(*)  >  2
```

### ❌ Predicted SQL

```sql
SELECT count(*) FROM (SELECT Country FROM car_makers GROUP BY Country HAVING count(*) > 2)
```

### 📊 Evaluation Metrics

|   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |   execution_time_ms |
|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|--------------------:|
|                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          0 |             953 |                  26 |           2161 |             7420.53 |              179.82 |

### 📘 Ground Truth Result(s)

**Result 1:**

|   count(*) |
|-----------:|
|          4 |
|          4 |
|          3 |
|          5 |

### 📕 Predicted Result

|   count(*) |
|-----------:|
|          4 |

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given sqlite database schema.

**Question:**:
What is the number of countries with more than 2 car makers ?

**Database Engine / Dialect:**:
sqlite

**Schema:**
Table: continents
  Columns:
    - ContId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Continent (TEXT) # Example values: america, europe, asia, africa, australia

Table: countries
  Columns:
    - CountryId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - CountryName (TEXT) # Example values: usa, germany, france, japan, italy
    - Continent (INTEGER) # Example values: 1, 2, 3, 4, 5

Table: car_makers
  Columns:
    - Id (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Maker (TEXT) # Example values: amc, volkswagen, bmw, gm, ford
    - FullName (TEXT) # Example values: American Motor Company, Volkswagen, BMW, General Motors, Ford Motor Company
    - Country (TEXT) # Example values: 1, 2, 3, 4, 5

Table: model_list
  Columns:
    - ModelId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Maker (INTEGER) # Example values: 1, 2, 3, 4, 5
    - Model (TEXT) # Example values: amc, audi, bmw, buick, cadillac

Table: car_names
  Columns:
    - MakeId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Model (TEXT) # Example values: chevrolet, buick, plymouth, amc, ford
    - Make (TEXT) # Example values: chevrolet chevelle malibu, buick skylark 320, plymouth satellite, amc rebel sst, ford torino

Table: cars_data
  Columns:
    - Id (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - MPG (TEXT) # Example values: 18, 15, 16, 17, 14
    - Cylinders (INTEGER) # Example values: 8, 4, 6, 3, 5
    - Edispl (REAL) # Example values: 307.0, 350.0, 318.0, 304.0, 302.0
    - Horsepower (TEXT) # Example values: 130, 165, 150, 140, 198
    - Weight (INTEGER) # Example values: 3504, 3693, 3436, 3433, 3449
    - Accelerate (REAL) # Example values: 12.0, 11.5, 11.0, 10.5, 10.0
    - Year (INTEGER) # Example values: 1970, 1971, 1972, 1973, 1974


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., sqlite.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

Question: What is the number of countries with more than 2 car makers ?</pre>

---

### ❓ Failed Question #17 (of 20 examples) - Question ID: `165`


**Question**: For the cars with 4 cylinders, which model has the largest horsepower?


### ✅ Ground Truth SQL(s)

```sql
SELECT T1.Model FROM CAR_NAMES AS T1 JOIN CARS_DATA AS T2 ON T1.MakeId  =  T2.Id WHERE T2.Cylinders  =  4 ORDER BY T2.horsepower DESC LIMIT 1;
```

### ❌ Predicted SQL

```sql
SELECT
  T1.Model
FROM car_names AS T1
JOIN cars_data AS T2
  ON T1.MakeId = T2.Id
WHERE
  T2.Cylinders = 4
ORDER BY
  CAST(T2.Horsepower AS INTEGER) DESC
LIMIT 1
```

### 📊 Evaluation Metrics

|   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |   execution_time_ms |
|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|--------------------:|
|                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          0 |             955 |                  75 |           3321 |             13275.7 |               133.9 |

### 📘 Ground Truth Result(s)

**Result 1:**

| Model   |
|:--------|
| ford    |

### 📕 Predicted Result

| Model   |
|:--------|
| citroen |

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given sqlite database schema.

**Question:**:
For the cars with 4 cylinders, which model has the largest horsepower?

**Database Engine / Dialect:**:
sqlite

**Schema:**
Table: continents
  Columns:
    - ContId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Continent (TEXT) # Example values: america, europe, asia, africa, australia

Table: countries
  Columns:
    - CountryId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - CountryName (TEXT) # Example values: usa, germany, france, japan, italy
    - Continent (INTEGER) # Example values: 1, 2, 3, 4, 5

Table: car_makers
  Columns:
    - Id (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Maker (TEXT) # Example values: amc, volkswagen, bmw, gm, ford
    - FullName (TEXT) # Example values: American Motor Company, Volkswagen, BMW, General Motors, Ford Motor Company
    - Country (TEXT) # Example values: 1, 2, 3, 4, 5

Table: model_list
  Columns:
    - ModelId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Maker (INTEGER) # Example values: 1, 2, 3, 4, 5
    - Model (TEXT) # Example values: amc, audi, bmw, buick, cadillac

Table: car_names
  Columns:
    - MakeId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Model (TEXT) # Example values: chevrolet, buick, plymouth, amc, ford
    - Make (TEXT) # Example values: chevrolet chevelle malibu, buick skylark 320, plymouth satellite, amc rebel sst, ford torino

Table: cars_data
  Columns:
    - Id (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - MPG (TEXT) # Example values: 18, 15, 16, 17, 14
    - Cylinders (INTEGER) # Example values: 8, 4, 6, 3, 5
    - Edispl (REAL) # Example values: 307.0, 350.0, 318.0, 304.0, 302.0
    - Horsepower (TEXT) # Example values: 130, 165, 150, 140, 198
    - Weight (INTEGER) # Example values: 3504, 3693, 3436, 3433, 3449
    - Accelerate (REAL) # Example values: 12.0, 11.5, 11.0, 10.5, 10.0
    - Year (INTEGER) # Example values: 1970, 1971, 1972, 1973, 1974


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., sqlite.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

Question: For the cars with 4 cylinders, which model has the largest horsepower?</pre>

---

### ❓ Failed Question #18 (of 20 examples) - Question ID: `166`


**Question**: For all of the 4 cylinder cars, which model has the most horsepower?


### ✅ Ground Truth SQL(s)

```sql
SELECT T1.Model FROM CAR_NAMES AS T1 JOIN CARS_DATA AS T2 ON T1.MakeId  =  T2.Id WHERE T2.Cylinders  =  4 ORDER BY T2.horsepower DESC LIMIT 1;
```

### ❌ Predicted SQL

```sql
SELECT
  T1.Model
FROM car_names AS T1
JOIN cars_data AS T2
  ON T1.MakeId = T2.Id
WHERE
  T2.Cylinders = 4
ORDER BY
  CAST(T2.Horsepower AS INTEGER) DESC
LIMIT 1
```

### 📊 Evaluation Metrics

|   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |   execution_time_ms |
|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|--------------------:|
|                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          0 |             957 |                  75 |           8731 |             43218.4 |               77.57 |

### 📘 Ground Truth Result(s)

**Result 1:**

| Model   |
|:--------|
| ford    |

### 📕 Predicted Result

| Model   |
|:--------|
| citroen |

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given sqlite database schema.

**Question:**:
For all of the 4 cylinder cars, which model has the most horsepower?

**Database Engine / Dialect:**:
sqlite

**Schema:**
Table: continents
  Columns:
    - ContId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Continent (TEXT) # Example values: america, europe, asia, africa, australia

Table: countries
  Columns:
    - CountryId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - CountryName (TEXT) # Example values: usa, germany, france, japan, italy
    - Continent (INTEGER) # Example values: 1, 2, 3, 4, 5

Table: car_makers
  Columns:
    - Id (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Maker (TEXT) # Example values: amc, volkswagen, bmw, gm, ford
    - FullName (TEXT) # Example values: American Motor Company, Volkswagen, BMW, General Motors, Ford Motor Company
    - Country (TEXT) # Example values: 1, 2, 3, 4, 5

Table: model_list
  Columns:
    - ModelId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Maker (INTEGER) # Example values: 1, 2, 3, 4, 5
    - Model (TEXT) # Example values: amc, audi, bmw, buick, cadillac

Table: car_names
  Columns:
    - MakeId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Model (TEXT) # Example values: chevrolet, buick, plymouth, amc, ford
    - Make (TEXT) # Example values: chevrolet chevelle malibu, buick skylark 320, plymouth satellite, amc rebel sst, ford torino

Table: cars_data
  Columns:
    - Id (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - MPG (TEXT) # Example values: 18, 15, 16, 17, 14
    - Cylinders (INTEGER) # Example values: 8, 4, 6, 3, 5
    - Edispl (REAL) # Example values: 307.0, 350.0, 318.0, 304.0, 302.0
    - Horsepower (TEXT) # Example values: 130, 165, 150, 140, 198
    - Weight (INTEGER) # Example values: 3504, 3693, 3436, 3433, 3449
    - Accelerate (REAL) # Example values: 12.0, 11.5, 11.0, 10.5, 10.0
    - Year (INTEGER) # Example values: 1970, 1971, 1972, 1973, 1974


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., sqlite.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

Question: For all of the 4 cylinder cars, which model has the most horsepower?</pre>

---

### ❓ Failed Question #19 (of 20 examples) - Question ID: `176`


**Question**: What are the ids and makers of all car makers that produce at least 2 models and make more than 3 cars?


### ✅ Ground Truth SQL(s)

```sql
SELECT T1.Id ,  T1.Maker FROM CAR_MAKERS AS T1 JOIN MODEL_LIST AS T2 ON T1.Id  =  T2.Maker GROUP BY T1.Id HAVING count(*)  >=  2 INTERSECT SELECT T1.Id ,  T1.Maker FROM CAR_MAKERS AS T1 JOIN MODEL_LIST AS T2 ON T1.Id  =  T2.Maker JOIN CAR_NAMES AS T3 ON T2.model  =  T3.model GROUP BY T1.Id HAVING count(*)  >  3;
```

### ❌ Predicted SQL

```sql
SELECT
  T1.Id,
  T1.Maker
FROM car_makers AS T1
JOIN model_list AS T2
  ON T1.Id = T2.Maker
JOIN car_names AS T3
  ON T2.Model = T3.Model
GROUP BY
  T1.Id
HAVING
  COUNT(DISTINCT T2.ModelId) >= 2 AND COUNT(T3.MakeId) > 3
```

### 📊 Evaluation Metrics

|   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |   execution_time_ms |
|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|--------------------:|
|                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          0 |             975 |                 107 |           3104 |             12184.4 |              138.59 |

### 📘 Ground Truth Result(s)

**Result 1:**

|   Id | Maker      |
|-----:|:-----------|
|    2 | volkswagen |
|    4 | gm         |
|    5 | ford       |
|    6 | chrysler   |
|    8 | nissan     |
|   19 | toyota     |

### 📕 Predicted Result

|   Id | Maker      |
|-----:|:-----------|
|    2 | volkswagen |
|    4 | gm         |
|    5 | ford       |
|    6 | chrysler   |
|    8 | nissan     |

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given sqlite database schema.

**Question:**:
What are the ids and makers of all car makers that produce at least 2 models and make more than 3 cars?

**Database Engine / Dialect:**:
sqlite

**Schema:**
Table: continents
  Columns:
    - ContId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Continent (TEXT) # Example values: america, europe, asia, africa, australia

Table: countries
  Columns:
    - CountryId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - CountryName (TEXT) # Example values: usa, germany, france, japan, italy
    - Continent (INTEGER) # Example values: 1, 2, 3, 4, 5

Table: car_makers
  Columns:
    - Id (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Maker (TEXT) # Example values: amc, volkswagen, bmw, gm, ford
    - FullName (TEXT) # Example values: American Motor Company, Volkswagen, BMW, General Motors, Ford Motor Company
    - Country (TEXT) # Example values: 1, 2, 3, 4, 5

Table: model_list
  Columns:
    - ModelId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Maker (INTEGER) # Example values: 1, 2, 3, 4, 5
    - Model (TEXT) # Example values: amc, audi, bmw, buick, cadillac

Table: car_names
  Columns:
    - MakeId (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Model (TEXT) # Example values: chevrolet, buick, plymouth, amc, ford
    - Make (TEXT) # Example values: chevrolet chevelle malibu, buick skylark 320, plymouth satellite, amc rebel sst, ford torino

Table: cars_data
  Columns:
    - Id (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - MPG (TEXT) # Example values: 18, 15, 16, 17, 14
    - Cylinders (INTEGER) # Example values: 8, 4, 6, 3, 5
    - Edispl (REAL) # Example values: 307.0, 350.0, 318.0, 304.0, 302.0
    - Horsepower (TEXT) # Example values: 130, 165, 150, 140, 198
    - Weight (INTEGER) # Example values: 3504, 3693, 3436, 3433, 3449
    - Accelerate (REAL) # Example values: 12.0, 11.5, 11.0, 10.5, 10.0
    - Year (INTEGER) # Example values: 1970, 1971, 1972, 1973, 1974


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., sqlite.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

Question: What are the ids and makers of all car makers that produce at least 2 models and make more than 3 cars?</pre>

---

### ❓ Failed Question #20 (of 20 examples) - Question ID: `182`


**Question**: Which abbreviation corresponds to Jetblue Airways?


### ✅ Ground Truth SQL(s)

```sql
SELECT Abbreviation FROM AIRLINES WHERE Airline  =  "JetBlue Airways"
```

### ❌ Predicted SQL

```sql
SELECT
  Abbreviation
FROM airlines
WHERE
  Airline = 'Jetblue Airways'
```

### 📊 Evaluation Metrics

|   execution_accuracy |   non_empty_execution_accuracy |   subset_non_empty_execution_accuracy |   logic_execution_accuracy |   bird_execution_accuracy |   is_sqlglot_parsable |   is_sqlparse_parsable |   sqlglot_equivalence |   sqlglot_optimized_equivalence |   sqlparse_equivalence |   sql_exact_match |   sql_syntactic_equivalence |   eval_error |   df_error |   prompt_tokens |   completion_tokens |   total_tokens |   inference_time_ms |   execution_time_ms |
|---------------------:|-------------------------------:|--------------------------------------:|---------------------------:|--------------------------:|----------------------:|-----------------------:|----------------------:|--------------------------------:|-----------------------:|------------------:|----------------------------:|-------------:|-----------:|----------------:|--------------------:|---------------:|--------------------:|--------------------:|
|                    0 |                              0 |                                     0 |                          0 |                         0 |                     1 |                      1 |                     0 |                               0 |                      0 |                 0 |                           0 |            0 |          0 |             545 |                  24 |            696 |             2048.01 |              144.35 |

### 📘 Ground Truth Result(s)

**Result 1:**

| Abbreviation   |
|:---------------|
| JetBlue        |

### 📕 Predicted Result

| Abbreviation   |
|----------------|

### 🧠 Prompt

<pre>Your task is to convert a natural language question into an accurate SQL query using the given sqlite database schema.

**Question:**:
Which abbreviation corresponds to Jetblue Airways?

**Database Engine / Dialect:**:
sqlite

**Schema:**
Table: airlines
  Columns:
    - uid (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - Airline (TEXT) # Example values: United Airlines, US Airways, Delta Airlines, Southwest Airlines, American Airlines
    - Abbreviation (TEXT) # Example values: UAL, USAir, Delta, Southwest, American
    - Country (TEXT) # Example values: USA

Table: airports
  Columns:
    - City (TEXT) # Example values: Aberdeen , Abilene , Abingdon , Ada , Adak Island 
    - AirportCode (TEXT) (Primary Key) # Example values: AAF, ABI, ABL, ABQ, ABR
    - AirportName (TEXT) # Example values: Phillips AAF , Municipal , Dyess AFB , Virginia Highlands , Ada 
    - Country (TEXT) # Example values: United States 
    - CountryAbbrev (TEXT) # Example values: US , US

Table: flights
  Columns:
    - Airline (INTEGER) (Primary Key) # Example values: 1, 2, 3, 4, 5
    - FlightNo (INTEGER) (Primary Key) # Example values: 28, 29, 44, 45, 54
    - SourceAirport (TEXT) # Example values:  APG,  ASY,  CVO,  ACV,  AHD
    - DestAirport (TEXT) # Example values:  ASY,  APG,  ACV,  CVO,  AHT


**Instructions:**
- Only use columns listed in the schema.
- Do not use any other columns or tables not mentioned in the schema.
- Ensure the SQL query is valid and executable.
- Use proper SQL syntax and conventions.
- Generate a complete SQL query that answers the question.
- Use the correct SQL dialect for the database, i.e., sqlite.
- Do not include any explanations or comments in the SQL output.
- Your output must start with ```sql and end with ```.

Question: Which abbreviation corresponds to Jetblue Airways?</pre>

---

