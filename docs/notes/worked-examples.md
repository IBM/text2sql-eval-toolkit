# Worked examples: when the metrics disagree

The interesting records in any evaluation run are the ones where two metrics
reach opposite conclusions. This note walks through the recurring shapes, what
each one means, and what to do about it.

Every example here is a pattern you will find in this dashboard's Error Analysis
view by filtering on one metric and comparing against another — the "disagree"
filter exists for exactly this.

## 1. Execution passes, structure fails

The most common disagreement, and usually the least interesting.

```sql
-- ground truth
SELECT T1.name FROM singer AS T1
  JOIN concert AS T2 ON T1.singer_id = T2.singer_id
  WHERE T2.year = 2014;

-- prediction
SELECT name FROM singer
  WHERE singer_id IN (SELECT singer_id FROM concert WHERE year = 2014);
```

`execution_accuracy` = 1. `sql_exact_match` = 0, and `sqlglot_equivalence`
probably 0 too — a join and an `IN`-subquery are not the same tree, and only the
optimiser-based comparison has any chance of reconciling them.

**Verdict:** the prediction is correct. This is the case that motivated
execution-based evaluation in the first place.

**What to do:** nothing, except stop reporting exact match as a primary number.
The volume of this shape is a good sanity check that your structural metrics are
working — if `sql_equivalence` agrees with `execution_accuracy` on nearly every
record, one of the two is not doing what you think.

## 2. Execution passes, and the prediction is wrong

The dangerous one.

```sql
-- question: "Which singers have never performed at a concert?"

-- ground truth
SELECT name FROM singer
  WHERE singer_id NOT IN (SELECT singer_id FROM concert);

-- prediction
SELECT name FROM singer WHERE singer_id > 9999;
```

If every singer in this database has performed, both queries return zero rows,
and `execution_accuracy` = 1.

**Verdict:** the prediction is nonsense and the metric says it is perfect.

**How to catch it:** `non_empty_execution_accuracy` is 0 here while
`execution_accuracy` is 1. That pair, disagreeing, *is* the detector. Filter the
Error Analysis view on `execution_accuracy = 1` and `non_empty_execution_accuracy
= 0` and read what comes back; on some benchmarks it is a few records and on
others it is several per cent of the total, which is several per cent of a
headline score that means nothing.

This is also the shape the LLM judge is best at, because it is the only metric
that sees the *question* and can notice that `singer_id > 9999` has nothing to do
with concerts.

## 3. Execution fails, and the prediction is right

The mirror image, and the one that makes systems look worse than they are.

```sql
-- ground truth
SELECT name, age FROM singer ORDER BY age DESC LIMIT 1;

-- prediction
SELECT age, name FROM singer ORDER BY age DESC LIMIT 1;
```

Same row, columns transposed. Strict comparison says no.

**Verdict:** correct for a human reading the answer; incorrect for a program
indexing into column 0. Which one you want is a decision about your application,
not a fact about the query.

**Where it shows up:** `subset_non_empty_execution_accuracy` and
`bird_relaxed_match` are the relaxed comparisons. If the relaxed metrics are much
higher than the strict one, the gap is largely made of this shape plus shape 4
below, and the honest thing is to report both numbers rather than pick the
flattering one.

## 4. Extra columns

```sql
-- ground truth
SELECT name FROM singer WHERE country = 'France';

-- prediction
SELECT name, country, age FROM singer WHERE country = 'France';
```

Right rows, extra columns — the model being helpful.

`execution_accuracy` = 0. `subset_non_empty_execution_accuracy` = 1, because the
ground-truth result is contained in the prediction's.

**Verdict:** depends entirely on the consumer. A dashboard is fine with it; a
downstream `df["col"][0]` is not.

**What to do:** decide once, per project, which of the two you report as primary,
write it down, and then never quietly switch. Most of the irreproducibility in
published text-to-SQL numbers is this decision being made differently and not
stated.

## 5. The ground truth is wrong

It happens more than the benchmarks admit.

```sql
-- question: "What is the average age of singers from France?"

-- ground truth
SELECT avg(age) FROM singer;          -- the country filter is missing

-- prediction
SELECT avg(age) FROM singer WHERE country = 'France';
```

Every execution metric scores this 0. Every structural metric scores it 0. The
model is right and the benchmark is wrong.

**How to catch it:** this is the case for the LLM judge, and close to the only
case where it earns its cost. It reads the question, and it is the only evaluator
in the stack that can notice the reference does not answer it. Filter on
`execution_accuracy = 0` and `llm_score = 1`, and read the `llm_explanation`
column.

Expect false positives — a judge that is willing to say the ground truth is wrong
is also willing to say it when it is not. Treat the result as a queue of records
to look at, never as a correction to apply.

## 6. The prediction does not parse

```
sqlglot_parsable = 0, sqlparse_parsable = 0, df_error = 1
```

Nothing to compare. Worth separating from "parsed and produced the wrong answer",
because the fix is completely different: an unparseable output is usually a
prompt or a decoding problem, not a reasoning one. If the rate is non-trivial,
look at `df_error_message` before looking at anything else — a run where 8% of
outputs are fenced Markdown that was never stripped is a bug in the pipeline, not
a result about the model.

## Reading a run, in order

A practical sequence, which is roughly the order the dashboard's views are laid
out in:

1. **`df_error` and the parse metrics first.** If these are non-zero, fix the
   pipeline before reading any accuracy number.
2. **`execution_accuracy` against `non_empty_execution_accuracy`.** The gap is
   how much of the headline is empty results.
3. **Strict against relaxed.** The gap is column order, extra columns and
   subsets — a different question, not a better score.
4. **Slice by SQL feature.** The profiling stage tags every query; an average
   over a benchmark hides that a system is fine until it meets a `GROUP BY`.
5. **Only then, the disagreement views.** Sample the records where two metrics
   conflict and read them. Fifty records read carefully will tell you more about
   a system than any aggregate.
6. **Cost last, and never leave it out.** Tokens and latency are recorded per
   prediction. Two points of accuracy for twenty times the cost is a different
   product, not a better one.

---

*The SQL above is illustrative and written against Spider's `concert_singer`
schema for familiarity; the shapes it demonstrates are what you will find in any
benchmark loaded into this dashboard.*
