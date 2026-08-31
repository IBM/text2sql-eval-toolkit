# The state of the art in text-to-SQL evaluation

Getting a model to write SQL is the easy half. Deciding whether the SQL it wrote
is *right* turns out to be the hard one, and most of the disagreement between
published results comes from that second half rather than the first.

This note is a map of how the field measures text-to-SQL systems, what each
family of measurements actually tells you, and where each one lies to you. It is
written against the metrics this toolkit computes, because those are the ones
you can inspect record by record in the views next door.

## Three families, and they do not agree

Every text-to-SQL metric in common use belongs to one of three families.

**String and structure comparison.** Compare the generated query to a reference
query as text or as a parse tree. `sql_exact_match` is the strictest version —
normalise whitespace and casing, then require the strings to be equal. Spider's
*exact set match* is the refined version: decompose both queries into clauses,
compare the clauses as sets, so `WHERE a AND b` matches `WHERE b AND a`.

**Execution comparison.** Run both queries against the database and compare the
result tables. This is `execution_accuracy` and its relatives. It is what most
modern leaderboards report, because it does not punish a model for arriving at
the right answer by a different route.

**Model-based judgement.** Show a language model the question, the reference
query, the generated query and often the results, and ask whether the generated
one answers the question. This toolkit's `llm_score` and `llm_explanation`.

The families disagree constantly, and the disagreements are not noise. A view
elsewhere in this dashboard exists specifically to show you records where two
metrics reach opposite verdicts, because those records are where the interesting
failures live.

## Why string comparison stopped being enough

Two queries can be textually different and semantically identical:

```sql
-- reference
SELECT name FROM singer WHERE age > 30 ORDER BY name;

-- prediction
SELECT s.name FROM singer AS s WHERE s.age > 30 ORDER BY s.name ASC;
```

Aliasing, an explicit `ASC`, a qualified column. Exact match scores this zero.
Every system is penalised for stylistic choices that no user would notice.

Set-based structural match fixes the easy cases and not the hard ones. Rewriting
a join as a subquery, a `HAVING` as a filtered subquery, or an `IN` as an
`EXISTS` produces a query that is provably equivalent and structurally
unrecognisable. This toolkit carries three separate attempts at the problem —
`sqlglot_equivalence`, `sqlglot_optimized_equivalence` and `sqlparse_equivalence`
— plus `sql_equivalence`, which fires if any of them does. Running three parsers
and taking the union is an admission that no single one of them is right.

The one thing structural comparison is genuinely good for is what execution
comparison cannot do at all: it is defined when there is no database to run
against, and it is deterministic.

## Why execution comparison is not enough either

Execution comparison has one dominant failure mode, and it is the reason the
field keeps adding metrics.

**The empty result problem.** If the reference query returns no rows on this
database, then so does `SELECT name FROM singer WHERE 1 = 0`, and so does a
query that filters on entirely the wrong column. Two empty tables compare equal.
On a benchmark with a meaningful number of empty-result questions, a model can
score well by being confidently wrong in a way that happens to return nothing.

That is what `non_empty_execution_accuracy` is for: it is `execution_accuracy`
with the empty case excluded. The gap between the two numbers is a direct
measure of how much of a reported score is being carried by empty results, and
it is worth looking at before believing any headline figure.

**The single-database problem.** A query is judged on one database instance. A
prediction that hardcodes a value seen in that instance — `WHERE country =
'France'` where the reference computes the country — passes. Test-suite
accuracy, introduced by Zhong et al. (2020), attacks this directly: generate
several databases designed to distinguish semantically different queries, and
require the results to match on all of them. It is a substantially stronger
signal and substantially more expensive to produce, which is why it is less
widely adopted than it deserves to be.

**The column order and naming problem.** Does `SELECT name, age` match
`SELECT age, name`? Does a column aliased differently match? Every framework
answers this slightly differently, which is a large part of why the same model
scores differently on the same benchmark in two papers.

**The strictness problem.** Sometimes a prediction returns the right answer plus
an extra column, or the right rows within a larger set. Strict comparison says
no. This toolkit's `subset_non_empty_execution_accuracy` says yes to a non-empty
subset or superset relationship, and `bird_relaxed_match` implements the more
forgiving comparison BIRD uses. Neither is *correct* in the abstract — they are
different questions, and which one you want depends on whether the consumer of
the query is a person reading a table or a program indexing into column 3.

## The benchmarks, and what they were each reacting to

**WikiSQL** established the task at scale and constrained it enough to be
tractable: single table, no joins, a template-shaped query space. Largely solved,
and largely stopped being informative once it was.

**Spider** made it hard again by making it cross-domain: the databases in the
test set are ones the model has never seen, with multiple tables and joins, so a
system cannot memorise schemas. It also fixed the evaluation vocabulary that is
still in use — exact set match and execution accuracy as separate reported
numbers.

**BIRD** made it hard in a different direction: bigger, dirtier databases with
the kind of column naming and missing values that real warehouses have, plus
external knowledge that has to be applied to answer the question at all. It also
introduced efficiency as a scored dimension rather than an afterthought, on the
argument that a query which takes four minutes is not a good answer even if the
rows are right.

The trajectory is consistent. Each benchmark closes the gap between the task as
measured and the task as encountered, and each one shows that the previous
generation's scores were higher than the underlying capability warranted.

## LLM-as-judge: what it is good for

Model-based judgement is the newest family and the most contested. The case for
it is real: it is the only method that can read the *question* rather than only
the two queries, so it is the only one that can catch a prediction which matches
the reference and answers a different question than the one asked — which
happens when the reference query is itself wrong, and that is more common in
these benchmarks than anyone would like.

The case against is equally real:

- It is not deterministic. Re-running it can change the verdict, which makes
  differences between systems hard to attribute.
- It is expensive per record, so it tends to be sampled rather than run
  everywhere, which reintroduces sampling error.
- The judge has opinions about SQL, and those opinions are correlated with the
  opinions of the model being judged, particularly when they are the same model.

This toolkit's position is that the judge is a lens rather than a score. Its
verdict is stored beside an `llm_explanation` — prose you can read — and it is
cached against the record, the pipeline and a digest of the judge's own
configuration, so a verdict is reproducible for as long as nothing that produced
it has changed. The design assumption is that you will read the explanations on
the records where the judge disagrees with execution, not that you will report
`llm_score` as a headline.

## What to actually report

If you take one thing from this note, take this: **a single number is not a
result.** The defensible minimum is three.

1. `execution_accuracy` — the headline, comparable to published work.
2. `non_empty_execution_accuracy` — how much of that headline is real.
3. A structural or judge-based measure — what kind of wrong the failures are.

And then the slice. A benchmark-wide average tells you almost nothing about
where a system breaks; the same average can come from uniform mediocrity or from
excellence on simple queries and collapse on anything with a join. That is what
the profiling stage is for, and why every metric in this toolkit is sliceable by
SQL feature.

## Where this is going

Three things look like the direction of travel.

**Execution-based evaluation on more than one database instance.** Test-suite
accuracy is the right idea and it is under-used, mostly for tooling reasons
rather than conceptual ones.

**Evaluation of the interaction, not the query.** Real analysts do not ask one
question and stop. A system that asks a clarifying question rather than guessing
is better, and every metric described above scores it as a failure.

**Cost as a first-class axis.** BIRD started this with efficiency. The full
version is tokens, latency and money per correct answer — which this toolkit
records per prediction, because a system that is two points better and twenty
times more expensive is a different product, not a better one.

---

*The metrics named here are computed by this toolkit and defined in
`evaluation/metric_definitions.py`; the dashboard renders its metric help from
that same list, so what you read there and what is computed cannot drift apart.*
