# A demo, in six screens

What to show, in what order, and the point each screen is making. Roughly twelve
minutes if nobody interrupts, which they will.

The argument the whole thing is making: **a text-to-SQL leaderboard number is a
summary of a summary, and this toolkit gives you back the two layers underneath
it.**

## 1. Benchmarks — the shape of what is here

Open with the benchmark tiles. Say what a benchmark is in this system: a set of
questions, a database, and one or more reference queries per question.

The point to make is that everything downstream is stored per record. There is
no aggregate here that cannot be opened.

## 2. A pipeline's summary — the number everyone quotes

Pick a benchmark, open one pipeline. This is the leaderboard view: one row per
metric, the numbers a paper would report.

Then say the thing the rest of the demo is about: *these numbers disagree with
each other, and the disagreement is the finding.* Point at `execution_accuracy`
and `non_empty_execution_accuracy` side by side. The gap between them is how much
of the headline is being carried by questions where the right answer is an empty
table — and where a query filtering on the wrong column scores exactly as well.

## 3. Pipeline Compare — two systems, one table

Two models on the same benchmark, metric by metric.

The point: the ordering of two systems can change depending on which metric you
read, and that is not a flaw in the tool. Strict execution match and the relaxed
comparison are answering different questions — "are these tables identical" and
"does this table contain the answer" — and a system tuned for one is not
automatically better at the other.

If the two systems rank differently under strict and relaxed matching, stop and
show it. It is the most persuasive thirty seconds available.

## 4. Error Analysis — the disagreement filter

This is the centre of the demo. Filter to records where two metrics conflict.

Good ones to reach for, in descending order of how well they land:

- `execution_accuracy = 1` and `non_empty_execution_accuracy = 0` — the model
  scored a point for returning nothing. Open one and read the SQL.
- `execution_accuracy = 0` and `llm_score = 1` — the judge thinks the model is
  right and the benchmark is wrong. Read the `llm_explanation`. Sometimes the
  judge is being credulous; sometimes the reference query really is missing a
  filter, and that is worth the audience's full attention.
- `execution_accuracy = 1` and `sql_exact_match = 0` — the ordinary case, and
  worth showing once so the room understands why execution-based evaluation
  exists at all.

Open the record. The question, the reference SQL, the generated SQL, both result
tables, every metric. This is the layer under the layer.

## 5. Eval Playground — do it live

Type a query, run it, evaluate it. Then run the LLM judge on the same record and
read the verdict.

Two things to say while it runs:

- The verdict is cached against the record, the pipeline, the config name and a
  digest of the config's *contents*. Re-running an unchanged judge costs nothing;
  changing a word in the prompt invalidates it. That is what makes a judge score
  reproducible rather than a coin flip.
- The address bar now carries the judge config. That link restores this exact
  verdict for whoever you send it to — and opening it reads the cache only. It
  never starts an inference, because sharing an answer is not authorisation to
  spend someone else's budget.

If there is time, open the judge config editor and show that the prompt is
editable, versioned as YAML, and that saving it writes to the data root rather
than into the installed package.

## 6. Docs — where the argument is written down

Close here. `state-of-the-art.md` for the survey, `worked-examples.md` for the
catalogue of disagreements, the embedded API reference for anyone who wants to
run it themselves.

The closing line, if one is wanted: every screen in this demo is addressable.
Everything shown can be linked to, in an issue or a paper, and it will open on
the same record.

---

## Practicalities

- **Have a benchmark with results loaded before you start.** `text2sql-eval-toolkit
  results fetch` pulls the pre-computed set; it is about 4 GB and it is not a
  thing to do in front of an audience.
- **Pick the disagreeing records in advance.** The filters will find them, but
  reading three candidates live to find a good one is dead air. Have the URLs.
- **The judge costs money per call.** If the deployment has a spend cap, check
  the meter before the demo rather than discovering it at screen 5.
- **Every view is a real link.** Open one in a new tab early, so the room sees
  that it works and you do not have to claim it.
