# Shareable links

Every view in the dashboard has its own address, so a benchmark, a pipeline, a
filtered error-analysis query, or a single record can be linked to directly.

```
/benchmark/{id}                                          benchmark summary
/benchmark/{id}/pipeline/{pipeline_id}                   pipeline detail
/benchmark/{id}/pipeline/{pipeline_id}/record/{record}   one record, in a pipeline
/benchmark/{id}/errors?pipeline=…&value=0                filtered error analysis
/benchmark/{id}/errors?…&record={record}                 one record, open
/benchmark/{id}/insights                                 metric insights
/benchmark/{id}/compare                                  pipeline comparison
/benchmark/{id}/compare/profile                          profile comparison
```

Filters, page number, page size and the open record are all carried in the query
string, so a link reproduces the exact view rather than the right page in a
default state.

**The address bar is the link.** There is no "copy this URL" button — it would
duplicate something every browser already offers.

## Short links

There is one case the address bar cannot cover. Pipeline ids embed the model
name, so a comparison link can carry two of
`wxai:openai/gpt-oss-120b-agentic-baseline1-3attempts` and end up long enough
for a mail client to wrap it and a chat app to truncate it.

**Copy short link**, which appears in the header only on an address that names a
pipeline, substitutes a ten-character alias for each one:

```
/benchmark/archer_en_dev/pipeline/ec64b733f4     the same view, 90 characters shorter
```

The dashboard expands the alias on arrival and rewrites the address, so the
readable form stays canonical and nothing downstream ever sees an alias.

Aliases are **derived** from the id, not assigned — `GET
/api/benchmarks/{id}/pipeline-aliases` returns the table — so any server reading
the same results agrees on them without coordinating, and a link survives
re-fetching the snapshot.

They shorten links; they do **not** survive a model being renamed. The alias is a
hash of the id, so it changes when the id does. Surviving a rename would need a
stored mapping from a stable key to whatever the id is called today, and the
artifacts are keyed by the id itself.

A colliding alias resolves to *neither* pipeline rather than an arbitrary one: a
link that says "not found" can be chased up with whoever sent it, and one that
quietly opens the wrong pipeline cannot.

## When a link does not resolve

Shared links are the one case where the target reliably might not exist on the
recipient's server — a different results snapshot has different benchmarks.

Every route renders an explicit state for that rather than a blank page: an
unknown benchmark says so and names it, an unknown alias says it may be from a
different snapshot, and an unknown path offers a way back.
