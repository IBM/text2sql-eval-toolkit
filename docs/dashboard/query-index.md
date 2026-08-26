# Query index

Evaluation artifacts reach hundreds of megabytes — Beaver's is 880 MB across 209
records, because each record carries the full result set of every query it ran.
The dashboard reads them through a SQLite index built alongside each file rather
than parsing JSON per request.

```bash
text2sql-eval-toolkit index build     # build or refresh
text2sql-eval-toolkit index status    # show what is current
```

The index is **derived and disposable**: deleting it costs a rebuild, never data.
It is rebuilt automatically when its artifact changes, detected by size and
mtime, so a re-run is picked up without restarting the server.

## What it holds

Records, their byte offsets in the source file, each prediction's evaluation
block, every numeric metric, and each record's profiling categories. Pipeline
ids and metric names are interned to integers — they average 55 and 19
characters and repeat on every row — which is what keeps the index at roughly 6%
of the artifact it indexes.

Record detail is a seek to a stored byte range, so its cost is independent of
file size.

## What it bought

Measured over the full published result set (1,915 MB of artifacts):

| | before | after |
|---|---|---|
| Indices | — | 117 MB (6%) |
| First page of records | 900+ ms | 3–11 ms |
| Record detail | 921 ms | 0.3 ms |
| Peak RSS | 2,151 MB | 170 MB |
| By-category summary (Beaver) | 13.9 s | 0.05 s |

The last row was found after deployment: that endpoint had been converted to
streaming, which bounded its memory but not its time — it still read every byte
to collect a few tens of thousands of floats. Bounded memory and bounded time
are different claims.

## Building it

Building is memory-hungry: peak is driven by the **largest single record**, not
by the file, and Beaver holds one of 108 MB whose parsed form costs several
hundred more. Build indices before serving rather than under load.

A shared deployment refuses to build on demand at all — an unprovisioned
benchmark returns 503, and provisioning owns index building. A local run builds
whatever is missing.

## Schema changes

The index records a schema version. When it changes, existing indices are
treated as stale and rebuilt — automatically on a local run, and by provisioning
on a shared one. Deploying a release that changes the schema without rebuilding
first gives 503 for every benchmark until you do.
