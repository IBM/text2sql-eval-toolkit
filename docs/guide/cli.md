# Command line

Two console scripts are installed with the package.

## `text2sql-eval-toolkit`

Manages pre-computed results and the dashboard's query index.

### `results`

```bash
text2sql-eval-toolkit results fetch     # download from the Hugging Face Hub
text2sql-eval-toolkit results list      # what the Hub manifest offers
text2sql-eval-toolkit results clear     # remove the local copy
```

The full set is around 7 GB, which is why it is fetched rather than shipped in
the wheel. `list` reads the manifest without downloading anything, so it is the
cheap way to see what a release contains.

The library equivalents are
[`fetch_results`][text2sql_eval_toolkit.fetch_results],
[`list_available_results`][text2sql_eval_toolkit.list_available_results] and
[`clear_cache`][text2sql_eval_toolkit.clear_cache].

### `index`

```bash
text2sql-eval-toolkit index build       # build the query index
text2sql-eval-toolkit index status      # is each benchmark's index current?
```

The index is what makes the dashboard usable on large benchmarks: without it,
fetching one record's detail parses the entire evaluation file and scans it
linearly. `status` reports per benchmark, so a stale index is visible rather
than merely slow.

The dashboard builds indices on demand, so this is mostly for provisioning a
deployment ahead of the first visitor.

## `text2sql-eval-dashboard`

Serves the web UI.

```bash
text2sql-eval-dashboard --open-browser
```

| Flag | Effect |
|---|---|
| `--host`, `--port` | Where to listen. Defaults to loopback |
| `--mode {public,judge,full}` | The capability ceiling — see below |
| `--allow-remote-full` | Permit `full` off loopback. Read the warning first |
| `--open-browser` | Open a browser once the server is up |
| `--enable-fetch` | Allow fetching results from the UI |
| `--forwarded-allow-ips` | Which proxies' `X-Forwarded-*` headers to believe |
| `--watch-dashboard` / `--no-watch-dashboard` | Rebuild the frontend on change; needs npm and a source tree |

### Modes

The mode is a **ceiling**, not a role. It bounds what anyone can do, and
identity can only narrow it further.

| Mode | Allows |
|---|---|
| `public` | Browsing pre-computed results, read-only |
| `judge` | The above, plus LLM-as-judge for signed-in users with the role |
| `full` | Everything: SQL execution, registry writes, re-evaluation |

`full` is the default because the common case is a single operator on their own
machine. It is **refused on a non-loopback interface** unless you pass
`--allow-remote-full`, because it exposes arbitrary SQL execution against
whatever database credentials the server holds to anyone who can reach the port.

!!! danger "`--allow-remote-full` is not how you serve a public dashboard"
    Use `public` or `judge` for anything reachable by people you do not trust
    with your databases. If you do run `full` remotely, sign-in is what stands
    between the internet and your credentials — anonymous callers resolve to
    `public` and are refused.

### Behind a proxy

`--forwarded-allow-ips` must name the proxy, or the app sees every request as
plain HTTP and builds an `http://` OAuth redirect URI that Google rejects. The
default of `127.0.0.1` is right for a local run and wrong for a proxy in another
container.

## Scripts

A source checkout also carries thin `argparse` wrappers in `scripts/`, one per
stage, plus `run_experiment.py` for a full inference → execution → evaluation
pass over one benchmark. They take the same arguments as the library functions
they wrap.
