# Profiler Benchmarks

This source-only package powers the impact-first **PR Performance Report**. It is
separate from the runtime profiler in `profiler/` and standalone `benchmarks/`.

## Local use

```bash
# Requires build dependencies, pyarrow, and an AdventureWorks2022 connection.
python -m eng.profiler_benchmarks.controller --base main --candidate HEAD \
    --leg Linux-SQL2022 --output profiler-results
python -m eng.profiler_benchmarks.report profiler-results/report.json
```

The fixed registry has 24 tasks. `--scenarios` runs a local subset, but subset
reports remain incomplete and cannot produce a verdict.

`lob_varchar_256k_fetchall` fetches one 256 KiB `VARCHAR(MAX)` value to exercise
multi-chunk streaming. Query setup and exact payload validation are outside the
timed fetch window.

`catalog_columns_2`, `catalog_columns_118`, and `catalog_columns_2111` time
`columns()` plus the complete `fetchall()` drain on a fresh cursor. Each task
creates UUID-prefixed tables in the current database's `dbo` schema, with exactly
2, 118, or 2,111 nullable `INT` columns in total (at most 704 per table). It needs
permission to create tables and uses the profiler-owned connection with autocommit
off, not a caller's connection with pending writes. Its DDL is uncommitted and
rolled back on success or failure. Fixture setup, cursor creation, EOF checks, and
comparison of all 29 provider fields, raw descriptions, ordered cells and Python
types with an independent rowwise drain are outside the measurement window.
The rowwise oracle runs after timing,
so it does not prime the measured catalog allocation. Native call counters verify
one `SQLColumns` and one `FetchAll` call in the measured window.

These tasks cover small results, growth through both initial tiers, and reuse of
the maximum tier. They measure instrumented latency, not allocation bytes, and
are not the same fixtures as the standalone catalog experiments. Ordinary SELECT
fetch-all remains a separate control. The unchanged advisory thresholds below can
leave smaller catalog slowdowns labeled "no signal"; that is not proof of no
regression.

## Measurement contract

CI uses the PR merge's first parent as the exact base. It reuses the
profiling-enabled candidate build after pytest and builds the base separately.
Fresh processes run five measured pairs after one warmup pair in alternating order.

Workers have six minutes each. CI allows 90 minutes inside a 100-minute step and
160-minute job; local runs receive 105 minutes because they build both revisions.
Partial results never produce a verdict.

## Publication

Two environments publish raw samples: Unix on Ubuntu with SQL Server 2022/2025.
Routine Windows and macOS profiling is intentionally excluded because neutral PRs
showed platform variance above the regression threshold, while both platforms
remain covered by functional CI. Same-repository PRs run their formatter directly;
fork PRs retain the trusted-base publisher. Both select the exact PR-head ADO build
and validate bounded artifacts as data. Publication begins as soon as both profiler
artifacts exist, without waiting for unrelated matrix legs. After build completion,
missing artifacts receive a two-minute propagation grace before a partial result
is published. A failed aggregate build can still publish usable profiler artifacts.
Exact-head reports may finalize after merge; stale heads are ignored. Missing,
malformed, canceled, incomplete, or invalid data remains unavailable.

The report highlights consistent slowdowns and improvements using the same 20%
median change, 1 ms absolute change, and 80% pair-agreement requirements.

The publisher waits up to 220 minutes inside a 230-minute workflow. The first main
comparison after introduction may be incomplete because its parent lacks this
infrastructure. A fresh ADO-only retry also requires rerunning the GitHub publisher.
