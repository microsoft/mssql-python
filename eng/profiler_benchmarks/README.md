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

The fixed registry has 23 tasks. `--scenarios` runs a local subset, but subset
reports remain incomplete and cannot produce a verdict.

`lob_varchar_256k_fetchall` fetches one 256 KiB `VARCHAR(MAX)` value to exercise
multi-chunk streaming. Query setup and exact payload validation are outside the
timed fetch window.

`scalar_fetchval` fetches 10,000 ordered, non-NULL integers from the existing test
table through `fetchval()`, followed by one EOF call. It requires debug logging to
be disabled without changing logger configuration. Query setup, exact-value/type
validation (including zero), EOF validation, and diagnostic checks are outside
the timed fetch window. Both revisions run the same workload; the report's
improvement thresholds are unchanged.

`row_to_dict` measures **warm-only** conversion of 10,000 real cursor-fetched rows
with 32 unique integer columns and deterministic NULLs using `dict(row._mapping)`.
It uses the existing `#perf_test` fixture and selects the first 10,000 rows in `id`
order. All 32 output values are NULL when the source `int_col` is divisible by 7.
SQL and expected values are constructed, rows are fetched, and every row's
mapping is converted once before timing on both revisions. Only construction and
retention of the 10,000 output dictionaries is timed; fetch, cold metadata
preparation, key/order/value/type/NULL checks, row counts, cursor diagnostics,
and cleanup are excluded. This is one additional workload, not a cold/warm or
width matrix.

This conversion has no guaranteed instrumented native phase, so **only**
`row_to_dict` permits an empty native diagnostics dictionary. Skipped results,
malformed counters, and missing native samples from the other 22 tasks remain
invalid in both the worker and report validation. The row uses the existing
paired wall-time comparison and unchanged thresholds: it can demonstrate an
observed main-relative improvement, not assert a speedup in advance or establish
pyodbc superiority or broader performance qualification.

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
