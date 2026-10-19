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

`scalar_fetchval` fetches 10,000 ordered, non-NULL integers from the existing test
table through `fetchval()`, followed by one EOF call. It requires debug logging to
be disabled without changing logger configuration. Query setup, exact-value/type
validation (including zero), EOF validation, and diagnostic checks are outside
the timed fetch window. Both revisions run the same workload; the report's
improvement thresholds are unchanged.

`output_converter_fetchall` and `output_converter_fetchmany1000` measure separate
fetch APIs over 10,000 ordered integer/NVARCHAR rows from the existing fixture.
An `SQL_WVARCHAR` converter decodes UTF-16LE bytes and uppercases the text.
Both retain all converted rows through the timer boundary; the batched case
includes the terminal empty `fetchmany(1000)` call and accumulating its batches.
The timed callback only performs UTF-16LE decode and uppercase conversion.
Exact output values/types, count/order, and diagnostics are checked after
collection; focused contract tests check callback bytes using a fake cursor.
Registration, query execution, expected-value construction, validation, cursor
cleanup, and restoring/removing the converter are outside the fetch timer.
Converter state is restored even if setup, fetching, or validation fails.

These CI workloads use the fixture's `unicode_row_{i}` text rather than the
separate profiling-OFF study's fixed-width text.
Reports calculate fresh paired changes from profiling-ON measurements;
they do not embed historical speedups or promise to reproduce production-latency
results. The two API rows use the existing comparison policy below.

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
