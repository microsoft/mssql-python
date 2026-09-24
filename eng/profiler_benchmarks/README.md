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

The fixed registry has 21 tasks. `--scenarios` runs a local subset, but subset
reports remain incomplete and cannot produce a verdict.

## Large-value fetch coverage

One task, `lob_varchar_256k_fetchall`, fetches one 256 KiB `VARCHAR(MAX)` value
through `fetchall()`. Unlike a million short rows, this value requires LOB
continuation calls. Unicode/binary variants, other fetch APIs, and chunk-boundary
combinations belong in functional regression coverage or targeted performance
investigations rather than multiplying routine CI tasks.

The payload size describes SQL data bytes, not row counts or a promise about the
driver's internal chunk count. Query execution/setup and exact payload/type
validation are outside the timed fetch window. An unexpected warning or truncated
value fails the workload rather than producing a successful performance verdict.

The existing elapsed-time thresholds remain unchanged. Available
`SQLGetDiagRec` profiler counters are supporting diagnostics, not a separate gate;
an absent counter is reported as unavailable, not zero. The old
`SQLGetAllDiagRecords` helper count is not equivalent to the number of underlying
ODBC calls. Mixed-warning preservation belongs in functional driver regressions,
not these clean-payload timings.

Both revisions run the same new workloads, so the first comparison can include a
base that predates them. Older artifacts missing these tasks remain incomplete or
invalid; they must not produce a full-coverage verdict.

The added task runs 24 times across routine CI: two revisions, six pairs including
warmup, and two environments. It needs no benchmark table or additional build.
Measure its incremental duration on each runner; do not infer the cost from task
count alone.

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

The two reported environments are Linux/SQL Server combinations, not all supported
operating systems. A clean report cannot rule out a macOS- or Windows-specific
LOB slowdown. Changes to streaming/diagnostic code still need targeted release-build
measurements and warning-preservation checks on those platforms.

The report highlights consistent slowdowns and improvements using the same 20%
median change, 1 ms absolute change, and 80% pair-agreement requirements.

The publisher waits up to 220 minutes inside a 230-minute workflow. The first main
comparison after introduction may be incomplete because its parent lacks this
infrastructure. A fresh ADO-only retry also requires rerunning the GitHub publisher.
