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

The fixed registry has 20 tasks. `--scenarios` runs a local subset, but subset
reports remain incomplete and cannot produce a verdict.

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
