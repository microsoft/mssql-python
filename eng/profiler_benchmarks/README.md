# Profiler Benchmarks

This package compares profiling-enabled base and candidate revisions on the same
agent and SQL Server, then validates and renders advisory performance results.
It is separate from the runtime profiler in `profiler/` and the standalone scripts
in `benchmarks/`.

The reviewer-facing experience is the **PR Performance Report**. It leads with
the performance impact, affected database tasks, and environment coverage. Phase
timings, all task measurements, commits, and methodology remain expandable evidence.

## Local use

```bash
# Requires build dependencies, pyarrow, and an AdventureWorks2022 connection.
python -m eng.profiler_benchmarks.controller --base main --candidate HEAD \
    --leg Linux-SQL2022 --output profiler-results
python -m eng.profiler_benchmarks.report profiler-results/report.json
```

The fixed 20-workload registry retains all 10 profiler scenarios, four
AdventureWorks queries, both legacy 100K-row insert variants, two fetch batch sizes,
and repeated positional and named-parameter execution. A local subset is available
through `--scenarios`; subset reports remain incomplete and cannot produce a verdict.

## Measurement contract

CI uses the PR-merge commit's first parent as the exact base. Selected legs build
the candidate with `ENABLE_PROFILING=1`, run pytest with recording disabled, and
reuse that binary. The base gets an isolated profiling build. Each side runs in a
fresh process, with five measured pairs after one discarded warmup pair and
alternating order.

Workers have a six-minute limit. CI allows 90 minutes for measurement inside a
100-minute step and 160-minute job. Local runs receive 105 minutes because they
build both revisions. Partial workers never count as measured pairs, and incomplete
reports never produce a verdict.

A regression signal requires over 20% paired-median slowdown, at least 1 ms added
median wall time, and at least 80% of pairs exceeding the relative threshold.
Per-phase inclusive deltas and call-count changes are diagnostics, not additive
wall-clock components.

## Publication

Five legs publish raw samples: Windows and macOS on SQL Server 2022/2025, and
Ubuntu on SQL Server 2022. The privileged GitHub publisher executes only trusted
base code, authenticates the ADO build and source/base suite trees, treats artifacts
as bounded data, and ignores stale heads. Missing, malformed, skipped, canceled, or
failed runs are incomplete.

The publisher waits up to 220 minutes inside a 230-minute workflow. The first main
comparison after introduction may be incomplete because its parent lacks this
infrastructure. A fresh ADO-only retry also requires rerunning the GitHub publisher.
