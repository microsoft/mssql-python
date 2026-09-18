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

Four environments publish raw samples: Windows and Unix on SQL Server 2022/2025.
Unix measurements run on Ubuntu. Routine macOS profiling is intentionally excluded:
hosted macOS plus Colima produced false regressions on a documentation-only control
PR, while macOS remains covered by functional CI. The privileged publisher runs
trusted base code, authenticates benchmark producers, validates bounded artifacts,
and ignores stale heads. A failed aggregate build can still publish when its
authenticated artifacts validate. Missing, malformed, canceled, incomplete, or
invalid data remains unavailable.

The publisher waits up to 220 minutes inside a 230-minute workflow. The first main
comparison after introduction may be incomplete because its parent lacks this
infrastructure. A fresh ADO-only retry also requires rerunning the GitHub publisher.
