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

The fixed registry has 22 tasks. `--scenarios` runs a local subset, but subset
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

## Measurement contract

CI uses the PR merge's first parent as the exact base. It reuses the
profiling-enabled candidate build after pytest and builds the base separately.
Fresh processes run five measured pairs after one discarded warmup pair in alternating order.
Each worker also warms every steady-state task once before measuring it in that
same process. Connection opening remains a single cold invocation.

Short read/query tasks use fixed blocks shared by both revisions: SELECT and
256 KiB LOB fetching repeat 100 times, row-by-row fetching 10 times, CTE queries
20 times, and positional/named query workloads 3 times. On the observed hosted
Unix controls these blocks target roughly 100 ms or more; they are not adaptive
duration gates. Other workloads remain single-pass. Setup and cleanup keep their
existing timing boundaries.

The reported wall time is the mean time per original workload within each block,
not the total block duration. Raw inner timings and aggregate counters are retained
in the artifacts; phase diagnostics and call counts are normalized per repetition.
Repetition and in-process warmup counts must match across all base/candidate pairs.
Retained single-pass reports remain readable. Repetitions do not increase the number
of independent paired comparisons, and the 1 ms floor remains per original workload.

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

The report highlights consistent slowdowns and improvements in two advisory tiers:

- Tentative small impact: more than 10% median paired movement.
- Large impact: more than 20% median paired movement.

Both tiers require at least 1 ms between median per-workload runtimes and at least
80% of pairs exceeding that tier's relative threshold in the same direction.
Large impact is evaluated first. A median above 20% with agreement only at 10%
can therefore remain tentative small impact. Inconsistent movement produces no
headline, badge, or signal fingerprint; its percentage and agreement remain in
the expanded timing table as no signal. Missing environments remain unavailable
and a clean partial assessment is labeled incomplete.

The publisher waits up to 220 minutes inside a 230-minute workflow. The first main
comparison after introduction may be incomplete because its parent lacks this
infrastructure. A fresh ADO-only retry also requires rerunning the GitHub publisher.
