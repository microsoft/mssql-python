# Benchmarks

This directory contains benchmark scripts for testing the performance of various database operations using `pyodbc` and `mssql_python`. The goal is to evaluate and compare the performance of these libraries for common database operations.

## Benchmark Scripts

### 1. `bench_mssql.py` - Richbench Framework Benchmarks
Comprehensive benchmarks using the richbench framework for detailed performance analysis.

### 2. `profiler_ci.py` - PR Regression Comparisons
CI compares profiling-enabled base and candidate revisions on the same agent and
SQL Server. It replaces the historical pyodbc-normalized CI comparison; the old
`perf-benchmarking.py` remains available for local driver-versus-pyodbc analysis.

The 20-workload registry retains all 10 scenarios from `profiler/scenarios.py`,
the four AdventureWorks queries (including 1.2M rows), both legacy 100K-row
insertmanyvalues variants, two additional fetch batch sizes, and repeated positional
and named-parameter execution.

```bash
# Requires build dependencies, pyarrow, and an AdventureWorks2022 connection.
python benchmarks/profiler_ci.py --base main --candidate HEAD \
    --leg Linux-SQL2022 --output profiler-results
python benchmarks/profiler_report.py profiler-results/report.json
```

CI uses the PR-merge commit's first parent as the exact base snapshot. The five
benchmark legs build the candidate with `ENABLE_PROFILING=1`, run pytest with
recording disabled by default, then reuse that binary for the benchmark. Existing
profiler-specific tests explicitly enable and clean up recording; ordinary driver
tests do not. A pre-test check requires the expected native configuration and
recording OFF. LocalDB, other Linux legs and the release pipelines still build the
default configuration. Windows profiling artifacts are named separately from
`ddbc_bindings` and are not release wheels.

Only the base requires a second build, in a temporary git archive with profiling
enabled. Local invocations build both archives unless `--reuse-candidate` is supplied;
that option requires the requested candidate to be the current checkout HEAD.
Both sides execute the same version of the workload suite. Each pass runs in a
fresh interpreter. Five measured
pairs follow one discarded warmup pair, alternating base/PR order. A local subset is
available through `--scenarios`; it is not accepted as a complete CI report.

The comparison is **advisory**, not a new merge gate. A regression signal requires
over 20% paired-median slowdown, at least 1 ms additional median wall time, and at
least 80% of pairs exceeding the relative threshold. Disagreement is reported as
noisy. Paired runs reduce agent-to-agent noise; they do not eliminate server
contention. Enabled profiler overhead is part of both measurements, so these are
not production-wheel latency estimates.

Per-phase inclusive duration deltas and call-count changes help locate regressions;
they are not summed into wall-clock totals. Raw pairs and build/worker logs are
published on PR and main runs as `profiler-<platform>-<SQL version>` artifacts.
The existing five benchmark legs are covered: Windows and macOS on SQL2022/2025,
and Linux Ubuntu on SQL2022. ARM, RHEL, Alpine, LocalDB, and Azure SQL are not
implicitly compared against other platforms.

`PR Profiler Report` creates or updates one comment per PR. Its privileged job
checks out only the trusted base revision, never executes PR/artifact code, validates
bounded JSON against the ADO build and GitHub merge/head identities, and ignores
stale heads. Missing, skipped, malformed, or failed runs are shown as incomplete,
not as a clean performance verdict. As a new base-branch reporting workflow, it
starts reporting automatically after this infrastructure has merged; it does not
grant fork-authored workflow code write credentials.

The first main comparison after introduction may lack profiling support on its
parent; that run is incomplete rather than falling back to an uninstrumented base.
Subsequent comparisons use the new artifact format and do not consume old
`perf-baseline-*` artifacts. For a fresh measurement after an ADO-only retry,
re-run the GitHub reporting workflow as well.

## Why Benchmarks?
- To measure the efficiency of `pyodbc` and `mssql_python` in handling database operations.
- To identify performance bottlenecks and optimize database interactions.
- To ensure the reliability and scalability of the libraries under different workloads.

## How to Run Benchmarks

### Running bench_mssql.py (Richbench Framework)

1. **Set Up the Environment Variable**:
   - Ensure you have a running SQL Server instance.
   - Set the `DB_CONNECTION_STRING` environment variable with the connection string to your database. For example:
     ```bash
     export DB_CONNECTION_STRING="Server=your_server;Database=AdventureWorks2022;UID=your_user;PWD=your_password;"
     ```

2. **Install Richbench - Benchmarking Tool**:
   ```bash
   pip install richbench
   ```

3. **Run the Benchmarks**:
   - Execute richbench from the parent folder (mssql-python):
     ```bash
     richbench benchmarks
     ```
   - Results will be displayed in the terminal with detailed performance metrics.

### Running perf-benchmarking.py (Real-World Queries)

This script tests performance with real-world queries from the AdventureWorks2022 database.

1. **Prerequisites**:
   - AdventureWorks2022 database must be available
   - Both `pyodbc` and `mssql-python` must be installed
   - Update the connection string in the script if needed

2. **Run from project root**:
   ```bash
   python benchmarks/perf-benchmarking.py
   ```

3. **Features**:
   - Runs each query multiple times (default: 5 iterations)
   - Calculates average, min, max, and standard deviation
   - Provides speedup comparisons between libraries
   - Tests various query patterns:
     - Complex joins with aggregations
     - Large dataset retrieval (10K+ rows)
     - Very large dataset (1.2M rows)
     - CTEs and subqueries
   - Detailed summary tables and conclusions

4. **Output**:
   The script provides:
   - Progress indicators during execution
   - Detailed results for each benchmark
   - Summary comparison table
   - Overall performance conclusion with speedup factors

## Key Features of `bench_mssql.py`
- **Comprehensive Benchmarks**: Includes SELECT, INSERT, UPDATE, DELETE, complex queries, stored procedures, and transaction handling.
- **Error Handling**: Each benchmark function is wrapped with error handling to ensure smooth execution.
- **Progress Messages**: Clear progress messages are printed during execution for better visibility.
- **Automated Setup and Cleanup**: The script automatically sets up and cleans up the database environment before and after the benchmarks.

## Key Features of `perf-benchmarking.py`
- **Statistical Analysis**: Multiple iterations with avg/min/max/stddev calculations
- **Real-World Queries**: Tests against AdventureWorks2022 with production-like queries
- **Automatic Import Resolution**: Correctly imports local `mssql_python` package
- **Comprehensive Reporting**: Detailed comparison tables and performance summaries
- **Speedup Calculations**: Clear indication of performance differences

## Notes
- Ensure the database user has the necessary permissions to create and drop tables and stored procedures.
- The `bench_mssql.py` script uses permanent tables prefixed with `perfbenchmark_` for benchmarking purposes.
- A stored procedure named `perfbenchmark_stored_procedure` is created and used during the benchmarks.
- The `perf-benchmarking.py` script connects to AdventureWorks2022 and requires read permissions only.