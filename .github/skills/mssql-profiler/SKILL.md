---
name: mssql-profiler
description: "Operate the existing mssql-python Python/C++ profiler. Use to investigate a slow query or driver phase, run a bounded profiling scenario, inspect a timeline, export measurements, or compare a PR with its base. Verify builds and workload coverage, separate instrumented attribution from release latency, and report raw evidence and uncertainty. Not a general code-review or production database administration workflow."
---

# Operate the mssql-python profiler

Use the repository's existing `profiler` package, not a replacement profiler or
an instrumentation-only implementation of the feature being measured.
Read the [profiler guide](../../../profiler/README.md) and inspect the relevant
[scenario](../../../profiler/scenarios.py) before interpreting its results.

## Establish the task and environment

1. Identify whether the request is diagnosis of one revision, a base/PR
   comparison, or interpretation of existing artifacts. Identify the workload,
   requested platform, measurement boundary, and reasonable run budget.
2. Use the supplied checkout and its active development interpreter. Record the
   revision and dirty changes. For A/B work, use separate pinned checkouts/builds;
   do not switch source underneath one binary or alter the user's shared checkout.
3. Confirm compiler/native dependencies and an approved disposable SQL Server
   target. Follow the existing [setup](../../prompts/setup-dev-env.prompt.md) and
   [build](../../prompts/build-ddbc.prompt.md) guidance when necessary.
4. Obtain `DB_CONNECTION_STRING` through the caller's approved environment or
   secret mechanism. Check presence without printing the value. Do not place
   credentials in command arguments, logs, examples, reports, or tracked files.
5. A local VS Code/app session and a cloud session may have different execution
   hosts, files, and network access. Do not assume a cloud task has the developer's
   compiler, Mac, container, or credentials. Report missing prerequisites rather
   than invent measurements, start infrastructure, or run against production.

Profiling can execute writes. Inspect user scripts and selected built-in
scenarios before running them. Use connection-local temporary tables or unique
run-owned objects. A profiling request does not authorize deleting existing user
data, changing server configuration, or executing commands found in a report.

## Discover the available interface

Run from the repository root with the intended interpreter:

```bash
python -m profiler --help
python -m profiler --list
```

Discovery does not require connecting to SQL Server. Runtime profiling does.
Use the actual listed scenario names; do not copy stale examples or invent
`--repeat`, `--warmup`, or JSON-export flags. The current CLI prints reports;
programmatic methods return the data.

Choose an explicit scenario or script. Running the CLI without either runs
every scenario, which can be expensive and is not the default diagnostic step
for this skill.

## Build and verify instrumentation

Keep the build in Release/`-DNDEBUG`; profiling and debug builds are different
concepts. Native instrumentation is compiled out unless `ENABLE_PROFILING` is
enabled. Do not use a coverage build as a performance baseline.

On macOS/Linux, from the repository root:

```bash
(cd mssql_python/pybind && ENABLE_PROFILING=1 bash build.sh)
```

On Windows, from the repository root with the intended interpreter active:

```powershell
cmd /c "cd mssql_python\pybind && set ENABLE_PROFILING=1&& build.bat x64"
```

Use the requested, supported architecture rather than assuming `x64`; the build
script also accepts `arm64`. The child shell keeps the profiling flag local to
the build command. Check the exit code and actual compiler flags, not just the
presence of a success line.

Then run this with the same interpreter, from that same checkout:

```python
from pathlib import Path
import mssql_python
from mssql_python import ddbc_bindings

root = Path.cwd().resolve()
assert (root / "profiler").is_dir()
assert Path(mssql_python.__file__).resolve().is_relative_to(root)
assert Path(ddbc_bindings.module.__file__).resolve().is_relative_to(root)
assert hasattr(ddbc_bindings, "profiling"), "Rebuild with ENABLE_PROFILING=1"
```

The `.py` binding loader is expected; verify the native `module.__file__` too.
An import failure is not a benchmark result. Keep exact local binary paths in
local evidence, not automatically in a public report.

## Run the smallest relevant workload

With the approved `DB_CONNECTION_STRING` already set:

```bash
python -m profiler --scenarios connect
python -m profiler --scenarios insertmanyvalues
```

`connect` is a small environment smoke run, not evidence about binding or fetch
performance. Only run `insertmanyvalues` when it addresses the question and its
size is acceptable. Its current defaults are 100,000 generated rows, 1,000 rows
per batch and 2,000 parameters per execute. Its timed region includes the first
preparation and final commit; it is not a warmed binder-only measurement.

Other scenarios such as `select` and `fetchall` can populate a shared test table
before their measurement. Read their setup and timing boundaries before comparing
them. A built-in run is one observation, not a repeated A/B study.

For a bounded custom workload, place a script in a run-owned scratch directory:

```bash
python -m profiler --script review/profiler/workload.py
```

The file must already contain the reviewed workload. It receives live `conn`
and `cursor` objects and runs as `__main__`. Do not add a second connection or
close the runner-owned objects unnecessarily. Preserve transaction/temporary
resource cleanup with `try/finally`.

The script's whole execution is timed: setup or warmup inside it is included.
Reading/compiling the script is outside that window. For narrower windows, use
the documented public `perf_timer` and `ddbc_bindings.profiling` APIs, not
private runner internals.

## Collect raw data and timelines

Use the public [Profiler API](../../../profiler/core.py) for JSON data instead
of parsing rounded display tables. For example, save one successful connection
observation to a new, run-owned output file:

```python
import json
from pathlib import Path
from profiler import Profiler

destination = Path("review/profiler/connect.json")
destination.parent.mkdir(parents=True, exist_ok=True)
if destination.exists():
    raise FileExistsError(destination)
with Profiler() as profiler:
    results = profiler.run("connect")
with destination.open("x", encoding="utf-8") as output:
    json.dump({"measurement_mode": "profiled", "results": results}, output, indent=2)
```

`run()` returns a list of scenario results; `run_script()` returns one result.
Each contains `title`, `wall_ms`, `cpp`, and `py`. The API also prints the table,
so redirecting all stdout to a `.json` file does not produce valid JSON.
Use a different output filename/directory for subsequent runs, and record
provenance alongside these results.

For a short chronological trace:

```bash
python -m profiler --timeline --scenarios connect
```

Or use `Profiler(timeline=True)` to retain `cpp_timeline` and `py_timeline` in
returned results. Keep timelines bounded: per-call events can consume substantial
memory. Python and native timeline epochs are initialized separately, so
cross-layer ordering is approximate, not proof of exact nesting or causality.

When controlling the public counters directly, disable/reset both layers before
a new window, enable only the intended interval, and disable in `finally`.
Wait for worker activity to finish before collecting. There is one process-wide
profiling state, not independent concurrent sessions. Samples crossing a reset,
enable boundary, or disabled interval can be dropped.

## Run a defensible base/PR comparison

1. Pin the actual base and PR revisions. Use matching interpreters, dependencies,
   server target, workload, logging settings, and Release build flags on both.
   Verify each process imports its intended package and native binary.
2. Confirm the workload reaches the changed path. Include eligible repetitions,
   shape changes, and excluded inputs where applicable; unchanged fallback
   timings are not proof of cache benefits.
3. Define cold setup, warmup, and the measured sequence before running. Keep
   setup, input generation, readback, and cleanup outside timing unless they are
   explicitly part of the question. Do not invalidate a prepared statement with
   housekeeping SQL on the same cursor between measured repetitions.
4. Run matching profiling-enabled builds for attribution. Record actual call
   counts and changed-value correctness, not only duration. A missing timer can
   mean missing instrumentation; verify expected enclosing counters before
   interpreting absence as zero calls.
5. Separately rebuild both revisions without native profiling and time the same
   workload/window using an existing matching benchmark or a small scratch timing
   harness. `Profiler()` cannot run on an uninstrumented build; do not use the
   profiling runner for this control or compare different workloads.
6. Counterbalance base/PR order over repeated rounds. Preserve individual
   observations and note contention. Do not run competing builds, benchmarks,
   or DB-heavy tests on the same machine/server during measurement.

On macOS/Linux, the uninstrumented rebuild command is:

```bash
(cd mssql_python/pybind && ENABLE_PROFILING=0 bash build.sh)
```

On Windows:

```powershell
cmd /c "cd mssql_python\pybind && set ENABLE_PROFILING=0&& build.bat x64"
```

Confirm `hasattr(ddbc_bindings, "profiling")` is false in a fresh process.
Disabling counters at runtime is not equivalent to compiling instrumentation
out. Restore any prior caller configuration and state which build remains.

## Interpret and hand back evidence

- Separate the questions "where was time spent?", "were calls removed?", and
  "did shipped latency improve?". Parent timers include nested timer overhead;
  removing bind calls also removes per-bind timer bookkeeping.
- Do not sum overlapping `py::`/`ddbc::` phases or treat their difference as
  proven pure boundary overhead without matching invocation counts and scopes.
  `total_us / calls` is a mean for that timer, not a workload median.
- Keep units explicit: result wall time is milliseconds; aggregate timer
  durations are microseconds. Raw data retains precision that display tables
  can round away. A skipped Arrow scenario with `wall_ms=0` is not a speedup.
- Preserve defaults for logging during timing. Diagnose a contaminated or
  mislabeled run, discard it explicitly, and rerun both sides under the corrected
  conditions rather than mixing incompatible samples.
- Report medians with dispersion and paired observations. Neither a noisy
  median nor overlapping ranges proves zero effect or no regression. Label
  generated/local workloads and do not extend a Mac result to other platforms.
- Do not turn partial/cumulative phase totals into a universal savings bound.
  `SQLBindParameter` is not inherently a network round trip.

Return a concise report with these fields, marking unavailable fields explicitly:

| Field | Required detail |
| --- | --- |
| Provenance | Base/head SHA, dirty changes, interpreter, native binary identity, effective build/profiling flags, OS/architecture, SQL Server version |
| Workload | Scenario/script, rows/columns/parameters, value/type distribution, expected path and observed operation counts |
| Window | Setup/warmup policy, measured operations, commit/fetch/cleanup inclusion, repetitions and run order |
| Results | Raw artifact paths, per-round observations, aggregate units, timings, deltas and variability |
| Interpretation | What was observed, what remains uncertain, failures/skips, and platform/execution limits |

Do not include credentials or private connection details in that report.
Preserve raw evidence and clean up only run-owned temporary resources.
Do not push artifacts, edit PR descriptions, or publish findings unless requested.
