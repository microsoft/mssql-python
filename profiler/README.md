# Profiler (internal)

A performance profiler for developing the mssql-python driver. It shows where
time goes inside a database call, split across the two layers the driver is
built from:

- the **Python layer** (`mssql_python/cursor.py` and friends), and
- the **native C++ layer** (`mssql_python/pybind/`, compiled into `ddbc_bindings`).

A normal Python profiler (cProfile, py-spy) sees the whole C++ layer as one
opaque block. This tool instruments both layers with named timers, so you can
see, for example, that a slow query spent its time in native parameter binding
rather than in Python.

This is a **development / internal tool** and is not meant for end users (yet).
The native (C++) instrumentation is compiled out of released wheels, so the
shipped driver's native path carries no profiler code. The Python-layer markers
(`perf_timer.py` and the `perf_phase(...)` calls in `cursor.py`) do ship, but
they are phase-level and, when profiling is disabled, reduce to a shared no-op
context manager whose end-to-end cost is within run-to-run noise.

Runtime-instrumentation tests remain part of the driver test suite. Tests that
require the dev-only `profiler/` package skip when it is absent from an installed
wheel. The [profiler benchmark guide](../eng/profiler_benchmarks/README.md) describes
isolated profiling builds, scenario coverage and advisory PR regression comments.
Broader profiler testing remains follow-up work.

Use controlled diagnostic workloads with one owner of the process-wide profiling
state: enable, run the workload, wait for worker threads to finish, then collect.
Recording supports worker threads; independent concurrent profiling sessions are
not supported. Enabled instrumentation adds bookkeeping overhead, so compare runs
using the same profiling configuration.

## How the timers are named

Every timer has a prefix telling you which layer it belongs to:

- `py::...`   — a phase in the Python layer (e.g. `py::execute::cpp_call`)
- `ddbc::...` — a function in the native C++ layer (e.g. `ddbc::FetchAll_wrap`)

## Step 1: build with profiling turned on

Profiling is **off by default** and the native instrumentation is compiled out
of normal builds (no native profiler code in the shipped driver). To get a
profiling build, set one environment variable before building the C++ extension:

```bash
# macOS / Linux
cd mssql_python/pybind
ENABLE_PROFILING=1 bash build.sh

# Windows
cd mssql_python\pybind
set ENABLE_PROFILING=1
build.bat
```

Without `ENABLE_PROFILING`, the native `ddbc_bindings.profiling` module does not
exist and the C++ timers do nothing.

## Step 2: run the profiler

You need a SQL Server to run against. Point `DB_CONNECTION_STRING` at it:

```bash
export DB_CONNECTION_STRING="Server=localhost,1433;Database=master;UID=sa;Pwd=...;Encrypt=no;TrustServerCertificate=yes;"
```

Then either use the command-line runner, or drive it from Python.

### Option A: command-line runner

```bash
python -m profiler --list                     # show the built-in scenarios
python -m profiler --scenarios select fetchall  # run specific ones
python -m profiler --script my_repro.py       # run your own script (see below)
```

`--script` runs any Python file with a live `conn` and `cursor` already created
for you, and reports whatever timers it hits. This is how you profile a specific
slow query you are trying to diagnose:

```python
# my_repro.py  —  `conn` and `cursor` are provided
cursor.execute("SELECT ... your slow query ...")
cursor.fetchall()
```

The script runs as `__main__`, with its directory first on the import path and
`sys.argv` containing only its filename (profiler flags are not forwarded).
These settings are restored on exit, including when the script raises or calls
`sys.exit()`. Script reading and compilation are outside the measured window.
Run scripts synchronously: the temporary interpreter settings are process-wide.

### Option B: from Python directly

If you want the raw numbers without the runner, enable both layers, run your
code, then read the stats:

```python
from mssql_python import perf_timer          # Python layer
from mssql_python import ddbc_bindings        # native layer (profiling build only)

perf_timer.enable()
ddbc_bindings.profiling.enable()

# ... run your queries ...

py_stats  = perf_timer.get_stats()            # {name: {calls, total_us, min_us, max_us}}
cpp_stats = ddbc_bindings.profiling.get_stats()

perf_timer.disable()                          # stop recording when done
ddbc_bindings.profiling.disable()
perf_timer.reset()                            # and clear the counters
ddbc_bindings.profiling.reset()
```

## Reading the output

Each timer reports four numbers:

- `calls`    — how many times it ran
- `total_us` — total microseconds spent inside it
- `min_us` / `max_us` — fastest and slowest single call

The runner merges both layers into one table sorted by total time, so the
biggest cost is at the top. A `py::` timer that wraps a `ddbc::` call (e.g.
`py::execute::cpp_call` around `ddbc::SQLExecute_wrap`) lets you see the
Python-to-C++ boundary cost as the difference between the two.

There is also a timeline view (`--timeline`, or `get_timeline()`), which returns
each timer event in the order it happened with a start offset — useful for
seeing the sequence and nesting of a single slow operation rather than just
totals.

Enabling profiling or resetting aggregate counters starts a new measurement window.
Samples crossing that boundary, or finishing while profiling is disabled, are dropped.
Restarting only the timeline drops spans that began before its new epoch but keeps
their aggregate timings. Cross-layer timeline ordering remains approximate because
the Python and native clocks have independently initialized epochs.

Reports use detached snapshots so garbage-collection finalizers can perform cleanup
without re-entering a held counter lock. Python samples triggered recursively during
profiler bookkeeping are intentionally omitted; the cleanup itself still runs.
Ordinary nested workload timers and other threads are not suppressed.

## Single-row fetch attribution

`fetchone()` and the default iterator/`fetchval()` path share the native
single-row helper. Full result column counts are cached per metadata generation,
separately from prefix `SQLGetData` metadata. `fetchmany(1)` reuses that count for
all result shapes, while still validating count and names before fetching. Cold
`fetchmany(1)` can make two count calls (eager validation and `DescribeColumns`);
subsequent size-one calls, including EOF, do not reacquire it within the same
generation. Larger batches and direct `DDBCSQLNumResultCols` calls retain their
uncached count behavior. `fetchval()` still calls `fetchone()` and constructs the full
row, including converters for columns beyond the first.

`ddbc::FetchSingleRow::SQL_UNBIND` counts attempted unbinds in that helper. A successful
unbind can be reused within the same generation, but does not certify row-array
attributes. Binding (including Arrow and partial binds), cleanup, and generation
changes invalidate reuse.

`ddbc::FetchMany::single_numeric_row` identifies the native `fetchmany(1)` route for
all-numeric results (integer, bit, real, float/double). It retains eager count/name
validation, row-array configuration and cleanup, and uses `SQLFetchScroll` plus
per-column `SQLGetData`. Mixed INT/NVARCHAR, text, LOB, decimal, temporal, UUID and
variant results retain their existing native routes. Numeric width can therefore
change the tradeoff; do not generalize a narrow-row result.

The Python one-row wrapping shortcut is independent of native eligibility. It
requires a built-in `int` request of one, exactly one returned row, the canonical
`Row` and factory, and no converter or UUID work. One-row tails of larger requests
and substituted factories retain batch wrapping. The existing
`py::fetchone::{cpp_call,row_wrap}` and `py::fetchmany::{cpp_call,row_wrap}` phases
use paired start/stop calls, avoiding context-manager entry/exit when disabled.

Row-wise integer, bit and floating-point values use checked CPython constructors
and append operations. This removes generic scalar marshalling, not the scalar
allocations themselves. The public `DDBCSQLGetData` destination still retains its
identity, preexisting elements and completed cells on a later-column error; it
is not replaced with a presized, partially initialized list.

Bounded narrow-character GetData decodes the driver buffer directly through
Python's codec API. Successful decoding avoids an intermediate bytes object,
bound `decode` method and Python call arguments. Codec failures retain the logged
bytes fallback, including codec names containing an embedded NUL rather than
silently truncating them for the C API. Allocation failures now propagate instead
of being mistaken for codec failures. The decoded value is still appended before
debug logging; if a custom string's length raises during logging, the existing
decoded cell and subsequent bytes fallback are retained. Wide text and LOB
streaming are unchanged.

Mixed INT/NVARCHAR is intentionally not routed through the numeric specialization:
bound and GetData paths differ on malformed UTF-16, `SQL_NO_TOTAL`, truncation
continuation and diagnostic timing. The unbind witness does not justify removing
row-array configuration/cleanup. Row user attributes, weakrefs and substituted
factories remain supported, as do dynamic `fetchone` overrides. No
first-column-only `fetchval` route is introduced.

The experimental shared `DDBCSQLFetchRow` entry performs the original native fetch,
then calls the extracted Python completion body for return checking, row counters
and map acquisition. An eligible completion returns a private construction plan;
the same native call builds the Row and returns it (or a one-element list for
`fetchmany(1)`). Unsupported/customized completion constructs the already-fetched
values in Python. It never refetches. `fetchval()` still dispatches through
`self.fetchone()` and all-column conversions remain in the completion body.

Eligibility inspects raw class dictionaries and the original factory code without
executing allocation/assignment descriptors or metaclass hooks. It is checked
before fetching and again after map acquisition. The captured factory code and
Row global are checked again in native code after final argument evaluation;
late changes call that captured factory on the fetched values, without refetching.
A final native raw-type/dictionary guard also rejects newly installed allocation or
assignment hooks without invoking them. The captured Python factory then preserves
`Row.__new__(Row)` lookup order, including a descriptor changing the second `Row`.
In-place factory changes, custom
allocation/assignment, converters and UUID policies retain their fallback behavior.
The original `Row._fast_create` body is unchanged. Constructor attribute names are
an immutable tuple owned by binding defaults, prepared once per module rather
than five Python string allocations per row; no static owning Python handles or
Row layout offsets are used. Native construction still uses `__new__` validation
and normal checked attribute assignment, preserving descriptor callbacks.

This is a shared fetch-and-construction call, **not** an all-native cursor:
completion still re-enters Python, and eligible rows allocate a small plan tuple.
The ordinary real-source frame trace removes one `_fast_create` frame but adds
two `_native_row_eligible` frames and one `_finish_fetch*` frame: net **two more
Python frames**. The outer Python-to-native call count is unchanged, and completion
adds a native-to-Python crossing. This is not reduced Python dispatch or fewer
crossings. These costs may outweigh native assignment work; no speedup or slowdown
is established without measurement. Larger requests, integer subclasses and substituted low-level
fetch bindings retain the original entry. Native numeric/non-numeric routing and
cleanup are unchanged.

When Python phase profiling is active, the original split entry is used so
`cpp_call` and `row_wrap` retain their existing boundaries. With Python phases off,
`ddbc::FetchRow::construct_row` attributes native construction when native profiling
is available. Timing the split Python-profiled path is not evidence of fused-path
latency; comparisons must identify which route actually ran.

The isolated `test_single_row_fusion_native_counters_in_subprocess` cases enable
native counters with Python phases disabled. For each API they require two native
Row constructions for two rows, none at EOF, and none for converter fallback while
all-column callbacks still run. These three cases skip on native profiling-OFF
builds; the constructor-frame tests still run there. Split-route profiling alone
cannot qualify fusion. These are future exact-source CI contracts, not local
runtime measurements.

Use profiling-enabled builds to check operation counts and normal uninstrumented
Release builds for latency comparisons. These routes are optimization hypotheses,
not a measured speedup; source-only checks do not establish native correctness.

## Adding a timer

To time a new spot in the code:

**Python** — wrap the block:

```python
from mssql_python.perf_timer import perf_phase

with perf_phase("py::my_area::my_step"):
    ...  # the code you want to measure
```

**C++** — add one line at the top of the scope (RAII, stops automatically):

```cpp
void MyFunction(...) {
    PERF_TIMER("MyFunction");   // becomes ddbc::MyFunction
    ...
}
```

`PERF_TIMER` compiles to nothing unless the build has `ENABLE_PROFILING`, so
adding timers costs nothing in released builds.


### Explicit single-row comparison modes

The paired controller retains its existing `diagnostic` default: native and Python
recording are enabled, so the guarded Row path deliberately uses the split route.
That report is not a latency measurement of ordinary fused fetching.

Two opt-in modes use the same six read-only workloads: `numeric_fetchone`,
`numeric_fetchmany`, `numeric_fetchval`, and their `mixed_` equivalents. Each fetches
1,000 ordered rows plus EOF; numeric rows contain two INT columns, mixed rows contain
INT and NVARCHAR. `fetchmany` always requests the built-in integer `1`. `fetchval`
uses its public API, not a first-column-only native shortcut.

- `--mode latency` builds both revisions with native instrumentation OFF and keeps
  Python phases OFF. It rejects `--reuse-candidate` rather than silently timing the
  profiling-enabled CI extension. Setup/execute and validation are outside the
  timed window; the API loop, result retention and EOF call are inside it.
- `--mode route` uses native instrumentation ON with Python phases OFF. Each case
  must record 1,001 native fetch calls and, when the guarded Row entry exists,
  exactly 1,000 native Row constructions. An older base without that entry must
  record no such constructions. This is route attribution, not production latency.
  Existing native regression tests separately cover converter fallback, all-column
  callbacks, repeated EOF, and customization failures.

For example, after obtaining approval for the required builds and database work:

```console
python -m eng.profiler_benchmarks.controller --mode latency --base <base-sha> --candidate <pr-sha> --leg Linux-SQL2022 --output <latency-results>
python -m eng.profiler_benchmarks.controller --mode route --base <base-sha> --candidate <pr-sha> --leg Linux-SQL2022 --output <route-results>
```

Run these separately, not concurrently. No extra CI arm or automatic execution is
introduced. Each invocation retains the controller's existing bounded sample,
worker and overall time limits. Use distinct output directories; subset runs remain
incomplete and cannot produce a full report. The standalone interactive Profiler's
recording and timeline defaults are unchanged.

These modes emit schema version 2 with explicit mode, worker revision, actual native
binary path/SHA256 and compile-capability identity. The validator rejects mixed modes,
changed binaries, missing/failed samples, unexpected recording, and wrong route counts.
Schema version 1 remains the existing 22-workload diagnostic report. Failed runs stay
incomplete/unavailable; missing timings are never replaced with zero. Raw samples
retain every paired observation. Tables show signed subthreshold changes and median
candidate/base ratios with their observed min/max range, not confidence intervals.
The classification policy remains **more than 20% median paired change, at least
1 ms between median times, and at least 80% of pairs beyond the relative threshold**.
A subthreshold change is not a proven win or proof of no effect. Neither mode adds a
pyodbc comparison or changes production fetch behavior.


### Latency-first PR CI report

The existing Ubuntu/SQL Server 2022 and Ubuntu/SQL Server 2025 PR legs invoke
`--reuse-candidate --ci-report`. Windows profiling remains disabled. The aggregate
runs OFF/OFF latency first, ON/OFF native-route attribution second, and the original
22-workload ON/ON diagnostics last. The six numeric/mixed workloads in each fetch
mode and all legacy workloads retain **five measured pairs and one warmup pair**.
Each mode alternates base/candidate order independently, using separate workers.

The aggregate builds isolated base-OFF, candidate-OFF and base-ON directories and
reuses the already tested candidate-ON build. Route and diagnostic workers must
attest the same ON binary paths, SHA256 digests, source revisions and environments.
Each reused-checkout worker also checks HEAD and tracked driver/provider source
cleanliness before importing the driver. The reused build is never rebuilt or
toggled in place. Per leg this permits at most
three performance-step builds, 36 measurement workers and 408 workload executions
(including warmups), rather than one build, 12 workers and 264 executions. Both
active legs together add at most four builds, 48 workers and 288 executions. No new
matrix arm, pyodbc comparison or database setup is introduced.

The controller retains a **90-minute aggregate budget**, including a three-minute
finish reserve; the pipeline step remains 100 minutes and its job 160 minutes.
Archives/preflight, builds and workers receive bounded timeouts clipped to the
remaining work budget. Fetch workers receive at most 30 seconds; diagnostic workers
retain their six-minute cap. The fetch allowance is not yet runtime-qualified.
The proposal already required 132 minutes if its build/worker caps and planning
overhead were all consumed. Metadata operations also consume the shared deadline;
they never extend it. Completing every mode is **not guaranteed**. Unused early time flows to later modes. Exhaustion or
failure leaves that mode unavailable with a stage/reason and its raw checkpoints;
there are no retries, trimmed workload sets, reduced pair counts or zero substitutes.
Unsafe process cleanup aborts subsequent work and reports the retained temporary
root rather than removing files beneath an unreaped worker.

There is still exactly one `report.json` per existing artifact. Its root remains the
schema-1 diagnostic report and its `status` describes diagnostics only. The additive
`measurement_bundle_version: 1` extension contains `fetch_measurements.latency` and
`fetch_measurements.route` (schema 2). Each mode has an independent complete/incomplete
status. Root and child checkpoints are written atomically; an incomplete aggregate
exits nonzero, while the pipeline's existing failure-tolerant artifact publication
retains independently valid modes. The finish deadline is checked after final
validation and the atomic write. An overrun gets one corrective incomplete checkpoint
and a nonzero exit, preserving completed latency/route measurements without a recheck
loop. Raw build logs and mode-prefixed worker JSON/log files stay in the same artifact,
without nested files named `report.json`.

The updated collector validates shared PR/build/base/merge identity, then each mode
independently. The primary performance verdict comes **only from OFF/OFF latency**.
Native route counts/timings and both-ON diagnostics appear in separately labeled
sections; neither replaces missing latency. Missing, malformed and unstarted modes
are explicitly unavailable. Legacy readers can still read the diagnostic root;
standalone schema-1/schema-2 reports and interactive Profiler defaults are unchanged.
Fork reporting continues to use trusted base code. Existing artifact, download,
comment-size and publisher time limits are not expanded.

This wiring is source-only until approved exact-head CI runs it. Source/fake-boundary
controls and prior-head CI do not establish native correctness, completion within
these allowances, or a speedup.
