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
wheel. Broader profiler testing and profiling-enabled CI builds are deferred to
follow-up work.

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
