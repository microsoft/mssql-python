# mssql-python profiler

Unified Python + C++ performance instrumentation for the mssql-python driver.

Timers on both layers are merged into a single sorted view. Python-layer entries use a `py::` prefix, C++ entries have no prefix — so you can immediately see where time is spent across the boundary.

## Quick start

```bash
# Set connection string
export DB_CONNECTION_STRING="Server=localhost,1433;Database=master;UID=sa;Pwd=...;Encrypt=no;TrustServerCertificate=yes;"

# Run all scenarios
python -m profiler

# Run specific scenarios
python -m profiler --scenarios fetchall insertmanyvalues

# List available scenarios
python -m profiler --list

# Pass connection string directly
python -m profiler --conn-str "Server=..."
```

## Programmatic usage

```python
from profiler import Profiler

with Profiler("Server=localhost,1433;...") as p:
    results = p.run("fetchall", "insertmanyvalues")
    # results is a list of dicts with keys: title, wall_ms, cpp, py, detail
```

## Available scenarios

| Name | What it measures |
|---|---|
| `connect` | Connection establishment |
| `select` | `cursor.execute()` SELECT + `fetchall()` (100 rows) |
| `insert` | 100 individual `cursor.execute()` INSERTs (9 params each) |
| `executemany` | `cursor.executemany()` with 5K rows |
| `fetchall` | `cursor.fetchall()` on 50K rows |
| `fetchone` | `cursor.fetchone()` loop over 1K rows |
| `fetchmany` | `cursor.fetchmany(1000)` loop over 50K rows |
| `commit_rollback` | 100 commits + 100 rollbacks |
| `arrow` | `cursor.fetch_arrow()` on 50K rows |
| `insertmanyvalues` | SQLAlchemy pattern — 100K rows via batched `cursor.execute()` with 2000 params/call |

## Architecture

```
profiler/
├── __init__.py     # Public API: Profiler class
├── __main__.py     # CLI entry point (python -m profiler)
├── core.py         # Profiler orchestration, connection management
├── scenarios.py    # Self-contained benchmark functions
├── reporter.py     # Stats merging and table formatting
└── README.md

mssql_python/
└── perf_timer.py   # Python-layer instrumentation (lives in the library)
```

**`perf_timer.py`** is the in-library instrumentation — `perf_phase()` context managers and `perf_start()`/`perf_stop()` pairs embedded in `cursor.py`. It's compile-time-toggled: when disabled (`_enabled = False`), each timer is a single `if` check (~20ns).

**`profiler/`** is the runner — it enables both layers, executes scenarios, collects stats, and reports.

## Output format

Both layers report `{calls, total_us, min_us, max_us}` per timer. The reporter merges and sorts by `total_us` descending:

```
====================================================================================
INSERTMANYVALUES (100,000 rows, 2000 params/call)
====================================================================================
  Function                                         Calls    Total(ms)      Avg(us)
  ---------------------------------------------------------------------------------
  py::execute::param_type_detection                  100       2009.3      20092.6    <-- Python
  py::execute::cpp_call                              100       1414.6      14146.1    <-- Python
  SQLExecute_wrap                                    100       1367.6      13676.3    <-- C++
  py::execute::diag_records                          100        962.2       9621.5    <-- Python
  SQLGetAllDiagRecords                               100        961.0       9610.3    <-- C++
  BindParameters                                     100        189.4       1893.9    <-- C++
```

The `py::execute::cpp_call` timer wraps the C++ call from the Python side — so `cpp_call - SQLExecute_wrap` = pybind11 boundary crossing overhead.
