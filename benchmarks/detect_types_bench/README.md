# DetectParamTypes microbenchmark

Three sibling extension modules that each implement the parameter type-detection
loop from `mssql_python/pybind/ddbc_bindings.cpp` — one per binding approach —
so we can measure the perf and maintainability trade-offs between them:

| Variant | File | Approach |
|---|---|---|
| pybind11 baseline | `detect_pybind11.cpp` | `py::isinstance<>`, `.attr()`, `.cast<>()` |
| raw CPython | `detect_cpython.cpp` | `PyLong_Check`, `PyObject_GetAttrString`, manual refcounts (the approach taken by the current insertmany-perf-detect-types PR) |
| nanobind | `detect_nanobind.cpp` | RAII C++ throughout; escapes to CPython macros only where they genuinely matter (`PyUnicode_KIND`, `PyDateTime_TIME_GET_HOUR`, etc.) |

All three do **identical work per parameter** — this is cross-validated by
`parity_test.py`, which compares their outputs on 61 representative inputs
(int boundaries, subclasses, ASCII/Latin-1/BMP/non-BMP/embedded-NUL strings,
bytes/bytearray of both inline and DAE size, timezone-aware and naive
datetimes, MONEY/SMALLMONEY/out-of-range Decimals including `Decimal("0.001")`
edge cases, UUIDs, None) and additionally cross-checks every SQL type constant
against `mssql_python.constants.ConstantsDDBC`.

## Prerequisites

- Linux/macOS (paths and build script assume POSIX; Windows would need MSVC adaptation)
- Python 3.8+ with headers (`python3-dev` on Debian/Ubuntu)
- `g++` with C++17 support
- `pip install pybind11 nanobind`

## Build and run

```bash
cd benchmarks/detect_types_bench

python build.py            # builds all three extensions into this folder
python parity_test.py      # correctness first — must show all 61 inputs OK
python run_benchmark.py    # then perf comparison across 5 workload mixes
```

## What each file does

- `detect_cpython.cpp` / `detect_pybind11.cpp` / `detect_nanobind.cpp`
  Each exposes two functions:
  - `detect(list) -> int` — runs the dispatch loop and returns a checksum
    (used for perf timing; the checksum prevents dead-code elimination).
  - `detect_types(list) -> list[tuple]` — returns
    `(sql_type, c_type, column_size, is_dae, decimal_digits)` per parameter
    (used for parity validation).
- `build.py` — one-shot build of all three shared objects (invokes `g++`
  directly; no CMake dependency).
- `run_benchmark.py` — the perf harness. Runs 5 workload mixes (OLTP,
  Analytics, Int-heavy, Str-heavy, Decimal-heavy), best-of-3 timing, GC
  disabled during measurement.
- `parity_test.py` — the correctness harness. Runs a 61-input corpus through
  all three variants and against a Python-side reference implementation of
  `DetectParamTypes` semantics.

## Sample results (Python 3.12.3, gcc 13.3.0 -O3, one Linux x86_64 workstation)

Numbers are nanoseconds per parameter, best-of-3, averaged across three
back-to-back runs.

| Workload | pybind11 | raw CPython | nanobind | nanobind vs pybind11 | nanobind vs raw CPython |
|---|---:|---:|---:|---:|---:|
| OLTP mixed (6 params) | 167 ns | 124 ns | 123 ns | 1.36x faster | 1% faster |
| Analytics (10 params) | 194 ns | 139 ns | 134 ns | 1.45x faster | 3% faster |
| Int-heavy (20 params) | 9.5 ns | 6.5 ns | 5.8 ns | 1.62x faster | 10% faster |
| Str-heavy (20 params) | 8.2 ns | 5.8 ns | 4.9 ns | 1.68x faster | 16% faster |
| Decimal-heavy (20 params) | 722 ns | 617 ns | 602 ns | 1.20x faster | 2% faster |

**How to read this:** Both raw CPython and nanobind deliver a substantial
1.2x-1.7x speedup over pybind11. In this environment nanobind matches or
slightly beats raw CPython on every workload, largely because nanobind uses
PEP 590 vectorcall for the function-call boundary while the raw-CPython
version relies on `METH_VARARGS + PyArg_ParseTuple` — a per-call cost that
matters when calls take only ~100-1000 ns each.

## Design notes

- All three cache implementations use "leak-on-purpose" singletons (never
  `Py_DECREF`ed) — this matches the pattern the PR uses and avoids
  static-destructor issues at interpreter shutdown.
- Both `raw CPython` and `nanobind` escape to CPython macros for the same
  ~5 hotspots where they genuinely matter (`PyUnicode_KIND`,
  `PyUnicode_IS_COMPACT_ASCII`, `PyDateTime_TIME_GET_HOUR`, etc.) — nanobind
  does not hide `PyObject*`, it just wraps it, so this escape hatch works
  the same in both.
- The pybind11 variant deliberately uses the same idioms the codebase used
  *before* the perf refactor: `py::isinstance<T>`, `obj.attr("hour").cast<int>()`,
  `py::module_::import`, `.cast<py::tuple>()`. This gives a faithful "before"
  baseline.
- Decimal handling exercises the real `DetectParamTypes` two-tier MONEY range
  check plus `__format__("f")` for in-range values, so the Decimal benchmark
  reflects genuine work (not a shortcut).

## What this benchmark does *not* measure

- End-to-end `execute()` latency including ODBC network calls — that is
  dominated by the network round-trip and would dilute the differences seen
  here.
- Binary size at scale — the standalone `.so` sizes reported by `build.py`
  are misleading for nanobind because they statically link `nb_combined.cpp`.
  In a real project with many binding modules, nanobind's runtime amortizes
  and the total `.so` size is typically smaller than pybind11.
- Compile time — nanobind is generally 2-5x faster to compile than pybind11,
  but the effect is invisible on a benchmark this small.
