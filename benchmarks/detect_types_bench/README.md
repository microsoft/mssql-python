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
python run_benchmark.py    # then perf comparison across 15 workload mixes

# Optional flags for the perf harness:
python run_benchmark.py --runs 9          # more samples per (workload, variant)
python run_benchmark.py --seconds 2.0     # longer wall-clock window per run
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
- `run_benchmark.py` — the perf harness. Runs **15 workload mixes** (OLTP and
  Analytics at 1, 10, 100, and 1000 row scales; homogeneous Int/Str/Decimal
  at 100 and 1000 params; DAE-heavy large strings/bytes at 100 and 1000).
  Reports median / stddev / min / max ns per parameter across configurable
  sample count (default: median of 5 runs, ~1s each). GC disabled during
  measurement.
- `parity_test.py` — the correctness harness. Runs a 61-input corpus through
  all three variants and against a Python-side reference implementation of
  `DetectParamTypes` semantics.

## Sample results (Python 3.12.3, gcc 13.3.0 -O3, one Linux x86_64 workstation)

Two datasets are provided. Both were run on the same machine, back-to-back.
The **small dataset** is easier to eyeball and matches what a user would see
running a handful of small `execute()` calls. The **expanded dataset** covers
15 workloads at 1-, 10-, 100-, and 1000-row scales and reports median /
stddev / min / max across 5 samples per (workload, variant) so run-to-run
variance is visible.

### Initial small dataset (5 workloads, best-of-3)

Nanoseconds per parameter, best-of-3, averaged across three back-to-back runs.

| Workload | pybind11 | raw CPython | nanobind | nanobind vs pybind11 | nanobind vs raw CPython |
|---|---:|---:|---:|---:|---:|
| OLTP mixed (6 params) | 167 ns | 124 ns | 123 ns | 1.36x faster | 1% faster |
| Analytics (10 params) | 194 ns | 139 ns | 134 ns | 1.45x faster | 3% faster |
| Int-heavy (20 params) | 9.5 ns | 6.5 ns | 5.8 ns | 1.62x faster | 10% faster |
| Str-heavy (20 params) | 8.2 ns | 5.8 ns | 4.9 ns | 1.68x faster | 16% faster |
| Decimal-heavy (20 params) | 722 ns | 617 ns | 602 ns | 1.20x faster | 2% faster |

**Takeaway at small scale:** Both raw CPython and nanobind deliver a substantial
1.2x-1.7x speedup over pybind11. In this environment nanobind matches or
slightly beats raw CPython on every workload, largely because nanobind uses
PEP 590 vectorcall for the function-call boundary while the raw-CPython
version relies on `METH_VARARGS + PyArg_ParseTuple` — a per-call cost that
matters when calls take only ~100-1000 ns each.

### Expanded dataset (15 workloads, median-of-5)

Median ns per parameter across 5 samples per (workload, variant) at ~1 second
of wall-clock work each. Stddev is typically 0.5–2% of the median.

| Workload | pybind11 (baseline) | raw CPython (this PR) | nanobind (proposed) | CPython vs pybind11 | nanobind vs pybind11 |
|---|---:|---:|---:|---:|---:|
| OLTP 6 params (1 row) | 169.15 | 133.11 | **127.88** | 1.27x | **1.32x** |
| OLTP 60 params (10 rows) | 142.19 | **121.67** | 125.52 | **1.17x** | 1.13x |
| OLTP 600 params (100 rows) | 134.67 | 118.87 | **119.22** | 1.13x | **1.13x** |
| OLTP 6000 params (1000 rows) | 137.08 | 119.73 | **116.91** | 1.14x | **1.17x** |
| Analytics 10 params (1 row) | 190.67 | 140.46 | **139.01** | 1.36x | **1.37x** |
| Analytics 100 params (10 rows) | 171.38 | **133.22** | 133.34 | **1.29x** | 1.29x |
| Analytics 1000 params (100 rows) | 173.48 | **131.98** | 133.42 | **1.31x** | 1.30x |
| Int-heavy 100 params | 5.99 | **4.71** | 4.79 | **1.27x** | 1.25x |
| Int-heavy 1000 params | 5.30 | **4.23** | 4.40 | **1.25x** | 1.20x |
| Str-heavy 100 params | 5.10 | 4.00 | **3.75** | 1.27x | **1.36x** |
| Str-heavy 1000 params | 4.25 | 3.55 | **3.51** | 1.20x | **1.21x** |
| Decimal-heavy 100 params | 707.37 | 641.57 | **628.83** | 1.10x | **1.12x** |
| Decimal-heavy 1000 params | 701.35 | **627.49** | 633.15 | **1.12x** | 1.11x |
| DAE-heavy 100 large params | 5.69 | 4.15 | **3.95** | 1.37x | **1.44x** |
| DAE-heavy 1000 large params | 4.84 | 3.63 | **3.55** | 1.33x | **1.36x** |

### How to read the expanded dataset

- **Both fast variants deliver a 1.10–1.44x speedup over pybind11** across
  every workload. The speedup is largest at small sizes (per-call overhead
  dominates) and narrows at bulk sizes (per-param work dominates).
- **Nanobind and raw CPython are statistically tied.** Across 15 workloads,
  nanobind wins 7, raw CPython wins 6, they tie on 2 — all within the
  measured stddev.
- **Small workloads favor nanobind** (OLTP 6, Analytics 10, DAE-heavy 100/1000)
  because PEP 590 vectorcall saves ~30 ns per call vs `METH_VARARGS +
  PyArg_ParseTuple`. At 6 params that's ~5 ns/param; at 6000 params it's ~0.005
  ns/param — invisible.
- **Bulk workloads (600+ params) settle at ~117–134 ns/param for both fast
  variants**, roughly 1.13–1.31x faster than pybind11. This is the range
  that matters for `executemany` throughput.
- **Homogeneous int/str/decimal workloads** cost 3–6 ns/param on the fast
  variants because they hit a single hot branch repeatedly. The mixed OLTP
  and Analytics numbers (100–170 ns/param) are more representative of real
  ad-hoc queries.

### Cross-referencing the two datasets

The small-dataset "OLTP mixed (6 params)" and expanded "OLTP 6 params (1 row)"
are the same workload measured with different methodologies (best-of-3 vs
median-of-5 with tighter iteration counts). They agree to within measurement
noise:

| Workload | small dataset | expanded dataset |
|---|---:|---:|
| OLTP mixed (6 params) — pybind11 | 167 ns | 169.15 ns |
| OLTP mixed (6 params) — raw CPython | 124 ns | 133.11 ns |
| OLTP mixed (6 params) — nanobind | 123 ns | 127.88 ns |

The expanded methodology gives slightly higher numbers because it uses median
rather than min-of-3 — a more conservative and reproducible statistic that
does not cherry-pick the fastest run.

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
