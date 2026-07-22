"""Extensive benchmark: cursor.bulkcopy() (row tuples) vs cursor.bulkcopy_arrow().

Feeds *identical* data to both code paths so the comparison is apples-to-apples,
then reports throughput, latency stability, data-volume rate, and peak Python
memory across a range of column profiles and row counts.

Three questions this benchmark answers
--------------------------------------
1. Insert-only (both inputs already materialized): how much faster is the Arrow
   write path per se?  -> Scenario A
2. Source *originates* as Arrow (Parquet / polars / DuckDB / pandas / ADBC):
   the tuple path must first box every cell into a Python object. What does that
   conversion tax cost end-to-end?  -> Scenario B
3. Peak Python heap: the tuple path must hold N row tuples in memory; the Arrow
   path streams typed buffers. How different is the memory footprint?  -> Scenario C

Statistics
----------
Each timed cell is run `--repeats` times after a warmup. We report the median
(headline), and also collect min / mean / stdev / p95 so a reviewer can judge
noise (see --csv for the full record). GC is disabled around each timed call.

Usage (PowerShell)
------------------
    $env:DB_CONNECTION_STRING = "Server=...;Database=...;UID=...;PWD=...;TrustServerCertificate=yes;"
    python benchmarks\bench_bulkcopy_arrow.py
    python benchmarks\bench_bulkcopy_arrow.py --rows 1000 10000 100000 --repeats 7
    python benchmarks\bench_bulkcopy_arrow.py --profiles narrow mixed decimal
    python benchmarks\bench_bulkcopy_arrow.py --batch-sizes 0 1000 10000 --rows 100000
    python benchmarks\bench_bulkcopy_arrow.py --quick            # fast smoke run
    python benchmarks\bench_bulkcopy_arrow.py --csv results.csv  # machine-readable

Notes
-----
* `pyarrow` is required (``pip install mssql-python[pyarrow]``).
* `psutil` is optional; if present, RSS deltas are shown alongside the
  tracemalloc-based Python-heap peak.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import gc
import os
import platform
import statistics
import sys
import time
import tracemalloc
from decimal import Decimal

try:
    import pyarrow as pa
except ImportError:  # pragma: no cover
    sys.exit("pyarrow is required: pip install mssql-python[pyarrow]")

try:
    import psutil  # optional, for RSS
except ImportError:  # pragma: no cover
    psutil = None

# Allow running as `python benchmarks/bench_bulkcopy_arrow.py`: put the repo
# root first on sys.path so `import mssql_python` resolves to the in-repo
# package rather than a stray namespace package.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import mssql_python


TABLE = "bulkcopy_arrow_bench"


# --------------------------------------------------------------------------- #
# Column profiles.
# Each column is a 4-tuple: (name, sql_type, arrow_type, value_fn)
#   value_fn(i) -> Python value for row i (drives BOTH the tuple rows and the
#   Arrow column, guaranteeing identical data on both paths).
# All columns are declared NULL-able so profiles may inject NULLs freely.
# --------------------------------------------------------------------------- #
_BASE_DT = dt.datetime(2024, 1, 1)
_NAMES = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"]


def _nullable(columns, every=3):
    """Wrap value_fns so every `every`-th row is NULL (schema unchanged)."""
    out = []
    for (name, sql, atype, fn) in columns:
        out.append((name, sql, atype, (lambda f: (lambda i: None if i % every == 0 else f(i)))(fn)))
    return out


def _profiles():
    narrow = [
        ("id", "INT", pa.int32(), lambda i: i),
        ("big", "BIGINT", pa.int64(), lambda i: i * 1_000_003),
        ("score", "FLOAT", pa.float64(), lambda i: i * 1.5),
    ]

    mixed = [
        ("id", "INT", pa.int32(), lambda i: i),
        ("name", "NVARCHAR(50)", pa.string(), lambda i: _NAMES[i % len(_NAMES)]),
        ("amount", "FLOAT", pa.float64(), lambda i: i * 3.14159),
        ("flag", "BIT", pa.bool_(), lambda i: bool(i % 2)),
        ("created", "DATETIME2", pa.timestamp("us"), lambda i: _BASE_DT + dt.timedelta(seconds=i)),
    ]

    wide = [(f"c{n}", "INT", pa.int32(), (lambda n: (lambda i: i + n))(n)) for n in range(20)]

    decimal = [
        ("id", "INT", pa.int32(), lambda i: i),
        ("price", "DECIMAL(18,4)", pa.decimal128(18, 4), lambda i: Decimal(i * 100 + 99) / Decimal(100)),
        ("qty", "NUMERIC(10,2)", pa.decimal128(10, 2), lambda i: Decimal(i % 1000) / Decimal(10)),
        ("cost", "MONEY", pa.decimal128(19, 4), lambda i: Decimal(i * 7 + 1) / Decimal(100)),
    ]

    strings = [
        ("id", "INT", pa.int32(), lambda i: i),
        ("short", "NVARCHAR(20)", pa.string(), lambda i: _NAMES[i % len(_NAMES)]),
        ("medium", "NVARCHAR(120)", pa.string(), lambda i: ("x" * 48) + str(i)),
        ("wide", "NVARCHAR(400)", pa.string(), lambda i: (f"row-{i}-" + "data-" * 70)[:380]),
    ]

    temporal = [
        ("id", "INT", pa.int32(), lambda i: i),
        ("d", "DATE", pa.date32(), lambda i: (_BASE_DT + dt.timedelta(days=i % 3650)).date()),
        ("dt2", "DATETIME2", pa.timestamp("us"), lambda i: _BASE_DT + dt.timedelta(seconds=i)),
        ("t", "TIME", pa.time64("us"), lambda i: (_BASE_DT + dt.timedelta(seconds=i % 86400)).time()),
    ]

    binary = [
        ("id", "INT", pa.int32(), lambda i: i),
        ("payload", "VARBINARY(64)", pa.binary(), lambda i: (str(i).encode() * 8)[:64]),
    ]

    profiles = {
        "narrow": narrow,
        "mixed": mixed,
        "wide": wide,
        "decimal": decimal,
        "strings": strings,
        "temporal": temporal,
        "binary": binary,
        "nullable": _nullable(mixed),
    }
    return profiles


# --------------------------------------------------------------------------- #
# Data construction
# --------------------------------------------------------------------------- #
def _build_data(columns, row_count):
    """Return (rows_as_tuples, arrow_table) carrying identical data."""
    per_col = [[fn(i) for i in range(row_count)] for (_, _, _, fn) in columns]
    rows = list(zip(*per_col))
    schema = pa.schema([(name, atype) for (name, _, atype, _) in columns])
    arrays = [pa.array(col, type=atype) for (_, _, atype, _), col in zip(columns, per_col)]
    table = pa.Table.from_arrays(arrays, schema=schema)
    return rows, table


def _arrow_to_tuples(table):
    """Materialize an Arrow table into row tuples the way a typical caller would.

    This is the tax the tuple path must pay when the data *originates* as Arrow
    (Parquet / polars / DuckDB / pandas / ADBC): every cell becomes a boxed
    Python object. bulkcopy_arrow() skips this entirely.
    """
    return list(zip(*(col.to_pylist() for col in table.columns)))


# --------------------------------------------------------------------------- #
# Table lifecycle
# --------------------------------------------------------------------------- #
def _recreate_table(cursor, columns):
    cols_ddl = ", ".join(f"[{name}] {sql} NULL" for (name, sql, _, _) in columns)
    cursor.execute(f"IF OBJECT_ID('{TABLE}', 'U') IS NOT NULL DROP TABLE [{TABLE}];")
    cursor.execute(f"CREATE TABLE [{TABLE}] ({cols_ddl});")
    cursor.connection.commit()


def _truncate(cursor):
    cursor.execute(f"TRUNCATE TABLE [{TABLE}];")
    cursor.connection.commit()


def _drop_table(cursor):
    cursor.execute(f"IF OBJECT_ID('{TABLE}', 'U') IS NOT NULL DROP TABLE [{TABLE}];")
    cursor.connection.commit()


# --------------------------------------------------------------------------- #
# Timing / statistics
# --------------------------------------------------------------------------- #
def _time_call(fn):
    """Time a single call with GC disabled to reduce noise."""
    gc.collect()
    gc.disable()
    try:
        start = time.perf_counter()
        result = fn()
        elapsed = time.perf_counter() - start
    finally:
        gc.enable()
    return elapsed, result


def _stats(samples):
    """Return a dict of summary statistics for a list of timings."""
    s = sorted(samples)
    n = len(s)
    p95 = s[min(n - 1, int(round(0.95 * (n - 1))))]
    mean = statistics.fmean(s)
    stdev = statistics.pstdev(s) if n > 1 else 0.0
    return {
        "min": s[0],
        "median": statistics.median(s),
        "mean": mean,
        "stdev": stdev,
        "p95": p95,
        "cv": (stdev / mean * 100.0) if mean else 0.0,
        "n": n,
    }


# --------------------------------------------------------------------------- #
# Scenarios
# --------------------------------------------------------------------------- #
def _run_insert_only(cursor, rows, table, repeats, batch_size):
    """Scenario A: both inputs pre-materialized; time the write only."""
    tup, arw = [], []
    r_tup = r_arw = None
    for _ in range(repeats):
        _truncate(cursor)
        e, r_tup = _time_call(lambda: cursor.bulkcopy(TABLE, rows, batch_size=batch_size))
        tup.append(e)
        _truncate(cursor)
        e, r_arw = _time_call(lambda: cursor.bulkcopy_arrow(TABLE, table, batch_size=batch_size))
        arw.append(e)
    return _stats(tup), _stats(arw), r_tup, r_arw


def _run_from_arrow(cursor, table, repeats, batch_size):
    """Scenario B: source is Arrow; tuple path must convert then insert."""
    conv, ins, total = [], [], []
    r = None
    for _ in range(repeats):
        _truncate(cursor)
        t_conv, rows = _time_call(lambda: _arrow_to_tuples(table))
        t_ins, r = _time_call(lambda: cursor.bulkcopy(TABLE, rows, batch_size=batch_size))
        conv.append(t_conv)
        ins.append(t_ins)
        total.append(t_conv + t_ins)
    return _stats(conv), _stats(ins), _stats(total), r


def _peak_python_mb(fn):
    """Return (result, peak_python_heap_MB) for a single call."""
    gc.collect()
    tracemalloc.start()
    try:
        result = fn()
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    return result, peak / 1e6


def _rss_mb():
    if psutil is None:
        return None
    return psutil.Process().memory_info().rss / 1e6


def _run_memory(cursor, table, batch_size):
    """Scenario C: peak Python heap for tuple-from-arrow vs arrow-direct."""
    # Tuple path: materialize rows, then insert (measure the whole thing).
    _truncate(cursor)
    rss0 = _rss_mb()

    def _tuple_path():
        rows = _arrow_to_tuples(table)
        return cursor.bulkcopy(TABLE, rows, batch_size=batch_size)

    _, tup_peak = _peak_python_mb(_tuple_path)
    rss_tup = None if rss0 is None else _rss_mb() - rss0

    # Arrow path: stream typed buffers, no tuple materialization.
    _truncate(cursor)
    rss1 = _rss_mb()
    _, arw_peak = _peak_python_mb(lambda: cursor.bulkcopy_arrow(TABLE, table, batch_size=batch_size))
    rss_arw = None if rss1 is None else _rss_mb() - rss1
    return tup_peak, arw_peak, rss_tup, rss_arw


# --------------------------------------------------------------------------- #
# Driver
# --------------------------------------------------------------------------- #
def _server_version(cursor):
    try:
        cursor.execute("SELECT @@VERSION;")
        return cursor.fetchone()[0].splitlines()[0].strip()
    except Exception:  # pragma: no cover
        return "unknown"


def _print_env(cursor):
    print("=" * 78)
    print("bulkcopy vs bulkcopy_arrow benchmark")
    print("-" * 78)
    print(f"  timestamp   : {dt.datetime.now().isoformat(timespec='seconds')}")
    print(f"  python      : {platform.python_version()} ({platform.platform()})")
    print(f"  pyarrow     : {pa.__version__}")
    print(f"  mssql_python: {getattr(mssql_python, '__version__', 'unknown')}")
    print(f"  sql server  : {_server_version(cursor)}")
    print(f"  psutil RSS  : {'available' if psutil else 'not installed (peak RSS skipped)'}")
    print("=" * 78)


def run(row_counts, repeats, selected_profiles, batch_sizes, do_memory, csv_path):
    conn_str = os.environ.get("DB_CONNECTION_STRING")
    if not conn_str:
        sys.exit("Set DB_CONNECTION_STRING to run this benchmark.")

    all_profiles = _profiles()
    unknown = [p for p in selected_profiles if p not in all_profiles]
    if unknown:
        sys.exit(f"Unknown profile(s): {unknown}. Available: {sorted(all_profiles)}")
    profiles = {p: all_profiles[p] for p in selected_profiles}

    conn = mssql_python.connect(conn_str)
    cursor = conn.cursor()
    _print_env(cursor)

    records = []
    try:
        for pname, columns in profiles.items():
            for row_count in row_counts:
                rows, table = _build_data(columns, row_count)
                bytes_total = table.nbytes
                _recreate_table(cursor, columns)

                for bsize in batch_sizes:
                    # Warmup each path once (buffers, plan cache, etc.).
                    _truncate(cursor)
                    cursor.bulkcopy(TABLE, rows, batch_size=bsize)
                    _truncate(cursor)
                    cursor.bulkcopy_arrow(TABLE, table, batch_size=bsize)

                    a_tup, a_arw, r_tup, r_arw = _run_insert_only(cursor, rows, table, repeats, bsize)
                    b_conv, b_ins, b_total, r_fa = _run_from_arrow(cursor, table, repeats, bsize)

                    assert r_tup["rows_copied"] == row_count, r_tup
                    assert r_arw["rows_copied"] == row_count, r_arw
                    assert r_fa["rows_copied"] == row_count, r_fa

                    mem = (None, None, None, None)
                    if do_memory:
                        mem = _run_memory(cursor, table, bsize)

                    records.append(
                        {
                            "profile": pname,
                            "cols": len(columns),
                            "rows": row_count,
                            "batch_size": bsize,
                            "bytes": bytes_total,
                            "A_tup": a_tup,
                            "A_arw": a_arw,
                            "B_conv": b_conv,
                            "B_ins": b_ins,
                            "B_total": b_total,
                            "C_tup_mb": mem[0],
                            "C_arw_mb": mem[1],
                            "C_tup_rss": mem[2],
                            "C_arw_rss": mem[3],
                        }
                    )
    finally:
        _drop_table(cursor)
        cursor.close()
        conn.close()

    _report(records, do_memory)
    if csv_path:
        _write_csv(records, csv_path, do_memory)
        print(f"\nFull records (incl. min/mean/stdev/p95/CV) written to {csv_path}")


# --------------------------------------------------------------------------- #
# Reporting
# --------------------------------------------------------------------------- #
def _mbps(bytes_total, seconds):
    return (bytes_total / 1e6) / seconds if seconds else 0.0


def _report(records, do_memory):
    # Scenario A -------------------------------------------------------------
    print("\n=== Scenario A: insert-only (data already in tuples / Arrow) ===")
    hdr = (
        f"{'profile':<9}{'cols':>5}{'rows':>10}{'batch':>8}"
        f"{'tuples(s)':>11}{'arrow(s)':>10}{'tup r/s':>12}{'arw r/s':>12}"
        f"{'arw MB/s':>10}{'speedup':>9}{'CV%tup':>8}"
    )
    print(hdr)
    print("-" * len(hdr))
    for r in records:
        t, a = r["A_tup"]["median"], r["A_arw"]["median"]
        print(
            f"{r['profile']:<9}{r['cols']:>5}{r['rows']:>10,}{r['batch_size']:>8}"
            f"{t:>11.4f}{a:>10.4f}{r['rows'] / t:>12,.0f}{r['rows'] / a:>12,.0f}"
            f"{_mbps(r['bytes'], a):>10.1f}{(t / a if a else 0):>8.2f}x{r['A_tup']['cv']:>7.1f}%"
        )

    # Scenario B -------------------------------------------------------------
    print("\n=== Scenario B: source originates as Arrow (tuple path must convert) ===")
    hdr2 = (
        f"{'profile':<9}{'cols':>5}{'rows':>10}{'batch':>8}"
        f"{'convert(s)':>12}{'+insert(s)':>12}{'total(s)':>10}{'arrow(s)':>10}{'speedup':>9}"
    )
    print(hdr2)
    print("-" * len(hdr2))
    for r in records:
        conv, ins, tot = r["B_conv"]["median"], r["B_ins"]["median"], r["B_total"]["median"]
        a = r["A_arw"]["median"]
        print(
            f"{r['profile']:<9}{r['cols']:>5}{r['rows']:>10,}{r['batch_size']:>8}"
            f"{conv:>12.4f}{ins:>12.4f}{tot:>10.4f}{a:>10.4f}{(tot / a if a else 0):>8.2f}x"
        )

    # Scenario C -------------------------------------------------------------
    if do_memory:
        print("\n=== Scenario C: peak Python heap (tuple-from-Arrow vs arrow-direct) ===")
        rss_note = "" if psutil else "  (install psutil for RSS columns)"
        hdr3 = (
            f"{'profile':<9}{'rows':>10}{'batch':>8}"
            f"{'tup heap MB':>13}{'arw heap MB':>13}{'heap x':>8}"
            f"{'tup RSS MB':>12}{'arw RSS MB':>12}"
        )
        print(hdr3 + rss_note)
        print("-" * len(hdr3))
        for r in records:
            tm, am = r["C_tup_mb"], r["C_arw_mb"]
            ratio = (tm / am) if (tm and am) else 0.0
            tr = "-" if r["C_tup_rss"] is None else f"{r['C_tup_rss']:.1f}"
            ar = "-" if r["C_arw_rss"] is None else f"{r['C_arw_rss']:.1f}"
            print(
                f"{r['profile']:<9}{r['rows']:>10,}{r['batch_size']:>8}"
                f"{tm:>13.1f}{am:>13.1f}{ratio:>7.1f}x{tr:>12}{ar:>12}"
            )


def _write_csv(records, path, do_memory):
    fields = [
        "profile", "cols", "rows", "batch_size", "bytes",
        "A_tup_median", "A_tup_min", "A_tup_mean", "A_tup_stdev", "A_tup_p95", "A_tup_cv",
        "A_arw_median", "A_arw_min", "A_arw_mean", "A_arw_stdev", "A_arw_p95", "A_arw_cv",
        "A_speedup",
        "B_convert_median", "B_insert_median", "B_total_median", "B_speedup",
    ]
    if do_memory:
        fields += ["C_tup_heap_mb", "C_arw_heap_mb", "C_heap_ratio", "C_tup_rss_mb", "C_arw_rss_mb"]

    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        for r in records:
            at, aa = r["A_tup"], r["A_arw"]
            a_speedup = at["median"] / aa["median"] if aa["median"] else 0.0
            b_speedup = r["B_total"]["median"] / aa["median"] if aa["median"] else 0.0
            row = {
                "profile": r["profile"], "cols": r["cols"], "rows": r["rows"],
                "batch_size": r["batch_size"], "bytes": r["bytes"],
                "A_tup_median": at["median"], "A_tup_min": at["min"], "A_tup_mean": at["mean"],
                "A_tup_stdev": at["stdev"], "A_tup_p95": at["p95"], "A_tup_cv": at["cv"],
                "A_arw_median": aa["median"], "A_arw_min": aa["min"], "A_arw_mean": aa["mean"],
                "A_arw_stdev": aa["stdev"], "A_arw_p95": aa["p95"], "A_arw_cv": aa["cv"],
                "A_speedup": a_speedup,
                "B_convert_median": r["B_conv"]["median"],
                "B_insert_median": r["B_ins"]["median"],
                "B_total_median": r["B_total"]["median"],
                "B_speedup": b_speedup,
            }
            if do_memory:
                tm, am = r["C_tup_mb"], r["C_arw_mb"]
                row.update(
                    {
                        "C_tup_heap_mb": tm,
                        "C_arw_heap_mb": am,
                        "C_heap_ratio": (tm / am) if (tm and am) else 0.0,
                        "C_tup_rss_mb": r["C_tup_rss"],
                        "C_arw_rss_mb": r["C_arw_rss"],
                    }
                )
            w.writerow(row)


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def main():
    parser = argparse.ArgumentParser(
        description="Benchmark cursor.bulkcopy() vs cursor.bulkcopy_arrow().",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--rows", type=int, nargs="+", default=[1_000, 10_000, 100_000],
                        help="Row counts to benchmark.")
    parser.add_argument("--repeats", type=int, default=5,
                        help="Timed repeats per cell (after a warmup).")
    parser.add_argument("--profiles", nargs="+", default=list(_profiles().keys()),
                        help="Column profiles to run (subset of the built-in set).")
    parser.add_argument("--batch-sizes", type=int, nargs="+", default=[0],
                        help="bulk-copy batch sizes to sweep (0 = server optimal).")
    parser.add_argument("--no-memory", action="store_true",
                        help="Skip the peak-memory scenario (C).")
    parser.add_argument("--csv", metavar="PATH", default=None,
                        help="Write full per-cell statistics to a CSV file.")
    parser.add_argument("--quick", action="store_true",
                        help="Fast smoke run: --rows 500 --repeats 1 --profiles narrow mixed.")
    args = parser.parse_args()

    if args.quick:
        args.rows = [500]
        args.repeats = 1
        args.profiles = ["narrow", "mixed"]

    run(
        row_counts=args.rows,
        repeats=args.repeats,
        selected_profiles=args.profiles,
        batch_sizes=args.batch_sizes,
        do_memory=not args.no_memory,
        csv_path=args.csv,
    )


if __name__ == "__main__":
    main()
