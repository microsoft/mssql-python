"""Benchmark: cursor.bulkcopy() (row tuples) vs cursor.bulkcopy_arrow() (Arrow).

Feeds *identical* data to both code paths so the comparison is apples-to-apples:
  - bulkcopy()       receives a list of row tuples
  - bulkcopy_arrow() receives the equivalent pyarrow.Table

For each (profile, row_count) it recreates the target table, runs each method
`REPEATS` times (after a warmup), and reports the median wall-clock time and
throughput plus the arrow-vs-tuples speedup.

Usage (PowerShell):
    $env:DB_CONNECTION_STRING = "Server=...;DATABASE=...;UID=...;PWD=...;TrustServerCertificate=yes;"
    python benchmarks\bench_bulkcopy_arrow.py
    python benchmarks\bench_bulkcopy_arrow.py --rows 1000 10000 100000 --repeats 5
"""

import argparse
import datetime as dt
import os
import statistics
import sys
import time

import pyarrow as pa

import mssql_python


TABLE = "bulkcopy_arrow_bench"


# --------------------------------------------------------------------------- #
# Column profiles. Each column: (name, sql_type, arrow_type, value_fn)
# value_fn(i) -> python value for row i (also used to build the Arrow column).
# --------------------------------------------------------------------------- #
def _profiles():
    base_dt = dt.datetime(2024, 1, 1)
    names = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot"]

    narrow = [
        ("id", "INT", pa.int32(), lambda i: i),
        ("big", "BIGINT", pa.int64(), lambda i: i * 1_000_003),
        ("score", "FLOAT", pa.float64(), lambda i: i * 1.5),
    ]

    mixed = [
        ("id", "INT", pa.int32(), lambda i: i),
        ("name", "NVARCHAR(50)", pa.string(), lambda i: names[i % len(names)]),
        ("amount", "FLOAT", pa.float64(), lambda i: i * 3.14159),
        ("flag", "BIT", pa.bool_(), lambda i: bool(i % 2)),
        ("created", "DATETIME2", pa.timestamp("us"), lambda i: base_dt + dt.timedelta(seconds=i)),
    ]

    wide = [(f"c{n}", "INT", pa.int32(), (lambda n: (lambda i: i + n))(n)) for n in range(20)]

    return {"narrow": narrow, "mixed": mixed, "wide": wide}


def _build_data(columns, row_count):
    """Return (rows_as_tuples, arrow_table) carrying identical data."""
    per_col = [[fn(i) for i in range(row_count)] for (_, _, _, fn) in columns]
    rows = list(zip(*per_col))
    schema = pa.schema([(name, atype) for (name, _, atype, _) in columns])
    table = pa.table({name: col for (name, _, _, _), col in zip(columns, per_col)}, schema=schema)
    return rows, table


def _arrow_to_tuples(table):
    """Materialize an Arrow table into row tuples the way a typical caller would.

    This is the tax the tuple path must pay when the data *originates* as Arrow
    (Parquet / polars / DuckDB / pandas / ADBC): every cell becomes a boxed
    Python object. bulkcopy_arrow() skips this entirely.
    """
    return list(zip(*(col.to_pylist() for col in table.columns)))


def _recreate_table(cursor, columns):
    cols_ddl = ", ".join(f"[{name}] {sql}" for (name, sql, _, _) in columns)
    cursor.execute(f"IF OBJECT_ID('{TABLE}', 'U') IS NOT NULL DROP TABLE [{TABLE}];")
    cursor.execute(f"CREATE TABLE [{TABLE}] ({cols_ddl});")
    cursor.connection.commit()


def _truncate(cursor):
    cursor.execute(f"TRUNCATE TABLE [{TABLE}];")
    cursor.connection.commit()


def _time_call(fn):
    start = time.perf_counter()
    result = fn()
    return time.perf_counter() - start, result


def _median_run(cursor, columns, rows, table, use_arrow, repeats):
    times = []
    reported = None
    for _ in range(repeats):
        _truncate(cursor)
        if use_arrow:
            elapsed, res = _time_call(lambda: cursor.bulkcopy_arrow(TABLE, table))
        else:
            elapsed, res = _time_call(lambda: cursor.bulkcopy(TABLE, rows))
        times.append(elapsed)
        reported = res
    return statistics.median(times), reported


def _median_run_from_arrow(cursor, table, repeats):
    """Tuple path when the source is an Arrow table: time convert + bulkcopy.

    Returns (median_convert_s, median_insert_s, median_total_s, last_result).
    """
    convert_times, insert_times, total_times = [], [], []
    reported = None
    for _ in range(repeats):
        _truncate(cursor)
        t_conv, rows = _time_call(lambda: _arrow_to_tuples(table))
        t_ins, res = _time_call(lambda: cursor.bulkcopy(TABLE, rows))
        convert_times.append(t_conv)
        insert_times.append(t_ins)
        total_times.append(t_conv + t_ins)
        reported = res
    return (
        statistics.median(convert_times),
        statistics.median(insert_times),
        statistics.median(total_times),
        reported,
    )


def run(row_counts, repeats):
    conn_str = os.environ.get("DB_CONNECTION_STRING")
    if not conn_str:
        sys.exit("Set DB_CONNECTION_STRING to run this benchmark.")

    profiles = _profiles()
    conn = mssql_python.connect(conn_str)
    cursor = conn.cursor()

    # Collect measurements once; print two views afterwards.
    rows_out = []
    try:
        for pname, columns in profiles.items():
            for row_count in row_counts:
                rows, table = _build_data(columns, row_count)
                _recreate_table(cursor, columns)

                # Warmup each path once (JIT paths, connection buffers, etc.).
                _truncate(cursor)
                cursor.bulkcopy(TABLE, rows)
                _truncate(cursor)
                cursor.bulkcopy_arrow(TABLE, table)

                # Scenario A: insert-only (inputs already materialized for free).
                t_tup, r_tup = _median_run(cursor, columns, rows, table, False, repeats)
                t_arw, r_arw = _median_run(cursor, columns, rows, table, True, repeats)
                # Scenario B: source originates as Arrow -> tuple path must convert.
                t_conv, t_ins, t_total, r_fa = _median_run_from_arrow(cursor, table, repeats)

                assert r_tup["rows_copied"] == row_count, r_tup
                assert r_arw["rows_copied"] == row_count, r_arw
                assert r_fa["rows_copied"] == row_count, r_fa

                rows_out.append(
                    {
                        "profile": pname,
                        "cols": len(columns),
                        "rows": row_count,
                        "t_tup": t_tup,
                        "t_arw": t_arw,
                        "t_conv": t_conv,
                        "t_ins": t_ins,
                        "t_total": t_total,
                    }
                )
    finally:
        cursor.execute(f"IF OBJECT_ID('{TABLE}', 'U') IS NOT NULL DROP TABLE [{TABLE}];")
        cursor.connection.commit()
        cursor.close()
        conn.close()

    # Scenario A: apples-to-apples insert, both inputs pre-materialized.
    print("\n=== Scenario A: insert-only (data already in tuples / Arrow) ===")
    hdr = f"{'profile':<8} {'cols':>4} {'rows':>9} {'tuples(s)':>11} {'arrow(s)':>10} {'tup r/s':>12} {'arw r/s':>12} {'speedup':>8}"
    print(hdr)
    print("-" * len(hdr))
    for r in rows_out:
        rps_tup = r["rows"] / r["t_tup"] if r["t_tup"] else 0
        rps_arw = r["rows"] / r["t_arw"] if r["t_arw"] else 0
        speedup = r["t_tup"] / r["t_arw"] if r["t_arw"] else 0
        print(
            f"{r['profile']:<8} {r['cols']:>4} {r['rows']:>9,} "
            f"{r['t_tup']:>11.4f} {r['t_arw']:>10.4f} "
            f"{rps_tup:>12,.0f} {rps_arw:>12,.0f} {speedup:>7.2f}x"
        )

    # Scenario B: source is Arrow. Tuple path pays convert + insert; arrow is direct.
    print("\n=== Scenario B: source originates as Arrow (tuple path must convert) ===")
    hdr2 = f"{'profile':<8} {'cols':>4} {'rows':>9} {'convert(s)':>11} {'+insert(s)':>11} {'total(s)':>10} {'arrow(s)':>10} {'speedup':>8}"
    print(hdr2)
    print("-" * len(hdr2))
    for r in rows_out:
        speedup = r["t_total"] / r["t_arw"] if r["t_arw"] else 0
        print(
            f"{r['profile']:<8} {r['cols']:>4} {r['rows']:>9,} "
            f"{r['t_conv']:>11.4f} {r['t_ins']:>11.4f} {r['t_total']:>10.4f} "
            f"{r['t_arw']:>10.4f} {speedup:>7.2f}x"
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, nargs="+", default=[1_000, 10_000, 100_000])
    parser.add_argument("--repeats", type=int, default=5)
    args = parser.parse_args()
    run(args.rows, args.repeats)


if __name__ == "__main__":
    main()
