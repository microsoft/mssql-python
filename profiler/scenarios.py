# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.
"""
Profiling scenarios — each is a self-contained benchmark that yields
a (title, wall_ms, cpp_stats, py_stats) result.

Scenarios generate their own test data and clean up after themselves.
Only requires a connection string and a live SQL Server instance.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mssql_python.connection import Connection

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
ROW_COUNT = 50_000
EXECUTEMANY_ROWS = 5_000
FETCHMANY_SIZE = 1_000
FETCHONE_ROWS = 1_000
INSERT_COUNT = 100
COMMIT_ROLLBACK_COUNT = 100
IMV_ROWS_PER_BATCH = 1_000  # 2000 params, under 2100 limit
IMV_TOTAL_ROWS = 100_000

_TEST_TABLE = "#perf_test"

_CREATE_TABLE = f"""
IF OBJECT_ID('tempdb..{_TEST_TABLE}', 'U') IS NOT NULL DROP TABLE {_TEST_TABLE};
CREATE TABLE {_TEST_TABLE} (
    id INT IDENTITY(1,1) PRIMARY KEY,
    int_col INT, bigint_col BIGINT, float_col FLOAT,
    varchar_col VARCHAR(100), nvarchar_col NVARCHAR(100),
    date_col DATE, datetime_col DATETIME2,
    decimal_col DECIMAL(18,4), bit_col BIT
);
"""

_INSERT_COLS = (
    "int_col, bigint_col, float_col, varchar_col, nvarchar_col, "
    "date_col, datetime_col, decimal_col, bit_col"
)
_INSERT_PLACEHOLDERS = "?, ?, ?, ?, ?, ?, ?, ?, ?"


def _make_rows(n: int) -> list[tuple]:
    return [
        (
            i,
            i * 1_000_000,
            i * 1.5,
            f"row_{i}_data",
            f"unicode_row_{i}",
            "2024-06-15",
            "2024-06-15 14:30:00.123456",
            f"{i}.1234",
            i % 2,
        )
        for i in range(n)
    ]


def setup_test_data(conn: "Connection", row_count: int = ROW_COUNT) -> str:
    cursor = conn.cursor()
    cursor.execute(_CREATE_TABLE)
    conn.commit()
    cursor.executemany(
        f"INSERT INTO {_TEST_TABLE} ({_INSERT_COLS}) VALUES ({_INSERT_PLACEHOLDERS})",
        _make_rows(row_count),
    )
    conn.commit()
    cursor.close()
    return _TEST_TABLE


# ---------------------------------------------------------------------------
# Individual scenarios
# ---------------------------------------------------------------------------


def connect(conn_str: str, ctx) -> dict:
    from mssql_python import connect as _connect

    ctx.enable()
    t0 = time.perf_counter()
    c = _connect(conn_str)
    wall_ms = (time.perf_counter() - t0) * 1000
    cpp, py = ctx.collect()
    c.close()
    return {"title": "CONNECT", "wall_ms": wall_ms, "cpp": cpp, "py": py}


def execute_select(conn, table, ctx) -> dict:
    cursor = conn.cursor()
    ctx.enable()
    t0 = time.perf_counter()
    cursor.execute(f"SELECT * FROM {table} WHERE id <= 100")
    rows = cursor.fetchall()
    wall_ms = (time.perf_counter() - t0) * 1000
    cpp, py = ctx.collect()
    cursor.close()
    return {
        "title": f"EXECUTE SELECT ({len(rows)} rows)",
        "wall_ms": wall_ms,
        "cpp": cpp,
        "py": py,
        "detail": f"Rows: {len(rows)}",
    }


def execute_insert(conn, table, ctx, count: int = INSERT_COUNT) -> dict:
    cursor = conn.cursor()
    ctx.enable()
    t0 = time.perf_counter()
    for i in range(count):
        cursor.execute(
            f"INSERT INTO {table} ({_INSERT_COLS}) VALUES ({_INSERT_PLACEHOLDERS})",
            (
                i,
                i * 1000,
                1.5,
                f"insert_{i}",
                f"ins_{i}",
                "2025-01-01",
                "2025-01-01 12:00:00",
                "99.99",
                1,
            ),
        )
    wall_ms = (time.perf_counter() - t0) * 1000
    cpp, py = ctx.collect()
    conn.commit()
    cursor.close()
    return {
        "title": f"EXECUTE INSERT ({count}x)",
        "wall_ms": wall_ms,
        "cpp": cpp,
        "py": py,
        "detail": f"{count} individual INSERTs",
    }


def executemany(conn, table, ctx, row_count: int = EXECUTEMANY_ROWS) -> dict:
    cursor = conn.cursor()
    params = [
        (i, i * 1000, 1.5, f"batch_{i}", f"b_{i}", "2025-01-01", "2025-01-01 12:00:00", "99.99", 1)
        for i in range(row_count)
    ]
    ctx.enable()
    t0 = time.perf_counter()
    cursor.executemany(
        f"INSERT INTO {table} ({_INSERT_COLS}) VALUES ({_INSERT_PLACEHOLDERS})",
        params,
    )
    wall_ms = (time.perf_counter() - t0) * 1000
    cpp, py = ctx.collect()
    conn.commit()
    cursor.close()
    return {
        "title": f"EXECUTEMANY ({row_count} rows)",
        "wall_ms": wall_ms,
        "cpp": cpp,
        "py": py,
        "detail": f"{row_count} rows via executemany",
    }


def fetchall(conn, table, ctx) -> dict:
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table}")
    ctx.enable()
    t0 = time.perf_counter()
    rows = cursor.fetchall()
    wall_ms = (time.perf_counter() - t0) * 1000
    cpp, py = ctx.collect()
    cursor.close()
    return {
        "title": f"FETCHALL ({len(rows)} rows)",
        "wall_ms": wall_ms,
        "cpp": cpp,
        "py": py,
        "detail": f"Rows: {len(rows)}",
    }


def fetchone(conn, table, ctx, row_count: int = FETCHONE_ROWS) -> dict:
    cursor = conn.cursor()
    cursor.execute(f"SELECT TOP {row_count} * FROM {table}")
    ctx.enable()
    t0 = time.perf_counter()
    count = 0
    while True:
        row = cursor.fetchone()
        if row is None:
            break
        count += 1
    wall_ms = (time.perf_counter() - t0) * 1000
    cpp, py = ctx.collect()
    cursor.close()
    return {
        "title": f"FETCHONE ({count} rows)",
        "wall_ms": wall_ms,
        "cpp": cpp,
        "py": py,
        "detail": f"Rows: {count}",
    }


def fetchmany(conn, table, ctx, batch_size: int = FETCHMANY_SIZE) -> dict:
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table}")
    ctx.enable()
    t0 = time.perf_counter()
    total = 0
    while True:
        batch = cursor.fetchmany(batch_size)
        if not batch:
            break
        total += len(batch)
    wall_ms = (time.perf_counter() - t0) * 1000
    cpp, py = ctx.collect()
    cursor.close()
    return {
        "title": f"FETCHMANY ({total} rows, batch={batch_size})",
        "wall_ms": wall_ms,
        "cpp": cpp,
        "py": py,
        "detail": f"Rows: {total}, Batch size: {batch_size}",
    }


def commit_rollback(conn, ctx, count: int = COMMIT_ROLLBACK_COUNT) -> dict:
    conn.autocommit = False
    cursor = conn.cursor()
    ctx.enable()
    t0 = time.perf_counter()
    for _ in range(count):
        cursor.execute("SELECT 1")
        conn.commit()
    for _ in range(count):
        cursor.execute("SELECT 1")
        conn.rollback()
    wall_ms = (time.perf_counter() - t0) * 1000
    cpp, py = ctx.collect()
    cursor.close()
    return {
        "title": f"COMMIT/ROLLBACK ({count} each)",
        "wall_ms": wall_ms,
        "cpp": cpp,
        "py": py,
        "detail": f"{count} commits + {count} rollbacks",
    }


def fetch_arrow(conn, table, ctx) -> dict:
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table}")
    ctx.enable()
    t0 = time.perf_counter()
    try:
        batch = cursor.fetch_arrow(size=ROW_COUNT)
        wall_ms = (time.perf_counter() - t0) * 1000
        cpp, py = ctx.collect()
        row_count = batch.num_rows if batch else 0
        cursor.close()
        return {
            "title": f"FETCH ARROW ({row_count} rows)",
            "wall_ms": wall_ms,
            "cpp": cpp,
            "py": py,
            "detail": f"Arrow rows: {row_count}",
        }
    except Exception as e:
        ctx.collect()  # drain counters
        cursor.close()
        return {
            "title": "FETCH ARROW",
            "wall_ms": 0,
            "cpp": None,
            "py": None,
            "detail": f"Skipped: {e}",
        }


def insertmanyvalues(
    conn, ctx, rows_per_batch: int = IMV_ROWS_PER_BATCH, total_rows: int = IMV_TOTAL_ROWS
) -> dict:
    """SQLAlchemy insertmanyvalues pattern: batched multi-row INSERT via cursor.execute()."""
    num_batches = total_rows // rows_per_batch
    params_per_call = rows_per_batch * 2

    cursor = conn.cursor()
    cursor.execute(
        "IF OBJECT_ID('tempdb..#imv_bench', 'U') IS NOT NULL DROP TABLE #imv_bench;"
        "CREATE TABLE #imv_bench (id INT, name VARCHAR(50))"
    )
    conn.commit()

    sql = "INSERT INTO #imv_bench (id, name) VALUES " + ",".join(["(?, ?)"] * rows_per_batch)
    params = []
    for i in range(rows_per_batch):
        params.extend([i, f"user_{i:06d}"])

    ctx.enable()
    t0 = time.perf_counter()
    for _ in range(num_batches):
        cursor.execute(sql, params)
    conn.commit()
    wall_ms = (time.perf_counter() - t0) * 1000
    cpp, py = ctx.collect()
    actual = num_batches * rows_per_batch
    rps = actual / (wall_ms / 1000) if wall_ms > 0 else 0
    cursor.close()
    return {
        "title": f"INSERTMANYVALUES ({actual:,} rows, {params_per_call} params/call)",
        "wall_ms": wall_ms,
        "cpp": cpp,
        "py": py,
        "detail": f"{actual:,} rows via {num_batches} execute() calls ({rps:,.0f} rows/s)",
    }


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

# Maps scenario name -> (function, needs_table)
SCENARIOS: dict[str, tuple] = {
    "connect": (connect, False),
    "select": (execute_select, True),
    "insert": (execute_insert, True),
    "executemany": (executemany, True),
    "fetchall": (fetchall, True),
    "fetchone": (fetchone, True),
    "fetchmany": (fetchmany, True),
    "commit_rollback": (commit_rollback, False),
    "arrow": (fetch_arrow, True),
    "insertmanyvalues": (insertmanyvalues, False),
}
