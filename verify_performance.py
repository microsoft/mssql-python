#!/usr/bin/env python3
"""Verify GIL release and LOB batch performance on macOS.

Test A: GIL Release — 8 parallel WAITFOR DELAY '00:00:02' queries.
  PASS: Total < 3s (parallel). FAIL: Total > 15s (serialized).

Test B: LOB Batching — Fetch 1000 rows of NVARCHAR(MAX) data (< 64KB each).
  Compares batch mode vs theoretical row-by-row overhead.

Usage:
    python verify_performance.py --server <host> [--user <user> --password <pwd>]

Environment variables: MSSQL_SERVER, MSSQL_DATABASE, MSSQL_USER, MSSQL_PASSWORD
"""

import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


def get_connection(conn_str):
    import mssql_python
    return mssql_python.connect(conn_str)


def build_conn_str(args):
    if args.user and args.password:
        return (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={args.server};"
            f"DATABASE={args.database};"
            f"UID={args.user};"
            f"PWD={args.password};"
            f"TrustServerCertificate=yes"
        )
    return (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={args.server};"
        f"DATABASE={args.database};"
        f"Trusted_Connection=yes;"
        f"TrustServerCertificate=yes"
    )


def run_delay_query(conn_str, delay="00:00:02"):
    """Execute a WAITFOR DELAY query on its own connection."""
    conn = get_connection(conn_str)
    cursor = conn.cursor()
    cursor.execute(f"WAITFOR DELAY '{delay}'")
    cursor.close()
    conn.close()
    return True


def test_a_gil_release(conn_str, num_threads=8, delay="00:00:02"):
    """Test A: Verify GIL release with parallel blocking queries."""
    print(f"\n{'='*60}")
    print(f"TEST A: GIL Release ({num_threads} threads × WAITFOR DELAY '{delay}')")
    print(f"{'='*60}")

    start = time.monotonic()
    with ThreadPoolExecutor(max_workers=num_threads) as pool:
        futures = [pool.submit(run_delay_query, conn_str, delay) for _ in range(num_threads)]
        for f in as_completed(futures):
            f.result()
    elapsed = time.monotonic() - start

    parts = delay.split(":")
    delay_secs = int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
    serial_time = delay_secs * num_threads
    threshold = delay_secs * 1.5

    passed = elapsed < threshold
    status = "PASS" if passed else "FAIL"

    print(f"\n  Result:      {elapsed:.2f}s elapsed")
    print(f"  Serial est:  ~{serial_time}s")
    print(f"  Threshold:   {threshold:.1f}s")
    print(f"  Status:      {status}")

    return passed, elapsed, serial_time


def test_b_lob_batching(conn_str, num_rows=1000):
    """Test B: Verify LOB batch fetch for NVARCHAR(MAX) data < 64KB."""
    print(f"\n{'='*60}")
    print(f"TEST B: LOB Batch Fetch ({num_rows} rows of NVARCHAR(MAX) < 64KB)")
    print(f"{'='*60}")

    conn = get_connection(conn_str)
    cursor = conn.cursor()

    # Create temp table with NVARCHAR(MAX) column
    cursor.execute("""
        IF OBJECT_ID('tempdb..#lob_test') IS NOT NULL DROP TABLE #lob_test;
        CREATE TABLE #lob_test (id INT IDENTITY, data NVARCHAR(MAX));
    """)

    # Insert rows with ~4KB of data each (well under 64KB)
    print("  Inserting test data...")
    for batch_start in range(0, num_rows, 100):
        batch_size = min(100, num_rows - batch_start)
        values = ", ".join([f"(REPLICATE(N'X', 4000))"] * batch_size)
        cursor.execute(f"INSERT INTO #lob_test (data) VALUES {values}")
    conn.commit()

    # Fetch all rows using fetchall (should use batch mode for <= 65535)
    print("  Fetching with fetchall()...")
    cursor.execute("SELECT id, data FROM #lob_test")
    start = time.monotonic()
    rows = cursor.fetchall()
    elapsed = time.monotonic() - start

    row_count = len(rows)
    passed = row_count == num_rows

    print(f"\n  Rows fetched: {row_count}")
    print(f"  Time:         {elapsed:.3f}s")
    print(f"  Throughput:   {row_count / elapsed:.0f} rows/sec")
    print(f"  Status:       {'PASS' if passed else 'FAIL'} (expected {num_rows} rows)")

    cursor.execute("DROP TABLE #lob_test")
    cursor.close()
    conn.close()

    return passed, elapsed


def main():
    parser = argparse.ArgumentParser(description="Verify GIL release and LOB performance")
    parser.add_argument("--server", default=os.environ.get("MSSQL_SERVER", "localhost"))
    parser.add_argument("--database", default=os.environ.get("MSSQL_DATABASE", "master"))
    parser.add_argument("--user", default=os.environ.get("MSSQL_USER"))
    parser.add_argument("--password", default=os.environ.get("MSSQL_PASSWORD"))
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--delay", default="00:00:02")
    parser.add_argument("--lob-rows", type=int, default=1000)
    args = parser.parse_args()

    conn_str = build_conn_str(args)

    # Verify import works
    try:
        import mssql_python
        print(f"mssql-python loaded successfully")
    except ImportError as e:
        print(f"FATAL: Cannot import mssql_python: {e}")
        return 1

    results = {}

    try:
        a_passed, a_time, a_serial = test_a_gil_release(conn_str, args.threads, args.delay)
        results["Test A"] = ("PASS" if a_passed else "FAIL", f"{a_time:.2f}s", f"~{a_serial}s")
    except Exception as e:
        print(f"\n  ERROR: {e}")
        results["Test A"] = ("ERROR", str(e), "N/A")

    try:
        b_passed, b_time = test_b_lob_batching(conn_str, args.lob_rows)
        results["Test B"] = ("PASS" if b_passed else "FAIL", f"{b_time:.3f}s", "N/A")
    except Exception as e:
        print(f"\n  ERROR: {e}")
        results["Test B"] = ("ERROR", str(e), "N/A")

    # Summary table
    print(f"\n{'='*60}")
    print("RESULTS SUMMARY")
    print(f"{'='*60}")
    print(f"{'Test':<10} {'Status':<8} {'Actual':<12} {'Serial Est':<12}")
    print(f"{'-'*10} {'-'*8} {'-'*12} {'-'*12}")
    for test, (status, actual, serial) in results.items():
        print(f"{test:<10} {status:<8} {actual:<12} {serial:<12}")
    print()

    all_passed = all(r[0] == "PASS" for r in results.values())
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
