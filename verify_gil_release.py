#!/usr/bin/env python3
"""Verify GIL release by running parallel WAITFOR DELAY queries.

With GIL release, 8 parallel 2-second queries should complete in ~2s.
Without GIL release, they would take ~16s (serialized).

Usage:
    python verify_gil_release.py --server <host> --database <db> [--user <user> --password <pwd>]

If no arguments given, tries to use environment variables:
    MSSQL_SERVER, MSSQL_DATABASE, MSSQL_USER, MSSQL_PASSWORD
"""

import argparse
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


def run_delay_query(conn_str, delay="00:00:02"):
    """Execute a WAITFOR DELAY query on its own connection."""
    import mssql_python

    conn = mssql_python.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute(f"WAITFOR DELAY '{delay}'")
    cursor.close()
    conn.close()
    return True


def main():
    parser = argparse.ArgumentParser(description="Verify GIL release with parallel queries")
    parser.add_argument("--server", default=os.environ.get("MSSQL_SERVER", "localhost"))
    parser.add_argument("--database", default=os.environ.get("MSSQL_DATABASE", "master"))
    parser.add_argument("--user", default=os.environ.get("MSSQL_USER"))
    parser.add_argument("--password", default=os.environ.get("MSSQL_PASSWORD"))
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--delay", default="00:00:02")
    args = parser.parse_args()

    if args.user and args.password:
        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={args.server};"
            f"DATABASE={args.database};"
            f"UID={args.user};"
            f"PWD={args.password};"
            f"TrustServerCertificate=yes"
        )
    else:
        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={args.server};"
            f"DATABASE={args.database};"
            f"Trusted_Connection=yes;"
            f"TrustServerCertificate=yes"
        )

    print(f"Running {args.threads} parallel WAITFOR DELAY '{args.delay}' queries...")
    start = time.monotonic()

    with ThreadPoolExecutor(max_workers=args.threads) as pool:
        futures = [pool.submit(run_delay_query, conn_str, args.delay) for _ in range(args.threads)]
        for f in as_completed(futures):
            f.result()  # raises if failed

    elapsed = time.monotonic() - start
    print(f"Elapsed: {elapsed:.2f}s")

    # Parse delay to seconds
    parts = args.delay.split(":")
    delay_secs = int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
    serial_time = delay_secs * args.threads
    threshold = delay_secs * 1.5  # Allow 50% overhead

    if elapsed < threshold:
        print(f"PASS: Queries ran in parallel ({elapsed:.1f}s < {threshold:.1f}s threshold)")
        print(f"      Serial would have been ~{serial_time}s")
        return 0
    else:
        print(f"FAIL: Queries appear serialized ({elapsed:.1f}s >= {threshold:.1f}s)")
        print(f"      Expected ~{delay_secs}s with GIL release, got {elapsed:.1f}s")
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
