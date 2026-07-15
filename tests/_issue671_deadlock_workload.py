"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.

Standalone workload for the issue #671 deadlock regression tests.

This module is intentionally NOT named ``test_*`` so pytest does not collect
it. It is executed as a separate process by
``tests/test_025_logging_concurrency_deadlock.py``.

Why a separate process: issue #671 is a hard GIL/native-mutex deadlock. When it
triggers, the interpreter freezes at 0% CPU and cannot be interrupted from
within (a thread- or signal-based timeout in the same process is unreliable
because the main thread is stuck holding/awaiting the GIL). Running the workload
in a child process lets the parent test enforce a wall-clock timeout and kill it,
turning a would-be hang into a clean test failure.

Configuration comes from the environment:
    DB_CONNECTION_STRING  connection string (required)
    MSSQL671_WORKERS      number of worker threads (default 8)
    MSSQL671_ITERS        connect/execute cycles per worker (default 25)
    MSSQL671_LOG_FILE     absolute path for the DEBUG log file (.log/.txt/.csv)

On success it prints ``WORKLOAD_OK`` and exits 0. On any error it lets the
exception propagate (non-zero exit with a traceback on stderr).
"""

import os
import sys
from concurrent.futures import ThreadPoolExecutor

import mssql_python


def main() -> int:
    conn_str = os.environ["DB_CONNECTION_STRING"]
    workers = int(os.environ.get("MSSQL671_WORKERS", "8"))
    iters = int(os.environ.get("MSSQL671_ITERS", "25"))
    log_file = os.environ.get("MSSQL671_LOG_FILE")

    # The deadlock only reproduces with DEBUG logging enabled (that is what
    # routes native log records through Python's logging under the GIL).
    if log_file:
        mssql_python.setup_logging(output="file", log_file_path=log_file)
    else:
        mssql_python.setup_logging(output="file")

    def worker(n: int) -> int:
        for i in range(iters):
            conn = mssql_python.connect(conn_str, autocommit=True)
            cur = conn.cursor()
            cur.execute("SELECT @@SPID, {}, {}".format(n, i))
            cur.fetchone()
            cur.execute("SELECT name FROM sys.objects WHERE object_id < 50")
            cur.fetchall()
            cur.close()
            conn.close()
        return n

    with ThreadPoolExecutor(max_workers=workers) as pool:
        # Force every future to be evaluated so exceptions propagate.
        list(pool.map(worker, range(workers)))

    print("WORKLOAD_OK", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
