"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.

Regression tests for issue #671: enabling DEBUG logging via ``setup_logging()``
and then opening connections / executing statements from several threads at once
permanently deadlocked the process at 0% CPU.

Native ``LOG()`` acquires the GIL to route records through Python's ``logging``.
Several native paths did that while holding a native mutex (the connection-pool
mutexes, the per-connection child-handle mutex, the logger's own mutex) or the
env-handle static-init guard. A thread holding the GIL and then blocking on one
of those native locks closed the cycle. The trigger is DEBUG logging + more than
one thread; logging off, or a single thread, never deadlocks.

These tests assert a binary property (the concurrent, DEBUG-logged workload runs
to completion), not a timing threshold, so they are stable across hardware. The
workload runs in a child process so the parent can enforce a wall-clock timeout
and kill it: a GIL/native-mutex deadlock freezes the interpreter and cannot be
interrupted from within the same process. Running it out-of-process also gives
each run a fresh logging singleton so it never leaks into the rest of the suite.
"""

import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor

import pytest

import mssql_python
from mssql_python import connect


@pytest.fixture(scope="module")
def conn_str():
    conn_str = os.getenv("DB_CONNECTION_STRING")
    if not conn_str:
        pytest.skip("DB_CONNECTION_STRING environment variable not set")
    return conn_str


def _run_workload(conn_str, workers, iters, log_file, timeout):
    """Run the DEBUG-logged concurrent workload (this file's ``__main__`` block)
    in a child process and fail if it deadlocks or errors.

    The child imports mssql_python the same way this process did (the parent's
    sys.path is forwarded via PYTHONPATH), and its configuration is passed via
    the environment so the connection string never appears in the process list.
    """
    env = dict(os.environ)
    env["DB_CONNECTION_STRING"] = conn_str
    env["MSSQL671_WORKERS"] = str(workers)
    env["MSSQL671_ITERS"] = str(iters)
    env["MSSQL671_LOG_FILE"] = log_file
    env["PYTHONPATH"] = os.pathsep.join(p for p in sys.path if p)

    try:
        proc = subprocess.run(
            [sys.executable, os.path.abspath(__file__)],
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        # subprocess.run kills the child on timeout; not finishing means the
        # workload deadlocked (issue #671 regression).
        pytest.fail(
            f"{workers} threads x {iters} iters with DEBUG logging did not finish "
            f"within {timeout}s - the connection/logging path deadlocked (#671)."
        )

    assert (
        proc.returncode == 0 and "WORKLOAD_OK" in proc.stdout
    ), f"workload exited {proc.returncode}\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"


def test_debug_logging_concurrent_connect_does_not_deadlock(conn_str, tmp_path):
    """Two threads opening connections and executing with DEBUG logging on must
    not deadlock. Completes in a few seconds on a healthy driver; the timeout
    only elapses if the deadlock regresses."""
    _run_workload(conn_str, workers=2, iters=50, log_file=str(tmp_path / "trace.log"), timeout=60)


@pytest.mark.stress
def test_debug_logging_concurrent_connect_does_not_deadlock_stress(conn_str, tmp_path):
    """Sustained high-concurrency version of the guard above."""
    _run_workload(
        conn_str, workers=16, iters=100, log_file=str(tmp_path / "trace.log"), timeout=300
    )


def _run_child_workload():
    """Child-process entry point (invoked by ``_run_workload``, never collected
    by pytest). Enables DEBUG logging, then hammers connect/execute/close from
    ``MSSQL671_WORKERS`` threads."""
    conn_str = os.environ["DB_CONNECTION_STRING"]
    workers = int(os.environ["MSSQL671_WORKERS"])
    iters = int(os.environ["MSSQL671_ITERS"])
    mssql_python.setup_logging(output="file", log_file_path=os.environ["MSSQL671_LOG_FILE"])

    def worker(_):
        for _ in range(iters):
            conn = connect(conn_str, autocommit=True)
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            conn.close()

    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(worker, range(workers)))
    print("WORKLOAD_OK")


if __name__ == "__main__":
    _run_child_workload()
