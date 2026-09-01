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


def _forward_pythonpath():
    """Build the PYTHONPATH used to forward the parent's import path to a child
    process, preserving every entry including an empty one.

    An empty ``sys.path`` entry means "the current working directory". A child
    launched as a script (``python /abs/path/to/this_file.py``) has ``sys.path[0]``
    set to the script's own directory, not the parent's cwd, so dropping the empty
    entry can leave the child unable to import the same local ``mssql_python`` the
    parent used. Mirrors the established pattern in
    ``test_023_ssh_tunnel_gil_release.py``. (#671 review follow-up)
    """
    return os.pathsep.join(sys.path)


@pytest.fixture(scope="module")
def conn_str():
    conn_str = os.getenv("DB_CONNECTION_STRING")
    if not conn_str:
        pytest.skip("DB_CONNECTION_STRING environment variable not set")
    return conn_str


def _run_workload(
    conn_str,
    workers,
    iters,
    log_file,
    timeout,
    scenario="concurrency",
    marker="WORKLOAD_OK",
):
    """Run one of this file's ``__main__`` child scenarios in a subprocess and
    fail if it deadlocks or errors.

    The child imports mssql_python the same way this process did (the parent's
    sys.path is forwarded verbatim via PYTHONPATH), and its configuration is
    passed via the environment so the connection string never appears in the
    process list.
    """
    env = dict(os.environ)
    env["DB_CONNECTION_STRING"] = conn_str
    env["MSSQL671_SCENARIO"] = scenario
    env["MSSQL671_WORKERS"] = str(workers)
    env["MSSQL671_ITERS"] = str(iters)
    env["MSSQL671_LOG_FILE"] = log_file
    env["PYTHONPATH"] = _forward_pythonpath()

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
            f"{scenario} workload ({workers} threads x {iters} iters) with DEBUG "
            f"logging did not finish within {timeout}s - the connection/logging "
            f"path deadlocked (#671)."
        )

    assert (
        proc.returncode == 0 and marker in proc.stdout
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


def test_child_pythonpath_forwards_cwd_entry():
    """Regression for the #671 review (connection.cpp deadlock PR): the child
    PYTHONPATH must forward the parent's import path verbatim, including an empty
    entry that stands for the current working directory. Filtering empty entries
    (the earlier ``if p`` form) drops cwd, so a script-launched child can fail to
    import the same local ``mssql_python`` the parent used. This asserts the
    empty entry survives, which is exactly what the filter used to remove."""
    saved = sys.path
    try:
        sys.path = ["", "/fake/site-packages", "/fake/repo-root"]
        forwarded = _forward_pythonpath().split(os.pathsep)
    finally:
        sys.path = saved
    assert "" in forwarded, (
        "empty (current-working-directory) sys.path entry must be preserved in the "
        "child PYTHONPATH; filtering it can break the child's local mssql_python import"
    )
    assert forwarded == ["", "/fake/site-packages", "/fake/repo-root"]


def test_debug_logging_pooled_connection_shutdown_exits_cleanly(conn_str, tmp_path):
    """Guard for the connection-teardown logging path with DEBUG logging on.

    The child opens and closes pooled connections, then exits without draining
    the pool. At interpreter shutdown the pooling ``atexit`` handler
    (``shutdown_pooling`` -> ``disable_pooling()`` -> ``closePools()``)
    disconnects the pooled physical connections, and ``disconnect()`` logs while
    doing so. This asserts that DEBUG-logged connection teardown at process exit
    completes cleanly and promptly; a hang or crash there trips the timeout.

    Note: this is a teardown smoke guard, not a strict revert-detector for the
    hasGil gating in connection.cpp. The pure GIL-less path (the static
    ConnectionPoolManager destructor running after Py_Finalize) is not
    deterministically reachable from Python because the atexit handler drains the
    pool with the GIL still held, before finalization.
    """
    _run_workload(
        conn_str,
        workers=1,
        iters=8,
        log_file=str(tmp_path / "trace.log"),
        timeout=60,
        scenario="shutdown",
        marker="SHUTDOWN_WORKLOAD_OK",
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


def _run_child_shutdown_workload():
    """Child-process entry point for the shutdown/teardown guard. Enables DEBUG
    logging, then opens and closes pooled connections. It returns without draining
    the pool, so the pooling ``atexit`` handler disconnects the pooled physical
    connections at interpreter shutdown while DEBUG logging is on. Reaching the
    exit marker and returning 0 is the assertion."""
    conn_str = os.environ["DB_CONNECTION_STRING"]
    iters = int(os.environ["MSSQL671_ITERS"])
    mssql_python.setup_logging(output="file", log_file_path=os.environ["MSSQL671_LOG_FILE"])

    for _ in range(iters):
        conn = connect(conn_str, autocommit=True)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        conn.close()  # returns the physical connection to the pool

    # Deliberately do NOT drain the pool here: let interpreter shutdown (the
    # pooling atexit handler, then process exit) disconnect the pooled
    # connections with DEBUG logging on.
    print("SHUTDOWN_WORKLOAD_OK", flush=True)


if __name__ == "__main__":
    _scenario = os.environ.get("MSSQL671_SCENARIO", "concurrency")
    if _scenario == "shutdown":
        _run_child_shutdown_workload()
    else:
        _run_child_workload()
