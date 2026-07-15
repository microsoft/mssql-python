"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.

Regression tests for issue #671: enabling DEBUG logging via ``setup_logging()``
and then opening connections / executing statements from multiple threads
concurrently permanently deadlocked the process at 0% CPU.

Root cause was a lock-order inversion: native ``LOG()`` acquires the GIL to route
records through Python's ``logging``, and several native code paths did that while
holding a native mutex (connection-pool mutexes, the per-connection child-handle
mutex, the logger's own mutex) or the env-handle static-init guard. A thread that
held the GIL and then blocked on one of those native locks completed the cycle.

These tests assert a binary correctness property (the concurrent, DEBUG-logged
workload runs to completion), not a performance threshold, so they are stable
across hardware.

The workload runs in a **separate process** (``_issue671_deadlock_workload.py``)
so the parent can enforce a wall-clock timeout and kill it. A GIL/native-mutex
deadlock freezes the interpreter and cannot be interrupted reliably from within
the same process, so an in-process (thread/signal) timeout would itself hang.
Isolating in a child process also gives every run a fresh logging singleton, so
enabling DEBUG logging here never leaks into the rest of the suite.
"""

import os
import subprocess
import sys
import tempfile

import pytest

_WORKLOAD = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_issue671_deadlock_workload.py")


def _run_workload(conn_str, workers, iters, timeout):
    """Run the concurrent DEBUG-logging workload in a child process.

    Returns nothing on success; calls pytest.fail on deadlock (timeout) or on a
    non-zero exit. Propagates the parent's import paths via PYTHONPATH so the
    child imports the same mssql_python the test suite uses (installed or
    in-place build), without any sys.path juggling in the workload.
    """
    log_dir = tempfile.mkdtemp(prefix="mssql671_")
    env = dict(os.environ)
    env["DB_CONNECTION_STRING"] = conn_str
    env["MSSQL671_WORKERS"] = str(workers)
    env["MSSQL671_ITERS"] = str(iters)
    env["MSSQL671_LOG_FILE"] = os.path.join(log_dir, "trace.log")
    env["PYTHONPATH"] = os.pathsep.join(p for p in sys.path if p)

    try:
        proc = subprocess.run(
            [sys.executable, _WORKLOAD],
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        # subprocess.run kills the child on timeout. A timeout here means the
        # workload never returned == the deadlock reproduced.
        pytest.fail(
            "issue #671 regression: concurrent DEBUG-logged workload did not "
            "finish within {}s ({} workers x {} iters) - likely deadlocked.\n"
            "stdout so far:\n{}\nstderr so far:\n{}".format(
                timeout, workers, iters, exc.stdout or "", exc.stderr or ""
            )
        )
    finally:
        # Best-effort cleanup of the log directory.
        try:
            for name in os.listdir(log_dir):
                os.remove(os.path.join(log_dir, name))
            os.rmdir(log_dir)
        except OSError:
            pass

    if proc.returncode != 0 or "WORKLOAD_OK" not in proc.stdout:
        pytest.fail(
            "concurrent DEBUG-logged workload failed (rc={}).\n"
            "stdout:\n{}\nstderr:\n{}".format(proc.returncode, proc.stdout, proc.stderr)
        )


def test_concurrent_debug_logging_does_not_deadlock(conn_str):
    """Functional guard: DEBUG logging + concurrent connect/execute must not hang.

    Small and fast on a healthy driver (a few seconds); the generous timeout only
    ever elapses if the deadlock regresses.
    """
    if not conn_str:
        pytest.skip("DB_CONNECTION_STRING environment variable not set")
    _run_workload(conn_str, workers=8, iters=25, timeout=90)


@pytest.mark.stress
def test_concurrent_debug_logging_does_not_deadlock_stress(conn_str):
    """Stress guard: sustained high-concurrency DEBUG-logged load must not hang."""
    if not conn_str:
        pytest.skip("DB_CONNECTION_STRING environment variable not set")
    _run_workload(conn_str, workers=16, iters=100, timeout=300)
