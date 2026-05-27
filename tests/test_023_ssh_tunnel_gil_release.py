"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.

Functional regression test for issue #565
("SSH tunneling fails using paramiko+sshtunnel").

The bug: ``mssql_python.connect()`` hangs indefinitely when the SQL Server
is reached through an in-process Python TCP forwarder (e.g. ``paramiko`` +
``sshtunnel``). Right after ``SQLDriverConnect`` returns, the bindings
call ``SQLSetConnectAttr(SQL_ATTR_AUTOCOMMIT, OFF)`` **while still
holding the GIL**. That call performs a network round-trip; the
in-process forwarder thread cannot run (it needs the GIL to call
``sock.sendall``), so the driver waits forever for a reply that never
arrives — deadlock.

How this test works
-------------------
We can't probe for the deadlock from within the same Python interpreter:
once the worker thread is stuck inside C code holding the GIL, the
entire interpreter can no longer execute bytecode, so even an in-process
watchdog thread is starved. We therefore re-execute *this same file* as
a subprocess (its ``__main__`` block sets up the forwarder and runs the
connect) and apply a hard external watchdog (``Popen.communicate(timeout=...)``).

Without the fix the subprocess hangs forever inside
``Connection.setautocommit`` and is killed by the watchdog. With the fix
it completes in well under a second.

Linux-only because the issue is specific to non-Windows builds, and to
keep the functional suite deterministic across developer/CI machines.
"""

from __future__ import annotations

import os
import re
import socket
import subprocess
import sys
import threading

import pytest

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux"),
    reason="Issue #565 repro is Linux-only in the functional suite",
)


# Generous upper bound: with the fix, connect()+SELECT through the
# in-process forwarder completes in well under a second locally.
# Without the fix the subprocess hangs forever, so any small watchdog
# catches it; we use 30s to absorb slow CI jitter.
WATCHDOG_SECONDS = 30


# ---------------------------------------------------------------------------
# Helpers shared by the test (parsing only) and the subprocess entry point.
# ---------------------------------------------------------------------------


def _parse_server(conn_str: str) -> tuple[str, int] | None:
    """Extract (host, port) from the Server=... clause of an ODBC conn string."""
    m = re.search(r"(?i)(?:^|;)\s*Server\s*=\s*([^;]+)", conn_str)
    if not m:
        return None
    raw = m.group(1).strip()
    if ":" in raw and raw.lower().startswith(("tcp:", "np:", "lpc:")):
        raw = raw.split(":", 1)[1]
    if "," in raw:
        host, port = raw.split(",", 1)
    elif ":" in raw and raw.count(":") == 1:
        host, port = raw.split(":", 1)
    else:
        host, port = raw, "1433"
    try:
        return host.strip(), int(port.strip())
    except ValueError:
        return None


def _replace_server(conn_str: str, host: str, port: int) -> str:
    """Return ``conn_str`` with its Server=... clause rewritten to host,port."""
    return re.sub(
        r"(?i)(^|;)\s*Server\s*=\s*[^;]+",
        rf"\1Server={host},{port}",
        conn_str,
        count=1,
    )


# ---------------------------------------------------------------------------
# Subprocess entry point: in-process Python TCP forwarder + mssql_python
# connect()+SELECT through it. Only runs when this file is executed as a
# script (e.g. by the test below). Pytest collection ignores this block.
# ---------------------------------------------------------------------------


def _pipe(src: socket.socket, dst: socket.socket) -> None:
    """
    Forward bytes from ``src`` to ``dst`` until EOF.

    The ``dst.sendall(data)`` call below is a bound-method dispatch that
    requires the GIL. If another thread holds the GIL across a blocking
    ODBC network call, this loop never makes progress — that is the
    deadlock condition from issue #565 with paramiko + sshtunnel.
    """
    try:
        while True:
            data = src.recv(8192)
            if not data:
                break
            dst.sendall(data)
    except OSError:
        pass
    finally:
        for s in (src, dst):
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                s.close()
            except OSError:
                pass


def _start_forwarder(target: tuple[str, int]) -> tuple[str, int]:
    """Start an in-process TCP forwarder; return (bind_host, bind_port)."""
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(8)
    host, port = listener.getsockname()

    def acceptor() -> None:
        while True:
            try:
                client, _ = listener.accept()
            except OSError:
                return
            try:
                upstream = socket.create_connection(target, timeout=10)
            except OSError:
                client.close()
                continue
            threading.Thread(target=_pipe, args=(client, upstream), daemon=True).start()
            threading.Thread(target=_pipe, args=(upstream, client), daemon=True).start()

    threading.Thread(target=acceptor, daemon=True).start()
    return host, port


def _run_forwarded_connect_subprocess() -> int:
    """Body of the subprocess: connect through the forwarder and SELECT 1."""
    import mssql_python
    from mssql_python import connect

    base = os.environ["DB_CONNECTION_STRING"]
    target = _parse_server(base)
    if target is None:
        print("ERR: could not parse Server=... clause", file=sys.stderr)
        return 2

    fwd_host, fwd_port = _start_forwarder(target)

    # Disable client-side pooling so we always exercise a fresh
    # SQLDriverConnect + SQLSetConnectAttr(AUTOCOMMIT) sequence — the
    # latter is the call that hangs in the unfixed binary.
    mssql_python.pooling(enabled=False)

    tunneled = _replace_server(base, fwd_host, fwd_port)

    conn = connect(tunneled)
    try:
        cur = conn.cursor()
        try:
            cur.execute("SELECT 1")
            row = cur.fetchone()
        finally:
            cur.close()
    finally:
        conn.close()

    print(f"OK {tuple(row) if row is not None else None}", flush=True)
    return 0


def _run_forwarded_param_query_subprocess() -> int:
    """
    Subprocess body for the parametrized-query teardown regression from
    issue #565 (latest comment by @jschuba).

    Executes parametrized SELECTs through the in-process forwarder, then
    closes the connection. Before the GIL-release fix on the teardown
    handle-freeing path, ``conn.close()`` blocks indefinitely inside
    ``SQLFreeHandle`` (the server-side cursor opened by the parametrized
    SELECT triggers a network round-trip during STMT-handle teardown,
    while the GIL is held - starving the in-process forwarder thread).
    """
    import mssql_python
    from mssql_python import connect

    base = os.environ["DB_CONNECTION_STRING"]
    target = _parse_server(base)
    if target is None:
        print("ERR: could not parse Server=... clause", file=sys.stderr)
        return 2

    fwd_host, fwd_port = _start_forwarder(target)
    mssql_python.pooling(enabled=False)
    tunneled = _replace_server(base, fwd_host, fwd_port)

    conn = connect(tunneled)

    # Use Connection.execute (same flow as the issue report) so cursors are
    # not explicitly closed by the user - the deadlock occurs when conn.close()
    # cascades through the still-open server-side cursors.
    res = conn.execute("SELECT 1 as result WHERE 1=?", (1,))
    row_qmark = res.fetchall()

    res = conn.execute("SELECT 1 as result WHERE 1=%(parameter)s", {"parameter": 1})
    row_named = res.fetchall()

    # This is the call that hangs in the unfixed binary.
    conn.close()

    print(f"OK qmark={row_qmark} named={row_named}", flush=True)
    return 0


# ---------------------------------------------------------------------------
# The actual pytest test.
# ---------------------------------------------------------------------------


def _run_subprocess_scenario(
    scenario_env_value: str,
    expected_marker: bytes,
    failure_message: str,
) -> None:
    """Shared helper: spawn this file as a subprocess and apply the watchdog."""
    base_conn_str = os.getenv("DB_CONNECTION_STRING")
    if not base_conn_str:
        pytest.skip("DB_CONNECTION_STRING environment variable not set")

    if _parse_server(base_conn_str) is None:
        pytest.skip("Could not parse Server=host,port from DB_CONNECTION_STRING")

    env = os.environ.copy()
    env["DB_CONNECTION_STRING"] = base_conn_str
    env["PYTHONUNBUFFERED"] = "1"
    env["PYTHONPATH"] = os.pathsep.join(sys.path)
    env["MSSQL_PYTHON_TEST_565_SCENARIO"] = scenario_env_value

    proc = subprocess.Popen(
        [sys.executable, os.path.abspath(__file__)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        out, err = proc.communicate(timeout=WATCHDOG_SECONDS)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.communicate()
        pytest.fail(failure_message)

    assert proc.returncode == 0, (
        f"Subprocess exited with {proc.returncode}.\n"
        f"stdout:\n{out.decode(errors='replace')}\n"
        f"stderr:\n{err.decode(errors='replace')}"
    )
    assert expected_marker in out, (
        f"Unexpected subprocess output.\n"
        f"stdout:\n{out.decode(errors='replace')}\n"
        f"stderr:\n{err.decode(errors='replace')}"
    )


def test_connect_through_python_tcp_forwarder_does_not_deadlock():
    """
    Regression test for issue #565.

    Routes ``mssql_python.connect()`` through a pure-Python TCP forwarder
    in a subprocess and applies a hard watchdog. Before the
    connection-attribute GIL-release fix, the subprocess hangs forever
    inside ``SQLSetConnectAttr(SQL_ATTR_AUTOCOMMIT, OFF)`` (called by
    ``Connection.setautocommit`` immediately after ``SQLDriverConnect``
    returns); with the fix it completes in well under a second.
    """
    _run_subprocess_scenario(
        scenario_env_value="connect",
        expected_marker=b"OK (1,)",
        failure_message=(
            f"connect()+SELECT through in-process Python TCP forwarder did "
            f"not complete within {WATCHDOG_SECONDS}s — this is the issue "
            f"#565 deadlock (GIL held across SQLSetConnectAttr_AUTOCOMMIT)."
        ),
    )


def test_param_query_close_through_python_tcp_forwarder_does_not_deadlock():
    """
    Regression test for the parametrized-query teardown variant of issue
    #565 (latest comment by @jschuba on the reopened issue).

    Executes parametrized SELECTs through the in-process forwarder and
    then calls ``conn.close()``. Before the fix that releases the GIL
    around ``SQLFreeHandle`` / ``SQLFreeStmt`` in the handle-teardown
    path, ``conn.close()`` deadlocks inside ``SQLFreeHandle(SQL_HANDLE_STMT)``
    because the server-side cursor opened by the parametrized SELECT
    triggers a network round-trip during handle teardown while the GIL
    is held — starving the in-process forwarder thread.
    """
    _run_subprocess_scenario(
        scenario_env_value="param_close",
        expected_marker=b"OK qmark=[(1,)] named=[(1,)]",
        failure_message=(
            f"Parametrized-query teardown through in-process Python TCP "
            f"forwarder did not complete within {WATCHDOG_SECONDS}s — this "
            f"is the issue #565 deadlock variant (GIL held across "
            f"SQLFreeHandle/SQLFreeStmt during cursor/connection close)."
        ),
    )


# Subprocess entry point: when this file is run as a script (by the tests
# above), execute the requested scenario. Pytest collection ignores this
# block.
if __name__ == "__main__":
    scenario = os.environ.get("MSSQL_PYTHON_TEST_565_SCENARIO", "connect")
    if scenario == "param_close":
        sys.exit(_run_forwarded_param_query_subprocess())
    sys.exit(_run_forwarded_connect_subprocess())
