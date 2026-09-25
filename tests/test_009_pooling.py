# tests/test_009_pooling.py
"""
Connection Pooling Tests

This module contains all tests related to connection pooling functionality.
Tests cover basic pooling operations, pool management, cleanup, performance,
and edge cases including the pooling disable bug fix.

Test Categories:
- Basic pooling functionality and configuration
- Pool resource management (size limits, timeouts)
- Connection reuse and lifecycle
- Performance benefits verification
- Cleanup and disable operations (bug fix tests)
- Error handling and recovery scenarios
"""

import pytest
import os
import re
import subprocess
import sys
import textwrap
import time
import threading


def _run_in_subprocess(body: str, conn_str: str) -> None:
    """Run a test body in a fresh Python process.

    Some tests need to be the *first* to call ``pooling(...)`` in the
    process (the C++ ``enable_pooling`` is wrapped in ``std::call_once``
    so only the first call's max_size/idle_timeout take effect). Running
    them in a subprocess gives each a clean process state.

    The subprocess inherits the current ``DB_CONNECTION_STRING`` env var
    so the worker uses the same database. ``body`` must be a self-contained
    Python snippet that exits non-zero on failure (any uncaught assertion
    is fine).
    """
    env = os.environ.copy()
    env["DB_CONNECTION_STRING"] = conn_str
    proc = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(body)],
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    # Sentinel exit code 77 means the subprocess decided to skip
    # (e.g. the test prerequisite is unmet on this server, like missing
    # KILL permission). The reason is printed to stderr.
    if proc.returncode == 77:
        pytest.skip(proc.stderr.strip() or "Subprocess requested skip")
    if proc.returncode != 0:
        pytest.fail(
            "Subprocess test body failed\n"
            f"--- stdout ---\n{proc.stdout}\n"
            f"--- stderr ---\n{proc.stderr}"
        )


import statistics
from mssql_python import connect, pooling
from mssql_python.pooling import PoolingManager
import mssql_python


@pytest.fixture(autouse=True)
def reset_pooling_state():
    """Reset pooling state before each test to ensure clean test isolation."""
    yield
    # Cleanup after each test
    try:
        pooling(enabled=False)
        PoolingManager._reset_for_testing()
    except Exception:
        pass  # Ignore cleanup errors


# =============================================================================
# Basic Pooling Functionality Tests
# =============================================================================


def test_connection_pooling_basic(conn_str):
    """Test basic connection pooling functionality with multiple connections."""
    # Enable pooling with small pool size
    pooling(max_size=2, idle_timeout=5)
    conn1 = connect(conn_str)
    conn2 = connect(conn_str)
    assert conn1 is not None
    assert conn2 is not None
    try:
        conn3 = connect(conn_str)
        assert (
            conn3 is not None
        ), "Third connection failed — pooling is not working or limit is too strict"
        conn3.close()
    except Exception as e:
        print(f"Expected: Could not open third connection due to max_size=2: {e}")

    conn1.close()
    conn2.close()


def test_connection_pooling_reuse_spid(conn_str):
    """Test that connections are actually reused from the pool using SQL Server SPID."""
    # Enable pooling
    pooling(max_size=1, idle_timeout=30)

    # Create and close a connection
    conn1 = connect(conn_str)
    cursor1 = conn1.cursor()
    cursor1.execute("SELECT @@SPID")  # Get SQL Server process ID
    spid1 = cursor1.fetchone()[0]
    conn1.close()

    # Get another connection - should be the same one from pool
    conn2 = connect(conn_str)
    cursor2 = conn2.cursor()
    cursor2.execute("SELECT @@SPID")
    spid2 = cursor2.fetchone()[0]
    conn2.close()

    # The SPID should be the same, indicating connection reuse
    assert spid1 == spid2, "Connections not reused - different SPIDs"


def test_connection_pooling_isolation_level_reset(conn_str):
    """Test that pooling correctly resets session state for isolation level.

    This test verifies that when a connection is returned to the pool and then
    reused, the isolation level setting is reset to the default (READ COMMITTED)
    to prevent session state from leaking between connection usages.

    Bug Fix: Previously, SQL_ATTR_RESET_CONNECTION was used which does NOT reset
    the isolation level. Now we explicitly reset it to prevent state leakage.
    """
    # Enable pooling with small pool to ensure connection reuse
    pooling(enabled=True, max_size=1, idle_timeout=30)

    # Create first connection and set isolation level to SERIALIZABLE
    conn1 = connect(conn_str)

    # Set isolation level to SERIALIZABLE (non-default)
    conn1.set_attr(mssql_python.SQL_ATTR_TXN_ISOLATION, mssql_python.SQL_TXN_SERIALIZABLE)

    # Verify the isolation level was set
    cursor1 = conn1.cursor()
    cursor1.execute(
        "SELECT CASE transaction_isolation_level "
        "WHEN 0 THEN 'Unspecified' "
        "WHEN 1 THEN 'ReadUncommitted' "
        "WHEN 2 THEN 'ReadCommitted' "
        "WHEN 3 THEN 'RepeatableRead' "
        "WHEN 4 THEN 'Serializable' "
        "WHEN 5 THEN 'Snapshot' END AS isolation_level "
        "FROM sys.dm_exec_sessions WHERE session_id = @@SPID"
    )
    isolation_level_1 = cursor1.fetchone()[0]
    assert isolation_level_1 == "Serializable", f"Expected Serializable, got {isolation_level_1}"

    # Get SPID for verification of connection reuse
    cursor1.execute("SELECT @@SPID")
    spid1 = cursor1.fetchone()[0]

    # Close connection (return to pool)
    cursor1.close()
    conn1.close()

    # Get second connection from pool (should reuse the same connection)
    conn2 = connect(conn_str)

    # Check if it's the same connection (same SPID)
    cursor2 = conn2.cursor()
    cursor2.execute("SELECT @@SPID")
    spid2 = cursor2.fetchone()[0]

    # Verify connection was reused
    assert spid1 == spid2, "Connection was not reused from pool"

    # Check if isolation level is reset to default
    cursor2.execute(
        "SELECT CASE transaction_isolation_level "
        "WHEN 0 THEN 'Unspecified' "
        "WHEN 1 THEN 'ReadUncommitted' "
        "WHEN 2 THEN 'ReadCommitted' "
        "WHEN 3 THEN 'RepeatableRead' "
        "WHEN 4 THEN 'Serializable' "
        "WHEN 5 THEN 'Snapshot' END AS isolation_level "
        "FROM sys.dm_exec_sessions WHERE session_id = @@SPID"
    )
    isolation_level_2 = cursor2.fetchone()[0]

    # Verify isolation level is reset to default (READ COMMITTED)
    # This is the CORRECT behavior for connection pooling - we should reset
    # session state to prevent settings from one usage affecting the next
    assert isolation_level_2 == "ReadCommitted", (
        f"Isolation level was not reset! Expected 'ReadCommitted', got '{isolation_level_2}'. "
        f"This indicates session state leaked from the previous connection usage."
    )

    # Clean up
    cursor2.close()
    conn2.close()


def test_connection_pooling_speed(conn_str):
    """Test that connection pooling provides performance benefits over multiple iterations."""
    # Warm up to eliminate cold start effects
    for _ in range(3):
        conn = connect(conn_str)
        conn.close()

    # Disable pooling first
    pooling(enabled=False)

    # Test without pooling (multiple times)
    no_pool_times = []
    for _ in range(10):
        start = time.perf_counter()
        conn = connect(conn_str)
        conn.close()
        end = time.perf_counter()
        no_pool_times.append(end - start)

    # Enable pooling
    pooling(max_size=5, idle_timeout=30)

    # Test with pooling (multiple times)
    pool_times = []
    for _ in range(10):
        start = time.perf_counter()
        conn = connect(conn_str)
        conn.close()
        end = time.perf_counter()
        pool_times.append(end - start)

    # Use median times to reduce impact of outliers
    median_no_pool = statistics.median(no_pool_times)
    median_pool = statistics.median(pool_times)

    # Allow for some variance - pooling should be at least 30% faster on average
    improvement_threshold = 0.7  # Pool should be <= 70% of no-pool time

    print(f"No pool median: {median_no_pool:.6f}s")
    print(f"Pool median: {median_pool:.6f}s")
    print(f"Improvement ratio: {median_pool/median_no_pool:.2f}")

    assert (
        median_pool <= median_no_pool * improvement_threshold
    ), f"Expected pooling to be at least 30% faster. No-pool: {median_no_pool:.6f}s, Pool: {median_pool:.6f}s"


# =============================================================================
# Pool Resource Management Tests
# =============================================================================


def test_pool_exhaustion_max_size_1(conn_str):
    """Test pool exhaustion when max_size=1 and multiple concurrent connections are requested."""
    pooling(max_size=1, idle_timeout=30)
    conn1 = connect(conn_str)
    results = []

    def try_connect():
        try:
            conn2 = connect(conn_str)
            results.append("success")
            conn2.close()
        except Exception as e:
            results.append(str(e))

    # Start a thread that will attempt to get a second connection while the first is open
    t = threading.Thread(target=try_connect)
    t.start()
    t.join(timeout=2)
    conn1.close()

    # Depending on implementation, either blocks, raises, or times out
    assert results, "Second connection attempt did not complete"
    # If pool blocks, the thread may not finish until conn1 is closed, so allow both outcomes
    assert (
        results[0] == "success" or "pool" in results[0].lower() or "timeout" in results[0].lower()
    ), f"Unexpected pool exhaustion result: {results[0]}"


def test_pool_capacity_limit_and_overflow(conn_str):
    """Test that pool does not grow beyond max_size and handles overflow gracefully."""
    pooling(max_size=2, idle_timeout=30)
    conns = []
    try:
        # Open up to max_size connections
        conns.append(connect(conn_str))
        conns.append(connect(conn_str))
        # Try to open a third connection, which should fail or block
        overflow_result = []

        def try_overflow():
            try:
                c = connect(conn_str)
                overflow_result.append("success")
                c.close()
            except Exception as e:
                overflow_result.append(str(e))

        t = threading.Thread(target=try_overflow)
        t.start()
        t.join(timeout=2)
        assert overflow_result, "Overflow connection attempt did not complete"
        # Accept either block, error, or success if pool implementation allows overflow
        assert (
            overflow_result[0] == "success"
            or "pool" in overflow_result[0].lower()
            or "timeout" in overflow_result[0].lower()
        ), f"Unexpected pool overflow result: {overflow_result[0]}"
    finally:
        for c in conns:
            c.close()


def test_pool_release_overflow_disconnects_outside_mutex(conn_str):
    """Test that releasing a connection when pool is full disconnects it correctly.

    When a connection is returned to a pool that is already at max_size,
    the connection must be disconnected. This exercises the overflow path in
    ConnectionPool::release() (connection_pool.cpp) where should_disconnect
    is set and disconnect happens outside the mutex.

    With the current pool semantics, max_size limits total concurrent
    connections, so we acquire two connections with max_size=2, then shrink
    the pool to max_size=1 before returning them. The second close hits
    the overflow path.
    """
    pooling(max_size=2, idle_timeout=30)

    conn1 = connect(conn_str)
    conn2 = connect(conn_str)

    # Shrink idle capacity so first close fills the pool and second overflows
    pooling(max_size=1, idle_timeout=30)

    # Close conn1 — returned to the pool (pool now has 1 idle entry)
    conn1.close()

    # Close conn2 — pool is full (1 idle already), so this connection
    # must be disconnected rather than pooled (overflow path).
    conn2.close()

    # Verify the pool is still functional
    conn3 = connect(conn_str)
    cursor = conn3.cursor()
    cursor.execute("SELECT 1")
    assert cursor.fetchone()[0] == 1
    conn3.close()


def test_pool_idle_timeout_removes_connections(conn_str):
    """Test that idle_timeout removes connections from the pool after the timeout.

    Run in a subprocess so this test's pooling(idle_timeout=1) is the
    first call in the process — the C++ ``enable_pooling`` is wrapped in
    ``std::call_once``, so only the first call's settings take effect for
    the lifetime of the process.

    A bare SPID-inequality assertion is unreliable: SQL Server is free to
    reassign a recently-freed SPID to the next session. So we identify a
    session by the (SPID, login_time) tuple from sys.dm_exec_sessions —
    login_time has millisecond resolution and is unique per physical
    connection.
    """
    _run_in_subprocess(
        """
        import os, time
        from mssql_python import connect, pooling

        conn_str = os.environ["DB_CONNECTION_STRING"]
        pooling(max_size=2, idle_timeout=1)

        def session_identity(conn):
            cur = conn.cursor()
            cur.execute(
                "SELECT @@SPID, "
                "       (SELECT login_time FROM sys.dm_exec_sessions "
                "        WHERE session_id = @@SPID)"
            )
            spid, login_time = cur.fetchone()
            return (spid, login_time)

        c1 = connect(conn_str)
        id1 = session_identity(c1)
        c1.close()

        time.sleep(3)

        c2 = connect(conn_str)
        id2 = session_identity(c2)
        c2.close()

        assert id1 != id2, (
            f"Idle timeout did not remove connection from pool: "
            f"got the same session both times {id1}"
        )
        """,
        conn_str,
    )


def test_idle_identity_pool_is_evicted_by_later_acquire(conn_str):
    """Regression test for lazy eviction of idle identity pools.

    David's ask verbatim: "connect as A, wait past idle timeout, connect as B,
    assert A's pool is gone (today it won't be)."

    Distinct connection strings (differing APP name) key to distinct pools —
    the same mechanism identity-aware pooling uses (connStr + identity). The old
    ``canEvict()`` required ``_current_size == 0 && _pool.empty()``, so a pool
    holding a single *idle* connection (``_current_size == 1``) was never
    reclaimed: connecting as B would leave A's pool alive forever. The fix makes
    ``canEvict()`` evaluate the idle timeout, so acquiring B sweeps the
    aged-out pool A away.

    Rather than reading an internal pool count, this asserts the observable
    outcome: pool A's physical connection is disconnected on the server once it
    is evicted. A persistent observer connection watches for A's connection_id
    in sys.dm_exec_connections; the observer needs VIEW SERVER STATE to see a
    session other than its own, so the test skips gracefully where that is not
    granted.

    Run in a subprocess so this test's ``pooling(idle_timeout=1)`` is the first
    (and therefore effective) call in a clean process.
    """
    _run_in_subprocess(
        """
        import os, re, sys, time
        from mssql_python import connect, pooling

        base = os.environ["DB_CONNECTION_STRING"]
        # Two distinct pool keys against the same server. The pool key is derived
        # from the processed connection string, so switching the target database
        # (master vs tempdb -- both always present) yields two separate pools,
        # the same way two different identities would.
        if re.search(r"(?i)database=", base):
            conn_a = re.sub(r"(?i)database=[^;]*", "Database=master", base)
            conn_b = re.sub(r"(?i)database=[^;]*", "Database=tempdb", base)
        else:
            trimmed = base.rstrip(";")
            conn_a = trimmed + ";Database=master"
            conn_b = trimmed + ";Database=tempdb"

        def phys_id(conn):
            # This connection's own physical connection_id. Reading
            # sys.dm_exec_connections requires VIEW SERVER STATE -- and on SQL
            # Server 2022+ the more granular VIEW SERVER PERFORMANCE STATE --
            # even for one's own session on some server configs. Where the login
            # lacks it, skip (exit 77) rather than fail with a permission error.
            cur = conn.cursor()
            try:
                cur.execute(
                    "SELECT CONVERT(nvarchar(36), connection_id) "
                    "FROM sys.dm_exec_connections "
                    "WHERE session_id = @@SPID AND parent_connection_id IS NULL"
                )
            except Exception as exc:
                if "permission" in str(exc).lower():
                    sys.stderr.write(
                        "requires VIEW SERVER STATE / VIEW SERVER PERFORMANCE "
                        "STATE to read sys.dm_exec_connections"
                    )
                    sys.exit(77)
                raise
            row = cur.fetchone()
            return row[0] if row else None

        def alive(observer, pid):
            # pid is a server-generated GUID; validate before inlining it so the
            # string interpolation cannot be an injection vector.
            assert re.fullmatch(r"[0-9A-Fa-f-]{36}", pid), pid
            cur = observer.cursor()
            cur.execute(
                "SELECT COUNT(*) FROM sys.dm_exec_connections "
                "WHERE CONVERT(nvarchar(36), connection_id) = '" + pid + "'"
            )
            return cur.fetchone()[0]

        pooling(max_size=2, idle_timeout=1)

        # Identity A: open, note its physical connection, then return it -> pool A
        # now holds one idle connection (_current_size == 1). Under the old
        # canEvict() this pool -- and this server session -- could never be
        # reclaimed.
        a = connect(conn_a)
        a_id = phys_id(a)

        # Persistent observer on a different pool key. It stays checked out for
        # the whole test so it can watch A's session on the server.
        obs = connect(conn_b)

        if alive(obs, a_id) != 1:
            # The observer cannot see A's session -> this login lacks VIEW SERVER
            # STATE. Skip rather than report a false negative (exit 77).
            sys.stderr.write(
                "requires VIEW SERVER STATE to observe pool eviction on the server"
            )
            sys.exit(77)

        a.close()
        assert alive(obs, a_id) == 1, "pool A's idle connection should still be open"

        # Let pool A's idle connection age past the 1s idle timeout.
        time.sleep(3)

        # Acquiring on a different key triggers the manager's lazy eviction
        # sweep, which must now reclaim the aged-out pool A and disconnect it.
        trigger = connect(conn_b)
        trigger.cursor().execute("SELECT 1")
        trigger.close()

        # The sweep disconnects synchronously, but give the server a moment to
        # tear the session down before asserting it is gone.
        deadline = time.time() + 5
        while alive(obs, a_id) != 0 and time.time() < deadline:
            time.sleep(0.2)
        assert alive(obs, a_id) == 0, (
            "pool A was not evicted: its physical connection is still open on "
            "the server. Under the old canEvict() the idle pool would linger "
            "forever."
        )

        obs.close()
        """,
        conn_str,
    )


def test_pool_full_raises_when_max_size_reached(conn_str):
    """Acquiring past ``max_size`` on a busy pool raises rather than blocking.

    Covers the "pool size limit reached" throw in ``ConnectionPool::acquire``:
    with ``max_size == 1`` and the single slot already checked out, a second
    acquire on the same key finds the pool empty and no free capacity, so it
    raises immediately (the pool never blocks waiting for a return).

    Run in a subprocess so ``pooling(max_size=1, ...)`` is the first (and thus
    effective) call in a clean process — the C++ pool config is locked in via
    ``std::call_once`` for the lifetime of a process.
    """
    _run_in_subprocess(
        """
        import os, sys
        from mssql_python import connect, pooling

        conn_str = os.environ["DB_CONNECTION_STRING"]
        pooling(max_size=1, idle_timeout=30)

        # Fill the single slot and keep it checked out.
        held = connect(conn_str)
        held.cursor().execute("SELECT 1")

        # A second acquire on the same pool key has no free capacity and no
        # idle connection to hand back -> it must raise, not hang.
        try:
            second = connect(conn_str)
        except Exception as exc:
            assert "pool size limit reached" in str(exc).lower() or "pool" in str(exc).lower(), (
                f"unexpected error for a full pool: {exc!r}"
            )
        else:
            second.close()
            held.close()
            raise AssertionError("expected a full-pool error, but the acquire succeeded")

        held.close()
        """,
        conn_str,
    )


def test_checked_out_pool_is_not_evicted(conn_str):
    """A pool with a checked-out connection is never swept, even when aged out.

    Covers the "checked out" guard in ``ConnectionPool::canEvict``: reserved
    capacity beyond what is sitting idle in the pool means a caller still holds a
    connection, so the eviction sweep must leave that pool alone regardless of
    the idle timeout.

    Asserts the observable consequence rather than an internal pool count: if the
    checked-out pool survives the sweep, the connection returned to it is reused
    on the next same-key acquire (same physical connection_id). Had the pool been
    wrongly evicted, returning the connection would orphan-close it and the next
    acquire would open a brand-new one (a different connection_id).

    Run in a subprocess so ``pooling(idle_timeout=1)`` is the effective config.
    """
    _run_in_subprocess(
        """
        import os, re, sys, time
        from mssql_python import connect, pooling

        base = os.environ["DB_CONNECTION_STRING"]
        # Two distinct pool keys against the same server (see the idle-eviction
        # test): switching the target database yields two separate pools.
        if re.search(r"(?i)database=", base):
            conn_a = re.sub(r"(?i)database=[^;]*", "Database=master", base)
            conn_b = re.sub(r"(?i)database=[^;]*", "Database=tempdb", base)
        else:
            trimmed = base.rstrip(";")
            conn_a = trimmed + ";Database=master"
            conn_b = trimmed + ";Database=tempdb"

        def phys_id(conn):
            # Reading sys.dm_exec_connections requires VIEW SERVER STATE -- and
            # on SQL Server 2022+ the more granular VIEW SERVER PERFORMANCE
            # STATE -- even for one's own session on some server configs. Where
            # the login lacks it, skip (exit 77) rather than fail.
            cur = conn.cursor()
            try:
                cur.execute(
                    "SELECT CONVERT(nvarchar(36), connection_id) "
                    "FROM sys.dm_exec_connections "
                    "WHERE session_id = @@SPID AND parent_connection_id IS NULL"
                )
            except Exception as exc:
                if "permission" in str(exc).lower():
                    sys.stderr.write(
                        "requires VIEW SERVER STATE / VIEW SERVER PERFORMANCE "
                        "STATE to read sys.dm_exec_connections"
                    )
                    sys.exit(77)
                raise
            row = cur.fetchone()
            return row[0] if row else None

        pooling(max_size=2, idle_timeout=1)

        # Identity A: open and KEEP OPEN. The connection is checked out, so pool
        # A's reserved capacity exceeds its idle contents (_current_size == 1,
        # _pool empty), so canEvict() must refuse to reclaim it.
        a = connect(conn_a)
        a_id = phys_id(a)

        # Age past the 1s idle timeout, then acquire on a different key to run
        # the manager's eviction sweep while A is still checked out.
        time.sleep(3)
        trigger = connect(conn_b)
        trigger.cursor().execute("SELECT 1")
        trigger.close()

        # Return A to pool A (which must have survived the sweep) and reacquire
        # on the same key: a surviving pool hands the very same physical
        # connection back.
        a.close()
        a2 = connect(conn_a)
        a2_id = phys_id(a2)
        assert a2_id == a_id, (
            "checked-out pool A was wrongly evicted: the reacquire opened a new "
            "physical connection (" + str(a2_id) + ") instead of reusing the "
            "pooled one (" + str(a_id) + ")."
        )

        a2.close()
        """,
        conn_str,
    )


# =============================================================================
# Error Handling and Recovery Tests
# =============================================================================


def test_pool_removes_invalid_connections(conn_str):
    """Pool must replace a pooled connection whose server-side session has died.

    Run in a subprocess so this test does not pollute the in-process pool
    state for sibling tests (KILL leaves dead pool entries that survive
    Python-side teardown because the C++ pool config is locked in for the
    lifetime of the process via ``std::call_once``).

    Simulates the realistic failure mode (DBA KILL, failover, server-side
    idle timeout) by:
      1. Opening two connections concurrently (distinct physical sessions)
         in autocommit mode.
      2. Using one to KILL the other's server-side session out-of-band.
      3. Returning both to the pool.
      4. Re-acquiring repeatedly: every connection must work and the
         killed SPID must never reappear.

    Only public APIs are used.
    """
    _run_in_subprocess(
        """
        import os
        import time
        from mssql_python import connect, pooling

        conn_str = os.environ["DB_CONNECTION_STRING"]
        pooling(max_size=2, idle_timeout=30)

        def session_identity(conn):
            cur = conn.cursor()
            cur.execute(
                "SELECT @@SPID, "
                "       (SELECT login_time FROM sys.dm_exec_sessions "
                "        WHERE session_id = @@SPID)"
            )
            spid, login_time = cur.fetchone()
            return (spid, login_time)

        # Step 1: two distinct, autocommit connections. Autocommit avoids
        # the implicit rollback in Connection.close(), which would
        # otherwise fail on the killed session and leak its pool slot.
        victim = connect(conn_str)
        admin = connect(conn_str)
        victim.autocommit = True
        admin.autocommit = True

        victim_id = session_identity(victim)
        admin_id = session_identity(admin)
        assert victim_id != admin_id, (
            "Pool handed out the same physical session to two concurrent "
            "acquires"
        )
        victim_spid = victim_id[0]

        # Step 2: admin KILLs the victim's session. Requires server
        # permission (ALTER ANY CONNECTION or sysadmin); on hosted/CI
        # databases the test login often lacks it, so skip gracefully.
        try:
            admin.cursor().execute(f"KILL {victim_spid}")
        except Exception as e:
            msg = str(e)
            if "permission" in msg.lower() or "KILL" in msg:
                import sys as _sys
                print(
                    f"Skipping: KILL not permitted for this login: {msg}",
                    file=_sys.stderr,
                )
                victim.close()
                admin.close()
                _sys.exit(77)
            raise

        # KILL is processed asynchronously on the server, but we don't
        # need to wait for it here. The test's correctness contract is
        # "the killed (SPID, login_time) must never reappear in
        # subsequent acquires." Any session that gets handed back
        # later — whether the same SPID reused by the server or a
        # transparently-reconnected one — necessarily has a different
        # login_time, so the identity check below catches the only
        # failure mode that matters.

        # Step 3: return both to the pool.
        victim.close()
        admin.close()

        # Step 4: re-acquire from the pool. Each must be working; the
        # killed *physical session* (SPID, login_time) must never come
        # back. SQL Server is free to reassign the SPID number to a new
        # session, so SPID alone is not a reliable identity.
        seen_ids = set()
        for _ in range(4):
            c = connect(conn_str)
            try:
                seen_ids.add(session_identity(c))
                assert c.cursor().execute("SELECT 1").fetchone()[0] == 1, (
                    "Pool handed out an unusable connection"
                )
            finally:
                c.close()
        assert victim_id not in seen_ids, (
            f"Pool returned the killed session {victim_id}; "
            f"saw sessions {seen_ids}"
        )
        """,
        conn_str,
    )


def test_pool_recovery_after_failed_connection(conn_str):
    """Test that the pool recovers after a failed connection attempt."""
    pooling(max_size=1, idle_timeout=30)
    # First, try to connect with a bad password (should fail).
    # Match the password keyword case-insensitively since ODBC accepts any case.
    bad_conn_str = re.sub(
        r"(?i)(\b(?:pwd|password)\s*=)([^;]*)",
        r"\1wrongpassword",
        conn_str,
        count=1,
    )
    if bad_conn_str == conn_str:
        pytest.skip("No password found in connection string to modify")
    with pytest.raises(Exception):
        connect(bad_conn_str)
    # Now, connect with the correct string and ensure it works
    conn = connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    result = cursor.fetchone()
    assert result is not None and result[0] == 1, "Pool did not recover after failed connection"
    conn.close()


# =============================================================================
# Pooling Disable Bug Fix Tests
# =============================================================================


def test_pooling_disable_without_hang(conn_str):
    """Test that pooling(enabled=False) does not hang after connections are created (Bug Fix Test)."""
    print("Testing pooling disable without hang...")

    # Enable pooling
    pooling(enabled=True)

    # Create and use a connection
    conn = connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    result = cursor.fetchone()
    assert result[0] == 1, "Basic query failed"
    conn.close()

    # This should not hang (was the original bug)
    start_time = time.time()
    pooling(enabled=False)
    elapsed = time.time() - start_time

    # Should complete quickly (within 2 seconds)
    assert elapsed < 2.0, f"pooling(enabled=False) took too long: {elapsed:.2f}s"
    print(f"pooling(enabled=False) completed in {elapsed:.3f}s")


def test_pooling_disable_without_closing_connection(conn_str):
    """Test that pooling(enabled=False) works even when connections are not explicitly closed."""
    print("Testing pooling disable with unclosed connection...")

    # Enable pooling
    pooling(enabled=True)

    # Create connection but don't close it
    conn = connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    result = cursor.fetchone()
    assert result[0] == 1, "Basic query failed"
    # Note: Not calling conn.close() here intentionally

    # This should still not hang
    start_time = time.time()
    pooling(enabled=False)
    elapsed = time.time() - start_time

    # Should complete quickly (within 2 seconds)
    assert elapsed < 2.0, f"pooling(enabled=False) took too long: {elapsed:.2f}s"
    print(f"pooling(enabled=False) with unclosed connection completed in {elapsed:.3f}s")


def test_multiple_pooling_disable_calls(conn_str):
    """Test that multiple calls to pooling(enabled=False) are safe (double-cleanup prevention)."""
    print("Testing multiple pooling disable calls...")

    # Enable pooling and create connection
    pooling(enabled=True)
    conn = connect(conn_str)
    conn.close()

    # Multiple disable calls should be safe
    start_time = time.time()
    pooling(enabled=False)  # First disable
    pooling(enabled=False)  # Second disable - should be safe
    pooling(enabled=False)  # Third disable - should be safe
    elapsed = time.time() - start_time

    # Should complete quickly
    assert elapsed < 2.0, f"Multiple pooling disable calls took too long: {elapsed:.2f}s"
    print(f"Multiple disable calls completed in {elapsed:.3f}s")


def test_pooling_disable_without_enable(conn_str):
    """Test that calling pooling(enabled=False) without enabling first is safe (edge case)."""
    print("Testing pooling disable without enable...")

    # Reset to clean state
    PoolingManager._reset_for_testing()

    # Disable without enabling should be safe
    start_time = time.time()
    pooling(enabled=False)
    pooling(enabled=False)  # Multiple calls should also be safe
    elapsed = time.time() - start_time

    # Should complete quickly
    assert elapsed < 1.0, f"Disable without enable took too long: {elapsed:.2f}s"
    print(f"Disable without enable completed in {elapsed:.3f}s")


def test_pooling_enable_disable_cycle(conn_str):
    """Test multiple enable/disable cycles work correctly."""
    print("Testing enable/disable cycles...")

    for cycle in range(3):
        print(f"  Cycle {cycle + 1}...")

        # Enable pooling
        pooling(enabled=True)
        assert PoolingManager.is_enabled(), f"Pooling not enabled in cycle {cycle + 1}"

        # Use pooling
        conn = connect(conn_str)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert result[0] == 1, f"Query failed in cycle {cycle + 1}"
        conn.close()

        # Disable pooling
        start_time = time.time()
        pooling(enabled=False)
        elapsed = time.time() - start_time

        assert not PoolingManager.is_enabled(), f"Pooling not disabled in cycle {cycle + 1}"
        assert elapsed < 2.0, f"Disable took too long in cycle {cycle + 1}: {elapsed:.2f}s"

    print("All enable/disable cycles completed successfully")


def test_reenable_rearms_disable_guard(conn_str):
    """After enable -> disable -> enable, a second disable() must actually
    disarm the native manager (call disable_pooling) rather than being skipped.

    Regression for the state-sync bug where enable() did not reset
    _pools_closed, so the second disable() saw _pools_closed=True and skipped
    ddbc_bindings.disable_pooling(), leaving the native manager accepting while
    the Python side believed pooling was off.
    """
    from unittest.mock import patch

    PoolingManager._reset_for_testing()
    try:
        PoolingManager.enable()
        PoolingManager.disable()
        # Re-enable: this must re-arm the guard (reset _pools_closed to False).
        PoolingManager.enable()
        assert PoolingManager._pools_closed is False

        with patch("mssql_python.ddbc_bindings.disable_pooling") as mock_disable:
            PoolingManager.disable()
            mock_disable.assert_called_once()
        assert PoolingManager.is_enabled() is False
        assert PoolingManager._pools_closed is True
    finally:
        PoolingManager._reset_for_testing()


def test_pooling_state_consistency(conn_str):
    """Test that pooling state remains consistent across operations."""
    print("Testing pooling state consistency...")

    # Initial state
    PoolingManager._reset_for_testing()
    assert not PoolingManager.is_enabled(), "Initial state should be disabled"
    assert not PoolingManager.is_initialized(), "Initial state should be uninitialized"

    # Enable pooling
    pooling(enabled=True)
    assert PoolingManager.is_enabled(), "Should be enabled after enable call"
    assert PoolingManager.is_initialized(), "Should be initialized after enable call"

    # Use pooling
    conn = connect(conn_str)
    conn.close()
    assert PoolingManager.is_enabled(), "Should remain enabled after connection usage"

    # Disable pooling
    pooling(enabled=False)
    assert not PoolingManager.is_enabled(), "Should be disabled after disable call"
    assert PoolingManager.is_initialized(), "Should remain initialized after disable call"

    print("Pooling state consistency verified")


# =============================================================================
# Native token-factory (lazy token acquisition) integration tests
# =============================================================================
#
# These white-box tests drive the native ``ddbc_bindings.Connection``
# constructor directly with a ``token_factory`` callable. The regular Python
# unit tests mock the native module, so they never execute the C++ lazy-token
# branches. Exercising them requires a live server, so these tests are guarded
# on ``conn_str`` and only run in the integration environment (where they also
# provide C++ line coverage for the token-factory paths).
#
# For a normal SQL-auth connection the credentials live in the connection
# string, so a factory that returns an empty attrs dict is sufficient to open
# a real connection while still forcing the C++ code down the ``token_factory``
# branch.


class TestNativeTokenFactory:
    """Integration tests for the native lazy token-factory path."""

    @pytest.fixture(autouse=True)
    def _drain_native_pool(self):
        """Drain the native connection pool after each test.

        These tests exercise ``ddbc_bindings.Connection`` directly with
        ``use_pool=True``, bypassing the high-level ``mssql_python.connect()``
        path that auto-enables :class:`PoolingManager`. Because pooling is never
        enabled here, the module's ``atexit`` drain (``shutdown_pooling`` in
        ``pooling.py``, guarded by ``PoolingManager._enabled``) does not run, so
        pooled connections would otherwise linger in the process-lifetime native
        pool singleton. Its C++ static destructor runs after the interpreter is
        finalized, where touching the GIL/ODBC blocks and the process hangs on
        exit. Draining here (the same call the product makes at exit) keeps the
        native pool empty so the interpreter can shut down cleanly.
        """
        from mssql_python import ddbc_bindings

        # These tests drive ``ddbc_bindings.Connection`` with ``use_pool=True``
        # directly, so the native manager must be armed to accept new pools. An
        # earlier test may have called ``pooling(enabled=False)``, which now
        # disarms the native manager (the disable-vs-connect race fix); re-arm
        # here so pooled reuse works. ``enable_pooling`` re-arms accepting
        # without re-running the one-time size/idle-timeout configuration.
        ddbc_bindings.enable_pooling(100, 600)
        yield
        ddbc_bindings.close_pooling()

    def test_pooled_factory_invoked_on_miss_and_skipped_on_reuse(self, conn_str):
        """The factory runs on a pool miss but not on a same-key pool reuse.

        Covers connection_pool.cpp (token_factory() invocation on the pool-miss
        connect path) and the pooled acquireConnection branch in connection.cpp.
        """
        if not conn_str:
            pytest.skip("Live database connection required")

        from mssql_python import ddbc_bindings

        calls = {"n": 0}

        def factory():
            calls["n"] += 1
            return {}  # SQL-auth creds are in conn_str; no attrs needed

        # Unique pool key so this test never collides with other pools.
        pool_key = conn_str + "\x00mssql_test_659_pooled"

        # First open -> pool miss -> factory materializes the (empty) attrs.
        conn1 = ddbc_bindings.Connection(conn_str, True, {}, pool_key, factory)
        assert calls["n"] == 1, "Factory should be invoked once on a pool miss"
        conn1.close()  # returns the connection to the pool under pool_key

        # Second open with the same key -> pool hit -> factory is NOT called.
        conn2 = ddbc_bindings.Connection(conn_str, True, {}, pool_key, factory)
        assert calls["n"] == 1, "Factory must be skipped on a same-key pool reuse"
        conn2.close()

    def test_non_pooled_factory_invoked(self, conn_str):
        """A non-pooled connection still honors the factory.

        Covers the non-pool token_factory branch in connection.cpp.
        """
        if not conn_str:
            pytest.skip("Live database connection required")

        from mssql_python import ddbc_bindings

        calls = {"n": 0}

        def factory():
            calls["n"] += 1
            return {}

        conn = ddbc_bindings.Connection(conn_str, False, {}, "", factory)
        assert calls["n"] == 1, "Factory should be invoked for a non-pooled connect"
        conn.close()

    def test_non_pooled_without_factory_uses_attrs_before(self, conn_str):
        """A non-pooled connection with no factory connects via attrs_before.

        Covers the non-pool ``else`` (no factory) branch in connection.cpp.
        """
        if not conn_str:
            pytest.skip("Live database connection required")

        from mssql_python import ddbc_bindings

        conn = ddbc_bindings.Connection(conn_str, False, {}, "", None)
        assert conn is not None
        conn.close()

    def test_distinct_pool_keys_do_not_share_connections(self, conn_str):
        """Different identity keys must never reuse each other's pooled connection.

        This is the cross-identity isolation guarantee: the native
        pool is keyed on the (identity-aware) pool key, so a connection opened
        under identity A's key must not be handed out to identity B. We prove it
        without real Entra tokens by counting factory invocations: a reuse skips
        the factory, a miss calls it. If B were wrongly served A's pooled
        connection, B's factory would never run.

        It also exercises the embedded-NUL separator (``\\x00``) that joins the
        connection string and the identity discriminator: both keys contain a
        NUL yet remain distinct, confirming the separator survives the
        Python ``str`` -> pybind11 ``std::u16string`` conversion and that the
        full key (not a NUL-truncated prefix) is used for pool lookup.
        """
        if not conn_str:
            pytest.skip("Live database connection required")

        from mssql_python import ddbc_bindings

        calls = {"a": 0, "b": 0}

        def factory_a():
            calls["a"] += 1
            return {}

        def factory_b():
            calls["b"] += 1
            return {}

        key_a = conn_str + "\x00identityA"
        key_b = conn_str + "\x00identityB"

        # Open + close under identity A: pool miss -> factory A runs once, then
        # the connection is returned to pool A.
        conn_a1 = ddbc_bindings.Connection(conn_str, True, {}, key_a, factory_a)
        assert calls["a"] == 1
        conn_a1.close()

        # Open under identity B (different key): must be a miss, not a reuse of
        # A's pooled connection -> factory B runs.
        conn_b1 = ddbc_bindings.Connection(conn_str, True, {}, key_b, factory_b)
        assert (
            calls["b"] == 1
        ), "distinct identity key must not reuse another identity's pooled connection"
        conn_b1.close()

        # Re-open under identity A: its own pooled connection is still there ->
        # pool hit -> factory A is NOT called again.
        conn_a2 = ddbc_bindings.Connection(conn_str, True, {}, key_a, factory_a)
        assert calls["a"] == 1, "same identity key should reuse its own pooled connection"
        conn_a2.close()

    def test_pooled_factory_accepts_tuple_return(self, conn_str):
        """The factory may return ``(attrs, expires_on)``.

        Covers the tuple-unpacking path in Connection::invokeTokenFactory and
        confirms a token whose expiry is far in the future is still reused on a
        same-key pool hit (i.e. expiry-aware checkout does not discard it).
        """
        if not conn_str:
            pytest.skip("Live database connection required")

        import time

        from mssql_python import ddbc_bindings

        calls = {"n": 0}
        far_future = int(time.time()) + 3600  # well beyond the 300s threshold

        def factory():
            calls["n"] += 1
            return {}, far_future  # (attrs, expires_on)

        pool_key = conn_str + "\x00mssql_test_tuple_return"

        conn1 = ddbc_bindings.Connection(conn_str, True, {}, pool_key, factory)
        assert calls["n"] == 1, "Factory (tuple return) should run once on a pool miss"
        conn1.close()

        conn2 = ddbc_bindings.Connection(conn_str, True, {}, pool_key, factory)
        assert calls["n"] == 1, "Non-expiring token must be reused on a same-key pool hit"
        conn2.close()

    def test_near_expiry_token_refreshed_on_checkout(self, conn_str):
        """A pooled connection with a near-expiry token is refreshed on checkout.

        Covers the expiry-aware checkout branch: when the
        factory reports an expiry within the refresh threshold, the pooled
        candidate is discarded and a fresh connection is opened, so the factory
        runs again on reuse instead of being skipped.
        """
        if not conn_str:
            pytest.skip("Live database connection required")

        import time

        from mssql_python import ddbc_bindings

        calls = {"n": 0}
        # Expiry inside the 300s refresh threshold => treated as near-expiry.
        near_expiry = int(time.time()) + 60

        def factory():
            calls["n"] += 1
            return {}, near_expiry

        pool_key = conn_str + "\x00mssql_test_651_nearexpiry"

        conn1 = ddbc_bindings.Connection(conn_str, True, {}, pool_key, factory)
        assert calls["n"] == 1, "Factory should run once on the initial pool miss"
        conn1.close()  # returned to the pool, but its token is near expiry

        # Same key, but the pooled connection's token is near expiry, so it must
        # be discarded and reopened -> factory runs a second time.
        conn2 = ddbc_bindings.Connection(conn_str, True, {}, pool_key, factory)
        assert calls["n"] == 2, "Near-expiry pooled connection must be refreshed on checkout"
        conn2.close()

    def test_near_expiry_factory_token_bytes_are_extracted(self, conn_str):
        """A refreshed factory that returns token bytes drives token extraction.

        Covers the ``extractAccessToken`` loop body in connection_pool.cpp: the
        earlier near-expiry tests hand back an empty attrs dict, so the loop that
        scans the dict for the ``SQL_COPT_SS_ACCESS_TOKEN`` (1256) entry never
        iterates. Here the refresh call returns a dict *with* that token key, so
        the C++ extracts the bytes and takes the token-rotation branch (fresh
        token != the pooled connection's empty token). The reopen with a bogus
        token against a SQL-auth server is expected to fail; we only need the
        extraction + rotation branch to execute, which it does before the reopen.
        """
        if not conn_str:
            pytest.skip("Live database connection required")

        import time

        from mssql_python import ddbc_bindings

        # ConstantsDDBC.SQL_COPT_SS_ACCESS_TOKEN — the attr id the C++ scans for.
        SQL_COPT_SS_ACCESS_TOKEN = 1256
        calls = {"n": 0}
        near_expiry = int(time.time()) + 60  # inside the 300s refresh threshold

        def factory():
            calls["n"] += 1
            if calls["n"] == 1:
                # Initial pool miss: SQL-auth creds are in conn_str, so an empty
                # attrs dict opens a real connection (its token stays empty).
                return {}, near_expiry
            # Refresh on checkout: hand back a token so extractAccessToken()
            # iterates its loop body and pulls the bytes out.
            return {SQL_COPT_SS_ACCESS_TOKEN: b"rotated-token-bytes"}, near_expiry

        pool_key = conn_str + "\x00mssql_test_token_bytes"

        conn1 = ddbc_bindings.Connection(conn_str, True, {}, pool_key, factory)
        assert calls["n"] == 1, "Factory should run once on the initial pool miss"
        conn1.close()  # returned to the pool with an empty token, near expiry

        # Same key: the pooled token is near expiry, so the factory is invoked
        # again and now returns real token bytes. The C++ extracts them, sees a
        # rotated (different) token, and reopens with it — which the SQL-auth
        # server rejects. The extraction + rotation branch has already run.
        with pytest.raises(Exception):
            ddbc_bindings.Connection(conn_str, True, {}, pool_key, factory)
        assert calls["n"] == 2, "Near-expiry checkout must re-invoke the factory"

    def test_factory_token_unknown_expiry_is_reused(self, conn_str):
        """A factory that reports no expiry (``None``/0) *and* supplies no token
        leaves the pooled connection with nothing that can expire, so the native
        checkout reuses it.

        ``isTokenNearExpiry`` fails closed on an unknown expiry only when the
        pooled connection actually holds an access token (connection.cpp). Here
        the SQL-auth creds live in the connection string and the factory returns
        empty attrs, so the pooled token is empty and there is nothing to
        refresh: the factory runs once on the miss and is skipped on the
        same-key hit. A pooled connection that *does* carry a token with an
        unknown expiry is instead refreshed on checkout (fail closed); that path
        needs a real Entra token to exercise end-to-end.
        """
        if not conn_str:
            pytest.skip("Live database connection required")

        from mssql_python import ddbc_bindings

        for expiry in (None, 0):
            calls = {"n": 0}

            def factory(_expiry=expiry):
                calls["n"] += 1
                return {}, _expiry

            pool_key = conn_str + f"\x00mssql_test_unknown_expiry_{expiry}"

            conn1 = ddbc_bindings.Connection(conn_str, True, {}, pool_key, factory)
            assert calls["n"] == 1, "Factory should run once on the initial pool miss"
            conn1.close()

            conn2 = ddbc_bindings.Connection(conn_str, True, {}, pool_key, factory)
            assert calls["n"] == 1, "No token held: unknown expiry is reused (nothing to refresh)"
            conn2.close()

    def test_factory_raises_on_near_expiry_checkout_recovers_pool(self, conn_str):
        """A factory that raises while refreshing a near-expiry pooled connection
        must not leak the reserved pool slot.

        Covers the exception path of the expiry-aware checkout: prime the pool
        with a near-expiry connection, then make the refresh factory raise on
        the next checkout. The connect fails (exception propagates), but the
        reserved slot is released under the pool mutex, so a subsequent connect
        under the same key still succeeds instead of finding the pool wedged or
        exhausted.
        """
        if not conn_str:
            pytest.skip("Live database connection required")

        import time

        from mssql_python import ddbc_bindings

        near_expiry = int(time.time()) + 60  # inside the 300s refresh threshold
        state = {"raise_on_checkout": False, "n": 0}

        def factory():
            state["n"] += 1
            if state["raise_on_checkout"]:
                raise RuntimeError("token refresh failed mid-checkout")
            return {}, near_expiry

        pool_key = conn_str + "\x00mssql_test_factory_raise_recover"

        # Prime the pool with a near-expiry pooled connection.
        conn1 = ddbc_bindings.Connection(conn_str, True, {}, pool_key, factory)
        assert state["n"] == 1
        conn1.close()

        # Next checkout is near-expiry -> factory runs to refresh, but raises.
        state["raise_on_checkout"] = True
        with pytest.raises(Exception):
            ddbc_bindings.Connection(conn_str, True, {}, pool_key, factory)

        # The reserved slot must have been released: a healthy factory can still
        # open a connection under the same key (pool is neither wedged nor full).
        state["raise_on_checkout"] = False
        conn3 = ddbc_bindings.Connection(conn_str, True, {}, pool_key, factory)
        assert conn3 is not None, "pool slot must be recovered after a factory failure"
        conn3.close()

    def test_orphaned_connection_is_disconnected_on_return(self, conn_str):
        """Returning a connection whose pool was evicted disconnects it cleanly.

        Covers the orphan branch of ``ConnectionPoolManager::returnConnection``:
        when a checked-out connection is returned but no pool is registered under
        its key (here because ``close_pooling()`` cleared the pool map while the
        connection was still out), the manager must disconnect the orphan rather
        than leak the ODBC handle.
        """
        if not conn_str:
            pytest.skip("Live database connection required")

        from mssql_python import ddbc_bindings

        def factory():
            return {}

        pool_key = conn_str + "\x00mssql_test_orphan_return"

        # Check a connection out of the pool, then drop the whole pool map while
        # it is still held. The pool under pool_key no longer exists.
        conn = ddbc_bindings.Connection(conn_str, True, {}, pool_key, factory)
        ddbc_bindings.close_pooling()

        # Returning it now finds no pool -> the orphan-disconnect path runs.
        conn.close()

    def test_disabled_manager_creates_no_pool(self, conn_str):
        """A pooled-style connect after disable_pooling() creates no pool.

        Regression test for the disable()-vs-connect() race: ``disable_pooling()``
        disarms the native manager under ``_manager_mutex`` before clearing the
        pool map, so any ``acquireConnection`` serialized after it declines
        (returns nullptr) and the connection transparently falls back to a
        non-pooled one. A connect can therefore never resurrect a pool after a
        disable. The non-pooled fallback still honors the token factory.

        Asserts the observable contract instead of an internal pool count: the
        token factory is invoked on every connect. With a live pool, a same-key
        connect after a close would reuse the idle connection and skip the
        factory; because a disabled manager creates no pool, each connect is a
        fresh non-pooled connection and the factory fires again.
        """
        if not conn_str:
            pytest.skip("Live database connection required")

        from mssql_python import ddbc_bindings

        try:
            # Arm, then disable: disarm new-pool creation and close everything.
            ddbc_bindings.enable_pooling(10, 600)
            ddbc_bindings.disable_pooling()

            calls = {"n": 0}

            def factory():
                calls["n"] += 1
                return {}

            pool_key = conn_str + "\x00mssql_test_disabled_nopool"

            # use_pool=True, but the manager is disarmed -> non-pooled fallback,
            # which still opens a real connection (the factory fires).
            conn = ddbc_bindings.Connection(conn_str, True, {}, pool_key, factory)
            assert calls["n"] == 1, "non-pooled fallback must still invoke the factory"
            conn.close()

            # If the disabled manager had wrongly created a pool, this same-key
            # connect would reuse the returned connection and skip the factory.
            # No pool exists, so it must open another fresh connection instead.
            conn2 = ddbc_bindings.Connection(conn_str, True, {}, pool_key, factory)
            assert calls["n"] == 2, (
                "a disabled manager must not pool the connection: the second "
                "same-key connect had to open a fresh non-pooled connection "
                "(the factory fired again) instead of reusing a pooled one"
            )
            conn2.close()
        finally:
            # Re-arm so sibling tests pool normally again.
            ddbc_bindings.enable_pooling(10, 600)
