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
    if conn_str:
        env["DB_CONNECTION_STRING"] = conn_str
    else:
        env.pop("DB_CONNECTION_STRING", None)
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


def test_pool_size_accounting_race_on_close_interleave(conn_str):
    """Regression test for GH-746: connection pool size accounting drift on close race.

    When a connection-open failure races pool close(), in-flight reservations
    and checked-out connections are retained in _current_size across close().
    Coupled with pool generation tracking, Thread A's cleanup on failure only
    decrements capacity if its generation still matches, preventing drift and
    ensuring max_size is never exceeded across close() interleavings.

    Uses _TestConnectionPool to ensure Thread A, Thread B, and Thread C all
    operate deterministically against the exact same pool instance.
    """
    _run_in_subprocess(
        """
        import threading
        from mssql_python import ddbc_bindings

        pool = ddbc_bindings._TestConnectionPool(2, 600)
        assert pool.current_size == 0
        assert pool.generation == 0

        in_factory_a = threading.Event()
        release_factory_a = threading.Event()

        def factory_a():
            in_factory_a.set()
            assert release_factory_a.wait(timeout=5.0), "Timed out waiting to release factory A"
            raise RuntimeError("simulated open failure A")

        t_a_error = []

        def run_a():
            try:
                pool.acquire("SERVER=dummy_test_746;", factory_a)
            except Exception as exc:
                t_a_error.append(exc)

        t_a = threading.Thread(target=run_a)
        t_a.start()
        assert in_factory_a.wait(timeout=5.0), "Timed out waiting for Thread A to enter factory"
        assert pool.current_size == 1
        assert pool.in_flight == 1

        # Thread A has reserved slot 1. Pool is closed while Thread A is in-flight.
        # Reserved capacity is retained for in-flight opens so max_size is never exceeded (#746).
        pool.close()
        assert pool.current_size == 1
        assert pool.in_flight == 1
        assert pool.generation == 1

        # Thread B initiates acquire and reserves the 2nd slot under the new generation.
        in_factory_b = threading.Event()
        release_factory_b = threading.Event()

        def factory_b():
            in_factory_b.set()
            assert release_factory_b.wait(timeout=5.0), "Timed out waiting to release factory B"
            raise RuntimeError("simulated open failure B")

        t_b_error = []

        def run_b():
            try:
                pool.acquire("SERVER=dummy_test_746;", factory_b)
            except Exception as exc:
                t_b_error.append(exc)

        t_b = threading.Thread(target=run_b)
        t_b.start()
        assert in_factory_b.wait(timeout=5.0), "Timed out waiting for Thread B to enter factory"
        assert pool.current_size == 2
        assert pool.in_flight == 2

        # Thread A now raises its error. Its cleanup decrements in_flight and current_size
        # for Thread A (2 -> 1). Thread B's reservation under generation 1 is preserved.
        release_factory_a.set()
        t_a.join(timeout=5.0)
        assert len(t_a_error) == 1 and "simulated open failure A" in str(t_a_error[0])

        # Under the generation fix, current_size MUST still be 1 (Thread B's reservation is preserved).
        assert pool.current_size == 1, (
            f"Expected pool.current_size to be 1, but got {pool.current_size} (drift occurred!)"
        )
        assert pool.in_flight == 1

        # Clean up Thread B
        release_factory_b.set()
        t_b.join(timeout=5.0)
        assert len(t_b_error) == 1 and "simulated open failure B" in str(t_b_error[0])
        assert pool.current_size == 0
        assert pool.in_flight == 0
        pool.close()
        """,
        conn_str,
    )


def test_pool_size_accounting_race_on_candidate_validation_close_interleave(conn_str):
    """Regression test for GH-746: candidate validation failure racing pool close().

    When a candidate popped from the pool fails validation (e.g. dead socket or
    token rotation failure) while racing a pool close(), the pool retains
    capacity for the in-flight validation across close(). Coupled with generation
    guarding, Thread A's cleanup on validation failure decrements only its own
    reservation without corrupting any newer generation's reservation.
    """
    _run_in_subprocess(
        """
        import threading
        from mssql_python import ddbc_bindings

        pool = ddbc_bindings._TestConnectionPool(2, 600)
        # Inject an expired candidate into the idle pool
        pool.inject_candidate("SERVER=dummy_test_746;", 1)
        assert pool.current_size == 1
        assert pool.generation == 0

        in_factory_a = threading.Event()
        release_factory_a = threading.Event()

        def factory_a():
            in_factory_a.set()
            assert release_factory_a.wait(timeout=5.0), "Timed out waiting to release factory A"
            raise RuntimeError("simulated token rotation validation failure")

        t_a_error = []

        def run_a():
            try:
                pool.acquire("SERVER=dummy_test_746;", factory_a)
            except Exception as exc:
                t_a_error.append(exc)

        t_a = threading.Thread(target=run_a)
        t_a.start()
        assert in_factory_a.wait(timeout=5.0), "Timed out waiting for Thread A to enter factory"
        assert pool.in_flight == 1

        # Thread A popped the candidate (generation 0) and is validating it in factory_a.
        # Pool close retains capacity for in-flight validation: current_size stays 1.
        pool.close()
        assert pool.current_size == 1
        assert pool.in_flight == 1
        assert pool.generation == 1

        # Thread B reserves the 2nd slot under generation 1.
        in_factory_b = threading.Event()
        release_factory_b = threading.Event()

        def factory_b():
            in_factory_b.set()
            assert release_factory_b.wait(timeout=5.0), "Timed out waiting to release factory B"
            raise RuntimeError("simulated open failure B")

        t_b_error = []

        def run_b():
            try:
                pool.acquire("SERVER=dummy_test_746;", factory_b)
            except Exception as exc:
                t_b_error.append(exc)

        t_b = threading.Thread(target=run_b)
        t_b.start()
        assert in_factory_b.wait(timeout=5.0), "Timed out waiting for Thread B to enter factory"
        assert pool.current_size == 2
        assert pool.in_flight == 2

        # Thread A finishes factory_a (validation fails).
        # Thread A releases its in-flight reservation (2 -> 1).
        # Thread B's reservation under generation 1 is preserved.
        release_factory_a.set()
        t_a.join(timeout=5.0)

        # Verify Thread B's reservation was not cancelled
        assert pool.current_size == 1, (
            f"Expected pool.current_size to be 1, but got {pool.current_size} (drift occurred!)"
        )
        assert pool.in_flight == 1

        release_factory_b.set()
        t_b.join(timeout=5.0)
        assert pool.current_size == 0
        assert pool.in_flight == 0
        pool.close()
        """,
        conn_str,
    )


def test_pool_size_accounting_race_on_successful_candidate_reuse_close_interleave(conn_str):
    """Regression test for GH-746: candidate reuse success racing pool close().

    When a candidate popped under generation 0 succeeds validation while racing
    a pool close(), the candidate must NOT be returned as a valid connection
    under the new generation. Returning it without an active reservation in the
    new generation would allow another thread to reserve up to max_size, causing
    the pool to exceed max_size. Instead, the stale candidate is discarded and
    acquire retries under the new generation (or fails if the pool is full).
    """
    _run_in_subprocess(
        """
        import threading
        from mssql_python import ddbc_bindings

        pool = ddbc_bindings._TestConnectionPool(2, 600)
        pool.set_mock_mode(True)
        # Inject candidate with near-expiry so factory_a runs to check it
        pool.inject_candidate("SERVER=dummy_test_746;", 1, True)
        assert pool.current_size == 1
        assert pool.generation == 0

        in_factory_a = threading.Event()
        release_factory_a = threading.Event()
        factory_a_calls = [0]

        def factory_a():
            factory_a_calls[0] += 1
            if factory_a_calls[0] == 1:
                in_factory_a.set()
                assert release_factory_a.wait(timeout=5.0), "Timed out waiting to release factory A"
            return {}, 9999999999

        t_a_conn = []
        t_a_error = []

        def run_a():
            try:
                conn = pool.acquire("SERVER=dummy_test_746;", factory_a)
                t_a_conn.append(conn)
            except Exception as exc:
                t_a_error.append(exc)

        t_a = threading.Thread(target=run_a)
        t_a.start()
        assert in_factory_a.wait(timeout=5.0), "Timed out waiting for Thread A to enter factory"
        assert pool.in_flight == 1

        # Thread A popped candidate (generation 0). Pool close retains in-flight validation.
        pool.close()
        assert pool.current_size == 1
        assert pool.in_flight == 1
        assert pool.generation == 1

        # Thread B reserves slot 2 under generation 1
        in_factory_b = threading.Event()
        release_factory_b = threading.Event()

        def factory_b():
            in_factory_b.set()
            assert release_factory_b.wait(timeout=5.0), "Timed out waiting to release factory B"
            return {}

        t_b_conn = []
        t_b_error = []

        def run_b():
            try:
                conn = pool.acquire("SERVER=dummy_test_746;", factory_b)
                t_b_conn.append(conn)
            except Exception as exc:
                t_b_error.append(exc)

        t_b = threading.Thread(target=run_b)
        t_b.start()
        assert in_factory_b.wait(timeout=5.0), "Timed out waiting for Thread B to enter factory"
        assert pool.current_size == 2
        assert pool.in_flight == 2

        # 1. Thread B finishes connecting first and publishes under generation 1
        release_factory_b.set()
        t_b.join(timeout=5.0)
        assert len(t_b_error) == 0
        assert len(t_b_conn) == 1
        assert pool.checked_out == 1
        assert pool.in_flight == 1
        assert pool.current_size == 2

        # 2. Thread A finishes validation second. Under the generation fix, Thread A detects
        # generation mismatch (0 != 1), discards stale candidate, decrements in_flight and current_size (2 -> 1).
        # Thread A retries acquire under generation 1 and successfully acquires slot 2.
        release_factory_a.set()
        t_a.join(timeout=5.0)
        assert len(t_a_error) == 0
        assert len(t_a_conn) == 1
        # Verify candidate was discarded and A performed a fresh open under generation 1 (#746)
        assert factory_a_calls[0] == 2, f"Expected 2 factory invocations (validation + retry open), got {factory_a_calls[0]}"
        assert t_a_conn[0].origin_generation == 1, f"Expected generation 1, got {t_a_conn[0].origin_generation}"
        assert pool.checked_out == 2
        assert pool.in_flight == 0
        assert pool.current_size == 2

        # Return both connections and clean up
        pool.release(t_a_conn[0])
        pool.release(t_b_conn[0])
        assert pool.checked_out == 0
        pool.close()
        assert pool.current_size == 0
        """,
        conn_str,
    )


def test_pool_size_accounting_race_on_successful_open_close_interleave(conn_str):
    """Regression test for GH-746: connection open success racing pool close().

    When a connection open succeeds after racing a pool close(), the newly opened
    connection must NOT be returned into the pool under the new generation without
    validating the generation counter. If Thread A connected under generation 0,
    close() reset the pool, and Thread B reserved generation 1, returning Thread A's
    connection would result in 2 live connections when max_size=1. The generation
    check ensures Thread A discards the orphaned connection and retries under the
    new generation (failing if Thread B has claimed the capacity).
    """
    _run_in_subprocess(
        """
        import threading
        from mssql_python import ddbc_bindings

        pool = ddbc_bindings._TestConnectionPool(2, 600)
        pool.set_mock_mode(True)
        assert pool.current_size == 0
        assert pool.generation == 0

        in_factory_a = threading.Event()
        release_factory_a = threading.Event()
        factory_a_calls = [0]

        def factory_a():
            factory_a_calls[0] += 1
            if factory_a_calls[0] == 1:
                in_factory_a.set()
                assert release_factory_a.wait(timeout=5.0), "Timed out waiting to release factory A"
            return {}

        t_a_conn = []
        t_a_error = []

        def run_a():
            try:
                conn = pool.acquire("SERVER=dummy_test_746;", factory_a)
                t_a_conn.append(conn)
            except Exception as exc:
                t_a_error.append(exc)

        t_a = threading.Thread(target=run_a)
        t_a.start()
        assert in_factory_a.wait(timeout=5.0), "Timed out waiting for Thread A to enter factory"
        assert pool.current_size == 1
        assert pool.in_flight == 1

        # Thread A reserved slot under generation 0. Pool is closed while Thread A is in-flight.
        # In-flight capacity is retained so max_size is not exceeded (#746).
        pool.close()
        assert pool.current_size == 1
        assert pool.in_flight == 1
        assert pool.generation == 1

        # Thread B begins acquiring under generation 1 (reserves slot 2 of max_size=2)
        in_factory_b = threading.Event()
        release_factory_b = threading.Event()

        def factory_b():
            in_factory_b.set()
            assert release_factory_b.wait(timeout=5.0), "Timed out waiting to release factory B"
            return {}

        t_b_conn = []
        t_b_error = []

        def run_b():
            try:
                conn = pool.acquire("SERVER=dummy_test_746;", factory_b)
                t_b_conn.append(conn)
            except Exception as exc:
                t_b_error.append(exc)

        t_b = threading.Thread(target=run_b)
        t_b.start()
        assert in_factory_b.wait(timeout=5.0), "Timed out waiting for Thread B to enter factory"
        assert pool.current_size == 2
        assert pool.in_flight == 2

        # 1. NEW-GENERATION OPEN COMPLETES FIRST:
        # Thread B finishes connecting and publishes valid_conn under generation 1
        release_factory_b.set()
        t_b.join(timeout=5.0)
        assert len(t_b_error) == 0
        assert len(t_b_conn) == 1
        assert t_b_conn[0].origin_generation == 1
        assert pool.checked_out == 1
        assert pool.in_flight == 1
        assert pool.current_size == 2

        # 2. OLD-GENERATION OPEN COMPLETES SECOND:
        # Thread A finishes connecting outside the lock.
        # Under generation fix, Thread A detects reservation generation mismatch (0 != 1),
        # disconnects its stale connection, decrements in_flight (1 -> 0) and current_size (2 -> 1).
        # Thread B's connection is intact and valid!
        # Thread A retries acquire under generation 1 and successfully acquires the freed slot.
        release_factory_a.set()
        t_a.join(timeout=5.0)
        assert len(t_a_error) == 0
        assert len(t_a_conn) == 1
        # Verify stale open was discarded and A performed a fresh open under generation 1 (#746)
        assert factory_a_calls[0] == 2, f"Expected 2 factory invocations (initial open + retry open), got {factory_a_calls[0]}"
        assert t_a_conn[0].origin_generation == 1, f"Expected generation 1, got {t_a_conn[0].origin_generation}"
        assert pool.checked_out == 2
        assert pool.in_flight == 0
        assert pool.current_size == 2

        # Clean up both connections
        pool.release(t_a_conn[0])
        pool.release(t_b_conn[0])
        assert pool.checked_out == 0
        pool.close()
        assert pool.current_size == 0
        """,
        conn_str,
    )


def test_pool_in_flight_open_blocks_acquire_exceeding_max_size_1(conn_str):
    """When max_size=1 and an open is in flight across close(), new acquire is blocked.

    Ensures that an in-flight open retains reserved capacity across close(),
    preventing a new-generation thread from opening another physical connection
    while the stale open is still establishing its socket (#746).
    """
    _run_in_subprocess(
        """
        import threading
        from mssql_python import ddbc_bindings

        pool = ddbc_bindings._TestConnectionPool(1, 600)
        pool.set_mock_mode(True)

        in_factory_a = threading.Event()
        release_factory_a = threading.Event()

        def factory_a():
            in_factory_a.set()
            assert release_factory_a.wait(timeout=5.0)
            return {}

        t_a_conn = []

        def run_a():
            conn = pool.acquire("SERVER=dummy_test_746;", factory_a)
            t_a_conn.append(conn)

        t_a = threading.Thread(target=run_a)
        t_a.start()
        assert in_factory_a.wait(timeout=5.0)
        assert pool.current_size == 1
        assert pool.in_flight == 1

        # Close pool while Thread A is in-flight
        pool.close()
        # In-flight capacity is retained: current_size stays 1!
        assert pool.current_size == 1
        assert pool.in_flight == 1
        assert pool.generation == 1

        # Thread B tries to acquire under generation 1: REJECTED because capacity is held!
        rejected = False
        try:
            pool.acquire("SERVER=dummy_test_746;", lambda: {})
        except RuntimeError as exc:
            if "pool size limit reached" in str(exc):
                rejected = True
        assert rejected, "Thread B must be rejected while Thread A's open is still in-flight"

        # Thread A completes and disconnects stale connection, freeing the slot
        release_factory_a.set()
        t_a.join(timeout=5.0)

        # Thread A's retry acquired the freed slot under generation 1
        assert len(t_a_conn) == 1
        assert pool.current_size == 1
        assert pool.checked_out == 1

        pool.release(t_a_conn[0])
        assert pool.checked_out == 0
        pool.close()
        assert pool.current_size == 0
        """,
        conn_str,
    )


def test_pool_release_from_stale_generation_does_not_pollute_pool(conn_str):
    """Regression test for GH-746: releasing a connection from an invalidated pool/generation.

    When a connection checked out from an earlier pool generation is released after
    pool.close() has advanced the generation, release() must NOT push that stale connection
    back into the active pool nor decrement current_size of the new generation. Instead, it
    must cleanly disconnect the stale connection, ensuring the new pool generation remains
    uncorrupted and never exceeds max_size.
    """
    _run_in_subprocess(
        """
        from mssql_python import ddbc_bindings

        pool = ddbc_bindings._TestConnectionPool(1, 600)
        pool.set_mock_mode(True)

        # 1. Acquire conn_1 under generation 0
        conn_1 = pool.acquire("SERVER=dummy_test_746;", lambda: {})
        assert conn_1 is not None
        assert pool.current_size == 1
        assert pool.checked_out == 1
        assert pool.generation == 0

        # 2. Pool is closed while conn_1 is still checked out.
        # Reserved capacity is retained for checked-out connections so the
        # max_size cap is not exceeded while conn_1 is live (#746).
        pool.close()
        assert pool.current_size == 1
        assert pool.checked_out == 1
        assert pool.generation == 1

        # 3. An acquire under generation 1 must be rejected while conn_1 is still checked out,
        # preserving the max_size=1 invariant across pool.close() (#746).
        rejected = False
        try:
            pool.acquire("SERVER=dummy_test_746;", lambda: {})
        except RuntimeError as exc:
            if "pool size limit reached" in str(exc):
                rejected = True
        assert rejected, "A new acquire must be rejected while conn_1 is still checked out"

        # 4. Release conn_1 (from generation 0).
        # It belongs to this pool but its generation is stale. It is disconnected,
        # and the retained checked-out capacity is released: current_size drops to 0.
        pool.release(conn_1)
        assert pool.current_size == 0
        assert pool.checked_out == 0

        # 5. Now that capacity has freed up, acquire conn_2 under generation 1 succeeds
        conn_2 = pool.acquire("SERVER=dummy_test_746;", lambda: {})
        assert conn_2 is not None
        assert pool.current_size == 1
        assert pool.checked_out == 1
        assert pool.generation == 1

        # 6. Release conn_2 (matches generation 1). It returns to the pool idle deque.
        pool.release(conn_2)
        assert pool.current_size == 1
        assert pool.checked_out == 0

        # 7. Next acquire reuses conn_2 from the pool
        conn_3 = pool.acquire("SERVER=dummy_test_746;", lambda: {})
        assert conn_3 is not None
        assert pool.current_size == 1
        assert pool.checked_out == 1

        pool.release(conn_3)
        assert pool.checked_out == 0
        pool.close()
        assert pool.current_size == 0
        """,
        conn_str,
    )


def test_pool_release_after_pool_recreation(conn_str):
    """Releasing a connection to a newly recreated pool must not corrupt the new pool's size.

    Verifies that monotonic pool IDs prevent address-reuse (ABA) corruption:
    even if pool_2 were to be allocated at the same memory address as pool_1,
    releasing conn_1 (from pool_1) to pool_2 will not match pool_2's monotonic pool ID.
    Therefore, pool_2's current_size is not erroneously decremented or corrupted (#746).
    """
    _run_in_subprocess(
        """
        from mssql_python import ddbc_bindings

        pool_1 = ddbc_bindings._TestConnectionPool(1, 600)
        pool_1.set_mock_mode(True)
        assert pool_1.pool_id > 0

        # Acquire conn_1 from pool_1
        conn_1 = pool_1.acquire("SERVER=dummy_test_746;", lambda: {})
        assert conn_1 is not None
        assert pool_1.current_size == 1
        assert pool_1.checked_out == 1

        # Create pool_2 with its own distinct monotonic pool_id
        pool_2 = ddbc_bindings._TestConnectionPool(1, 600)
        pool_2.set_mock_mode(True)
        assert pool_2.pool_id > pool_1.pool_id
        assert pool_2.current_size == 0
        assert pool_2.checked_out == 0

        # Release conn_1 into pool_2 (wrong pool ID)
        pool_2.release(conn_1)
        # pool_2 must not adopt conn_1 or decrement its size: stays 0
        assert pool_2.current_size == 0
        assert pool_2.checked_out == 0

        # pool_2 can acquire normally
        conn_2 = pool_2.acquire("SERVER=dummy_test_746;", lambda: {})
        assert conn_2 is not None
        assert pool_2.current_size == 1
        assert pool_2.checked_out == 1

        # Releasing conn_1 again to pool_2 does not corrupt pool_2's active connection
        pool_2.release(conn_1)
        assert pool_2.current_size == 1
        assert pool_2.checked_out == 1

        # Cleanly release conn_2
        pool_2.release(conn_2)
        assert pool_2.checked_out == 0
        pool_2.close()
        assert pool_2.current_size == 0
        """,
        conn_str,
    )


def test_pool_manager_defers_replacement_while_connection_checked_out(conn_str):
    """Across a disable/enable cycle, acquires respect capacity of checked-out connections.

    When ConnectionPoolManager::closePools() runs while a connection is checked out,
    the pool is retained in _pools, deferring replacement pool creation until the old pool
    has no live work. Subsequent acquires under the re-enabled pool manager must not
    allow exceeding max_size while the old connection remains checked out (#746).
    """
    _run_in_subprocess(
        """
        from mssql_python import ddbc_bindings

        ddbc_bindings._set_pool_manager_mock_mode(True)

        pool_key = "SERVER=dummy_test_746;test_replace"
        conn_str = "SERVER=dummy_test_746;"

        # 1. Enable pooling with max_size=2
        ddbc_bindings.enable_pooling(2, 600)

        # 2. Acquire conn_1 from pool (generation 0)
        conn_1 = ddbc_bindings.Connection(conn_str, True, {}, pool_key, lambda: {})
        assert conn_1.origin_generation == 0

        # 3. Disable pooling (invokes closePools())
        # The pool has checked_out=1, so it is retained in _pools with generation bumped to 1.
        ddbc_bindings.disable_pooling()

        # 4. Re-enable pooling with max_size=2
        ddbc_bindings.enable_pooling(2, 600)

        # 5. Acquire conn_2 from the manager while conn_1 is still checked out.
        # Replacement pool creation is deferred because conn_1 is still live.
        # conn_2 is acquired under generation 1 (1 + 1 = 2 connections active).
        conn_2 = ddbc_bindings.Connection(conn_str, True, {}, pool_key, lambda: {})
        assert conn_2.origin_generation == 1
        assert conn_2.origin_pool_id == conn_1.origin_pool_id

        # 6. Attempting to acquire a 3rd connection must fail because max_size=2 is reached!
        rejected = False
        try:
            ddbc_bindings.Connection(conn_str, True, {}, pool_key, lambda: {})
        except RuntimeError as exc:
            if "pool size limit reached" in str(exc):
                rejected = True
        assert rejected, "Must reject acquire when max_size=2 is reached across recreation!"

        # 7. Close conn_1: releases the old-generation connection and frees capacity.
        conn_1.close()

        # 8. Now acquire conn_3: capacity is freed, so acquire succeeds!
        conn_3 = ddbc_bindings.Connection(conn_str, True, {}, pool_key, lambda: {})
        assert conn_3.origin_generation == 1

        # Clean up remaining connections
        conn_2.close()
        conn_3.close()
        ddbc_bindings.close_pooling()
        """,
        conn_str,
    )


def test_pool_release_disconnect_keeps_in_flight_until_disconnected(conn_str):
    """Regression test for GH-746: stale/overflow release disconnect retains in-flight accounting.

    When an expired, generation-mismatched, or overflow connection is released,
    it must be transitioned from _checked_out to _in_flight during the
    unlocked conn->disconnect() call, and only decremented after disconnect finishes.
    This guarantees that concurrent callers cannot reserve a slot or open a new
    physical connection before the old physical connection has finished closing.
    """
    _run_in_subprocess(
        """
        import threading
        from mssql_python import ddbc_bindings

        pool = ddbc_bindings._TestConnectionPool(1, 600)
        pool.set_mock_mode(True)

        # 1. Acquire connection (generation 0, checked_out=1, current_size=1)
        conn = pool.acquire("SERVER=dummy_test_746;", None)
        assert conn is not None
        assert pool.current_size == 1
        assert pool.checked_out == 1
        assert pool.in_flight == 0

        # 2. Close the pool to bump the generation so releasing conn triggers a disconnect.
        # Since conn was checked out, close() keeps current_size=1, checked_out=1, in_flight=0.
        pool.close()
        assert pool.current_size == 1
        assert pool.checked_out == 1
        assert pool.in_flight == 0
        assert pool.generation == 1

        in_disconnect = threading.Event()
        release_disconnect = threading.Event()
        hook_observed = {}

        def on_disconnect():
            # At this point, release() has moved conn from _checked_out to _in_flight,
            # but has NOT yet decremented current_size.
            hook_observed["current_size"] = pool.current_size
            hook_observed["in_flight"] = pool.in_flight
            hook_observed["checked_out"] = pool.checked_out
            in_disconnect.set()
            assert release_disconnect.wait(timeout=5.0), "Timed out waiting to release disconnect"

        pool.set_on_disconnect_hook(on_disconnect)

        t_err = []
        def run_release():
            try:
                pool.release(conn)
            except Exception as exc:
                t_err.append(exc)

        t = threading.Thread(target=run_release)
        t.start()
        assert in_disconnect.wait(timeout=5.0), "Timed out waiting for disconnect hook"

        # Verify hook observed state:
        assert hook_observed["current_size"] == 1
        assert hook_observed["in_flight"] == 1
        assert hook_observed["checked_out"] == 0

        # While disconnect is in progress, any concurrent acquire is blocked from
        # allocating because max_size=1 is still fully occupied by in_flight teardown!
        rejected = False
        try:
            pool.acquire("SERVER=dummy_test_746;", None)
        except RuntimeError as exc:
            if "pool size limit reached" in str(exc):
                rejected = True
        assert rejected, "Concurrent acquire must be rejected while teardown disconnect is in-flight!"

        # Let the disconnect complete
        release_disconnect.set()
        t.join(timeout=5.0)
        assert not t_err, f"Release thread error: {t_err}"

        # Now that disconnect is finished, counters are decremented to 0
        assert pool.current_size == 0
        assert pool.in_flight == 0
        assert pool.checked_out == 0

        # Now acquire succeeds
        pool.set_on_disconnect_hook(None)
        conn_new = pool.acquire("SERVER=dummy_test_746;", None)
        assert conn_new is not None
        assert pool.current_size == 1
        assert pool.checked_out == 1
        pool.release(conn_new)
        pool.close()
        """,
        conn_str,
    )


def test_pool_close_disconnect_keeps_in_flight_until_disconnected(conn_str):
    """Regression test for GH-746: idle connection disconnect in close() retains in-flight accounting.

    When close() drains idle connections from the pool, they must be added to
    _in_flight and only decremented from _in_flight and _current_size after each
    physical disconnect finishes, preventing concurrent acquires from observing freed
    slots while physical sockets are still closing.
    """
    _run_in_subprocess(
        """
        import threading
        from mssql_python import ddbc_bindings

        pool = ddbc_bindings._TestConnectionPool(1, 600)
        pool.set_mock_mode(True)

        # Inject an idle candidate into the pool
        pool.inject_candidate("SERVER=dummy_test_746;", 0, True)
        assert pool.current_size == 1
        assert pool.checked_out == 0
        assert pool.in_flight == 0

        in_disconnect = threading.Event()
        release_disconnect = threading.Event()
        hook_observed = {}

        def on_disconnect():
            hook_observed["current_size"] = pool.current_size
            hook_observed["in_flight"] = pool.in_flight
            hook_observed["checked_out"] = pool.checked_out
            in_disconnect.set()
            assert release_disconnect.wait(timeout=5.0), "Timed out waiting to release disconnect"

        pool.set_on_disconnect_hook(on_disconnect)

        t_err = []
        def run_close():
            try:
                pool.close()
            except Exception as exc:
                t_err.append(exc)

        t = threading.Thread(target=run_close)
        t.start()
        assert in_disconnect.wait(timeout=5.0), "Timed out waiting for disconnect hook in close"

        # Verify hook observed state:
        assert hook_observed["current_size"] == 1
        assert hook_observed["in_flight"] == 1
        assert hook_observed["checked_out"] == 0

        # Concurrent acquire cannot exceed max_size while idle connection is disconnecting
        rejected = False
        try:
            pool.acquire("SERVER=dummy_test_746;", None)
        except RuntimeError as exc:
            if "pool size limit reached" in str(exc):
                rejected = True
        assert rejected, "Concurrent acquire must be rejected while close disconnect is in-flight!"

        # Let close complete
        release_disconnect.set()
        t.join(timeout=5.0)
        assert not t_err, f"Close thread error: {t_err}"

        # Verify clean post-close state
        assert pool.current_size == 0
        assert pool.in_flight == 0
        assert pool.checked_out == 0
        """,
        conn_str,
    )


def test_pool_prune_stale_disconnect_keeps_in_flight_until_disconnected(conn_str):
    """Regression test for GH-746: Phase 1 stale idle pruning retains in-flight capacity.

    When Phase 1 prunes stale idle connections past idle_timeout, they are moved to
    _in_flight and only decremented after physical disconnect in Phase 4 finishes,
    preventing concurrent callers from allocating into slots of disconnecting handles.
    """
    _run_in_subprocess(
        """
        import time
        import threading
        from mssql_python import ddbc_bindings

        # Pool with idle timeout of 0 seconds and max_size=1
        pool = ddbc_bindings._TestConnectionPool(1, 0)
        pool.set_mock_mode(True)

        pool.inject_candidate("SERVER=dummy_test_746;", 0, True)
        assert pool.current_size == 1
        assert pool.checked_out == 0
        assert pool.in_flight == 0

        # Wait for candidate to exceed idle timeout
        time.sleep(1.1)

        in_disconnect = threading.Event()
        release_disconnect = threading.Event()
        hook_observed = {}

        def on_disconnect():
            hook_observed["current_size"] = pool.current_size
            hook_observed["in_flight"] = pool.in_flight
            hook_observed["checked_out"] = pool.checked_out
            in_disconnect.set()
            assert release_disconnect.wait(timeout=5.0), "Timed out waiting to release disconnect"

        pool.set_on_disconnect_hook(on_disconnect)

        t_err = []
        t_conn = []

        def run_acquire():
            try:
                conn = pool.acquire("SERVER=dummy_test_746;", None)
                t_conn.append(conn)
            except Exception as exc:
                t_err.append(exc)

        t = threading.Thread(target=run_acquire)
        t.start()
        assert in_disconnect.wait(timeout=5.0), "Timed out waiting for disconnect hook in Phase 4"

        # Verify hook observed state:
        assert hook_observed["current_size"] == 1
        assert hook_observed["in_flight"] == 1
        assert hook_observed["checked_out"] == 0

        # Concurrent acquire cannot exceed max_size while pruned connection is disconnecting
        rejected = False
        try:
            pool.acquire("SERVER=dummy_test_746;", None)
        except RuntimeError as exc:
            if "pool size limit reached" in str(exc):
                rejected = True
        assert rejected, "Concurrent acquire must be rejected while pruned disconnect is in-flight!"

        # Let disconnect finish
        release_disconnect.set()
        t.join(timeout=5.0)
        assert not t_err, f"Acquire thread error: {t_err}"
        assert len(t_conn) == 1

        # Now newly acquired connection is checked out
        assert pool.current_size == 1
        assert pool.checked_out == 1
        assert pool.in_flight == 0

        pool.set_on_disconnect_hook(None)
        pool.release(t_conn[0])
        pool.close()
        """,
        conn_str,
    )


def test_pool_manager_serializes_same_key_replacement_while_old_pool_closing(conn_str):
    """Regression test for GH-746: serialize replacement-pool creation while old pool is closing.

    When ConnectionPoolManager evicts an evictable pool and begins closing it, concurrent
    acquires for the same key must wait for teardown to finish rather than creating and
    publishing a competing pool before old idle handles are disconnected.
    """
    _run_in_subprocess(
        """
        import threading
        import time
        from mssql_python import ddbc_bindings

        ddbc_bindings._set_pool_manager_mock_mode(True)

        pool_key = "SERVER=dummy_test_746;test_replace_closing"
        conn_str = "SERVER=dummy_test_746;"

        # 1. Enable pooling with max_size=1, idle_timeout=0 so idle pools become evictable
        ddbc_bindings.enable_pooling(1, 0)

        # 2. Acquire a connection from the manager and return it to make the pool idle
        conn_init = ddbc_bindings.Connection(conn_str, True, {}, pool_key, lambda: {})
        conn_init.close()

        # Sleep briefly so idle_time > 0 (idle_timeout=0) makes canEvict() return True
        time.sleep(1.05)

        # 3. Retrieve the internal pool instance for pool_key and attach a disconnect hook
        old_pool = ddbc_bindings._get_pool_for_key(pool_key)
        assert old_pool is not None

        hook_entered = threading.Event()
        proceed_disconnect = threading.Event()
        thread_b_started = threading.Event()
        thread_b_finished = threading.Event()
        thread_b_result = []

        def on_disconnect():
            hook_entered.set()
            # Wait until Thread B has launched its acquire attempt
            thread_b_started.wait(timeout=5.0)
            # Sleep a moment to ensure Thread B enters acquireConnection and waits on _manager_cv
            time.sleep(0.15)
            assert not thread_b_finished.is_set(), "Thread B must be blocked waiting on _closing_keys!"
            proceed_disconnect.wait(timeout=5.0)

        old_pool.set_on_disconnect_hook(on_disconnect)
        # Drop Python reference so it->second.use_count() == 1 allows eviction
        del old_pool

        # 4. Thread A triggers acquireConnection, detecting the evictable pool and calling close()
        thread_a_result = []
        def thread_a_worker():
            try:
                c = ddbc_bindings.Connection(conn_str, True, {}, pool_key, lambda: {})
                thread_a_result.append(c)
            except Exception as e:
                thread_a_result.append(e)

        def thread_b_worker():
            thread_b_started.set()
            try:
                c = ddbc_bindings.Connection(conn_str, True, {}, pool_key, lambda: {})
                thread_b_result.append(c)
            except Exception as e:
                thread_b_result.append(e)
            finally:
                thread_b_finished.set()

        t_a = threading.Thread(target=thread_a_worker)
        t_a.start()

        assert hook_entered.wait(timeout=5.0), "Disconnect hook was not reached"

        # 5. Launch Thread B: tries to acquire for the same key while Thread A is closing old pool
        t_b = threading.Thread(target=thread_b_worker)
        t_b.start()

        # Let Thread A complete the disconnect and pool replacement
        proceed_disconnect.set()

        t_a.join(timeout=5.0)
        t_b.join(timeout=5.0)

        assert len(thread_a_result) == 1 and not isinstance(thread_a_result[0], Exception)
        conn_a = thread_a_result[0]

        # Thread B must have been serialized and checked out from the replacement pool.
        # Since Thread A took the only slot on the replacement pool (max_size=1),
        # Thread B was rejected with 'pool size limit reached'.
        assert len(thread_b_result) == 1
        res_b = thread_b_result[0]
        assert isinstance(res_b, RuntimeError) and "pool size limit reached" in str(res_b), (
            f"Expected pool size limit reached on serialized replacement pool, got: {res_b}"
        )

        # Free slot on replacement pool and verify subsequent acquire succeeds
        expected_pool_id = conn_a.origin_pool_id
        conn_a.close()
        conn_after = ddbc_bindings.Connection(conn_str, True, {}, pool_key, lambda: {})
        assert conn_after.origin_pool_id == expected_pool_id
        conn_after.close()

        ddbc_bindings.close_pooling()
        """,
        conn_str,
    )


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
