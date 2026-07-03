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
# Native token-factory (lazy token acquisition, #659) integration tests
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
    """Integration tests for the native lazy token-factory path (#659)."""

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

        This is the cross-identity isolation guarantee behind #651: the native
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
        assert calls["b"] == 1, (
            "distinct identity key must not reuse another identity's pooled connection"
        )
        conn_b1.close()

        # Re-open under identity A: its own pooled connection is still there ->
        # pool hit -> factory A is NOT called again.
        conn_a2 = ddbc_bindings.Connection(conn_str, True, {}, key_a, factory_a)
        assert calls["a"] == 1, "same identity key should reuse its own pooled connection"
        conn_a2.close()

