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
import time
import threading
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

    # Verify the isolation level was set (use DBCC USEROPTIONS to avoid
    # requiring VIEW SERVER PERFORMANCE STATE permission for sys.dm_exec_sessions)
    cursor1 = conn1.cursor()
    cursor1.execute("DBCC USEROPTIONS WITH NO_INFOMSGS")
    isolation_level_1 = None
    for row in cursor1.fetchall():
        if row[0] == "isolation level":
            isolation_level_1 = row[1]
            break
    assert isolation_level_1 == "serializable", f"Expected serializable, got {isolation_level_1}"

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

    # Check if isolation level is reset to default (use DBCC USEROPTIONS to avoid
    # requiring VIEW SERVER PERFORMANCE STATE permission for sys.dm_exec_sessions)
    cursor2.execute("DBCC USEROPTIONS WITH NO_INFOMSGS")
    isolation_level_2 = None
    for row in cursor2.fetchall():
        if row[0] == "isolation level":
            isolation_level_2 = row[1]
            break

    # Verify isolation level is reset to default (READ COMMITTED)
    # This is the CORRECT behavior for connection pooling - we should reset
    # session state to prevent settings from one usage affecting the next
    assert isolation_level_2 == "read committed", (
        f"Isolation level was not reset! Expected 'read committed', got '{isolation_level_2}'. "
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


def test_pool_idle_timeout_removes_connections(conn_str):
    """Test that idle_timeout removes connections from the pool after the timeout."""
    pooling(max_size=2, idle_timeout=1)
    conn1 = connect(conn_str)
    cursor1 = conn1.cursor()
    # Use @@SPID to identify the connection without requiring
    # VIEW SERVER PERFORMANCE STATE permission for sys.dm_exec_connections.
    cursor1.execute("SELECT @@SPID")
    spid1 = cursor1.fetchone()[0]
    conn1.close()

    # Wait well beyond the idle_timeout to account for slow CI and integer-second granularity
    time.sleep(5)

    # Get a new connection — the idle one should have been evicted during acquire()
    conn2 = connect(conn_str)
    cursor2 = conn2.cursor()
    cursor2.execute("SELECT @@SPID")
    spid2 = cursor2.fetchone()[0]
    conn2.close()

    assert spid1 != spid2, "Idle timeout did not remove connection from pool — same SPID reused"


# =============================================================================
# Error Handling and Recovery Tests
# =============================================================================


def test_pool_removes_invalid_connections(conn_str):
    """Test that the pool removes connections that become invalid and recovers gracefully.

    This test simulates a connection being returned to the pool in a dirty state
    (with an open transaction) by calling _conn.close() directly, bypassing the
    normal Python close() which does a rollback. The pool's acquire() should detect
    the bad connection during reset(), discard it, and create a fresh one.
    """
    pooling(max_size=1, idle_timeout=30)
    conn = connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    cursor.fetchone()

    # Record the SPID of the original connection (avoids requiring
    # VIEW SERVER PERFORMANCE STATE permission for sys.dm_exec_connections)
    cursor.execute("SELECT @@SPID")
    original_spid = cursor.fetchone()[0]

    # Force-return the connection to the pool WITHOUT rollback.
    # This leaves the pooled connection in a dirty state (open implicit transaction)
    # which will cause reset() to fail on next acquire().
    conn._conn.close()

    # Python close() will fail since the underlying handle is already gone
    try:
        conn.close()
    except RuntimeError:
        pass

    # Now get a new connection — the pool should discard the dirty one and create fresh
    new_conn = connect(conn_str)
    new_cursor = new_conn.cursor()
    new_cursor.execute("SELECT 1")
    result = new_cursor.fetchone()
    assert result is not None and result[0] == 1, "Pool did not recover from invalid connection"

    # Verify it's a different physical connection
    new_cursor.execute("SELECT @@SPID")
    new_spid = new_cursor.fetchone()[0]
    assert (
        original_spid != new_spid
    ), "Expected a new physical connection after pool discarded the dirty one"

    new_conn.close()


def test_pool_recovery_after_failed_connection(conn_str):
    """Test that the pool recovers after a failed connection attempt."""
    pooling(max_size=1, idle_timeout=30)
    # First, try to connect with a bad password (should fail)
    import re

    # Replace the value of the first Pwd/Password key-value pair with "wrongpassword"
    pattern = re.compile(r"(?i)((?:Pwd|Password)\s*=\s*)([^;]*)")
    bad_conn_str, num_subs = pattern.subn(lambda m: m.group(1) + "wrongpassword", conn_str, count=1)
    if num_subs == 0:
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


def test_pooling_reconfigure_while_enabled(conn_str):
    """Test that calling pooling() with new parameters reconfigures the pool without disable/enable."""
    pooling(max_size=50, idle_timeout=600)
    assert PoolingManager.is_enabled(), "Pooling should be enabled"
    assert PoolingManager._config["max_size"] == 50

    # Reconfigure with smaller pool — should take effect immediately
    pooling(max_size=10, idle_timeout=300)
    assert PoolingManager.is_enabled(), "Pooling should still be enabled after reconfigure"
    assert (
        PoolingManager._config["max_size"] == 10
    ), f"max_size not updated: expected 10, got {PoolingManager._config['max_size']}"
    assert (
        PoolingManager._config["idle_timeout"] == 300
    ), f"idle_timeout not updated: expected 300, got {PoolingManager._config['idle_timeout']}"

    # Verify connections still work after reconfiguration
    conn = connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    assert cursor.fetchone()[0] == 1
    conn.close()


def test_pooling_disable_enable_cycle_state(conn_str):
    """Test that disable >> enable properly resets _pools_closed so a second disable cleans up."""
    pooling(max_size=5, idle_timeout=30)
    conn = connect(conn_str)
    conn.close()

    # Disable — sets _pools_closed = True
    pooling(enabled=False)
    assert not PoolingManager.is_enabled()

    # Re-enable — should reset _pools_closed so future disable works
    pooling(max_size=5, idle_timeout=30)
    assert PoolingManager.is_enabled()
    assert not PoolingManager._pools_closed, "_pools_closed should be False after re-enable"

    conn = connect(conn_str)
    conn.close()

    # Second disable should actually call close_pooling (not skip it)
    pooling(enabled=False)
    assert not PoolingManager.is_enabled()
    assert PoolingManager._pools_closed
