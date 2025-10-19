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
        results[0] == "success"
        or "pool" in results[0].lower()
        or "timeout" in results[0].lower()
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


@pytest.mark.skip("Flaky test - idle timeout behavior needs investigation")
def test_pool_idle_timeout_removes_connections(conn_str):
    """Test that idle_timeout removes connections from the pool after the timeout."""
    pooling(max_size=2, idle_timeout=1)
    conn1 = connect(conn_str)
    spid_list = []
    cursor1 = conn1.cursor()
    cursor1.execute("SELECT @@SPID")
    spid1 = cursor1.fetchone()[0]
    spid_list.append(spid1)
    conn1.close()

    # Wait for longer than idle_timeout
    time.sleep(3)

    # Get a new connection, which should not reuse the previous SPID
    conn2 = connect(conn_str)
    cursor2 = conn2.cursor()
    cursor2.execute("SELECT @@SPID")
    spid2 = cursor2.fetchone()[0]
    spid_list.append(spid2)
    conn2.close()

    assert spid1 != spid2, "Idle timeout did not remove connection from pool"


# =============================================================================
# Error Handling and Recovery Tests
# =============================================================================


def test_pool_removes_invalid_connections(conn_str):
    """Test that the pool removes connections that become invalid (simulate by closing underlying connection)."""
    pooling(max_size=1, idle_timeout=30)
    conn = connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    # Simulate invalidation by forcibly closing the connection at the driver level
    try:
        # Try to access a private attribute or method to forcibly close the underlying connection
        # This is implementation-specific; if not possible, skip
        if hasattr(conn, "_conn") and hasattr(conn._conn, "close"):
            conn._conn.close()
        else:
            pytest.skip("Cannot forcibly close underlying connection for this driver")
    except Exception:
        pass
    # Safely close the connection, ignoring errors due to forced invalidation
    try:
        conn.close()
    except RuntimeError as e:
        if "not initialized" not in str(e):
            raise
    # Now, get a new connection from the pool and ensure it works
    new_conn = connect(conn_str)
    new_cursor = new_conn.cursor()
    try:
        new_cursor.execute("SELECT 1")
        result = new_cursor.fetchone()
        assert (
            result is not None and result[0] == 1
        ), "Pool did not remove invalid connection"
    finally:
        new_conn.close()


def test_pool_recovery_after_failed_connection(conn_str):
    """Test that the pool recovers after a failed connection attempt."""
    pooling(max_size=1, idle_timeout=30)
    # First, try to connect with a bad password (should fail)
    if "Pwd=" in conn_str:
        bad_conn_str = conn_str.replace("Pwd=", "Pwd=wrongpassword")
    elif "Password=" in conn_str:
        bad_conn_str = conn_str.replace("Password=", "Password=wrongpassword")
    else:
        pytest.skip("No password found in connection string to modify")
    with pytest.raises(Exception):
        connect(bad_conn_str)
    # Now, connect with the correct string and ensure it works
    conn = connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    result = cursor.fetchone()
    assert (
        result is not None and result[0] == 1
    ), "Pool did not recover after failed connection"
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
    print(
        f"pooling(enabled=False) with unclosed connection completed in {elapsed:.3f}s"
    )


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
    assert (
        elapsed < 2.0
    ), f"Multiple pooling disable calls took too long: {elapsed:.2f}s"
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

        assert (
            not PoolingManager.is_enabled()
        ), f"Pooling not disabled in cycle {cycle + 1}"
        assert (
            elapsed < 2.0
        ), f"Disable took too long in cycle {cycle + 1}: {elapsed:.2f}s"

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
    assert (
        PoolingManager.is_initialized()
    ), "Should remain initialized after disable call"

    print("Pooling state consistency verified")
