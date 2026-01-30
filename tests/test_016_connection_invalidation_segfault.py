"""
Test for connection invalidation segfault scenario (Issue: Use-after-free on statement handles)

This test reproduces the segfault that occurred in SQLAlchemy's RealReconnectTest
when connection invalidation triggered automatic freeing of child statement handles
by the ODBC driver, followed by Python GC attempting to free the same handles again.

The fix uses state tracking where Connection marks child handles as "implicitly freed"
before disconnecting, preventing SqlHandle::free() from calling ODBC functions on
already-freed handles.

Background:
- When Connection::disconnect() frees a DBC handle, ODBC automatically frees all child STMT handles
- Python SqlHandle objects weren't aware of this implicit freeing
- GC later tried to free those handles again via SqlHandle::free(), causing use-after-free
- Fix: Connection tracks children in _childStatementHandles vector and marks them as
  implicitly freed before DBC is freed
"""

import gc
import pytest
from mssql_python import connect, DatabaseError, OperationalError


def test_connection_invalidation_with_multiple_cursors(conn_str):
    """
    Test connection invalidation scenario that previously caused segfaults.

    This test:
    1. Creates a connection with multiple active cursors
    2. Executes queries on those cursors to create statement handles
    3. Simulates connection invalidation by closing the connection
    4. Forces garbage collection to trigger handle cleanup
    5. Verifies no segfault occurs during the cleanup process

    Previously, this would crash because:
    - Closing connection freed the DBC handle
    - ODBC driver automatically freed all child STMT handles
    - Python GC later tried to free those same STMT handles
    - Result: use-after-free crash (segfault)

    With the fix:
    - Connection marks all child handles as "implicitly freed" before closing
    - SqlHandle::free() checks the flag and skips ODBC calls if true
    - Result: No crash, clean shutdown
    """
    # Create connection
    conn = connect(conn_str)

    # Create multiple cursors with statement handles
    cursors = []
    for i in range(5):
        cursor = conn.cursor()
        cursor.execute("SELECT 1 AS id, 'test' AS name")
        cursor.fetchall()  # Fetch results to complete the query
        cursors.append(cursor)

    # Close connection without explicitly closing cursors first
    # This simulates the invalidation scenario where connection is lost
    conn.close()

    # Force garbage collection to trigger cursor cleanup
    # This is where the segfault would occur without the fix
    cursors = None
    gc.collect()

    # If we reach here without crashing, the fix is working
    assert True


def test_connection_invalidation_without_cursor_close(conn_str):
    """
    Test that cursors are properly cleaned up when connection is closed
    without explicitly closing the cursors.

    This mimics the SQLAlchemy scenario where connection pools may
    invalidate connections without first closing all cursors.
    """
    conn = connect(conn_str)

    # Create cursors and execute queries
    cursor1 = conn.cursor()
    cursor1.execute("SELECT 1")
    cursor1.fetchone()

    cursor2 = conn.cursor()
    cursor2.execute("SELECT 2")
    cursor2.fetchone()

    cursor3 = conn.cursor()
    cursor3.execute("SELECT 3")
    cursor3.fetchone()

    # Close connection with active cursors
    conn.close()

    # Trigger GC - should not crash
    del cursor1, cursor2, cursor3
    gc.collect()

    assert True


def test_repeated_connection_invalidation_cycles(conn_str):
    """
    Test repeated connection invalidation cycles to ensure no memory
    corruption or handle leaks occur across multiple iterations.

    This stress test simulates the scenario from SQLAlchemy's
    RealReconnectTest which ran multiple invalidation tests in sequence.
    """
    for iteration in range(10):
        # Create connection
        conn = connect(conn_str)

        # Create and use cursors
        for cursor_num in range(3):
            cursor = conn.cursor()
            cursor.execute(f"SELECT {iteration} AS iteration, {cursor_num} AS cursor_num")
            result = cursor.fetchone()
            assert result[0] == iteration
            assert result[1] == cursor_num

        # Close connection (invalidate)
        conn.close()

        # Force GC after each iteration
        gc.collect()

    # Final GC to clean up any remaining references
    gc.collect()
    assert True


def test_connection_close_with_uncommitted_transaction(conn_str):
    """
    Test that closing a connection with an uncommitted transaction
    properly cleans up statement handles without crashing.
    """
    conn = connect(conn_str)
    cursor = conn.cursor()

    try:
        # Start a transaction
        cursor.execute("CREATE TABLE #temp_test (id INT, name VARCHAR(50))")
        cursor.execute("INSERT INTO #temp_test VALUES (1, 'test')")
        # Don't commit - leave transaction open

        # Close connection without commit or rollback
        conn.close()

        # Trigger GC
        del cursor
        gc.collect()

        assert True
    except Exception as e:
        pytest.fail(f"Unexpected exception during connection close: {e}")


def test_cursor_after_connection_invalidation_raises_error(conn_str):
    """
    Test that attempting to use a cursor after connection is closed
    raises an appropriate error rather than crashing.
    """
    conn = connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    cursor.fetchone()

    # Close connection
    conn.close()

    # Attempting to execute on cursor should raise an error, not crash
    with pytest.raises((DatabaseError, OperationalError)):
        cursor.execute("SELECT 2")

    # GC should not crash
    del cursor
    gc.collect()


def test_multiple_connections_concurrent_invalidation(conn_str):
    """
    Test that multiple connections can be invalidated concurrently
    without interfering with each other's handle cleanup.
    """
    connections = []
    all_cursors = []

    # Create multiple connections with cursors
    for conn_num in range(5):
        conn = connect(conn_str)
        connections.append(conn)

        for cursor_num in range(3):
            cursor = conn.cursor()
            cursor.execute(f"SELECT {conn_num} AS conn, {cursor_num} AS cursor_num")
            cursor.fetchone()
            all_cursors.append(cursor)

    # Close all connections
    for conn in connections:
        conn.close()

    # Verify we have cursors alive (keep them referenced until after connection close)
    assert len(all_cursors) == 15  # 5 connections * 3 cursors each

    # Clear references and force GC
    connections = None
    all_cursors = None
    gc.collect()

    assert True


def test_connection_invalidation_with_prepared_statements(conn_str):
    """
    Test connection invalidation when cursors have prepared statements.
    This ensures statement handles are properly marked as implicitly freed.
    """
    conn = connect(conn_str)

    # Create cursor with parameterized query (prepared statement)
    cursor = conn.cursor()
    cursor.execute("SELECT ? AS value", (42,))
    result = cursor.fetchone()
    assert result[0] == 42

    # Execute another parameterized query
    cursor.execute("SELECT ? AS name, ? AS age", ("John", 30))
    result = cursor.fetchone()
    assert result[0] == "John"
    assert result[1] == 30

    # Close connection with prepared statements
    conn.close()

    # GC should handle cleanup without crash
    del cursor
    gc.collect()

    assert True


def test_verify_sqlhandle_free_method_exists():
    """
    Verify that the free method exists on SqlHandle.
    The segfault fix uses markImplicitlyFreed internally in C++ (not exposed to Python).
    """
    from mssql_python import ddbc_bindings

    # Verify free method exists
    assert hasattr(ddbc_bindings.SqlHandle, "free"), "SqlHandle should have free method"


def test_connection_invalidation_with_fetchall(conn_str):
    """
    Test connection invalidation when cursors have fetched all results.
    This ensures all statement handle states are properly cleaned up.
    """
    conn = connect(conn_str)

    cursor = conn.cursor()
    cursor.execute("SELECT number FROM (VALUES (1), (2), (3), (4), (5)) AS numbers(number)")
    results = cursor.fetchall()
    assert len(results) == 5

    # Close connection after fetchall
    conn.close()

    # GC cleanup should work without issues
    del cursor
    gc.collect()

    assert True


def test_nested_connection_cursor_cleanup(conn_str):
    """
    Test nested connection/cursor creation and cleanup pattern.
    This mimics complex application patterns where connections
    and cursors are created in nested scopes.
    """

    def inner_function(connection):
        cursor = connection.cursor()
        cursor.execute("SELECT 'inner' AS scope")
        return cursor.fetchone()

    def outer_function(conn_str):
        conn = connect(conn_str)
        result = inner_function(conn)
        conn.close()
        return result

    # Run multiple times to ensure no accumulated state issues
    for _ in range(5):
        result = outer_function(conn_str)
        assert result[0] == "inner"
        gc.collect()

    # Final cleanup
    gc.collect()
    assert True
