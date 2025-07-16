
"""
This file contains tests for the Connection class.
Functions:
- test_cursor_cleanup_on_connection_close: Tests that cursors are properly cleaned up when the connection is closed.
- test_cursor_cleanup_without_close: Tests that cursors are properly cleaned up without explicitly closing the connection.
- test_no_segfault_on_gc: Tests that no segmentation fault occurs during garbage collection.
- test_multiple_connections_interleaved_cursors: Tests that multiple connections with interleaved cursors do not cause issues.
- test_cursor_outlives_connection: Tests that a cursor can outlive its connection without causing issues.
- test_cursor_weakref_cleanup: Tests that WeakSet properly removes garbage collected cursors.
- test_cursor_cleanup_order_no_segfault: Tests that proper cleanup order prevents segmentation faults.
- test_cursor_close_removes_from_connection: Tests that closing a cursor properly cleans up references.
- test_connection_close_idempotent: Tests that calling close() multiple times is safe.
- test_cursor_after_connection_close: Tests that creating a cursor after closing the connection raises an error.
- test_multiple_cursor_operations_cleanup: Tests cleanup with multiple cursor operations.
"""

import pytest
import subprocess
import sys
from mssql_python import connect, InterfaceError

def drop_table_if_exists(cursor, table_name):
    """Drop the table if it exists"""
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    except Exception as e:
        pytest.fail(f"Failed to drop table {table_name}: {e}")

def test_cursor_cleanup_on_connection_close(conn_str):
    """Test that cursors are properly cleaned up when connection is closed"""
    # Create a new connection for this test
    conn = connect(conn_str)
    
    # Create multiple cursors
    cursor1 = conn.cursor()
    cursor2 = conn.cursor()
    cursor3 = conn.cursor()
    
    # Execute something on each cursor to ensure they have statement handles
    # Option 1: Fetch results immediately to free the connection
    cursor1.execute("SELECT 1")
    cursor1.fetchall() 
    
    cursor2.execute("SELECT 2")
    cursor2.fetchall()
    
    cursor3.execute("SELECT 3")
    cursor3.fetchall()

    # Close one cursor explicitly
    cursor1.close()
    assert cursor1.closed is True, "Cursor1 should be closed"
    
    # Close the connection (should clean up remaining cursors)
    conn.close()
    
    # Verify all cursors are closed
    assert cursor1.closed is True, "Cursor1 should remain closed"
    assert cursor2.closed is True, "Cursor2 should be closed by connection.close()"
    assert cursor3.closed is True, "Cursor3 should be closed by connection.close()"

def test_cursor_cleanup_without_close(conn_str):
    """Test that cursors are properly cleaned up without closing the connection"""
    conn_new = connect(conn_str)
    cursor = conn_new.cursor()
    cursor.execute("SELECT 1")
    cursor.fetchall()
    assert len(conn_new._cursors) == 1
    del cursor # Remove the last reference
    assert len(conn_new._cursors) == 0  # Now the WeakSet should be empty

def test_no_segfault_on_gc(conn_str):
    """Test that no segmentation fault occurs during garbage collection"""
    # Properly escape the connection string for embedding in code
    escaped_conn_str = conn_str.replace('\\', '\\\\').replace('"', '\\"')
    code = f"""
from mssql_python import connect
conn = connect("{escaped_conn_str}")
cursors = [conn.cursor() for _ in range(5)]
for cur in cursors:
    cur.execute("SELECT 1")
    cur.fetchall()
del conn
import gc; gc.collect()
del cursors
gc.collect()
    """
    # Run the code in a subprocess to avoid segfaults in the main process
    # This is a workaround to test for segfaults in Python, as they can crash the interpreter
    # and pytest does not handle segfaults gracefully.
    # Note: This is a simplified example; in practice, you might want to use a more robust method
    # to handle subprocesses and capture their output/errors.
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert result.returncode == 0, f"Expected no segfault, but got: {result.stderr}"

def test_multiple_connections_interleaved_cursors(conn_str):
    # Properly escape the connection string for embedding in code
    escaped_conn_str = conn_str.replace('\\', '\\\\').replace('"', '\\"')
    code = f"""
from mssql_python import connect
conns = [connect("{escaped_conn_str}") for _ in range(3)]
cursors = []
for conn in conns:
    # Create a cursor for each connection and execute a simple query
    cursor = conn.cursor()
    cursor.execute('SELECT 1')
    cursor.fetchall()
    cursors.append(cursor)
del conns
import gc; gc.collect()
del cursors
gc.collect()
"""
    # Run the code in a subprocess to avoid segfaults in the main process
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert result.returncode == 0, f"Expected no segfault, but got: {result.stderr}"

def test_cursor_outlives_connection(conn_str):
    # Properly escape the connection string for embedding in code
    escaped_conn_str = conn_str.replace('\\', '\\\\').replace('"', '\\"')
    code = f"""
from mssql_python import connect
conn = connect("{escaped_conn_str}")
cursor = conn.cursor()
cursor.execute("SELECT 1")
cursor.fetchall()
del conn
import gc; gc.collect()
cursor.execute("SELECT 2")
del cursor
gc.collect()
"""
    # Run the code in a subprocess to avoid segfaults in the main process
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert result.returncode == 0, f"Expected no segfault, but got: {result.stderr}"

def test_cursor_weakref_cleanup(conn_str):
    """Test that WeakSet properly removes garbage collected cursors"""
    conn = connect(conn_str)
    
    # Create cursors
    cursor1 = conn.cursor()
    cursor2 = conn.cursor()
    
    # Check initial cursor count
    assert len(conn._cursors) == 2, "Should have 2 cursors"
    
    # Delete reference to cursor1 (should be garbage collected)
    cursor1_id = id(cursor1)
    del cursor1
    
    # Force garbage collection
    import gc
    gc.collect()
    
    # Check cursor count after garbage collection
    assert len(conn._cursors) == 1, "Should have 1 cursor after garbage collection"
    
    # Verify cursor2 is still there
    assert cursor2 in conn._cursors, "Cursor2 should still be in the set"
    
    conn.close()

def test_cursor_cleanup_order_no_segfault(conn_str):
    """Test that proper cleanup order prevents segfaults"""
    # This test ensures cursors are cleaned before connection
    conn = connect(conn_str)
    
    # Create multiple cursors with active statements
    cursors = []
    for i in range(5):
        cursor = conn.cursor()
        cursor.execute(f"SELECT {i}")
        cursor.fetchall()
        cursors.append(cursor)
    
    # Don't close any cursors explicitly
    # Just close the connection - it should handle cleanup properly
    conn.close()
    
    # Verify all cursors were closed
    for cursor in cursors:
        assert cursor.closed is True, "All cursors should be closed"

def test_cursor_close_removes_from_connection(conn_str):
    """Test that closing a cursor properly cleans up references"""
    conn = connect(conn_str)
    
    # Create cursors
    cursor1 = conn.cursor()
    cursor2 = conn.cursor()
    cursor3 = conn.cursor()
    
    assert len(conn._cursors) == 3, "Should have 3 cursors"
    
    # Close cursor2
    cursor2.close()
    
    # cursor2 should still be in the WeakSet (until garbage collected)
    # but it should be marked as closed
    assert cursor2.closed is True, "Cursor2 should be closed"
    
    # Delete the reference and force garbage collection
    del cursor2
    import gc
    gc.collect()
    
    # Now should have 2 cursors
    assert len(conn._cursors) == 2, "Should have 2 cursors after closing and GC"
    
    conn.close()

def test_connection_close_idempotent(conn_str):
    """Test that calling close() multiple times is safe"""
    conn = connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    
    # First close
    conn.close()
    assert conn._closed is True, "Connection should be closed"
    
    # Second close (should not raise exception)
    conn.close()
    assert conn._closed is True, "Connection should remain closed"
    
    # Cursor should also be closed
    assert cursor.closed is True, "Cursor should be closed"

def test_cursor_after_connection_close(conn_str):
    """Test that creating cursor after connection close raises error"""
    conn = connect(conn_str)
    conn.close()
    
    # Should raise exception when trying to create cursor on closed connection
    with pytest.raises(InterfaceError) as excinfo:
        cursor = conn.cursor()
    
    assert "closed connection" in str(excinfo.value).lower(), "Should mention closed connection"

def test_multiple_cursor_operations_cleanup(conn_str):
    """Test cleanup with multiple cursor operations"""
    conn = connect(conn_str)
    
    # Create table for testing
    cursor_setup = conn.cursor()
    drop_table_if_exists(cursor_setup, "#test_cleanup")
    cursor_setup.execute("CREATE TABLE #test_cleanup (id INT, value VARCHAR(50))")
    cursor_setup.close()
    
    # Create multiple cursors doing different operations
    cursor_insert = conn.cursor()
    cursor_insert.execute("INSERT INTO #test_cleanup VALUES (1, 'test1'), (2, 'test2')")
    
    cursor_select1 = conn.cursor()
    cursor_select1.execute("SELECT * FROM #test_cleanup WHERE id = 1")
    cursor_select1.fetchall()
    
    cursor_select2 = conn.cursor()
    cursor_select2.execute("SELECT * FROM #test_cleanup WHERE id = 2")
    cursor_select2.fetchall()

    # Close connection without closing cursors
    conn.close()
    
    # All cursors should be closed
    assert cursor_insert.closed is True
    assert cursor_select1.closed is True
    assert cursor_select2.closed is True