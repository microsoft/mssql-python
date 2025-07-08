"""
This file contains tests for the Connection class.
Functions:
- test_connection_string: Check if the connection string is not None.
- test_connection: Check if the database connection is established.
- test_connection_close: Check if the database connection is closed.
- test_commit: Make a transaction and commit.
- test_rollback: Make a transaction and rollback.
- test_invalid_connection_string: Check if initializing with an invalid connection string raises an exception.
Note: The cursor function is not yet implemented, so related tests are commented out.
"""

from mssql_python.exceptions import InterfaceError
import pytest
import time
from mssql_python import Connection, connect, pooling

def drop_table_if_exists(cursor, table_name):
    """Drop the table if it exists"""
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    except Exception as e:
        pytest.fail(f"Failed to drop table {table_name}: {e}")

def test_connection_string(conn_str):
    # Check if the connection string is not None
    assert conn_str is not None, "Connection string should not be None"

def test_connection(db_connection):
    # Check if the database connection is established
    assert db_connection is not None, "Database connection variable should not be None"
    cursor = db_connection.cursor()
    assert cursor is not None, "Database connection failed - Cursor cannot be None"


def test_construct_connection_string(db_connection):
    # Check if the connection string is constructed correctly with kwargs
    conn_str = db_connection._construct_connection_string(host="localhost", user="me", password="mypwd", database="mydb", encrypt="yes", trust_server_certificate="yes")
    assert "Server=localhost;" in conn_str, "Connection string should contain 'Server=localhost;'"
    assert "Uid=me;" in conn_str, "Connection string should contain 'Uid=me;'"
    assert "Pwd=mypwd;" in conn_str, "Connection string should contain 'Pwd=mypwd;'"
    assert "Database=mydb;" in conn_str, "Connection string should contain 'Database=mydb;'"
    assert "Encrypt=yes;" in conn_str, "Connection string should contain 'Encrypt=yes;'"
    assert "TrustServerCertificate=yes;" in conn_str, "Connection string should contain 'TrustServerCertificate=yes;'"
    assert "APP=MSSQL-Python" in conn_str, "Connection string should contain 'APP=MSSQL-Python'"
    assert "Driver={ODBC Driver 18 for SQL Server}" in conn_str, "Connection string should contain 'Driver={ODBC Driver 18 for SQL Server}'"
    assert "Driver={ODBC Driver 18 for SQL Server};;APP=MSSQL-Python;Server=localhost;Uid=me;Pwd=mypwd;Database=mydb;Encrypt=yes;TrustServerCertificate=yes;" == conn_str, "Connection string is incorrect"

def test_connection_string_with_attrs_before(db_connection):
    # Check if the connection string is constructed correctly with attrs_before
    conn_str = db_connection._construct_connection_string(host="localhost", user="me", password="mypwd", database="mydb", encrypt="yes", trust_server_certificate="yes", attrs_before={1256: "token"})
    assert "Server=localhost;" in conn_str, "Connection string should contain 'Server=localhost;'"
    assert "Uid=me;" in conn_str, "Connection string should contain 'Uid=me;'"
    assert "Pwd=mypwd;" in conn_str, "Connection string should contain 'Pwd=mypwd;'"
    assert "Database=mydb;" in conn_str, "Connection string should contain 'Database=mydb;'"
    assert "Encrypt=yes;" in conn_str, "Connection string should contain 'Encrypt=yes;'"
    assert "TrustServerCertificate=yes;" in conn_str, "Connection string should contain 'TrustServerCertificate=yes;'"
    assert "APP=MSSQL-Python" in conn_str, "Connection string should contain 'APP=MSSQL-Python'"
    assert "Driver={ODBC Driver 18 for SQL Server}" in conn_str, "Connection string should contain 'Driver={ODBC Driver 18 for SQL Server}'"
    assert "{1256: token}" not in conn_str, "Connection string should not contain '{1256: token}'"

def test_connection_string_with_odbc_param(db_connection):
    # Check if the connection string is constructed correctly with ODBC parameters
    conn_str = db_connection._construct_connection_string(server="localhost", uid="me", pwd="mypwd", database="mydb", encrypt="yes", trust_server_certificate="yes")
    assert "Server=localhost;" in conn_str, "Connection string should contain 'Server=localhost;'"
    assert "Uid=me;" in conn_str, "Connection string should contain 'Uid=me;'"
    assert "Pwd=mypwd;" in conn_str, "Connection string should contain 'Pwd=mypwd;'"
    assert "Database=mydb;" in conn_str, "Connection string should contain 'Database=mydb;'"
    assert "Encrypt=yes;" in conn_str, "Connection string should contain 'Encrypt=yes;'"
    assert "TrustServerCertificate=yes;" in conn_str, "Connection string should contain 'TrustServerCertificate=yes;'"
    assert "APP=MSSQL-Python" in conn_str, "Connection string should contain 'APP=MSSQL-Python'"
    assert "Driver={ODBC Driver 18 for SQL Server}" in conn_str, "Connection string should contain 'Driver={ODBC Driver 18 for SQL Server}'"
    assert "Driver={ODBC Driver 18 for SQL Server};;APP=MSSQL-Python;Server=localhost;Uid=me;Pwd=mypwd;Database=mydb;Encrypt=yes;TrustServerCertificate=yes;" == conn_str, "Connection string is incorrect"

def test_autocommit_default(db_connection):
    assert db_connection.autocommit is True, "Autocommit should be True by default"

def test_autocommit_setter(db_connection):
    db_connection.autocommit = True
    cursor = db_connection.cursor()
    # Make a transaction and check if it is autocommited
    drop_table_if_exists(cursor, "#pytest_test_autocommit")
    try:
        cursor.execute("CREATE TABLE #pytest_test_autocommit (id INT PRIMARY KEY, value VARCHAR(50));")
        cursor.execute("INSERT INTO #pytest_test_autocommit (id, value) VALUES (1, 'test');")
        cursor.execute("SELECT * FROM #pytest_test_autocommit WHERE id = 1;")
        result = cursor.fetchone()
        assert result is not None, "Autocommit failed: No data found"
        assert result[1] == 'test', "Autocommit failed: Incorrect data"
    except Exception as e:
        pytest.fail(f"Autocommit failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_test_autocommit;")
        db_connection.commit()
    assert db_connection.autocommit is True, "Autocommit should be True"
    
    db_connection.autocommit = False
    cursor = db_connection.cursor()
    # Make a transaction and check if it is not autocommited
    drop_table_if_exists(cursor, "#pytest_test_autocommit")
    try:
        cursor.execute("CREATE TABLE #pytest_test_autocommit (id INT PRIMARY KEY, value VARCHAR(50));")
        cursor.execute("INSERT INTO #pytest_test_autocommit (id, value) VALUES (1, 'test');")
        cursor.execute("SELECT * FROM #pytest_test_autocommit WHERE id = 1;")
        result = cursor.fetchone()
        assert result is not None, "Autocommit failed: No data found"
        assert result[1] == 'test', "Autocommit failed: Incorrect data"
        db_connection.commit()
        cursor.execute("SELECT * FROM #pytest_test_autocommit WHERE id = 1;")
        result = cursor.fetchone()
        assert result is not None, "Autocommit failed: No data found after commit"
        assert result[1] == 'test', "Autocommit failed: Incorrect data after commit"
    except Exception as e:
        pytest.fail(f"Autocommit failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_test_autocommit;")
        db_connection.commit()
    
def test_set_autocommit(db_connection):
    db_connection.setautocommit(True)
    assert db_connection.autocommit is True, "Autocommit should be True"
    db_connection.setautocommit(False)
    assert db_connection.autocommit is False, "Autocommit should be False"

def test_commit(db_connection):
    # Make a transaction and commit
    cursor = db_connection.cursor()
    drop_table_if_exists(cursor, "#pytest_test_commit")
    try:
        cursor.execute("CREATE TABLE #pytest_test_commit (id INT PRIMARY KEY, value VARCHAR(50));")
        cursor.execute("INSERT INTO #pytest_test_commit (id, value) VALUES (1, 'test');")
        db_connection.commit()
        cursor.execute("SELECT * FROM #pytest_test_commit WHERE id = 1;")
        result = cursor.fetchone()
        assert result is not None, "Commit failed: No data found"
        assert result[1] == 'test', "Commit failed: Incorrect data"
    except Exception as e:
        pytest.fail(f"Commit failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_test_commit;")
        db_connection.commit()

def test_rollback(db_connection):
    # Make a transaction and rollback
    cursor = db_connection.cursor()
    drop_table_if_exists(cursor, "#pytest_test_rollback")
    try:
        # Create a table and insert data
        cursor.execute("CREATE TABLE #pytest_test_rollback (id INT PRIMARY KEY, value VARCHAR(50));")
        cursor.execute("INSERT INTO #pytest_test_rollback (id, value) VALUES (1, 'test');")
        db_connection.commit()
        
        # Check if the data is present before rollback
        cursor.execute("SELECT * FROM #pytest_test_rollback WHERE id = 1;")
        result = cursor.fetchone()
        assert result is not None, "Rollback failed: No data found before rollback"
        assert result[1] == 'test', "Rollback failed: Incorrect data"

        # Insert data and rollback
        cursor.execute("INSERT INTO #pytest_test_rollback (id, value) VALUES (2, 'test');")
        db_connection.rollback()
        
        # Check if the data is not present after rollback
        cursor.execute("SELECT * FROM #pytest_test_rollback WHERE id = 2;")
        result = cursor.fetchone()
        assert result is None, "Rollback failed: Data found after rollback"
    except Exception as e:
        pytest.fail(f"Rollback failed: {e}")
    finally:
        cursor.execute("DROP TABLE #pytest_test_rollback;")
        db_connection.commit()

def test_invalid_connection_string():
    # Check if initializing with an invalid connection string raises an exception
    with pytest.raises(Exception):
        Connection("invalid_connection_string")

def test_connection_close(conn_str):
    # Create a separate connection just for this test
    temp_conn = connect(conn_str)
    # Check if the database connection can be closed
    temp_conn.close()    

def test_connection_pooling_speed(conn_str):
    # No pooling
    start_no_pool = time.perf_counter()
    conn1 = connect(conn_str)
    conn1.close()
    end_no_pool = time.perf_counter()
    no_pool_duration = end_no_pool - start_no_pool

    # Second connection
    start2 = time.perf_counter()
    conn2 = connect(conn_str)
    conn2.close()
    end2 = time.perf_counter()
    duration2 = end2 - start2

    # Pooling enabled
    pooling(max_size=2, idle_timeout=10)
    connect(conn_str).close()

    # Pooled connection (should be reused, hence faster)
    start_pool = time.perf_counter()
    conn2 = connect(conn_str)
    conn2.close()
    end_pool = time.perf_counter()
    pool_duration = end_pool - start_pool
    assert pool_duration < no_pool_duration, "Expected faster connection with pooling"

def test_connection_pooling_basic(conn_str):
    # Enable pooling with small pool size
    pooling(max_size=2, idle_timeout=5)
    conn1 = connect(conn_str)
    conn2 = connect(conn_str)
    assert conn1 is not None
    assert conn2 is not None
    try:
        conn3 = connect(conn_str)
        assert conn3 is not None, "Third connection failed — pooling is not working or limit is too strict"
        conn3.close()
    except Exception as e:
        print(f"Expected: Could not open third connection due to max_size=2: {e}")

    conn1.close()
    conn2.close()

# Add these tests at the end of the file

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
    code = """
from mssql_python import connect
conn = connect(\"""" + conn_str + """\")
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
    import subprocess
    import sys
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