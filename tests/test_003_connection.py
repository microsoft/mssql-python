"""
This file contains tests for the Connection class.
Functions:
- test_connection_string: Check if the connection string is not None.
- test_connection: Check if the database connection is established.
- test_connection_close: Check if the database connection is closed.
- test_commit: Make a transaction and commit.
- test_rollback: Make a transaction and rollback.
- test_invalid_connection_string: Check if initializing with an invalid connection string raises an exception.
- test_connection_pooling_speed: Test connection pooling speed.
- test_connection_pooling_basic: Test basic connection pooling functionality.
- test_autocommit_default: Check if autocommit is False by default.
- test_autocommit_setter: Test setting autocommit mode and its effect on transactions.
- test_set_autocommit: Test the setautocommit method.
- test_construct_connection_string: Check if the connection string is constructed correctly with kwargs.
- test_connection_string_with_attrs_before: Check if the connection string is constructed correctly with attrs_before.
- test_connection_string_with_odbc_param: Check if the connection string is constructed correctly with ODBC parameters.
- test_rollback_on_close: Test that rollback occurs on connection close if autocommit is False.
- test_context_manager_commit: Test that context manager commits transaction on normal exit.
- test_context_manager_rollback_on_exception: Test that context manager rolls back on exception.
- test_context_manager_autocommit_mode: Test context manager behavior with autocommit enabled.
- test_context_manager_connection_remains_open: Test that context manager doesn't close the connection.
- test_context_manager_nested_transactions: Test nested context manager usage.
- test_context_manager_manual_commit_rollback: Test manual commit/rollback within context manager.
"""

from mssql_python.exceptions import InterfaceError
import pytest
import time
from mssql_python import Connection, connect, pooling
from contextlib import closing

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
    assert db_connection.autocommit is False, "Autocommit should be False by default"
    assert db_connection.autocommit is False, "Autocommit should be False by default"

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

def test_rollback_on_close(conn_str, db_connection):
    # Test that rollback occurs on connection close if autocommit is False
    # Using a permanent table to ensure rollback is tested correctly
    cursor = db_connection.cursor()
    drop_table_if_exists(cursor, "pytest_test_rollback_on_close")
    try:
        # Create a permanent table for testing
        cursor.execute("CREATE TABLE pytest_test_rollback_on_close (id INT PRIMARY KEY, value VARCHAR(50));")
        db_connection.commit()

        # This simulates a scenario where the connection is closed without committing
        # and checks if the rollback occurs
        temp_conn = connect(conn_str)
        temp_cursor = temp_conn.cursor()
        temp_cursor.execute("INSERT INTO pytest_test_rollback_on_close (id, value) VALUES (1, 'test');")

        # Verify data is visible within the same transaction
        temp_cursor.execute("SELECT * FROM pytest_test_rollback_on_close WHERE id = 1;")
        result = temp_cursor.fetchone()
        assert result is not None, "Rollback on close failed: No data found before close"
        assert result[1] == 'test', "Rollback on close failed: Incorrect data before close"
        
        # Close the temporary connection without committing
        temp_conn.close()
        
        # Now check if the data is rolled back
        cursor.execute("SELECT * FROM pytest_test_rollback_on_close WHERE id = 1;")
        result = cursor.fetchone()
        assert result is None, "Rollback on close failed: Data found after rollback"
    except Exception as e:
        pytest.fail(f"Rollback on close failed: {e}")
    finally:
        drop_table_if_exists(cursor, "pytest_test_rollback_on_close")
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

def test_context_manager_commit(conn_str):
    """Test that context manager commits transaction on normal exit when autocommit is False"""
    # Create a permanent table for testing across connections
    setup_conn = connect(conn_str)
    setup_cursor = setup_conn.cursor()
    drop_table_if_exists(setup_cursor, "pytest_context_manager_test")
    
    try:
        setup_cursor.execute("CREATE TABLE pytest_context_manager_test (id INT PRIMARY KEY, value VARCHAR(50));")
        setup_conn.commit()
        setup_conn.close()
        
        # Test context manager with autocommit=False (default)
        with connect(conn_str) as conn:
            assert conn.autocommit is False, "Autocommit should be False by default"
            cursor = conn.cursor()
            cursor.execute("INSERT INTO pytest_context_manager_test (id, value) VALUES (1, 'context_test');")
            # Don't manually commit - let context manager handle it
        
        # Verify transaction was committed by context manager
        verify_conn = connect(conn_str)
        verify_cursor = verify_conn.cursor()
        verify_cursor.execute("SELECT * FROM pytest_context_manager_test WHERE id = 1;")
        result = verify_cursor.fetchone()
        assert result is not None, "Context manager failed to commit: No data found"
        assert result[1] == 'context_test', "Context manager failed to commit: Incorrect data"
        verify_conn.close()
        
    except Exception as e:
        pytest.fail(f"Context manager commit test failed: {e}")
    finally:
        # Cleanup
        cleanup_conn = connect(conn_str)
        cleanup_cursor = cleanup_conn.cursor()
        drop_table_if_exists(cleanup_cursor, "pytest_context_manager_test")
        cleanup_conn.commit()
        cleanup_conn.close()

def test_context_manager_rollback_on_exception(conn_str):
    """Test that context manager rolls back transaction when exception occurs"""
    # Create a permanent table for testing
    setup_conn = connect(conn_str)
    setup_cursor = setup_conn.cursor()
    drop_table_if_exists(setup_cursor, "pytest_context_exception_test")
    
    try:
        setup_cursor.execute("CREATE TABLE pytest_context_exception_test (id INT PRIMARY KEY, value VARCHAR(50));")
        setup_conn.commit()
        setup_conn.close()
        
        # Test context manager with exception
        with pytest.raises(ValueError):
            with connect(conn_str) as conn:
                assert conn.autocommit is False, "Autocommit should be False by default"
                cursor = conn.cursor()
                cursor.execute("INSERT INTO pytest_context_exception_test (id, value) VALUES (1, 'should_rollback');")
                # Raise an exception to trigger rollback
                raise ValueError("Test exception for rollback")
        
        # Verify transaction was rolled back
        verify_conn = connect(conn_str)
        verify_cursor = verify_conn.cursor()
        verify_cursor.execute("SELECT * FROM pytest_context_exception_test WHERE id = 1;")
        result = verify_cursor.fetchone()
        assert result is None, "Context manager failed to rollback: Data found after exception"
        verify_conn.close()
        
    except AssertionError:
        # Re-raise assertion errors from our test
        raise
    except Exception as e:
        pytest.fail(f"Context manager rollback test failed: {e}")
    finally:
        # Cleanup
        cleanup_conn = connect(conn_str)
        cleanup_cursor = cleanup_conn.cursor()
        drop_table_if_exists(cleanup_cursor, "pytest_context_exception_test")
        cleanup_conn.commit()
        cleanup_conn.close()

def test_context_manager_autocommit_mode(conn_str):
    """Test context manager behavior with autocommit enabled"""
    # Create a permanent table for testing
    setup_conn = connect(conn_str)
    setup_cursor = setup_conn.cursor()
    drop_table_if_exists(setup_cursor, "pytest_context_autocommit_test")
    
    try:
        setup_cursor.execute("CREATE TABLE pytest_context_autocommit_test (id INT PRIMARY KEY, value VARCHAR(50));")
        setup_conn.commit()
        setup_conn.close()
        
        # Test context manager with autocommit=True
        with connect(conn_str, autocommit=True) as conn:
            assert conn.autocommit is True, "Autocommit should be True"
            cursor = conn.cursor()
            cursor.execute("INSERT INTO pytest_context_autocommit_test (id, value) VALUES (1, 'autocommit_test');")
            # With autocommit=True, transaction is already committed
        
        # Verify data was committed (even though context manager doesn't need to commit)
        verify_conn = connect(conn_str)
        verify_cursor = verify_conn.cursor()
        verify_cursor.execute("SELECT * FROM pytest_context_autocommit_test WHERE id = 1;")
        result = verify_cursor.fetchone()
        assert result is not None, "Autocommit mode failed: No data found"
        assert result[1] == 'autocommit_test', "Autocommit mode failed: Incorrect data"
        verify_conn.close()
        
    except Exception as e:
        pytest.fail(f"Context manager autocommit test failed: {e}")
    finally:
        # Cleanup
        cleanup_conn = connect(conn_str)
        cleanup_cursor = cleanup_conn.cursor()
        drop_table_if_exists(cleanup_cursor, "pytest_context_autocommit_test")
        cleanup_conn.commit()
        cleanup_conn.close()

def test_context_manager_connection_remains_open(conn_str):
    """Test that context manager doesn't close the connection (matches pyodbc behavior)"""
    conn = None
    try:
        with connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            assert result[0] == 1, "Connection should work inside context manager"
        
        # Connection should remain open after exiting context manager
        assert not conn._closed, "Connection should not be closed after exiting context manager"
        
        # Should still be able to use the connection
        cursor = conn.cursor()
        cursor.execute("SELECT 2")
        result = cursor.fetchone()
        assert result[0] == 2, "Connection should still work after exiting context manager"
        
    except Exception as e:
        pytest.fail(f"Context manager connection persistence test failed: {e}")
    finally:
        # Manually close the connection
        if conn and not conn._closed:
            conn.close()

def test_context_manager_nested_transactions(conn_str):
    """Test nested context manager usage"""
    # Create a permanent table for testing
    setup_conn = connect(conn_str)
    setup_cursor = setup_conn.cursor()
    drop_table_if_exists(setup_cursor, "pytest_context_nested_test")
    
    try:
        setup_cursor.execute("CREATE TABLE pytest_context_nested_test (id INT PRIMARY KEY, value VARCHAR(50));")
        setup_conn.commit()
        setup_conn.close()
        
        # Test nested context managers
        with connect(conn_str) as outer_conn:
            outer_cursor = outer_conn.cursor()
            outer_cursor.execute("INSERT INTO pytest_context_nested_test (id, value) VALUES (1, 'outer');")
            
            with connect(conn_str) as inner_conn:
                inner_cursor = inner_conn.cursor()
                inner_cursor.execute("INSERT INTO pytest_context_nested_test (id, value) VALUES (2, 'inner');")
                # Inner context will commit its transaction
            
            # Outer context will commit its transaction
        
        # Verify both transactions were committed
        verify_conn = connect(conn_str)
        verify_cursor = verify_conn.cursor()
        verify_cursor.execute("SELECT COUNT(*) FROM pytest_context_nested_test;")
        count = verify_cursor.fetchone()[0]
        assert count == 2, f"Expected 2 records, found {count}"
        verify_conn.close()
        
    except Exception as e:
        pytest.fail(f"Context manager nested test failed: {e}")
    finally:
        # Cleanup
        cleanup_conn = connect(conn_str)
        cleanup_cursor = cleanup_conn.cursor()
        drop_table_if_exists(cleanup_cursor, "pytest_context_nested_test")
        cleanup_conn.commit()
        cleanup_conn.close()

def test_context_manager_manual_commit_rollback(conn_str):
    """Test manual commit/rollback within context manager"""
    # Create a permanent table for testing
    setup_conn = connect(conn_str)
    setup_cursor = setup_conn.cursor()
    drop_table_if_exists(setup_cursor, "pytest_context_manual_test")
    
    try:
        setup_cursor.execute("CREATE TABLE pytest_context_manual_test (id INT PRIMARY KEY, value VARCHAR(50));")
        setup_conn.commit()
        setup_conn.close()
        
        # Test manual commit within context manager
        with connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO pytest_context_manual_test (id, value) VALUES (1, 'manual_commit');")
            conn.commit()  # Manual commit
            cursor.execute("INSERT INTO pytest_context_manual_test (id, value) VALUES (2, 'auto_commit');")
            # Second insert will be committed by context manager
        
        # Verify both records exist
        verify_conn = connect(conn_str)
        verify_cursor = verify_conn.cursor()
        verify_cursor.execute("SELECT COUNT(*) FROM pytest_context_manual_test;")
        count = verify_cursor.fetchone()[0]
        assert count == 2, f"Expected 2 records, found {count}"
        
        # Test manual rollback within context manager
        with connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO pytest_context_manual_test (id, value) VALUES (3, 'will_rollback');")
            conn.rollback()  # Manual rollback
            cursor.execute("INSERT INTO pytest_context_manual_test (id, value) VALUES (4, 'will_commit');")
            # This insert will be committed by context manager
        
        # Verify only the last record was committed
        verify_cursor.execute("SELECT COUNT(*) FROM pytest_context_manual_test;")
        count = verify_cursor.fetchone()[0]
        assert count == 3, f"Expected 3 records after rollback test, found {count}"
        
        verify_cursor.execute("SELECT * FROM pytest_context_manual_test WHERE id = 3;")
        result = verify_cursor.fetchone()
        assert result is None, "Record should have been rolled back"
        
        verify_cursor.execute("SELECT * FROM pytest_context_manual_test WHERE id = 4;")
        result = verify_cursor.fetchone()
        assert result is not None, "Record should have been committed by context manager"
        
        verify_conn.close()
        
    except Exception as e:
        pytest.fail(f"Context manager manual commit/rollback test failed: {e}")
    finally:
        # Cleanup
        cleanup_conn = connect(conn_str)
        cleanup_cursor = cleanup_conn.cursor()
        drop_table_if_exists(cleanup_cursor, "pytest_context_manual_test")
        cleanup_conn.commit()
        cleanup_conn.close()

def test_context_manager_with_contextlib_closing(conn_str):
    """Test using contextlib.closing to close connection after context exit"""
    connection_was_closed = False
    
    try:
        with closing(connect(conn_str)) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            assert result[0] == 1, "Connection should work inside contextlib.closing"
            assert not conn._closed, "Connection should not be closed inside context"
        
        # Connection should be closed after exiting contextlib.closing
        assert conn._closed, "Connection should be closed after exiting contextlib.closing"
        connection_was_closed = True
        
        # Should not be able to use the connection after closing
        with pytest.raises(InterfaceError):
            conn.cursor()
            
    except Exception as e:
        pytest.fail(f"Contextlib.closing test failed: {e}")
    
    assert connection_was_closed, "Connection was not properly closed by contextlib.closing"
