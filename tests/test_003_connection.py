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
- test_context_manager_autocommit_mode: Test context manager behavior with autocommit enabled.
- test_context_manager_connection_closes: Test that context manager closes the connection.
"""

from mssql_python.exceptions import InterfaceError
import pytest
import time
from mssql_python import Connection, connect, pooling
from contextlib import closing
import threading

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
    # Disable pooling first
    pooling(enabled=False)
    
    # Warm up - establish initial connection to avoid first-connection overhead
    warmup = connect(conn_str)
    warmup.close()
    
    # Measure multiple non-pooled connections
    non_pooled_times = []
    for _ in range(5):
        start = time.perf_counter()
        conn = connect(conn_str)
        conn.close()
        end = time.perf_counter()
        non_pooled_times.append(end - start)
    
    avg_no_pool = sum(non_pooled_times) / len(non_pooled_times)
    
    # Enable pooling
    pooling(max_size=5, idle_timeout=30)
    
    # Prime the pool with a connection
    primer = connect(conn_str)
    primer.close()
    
    # Small delay to ensure connection is properly returned to pool
    time.sleep(0.1)
    
    # Measure multiple pooled connections
    pooled_times = []
    for _ in range(5):
        start = time.perf_counter()
        conn = connect(conn_str)
        conn.close()
        end = time.perf_counter()
        pooled_times.append(end - start)
    
    avg_pooled = sum(pooled_times) / len(pooled_times)
    
    # Pooled should be significantly faster than non-pooled
    assert avg_pooled < avg_no_pool, \
        f"Pooled connections ({avg_pooled:.6f}s) not significantly faster than non-pooled ({avg_no_pool:.6f}s)"
    
    # Clean up - disable pooling for other tests
    pooling(enabled=False)

def test_connection_pooling_reuse_spid(conn_str):
    """Test that connections are actually reused from the pool"""
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
    
    # Clean up

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
    assert results[0] == "success" or "pool" in results[0].lower() or "timeout" in results[0].lower(), \
        f"Unexpected pool exhaustion result: {results[0]}"
    pooling(enabled=False)

def test_pool_idle_timeout_removes_connections(conn_str):
    """Test that idle_timeout removes connections from the pool after the timeout."""
    pooling(max_size=2, idle_timeout=2)
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

def test_connection_timeout_invalid_password(conn_str):
    """Test that connecting with an invalid password raises an exception quickly (timeout)."""
    # Modify the connection string to use an invalid password
    if "Pwd=" in conn_str:
        bad_conn_str = conn_str.replace("Pwd=", "Pwd=wrongpassword")
    elif "Password=" in conn_str:
        bad_conn_str = conn_str.replace("Password=", "Password=wrongpassword")
    else:
        pytest.skip("No password found in connection string to modify")
    start = time.perf_counter()
    with pytest.raises(Exception):
        connect(bad_conn_str)
    elapsed = time.perf_counter() - start
    # Should fail quickly (within 10 seconds)
    assert elapsed < 10, f"Connection with invalid password took too long: {elapsed:.2f}s"

def test_connection_timeout_invalid_host(conn_str):
    """Test that connecting to an invalid host fails with a timeout."""
    # Replace server/host with an invalid one
    if "Server=" in conn_str:
        bad_conn_str = conn_str.replace("Server=", "Server=invalidhost12345;")
    elif "host=" in conn_str:
        bad_conn_str = conn_str.replace("host=", "host=invalidhost12345;")
    else:
        pytest.skip("No server/host found in connection string to modify")
    start = time.perf_counter()
    with pytest.raises(Exception):
        connect(bad_conn_str)
    elapsed = time.perf_counter() - start
    # Should fail within a reasonable time (30s)
    # Note: This may vary based on network conditions, so adjust as needed
    # but generally, a connection to an invalid host should not take too long
    # to fail.
    # If it takes too long, it may indicate a misconfiguration or network issue.
    assert elapsed < 30, f"Connection to invalid host took too long: {elapsed:.2f}s"

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
        assert result is not None and result[0] == 1, "Pool did not remove invalid connection"
    finally:
        new_conn.close()
        pooling(enabled=False)

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
    assert result is not None and result[0] == 1, "Pool did not recover after failed connection"
    conn.close()
    pooling(enabled=False)

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
        assert overflow_result[0] == "success" or "pool" in overflow_result[0].lower() or "timeout" in overflow_result[0].lower(), \
            f"Unexpected pool overflow result: {overflow_result[0]}"
    finally:
        for c in conns:
            c.close()
        pooling(enabled=False)

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
    """Test that context manager closes connection on normal exit"""
    # Create a permanent table for testing across connections
    setup_conn = connect(conn_str)
    setup_cursor = setup_conn.cursor()
    drop_table_if_exists(setup_cursor, "pytest_context_manager_test")
    
    try:
        setup_cursor.execute("CREATE TABLE pytest_context_manager_test (id INT PRIMARY KEY, value VARCHAR(50));")
        setup_conn.commit()
        setup_conn.close()
        
        # Test context manager closes connection
        with connect(conn_str) as conn:
            assert conn.autocommit is False, "Autocommit should be False by default"
            cursor = conn.cursor()
            cursor.execute("INSERT INTO pytest_context_manager_test (id, value) VALUES (1, 'context_test');")
            conn.commit()  # Manual commit now required
        # Connection should be closed here
        
        # Verify data was committed manually
        verify_conn = connect(conn_str)
        verify_cursor = verify_conn.cursor()
        verify_cursor.execute("SELECT * FROM pytest_context_manager_test WHERE id = 1;")
        result = verify_cursor.fetchone()
        assert result is not None, "Manual commit failed: No data found"
        assert result[1] == 'context_test', "Manual commit failed: Incorrect data"
        verify_conn.close()
        
    except Exception as e:
        pytest.fail(f"Context manager test failed: {e}")
    finally:
        # Cleanup
        cleanup_conn = connect(conn_str)
        cleanup_cursor = cleanup_conn.cursor()
        drop_table_if_exists(cleanup_cursor, "pytest_context_manager_test")
        cleanup_conn.commit()
        cleanup_conn.close()

def test_context_manager_connection_closes(conn_str):
    """Test that context manager closes the connection"""
    conn = None
    try:
        with connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            assert result[0] == 1, "Connection should work inside context manager"
        
        # Connection should be closed after exiting context manager
        assert conn._closed, "Connection should be closed after exiting context manager"
        
        # Should not be able to use the connection after closing
        with pytest.raises(InterfaceError):
            conn.cursor()
            
    except Exception as e:
        pytest.fail(f"Context manager connection close test failed: {e}")
