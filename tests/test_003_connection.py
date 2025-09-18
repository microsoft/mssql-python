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

from mssql_python.exceptions import InterfaceError, ProgrammingError
import mssql_python
import pytest
import time
from mssql_python import connect, Connection, pooling, SQL_CHAR, SQL_WCHAR
from contextlib import closing
import threading

# Import all exception classes for testing
from mssql_python.exceptions import (
    Warning,
    Error,
    InterfaceError,
    DatabaseError,
    DataError,
    OperationalError,
    IntegrityError,
    InternalError,
    ProgrammingError,
    NotSupportedError,
)

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
    """Test that connection pooling provides performance benefits over multiple iterations."""
    import statistics
    
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
    
    # Clean up - disable pooling for other tests
    pooling(enabled=False)
    
    assert median_pool <= median_no_pool * improvement_threshold, \
        f"Expected pooling to be at least 30% faster. No-pool: {median_no_pool:.6f}s, Pool: {median_pool:.6f}s"

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

def test_close_with_autocommit_true(conn_str):
    """Test that connection.close() with autocommit=True doesn't trigger rollback."""
    cursor = None
    conn = None
    
    try:
        # Create a temporary table for testing
        setup_conn = connect(conn_str)
        setup_cursor = setup_conn.cursor()
        drop_table_if_exists(setup_cursor, "pytest_autocommit_close_test")
        setup_cursor.execute("CREATE TABLE pytest_autocommit_close_test (id INT PRIMARY KEY, value VARCHAR(50));")
        setup_conn.commit()
        setup_conn.close()
        
        # Create a connection with autocommit=True
        conn = connect(conn_str)
        conn.autocommit = True
        assert conn.autocommit is True, "Autocommit should be True"
        
        # Insert data
        cursor = conn.cursor()
        cursor.execute("INSERT INTO pytest_autocommit_close_test (id, value) VALUES (1, 'test_autocommit');")
        
        # Close the connection without explicitly committing
        conn.close()
        
        # Verify the data was committed automatically despite connection.close()
        verify_conn = connect(conn_str)
        verify_cursor = verify_conn.cursor()
        verify_cursor.execute("SELECT * FROM pytest_autocommit_close_test WHERE id = 1;")
        result = verify_cursor.fetchone()
        
        # Data should be present if autocommit worked and wasn't affected by close()
        assert result is not None, "Autocommit failed: Data not found after connection close"
        assert result[1] == 'test_autocommit', "Autocommit failed: Incorrect data after connection close"
        
        verify_conn.close()
        
    except Exception as e:
        pytest.fail(f"Test failed: {e}")
    finally:
        # Clean up
        cleanup_conn = connect(conn_str)
        cleanup_cursor = cleanup_conn.cursor()
        drop_table_if_exists(cleanup_cursor, "pytest_autocommit_close_test")
        cleanup_conn.commit()
        cleanup_conn.close()
        
def test_setencoding_default_settings(db_connection):
    """Test that default encoding settings are correct."""
    settings = db_connection.getencoding()
    assert settings['encoding'] == 'utf-16le', "Default encoding should be utf-16le"
    assert settings['ctype'] == -8, "Default ctype should be SQL_WCHAR (-8)"

def test_setencoding_basic_functionality(db_connection):
    """Test basic setencoding functionality."""
    # Test setting UTF-8 encoding
    db_connection.setencoding(encoding='utf-8')
    settings = db_connection.getencoding()
    assert settings['encoding'] == 'utf-8', "Encoding should be set to utf-8"
    assert settings['ctype'] == 1, "ctype should default to SQL_CHAR (1) for utf-8"
    
    # Test setting UTF-16LE with explicit ctype
    db_connection.setencoding(encoding='utf-16le', ctype=-8)
    settings = db_connection.getencoding()
    assert settings['encoding'] == 'utf-16le', "Encoding should be set to utf-16le"
    assert settings['ctype'] == -8, "ctype should be SQL_WCHAR (-8)"

def test_setencoding_automatic_ctype_detection(db_connection):
    """Test automatic ctype detection based on encoding."""
    # UTF-16 variants should default to SQL_WCHAR
    utf16_encodings = ['utf-16', 'utf-16le', 'utf-16be']
    for encoding in utf16_encodings:
        db_connection.setencoding(encoding=encoding)
        settings = db_connection.getencoding()
        assert settings['ctype'] == -8, f"{encoding} should default to SQL_WCHAR (-8)"
    
    # Other encodings should default to SQL_CHAR
    other_encodings = ['utf-8', 'latin-1', 'ascii']
    for encoding in other_encodings:
        db_connection.setencoding(encoding=encoding)
        settings = db_connection.getencoding()
        assert settings['ctype'] == 1, f"{encoding} should default to SQL_CHAR (1)"

def test_setencoding_explicit_ctype_override(db_connection):
    """Test that explicit ctype parameter overrides automatic detection."""
    # Set UTF-8 with SQL_WCHAR (override default)
    db_connection.setencoding(encoding='utf-8', ctype=-8)
    settings = db_connection.getencoding()
    assert settings['encoding'] == 'utf-8', "Encoding should be utf-8"
    assert settings['ctype'] == -8, "ctype should be SQL_WCHAR (-8) when explicitly set"
    
    # Set UTF-16LE with SQL_CHAR (override default)
    db_connection.setencoding(encoding='utf-16le', ctype=1)
    settings = db_connection.getencoding()
    assert settings['encoding'] == 'utf-16le', "Encoding should be utf-16le"
    assert settings['ctype'] == 1, "ctype should be SQL_CHAR (1) when explicitly set"

def test_setencoding_none_parameters(db_connection):
    """Test setencoding with None parameters."""
    # Test with encoding=None (should use default)
    db_connection.setencoding(encoding=None)
    settings = db_connection.getencoding()
    assert settings['encoding'] == 'utf-16le', "encoding=None should use default utf-16le"
    assert settings['ctype'] == -8, "ctype should be SQL_WCHAR for utf-16le"
    
    # Test with both None (should use defaults)
    db_connection.setencoding(encoding=None, ctype=None)
    settings = db_connection.getencoding()
    assert settings['encoding'] == 'utf-16le', "encoding=None should use default utf-16le"
    assert settings['ctype'] == -8, "ctype=None should use default SQL_WCHAR"

def test_setencoding_invalid_encoding(db_connection):
    """Test setencoding with invalid encoding."""
    
    with pytest.raises(ProgrammingError) as exc_info:
        db_connection.setencoding(encoding='invalid-encoding-name')
    
    assert "Unsupported encoding" in str(exc_info.value), "Should raise ProgrammingError for invalid encoding"
    assert "invalid-encoding-name" in str(exc_info.value), "Error message should include the invalid encoding name"

def test_setencoding_invalid_ctype(db_connection):
    """Test setencoding with invalid ctype."""
    
    with pytest.raises(ProgrammingError) as exc_info:
        db_connection.setencoding(encoding='utf-8', ctype=999)
    
    assert "Invalid ctype" in str(exc_info.value), "Should raise ProgrammingError for invalid ctype"
    assert "999" in str(exc_info.value), "Error message should include the invalid ctype value"

def test_setencoding_closed_connection(conn_str):
    """Test setencoding on closed connection."""
    
    temp_conn = connect(conn_str)
    temp_conn.close()
    
    with pytest.raises(InterfaceError) as exc_info:
        temp_conn.setencoding(encoding='utf-8')
    
    assert "Connection is closed" in str(exc_info.value), "Should raise InterfaceError for closed connection"

def test_setencoding_constants_access():
    """Test that SQL_CHAR and SQL_WCHAR constants are accessible."""
    import mssql_python
    
    # Test constants exist and have correct values
    assert hasattr(mssql_python, 'SQL_CHAR'), "SQL_CHAR constant should be available"
    assert hasattr(mssql_python, 'SQL_WCHAR'), "SQL_WCHAR constant should be available"
    assert mssql_python.SQL_CHAR == 1, "SQL_CHAR should have value 1"
    assert mssql_python.SQL_WCHAR == -8, "SQL_WCHAR should have value -8"

def test_setencoding_with_constants(db_connection):
    """Test setencoding using module constants."""
    import mssql_python
    
    # Test with SQL_CHAR constant
    db_connection.setencoding(encoding='utf-8', ctype=mssql_python.SQL_CHAR)
    settings = db_connection.getencoding()
    assert settings['ctype'] == mssql_python.SQL_CHAR, "Should accept SQL_CHAR constant"
    
    # Test with SQL_WCHAR constant
    db_connection.setencoding(encoding='utf-16le', ctype=mssql_python.SQL_WCHAR)
    settings = db_connection.getencoding()
    assert settings['ctype'] == mssql_python.SQL_WCHAR, "Should accept SQL_WCHAR constant"

def test_setencoding_common_encodings(db_connection):
    """Test setencoding with various common encodings."""
    common_encodings = [
        'utf-8',
        'utf-16le', 
        'utf-16be',
        'utf-16',
        'latin-1',
        'ascii',
        'cp1252'
    ]
    
    for encoding in common_encodings:
        try:
            db_connection.setencoding(encoding=encoding)
            settings = db_connection.getencoding()
            assert settings['encoding'] == encoding, f"Failed to set encoding {encoding}"
        except Exception as e:
            pytest.fail(f"Failed to set valid encoding {encoding}: {e}")

def test_setencoding_persistence_across_cursors(db_connection):
    """Test that encoding settings persist across cursor operations."""
    # Set custom encoding
    db_connection.setencoding(encoding='utf-8', ctype=1)
    
    # Create cursors and verify encoding persists
    cursor1 = db_connection.cursor()
    settings1 = db_connection.getencoding()
    
    cursor2 = db_connection.cursor()
    settings2 = db_connection.getencoding()
    
    assert settings1 == settings2, "Encoding settings should persist across cursor creation"
    assert settings1['encoding'] == 'utf-8', "Encoding should remain utf-8"
    assert settings1['ctype'] == 1, "ctype should remain SQL_CHAR"
    
    cursor1.close()
    cursor2.close()

@pytest.mark.skip("Skipping Unicode data tests till we have support for Unicode")
def test_setencoding_with_unicode_data(db_connection):
    """Test setencoding with actual Unicode data operations."""
    # Test UTF-8 encoding with Unicode data
    db_connection.setencoding(encoding='utf-8')
    cursor = db_connection.cursor()
    
    try:
        # Create test table
        cursor.execute("CREATE TABLE #test_encoding_unicode (text_col NVARCHAR(100))")
        
        # Test various Unicode strings
        test_strings = [
            "Hello, World!",
            "Hello, 世界!",  # Chinese
            "Привет, мир!",   # Russian
            "مرحبا بالعالم",   # Arabic
            "🌍🌎🌏",        # Emoji
        ]
        
        for test_string in test_strings:
            # Insert data
            cursor.execute("INSERT INTO #test_encoding_unicode (text_col) VALUES (?)", test_string)
            
            # Retrieve and verify
            cursor.execute("SELECT text_col FROM #test_encoding_unicode WHERE text_col = ?", test_string)
            result = cursor.fetchone()
            
            assert result is not None, f"Failed to retrieve Unicode string: {test_string}"
            assert result[0] == test_string, f"Unicode string mismatch: expected {test_string}, got {result[0]}"
            
            # Clear for next test
            cursor.execute("DELETE FROM #test_encoding_unicode")
    
    except Exception as e:
        pytest.fail(f"Unicode data test failed with UTF-8 encoding: {e}")
    finally:
        try:
            cursor.execute("DROP TABLE #test_encoding_unicode")
        except:
            pass
        cursor.close()

def test_setencoding_before_and_after_operations(db_connection):
    """Test that setencoding works both before and after database operations."""
    cursor = db_connection.cursor()
    
    try:
        # Initial encoding setting
        db_connection.setencoding(encoding='utf-16le')
        
        # Perform database operation
        cursor.execute("SELECT 'Initial test' as message")
        result1 = cursor.fetchone()
        assert result1[0] == 'Initial test', "Initial operation failed"
        
        # Change encoding after operation
        db_connection.setencoding(encoding='utf-8')
        settings = db_connection.getencoding()
        assert settings['encoding'] == 'utf-8', "Failed to change encoding after operation"
        
        # Perform another operation with new encoding
        cursor.execute("SELECT 'Changed encoding test' as message")
        result2 = cursor.fetchone()
        assert result2[0] == 'Changed encoding test', "Operation after encoding change failed"
        
    except Exception as e:
        pytest.fail(f"Encoding change test failed: {e}")
    finally:
        cursor.close()

def test_getencoding_default(conn_str):
    """Test getencoding returns default settings"""
    conn = connect(conn_str)
    try:
        encoding_info = conn.getencoding()
        assert isinstance(encoding_info, dict)
        assert 'encoding' in encoding_info
        assert 'ctype' in encoding_info
        # Default should be utf-16le with SQL_WCHAR
        assert encoding_info['encoding'] == 'utf-16le'
        assert encoding_info['ctype'] == SQL_WCHAR
    finally:
        conn.close()

def test_getencoding_returns_copy(conn_str):
    """Test getencoding returns a copy (not reference)"""
    conn = connect(conn_str)
    try:
        encoding_info1 = conn.getencoding()
        encoding_info2 = conn.getencoding()
        
        # Should be equal but not the same object
        assert encoding_info1 == encoding_info2
        assert encoding_info1 is not encoding_info2
        
        # Modifying one shouldn't affect the other
        encoding_info1['encoding'] = 'modified'
        assert encoding_info2['encoding'] != 'modified'
    finally:
        conn.close()

def test_getencoding_closed_connection(conn_str):
    """Test getencoding on closed connection raises InterfaceError"""
    conn = connect(conn_str)
    conn.close()
    
    with pytest.raises(InterfaceError, match="Connection is closed"):
        conn.getencoding()

def test_setencoding_getencoding_consistency(conn_str):
    """Test that setencoding and getencoding work consistently together"""
    conn = connect(conn_str)
    try:
        test_cases = [
            ('utf-8', SQL_CHAR),
            ('utf-16le', SQL_WCHAR),
            ('latin-1', SQL_CHAR),
            ('ascii', SQL_CHAR),
        ]
        
        for encoding, expected_ctype in test_cases:
            conn.setencoding(encoding)
            encoding_info = conn.getencoding()
            assert encoding_info['encoding'] == encoding.lower()
            assert encoding_info['ctype'] == expected_ctype
    finally:
        conn.close()

def test_setencoding_default_encoding(conn_str):
    """Test setencoding with default UTF-16LE encoding"""
    conn = connect(conn_str)
    try:
        conn.setencoding()
        encoding_info = conn.getencoding()
        assert encoding_info['encoding'] == 'utf-16le'
        assert encoding_info['ctype'] == SQL_WCHAR
    finally:
        conn.close()

def test_setencoding_utf8(conn_str):
    """Test setencoding with UTF-8 encoding"""
    conn = connect(conn_str)
    try:
        conn.setencoding('utf-8')
        encoding_info = conn.getencoding()
        assert encoding_info['encoding'] == 'utf-8'
        assert encoding_info['ctype'] == SQL_CHAR
    finally:
        conn.close()

def test_setencoding_latin1(conn_str):
    """Test setencoding with latin-1 encoding"""
    conn = connect(conn_str)
    try:
        conn.setencoding('latin-1')
        encoding_info = conn.getencoding()
        assert encoding_info['encoding'] == 'latin-1'
        assert encoding_info['ctype'] == SQL_CHAR
    finally:
        conn.close()

def test_setencoding_with_explicit_ctype_sql_char(conn_str):
    """Test setencoding with explicit SQL_CHAR ctype"""
    conn = connect(conn_str)
    try:
        conn.setencoding('utf-8', SQL_CHAR)
        encoding_info = conn.getencoding()
        assert encoding_info['encoding'] == 'utf-8'
        assert encoding_info['ctype'] == SQL_CHAR
    finally:
        conn.close()

def test_setencoding_with_explicit_ctype_sql_wchar(conn_str):
    """Test setencoding with explicit SQL_WCHAR ctype"""
    conn = connect(conn_str)
    try:
        conn.setencoding('utf-16le', SQL_WCHAR)
        encoding_info = conn.getencoding()
        assert encoding_info['encoding'] == 'utf-16le'
        assert encoding_info['ctype'] == SQL_WCHAR
    finally:
        conn.close()

def test_setencoding_invalid_ctype_error(conn_str):
    """Test setencoding with invalid ctype raises ProgrammingError"""
    
    conn = connect(conn_str)
    try:
        with pytest.raises(ProgrammingError, match="Invalid ctype"):
            conn.setencoding('utf-8', 999)
    finally:
        conn.close()

def test_setencoding_case_insensitive_encoding(conn_str):
    """Test setencoding with case variations"""
    conn = connect(conn_str)
    try:
        # Test various case formats
        conn.setencoding('UTF-8')
        encoding_info = conn.getencoding()
        assert encoding_info['encoding'] == 'utf-8'  # Should be normalized
        
        conn.setencoding('Utf-16LE')
        encoding_info = conn.getencoding()
        assert encoding_info['encoding'] == 'utf-16le'  # Should be normalized
    finally:
        conn.close()

def test_setencoding_none_encoding_default(conn_str):
    """Test setencoding with None encoding uses default"""
    conn = connect(conn_str)
    try:
        conn.setencoding(None)
        encoding_info = conn.getencoding()
        assert encoding_info['encoding'] == 'utf-16le'
        assert encoding_info['ctype'] == SQL_WCHAR
    finally:
        conn.close()

def test_setencoding_override_previous(conn_str):
    """Test setencoding overrides previous settings"""
    conn = connect(conn_str)
    try:
        # Set initial encoding
        conn.setencoding('utf-8')
        encoding_info = conn.getencoding()
        assert encoding_info['encoding'] == 'utf-8'
        assert encoding_info['ctype'] == SQL_CHAR
        
        # Override with different encoding
        conn.setencoding('utf-16le')
        encoding_info = conn.getencoding()
        assert encoding_info['encoding'] == 'utf-16le'
        assert encoding_info['ctype'] == SQL_WCHAR
    finally:
        conn.close()

def test_setencoding_ascii(conn_str):
    """Test setencoding with ASCII encoding"""
    conn = connect(conn_str)
    try:
        conn.setencoding('ascii')
        encoding_info = conn.getencoding()
        assert encoding_info['encoding'] == 'ascii'
        assert encoding_info['ctype'] == SQL_CHAR
    finally:
        conn.close()

def test_setencoding_cp1252(conn_str):
    """Test setencoding with Windows-1252 encoding"""
    conn = connect(conn_str)
    try:
        conn.setencoding('cp1252')
        encoding_info = conn.getencoding()
        assert encoding_info['encoding'] == 'cp1252'
        assert encoding_info['ctype'] == SQL_CHAR
    finally:
        conn.close()

def test_setdecoding_default_settings(db_connection):
    """Test that default decoding settings are correct for all SQL types."""
    
    # Check SQL_CHAR defaults
    sql_char_settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert sql_char_settings['encoding'] == 'utf-8', "Default SQL_CHAR encoding should be utf-8"
    assert sql_char_settings['ctype'] == mssql_python.SQL_CHAR, "Default SQL_CHAR ctype should be SQL_CHAR"
    
    # Check SQL_WCHAR defaults
    sql_wchar_settings = db_connection.getdecoding(mssql_python.SQL_WCHAR)
    assert sql_wchar_settings['encoding'] == 'utf-16le', "Default SQL_WCHAR encoding should be utf-16le"
    assert sql_wchar_settings['ctype'] == mssql_python.SQL_WCHAR, "Default SQL_WCHAR ctype should be SQL_WCHAR"
    
    # Check SQL_WMETADATA defaults
    sql_wmetadata_settings = db_connection.getdecoding(mssql_python.SQL_WMETADATA)
    assert sql_wmetadata_settings['encoding'] == 'utf-16le', "Default SQL_WMETADATA encoding should be utf-16le"
    assert sql_wmetadata_settings['ctype'] == mssql_python.SQL_WCHAR, "Default SQL_WMETADATA ctype should be SQL_WCHAR"

def test_setdecoding_basic_functionality(db_connection):
    """Test basic setdecoding functionality for different SQL types."""
    
    # Test setting SQL_CHAR decoding
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding='latin-1')
    settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert settings['encoding'] == 'latin-1', "SQL_CHAR encoding should be set to latin-1"
    assert settings['ctype'] == mssql_python.SQL_CHAR, "SQL_CHAR ctype should default to SQL_CHAR for latin-1"
    
    # Test setting SQL_WCHAR decoding
    db_connection.setdecoding(mssql_python.SQL_WCHAR, encoding='utf-16be')
    settings = db_connection.getdecoding(mssql_python.SQL_WCHAR)
    assert settings['encoding'] == 'utf-16be', "SQL_WCHAR encoding should be set to utf-16be"
    assert settings['ctype'] == mssql_python.SQL_WCHAR, "SQL_WCHAR ctype should default to SQL_WCHAR for utf-16be"
    
    # Test setting SQL_WMETADATA decoding
    db_connection.setdecoding(mssql_python.SQL_WMETADATA, encoding='utf-16le')
    settings = db_connection.getdecoding(mssql_python.SQL_WMETADATA)
    assert settings['encoding'] == 'utf-16le', "SQL_WMETADATA encoding should be set to utf-16le"
    assert settings['ctype'] == mssql_python.SQL_WCHAR, "SQL_WMETADATA ctype should default to SQL_WCHAR"

def test_setdecoding_automatic_ctype_detection(db_connection):
    """Test automatic ctype detection based on encoding for different SQL types."""
    
    # UTF-16 variants should default to SQL_WCHAR
    utf16_encodings = ['utf-16', 'utf-16le', 'utf-16be']
    for encoding in utf16_encodings:
        db_connection.setdecoding(mssql_python.SQL_CHAR, encoding=encoding)
        settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
        assert settings['ctype'] == mssql_python.SQL_WCHAR, f"SQL_CHAR with {encoding} should auto-detect SQL_WCHAR ctype"
    
    # Other encodings should default to SQL_CHAR
    other_encodings = ['utf-8', 'latin-1', 'ascii', 'cp1252']
    for encoding in other_encodings:
        db_connection.setdecoding(mssql_python.SQL_WCHAR, encoding=encoding)
        settings = db_connection.getdecoding(mssql_python.SQL_WCHAR)
        assert settings['ctype'] == mssql_python.SQL_CHAR, f"SQL_WCHAR with {encoding} should auto-detect SQL_CHAR ctype"

def test_setdecoding_explicit_ctype_override(db_connection):
    """Test that explicit ctype parameter overrides automatic detection."""
    
    # Set SQL_CHAR with UTF-8 encoding but explicit SQL_WCHAR ctype
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding='utf-8', ctype=mssql_python.SQL_WCHAR)
    settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert settings['encoding'] == 'utf-8', "Encoding should be utf-8"
    assert settings['ctype'] == mssql_python.SQL_WCHAR, "ctype should be SQL_WCHAR when explicitly set"
    
    # Set SQL_WCHAR with UTF-16LE encoding but explicit SQL_CHAR ctype
    db_connection.setdecoding(mssql_python.SQL_WCHAR, encoding='utf-16le', ctype=mssql_python.SQL_CHAR)
    settings = db_connection.getdecoding(mssql_python.SQL_WCHAR)
    assert settings['encoding'] == 'utf-16le', "Encoding should be utf-16le"
    assert settings['ctype'] == mssql_python.SQL_CHAR, "ctype should be SQL_CHAR when explicitly set"

def test_setdecoding_none_parameters(db_connection):
    """Test setdecoding with None parameters uses appropriate defaults."""
    
    # Test SQL_CHAR with encoding=None (should use utf-8 default)
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding=None)
    settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert settings['encoding'] == 'utf-8', "SQL_CHAR with encoding=None should use utf-8 default"
    assert settings['ctype'] == mssql_python.SQL_CHAR, "ctype should be SQL_CHAR for utf-8"
    
    # Test SQL_WCHAR with encoding=None (should use utf-16le default)
    db_connection.setdecoding(mssql_python.SQL_WCHAR, encoding=None)
    settings = db_connection.getdecoding(mssql_python.SQL_WCHAR)
    assert settings['encoding'] == 'utf-16le', "SQL_WCHAR with encoding=None should use utf-16le default"
    assert settings['ctype'] == mssql_python.SQL_WCHAR, "ctype should be SQL_WCHAR for utf-16le"
    
    # Test with both parameters None
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding=None, ctype=None)
    settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert settings['encoding'] == 'utf-8', "SQL_CHAR with both None should use utf-8 default"
    assert settings['ctype'] == mssql_python.SQL_CHAR, "ctype should default to SQL_CHAR"

def test_setdecoding_invalid_sqltype(db_connection):
    """Test setdecoding with invalid sqltype raises ProgrammingError."""
    
    with pytest.raises(ProgrammingError) as exc_info:
        db_connection.setdecoding(999, encoding='utf-8')
    
    assert "Invalid sqltype" in str(exc_info.value), "Should raise ProgrammingError for invalid sqltype"
    assert "999" in str(exc_info.value), "Error message should include the invalid sqltype value"

def test_setdecoding_invalid_encoding(db_connection):
    """Test setdecoding with invalid encoding raises ProgrammingError."""
    
    with pytest.raises(ProgrammingError) as exc_info:
        db_connection.setdecoding(mssql_python.SQL_CHAR, encoding='invalid-encoding-name')
    
    assert "Unsupported encoding" in str(exc_info.value), "Should raise ProgrammingError for invalid encoding"
    assert "invalid-encoding-name" in str(exc_info.value), "Error message should include the invalid encoding name"

def test_setdecoding_invalid_ctype(db_connection):
    """Test setdecoding with invalid ctype raises ProgrammingError."""
    
    with pytest.raises(ProgrammingError) as exc_info:
        db_connection.setdecoding(mssql_python.SQL_CHAR, encoding='utf-8', ctype=999)
    
    assert "Invalid ctype" in str(exc_info.value), "Should raise ProgrammingError for invalid ctype"
    assert "999" in str(exc_info.value), "Error message should include the invalid ctype value"

def test_setdecoding_closed_connection(conn_str):
    """Test setdecoding on closed connection raises InterfaceError."""
    
    temp_conn = connect(conn_str)
    temp_conn.close()
    
    with pytest.raises(InterfaceError) as exc_info:
        temp_conn.setdecoding(mssql_python.SQL_CHAR, encoding='utf-8')
    
    assert "Connection is closed" in str(exc_info.value), "Should raise InterfaceError for closed connection"

def test_setdecoding_constants_access():
    """Test that SQL constants are accessible."""
    
    # Test constants exist and have correct values
    assert hasattr(mssql_python, 'SQL_CHAR'), "SQL_CHAR constant should be available"
    assert hasattr(mssql_python, 'SQL_WCHAR'), "SQL_WCHAR constant should be available"
    assert hasattr(mssql_python, 'SQL_WMETADATA'), "SQL_WMETADATA constant should be available"
    
    assert mssql_python.SQL_CHAR == 1, "SQL_CHAR should have value 1"
    assert mssql_python.SQL_WCHAR == -8, "SQL_WCHAR should have value -8"
    assert mssql_python.SQL_WMETADATA == -99, "SQL_WMETADATA should have value -99"

def test_setdecoding_with_constants(db_connection):
    """Test setdecoding using module constants."""
    
    # Test with SQL_CHAR constant
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding='utf-8', ctype=mssql_python.SQL_CHAR)
    settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert settings['ctype'] == mssql_python.SQL_CHAR, "Should accept SQL_CHAR constant"
    
    # Test with SQL_WCHAR constant
    db_connection.setdecoding(mssql_python.SQL_WCHAR, encoding='utf-16le', ctype=mssql_python.SQL_WCHAR)
    settings = db_connection.getdecoding(mssql_python.SQL_WCHAR)
    assert settings['ctype'] == mssql_python.SQL_WCHAR, "Should accept SQL_WCHAR constant"
    
    # Test with SQL_WMETADATA constant
    db_connection.setdecoding(mssql_python.SQL_WMETADATA, encoding='utf-16be')
    settings = db_connection.getdecoding(mssql_python.SQL_WMETADATA)
    assert settings['encoding'] == 'utf-16be', "Should accept SQL_WMETADATA constant"

def test_setdecoding_common_encodings(db_connection):
    """Test setdecoding with various common encodings."""
    
    common_encodings = [
        'utf-8',
        'utf-16le', 
        'utf-16be',
        'utf-16',
        'latin-1',
        'ascii',
        'cp1252'
    ]
    
    for encoding in common_encodings:
        try:
            db_connection.setdecoding(mssql_python.SQL_CHAR, encoding=encoding)
            settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
            assert settings['encoding'] == encoding, f"Failed to set SQL_CHAR decoding to {encoding}"
            
            db_connection.setdecoding(mssql_python.SQL_WCHAR, encoding=encoding)
            settings = db_connection.getdecoding(mssql_python.SQL_WCHAR)
            assert settings['encoding'] == encoding, f"Failed to set SQL_WCHAR decoding to {encoding}"
        except Exception as e:
            pytest.fail(f"Failed to set valid encoding {encoding}: {e}")

def test_setdecoding_case_insensitive_encoding(db_connection):
    """Test setdecoding with case variations normalizes encoding."""
    
    # Test various case formats
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding='UTF-8')
    settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert settings['encoding'] == 'utf-8', "Encoding should be normalized to lowercase"
    
    db_connection.setdecoding(mssql_python.SQL_WCHAR, encoding='Utf-16LE')
    settings = db_connection.getdecoding(mssql_python.SQL_WCHAR)
    assert settings['encoding'] == 'utf-16le', "Encoding should be normalized to lowercase"

def test_setdecoding_independent_sql_types(db_connection):
    """Test that decoding settings for different SQL types are independent."""
    
    # Set different encodings for each SQL type
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding='utf-8')
    db_connection.setdecoding(mssql_python.SQL_WCHAR, encoding='utf-16le')
    db_connection.setdecoding(mssql_python.SQL_WMETADATA, encoding='utf-16be')
    
    # Verify each maintains its own settings
    sql_char_settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    sql_wchar_settings = db_connection.getdecoding(mssql_python.SQL_WCHAR)
    sql_wmetadata_settings = db_connection.getdecoding(mssql_python.SQL_WMETADATA)
    
    assert sql_char_settings['encoding'] == 'utf-8', "SQL_CHAR should maintain utf-8"
    assert sql_wchar_settings['encoding'] == 'utf-16le', "SQL_WCHAR should maintain utf-16le"
    assert sql_wmetadata_settings['encoding'] == 'utf-16be', "SQL_WMETADATA should maintain utf-16be"

def test_setdecoding_override_previous(db_connection):
    """Test setdecoding overrides previous settings for the same SQL type."""
    
    # Set initial decoding
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding='utf-8')
    settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert settings['encoding'] == 'utf-8', "Initial encoding should be utf-8"
    assert settings['ctype'] == mssql_python.SQL_CHAR, "Initial ctype should be SQL_CHAR"
    
    # Override with different settings
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding='latin-1', ctype=mssql_python.SQL_WCHAR)
    settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert settings['encoding'] == 'latin-1', "Encoding should be overridden to latin-1"
    assert settings['ctype'] == mssql_python.SQL_WCHAR, "ctype should be overridden to SQL_WCHAR"

def test_getdecoding_invalid_sqltype(db_connection):
    """Test getdecoding with invalid sqltype raises ProgrammingError."""
    
    with pytest.raises(ProgrammingError) as exc_info:
        db_connection.getdecoding(999)
    
    assert "Invalid sqltype" in str(exc_info.value), "Should raise ProgrammingError for invalid sqltype"
    assert "999" in str(exc_info.value), "Error message should include the invalid sqltype value"

def test_getdecoding_closed_connection(conn_str):
    """Test getdecoding on closed connection raises InterfaceError."""
    
    temp_conn = connect(conn_str)
    temp_conn.close()
    
    with pytest.raises(InterfaceError) as exc_info:
        temp_conn.getdecoding(mssql_python.SQL_CHAR)
    
    assert "Connection is closed" in str(exc_info.value), "Should raise InterfaceError for closed connection"

def test_getdecoding_returns_copy(db_connection):
    """Test getdecoding returns a copy (not reference)."""
    
    # Set custom decoding
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding='utf-8')
    
    # Get settings twice
    settings1 = db_connection.getdecoding(mssql_python.SQL_CHAR)
    settings2 = db_connection.getdecoding(mssql_python.SQL_CHAR)
    
    # Should be equal but not the same object
    assert settings1 == settings2, "Settings should be equal"
    assert settings1 is not settings2, "Settings should be different objects"
    
    # Modifying one shouldn't affect the other
    settings1['encoding'] = 'modified'
    assert settings2['encoding'] != 'modified', "Modification should not affect other copy"

def test_setdecoding_getdecoding_consistency(db_connection):
    """Test that setdecoding and getdecoding work consistently together."""
    
    test_cases = [
        (mssql_python.SQL_CHAR, 'utf-8', mssql_python.SQL_CHAR),
        (mssql_python.SQL_CHAR, 'utf-16le', mssql_python.SQL_WCHAR),
        (mssql_python.SQL_WCHAR, 'latin-1', mssql_python.SQL_CHAR),
        (mssql_python.SQL_WCHAR, 'utf-16be', mssql_python.SQL_WCHAR),
        (mssql_python.SQL_WMETADATA, 'utf-16le', mssql_python.SQL_WCHAR),
    ]
    
    for sqltype, encoding, expected_ctype in test_cases:
        db_connection.setdecoding(sqltype, encoding=encoding)
        settings = db_connection.getdecoding(sqltype)
        assert settings['encoding'] == encoding.lower(), f"Encoding should be {encoding.lower()}"
        assert settings['ctype'] == expected_ctype, f"ctype should be {expected_ctype}"

def test_setdecoding_persistence_across_cursors(db_connection):
    """Test that decoding settings persist across cursor operations."""
    
    # Set custom decoding settings
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding='latin-1', ctype=mssql_python.SQL_CHAR)
    db_connection.setdecoding(mssql_python.SQL_WCHAR, encoding='utf-16be', ctype=mssql_python.SQL_WCHAR)
    
    # Create cursors and verify settings persist
    cursor1 = db_connection.cursor()
    char_settings1 = db_connection.getdecoding(mssql_python.SQL_CHAR)
    wchar_settings1 = db_connection.getdecoding(mssql_python.SQL_WCHAR)
    
    cursor2 = db_connection.cursor()
    char_settings2 = db_connection.getdecoding(mssql_python.SQL_CHAR)
    wchar_settings2 = db_connection.getdecoding(mssql_python.SQL_WCHAR)
    
    # Settings should persist across cursor creation
    assert char_settings1 == char_settings2, "SQL_CHAR settings should persist across cursors"
    assert wchar_settings1 == wchar_settings2, "SQL_WCHAR settings should persist across cursors"
    
    assert char_settings1['encoding'] == 'latin-1', "SQL_CHAR encoding should remain latin-1"
    assert wchar_settings1['encoding'] == 'utf-16be', "SQL_WCHAR encoding should remain utf-16be"
    
    cursor1.close()
    cursor2.close()

def test_setdecoding_before_and_after_operations(db_connection):
    """Test that setdecoding works both before and after database operations."""
    cursor = db_connection.cursor()
    
    try:
        # Initial decoding setting
        db_connection.setdecoding(mssql_python.SQL_CHAR, encoding='utf-8')
        
        # Perform database operation
        cursor.execute("SELECT 'Initial test' as message")
        result1 = cursor.fetchone()
        assert result1[0] == 'Initial test', "Initial operation failed"
        
        # Change decoding after operation
        db_connection.setdecoding(mssql_python.SQL_CHAR, encoding='latin-1')
        settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
        assert settings['encoding'] == 'latin-1', "Failed to change decoding after operation"
        
        # Perform another operation with new decoding
        cursor.execute("SELECT 'Changed decoding test' as message")
        result2 = cursor.fetchone()
        assert result2[0] == 'Changed decoding test', "Operation after decoding change failed"
        
    except Exception as e:
        pytest.fail(f"Decoding change test failed: {e}")
    finally:
        cursor.close()

def test_setdecoding_all_sql_types_independently(conn_str):
    """Test setdecoding with all SQL types on a fresh connection."""
    
    conn = connect(conn_str)
    try:
        # Test each SQL type with different configurations
        test_configs = [
            (mssql_python.SQL_CHAR, 'ascii', mssql_python.SQL_CHAR),
            (mssql_python.SQL_WCHAR, 'utf-16le', mssql_python.SQL_WCHAR),
            (mssql_python.SQL_WMETADATA, 'utf-16be', mssql_python.SQL_WCHAR),
        ]
        
        for sqltype, encoding, ctype in test_configs:
            conn.setdecoding(sqltype, encoding=encoding, ctype=ctype)
            settings = conn.getdecoding(sqltype)
            assert settings['encoding'] == encoding, f"Failed to set encoding for sqltype {sqltype}"
            assert settings['ctype'] == ctype, f"Failed to set ctype for sqltype {sqltype}"
            
    finally:
        conn.close()

def test_setdecoding_security_logging(db_connection):
    """Test that setdecoding logs invalid attempts safely."""
    
    # These should raise exceptions but not crash due to logging
    test_cases = [
        (999, 'utf-8', None),  # Invalid sqltype
        (mssql_python.SQL_CHAR, 'invalid-encoding', None),  # Invalid encoding
        (mssql_python.SQL_CHAR, 'utf-8', 999),  # Invalid ctype
    ]
    
    for sqltype, encoding, ctype in test_cases:
        with pytest.raises(ProgrammingError):
            db_connection.setdecoding(sqltype, encoding=encoding, ctype=ctype)

@pytest.mark.skip("Skipping Unicode data tests till we have support for Unicode")
def test_setdecoding_with_unicode_data(db_connection):
    """Test setdecoding with actual Unicode data operations."""
    
    # Test different decoding configurations with Unicode data
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding='utf-8')
    db_connection.setdecoding(mssql_python.SQL_WCHAR, encoding='utf-16le')
    
    cursor = db_connection.cursor()
    
    try:
        # Create test table with both CHAR and NCHAR columns
        cursor.execute("""
            CREATE TABLE #test_decoding_unicode (
                char_col VARCHAR(100),
                nchar_col NVARCHAR(100)
            )
        """)
        
        # Test various Unicode strings
        test_strings = [
            "Hello, World!",
            "Hello, 世界!",  # Chinese
            "Привет, мир!",   # Russian
            "مرحبا بالعالم",   # Arabic
        ]
        
        for test_string in test_strings:
            # Insert data
            cursor.execute(
                "INSERT INTO #test_decoding_unicode (char_col, nchar_col) VALUES (?, ?)", 
                test_string, test_string
            )
            
            # Retrieve and verify
            cursor.execute("SELECT char_col, nchar_col FROM #test_decoding_unicode WHERE char_col = ?", test_string)
            result = cursor.fetchone()
            
            assert result is not None, f"Failed to retrieve Unicode string: {test_string}"
            assert result[0] == test_string, f"CHAR column mismatch: expected {test_string}, got {result[0]}"
            assert result[1] == test_string, f"NCHAR column mismatch: expected {test_string}, got {result[1]}"
            
            # Clear for next test
            cursor.execute("DELETE FROM #test_decoding_unicode")
    
    except Exception as e:
        pytest.fail(f"Unicode data test failed with custom decoding: {e}")
    finally:
        try:
            cursor.execute("DROP TABLE #test_decoding_unicode")
        except:
            pass
        cursor.close()

# DB-API 2.0 Exception Attribute Tests
def test_connection_exception_attributes_exist(db_connection):
    """Test that all DB-API 2.0 exception classes are available as Connection attributes"""
    # Test that all required exception attributes exist
    assert hasattr(db_connection, 'Warning'), "Connection should have Warning attribute"
    assert hasattr(db_connection, 'Error'), "Connection should have Error attribute"
    assert hasattr(db_connection, 'InterfaceError'), "Connection should have InterfaceError attribute"
    assert hasattr(db_connection, 'DatabaseError'), "Connection should have DatabaseError attribute"
    assert hasattr(db_connection, 'DataError'), "Connection should have DataError attribute"
    assert hasattr(db_connection, 'OperationalError'), "Connection should have OperationalError attribute"
    assert hasattr(db_connection, 'IntegrityError'), "Connection should have IntegrityError attribute"
    assert hasattr(db_connection, 'InternalError'), "Connection should have InternalError attribute"
    assert hasattr(db_connection, 'ProgrammingError'), "Connection should have ProgrammingError attribute"
    assert hasattr(db_connection, 'NotSupportedError'), "Connection should have NotSupportedError attribute"

def test_connection_exception_attributes_are_classes(db_connection):
    """Test that all exception attributes are actually exception classes"""
    # Test that the attributes are the correct exception classes
    assert db_connection.Warning is Warning, "Connection.Warning should be the Warning class"
    assert db_connection.Error is Error, "Connection.Error should be the Error class"
    assert db_connection.InterfaceError is InterfaceError, "Connection.InterfaceError should be the InterfaceError class"
    assert db_connection.DatabaseError is DatabaseError, "Connection.DatabaseError should be the DatabaseError class"
    assert db_connection.DataError is DataError, "Connection.DataError should be the DataError class"
    assert db_connection.OperationalError is OperationalError, "Connection.OperationalError should be the OperationalError class"
    assert db_connection.IntegrityError is IntegrityError, "Connection.IntegrityError should be the IntegrityError class"
    assert db_connection.InternalError is InternalError, "Connection.InternalError should be the InternalError class"
    assert db_connection.ProgrammingError is ProgrammingError, "Connection.ProgrammingError should be the ProgrammingError class"
    assert db_connection.NotSupportedError is NotSupportedError, "Connection.NotSupportedError should be the NotSupportedError class"

def test_connection_exception_inheritance(db_connection):
    """Test that exception classes have correct inheritance hierarchy"""
    # Test inheritance hierarchy according to DB-API 2.0
    
    # All exceptions inherit from Error (except Warning)
    assert issubclass(db_connection.InterfaceError, db_connection.Error), "InterfaceError should inherit from Error"
    assert issubclass(db_connection.DatabaseError, db_connection.Error), "DatabaseError should inherit from Error"
    
    # Database exceptions inherit from DatabaseError
    assert issubclass(db_connection.DataError, db_connection.DatabaseError), "DataError should inherit from DatabaseError"
    assert issubclass(db_connection.OperationalError, db_connection.DatabaseError), "OperationalError should inherit from DatabaseError"
    assert issubclass(db_connection.IntegrityError, db_connection.DatabaseError), "IntegrityError should inherit from DatabaseError"
    assert issubclass(db_connection.InternalError, db_connection.DatabaseError), "InternalError should inherit from DatabaseError"
    assert issubclass(db_connection.ProgrammingError, db_connection.DatabaseError), "ProgrammingError should inherit from DatabaseError"
    assert issubclass(db_connection.NotSupportedError, db_connection.DatabaseError), "NotSupportedError should inherit from DatabaseError"

def test_connection_exception_instantiation(db_connection):
    """Test that exception classes can be instantiated from Connection attributes"""
    # Test that we can create instances of exceptions using connection attributes
    warning = db_connection.Warning("Test warning", "DDBC warning")
    assert isinstance(warning, db_connection.Warning), "Should be able to create Warning instance"
    assert "Test warning" in str(warning), "Warning should contain driver error message"
    
    error = db_connection.Error("Test error", "DDBC error")
    assert isinstance(error, db_connection.Error), "Should be able to create Error instance"
    assert "Test error" in str(error), "Error should contain driver error message"
    
    interface_error = db_connection.InterfaceError("Interface error", "DDBC interface error")
    assert isinstance(interface_error, db_connection.InterfaceError), "Should be able to create InterfaceError instance"
    assert "Interface error" in str(interface_error), "InterfaceError should contain driver error message"
    
    db_error = db_connection.DatabaseError("Database error", "DDBC database error")
    assert isinstance(db_error, db_connection.DatabaseError), "Should be able to create DatabaseError instance"
    assert "Database error" in str(db_error), "DatabaseError should contain driver error message"

def test_connection_exception_catching_with_connection_attributes(db_connection):
    """Test that we can catch exceptions using Connection attributes in multi-connection scenarios"""
    cursor = db_connection.cursor()
    
    try:
        # Test catching InterfaceError using connection attribute
        cursor.close()
        cursor.execute("SELECT 1")  # Should raise InterfaceError on closed cursor
        pytest.fail("Should have raised an exception")
    except db_connection.ProgrammingError as e:
        assert "closed" in str(e).lower(), "Error message should mention closed cursor"
    except Exception as e:
        pytest.fail(f"Should have caught InterfaceError, but got {type(e).__name__}: {e}")

def test_connection_exception_error_handling_example(db_connection):
    """Test real-world error handling example using Connection exception attributes"""
    cursor = db_connection.cursor()
    
    try:
        # Try to create a table with invalid syntax (should raise ProgrammingError)
        cursor.execute("CREATE INVALID TABLE syntax_error")
        pytest.fail("Should have raised ProgrammingError")
    except db_connection.ProgrammingError as e:
        # This is the expected exception for syntax errors
        assert "syntax" in str(e).lower() or "incorrect" in str(e).lower() or "near" in str(e).lower(), "Should be a syntax-related error"
    except db_connection.DatabaseError as e:
        # ProgrammingError inherits from DatabaseError, so this might catch it too
        # This is acceptable according to DB-API 2.0
        pass
    except Exception as e:
        pytest.fail(f"Expected ProgrammingError or DatabaseError, got {type(e).__name__}: {e}")

def test_connection_exception_multi_connection_scenario(conn_str):
    """Test exception handling in multi-connection environment"""
    # Create two separate connections
    conn1 = connect(conn_str)
    conn2 = connect(conn_str)
    
    try:
        cursor1 = conn1.cursor()
        cursor2 = conn2.cursor()
        
        # Close first connection but try to use its cursor
        conn1.close()
        
        try:
            cursor1.execute("SELECT 1")
            pytest.fail("Should have raised an exception")
        except conn1.ProgrammingError as e:
            # Using conn1.ProgrammingError even though conn1 is closed
            # The exception class attribute should still be accessible
            assert "closed" in str(e).lower(), "Should mention closed cursor"
        except Exception as e:
            pytest.fail(f"Expected ProgrammingError from conn1 attributes, got {type(e).__name__}: {e}")
        
        # Second connection should still work
        cursor2.execute("SELECT 1")
        result = cursor2.fetchone()
        assert result[0] == 1, "Second connection should still work"
        
        # Test using conn2 exception attributes
        try:
            cursor2.execute("SELECT * FROM nonexistent_table_12345")
            pytest.fail("Should have raised an exception")
        except conn2.ProgrammingError as e:
            # Using conn2.ProgrammingError for table not found
            assert "nonexistent_table_12345" in str(e) or "object" in str(e).lower() or "not" in str(e).lower(), "Should mention the missing table"
        except conn2.DatabaseError as e:
            # Acceptable since ProgrammingError inherits from DatabaseError
            pass
        except Exception as e:
            pytest.fail(f"Expected ProgrammingError or DatabaseError from conn2, got {type(e).__name__}: {e}")
            
    finally:
        try:
            if not conn1._closed:
                conn1.close()
        except:
            pass
        try:
            if not conn2._closed:
                conn2.close()
        except:
            pass

def test_connection_exception_attributes_consistency(conn_str):
    """Test that exception attributes are consistent across multiple Connection instances"""
    conn1 = connect(conn_str)
    conn2 = connect(conn_str)
    
    try:
        # Test that the same exception classes are referenced by different connections
        assert conn1.Error is conn2.Error, "All connections should reference the same Error class"
        assert conn1.InterfaceError is conn2.InterfaceError, "All connections should reference the same InterfaceError class"
        assert conn1.DatabaseError is conn2.DatabaseError, "All connections should reference the same DatabaseError class"
        assert conn1.ProgrammingError is conn2.ProgrammingError, "All connections should reference the same ProgrammingError class"
        
        # Test that the classes are the same as module-level imports
        assert conn1.Error is Error, "Connection.Error should be the same as module-level Error"
        assert conn1.InterfaceError is InterfaceError, "Connection.InterfaceError should be the same as module-level InterfaceError"
        assert conn1.DatabaseError is DatabaseError, "Connection.DatabaseError should be the same as module-level DatabaseError"
        
    finally:
        conn1.close()
        conn2.close()

def test_connection_exception_attributes_comprehensive_list():
    """Test that all DB-API 2.0 required exception attributes are present on Connection class"""
    # Test at the class level (before instantiation)
    required_exceptions = [
        'Warning', 'Error', 'InterfaceError', 'DatabaseError',
        'DataError', 'OperationalError', 'IntegrityError', 
        'InternalError', 'ProgrammingError', 'NotSupportedError'
    ]
    
    for exc_name in required_exceptions:
        assert hasattr(Connection, exc_name), f"Connection class should have {exc_name} attribute"
        exc_class = getattr(Connection, exc_name)
        assert isinstance(exc_class, type), f"Connection.{exc_name} should be a class"
        assert issubclass(exc_class, Exception), f"Connection.{exc_name} should be an Exception subclass"

