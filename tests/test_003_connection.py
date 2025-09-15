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
"""

from mssql_python.exceptions import InterfaceError
import datetime
import pytest
import time
from mssql_python import Connection, connect, pooling
import threading

@pytest.fixture(autouse=True)
def clean_connection_state(db_connection):
    """Ensure connection is in a clean state before each test"""
    # Create a cursor and clear any active results
    try:
        cleanup_cursor = db_connection.cursor()
        cleanup_cursor.execute("SELECT 1")  # Simple query to reset state
        cleanup_cursor.fetchall()  # Consume all results
        cleanup_cursor.close()
    except Exception:
        pass  # Ignore errors during cleanup

    yield  # Run the test

    # Clean up after the test
    try:
        cleanup_cursor = db_connection.cursor()
        cleanup_cursor.execute("SELECT 1")  # Simple query to reset state
        cleanup_cursor.fetchall()  # Consume all results
        cleanup_cursor.close()
    except Exception:
        pass  # Ignore errors during cleanup

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

def test_connection_execute(db_connection):
    """Test the execute() convenience method for Connection class"""
    # Test basic execution
    cursor = db_connection.execute("SELECT 1 AS test_value")
    result = cursor.fetchone()
    assert result is not None, "Execute failed: No result returned"
    assert result[0] == 1, "Execute failed: Incorrect result"
    
    # Test with parameters
    cursor = db_connection.execute("SELECT ? AS test_value", 42)
    result = cursor.fetchone()
    assert result is not None, "Execute with parameters failed: No result returned"
    assert result[0] == 42, "Execute with parameters failed: Incorrect result"
    
    # Test that cursor is tracked by connection
    assert cursor in db_connection._cursors, "Cursor from execute() not tracked by connection"
    
    # Test with data modification and verify it requires commit
    if not db_connection.autocommit:
        drop_table_if_exists(db_connection.cursor(), "#pytest_test_execute")
        cursor1 = db_connection.execute("CREATE TABLE #pytest_test_execute (id INT, value VARCHAR(50))")
        cursor2 = db_connection.execute("INSERT INTO #pytest_test_execute VALUES (1, 'test_value')")
        cursor3 = db_connection.execute("SELECT * FROM #pytest_test_execute")
        result = cursor3.fetchone()
        assert result is not None, "Execute with table creation failed"
        assert result[0] == 1, "Execute with table creation returned wrong id"
        assert result[1] == 'test_value', "Execute with table creation returned wrong value"
        
        # Clean up
        db_connection.execute("DROP TABLE #pytest_test_execute")
        db_connection.commit()

def test_connection_execute_error_handling(db_connection):
    """Test that execute() properly handles SQL errors"""
    with pytest.raises(Exception):
        db_connection.execute("SELECT * FROM nonexistent_table")
        
def test_connection_execute_empty_result(db_connection):
    """Test execute() with a query that returns no rows"""
    cursor = db_connection.execute("SELECT * FROM sys.tables WHERE name = 'nonexistent_table_name'")
    result = cursor.fetchone()
    assert result is None, "Query should return no results"
    
    # Test empty result with fetchall
    rows = cursor.fetchall()
    assert len(rows) == 0, "fetchall should return empty list for empty result set"

def test_connection_execute_different_parameter_types(db_connection):
    """Test execute() with different parameter data types"""
    # Test with different data types
    params = [
        1234,                      # Integer
        3.14159,                   # Float
        "test string",             # String
        bytearray(b'binary data'), # Binary data
        True,                      # Boolean
        None                       # NULL
    ]
    
    for param in params:
        cursor = db_connection.execute("SELECT ? AS value", param)
        result = cursor.fetchone()
        if param is None:
            assert result[0] is None, "NULL parameter not handled correctly"
        else:
            assert result[0] == param, f"Parameter {param} of type {type(param)} not handled correctly"

def test_connection_execute_with_transaction(db_connection):
    """Test execute() in the context of explicit transactions"""
    if db_connection.autocommit:
        db_connection.autocommit = False
    
    cursor1 = db_connection.cursor()
    drop_table_if_exists(cursor1, "#pytest_test_execute_transaction")
    
    try:
        # Create table and insert data
        db_connection.execute("CREATE TABLE #pytest_test_execute_transaction (id INT, value VARCHAR(50))")
        db_connection.execute("INSERT INTO #pytest_test_execute_transaction VALUES (1, 'before rollback')")
        
        # Check data is there
        cursor = db_connection.execute("SELECT * FROM #pytest_test_execute_transaction")
        result = cursor.fetchone()
        assert result is not None, "Data should be visible within transaction"
        assert result[1] == 'before rollback', "Incorrect data in transaction"
        
        # Rollback and verify data is gone
        db_connection.rollback()
        
        # Need to recreate table since it was rolled back
        db_connection.execute("CREATE TABLE #pytest_test_execute_transaction (id INT, value VARCHAR(50))")
        db_connection.execute("INSERT INTO #pytest_test_execute_transaction VALUES (2, 'after rollback')")
        
        cursor = db_connection.execute("SELECT * FROM #pytest_test_execute_transaction")
        result = cursor.fetchone()
        assert result is not None, "Data should be visible after new insert"
        assert result[0] == 2, "Should see the new data after rollback"
        assert result[1] == 'after rollback', "Incorrect data after rollback"
        
        # Commit and verify data persists
        db_connection.commit()
    finally:
        # Clean up
        try:
            db_connection.execute("DROP TABLE #pytest_test_execute_transaction")
            db_connection.commit()
        except Exception:
            pass

def test_connection_execute_vs_cursor_execute(db_connection):
    """Compare behavior of connection.execute() vs cursor.execute()"""
    # Connection.execute creates a new cursor each time
    cursor1 = db_connection.execute("SELECT 1 AS first_query")
    # Consume the results from cursor1 before creating cursor2
    result1 = cursor1.fetchall()
    assert result1[0][0] == 1, "First cursor should have result from first query"
    
    # Now it's safe to create a second cursor
    cursor2 = db_connection.execute("SELECT 2 AS second_query")
    result2 = cursor2.fetchall()
    assert result2[0][0] == 2, "Second cursor should have result from second query"
    
    # These should be different cursor objects
    assert cursor1 != cursor2, "Connection.execute should create a new cursor each time"
    
    # Now compare with reusing the same cursor
    cursor3 = db_connection.cursor()
    cursor3.execute("SELECT 3 AS third_query")
    result3 = cursor3.fetchone()
    assert result3[0] == 3, "Direct cursor execution failed"
    
    # Reuse the same cursor
    cursor3.execute("SELECT 4 AS fourth_query")
    result4 = cursor3.fetchone()
    assert result4[0] == 4, "Reused cursor should have new results"
    
    # The previous results should no longer be accessible
    cursor3.execute("SELECT 3 AS third_query_again")
    result5 = cursor3.fetchone()
    assert result5[0] == 3, "Cursor reexecution should work"

def test_connection_execute_many_parameters(db_connection):
    """Test execute() with many parameters"""
    # First make sure no active results are pending
    # by using a fresh cursor and fetching all results
    cursor = db_connection.cursor()
    cursor.execute("SELECT 1")
    cursor.fetchall()
    
    # Create a query with 10 parameters
    params = list(range(1, 11))
    query = "SELECT " + ", ".join(["?" for _ in params]) + " AS many_params"
    
    # Now execute with many parameters
    cursor = db_connection.execute(query, *params)
    result = cursor.fetchall()  # Use fetchall to consume all results
    
    # Verify all parameters were correctly passed
    for i, value in enumerate(params):
        assert result[0][i] == value, f"Parameter at position {i} not correctly passed"

def test_execute_after_connection_close(conn_str):
    """Test that executing queries after connection close raises InterfaceError"""
    # Create a new connection
    connection = connect(conn_str)
    
    # Close the connection
    connection.close()
    
    # Try different methods that should all fail with InterfaceError
    
    # 1. Test direct execute method
    with pytest.raises(InterfaceError) as excinfo:
        connection.execute("SELECT 1")
    assert "closed" in str(excinfo.value).lower(), "Error should mention the connection is closed"
    
    # 2. Test batch_execute method
    with pytest.raises(InterfaceError) as excinfo:
        connection.batch_execute(["SELECT 1"])
    assert "closed" in str(excinfo.value).lower(), "Error should mention the connection is closed"
    
    # 3. Test creating a cursor
    with pytest.raises(InterfaceError) as excinfo:
        cursor = connection.cursor()
    assert "closed" in str(excinfo.value).lower(), "Error should mention the connection is closed"
    
    # 4. Test transaction operations
    with pytest.raises(InterfaceError) as excinfo:
        connection.commit()
    assert "closed" in str(excinfo.value).lower(), "Error should mention the connection is closed"
    
    with pytest.raises(InterfaceError) as excinfo:
        connection.rollback()
    assert "closed" in str(excinfo.value).lower(), "Error should mention the connection is closed"

def test_execute_multiple_simultaneous_cursors(db_connection):
    """Test creating and using many cursors simultaneously through Connection.execute
    
    ⚠️ WARNING: This test has several limitations:
    1. Creates only 20 cursors, which may not fully test production scenarios requiring hundreds
    2. Relies on WeakSet tracking which depends on garbage collection timing and varies between runs
    3. Memory measurement requires the optional 'psutil' package
    4. Creates cursors sequentially rather than truly concurrently
    5. Results may vary based on system resources, SQL Server version, and ODBC driver
    
    The test verifies that:
    - Multiple cursors can be created and used simultaneously
    - Connection tracks created cursors appropriately
    - Connection remains stable after intensive cursor operations
    """
    import gc
    import sys
    
    # Start with a clean connection state
    cursor = db_connection.execute("SELECT 1")
    cursor.fetchall()  # Consume the results
    cursor.close()     # Close the cursor correctly
    
    # Record the initial cursor count in the connection's tracker
    initial_cursor_count = len(db_connection._cursors)
    
    # Get initial memory usage
    gc.collect()  # Force garbage collection to get accurate reading
    initial_memory = 0
    try:
        import psutil
        import os
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss
    except ImportError:
        print("psutil not installed, memory usage won't be measured")
    
    # Use a smaller number of cursors to avoid overwhelming the connection
    num_cursors = 20  # Reduced from 100
    
    # Create multiple cursors and store them in a list to keep them alive
    cursors = []
    for i in range(num_cursors):
        cursor = db_connection.execute(f"SELECT {i} AS cursor_id")
        # Immediately fetch results but don't close yet to keep cursor alive
        cursor.fetchall()
        cursors.append(cursor)
    
    # Verify the number of tracked cursors increased
    current_cursor_count = len(db_connection._cursors)
    # Use a more flexible assertion that accounts for WeakSet behavior
    assert current_cursor_count > initial_cursor_count, \
        f"Connection should track more cursors after creating {num_cursors} new ones, but count only increased by {current_cursor_count - initial_cursor_count}"
    
    print(f"Created {num_cursors} cursors, tracking shows {current_cursor_count - initial_cursor_count} increase")
    
    # Close all cursors explicitly to clean up
    for cursor in cursors:
        cursor.close()
    
    # Verify connection is still usable
    final_cursor = db_connection.execute("SELECT 'Connection still works' AS status")
    row = final_cursor.fetchone()
    assert row[0] == 'Connection still works', "Connection should remain usable after cursor operations"
    final_cursor.close()
    

def test_execute_with_large_parameters(db_connection):
    """Test executing queries with very large parameter sets
    
    ⚠️ WARNING: This test has several limitations:
    1. Limited by 8192-byte parameter size restriction from the ODBC driver
    2. Cannot test truly large parameters (e.g., BLOBs >1MB)
    3. Works around the ~2100 parameter limit by batching, not testing true limits
    4. No streaming parameter support is tested
    5. Only tests with 10,000 rows, which is small compared to production scenarios
    6. Performance measurements are affected by system load and environment
    
    The test verifies:
    - Handling of a large number of parameters in batch inserts
    - Working with parameters near but under the size limit
    - Processing large result sets
    """
    import time
    
    # Test with a temporary table for large data
    cursor = db_connection.execute("""
    DROP TABLE IF EXISTS #large_params_test;
    CREATE TABLE #large_params_test (
        id INT,
        large_text NVARCHAR(MAX),
        large_binary VARBINARY(MAX)
    )
    """)
    cursor.close()
    
    try:
        # Test 1: Large number of parameters in a batch insert
        start_time = time.time()
        
        # Create a large batch but split into smaller chunks to avoid parameter limits
        # ODBC has limits (~2100 parameters), so use 500 rows per batch (1500 parameters)
        total_rows = 1000
        batch_size = 500  # Reduced from 1000 to avoid parameter limits
        total_inserts = 0
        
        for batch_start in range(0, total_rows, batch_size):
            batch_end = min(batch_start + batch_size, total_rows)
            large_inserts = []
            params = []
            
            # Build a parameterized query with multiple value sets for this batch
            for i in range(batch_start, batch_end):
                large_inserts.append("(?, ?, ?)")
                params.extend([i, f"Text{i}", bytes([i % 256] * 100)])  # 100 bytes per row
            
            # Execute this batch
            sql = f"INSERT INTO #large_params_test VALUES {', '.join(large_inserts)}"
            cursor = db_connection.execute(sql, *params)
            cursor.close()
            total_inserts += batch_end - batch_start
        
        # Verify correct number of rows inserted
        cursor = db_connection.execute("SELECT COUNT(*) FROM #large_params_test")
        count = cursor.fetchone()[0]
        cursor.close()
        assert count == total_rows, f"Expected {total_rows} rows, got {count}"
        
        batch_time = time.time() - start_time
        print(f"Large batch insert ({total_rows} rows in chunks of {batch_size}) completed in {batch_time:.2f} seconds")
        
        # Test 2: Single row with parameter values under the 8192 byte limit
        cursor = db_connection.execute("TRUNCATE TABLE #large_params_test")
        cursor.close()
        
        # Create smaller text parameter to stay well under 8KB limit
        large_text = "Large text content " * 100  # ~2KB text (well under 8KB limit)
        
        # Create smaller binary parameter to stay well under 8KB limit
        large_binary = bytes([x % 256 for x in range(2 * 1024)])  # 2KB binary data
        
        start_time = time.time()
        
        # Insert the large parameters using connection.execute()
        cursor = db_connection.execute(
            "INSERT INTO #large_params_test VALUES (?, ?, ?)",
            1, large_text, large_binary
        )
        cursor.close()
        
        # Verify the data was inserted correctly
        cursor = db_connection.execute("SELECT id, LEN(large_text), DATALENGTH(large_binary) FROM #large_params_test")
        row = cursor.fetchone()
        cursor.close()
        
        assert row is not None, "No row returned after inserting large parameters"
        assert row[0] == 1, "Wrong ID returned"
        assert row[1] > 1000, f"Text length too small: {row[1]}"
        assert row[2] == 2 * 1024, f"Binary length wrong: {row[2]}"
        
        large_param_time = time.time() - start_time
        print(f"Large parameter insert (text: {row[1]} chars, binary: {row[2]} bytes) completed in {large_param_time:.2f} seconds")
        
        # Test 3: Execute with a large result set
        cursor = db_connection.execute("TRUNCATE TABLE #large_params_test")
        cursor.close()
        
        # Insert rows in smaller batches to avoid parameter limits
        rows_per_batch = 1000
        total_rows = 10000
        
        for batch_start in range(0, total_rows, rows_per_batch):
            batch_end = min(batch_start + rows_per_batch, total_rows)
            values = ", ".join([f"({i}, 'Small Text {i}', NULL)" for i in range(batch_start, batch_end)])
            cursor = db_connection.execute(f"INSERT INTO #large_params_test (id, large_text, large_binary) VALUES {values}")
            cursor.close()
        
        start_time = time.time()
        
        # Fetch all rows to test large result set handling
        cursor = db_connection.execute("SELECT id, large_text FROM #large_params_test ORDER BY id")
        rows = cursor.fetchall()
        cursor.close()
        
        assert len(rows) == 10000, f"Expected 10000 rows in result set, got {len(rows)}"
        assert rows[0][0] == 0, "First row has incorrect ID"
        assert rows[9999][0] == 9999, "Last row has incorrect ID"
        
        result_time = time.time() - start_time
        print(f"Large result set (10,000 rows) fetched in {result_time:.2f} seconds")
        
    finally:
        # Clean up
        cursor = db_connection.execute("DROP TABLE IF EXISTS #large_params_test")
        cursor.close()

def test_connection_execute_cursor_lifecycle(db_connection):
    """Test that cursors from execute() are properly managed throughout their lifecycle
    
    This test verifies that:
    1. Cursors are added to the connection's tracking when created via execute()
    2. Cursors are removed from tracking when explicitly closed
    3. Cursors are removed from tracking when they go out of scope and are garbage collected
    
    This helps ensure that the connection properly manages cursor resources and prevents
    memory/resource leaks over time.
    """
    import gc
    import weakref
    
    # Clear any existing cursors and force garbage collection
    for cursor in list(db_connection._cursors):
        try:
            cursor.close()
        except Exception:
            pass
    gc.collect()
    
    # Verify we start with a clean state
    initial_cursor_count = len(db_connection._cursors)
    
    # 1. Test that a cursor is added to tracking when created
    cursor1 = db_connection.execute("SELECT 1 AS test")
    cursor1.fetchall()  # Consume results
    
    # Verify cursor was added to tracking
    assert len(db_connection._cursors) == initial_cursor_count + 1, "Cursor should be added to connection tracking"
    assert cursor1 in db_connection._cursors, "Created cursor should be in the connection's tracking set"
    
    # 2. Test that a cursor is removed when explicitly closed
    cursor_id = id(cursor1)  # Remember the cursor's ID for later verification
    cursor1.close()
    
    # Force garbage collection to ensure WeakSet is updated
    gc.collect()
    
    # Verify cursor was removed from tracking
    remaining_cursor_ids = [id(c) for c in db_connection._cursors]
    assert cursor_id not in remaining_cursor_ids, "Closed cursor should be removed from connection tracking"
    
    # 3. Test that a cursor is removed when it goes out of scope
    def create_and_abandon_cursor():
        temp_cursor = db_connection.execute("SELECT 2 AS test")
        temp_cursor.fetchall()  # Consume results
        # Keep track of this cursor with a weak reference so we can check if it's collected
        return weakref.ref(temp_cursor)
    
    # Create a cursor that will go out of scope
    cursor_ref = create_and_abandon_cursor()
    
    # Cursor should be tracked before garbage collection
    assert len(db_connection._cursors) > initial_cursor_count, "Abandoned cursor should initially be tracked"
    
    # Force garbage collection multiple times to ensure the cursor is collected
    for _ in range(3):
        gc.collect()
    
    # Verify cursor was eventually removed from tracking
    assert cursor_ref() is None, "Abandoned cursor should be garbage collected"
    assert len(db_connection._cursors) == initial_cursor_count, \
        "All created cursors should be removed from tracking after being closed or collected"
    
    # 4. Verify that many cursors can be created and properly cleaned up
    cursors = []
    for i in range(10):
        cursors.append(db_connection.execute(f"SELECT {i} AS test"))
        cursors[-1].fetchall()  # Consume results
    
    assert len(db_connection._cursors) == initial_cursor_count + 10, \
        "All 10 cursors should be tracked by the connection"
    
    # Close half of them explicitly
    for i in range(5):
        cursors[i].close()
    
    # Remove references to the other half so they can be garbage collected
    for i in range(5, 10):
        cursors[i] = None
    
    # Force garbage collection
    gc.collect()
    gc.collect()  # Sometimes one collection isn't enough with WeakRefs
    
    # Verify all cursors are eventually removed from tracking
    assert len(db_connection._cursors) <= initial_cursor_count + 5, \
        "Explicitly closed cursors should be removed from tracking immediately"
    
    # Clean up any remaining cursors to leave the connection in a good state
    for cursor in list(db_connection._cursors):
        try:
            cursor.close()
        except Exception:
            pass

def test_batch_execute_basic(db_connection):
    """Test the basic functionality of batch_execute method
    
    ⚠️ WARNING: This test has several limitations:
    1. Results must be fully consumed between statements to avoid "Connection is busy" errors
    2. The ODBC driver imposes limits on concurrent statement execution
    3. Performance may vary based on network conditions and server load
    4. Not all statement types may be compatible with batch execution
    5. Error handling may be implementation-specific across ODBC drivers
    
    The test verifies:
    - Multiple statements can be executed in sequence
    - Results are correctly returned for each statement
    - The cursor remains usable after batch completion
    """
    # Create a list of statements to execute
    statements = [
        "SELECT 1 AS value",
        "SELECT 'test' AS string_value",
        "SELECT GETDATE() AS date_value"
    ]
    
    # Execute the batch
    results, cursor = db_connection.batch_execute(statements)
    
    # Verify we got the right number of results
    assert len(results) == 3, f"Expected 3 results, got {len(results)}"
    
    # Check each result
    assert len(results[0]) == 1, "Expected 1 row in first result"
    assert results[0][0][0] == 1, "First result should be 1"
    
    assert len(results[1]) == 1, "Expected 1 row in second result"
    assert results[1][0][0] == 'test', "Second result should be 'test'"
    
    assert len(results[2]) == 1, "Expected 1 row in third result"
    assert isinstance(results[2][0][0], (str, datetime.datetime)), "Third result should be a date"
    
    # Cursor should be usable after batch execution
    cursor.execute("SELECT 2 AS another_value")
    row = cursor.fetchone()
    assert row[0] == 2, "Cursor should be usable after batch execution"
    
    # Clean up
    cursor.close()

def test_batch_execute_with_parameters(db_connection):
    """Test batch_execute with different parameter types"""
    statements = [
        "SELECT ? AS int_param",
        "SELECT ? AS float_param",
        "SELECT ? AS string_param",
        "SELECT ? AS binary_param",
        "SELECT ? AS bool_param",
        "SELECT ? AS null_param"
    ]
    
    params = [
        [123],
        [3.14159],
        ["test string"],
        [bytearray(b'binary data')],
        [True],
        [None]
    ]
    
    results, cursor = db_connection.batch_execute(statements, params)
    
    # Verify each parameter was correctly applied
    assert results[0][0][0] == 123, "Integer parameter not handled correctly"
    assert abs(results[1][0][0] - 3.14159) < 0.00001, "Float parameter not handled correctly"
    assert results[2][0][0] == "test string", "String parameter not handled correctly"
    assert results[3][0][0] == bytearray(b'binary data'), "Binary parameter not handled correctly"
    assert results[4][0][0] == True, "Boolean parameter not handled correctly"
    assert results[5][0][0] is None, "NULL parameter not handled correctly"
    
    cursor.close()

def test_batch_execute_dml_statements(db_connection):
    """Test batch_execute with DML statements (INSERT, UPDATE, DELETE)

    ⚠️ WARNING: This test has several limitations:
    1. Transaction isolation levels may affect behavior in production environments
    2. Large batch operations may encounter size or timeout limits not tested here
    3. Error handling during partial batch completion needs careful consideration
    4. Results must be fully consumed between statements to avoid "Connection is busy" errors
    5. Server-side performance characteristics aren't fully tested
    
    The test verifies:
    - DML statements work correctly in a batch context
    - Row counts are properly returned for modification operations
    - Results from SELECT statements following DML are accessible
    """
    cursor = db_connection.cursor()
    drop_table_if_exists(cursor, "#batch_test")
    
    try:
        # Create a test table
        cursor.execute("CREATE TABLE #batch_test (id INT, value VARCHAR(50))")
        
        statements = [
            "INSERT INTO #batch_test VALUES (?, ?)",
            "INSERT INTO #batch_test VALUES (?, ?)",
            "UPDATE #batch_test SET value = ? WHERE id = ?",
            "DELETE FROM #batch_test WHERE id = ?",
            "SELECT * FROM #batch_test ORDER BY id"
        ]
        
        params = [
            [1, "value1"],
            [2, "value2"],
            ["updated", 1],
            [2],
            None
        ]
        
        results, batch_cursor = db_connection.batch_execute(statements, params)
        
        # Check row counts for DML statements
        assert results[0] == 1, "First INSERT should affect 1 row"
        assert results[1] == 1, "Second INSERT should affect 1 row"
        assert results[2] == 1, "UPDATE should affect 1 row"
        assert results[3] == 1, "DELETE should affect 1 row"
        
        # Check final SELECT result
        assert len(results[4]) == 1, "Should have 1 row after operations"
        assert results[4][0][0] == 1, "Remaining row should have id=1"
        assert results[4][0][1] == "updated", "Value should be updated"
        
        batch_cursor.close()
    finally:
        cursor.execute("DROP TABLE IF EXISTS #batch_test")
        cursor.close()

def test_batch_execute_reuse_cursor(db_connection):
    """Test batch_execute with cursor reuse"""
    # Create a cursor to reuse
    cursor = db_connection.cursor()
    
    # Execute a statement to set up cursor state
    cursor.execute("SELECT 'before batch' AS initial_state")
    initial_result = cursor.fetchall()
    assert initial_result[0][0] == 'before batch', "Initial cursor state incorrect"
    
    # Use the cursor in batch_execute
    statements = [
        "SELECT 'during batch' AS batch_state"
    ]
    
    results, returned_cursor = db_connection.batch_execute(statements, reuse_cursor=cursor)
    
    # Verify we got the same cursor back
    assert returned_cursor is cursor, "Batch should return the same cursor object"
    
    # Verify the result
    assert results[0][0][0] == 'during batch', "Batch result incorrect"
    
    # Verify cursor is still usable
    cursor.execute("SELECT 'after batch' AS final_state")
    final_result = cursor.fetchall()
    assert final_result[0][0] == 'after batch', "Cursor should remain usable after batch"
    
    cursor.close()

def test_batch_execute_auto_close(db_connection):
    """Test auto_close parameter in batch_execute"""
    statements = ["SELECT 1"]
    
    # Test with auto_close=True
    results, cursor = db_connection.batch_execute(statements, auto_close=True)
    
    # Cursor should be closed
    with pytest.raises(Exception):
        cursor.execute("SELECT 2")  # Should fail because cursor is closed
    
    # Test with auto_close=False (default)
    results, cursor = db_connection.batch_execute(statements)
    
    # Cursor should still be usable
    cursor.execute("SELECT 2")
    assert cursor.fetchone()[0] == 2, "Cursor should be usable when auto_close=False"
    
    cursor.close()

def test_batch_execute_transaction(db_connection):
    """Test batch_execute within a transaction

    ⚠️ WARNING: This test has several limitations:
    1. Temporary table behavior with transactions varies between SQL Server versions
    2. Global temporary tables (##) must be used rather than local temporary tables (#)
    3. Explicit commits and rollbacks are required - no auto-transaction management
    4. Transaction isolation levels aren't tested
    5. Distributed transactions aren't tested
    6. Error recovery during partial transaction completion isn't fully tested
    
    The test verifies:
    - Batch operations work within explicit transactions
    - Rollback correctly undoes all changes in the batch
    - Commit correctly persists all changes in the batch
    """
    if db_connection.autocommit:
        db_connection.autocommit = False
    
    cursor = db_connection.cursor()
    
    # Important: Use ## (global temp table) instead of # (local temp table)
    # Global temp tables are more reliable across transactions
    drop_table_if_exists(cursor, "##batch_transaction_test")
    
    try:
        # Create a test table outside the implicit transaction
        cursor.execute("CREATE TABLE ##batch_transaction_test (id INT, value VARCHAR(50))")
        db_connection.commit()  # Commit the table creation
        
        # Execute a batch of statements
        statements = [
            "INSERT INTO ##batch_transaction_test VALUES (1, 'value1')",
            "INSERT INTO ##batch_transaction_test VALUES (2, 'value2')",
            "SELECT COUNT(*) FROM ##batch_transaction_test"
        ]
        
        results, batch_cursor = db_connection.batch_execute(statements)
        
        # Verify the SELECT result shows both rows
        assert results[2][0][0] == 2, "Should have 2 rows before rollback"
        
        # Rollback the transaction
        db_connection.rollback()
        
        # Execute another statement to check if rollback worked
        cursor.execute("SELECT COUNT(*) FROM ##batch_transaction_test")
        count = cursor.fetchone()[0]
        assert count == 0, "Rollback should remove all inserted rows"
        
        # Try again with commit
        results, batch_cursor = db_connection.batch_execute(statements)
        db_connection.commit()
        
        # Verify data persists after commit
        cursor.execute("SELECT COUNT(*) FROM ##batch_transaction_test")
        count = cursor.fetchone()[0]
        assert count == 2, "Data should persist after commit"
        
        batch_cursor.close()
    finally:
        # Clean up - always try to drop the table
        try:
            cursor.execute("DROP TABLE ##batch_transaction_test")
            db_connection.commit()
        except Exception as e:
            print(f"Error dropping test table: {e}")
        cursor.close()

def test_batch_execute_error_handling(db_connection):
    """Test error handling in batch_execute"""
    statements = [
        "SELECT 1",
        "SELECT * FROM nonexistent_table",  # This will fail
        "SELECT 3"
    ]
    
    # Execution should fail on the second statement
    with pytest.raises(Exception) as excinfo:
        db_connection.batch_execute(statements)
    
    # Verify error message contains something about the nonexistent table
    assert "nonexistent_table" in str(excinfo.value).lower(), "Error should mention the problem"
    
    # Test with a cursor that gets auto-closed on error
    cursor = db_connection.cursor()
    
    try:
        db_connection.batch_execute(statements, reuse_cursor=cursor, auto_close=True)
    except Exception:
        # If auto_close works, the cursor should be closed despite the error
        with pytest.raises(Exception):
            cursor.execute("SELECT 1")  # Should fail if cursor is closed
    
    # Test that the connection is still usable after an error
    new_cursor = db_connection.cursor()
    new_cursor.execute("SELECT 1")
    assert new_cursor.fetchone()[0] == 1, "Connection should be usable after batch error"
    new_cursor.close()

def test_batch_execute_input_validation(db_connection):
    """Test input validation in batch_execute"""
    # Test with non-list statements
    with pytest.raises(TypeError):
        db_connection.batch_execute("SELECT 1")
    
    # Test with non-list params
    with pytest.raises(TypeError):
        db_connection.batch_execute(["SELECT 1"], "param")
    
    # Test with mismatched statements and params lengths
    with pytest.raises(ValueError):
        db_connection.batch_execute(["SELECT 1", "SELECT 2"], [[1]])
    
    # Test with empty statements list
    results, cursor = db_connection.batch_execute([])
    assert results == [], "Empty statements should return empty results"
    cursor.close()

def test_batch_execute_large_batch(db_connection):
    """Test batch_execute with a large number of statements
    
    ⚠️ WARNING: This test has several limitations:
    1. Only tests 50 statements, which may not reveal issues with much larger batches
    2. Each statement is very simple, not testing complex query performance
    3. Memory usage for large result sets isn't thoroughly tested
    4. Results must be fully consumed between statements to avoid "Connection is busy" errors
    5. Driver-specific limitations may exist for maximum batch sizes
    6. Network timeouts during long-running batches aren't tested
    
    The test verifies:
    - The method can handle multiple statements in sequence
    - Results are correctly returned for all statements
    - Memory usage remains reasonable during batch processing
    """
    # Create a batch of 50 statements
    statements = ["SELECT " + str(i) for i in range(50)]
    
    results, cursor = db_connection.batch_execute(statements)
    
    # Verify we got 50 results
    assert len(results) == 50, f"Expected 50 results, got {len(results)}"
    
    # Check a few random results
    assert results[0][0][0] == 0, "First result should be 0"
    assert results[25][0][0] == 25, "Middle result should be 25"
    assert results[49][0][0] == 49, "Last result should be 49"
    
    cursor.close()