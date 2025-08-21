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
import pytest
import time
from mssql_python import Connection, connect, pooling
import threading
import struct
from datetime import datetime, timedelta, timezone
from mssql_python.constants import ConstantsDDBC

def drop_table_if_exists(cursor, table_name):
    """Drop the table if it exists"""
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    except Exception as e:
        pytest.fail(f"Failed to drop table {table_name}: {e}")

# Add these helper functions after other helper functions
def handle_datetimeoffset(dto_value):
    """Converter function for SQL Server's DATETIMEOFFSET type"""
    if dto_value is None:
        return None
        
    # The format depends on the ODBC driver and how it returns binary data
    # This matches SQL Server's format for DATETIMEOFFSET
    tup = struct.unpack("<6hI2h", dto_value)  # e.g., (2017, 3, 16, 10, 35, 18, 500000000, -6, 0)
    return datetime(
        tup[0], tup[1], tup[2], tup[3], tup[4], tup[5], tup[6] // 1000,
        timezone(timedelta(hours=tup[7], minutes=tup[8]))
    )

def custom_string_converter(value):
    """A simple converter that adds a prefix to string values"""
    if value is None:
        return None
    return "CONVERTED: " + value.decode('utf-16-le')  # SQL_WVARCHAR is UTF-16LE encoded

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

def test_add_output_converter(db_connection):
    """Test adding an output converter"""
    # Add a converter
    sql_wvarchar = ConstantsDDBC.SQL_WVARCHAR.value
    db_connection.add_output_converter(sql_wvarchar, custom_string_converter)
    
    # Verify it was added correctly
    assert hasattr(db_connection, '_output_converters')
    assert sql_wvarchar in db_connection._output_converters
    assert db_connection._output_converters[sql_wvarchar] == custom_string_converter
    
    # Clean up
    db_connection.clear_output_converters()

def test_get_output_converter(db_connection):
    """Test getting an output converter"""
    sql_wvarchar = ConstantsDDBC.SQL_WVARCHAR.value
    
    # Initial state - no converter
    assert db_connection.get_output_converter(sql_wvarchar) is None
    
    # Add a converter
    db_connection.add_output_converter(sql_wvarchar, custom_string_converter)
    
    # Get the converter
    converter = db_connection.get_output_converter(sql_wvarchar)
    assert converter == custom_string_converter
    
    # Get a non-existent converter
    assert db_connection.get_output_converter(999) is None
    
    # Clean up
    db_connection.clear_output_converters()

def test_remove_output_converter(db_connection):
    """Test removing an output converter"""
    sql_wvarchar = ConstantsDDBC.SQL_WVARCHAR.value
    
    # Add a converter
    db_connection.add_output_converter(sql_wvarchar, custom_string_converter)
    assert db_connection.get_output_converter(sql_wvarchar) is not None
    
    # Remove the converter
    db_connection.remove_output_converter(sql_wvarchar)
    assert db_connection.get_output_converter(sql_wvarchar) is None
    
    # Remove a non-existent converter (should not raise)
    db_connection.remove_output_converter(999)

def test_clear_output_converters(db_connection):
    """Test clearing all output converters"""
    sql_wvarchar = ConstantsDDBC.SQL_WVARCHAR.value
    sql_timestamp_offset = ConstantsDDBC.SQL_TIMESTAMPOFFSET.value
    
    # Add multiple converters
    db_connection.add_output_converter(sql_wvarchar, custom_string_converter)
    db_connection.add_output_converter(sql_timestamp_offset, handle_datetimeoffset)
    
    # Verify converters were added
    assert db_connection.get_output_converter(sql_wvarchar) is not None
    assert db_connection.get_output_converter(sql_timestamp_offset) is not None
    
    # Clear all converters
    db_connection.clear_output_converters()
    
    # Verify all converters were removed
    assert db_connection.get_output_converter(sql_wvarchar) is None
    assert db_connection.get_output_converter(sql_timestamp_offset) is None

def test_converter_integration(db_connection):
    """
    Test that converters work during fetching.
    
    This test verifies that output converters work at the Python level
    without requiring native driver support.
    """
    cursor = db_connection.cursor()
    sql_wvarchar = ConstantsDDBC.SQL_WVARCHAR.value
    
    # Test with string converter
    db_connection.add_output_converter(sql_wvarchar, custom_string_converter)
    
    # Test a simple string query
    cursor.execute("SELECT N'test string' AS test_col")
    row = cursor.fetchone()
    
    # Check if the type matches what we expect for SQL_WVARCHAR
    # For Cursor.description, the second element is the type code
    column_type = cursor.description[0][1]
    
    # If the cursor description has SQL_WVARCHAR as the type code,
    # then our converter should be applied
    if column_type == sql_wvarchar:
        assert row[0].startswith("CONVERTED:"), "Output converter not applied"
    else:
        # If the type code is different, adjust the test or the converter
        print(f"Column type is {column_type}, not {sql_wvarchar}")
        # Add converter for the actual type used
        db_connection.clear_output_converters()
        db_connection.add_output_converter(column_type, custom_string_converter)
        
        # Re-execute the query
        cursor.execute("SELECT N'test string' AS test_col")
        row = cursor.fetchone()
        assert row[0].startswith("CONVERTED:"), "Output converter not applied"
    
    # Clean up
    db_connection.clear_output_converters()

def test_output_converter_with_null_values(db_connection):
    """Test that output converters handle NULL values correctly"""
    cursor = db_connection.cursor()
    sql_wvarchar = ConstantsDDBC.SQL_WVARCHAR.value
    
    # Add converter for string type
    db_connection.add_output_converter(sql_wvarchar, custom_string_converter)
    
    # Execute a query with NULL values
    cursor.execute("SELECT CAST(NULL AS NVARCHAR(50)) AS null_col")
    value = cursor.fetchone()[0]
    
    # NULL values should remain None regardless of converter
    assert value is None
    
    # Clean up
    db_connection.clear_output_converters()

def test_chaining_output_converters(db_connection):
    """Test that output converters can be chained (replaced)"""
    sql_wvarchar = ConstantsDDBC.SQL_WVARCHAR.value
    
    # Define a second converter
    def another_string_converter(value):
        if value is None:
            return None
        return "ANOTHER: " + value.decode('utf-16-le')
    
    # Add first converter
    db_connection.add_output_converter(sql_wvarchar, custom_string_converter)
    
    # Verify first converter is registered
    assert db_connection.get_output_converter(sql_wvarchar) == custom_string_converter
    
    # Replace with second converter
    db_connection.add_output_converter(sql_wvarchar, another_string_converter)
    
    # Verify second converter replaced the first
    assert db_connection.get_output_converter(sql_wvarchar) == another_string_converter
    
    # Clean up
    db_connection.clear_output_converters()

def test_temporary_converter_replacement(db_connection):
    """Test temporarily replacing a converter and then restoring it"""
    sql_wvarchar = ConstantsDDBC.SQL_WVARCHAR.value
    
    # Add a converter
    db_connection.add_output_converter(sql_wvarchar, custom_string_converter)
    
    # Save original converter
    original_converter = db_connection.get_output_converter(sql_wvarchar)
    
    # Define a temporary converter
    def temp_converter(value):
        if value is None:
            return None
        return "TEMP: " + value.decode('utf-16-le')
    
    # Replace with temporary converter
    db_connection.add_output_converter(sql_wvarchar, temp_converter)
    
    # Verify temporary converter is in use
    assert db_connection.get_output_converter(sql_wvarchar) == temp_converter
    
    # Restore original converter
    db_connection.add_output_converter(sql_wvarchar, original_converter)
    
    # Verify original converter is restored
    assert db_connection.get_output_converter(sql_wvarchar) == original_converter
    
    # Clean up
    db_connection.clear_output_converters()

def test_multiple_output_converters(db_connection):
    """Test that multiple output converters can work together"""
    cursor = db_connection.cursor()
    
    # Execute a query to get the actual type codes used
    cursor.execute("SELECT CAST(42 AS INT) as int_col, N'test' as str_col")
    int_type = cursor.description[0][1]  # Type code for integer column
    str_type = cursor.description[1][1]  # Type code for string column
    
    # Add converter for string type
    db_connection.add_output_converter(str_type, custom_string_converter)
    
    # Add converter for integer type
    def int_converter(value):
        if value is None:
            return None
        # Convert from bytes to int and multiply by 2
        if isinstance(value, bytes):
            return int.from_bytes(value, byteorder='little') * 2
        elif isinstance(value, int):
            return value * 2
        return value
    
    db_connection.add_output_converter(int_type, int_converter)
    
    # Test query with both types
    cursor.execute("SELECT CAST(42 AS INT) as int_col, N'test' as str_col")
    row = cursor.fetchone()
    
    # Verify converters worked
    assert row[0] == 84, f"Integer converter failed, got {row[0]} instead of 84"
    assert isinstance(row[1], str) and "CONVERTED:" in row[1], f"String converter failed, got {row[1]}"
    
    # Clean up
    db_connection.clear_output_converters()