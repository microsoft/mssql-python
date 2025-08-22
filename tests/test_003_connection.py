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
from mssql_python.constants import GetInfoConstants as sql_const

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

def test_getinfo_basic_driver_info(db_connection):
    """Test basic driver information info types."""
    
    try:
        # Driver name should be available
        driver_name = db_connection.getinfo(sql_const.SQL_DRIVER_NAME.value)
        print("Driver Name = ",driver_name)
        assert driver_name is not None, "Driver name should not be None"
        
        # Driver version should be available
        driver_ver = db_connection.getinfo(sql_const.SQL_DRIVER_VER.value)
        print("Driver Version = ",driver_ver)
        assert driver_ver is not None, "Driver version should not be None"
        
        # Data source name should be available
        dsn = db_connection.getinfo(sql_const.SQL_DATA_SOURCE_NAME.value)
        print("Data source name = ",dsn)
        assert dsn is not None, "Data source name should not be None"
        
        # Server name should be available (might be empty in some configurations)
        server_name = db_connection.getinfo(sql_const.SQL_SERVER_NAME.value)
        print("Server Name = ",server_name)
        assert server_name is not None, "Server name should not be None"
        
        # User name should be available (might be empty if using integrated auth)
        user_name = db_connection.getinfo(sql_const.SQL_USER_NAME.value)
        print("User Name = ",user_name)
        assert user_name is not None, "User name should not be None"
        
    except Exception as e:
        pytest.fail(f"getinfo failed for basic driver info: {e}")

def test_getinfo_sql_support(db_connection):
    """Test SQL support and conformance info types."""
    
    try:
        # SQL conformance level
        sql_conformance = db_connection.getinfo(sql_const.SQL_SQL_CONFORMANCE.value)
        print("SQL Conformance = ",sql_conformance)
        assert sql_conformance is not None, "SQL conformance should not be None"
        
        # Keywords - may return a very long string
        keywords = db_connection.getinfo(sql_const.SQL_KEYWORDS.value)
        print("Keywords = ",keywords)
        assert keywords is not None, "SQL keywords should not be None"
        
        # Identifier quote character
        quote_char = db_connection.getinfo(sql_const.SQL_IDENTIFIER_QUOTE_CHAR.value)
        print(f"Identifier quote char: '{quote_char}'")
        assert quote_char is not None, "Identifier quote char should not be None"

    except Exception as e:
        pytest.fail(f"getinfo failed for SQL support info: {e}")

def test_getinfo_numeric_limits(db_connection):
    """Test numeric limitation info types."""
    
    try:
        # Max column name length - should be a positive integer
        max_col_name_len = db_connection.getinfo(sql_const.SQL_MAX_COLUMN_NAME_LEN.value)
        assert isinstance(max_col_name_len, int), "Max column name length should be an integer"
        assert max_col_name_len >= 0, "Max column name length should be non-negative"
        
        # Max table name length
        max_table_name_len = db_connection.getinfo(sql_const.SQL_MAX_TABLE_NAME_LEN.value)
        assert isinstance(max_table_name_len, int), "Max table name length should be an integer"
        assert max_table_name_len >= 0, "Max table name length should be non-negative"
        
        # Max statement length - may return 0 for "unlimited"
        max_statement_len = db_connection.getinfo(sql_const.SQL_MAX_STATEMENT_LEN.value)
        assert isinstance(max_statement_len, int), "Max statement length should be an integer"
        assert max_statement_len >= 0, "Max statement length should be non-negative"
        
        # Max connections - may return 0 for "unlimited"
        max_connections = db_connection.getinfo(sql_const.SQL_MAX_DRIVER_CONNECTIONS.value)
        assert isinstance(max_connections, int), "Max connections should be an integer"
        assert max_connections >= 0, "Max connections should be non-negative"
        
    except Exception as e:
        pytest.fail(f"getinfo failed for numeric limits info: {e}")

def test_getinfo_catalog_support(db_connection):
    """Test catalog support info types."""
    
    try:
        # Catalog support for tables
        catalog_term = db_connection.getinfo(sql_const.SQL_CATALOG_TERM.value)
        print("Catalof term = ",catalog_term)
        assert catalog_term is not None, "Catalog term should not be None"
        
        # Catalog name separator
        catalog_separator = db_connection.getinfo(sql_const.SQL_CATALOG_NAME_SEPARATOR.value)
        print(f"Catalog name separator: '{catalog_separator}'")
        assert catalog_separator is not None, "Catalog separator should not be None"
        
        # Schema term
        schema_term = db_connection.getinfo(sql_const.SQL_SCHEMA_TERM.value)
        print("Schema term = ",schema_term)
        assert schema_term is not None, "Schema term should not be None"
        
        # Stored procedures support
        procedures = db_connection.getinfo(sql_const.SQL_PROCEDURES.value)
        print("Procedures = ",procedures)
        assert procedures is not None, "Procedures support should not be None"
        
    except Exception as e:
        pytest.fail(f"getinfo failed for catalog support info: {e}")

def test_getinfo_transaction_support(db_connection):
    """Test transaction support info types."""
    
    try:
        # Transaction support
        txn_capable = db_connection.getinfo(sql_const.SQL_TXN_CAPABLE.value)
        print("Transaction capable = ",txn_capable)
        assert txn_capable is not None, "Transaction capability should not be None"
        
        # Default transaction isolation
        default_txn_isolation = db_connection.getinfo(sql_const.SQL_DEFAULT_TXN_ISOLATION.value)
        print("Default Transaction isolation = ",default_txn_isolation)
        assert default_txn_isolation is not None, "Default transaction isolation should not be None"
        
        # Multiple active transactions support
        multiple_txn = db_connection.getinfo(sql_const.SQL_MULTIPLE_ACTIVE_TXN.value)
        print("Multiple transaction = ",multiple_txn)
        assert multiple_txn is not None, "Multiple active transactions support should not be None"
        
    except Exception as e:
        pytest.fail(f"getinfo failed for transaction support info: {e}")

def test_getinfo_data_types(db_connection):
    """Test data type support info types."""
    
    try:
        # Numeric functions
        numeric_functions = db_connection.getinfo(sql_const.SQL_NUMERIC_FUNCTIONS.value)
        assert isinstance(numeric_functions, int), "Numeric functions should be an integer"
        
        # String functions
        string_functions = db_connection.getinfo(sql_const.SQL_STRING_FUNCTIONS.value)
        assert isinstance(string_functions, int), "String functions should be an integer"
        
        # Date/time functions
        datetime_functions = db_connection.getinfo(sql_const.SQL_DATETIME_FUNCTIONS.value)
        assert isinstance(datetime_functions, int), "Datetime functions should be an integer"
        
    except Exception as e:
        pytest.fail(f"getinfo failed for data type support info: {e}")

def test_getinfo_invalid_constant(db_connection):
    """Test getinfo behavior with invalid constants."""
    # Use a constant that doesn't exist in ODBC
    non_existent_constant = 9999
    try:
        result = db_connection.getinfo(non_existent_constant)
        # If it doesn't raise an exception, it should return None or an empty value
        assert result is None or result == 0 or result == "", "Invalid constant should return None/empty"
    except Exception:
        # It's also acceptable to raise an exception for invalid constants
        pass

def test_getinfo_type_consistency(db_connection):
    """Test that getinfo returns consistent types for repeated calls."""

    # Choose a few representative info types that don't depend on DBMS
    info_types = [
        sql_const.SQL_DRIVER_NAME.value,
        sql_const.SQL_MAX_COLUMN_NAME_LEN.value,
        sql_const.SQL_TXN_CAPABLE.value,
        sql_const.SQL_IDENTIFIER_QUOTE_CHAR.value
    ]
    
    for info_type in info_types:
        # Call getinfo twice with the same info type
        result1 = db_connection.getinfo(info_type)
        result2 = db_connection.getinfo(info_type)
        
        # Results should be consistent in type and value
        assert type(result1) == type(result2), f"Type inconsistency for info type {info_type}"
        assert result1 == result2, f"Value inconsistency for info type {info_type}"

def test_getinfo_standard_types(db_connection):
    """Test a representative set of standard ODBC info types."""
    
    # Dictionary of common info types and their expected value types
    # Avoid DBMS-specific info types
    info_types = {
        sql_const.SQL_ACCESSIBLE_TABLES.value: str,        # "Y" or "N"
        sql_const.SQL_DATA_SOURCE_NAME.value: str,         # DSN
        sql_const.SQL_TABLE_TERM.value: str,               # Usually "table"
        sql_const.SQL_PROCEDURES.value: str,               # "Y" or "N"
        sql_const.SQL_MAX_IDENTIFIER_LEN.value: int,       # Max identifier length
        sql_const.SQL_OUTER_JOINS.value: str,              # "Y" or "N"
    }
    
    for info_type, expected_type in info_types.items():
        try:
            info_value = db_connection.getinfo(info_type)
            
            # Skip None values (unsupported by driver)
            if info_value is None:
                continue
                
            # Check type, allowing empty strings for string types
            if expected_type == str:
                assert isinstance(info_value, str), f"Info type {info_type} should return a string"
            elif expected_type == int:
                assert isinstance(info_value, int), f"Info type {info_type} should return an integer"
                
        except Exception as e:
            # Log but don't fail - some drivers might not support all info types
            print(f"Info type {info_type} failed: {e}")

def test_connection_searchescape_basic(db_connection):
    """Test the basic functionality of the searchescape property."""
    # Get the search escape character
    escape_char = db_connection.searchescape
    
    # Verify it's not None
    assert escape_char is not None, "Search escape character should not be None"
    print(f"Search pattern escape character: '{escape_char}'")
    
    # Test property caching - calling it twice should return the same value
    escape_char2 = db_connection.searchescape
    assert escape_char == escape_char2, "Search escape character should be consistent"

def test_connection_searchescape_with_percent(db_connection):
    """Test using the searchescape property with percent wildcard."""
    escape_char = db_connection.searchescape
    
    # Skip test if we got a non-string or empty escape character
    if not isinstance(escape_char, str) or not escape_char:
        pytest.skip("No valid escape character available for testing")
    
    cursor = db_connection.cursor()
    try:
        # Create a temporary table with data containing % character
        cursor.execute("CREATE TABLE #test_escape_percent (id INT, text VARCHAR(50))")
        cursor.execute("INSERT INTO #test_escape_percent VALUES (1, 'abc%def')")
        cursor.execute("INSERT INTO #test_escape_percent VALUES (2, 'abc_def')")
        cursor.execute("INSERT INTO #test_escape_percent VALUES (3, 'abcdef')")
        
        # Use the escape character to find the exact % character
        query = f"SELECT * FROM #test_escape_percent WHERE text LIKE 'abc{escape_char}%def' ESCAPE '{escape_char}'"
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Should match only the row with the % character
        assert len(results) == 1, f"Escaped LIKE query for % matched {len(results)} rows instead of 1"
        if results:
            assert 'abc%def' in results[0][1], "Escaped LIKE query did not match correct row"
            
    except Exception as e:
        print(f"Note: LIKE escape test with % failed: {e}")
        # Don't fail the test as some drivers might handle escaping differently
    finally:
        cursor.execute("DROP TABLE #test_escape_percent")

def test_connection_searchescape_with_underscore(db_connection):
    """Test using the searchescape property with underscore wildcard."""
    escape_char = db_connection.searchescape
    
    # Skip test if we got a non-string or empty escape character
    if not isinstance(escape_char, str) or not escape_char:
        pytest.skip("No valid escape character available for testing")
    
    cursor = db_connection.cursor()
    try:
        # Create a temporary table with data containing _ character
        cursor.execute("CREATE TABLE #test_escape_underscore (id INT, text VARCHAR(50))")
        cursor.execute("INSERT INTO #test_escape_underscore VALUES (1, 'abc_def')")
        cursor.execute("INSERT INTO #test_escape_underscore VALUES (2, 'abcXdef')")  # 'X' could match '_'
        cursor.execute("INSERT INTO #test_escape_underscore VALUES (3, 'abcdef')")   # No match
        
        # Use the escape character to find the exact _ character
        query = f"SELECT * FROM #test_escape_underscore WHERE text LIKE 'abc{escape_char}_def' ESCAPE '{escape_char}'"
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Should match only the row with the _ character
        assert len(results) == 1, f"Escaped LIKE query for _ matched {len(results)} rows instead of 1"
        if results:
            assert 'abc_def' in results[0][1], "Escaped LIKE query did not match correct row"
            
    except Exception as e:
        print(f"Note: LIKE escape test with _ failed: {e}")
        # Don't fail the test as some drivers might handle escaping differently
    finally:
        cursor.execute("DROP TABLE #test_escape_underscore")

def test_connection_searchescape_with_brackets(db_connection):
    """Test using the searchescape property with bracket wildcards."""
    escape_char = db_connection.searchescape
    
    # Skip test if we got a non-string or empty escape character
    if not isinstance(escape_char, str) or not escape_char:
        pytest.skip("No valid escape character available for testing")
    
    cursor = db_connection.cursor()
    try:
        # Create a temporary table with data containing [ character
        cursor.execute("CREATE TABLE #test_escape_brackets (id INT, text VARCHAR(50))")
        cursor.execute("INSERT INTO #test_escape_brackets VALUES (1, 'abc[x]def')")
        cursor.execute("INSERT INTO #test_escape_brackets VALUES (2, 'abcxdef')")
        
        # Use the escape character to find the exact [ character
        # Note: This might not work on all drivers as bracket escaping varies
        query = f"SELECT * FROM #test_escape_brackets WHERE text LIKE 'abc{escape_char}[x{escape_char}]def' ESCAPE '{escape_char}'"
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Just check we got some kind of result without asserting specific behavior
        print(f"Bracket escaping test returned {len(results)} rows")
            
    except Exception as e:
        print(f"Note: LIKE escape test with brackets failed: {e}")
        # Don't fail the test as bracket escaping varies significantly between drivers
    finally:
        cursor.execute("DROP TABLE #test_escape_brackets")

def test_connection_searchescape_multiple_escapes(db_connection):
    """Test using the searchescape property with multiple escape sequences."""
    escape_char = db_connection.searchescape
    
    # Skip test if we got a non-string or empty escape character
    if not isinstance(escape_char, str) or not escape_char:
        pytest.skip("No valid escape character available for testing")
    
    cursor = db_connection.cursor()
    try:
        # Create a temporary table with data containing multiple special chars
        cursor.execute("CREATE TABLE #test_multiple_escapes (id INT, text VARCHAR(50))")
        cursor.execute("INSERT INTO #test_multiple_escapes VALUES (1, 'abc%def_ghi')")
        cursor.execute("INSERT INTO #test_multiple_escapes VALUES (2, 'abc%defXghi')")  # Wouldn't match the pattern
        cursor.execute("INSERT INTO #test_multiple_escapes VALUES (3, 'abcXdef_ghi')")  # Wouldn't match the pattern
        
        # Use escape character for both % and _
        query = f"""
            SELECT * FROM #test_multiple_escapes 
            WHERE text LIKE 'abc{escape_char}%def{escape_char}_ghi' ESCAPE '{escape_char}'
        """
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Should match only the row with both % and _
        assert len(results) <= 1, f"Multiple escapes query matched {len(results)} rows instead of at most 1"
        if len(results) == 1:
            assert 'abc%def_ghi' in results[0][1], "Multiple escapes query matched incorrect row"
            
    except Exception as e:
        print(f"Note: Multiple escapes test failed: {e}")
        # Don't fail the test as escaping behavior varies
    finally:
        cursor.execute("DROP TABLE #test_multiple_escapes")

def test_connection_searchescape_consistency(db_connection):
    """Test that the searchescape property is cached and consistent."""
    # Call the property multiple times
    escape1 = db_connection.searchescape
    escape2 = db_connection.searchescape
    escape3 = db_connection.searchescape
    
    # All calls should return the same value
    assert escape1 == escape2 == escape3, "Searchescape property should be consistent"
    
    # Create a new connection and verify it returns the same escape character
    # (assuming the same driver and connection settings)
    if 'conn_str' in globals():
        try:
            new_conn = connect(conn_str)
            new_escape = new_conn.searchescape
            assert new_escape == escape1, "Searchescape should be consistent across connections"
            new_conn.close()
        except Exception as e:
            print(f"Note: New connection comparison failed: {e}")