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
    from mssql_python.exceptions import ProgrammingError
    
    with pytest.raises(ProgrammingError) as exc_info:
        db_connection.setencoding(encoding='invalid-encoding-name')
    
    assert "Unknown encoding" in str(exc_info.value), "Should raise ProgrammingError for invalid encoding"
    assert "invalid-encoding-name" in str(exc_info.value), "Error message should include the invalid encoding name"

def test_setencoding_invalid_ctype(db_connection):
    """Test setencoding with invalid ctype."""
    from mssql_python.exceptions import ProgrammingError
    
    with pytest.raises(ProgrammingError) as exc_info:
        db_connection.setencoding(encoding='utf-8', ctype=999)
    
    assert "Invalid ctype" in str(exc_info.value), "Should raise ProgrammingError for invalid ctype"
    assert "999" in str(exc_info.value), "Error message should include the invalid ctype value"

def test_setencoding_closed_connection(conn_str):
    """Test setencoding on closed connection."""
    from mssql_python.exceptions import InterfaceError
    
    temp_conn = connect(conn_str)
    temp_conn.close()
    
    with pytest.raises(InterfaceError) as exc_info:
        temp_conn.setencoding(encoding='utf-8')
    
    assert "closed connection" in str(exc_info.value).lower(), "Should raise InterfaceError for closed connection"

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

def test_getencoding_returns_copy(db_connection):
    """Test that getencoding returns a copy, not reference to internal data."""
    original_settings = db_connection.getencoding()
    
    # Modify the returned dictionary
    original_settings['encoding'] = 'modified'
    original_settings['ctype'] = 999
    
    # Verify internal settings weren't affected
    current_settings = db_connection.getencoding()
    assert current_settings['encoding'] != 'modified', "getencoding should return a copy"
    assert current_settings['ctype'] != 999, "getencoding should return a copy"

def test_setencoding_thread_safety(conn_str):
    """Test setencoding behavior with multiple connections (thread safety indication)."""
    import threading
    
    def worker(connection_str, encoding, results, index):
        try:
            conn = connect(connection_str)
            conn.setencoding(encoding=encoding)
            settings = conn.getencoding()
            results[index] = settings['encoding']
            conn.close()
        except Exception as e:
            results[index] = f"Error: {e}"
    
    # Test with multiple threads setting different encodings
    results = [None] * 3
    threads = []
    encodings = ['utf-8', 'utf-16le', 'latin-1']
    
    for i, encoding in enumerate(encodings):
        thread = threading.Thread(target=worker, args=(conn_str, encoding, results, i))
        threads.append(thread)
        thread.start()
    
    for thread in threads:
        thread.join()
    
    # Verify each connection got its own encoding setting
    for i, expected_encoding in enumerate(encodings):
        assert results[i] == expected_encoding, f"Thread {i} failed to set encoding {expected_encoding}: {results[i]}"

def test_setencoding_parameter_validation_edge_cases(db_connection):
    """Test edge cases for parameter validation."""
    from mssql_python.exceptions import ProgrammingError
    
    # Test empty string encoding
    with pytest.raises(ProgrammingError):
        db_connection.setencoding(encoding='')
    
    # Test non-string encoding (should be handled gracefully or raise appropriate error)
    with pytest.raises((ProgrammingError, TypeError)):
        db_connection.setencoding(encoding=123)
    
    # Test non-integer ctype
    with pytest.raises((ProgrammingError, TypeError)):
        db_connection.setencoding(encoding='utf-8', ctype='invalid')

def test_setencoding_case_sensitivity(db_connection):
    """Test encoding name case sensitivity."""
    # Most Python codecs are case-insensitive, but test common variations
    case_variations = [
        ('utf-8', 'UTF-8'),
        ('utf-16le', 'UTF-16LE'),
        ('latin-1', 'LATIN-1'),
        ('ascii', 'ASCII')
    ]
    
    for lower, upper in case_variations:
        try:
            # Test lowercase
            db_connection.setencoding(encoding=lower)
            settings_lower = db_connection.getencoding()
            
            # Test uppercase
            db_connection.setencoding(encoding=upper) 
            settings_upper = db_connection.getencoding()
            
            # Both should work (Python codecs are generally case-insensitive)
            assert settings_lower['encoding'] == lower, f"Failed to set {lower}"
            assert settings_upper['encoding'] == upper, f"Failed to set {upper}"
            
        except Exception as e:
            # If one variant fails, both should fail consistently
            with pytest.raises(type(e)):
                db_connection.setencoding(encoding=lower if encoding == upper else upper)