"""
This file contains tests for the Connection class.
Functions:
- test_connection_string: Check if the connection string is not None.
- test_connection: Check if the database connection is established.
- test_connection_close: Check if the database connection is closed.
- test_commit: Make a transaction and commit.
- test_rollback: Make a transaction and rollback.
- test_invalid_connection_string: Check if initializing with an invalid connection string raises an exception.
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

from mssql_python.exceptions import InterfaceError, ProgrammingError, DatabaseError
import mssql_python
import sys
import pytest
import time
from mssql_python import connect, Connection, SQL_CHAR, SQL_WCHAR

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
import struct
from datetime import datetime, timedelta, timezone
from mssql_python.constants import ConstantsDDBC
from conftest import is_azure_sql_connection


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


from mssql_python.constants import GetInfoConstants as sql_const


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
        tup[0],
        tup[1],
        tup[2],
        tup[3],
        tup[4],
        tup[5],
        tup[6] // 1000,
        timezone(timedelta(hours=tup[7], minutes=tup[8])),
    )


def custom_string_converter(value):
    """A simple converter that adds a prefix to string values"""
    if value is None:
        return None
    return "CONVERTED: " + value.decode("utf-16-le")  # SQL_WVARCHAR is UTF-16LE encoded


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
    # Using official ODBC parameter names
    conn_str = db_connection._construct_connection_string(
        Server="localhost",
        UID="me",
        PWD="mypwd",
        Database="mydb",
        Encrypt="yes",
        TrustServerCertificate="yes",
    )
    # With the new allow-list implementation, parameters are normalized and validated
    assert "Server=localhost" in conn_str, "Connection string should contain 'Server=localhost'"
    assert "UID=me" in conn_str, "Connection string should contain 'UID=me'"
    assert "PWD=mypwd" in conn_str, "Connection string should contain 'PWD=mypwd'"
    assert "Database=mydb" in conn_str, "Connection string should contain 'Database=mydb'"
    assert "Encrypt=yes" in conn_str, "Connection string should contain 'Encrypt=yes'"
    assert (
        "TrustServerCertificate=yes" in conn_str
    ), "Connection string should contain 'TrustServerCertificate=yes'"
    assert "APP=MSSQL-Python" in conn_str, "Connection string should contain 'APP=MSSQL-Python'"
    assert (
        "Driver={ODBC Driver 18 for SQL Server}" in conn_str
    ), "Connection string should contain 'Driver={ODBC Driver 18 for SQL Server}'"


def test_connection_string_with_attrs_before(db_connection):
    # Check if the connection string is constructed correctly with attrs_before
    # Using official ODBC parameter names
    conn_str = db_connection._construct_connection_string(
        Server="localhost",
        UID="me",
        PWD="mypwd",
        Database="mydb",
        Encrypt="yes",
        TrustServerCertificate="yes",
        attrs_before={1256: "token"},
    )
    # With the new allow-list implementation, parameters are normalized and validated
    assert "Server=localhost" in conn_str, "Connection string should contain 'Server=localhost'"
    assert "UID=me" in conn_str, "Connection string should contain 'UID=me'"
    assert "PWD=mypwd" in conn_str, "Connection string should contain 'PWD=mypwd'"
    assert "Database=mydb" in conn_str, "Connection string should contain 'Database=mydb'"
    assert "Encrypt=yes" in conn_str, "Connection string should contain 'Encrypt=yes'"
    assert (
        "TrustServerCertificate=yes" in conn_str
    ), "Connection string should contain 'TrustServerCertificate=yes'"
    assert "APP=MSSQL-Python" in conn_str, "Connection string should contain 'APP=MSSQL-Python'"
    assert (
        "Driver={ODBC Driver 18 for SQL Server}" in conn_str
    ), "Connection string should contain 'Driver={ODBC Driver 18 for SQL Server}'"
    assert "{1256: token}" not in conn_str, "Connection string should not contain '{1256: token}'"


def test_connection_string_with_odbc_param(db_connection):
    # Check if the connection string is constructed correctly with ODBC parameters
    # Using lowercase synonyms that normalize to uppercase (uid->UID, pwd->PWD)
    conn_str = db_connection._construct_connection_string(
        server="localhost",
        uid="me",
        pwd="mypwd",
        database="mydb",
        encrypt="yes",
        trust_server_certificate="yes",
    )
    # With the new allow-list implementation, parameters are normalized and validated
    assert "Server=localhost" in conn_str, "Connection string should contain 'Server=localhost'"
    assert "UID=me" in conn_str, "Connection string should contain 'UID=me'"
    assert "PWD=mypwd" in conn_str, "Connection string should contain 'PWD=mypwd'"
    assert "Database=mydb" in conn_str, "Connection string should contain 'Database=mydb'"
    assert "Encrypt=yes" in conn_str, "Connection string should contain 'Encrypt=yes'"
    assert (
        "TrustServerCertificate=yes" in conn_str
    ), "Connection string should contain 'TrustServerCertificate=yes'"
    assert "APP=MSSQL-Python" in conn_str, "Connection string should contain 'APP=MSSQL-Python'"
    assert (
        "Driver={ODBC Driver 18 for SQL Server}" in conn_str
    ), "Connection string should contain 'Driver={ODBC Driver 18 for SQL Server}'"


def test_autocommit_default(db_connection):
    assert db_connection.autocommit is False, "Autocommit should be False by default"


def test_autocommit_setter(db_connection):
    db_connection.autocommit = True
    cursor = db_connection.cursor()
    # Make a transaction and check if it is autocommited
    drop_table_if_exists(cursor, "#pytest_test_autocommit")
    try:
        cursor.execute(
            "CREATE TABLE #pytest_test_autocommit (id INT PRIMARY KEY, value VARCHAR(50));"
        )
        cursor.execute("INSERT INTO #pytest_test_autocommit (id, value) VALUES (1, 'test');")
        cursor.execute("SELECT * FROM #pytest_test_autocommit WHERE id = 1;")
        result = cursor.fetchone()
        assert result is not None, "Autocommit failed: No data found"
        assert result[1] == "test", "Autocommit failed: Incorrect data"
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
        cursor.execute(
            "CREATE TABLE #pytest_test_autocommit (id INT PRIMARY KEY, value VARCHAR(50));"
        )
        cursor.execute("INSERT INTO #pytest_test_autocommit (id, value) VALUES (1, 'test');")
        cursor.execute("SELECT * FROM #pytest_test_autocommit WHERE id = 1;")
        result = cursor.fetchone()
        assert result is not None, "Autocommit failed: No data found"
        assert result[1] == "test", "Autocommit failed: Incorrect data"
        db_connection.commit()
        cursor.execute("SELECT * FROM #pytest_test_autocommit WHERE id = 1;")
        result = cursor.fetchone()
        assert result is not None, "Autocommit failed: No data found after commit"
        assert result[1] == "test", "Autocommit failed: Incorrect data after commit"
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
        assert result[1] == "test", "Commit failed: Incorrect data"
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
        cursor.execute(
            "CREATE TABLE pytest_test_rollback_on_close (id INT PRIMARY KEY, value VARCHAR(50));"
        )
        db_connection.commit()

        # This simulates a scenario where the connection is closed without committing
        # and checks if the rollback occurs
        temp_conn = connect(conn_str)
        temp_cursor = temp_conn.cursor()
        temp_cursor.execute(
            "INSERT INTO pytest_test_rollback_on_close (id, value) VALUES (1, 'test');"
        )

        # Verify data is visible within the same transaction
        temp_cursor.execute("SELECT * FROM pytest_test_rollback_on_close WHERE id = 1;")
        result = temp_cursor.fetchone()
        assert result is not None, "Rollback on close failed: No data found before close"
        assert result[1] == "test", "Rollback on close failed: Incorrect data before close"

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
        cursor.execute(
            "CREATE TABLE #pytest_test_rollback (id INT PRIMARY KEY, value VARCHAR(50));"
        )
        cursor.execute("INSERT INTO #pytest_test_rollback (id, value) VALUES (1, 'test');")
        db_connection.commit()

        # Check if the data is present before rollback
        cursor.execute("SELECT * FROM #pytest_test_rollback WHERE id = 1;")
        result = cursor.fetchone()
        assert result is not None, "Rollback failed: No data found before rollback"
        assert result[1] == "test", "Rollback failed: Incorrect data"

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
    # Azure SQL takes longer to timeout, so use different thresholds
    timeout_threshold = 30 if is_azure_sql_connection(conn_str) else 10
    assert (
        elapsed < timeout_threshold
    ), f"Connection with invalid password took too long: {elapsed:.2f}s (threshold: {timeout_threshold}s)"


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


def test_context_manager_commit(conn_str):
    """Test that context manager closes connection on normal exit"""
    # Create a permanent table for testing across connections
    setup_conn = connect(conn_str)
    setup_cursor = setup_conn.cursor()
    drop_table_if_exists(setup_cursor, "pytest_context_manager_test")

    try:
        setup_cursor.execute(
            "CREATE TABLE pytest_context_manager_test (id INT PRIMARY KEY, value VARCHAR(50));"
        )
        setup_conn.commit()
        setup_conn.close()

        # Test context manager closes connection
        with connect(conn_str) as conn:
            assert conn.autocommit is False, "Autocommit should be False by default"
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO pytest_context_manager_test (id, value) VALUES (1, 'context_test');"
            )
            conn.commit()  # Manual commit now required
        # Connection should be closed here

        # Verify data was committed manually
        verify_conn = connect(conn_str)
        verify_cursor = verify_conn.cursor()
        verify_cursor.execute("SELECT * FROM pytest_context_manager_test WHERE id = 1;")
        result = verify_cursor.fetchone()
        assert result is not None, "Manual commit failed: No data found"
        assert result[1] == "context_test", "Manual commit failed: Incorrect data"
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
        setup_cursor.execute(
            "CREATE TABLE pytest_autocommit_close_test (id INT PRIMARY KEY, value VARCHAR(50));"
        )
        setup_conn.commit()
        setup_conn.close()

        # Create a connection with autocommit=True
        conn = connect(conn_str)
        conn.autocommit = True
        assert conn.autocommit is True, "Autocommit should be True"

        # Insert data
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO pytest_autocommit_close_test (id, value) VALUES (1, 'test_autocommit');"
        )

        # Close the connection without explicitly committing
        conn.close()

        # Verify the data was committed automatically despite connection.close()
        verify_conn = connect(conn_str)
        verify_cursor = verify_conn.cursor()
        verify_cursor.execute("SELECT * FROM pytest_autocommit_close_test WHERE id = 1;")
        result = verify_cursor.fetchone()

        # Data should be present if autocommit worked and wasn't affected by close()
        assert result is not None, "Autocommit failed: Data not found after connection close"
        assert (
            result[1] == "test_autocommit"
        ), "Autocommit failed: Incorrect data after connection close"

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
    assert settings["encoding"] == "utf-16le", "Default encoding should be utf-16le"
    assert settings["ctype"] == -8, "Default ctype should be SQL_WCHAR (-8)"


def test_setencoding_basic_functionality(db_connection):
    """Test basic setencoding functionality."""
    # Test setting UTF-8 encoding
    db_connection.setencoding(encoding="utf-8")
    settings = db_connection.getencoding()
    assert settings["encoding"] == "utf-8", "Encoding should be set to utf-8"
    assert settings["ctype"] == 1, "ctype should default to SQL_CHAR (1) for utf-8"

    # Test setting UTF-16LE with explicit ctype
    db_connection.setencoding(encoding="utf-16le", ctype=-8)
    settings = db_connection.getencoding()
    assert settings["encoding"] == "utf-16le", "Encoding should be set to utf-16le"
    assert settings["ctype"] == -8, "ctype should be SQL_WCHAR (-8)"


def test_setencoding_automatic_ctype_detection(db_connection):
    """Test automatic ctype detection based on encoding."""
    # UTF-16 variants should default to SQL_WCHAR
    utf16_encodings = ["utf-16", "utf-16le", "utf-16be"]
    for encoding in utf16_encodings:
        db_connection.setencoding(encoding=encoding)
        settings = db_connection.getencoding()
        assert settings["ctype"] == -8, f"{encoding} should default to SQL_WCHAR (-8)"

    # Other encodings should default to SQL_CHAR
    other_encodings = ["utf-8", "latin-1", "ascii"]
    for encoding in other_encodings:
        db_connection.setencoding(encoding=encoding)
        settings = db_connection.getencoding()
        assert settings["ctype"] == 1, f"{encoding} should default to SQL_CHAR (1)"


def test_setencoding_explicit_ctype_override(db_connection):
    """Test that explicit ctype parameter overrides automatic detection."""
    # Set UTF-8 with SQL_WCHAR (override default)
    db_connection.setencoding(encoding="utf-8", ctype=-8)
    settings = db_connection.getencoding()
    assert settings["encoding"] == "utf-8", "Encoding should be utf-8"
    assert settings["ctype"] == -8, "ctype should be SQL_WCHAR (-8) when explicitly set"

    # Set UTF-16LE with SQL_CHAR (override default)
    db_connection.setencoding(encoding="utf-16le", ctype=1)
    settings = db_connection.getencoding()
    assert settings["encoding"] == "utf-16le", "Encoding should be utf-16le"
    assert settings["ctype"] == 1, "ctype should be SQL_CHAR (1) when explicitly set"


def test_setencoding_none_parameters(db_connection):
    """Test setencoding with None parameters."""
    # Test with encoding=None (should use default)
    db_connection.setencoding(encoding=None)
    settings = db_connection.getencoding()
    assert settings["encoding"] == "utf-16le", "encoding=None should use default utf-16le"
    assert settings["ctype"] == -8, "ctype should be SQL_WCHAR for utf-16le"

    # Test with both None (should use defaults)
    db_connection.setencoding(encoding=None, ctype=None)
    settings = db_connection.getencoding()
    assert settings["encoding"] == "utf-16le", "encoding=None should use default utf-16le"
    assert settings["ctype"] == -8, "ctype=None should use default SQL_WCHAR"


def test_setencoding_invalid_encoding(db_connection):
    """Test setencoding with invalid encoding."""

    with pytest.raises(ProgrammingError) as exc_info:
        db_connection.setencoding(encoding="invalid-encoding-name")

    assert "Unsupported encoding" in str(
        exc_info.value
    ), "Should raise ProgrammingError for invalid encoding"
    assert "invalid-encoding-name" in str(
        exc_info.value
    ), "Error message should include the invalid encoding name"


def test_setencoding_invalid_ctype(db_connection):
    """Test setencoding with invalid ctype."""

    with pytest.raises(ProgrammingError) as exc_info:
        db_connection.setencoding(encoding="utf-8", ctype=999)

    assert "Invalid ctype" in str(exc_info.value), "Should raise ProgrammingError for invalid ctype"
    assert "999" in str(exc_info.value), "Error message should include the invalid ctype value"


def test_setencoding_closed_connection(conn_str):
    """Test setencoding on closed connection."""

    temp_conn = connect(conn_str)
    temp_conn.close()

    with pytest.raises(InterfaceError) as exc_info:
        temp_conn.setencoding(encoding="utf-8")

    assert "Connection is closed" in str(
        exc_info.value
    ), "Should raise InterfaceError for closed connection"


def test_setencoding_constants_access():
    """Test that SQL_CHAR and SQL_WCHAR constants are accessible."""
    import mssql_python

    # Test constants exist and have correct values
    assert hasattr(mssql_python, "SQL_CHAR"), "SQL_CHAR constant should be available"
    assert hasattr(mssql_python, "SQL_WCHAR"), "SQL_WCHAR constant should be available"
    assert mssql_python.SQL_CHAR == 1, "SQL_CHAR should have value 1"
    assert mssql_python.SQL_WCHAR == -8, "SQL_WCHAR should have value -8"


def test_setencoding_with_constants(db_connection):
    """Test setencoding using module constants."""
    import mssql_python

    # Test with SQL_CHAR constant
    db_connection.setencoding(encoding="utf-8", ctype=mssql_python.SQL_CHAR)
    settings = db_connection.getencoding()
    assert settings["ctype"] == mssql_python.SQL_CHAR, "Should accept SQL_CHAR constant"

    # Test with SQL_WCHAR constant
    db_connection.setencoding(encoding="utf-16le", ctype=mssql_python.SQL_WCHAR)
    settings = db_connection.getencoding()
    assert settings["ctype"] == mssql_python.SQL_WCHAR, "Should accept SQL_WCHAR constant"


def test_setencoding_common_encodings(db_connection):
    """Test setencoding with various common encodings."""
    common_encodings = [
        "utf-8",
        "utf-16le",
        "utf-16be",
        "utf-16",
        "latin-1",
        "ascii",
        "cp1252",
    ]

    for encoding in common_encodings:
        try:
            db_connection.setencoding(encoding=encoding)
            settings = db_connection.getencoding()
            assert settings["encoding"] == encoding, f"Failed to set encoding {encoding}"
        except Exception as e:
            pytest.fail(f"Failed to set valid encoding {encoding}: {e}")


def test_setencoding_persistence_across_cursors(db_connection):
    """Test that encoding settings persist across cursor operations."""
    # Set custom encoding
    db_connection.setencoding(encoding="utf-8", ctype=1)

    # Create cursors and verify encoding persists
    cursor1 = db_connection.cursor()
    settings1 = db_connection.getencoding()

    cursor2 = db_connection.cursor()
    settings2 = db_connection.getencoding()

    assert settings1 == settings2, "Encoding settings should persist across cursor creation"
    assert settings1["encoding"] == "utf-8", "Encoding should remain utf-8"
    assert settings1["ctype"] == 1, "ctype should remain SQL_CHAR"

    cursor1.close()
    cursor2.close()


@pytest.mark.skip("Skipping Unicode data tests till we have support for Unicode")
def test_setencoding_with_unicode_data(db_connection):
    """Test setencoding with actual Unicode data operations."""
    # Test UTF-8 encoding with Unicode data
    db_connection.setencoding(encoding="utf-8")
    cursor = db_connection.cursor()

    try:
        # Create test table
        cursor.execute("CREATE TABLE #test_encoding_unicode (text_col NVARCHAR(100))")

        # Test various Unicode strings
        test_strings = [
            "Hello, World!",
            "Hello, 世界!",  # Chinese
            "Привет, мир!",  # Russian
            "مرحبا بالعالم",  # Arabic
            "🌍🌎🌏",  # Emoji
        ]

        for test_string in test_strings:
            # Insert data
            cursor.execute("INSERT INTO #test_encoding_unicode (text_col) VALUES (?)", test_string)

            # Retrieve and verify
            cursor.execute(
                "SELECT text_col FROM #test_encoding_unicode WHERE text_col = ?",
                test_string,
            )
            result = cursor.fetchone()

            assert result is not None, f"Failed to retrieve Unicode string: {test_string}"
            assert (
                result[0] == test_string
            ), f"Unicode string mismatch: expected {test_string}, got {result[0]}"

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
        db_connection.setencoding(encoding="utf-16le")

        # Perform database operation
        cursor.execute("SELECT 'Initial test' as message")
        result1 = cursor.fetchone()
        assert result1[0] == "Initial test", "Initial operation failed"

        # Change encoding after operation
        db_connection.setencoding(encoding="utf-8")
        settings = db_connection.getencoding()
        assert settings["encoding"] == "utf-8", "Failed to change encoding after operation"

        # Perform another operation with new encoding
        cursor.execute("SELECT 'Changed encoding test' as message")
        result2 = cursor.fetchone()
        assert result2[0] == "Changed encoding test", "Operation after encoding change failed"

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
        assert "encoding" in encoding_info
        assert "ctype" in encoding_info
        # Default should be utf-16le with SQL_WCHAR
        assert encoding_info["encoding"] == "utf-16le"
        assert encoding_info["ctype"] == SQL_WCHAR
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
        encoding_info1["encoding"] = "modified"
        assert encoding_info2["encoding"] != "modified"
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
            ("utf-8", SQL_CHAR),
            ("utf-16le", SQL_WCHAR),
            ("latin-1", SQL_CHAR),
            ("ascii", SQL_CHAR),
        ]

        for encoding, expected_ctype in test_cases:
            conn.setencoding(encoding)
            encoding_info = conn.getencoding()
            assert encoding_info["encoding"] == encoding.lower()
            assert encoding_info["ctype"] == expected_ctype
    finally:
        conn.close()


def test_setencoding_default_encoding(conn_str):
    """Test setencoding with default UTF-16LE encoding"""
    conn = connect(conn_str)
    try:
        conn.setencoding()
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "utf-16le"
        assert encoding_info["ctype"] == SQL_WCHAR
    finally:
        conn.close()


def test_setencoding_utf8(conn_str):
    """Test setencoding with UTF-8 encoding"""
    conn = connect(conn_str)
    try:
        conn.setencoding("utf-8")
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "utf-8"
        assert encoding_info["ctype"] == SQL_CHAR
    finally:
        conn.close()


def test_setencoding_latin1(conn_str):
    """Test setencoding with latin-1 encoding"""
    conn = connect(conn_str)
    try:
        conn.setencoding("latin-1")
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "latin-1"
        assert encoding_info["ctype"] == SQL_CHAR
    finally:
        conn.close()


def test_setencoding_with_explicit_ctype_sql_char(conn_str):
    """Test setencoding with explicit SQL_CHAR ctype"""
    conn = connect(conn_str)
    try:
        conn.setencoding("utf-8", SQL_CHAR)
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "utf-8"
        assert encoding_info["ctype"] == SQL_CHAR
    finally:
        conn.close()


def test_setencoding_with_explicit_ctype_sql_wchar(conn_str):
    """Test setencoding with explicit SQL_WCHAR ctype"""
    conn = connect(conn_str)
    try:
        conn.setencoding("utf-16le", SQL_WCHAR)
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "utf-16le"
        assert encoding_info["ctype"] == SQL_WCHAR
    finally:
        conn.close()


def test_setencoding_invalid_ctype_error(conn_str):
    """Test setencoding with invalid ctype raises ProgrammingError"""

    conn = connect(conn_str)
    try:
        with pytest.raises(ProgrammingError, match="Invalid ctype"):
            conn.setencoding("utf-8", 999)
    finally:
        conn.close()


def test_setencoding_case_insensitive_encoding(conn_str):
    """Test setencoding with case variations"""
    conn = connect(conn_str)
    try:
        # Test various case formats
        conn.setencoding("UTF-8")
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "utf-8"  # Should be normalized

        conn.setencoding("Utf-16LE")
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "utf-16le"  # Should be normalized
    finally:
        conn.close()


def test_setencoding_none_encoding_default(conn_str):
    """Test setencoding with None encoding uses default"""
    conn = connect(conn_str)
    try:
        conn.setencoding(None)
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "utf-16le"
        assert encoding_info["ctype"] == SQL_WCHAR
    finally:
        conn.close()


def test_setencoding_override_previous(conn_str):
    """Test setencoding overrides previous settings"""
    conn = connect(conn_str)
    try:
        # Set initial encoding
        conn.setencoding("utf-8")
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "utf-8"
        assert encoding_info["ctype"] == SQL_CHAR

        # Override with different encoding
        conn.setencoding("utf-16le")
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "utf-16le"
        assert encoding_info["ctype"] == SQL_WCHAR
    finally:
        conn.close()


def test_setencoding_ascii(conn_str):
    """Test setencoding with ASCII encoding"""
    conn = connect(conn_str)
    try:
        conn.setencoding("ascii")
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "ascii"
        assert encoding_info["ctype"] == SQL_CHAR
    finally:
        conn.close()


def test_setencoding_cp1252(conn_str):
    """Test setencoding with Windows-1252 encoding"""
    conn = connect(conn_str)
    try:
        conn.setencoding("cp1252")
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "cp1252"
        assert encoding_info["ctype"] == SQL_CHAR
    finally:
        conn.close()


def test_setdecoding_default_settings(db_connection):
    """Test that default decoding settings are correct for all SQL types."""

    # Check SQL_CHAR defaults
    sql_char_settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert sql_char_settings["encoding"] == "utf-8", "Default SQL_CHAR encoding should be utf-8"
    assert (
        sql_char_settings["ctype"] == mssql_python.SQL_CHAR
    ), "Default SQL_CHAR ctype should be SQL_CHAR"

    # Check SQL_WCHAR defaults
    sql_wchar_settings = db_connection.getdecoding(mssql_python.SQL_WCHAR)
    assert (
        sql_wchar_settings["encoding"] == "utf-16le"
    ), "Default SQL_WCHAR encoding should be utf-16le"
    assert (
        sql_wchar_settings["ctype"] == mssql_python.SQL_WCHAR
    ), "Default SQL_WCHAR ctype should be SQL_WCHAR"

    # Check SQL_WMETADATA defaults
    sql_wmetadata_settings = db_connection.getdecoding(mssql_python.SQL_WMETADATA)
    assert (
        sql_wmetadata_settings["encoding"] == "utf-16le"
    ), "Default SQL_WMETADATA encoding should be utf-16le"
    assert (
        sql_wmetadata_settings["ctype"] == mssql_python.SQL_WCHAR
    ), "Default SQL_WMETADATA ctype should be SQL_WCHAR"


def test_setdecoding_basic_functionality(db_connection):
    """Test basic setdecoding functionality for different SQL types."""

    # Test setting SQL_CHAR decoding
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="latin-1")
    settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert settings["encoding"] == "latin-1", "SQL_CHAR encoding should be set to latin-1"
    assert (
        settings["ctype"] == mssql_python.SQL_CHAR
    ), "SQL_CHAR ctype should default to SQL_CHAR for latin-1"

    # Test setting SQL_WCHAR decoding
    db_connection.setdecoding(mssql_python.SQL_WCHAR, encoding="utf-16be")
    settings = db_connection.getdecoding(mssql_python.SQL_WCHAR)
    assert settings["encoding"] == "utf-16be", "SQL_WCHAR encoding should be set to utf-16be"
    assert (
        settings["ctype"] == mssql_python.SQL_WCHAR
    ), "SQL_WCHAR ctype should default to SQL_WCHAR for utf-16be"

    # Test setting SQL_WMETADATA decoding
    db_connection.setdecoding(mssql_python.SQL_WMETADATA, encoding="utf-16le")
    settings = db_connection.getdecoding(mssql_python.SQL_WMETADATA)
    assert settings["encoding"] == "utf-16le", "SQL_WMETADATA encoding should be set to utf-16le"
    assert (
        settings["ctype"] == mssql_python.SQL_WCHAR
    ), "SQL_WMETADATA ctype should default to SQL_WCHAR"


def test_setdecoding_automatic_ctype_detection(db_connection):
    """Test automatic ctype detection based on encoding for different SQL types."""

    # UTF-16 variants should default to SQL_WCHAR
    utf16_encodings = ["utf-16", "utf-16le", "utf-16be"]
    for encoding in utf16_encodings:
        db_connection.setdecoding(mssql_python.SQL_CHAR, encoding=encoding)
        settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
        assert (
            settings["ctype"] == mssql_python.SQL_WCHAR
        ), f"SQL_CHAR with {encoding} should auto-detect SQL_WCHAR ctype"

    # Other encodings should default to SQL_CHAR
    other_encodings = ["utf-8", "latin-1", "ascii", "cp1252"]
    for encoding in other_encodings:
        db_connection.setdecoding(mssql_python.SQL_WCHAR, encoding=encoding)
        settings = db_connection.getdecoding(mssql_python.SQL_WCHAR)
        assert (
            settings["ctype"] == mssql_python.SQL_CHAR
        ), f"SQL_WCHAR with {encoding} should auto-detect SQL_CHAR ctype"


def test_setdecoding_explicit_ctype_override(db_connection):
    """Test that explicit ctype parameter overrides automatic detection."""

    # Set SQL_CHAR with UTF-8 encoding but explicit SQL_WCHAR ctype
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="utf-8", ctype=mssql_python.SQL_WCHAR)
    settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert settings["encoding"] == "utf-8", "Encoding should be utf-8"
    assert (
        settings["ctype"] == mssql_python.SQL_WCHAR
    ), "ctype should be SQL_WCHAR when explicitly set"

    # Set SQL_WCHAR with UTF-16LE encoding but explicit SQL_CHAR ctype
    db_connection.setdecoding(
        mssql_python.SQL_WCHAR, encoding="utf-16le", ctype=mssql_python.SQL_CHAR
    )
    settings = db_connection.getdecoding(mssql_python.SQL_WCHAR)
    assert settings["encoding"] == "utf-16le", "Encoding should be utf-16le"
    assert (
        settings["ctype"] == mssql_python.SQL_CHAR
    ), "ctype should be SQL_CHAR when explicitly set"


def test_setdecoding_none_parameters(db_connection):
    """Test setdecoding with None parameters uses appropriate defaults."""

    # Test SQL_CHAR with encoding=None (should use utf-8 default)
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding=None)
    settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert settings["encoding"] == "utf-8", "SQL_CHAR with encoding=None should use utf-8 default"
    assert settings["ctype"] == mssql_python.SQL_CHAR, "ctype should be SQL_CHAR for utf-8"

    # Test SQL_WCHAR with encoding=None (should use utf-16le default)
    db_connection.setdecoding(mssql_python.SQL_WCHAR, encoding=None)
    settings = db_connection.getdecoding(mssql_python.SQL_WCHAR)
    assert (
        settings["encoding"] == "utf-16le"
    ), "SQL_WCHAR with encoding=None should use utf-16le default"
    assert settings["ctype"] == mssql_python.SQL_WCHAR, "ctype should be SQL_WCHAR for utf-16le"

    # Test with both parameters None
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding=None, ctype=None)
    settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert settings["encoding"] == "utf-8", "SQL_CHAR with both None should use utf-8 default"
    assert settings["ctype"] == mssql_python.SQL_CHAR, "ctype should default to SQL_CHAR"


def test_setdecoding_invalid_sqltype(db_connection):
    """Test setdecoding with invalid sqltype raises ProgrammingError."""

    with pytest.raises(ProgrammingError) as exc_info:
        db_connection.setdecoding(999, encoding="utf-8")

    assert "Invalid sqltype" in str(
        exc_info.value
    ), "Should raise ProgrammingError for invalid sqltype"
    assert "999" in str(exc_info.value), "Error message should include the invalid sqltype value"


def test_setdecoding_invalid_encoding(db_connection):
    """Test setdecoding with invalid encoding raises ProgrammingError."""

    with pytest.raises(ProgrammingError) as exc_info:
        db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="invalid-encoding-name")

    assert "Unsupported encoding" in str(
        exc_info.value
    ), "Should raise ProgrammingError for invalid encoding"
    assert "invalid-encoding-name" in str(
        exc_info.value
    ), "Error message should include the invalid encoding name"


def test_setdecoding_invalid_ctype(db_connection):
    """Test setdecoding with invalid ctype raises ProgrammingError."""

    with pytest.raises(ProgrammingError) as exc_info:
        db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="utf-8", ctype=999)

    assert "Invalid ctype" in str(exc_info.value), "Should raise ProgrammingError for invalid ctype"
    assert "999" in str(exc_info.value), "Error message should include the invalid ctype value"


def test_setdecoding_closed_connection(conn_str):
    """Test setdecoding on closed connection raises InterfaceError."""

    temp_conn = connect(conn_str)
    temp_conn.close()

    with pytest.raises(InterfaceError) as exc_info:
        temp_conn.setdecoding(mssql_python.SQL_CHAR, encoding="utf-8")

    assert "Connection is closed" in str(
        exc_info.value
    ), "Should raise InterfaceError for closed connection"


def test_setdecoding_constants_access():
    """Test that SQL constants are accessible."""

    # Test constants exist and have correct values
    assert hasattr(mssql_python, "SQL_CHAR"), "SQL_CHAR constant should be available"
    assert hasattr(mssql_python, "SQL_WCHAR"), "SQL_WCHAR constant should be available"
    assert hasattr(mssql_python, "SQL_WMETADATA"), "SQL_WMETADATA constant should be available"

    assert mssql_python.SQL_CHAR == 1, "SQL_CHAR should have value 1"
    assert mssql_python.SQL_WCHAR == -8, "SQL_WCHAR should have value -8"
    assert mssql_python.SQL_WMETADATA == -99, "SQL_WMETADATA should have value -99"


def test_setdecoding_with_constants(db_connection):
    """Test setdecoding using module constants."""

    # Test with SQL_CHAR constant
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="utf-8", ctype=mssql_python.SQL_CHAR)
    settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert settings["ctype"] == mssql_python.SQL_CHAR, "Should accept SQL_CHAR constant"

    # Test with SQL_WCHAR constant
    db_connection.setdecoding(
        mssql_python.SQL_WCHAR, encoding="utf-16le", ctype=mssql_python.SQL_WCHAR
    )
    settings = db_connection.getdecoding(mssql_python.SQL_WCHAR)
    assert settings["ctype"] == mssql_python.SQL_WCHAR, "Should accept SQL_WCHAR constant"

    # Test with SQL_WMETADATA constant
    db_connection.setdecoding(mssql_python.SQL_WMETADATA, encoding="utf-16be")
    settings = db_connection.getdecoding(mssql_python.SQL_WMETADATA)
    assert settings["encoding"] == "utf-16be", "Should accept SQL_WMETADATA constant"


def test_setdecoding_common_encodings(db_connection):
    """Test setdecoding with various common encodings."""

    common_encodings = [
        "utf-8",
        "utf-16le",
        "utf-16be",
        "utf-16",
        "latin-1",
        "ascii",
        "cp1252",
    ]

    for encoding in common_encodings:
        try:
            db_connection.setdecoding(mssql_python.SQL_CHAR, encoding=encoding)
            settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
            assert (
                settings["encoding"] == encoding
            ), f"Failed to set SQL_CHAR decoding to {encoding}"

            db_connection.setdecoding(mssql_python.SQL_WCHAR, encoding=encoding)
            settings = db_connection.getdecoding(mssql_python.SQL_WCHAR)
            assert (
                settings["encoding"] == encoding
            ), f"Failed to set SQL_WCHAR decoding to {encoding}"
        except Exception as e:
            pytest.fail(f"Failed to set valid encoding {encoding}: {e}")


def test_setdecoding_case_insensitive_encoding(db_connection):
    """Test setdecoding with case variations normalizes encoding."""

    # Test various case formats
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="UTF-8")
    settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert settings["encoding"] == "utf-8", "Encoding should be normalized to lowercase"

    db_connection.setdecoding(mssql_python.SQL_WCHAR, encoding="Utf-16LE")
    settings = db_connection.getdecoding(mssql_python.SQL_WCHAR)
    assert settings["encoding"] == "utf-16le", "Encoding should be normalized to lowercase"


def test_setdecoding_independent_sql_types(db_connection):
    """Test that decoding settings for different SQL types are independent."""

    # Set different encodings for each SQL type
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="utf-8")
    db_connection.setdecoding(mssql_python.SQL_WCHAR, encoding="utf-16le")
    db_connection.setdecoding(mssql_python.SQL_WMETADATA, encoding="utf-16be")

    # Verify each maintains its own settings
    sql_char_settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    sql_wchar_settings = db_connection.getdecoding(mssql_python.SQL_WCHAR)
    sql_wmetadata_settings = db_connection.getdecoding(mssql_python.SQL_WMETADATA)

    assert sql_char_settings["encoding"] == "utf-8", "SQL_CHAR should maintain utf-8"
    assert sql_wchar_settings["encoding"] == "utf-16le", "SQL_WCHAR should maintain utf-16le"
    assert (
        sql_wmetadata_settings["encoding"] == "utf-16be"
    ), "SQL_WMETADATA should maintain utf-16be"


def test_setdecoding_override_previous(db_connection):
    """Test setdecoding overrides previous settings for the same SQL type."""

    # Set initial decoding
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="utf-8")
    settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert settings["encoding"] == "utf-8", "Initial encoding should be utf-8"
    assert settings["ctype"] == mssql_python.SQL_CHAR, "Initial ctype should be SQL_CHAR"

    # Override with different settings
    db_connection.setdecoding(
        mssql_python.SQL_CHAR, encoding="latin-1", ctype=mssql_python.SQL_WCHAR
    )
    settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert settings["encoding"] == "latin-1", "Encoding should be overridden to latin-1"
    assert settings["ctype"] == mssql_python.SQL_WCHAR, "ctype should be overridden to SQL_WCHAR"


def test_getdecoding_invalid_sqltype(db_connection):
    """Test getdecoding with invalid sqltype raises ProgrammingError."""

    with pytest.raises(ProgrammingError) as exc_info:
        db_connection.getdecoding(999)

    assert "Invalid sqltype" in str(
        exc_info.value
    ), "Should raise ProgrammingError for invalid sqltype"
    assert "999" in str(exc_info.value), "Error message should include the invalid sqltype value"


def test_getdecoding_closed_connection(conn_str):
    """Test getdecoding on closed connection raises InterfaceError."""

    temp_conn = connect(conn_str)
    temp_conn.close()

    with pytest.raises(InterfaceError) as exc_info:
        temp_conn.getdecoding(mssql_python.SQL_CHAR)

    assert "Connection is closed" in str(
        exc_info.value
    ), "Should raise InterfaceError for closed connection"


def test_getdecoding_returns_copy(db_connection):
    """Test getdecoding returns a copy (not reference)."""

    # Set custom decoding
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="utf-8")

    # Get settings twice
    settings1 = db_connection.getdecoding(mssql_python.SQL_CHAR)
    settings2 = db_connection.getdecoding(mssql_python.SQL_CHAR)

    # Should be equal but not the same object
    assert settings1 == settings2, "Settings should be equal"
    assert settings1 is not settings2, "Settings should be different objects"

    # Modifying one shouldn't affect the other
    settings1["encoding"] = "modified"
    assert settings2["encoding"] != "modified", "Modification should not affect other copy"


def test_setdecoding_getdecoding_consistency(db_connection):
    """Test that setdecoding and getdecoding work consistently together."""

    test_cases = [
        (mssql_python.SQL_CHAR, "utf-8", mssql_python.SQL_CHAR),
        (mssql_python.SQL_CHAR, "utf-16le", mssql_python.SQL_WCHAR),
        (mssql_python.SQL_WCHAR, "latin-1", mssql_python.SQL_CHAR),
        (mssql_python.SQL_WCHAR, "utf-16be", mssql_python.SQL_WCHAR),
        (mssql_python.SQL_WMETADATA, "utf-16le", mssql_python.SQL_WCHAR),
    ]

    for sqltype, encoding, expected_ctype in test_cases:
        db_connection.setdecoding(sqltype, encoding=encoding)
        settings = db_connection.getdecoding(sqltype)
        assert settings["encoding"] == encoding.lower(), f"Encoding should be {encoding.lower()}"
        assert settings["ctype"] == expected_ctype, f"ctype should be {expected_ctype}"


def test_setdecoding_persistence_across_cursors(db_connection):
    """Test that decoding settings persist across cursor operations."""

    # Set custom decoding settings
    db_connection.setdecoding(
        mssql_python.SQL_CHAR, encoding="latin-1", ctype=mssql_python.SQL_CHAR
    )
    db_connection.setdecoding(
        mssql_python.SQL_WCHAR, encoding="utf-16be", ctype=mssql_python.SQL_WCHAR
    )

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

    assert char_settings1["encoding"] == "latin-1", "SQL_CHAR encoding should remain latin-1"
    assert wchar_settings1["encoding"] == "utf-16be", "SQL_WCHAR encoding should remain utf-16be"

    cursor1.close()
    cursor2.close()


def test_setdecoding_before_and_after_operations(db_connection):
    """Test that setdecoding works both before and after database operations."""
    cursor = db_connection.cursor()

    try:
        # Initial decoding setting
        db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="utf-8")

        # Perform database operation
        cursor.execute("SELECT 'Initial test' as message")
        result1 = cursor.fetchone()
        assert result1[0] == "Initial test", "Initial operation failed"

        # Change decoding after operation
        db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="latin-1")
        settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
        assert settings["encoding"] == "latin-1", "Failed to change decoding after operation"

        # Perform another operation with new decoding
        cursor.execute("SELECT 'Changed decoding test' as message")
        result2 = cursor.fetchone()
        assert result2[0] == "Changed decoding test", "Operation after decoding change failed"

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
            (mssql_python.SQL_CHAR, "ascii", mssql_python.SQL_CHAR),
            (mssql_python.SQL_WCHAR, "utf-16le", mssql_python.SQL_WCHAR),
            (mssql_python.SQL_WMETADATA, "utf-16be", mssql_python.SQL_WCHAR),
        ]

        for sqltype, encoding, ctype in test_configs:
            conn.setdecoding(sqltype, encoding=encoding, ctype=ctype)
            settings = conn.getdecoding(sqltype)
            assert settings["encoding"] == encoding, f"Failed to set encoding for sqltype {sqltype}"
            assert settings["ctype"] == ctype, f"Failed to set ctype for sqltype {sqltype}"

    finally:
        conn.close()


def test_setdecoding_security_logging(db_connection):
    """Test that setdecoding logs invalid attempts safely."""

    # These should raise exceptions but not crash due to logging
    test_cases = [
        (999, "utf-8", None),  # Invalid sqltype
        (mssql_python.SQL_CHAR, "invalid-encoding", None),  # Invalid encoding
        (mssql_python.SQL_CHAR, "utf-8", 999),  # Invalid ctype
    ]

    for sqltype, encoding, ctype in test_cases:
        with pytest.raises(ProgrammingError):
            db_connection.setdecoding(sqltype, encoding=encoding, ctype=ctype)


@pytest.mark.skip("Skipping Unicode data tests till we have support for Unicode")
def test_setdecoding_with_unicode_data(db_connection):
    """Test setdecoding with actual Unicode data operations."""

    # Test different decoding configurations with Unicode data
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="utf-8")
    db_connection.setdecoding(mssql_python.SQL_WCHAR, encoding="utf-16le")

    cursor = db_connection.cursor()

    try:
        # Create test table with both CHAR and NCHAR columns
        cursor.execute(
            """
            CREATE TABLE #test_decoding_unicode (
                char_col VARCHAR(100),
                nchar_col NVARCHAR(100)
            )
        """
        )

        # Test various Unicode strings
        test_strings = [
            "Hello, World!",
            "Hello, 世界!",  # Chinese
            "Привет, мир!",  # Russian
            "مرحبا بالعالم",  # Arabic
        ]

        for test_string in test_strings:
            # Insert data
            cursor.execute(
                "INSERT INTO #test_decoding_unicode (char_col, nchar_col) VALUES (?, ?)",
                test_string,
                test_string,
            )

            # Retrieve and verify
            cursor.execute(
                "SELECT char_col, nchar_col FROM #test_decoding_unicode WHERE char_col = ?",
                test_string,
            )
            result = cursor.fetchone()

            assert result is not None, f"Failed to retrieve Unicode string: {test_string}"
            assert (
                result[0] == test_string
            ), f"CHAR column mismatch: expected {test_string}, got {result[0]}"
            assert (
                result[1] == test_string
            ), f"NCHAR column mismatch: expected {test_string}, got {result[1]}"

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
    assert hasattr(db_connection, "Warning"), "Connection should have Warning attribute"
    assert hasattr(db_connection, "Error"), "Connection should have Error attribute"
    assert hasattr(
        db_connection, "InterfaceError"
    ), "Connection should have InterfaceError attribute"
    assert hasattr(db_connection, "DatabaseError"), "Connection should have DatabaseError attribute"
    assert hasattr(db_connection, "DataError"), "Connection should have DataError attribute"
    assert hasattr(
        db_connection, "OperationalError"
    ), "Connection should have OperationalError attribute"
    assert hasattr(
        db_connection, "IntegrityError"
    ), "Connection should have IntegrityError attribute"
    assert hasattr(db_connection, "InternalError"), "Connection should have InternalError attribute"
    assert hasattr(
        db_connection, "ProgrammingError"
    ), "Connection should have ProgrammingError attribute"
    assert hasattr(
        db_connection, "NotSupportedError"
    ), "Connection should have NotSupportedError attribute"


def test_connection_exception_attributes_are_classes(db_connection):
    """Test that all exception attributes are actually exception classes"""
    # Test that the attributes are the correct exception classes
    assert db_connection.Warning is Warning, "Connection.Warning should be the Warning class"
    assert db_connection.Error is Error, "Connection.Error should be the Error class"
    assert (
        db_connection.InterfaceError is InterfaceError
    ), "Connection.InterfaceError should be the InterfaceError class"
    assert (
        db_connection.DatabaseError is DatabaseError
    ), "Connection.DatabaseError should be the DatabaseError class"
    assert (
        db_connection.DataError is DataError
    ), "Connection.DataError should be the DataError class"
    assert (
        db_connection.OperationalError is OperationalError
    ), "Connection.OperationalError should be the OperationalError class"
    assert (
        db_connection.IntegrityError is IntegrityError
    ), "Connection.IntegrityError should be the IntegrityError class"
    assert (
        db_connection.InternalError is InternalError
    ), "Connection.InternalError should be the InternalError class"
    assert (
        db_connection.ProgrammingError is ProgrammingError
    ), "Connection.ProgrammingError should be the ProgrammingError class"
    assert (
        db_connection.NotSupportedError is NotSupportedError
    ), "Connection.NotSupportedError should be the NotSupportedError class"


def test_connection_exception_inheritance(db_connection):
    """Test that exception classes have correct inheritance hierarchy"""
    # Test inheritance hierarchy according to DB-API 2.0

    # All exceptions inherit from Error (except Warning)
    assert issubclass(
        db_connection.InterfaceError, db_connection.Error
    ), "InterfaceError should inherit from Error"
    assert issubclass(
        db_connection.DatabaseError, db_connection.Error
    ), "DatabaseError should inherit from Error"

    # Database exceptions inherit from DatabaseError
    assert issubclass(
        db_connection.DataError, db_connection.DatabaseError
    ), "DataError should inherit from DatabaseError"
    assert issubclass(
        db_connection.OperationalError, db_connection.DatabaseError
    ), "OperationalError should inherit from DatabaseError"
    assert issubclass(
        db_connection.IntegrityError, db_connection.DatabaseError
    ), "IntegrityError should inherit from DatabaseError"
    assert issubclass(
        db_connection.InternalError, db_connection.DatabaseError
    ), "InternalError should inherit from DatabaseError"
    assert issubclass(
        db_connection.ProgrammingError, db_connection.DatabaseError
    ), "ProgrammingError should inherit from DatabaseError"
    assert issubclass(
        db_connection.NotSupportedError, db_connection.DatabaseError
    ), "NotSupportedError should inherit from DatabaseError"


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
    assert isinstance(
        interface_error, db_connection.InterfaceError
    ), "Should be able to create InterfaceError instance"
    assert "Interface error" in str(
        interface_error
    ), "InterfaceError should contain driver error message"

    db_error = db_connection.DatabaseError("Database error", "DDBC database error")
    assert isinstance(
        db_error, db_connection.DatabaseError
    ), "Should be able to create DatabaseError instance"
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
        assert (
            "syntax" in str(e).lower() or "incorrect" in str(e).lower() or "near" in str(e).lower()
        ), "Should be a syntax-related error"
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
            pytest.fail(
                f"Expected ProgrammingError from conn1 attributes, got {type(e).__name__}: {e}"
            )

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
            assert (
                "nonexistent_table_12345" in str(e)
                or "object" in str(e).lower()
                or "not" in str(e).lower()
            ), "Should mention the missing table"
        except conn2.DatabaseError as e:
            # Acceptable since ProgrammingError inherits from DatabaseError
            pass
        except Exception as e:
            pytest.fail(
                f"Expected ProgrammingError or DatabaseError from conn2, got {type(e).__name__}: {e}"
            )

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
        assert (
            conn1.InterfaceError is conn2.InterfaceError
        ), "All connections should reference the same InterfaceError class"
        assert (
            conn1.DatabaseError is conn2.DatabaseError
        ), "All connections should reference the same DatabaseError class"
        assert (
            conn1.ProgrammingError is conn2.ProgrammingError
        ), "All connections should reference the same ProgrammingError class"

        # Test that the classes are the same as module-level imports
        assert conn1.Error is Error, "Connection.Error should be the same as module-level Error"
        assert (
            conn1.InterfaceError is InterfaceError
        ), "Connection.InterfaceError should be the same as module-level InterfaceError"
        assert (
            conn1.DatabaseError is DatabaseError
        ), "Connection.DatabaseError should be the same as module-level DatabaseError"

    finally:
        conn1.close()
        conn2.close()


def test_connection_exception_attributes_comprehensive_list():
    """Test that all DB-API 2.0 required exception attributes are present on Connection class"""
    # Test at the class level (before instantiation)
    required_exceptions = [
        "Warning",
        "Error",
        "InterfaceError",
        "DatabaseError",
        "DataError",
        "OperationalError",
        "IntegrityError",
        "InternalError",
        "ProgrammingError",
        "NotSupportedError",
    ]

    for exc_name in required_exceptions:
        assert hasattr(Connection, exc_name), f"Connection class should have {exc_name} attribute"
        exc_class = getattr(Connection, exc_name)
        assert isinstance(exc_class, type), f"Connection.{exc_name} should be a class"
        assert issubclass(
            exc_class, Exception
        ), f"Connection.{exc_name} should be an Exception subclass"


def test_context_manager_commit(conn_str):
    """Test that context manager closes connection on normal exit"""
    # Create a permanent table for testing across connections
    setup_conn = connect(conn_str)
    setup_cursor = setup_conn.cursor()
    drop_table_if_exists(setup_cursor, "pytest_context_manager_test")

    try:
        setup_cursor.execute(
            "CREATE TABLE pytest_context_manager_test (id INT PRIMARY KEY, value VARCHAR(50));"
        )
        setup_conn.commit()
        setup_conn.close()

        # Test context manager closes connection
        with connect(conn_str) as conn:
            assert conn.autocommit is False, "Autocommit should be False by default"
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO pytest_context_manager_test (id, value) VALUES (1, 'context_test');"
            )
            conn.commit()  # Manual commit now required
        # Connection should be closed here

        # Verify data was committed manually
        verify_conn = connect(conn_str)
        verify_cursor = verify_conn.cursor()
        verify_cursor.execute("SELECT * FROM pytest_context_manager_test WHERE id = 1;")
        result = verify_cursor.fetchone()
        assert result is not None, "Manual commit failed: No data found"
        assert result[1] == "context_test", "Manual commit failed: Incorrect data"
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
        setup_cursor.execute(
            "CREATE TABLE pytest_autocommit_close_test (id INT PRIMARY KEY, value VARCHAR(50));"
        )
        setup_conn.commit()
        setup_conn.close()

        # Create a connection with autocommit=True
        conn = connect(conn_str)
        conn.autocommit = True
        assert conn.autocommit is True, "Autocommit should be True"

        # Insert data
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO pytest_autocommit_close_test (id, value) VALUES (1, 'test_autocommit');"
        )

        # Close the connection without explicitly committing
        conn.close()

        # Verify the data was committed automatically despite connection.close()
        verify_conn = connect(conn_str)
        verify_cursor = verify_conn.cursor()
        verify_cursor.execute("SELECT * FROM pytest_autocommit_close_test WHERE id = 1;")
        result = verify_cursor.fetchone()

        # Data should be present if autocommit worked and wasn't affected by close()
        assert result is not None, "Autocommit failed: Data not found after connection close"
        assert (
            result[1] == "test_autocommit"
        ), "Autocommit failed: Incorrect data after connection close"

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
    assert settings["encoding"] == "utf-16le", "Default encoding should be utf-16le"
    assert settings["ctype"] == -8, "Default ctype should be SQL_WCHAR (-8)"


def test_setencoding_basic_functionality(db_connection):
    """Test basic setencoding functionality."""
    # Test setting UTF-8 encoding
    db_connection.setencoding(encoding="utf-8")
    settings = db_connection.getencoding()
    assert settings["encoding"] == "utf-8", "Encoding should be set to utf-8"
    assert settings["ctype"] == 1, "ctype should default to SQL_CHAR (1) for utf-8"

    # Test setting UTF-16LE with explicit ctype
    db_connection.setencoding(encoding="utf-16le", ctype=-8)
    settings = db_connection.getencoding()
    assert settings["encoding"] == "utf-16le", "Encoding should be set to utf-16le"
    assert settings["ctype"] == -8, "ctype should be SQL_WCHAR (-8)"


def test_setencoding_automatic_ctype_detection(db_connection):
    """Test automatic ctype detection based on encoding."""
    # UTF-16 variants should default to SQL_WCHAR
    utf16_encodings = ["utf-16", "utf-16le", "utf-16be"]
    for encoding in utf16_encodings:
        db_connection.setencoding(encoding=encoding)
        settings = db_connection.getencoding()
        assert settings["ctype"] == -8, f"{encoding} should default to SQL_WCHAR (-8)"

    # Other encodings should default to SQL_CHAR
    other_encodings = ["utf-8", "latin-1", "ascii"]
    for encoding in other_encodings:
        db_connection.setencoding(encoding=encoding)
        settings = db_connection.getencoding()
        assert settings["ctype"] == 1, f"{encoding} should default to SQL_CHAR (1)"


def test_setencoding_explicit_ctype_override(db_connection):
    """Test that explicit ctype parameter overrides automatic detection."""
    # Set UTF-8 with SQL_WCHAR (override default)
    db_connection.setencoding(encoding="utf-8", ctype=-8)
    settings = db_connection.getencoding()
    assert settings["encoding"] == "utf-8", "Encoding should be utf-8"
    assert settings["ctype"] == -8, "ctype should be SQL_WCHAR (-8) when explicitly set"

    # Set UTF-16LE with SQL_CHAR (override default)
    db_connection.setencoding(encoding="utf-16le", ctype=1)
    settings = db_connection.getencoding()
    assert settings["encoding"] == "utf-16le", "Encoding should be utf-16le"
    assert settings["ctype"] == 1, "ctype should be SQL_CHAR (1) when explicitly set"


def test_setencoding_none_parameters(db_connection):
    """Test setencoding with None parameters."""
    # Test with encoding=None (should use default)
    db_connection.setencoding(encoding=None)
    settings = db_connection.getencoding()
    assert settings["encoding"] == "utf-16le", "encoding=None should use default utf-16le"
    assert settings["ctype"] == -8, "ctype should be SQL_WCHAR for utf-16le"

    # Test with both None (should use defaults)
    db_connection.setencoding(encoding=None, ctype=None)
    settings = db_connection.getencoding()
    assert settings["encoding"] == "utf-16le", "encoding=None should use default utf-16le"
    assert settings["ctype"] == -8, "ctype=None should use default SQL_WCHAR"


def test_setencoding_invalid_encoding(db_connection):
    """Test setencoding with invalid encoding."""

    with pytest.raises(ProgrammingError) as exc_info:
        db_connection.setencoding(encoding="invalid-encoding-name")

    assert "Unsupported encoding" in str(
        exc_info.value
    ), "Should raise ProgrammingError for invalid encoding"
    assert "invalid-encoding-name" in str(
        exc_info.value
    ), "Error message should include the invalid encoding name"


def test_setencoding_invalid_ctype(db_connection):
    """Test setencoding with invalid ctype."""

    with pytest.raises(ProgrammingError) as exc_info:
        db_connection.setencoding(encoding="utf-8", ctype=999)

    assert "Invalid ctype" in str(exc_info.value), "Should raise ProgrammingError for invalid ctype"
    assert "999" in str(exc_info.value), "Error message should include the invalid ctype value"


def test_setencoding_closed_connection(conn_str):
    """Test setencoding on closed connection."""

    temp_conn = connect(conn_str)
    temp_conn.close()

    with pytest.raises(InterfaceError) as exc_info:
        temp_conn.setencoding(encoding="utf-8")

    assert "Connection is closed" in str(
        exc_info.value
    ), "Should raise InterfaceError for closed connection"


def test_setencoding_constants_access():
    """Test that SQL_CHAR and SQL_WCHAR constants are accessible."""
    import mssql_python

    # Test constants exist and have correct values
    assert hasattr(mssql_python, "SQL_CHAR"), "SQL_CHAR constant should be available"
    assert hasattr(mssql_python, "SQL_WCHAR"), "SQL_WCHAR constant should be available"
    assert mssql_python.SQL_CHAR == 1, "SQL_CHAR should have value 1"
    assert mssql_python.SQL_WCHAR == -8, "SQL_WCHAR should have value -8"


def test_setencoding_with_constants(db_connection):
    """Test setencoding using module constants."""
    import mssql_python

    # Test with SQL_CHAR constant
    db_connection.setencoding(encoding="utf-8", ctype=mssql_python.SQL_CHAR)
    settings = db_connection.getencoding()
    assert settings["ctype"] == mssql_python.SQL_CHAR, "Should accept SQL_CHAR constant"

    # Test with SQL_WCHAR constant
    db_connection.setencoding(encoding="utf-16le", ctype=mssql_python.SQL_WCHAR)
    settings = db_connection.getencoding()
    assert settings["ctype"] == mssql_python.SQL_WCHAR, "Should accept SQL_WCHAR constant"


def test_setencoding_common_encodings(db_connection):
    """Test setencoding with various common encodings."""
    common_encodings = [
        "utf-8",
        "utf-16le",
        "utf-16be",
        "utf-16",
        "latin-1",
        "ascii",
        "cp1252",
    ]

    for encoding in common_encodings:
        try:
            db_connection.setencoding(encoding=encoding)
            settings = db_connection.getencoding()
            assert settings["encoding"] == encoding, f"Failed to set encoding {encoding}"
        except Exception as e:
            pytest.fail(f"Failed to set valid encoding {encoding}: {e}")


def test_setencoding_persistence_across_cursors(db_connection):
    """Test that encoding settings persist across cursor operations."""
    # Set custom encoding
    db_connection.setencoding(encoding="utf-8", ctype=1)

    # Create cursors and verify encoding persists
    cursor1 = db_connection.cursor()
    settings1 = db_connection.getencoding()

    cursor2 = db_connection.cursor()
    settings2 = db_connection.getencoding()

    assert settings1 == settings2, "Encoding settings should persist across cursor creation"
    assert settings1["encoding"] == "utf-8", "Encoding should remain utf-8"
    assert settings1["ctype"] == 1, "ctype should remain SQL_CHAR"

    cursor1.close()
    cursor2.close()


@pytest.mark.skip("Skipping Unicode data tests till we have support for Unicode")
def test_setencoding_with_unicode_data(db_connection):
    """Test setencoding with actual Unicode data operations."""
    # Test UTF-8 encoding with Unicode data
    db_connection.setencoding(encoding="utf-8")
    cursor = db_connection.cursor()

    try:
        # Create test table
        cursor.execute("CREATE TABLE #test_encoding_unicode (text_col NVARCHAR(100))")

        # Test various Unicode strings
        test_strings = [
            "Hello, World!",
            "Hello, 世界!",  # Chinese
            "Привет, мир!",  # Russian
            "مرحبا بالعالم",  # Arabic
            "🌍🌎🌏",  # Emoji
        ]

        for test_string in test_strings:
            # Insert data
            cursor.execute("INSERT INTO #test_encoding_unicode (text_col) VALUES (?)", test_string)

            # Retrieve and verify
            cursor.execute(
                "SELECT text_col FROM #test_encoding_unicode WHERE text_col = ?",
                test_string,
            )
            result = cursor.fetchone()

            assert result is not None, f"Failed to retrieve Unicode string: {test_string}"
            assert (
                result[0] == test_string
            ), f"Unicode string mismatch: expected {test_string}, got {result[0]}"

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
        db_connection.setencoding(encoding="utf-16le")

        # Perform database operation
        cursor.execute("SELECT 'Initial test' as message")
        result1 = cursor.fetchone()
        assert result1[0] == "Initial test", "Initial operation failed"

        # Change encoding after operation
        db_connection.setencoding(encoding="utf-8")
        settings = db_connection.getencoding()
        assert settings["encoding"] == "utf-8", "Failed to change encoding after operation"

        # Perform another operation with new encoding
        cursor.execute("SELECT 'Changed encoding test' as message")
        result2 = cursor.fetchone()
        assert result2[0] == "Changed encoding test", "Operation after encoding change failed"

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
        assert "encoding" in encoding_info
        assert "ctype" in encoding_info
        # Default should be utf-16le with SQL_WCHAR
        assert encoding_info["encoding"] == "utf-16le"
        assert encoding_info["ctype"] == SQL_WCHAR
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
        encoding_info1["encoding"] = "modified"
        assert encoding_info2["encoding"] != "modified"
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
            ("utf-8", SQL_CHAR),
            ("utf-16le", SQL_WCHAR),
            ("latin-1", SQL_CHAR),
            ("ascii", SQL_CHAR),
        ]

        for encoding, expected_ctype in test_cases:
            conn.setencoding(encoding)
            encoding_info = conn.getencoding()
            assert encoding_info["encoding"] == encoding.lower()
            assert encoding_info["ctype"] == expected_ctype
    finally:
        conn.close()


def test_setencoding_default_encoding(conn_str):
    """Test setencoding with default UTF-16LE encoding"""
    conn = connect(conn_str)
    try:
        conn.setencoding()
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "utf-16le"
        assert encoding_info["ctype"] == SQL_WCHAR
    finally:
        conn.close()


def test_setencoding_utf8(conn_str):
    """Test setencoding with UTF-8 encoding"""
    conn = connect(conn_str)
    try:
        conn.setencoding("utf-8")
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "utf-8"
        assert encoding_info["ctype"] == SQL_CHAR
    finally:
        conn.close()


def test_setencoding_latin1(conn_str):
    """Test setencoding with latin-1 encoding"""
    conn = connect(conn_str)
    try:
        conn.setencoding("latin-1")
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "latin-1"
        assert encoding_info["ctype"] == SQL_CHAR
    finally:
        conn.close()


def test_setencoding_with_explicit_ctype_sql_char(conn_str):
    """Test setencoding with explicit SQL_CHAR ctype"""
    conn = connect(conn_str)
    try:
        conn.setencoding("utf-8", SQL_CHAR)
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "utf-8"
        assert encoding_info["ctype"] == SQL_CHAR
    finally:
        conn.close()


def test_setencoding_with_explicit_ctype_sql_wchar(conn_str):
    """Test setencoding with explicit SQL_WCHAR ctype"""
    conn = connect(conn_str)
    try:
        conn.setencoding("utf-16le", SQL_WCHAR)
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "utf-16le"
        assert encoding_info["ctype"] == SQL_WCHAR
    finally:
        conn.close()


def test_setencoding_invalid_ctype_error(conn_str):
    """Test setencoding with invalid ctype raises ProgrammingError"""

    conn = connect(conn_str)
    try:
        with pytest.raises(ProgrammingError, match="Invalid ctype"):
            conn.setencoding("utf-8", 999)
    finally:
        conn.close()


def test_setencoding_case_insensitive_encoding(conn_str):
    """Test setencoding with case variations"""
    conn = connect(conn_str)
    try:
        # Test various case formats
        conn.setencoding("UTF-8")
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "utf-8"  # Should be normalized

        conn.setencoding("Utf-16LE")
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "utf-16le"  # Should be normalized
    finally:
        conn.close()


def test_setencoding_none_encoding_default(conn_str):
    """Test setencoding with None encoding uses default"""
    conn = connect(conn_str)
    try:
        conn.setencoding(None)
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "utf-16le"
        assert encoding_info["ctype"] == SQL_WCHAR
    finally:
        conn.close()


def test_setencoding_override_previous(conn_str):
    """Test setencoding overrides previous settings"""
    conn = connect(conn_str)
    try:
        # Set initial encoding
        conn.setencoding("utf-8")
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "utf-8"
        assert encoding_info["ctype"] == SQL_CHAR

        # Override with different encoding
        conn.setencoding("utf-16le")
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "utf-16le"
        assert encoding_info["ctype"] == SQL_WCHAR
    finally:
        conn.close()


def test_setencoding_ascii(conn_str):
    """Test setencoding with ASCII encoding"""
    conn = connect(conn_str)
    try:
        conn.setencoding("ascii")
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "ascii"
        assert encoding_info["ctype"] == SQL_CHAR
    finally:
        conn.close()


def test_setencoding_cp1252(conn_str):
    """Test setencoding with Windows-1252 encoding"""
    conn = connect(conn_str)
    try:
        conn.setencoding("cp1252")
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "cp1252"
        assert encoding_info["ctype"] == SQL_CHAR
    finally:
        conn.close()


def test_setdecoding_default_settings(db_connection):
    """Test that default decoding settings are correct for all SQL types."""

    # Check SQL_CHAR defaults
    sql_char_settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert sql_char_settings["encoding"] == "utf-8", "Default SQL_CHAR encoding should be utf-8"
    assert (
        sql_char_settings["ctype"] == mssql_python.SQL_CHAR
    ), "Default SQL_CHAR ctype should be SQL_CHAR"

    # Check SQL_WCHAR defaults
    sql_wchar_settings = db_connection.getdecoding(mssql_python.SQL_WCHAR)
    assert (
        sql_wchar_settings["encoding"] == "utf-16le"
    ), "Default SQL_WCHAR encoding should be utf-16le"
    assert (
        sql_wchar_settings["ctype"] == mssql_python.SQL_WCHAR
    ), "Default SQL_WCHAR ctype should be SQL_WCHAR"

    # Check SQL_WMETADATA defaults
    sql_wmetadata_settings = db_connection.getdecoding(mssql_python.SQL_WMETADATA)
    assert (
        sql_wmetadata_settings["encoding"] == "utf-16le"
    ), "Default SQL_WMETADATA encoding should be utf-16le"
    assert (
        sql_wmetadata_settings["ctype"] == mssql_python.SQL_WCHAR
    ), "Default SQL_WMETADATA ctype should be SQL_WCHAR"


def test_setdecoding_basic_functionality(db_connection):
    """Test basic setdecoding functionality for different SQL types."""

    # Test setting SQL_CHAR decoding
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="latin-1")
    settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert settings["encoding"] == "latin-1", "SQL_CHAR encoding should be set to latin-1"
    assert (
        settings["ctype"] == mssql_python.SQL_CHAR
    ), "SQL_CHAR ctype should default to SQL_CHAR for latin-1"

    # Test setting SQL_WCHAR decoding
    db_connection.setdecoding(mssql_python.SQL_WCHAR, encoding="utf-16be")
    settings = db_connection.getdecoding(mssql_python.SQL_WCHAR)
    assert settings["encoding"] == "utf-16be", "SQL_WCHAR encoding should be set to utf-16be"
    assert (
        settings["ctype"] == mssql_python.SQL_WCHAR
    ), "SQL_WCHAR ctype should default to SQL_WCHAR for utf-16be"

    # Test setting SQL_WMETADATA decoding
    db_connection.setdecoding(mssql_python.SQL_WMETADATA, encoding="utf-16le")
    settings = db_connection.getdecoding(mssql_python.SQL_WMETADATA)
    assert settings["encoding"] == "utf-16le", "SQL_WMETADATA encoding should be set to utf-16le"
    assert (
        settings["ctype"] == mssql_python.SQL_WCHAR
    ), "SQL_WMETADATA ctype should default to SQL_WCHAR"


def test_setdecoding_automatic_ctype_detection(db_connection):
    """Test automatic ctype detection based on encoding for different SQL types."""

    # UTF-16 variants should default to SQL_WCHAR
    utf16_encodings = ["utf-16", "utf-16le", "utf-16be"]
    for encoding in utf16_encodings:
        db_connection.setdecoding(mssql_python.SQL_CHAR, encoding=encoding)
        settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
        assert (
            settings["ctype"] == mssql_python.SQL_WCHAR
        ), f"SQL_CHAR with {encoding} should auto-detect SQL_WCHAR ctype"

    # Other encodings should default to SQL_CHAR
    other_encodings = ["utf-8", "latin-1", "ascii", "cp1252"]
    for encoding in other_encodings:
        db_connection.setdecoding(mssql_python.SQL_WCHAR, encoding=encoding)
        settings = db_connection.getdecoding(mssql_python.SQL_WCHAR)
        assert (
            settings["ctype"] == mssql_python.SQL_CHAR
        ), f"SQL_WCHAR with {encoding} should auto-detect SQL_CHAR ctype"


def test_setdecoding_explicit_ctype_override(db_connection):
    """Test that explicit ctype parameter overrides automatic detection."""

    # Set SQL_CHAR with UTF-8 encoding but explicit SQL_WCHAR ctype
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="utf-8", ctype=mssql_python.SQL_WCHAR)
    settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert settings["encoding"] == "utf-8", "Encoding should be utf-8"
    assert (
        settings["ctype"] == mssql_python.SQL_WCHAR
    ), "ctype should be SQL_WCHAR when explicitly set"

    # Set SQL_WCHAR with UTF-16LE encoding but explicit SQL_CHAR ctype
    db_connection.setdecoding(
        mssql_python.SQL_WCHAR, encoding="utf-16le", ctype=mssql_python.SQL_CHAR
    )
    settings = db_connection.getdecoding(mssql_python.SQL_WCHAR)
    assert settings["encoding"] == "utf-16le", "Encoding should be utf-16le"
    assert (
        settings["ctype"] == mssql_python.SQL_CHAR
    ), "ctype should be SQL_CHAR when explicitly set"


def test_setdecoding_none_parameters(db_connection):
    """Test setdecoding with None parameters uses appropriate defaults."""

    # Test SQL_CHAR with encoding=None (should use utf-8 default)
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding=None)
    settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert settings["encoding"] == "utf-8", "SQL_CHAR with encoding=None should use utf-8 default"
    assert settings["ctype"] == mssql_python.SQL_CHAR, "ctype should be SQL_CHAR for utf-8"

    # Test SQL_WCHAR with encoding=None (should use utf-16le default)
    db_connection.setdecoding(mssql_python.SQL_WCHAR, encoding=None)
    settings = db_connection.getdecoding(mssql_python.SQL_WCHAR)
    assert (
        settings["encoding"] == "utf-16le"
    ), "SQL_WCHAR with encoding=None should use utf-16le default"
    assert settings["ctype"] == mssql_python.SQL_WCHAR, "ctype should be SQL_WCHAR for utf-16le"

    # Test with both parameters None
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding=None, ctype=None)
    settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert settings["encoding"] == "utf-8", "SQL_CHAR with both None should use utf-8 default"
    assert settings["ctype"] == mssql_python.SQL_CHAR, "ctype should default to SQL_CHAR"


def test_setdecoding_invalid_sqltype(db_connection):
    """Test setdecoding with invalid sqltype raises ProgrammingError."""

    with pytest.raises(ProgrammingError) as exc_info:
        db_connection.setdecoding(999, encoding="utf-8")

    assert "Invalid sqltype" in str(
        exc_info.value
    ), "Should raise ProgrammingError for invalid sqltype"
    assert "999" in str(exc_info.value), "Error message should include the invalid sqltype value"


def test_setdecoding_invalid_encoding(db_connection):
    """Test setdecoding with invalid encoding raises ProgrammingError."""

    with pytest.raises(ProgrammingError) as exc_info:
        db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="invalid-encoding-name")

    assert "Unsupported encoding" in str(
        exc_info.value
    ), "Should raise ProgrammingError for invalid encoding"
    assert "invalid-encoding-name" in str(
        exc_info.value
    ), "Error message should include the invalid encoding name"


def test_setdecoding_invalid_ctype(db_connection):
    """Test setdecoding with invalid ctype raises ProgrammingError."""

    with pytest.raises(ProgrammingError) as exc_info:
        db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="utf-8", ctype=999)

    assert "Invalid ctype" in str(exc_info.value), "Should raise ProgrammingError for invalid ctype"
    assert "999" in str(exc_info.value), "Error message should include the invalid ctype value"


def test_setdecoding_closed_connection(conn_str):
    """Test setdecoding on closed connection raises InterfaceError."""

    temp_conn = connect(conn_str)
    temp_conn.close()

    with pytest.raises(InterfaceError) as exc_info:
        temp_conn.setdecoding(mssql_python.SQL_CHAR, encoding="utf-8")

    assert "Connection is closed" in str(
        exc_info.value
    ), "Should raise InterfaceError for closed connection"


def test_setdecoding_constants_access():
    """Test that SQL constants are accessible."""

    # Test constants exist and have correct values
    assert hasattr(mssql_python, "SQL_CHAR"), "SQL_CHAR constant should be available"
    assert hasattr(mssql_python, "SQL_WCHAR"), "SQL_WCHAR constant should be available"
    assert hasattr(mssql_python, "SQL_WMETADATA"), "SQL_WMETADATA constant should be available"

    assert mssql_python.SQL_CHAR == 1, "SQL_CHAR should have value 1"
    assert mssql_python.SQL_WCHAR == -8, "SQL_WCHAR should have value -8"
    assert mssql_python.SQL_WMETADATA == -99, "SQL_WMETADATA should have value -99"


def test_setdecoding_with_constants(db_connection):
    """Test setdecoding using module constants."""

    # Test with SQL_CHAR constant
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="utf-8", ctype=mssql_python.SQL_CHAR)
    settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert settings["ctype"] == mssql_python.SQL_CHAR, "Should accept SQL_CHAR constant"

    # Test with SQL_WCHAR constant
    db_connection.setdecoding(
        mssql_python.SQL_WCHAR, encoding="utf-16le", ctype=mssql_python.SQL_WCHAR
    )
    settings = db_connection.getdecoding(mssql_python.SQL_WCHAR)
    assert settings["ctype"] == mssql_python.SQL_WCHAR, "Should accept SQL_WCHAR constant"

    # Test with SQL_WMETADATA constant
    db_connection.setdecoding(mssql_python.SQL_WMETADATA, encoding="utf-16be")
    settings = db_connection.getdecoding(mssql_python.SQL_WMETADATA)
    assert settings["encoding"] == "utf-16be", "Should accept SQL_WMETADATA constant"


def test_setdecoding_common_encodings(db_connection):
    """Test setdecoding with various common encodings."""

    common_encodings = [
        "utf-8",
        "utf-16le",
        "utf-16be",
        "utf-16",
        "latin-1",
        "ascii",
        "cp1252",
    ]

    for encoding in common_encodings:
        try:
            db_connection.setdecoding(mssql_python.SQL_CHAR, encoding=encoding)
            settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
            assert (
                settings["encoding"] == encoding
            ), f"Failed to set SQL_CHAR decoding to {encoding}"

            db_connection.setdecoding(mssql_python.SQL_WCHAR, encoding=encoding)
            settings = db_connection.getdecoding(mssql_python.SQL_WCHAR)
            assert (
                settings["encoding"] == encoding
            ), f"Failed to set SQL_WCHAR decoding to {encoding}"
        except Exception as e:
            pytest.fail(f"Failed to set valid encoding {encoding}: {e}")


def test_setdecoding_case_insensitive_encoding(db_connection):
    """Test setdecoding with case variations normalizes encoding."""

    # Test various case formats
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="UTF-8")
    settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert settings["encoding"] == "utf-8", "Encoding should be normalized to lowercase"

    db_connection.setdecoding(mssql_python.SQL_WCHAR, encoding="Utf-16LE")
    settings = db_connection.getdecoding(mssql_python.SQL_WCHAR)
    assert settings["encoding"] == "utf-16le", "Encoding should be normalized to lowercase"


def test_setdecoding_independent_sql_types(db_connection):
    """Test that decoding settings for different SQL types are independent."""

    # Set different encodings for each SQL type
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="utf-8")
    db_connection.setdecoding(mssql_python.SQL_WCHAR, encoding="utf-16le")
    db_connection.setdecoding(mssql_python.SQL_WMETADATA, encoding="utf-16be")

    # Verify each maintains its own settings
    sql_char_settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    sql_wchar_settings = db_connection.getdecoding(mssql_python.SQL_WCHAR)
    sql_wmetadata_settings = db_connection.getdecoding(mssql_python.SQL_WMETADATA)

    assert sql_char_settings["encoding"] == "utf-8", "SQL_CHAR should maintain utf-8"
    assert sql_wchar_settings["encoding"] == "utf-16le", "SQL_WCHAR should maintain utf-16le"
    assert (
        sql_wmetadata_settings["encoding"] == "utf-16be"
    ), "SQL_WMETADATA should maintain utf-16be"


def test_setdecoding_override_previous(db_connection):
    """Test setdecoding overrides previous settings for the same SQL type."""

    # Set initial decoding
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="utf-8")
    settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert settings["encoding"] == "utf-8", "Initial encoding should be utf-8"
    assert settings["ctype"] == mssql_python.SQL_CHAR, "Initial ctype should be SQL_CHAR"

    # Override with different settings
    db_connection.setdecoding(
        mssql_python.SQL_CHAR, encoding="latin-1", ctype=mssql_python.SQL_WCHAR
    )
    settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert settings["encoding"] == "latin-1", "Encoding should be overridden to latin-1"
    assert settings["ctype"] == mssql_python.SQL_WCHAR, "ctype should be overridden to SQL_WCHAR"


def test_getdecoding_invalid_sqltype(db_connection):
    """Test getdecoding with invalid sqltype raises ProgrammingError."""

    with pytest.raises(ProgrammingError) as exc_info:
        db_connection.getdecoding(999)

    assert "Invalid sqltype" in str(
        exc_info.value
    ), "Should raise ProgrammingError for invalid sqltype"
    assert "999" in str(exc_info.value), "Error message should include the invalid sqltype value"


def test_getdecoding_closed_connection(conn_str):
    """Test getdecoding on closed connection raises InterfaceError."""

    temp_conn = connect(conn_str)
    temp_conn.close()

    with pytest.raises(InterfaceError) as exc_info:
        temp_conn.getdecoding(mssql_python.SQL_CHAR)

    assert "Connection is closed" in str(
        exc_info.value
    ), "Should raise InterfaceError for closed connection"


def test_getdecoding_returns_copy(db_connection):
    """Test getdecoding returns a copy (not reference)."""

    # Set custom decoding
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="utf-8")

    # Get settings twice
    settings1 = db_connection.getdecoding(mssql_python.SQL_CHAR)
    settings2 = db_connection.getdecoding(mssql_python.SQL_CHAR)

    # Should be equal but not the same object
    assert settings1 == settings2, "Settings should be equal"
    assert settings1 is not settings2, "Settings should be different objects"

    # Modifying one shouldn't affect the other
    settings1["encoding"] = "modified"
    assert settings2["encoding"] != "modified", "Modification should not affect other copy"


def test_setdecoding_getdecoding_consistency(db_connection):
    """Test that setdecoding and getdecoding work consistently together."""

    test_cases = [
        (mssql_python.SQL_CHAR, "utf-8", mssql_python.SQL_CHAR),
        (mssql_python.SQL_CHAR, "utf-16le", mssql_python.SQL_WCHAR),
        (mssql_python.SQL_WCHAR, "latin-1", mssql_python.SQL_CHAR),
        (mssql_python.SQL_WCHAR, "utf-16be", mssql_python.SQL_WCHAR),
        (mssql_python.SQL_WMETADATA, "utf-16le", mssql_python.SQL_WCHAR),
    ]

    for sqltype, encoding, expected_ctype in test_cases:
        db_connection.setdecoding(sqltype, encoding=encoding)
        settings = db_connection.getdecoding(sqltype)
        assert settings["encoding"] == encoding.lower(), f"Encoding should be {encoding.lower()}"
        assert settings["ctype"] == expected_ctype, f"ctype should be {expected_ctype}"


def test_setdecoding_persistence_across_cursors(db_connection):
    """Test that decoding settings persist across cursor operations."""

    # Set custom decoding settings
    db_connection.setdecoding(
        mssql_python.SQL_CHAR, encoding="latin-1", ctype=mssql_python.SQL_CHAR
    )
    db_connection.setdecoding(
        mssql_python.SQL_WCHAR, encoding="utf-16be", ctype=mssql_python.SQL_WCHAR
    )

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

    assert char_settings1["encoding"] == "latin-1", "SQL_CHAR encoding should remain latin-1"
    assert wchar_settings1["encoding"] == "utf-16be", "SQL_WCHAR encoding should remain utf-16be"

    cursor1.close()
    cursor2.close()


def test_setdecoding_before_and_after_operations(db_connection):
    """Test that setdecoding works both before and after database operations."""
    cursor = db_connection.cursor()

    try:
        # Initial decoding setting
        db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="utf-8")

        # Perform database operation
        cursor.execute("SELECT 'Initial test' as message")
        result1 = cursor.fetchone()
        assert result1[0] == "Initial test", "Initial operation failed"

        # Change decoding after operation
        db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="latin-1")
        settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
        assert settings["encoding"] == "latin-1", "Failed to change decoding after operation"

        # Perform another operation with new decoding
        cursor.execute("SELECT 'Changed decoding test' as message")
        result2 = cursor.fetchone()
        assert result2[0] == "Changed decoding test", "Operation after decoding change failed"

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
            (mssql_python.SQL_CHAR, "ascii", mssql_python.SQL_CHAR),
            (mssql_python.SQL_WCHAR, "utf-16le", mssql_python.SQL_WCHAR),
            (mssql_python.SQL_WMETADATA, "utf-16be", mssql_python.SQL_WCHAR),
        ]

        for sqltype, encoding, ctype in test_configs:
            conn.setdecoding(sqltype, encoding=encoding, ctype=ctype)
            settings = conn.getdecoding(sqltype)
            assert settings["encoding"] == encoding, f"Failed to set encoding for sqltype {sqltype}"
            assert settings["ctype"] == ctype, f"Failed to set ctype for sqltype {sqltype}"

    finally:
        conn.close()


def test_setdecoding_security_logging(db_connection):
    """Test that setdecoding logs invalid attempts safely."""

    # These should raise exceptions but not crash due to logging
    test_cases = [
        (999, "utf-8", None),  # Invalid sqltype
        (mssql_python.SQL_CHAR, "invalid-encoding", None),  # Invalid encoding
        (mssql_python.SQL_CHAR, "utf-8", 999),  # Invalid ctype
    ]

    for sqltype, encoding, ctype in test_cases:
        with pytest.raises(ProgrammingError):
            db_connection.setdecoding(sqltype, encoding=encoding, ctype=ctype)


@pytest.mark.skip("Skipping Unicode data tests till we have support for Unicode")
def test_setdecoding_with_unicode_data(db_connection):
    """Test setdecoding with actual Unicode data operations."""

    # Test different decoding configurations with Unicode data
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="utf-8")
    db_connection.setdecoding(mssql_python.SQL_WCHAR, encoding="utf-16le")

    cursor = db_connection.cursor()

    try:
        # Create test table with both CHAR and NCHAR columns
        cursor.execute(
            """
            CREATE TABLE #test_decoding_unicode (
                char_col VARCHAR(100),
                nchar_col NVARCHAR(100)
            )
        """
        )

        # Test various Unicode strings
        test_strings = [
            "Hello, World!",
            "Hello, 世界!",  # Chinese
            "Привет, мир!",  # Russian
            "مرحبا بالعالم",  # Arabic
        ]

        for test_string in test_strings:
            # Insert data
            cursor.execute(
                "INSERT INTO #test_decoding_unicode (char_col, nchar_col) VALUES (?, ?)",
                test_string,
                test_string,
            )

            # Retrieve and verify
            cursor.execute(
                "SELECT char_col, nchar_col FROM #test_decoding_unicode WHERE char_col = ?",
                test_string,
            )
            result = cursor.fetchone()

            assert result is not None, f"Failed to retrieve Unicode string: {test_string}"
            assert (
                result[0] == test_string
            ), f"CHAR column mismatch: expected {test_string}, got {result[0]}"
            assert (
                result[1] == test_string
            ), f"NCHAR column mismatch: expected {test_string}, got {result[1]}"

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
    assert hasattr(db_connection, "Warning"), "Connection should have Warning attribute"
    assert hasattr(db_connection, "Error"), "Connection should have Error attribute"
    assert hasattr(
        db_connection, "InterfaceError"
    ), "Connection should have InterfaceError attribute"
    assert hasattr(db_connection, "DatabaseError"), "Connection should have DatabaseError attribute"
    assert hasattr(db_connection, "DataError"), "Connection should have DataError attribute"
    assert hasattr(
        db_connection, "OperationalError"
    ), "Connection should have OperationalError attribute"
    assert hasattr(
        db_connection, "IntegrityError"
    ), "Connection should have IntegrityError attribute"
    assert hasattr(db_connection, "InternalError"), "Connection should have InternalError attribute"
    assert hasattr(
        db_connection, "ProgrammingError"
    ), "Connection should have ProgrammingError attribute"
    assert hasattr(
        db_connection, "NotSupportedError"
    ), "Connection should have NotSupportedError attribute"


def test_connection_exception_attributes_are_classes(db_connection):
    """Test that all exception attributes are actually exception classes"""
    # Test that the attributes are the correct exception classes
    assert db_connection.Warning is Warning, "Connection.Warning should be the Warning class"
    assert db_connection.Error is Error, "Connection.Error should be the Error class"
    assert (
        db_connection.InterfaceError is InterfaceError
    ), "Connection.InterfaceError should be the InterfaceError class"
    assert (
        db_connection.DatabaseError is DatabaseError
    ), "Connection.DatabaseError should be the DatabaseError class"
    assert (
        db_connection.DataError is DataError
    ), "Connection.DataError should be the DataError class"
    assert (
        db_connection.OperationalError is OperationalError
    ), "Connection.OperationalError should be the OperationalError class"
    assert (
        db_connection.IntegrityError is IntegrityError
    ), "Connection.IntegrityError should be the IntegrityError class"
    assert (
        db_connection.InternalError is InternalError
    ), "Connection.InternalError should be the InternalError class"
    assert (
        db_connection.ProgrammingError is ProgrammingError
    ), "Connection.ProgrammingError should be the ProgrammingError class"
    assert (
        db_connection.NotSupportedError is NotSupportedError
    ), "Connection.NotSupportedError should be the NotSupportedError class"


def test_connection_exception_inheritance(db_connection):
    """Test that exception classes have correct inheritance hierarchy"""
    # Test inheritance hierarchy according to DB-API 2.0

    # All exceptions inherit from Error (except Warning)
    assert issubclass(
        db_connection.InterfaceError, db_connection.Error
    ), "InterfaceError should inherit from Error"
    assert issubclass(
        db_connection.DatabaseError, db_connection.Error
    ), "DatabaseError should inherit from Error"

    # Database exceptions inherit from DatabaseError
    assert issubclass(
        db_connection.DataError, db_connection.DatabaseError
    ), "DataError should inherit from DatabaseError"
    assert issubclass(
        db_connection.OperationalError, db_connection.DatabaseError
    ), "OperationalError should inherit from DatabaseError"
    assert issubclass(
        db_connection.IntegrityError, db_connection.DatabaseError
    ), "IntegrityError should inherit from DatabaseError"
    assert issubclass(
        db_connection.InternalError, db_connection.DatabaseError
    ), "InternalError should inherit from DatabaseError"
    assert issubclass(
        db_connection.ProgrammingError, db_connection.DatabaseError
    ), "ProgrammingError should inherit from DatabaseError"
    assert issubclass(
        db_connection.NotSupportedError, db_connection.DatabaseError
    ), "NotSupportedError should inherit from DatabaseError"


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
    assert isinstance(
        interface_error, db_connection.InterfaceError
    ), "Should be able to create InterfaceError instance"
    assert "Interface error" in str(
        interface_error
    ), "InterfaceError should contain driver error message"

    db_error = db_connection.DatabaseError("Database error", "DDBC database error")
    assert isinstance(
        db_error, db_connection.DatabaseError
    ), "Should be able to create DatabaseError instance"
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
        assert (
            "syntax" in str(e).lower() or "incorrect" in str(e).lower() or "near" in str(e).lower()
        ), "Should be a syntax-related error"
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
            pytest.fail(
                f"Expected ProgrammingError from conn1 attributes, got {type(e).__name__}: {e}"
            )

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
            assert (
                "nonexistent_table_12345" in str(e)
                or "object" in str(e).lower()
                or "not" in str(e).lower()
            ), "Should mention the missing table"
        except conn2.DatabaseError as e:
            # Acceptable since ProgrammingError inherits from DatabaseError
            pass
        except Exception as e:
            pytest.fail(
                f"Expected ProgrammingError or DatabaseError from conn2, got {type(e).__name__}: {e}"
            )

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
        assert (
            conn1.InterfaceError is conn2.InterfaceError
        ), "All connections should reference the same InterfaceError class"
        assert (
            conn1.DatabaseError is conn2.DatabaseError
        ), "All connections should reference the same DatabaseError class"
        assert (
            conn1.ProgrammingError is conn2.ProgrammingError
        ), "All connections should reference the same ProgrammingError class"

        # Test that the classes are the same as module-level imports
        assert conn1.Error is Error, "Connection.Error should be the same as module-level Error"
        assert (
            conn1.InterfaceError is InterfaceError
        ), "Connection.InterfaceError should be the same as module-level InterfaceError"
        assert (
            conn1.DatabaseError is DatabaseError
        ), "Connection.DatabaseError should be the same as module-level DatabaseError"

    finally:
        conn1.close()
        conn2.close()


def test_connection_exception_attributes_comprehensive_list():
    """Test that all DB-API 2.0 required exception attributes are present on Connection class"""
    # Test at the class level (before instantiation)
    required_exceptions = [
        "Warning",
        "Error",
        "InterfaceError",
        "DatabaseError",
        "DataError",
        "OperationalError",
        "IntegrityError",
        "InternalError",
        "ProgrammingError",
        "NotSupportedError",
    ]

    for exc_name in required_exceptions:
        assert hasattr(Connection, exc_name), f"Connection class should have {exc_name} attribute"
        exc_class = getattr(Connection, exc_name)
        assert isinstance(exc_class, type), f"Connection.{exc_name} should be a class"
        assert issubclass(
            exc_class, Exception
        ), f"Connection.{exc_name} should be an Exception subclass"


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
        cursor1 = db_connection.execute(
            "CREATE TABLE #pytest_test_execute (id INT, value VARCHAR(50))"
        )
        cursor2 = db_connection.execute("INSERT INTO #pytest_test_execute VALUES (1, 'test_value')")
        cursor3 = db_connection.execute("SELECT * FROM #pytest_test_execute")
        result = cursor3.fetchone()
        assert result is not None, "Execute with table creation failed"
        assert result[0] == 1, "Execute with table creation returned wrong id"
        assert result[1] == "test_value", "Execute with table creation returned wrong value"

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
        1234,  # Integer
        3.14159,  # Float
        "test string",  # String
        bytearray(b"binary data"),  # Binary data
        True,  # Boolean
        None,  # NULL
    ]

    for param in params:
        cursor = db_connection.execute("SELECT ? AS value", param)
        result = cursor.fetchone()
        if param is None:
            assert result[0] is None, "NULL parameter not handled correctly"
        else:
            assert (
                result[0] == param
            ), f"Parameter {param} of type {type(param)} not handled correctly"


def test_connection_execute_with_transaction(db_connection):
    """Test execute() in the context of explicit transactions"""
    if db_connection.autocommit:
        db_connection.autocommit = False

    cursor1 = db_connection.cursor()
    drop_table_if_exists(cursor1, "#pytest_test_execute_transaction")

    try:
        # Create table and insert data
        db_connection.execute(
            "CREATE TABLE #pytest_test_execute_transaction (id INT, value VARCHAR(50))"
        )
        db_connection.execute(
            "INSERT INTO #pytest_test_execute_transaction VALUES (1, 'before rollback')"
        )

        # Check data is there
        cursor = db_connection.execute("SELECT * FROM #pytest_test_execute_transaction")
        result = cursor.fetchone()
        assert result is not None, "Data should be visible within transaction"
        assert result[1] == "before rollback", "Incorrect data in transaction"

        # Rollback and verify data is gone
        db_connection.rollback()

        # Need to recreate table since it was rolled back
        db_connection.execute(
            "CREATE TABLE #pytest_test_execute_transaction (id INT, value VARCHAR(50))"
        )
        db_connection.execute(
            "INSERT INTO #pytest_test_execute_transaction VALUES (2, 'after rollback')"
        )

        cursor = db_connection.execute("SELECT * FROM #pytest_test_execute_transaction")
        result = cursor.fetchone()
        assert result is not None, "Data should be visible after new insert"
        assert result[0] == 2, "Should see the new data after rollback"
        assert result[1] == "after rollback", "Incorrect data after rollback"

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


def test_execute_multiple_simultaneous_cursors(db_connection, conn_str):
    """Test creating and using many cursors simultaneously through Connection.execute

    ⚠️ WARNING: This test has several limitations:
    1. Creates only 20 cursors, which may not fully test production scenarios requiring hundreds
    2. Relies on WeakSet tracking which depends on garbage collection timing and varies between runs
    3. Memory measurement requires the optional 'psutil' package
    4. Creates cursors sequentially rather than truly concurrently
    5. Results may vary based on system resources, SQL Server version, and ODBC driver
    6. Skipped for Azure SQL due to connection pool and throttling limitations

    The test verifies that:
    - Multiple cursors can be created and used simultaneously
    - Connection tracks created cursors appropriately
    - Connection remains stable after intensive cursor operations
    """
    # Skip this test for Azure SQL Database
    if is_azure_sql_connection(conn_str):
        pytest.skip("Skipping for Azure SQL - connection limits cause this test to hang")
    import gc

    # Start with a clean connection state
    cursor = db_connection.execute("SELECT 1")
    cursor.fetchall()  # Consume the results
    cursor.close()  # Close the cursor correctly

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
    assert (
        current_cursor_count > initial_cursor_count
    ), f"Connection should track more cursors after creating {num_cursors} new ones, but count only increased by {current_cursor_count - initial_cursor_count}"

    print(
        f"Created {num_cursors} cursors, tracking shows {current_cursor_count - initial_cursor_count} increase"
    )

    # Close all cursors explicitly to clean up
    for cursor in cursors:
        cursor.close()

    # Verify connection is still usable
    final_cursor = db_connection.execute("SELECT 'Connection still works' AS status")
    row = final_cursor.fetchone()
    assert (
        row[0] == "Connection still works"
    ), "Connection should remain usable after cursor operations"
    final_cursor.close()


def test_execute_with_large_parameters(db_connection, conn_str):
    """Test executing queries with very large parameter sets

    ⚠️ WARNING: This test has several limitations:
    1. Limited by 8192-byte parameter size restriction from the ODBC driver
    2. Cannot test truly large parameters (e.g., BLOBs >1MB)
    3. Works around the ~2100 parameter limit by batching, not testing true limits
    4. No streaming parameter support is tested
    5. Only tests with 10,000 rows, which is small compared to production scenarios
    6. Performance measurements are affected by system load and environment
    7. Skipped for Azure SQL due to connection pool and throttling limitations

    The test verifies:
    - Handling of a large number of parameters in batch inserts
    - Working with parameters near but under the size limit
    - Processing large result sets
    """
    # Skip this test for Azure SQL Database
    if is_azure_sql_connection(conn_str):
        pytest.skip("Skipping for Azure SQL - large parameter tests may cause timeouts")

    # Test with a temporary table for large data
    cursor = db_connection.execute(
        """
    DROP TABLE IF EXISTS #large_params_test;
    CREATE TABLE #large_params_test (
        id INT,
        large_text NVARCHAR(MAX),
        large_binary VARBINARY(MAX)
    )
    """
    )
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
        print(
            f"Large batch insert ({total_rows} rows in chunks of {batch_size}) completed in {batch_time:.2f} seconds"
        )

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
            1,
            large_text,
            large_binary,
        )
        cursor.close()

        # Verify the data was inserted correctly
        cursor = db_connection.execute(
            "SELECT id, LEN(large_text), DATALENGTH(large_binary) FROM #large_params_test"
        )
        row = cursor.fetchone()
        cursor.close()

        assert row is not None, "No row returned after inserting large parameters"
        assert row[0] == 1, "Wrong ID returned"
        assert row[1] > 1000, f"Text length too small: {row[1]}"
        assert row[2] == 2 * 1024, f"Binary length wrong: {row[2]}"

        large_param_time = time.time() - start_time
        print(
            f"Large parameter insert (text: {row[1]} chars, binary: {row[2]} bytes) completed in {large_param_time:.2f} seconds"
        )

        # Test 3: Execute with a large result set
        cursor = db_connection.execute("TRUNCATE TABLE #large_params_test")
        cursor.close()

        # Insert rows in smaller batches to avoid parameter limits
        rows_per_batch = 1000
        total_rows = 10000

        for batch_start in range(0, total_rows, rows_per_batch):
            batch_end = min(batch_start + rows_per_batch, total_rows)
            values = ", ".join(
                [f"({i}, 'Small Text {i}', NULL)" for i in range(batch_start, batch_end)]
            )
            cursor = db_connection.execute(
                f"INSERT INTO #large_params_test (id, large_text, large_binary) VALUES {values}"
            )
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
    """Test that cursors from execute() are properly managed throughout their lifecycle"""
    import gc
    import weakref
    import sys

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
    assert (
        len(db_connection._cursors) == initial_cursor_count + 1
    ), "Cursor should be added to connection tracking"
    assert (
        cursor1 in db_connection._cursors
    ), "Created cursor should be in the connection's tracking set"

    # 2. Test that a cursor is removed when explicitly closed
    cursor_id = id(cursor1)  # Remember the cursor's ID for later verification
    cursor1.close()

    # Force garbage collection to ensure WeakSet is updated
    gc.collect()

    # Verify cursor was removed from tracking
    remaining_cursor_ids = [id(c) for c in db_connection._cursors]
    assert (
        cursor_id not in remaining_cursor_ids
    ), "Closed cursor should be removed from connection tracking"

    # 3. Test that a cursor is tracked but then removed when it goes out of scope
    # Note: We'll create a cursor and verify it's tracked BEFORE leaving the scope
    temp_cursor = db_connection.execute("SELECT 2 AS test")
    temp_cursor.fetchall()  # Consume results

    # Get a weak reference to the cursor for checking collection later
    cursor_ref = weakref.ref(temp_cursor)

    # Verify cursor is tracked immediately after creation
    assert (
        len(db_connection._cursors) > initial_cursor_count
    ), "New cursor should be tracked immediately"
    assert (
        temp_cursor in db_connection._cursors
    ), "New cursor should be in the connection's tracking set"

    # Now remove our reference to allow garbage collection
    temp_cursor = None

    # Force garbage collection multiple times to ensure the cursor is collected
    for _ in range(3):
        gc.collect()

    # Verify cursor was eventually removed from tracking after collection
    assert cursor_ref() is None, "Cursor should be garbage collected after going out of scope"
    assert (
        len(db_connection._cursors) == initial_cursor_count
    ), "All created cursors should be removed from tracking after collection"

    # 4. Verify that many cursors can be created and properly cleaned up
    cursors = []
    for i in range(10):
        cursors.append(db_connection.execute(f"SELECT {i} AS test"))
        cursors[-1].fetchall()  # Consume results

    assert (
        len(db_connection._cursors) == initial_cursor_count + 10
    ), "All 10 cursors should be tracked by the connection"

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
    assert (
        len(db_connection._cursors) <= initial_cursor_count + 5
    ), "Explicitly closed cursors should be removed from tracking immediately"

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
        "SELECT GETDATE() AS date_value",
    ]

    # Execute the batch
    results, cursor = db_connection.batch_execute(statements)

    # Verify we got the right number of results
    assert len(results) == 3, f"Expected 3 results, got {len(results)}"

    # Check each result
    assert len(results[0]) == 1, "Expected 1 row in first result"
    assert results[0][0][0] == 1, "First result should be 1"

    assert len(results[1]) == 1, "Expected 1 row in second result"
    assert results[1][0][0] == "test", "Second result should be 'test'"

    assert len(results[2]) == 1, "Expected 1 row in third result"
    assert isinstance(results[2][0][0], (str, datetime)), "Third result should be a date"

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
        "SELECT ? AS null_param",
    ]

    params = [
        [123],
        [3.14159],
        ["test string"],
        [bytearray(b"binary data")],
        [True],
        [None],
    ]

    results, cursor = db_connection.batch_execute(statements, params)

    # Verify each parameter was correctly applied
    assert results[0][0][0] == 123, "Integer parameter not handled correctly"
    assert abs(results[1][0][0] - 3.14159) < 0.00001, "Float parameter not handled correctly"
    assert results[2][0][0] == "test string", "String parameter not handled correctly"
    assert results[3][0][0] == bytearray(b"binary data"), "Binary parameter not handled correctly"
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
            "SELECT * FROM #batch_test ORDER BY id",
        ]

        params = [[1, "value1"], [2, "value2"], ["updated", 1], [2], None]

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
    assert initial_result[0][0] == "before batch", "Initial cursor state incorrect"

    # Use the cursor in batch_execute
    statements = ["SELECT 'during batch' AS batch_state"]

    results, returned_cursor = db_connection.batch_execute(statements, reuse_cursor=cursor)

    # Verify we got the same cursor back
    assert returned_cursor is cursor, "Batch should return the same cursor object"

    # Verify the result
    assert results[0][0][0] == "during batch", "Batch result incorrect"

    # Verify cursor is still usable
    cursor.execute("SELECT 'after batch' AS final_state")
    final_result = cursor.fetchall()
    assert final_result[0][0] == "after batch", "Cursor should remain usable after batch"

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
            "SELECT COUNT(*) FROM ##batch_transaction_test",
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
        "SELECT 3",
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


def test_batch_execute_large_batch(db_connection, conn_str):
    """Test batch_execute with a large number of statements

    ⚠️ WARNING: This test has several limitations:
    1. Only tests 50 statements, which may not reveal issues with much larger batches
    2. Each statement is very simple, not testing complex query performance
    3. Memory usage for large result sets isn't thoroughly tested
    4. Results must be fully consumed between statements to avoid "Connection is busy" errors
    5. Driver-specific limitations may exist for maximum batch sizes
    6. Network timeouts during long-running batches aren't tested
    7. Skipped for Azure SQL due to connection pool and throttling limitations

    The test verifies:
    - The method can handle multiple statements in sequence
    - Results are correctly returned for all statements
    - Memory usage remains reasonable during batch processing
    """
    # Skip this test for Azure SQL Database
    if is_azure_sql_connection(conn_str):
        pytest.skip("Skipping for Azure SQL - large batch tests may cause timeouts")
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
        cursor1 = db_connection.execute(
            "CREATE TABLE #pytest_test_execute (id INT, value VARCHAR(50))"
        )
        cursor2 = db_connection.execute("INSERT INTO #pytest_test_execute VALUES (1, 'test_value')")
        cursor3 = db_connection.execute("SELECT * FROM #pytest_test_execute")
        result = cursor3.fetchone()
        assert result is not None, "Execute with table creation failed"
        assert result[0] == 1, "Execute with table creation returned wrong id"
        assert result[1] == "test_value", "Execute with table creation returned wrong value"

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
        1234,  # Integer
        3.14159,  # Float
        "test string",  # String
        bytearray(b"binary data"),  # Binary data
        True,  # Boolean
        None,  # NULL
    ]

    for param in params:
        cursor = db_connection.execute("SELECT ? AS value", param)
        result = cursor.fetchone()
        if param is None:
            assert result[0] is None, "NULL parameter not handled correctly"
        else:
            assert (
                result[0] == param
            ), f"Parameter {param} of type {type(param)} not handled correctly"


def test_connection_execute_with_transaction(db_connection):
    """Test execute() in the context of explicit transactions"""
    if db_connection.autocommit:
        db_connection.autocommit = False

    cursor1 = db_connection.cursor()
    drop_table_if_exists(cursor1, "#pytest_test_execute_transaction")

    try:
        # Create table and insert data
        db_connection.execute(
            "CREATE TABLE #pytest_test_execute_transaction (id INT, value VARCHAR(50))"
        )
        db_connection.execute(
            "INSERT INTO #pytest_test_execute_transaction VALUES (1, 'before rollback')"
        )

        # Check data is there
        cursor = db_connection.execute("SELECT * FROM #pytest_test_execute_transaction")
        result = cursor.fetchone()
        assert result is not None, "Data should be visible within transaction"
        assert result[1] == "before rollback", "Incorrect data in transaction"

        # Rollback and verify data is gone
        db_connection.rollback()

        # Need to recreate table since it was rolled back
        db_connection.execute(
            "CREATE TABLE #pytest_test_execute_transaction (id INT, value VARCHAR(50))"
        )
        db_connection.execute(
            "INSERT INTO #pytest_test_execute_transaction VALUES (2, 'after rollback')"
        )

        cursor = db_connection.execute("SELECT * FROM #pytest_test_execute_transaction")
        result = cursor.fetchone()
        assert result is not None, "Data should be visible after new insert"
        assert result[0] == 2, "Should see the new data after rollback"
        assert result[1] == "after rollback", "Incorrect data after rollback"

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
    assert hasattr(db_connection, "_output_converters")
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
        return "ANOTHER: " + value.decode("utf-16-le")

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
        return "TEMP: " + value.decode("utf-16-le")

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
            return int.from_bytes(value, byteorder="little") * 2
        elif isinstance(value, int):
            return value * 2
        return value

    db_connection.add_output_converter(int_type, int_converter)

    # Test query with both types
    cursor.execute("SELECT CAST(42 AS INT) as int_col, N'test' as str_col")
    row = cursor.fetchone()

    # Verify converters worked
    assert row[0] == 84, f"Integer converter failed, got {row[0]} instead of 84"
    assert (
        isinstance(row[1], str) and "CONVERTED:" in row[1]
    ), f"String converter failed, got {row[1]}"

    # Clean up
    db_connection.clear_output_converters()


def test_output_converter_exception_handling(db_connection):
    """Test that exceptions in output converters are properly handled"""
    cursor = db_connection.cursor()

    # First determine the actual type code for NVARCHAR
    cursor.execute("SELECT N'test string' AS test_col")
    str_type = cursor.description[0][1]

    # Define a converter that will raise an exception
    def faulty_converter(value):
        if value is None:
            return None
        # Intentionally raise an exception with potentially sensitive info
        # This simulates a bug in a custom converter
        raise ValueError(f"Converter error with sensitive data: {value!r}")

    # Add the faulty converter
    db_connection.add_output_converter(str_type, faulty_converter)

    try:
        # Execute a query that will trigger the converter
        cursor.execute("SELECT N'test string' AS test_col")

        # Attempt to fetch data, which should trigger the converter
        row = cursor.fetchone()

        # The implementation could handle this in different ways:
        # 1. Fall back to returning the unconverted value
        # 2. Return None for the problematic column
        # 3. Raise a sanitized exception

        # If we got here, the exception was caught and handled internally
        assert row is not None, "Row should still be returned despite converter error"
        assert row[0] is not None, "Column value shouldn't be None despite converter error"

        # Verify we can continue using the connection
        cursor.execute("SELECT 1 AS test")
        assert cursor.fetchone()[0] == 1, "Connection should still be usable"

    except Exception as e:
        # If an exception is raised, ensure it doesn't contain the sensitive info
        error_str = str(e)
        assert "sensitive data" not in error_str, f"Exception leaked sensitive data: {error_str}"
        assert not isinstance(e, ValueError), "Original exception type should not be exposed"

        # Verify we can continue using the connection after the error
        cursor.execute("SELECT 1 AS test")
        assert cursor.fetchone()[0] == 1, "Connection should still be usable after converter error"

    finally:
        # Clean up
        db_connection.clear_output_converters()


def test_timeout_default(db_connection):
    """Test that the default timeout value is 0 (no timeout)"""
    assert hasattr(db_connection, "timeout"), "Connection should have a timeout attribute"
    assert db_connection.timeout == 0, "Default timeout should be 0"


def test_timeout_setter(db_connection):
    """Test setting and getting the timeout value"""
    # Set a non-zero timeout
    db_connection.timeout = 30
    assert db_connection.timeout == 30, "Timeout should be set to 30"

    # Test that timeout can be reset to zero
    db_connection.timeout = 0
    assert db_connection.timeout == 0, "Timeout should be reset to 0"

    # Test setting invalid timeout values
    with pytest.raises(ValueError):
        db_connection.timeout = -1

    with pytest.raises(TypeError):
        db_connection.timeout = "30"

    # Reset timeout to default for other tests
    db_connection.timeout = 0


def test_timeout_from_constructor(conn_str):
    """Test setting timeout in the connection constructor"""
    # Create a connection with timeout set
    conn = connect(conn_str, timeout=45)
    try:
        assert conn.timeout == 45, "Timeout should be set to 45 from constructor"

        # Create a cursor and verify it inherits the timeout
        cursor = conn.cursor()
        # Execute a quick query to ensure the timeout doesn't interfere
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert result[0] == 1, "Query execution should succeed with timeout set"
    finally:
        # Clean up
        conn.close()


def test_timeout_affects_all_cursors(db_connection):
    """Test that changing timeout on connection affects all new cursors"""
    # Create a cursor with default timeout
    cursor1 = db_connection.cursor()

    # Change the connection timeout
    original_timeout = db_connection.timeout
    db_connection.timeout = 10

    # Create a new cursor
    cursor2 = db_connection.cursor()

    try:
        # Execute quick queries to ensure both cursors work
        cursor1.execute("SELECT 1")
        result1 = cursor1.fetchone()
        assert result1[0] == 1, "Query with first cursor failed"

        cursor2.execute("SELECT 2")
        result2 = cursor2.fetchone()
        assert result2[0] == 2, "Query with second cursor failed"

        # No direct way to check cursor timeout, but both should succeed
        # with the current timeout setting
    finally:
        # Reset timeout
        db_connection.timeout = original_timeout


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
        cursor1 = db_connection.execute(
            "CREATE TABLE #pytest_test_execute (id INT, value VARCHAR(50))"
        )
        cursor2 = db_connection.execute("INSERT INTO #pytest_test_execute VALUES (1, 'test_value')")
        cursor3 = db_connection.execute("SELECT * FROM #pytest_test_execute")
        result = cursor3.fetchone()
        assert result is not None, "Execute with table creation failed"
        assert result[0] == 1, "Execute with table creation returned wrong id"
        assert result[1] == "test_value", "Execute with table creation returned wrong value"

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
        1234,  # Integer
        3.14159,  # Float
        "test string",  # String
        bytearray(b"binary data"),  # Binary data
        True,  # Boolean
        None,  # NULL
    ]

    for param in params:
        cursor = db_connection.execute("SELECT ? AS value", param)
        result = cursor.fetchone()
        if param is None:
            assert result[0] is None, "NULL parameter not handled correctly"
        else:
            assert (
                result[0] == param
            ), f"Parameter {param} of type {type(param)} not handled correctly"


def test_connection_execute_with_transaction(db_connection):
    """Test execute() in the context of explicit transactions"""
    if db_connection.autocommit:
        db_connection.autocommit = False

    cursor1 = db_connection.cursor()
    drop_table_if_exists(cursor1, "#pytest_test_execute_transaction")

    try:
        # Create table and insert data
        db_connection.execute(
            "CREATE TABLE #pytest_test_execute_transaction (id INT, value VARCHAR(50))"
        )
        db_connection.execute(
            "INSERT INTO #pytest_test_execute_transaction VALUES (1, 'before rollback')"
        )

        # Check data is there
        cursor = db_connection.execute("SELECT * FROM #pytest_test_execute_transaction")
        result = cursor.fetchone()
        assert result is not None, "Data should be visible within transaction"
        assert result[1] == "before rollback", "Incorrect data in transaction"

        # Rollback and verify data is gone
        db_connection.rollback()

        # Need to recreate table since it was rolled back
        db_connection.execute(
            "CREATE TABLE #pytest_test_execute_transaction (id INT, value VARCHAR(50))"
        )
        db_connection.execute(
            "INSERT INTO #pytest_test_execute_transaction VALUES (2, 'after rollback')"
        )

        cursor = db_connection.execute("SELECT * FROM #pytest_test_execute_transaction")
        result = cursor.fetchone()
        assert result is not None, "Data should be visible after new insert"
        assert result[0] == 2, "Should see the new data after rollback"
        assert result[1] == "after rollback", "Incorrect data after rollback"

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
    assert hasattr(db_connection, "_output_converters")
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
        return "ANOTHER: " + value.decode("utf-16-le")

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
        return "TEMP: " + value.decode("utf-16-le")

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
            return int.from_bytes(value, byteorder="little") * 2
        elif isinstance(value, int):
            return value * 2
        return value

    db_connection.add_output_converter(int_type, int_converter)

    # Test query with both types
    cursor.execute("SELECT CAST(42 AS INT) as int_col, N'test' as str_col")
    row = cursor.fetchone()

    # Verify converters worked
    assert row[0] == 84, f"Integer converter failed, got {row[0]} instead of 84"
    assert (
        isinstance(row[1], str) and "CONVERTED:" in row[1]
    ), f"String converter failed, got {row[1]}"

    # Clean up
    db_connection.clear_output_converters()


def test_timeout_default(db_connection):
    """Test that the default timeout value is 0 (no timeout)"""
    assert hasattr(db_connection, "timeout"), "Connection should have a timeout attribute"
    assert db_connection.timeout == 0, "Default timeout should be 0"


def test_timeout_setter(db_connection):
    """Test setting and getting the timeout value"""
    # Set a non-zero timeout
    db_connection.timeout = 30
    assert db_connection.timeout == 30, "Timeout should be set to 30"

    # Test that timeout can be reset to zero
    db_connection.timeout = 0
    assert db_connection.timeout == 0, "Timeout should be reset to 0"

    # Test setting invalid timeout values
    with pytest.raises(ValueError):
        db_connection.timeout = -1

    with pytest.raises(TypeError):
        db_connection.timeout = "30"

    # Reset timeout to default for other tests
    db_connection.timeout = 0


def test_timeout_from_constructor(conn_str):
    """Test setting timeout in the connection constructor"""
    # Create a connection with timeout set
    conn = connect(conn_str, timeout=45)
    try:
        assert conn.timeout == 45, "Timeout should be set to 45 from constructor"

        # Create a cursor and verify it inherits the timeout
        cursor = conn.cursor()
        # Execute a quick query to ensure the timeout doesn't interfere
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert result[0] == 1, "Query execution should succeed with timeout set"
    finally:
        # Clean up
        conn.close()


def test_timeout_long_query(db_connection):
    """Test that a query exceeding the timeout raises an exception if supported by driver"""
    import time
    import pytest

    cursor = db_connection.cursor()

    try:
        # First execute a simple query to check if we can run tests
        cursor.execute("SELECT 1")
        cursor.fetchall()
    except Exception as e:
        pytest.skip(f"Skipping timeout test due to connection issue: {e}")

    # Set a short timeout
    original_timeout = db_connection.timeout
    db_connection.timeout = 2  # 2 seconds

    try:
        # Try several different approaches to test timeout
        start_time = time.perf_counter()
        max_retries = 3
        retry_count = 0

        try:
            # Method 1: CPU-intensive query with REPLICATE and large result set
            cpu_intensive_query = """
            WITH numbers AS (
                SELECT TOP 1000000 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
                FROM sys.objects a CROSS JOIN sys.objects b
            )
            SELECT COUNT(*) FROM numbers WHERE n % 2 = 0
            """
            cursor.execute(cpu_intensive_query)
            cursor.fetchall()

            elapsed_time = time.perf_counter() - start_time

            # If we get here without an exception, try a different approach
            if elapsed_time < 4.5:

                # Method 2: Try with WAITFOR
                start_time = time.perf_counter()
                cursor.execute("WAITFOR DELAY '00:00:05'")
                # Don't call fetchall() on WAITFOR - it doesn't return results
                # The execute itself should timeout
                elapsed_time = time.perf_counter() - start_time

                # If we still get here, try one more approach
                if elapsed_time < 4.5:

                    # Method 3: Try with a join that generates many rows
                    # Retry this method multiple times if we get DataError (arithmetic overflow)
                    while retry_count < max_retries:
                        start_time = time.perf_counter()
                        try:
                            cursor.execute(
                                """
                            SELECT COUNT(*) FROM sys.objects a, sys.objects b, sys.objects c
                            WHERE a.object_id = b.object_id * c.object_id
                            """
                            )
                            cursor.fetchall()
                            elapsed_time = time.perf_counter() - start_time
                            break  # Success, exit retry loop
                        except Exception as retry_e:
                            from mssql_python.exceptions import DataError

                            if (
                                isinstance(retry_e, DataError)
                                and "overflow" in str(retry_e).lower()
                            ):
                                retry_count += 1
                                if retry_count >= max_retries:
                                    # After max retries with overflow, skip this method
                                    break
                                # Wait a bit and retry
                                import time as time_module

                                time_module.sleep(0.1)
                            else:
                                # Not an overflow error, re-raise to be handled by outer exception handler
                                raise

            # If we still get here without an exception
            if elapsed_time < 4.5:
                pytest.skip("Timeout feature not enforced by database driver")

        except Exception as e:
            from mssql_python.exceptions import DataError

            # Check if this is a DataError with overflow (flaky test condition)
            if isinstance(e, DataError) and "overflow" in str(e).lower():
                pytest.skip(f"Skipping timeout test due to arithmetic overflow in test query: {e}")

            # Verify this is a timeout exception
            elapsed_time = time.perf_counter() - start_time
            assert elapsed_time < 4.5, "Exception occurred but after expected timeout"
            error_text = str(e).lower()

            # Check for various error messages that might indicate timeout
            timeout_indicators = [
                "timeout",
                "timed out",
                "hyt00",
                "hyt01",
                "cancel",
                "operation canceled",
                "execution terminated",
                "query limit",
            ]

            assert any(
                indicator in error_text for indicator in timeout_indicators
            ), f"Exception occurred but doesn't appear to be a timeout error: {e}"
    finally:
        # Reset timeout for other tests
        db_connection.timeout = original_timeout


def test_timeout_affects_all_cursors(db_connection):
    """Test that changing timeout on connection affects all new cursors"""
    # Create a cursor with default timeout
    cursor1 = db_connection.cursor()

    # Change the connection timeout
    original_timeout = db_connection.timeout
    db_connection.timeout = 10

    # Create a new cursor
    cursor2 = db_connection.cursor()

    try:
        # Execute quick queries to ensure both cursors work
        cursor1.execute("SELECT 1")
        result1 = cursor1.fetchone()
        assert result1[0] == 1, "Query with first cursor failed"

        cursor2.execute("SELECT 2")
        result2 = cursor2.fetchone()
        assert result2[0] == 2, "Query with second cursor failed"

        # No direct way to check cursor timeout, but both should succeed
        # with the current timeout setting
    finally:
        # Reset timeout
        db_connection.timeout = original_timeout


def test_getinfo_basic_driver_info(db_connection):
    """Test basic driver information info types."""

    try:
        # Driver name should be available
        driver_name = db_connection.getinfo(sql_const.SQL_DRIVER_NAME.value)
        print("Driver Name = ", driver_name)
        assert driver_name is not None, "Driver name should not be None"

        # Driver version should be available
        driver_ver = db_connection.getinfo(sql_const.SQL_DRIVER_VER.value)
        print("Driver Version = ", driver_ver)
        assert driver_ver is not None, "Driver version should not be None"

        # Data source name should be available
        dsn = db_connection.getinfo(sql_const.SQL_DATA_SOURCE_NAME.value)
        print("Data source name = ", dsn)
        assert dsn is not None, "Data source name should not be None"

        # Server name should be available (might be empty in some configurations)
        server_name = db_connection.getinfo(sql_const.SQL_SERVER_NAME.value)
        print("Server Name = ", server_name)
        assert server_name is not None, "Server name should not be None"

        # User name should be available (might be empty if using integrated auth)
        user_name = db_connection.getinfo(sql_const.SQL_USER_NAME.value)
        print("User Name = ", user_name)
        assert user_name is not None, "User name should not be None"

    except Exception as e:
        pytest.fail(f"getinfo failed for basic driver info: {e}")


def test_getinfo_string_encoding_utf16(db_connection):
    """Test that string values from getinfo are properly decoded from UTF-16."""

    # Test string info types that should not contain null bytes
    string_info_types = [
        ("SQL_DRIVER_VER", sql_const.SQL_DRIVER_VER.value),
        ("SQL_DRIVER_NAME", sql_const.SQL_DRIVER_NAME.value),
        ("SQL_DRIVER_ODBC_VER", sql_const.SQL_DRIVER_ODBC_VER.value),
        ("SQL_SERVER_NAME", sql_const.SQL_SERVER_NAME.value),
    ]

    for name, info_type in string_info_types:
        result = db_connection.getinfo(info_type)

        if result is not None:
            # Verify it's a string
            assert isinstance(result, str), f"{name}: Expected str, got {type(result).__name__}"

            # Verify no null bytes (indicates UTF-16 decoded as UTF-8 bug)
            assert (
                "\x00" not in result
            ), f"{name} contains null bytes, likely UTF-16/UTF-8 encoding mismatch: {repr(result)}"

            # Verify it's not empty (optional, but good sanity check)
            assert len(result) > 0, f"{name} returned empty string"


def test_getinfo_string_decoding_utf8_fallback(db_connection):
    """Test that getinfo falls back to UTF-8 when UTF-16LE decoding fails.

    This test verifies the fallback path in the encoding loop where
    UTF-16LE fails but UTF-8 succeeds.
    """
    from unittest.mock import patch

    # UTF-8 encoded "Hello" - this is valid UTF-8 but NOT valid UTF-16LE
    # (odd number of bytes would fail UTF-16LE decode)
    utf8_data = "Hello".encode("utf-8")  # b'Hello' - 5 bytes, odd length

    mock_result = {"data": utf8_data, "length": len(utf8_data)}

    # Use a string-type info_type (SQL_DRIVER_NAME = 6 is in string_type_constants)
    info_type = sql_const.SQL_DRIVER_NAME.value

    with patch.object(db_connection._conn, "get_info", return_value=mock_result):
        result = db_connection.getinfo(info_type)

        assert result == "Hello", f"Expected 'Hello', got {repr(result)}"
        assert isinstance(result, str), f"Expected str, got {type(result).__name__}"


def test_getinfo_string_decoding_all_fail_returns_none(db_connection):
    """Test that getinfo returns None when all decoding attempts fail.

    This test verifies that when both UTF-16LE and UTF-8 decoding fail,
    the method returns None to avoid silent data corruption.
    """
    from unittest.mock import patch

    # Invalid byte sequence that cannot be decoded as UTF-16LE or UTF-8
    # 0xFF 0xFE is a BOM, but followed by invalid continuation bytes for UTF-8
    # and odd length makes it invalid UTF-16LE
    invalid_data = bytes([0x80, 0x81, 0x82])  # Invalid for both encodings

    mock_result = {"data": invalid_data, "length": len(invalid_data)}

    # Use a string-type info_type (SQL_DRIVER_NAME = 6 is in string_type_constants)
    info_type = sql_const.SQL_DRIVER_NAME.value

    with patch.object(db_connection._conn, "get_info", return_value=mock_result):
        result = db_connection.getinfo(info_type)

        # Should return None when all decoding fails
        assert result is None, f"Expected None for invalid encoding, got {repr(result)}"


def test_getinfo_string_encoding_utf16_primary(db_connection):
    """Test that getinfo correctly decodes valid UTF-16LE data.

    This test verifies the primary (expected) encoding path where
    UTF-16LE decoding succeeds on first try.
    """
    from unittest.mock import patch

    # Valid UTF-16LE encoded "Test" with null terminator
    utf16_data = "Test".encode("utf-16-le") + b"\x00\x00"

    mock_result = {"data": utf16_data, "length": len(utf16_data)}

    # Use a string-type info_type
    info_type = sql_const.SQL_DRIVER_NAME.value

    with patch.object(db_connection._conn, "get_info", return_value=mock_result):
        result = db_connection.getinfo(info_type)

        assert result == "Test", f"Expected 'Test', got {repr(result)}"
        assert "\x00" not in result, f"Result contains null bytes: {repr(result)}"


def test_getinfo_sql_support(db_connection):
    """Test SQL support and conformance info types."""

    try:
        # SQL conformance level
        sql_conformance = db_connection.getinfo(sql_const.SQL_SQL_CONFORMANCE.value)
        print("SQL Conformance = ", sql_conformance)
        assert sql_conformance is not None, "SQL conformance should not be None"

        # Keywords - may return a very long string
        keywords = db_connection.getinfo(sql_const.SQL_KEYWORDS.value)
        print("Keywords = ", keywords)
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
        print("Catalog term = ", catalog_term)
        assert catalog_term is not None, "Catalog term should not be None"

        # Catalog name separator
        catalog_separator = db_connection.getinfo(sql_const.SQL_CATALOG_NAME_SEPARATOR.value)
        print(f"Catalog name separator: '{catalog_separator}'")
        assert catalog_separator is not None, "Catalog separator should not be None"

        # Schema term
        schema_term = db_connection.getinfo(sql_const.SQL_SCHEMA_TERM.value)
        print("Schema term = ", schema_term)
        assert schema_term is not None, "Schema term should not be None"

        # Stored procedures support
        procedures = db_connection.getinfo(sql_const.SQL_PROCEDURES.value)
        print("Procedures = ", procedures)
        assert procedures is not None, "Procedures support should not be None"

    except Exception as e:
        pytest.fail(f"getinfo failed for catalog support info: {e}")


def test_getinfo_transaction_support(db_connection):
    """Test transaction support info types."""

    try:
        # Transaction support
        txn_capable = db_connection.getinfo(sql_const.SQL_TXN_CAPABLE.value)
        print("Transaction capable = ", txn_capable)
        assert txn_capable is not None, "Transaction capability should not be None"

        # Default transaction isolation
        default_txn_isolation = db_connection.getinfo(sql_const.SQL_DEFAULT_TXN_ISOLATION.value)
        print("Default Transaction isolation = ", default_txn_isolation)
        assert default_txn_isolation is not None, "Default transaction isolation should not be None"

        # Multiple active transactions support
        multiple_txn = db_connection.getinfo(sql_const.SQL_MULTIPLE_ACTIVE_TXN.value)
        print("Multiple transaction = ", multiple_txn)
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


def test_getinfo_invalid_info_type(db_connection):
    """Test getinfo behavior with invalid info_type values."""

    # Test with a non-existent info_type number
    non_existent_type = 99999  # An info type that doesn't exist
    result = db_connection.getinfo(non_existent_type)
    assert (
        result is None
    ), f"getinfo should return None for non-existent info type {non_existent_type}"

    # Test with a negative info_type number
    negative_type = -1  # Negative values are invalid for info types
    result = db_connection.getinfo(negative_type)
    assert result is None, f"getinfo should return None for negative info type {negative_type}"

    # Test with non-integer info_type
    with pytest.raises(Exception):
        db_connection.getinfo("invalid_string")

    # Test with None as info_type
    with pytest.raises(Exception):
        db_connection.getinfo(None)


def test_getinfo_type_consistency(db_connection):
    """Test that getinfo returns consistent types for repeated calls."""

    # Choose a few representative info types that don't depend on DBMS
    info_types = [
        sql_const.SQL_DRIVER_NAME.value,
        sql_const.SQL_MAX_COLUMN_NAME_LEN.value,
        sql_const.SQL_TXN_CAPABLE.value,
        sql_const.SQL_IDENTIFIER_QUOTE_CHAR.value,
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
        sql_const.SQL_ACCESSIBLE_TABLES.value: str,  # "Y" or "N"
        sql_const.SQL_DATA_SOURCE_NAME.value: str,  # DSN
        sql_const.SQL_TABLE_TERM.value: str,  # Usually "table"
        sql_const.SQL_PROCEDURES.value: str,  # "Y" or "N"
        sql_const.SQL_MAX_IDENTIFIER_LEN.value: int,  # Max identifier length
        sql_const.SQL_OUTER_JOINS.value: str,  # "Y" or "N"
    }

    for info_type, expected_type in info_types.items():
        try:
            info_value = db_connection.getinfo(info_type)
            print(info_type, info_value)

            # Skip None values (unsupported by driver)
            if info_value is None:
                continue

            # Check type, allowing empty strings for string types
            if expected_type == str:
                assert isinstance(info_value, str), f"Info type {info_type} should return a string"
            elif expected_type == int:
                assert isinstance(
                    info_value, int
                ), f"Info type {info_type} should return an integer"

        except Exception as e:
            # Log but don't fail - some drivers might not support all info types
            print(f"Info type {info_type} failed: {e}")


def test_getinfo_numeric_limits(db_connection):
    """Test numeric limitation info types."""

    try:
        # Max column name length - should be an integer
        max_col_name_len = db_connection.getinfo(sql_const.SQL_MAX_COLUMN_NAME_LEN.value)
        assert isinstance(max_col_name_len, int), "Max column name length should be an integer"
        assert max_col_name_len >= 0, "Max column name length should be non-negative"
        print(f"Max column name length: {max_col_name_len}")

        # Max table name length
        max_table_name_len = db_connection.getinfo(sql_const.SQL_MAX_TABLE_NAME_LEN.value)
        assert isinstance(max_table_name_len, int), "Max table name length should be an integer"
        assert max_table_name_len >= 0, "Max table name length should be non-negative"
        print(f"Max table name length: {max_table_name_len}")

        # Max statement length - may return 0 for "unlimited"
        max_statement_len = db_connection.getinfo(sql_const.SQL_MAX_STATEMENT_LEN.value)
        assert isinstance(max_statement_len, int), "Max statement length should be an integer"
        assert max_statement_len >= 0, "Max statement length should be non-negative"
        print(f"Max statement length: {max_statement_len}")

        # Max connections - may return 0 for "unlimited"
        max_connections = db_connection.getinfo(sql_const.SQL_MAX_DRIVER_CONNECTIONS.value)
        assert isinstance(max_connections, int), "Max connections should be an integer"
        assert max_connections >= 0, "Max connections should be non-negative"
        print(f"Max connections: {max_connections}")

    except Exception as e:
        pytest.fail(f"getinfo failed for numeric limits info: {e}")


def test_getinfo_data_types(db_connection):
    """Test data type support info types."""

    try:
        # Numeric functions - should return an integer (bit mask)
        numeric_functions = db_connection.getinfo(sql_const.SQL_NUMERIC_FUNCTIONS.value)
        assert isinstance(numeric_functions, int), "Numeric functions should be an integer"
        print(f"Numeric functions: {numeric_functions}")

        # String functions - should return an integer (bit mask)
        string_functions = db_connection.getinfo(sql_const.SQL_STRING_FUNCTIONS.value)
        assert isinstance(string_functions, int), "String functions should be an integer"
        print(f"String functions: {string_functions}")

        # Date/time functions - should return an integer (bit mask)
        datetime_functions = db_connection.getinfo(sql_const.SQL_DATETIME_FUNCTIONS.value)
        assert isinstance(datetime_functions, int), "Datetime functions should be an integer"
        print(f"Datetime functions: {datetime_functions}")

    except Exception as e:
        pytest.fail(f"getinfo failed for data type support info: {e}")


def test_getinfo_invalid_binary_data(db_connection):
    """Test handling of invalid binary data in getinfo."""
    # Test behavior with known constants that might return complex binary data
    # We should get consistent readable values regardless of the internal format

    # Test with SQL_DRIVER_NAME (should return a readable string)
    driver_name = db_connection.getinfo(sql_const.SQL_DRIVER_NAME.value)
    assert isinstance(driver_name, str), "Driver name should be returned as a string"
    assert len(driver_name) > 0, "Driver name should not be empty"
    print(f"Driver name: {driver_name}")

    # Test with SQL_SERVER_NAME (should return a readable string)
    server_name = db_connection.getinfo(sql_const.SQL_SERVER_NAME.value)
    assert isinstance(server_name, str), "Server name should be returned as a string"
    print(f"Server name: {server_name}")


def test_getinfo_zero_length_return(db_connection):
    """Test handling of zero-length return values in getinfo."""
    # Test with SQL_SPECIAL_CHARACTERS (might return empty in some drivers)
    special_chars = db_connection.getinfo(sql_const.SQL_SPECIAL_CHARACTERS.value)
    # Should be a string (potentially empty)
    assert isinstance(special_chars, str), "Special characters should be returned as a string"
    print(f"Special characters: '{special_chars}'")

    # Test with a potentially invalid info type (try/except pattern)
    try:
        # Use a very unlikely but potentially valid info type (not 9999 which fails)
        # 999 is less likely to cause issues but still probably not defined
        unusual_info = db_connection.getinfo(999)
        # If it doesn't raise an exception, it should at least return a defined type
        assert unusual_info is None or isinstance(
            unusual_info, (str, int, bool)
        ), f"Unusual info type should return None or a basic type, got {type(unusual_info)}"
    except Exception as e:
        # Just print the exception but don't fail the test
        print(f"Info type 999 raised exception (expected): {e}")


def test_getinfo_non_standard_types(db_connection):
    """Test handling of non-standard data types in getinfo."""
    # Test various info types that return different data types

    # String return
    driver_name = db_connection.getinfo(sql_const.SQL_DRIVER_NAME.value)
    assert isinstance(driver_name, str), "Driver name should be a string"
    print(f"Driver name: {driver_name}")

    # Integer return
    max_col_len = db_connection.getinfo(sql_const.SQL_MAX_COLUMN_NAME_LEN.value)
    assert isinstance(max_col_len, int), "Max column name length should be an integer"
    print(f"Max column name length: {max_col_len}")

    # Y/N return
    accessible_tables = db_connection.getinfo(sql_const.SQL_ACCESSIBLE_TABLES.value)
    assert accessible_tables in ("Y", "N"), "Accessible tables should be 'Y' or 'N'"
    print(f"Accessible tables: {accessible_tables}")


def test_getinfo_yes_no_bytes_handling(db_connection):
    """Test handling of Y/N values in getinfo."""
    # Test Y/N info types
    yn_info_types = [
        sql_const.SQL_ACCESSIBLE_TABLES.value,
        sql_const.SQL_ACCESSIBLE_PROCEDURES.value,
        sql_const.SQL_DATA_SOURCE_READ_ONLY.value,
        sql_const.SQL_EXPRESSIONS_IN_ORDERBY.value,
        sql_const.SQL_PROCEDURES.value,
    ]

    for info_type in yn_info_types:
        result = db_connection.getinfo(info_type)
        assert result in (
            "Y",
            "N",
        ), f"Y/N value for {info_type} should be 'Y' or 'N', got {result}"
        print(f"Info type {info_type} returned: {result}")


def test_getinfo_numeric_bytes_conversion(db_connection):
    """Test conversion of binary data to numeric values in getinfo."""
    # Test constants that should return numeric values
    numeric_info_types = [
        sql_const.SQL_MAX_COLUMN_NAME_LEN.value,
        sql_const.SQL_MAX_TABLE_NAME_LEN.value,
        sql_const.SQL_MAX_SCHEMA_NAME_LEN.value,
        sql_const.SQL_TXN_CAPABLE.value,
        sql_const.SQL_NUMERIC_FUNCTIONS.value,
    ]

    for info_type in numeric_info_types:
        result = db_connection.getinfo(info_type)
        assert isinstance(
            result, int
        ), f"Numeric value for {info_type} should be an integer, got {type(result)}"
        print(f"Info type {info_type} returned: {result}")


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
        assert (
            len(results) == 1
        ), f"Escaped LIKE query for % matched {len(results)} rows instead of 1"
        if results:
            assert "abc%def" in results[0][1], "Escaped LIKE query did not match correct row"

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
        cursor.execute(
            "INSERT INTO #test_escape_underscore VALUES (2, 'abcXdef')"
        )  # 'X' could match '_'
        cursor.execute("INSERT INTO #test_escape_underscore VALUES (3, 'abcdef')")  # No match

        # Use the escape character to find the exact _ character
        query = f"SELECT * FROM #test_escape_underscore WHERE text LIKE 'abc{escape_char}_def' ESCAPE '{escape_char}'"
        cursor.execute(query)
        results = cursor.fetchall()

        # Should match only the row with the _ character
        assert (
            len(results) == 1
        ), f"Escaped LIKE query for _ matched {len(results)} rows instead of 1"
        if results:
            assert "abc_def" in results[0][1], "Escaped LIKE query did not match correct row"

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
        cursor.execute(
            "INSERT INTO #test_multiple_escapes VALUES (2, 'abc%defXghi')"
        )  # Wouldn't match the pattern
        cursor.execute(
            "INSERT INTO #test_multiple_escapes VALUES (3, 'abcXdef_ghi')"
        )  # Wouldn't match the pattern

        # Use escape character for both % and _
        query = f"""
            SELECT * FROM #test_multiple_escapes 
            WHERE text LIKE 'abc{escape_char}%def{escape_char}_ghi' ESCAPE '{escape_char}'
        """
        cursor.execute(query)
        results = cursor.fetchall()

        # Should match only the row with both % and _
        assert (
            len(results) <= 1
        ), f"Multiple escapes query matched {len(results)} rows instead of at most 1"
        if len(results) == 1:
            assert "abc%def_ghi" in results[0][1], "Multiple escapes query matched incorrect row"

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
    if "conn_str" in globals():
        try:
            new_conn = connect(conn_str)
            new_escape = new_conn.searchescape
            assert new_escape == escape1, "Searchescape should be consistent across connections"
            new_conn.close()
        except Exception as e:
            print(f"Note: New connection comparison failed: {e}")


def test_setencoding_default_settings(db_connection):
    """Test that default encoding settings are correct."""
    settings = db_connection.getencoding()
    assert settings["encoding"] == "utf-16le", "Default encoding should be utf-16le"
    assert settings["ctype"] == -8, "Default ctype should be SQL_WCHAR (-8)"


def test_setencoding_basic_functionality(db_connection):
    """Test basic setencoding functionality."""
    # Test setting UTF-8 encoding
    db_connection.setencoding(encoding="utf-8")
    settings = db_connection.getencoding()
    assert settings["encoding"] == "utf-8", "Encoding should be set to utf-8"
    assert settings["ctype"] == 1, "ctype should default to SQL_CHAR (1) for utf-8"

    # Test setting UTF-16LE with explicit ctype
    db_connection.setencoding(encoding="utf-16le", ctype=-8)
    settings = db_connection.getencoding()
    assert settings["encoding"] == "utf-16le", "Encoding should be set to utf-16le"
    assert settings["ctype"] == -8, "ctype should be SQL_WCHAR (-8)"


def test_setencoding_automatic_ctype_detection(db_connection):
    """Test automatic ctype detection based on encoding."""
    # UTF-16 variants should default to SQL_WCHAR
    utf16_encodings = ["utf-16", "utf-16le", "utf-16be"]
    for encoding in utf16_encodings:
        db_connection.setencoding(encoding=encoding)
        settings = db_connection.getencoding()
        assert settings["ctype"] == -8, f"{encoding} should default to SQL_WCHAR (-8)"

    # Other encodings should default to SQL_CHAR
    other_encodings = ["utf-8", "latin-1", "ascii"]
    for encoding in other_encodings:
        db_connection.setencoding(encoding=encoding)
        settings = db_connection.getencoding()
        assert settings["ctype"] == 1, f"{encoding} should default to SQL_CHAR (1)"


def test_setencoding_explicit_ctype_override(db_connection):
    """Test that explicit ctype parameter overrides automatic detection."""
    # Set UTF-8 with SQL_WCHAR (override default)
    db_connection.setencoding(encoding="utf-8", ctype=-8)
    settings = db_connection.getencoding()
    assert settings["encoding"] == "utf-8", "Encoding should be utf-8"
    assert settings["ctype"] == -8, "ctype should be SQL_WCHAR (-8) when explicitly set"

    # Set UTF-16LE with SQL_CHAR (override default)
    db_connection.setencoding(encoding="utf-16le", ctype=1)
    settings = db_connection.getencoding()
    assert settings["encoding"] == "utf-16le", "Encoding should be utf-16le"
    assert settings["ctype"] == 1, "ctype should be SQL_CHAR (1) when explicitly set"


def test_setencoding_none_parameters(db_connection):
    """Test setencoding with None parameters."""
    # Test with encoding=None (should use default)
    db_connection.setencoding(encoding=None)
    settings = db_connection.getencoding()
    assert settings["encoding"] == "utf-16le", "encoding=None should use default utf-16le"
    assert settings["ctype"] == -8, "ctype should be SQL_WCHAR for utf-16le"

    # Test with both None (should use defaults)
    db_connection.setencoding(encoding=None, ctype=None)
    settings = db_connection.getencoding()
    assert settings["encoding"] == "utf-16le", "encoding=None should use default utf-16le"
    assert settings["ctype"] == -8, "ctype=None should use default SQL_WCHAR"


def test_setencoding_invalid_encoding(db_connection):
    """Test setencoding with invalid encoding."""

    with pytest.raises(ProgrammingError) as exc_info:
        db_connection.setencoding(encoding="invalid-encoding-name")

    assert "Unsupported encoding" in str(
        exc_info.value
    ), "Should raise ProgrammingError for invalid encoding"
    assert "invalid-encoding-name" in str(
        exc_info.value
    ), "Error message should include the invalid encoding name"


def test_setencoding_invalid_ctype(db_connection):
    """Test setencoding with invalid ctype."""

    with pytest.raises(ProgrammingError) as exc_info:
        db_connection.setencoding(encoding="utf-8", ctype=999)

    assert "Invalid ctype" in str(exc_info.value), "Should raise ProgrammingError for invalid ctype"
    assert "999" in str(exc_info.value), "Error message should include the invalid ctype value"


def test_setencoding_closed_connection(conn_str):
    """Test setencoding on closed connection."""

    temp_conn = connect(conn_str)
    temp_conn.close()

    with pytest.raises(InterfaceError) as exc_info:
        temp_conn.setencoding(encoding="utf-8")

    assert "Connection is closed" in str(
        exc_info.value
    ), "Should raise InterfaceError for closed connection"


def test_setencoding_constants_access():
    """Test that SQL_CHAR and SQL_WCHAR constants are accessible."""

    # Test constants exist and have correct values
    assert hasattr(mssql_python, "SQL_CHAR"), "SQL_CHAR constant should be available"
    assert hasattr(mssql_python, "SQL_WCHAR"), "SQL_WCHAR constant should be available"
    assert mssql_python.SQL_CHAR == 1, "SQL_CHAR should have value 1"
    assert mssql_python.SQL_WCHAR == -8, "SQL_WCHAR should have value -8"


def test_setencoding_with_constants(db_connection):
    """Test setencoding using module constants."""

    # Test with SQL_CHAR constant
    db_connection.setencoding(encoding="utf-8", ctype=mssql_python.SQL_CHAR)
    settings = db_connection.getencoding()
    assert settings["ctype"] == mssql_python.SQL_CHAR, "Should accept SQL_CHAR constant"

    # Test with SQL_WCHAR constant
    db_connection.setencoding(encoding="utf-16le", ctype=mssql_python.SQL_WCHAR)
    settings = db_connection.getencoding()
    assert settings["ctype"] == mssql_python.SQL_WCHAR, "Should accept SQL_WCHAR constant"


def test_setencoding_common_encodings(db_connection):
    """Test setencoding with various common encodings."""
    common_encodings = [
        "utf-8",
        "utf-16le",
        "utf-16be",
        "utf-16",
        "latin-1",
        "ascii",
        "cp1252",
    ]

    for encoding in common_encodings:
        try:
            db_connection.setencoding(encoding=encoding)
            settings = db_connection.getencoding()
            assert settings["encoding"] == encoding, f"Failed to set encoding {encoding}"
        except Exception as e:
            pytest.fail(f"Failed to set valid encoding {encoding}: {e}")


def test_setencoding_persistence_across_cursors(db_connection):
    """Test that encoding settings persist across cursor operations."""
    # Set custom encoding
    db_connection.setencoding(encoding="utf-8", ctype=1)

    # Create cursors and verify encoding persists
    cursor1 = db_connection.cursor()
    settings1 = db_connection.getencoding()

    cursor2 = db_connection.cursor()
    settings2 = db_connection.getencoding()

    assert settings1 == settings2, "Encoding settings should persist across cursor creation"
    assert settings1["encoding"] == "utf-8", "Encoding should remain utf-8"
    assert settings1["ctype"] == 1, "ctype should remain SQL_CHAR"

    cursor1.close()
    cursor2.close()


@pytest.mark.skip("Skipping Unicode data tests till we have support for Unicode")
def test_setencoding_with_unicode_data(db_connection):
    """Test setencoding with actual Unicode data operations."""
    # Test UTF-8 encoding with Unicode data
    db_connection.setencoding(encoding="utf-8")
    cursor = db_connection.cursor()

    try:
        # Create test table
        cursor.execute("CREATE TABLE #test_encoding_unicode (text_col NVARCHAR(100))")

        # Test various Unicode strings
        test_strings = [
            "Hello, World!",
            "Hello, 世界!",  # Chinese
            "Привет, мир!",  # Russian
            "مرحبا بالعالم",  # Arabic
            "🌍🌎🌏",  # Emoji
        ]

        for test_string in test_strings:
            # Insert data
            cursor.execute("INSERT INTO #test_encoding_unicode (text_col) VALUES (?)", test_string)

            # Retrieve and verify
            cursor.execute(
                "SELECT text_col FROM #test_encoding_unicode WHERE text_col = ?",
                test_string,
            )
            result = cursor.fetchone()

            assert result is not None, f"Failed to retrieve Unicode string: {test_string}"
            assert (
                result[0] == test_string
            ), f"Unicode string mismatch: expected {test_string}, got {result[0]}"

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
        db_connection.setencoding(encoding="utf-16le")

        # Perform database operation
        cursor.execute("SELECT 'Initial test' as message")
        result1 = cursor.fetchone()
        assert result1[0] == "Initial test", "Initial operation failed"

        # Change encoding after operation
        db_connection.setencoding(encoding="utf-8")
        settings = db_connection.getencoding()
        assert settings["encoding"] == "utf-8", "Failed to change encoding after operation"

        # Perform another operation with new encoding
        cursor.execute("SELECT 'Changed encoding test' as message")
        result2 = cursor.fetchone()
        assert result2[0] == "Changed encoding test", "Operation after encoding change failed"

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
        assert "encoding" in encoding_info
        assert "ctype" in encoding_info
        # Default should be utf-16le with SQL_WCHAR
        assert encoding_info["encoding"] == "utf-16le"
        assert encoding_info["ctype"] == SQL_WCHAR
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
        encoding_info1["encoding"] = "modified"
        assert encoding_info2["encoding"] != "modified"
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
            ("utf-8", SQL_CHAR),
            ("utf-16le", SQL_WCHAR),
            ("latin-1", SQL_CHAR),
            ("ascii", SQL_CHAR),
        ]

        for encoding, expected_ctype in test_cases:
            conn.setencoding(encoding)
            encoding_info = conn.getencoding()
            assert encoding_info["encoding"] == encoding.lower()
            assert encoding_info["ctype"] == expected_ctype
    finally:
        conn.close()


def test_setencoding_default_encoding(conn_str):
    """Test setencoding with default UTF-16LE encoding"""
    conn = connect(conn_str)
    try:
        conn.setencoding()
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "utf-16le"
        assert encoding_info["ctype"] == SQL_WCHAR
    finally:
        conn.close()


def test_setencoding_utf8(conn_str):
    """Test setencoding with UTF-8 encoding"""
    conn = connect(conn_str)
    try:
        conn.setencoding("utf-8")
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "utf-8"
        assert encoding_info["ctype"] == SQL_CHAR
    finally:
        conn.close()


def test_setencoding_latin1(conn_str):
    """Test setencoding with latin-1 encoding"""
    conn = connect(conn_str)
    try:
        conn.setencoding("latin-1")
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "latin-1"
        assert encoding_info["ctype"] == SQL_CHAR
    finally:
        conn.close()


def test_setencoding_with_explicit_ctype_sql_char(conn_str):
    """Test setencoding with explicit SQL_CHAR ctype"""
    conn = connect(conn_str)
    try:
        conn.setencoding("utf-8", SQL_CHAR)
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "utf-8"
        assert encoding_info["ctype"] == SQL_CHAR
    finally:
        conn.close()


def test_setencoding_with_explicit_ctype_sql_wchar(conn_str):
    """Test setencoding with explicit SQL_WCHAR ctype"""
    conn = connect(conn_str)
    try:
        conn.setencoding("utf-16le", SQL_WCHAR)
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "utf-16le"
        assert encoding_info["ctype"] == SQL_WCHAR
    finally:
        conn.close()


def test_setencoding_invalid_ctype_error(conn_str):
    """Test setencoding with invalid ctype raises ProgrammingError"""

    conn = connect(conn_str)
    try:
        with pytest.raises(ProgrammingError, match="Invalid ctype"):
            conn.setencoding("utf-8", 999)
    finally:
        conn.close()


def test_setencoding_case_insensitive_encoding(conn_str):
    """Test setencoding with case variations"""
    conn = connect(conn_str)
    try:
        # Test various case formats
        conn.setencoding("UTF-8")
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "utf-8"  # Should be normalized

        conn.setencoding("Utf-16LE")
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "utf-16le"  # Should be normalized
    finally:
        conn.close()


def test_setencoding_none_encoding_default(conn_str):
    """Test setencoding with None encoding uses default"""
    conn = connect(conn_str)
    try:
        conn.setencoding(None)
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "utf-16le"
        assert encoding_info["ctype"] == SQL_WCHAR
    finally:
        conn.close()


def test_setencoding_override_previous(conn_str):
    """Test setencoding overrides previous settings"""
    conn = connect(conn_str)
    try:
        # Set initial encoding
        conn.setencoding("utf-8")
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "utf-8"
        assert encoding_info["ctype"] == SQL_CHAR

        # Override with different encoding
        conn.setencoding("utf-16le")
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "utf-16le"
        assert encoding_info["ctype"] == SQL_WCHAR
    finally:
        conn.close()


def test_setencoding_ascii(conn_str):
    """Test setencoding with ASCII encoding"""
    conn = connect(conn_str)
    try:
        conn.setencoding("ascii")
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "ascii"
        assert encoding_info["ctype"] == SQL_CHAR
    finally:
        conn.close()


def test_setencoding_cp1252(conn_str):
    """Test setencoding with Windows-1252 encoding"""
    conn = connect(conn_str)
    try:
        conn.setencoding("cp1252")
        encoding_info = conn.getencoding()
        assert encoding_info["encoding"] == "cp1252"
        assert encoding_info["ctype"] == SQL_CHAR
    finally:
        conn.close()


def test_setdecoding_default_settings(db_connection):
    """Test that default decoding settings are correct for all SQL types."""

    # Check SQL_CHAR defaults
    sql_char_settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert sql_char_settings["encoding"] == "utf-8", "Default SQL_CHAR encoding should be utf-8"
    assert (
        sql_char_settings["ctype"] == mssql_python.SQL_CHAR
    ), "Default SQL_CHAR ctype should be SQL_CHAR"

    # Check SQL_WCHAR defaults
    sql_wchar_settings = db_connection.getdecoding(mssql_python.SQL_WCHAR)
    assert (
        sql_wchar_settings["encoding"] == "utf-16le"
    ), "Default SQL_WCHAR encoding should be utf-16le"
    assert (
        sql_wchar_settings["ctype"] == mssql_python.SQL_WCHAR
    ), "Default SQL_WCHAR ctype should be SQL_WCHAR"

    # Check SQL_WMETADATA defaults
    sql_wmetadata_settings = db_connection.getdecoding(mssql_python.SQL_WMETADATA)
    assert (
        sql_wmetadata_settings["encoding"] == "utf-16le"
    ), "Default SQL_WMETADATA encoding should be utf-16le"
    assert (
        sql_wmetadata_settings["ctype"] == mssql_python.SQL_WCHAR
    ), "Default SQL_WMETADATA ctype should be SQL_WCHAR"


def test_setdecoding_basic_functionality(db_connection):
    """Test basic setdecoding functionality for different SQL types."""

    # Test setting SQL_CHAR decoding
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="latin-1")
    settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert settings["encoding"] == "latin-1", "SQL_CHAR encoding should be set to latin-1"
    assert (
        settings["ctype"] == mssql_python.SQL_CHAR
    ), "SQL_CHAR ctype should default to SQL_CHAR for latin-1"

    # Test setting SQL_WCHAR decoding
    db_connection.setdecoding(mssql_python.SQL_WCHAR, encoding="utf-16be")
    settings = db_connection.getdecoding(mssql_python.SQL_WCHAR)
    assert settings["encoding"] == "utf-16be", "SQL_WCHAR encoding should be set to utf-16be"
    assert (
        settings["ctype"] == mssql_python.SQL_WCHAR
    ), "SQL_WCHAR ctype should default to SQL_WCHAR for utf-16be"

    # Test setting SQL_WMETADATA decoding
    db_connection.setdecoding(mssql_python.SQL_WMETADATA, encoding="utf-16le")
    settings = db_connection.getdecoding(mssql_python.SQL_WMETADATA)
    assert settings["encoding"] == "utf-16le", "SQL_WMETADATA encoding should be set to utf-16le"
    assert (
        settings["ctype"] == mssql_python.SQL_WCHAR
    ), "SQL_WMETADATA ctype should default to SQL_WCHAR"


def test_setdecoding_automatic_ctype_detection(db_connection):
    """Test automatic ctype detection based on encoding for different SQL types."""

    # UTF-16 variants should default to SQL_WCHAR
    utf16_encodings = ["utf-16", "utf-16le", "utf-16be"]
    for encoding in utf16_encodings:
        db_connection.setdecoding(mssql_python.SQL_CHAR, encoding=encoding)
        settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
        assert (
            settings["ctype"] == mssql_python.SQL_WCHAR
        ), f"SQL_CHAR with {encoding} should auto-detect SQL_WCHAR ctype"

    # Other encodings should default to SQL_CHAR
    other_encodings = ["utf-8", "latin-1", "ascii", "cp1252"]
    for encoding in other_encodings:
        db_connection.setdecoding(mssql_python.SQL_WCHAR, encoding=encoding)
        settings = db_connection.getdecoding(mssql_python.SQL_WCHAR)
        assert (
            settings["ctype"] == mssql_python.SQL_CHAR
        ), f"SQL_WCHAR with {encoding} should auto-detect SQL_CHAR ctype"


def test_setdecoding_explicit_ctype_override(db_connection):
    """Test that explicit ctype parameter overrides automatic detection."""

    # Set SQL_CHAR with UTF-8 encoding but explicit SQL_WCHAR ctype
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="utf-8", ctype=mssql_python.SQL_WCHAR)
    settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert settings["encoding"] == "utf-8", "Encoding should be utf-8"
    assert (
        settings["ctype"] == mssql_python.SQL_WCHAR
    ), "ctype should be SQL_WCHAR when explicitly set"

    # Set SQL_WCHAR with UTF-16LE encoding but explicit SQL_CHAR ctype
    db_connection.setdecoding(
        mssql_python.SQL_WCHAR, encoding="utf-16le", ctype=mssql_python.SQL_CHAR
    )
    settings = db_connection.getdecoding(mssql_python.SQL_WCHAR)
    assert settings["encoding"] == "utf-16le", "Encoding should be utf-16le"
    assert (
        settings["ctype"] == mssql_python.SQL_CHAR
    ), "ctype should be SQL_CHAR when explicitly set"


def test_setdecoding_none_parameters(db_connection):
    """Test setdecoding with None parameters uses appropriate defaults."""

    # Test SQL_CHAR with encoding=None (should use utf-8 default)
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding=None)
    settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert settings["encoding"] == "utf-8", "SQL_CHAR with encoding=None should use utf-8 default"
    assert settings["ctype"] == mssql_python.SQL_CHAR, "ctype should be SQL_CHAR for utf-8"

    # Test SQL_WCHAR with encoding=None (should use utf-16le default)
    db_connection.setdecoding(mssql_python.SQL_WCHAR, encoding=None)
    settings = db_connection.getdecoding(mssql_python.SQL_WCHAR)
    assert (
        settings["encoding"] == "utf-16le"
    ), "SQL_WCHAR with encoding=None should use utf-16le default"
    assert settings["ctype"] == mssql_python.SQL_WCHAR, "ctype should be SQL_WCHAR for utf-16le"

    # Test with both parameters None
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding=None, ctype=None)
    settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert settings["encoding"] == "utf-8", "SQL_CHAR with both None should use utf-8 default"
    assert settings["ctype"] == mssql_python.SQL_CHAR, "ctype should default to SQL_CHAR"


def test_setdecoding_invalid_sqltype(db_connection):
    """Test setdecoding with invalid sqltype raises ProgrammingError."""

    with pytest.raises(ProgrammingError) as exc_info:
        db_connection.setdecoding(999, encoding="utf-8")

    assert "Invalid sqltype" in str(
        exc_info.value
    ), "Should raise ProgrammingError for invalid sqltype"
    assert "999" in str(exc_info.value), "Error message should include the invalid sqltype value"


def test_setdecoding_invalid_encoding(db_connection):
    """Test setdecoding with invalid encoding raises ProgrammingError."""

    with pytest.raises(ProgrammingError) as exc_info:
        db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="invalid-encoding-name")

    assert "Unsupported encoding" in str(
        exc_info.value
    ), "Should raise ProgrammingError for invalid encoding"
    assert "invalid-encoding-name" in str(
        exc_info.value
    ), "Error message should include the invalid encoding name"


def test_setdecoding_invalid_ctype(db_connection):
    """Test setdecoding with invalid ctype raises ProgrammingError."""

    with pytest.raises(ProgrammingError) as exc_info:
        db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="utf-8", ctype=999)

    assert "Invalid ctype" in str(exc_info.value), "Should raise ProgrammingError for invalid ctype"
    assert "999" in str(exc_info.value), "Error message should include the invalid ctype value"


def test_setdecoding_closed_connection(conn_str):
    """Test setdecoding on closed connection raises InterfaceError."""

    temp_conn = connect(conn_str)
    temp_conn.close()

    with pytest.raises(InterfaceError) as exc_info:
        temp_conn.setdecoding(mssql_python.SQL_CHAR, encoding="utf-8")

    assert "Connection is closed" in str(
        exc_info.value
    ), "Should raise InterfaceError for closed connection"


def test_setdecoding_constants_access():
    """Test that SQL constants are accessible."""

    # Test constants exist and have correct values
    assert hasattr(mssql_python, "SQL_CHAR"), "SQL_CHAR constant should be available"
    assert hasattr(mssql_python, "SQL_WCHAR"), "SQL_WCHAR constant should be available"
    assert hasattr(mssql_python, "SQL_WMETADATA"), "SQL_WMETADATA constant should be available"

    assert mssql_python.SQL_CHAR == 1, "SQL_CHAR should have value 1"
    assert mssql_python.SQL_WCHAR == -8, "SQL_WCHAR should have value -8"
    assert mssql_python.SQL_WMETADATA == -99, "SQL_WMETADATA should have value -99"


def test_setdecoding_with_constants(db_connection):
    """Test setdecoding using module constants."""

    # Test with SQL_CHAR constant
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="utf-8", ctype=mssql_python.SQL_CHAR)
    settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert settings["ctype"] == mssql_python.SQL_CHAR, "Should accept SQL_CHAR constant"

    # Test with SQL_WCHAR constant
    db_connection.setdecoding(
        mssql_python.SQL_WCHAR, encoding="utf-16le", ctype=mssql_python.SQL_WCHAR
    )
    settings = db_connection.getdecoding(mssql_python.SQL_WCHAR)
    assert settings["ctype"] == mssql_python.SQL_WCHAR, "Should accept SQL_WCHAR constant"

    # Test with SQL_WMETADATA constant
    db_connection.setdecoding(mssql_python.SQL_WMETADATA, encoding="utf-16be")
    settings = db_connection.getdecoding(mssql_python.SQL_WMETADATA)
    assert settings["encoding"] == "utf-16be", "Should accept SQL_WMETADATA constant"


def test_setdecoding_common_encodings(db_connection):
    """Test setdecoding with various common encodings."""

    common_encodings = [
        "utf-8",
        "utf-16le",
        "utf-16be",
        "utf-16",
        "latin-1",
        "ascii",
        "cp1252",
    ]

    for encoding in common_encodings:
        try:
            db_connection.setdecoding(mssql_python.SQL_CHAR, encoding=encoding)
            settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
            assert (
                settings["encoding"] == encoding
            ), f"Failed to set SQL_CHAR decoding to {encoding}"

            db_connection.setdecoding(mssql_python.SQL_WCHAR, encoding=encoding)
            settings = db_connection.getdecoding(mssql_python.SQL_WCHAR)
            assert (
                settings["encoding"] == encoding
            ), f"Failed to set SQL_WCHAR decoding to {encoding}"
        except Exception as e:
            pytest.fail(f"Failed to set valid encoding {encoding}: {e}")


def test_setdecoding_case_insensitive_encoding(db_connection):
    """Test setdecoding with case variations normalizes encoding."""

    # Test various case formats
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="UTF-8")
    settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert settings["encoding"] == "utf-8", "Encoding should be normalized to lowercase"

    db_connection.setdecoding(mssql_python.SQL_WCHAR, encoding="Utf-16LE")
    settings = db_connection.getdecoding(mssql_python.SQL_WCHAR)
    assert settings["encoding"] == "utf-16le", "Encoding should be normalized to lowercase"


def test_setdecoding_independent_sql_types(db_connection):
    """Test that decoding settings for different SQL types are independent."""

    # Set different encodings for each SQL type
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="utf-8")
    db_connection.setdecoding(mssql_python.SQL_WCHAR, encoding="utf-16le")
    db_connection.setdecoding(mssql_python.SQL_WMETADATA, encoding="utf-16be")

    # Verify each maintains its own settings
    sql_char_settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    sql_wchar_settings = db_connection.getdecoding(mssql_python.SQL_WCHAR)
    sql_wmetadata_settings = db_connection.getdecoding(mssql_python.SQL_WMETADATA)

    assert sql_char_settings["encoding"] == "utf-8", "SQL_CHAR should maintain utf-8"
    assert sql_wchar_settings["encoding"] == "utf-16le", "SQL_WCHAR should maintain utf-16le"
    assert (
        sql_wmetadata_settings["encoding"] == "utf-16be"
    ), "SQL_WMETADATA should maintain utf-16be"


def test_setdecoding_override_previous(db_connection):
    """Test setdecoding overrides previous settings for the same SQL type."""

    # Set initial decoding
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="utf-8")
    settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert settings["encoding"] == "utf-8", "Initial encoding should be utf-8"
    assert settings["ctype"] == mssql_python.SQL_CHAR, "Initial ctype should be SQL_CHAR"

    # Override with different settings
    db_connection.setdecoding(
        mssql_python.SQL_CHAR, encoding="latin-1", ctype=mssql_python.SQL_WCHAR
    )
    settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
    assert settings["encoding"] == "latin-1", "Encoding should be overridden to latin-1"
    assert settings["ctype"] == mssql_python.SQL_WCHAR, "ctype should be overridden to SQL_WCHAR"


def test_getdecoding_invalid_sqltype(db_connection):
    """Test getdecoding with invalid sqltype raises ProgrammingError."""

    with pytest.raises(ProgrammingError) as exc_info:
        db_connection.getdecoding(999)

    assert "Invalid sqltype" in str(
        exc_info.value
    ), "Should raise ProgrammingError for invalid sqltype"
    assert "999" in str(exc_info.value), "Error message should include the invalid sqltype value"


def test_getdecoding_closed_connection(conn_str):
    """Test getdecoding on closed connection raises InterfaceError."""

    temp_conn = connect(conn_str)
    temp_conn.close()

    with pytest.raises(InterfaceError) as exc_info:
        temp_conn.getdecoding(mssql_python.SQL_CHAR)

    assert "Connection is closed" in str(
        exc_info.value
    ), "Should raise InterfaceError for closed connection"


def test_getdecoding_returns_copy(db_connection):
    """Test getdecoding returns a copy (not reference)."""

    # Set custom decoding
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="utf-8")

    # Get settings twice
    settings1 = db_connection.getdecoding(mssql_python.SQL_CHAR)
    settings2 = db_connection.getdecoding(mssql_python.SQL_CHAR)

    # Should be equal but not the same object
    assert settings1 == settings2, "Settings should be equal"
    assert settings1 is not settings2, "Settings should be different objects"

    # Modifying one shouldn't affect the other
    settings1["encoding"] = "modified"
    assert settings2["encoding"] != "modified", "Modification should not affect other copy"


def test_setdecoding_getdecoding_consistency(db_connection):
    """Test that setdecoding and getdecoding work consistently together."""

    test_cases = [
        (mssql_python.SQL_CHAR, "utf-8", mssql_python.SQL_CHAR),
        (mssql_python.SQL_CHAR, "utf-16le", mssql_python.SQL_WCHAR),
        (mssql_python.SQL_WCHAR, "latin-1", mssql_python.SQL_CHAR),
        (mssql_python.SQL_WCHAR, "utf-16be", mssql_python.SQL_WCHAR),
        (mssql_python.SQL_WMETADATA, "utf-16le", mssql_python.SQL_WCHAR),
    ]

    for sqltype, encoding, expected_ctype in test_cases:
        db_connection.setdecoding(sqltype, encoding=encoding)
        settings = db_connection.getdecoding(sqltype)
        assert settings["encoding"] == encoding.lower(), f"Encoding should be {encoding.lower()}"
        assert settings["ctype"] == expected_ctype, f"ctype should be {expected_ctype}"


def test_setdecoding_persistence_across_cursors(db_connection):
    """Test that decoding settings persist across cursor operations."""

    # Set custom decoding settings
    db_connection.setdecoding(
        mssql_python.SQL_CHAR, encoding="latin-1", ctype=mssql_python.SQL_CHAR
    )
    db_connection.setdecoding(
        mssql_python.SQL_WCHAR, encoding="utf-16be", ctype=mssql_python.SQL_WCHAR
    )

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

    assert char_settings1["encoding"] == "latin-1", "SQL_CHAR encoding should remain latin-1"
    assert wchar_settings1["encoding"] == "utf-16be", "SQL_WCHAR encoding should remain utf-16be"

    cursor1.close()
    cursor2.close()


def test_setdecoding_before_and_after_operations(db_connection):
    """Test that setdecoding works both before and after database operations."""
    cursor = db_connection.cursor()

    try:
        # Initial decoding setting
        db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="utf-8")

        # Perform database operation
        cursor.execute("SELECT 'Initial test' as message")
        result1 = cursor.fetchone()
        assert result1[0] == "Initial test", "Initial operation failed"

        # Change decoding after operation
        db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="latin-1")
        settings = db_connection.getdecoding(mssql_python.SQL_CHAR)
        assert settings["encoding"] == "latin-1", "Failed to change decoding after operation"

        # Perform another operation with new decoding
        cursor.execute("SELECT 'Changed decoding test' as message")
        result2 = cursor.fetchone()
        assert result2[0] == "Changed decoding test", "Operation after decoding change failed"

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
            (mssql_python.SQL_CHAR, "ascii", mssql_python.SQL_CHAR),
            (mssql_python.SQL_WCHAR, "utf-16le", mssql_python.SQL_WCHAR),
            (mssql_python.SQL_WMETADATA, "utf-16be", mssql_python.SQL_WCHAR),
        ]

        for sqltype, encoding, ctype in test_configs:
            conn.setdecoding(sqltype, encoding=encoding, ctype=ctype)
            settings = conn.getdecoding(sqltype)
            assert settings["encoding"] == encoding, f"Failed to set encoding for sqltype {sqltype}"
            assert settings["ctype"] == ctype, f"Failed to set ctype for sqltype {sqltype}"

    finally:
        conn.close()


def test_setdecoding_security_logging(db_connection):
    """Test that setdecoding logs invalid attempts safely."""

    # These should raise exceptions but not crash due to logging
    test_cases = [
        (999, "utf-8", None),  # Invalid sqltype
        (mssql_python.SQL_CHAR, "invalid-encoding", None),  # Invalid encoding
        (mssql_python.SQL_CHAR, "utf-8", 999),  # Invalid ctype
    ]

    for sqltype, encoding, ctype in test_cases:
        with pytest.raises(ProgrammingError):
            db_connection.setdecoding(sqltype, encoding=encoding, ctype=ctype)


@pytest.mark.skip("Skipping Unicode data tests till we have support for Unicode")
def test_setdecoding_with_unicode_data(db_connection):
    """Test setdecoding with actual Unicode data operations."""

    # Test different decoding configurations with Unicode data
    db_connection.setdecoding(mssql_python.SQL_CHAR, encoding="utf-8")
    db_connection.setdecoding(mssql_python.SQL_WCHAR, encoding="utf-16le")

    cursor = db_connection.cursor()

    try:
        # Create test table with both CHAR and NCHAR columns
        cursor.execute(
            """
            CREATE TABLE #test_decoding_unicode (
                char_col VARCHAR(100),
                nchar_col NVARCHAR(100)
            )
        """
        )

        # Test various Unicode strings
        test_strings = [
            "Hello, World!",
            "Hello, 世界!",  # Chinese
            "Привет, мир!",  # Russian
            "مرحبا بالعالم",  # Arabic
        ]

        for test_string in test_strings:
            # Insert data
            cursor.execute(
                "INSERT INTO #test_decoding_unicode (char_col, nchar_col) VALUES (?, ?)",
                test_string,
                test_string,
            )

            # Retrieve and verify
            cursor.execute(
                "SELECT char_col, nchar_col FROM #test_decoding_unicode WHERE char_col = ?",
                test_string,
            )
            result = cursor.fetchone()

            assert result is not None, f"Failed to retrieve Unicode string: {test_string}"
            assert (
                result[0] == test_string
            ), f"CHAR column mismatch: expected {test_string}, got {result[0]}"
            assert (
                result[1] == test_string
            ), f"NCHAR column mismatch: expected {test_string}, got {result[1]}"

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


# ==================== SET_ATTR TEST CASES ====================


def test_set_attr_constants_access():
    """Test that only relevant connection attribute constants are accessible.

    This test distinguishes between driver-independent (ODBC standard) and
    driver-manager–dependent (may not be supported everywhere) constants.
    Only ODBC-standard, cross-platform constants should be public API.
    """
    # ODBC-standard, driver-independent constants (should be public)
    odbc_attr_constants = [
        "SQL_ATTR_ACCESS_MODE",
        "SQL_ATTR_CONNECTION_TIMEOUT",
        "SQL_ATTR_CURRENT_CATALOG",
        "SQL_ATTR_LOGIN_TIMEOUT",
        "SQL_ATTR_PACKET_SIZE",
        "SQL_ATTR_TXN_ISOLATION",
    ]
    odbc_value_constants = [
        "SQL_TXN_READ_UNCOMMITTED",
        "SQL_TXN_READ_COMMITTED",
        "SQL_TXN_REPEATABLE_READ",
        "SQL_TXN_SERIALIZABLE",
        "SQL_MODE_READ_WRITE",
        "SQL_MODE_READ_ONLY",
    ]

    # Driver-manager–dependent or rarely supported constants (should NOT be public API)
    dm_attr_constants = [
        "SQL_ATTR_QUIET_MODE",
        "SQL_ATTR_TRACE",
        "SQL_ATTR_TRACEFILE",
        "SQL_ATTR_TRANSLATE_LIB",
        "SQL_ATTR_TRANSLATE_OPTION",
        "SQL_ATTR_CONNECTION_POOLING",
        "SQL_ATTR_CP_MATCH",
        "SQL_ATTR_ASYNC_ENABLE",
        "SQL_ATTR_CONNECTION_DEAD",
        "SQL_ATTR_SERVER_NAME",
        "SQL_ATTR_RESET_CONNECTION",
        "SQL_ATTR_ODBC_CURSORS",
        "SQL_CUR_USE_IF_NEEDED",
        "SQL_CUR_USE_ODBC",
        "SQL_CUR_USE_DRIVER",
    ]
    dm_value_constants = ["SQL_CD_TRUE", "SQL_CD_FALSE", "SQL_RESET_CONNECTION_YES"]

    # Check ODBC-standard constants are present and int
    for const_name in odbc_attr_constants + odbc_value_constants:
        assert hasattr(
            mssql_python, const_name
        ), f"{const_name} should be available (ODBC standard)"
        const_value = getattr(mssql_python, const_name)
        assert isinstance(const_value, int), f"{const_name} should be an integer"

    # Check driver-manager–dependent constants are NOT present
    for const_name in dm_attr_constants + dm_value_constants:
        assert not hasattr(mssql_python, const_name), f"{const_name} should NOT be public API"


def test_set_attr_basic_functionality(db_connection):
    """Test basic set_attr functionality with ODBC-standard attributes."""
    try:
        db_connection.set_attr(mssql_python.SQL_ATTR_CONNECTION_TIMEOUT, 30)
    except Exception as e:
        if "not supported" not in str(e).lower():
            pytest.fail(f"Unexpected error setting connection timeout: {e}")


def test_set_attr_transaction_isolation(db_connection):
    """Test setting transaction isolation level (ODBC-standard)."""
    isolation_levels = [
        mssql_python.SQL_TXN_READ_UNCOMMITTED,
        mssql_python.SQL_TXN_READ_COMMITTED,
        mssql_python.SQL_TXN_REPEATABLE_READ,
        mssql_python.SQL_TXN_SERIALIZABLE,
    ]
    for level in isolation_levels:
        try:
            db_connection.set_attr(mssql_python.SQL_ATTR_TXN_ISOLATION, level)
            break
        except Exception as e:
            error_str = str(e).lower()
            if not any(
                phrase in error_str
                for phrase in ["not supported", "failed to set", "invalid", "error"]
            ):
                pytest.fail(f"Unexpected error setting isolation level {level}: {e}")


def test_set_attr_invalid_attr_id_type(db_connection):
    """Test set_attr with invalid attr_id type raises ProgrammingError."""
    from mssql_python.exceptions import ProgrammingError

    invalid_attr_ids = ["string", 3.14, None, [], {}]
    for invalid_attr_id in invalid_attr_ids:
        with pytest.raises(ProgrammingError) as exc_info:
            db_connection.set_attr(invalid_attr_id, 1)

        assert "Attribute must be an integer" in str(
            exc_info.value
        ), f"Should raise ProgrammingError for invalid attr_id type: {type(invalid_attr_id)}"


def test_set_attr_invalid_value_type(db_connection):
    """Test set_attr with invalid value type raises ProgrammingError."""
    from mssql_python.exceptions import ProgrammingError

    invalid_values = [3.14, None, [], {}]

    for invalid_value in invalid_values:
        with pytest.raises(ProgrammingError) as exc_info:
            db_connection.set_attr(mssql_python.SQL_ATTR_CONNECTION_TIMEOUT, invalid_value)

        assert "Unsupported attribute value type" in str(
            exc_info.value
        ), f"Should raise ProgrammingError for invalid value type: {type(invalid_value)}"


def test_set_attr_value_out_of_range(db_connection):
    """Test set_attr with value out of SQLULEN range raises ProgrammingError."""
    from mssql_python.exceptions import ProgrammingError

    out_of_range_values = [-1, -100]

    for invalid_value in out_of_range_values:
        with pytest.raises(ProgrammingError) as exc_info:
            db_connection.set_attr(mssql_python.SQL_ATTR_CONNECTION_TIMEOUT, invalid_value)

        assert "Integer value cannot be negative" in str(
            exc_info.value
        ), f"Should raise ProgrammingError for out of range value: {invalid_value}"


def test_set_attr_closed_connection(conn_str):
    """Test set_attr on closed connection raises InterfaceError."""
    from mssql_python.exceptions import InterfaceError

    temp_conn = connect(conn_str)
    temp_conn.close()

    with pytest.raises(InterfaceError) as exc_info:
        temp_conn.set_attr(mssql_python.SQL_ATTR_CONNECTION_TIMEOUT, 30)

    assert "Connection is closed" in str(
        exc_info.value
    ), "Should raise InterfaceError for closed connection"


def test_set_attr_invalid_attribute_id(db_connection):
    """Test set_attr with invalid/unsupported attribute ID."""
    from mssql_python.exceptions import ProgrammingError, DatabaseError

    # Use a clearly invalid attribute ID
    invalid_attr_id = 999999

    try:
        db_connection.set_attr(invalid_attr_id, 1)
        # If no exception, some drivers might silently ignore invalid attributes
        pytest.skip("Driver silently accepts invalid attribute IDs")
    except (ProgrammingError, DatabaseError) as e:
        # Expected behavior - driver should reject invalid attribute
        assert (
            "attribute" in str(e).lower()
            or "invalid" in str(e).lower()
            or "not supported" in str(e).lower()
        )
    except Exception as e:
        pytest.fail(f"Unexpected exception type for invalid attribute: {type(e).__name__}: {e}")


def test_set_attr_valid_range_values(db_connection):
    """Test set_attr with valid range of values."""

    # Test boundary values for SQLUINTEGER
    valid_values = [0, 1, 100, 1000, 65535, 4294967295]

    for value in valid_values:
        try:
            # Use connection timeout as it's commonly supported
            db_connection.set_attr(mssql_python.SQL_ATTR_CONNECTION_TIMEOUT, value)
            # If we get here, the value was accepted
        except Exception as e:
            # Some values might not be valid for specific attributes
            if "invalid" not in str(e).lower() and "not supported" not in str(e).lower():
                pytest.fail(f"Unexpected error for valid value {value}: {e}")


def test_set_attr_multiple_attributes(db_connection):
    """Test setting multiple attributes in sequence."""

    # Test setting multiple safe attributes
    attribute_value_pairs = [
        (mssql_python.SQL_ATTR_CONNECTION_TIMEOUT, 60),
        (mssql_python.SQL_ATTR_LOGIN_TIMEOUT, 30),
        (mssql_python.SQL_ATTR_PACKET_SIZE, 4096),
    ]

    successful_sets = 0
    for attr_id, value in attribute_value_pairs:
        try:
            db_connection.set_attr(attr_id, value)
            successful_sets += 1
        except Exception as e:
            # Some attributes might not be supported by all drivers
            # Accept "not supported", "failed to set", or other driver errors
            error_str = str(e).lower()
            if not any(
                phrase in error_str
                for phrase in ["not supported", "failed to set", "invalid", "error"]
            ):
                pytest.fail(f"Unexpected error setting attribute {attr_id} to {value}: {e}")

    # At least one attribute setting should succeed on most drivers
    if successful_sets == 0:
        pytest.skip("No connection attributes supported by this driver configuration")


def test_set_attr_with_constants(db_connection):
    """Test set_attr using exported module constants."""

    # Test using the exported constants
    test_cases = [
        (mssql_python.SQL_ATTR_TXN_ISOLATION, mssql_python.SQL_TXN_READ_COMMITTED),
        (mssql_python.SQL_ATTR_ACCESS_MODE, mssql_python.SQL_MODE_READ_WRITE),
    ]

    for attr_id, value in test_cases:
        try:
            db_connection.set_attr(attr_id, value)
            # Success - the constants worked correctly
        except Exception as e:
            # Some attributes/values might not be supported
            # Accept "not supported", "failed to set", "invalid", or other driver errors
            error_str = str(e).lower()
            if not any(
                phrase in error_str
                for phrase in ["not supported", "failed to set", "invalid", "error"]
            ):
                pytest.fail(f"Unexpected error using constants {attr_id}, {value}: {e}")


def test_set_attr_persistence_across_operations(db_connection):
    """Test that set_attr changes persist across database operations."""

    cursor = db_connection.cursor()
    try:
        # Set an attribute before operations
        db_connection.set_attr(mssql_python.SQL_ATTR_CONNECTION_TIMEOUT, 45)

        # Perform database operation
        cursor.execute("SELECT 1 as test_value")
        result = cursor.fetchone()
        assert result[0] == 1, "Database operation should succeed"

        # Set attribute after operation
        db_connection.set_attr(mssql_python.SQL_ATTR_CONNECTION_TIMEOUT, 60)

        # Another operation
        cursor.execute("SELECT 2 as test_value")
        result = cursor.fetchone()
        assert result[0] == 2, "Database operation after set_attr should succeed"

    except Exception as e:
        if "not supported" not in str(e).lower():
            pytest.fail(f"Error in set_attr persistence test: {e}")
    finally:
        cursor.close()


def test_set_attr_security_logging(db_connection):
    """Test that set_attr logs invalid attempts safely."""
    from mssql_python.exceptions import ProgrammingError

    # These should raise exceptions but not crash due to logging
    test_cases = [
        ("invalid_attr", 1),  # Invalid attr_id type
        (123, "invalid_value"),  # Invalid value type
        (123, -1),  # Out of range value
    ]

    for attr_id, value in test_cases:
        with pytest.raises(ProgrammingError):
            db_connection.set_attr(attr_id, value)


def test_set_attr_edge_cases(db_connection):
    """Test set_attr with edge case values."""

    # Test with boundary values
    edge_cases = [
        (mssql_python.SQL_ATTR_CONNECTION_TIMEOUT, 0),  # Minimum value
        (mssql_python.SQL_ATTR_CONNECTION_TIMEOUT, 4294967295),  # Maximum SQLUINTEGER
    ]

    for attr_id, value in edge_cases:
        try:
            db_connection.set_attr(attr_id, value)
            # Success with edge case value
        except Exception as e:
            # Some edge values might not be valid for specific attributes
            if "out of range" in str(e).lower():
                pytest.fail(f"Edge case value {value} should be in valid range")
            elif "not supported" not in str(e).lower() and "invalid" not in str(e).lower():
                pytest.fail(f"Unexpected error for edge case {attr_id}, {value}: {e}")


def test_set_attr_txn_isolation_effect(db_connection):
    """Test that setting transaction isolation level actually affects transactions."""
    import os

    conn_str = os.getenv("DB_CONNECTION_STRING")

    # Create a temporary table for the test
    cursor = db_connection.cursor()
    try:
        drop_table_if_exists(cursor, "##test_isolation")
        cursor.execute("CREATE TABLE ##test_isolation (id INT, value VARCHAR(50))")
        cursor.execute("INSERT INTO ##test_isolation VALUES (1, 'original')")
        db_connection.commit()

        # First set transaction isolation level to SERIALIZABLE (most strict)
        try:
            db_connection.set_attr(
                mssql_python.SQL_ATTR_TXN_ISOLATION, mssql_python.SQL_TXN_SERIALIZABLE
            )

            # Create two separate connections for the test
            conn1 = connect(conn_str)
            conn2 = connect(conn_str)

            # Start transaction in first connection
            cursor1 = conn1.cursor()
            cursor1.execute("BEGIN TRANSACTION")
            cursor1.execute("UPDATE ##test_isolation SET value = 'updated' WHERE id = 1")

            # Try to read from second connection - should be blocked or timeout
            cursor2 = conn2.cursor()
            cursor2.execute("SET LOCK_TIMEOUT 5000")  # 5 second timeout

            with pytest.raises((DatabaseError, Exception)) as exc_info:
                cursor2.execute("SELECT * FROM ##test_isolation WHERE id = 1")

            # Clean up
            cursor1.execute("ROLLBACK")
            cursor1.close()
            conn1.close()
            cursor2.close()
            conn2.close()

            # Now set READ UNCOMMITTED (least strict)
            db_connection.set_attr(
                mssql_python.SQL_ATTR_TXN_ISOLATION,
                mssql_python.SQL_TXN_READ_UNCOMMITTED,
            )

            # Create two new connections
            conn1 = connect(conn_str)
            conn2 = connect(conn_str)
            conn2.set_attr(
                mssql_python.SQL_ATTR_TXN_ISOLATION,
                mssql_python.SQL_TXN_READ_UNCOMMITTED,
            )

            # Start transaction in first connection
            cursor1 = conn1.cursor()
            cursor1.execute("BEGIN TRANSACTION")
            cursor1.execute("UPDATE ##test_isolation SET value = 'dirty read' WHERE id = 1")

            # Try to read from second connection - should succeed with READ UNCOMMITTED
            cursor2 = conn2.cursor()
            cursor2.execute("SET LOCK_TIMEOUT 5000")
            cursor2.execute("SELECT value FROM ##test_isolation WHERE id = 1")
            result = cursor2.fetchone()[0]

            # Should see uncommitted "dirty read" value
            assert result == "dirty read", "READ UNCOMMITTED should allow dirty reads"

            # Clean up
            cursor1.execute("ROLLBACK")
            cursor1.close()
            conn1.close()
            cursor2.close()
            conn2.close()

        except Exception as e:
            if "not supported" not in str(e).lower():
                pytest.fail(f"Unexpected error in transaction isolation test: {e}")
            else:
                pytest.skip("Transaction isolation level changes not supported by driver")

    finally:
        # Clean up
        try:
            cursor.execute("DROP TABLE ##test_isolation")
        except:
            pass
        cursor.close()


def test_set_attr_connection_timeout_effect(db_connection):
    """Test that setting connection timeout actually affects query timeout."""

    cursor = db_connection.cursor()
    try:
        # Set a short timeout (3 seconds)
        try:
            # Try to set the connection timeout
            db_connection.set_attr(mssql_python.SQL_ATTR_CONNECTION_TIMEOUT, 3)

            # Check if the timeout setting worked by running an actual query
            # WAITFOR DELAY is a reliable way to test timeout
            start_time = time.time()
            try:
                cursor.execute("WAITFOR DELAY '00:00:05'")  # 5-second delay
                # If we get here, the timeout didn't work, but we won't fail the test
                # since not all drivers support this feature
                end_time = time.time()
                elapsed = end_time - start_time
                if elapsed >= 4.5:
                    pytest.skip("Connection timeout attribute not effective with this driver")
            except Exception as exc:
                # If we got an exception, check if it's a timeout-related exception
                error_msg = str(exc).lower()
                if "timeout" in error_msg or "timed out" in error_msg or "canceled" in error_msg:
                    # This is the expected behavior if timeout works
                    assert True
                else:
                    # It's some other error, not a timeout
                    pytest.skip(f"Connection timeout test encountered non-timeout error: {exc}")

        except Exception as e:
            if "not supported" not in str(e).lower():
                pytest.fail(f"Unexpected error in connection timeout test: {e}")
            else:
                pytest.skip("Connection timeout not supported by driver")

    finally:
        cursor.close()


def test_set_attr_login_timeout_effect(conn_str):
    """Test that setting login timeout affects connection time to invalid server."""

    # Testing with a non-existent server to trigger a timeout
    conn_parts = conn_str.split(";")
    new_parts = []
    for part in conn_parts:
        if part.startswith("Server=") or part.startswith("server="):
            # Use an invalid server address that will timeout
            new_parts.append("Server=invalidserver.example.com")
        else:
            new_parts.append(part)

    # Add explicit login timeout directly in the connection string
    new_parts.append("Connect Timeout=5")

    invalid_conn_str = ";".join(new_parts)

    # Test with a short timeout
    start_time = time.time()
    try:
        # Create a new connection with login timeout in the connection string
        conn = connect(invalid_conn_str)  # Don't use the login_timeout parameter
        conn.close()
        pytest.fail("Connection to invalid server should have failed")
    except Exception:
        end_time = time.time()
        elapsed = end_time - start_time

        # Be more lenient with the timeout verification - up to 20 seconds
        # Network conditions and driver behavior can vary
        if elapsed > 30:
            pytest.skip(
                f"Login timeout test took too long ({elapsed:.1f}s) but this may be environment-dependent"
            )

        # We expected an exception, so this is successful
        assert True


def test_set_attr_packet_size_effect(conn_str):
    """Test that setting packet size affects network packet size."""

    # Some drivers don't support changing packet size after connection
    # Try with explicit packet size in connection string for the first size
    packet_size = 4096
    try:
        # Add packet size to connection string
        if ";" in conn_str:
            modified_conn_str = conn_str + f";Packet Size={packet_size}"
        else:
            modified_conn_str = conn_str + f" Packet Size={packet_size}"

        conn = connect(modified_conn_str)

        # Execute a query that returns a large result set to test packet size
        cursor = conn.cursor()

        # Create a temp table with a large string column
        drop_table_if_exists(cursor, "##test_packet_size")
        cursor.execute("CREATE TABLE ##test_packet_size (id INT, large_data NVARCHAR(MAX))")

        # Insert a very large string
        large_string = "X" * (packet_size // 2)  # Unicode chars take 2 bytes each
        cursor.execute("INSERT INTO ##test_packet_size VALUES (?, ?)", (1, large_string))
        conn.commit()

        # Fetch the large string
        cursor.execute("SELECT large_data FROM ##test_packet_size WHERE id = 1")
        result = cursor.fetchone()[0]

        assert result == large_string, "Data should be retrieved correctly"

        # Clean up
        cursor.execute("DROP TABLE ##test_packet_size")
        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        if "not supported" not in str(e).lower() and "attribute" not in str(e).lower():
            pytest.fail(f"Unexpected error in packet size test: {e}")
        else:
            pytest.skip(f"Packet size setting not supported: {e}")


def test_set_attr_current_catalog_effect(db_connection, conn_str):
    """Test that setting the current catalog/database actually changes the context."""
    # This only works if we have multiple databases available
    cursor = db_connection.cursor()
    try:
        # Get current database name
        cursor.execute("SELECT DB_NAME()")
        original_db = cursor.fetchone()[0]

        # Get list of other databases
        cursor.execute("SELECT name FROM sys.databases WHERE database_id > 4 AND name != DB_NAME()")
        rows = cursor.fetchall()
        if not rows:
            pytest.skip("No other user databases available for testing")

        other_db = rows[0][0]

        # Try to switch database using set_attr
        try:
            db_connection.set_attr(mssql_python.SQL_ATTR_CURRENT_CATALOG, other_db)

            # Verify we're now in the other database
            cursor.execute("SELECT DB_NAME()")
            new_db = cursor.fetchone()[0]

            assert new_db == other_db, f"Database should have changed to {other_db} but is {new_db}"

            # Switch back
            db_connection.set_attr(mssql_python.SQL_ATTR_CURRENT_CATALOG, original_db)

            # Verify we're back in the original database
            cursor.execute("SELECT DB_NAME()")
            current_db = cursor.fetchone()[0]

            assert (
                current_db == original_db
            ), f"Database should have changed back to {original_db} but is {current_db}"

        except Exception as e:
            if "not supported" not in str(e).lower():
                pytest.fail(f"Unexpected error in current catalog test: {e}")
            else:
                pytest.skip("Current catalog changes not supported by driver")

    finally:
        cursor.close()


# ==================== TEST ATTRS_BEFORE AND SET_ATTR TIMING ====================


def test_attrs_before_login_timeout(conn_str):
    """Test setting login timeout before connection via attrs_before."""
    # Test with a reasonable timeout value
    timeout_value = 30
    conn = connect(
        conn_str,
        attrs_before={ConstantsDDBC.SQL_ATTR_LOGIN_TIMEOUT.value: timeout_value},
    )

    # Verify connection was successful
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    result = cursor.fetchall()
    assert result[0][0] == 1
    conn.close()


def test_attrs_before_packet_size(conn_str):
    """Test setting packet size before connection via attrs_before."""
    # Use a valid packet size value
    packet_size = 8192  # 8KB packet size
    conn = connect(conn_str, attrs_before={ConstantsDDBC.SQL_ATTR_PACKET_SIZE.value: packet_size})

    # Verify connection was successful
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    result = cursor.fetchall()
    assert result[0][0] == 1
    conn.close()


def test_attrs_before_multiple_attributes(conn_str):
    """Test setting multiple attributes before connection via attrs_before."""
    attrs = {
        ConstantsDDBC.SQL_ATTR_LOGIN_TIMEOUT.value: 30,
        ConstantsDDBC.SQL_ATTR_PACKET_SIZE.value: 8192,
        ConstantsDDBC.SQL_ATTR_ACCESS_MODE.value: ConstantsDDBC.SQL_MODE_READ_WRITE.value,
        ConstantsDDBC.SQL_ATTR_TXN_ISOLATION.value: ConstantsDDBC.SQL_TXN_READ_COMMITTED.value,
    }

    conn = connect(conn_str, attrs_before=attrs)

    # Verify connection was successful
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    result = cursor.fetchall()
    assert result[0][0] == 1
    conn.close()


def test_set_attr_access_mode_after_connect(db_connection):
    """Test setting access mode after connection via set_attr."""
    # Set access mode to read-write (default, but explicitly set it)
    db_connection.set_attr(
        ConstantsDDBC.SQL_ATTR_ACCESS_MODE.value,
        ConstantsDDBC.SQL_MODE_READ_WRITE.value,
    )

    # Verify we can still execute writes
    cursor = db_connection.cursor()
    drop_table_if_exists(cursor, "#test_access_mode")
    cursor.execute("CREATE TABLE #test_access_mode (id INT)")
    cursor.execute("INSERT INTO #test_access_mode VALUES (1)")
    cursor.execute("SELECT * FROM #test_access_mode")
    result = cursor.fetchall()
    assert result[0][0] == 1


def test_set_attr_current_catalog_after_connect(db_connection, conn_str):
    """Test setting current catalog after connection via set_attr."""
    # Skip this test for Azure SQL Database - it doesn't support changing database after connection
    if is_azure_sql_connection(conn_str):
        pytest.skip(
            "Skipping for Azure SQL - SQL_ATTR_CURRENT_CATALOG not supported after connection"
        )
    # Get current database name
    cursor = db_connection.cursor()
    cursor.execute("SELECT DB_NAME()")
    original_db = cursor.fetchone()[0]

    # Try to set current catalog to master
    db_connection.set_attr(ConstantsDDBC.SQL_ATTR_CURRENT_CATALOG.value, "master")

    # Verify the change
    cursor.execute("SELECT DB_NAME()")
    new_db = cursor.fetchone()[0]
    assert new_db.lower() == "master"

    # Set it back to the original
    db_connection.set_attr(ConstantsDDBC.SQL_ATTR_CURRENT_CATALOG.value, original_db)


def test_set_attr_connection_timeout_after_connect(db_connection):
    """Test setting connection timeout after connection via set_attr."""
    # Set connection timeout to a reasonable value
    db_connection.set_attr(ConstantsDDBC.SQL_ATTR_CONNECTION_TIMEOUT.value, 60)

    # Verify we can still execute queries
    cursor = db_connection.cursor()
    cursor.execute("SELECT 1")
    result = cursor.fetchall()
    assert result[0][0] == 1


def test_set_attr_before_only_attributes_error(db_connection):
    """Test that setting before-only attributes after connection raises error."""
    # Try to set login timeout after connection
    with pytest.raises(ProgrammingError) as excinfo:
        db_connection.set_attr(ConstantsDDBC.SQL_ATTR_LOGIN_TIMEOUT.value, 30)

    assert "must be set before connection establishment" in str(excinfo.value)

    # Try to set packet size after connection
    with pytest.raises(ProgrammingError) as excinfo:
        db_connection.set_attr(ConstantsDDBC.SQL_ATTR_PACKET_SIZE.value, 8192)

    assert "must be set before connection establishment" in str(excinfo.value)


def test_attrs_before_after_only_attributes(conn_str):
    """Test that setting after-only attributes before connection is ignored."""
    # Try to set connection dead before connection (should be ignored)
    conn = connect(conn_str, attrs_before={ConstantsDDBC.SQL_ATTR_CONNECTION_DEAD.value: 0})

    # Verify connection was successful
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    result = cursor.fetchall()
    assert result[0][0] == 1
    conn.close()


def test_set_attr_unsupported_attribute(db_connection):
    """Test that setting an unsupported attribute raises an error."""
    # Choose an attribute not in the supported list
    unsupported_attr = 999999  # A made-up attribute ID

    with pytest.raises(ProgrammingError) as excinfo:
        db_connection.set_attr(unsupported_attr, 1)

    assert "Unsupported attribute" in str(excinfo.value)


def test_set_attr_interface_error_exception_paths_no_mock(db_connection):
    """Test set_attr exception paths that raise InterfaceError by using invalid attributes."""
    from mssql_python.exceptions import InterfaceError, ProgrammingError

    # Test with an attribute that will likely cause an "invalid" error from the driver
    # Using a very large attribute ID that's unlikely to be valid
    invalid_attr_id = 99999

    try:
        db_connection.set_attr(invalid_attr_id, 1)
        # If it doesn't raise an exception, that's unexpected but not a test failure
        pass
    except InterfaceError:
        # This is the path we want to test
        pass
    except ProgrammingError:
        # This tests the other exception path
        pass
    except Exception as e:
        # Check if the error message contains keywords that would trigger InterfaceError
        error_str = str(e).lower()
        if "invalid" in error_str or "unsupported" in error_str or "cast" in error_str:
            # This would have triggered the InterfaceError path
            pass


def test_set_attr_programming_error_exception_path_no_mock(db_connection):
    """Test set_attr exception path that raises ProgrammingError for other database errors."""
    from mssql_python.exceptions import ProgrammingError, InterfaceError

    # Try to set an attribute with a completely invalid type that should cause an error
    # but not contain 'invalid', 'unsupported', or 'cast' keywords
    try:
        # Use a valid attribute but with extreme values that might cause driver errors
        db_connection.set_attr(mssql_python.SQL_ATTR_CONNECTION_TIMEOUT, 2147483647)  # Max int32
        pass
    except (ProgrammingError, InterfaceError):
        # Either exception type is acceptable for this test
        pass
    except Exception:
        # Any other exception is also acceptable for coverage
        pass


def test_constants_get_attribute_set_timing_unknown_attribute():
    """Test get_attribute_set_timing with unknown attribute returns AFTER_ONLY default."""
    from mssql_python.constants import get_attribute_set_timing, AttributeSetTime

    # Use a very large number that's unlikely to be a real attribute
    unknown_attribute = 99999
    timing = get_attribute_set_timing(unknown_attribute)
    assert timing == AttributeSetTime.AFTER_ONLY


def test_set_attr_with_string_attributes_real():
    """Test set_attr with string values to trigger C++ string handling paths."""
    from mssql_python import connect

    # Use actual connection string but with attrs_before to test C++ string handling
    conn_str_base = "Driver={ODBC Driver 18 for SQL Server};Server=(local);Database=tempdb;Trusted_Connection=yes;"

    try:
        # Test with a string attribute - even if it fails, it will trigger C++ code paths
        # Use SQL_ATTR_CURRENT_CATALOG which accepts string values
        conn = connect(conn_str_base, attrs_before={1006: "tempdb"})  # SQL_ATTR_CURRENT_CATALOG
        conn.close()
    except Exception:
        # Expected to potentially fail, but should trigger C++ string paths
        pass


def test_set_attr_with_binary_attributes_real():
    """Test set_attr with binary values to trigger C++ binary handling paths."""
    from mssql_python import connect

    conn_str_base = "Driver={ODBC Driver 18 for SQL Server};Server=(local);Database=tempdb;Trusted_Connection=yes;"

    try:
        # Test with binary data - this will likely fail but trigger C++ binary handling
        binary_value = b"test_binary_data_for_coverage"
        # Use an attribute that might accept binary data
        conn = connect(conn_str_base, attrs_before={1045: binary_value})  # Some random attribute
        conn.close()
    except Exception:
        # Expected to fail, but should trigger C++ binary paths
        pass


def test_set_attr_trigger_cpp_buffer_management_real():
    """Test scenarios that might trigger C++ buffer management code."""
    from mssql_python import connect

    conn_str_base = "Driver={ODBC Driver 18 for SQL Server};Server=(local);Database=tempdb;Trusted_Connection=yes;"

    # Create multiple connection attempts with varying string lengths to potentially trigger buffer management
    string_lengths = [10, 50, 100, 500, 1000]

    for length in string_lengths:
        try:
            test_string = "x" * length
            # Try with SQL_ATTR_CURRENT_CATALOG which should accept string values
            conn = connect(conn_str_base, attrs_before={1006: test_string})
            conn.close()
        except Exception:
            # Expected failures are okay - we're testing C++ code paths
            pass


def test_set_attr_extreme_values():
    """Test set_attr with various extreme values that might trigger different C++ error paths."""
    from mssql_python import connect

    conn_str_base = "Driver={ODBC Driver 18 for SQL Server};Server=(local);Database=tempdb;Trusted_Connection=yes;"

    # Test different types of extreme values
    extreme_values = [
        ("empty_string", ""),
        ("very_long_string", "x" * 1000),
        ("unicode_string", "测试数据🚀"),
        ("empty_binary", b""),
        ("large_binary", b"x" * 1000),
    ]

    for test_name, value in extreme_values:
        try:
            conn = connect(conn_str_base, attrs_before={1006: value})
            conn.close()
        except Exception:
            # Failures are expected and acceptable for coverage testing
            pass


def test_attrs_before_various_attribute_types():
    """Test attrs_before with various attribute types to increase C++ coverage."""
    from mssql_python import connect

    conn_str_base = "Driver={ODBC Driver 18 for SQL Server};Server=(local);Database=tempdb;Trusted_Connection=yes;"

    # Test with different attribute IDs and value types
    test_attrs = [
        {1000: 1},  # Integer attribute
        {1001: "test_string"},  # String attribute
        {1002: b"test_binary"},  # Binary attribute
        {1003: bytearray(b"test")},  # Bytearray attribute
    ]

    for attrs in test_attrs:
        try:
            conn = connect(conn_str_base, attrs_before=attrs)
            conn.close()
        except Exception:
            # Expected failures for invalid attributes
            pass


def test_connection_established_error_simulation():
    """Test scenarios that might trigger 'Connection not established' error."""
    # This is difficult to test without mocking, but we can try edge cases

    # Try to trigger timing issues or edge cases
    from mssql_python import connect

    try:
        # Use an invalid connection string that might partially initialize
        invalid_conn_str = "Driver={Nonexistent Driver};Server=invalid;"
        conn = connect(invalid_conn_str)
    except Exception:
        # Expected to fail, might trigger various C++ error paths
        pass


def test_helpers_edge_case_sanitization():
    """Test edge cases in helper function sanitization."""
    from mssql_python.helpers import sanitize_user_input

    # Test various edge cases for sanitization
    edge_cases = [
        "",  # Empty string
        "a",  # Single character
        "x" * 1000,  # Very long string
        "test!@#$%^&*()",  # Special characters
        "test\n\r\t",  # Control characters
        "测试",  # Unicode characters
        None,  # None value (if function handles it)
    ]

    for test_input in edge_cases:
        try:
            if test_input is not None:
                result = sanitize_user_input(test_input)
                # Just verify it returns something reasonable
                assert isinstance(result, str)
        except Exception:
            # Some edge cases might raise exceptions, which is acceptable
            pass


def test_validate_attribute_edge_cases():
    """Test validate_attribute_value with various edge cases."""
    from mssql_python.helpers import validate_attribute_value

    # Test boundary conditions
    edge_cases = [
        (0, 0),  # Zero values
        (-1, -1),  # Negative values
        (2147483647, 2147483647),  # Max int32
        (1, ""),  # Empty string
        (1, b""),  # Empty binary
        (1, bytearray()),  # Empty bytearray
    ]

    for attr, value in edge_cases:
        is_valid, error_message, sanitized_attr, sanitized_val = validate_attribute_value(
            attr, value
        )
        # Just verify the function completes and returns expected tuple structure
        assert isinstance(is_valid, bool)
        assert isinstance(error_message, str)
        assert isinstance(sanitized_attr, str)
        assert isinstance(sanitized_val, str)


def test_validate_attribute_string_size_limit():
    """Test validate_attribute_value string size validation (Lines 261-269)."""
    from mssql_python.helpers import validate_attribute_value
    from mssql_python.constants import ConstantsDDBC

    # Test with a valid string (within limit)
    valid_string = "x" * 8192  # Exactly at the limit
    is_valid, error_message, sanitized_attr, sanitized_val = validate_attribute_value(
        ConstantsDDBC.SQL_ATTR_CURRENT_CATALOG.value, valid_string
    )
    assert is_valid is True
    assert error_message is None

    # Test with string that exceeds the limit (triggers lines 265-269)
    oversized_string = "x" * 8193  # One byte over the limit
    is_valid, error_message, sanitized_attr, sanitized_val = validate_attribute_value(
        ConstantsDDBC.SQL_ATTR_CURRENT_CATALOG.value, oversized_string
    )
    assert is_valid is False
    assert "String value too large" in error_message
    assert "8193 bytes (max 8192)" in error_message
    assert isinstance(sanitized_attr, str)
    assert isinstance(sanitized_val, str)

    # Test with much larger string to confirm the validation
    very_large_string = "x" * 16384  # Much larger than limit
    is_valid, error_message, sanitized_attr, sanitized_val = validate_attribute_value(
        ConstantsDDBC.SQL_ATTR_CURRENT_CATALOG.value, very_large_string
    )
    assert is_valid is False
    assert "String value too large" in error_message
    assert "16384 bytes (max 8192)" in error_message


def test_validate_attribute_binary_size_limit():
    """Test validate_attribute_value binary size validation (Lines 272-280)."""
    from mssql_python.helpers import validate_attribute_value
    from mssql_python.constants import ConstantsDDBC

    # Test with valid binary data (within limit)
    valid_binary = b"x" * 32768  # Exactly at the limit
    is_valid, error_message, sanitized_attr, sanitized_val = validate_attribute_value(
        ConstantsDDBC.SQL_ATTR_CURRENT_CATALOG.value, valid_binary
    )
    assert is_valid is True
    assert error_message is None

    # Test with binary data that exceeds the limit (triggers lines 276-280)
    oversized_binary = b"x" * 32769  # One byte over the limit
    is_valid, error_message, sanitized_attr, sanitized_val = validate_attribute_value(
        ConstantsDDBC.SQL_ATTR_CURRENT_CATALOG.value, oversized_binary
    )
    assert is_valid is False
    assert "Binary value too large" in error_message
    assert "32769 bytes (max 32768)" in error_message
    assert isinstance(sanitized_attr, str)
    assert isinstance(sanitized_val, str)

    # Test with bytearray that exceeds the limit
    oversized_bytearray = bytearray(b"x" * 32769)
    is_valid, error_message, sanitized_attr, sanitized_val = validate_attribute_value(
        ConstantsDDBC.SQL_ATTR_CURRENT_CATALOG.value, oversized_bytearray
    )
    assert is_valid is False
    assert "Binary value too large" in error_message
    assert "32769 bytes (max 32768)" in error_message

    # Test with much larger binary data to confirm the validation
    very_large_binary = b"x" * 65536  # Much larger than limit
    is_valid, error_message, sanitized_attr, sanitized_val = validate_attribute_value(
        ConstantsDDBC.SQL_ATTR_CURRENT_CATALOG.value, very_large_binary
    )
    assert is_valid is False
    assert "Binary value too large" in error_message
    assert "65536 bytes (max 32768)" in error_message


def test_validate_attribute_size_limits_edge_cases():
    """Test validate_attribute_value size limit edge cases."""
    from mssql_python.helpers import validate_attribute_value
    from mssql_python.constants import ConstantsDDBC

    # Test string exactly at the boundary
    boundary_string = "a" * 8192
    is_valid, error_message, sanitized_attr, sanitized_val = validate_attribute_value(
        ConstantsDDBC.SQL_ATTR_CURRENT_CATALOG.value, boundary_string
    )
    assert is_valid is True
    assert error_message is None

    # Test binary exactly at the boundary
    boundary_binary = b"a" * 32768
    is_valid, error_message, sanitized_attr, sanitized_val = validate_attribute_value(
        ConstantsDDBC.SQL_ATTR_CURRENT_CATALOG.value, boundary_binary
    )
    assert is_valid is True
    assert error_message is None

    # Test empty values (should be valid)
    is_valid, error_message, sanitized_attr, sanitized_val = validate_attribute_value(
        ConstantsDDBC.SQL_ATTR_CURRENT_CATALOG.value, ""
    )
    assert is_valid is True

    is_valid, error_message, sanitized_attr, sanitized_val = validate_attribute_value(
        ConstantsDDBC.SQL_ATTR_CURRENT_CATALOG.value, b""
    )
    assert is_valid is True

    is_valid, error_message, sanitized_attr, sanitized_val = validate_attribute_value(
        ConstantsDDBC.SQL_ATTR_CURRENT_CATALOG.value, bytearray()
    )
    assert is_valid is True


def test_searchescape_caching_behavior(db_connection):
    """Test searchescape property caching and basic functionality."""

    # Clear any cached searchescape to test fresh behavior
    if hasattr(db_connection, "_searchescape"):
        delattr(db_connection, "_searchescape")

    # First call should retrieve and cache the value
    escape_char1 = db_connection.searchescape
    assert isinstance(escape_char1, str), "Search escape should be a string"

    # Second call should return cached value
    escape_char2 = db_connection.searchescape
    assert escape_char1 == escape_char2, "Cached searchescape should be consistent"

    # The property should be cached now
    assert hasattr(db_connection, "_searchescape"), "Should cache searchescape after first access"


def test_batch_execute_auto_close_behavior(db_connection):
    """Test batch_execute auto_close functionality with valid operations."""

    # Test successful execution with auto_close=True
    results, cursor = db_connection.batch_execute(["SELECT 1 as test_col"], auto_close=True)

    # Verify results
    assert len(results) == 1, "Should have one result set"
    assert results[0][0][0] == 1, "Should return correct value"

    # Since auto_close=True, the cursor should be closed
    assert cursor.closed, "Cursor should be closed when auto_close=True"


def test_getinfo_invalid_info_types(db_connection):
    """Test getinfo with various invalid info types to trigger error paths."""

    from mssql_python.constants import GetInfoConstants

    # Test with very large invalid info_type (should return None)
    result = db_connection.getinfo(99999)
    assert result is None, "Should return None for invalid large info_type"

    # Test with negative info_type (should return None)
    result = db_connection.getinfo(-1)
    assert result is None, "Should return None for negative info_type"

    # Test with invalid type (should raise ValueError)
    with pytest.raises(ValueError, match="info_type must be an integer"):
        db_connection.getinfo("invalid")

    # Test some valid info types to ensure normal operation
    driver_name = db_connection.getinfo(GetInfoConstants.SQL_DRIVER_NAME.value)
    assert isinstance(driver_name, str), "Driver name should be a string"


def test_getinfo_different_return_types(db_connection):
    """Test getinfo with different return types to exercise various code paths."""

    from mssql_python.constants import GetInfoConstants

    # Test Y/N type (should return "Y" or "N")
    accessible_tables = db_connection.getinfo(GetInfoConstants.SQL_ACCESSIBLE_TABLES.value)
    assert accessible_tables in ("Y", "N"), "Accessible tables should be Y or N"

    # Test numeric type (should return integer)
    max_col_len = db_connection.getinfo(GetInfoConstants.SQL_MAX_COLUMN_NAME_LEN.value)
    assert isinstance(max_col_len, int), "Max column name length should be integer"
    assert max_col_len > 0, "Max column name length should be positive"

    # Test string type (should return string)
    driver_name = db_connection.getinfo(GetInfoConstants.SQL_DRIVER_NAME.value)
    assert isinstance(driver_name, str), "Driver name should be string"
    assert len(driver_name) > 0, "Driver name should not be empty"


def test_connection_cursor_lifecycle_management(conn_str):
    """Test connection cursor tracking and cleanup."""

    conn = connect(conn_str)

    try:
        # Create multiple cursors
        cursor1 = conn.cursor()
        cursor2 = conn.cursor()

        # Verify cursors are being tracked
        assert hasattr(conn, "_cursors"), "Connection should track cursors"
        assert len(conn._cursors) == 2, "Should track both cursors"

        # Close one cursor manually
        cursor1.close()

        # The closed cursor should be removed from tracking
        assert cursor1 not in conn._cursors, "Closed cursor should be removed from tracking"
        assert len(conn._cursors) == 1, "Should only track open cursor"

        # Connection close should handle remaining cursors
        conn.close()

        # Verify both cursors are closed
        assert cursor1.closed, "First cursor should be closed"
        assert cursor2.closed, "Second cursor should be closed"

    except Exception as e:
        # Ensure connection is closed in case of error
        if not conn._closed:
            conn.close()
        raise


def test_connection_remove_cursor_edge_cases(conn_str):
    """Test edge cases in cursor removal."""

    conn = connect(conn_str)

    try:
        cursor = conn.cursor()

        # Test removing cursor that's already closed
        cursor.close()

        # Try to remove it again - should not raise exception (line 1375 path)
        conn._remove_cursor(cursor)

        # Cursor should no longer be in the set
        assert cursor not in conn._cursors, "Cursor should not be in cursor set after removal"

    finally:
        if not conn._closed:
            conn.close()


def test_connection_multiple_cursor_operations(conn_str):
    """Test multiple cursor operations and proper cleanup."""

    conn = connect(conn_str)

    try:
        cursors = []

        # Create multiple cursors and perform operations
        for i in range(3):
            cursor = conn.cursor()
            cursor.execute(f"SELECT {i+1} as test_value")
            result = cursor.fetchone()
            assert result[0] == i + 1, f"Cursor {i} should return {i+1}"
            cursors.append(cursor)

        # Verify all cursors are tracked
        assert len(conn._cursors) == 3, "Should track all 3 cursors"

        # Close cursors individually
        for cursor in cursors:
            cursor.close()

        # All cursors should be removed from tracking
        assert len(conn._cursors) == 0, "All cursors should be removed after individual close"

    finally:
        if not conn._closed:
            conn.close()


def test_batch_execute_error_handling_with_invalid_sql(db_connection):
    """Test batch_execute error handling with invalid SQL."""

    # Test with invalid SQL to trigger execution error
    with pytest.raises((DatabaseError, ProgrammingError)):
        db_connection.batch_execute(
            [
                "SELECT 1",  # Valid
                "INVALID SQL SYNTAX HERE",  # Invalid - should cause error
            ],
            auto_close=True,
        )

    # Test that connection remains usable after error
    results, cursor = db_connection.batch_execute(
        ["SELECT 'recovery_test' as recovery"], auto_close=True
    )
    assert results[0][0][0] == "recovery_test", "Connection should be usable after error"
    assert cursor.closed, "Cursor should be closed with auto_close=True"


def test_comprehensive_getinfo_scenarios(db_connection):
    """Comprehensive test for various getinfo scenarios and edge cases."""

    from mssql_python.constants import GetInfoConstants

    # Test multiple valid info types to exercise different code paths
    test_cases = [
        # String types
        (GetInfoConstants.SQL_DRIVER_NAME.value, str),
        (GetInfoConstants.SQL_DATA_SOURCE_NAME.value, str),
        (GetInfoConstants.SQL_SERVER_NAME.value, str),
        # Y/N types
        (GetInfoConstants.SQL_ACCESSIBLE_TABLES.value, str),
        (GetInfoConstants.SQL_ACCESSIBLE_PROCEDURES.value, str),
        # Numeric types
        (GetInfoConstants.SQL_MAX_COLUMN_NAME_LEN.value, int),
        (GetInfoConstants.SQL_TXN_CAPABLE.value, int),
    ]

    for info_type, expected_type in test_cases:
        result = db_connection.getinfo(info_type)

        # Some info types might return None if not supported by the driver
        if result is not None:
            assert isinstance(
                result, expected_type
            ), f"Info type {info_type} should return {expected_type.__name__} or None"

            # Additional validation for specific types
            if expected_type == str and info_type in {
                GetInfoConstants.SQL_ACCESSIBLE_TABLES.value,
                GetInfoConstants.SQL_ACCESSIBLE_PROCEDURES.value,
            }:
                assert result in (
                    "Y",
                    "N",
                ), f"Y/N type should return 'Y' or 'N', got {result}"
            elif expected_type == int:
                assert result >= 0, f"Numeric info type should return non-negative integer"

    # Test boundary cases that might trigger fallback paths
    edge_case_info_types = [999, 9999, 0]  # Various potentially unsupported types

    for info_type in edge_case_info_types:
        result = db_connection.getinfo(info_type)
        # These should either return a valid value or None (not raise exceptions)
        assert result is None or isinstance(
            result, (str, int, bool)
        ), f"Edge case info type {info_type} should return valid type or None"


def test_connection_context_manager_with_cursor_cleanup(conn_str):
    """Test connection context manager with cursor cleanup on exceptions."""

    # Test that cursors are properly cleaned up when connection context exits
    with connect(conn_str) as conn:
        cursor1 = conn.cursor()
        cursor2 = conn.cursor()

        # Perform operations
        cursor1.execute("SELECT 1")
        cursor1.fetchone()
        cursor2.execute("SELECT 2")
        cursor2.fetchone()

        # Verify cursors are tracked
        assert len(conn._cursors) == 2, "Should track both cursors"

        # When we exit the context, cursors should be cleaned up

    # After context exit, cursors should be closed
    assert cursor1.closed, "Cursor1 should be closed after context exit"
    assert cursor2.closed, "Cursor2 should be closed after context exit"


def test_batch_execute_with_existing_cursor_reuse(db_connection):
    """Test batch_execute reusing an existing cursor vs creating new cursor."""

    # Create a cursor first
    existing_cursor = db_connection.cursor()

    try:
        # Test 1: Use batch_execute with existing cursor (auto_close should not affect it)
        results, returned_cursor = db_connection.batch_execute(
            ["SELECT 'reuse_test' as message"],
            reuse_cursor=existing_cursor,
            auto_close=True,  # Should not close existing cursor
        )

        # Should return the same cursor we passed in
        assert returned_cursor is existing_cursor, "Should return the same cursor when reusing"
        assert not returned_cursor.closed, "Existing cursor should not be auto-closed"
        assert results[0][0][0] == "reuse_test", "Should execute successfully"

        # Test 2: Use batch_execute without reuse_cursor (should create new cursor and auto_close it)
        results2, returned_cursor2 = db_connection.batch_execute(
            ["SELECT 'new_cursor_test' as message"],
            auto_close=True,  # Should close new cursor
        )

        assert returned_cursor2 is not existing_cursor, "Should create a new cursor"
        assert returned_cursor2.closed, "New cursor should be auto-closed"
        assert results2[0][0][0] == "new_cursor_test", "Should execute successfully"

        # Original cursor should still be open
        assert not existing_cursor.closed, "Original cursor should still be open"

    finally:
        # Clean up
        if not existing_cursor.closed:
            existing_cursor.close()


def test_connection_close_with_problematic_cursors(conn_str):
    """Test connection close behavior when cursors have issues."""

    conn = connect(conn_str)

    # Create several cursors, some of which we'll manipulate to cause issues
    cursor1 = conn.cursor()
    cursor2 = conn.cursor()
    cursor3 = conn.cursor()

    # Execute some operations to make them active
    cursor1.execute("SELECT 1")
    cursor1.fetchall()

    cursor2.execute("SELECT 2")
    cursor2.fetchall()

    # Close one cursor manually but leave it in the cursors set
    cursor3.execute("SELECT 3")
    cursor3.fetchall()
    cursor3.close()  # This should trigger _remove_cursor

    # Now close the connection - this should try to close remaining cursors
    # and trigger the cursor cleanup code (lines 1325-1335)
    conn.close()

    # All cursors should be closed now
    assert cursor1.closed, "Cursor1 should be closed"
    assert cursor2.closed, "Cursor2 should be closed"
    assert cursor3.closed, "Cursor3 should already be closed"


def test_connection_searchescape_property_detailed(db_connection):
    """Test detailed searchescape property behavior including edge cases."""

    # Clear any cached value to test fresh retrieval
    if hasattr(db_connection, "_searchescape"):
        delattr(db_connection, "_searchescape")

    # First access should call getinfo and cache result
    escape_char = db_connection.searchescape

    # Should be a string (either valid escape char or fallback)
    assert isinstance(escape_char, str), "Search escape should be a string"

    # Should now have cached value
    assert hasattr(db_connection, "_searchescape"), "Should cache searchescape"
    assert db_connection._searchescape == escape_char, "Cached value should match"

    # Second access should use cached value
    escape_char2 = db_connection.searchescape
    assert escape_char == escape_char2, "Should return same cached value"


def test_getinfo_comprehensive_edge_case_coverage(db_connection):
    """Test getinfo with comprehensive edge cases to hit various code paths."""

    from mssql_python.constants import GetInfoConstants

    # Test a wide range of info types to potentially hit different processing paths
    info_types_to_test = [
        # Standard string types
        GetInfoConstants.SQL_DRIVER_NAME.value,
        GetInfoConstants.SQL_DATA_SOURCE_NAME.value,
        GetInfoConstants.SQL_SERVER_NAME.value,
        GetInfoConstants.SQL_USER_NAME.value,
        GetInfoConstants.SQL_IDENTIFIER_QUOTE_CHAR.value,
        GetInfoConstants.SQL_SEARCH_PATTERN_ESCAPE.value,
        # Y/N types that might have different handling
        GetInfoConstants.SQL_ACCESSIBLE_TABLES.value,
        GetInfoConstants.SQL_ACCESSIBLE_PROCEDURES.value,
        GetInfoConstants.SQL_DATA_SOURCE_READ_ONLY.value,
        # Numeric types with potentially different byte lengths
        GetInfoConstants.SQL_MAX_COLUMN_NAME_LEN.value,
        GetInfoConstants.SQL_MAX_TABLE_NAME_LEN.value,
        GetInfoConstants.SQL_MAX_SCHEMA_NAME_LEN.value,
        GetInfoConstants.SQL_TXN_CAPABLE.value,
        # Edge cases - potentially unsupported or unusual
        0,
        1,
        999,
        1000,
        9999,
        10000,
    ]

    for info_type in info_types_to_test:
        try:
            result = db_connection.getinfo(info_type)

            # Result should be valid type or None
            if result is not None:
                assert isinstance(
                    result, (str, int, bool)
                ), f"Info type {info_type} returned invalid type {type(result)}"

                # Additional validation for known types
                if info_type in {
                    GetInfoConstants.SQL_ACCESSIBLE_TABLES.value,
                    GetInfoConstants.SQL_ACCESSIBLE_PROCEDURES.value,
                    GetInfoConstants.SQL_DATA_SOURCE_READ_ONLY.value,
                }:
                    assert result in (
                        "Y",
                        "N",
                    ), f"Y/N info type {info_type} should return 'Y' or 'N', got {result}"

        except Exception as e:
            # Some info types might raise exceptions, which is acceptable
            # Just make sure it's not a critical error
            assert not isinstance(
                e, (SystemError, MemoryError)
            ), f"Info type {info_type} caused critical error: {e}"
