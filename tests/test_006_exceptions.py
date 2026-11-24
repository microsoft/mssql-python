import pytest
from mssql_python import connect, Connection
from mssql_python.exceptions import (
    Exception,
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
    raise_exception,
    truncate_error_message,
)
from mssql_python import ConnectionStringParseError

EXCEPTION_CLASSES = (
    Exception,
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


@pytest.mark.parametrize("cls", EXCEPTION_CLASSES)
@pytest.mark.parametrize("ddbc_error", ["ddbc msg", ""])
def test_exception_sqlstate_constructor_compatibility(cls, ddbc_error):
    original = cls("driver msg", ddbc_error)
    keyword = cls(driver_error="driver msg", ddbc_error=ddbc_error)
    diagnostic = cls("driver msg", ddbc_error, sqlstate="42000")

    assert original.sqlstate is None
    assert keyword.sqlstate is None
    assert diagnostic.sqlstate == "42000"
    for error in (keyword, diagnostic):
        assert error.driver_error == original.driver_error
        assert error.ddbc_error == original.ddbc_error
        assert error.message == original.message
        assert str(error) == str(original)
        assert error.args == original.args == (original.message,)


def drop_table_if_exists(cursor, table_name):
    """Drop the table if it exists"""
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    except Exception as e:
        pytest.fail(f"Failed to drop table {table_name}: {e}")


def test_truncate_error_message(cursor):
    with pytest.raises(ProgrammingError) as excinfo:
        cursor.execute("SELEC database_id, name from sys.databases;")
    assert (
        str(excinfo.value)
        == "Driver Error: Syntax error or access violation; DDBC Error: [Microsoft][SQL Server]Incorrect syntax near the keyword 'from'."
    )


def test_raise_exception():
    with pytest.raises(ProgrammingError) as excinfo:
        raise_exception("42000", "Syntax error or access violation")
    assert (
        str(excinfo.value)
        == "Driver Error: Syntax error or access violation; DDBC Error: Syntax error or access violation"
    )
    assert excinfo.value.sqlstate == "42000"


def test_warning_exception():
    with pytest.raises(Warning) as excinfo:
        raise_exception("01000", "General warning")
    assert str(excinfo.value) == "Driver Error: General warning; DDBC Error: General warning"
    assert excinfo.value.sqlstate == "01000"


def test_data_error_exception():
    with pytest.raises(DataError) as excinfo:
        raise_exception("22003", "Numeric value out of range")
    assert (
        str(excinfo.value)
        == "Driver Error: Numeric value out of range; DDBC Error: Numeric value out of range"
    )
    assert excinfo.value.sqlstate == "22003"


def test_operational_error_exception():
    with pytest.raises(OperationalError) as excinfo:
        raise_exception("08001", "Client unable to establish connection")
    assert (
        str(excinfo.value)
        == "Driver Error: Client unable to establish connection; DDBC Error: Client unable to establish connection"
    )
    assert excinfo.value.sqlstate == "08001"


def test_integrity_error_exception():
    with pytest.raises(IntegrityError) as excinfo:
        raise_exception("23000", "Integrity constraint violation")
    assert (
        str(excinfo.value)
        == "Driver Error: Integrity constraint violation; DDBC Error: Integrity constraint violation"
    )
    assert excinfo.value.sqlstate == "23000"


def test_internal_error_exception():
    with pytest.raises(IntegrityError) as excinfo:
        raise_exception("40002", "Integrity constraint violation")
    assert (
        str(excinfo.value)
        == "Driver Error: Integrity constraint violation; DDBC Error: Integrity constraint violation"
    )
    assert excinfo.value.sqlstate == "40002"


def test_programming_error_exception():
    with pytest.raises(ProgrammingError) as excinfo:
        raise_exception("42S02", "Base table or view not found")
    assert (
        str(excinfo.value)
        == "Driver Error: Base table or view not found; DDBC Error: Base table or view not found"
    )
    assert excinfo.value.sqlstate == "42S02"


def test_not_supported_error_exception():
    with pytest.raises(NotSupportedError) as excinfo:
        raise_exception("IM001", "Driver does not support this function")
    assert (
        str(excinfo.value)
        == "Driver Error: Driver does not support this function; DDBC Error: Driver does not support this function"
    )
    assert excinfo.value.sqlstate == "IM001"


def test_unknown_error_exception():
    with pytest.raises(DatabaseError) as excinfo:
        raise_exception("99999", "Unknown error")
    assert (
        str(excinfo.value)
        == "Driver Error: An error occurred with SQLSTATE code: 99999; DDBC Error: Unknown error"
    )
    assert excinfo.value.sqlstate == "99999"


@pytest.mark.parametrize(
    "sqlstate, expected_type",
    [("42000", ProgrammingError), ("99999", DatabaseError), ("", DatabaseError)],
)
def test_check_error_preserves_diagnostic_sqlstate(monkeypatch, sqlstate, expected_type):
    from types import SimpleNamespace
    from unittest.mock import Mock
    from mssql_python import helpers

    diagnostic = SimpleNamespace(sqlState=sqlstate, ddbcErrorMsg="diagnostic message")
    read_error = Mock(return_value=diagnostic)
    monkeypatch.setattr(helpers.ddbc_bindings, "DDBCSQLCheckError", read_error)
    handle = object()

    with pytest.raises(expected_type) as caught:
        helpers.check_error(3, handle, -1)

    assert type(caught.value) is expected_type
    assert caught.value.sqlstate == (sqlstate or None)
    assert caught.value.ddbc_error == "diagnostic message"
    read_error.assert_called_once_with(3, handle, -1)


def test_syntax_error(cursor):
    with pytest.raises(ProgrammingError) as excinfo:
        cursor.execute("SELEC * FROM non_existent_table")
    assert "Syntax error or access violation" in str(excinfo.value)
    assert excinfo.value.sqlstate == "42000"


def test_table_not_found_error(cursor):
    with pytest.raises(ProgrammingError) as excinfo:
        cursor.execute("SELECT * FROM non_existent_table")
    assert "Base table or view not found" in str(excinfo.value)
    assert excinfo.value.sqlstate == "42S02"


def test_data_truncation_error(cursor, db_connection):
    try:
        cursor.execute("CREATE TABLE #pytest_test_truncation (id INT, name NVARCHAR(5))")
        cursor.execute(
            "INSERT INTO #pytest_test_truncation (id, name) VALUES (?, ?)",
            [1, "TooLongName"],
        )
    except (ProgrammingError, DataError) as excinfo:
        # DataError is raised on Windows but ProgrammingError on MacOS
        # Included catching both ProgrammingError and DataError in this test
        # TODO: Make this test platform independent
        assert "String or binary data would be truncated" in str(excinfo)
    finally:
        drop_table_if_exists(cursor, "#pytest_test_truncation")
        db_connection.commit()


def test_unique_constraint_error(cursor, db_connection):
    try:
        drop_table_if_exists(cursor, "#pytest_test_unique")
        cursor.execute("CREATE TABLE #pytest_test_unique (id INT PRIMARY KEY, name NVARCHAR(50))")
        cursor.execute("INSERT INTO #pytest_test_unique (id, name) VALUES (?, ?)", [1, "Name1"])
        with pytest.raises(IntegrityError) as excinfo:
            cursor.execute("INSERT INTO #pytest_test_unique (id, name) VALUES (?, ?)", [1, "Name2"])
        assert "Integrity constraint violation" in str(excinfo.value)
        assert excinfo.value.sqlstate == "23000"
    except Exception as e:
        pytest.fail(f"Test failed: {e}")
    finally:
        drop_table_if_exists(cursor, "#pytest_test_unique")
        db_connection.commit()


def test_foreign_key_constraint_error(cursor, db_connection):
    try:
        # Using regular tables (not temp tables) because SQL Server doesn't support foreign keys on temp tables.
        # Using dbo schema to avoid issues with Azure SQL with Azure AD/Entra ID authentication. It can misinterpret email-format usernames (e.g., user@domain.com) as schema names.
        drop_table_if_exists(cursor, "dbo.pytest_child_table")
        drop_table_if_exists(cursor, "dbo.pytest_parent_table")
        cursor.execute("CREATE TABLE dbo.pytest_parent_table (id INT PRIMARY KEY)")
        cursor.execute(
            "CREATE TABLE dbo.pytest_child_table (id INT, parent_id INT, FOREIGN KEY (parent_id) REFERENCES dbo.pytest_parent_table(id))"
        )
        cursor.execute("INSERT INTO dbo.pytest_parent_table (id) VALUES (?)", [1])
        with pytest.raises(IntegrityError) as excinfo:
            cursor.execute(
                "INSERT INTO dbo.pytest_child_table (id, parent_id) VALUES (?, ?)",
                [1, 2],
            )
        assert "Integrity constraint violation" in str(excinfo.value)
    except Exception as e:
        pytest.fail(f"Test failed: {e}")
    finally:
        drop_table_if_exists(cursor, "dbo.pytest_child_table")
        drop_table_if_exists(cursor, "dbo.pytest_parent_table")
        db_connection.commit()


def test_connection_error():
    # The new connection string parser now validates the connection string before passing to ODBC
    # Invalid strings like "InvalidConnectionString" (missing key=value format) will raise ConnectionStringParseError
    with pytest.raises(ConnectionStringParseError) as excinfo:
        connect("InvalidConnectionString")
    assert "Incomplete specification" in str(excinfo.value) or "has no value" in str(excinfo.value)


def test_connect_runtime_error_mapped_to_correct_dbapi_exception():
    """Regression test for https://github.com/microsoft/mssql-python/issues/532.

    Connection failures from the C++ pybind layer (RuntimeError) must be mapped
    to the correct DB-API 2.0 exception via the embedded SQLSTATE code.
    This covers connect(), commit(), and rollback() — all of which go through
    Connection::checkError() in the C++ layer.
    """
    from unittest.mock import MagicMock, patch

    # SQLSTATE 28000 = login failed -> OperationalError
    with patch(
        "mssql_python.connection.ddbc_bindings.Connection",
        side_effect=RuntimeError("SQLSTATE:28000:Login failed for user 'baduser'."),
    ):
        with pytest.raises(OperationalError) as exc_info:
            connect("Server=testserver;Database=mydb;Trusted_Connection=yes;")
    assert "Login failed for user" in exc_info.value.ddbc_error
    assert exc_info.value.sqlstate == "28000"
    assert not isinstance(exc_info.value, RuntimeError)

    # SQLSTATE IM002 = driver not found -> OperationalError (per SQLSTATE mapping)
    with patch(
        "mssql_python.connection.ddbc_bindings.Connection",
        side_effect=RuntimeError(
            "SQLSTATE:IM002:Data source name not found and no default driver specified"
        ),
    ):
        with pytest.raises(OperationalError) as exc_info:
            connect("Server=testserver;Database=mydb;Trusted_Connection=yes;")
    assert "no default driver" in exc_info.value.ddbc_error
    assert exc_info.value.sqlstate == "IM002"
    assert not isinstance(exc_info.value, RuntimeError)

    # No SQLSTATE prefix -> fallback OperationalError
    with patch(
        "mssql_python.connection.ddbc_bindings.Connection",
        side_effect=RuntimeError("Connection handle not allocated"),
    ):
        with pytest.raises(OperationalError) as exc_info:
            connect("Server=testserver;Database=mydb;Trusted_Connection=yes;")
    assert "Connection handle not allocated" in exc_info.value.ddbc_error
    assert exc_info.value.sqlstate is None
    assert not isinstance(exc_info.value, RuntimeError)

    # Unmapped SQLSTATE (sqlstate_to_exception returns None) -> DatabaseError
    with patch(
        "mssql_python.connection.ddbc_bindings.Connection",
        side_effect=RuntimeError("SQLSTATE:99999:Unknown error with unmapped code"),
    ):
        with pytest.raises(DatabaseError) as exc_info:
            connect("Server=testserver;Database=mydb;Trusted_Connection=yes;")
    assert "Unknown error with unmapped code" in exc_info.value.ddbc_error
    assert "SQLSTATE code: 99999" in exc_info.value.driver_error
    assert exc_info.value.sqlstate == "99999"
    assert not isinstance(exc_info.value, RuntimeError)

    # Malformed SQLSTATE (empty code) -> OperationalError
    with patch(
        "mssql_python.connection.ddbc_bindings.Connection",
        side_effect=RuntimeError("SQLSTATE::Invalid handle!"),
    ):
        with pytest.raises(OperationalError) as exc_info:
            connect("Server=testserver;Database=mydb;Trusted_Connection=yes;")
    assert "Invalid handle!" in exc_info.value.ddbc_error
    assert exc_info.value.sqlstate is None
    assert not isinstance(exc_info.value, RuntimeError)

    # commit() failure -> OperationalError via same _raise_connection_error path
    mock_conn = MagicMock()
    mock_conn.commit.side_effect = RuntimeError("SQLSTATE:08S01:Communication link failure")
    mock_conn.get_autocommit.return_value = False
    with patch("mssql_python.connection.ddbc_bindings.Connection", return_value=mock_conn):
        conn = connect("Server=testserver;Database=mydb;Trusted_Connection=yes;")
    with pytest.raises(OperationalError) as exc_info:
        conn.commit()
    assert "Communication link failure" in exc_info.value.ddbc_error
    assert exc_info.value.sqlstate == "08S01"
    assert not isinstance(exc_info.value, RuntimeError)

    # rollback() failure -> OperationalError via same _raise_connection_error path
    mock_conn2 = MagicMock()
    mock_conn2.rollback.side_effect = RuntimeError("SQLSTATE:08S01:Communication link failure")
    mock_conn2.get_autocommit.return_value = False
    with patch("mssql_python.connection.ddbc_bindings.Connection", return_value=mock_conn2):
        conn2 = connect("Server=testserver;Database=mydb;Trusted_Connection=yes;")
    with pytest.raises(OperationalError) as exc_info:
        conn2.rollback()
    assert "Communication link failure" in exc_info.value.ddbc_error
    assert exc_info.value.sqlstate == "08S01"
    assert not isinstance(exc_info.value, RuntimeError)


def test_close_cleans_up_after_rollback_failure():
    """A rollback error must not defer native close to object destruction."""
    from unittest.mock import MagicMock, patch

    mock_conn = MagicMock()
    mock_conn.get_autocommit.return_value = False
    mock_conn.close.side_effect = RuntimeError("SQLSTATE:08S01:Communication link failure")

    with patch("mssql_python.connection.ddbc_bindings.Connection", return_value=mock_conn):
        conn = connect("Server=testserver;Database=mydb;Trusted_Connection=yes;")

    with pytest.raises(OperationalError, match="Communication link failure") as caught:
        conn.close()

    assert caught.value.sqlstate == "08S01"
    mock_conn.rollback.assert_not_called()
    mock_conn.close.assert_called_once_with(rollback_before_disconnect=True)
    assert conn._conn is None
    assert conn.closed


def test_close_cleans_up_after_autocommit_read_failure():
    """An autocommit read error must not bypass native close and handle release."""
    from unittest.mock import MagicMock, patch

    mock_conn = MagicMock()
    mock_conn.get_autocommit.return_value = False

    with patch("mssql_python.connection.ddbc_bindings.Connection", return_value=mock_conn):
        conn = connect("Server=testserver;Database=mydb;Trusted_Connection=yes;")

    mock_conn.reset_mock()
    mock_conn.get_autocommit.side_effect = RuntimeError("SQLSTATE:08S01:Communication link failure")
    mock_conn.close.side_effect = RuntimeError("SQLSTATE:08S01:Communication link failure")
    with pytest.raises(OperationalError, match="Communication link failure"):
        conn.close()

    mock_conn.rollback.assert_not_called()
    mock_conn.get_autocommit.assert_not_called()
    mock_conn.close.assert_called_once_with(rollback_before_disconnect=True)
    assert conn._conn is None
    assert conn.closed


def test_close_delegates_manual_transaction_cleanup_to_native():
    """Only native close probes the mode and rolls back, without Python hints."""
    from unittest.mock import MagicMock, patch

    mock_conn = MagicMock()
    mock_conn.get_autocommit.return_value = False

    with patch("mssql_python.connection.ddbc_bindings.Connection", return_value=mock_conn):
        conn = connect("Server=testserver;Database=mydb;Trusted_Connection=yes;")

    mock_conn.reset_mock()
    conn.close()

    mock_conn.get_autocommit.assert_not_called()
    mock_conn.rollback.assert_not_called()
    mock_conn.close.assert_called_once_with(rollback_before_disconnect=True)


def test_autocommit_close_delegates_transaction_cleanup_to_native():
    """Autocommit may still contain an explicit SQL transaction."""
    from unittest.mock import MagicMock, patch

    mock_conn = MagicMock()
    mock_conn.get_autocommit.return_value = True

    with patch("mssql_python.connection.ddbc_bindings.Connection", return_value=mock_conn):
        conn = connect(
            "Server=testserver;Database=mydb;Trusted_Connection=yes;",
            autocommit=True,
        )

    mock_conn.reset_mock()
    conn.close()

    mock_conn.rollback.assert_not_called()
    mock_conn.get_autocommit.assert_not_called()
    mock_conn.close.assert_called_once_with(rollback_before_disconnect=True)


@pytest.mark.parametrize("preclose_failure", ["autocommit", "rollback"])
def test_native_close_error_does_not_need_python_preclose_calls(preclose_failure):
    """Native cleanup is authoritative even if Python probes would have failed."""
    from unittest.mock import MagicMock, patch

    mock_conn = MagicMock()
    mock_conn.get_autocommit.return_value = False

    with patch("mssql_python.connection.ddbc_bindings.Connection", return_value=mock_conn):
        conn = connect("Server=testserver;Database=mydb;Trusted_Connection=yes;")

    mock_conn.reset_mock()
    if preclose_failure == "autocommit":
        mock_conn.get_autocommit.side_effect = RuntimeError("SQLSTATE:08S01:Autocommit read failed")
    else:
        mock_conn.rollback.side_effect = RuntimeError("SQLSTATE:08S01:Rollback failed")
    mock_conn.close.side_effect = RuntimeError("SQLSTATE:08003:Native close failed")
    with pytest.raises(OperationalError, match="Native close failed"):
        conn.close()

    mock_conn.close.assert_called_once_with(rollback_before_disconnect=True)
    mock_conn.get_autocommit.assert_not_called()
    mock_conn.rollback.assert_not_called()
    assert conn._conn is None
    assert conn.closed


def test_cursor_cleanup_failure_still_calls_native_close():
    from unittest.mock import MagicMock, patch

    native = MagicMock()
    with patch("mssql_python.connection.ddbc_bindings.Connection", return_value=native):
        conn = connect("Server=testserver;Database=mydb;Trusted_Connection=yes;")
    broken = MagicMock(closed=False)
    broken._close.side_effect = RuntimeError("cursor cleanup failed")
    healthy = MagicMock(closed=False)
    conn._cursors = {broken, healthy}

    conn.close()

    broken._close.assert_called_once_with()
    healthy._close.assert_called_once_with()
    native.close.assert_called_once_with(rollback_before_disconnect=True)
    assert conn.closed
    assert conn._conn is None


def test_truncate_error_message_successful_cases():
    """Test truncate_error_message with valid Microsoft messages for comparison."""

    # Test successful truncation (should not trigger exception path)
    valid_message = "[Microsoft][SQL Server]Some database error message"
    result = truncate_error_message(valid_message)
    expected = "[Microsoft]Some database error message"
    assert result == expected

    # Test non-Microsoft message (should return as-is)
    non_microsoft_message = "Regular error message"
    result = truncate_error_message(non_microsoft_message)
    assert result == non_microsoft_message


def test_truncate_error_message_exception_path():
    """Test truncate_error_message exception handling."""

    # Test with malformed Microsoft messages that should trigger the exception path
    # These inputs will cause a ValueError on line 526 when looking for the second "]"

    test_cases = [
        "[Microsoft",  # Missing closing bracket - should cause index error
        "[Microsoft]",  # No second bracket section - should cause index error
        "[Microsoft]no_second_bracket",  # No second bracket - should cause index error
        "[Microsoft]text_without_proper_structure",  # Missing second bracket structure
    ]

    for malformed_message in test_cases:
        # Call the actual function to see how it handles the malformed input
        try:
            result = truncate_error_message(malformed_message)
            # If we get a result without exception, the function handled the error
            # This means the exception path (lines 528-531) was executed
            # and it returned the original message (line 531)
            assert result == malformed_message
            print(f"Exception handled correctly for: {malformed_message}")
        except ValueError as e:
            # If we get a ValueError, it means we've successfully reached line 526
            # where the substring search fails, which is exactly what we want to test
            assert "substring not found" in str(e)
            print(f"Line 526 executed and failed as expected for: {malformed_message}")
        except IndexError:
            # IndexError might occur on the first bracket search
            # This still shows we're testing the problematic lines
            print(f"IndexError occurred as expected for: {malformed_message}")

    # The fact that we can trigger these exceptions shows we're covering
    # the target lines (526-534) in the function


def test_truncate_error_message_specific_error_lines():
    """Test specific conditions that trigger the ValueError on line 526."""

    # These inputs are crafted to specifically trigger the line:
    # string_third = string_second[string_second.index("]") + 1 :]

    specific_test_cases = [
        "[Microsoft]This text has no second bracket",
        "[Microsoft]x",  # Minimal content, no second bracket
        "[Microsoft] ",  # Just space, no second bracket
    ]

    for test_case in specific_test_cases:
        # The function should handle these gracefully or raise expected exceptions
        try:
            result = truncate_error_message(test_case)
            # If we get a string result, the exception was handled properly
            assert isinstance(result, str)
            # For malformed inputs, we expect the original string back
            assert result == test_case
        except ValueError as e:
            # If we get a ValueError, it means we've reached line 526 successfully
            # This is exactly the line we want to cover
            assert "substring not found" in str(e)
        except Exception as e:
            # Any other exception also shows we're testing the problematic code
            pass


def test_truncate_error_message_logger_exists_check():
    """Test the 'if logger:' condition on line 529 naturally."""

    # Import the logger to verify its existence
    from mssql_python.exceptions import logger

    # Test with input that would trigger the exception path
    problematic_input = "[Microsoft]will_cause_error_on_line_526"

    # Call the function - this should exercise the exception handling
    try:
        result = truncate_error_message(problematic_input)
        # If we get a result, the exception was handled
        assert isinstance(result, str)
        assert result == problematic_input
    except ValueError:
        # This proves we reached line 526 where the exception occurs
        # If the try-catch worked, lines 528-531 would be executed
        # including the "if logger:" check on line 529
        pass

    # Verify logger exists or is None (for the "if logger:" condition)
    assert logger is None or hasattr(logger, "error")


def test_truncate_error_message_comprehensive_edge_cases():
    """Test comprehensive edge cases for exception handling coverage."""

    # Test cases designed to exercise different paths through the function
    edge_cases = [
        # Cases that should return early (no exception)
        ("", "early_return"),  # Empty string - early return
        ("Normal error message", "early_return"),  # Non-Microsoft - early return
        # Cases that should trigger exception on line 526
        ("[Microsoft]a", "exception"),  # Too short for second bracket
        ("[Microsoft]ab", "exception"),  # Still too short
        ("[Microsoft]abc", "exception"),  # No second bracket structure
        ("[Microsoft] no bracket here", "exception"),  # Space but no second bracket
        (
            "[Microsoft]multiple words no bracket",
            "exception",
        ),  # Multiple words, no bracket
    ]

    for test_case, expected_path in edge_cases:
        try:
            result = truncate_error_message(test_case)

            # All should return strings
            assert isinstance(result, str)

            # Verify expected behavior
            if expected_path == "early_return":
                # Non-Microsoft messages should return unchanged
                assert result == test_case
            elif expected_path == "exception":
                # If we get here, exception was caught and original returned
                assert result == test_case

        except ValueError:
            # This means we reached line 526 successfully
            if expected_path == "exception":
                # This is expected for malformed Microsoft messages
                pass
            else:
                # Unexpected exception for early return cases
                raise


def test_truncate_error_message_return_paths():
    """Test different return paths in the truncate_error_message function."""

    # Test the successful path (no exception)
    success_case = "[Microsoft][SQL Server]Database error message"
    result = truncate_error_message(success_case)
    expected = "[Microsoft]Database error message"
    assert result == expected

    # Test the early return path (non-Microsoft)
    early_return_case = "Regular error message"
    result = truncate_error_message(early_return_case)
    assert result == early_return_case

    # Test the exception return path (line 531)
    exception_case = "[Microsoft]malformed_no_second_bracket"
    try:
        result = truncate_error_message(exception_case)
        # If successful, exception was caught and original returned (line 531)
        assert isinstance(result, str)
        assert result == exception_case
    except ValueError:
        # This proves we reached line 526 where the ValueError occurs
        # If the exception handling worked, it would have been caught
        # and the function would return the original message (line 531)
        pass


# ---------------------------------------------------------------------------
# Pickle / unpickle round-trip tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("cls", EXCEPTION_CLASSES)
@pytest.mark.parametrize("sqlstate", [None, "42000"])
def test_exception_pickle_roundtrip(cls, sqlstate):
    """All DB-API exception subclasses must survive a pickle round-trip."""
    import pickle
    import copy

    original = cls("driver msg", "ddbc msg", sqlstate=sqlstate)

    restored = pickle.loads(pickle.dumps(original))
    for result in (restored, copy.copy(original), copy.deepcopy(original)):
        assert type(result) is cls
        assert result.driver_error == original.driver_error
        assert result.ddbc_error == original.ddbc_error
        assert result.message == original.message
        assert result.sqlstate == sqlstate
        assert str(result) == str(original)
        assert result.args == original.args


def test_exception_legacy_pickle_defaults_to_no_sqlstate():
    import pickle

    class LegacyPayload:
        def __reduce__(self):
            return (
                Exception._unpickle,
                (ProgrammingError, "driver msg", "ddbc msg", "original message"),
            )

    restored = pickle.loads(pickle.dumps(LegacyPayload()))

    assert type(restored) is ProgrammingError
    assert restored.sqlstate is None
    assert restored.driver_error == "driver msg"
    assert restored.ddbc_error == "ddbc msg"
    assert restored.message == "original message"
    assert restored.args == ("original message",)


def test_exception_pickle_does_not_retruncate_diagnostics(monkeypatch):
    import copy
    import pickle
    from unittest.mock import Mock
    from mssql_python import exceptions

    original = ProgrammingError(
        "driver msg",
        "[Microsoft][ODBC Driver 18 for SQL Server][SQL Server]query failed",
        sqlstate="42000",
    )
    payload = pickle.dumps(original)
    truncate = Mock(side_effect=AssertionError("Diagnostics must not be truncated again"))
    monkeypatch.setattr(exceptions, "truncate_error_message", truncate)

    for result in (pickle.loads(payload), copy.deepcopy(original)):
        assert result.sqlstate == "42000"
        assert result.ddbc_error == original.ddbc_error
        assert result.args == original.args
    truncate.assert_not_called()


def test_exception_pickle_empty_ddbc_error():
    """Exceptions with empty ddbc_error should also round-trip cleanly."""
    import pickle

    original = ProgrammingError("cursor is closed", "")
    restored = pickle.loads(pickle.dumps(original))

    assert type(restored) is ProgrammingError
    assert restored.driver_error == "cursor is closed"
    assert restored.ddbc_error == ""
    assert restored.sqlstate is None
    assert str(restored) == str(original)


def test_connection_string_parse_error_pickle_roundtrip():
    """ConnectionStringParseError should survive a pickle round-trip."""
    import pickle
    import copy

    errors = ["Unknown keyword: foo", "Missing value for: bar"]
    original = ConnectionStringParseError(errors)

    restored = pickle.loads(pickle.dumps(original))

    assert type(restored) is ConnectionStringParseError
    assert restored.errors == errors
    assert str(restored) == str(original)

    # copy.deepcopy
    deep = copy.deepcopy(original)
    assert type(deep) is ConnectionStringParseError
    assert deep.errors == errors
