import pytest
from mssql_python import connect
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
    truncate_error_message
)

def drop_table_if_exists(cursor, table_name):
    """Drop the table if it exists"""
    try:
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    except Exception as e:
        pytest.fail(f"Failed to drop table {table_name}: {e}")

def test_truncate_error_message(cursor):
    with pytest.raises(ProgrammingError) as excinfo:
        cursor.execute("SELEC database_id, name from sys.databases;")
    assert str(excinfo.value) == "Driver Error: Syntax error or access violation; DDBC Error: [Microsoft][SQL Server]Incorrect syntax near the keyword 'from'."

def test_raise_exception():
    with pytest.raises(ProgrammingError) as excinfo:
        raise_exception('42000', 'Syntax error or access violation')
    assert str(excinfo.value) == "Driver Error: Syntax error or access violation; DDBC Error: Syntax error or access violation"

def test_warning_exception():
    with pytest.raises(Warning) as excinfo:
        raise_exception('01000', 'General warning')
    assert str(excinfo.value) == "Driver Error: General warning; DDBC Error: General warning"

def test_data_error_exception():
    with pytest.raises(DataError) as excinfo:
        raise_exception('22003', 'Numeric value out of range')
    assert str(excinfo.value) == "Driver Error: Numeric value out of range; DDBC Error: Numeric value out of range"

def test_operational_error_exception():
    with pytest.raises(OperationalError) as excinfo:
        raise_exception('08001', 'Client unable to establish connection')
    assert str(excinfo.value) == "Driver Error: Client unable to establish connection; DDBC Error: Client unable to establish connection"

def test_integrity_error_exception():
    with pytest.raises(IntegrityError) as excinfo:
        raise_exception('23000', 'Integrity constraint violation')
    assert str(excinfo.value) == "Driver Error: Integrity constraint violation; DDBC Error: Integrity constraint violation"

def test_internal_error_exception():
    with pytest.raises(IntegrityError) as excinfo:
        raise_exception('40002', 'Integrity constraint violation')
    assert str(excinfo.value) == "Driver Error: Integrity constraint violation; DDBC Error: Integrity constraint violation"

def test_programming_error_exception():
    with pytest.raises(ProgrammingError) as excinfo:
        raise_exception('42S02', 'Base table or view not found')
    assert str(excinfo.value) == "Driver Error: Base table or view not found; DDBC Error: Base table or view not found"

def test_not_supported_error_exception():
    with pytest.raises(NotSupportedError) as excinfo:
        raise_exception('IM001', 'Driver does not support this function')
    assert str(excinfo.value) == "Driver Error: Driver does not support this function; DDBC Error: Driver does not support this function"

def test_unknown_error_exception():
    with pytest.raises(DatabaseError) as excinfo:
        raise_exception('99999', 'Unknown error')
    assert str(excinfo.value) == "Driver Error: An error occurred with SQLSTATE code: 99999; DDBC Error: Unknown error"

def test_syntax_error(cursor):
    with pytest.raises(ProgrammingError) as excinfo:
        cursor.execute("SELEC * FROM non_existent_table")
    assert "Syntax error or access violation" in str(excinfo.value)

def test_table_not_found_error(cursor):
    with pytest.raises(ProgrammingError) as excinfo:
        cursor.execute("SELECT * FROM non_existent_table")
    assert "Base table or view not found" in str(excinfo.value)

def test_data_truncation_error(cursor):
    try:
        cursor.execute("CREATE TABLE pytest_test_truncation (id INT, name NVARCHAR(5))")
        cursor.execute("INSERT INTO pytest_test_truncation (id, name) VALUES (?, ?)", [1, 'TooLongName'])
    except ProgrammingError as excinfo:
        assert "String or binary data would be truncated" in str(excinfo)
    finally:
        drop_table_if_exists(cursor, "pytest_test_truncation")

def test_unique_constraint_error(cursor):
    try:
        drop_table_if_exists(cursor, "pytest_test_unique")
        cursor.execute("CREATE TABLE pytest_test_unique (id INT PRIMARY KEY, name NVARCHAR(50))")
        cursor.execute("INSERT INTO pytest_test_unique (id, name) VALUES (?, ?)", [1, 'Name1'])
        with pytest.raises(IntegrityError) as excinfo:
            cursor.execute("INSERT INTO pytest_test_unique (id, name) VALUES (?, ?)", [1, 'Name2'])
        assert "Integrity constraint violation" in str(excinfo.value)
    except Exception as e:
        pytest.fail(f"Test failed: {e}")
    finally:
        drop_table_if_exists(cursor, "pytest_test_unique")

def test_foreign_key_constraint_error(cursor):
    try:
        drop_table_if_exists(cursor, "pytest_child_table")
        drop_table_if_exists(cursor, "pytest_parent_table")
        cursor.execute("CREATE TABLE pytest_parent_table (id INT PRIMARY KEY)")
        cursor.execute("CREATE TABLE pytest_child_table (id INT, parent_id INT, FOREIGN KEY (parent_id) REFERENCES pytest_parent_table(id))")
        cursor.execute("INSERT INTO pytest_parent_table (id) VALUES (?)", [1])
        with pytest.raises(IntegrityError) as excinfo:
            cursor.execute("INSERT INTO pytest_child_table (id, parent_id) VALUES (?, ?)", [1, 2])
        assert "Integrity constraint violation" in str(excinfo.value)
    except Exception as e:
        pytest.fail(f"Test failed: {e}")
    finally:
        drop_table_if_exists(cursor, "pytest_child_table")
        drop_table_if_exists(cursor, "pytest_parent_table")

def test_connection_error(db_connection):
    with pytest.raises(OperationalError) as excinfo:
        connect("InvalidConnectionString")
    assert "Client unable to establish connection" in str(excinfo.value)
