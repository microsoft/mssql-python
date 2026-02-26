# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Basic integration tests for bulkcopy via the mssql_python driver."""

import pytest

# Skip the entire module when mssql_py_core can't be loaded at runtime
# (e.g. manylinux_2_28 build containers where glibc is too old for the .so).
mssql_py_core = pytest.importorskip(
    "mssql_py_core", reason="mssql_py_core not loadable (glibc too old?)"
)


def test_connection_and_cursor(cursor):
    """Test that connection and cursor work correctly."""
    cursor.execute("SELECT 1 AS connected")
    result = cursor.fetchone()
    assert result[0] == 1


def test_insert_and_fetch(cursor):
    """Test basic insert and fetch operations."""
    table_name = "mssql_python_test_basic"

    # Create table
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (id INT, name NVARCHAR(50))")
    cursor.connection.commit()

    # Insert data
    cursor.execute(f"INSERT INTO {table_name} (id, name) VALUES (?, ?)", (1, "Alice"))
    cursor.execute(f"INSERT INTO {table_name} (id, name) VALUES (?, ?)", (2, "Bob"))

    # Fetch and verify
    cursor.execute(f"SELECT id, name FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()

    assert len(rows) == 2
    assert rows[0][0] == 1 and rows[0][1] == "Alice"
    assert rows[1][0] == 2 and rows[1][1] == "Bob"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")


def test_bulkcopy_basic(cursor):
    """Test basic bulkcopy operation via mssql_python driver with auto-mapping.

    Uses automatic column mapping (columns mapped by ordinal position).
    """
    table_name = "mssql_python_bulkcopy_test"

    # Create table
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (id INT, name VARCHAR(50), value FLOAT)")
    cursor.connection.commit()

    # Prepare test data - columns match table order (id, name, value)
    data = [
        (1, "Alice", 100.5),
        (2, "Bob", 200.75),
        (3, "Charlie", 300.25),
    ]

    # Perform bulkcopy with auto-mapping (no column_mappings specified)
    # Using explicit timeout parameter instead of kwargs
    result = cursor.bulkcopy(table_name, data, timeout=60)

    # Verify result
    assert result is not None
    assert result["rows_copied"] == 3

    # Verify data was inserted correctly
    cursor.execute(f"SELECT id, name, value FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()

    assert len(rows) == 3
    assert rows[0][0] == 1 and rows[0][1] == "Alice" and abs(rows[0][2] - 100.5) < 0.01
    assert rows[1][0] == 2 and rows[1][1] == "Bob" and abs(rows[1][2] - 200.75) < 0.01
    assert rows[2][0] == 3 and rows[2][1] == "Charlie" and abs(rows[2][2] - 300.25) < 0.01

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")


def test_bulkcopy_without_database_parameter(conn_str):
    """Test bulkcopy operation works when DATABASE is not specified in connection string.

    The database keyword in connection string is optional. In its absence,
    the client sends an empty database name and the server responds with
    the default database the client was connected to.
    """
    from mssql_python import connect
    from mssql_python.connection_string_parser import _ConnectionStringParser
    from mssql_python.connection_string_builder import _ConnectionStringBuilder

    # Parse the connection string using the proper parser
    parser = _ConnectionStringParser(validate_keywords=False)
    params = parser._parse(conn_str)

    # Save the original database name to use it explicitly in our operations
    original_database = params.get("database")

    # Remove DATABASE parameter if present (case-insensitive, handles all synonyms)
    params.pop("database", None)

    # Rebuild the connection string using the builder to preserve braced values
    builder = _ConnectionStringBuilder(params)
    conn_str_no_db = builder.build()

    # Create connection without DATABASE parameter
    conn = connect(conn_str_no_db)
    try:
        cursor = conn.cursor()

        # Verify we're connected to a database (should be the default)
        cursor.execute("SELECT DB_NAME() AS current_db")
        current_db = cursor.fetchone()[0]
        assert current_db is not None, "Should be connected to a database"

        # If original database was specified, switch to it to ensure we have permissions
        if original_database:
            cursor.execute(f"USE [{original_database}]")

        # Create test table in the current database
        table_name = "mssql_python_bulkcopy_no_db_test"
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
        cursor.execute(f"CREATE TABLE {table_name} (id INT, name VARCHAR(50), value FLOAT)")
        conn.commit()

        # Prepare test data
        data = [
            (1, "Alice", 100.5),
            (2, "Bob", 200.75),
            (3, "Charlie", 300.25),
        ]

        # Perform bulkcopy - this should NOT raise ValueError about missing DATABASE
        # Note: bulkcopy creates its own connection, so we need to use fully qualified table name
        # if we had a database in the original connection string
        bulkcopy_table_name = (
            f"[{original_database}].[dbo].{table_name}" if original_database else table_name
        )
        result = cursor.bulkcopy(bulkcopy_table_name, data, timeout=60)

        # Verify result
        assert result is not None
        assert result["rows_copied"] == 3

        # Verify data was inserted correctly
        cursor.execute(f"SELECT id, name, value FROM {table_name} ORDER BY id")
        rows = cursor.fetchall()

        assert len(rows) == 3
        assert rows[0][0] == 1 and rows[0][1] == "Alice" and abs(rows[0][2] - 100.5) < 0.01
        assert rows[1][0] == 2 and rows[1][1] == "Bob" and abs(rows[1][2] - 200.75) < 0.01
        assert rows[2][0] == 3 and rows[2][1] == "Charlie" and abs(rows[2][2] - 300.25) < 0.01

        # Cleanup
        cursor.execute(f"DROP TABLE {table_name}")
        cursor.close()
    finally:
        conn.close()


def test_bulkcopy_with_server_synonyms(conn_str):
    """Test that bulkcopy works with all SERVER parameter synonyms: server, addr, address."""
    from mssql_python import connect
    from mssql_python.connection_string_parser import _ConnectionStringParser
    from mssql_python.connection_string_builder import _ConnectionStringBuilder

    # Parse the connection string using the proper parser
    parser = _ConnectionStringParser(validate_keywords=False)
    params = parser._parse(conn_str)

    # Test with 'Addr' synonym - replace 'server' with 'addr'
    server_value = (
        params.pop("server", None) or params.pop("addr", None) or params.pop("address", None)
    )
    params["addr"] = server_value
    builder = _ConnectionStringBuilder(params)
    conn_string_addr = builder.build()

    conn = connect(conn_string_addr)
    try:
        cursor = conn.cursor()
        table_name = "test_bulkcopy_addr_synonym"

        # Create table
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                id INT,
                name NVARCHAR(50),
                value FLOAT
            )
        """)
        conn.commit()

        # Test data
        test_data = [(1, "Test1", 1.5), (2, "Test2", 2.5), (3, "Test3", 3.5)]

        # Perform bulkcopy with connection using Addr parameter
        result = cursor.bulkcopy(table_name, test_data)

        # Verify result
        assert result is not None
        assert "rows_copied" in result
        assert result["rows_copied"] == 3

        # Verify data
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        assert count == 3

        # Cleanup
        cursor.execute(f"DROP TABLE {table_name}")
        cursor.close()
    finally:
        conn.close()

    # Test with 'Address' synonym - replace with 'address'
    params = parser._parse(conn_str)
    server_value = (
        params.pop("server", None) or params.pop("addr", None) or params.pop("address", None)
    )
    params["address"] = server_value
    builder = _ConnectionStringBuilder(params)
    conn_string_address = builder.build()

    conn = connect(conn_string_address)
    try:
        cursor = conn.cursor()
        table_name = "test_bulkcopy_address_synonym"

        # Create table
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                id INT,
                name NVARCHAR(50),
                value FLOAT
            )
        """)
        conn.commit()

        # Test data
        test_data = [(1, "Test1", 1.5), (2, "Test2", 2.5), (3, "Test3", 3.5)]

        # Perform bulkcopy with connection using Address parameter
        result = cursor.bulkcopy(table_name, test_data)

        # Verify result
        assert result is not None
        assert "rows_copied" in result
        assert result["rows_copied"] == 3

        # Verify data
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        assert count == 3

        # Cleanup
        cursor.execute(f"DROP TABLE {table_name}")
        cursor.close()
    finally:
        conn.close()

    # Test that bulkcopy fails when SERVER parameter is missing entirely
    params = parser._parse(conn_str)
    # Remove all server synonyms
    params.pop("server", None)
    params.pop("addr", None)
    params.pop("address", None)
    builder = _ConnectionStringBuilder(params)
    conn_string_no_server = builder.build()

    # Ensure we have a valid connection string for the main connection
    conn = connect(conn_str)
    try:
        cursor = conn.cursor()
        # Manually override the connection string to one without server
        cursor.connection.connection_str = conn_string_no_server

        table_name = "test_bulkcopy_no_server"
        test_data = [(1, "Test1", 1.5)]

        # This should raise ValueError due to missing SERVER parameter
        try:
            cursor.bulkcopy(table_name, test_data)
            assert False, "Expected ValueError for missing SERVER parameter"
        except ValueError as e:
            assert "SERVER parameter is required" in str(e)

        cursor.close()
    finally:
        conn.close()
