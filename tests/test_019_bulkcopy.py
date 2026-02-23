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
    result = cursor._bulkcopy(table_name, data, timeout=60)

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
