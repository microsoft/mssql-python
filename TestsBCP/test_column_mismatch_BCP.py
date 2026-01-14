"""Bulk copy tests for column count mismatch scenarios.

Tests the behavior when bulk copying data where:
1. Source data has more columns than the target table
2. Source data has fewer columns than the target table

According to expected behavior:
- Extra columns in the source should be dropped/ignored
- Missing columns should result in NULL values (if nullable) or defaults
"""

import pytest


@pytest.mark.integration
def test_bulkcopy_more_columns_than_table(cursor):
    """Test bulk copy where source has more columns than the target table.
    
    The extra columns should be dropped and the bulk copy should succeed.
    Only the columns specified in column_mappings should be inserted.
    """
    
    # Create a test table with 3 INT columns
    table_name = "BulkCopyMoreColumnsTest"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"CREATE TABLE {table_name} (id INT PRIMARY KEY, value1 INT, value2 INT)"
    )
    cursor.connection.commit()

    # Source data has 5 columns, but table only has 3
    # Extra columns (indices 3 and 4) should be ignored via column_mappings
    data = [
        (1, 100, 30, 999, 888),
        (2, 200, 25, 999, 888),
        (3, 300, 35, 999, 888),
        (4, 400, 28, 999, 888),
    ]

    # Execute bulk copy with explicit column mappings for first 3 columns only
    result = cursor.bulkcopy(
        table_name=table_name,
        data=data,
        batch_size=1000,
        timeout=30,
        column_mappings=[
            (0, "id"),       # Map source column 0 to 'id'
            (1, "value1"),   # Map source column 1 to 'value1'
            (2, "value2"),   # Map source column 2 to 'value2'
            # Columns 3 and 4 are NOT mapped, so they're dropped
        ]
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 4, "Expected 4 rows to be copied"
    assert result["batch_count"] >= 1

    # Verify data was inserted correctly (only first 3 columns)
    cursor.execute(f"SELECT id, value1, value2 FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()

    assert len(rows) == 4, "Expected 4 rows in table"
    assert rows[0][0] == 1 and rows[0][1] == 100 and rows[0][2] == 30
    assert rows[1][0] == 2 and rows[1][1] == 200 and rows[1][2] == 25
    assert rows[2][0] == 3 and rows[2][1] == 300 and rows[2][2] == 35
    assert rows[3][0] == 4 and rows[3][1] == 400 and rows[3][2] == 28

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    cursor.connection.commit()


@pytest.mark.integration
def test_bulkcopy_fewer_columns_than_table(cursor):
    """Test bulk copy where source has fewer columns than the target table.
    
    Missing columns should be filled with NULL (if nullable).
    The bulk copy should succeed.
    """
    
    # Create a test table with 3 INT columns (value2 is nullable)
    table_name = "BulkCopyFewerColumnsTest"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"CREATE TABLE {table_name} (id INT PRIMARY KEY, value1 INT, value2 INT NULL)"
    )
    cursor.connection.commit()

    # Source data has only 2 columns (id, value1) - missing 'value2'
    data = [
        (1, 100),
        (2, 200),
        (3, 300),
        (4, 400),
    ]

    # Execute bulk copy with mappings for only 2 columns
    # 'value2' column is not mapped, so it should get NULL values
    result = cursor.bulkcopy(
        table_name=table_name,
        data=data,
        batch_size=1000,
        timeout=30,
        column_mappings=[
            (0, "id"),       # Map source column 0 to 'id'
            (1, "value1"),   # Map source column 1 to 'value1'
            # 'value2' is not mapped, should be NULL
        ]
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 4, "Expected 4 rows to be copied"
    assert result["batch_count"] >= 1

    # Verify data was inserted with NULL for missing 'value2' column
    cursor.execute(f"SELECT id, value1, value2 FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()

    assert len(rows) == 4, "Expected 4 rows in table"
    assert rows[0][0] == 1 and rows[0][1] == 100 and rows[0][2] is None, "value2 should be NULL"
    assert rows[1][0] == 2 and rows[1][1] == 200 and rows[1][2] is None, "value2 should be NULL"
    assert rows[2][0] == 3 and rows[2][1] == 300 and rows[2][2] is None, "value2 should be NULL"
    assert rows[3][0] == 4 and rows[3][1] == 400 and rows[3][2] is None, "value2 should be NULL"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    cursor.connection.commit()


@pytest.mark.integration
def test_bulkcopy_auto_mapping_with_extra_columns(cursor):
    """Test bulk copy with auto-mapping when source has more columns than table.
    
    Without explicit column_mappings, auto-mapping should use the first N columns
    where N is the number of columns in the target table. Extra source columns are ignored.
    """
    
    # Create a test table with 3 INT columns
    table_name = "BulkCopyAutoMapExtraTest"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"CREATE TABLE {table_name} (id INT, value1 INT, value2 INT)"
    )
    cursor.connection.commit()

    # Source data has 5 columns, table has 3
    # Auto-mapping should use first 3 columns
    data = [
        (1, 100, 30, 777, 666),
        (2, 200, 25, 777, 666),
        (3, 300, 35, 777, 666),
    ]

    # Execute bulk copy WITHOUT explicit column mappings
    # Auto-mapping should map first 3 columns to table's 3 columns
    result = cursor.bulkcopy(
        table_name=table_name,
        data=data,
        batch_size=1000,
        timeout=30
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 3, "Expected 3 rows to be copied"

    # Verify data was inserted correctly (first 3 columns only)
    cursor.execute(f"SELECT id, value1, value2 FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()

    assert len(rows) == 3, "Expected 3 rows in table"
    assert rows[0][0] == 1 and rows[0][1] == 100 and rows[0][2] == 30
    assert rows[1][0] == 2 and rows[1][1] == 200 and rows[1][2] == 25
    assert rows[2][0] == 3 and rows[2][1] == 300 and rows[2][2] == 35

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    cursor.connection.commit()


@pytest.mark.integration
def test_bulkcopy_fewer_columns_with_defaults(cursor):
    """Test bulk copy where missing columns have default values.
    
    Missing columns should use their default values instead of NULL.
    """
    
    # Create a test table with default values
    table_name = "BulkCopyFewerColumnsDefaultTest"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"""CREATE TABLE {table_name} (
            id INT PRIMARY KEY, 
            value1 INT, 
            value2 INT DEFAULT 999,
            status VARCHAR(10) DEFAULT 'active'
        )"""
    )
    cursor.connection.commit()

    # Source data has only 2 columns - missing value2 and status
    data = [
        (1, 100),
        (2, 200),
        (3, 300),
    ]

    # Execute bulk copy mapping only 2 columns
    result = cursor.bulkcopy(
        table_name=table_name,
        data=data,
        batch_size=1000,
        timeout=30,
        column_mappings=[
            (0, "id"),
            (1, "value1"),
            # value2 and status not mapped - should use defaults
        ]
    )

    # Verify results
    assert result["rows_copied"] == 3

    # Verify data was inserted with default values
    cursor.execute(f"SELECT id, value1, value2, status FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()

    assert len(rows) == 3
    assert rows[0][0] == 1 and rows[0][1] == 100 and rows[0][2] == 999 and rows[0][3] == 'active'
    assert rows[1][0] == 2 and rows[1][1] == 200 and rows[1][2] == 999 and rows[1][3] == 'active'
    assert rows[2][0] == 3 and rows[2][1] == 300 and rows[2][2] == 999 and rows[2][3] == 'active'

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    cursor.connection.commit()


@pytest.mark.integration
def test_bulkcopy_column_reordering(cursor):
    """Test bulk copy with column reordering.
    
    Source columns can be mapped to target columns in different order.
    """
    
    # Create a test table
    table_name = "BulkCopyColumnReorderTest"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"CREATE TABLE {table_name} (id INT, name VARCHAR(50), age INT, city VARCHAR(50))"
    )
    cursor.connection.commit()

    # Source data: (name, age, city, id) - different order than table
    data = [
        ("Alice", 30, "NYC", 1),
        ("Bob", 25, "LA", 2),
        ("Carol", 35, "Chicago", 3),
    ]

    # Map source columns to target in different order
    result = cursor.bulkcopy(
        table_name=table_name,
        data=data,
        batch_size=1000,
        timeout=30,
        column_mappings=[
            (3, "id"),      # Source column 3 (id) → Target id
            (0, "name"),    # Source column 0 (name) → Target name
            (1, "age"),     # Source column 1 (age) → Target age
            (2, "city"),    # Source column 2 (city) → Target city
        ]
    )

    # Verify results
    assert result["rows_copied"] == 3

    # Verify data was inserted correctly with proper column mapping
    cursor.execute(f"SELECT id, name, age, city FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()

    assert len(rows) == 3
    assert rows[0][0] == 1 and rows[0][1] == "Alice" and rows[0][2] == 30 and rows[0][3] == "NYC"
    assert rows[1][0] == 2 and rows[1][1] == "Bob" and rows[1][2] == 25 and rows[1][3] == "LA"
    assert rows[2][0] == 3 and rows[2][1] == "Carol" and rows[2][2] == 35 and rows[2][3] == "Chicago"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    cursor.connection.commit()
