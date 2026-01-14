"""Bulk copy tests for DATETIME data type."""

import pytest
import datetime


@pytest.mark.integration
def test_cursor_bulkcopy_datetime_basic(cursor):
    """Test cursor bulkcopy method with two datetime columns and explicit mappings."""
    # Create a test table with two datetime columns
    table_name = "BulkCopyTestTableDateTime"
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (start_datetime DATETIME, end_datetime DATETIME)")
    cursor.connection.commit()

    # Prepare test data - two columns, both datetime
    data = [
        (datetime.datetime(2024, 1, 15, 9, 30, 0), datetime.datetime(2024, 1, 15, 17, 45, 30)),
        (datetime.datetime(2024, 2, 20, 8, 15, 45), datetime.datetime(2024, 2, 20, 16, 30, 15)),
        (datetime.datetime(2024, 3, 10, 10, 0, 0), datetime.datetime(2024, 3, 10, 18, 0, 0)),
    ]

    # Execute bulk copy with explicit column mappings
    result = cursor.bulkcopy(
        table_name,
        data,
        batch_size=1000,
        timeout=30,
        column_mappings=[
            (0, "start_datetime"),
            (1, "end_datetime"),
        ],
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 3
    assert result["batch_count"] == 1
    assert "elapsed_time" in result

    # Verify data was inserted by checking the count
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    rows = cursor.fetchall()
    count = rows[0][0]
    assert count == 3

    # Verify actual datetime values
    cursor.execute(f"SELECT start_datetime, end_datetime FROM {table_name} ORDER BY start_datetime")
    rows = cursor.fetchall()
    assert len(rows) == 3

    # Verify first row
    assert rows[0][0] == datetime.datetime(2024, 1, 15, 9, 30, 0)
    assert rows[0][1] == datetime.datetime(2024, 1, 15, 17, 45, 30)

    # Verify second row
    assert rows[1][0] == datetime.datetime(2024, 2, 20, 8, 15, 45)
    assert rows[1][1] == datetime.datetime(2024, 2, 20, 16, 30, 15)

    # Verify third row
    assert rows[2][0] == datetime.datetime(2024, 3, 10, 10, 0, 0)
    assert rows[2][1] == datetime.datetime(2024, 3, 10, 18, 0, 0)

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    cursor.connection.commit()


@pytest.mark.integration
def test_cursor_bulkcopy_datetime_auto_mapping(cursor):
    """Test cursor bulkcopy with automatic column mapping.

    Tests bulkcopy when no mappings are specified, including NULL value handling.
    """
    # Create a test table with two nullable datetime columns
    table_name = "BulkCopyAutoMapTableDateTime"
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (start_datetime DATETIME, end_datetime DATETIME)")
    cursor.connection.commit()

    # Prepare test data - two columns, both datetime, with NULL values
    data = [
        (datetime.datetime(2024, 1, 15, 9, 30, 0), datetime.datetime(2024, 1, 15, 17, 45, 30)),
        (datetime.datetime(2024, 2, 20, 8, 15, 45), None),  # NULL value in second column
        (None, datetime.datetime(2024, 2, 20, 16, 30, 15)),  # NULL value in first column
        (datetime.datetime(2024, 3, 10, 10, 0, 0), datetime.datetime(2024, 3, 10, 18, 0, 0)),
    ]

    # Execute bulk copy WITHOUT column mappings - should auto-generate
    result = cursor.bulkcopy(table_name, data, batch_size=1000, timeout=30)

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 4
    assert result["batch_count"] == 1
    assert "elapsed_time" in result

    # Verify data was inserted by checking the count
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    rows = cursor.fetchall()
    count = rows[0][0]
    assert count == 4

    # Verify NULL handling
    cursor.execute(
        f"SELECT start_datetime, end_datetime FROM {table_name} ORDER BY ISNULL(start_datetime, '1900-01-01')"
    )
    rows = cursor.fetchall()
    assert len(rows) == 4

    # Verify NULL value in first column (third row after sorting)
    assert rows[0][0] is None
    assert rows[0][1] == datetime.datetime(2024, 2, 20, 16, 30, 15)

    # Verify NULL value in second column
    assert rows[2][0] == datetime.datetime(2024, 2, 20, 8, 15, 45)
    assert rows[2][1] is None

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    cursor.connection.commit()


@pytest.mark.integration
def test_cursor_bulkcopy_datetime_string_to_datetime_conversion(cursor):
    """Test cursor bulkcopy with string values that should convert to datetime columns.

    Tests type coercion when source data contains datetime strings but
    destination columns are DATETIME type.
    """
    # Create a test table with two datetime columns
    table_name = "BulkCopyStringToDateTimeTable"
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (start_datetime DATETIME, end_datetime DATETIME)")
    cursor.connection.commit()

    # Prepare test data - strings containing valid datetimes in ISO format
    data = [
        ("2024-01-15 09:30:00", "2024-01-15 17:45:30"),
        ("2024-02-20 08:15:45", "2024-02-20 16:30:15"),
        ("2024-03-10 10:00:00", "2024-03-10 18:00:00"),
    ]

    # Execute bulk copy without explicit mappings
    result = cursor.bulkcopy(table_name, data, batch_size=1000, timeout=30)

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 3
    assert result["batch_count"] == 1
    assert "elapsed_time" in result

    # Verify data was inserted by checking the count
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    rows = cursor.fetchall()
    count = rows[0][0]
    assert count == 3

    # Verify the datetime values were properly converted from strings
    cursor.execute(f"SELECT start_datetime, end_datetime FROM {table_name} ORDER BY start_datetime")
    rows = cursor.fetchall()
    assert len(rows) == 3

    # Verify first row
    assert rows[0][0] == datetime.datetime(2024, 1, 15, 9, 30, 0)
    assert rows[0][1] == datetime.datetime(2024, 1, 15, 17, 45, 30)

    # Verify second row
    assert rows[1][0] == datetime.datetime(2024, 2, 20, 8, 15, 45)
    assert rows[1][1] == datetime.datetime(2024, 2, 20, 16, 30, 15)

    # Verify third row
    assert rows[2][0] == datetime.datetime(2024, 3, 10, 10, 0, 0)
    assert rows[2][1] == datetime.datetime(2024, 3, 10, 18, 0, 0)

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    cursor.connection.commit()


@pytest.mark.integration
def test_cursor_bulkcopy_datetime_boundary_values(cursor):
    """Test cursor bulkcopy with DATETIME boundary values.

    DATETIME range: 1753-01-01 00:00:00 to 9999-12-31 23:59:59.997
    Precision: Rounded to increments of .000, .003, or .007 seconds
    """
    table_name = "BulkCopyDateTimeBoundaryTable"
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (dt_value DATETIME)")
    cursor.connection.commit()

    # Test data with boundary values and edge cases
    data = [
        (datetime.datetime(1753, 1, 1, 0, 0, 0),),  # Minimum datetime
        (datetime.datetime(9999, 12, 31, 23, 59, 59),),  # Maximum datetime
        (datetime.datetime(2000, 1, 1, 0, 0, 0),),  # Y2K
        (datetime.datetime(1999, 12, 31, 23, 59, 59),),  # Pre-Y2K
        (datetime.datetime(2024, 2, 29, 12, 0, 0),),  # Leap year
        (datetime.datetime(2024, 12, 31, 23, 59, 59),),  # End of year
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(table_name, data, batch_size=1000, timeout=30)

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 6
    assert result["batch_count"] == 1

    # Verify data
    cursor.execute(f"SELECT dt_value FROM {table_name} ORDER BY dt_value")
    rows = cursor.fetchall()
    assert len(rows) == 6

    # Verify minimum datetime
    assert rows[0][0] == datetime.datetime(1753, 1, 1, 0, 0, 0)

    # Verify pre-Y2K
    assert rows[1][0] == datetime.datetime(1999, 12, 31, 23, 59, 59)

    # Verify Y2K
    assert rows[2][0] == datetime.datetime(2000, 1, 1, 0, 0, 0)

    # Verify leap year
    assert rows[3][0] == datetime.datetime(2024, 2, 29, 12, 0, 0)

    # Verify end of year
    assert rows[4][0] == datetime.datetime(2024, 12, 31, 23, 59, 59)

    # Verify maximum datetime
    assert rows[5][0] == datetime.datetime(9999, 12, 31, 23, 59, 59)

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    cursor.connection.commit()


@pytest.mark.integration
def test_cursor_bulkcopy_datetime_mixed_types(cursor):
    """Test cursor bulkcopy with DATETIME in a table with mixed column types.

    Verifies that DATETIME columns work correctly alongside other data types.
    """
    table_name = "BulkCopyDateTimeMixedTable"
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.execute(
        f"""
        CREATE TABLE {table_name} (
            id INT,
            created_at DATETIME,
            is_active BIT,
            modified_at DATETIME
        )
    """
    )
    cursor.connection.commit()

    # Test data with mixed types (INT, DATETIME, BIT, DATETIME)
    data = [
        (
            1,
            datetime.datetime(2024, 1, 15, 9, 30, 0),
            True,
            datetime.datetime(2024, 1, 15, 10, 0, 0),
        ),
        (
            2,
            datetime.datetime(2024, 2, 20, 8, 15, 45),
            False,
            datetime.datetime(2024, 2, 20, 14, 30, 0),
        ),
        (3, datetime.datetime(2024, 3, 10, 10, 0, 0), True, None),  # NULL datetime
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(table_name, data, batch_size=1000, timeout=30)

    # Verify bulk copy succeeded
    assert result is not None
    assert result["rows_copied"] == 3

    # Verify the data was inserted correctly
    cursor.execute(f"SELECT id, created_at, is_active, modified_at FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()

    assert len(rows) == 3

    # Verify first row
    assert rows[0][0] == 1
    assert rows[0][1] == datetime.datetime(2024, 1, 15, 9, 30, 0)
    assert rows[0][2] == True
    assert rows[0][3] == datetime.datetime(2024, 1, 15, 10, 0, 0)

    # Verify second row
    assert rows[1][0] == 2
    assert rows[1][1] == datetime.datetime(2024, 2, 20, 8, 15, 45)
    assert rows[1][2] == False
    assert rows[1][3] == datetime.datetime(2024, 2, 20, 14, 30, 0)

    # Verify third row (with NULL datetime)
    assert rows[2][0] == 3
    assert rows[2][1] == datetime.datetime(2024, 3, 10, 10, 0, 0)
    assert rows[2][2] == True
    assert rows[2][3] is None  # NULL modified_at

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    cursor.connection.commit()
