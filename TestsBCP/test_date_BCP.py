"""Bulk copy tests for DATE data type."""
import pytest
import datetime


@pytest.mark.integration
def test_cursor_bulkcopy_date_basic(cursor):
    """Test cursor bulkcopy method with two date columns and explicit mappings."""
    
    # Create a test table with two date columns
    table_name = "BulkCopyTestTableDate"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (event_date DATE, birth_date DATE)")
    cursor.connection.commit()

    # Prepare test data - two columns, both date
    data = [
        (datetime.date(2020, 1, 15), datetime.date(1990, 5, 20)),
        (datetime.date(2021, 6, 10), datetime.date(1985, 3, 25)),
        (datetime.date(2022, 12, 25), datetime.date(2000, 7, 4)),
    ]

    # Execute bulk copy with explicit column mappings
    result = cursor.bulkcopy(
        table_name=table_name,
        data=data,
        batch_size=1000,
        timeout=30,
        column_mappings=[
            (0, "event_date"),
            (1, "birth_date"),
        ]
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 3
    assert result["batch_count"] == 1
    assert "elapsed_time" in result

    # Verify data was inserted correctly
    cursor.execute(f"SELECT event_date, birth_date FROM {table_name} ORDER BY event_date")
    rows = cursor.fetchall()
    assert len(rows) == 3
    assert rows[0][0] == datetime.date(2020, 1, 15) and rows[0][1] == datetime.date(1990, 5, 20)
    assert rows[1][0] == datetime.date(2021, 6, 10) and rows[1][1] == datetime.date(1985, 3, 25)
    assert rows[2][0] == datetime.date(2022, 12, 25) and rows[2][1] == datetime.date(2000, 7, 4)

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    cursor.connection.commit()


@pytest.mark.integration
def test_cursor_bulkcopy_date_auto_mapping(cursor):
    """Test cursor bulkcopy with automatic column mapping.

    Tests bulkcopy when no mappings are specified, including NULL value handling.
    """
    
    # Create a test table with two nullable date columns
    table_name = "BulkCopyAutoMapTableDate"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (event_date DATE, birth_date DATE)")
    cursor.connection.commit()

    # Prepare test data - two columns, both date, with NULL values
    data = [
        (datetime.date(2020, 1, 15), datetime.date(1990, 5, 20)),
        (datetime.date(2021, 6, 10), None),  # NULL value in second column
        (None, datetime.date(1985, 3, 25)),  # NULL value in first column
        (datetime.date(2022, 12, 25), datetime.date(2000, 7, 4)),
    ]

    # Execute bulk copy WITHOUT column mappings - should auto-generate
    result = cursor.bulkcopy(
        table_name=table_name,
        data=data,
        batch_size=1000,
        timeout=30
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 4
    assert result["batch_count"] == 1
    assert "elapsed_time" in result

    # Verify data including NULLs
    cursor.execute(f"SELECT event_date, birth_date FROM {table_name} ORDER BY COALESCE(event_date, '9999-12-31')")
    rows = cursor.fetchall()
    assert len(rows) == 4
    assert rows[0][0] == datetime.date(2020, 1, 15) and rows[0][1] == datetime.date(1990, 5, 20)
    assert rows[1][0] == datetime.date(2021, 6, 10) and rows[1][1] is None
    assert rows[2][0] == datetime.date(2022, 12, 25) and rows[2][1] == datetime.date(2000, 7, 4)
    assert rows[3][0] is None and rows[3][1] == datetime.date(1985, 3, 25)

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    cursor.connection.commit()


@pytest.mark.integration
def test_cursor_bulkcopy_date_string_to_date_conversion(cursor):
    """Test cursor bulkcopy with string values that should convert to date columns.

    Tests type coercion when source data contains date strings but
    destination columns are DATE type.
    """
    
    # Create a test table with two date columns
    table_name = "BulkCopyStringToDateTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (event_date DATE, birth_date DATE)")
    cursor.connection.commit()

    # Prepare test data - strings containing valid dates in ISO format
    data = [
        ("2020-01-15", "1990-05-20"),
        ("2021-06-10", "1985-03-25"),
        ("2022-12-25", "2000-07-04"),
    ]

    # Execute bulk copy without explicit mappings
    result = cursor.bulkcopy(
        table_name=table_name,
        data=data,
        batch_size=1000,
        timeout=30
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 3
    assert result["batch_count"] == 1
    assert "elapsed_time" in result

    # Verify data was converted correctly
    cursor.execute(f"SELECT event_date, birth_date FROM {table_name} ORDER BY event_date")
    rows = cursor.fetchall()
    assert len(rows) == 3
    assert rows[0][0] == datetime.date(2020, 1, 15) and rows[0][1] == datetime.date(1990, 5, 20)
    assert rows[1][0] == datetime.date(2021, 6, 10) and rows[1][1] == datetime.date(1985, 3, 25)
    assert rows[2][0] == datetime.date(2022, 12, 25) and rows[2][1] == datetime.date(2000, 7, 4)

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    cursor.connection.commit()


@pytest.mark.integration
def test_cursor_bulkcopy_date_boundary_values(cursor):
    """Test cursor bulkcopy with DATE boundary values."""
    
    # Create a test table
    table_name = "BulkCopyDateBoundaryTest"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, test_date DATE)")
    cursor.connection.commit()

    # Test data with boundary values
    # DATE range: 0001-01-01 to 9999-12-31
    data = [
        (1, datetime.date(1, 1, 1)),        # Min DATE
        (2, datetime.date(9999, 12, 31)),   # Max DATE
        (3, datetime.date(2000, 1, 1)),     # Y2K
        (4, datetime.date(1900, 1, 1)),     # Century boundary
        (5, datetime.date(2024, 2, 29)),    # Leap year
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name=table_name,
        data=data,
        batch_size=1000,
        timeout=30,
        column_mappings=[
            (0, "id"),
            (1, "test_date"),
        ]
    )

    # Verify results
    assert result["rows_copied"] == 5
    assert result["batch_count"] == 1

    # Verify data
    cursor.execute(f"SELECT id, test_date FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 5
    assert rows[0][1] == datetime.date(1, 1, 1)        # Min DATE
    assert rows[1][1] == datetime.date(9999, 12, 31)   # Max DATE
    assert rows[2][1] == datetime.date(2000, 1, 1)     # Y2K
    assert rows[3][1] == datetime.date(1900, 1, 1)     # Century boundary
    assert rows[4][1] == datetime.date(2024, 2, 29)    # Leap year

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    cursor.connection.commit()


@pytest.mark.integration
def test_cursor_bulkcopy_date_large_batch(cursor):
    """Test cursor bulkcopy with a large number of DATE rows."""
    
    # Create a test table
    table_name = "BulkCopyDateLargeBatchTest"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, test_date DATE)")
    cursor.connection.commit()

    # Generate 365 rows (one for each day of 2024)
    base_date = datetime.date(2024, 1, 1)
    data = [(i + 1, base_date + datetime.timedelta(days=i)) for i in range(365)]

    # Execute bulk copy with smaller batch size
    result = cursor.bulkcopy(
        table_name=table_name,
        data=data,
        batch_size=50,  # ~8 batches
        timeout=30,
        column_mappings=[
            (0, "id"),
            (1, "test_date"),
        ]
    )

    # Verify results
    assert result["rows_copied"] == 365
    assert result["batch_count"] >= 7  # 365 / 50 = 7.3

    # Verify row count
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    assert count == 365

    # Verify sample data
    cursor.execute(f"SELECT id, test_date FROM {table_name} WHERE id IN (1, 100, 200, 365)")
    rows = cursor.fetchall()
    assert len(rows) == 4
    assert rows[0][0] == 1 and rows[0][1] == datetime.date(2024, 1, 1)
    assert rows[1][0] == 100 and rows[1][1] == datetime.date(2024, 4, 9)
    assert rows[2][0] == 200 and rows[2][1] == datetime.date(2024, 7, 18)
    assert rows[3][0] == 365 and rows[3][1] == datetime.date(2024, 12, 30)

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    cursor.connection.commit()
