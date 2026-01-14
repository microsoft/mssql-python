import sys
import os
import pytest

# Add parent directory to path to import mssql_python
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mssql_python import connect


def test_bit_bulkcopy():
    """Test bulk copy functionality with BIT data type"""
    # Get connection string from environment
    conn_str = os.getenv("DB_CONNECTION_STRING")
    assert conn_str is not None, "DB_CONNECTION_STRING environment variable must be set"

    print(f"Connection string length: {len(conn_str)}")

    # Connect using the regular mssql_python connection
    conn = connect(conn_str)
    print(f"Connection created: {type(conn)}")

    # Create cursor
    cursor = conn.cursor()
    print(f"Cursor created: {type(cursor)}")

    # Create a test table with BIT columns
    table_name = "BulkCopyBitTest"

    print(f"\nCreating test table: {table_name}")
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.execute(
        f"""
        CREATE TABLE {table_name} (
            id INT,
            bit_value BIT,
            is_active BIT,
            is_deleted BIT,
            description VARCHAR(100)
        )
    """
    )
    conn.commit()
    print("Test table created successfully")

    # Prepare test data with various BIT values
    # BIT can be 0, 1, True, False, or NULL
    test_data = [
        (1, 0, 0, 0, "All zeros (False)"),
        (2, 1, 1, 1, "All ones (True)"),
        (3, True, True, True, "All True"),
        (4, False, False, False, "All False"),
        (5, 1, 0, 1, "Mixed 1-0-1"),
        (6, 0, 1, 0, "Mixed 0-1-0"),
        (7, True, False, True, "Mixed True-False-True"),
        (8, False, True, False, "Mixed False-True-False"),
        (9, 1, True, 0, "Mixed 1-True-0"),
        (10, False, 1, True, "Mixed False-1-True"),
    ]

    print(f"\nPerforming bulk copy with {len(test_data)} rows using cursor.bulkcopy()...")
    print("Testing BIT data type with True/False and 0/1 values...")

    # Perform bulk copy via cursor
    result = cursor.bulkcopy(
        table_name=table_name,
        data=test_data,
        batch_size=5,
        timeout=30,
        column_mappings=[
            (0, "id"),
            (1, "bit_value"),
            (2, "is_active"),
            (3, "is_deleted"),
            (4, "description"),
        ],
    )

    print(f"\nBulk copy completed successfully!")
    print(f"  Rows copied: {result['rows_copied']}")
    print(f"  Batch count: {result['batch_count']}")
    print(f"  Elapsed time: {result['elapsed_time']}")

    # Assertions
    assert result["rows_copied"] == 10, f"Expected 10 rows copied, got {result['rows_copied']}"
    assert result["batch_count"] == 2, f"Expected 2 batches, got {result['batch_count']}"

    # Verify the data
    print(f"\nVerifying inserted data...")
    cursor.execute(
        f"SELECT id, bit_value, is_active, is_deleted, description FROM {table_name} ORDER BY id"
    )
    rows = cursor.fetchall()

    print(f"Retrieved {len(rows)} rows:")
    assert len(rows) == 10, f"Expected 10 rows retrieved, got {len(rows)}"

    # Expected values after conversion (SQL Server stores BIT as 0 or 1)
    expected_values = [
        (1, False, False, False, "All zeros (False)"),
        (2, True, True, True, "All ones (True)"),
        (3, True, True, True, "All True"),
        (4, False, False, False, "All False"),
        (5, True, False, True, "Mixed 1-0-1"),
        (6, False, True, False, "Mixed 0-1-0"),
        (7, True, False, True, "Mixed True-False-True"),
        (8, False, True, False, "Mixed False-True-False"),
        (9, True, True, False, "Mixed 1-True-0"),
        (10, False, True, True, "Mixed False-1-True"),
    ]

    for i, row in enumerate(rows):
        print(
            f"  ID: {row[0]}, BIT: {row[1]}, IS_ACTIVE: {row[2]}, IS_DELETED: {row[3]}, Description: {row[4]}"
        )

        assert row[0] == expected_values[i][0], f"ID mismatch at row {i}"
        assert (
            row[1] == expected_values[i][1]
        ), f"BIT value mismatch at row {i}: expected {expected_values[i][1]}, got {row[1]}"
        assert (
            row[2] == expected_values[i][2]
        ), f"IS_ACTIVE mismatch at row {i}: expected {expected_values[i][2]}, got {row[2]}"
        assert (
            row[3] == expected_values[i][3]
        ), f"IS_DELETED mismatch at row {i}: expected {expected_values[i][3]}, got {row[3]}"
        assert row[4] == expected_values[i][4], f"Description mismatch at row {i}"

    # Additional verification for specific cases
    print("\nVerifying specific edge cases...")

    # All False values
    cursor.execute(f"SELECT bit_value, is_active, is_deleted FROM {table_name} WHERE id = 1")
    all_false = cursor.fetchone()
    assert (
        all_false[0] == False and all_false[1] == False and all_false[2] == False
    ), f"All False verification failed"
    print(f"  ✓ All False values verified: {all_false}")

    # All True values
    cursor.execute(f"SELECT bit_value, is_active, is_deleted FROM {table_name} WHERE id = 2")
    all_true = cursor.fetchone()
    assert (
        all_true[0] == True and all_true[1] == True and all_true[2] == True
    ), f"All True verification failed"
    print(f"  ✓ All True values verified: {all_true}")

    # Count True values
    cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE bit_value = 1")
    true_count = cursor.fetchone()[0]
    print(f"  ✓ Count of True bit_value: {true_count}")

    # Count False values
    cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE bit_value = 0")
    false_count = cursor.fetchone()[0]
    print(f"  ✓ Count of False bit_value: {false_count}")

    assert true_count + false_count == 10, f"Total count mismatch"

    # Cleanup
    print(f"\nCleaning up test table...")
    cursor.execute(f"DROP TABLE {table_name}")
    conn.commit()

    # Close cursor and connection
    cursor.close()
    conn.close()
    print("\nTest completed successfully!")


if __name__ == "__main__":
    test_bit_bulkcopy()
