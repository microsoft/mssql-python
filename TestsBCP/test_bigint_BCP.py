import sys
import os
import pytest

# Add parent directory to path to import mssql_python
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mssql_python import connect


def test_bigint_bulkcopy():
    """Test bulk copy functionality with BIGINT data type"""
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
    
    # Create a test table with BIGINT columns
    table_name = "BulkCopyBigIntTest"
    
    print(f"\nCreating test table: {table_name}")
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (id INT, bigint_value BIGINT, description VARCHAR(100))")
    conn.commit()
    print("Test table created successfully")
    
    # Prepare test data with various BIGINT values
    # BIGINT range: -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807
    test_data = [
        (1, 0, "Zero"),
        (2, 1, "Positive one"),
        (3, -1, "Negative one"),
        (4, 9223372036854775807, "Max BIGINT value"),
        (5, -9223372036854775808, "Min BIGINT value"),
        (6, 1000000000000, "One trillion"),
        (7, -1000000000000, "Negative one trillion"),
        (8, 9223372036854775806, "Near max value"),
        (9, -9223372036854775807, "Near min value"),
        (10, 123456789012345, "Random large value"),
    ]
    
    print(f"\nPerforming bulk copy with {len(test_data)} rows using cursor.bulkcopy()...")
    print("Testing BIGINT data type with edge cases...")
    
    # Perform bulk copy via cursor
    result = cursor.bulkcopy(
        table_name=table_name,
        data=test_data,
        batch_size=5,
        timeout=30,
        column_mappings=[
            (0, "id"),
            (1, "bigint_value"),
            (2, "description"),
        ]
    )
    
    print(f"\nBulk copy completed successfully!")
    print(f"  Rows copied: {result['rows_copied']}")
    print(f"  Batch count: {result['batch_count']}")
    print(f"  Elapsed time: {result['elapsed_time']}")
    
    # Assertions
    assert result['rows_copied'] == 10, f"Expected 10 rows copied, got {result['rows_copied']}"
    assert result['batch_count'] == 2, f"Expected 2 batches, got {result['batch_count']}"
    
    # Verify the data
    print(f"\nVerifying inserted data...")
    cursor.execute(f"SELECT id, bigint_value, description FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    
    print(f"Retrieved {len(rows)} rows:")
    assert len(rows) == 10, f"Expected 10 rows retrieved, got {len(rows)}"
    
    for i, row in enumerate(rows):
        print(f"  ID: {row[0]}, BIGINT Value: {row[1]}, Description: {row[2]}")
        assert row[0] == test_data[i][0], f"ID mismatch at row {i}"
        assert row[1] == test_data[i][1], f"BIGINT value mismatch at row {i}: expected {test_data[i][1]}, got {row[1]}"
        assert row[2] == test_data[i][2], f"Description mismatch at row {i}"
    
    # Additional verification for edge cases
    print("\nVerifying edge case values...")
    cursor.execute(f"SELECT bigint_value FROM {table_name} WHERE id = 4")
    max_value = cursor.fetchone()[0]
    assert max_value == 9223372036854775807, f"Max BIGINT verification failed"
    print(f"  ✓ Max BIGINT value verified: {max_value}")
    
    cursor.execute(f"SELECT bigint_value FROM {table_name} WHERE id = 5")
    min_value = cursor.fetchone()[0]
    assert min_value == -9223372036854775808, f"Min BIGINT verification failed"
    print(f"  ✓ Min BIGINT value verified: {min_value}")
    
    # Cleanup
    print(f"\nCleaning up test table...")
    cursor.execute(f"DROP TABLE {table_name}")
    conn.commit()
    
    # Close cursor and connection
    cursor.close()
    conn.close()
    print("\nTest completed successfully!")


if __name__ == "__main__":
    test_bigint_bulkcopy()
