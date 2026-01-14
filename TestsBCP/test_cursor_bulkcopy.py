import sys
import os
import pytest

# Add parent directory to path to import mssql_python
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mssql_python import connect


def test_cursor_bulkcopy():
    """Test bulk copy functionality through cursor.bulkcopy() method"""
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
    
    # Create a test table
    table_name = "BulkCopyCursorTest"
    
    print(f"\nCreating test table: {table_name}")
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (id INT, name VARCHAR(50), amount DECIMAL(10,2))")
    conn.commit()
    print("Test table created successfully")
    
    # Prepare test data
    test_data = [
        (1, "Product A", 99.99),
        (2, "Product B", 149.50),
        (3, "Product C", 199.99),
        (4, "Product D", 249.00),
        (5, "Product E", 299.99),
        (6, "Product F", 349.50),
        (7, "Product G", 399.99),
        (8, "Product H", 449.00),
        (9, "Product I", 499.99),
        (10, "Product J", 549.50),
    ]
    
    print(f"\nPerforming bulk copy with {len(test_data)} rows using cursor.bulkcopy()...")
    
    # Perform bulk copy via cursor
    result = cursor.bulkcopy(
        table_name=table_name,
        data=test_data,
        batch_size=5,
        timeout=30,
        column_mappings=[
            (0, "id"),
            (1, "name"),
            (2, "amount"),
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
    cursor.execute(f"SELECT id, name, amount FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    
    print(f"Retrieved {len(rows)} rows:")
    assert len(rows) == 10, f"Expected 10 rows retrieved, got {len(rows)}"
    
    for i, row in enumerate(rows):
        print(f"  ID: {row[0]}, Name: {row[1]}, Amount: {row[2]}")
        assert row[0] == test_data[i][0], f"ID mismatch at row {i}"
        assert row[1] == test_data[i][1], f"Name mismatch at row {i}"
        assert float(row[2]) == test_data[i][2], f"Amount mismatch at row {i}"
    
    # Cleanup
    print(f"\nCleaning up test table...")
    cursor.execute(f"DROP TABLE {table_name}")
    conn.commit()
    
    # Close cursor and connection
    cursor.close()
    conn.close()
    print("\nTest completed successfully!")
