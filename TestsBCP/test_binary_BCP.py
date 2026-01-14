import sys
import os
import pytest

# Add parent directory to path to import mssql_python
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mssql_python import connect


def test_binary_varbinary_bulkcopy():
    """Test bulk copy functionality with BINARY and VARBINARY data types"""
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
    
    # Create a test table with BINARY and VARBINARY columns
    table_name = "BulkCopyBinaryTest"
    
    print(f"\nCreating test table: {table_name}")
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.execute(f"""
        CREATE TABLE {table_name} (
            id INT,
            binary_data BINARY(16),
            varbinary_data VARBINARY(100),
            description VARCHAR(100)
        )
    """)
    conn.commit()
    print("Test table created successfully")
    
    # Prepare test data with various BINARY/VARBINARY values
    test_data = [
        (1, b'\x00' * 16, b'', "Empty varbinary"),
        (2, b'\x01\x02\x03\x04' + b'\x00' * 12, b'\x01\x02\x03\x04', "Small binary data"),
        (3, b'\xFF' * 16, b'\xFF' * 16, "All 0xFF bytes"),
        (4, b'\x00\x11\x22\x33\x44\x55\x66\x77\x88\x99\xAA\xBB\xCC\xDD\xEE\xFF', 
            b'\x00\x11\x22\x33\x44\x55\x66\x77\x88\x99\xAA\xBB\xCC\xDD\xEE\xFF', "Hex sequence"),
        (5, b'Hello World!!!!!' [:16], b'Hello World!', "ASCII text as binary"),
        (6, bytes(range(16)), bytes(range(50)), "Sequential bytes"),
        (7, b'\x00' * 16, b'\x00' * 100, "Max varbinary length"),
        (8, b'\xDE\xAD\xBE\xEF' * 4, b'\xDE\xAD\xBE\xEF' * 5, "Repeated pattern"),
        (9, b'\x01' * 16, b'\x01', "Single byte varbinary"),
        (10, b'\x80' * 16, b'\x80\x90\xA0\xB0\xC0\xD0\xE0\xF0', "High-bit bytes"),
    ]
    
    print(f"\nPerforming bulk copy with {len(test_data)} rows using cursor.bulkcopy()...")
    print("Testing BINARY and VARBINARY data types with edge cases...")
    
    # Perform bulk copy via cursor
    result = cursor.bulkcopy(
        table_name=table_name,
        data=test_data,
        batch_size=5,
        timeout=30,
        column_mappings=[
            (0, "id"),
            (1, "binary_data"),
            (2, "varbinary_data"),
            (3, "description"),
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
    cursor.execute(f"SELECT id, binary_data, varbinary_data, description FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    
    print(f"Retrieved {len(rows)} rows:")
    assert len(rows) == 10, f"Expected 10 rows retrieved, got {len(rows)}"
    
    for i, row in enumerate(rows):
        print(f"  ID: {row[0]}, BINARY: {row[1].hex() if row[1] else 'NULL'}, " +
              f"VARBINARY: {row[2].hex() if row[2] else 'NULL'}, Description: {row[3]}")
        
        assert row[0] == test_data[i][0], f"ID mismatch at row {i}"
        
        # BINARY comparison - SQL Server pads with zeros to fixed length
        expected_binary = test_data[i][1] if len(test_data[i][1]) == 16 else test_data[i][1] + b'\x00' * (16 - len(test_data[i][1]))
        assert row[1] == expected_binary, f"BINARY mismatch at row {i}: expected {expected_binary.hex()}, got {row[1].hex()}"
        
        # VARBINARY comparison - exact match expected
        assert row[2] == test_data[i][2], f"VARBINARY mismatch at row {i}: expected {test_data[i][2].hex()}, got {row[2].hex()}"
        
        assert row[3] == test_data[i][3], f"Description mismatch at row {i}"
    
    # Additional verification for specific cases
    print("\nVerifying specific edge cases...")
    
    # Empty varbinary
    cursor.execute(f"SELECT varbinary_data FROM {table_name} WHERE id = 1")
    empty_varbinary = cursor.fetchone()[0]
    assert empty_varbinary == b'', f"Empty varbinary verification failed"
    print(f"  ✓ Empty varbinary verified: length = {len(empty_varbinary)}")
    
    # Max varbinary length
    cursor.execute(f"SELECT varbinary_data FROM {table_name} WHERE id = 7")
    max_varbinary = cursor.fetchone()[0]
    assert len(max_varbinary) == 100, f"Max varbinary length verification failed"
    assert max_varbinary == b'\x00' * 100, f"Max varbinary content verification failed"
    print(f"  ✓ Max varbinary length verified: {len(max_varbinary)} bytes")
    
    # All 0xFF bytes
    cursor.execute(f"SELECT binary_data, varbinary_data FROM {table_name} WHERE id = 3")
    all_ff_row = cursor.fetchone()
    assert all_ff_row[0] == b'\xFF' * 16, f"All 0xFF BINARY verification failed"
    assert all_ff_row[1] == b'\xFF' * 16, f"All 0xFF VARBINARY verification failed"
    print(f"  ✓ All 0xFF bytes verified for both BINARY and VARBINARY")
    
    # Cleanup
    print(f"\nCleaning up test table...")
    cursor.execute(f"DROP TABLE {table_name}")
    conn.commit()
    
    # Close cursor and connection
    cursor.close()
    conn.close()
    print("\nTest completed successfully!")


if __name__ == "__main__":
    test_binary_varbinary_bulkcopy()
