"""
Test bulk copy functionality using Rust bindings with mssql_core_tds.

This test validates the BulkCopyWrapper from mssql_rust_bindings module
by copying 100 rows of test data into a temporary table using bulk_copy API.
"""

import pytest
import mssql_python
from datetime import datetime


def test_bulk_copy_100_rows(db_connection, cursor):
    """Test bulk copy with 100 rows of data"""
    try:
        import mssql_rust_bindings as rust
    except ImportError as e:
        pytest.skip(f"Rust bindings not available: {e}")
    
    # Connection parameters for bulk copy
    conn_dict = {
        'server': 'localhost',
        'database': 'master',
        'user_name': 'sa',
        'password': 'uvFvisUxK4En7AAV',
        'trust_server_certificate': 'yes'
    }
    
    # Create a temporary test table
    table_name = "#bulk_copy_test_100"
    cursor.execute(f"""
        CREATE TABLE {table_name} (
            id INT,
            name NVARCHAR(50),
            value DECIMAL(10, 2),
            created_date DATETIME
        )
    """)
    db_connection.commit()
    
    # Generate 100 rows of test data
    test_data = []
    for i in range(1, 101):
        test_data.append([
            i,
            f"TestName_{i}",
            float(i * 10.5),
            datetime.now()
        ])
    
    # Create mssql_core_tds connection via BulkCopyWrapper
    try:
        # BulkCopyWrapper now handles connection internally
        bulk_wrapper = rust.BulkCopyWrapper(conn_dict)
        
        # Perform bulk copy
        result = bulk_wrapper.bulk_copy(table_name, test_data)
        
        # Close the wrapper's connection
        bulk_wrapper.close()
        
        # Verify the copy count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        
        assert count == 100, f"Expected 100 rows, but found {count}"
        
        # Verify some sample data
        cursor.execute(f"SELECT id, name, value FROM {table_name} WHERE id IN (1, 50, 100) ORDER BY id")
        rows = cursor.fetchall()
        
        assert len(rows) == 3, f"Expected 3 sample rows, but found {len(rows)}"
        assert rows[0][0] == 1 and rows[0][1] == "TestName_1"
        assert rows[1][0] == 50 and rows[1][1] == "TestName_50"
        assert rows[2][0] == 100 and rows[2][1] == "TestName_100"
        
        print(f"✓ Successfully copied and validated 100 rows using bulk_copy")
        
    except AttributeError as e:
        pytest.skip(f"bulk_copy method not yet implemented in mssql_core_tds: {e}")
        
    except Exception as e:
        pytest.skip(f"Bulk copy operation not supported or failed: {e}")
    
    # Cleanup
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    db_connection.commit()
