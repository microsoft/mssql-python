from mssql_python import connect
from mssql_python.logging import setup_logging
import os
from datetime import datetime

# Clean one-liner: set level and output mode together
setup_logging(output="both")

print("=" * 70)
print("SQL Server - Bulk Copy Demo")
print("=" * 70)

# Use local SQL Server or environment variable
conn_str = os.getenv("DB_CONNECTION_STRING", 
                      "Server=localhost,1433;Database=master;UID=sa;PWD=uvFvisUxK4En7AAV;TrustServerCertificate=yes;")

print("\n[1] Connecting to database...")
conn = connect(conn_str)
cursor = conn.cursor()

# Query databases
print("[2] Querying sys.databases...")
cursor.execute("SELECT database_id, name from sys.databases;")
rows = cursor.fetchall()

for row in rows:
    print(f"  Database ID: {row[0]}, Name: {row[1]}")

print(f"\n  Total databases: {len(rows)}")

# Demonstrate bulk copy functionality
print("\n" + "=" * 70)
print("Bulk Copy with Rust Bindings")
print("=" * 70)

try:
    print("\n[3] Creating temporary table for bulk copy...")
    cursor.execute("""
        CREATE TABLE #bulk_copy_demo (
            id INT,
            name NVARCHAR(50),
            value DECIMAL(10, 2),
            created_date DATETIME
        )
    """)
    conn.commit()
    print("  ✓ Table created")
    
    # Generate 100 rows of test data
    print("\n[4] Generating 100 rows of test data...")
    test_data = []
    for i in range(1, 101):
        test_data.append([
            i,
            f"TestItem_{i}",
            float(i * 10.5),
            datetime.now()
        ])
    print(f"  ✓ Generated {len(test_data)} rows")
    
    # Perform bulk copy using cursor method
    print("\n[5] Performing bulk copy via cursor...")
    result = cursor.bulk_copy('#bulk_copy_demo', test_data)
    print(f"  ✓ Bulk copy completed: {result}")
    
    # Verify the data
    print("\n[6] Verifying bulk copy results...")
    cursor.execute("SELECT COUNT(*) FROM #bulk_copy_demo")
    count = cursor.fetchone()[0]
    print(f"  ✓ Total rows copied: {count}")
    
    # Show sample data
    cursor.execute("SELECT TOP 5 id, name, value FROM #bulk_copy_demo ORDER BY id")
    sample_rows = cursor.fetchall()
    print("\n  Sample data:")
    for row in sample_rows:
        print(f"    ID: {row[0]}, Name: {row[1]}, Value: {row[2]}")
    
    print("\n✓ Bulk copy demo completed successfully!")
    
    # Cleanup
    cursor.execute("DROP TABLE IF EXISTS #bulk_copy_demo")
    conn.commit()
    
except ImportError as e:
    print(f"\n✗ Rust bindings or mssql_core_tds not available: {e}")
except AttributeError as e:
    print(f"\n⚠ {e}")
    print("  Skipping bulk copy demo")
    # Cleanup
    cursor.execute("DROP TABLE IF EXISTS #bulk_copy_demo")
    conn.commit()
except Exception as e:
    print(f"\n✗ Bulk copy failed: {e}")
    # Cleanup
    cursor.execute("DROP TABLE IF EXISTS #bulk_copy_demo")
    conn.commit()

print("\n" + "=" * 70)
cursor.close()
conn.close()
print("✓ Connection closed")