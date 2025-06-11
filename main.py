from mssql_python import connect
from mssql_python import setup_logging
import os
import decimal

# setup_logging('stdout')

conn_str = os.getenv("DB_CONNECTION_STRING")
conn = connect(conn_str)

cursor = conn.cursor()
cursor.execute("SELECT database_id, name from sys.databases;")
rows = cursor.fetchone()

# Debug: Print the type and content of rows
print(f"Type of rows: {type(rows)}")
print(f"Value of rows: {rows}")

# Only try to access properties if rows is not None
if rows is not None:
    try:
        # Try different ways to access the data
        print(f"First column by index: {rows[0]}")
        
        # Access by attribute name (these should now work)
        print(f"First column by name: {rows.database_id}")
        print(f"Second column by name: {rows.name}")
        
        # Print all available attributes
        print(f"Available attributes: {dir(rows)}")
    except Exception as e:
        print(f"Exception accessing row data: {e}")

cursor.close()
conn.close()