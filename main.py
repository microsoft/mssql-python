from mssql_python import connect
from mssql_python import setup_logging
import os
import decimal

setup_logging('stdout')

conn_str = os.getenv("DB_CONNECTION_STRING")
print(f"Connecting to database with connection string: {conn_str[:20]}")

try:
    conn = connect(conn_str)
except Exception as e:
    if "Timeout error" in str(e):
        print(f"Database connection failed due to Timeout: {e}. Retrying in 80 seconds.")
        import time
        time.sleep(80)
        conn = connect(conn_str)
    else:
        raise

# conn.autocommit = True

cursor = conn.cursor()
cursor.execute("SELECT database_id, name from sys.databases;")
rows = cursor.fetchall()

for row in rows:
    print(f"Database ID: {row[0]}, Name: {row[1]}")

# cursor.close()
conn.close()