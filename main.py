from pyodbc import connect
# from pyodbc import setup_logging
import os
import decimal

# setup_logging('stdout')

conn_str = "Driver={ODBC Driver 18 for SQL Server};" + os.getenv("DB_CONNECTION_STRING")

try:
    print("[SEGDEBUGGING - PYTHON] Declaring the conn variable")
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

print("[SEGDEBUGGING - PYTHON] Declaring the cursor variable")
cursor = conn.cursor()
print(f"[SEGDEBUGGING - PYTHON] Cursor object declared as {cursor}, executing cursor")
cursor.execute("SELECT database_id, name from sys.databases;")
print("[SEGDEBUGGING - PYTHON] Cursor executed, fetching all rows")
rows = cursor.fetchall()
print("[SEGDEBUGGING - PYTHON] Rows fetched")

for row in rows:
    print(f"Database ID: {row[0]}, Name: {row[1]}")

print("[SEGDEBUGGING - PYTHON] Closing the connection")
# cursor.close()
conn.close()