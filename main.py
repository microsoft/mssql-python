from mssql_python import connect
from mssql_python import setup_logging
import os
import decimal
# import pyodbc

setup_logging('stdout')

# conn_str = os.getenv("DB_CONNECTION_STRING")
conn_str = "DRIVER={ODBC Driver 18 for SQL Server};Server=Saumya;DATABASE=master;UID=sa;PWD=HappyPass1234;Trust_Connection=yes;TrustServerCertificate=yes;"

conn = connect(conn_str)

# # conn.autocommit = True

# cursor = conn.cursor()
# cursor.execute("SELECT database_id, name from sys.databases;")
# rows = cursor.fetchall()

# for row in rows:
#     print(f"Database ID: {row[0]}, Name: {row[1]}")

# cursor.close()
# conn.close()

from datetime import datetime, timezone, timedelta

# Connect and get cursor
cursor = conn.cursor()

# Create table (drop if exists)
cursor.execute("""
IF OBJECT_ID('dbo.test_datetimeoffset', 'U') IS NOT NULL
    DROP TABLE dbo.test_datetimeoffset;

CREATE TABLE dbo.test_datetimeoffset (
    id INT PRIMARY KEY,
    dt DATETIMEOFFSET
)
""")

# Insert a row
dt_offset = datetime(2025, 9, 9, 15, 30, 45, 123456, tzinfo=timezone(timedelta(hours=5, minutes=30)))
cursor.execute("INSERT INTO test_datetimeoffset (id, dt) VALUES (?, ?)", 1, dt_offset)
conn.commit()
print("Insertion done. Verify in SSMS.")

# --- Fetch the row ---
cursor.execute("SELECT id, dt FROM dbo.test_datetimeoffset WHERE id = ?", 1)
row = cursor.fetchone()

if row:
    fetched_id, fetched_dt = row
    print(f"Fetched ID: {fetched_id}")
    print(f"Fetched DATETIMEOFFSET: {fetched_dt} (tzinfo: {fetched_dt.tzinfo})")

    # Optional check
    if fetched_dt == dt_offset:
        print("✅ Fetch successful: Datetime matches inserted value")
    else:
        print("⚠️ Fetch mismatch: Datetime does not match inserted value")
else:
    print("No row fetched.")
