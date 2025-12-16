from mssql_python import connect
from mssql_python.logging import setup_logging
import os

# Clean one-liner: set level and output mode together
setup_logging(output="both")
print("Logging is set up.")
print("This is a test PR for mssql-python.")
conn_str = os.getenv("DB_CONNECTION_STRING")
conn = connect(conn_str)
cursor = conn.cursor()
cursor.execute("SELECT database_id, name from sys.databases;")
rows = cursor.fetchall()

for row in rows:
    print(f"Database ID: {row[0]}, Name: {row[1]}")

cursor.close()
conn.close()
