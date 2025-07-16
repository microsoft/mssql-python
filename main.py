from mssql_python import connect
from mssql_python import setup_logging, get_logger
import os
import decimal

setup_logging('stdout')

logger = get_logger()
if logger:
    logger.info("This should print")
else:
    print("Logger is None - logging not enabled!")

conn_str = os.getenv("DB_CONNECTION_STRING")
conn = connect(conn_str)

# conn.autocommit = True

cursor = conn.cursor()
cursor.execute("SELECT database_id, name from sys.databases;")
rows = cursor.fetchall()

for row in rows:
    print(f"Database ID: {row[0]}, Name: {row[1]}")

cursor.close()
conn.close()