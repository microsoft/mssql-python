import os
import decimal


conn_str = os.getenv("DB_CONNECTION_STRING")
# print the connection string without pwd
if conn_str:
    secure_connection_str = ''
    for i in conn_str.split(';'):
        if i.lower().startswith('pwd='):
            secure_connection_str += 'pwd=******;'
        else:
            secure_connection_str += i + ';'
    print(f"Connecting to database with connection string: {secure_connection_str}")

from mssql_python import connect
from mssql_python import setup_logging
setup_logging('stdout')

try:
    conn = connect(conn_str)
except Exception as e:
    if 'Timeout' in str(e):
        print("retrying in 60 seconds...")
        import time
        time.sleep(60)
        conn = connect(conn_str)
    else:
        raise e

# conn.autocommit = True

cursor = conn.cursor()
cursor.execute("SELECT database_id, name from sys.databases;")
rows = cursor.fetchall()

for row in rows:
    print(f"Database ID: {row[0]}, Name: {row[1]}")

cursor.close()
conn.close()