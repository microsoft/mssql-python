from mssql_python import connect
import os

conn_str = os.getenv("DB_CONNECTION_STRING")
conn = connect(conn_str)

conn.close()