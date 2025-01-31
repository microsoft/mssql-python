from mssql_python import connect
import os

conn_str = os.getenv("DB_CONNECTION_STRING")
conn = connect(conn_str)

cursor = conn.cursor()
cursor.execute("SELECT database_id, name from sys.databases;")
row = cursor.fetchmany(1)
print(row)

cursor.execute("SELECT database_id, name from sys.databases;")
row = cursor.fetchone()
print(row)

cursor.execute("SELECT database_id, name from sys.databases;")
row = cursor.fetchall()
print(row)

cursor.close()
conn.close()