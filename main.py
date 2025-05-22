from mssql_python import connect
from mssql_python import enable_pooling
from mssql_python import setup_logging
import os
import decimal
import time

setup_logging('stdout')

# conn_str = os.getenv("DB_CONNECTION_STRING")
conn_str = "Server=Saumya;DATABASE=master;UID=sa;PWD=HappyPass1234;Trust_Connection=yes;TrustServerCertificate=yes;"

enable_pooling(max_size=10, idle_timeout=300)
conn1 = connect(conn_str)

# conn.autocommit = True

cursor1 = conn1.cursor()
cursor1.execute("SELECT database_id, name from sys.databases;")
rows = cursor1.fetchone()
print (rows)

print(conn1._conn)
print("First time check")
# time.sleep(10) 

# cursor1.close()
# conn1.close()
print("Second time check")
# time.sleep(10)

conn2 = connect(conn_str)
cursor2 = conn2.cursor()
cursor2.execute("SELECT database_id, name from sys.databases;") 
row2 = cursor2.fetchone()
print(row2)

print(conn2._conn)
print("Third time check")
# time.sleep(10)


cursor2.close()
conn2.close()