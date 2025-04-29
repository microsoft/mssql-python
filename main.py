from mssql_python import connect
from mssql_python import setup_logging
import os
import decimal

setup_logging('stdout')

conn_str = os.getenv("DB_CONNECTION_STRING")
conn = connect(conn_str)

# conn.autocommit = True

cursor = conn.cursor()
cursor.execute("SELECT database_id, name from sys.databases;")
row = cursor.fetchmany(1)
print(row)

cursor.execute("DROP TABLE IF EXISTS main_single_column")

# cursor.execute("CREATE TABLE main_single_column (float_column FLOAT)")
# cursor.executemany("INSERT INTO main_single_column (float_column) VALUES (?)", [[12.34], [1.234], [0.125], [0.0125], [0.00125], [23243243232.432432432], [0.247985732852735032750973209750]])
# cursor.execute("SELECT * FROM main_single_column")
# row = cursor.fetchall()
# print(row)
# print(len(row))

import time
cursor.execute("CREATE TABLE main_single_column (decimal_column NUMERIC(10, 4))")
# time.sleep(45)

cursor.execute("INSERT INTO main_single_column (decimal_column) VALUES (?)", [decimal.Decimal(123.45).quantize(decimal.Decimal('0.00'))])
cursor.execute("SELECT * FROM main_single_column")
row = cursor.fetchone()[0]

print(row)
print(row.val)
print(row.precision)
print(row.scale)
print(row.sign)

cursor.close()
conn.close()
