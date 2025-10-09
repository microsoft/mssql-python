from mssql_python import connect
from mssql_python import setup_logging
import os
import decimal

setup_logging('stdout')

# conn_str = os.getenv("DB_CONNECTION_STRING")
conn_str = "Server=Saumya;DATABASE=master;UID=sa;PWD=HappyPass1234;Trust_Connection=yes;TrustServerCertificate=yes;"

conn = connect(conn_str)

# conn.autocommit = True

cursor = conn.cursor()
cursor.execute("DROP TABLE IF EXISTS test_decimal")
cursor.execute("CREATE TABLE test_decimal (val DECIMAL(38, 10))")
cursor.execute("INSERT INTO test_decimal (val) VALUES (?)", (decimal.Decimal('1234567890.1234567890'),))
cursor.commit()
print("Inserted value")
cursor.execute("SELECT val FROM test_decimal")
row = cursor.fetchone()
print(f"Fetched value: {row[0]}")
print(f"Type of fetched value: {type(row[0])}")
assert row[0] == decimal.Decimal('1234567890.1234567890')