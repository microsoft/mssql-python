from mssql_python import connect
from mssql_python import setup_logging
import os
import decimal

setup_logging('stdout')

# conn_str = os.getenv("DB_CONNECTION_STRING")
# conn = connect(conn_str)

# # conn.autocommit = True

# cursor = conn.cursor()
# cursor.execute("SELECT database_id, name from sys.databases;")
# rows = cursor.fetchall()

# for row in rows:
#     print(f"Database ID: {row[0]}, Name: {row[1]}")

# cursor.close()
# conn.close()

conn_str = "Server=Saumya;DATABASE=master;UID=sa;PWD=HappyPass1234;Trust_Connection=yes;TrustServerCertificate=yes;"
db_connection = connect(conn_str)
db_connection.autocommit = True
values = ["Ω" * 4100, "漢" * 5000]
cursor = db_connection.cursor()
cursor.execute("CREATE TABLE #pytest_nvarcharmax (col NVARCHAR(MAX))")
db_connection.commit()

# --- use executemany for inserts ---
cursor.executemany(
    "INSERT INTO #pytest_nvarcharmax VALUES (?)",
    [(v,) for v in values],
)
db_connection.commit()

# --- fetchall ---
cursor.execute("SELECT col FROM #pytest_nvarcharmax ORDER BY LEN(col)")
rows = [r[0] for r in cursor.fetchall()]
assert rows == sorted(values, key=len)

# --- fetchone ---
cursor.execute("SELECT col FROM #pytest_nvarcharmax ORDER BY LEN(col)")
r1 = cursor.fetchone()[0]
r2 = cursor.fetchone()[0]
assert {r1, r2} == set(values)
assert cursor.fetchone() is None

# --- fetchmany ---
cursor.execute("SELECT col FROM #pytest_nvarcharmax ORDER BY LEN(col)")

batch = cursor.fetchmany(1)
assert set(r[0] for r in batch).issubset(set(values))

remaining = []
while True:
    rows = cursor.fetchmany(1)
    if not rows:
        break
    remaining.extend(rows)

all_fetched = [r[0] for r in batch + remaining]
assert set(all_fetched) == set(values)


# --- cleanup ---
cursor.execute("DROP TABLE #pytest_nvarcharmax")
db_connection.commit()
