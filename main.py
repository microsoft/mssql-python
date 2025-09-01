from mssql_python import connect
from mssql_python import setup_logging
import os
import decimal
# import pyodbc

# setup_logging('stdout')

# conn_str = os.getenv("DB_CONNECTION_STRING")
conn_str = "Server=Saumya;DATABASE=master;UID=sa;PWD=HappyPass1234;Trust_Connection=yes;TrustServerCertificate=yes;"
conn = connect(conn_str)
# conn_str = "DRIVER={ODBC Driver 18 for SQL Server};Server=Saumya;DATABASE=master;UID=sa;PWD=HappyPass1234;Trust_Connection=yes;TrustServerCertificate=yes;"
# conn = pyodbc.connect(conn_str)
# conn.autocommit = True

cursor = conn.cursor()
# cursor.execute("SELECT database_id, name from sys.databases;")
# rows = cursor.fetchall()

# for row in rows:
#     print(f"Database ID: {row[0]}, Name: {row[1]}")

# cursor.execute("DROP TABLE IF EXISTS #pytest_nvarchar_chunk")
# cursor.execute("CREATE TABLE #pytest_nvarchar_chunk (col NVARCHAR(MAX))")
# conn.commit()

# chunk_size = 8192  # bytes
# # test_str = "😄" * ((chunk_size // 4) + 3)  # slightly > 1 chunk
# test_str = "😄" * 50000

# cursor.execute("INSERT INTO #pytest_nvarchar_chunk (col) VALUES (?)", [test_str])
# conn.commit()

# cursor.execute("CREATE TABLE #pytest_empty_batch (id INT, data NVARCHAR(50))")
# conn.commit()

# # Insert multiple rows with mix of empty and non-empty
# test_data = [
#     (1, ''),
#     (2, 'non-empty'),
#     (3, ''),
#     (4, 'another'),
#     (5, ''),
# ]
# cursor.executemany("INSERT INTO #pytest_empty_batch VALUES (?, ?)", test_data)
# conn.commit()

# # Test fetchmany with different batch sizes
# cursor.execute("SELECT id, data FROM #pytest_empty_batch ORDER BY id")

# # Fetch in batches of 2
# batch1 = cursor.fetchall()
# print(batch1)


# Drop & recreate table
cursor.execute("IF OBJECT_ID('dbo.money_test', 'U') IS NOT NULL DROP TABLE dbo.money_test;")
conn.commit()

cursor.execute("""
    CREATE TABLE money_test (
        id INT IDENTITY PRIMARY KEY,
        m MONEY,
        sm SMALLMONEY,
        d DECIMAL(19,4),
        n NUMERIC(10,4)
    )
""")
conn.commit()

# Insert valid rows covering MONEY & SMALLMONEY ranges
cursor.execute("""
    INSERT INTO money_test (m, sm, d, n) VALUES
        -- Max values
        (922337203685477.5807, 214748.3647, 9999999999999.9999, 1234.5678),
        -- Min values
        (-922337203685477.5808, -214748.3648, -9999999999999.9999, -1234.5678),
        -- Typical mid values
        (1234567.8901, 12345.6789, 42.4242, 3.1415),
        -- Nulls
        (NULL, NULL, NULL, NULL)
""")
conn.commit()

# Fetch rows one by one
cursor.execute("SELECT m, sm, d, n FROM money_test ORDER BY id")

while True:
    row = cursor.fetchone()
    if not row:
        break
    print("Row:", row)
    for idx, col in enumerate(row, 1):
        print(f"  col{idx}: {col!r} ({type(col)})")

# Roundtrip check with Decimal parameters
cursor.execute(
    "INSERT INTO money_test (m, sm, d, n) VALUES (?, ?, ?, ?)",
    (
        decimal.Decimal("123.4567"),
        decimal.Decimal("99.9999"),
        decimal.Decimal("42.4242"),
        decimal.Decimal("3.1415"),
    ),
)
conn.commit()

cursor.execute("SELECT TOP 1 m, sm, d, n FROM money_test ORDER BY id DESC")
row = cursor.fetchone()
print("Inserted decimal roundtrip ->", row, [type(c) for c in row])

cursor.close()
conn.close()