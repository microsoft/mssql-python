from mssql_python import connect
from mssql_python import setup_logging
import os
import decimal

# setup_logging('stdout')

# conn = connect(conn_str)

# # conn.autocommit = True

# cursor = conn.cursor()
# cursor.execute("SELECT database_id, name from sys.databases;")
# rows = cursor.fetchall()

# for row in rows:
#     print(f"Database ID: {row[0]}, Name: {row[1]}")

# cursor.close()
# conn.close()

from mssql_python import connect, setup_logging

setup_logging('stdout')

# conn = connect(conn_str)

# cursor = conn.cursor()

# # Setup table for testing NVARCHAR(MAX)
# cursor.execute("IF OBJECT_ID('dbo.test_lob', 'U') IS NOT NULL DROP TABLE dbo.test_lob;")
# cursor.execute("CREATE TABLE dbo.test_lob (id INT IDENTITY PRIMARY KEY, long_text NVARCHAR(MAX));")

# # Large string (>4000 chars) to trigger streaming
# long_text = "A" * 10000

# # Insert using parameterized query
# cursor.execute("INSERT INTO dbo.test_lob (long_text) VALUES (?);", (long_text,))

# # Commit if needed
# conn.commit()

# # # Verify insert
# # cursor.execute("SELECT TOP 1 LEN(long_text) FROM dbo.test_lob ORDER BY id DESC;")
# # row = cursor.fetchone()
# print(f"Inserted string length")  # Expect 10000

# cursor.close()
# conn.close()


# Drop and recreate test table
# cursor.execute("""
# IF OBJECT_ID('dbo.LobTest', 'U') IS NOT NULL
#     DROP TABLE dbo.LobTest;
# CREATE TABLE dbo.LobTest (
#     id INT IDENTITY PRIMARY KEY,
#     vdata VARCHAR(MAX),
#     nvdata NVARCHAR(MAX)
# )
# """)
# conn.commit()

# # Test case 1: Insert small strings
# cursor.execute("INSERT INTO dbo.LobTest (vdata, nvdata) VALUES (?, ?)", "hello", "नमस्ते")
# print("Inserted small strings.")

# # Test case 2: Insert large VARCHAR(MAX)
# large_varchar = "A" * 100000  # 100k ASCII chars
# cursor.execute("INSERT INTO dbo.LobTest (vdata, nvdata) VALUES (?, ?)", large_varchar, None)
# print("Inserted large VARCHAR(MAX).")

# # Test case 3: Insert large NVARCHAR(MAX)
# large_nvarchar = "अ" * 50000  # 50k Unicode chars
# cursor.execute("INSERT INTO dbo.LobTest (vdata, nvdata) VALUES (?, ?)", None, large_nvarchar)
# print("Inserted large NVARCHAR(MAX).")

# # Commit
# conn.commit()

# print("All test inserts completed. Validate results in SSMS.")


# # Drop + recreate table
# cursor.execute("IF OBJECT_ID('dbo.VarBinMaxTest', 'U') IS NOT NULL DROP TABLE dbo.VarBinMaxTest")
# cursor.execute("CREATE TABLE dbo.VarBinMaxTest (id INT IDENTITY PRIMARY KEY, data VARBINARY(MAX))")
# conn.commit()

# # Insert small binary
# small_bin = b"hello world"
# cursor.execute("INSERT INTO dbo.VarBinMaxTest (data) VALUES (?)", [small_bin])

# # Insert large binary (100 KB)
# large_bin = bytes([7] * 100_000)   # 100 KB filled with 0x07
# cursor.execute("INSERT INTO dbo.VarBinMaxTest (data) VALUES (?)", [large_bin])

# conn.commit()
# cursor.close()
# conn.close()

# print("Inserted small and large VARBINARY(MAX) rows. Check in SSMS with:")
# print("  SELECT id, DATALENGTH(data) FROM dbo.VarBinMaxTest")

# def setup_table(cursor):
#     cursor.execute("IF OBJECT_ID('dbo.VarBinMaxTest', 'U') IS NOT NULL DROP TABLE dbo.VarBinMaxTest;")
#     cursor.execute("CREATE TABLE dbo.VarBinMaxTest (data VARBINARY(MAX));")
#     print("✅ VarBinMaxTest table created.")

# def test_varbinarymax(cursor):
#     # Small binary value
#     cursor.execute("INSERT INTO dbo.VarBinMaxTest (data) VALUES (?)", [b"\x01\x02\x03"])

#     # Large binary value (~150KB, triggers DAE)
#     cursor.execute("INSERT INTO dbo.VarBinMaxTest (data) VALUES (?)", [b"\x07" * 150_000])

#     # Empty binary
#     cursor.execute("INSERT INTO dbo.VarBinMaxTest (data) VALUES (?)", [b""])

#     # NULL
#     cursor.execute("INSERT INTO dbo.VarBinMaxTest (data) VALUES (?)", [None])

#     print("✅ VarBinMax insert tests executed. Verify with SSMS.")

# if __name__ == "__main__":
#     conn = connect(conn_str)
#     cursor = conn.cursor()
#     setup_table(cursor)
#     test_varbinarymax(cursor)
#     conn.commit()
#     print("🎉 VarBinMax test committed. Run: SELECT DATALENGTH(data), data FROM dbo.VarBinMaxTest")


# def setup_table(cursor):
#     cursor.execute("IF OBJECT_ID('dbo.NVarCharMaxTest', 'U') IS NOT NULL DROP TABLE dbo.NVarCharMaxTest;")
#     cursor.execute("CREATE TABLE dbo.NVarCharMaxTest (data NVARCHAR(MAX));")
#     print("✅ NVarCharMaxTest table created.")

# def test_nvarcharmax(cursor):
#     # Small Unicode value
#     cursor.execute("INSERT INTO dbo.NVarCharMaxTest (data) VALUES (?)", ["नमस्ते"])

#     # Large Unicode value (~350KB, triggers DAE)
#     cursor.execute("INSERT INTO dbo.NVarCharMaxTest (data) VALUES (?)", ["नमस्ते_" * 50_000])

#     # Empty Unicode string
#     cursor.execute("INSERT INTO dbo.NVarCharMaxTest (data) VALUES (?)", [""])

#     # NULL
#     cursor.execute("INSERT INTO dbo.NVarCharMaxTest (data) VALUES (?)", [None])

#     print("✅ NVarCharMax insert tests executed. Verify with SSMS.")

# if __name__ == "__main__":
#     conn = connect(conn_str)
#     cursor = conn.cursor()
#     setup_table(cursor)
#     test_nvarcharmax(cursor)
#     conn.commit()
#     print("🎉 NVarCharMax test committed. Run: SELECT LEN(data), data FROM dbo.NVarCharMaxTest")


def setup_table(cursor):
    cursor.execute("IF OBJECT_ID('dbo.VarCharMaxTest', 'U') IS NOT NULL DROP TABLE dbo.VarCharMaxTest;")
    cursor.execute("CREATE TABLE dbo.VarCharMaxTest (data VARCHAR(MAX));")
    print("✅ VarCharMaxTest table created.")

def test_varcharmax(cursor):
    # Small value
    cursor.execute("INSERT INTO dbo.VarCharMaxTest (data) VALUES (?)", ["Hello World"])

    # Large value (~200KB, triggers DAE)
    cursor.execute("INSERT INTO dbo.VarCharMaxTest (data) VALUES (?)", ["A" * 200_000])

    # Empty string
    cursor.execute("INSERT INTO dbo.VarCharMaxTest (data) VALUES (?)", [""])

    # NULL
    cursor.execute("INSERT INTO dbo.VarCharMaxTest (data) VALUES (?)", [None])

    print("✅ VarCharMax insert tests executed. Verify with SSMS.")

if __name__ == "__main__":
    conn = connect(conn_str)
    cursor = conn.cursor()
    setup_table(cursor)
    test_varcharmax(cursor)
    conn.commit()
    print("🎉 VarCharMax test committed. Run: SELECT LEN(data), data FROM dbo.VarCharMaxTest")