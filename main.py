# from mssql_python import connect
# from mssql_python import setup_logging
# import os
# import decimal

# setup_logging('stdout')

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

# from mssql_python import connect  # Replace with actual module name
# from mssql_python import setup_logging
# # setup_logging('stdout')

# def test_large_nvarchar():
#     # Replace with your actual connection string
#     conn_str = "Server=Saumya;DATABASE=master;UID=sa;PWD=HappyPass1234;Trust_Connection=yes;TrustServerCertificate=yes;"

#     # Connect
#     conn = connect(conn_str)
#     cursor = conn.cursor()

#     # Prepare test table
#     cursor.execute("""
#         IF OBJECT_ID('dbo.LargeTextTest', 'U') IS NOT NULL DROP TABLE dbo.LargeTextTest;
#         CREATE TABLE dbo.LargeTextTest (
#             id INT PRIMARY KEY IDENTITY(1,1),
#             content NVARCHAR(MAX)
#         )
#     """)
#     conn.commit()

#     # Prepare large string (~1MB)
#     large_text = "AB" * (1024 * 1024)  # 1MB of 'A'

#     # Insert using parameterized query (should trigger DAE path)
#     cursor.execute("INSERT INTO dbo.LargeTextTest (content) VALUES (?)", [large_text])
#     conn.commit()

#     print("✅ Large text inserted successfully!")

#     # Read back
#     cursor.execute("SELECT DATALENGTH(content), LEFT(content, 100) FROM dbo.LargeTextTest")
#     row = cursor.fetchone()
#     if row:
#         size_bytes, preview = row
#         print(f"Read back size: {size_bytes} bytes")
#         print(f"Preview: {preview[:50]}...")
#     else:
#         print("❌ No row returned!")

# if __name__ == "__main__":
#     test_large_nvarchar()

from mssql_python import connect, setup_logging

# setup_logging('stdout')
def test_small_nvarchar():
    conn_str = "Server=Saumya;DATABASE=master;UID=sa;PWD=HappyPass1234;Trust_Connection=yes;TrustServerCertificate=yes;"
    conn = connect(conn_str)
    cursor = conn.cursor()

    cursor.execute("""
        IF OBJECT_ID('dbo.SmallTextTest', 'U') IS NOT NULL DROP TABLE dbo.SmallTextTest;
        CREATE TABLE dbo.SmallTextTest (
            id INT PRIMARY KEY IDENTITY(1,1),
            content NVARCHAR(100)  -- small, will use single-fetch path
        )
    """)
    conn.commit()

    small_text = "Hello, this is a short test string ✅"
    cursor.execute("INSERT INTO dbo.SmallTextTest (content) VALUES (?)", [small_text])
    conn.commit()
    print("✅ Small text inserted successfully!")

    cursor.execute("SELECT DATALENGTH(content), content FROM dbo.SmallTextTest")
    row = cursor.fetchone()
    if not row:
        print("❌ No row returned!")
        return

    size_bytes, full_text = row
    print(f"Read back size: {size_bytes} bytes")
    print(f"Fetched text: {full_text}")

    # --- validation ---
    if full_text == small_text:
        print("✅ Round-trip validation passed (small string).")
    else:
        print("❌ Round-trip validation FAILED!")
        print(f"Inserted: {small_text}")
        print(f"Fetched:  {full_text}")

def test_large_nvarchar():
    conn_str = "Server=Saumya;DATABASE=master;UID=sa;PWD=HappyPass1234;Trust_Connection=yes;TrustServerCertificate=yes;"


    conn = connect(conn_str)
    cursor = conn.cursor()

    cursor.execute("""
        IF OBJECT_ID('dbo.LargeTextTest', 'U') IS NOT NULL DROP TABLE dbo.LargeTextTest;
        CREATE TABLE dbo.LargeTextTest (
            id INT PRIMARY KEY IDENTITY(1,1),
            content NVARCHAR(MAX)
        )
    """)
    conn.commit()

    large_text = "AB" * (1024 * 1024)  # ~1MB string
    cursor.execute("INSERT INTO dbo.LargeTextTest (content) VALUES (?)", [large_text])
    conn.commit()
    print("✅ Large text inserted successfully!")

    cursor.execute("SELECT DATALENGTH(content), content FROM dbo.LargeTextTest")
    row = cursor.fetchone()
    if not row:
        print("❌ No row returned!")
        return

    size_bytes, full_text = row
    print(f"Read back size: {size_bytes} bytes")
    print(f"Preview: {full_text[:50]}...")

    # --- round-trip validation ---
    if full_text == large_text:
        print("✅ Round-trip validation passed (fetched text matches inserted text).")
    else:
        print("❌ Round-trip validation FAILED!")
        print(f"Inserted length: {len(large_text)}, Fetched length: {len(full_text)}")

def test_fetch_modes():
    conn_str = "Server=Saumya;DATABASE=master;UID=sa;PWD=HappyPass1234;Trust_Connection=yes;TrustServerCertificate=yes;"
    conn = connect(conn_str)
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS dbo.MixedTextTest;")
    cursor.execute("""
        CREATE TABLE dbo.MixedTextTest (
            id INT PRIMARY KEY IDENTITY(1,1),
            small_col NVARCHAR(100),
            large_col NVARCHAR(MAX)
        )
    """)
    conn.commit()

    small_text = "Short string ✅"
    large_text = "AB" * (1024 * 1024)  # ~2MB text

    cursor.execute("INSERT INTO dbo.MixedTextTest (small_col, large_col) VALUES (?, ?)", [small_text, large_text])
    conn.commit()
    print("✅ Inserted mixed test row")

    # --- FetchOne ---
    cursor.execute("SELECT small_col, large_col FROM dbo.MixedTextTest")
    row = cursor.fetchone()
    print(f"FetchOne -> small len={len(row[0])}, large len={len(row[1])}")

    # --- FetchMany ---
    cursor.execute("SELECT small_col, large_col FROM dbo.MixedTextTest")
    rows = cursor.fetchmany(1)
    print(f"FetchMany -> got {len(rows)} row(s), small len={len(rows[0][0])}, large len={len(rows[0][1])}")

    # --- FetchAll ---
    cursor.execute("SELECT small_col, large_col FROM dbo.MixedTextTest")
    rows = cursor.fetchall()
    print(f"FetchAll -> got {len(rows)} row(s), small len={len(rows[0][0])}, large len={len(rows[0][1])}")

    # Round-trip validation
    assert row[0] == small_text, "Small text mismatch!"
    assert row[1] == large_text, "Large text mismatch!"
    print("✅ Round-trip validation passed for all fetch modes")

if __name__ == "__main__":
    test_large_nvarchar()
    test_small_nvarchar()
    test_fetch_modes()