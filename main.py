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

# import pyodbc

# --- Connection ---
conn_str = "DRIVER={ODBC Driver 18 for SQL Server};Server=Saumya;DATABASE=master;UID=sa;PWD=HappyPass1234;Trust_Connection=yes;TrustServerCertificate=yes;"
conn = connect(conn_str)
cursor = conn.cursor()

# --- Create a test table ---
# cursor.execute("""
# IF OBJECT_ID('dbo.TestXML', 'U') IS NOT NULL DROP TABLE dbo.TestXML;
# CREATE TABLE dbo.TestXML (
#     id INT IDENTITY PRIMARY KEY,
#     xml_col XML
# )
# """)
# conn.commit()

# # --- Test short XML insertion ---
# short_xml = "<root><item>123</item></root>"
# cursor.execute("INSERT INTO dbo.TestXML (xml_col) VALUES (?)", short_xml)
# conn.commit()

# # --- Test long XML insertion (simulate DAE / large XML) ---
# long_xml = "<root>" + "".join(f"<item>{i}</item>" for i in range(10000)) + "</root>"
# cursor.execute("INSERT INTO dbo.TestXML (xml_col) VALUES (?)", long_xml)
# conn.commit()

# # --- Fetch and verify ---
# cursor.execute("SELECT id, xml_col FROM dbo.TestXML ORDER BY id")
# rows = cursor.fetchall()

# for row in rows:
#     print(f"ID: {row.id}, XML Length: {len(str(row.xml_col))}")

# --- Clean up ---
# cursor.execute("DROP TABLE dbo.TestXML")
# conn.commit()
# cursor.close()
# conn.close()


       
SMALL_XML = "<root><item>1</item></root>"
LARGE_XML = "<root>" + "".join(f"<item>{i}</item>" for i in range(10000)) + "</root>"
EMPTY_XML = ""
INVALID_XML = "<root><item></root>"  # malformed


cursor.execute("CREATE TABLE #pytest_xml_empty_null (id INT PRIMARY KEY IDENTITY(1,1), xml_col XML NULL);")
conn.commit()

cursor.execute("INSERT INTO #pytest_xml_empty_null (xml_col) VALUES (?);", EMPTY_XML)
cursor.execute("INSERT INTO #pytest_xml_empty_null (xml_col) VALUES (?);", None)
conn.commit()

rows = [r[0] for r in cursor.execute("SELECT xml_col FROM #pytest_xml_empty_null ORDER BY id;").fetchall()]
print(rows)
assert rows[0] == EMPTY_XML
assert rows[1] is None

cursor.execute("DROP TABLE IF EXISTS #pytest_xml_empty_null;")
conn.commit()
