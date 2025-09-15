import pytest
from mssql_python import connect
from mssql_python import setup_logging
import os
import decimal

setup_logging('stdout')

# conn_str = os.getenv("DB_CONNECTION_STRING")
conn_str = "Server=Saumya;DATABASE=master;UID=sa;PWD=HappyPass1234;Trust_Connection=yes;TrustServerCertificate=yes;"

conn = connect(conn_str)
cursor = conn.cursor()

test_inputs = [
"Hello 😄",
"Flags 🇮🇳🇺🇸",
"Family 👨‍👩‍👧‍👦",
"Skin tone 👍🏽",
"Brain 🧠",
"Ice 🧊",
"Melting face 🫠",
"Accented éüñç",
"Chinese: 中文",
"Japanese: 日本語",
"Hello 🚀 World",
"admin🔒user",
"1🚀' OR '1'='1",
]

cursor.execute("""
CREATE TABLE #pytest_emoji_test (
id INT IDENTITY PRIMARY KEY,
content NVARCHAR(MAX)
);
""")
conn.commit()

for text in test_inputs:
    try:
        cursor.execute("INSERT INTO #pytest_emoji_test (content) OUTPUT INSERTED.id VALUES (?)", [text])
        inserted_id = cursor.fetchone()[0]
        cursor.execute("SELECT content FROM #pytest_emoji_test WHERE id = ?", [inserted_id])
        result = cursor.fetchone()
        assert result is not None, f"No row returned for ID {inserted_id}"
        assert result[0] == text, f"Mismatch! Sent: {text}, Got: {result[0]}"
        print(f"Test passed for input: {repr(text)}")

    except Exception as e:
        print(f"Error for input {repr(text)}: {e}")
# conn.autocommit = True


# cursor.execute("SELECT database_id, name from sys.databases;")
# rows = cursor.fetchall()

# for row in rows:
#     print(f"Database ID: {row[0]}, Name: {row[1]}")

cursor.close()
conn.close()