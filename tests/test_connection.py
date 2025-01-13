"""
This file contains tests for the Connection class.
Functions:
- test_connection_string: Check if the connection string is not None.
- test_connection: Check if the database connection is established.
- test_connection_close: Check if the database connection is closed.
- test_commit: Make a transaction and commit.
- test_rollback: Make a transaction and rollback.
- test_invalid_connection_string: Check if initializing with an invalid connection string raises an exception.
Note: The cursor function is not yet implemented, so related tests are commented out.
"""

import pytest
from mssql_python.connection import Connection

def test_connection_string(conn_str):
    # Check if the connection string is not None
    assert conn_str is not None, "Connection string should not be None"

def test_connection(db_connection):
    # Check if the database connection is established
    assert db_connection is not None, "Database connection variable should not be None"
    # cursor = db_connection.cursor()
    # assert cursor is not None, "Database connection failed - Cursor cannot be None"

def test_commit(db_connection):
    db_connection.commit()
#     # Make a transaction and commit
#     cursor = db_connection.cursor()
#     cursor.execute("CREATE TABLE test_commit (id INT PRIMARY KEY, value VARCHAR(50));")
#     cursor.execute("INSERT INTO test_commit (id, value) VALUES (1, 'test');")
#     try:
#         db_connection.commit()
#         cursor.execute("SELECT * FROM test_commit WHERE id = 1;")
#         result = cursor.fetchone()
#         assert result is not None, "Commit failed: No data found"
#         assert result[1] == 'test', "Commit failed: Incorrect data"
#     except Exception as e:
#         pytest.fail(f"Commit failed: {e}")
#     finally:
#         cursor.execute("DROP TABLE test_commit;")

def test_rollback(db_connection):
    db_connection.rollback()
#     # Make a transaction and rollback
#     cursor = db_connection.cursor()
#     cursor.execute("CREATE TABLE test_rollback (id INT PRIMARY KEY, value VARCHAR(50));")
#     cursor.execute("INSERT INTO test_rollback (id, value) VALUES (1, 'test');")
#     try:
#         db_connection.rollback()
#         cursor.execute("SELECT * FROM test_rollback WHERE id = 1;")
#         result = cursor.fetchone()
#         assert result is None, "Rollback failed: Data found"
#     except Exception as e:
#         pytest.fail(f"Rollback failed: {e}")
#     finally:
#         cursor.execute("DROP TABLE test_rollback;")

def test_invalid_connection_string():
    # Check if initializing with an invalid connection string raises an exception
    with pytest.raises(Exception):
        Connection("invalid_connection_string")

def test_connection_close(db_connection):
    # Check if the database connection is closed
    db_connection.close()
    # with pytest.raises(Exception):
    #     # Attempt to create a cursor after closing the connection should raise an exception
    #     db_connection.cursor()
