# tests/test_connection.py
import pytest
from mssql_python.connection import Connection

def test_connection_string(conn_str):
    # Check if the connection string is not None
    assert conn_str is not None, "Connection string should not be None"

def test_connection(db_connection):
    # Check if the database connection is established
    assert db_connection is not None, "Database connection variable should not be None"
    cursor = db_connection.cursor()
    assert cursor is not None, "Database connection failed - Cursor cannot be None"

def test_connection_close(db_connection):
    # Check if the database connection is closed
    db_connection.close()
    with pytest.raises(Exception):
        # Attempt to create a cursor after closing the connection should raise an exception
        db_connection.cursor()

def test_commit(db_connection):
    # Check if commit works without raising an exception
    try:
        db_connection.commit()
    except Exception as e:
        pytest.fail(f"Commit failed: {e}")

def test_rollback(db_connection):
    # Check if rollback works without raising an exception
    try:
        db_connection.rollback()
    except Exception as e:
        pytest.fail(f"Rollback failed: {e}")

def test_invalid_connection_string():
    # Check if initializing with an invalid connection string raises an exception
    with pytest.raises(Exception):
        Connection("invalid_connection_string")
