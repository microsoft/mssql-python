# tests/test_cursor.py
import pytest

def test_cursor(cursor):
    # Check if the cursor is created
    assert cursor is not None, "Cursor should not be None"

def test_execute(cursor):
    # Ensure test_cursor passed
    test_cursor(cursor)
    try:
        # Execute a query to fetch database names and IDs
        cursor.execute("SELECT name, database_id from sys.databases;")
    except Exception as e:
        # Fail the test if an exception is raised
        pytest.fail(f"Execution of query failed: {e}")

def test_fetch_data(cursor):
    # Ensure test_cursor passed
    test_cursor(cursor)
    # Execute a query to fetch database names and IDs
    cursor.execute("SELECT name, database_id from sys.databases;")
    data = cursor.fetchall()
    # Check if the fetched data is not empty
    assert len(data) > 0, "Fetched data should not be empty"

def test_execute_invalid_query(cursor):
    # Ensure test_cursor passed
    test_cursor(cursor)
    with pytest.raises(Exception):
        # Check if executing an invalid query raises an exception
        cursor.execute("SELECT invalid_column from sys.databases;")