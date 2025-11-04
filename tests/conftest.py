"""
This file contains fixtures for the tests in the mssql_python package.
Functions:
- pytest_configure: Add any necessary configuration.
- conn_str: Fixture to get the connection string from environment variables.
- db_connection: Fixture to create and yield a database connection.
- cursor: Fixture to create and yield a cursor from the database connection.
- is_azure_sql_connection: Helper function to detect Azure SQL Database connections.
"""

import pytest
import os
import re
from mssql_python import connect
import time


def is_azure_sql_connection(conn_str):
    """Helper function to detect if connection string is for Azure SQL Database"""
    if not conn_str:
        return False
    # Check if database.windows.net appears in the Server parameter
    conn_str_lower = conn_str.lower()
    # Look for Server= or server= followed by database.windows.net
    server_match = re.search(r'server\s*=\s*[^;]*database\.windows\.net', conn_str_lower)
    return server_match is not None


def pytest_configure(config):
    # Add any necessary configuration here
    pass


@pytest.fixture(scope="session")
def conn_str():
    conn_str = os.getenv("DB_CONNECTION_STRING")
    return conn_str


@pytest.fixture(scope="module")
def db_connection(conn_str):
    try:
        conn = connect(conn_str)
    except Exception as e:
        if "Timeout error" in str(e):
            print(
                f"Database connection failed due to Timeout: {e}. Retrying in 60 seconds."
            )
            time.sleep(60)
            conn = connect(conn_str)
        else:
            pytest.fail(f"Database connection failed: {e}")
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def cursor(db_connection):
    cursor = db_connection.cursor()
    yield cursor
    cursor.close()
