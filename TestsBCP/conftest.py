"""Pytest configuration and fixtures for BCP tests."""
import pytest
import os
import sys

# Add parent directory to path to import mssql_python
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import mssql_python


def get_connection_string():
    """Get connection string from environment variable."""
    conn_str = os.environ.get("DB_CONNECTION_STRING")
    if not conn_str:
        pytest.skip("DB_CONNECTION_STRING environment variable not set")
    return conn_str


@pytest.fixture
def connection():
    """Provide a connected database connection."""
    conn_str = get_connection_string()
    conn = mssql_python.connect(conn_str)
    yield conn
    conn.close()


@pytest.fixture
def cursor(connection):
    """Provide a database cursor."""
    cur = connection.cursor()
    yield cur
    cur.close()
