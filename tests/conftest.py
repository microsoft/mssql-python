"""
This file contains fixtures for the tests in the mssql_python package.
Functions:
- pytest_configure: Add any necessary configuration.
- conn_str: Fixture to get the connection string from environment variables.
- db_connection: Fixture to create and yield a database connection.
- cursor: Fixture to create and yield a cursor from the database connection.
"""

import pytest
import os
from mssql_python import connect
import time

def pytest_configure(config):
    # Add any necessary configuration here
    pass

@pytest.fixture(scope='module')
def conn_str():
    conn_str = os.getenv('DB_CONNECTION_STRING')
    return conn_str

@pytest.fixture(scope="module")
def db_connection(conn_str):
    conn = connect(conn_str)
    yield conn

@pytest.fixture(scope="module")
def cursor(db_connection):
    cursor = db_connection.cursor()
    yield cursor

