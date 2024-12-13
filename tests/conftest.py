import pytest
import os
from mssql_python import connect

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
    conn.close()

@pytest.fixture(scope="module")
def cursor(db_connection):
    cursor = db_connection.cursor()
    yield cursor

