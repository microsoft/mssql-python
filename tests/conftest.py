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
    try:
        conn = connect(conn_str)
    except Exception as e:
        # If connection has a Timeout Error, wait for 30 seconds and try again
        if "Timeout error" in str(e):
            print("Timeout Error. Retrying in 30 seconds...")
            time.sleep(30)
            conn = connect(conn_str)
        else:
            raise e
    yield conn
    conn.close()

@pytest.fixture(scope="module")
def cursor(db_connection):
    cursor = db_connection.cursor()
    yield cursor

