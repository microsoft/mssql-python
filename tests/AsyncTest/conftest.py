import pytest
import pytest_asyncio

from mssql_python.async_query import AsyncConnection


@pytest.fixture
def async_connection_string(conn_str):
    if not conn_str:
        pytest.fail("DB_CONNECTION_STRING is required for async integration tests")
    return conn_str


@pytest_asyncio.fixture
async def async_connection(async_connection_string):
    connection = await AsyncConnection.connect(async_connection_string)
    try:
        yield connection
    finally:
        if not connection.closed:
            await connection.close()


@pytest_asyncio.fixture
async def async_cursor(async_connection):
    cursor = async_connection.cursor()
    try:
        yield cursor
    finally:
        await cursor.close()
