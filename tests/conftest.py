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

# Phase 5: shared fixtures for the async POC suite. Kept in this top-level
# conftest so pytest auto-loads them without needing tests/ to be a package.
import pytest_asyncio
import mssql_python as _mssql_python


def is_qemu_emulated():
    """Detect if running under QEMU user-mode emulation (e.g. ARM64 on x86_64 host).

    QEMU reports CPU implementer 0x51 in /proc/cpuinfo. Native ARM64 hardware
    uses vendor-specific IDs (0x41 ARM, 0x61 Apple, etc.).
    """
    try:
        with open("/proc/cpuinfo") as f:
            for line in f:
                if line.startswith("CPU implementer") and "0x51" in line:
                    return True
    except (FileNotFoundError, PermissionError):
        pass
    return False


QEMU = is_qemu_emulated()


def is_azure_sql_connection(conn_str):
    """Helper function to detect if connection string is for Azure SQL Database"""
    if not conn_str:
        return False
    # Check if database.windows.net appears in the Server parameter
    conn_str_lower = conn_str.lower()
    # Look for Server= or server= followed by database.windows.net
    server_match = re.search(r"server\s*=\s*[^;]*database\.windows\.net", conn_str_lower)
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
            print(f"Database connection failed due to Timeout: {e}. Retrying in 60 seconds.")
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


# ---------------------------------------------------------------------------
# Async POC fixtures (used by tests/test_030_async_poc.py)
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def async_conn_str(conn_str):
    """Session-scoped connection string. Skips the whole async suite if the
    ``DB_CONNECTION_STRING`` env var is unset."""
    if not conn_str:
        pytest.skip("DB_CONNECTION_STRING is not set")
    return conn_str


@pytest_asyncio.fixture
async def async_connection(async_conn_str):
    """Function-scoped ``AsyncConnection`` — each test gets a fresh session so
    cancellation / close tests don't leak state between cases."""
    conn = await _mssql_python.connect_async(async_conn_str)
    try:
        yield conn
    finally:
        if not conn.closed:
            await conn.close()


@pytest_asyncio.fixture
async def async_cursor(async_connection):
    """Function-scoped ``AsyncCursor`` bound to :func:`async_connection`."""
    cur = async_connection.cursor()
    try:
        yield cur
    finally:
        if not cur.closed:
            await cur.close()
