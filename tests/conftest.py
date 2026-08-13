"""
This file contains fixtures for the tests in the mssql_python package.
Functions:
- pytest_configure: Add any necessary configuration.
- conn_str: Fixture to get the connection string from environment variables,
  wrapped so its password is not printed in pytest failure output.
- db_connection: Fixture to create and yield a database connection.
- cursor: Fixture to create and yield a cursor from the database connection.
- is_azure_sql_connection: Helper function to detect Azure SQL Database connections.
"""

import pytest
import os
import re
from mssql_python import connect
from mssql_python.connection_string_parser import sanitize_connection_string
import time


class _MaskedConnectionString(str):
    """A str that behaves like the connection string but never reveals its
    password when repr()'d.

    pytest prints every test argument in the failure header (``conn_str =
    '...'``) and uses repr() to do it, so a plain str fixture puts the live
    credential into CI logs on any failure in any test that takes conn_str,
    not just the ones asserting on connection strings. Masking repr() keeps
    the value fully usable while keeping the password out of that output.

    This has to be a subclass rather than a call to
    sanitize_connection_string() at the point of use: pytest reads repr() off
    the object it holds, str.__repr__ cannot be reassigned on the builtin, and
    sanitizing the fixture value itself would leave the tests unable to
    connect.
    """

    __slots__ = ()

    def __repr__(self):
        return repr(sanitize_connection_string(str(self)))


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
    return _MaskedConnectionString(conn_str) if conn_str else conn_str


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
