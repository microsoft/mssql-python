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
import platform
from mssql_python import connect
import time


def is_qemu_emulated():
    """Detect whether we are an ARM64 process running under QEMU emulation.

    Covers the two QEMU modes that appear in CI:

    * ``qemu-user`` (``multiarch/qemu-user-static``, used by the Linux ARM64
      pipeline legs): an aarch64 process is emulated on an x86_64 host. uname /
      ``platform.machine()`` is faked to ``aarch64``, but ``/proc/cpuinfo`` is the
      HOST's x86_64 info, so it has NO ARM-only ``CPU implementer`` field. Native
      ARM64 hardware always exposes one (0x41 ARM, 0x61 Apple, 0xc0 Ampere, ...).
    * ``qemu-system`` (full-system ARM64 emulation): reports CPU implementer 0x51.

    Native ARM64 hardware (Graviton, Apple silicon, ...) and x86 are NOT flagged.

    WHY THIS EXISTS / KNOWN-FAILURE CASE:
    Several subprocess *shutdown* tests (this file's callers in
    ``test_013_SqlHandle_free_shutdown`` and ``test_024_context_manager_transaction``)
    intermittently SIGSEGV under QEMU. That crash is a REAL but latent
    use-after-free in ODBC handle-teardown ORDERING, not a QEMU bug: an unclosed
    DBC (connection) handle can be finalized by Python's GC at interpreter shutdown
    AFTER the process-lifetime ENV handle is gone, so ``SQLFreeHandle(DBC)`` reaches
    into freed ENV memory. On native platforms it is masked -- the freed page stays
    mapped and the ``is_python_finalizing()`` skip in
    ``pybind/ddbc_bindings.cpp::SqlHandle::free`` usually wins the race -- but QEMU's
    aggressive page reuse plus altered GC/thread timing turns it into a hard fault,
    and only some runs lose the race (hence the CI flakiness / green-on-rerun).
    We skip those tests under QEMU (a config essentially nobody ships a driver on)
    instead of eating CI reruns; the underlying teardown-order UAF is tracked
    separately and is unaffected by this skip. Remove/narrow this skip once that
    root-cause bug is fixed (native legs still run these tests, so a regression
    there is not masked).
    """
    machine = platform.machine().lower()
    if machine not in ("aarch64", "arm64"):
        return False  # only an ARM64 process can be QEMU-emulated in our matrix

    try:
        with open("/proc/cpuinfo") as f:
            cpuinfo = f.read()
    except (FileNotFoundError, PermissionError):
        return False

    # qemu-system ARM64 advertises CPU implementer 0x51 (anchored to the field to
    # avoid matching an unrelated 0x51 elsewhere). Real Qualcomm ARM64 also uses
    # 0x51, but our CI matrix has no native Qualcomm runner, so this is safe here.
    if re.search(r"^CPU implementer\s*:\s*0x51", cpuinfo, re.MULTILINE):
        return True
    # qemu-user passes through the x86_64 host's /proc/cpuinfo, which lacks the
    # ARM-only "CPU implementer" field that every native ARM64 kernel exposes.
    if "CPU implementer" not in cpuinfo:
        return True
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
