"""
Integration tests for mssql_python logging with real database operations.
Tests that logging statements in connection.py, cursor.py, etc. work correctly.
"""

import pytest
import os
import logging
import tempfile
import shutil
from mssql_python import connect
from mssql_python.logging import setup_logging, logger


# Skip all tests if no database connection string available
pytestmark = pytest.mark.skipif(
    not os.getenv("DB_CONNECTION_STRING"), reason="Database connection string not provided"
)


@pytest.fixture
def temp_log_dir():
    """Create a temporary directory for log files"""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def cleanup_logger():
    """Reset logger state and cleanup log files"""
    # Disable and clear
    logger._logger.setLevel(logging.CRITICAL)
    for handler in logger._logger.handlers[:]:
        handler.close()
        logger._logger.removeHandler(handler)
    logger._handlers_initialized = False
    logger._custom_log_path = None

    log_dir = os.path.join(os.getcwd(), "mssql_python_logs")
    if os.path.exists(log_dir):
        shutil.rmtree(log_dir, ignore_errors=True)

    yield

    # Cleanup after
    logger._logger.setLevel(logging.CRITICAL)
    for handler in logger._logger.handlers[:]:
        handler.close()
        logger._logger.removeHandler(handler)
    logger._handlers_initialized = False

    if os.path.exists(log_dir):
        shutil.rmtree(log_dir, ignore_errors=True)


@pytest.fixture
def conn_str():
    """Get connection string from environment"""
    return os.getenv("DB_CONNECTION_STRING")


class TestConnectionLogging:
    """Test logging during connection operations"""

    def test_connection_logs_sanitized_connection_string(
        self, cleanup_logger, temp_log_dir, conn_str
    ):
        """Connection should log sanitized connection string"""
        log_file = os.path.join(temp_log_dir, "conn_test.log")
        setup_logging(log_file_path=log_file)

        conn = connect(conn_str)
        conn.close()

        with open(log_file, "r") as f:
            content = f.read()

        # Should contain "Final connection string" log
        assert "Final connection string" in content

        # Should have sanitized password
        assert "PWD=***" in content or "Password=***" in content

        # Should NOT contain actual password (if there was one)
        # We can't check specific password here since we don't know it

    def test_connection_close_logging(self, cleanup_logger, temp_log_dir, conn_str):
        """Connection close should log success message"""
        log_file = os.path.join(temp_log_dir, "close_test.log")
        setup_logging(log_file_path=log_file)

        conn = connect(conn_str)
        conn.close()

        with open(log_file, "r") as f:
            content = f.read()

        assert "Connection closed successfully" in content

    def test_transaction_commit_logging(self, cleanup_logger, temp_log_dir, conn_str):
        """Transaction commit should log"""
        log_file = os.path.join(temp_log_dir, "commit_test.log")
        setup_logging(log_file_path=log_file)

        conn = connect(conn_str, autocommit=False)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        conn.commit()
        cursor.close()
        conn.close()

        with open(log_file, "r") as f:
            content = f.read()

        assert "Transaction committed successfully" in content

    def test_transaction_rollback_logging(self, cleanup_logger, temp_log_dir, conn_str):
        """Transaction rollback should log"""
        log_file = os.path.join(temp_log_dir, "rollback_test.log")
        setup_logging(log_file_path=log_file)

        conn = connect(conn_str, autocommit=False)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        conn.rollback()
        cursor.close()
        conn.close()

        with open(log_file, "r") as f:
            content = f.read()

        assert "Transaction rolled back successfully" in content


class TestCursorLogging:
    """Test logging during cursor operations"""

    def test_cursor_execute_logging(self, cleanup_logger, temp_log_dir, conn_str):
        """Cursor execute should log query"""
        log_file = os.path.join(temp_log_dir, "execute_test.log")
        setup_logging(log_file_path=log_file)

        conn = connect(conn_str)
        cursor = conn.cursor()
        cursor.execute("SELECT database_id, name FROM sys.databases")
        cursor.close()
        conn.close()

        with open(log_file, "r") as f:
            content = f.read()

        # Should contain execute debug logs
        assert "execute: Starting" in content or "Executing query" in content

    def test_cursor_fetchall_logging(self, cleanup_logger, temp_log_dir, conn_str):
        """Cursor fetchall should have DEBUG logs"""
        log_file = os.path.join(temp_log_dir, "fetch_test.log")
        setup_logging(log_file_path=log_file)

        conn = connect(conn_str)
        cursor = conn.cursor()
        cursor.execute("SELECT database_id, name FROM sys.databases")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        with open(log_file, "r") as f:
            content = f.read()

        # Should contain fetch-related logs
        assert "FetchAll" in content or "Fetching" in content


class TestErrorLogging:
    """Test error logging and exception raising"""

    def test_connection_error_logs_and_raises(self, cleanup_logger, temp_log_dir):
        """Connection error should log ERROR and raise exception"""
        log_file = os.path.join(temp_log_dir, "error_test.log")
        setup_logging(log_file_path=log_file)

        with pytest.raises(Exception):  # Will raise some connection error
            conn = connect("Server=invalid_server;Database=test")

        with open(log_file, "r") as f:
            content = f.read()

        # Should have ERROR level logs
        assert "ERROR" in content

    def test_invalid_query_logs_error(self, cleanup_logger, temp_log_dir, conn_str):
        """Invalid query should log error"""
        log_file = os.path.join(temp_log_dir, "query_error_test.log")
        setup_logging(log_file_path=log_file)

        conn = connect(conn_str)
        cursor = conn.cursor()

        try:
            cursor.execute("SELECT * FROM nonexistent_table_xyz")
        except Exception:
            pass  # Expected to fail

        cursor.close()
        conn.close()

        with open(log_file, "r") as f:
            content = f.read()

        # Should contain error-related logs
        # Note: The actual error might be caught and logged at different levels
        assert "ERROR" in content or "WARNING" in content


class TestLogLevelsInPractice:
    """Test that appropriate log levels are used in real operations"""

    def test_debug_logs_for_normal_operations(self, cleanup_logger, temp_log_dir, conn_str):
        """Normal operations should use DEBUG level"""
        log_file = os.path.join(temp_log_dir, "levels_test.log")
        setup_logging(log_file_path=log_file)

        conn = connect(conn_str)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        conn.close()

        with open(log_file, "r") as f:
            lines = f.readlines()

        # Count log levels
        debug_count = sum(1 for line in lines if ", DEBUG," in line)
        info_count = sum(1 for line in lines if ", INFO," in line)

        # Should have many DEBUG logs
        assert debug_count > 0

        # Should have some INFO logs (connection string, close, etc.)
        assert info_count > 0

    def test_info_logs_for_significant_events(self, cleanup_logger, temp_log_dir, conn_str):
        """Significant events should use INFO level"""
        log_file = os.path.join(temp_log_dir, "info_test.log")
        setup_logging(log_file_path=log_file)

        conn = connect(conn_str)
        conn.close()

        with open(log_file, "r") as f:
            content = f.read()

        # These should be INFO level
        info_messages = ["Final connection string", "Connection closed successfully"]

        for msg in info_messages:
            if msg in content:
                # Verify it's at INFO level
                lines = content.split("\n")
                for line in lines:
                    if msg in line:
                        assert ", INFO," in line
                        break


class TestThreadSafety:
    """Test logging in multi-threaded scenarios"""

    @pytest.mark.skip(
        reason="Threading test causes pytest GC issues - thread ID functionality validated in unit tests"
    )
    def test_concurrent_connections_have_different_thread_ids(
        self, cleanup_logger, temp_log_dir, conn_str
    ):
        """Concurrent operations should log different thread IDs - runs in subprocess to avoid pytest GC issues"""
        import subprocess
        import sys

        log_file = os.path.join(temp_log_dir, "threads_test.log")

        # Get the project root directory
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

        # Run threading test in subprocess to avoid interfering with pytest GC
        test_script = f"""
import sys
sys.path.insert(0, r'{project_root}')

import os
import mssql_python
import threading

log_file = r'{log_file}'
mssql_python.setup_logging(log_file_path=log_file)

def worker():
    conn = mssql_python.connect(r'{conn_str}')
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    cursor.close()
    conn.close()

threads = [threading.Thread(target=worker) for _ in range(3)]
for t in threads:
    t.start()
for t in threads:
    t.join()

# Check the log file
with open(log_file, 'r') as f:
    lines = f.readlines()

thread_ids = set()
for line in lines:
    if not line.startswith('#') and 'Timestamp' not in line:
        parts = [p.strip() for p in line.split(',')]
        if len(parts) >= 2 and parts[1].isdigit():
            thread_ids.add(parts[1])

assert len(thread_ids) >= 2, f"Expected at least 2 thread IDs, got {{len(thread_ids)}}"
print(f"SUCCESS: Found {{len(thread_ids)}} different thread IDs")
"""

        result = subprocess.run(
            [sys.executable, "-c", test_script], capture_output=True, text=True, timeout=30
        )

        if result.returncode != 0:
            print(f"STDOUT: {result.stdout}")
            print(f"STDERR: {result.stderr}")
            pytest.fail(f"Subprocess failed with code {result.returncode}: {result.stderr}")

        assert "SUCCESS" in result.stdout


class TestDDBCLogging:
    """Test that DDBC (C++) logs are captured"""

    def test_ddbc_logs_appear_in_output(self, cleanup_logger, temp_log_dir, conn_str):
        """DDBC logs should appear with [DDBC] source"""
        log_file = os.path.join(temp_log_dir, "ddbc_test.log")
        setup_logging(log_file_path=log_file)

        conn = connect(conn_str)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        conn.close()

        with open(log_file, "r") as f:
            content = f.read()

        # Should contain DDBC logs (from C++ layer)
        assert "DDBC" in content or "[DDBC]" in content


class TestPasswordSanitizationIntegration:
    """Test password sanitization with real connection strings"""

    def test_connection_string_passwords_sanitized(self, cleanup_logger, temp_log_dir):
        """Passwords in connection strings should be sanitized in logs"""
        log_file = os.path.join(temp_log_dir, "sanitize_test.log")
        setup_logging(log_file_path=log_file)

        # Use an invalid connection string with a fake password
        try:
            conn = connect("Server=localhost;Database=test;PWD=MySecretPassword123")
        except Exception:
            pass  # Expected to fail

        with open(log_file, "r") as f:
            content = f.read()

        # Password should be sanitized
        assert "PWD=***" in content
        assert "MySecretPassword123" not in content
