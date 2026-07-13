# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for bulkcopy auth field cleanup in cursor.py.

When cursor.bulkcopy() acquires an Azure AD token, it must strip stale
authentication/user_name/password keys from the pycore_context dict before
passing it to mssql_py_core.  The Rust validator rejects access_token
combined with those fields (ODBC parity).
"""

import secrets
from enum import IntEnum
from unittest.mock import MagicMock, patch

SAMPLE_TOKEN = secrets.token_hex(44)


def _make_cursor(connection_str, auth_type):
    """Build a mock Cursor with just enough wiring for bulkcopy's auth path."""
    from mssql_python.cursor import Cursor

    mock_conn = MagicMock()
    mock_conn.connection_str = connection_str
    mock_conn._auth_type = auth_type
    mock_conn._is_connected = True

    cursor = Cursor.__new__(Cursor)
    cursor._connection = mock_conn
    cursor._timeout = 0
    cursor.closed = False
    cursor.hstmt = None
    return cursor


class TestBulkcopyAuthCleanup:
    """Verify cursor.bulkcopy strips stale auth fields after token acquisition."""

    @patch("mssql_python.cursor.logger")
    def test_token_replaces_auth_fields(self, mock_logger):
        """access_token present ⇒ authentication, user_name, password removed."""
        mock_logger.is_debug_enabled = False

        cursor = _make_cursor(
            "Server=tcp:test.database.windows.net;Database=testdb;"
            "Authentication=ActiveDirectoryDefault;UID=user@test.com;PWD=secret",
            "activedirectorydefault",
        )

        captured_context = {}

        mock_pycore_cursor = MagicMock()
        mock_pycore_cursor.bulkcopy.return_value = {
            "rows_copied": 1,
            "batch_count": 1,
            "elapsed_time": 0.1,
        }
        mock_pycore_conn = MagicMock()
        mock_pycore_conn.cursor.return_value = mock_pycore_cursor

        def capture_context(ctx, **kwargs):
            captured_context.update(ctx)
            return mock_pycore_conn

        mock_pycore_module = MagicMock()
        mock_pycore_module.PyCoreConnection = capture_context

        with (
            patch.dict("sys.modules", {"mssql_py_core": mock_pycore_module}),
            patch("mssql_python.auth.AADAuth.get_raw_token", return_value=SAMPLE_TOKEN),
        ):
            cursor.bulkcopy("dbo.test_table", [(1, "row")], timeout=10)

        assert captured_context.get("access_token") == SAMPLE_TOKEN
        assert "authentication" not in captured_context
        assert "user_name" not in captured_context
        assert "password" not in captured_context

    @patch("mssql_python.cursor.logger")
    def test_no_auth_type_leaves_fields_intact(self, mock_logger):
        """No _auth_type ⇒ credentials pass through unchanged (SQL auth path)."""
        mock_logger.is_debug_enabled = False

        cursor = _make_cursor(
            "Server=localhost;Database=testdb;" "UID=sa;PWD=mypwd",
            None,  # no AD auth
        )

        captured_context = {}

        mock_pycore_cursor = MagicMock()
        mock_pycore_cursor.bulkcopy.return_value = {
            "rows_copied": 1,
            "batch_count": 1,
            "elapsed_time": 0.1,
        }
        mock_pycore_conn = MagicMock()
        mock_pycore_conn.cursor.return_value = mock_pycore_cursor

        def capture_context(ctx, **kwargs):
            captured_context.update(ctx)
            return mock_pycore_conn

        mock_pycore_module = MagicMock()
        mock_pycore_module.PyCoreConnection = capture_context

        with patch.dict("sys.modules", {"mssql_py_core": mock_pycore_module}):
            cursor.bulkcopy("dbo.test_table", [(1, "row")], timeout=10)

        assert "access_token" not in captured_context
        assert captured_context.get("user_name") == "sa"
        assert captured_context.get("password") == "mypwd"


def _capture_bulkcopy_context(cursor):
    """Run bulkcopy with a mocked pycore module and return the captured context."""
    captured_context = {}

    mock_pycore_cursor = MagicMock()
    mock_pycore_cursor.bulkcopy.return_value = {
        "rows_copied": 1,
        "batch_count": 1,
        "elapsed_time": 0.1,
    }
    mock_pycore_conn = MagicMock()
    mock_pycore_conn.cursor.return_value = mock_pycore_cursor

    def capture_context(ctx, **kwargs):
        captured_context.update(ctx)
        return mock_pycore_conn

    mock_pycore_module = MagicMock()
    mock_pycore_module.PyCoreConnection = capture_context

    with patch.dict("sys.modules", {"mssql_py_core": mock_pycore_module}):
        cursor.bulkcopy("dbo.test_table", [(1, "row")], timeout=10)

    return captured_context


class TestBulkcopyConnectTimeout:
    """Verify cursor.bulkcopy forwards the cursor timeout to pycore (issue #626)."""

    @patch("mssql_python.cursor.logger")
    def test_positive_timeout_forwarded(self, mock_logger):
        """cursor._timeout > 0 ⇒ connect_timeout reaches pycore, overriding 15s."""
        mock_logger.is_debug_enabled = False
        cursor = _make_cursor("Server=localhost;Database=testdb;UID=sa;PWD=pwd", None)
        cursor._timeout = 30

        captured = _capture_bulkcopy_context(cursor)

        assert captured.get("connect_timeout") == 30

    @patch("mssql_python.cursor.logger")
    def test_zero_timeout_not_forwarded(self, mock_logger):
        """cursor._timeout == 0 ⇒ no override, pycore keeps its default."""
        mock_logger.is_debug_enabled = False
        cursor = _make_cursor("Server=localhost;Database=testdb;UID=sa;PWD=pwd", None)
        cursor._timeout = 0

        captured = _capture_bulkcopy_context(cursor)

        assert "connect_timeout" not in captured

    @patch("mssql_python.cursor.logger")
    def test_uses_cursor_snapshot_not_live_connection(self, mock_logger):
        """timeout is the cursor snapshot; later connection changes don't apply."""
        mock_logger.is_debug_enabled = False
        cursor = _make_cursor("Server=localhost;Database=testdb;UID=sa;PWD=pwd", None)
        cursor._timeout = 45
        cursor._connection.timeout = 99  # changed after cursor creation, must be ignored

        captured = _capture_bulkcopy_context(cursor)

        assert captured.get("connect_timeout") == 45

    @patch("mssql_python.cursor.logger")
    def test_intenum_timeout_forwarded_as_plain_int(self, mock_logger):
        """IntEnum (accepted by the public setter) is forwarded, normalised to int."""
        mock_logger.is_debug_enabled = False

        class _T(IntEnum):
            thirty = 30

        cursor = _make_cursor("Server=localhost;Database=testdb;UID=sa;PWD=pwd", None)
        cursor._timeout = _T.thirty

        captured = _capture_bulkcopy_context(cursor)

        assert captured.get("connect_timeout") == 30
        assert type(captured.get("connect_timeout")) is int

    @patch("mssql_python.cursor.logger")
    def test_bool_timeout_not_forwarded(self, mock_logger):
        """bool is a subclass of int but must not be treated as a timeout."""
        mock_logger.is_debug_enabled = False
        cursor = _make_cursor("Server=localhost;Database=testdb;UID=sa;PWD=pwd", None)
        cursor._timeout = True

        captured = _capture_bulkcopy_context(cursor)

        assert "connect_timeout" not in captured
