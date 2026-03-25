# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for bulkcopy auth field cleanup in cursor.py.

When cursor.bulkcopy() acquires an Azure AD token, it must strip stale
authentication/user_name/password keys from the pycore_context dict before
passing it to mssql_py_core.  The Rust validator rejects access_token
combined with those fields (ODBC parity).
"""

import pytest
import secrets
from unittest.mock import MagicMock, patch, PropertyMock

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
    cursor.closed = False
    cursor.hstmt = None
    return cursor


class TestBulkcopyAuthCleanup:
    """Verify cursor.bulkcopy strips stale auth fields after token acquisition."""

    @patch("mssql_python.cursor.get_settings")
    @patch("mssql_python.cursor.logger")
    def test_token_replaces_auth_fields(self, mock_logger, mock_settings):
        """access_token present ⇒ authentication, user_name, password removed."""
        mock_settings.return_value = MagicMock(logging=False)
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

    @patch("mssql_python.cursor.get_settings")
    @patch("mssql_python.cursor.logger")
    def test_no_auth_type_leaves_fields_intact(self, mock_logger, mock_settings):
        """No _auth_type ⇒ credentials pass through unchanged (SQL auth path)."""
        mock_settings.return_value = MagicMock(logging=False)
        mock_logger.is_debug_enabled = False

        cursor = _make_cursor(
            "Server=tcp:test.database.windows.net;Database=testdb;"
            "UID=sa;PWD=password123",
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
        assert captured_context.get("password") == "password123"
