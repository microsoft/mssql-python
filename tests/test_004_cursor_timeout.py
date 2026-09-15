"""Regression tests for query-timeout statement-handle routing."""

from unittest.mock import Mock

import pytest

from mssql_python import ddbc_bindings
from mssql_python.constants import ConstantsDDBC
from mssql_python.cursor import Cursor, logger


@pytest.mark.parametrize("metadata_handle", [False, True], ids=["default", "metadata"])
@pytest.mark.parametrize("timeout", [0, 2], ids=["disabled", "enabled"])
def test_set_timeout_uses_target_handle(monkeypatch, metadata_handle, timeout):
    cursor = Cursor.__new__(Cursor)
    cursor.hstmt = object()
    cursor._timeout = timeout
    statement_handle = object() if metadata_handle else None
    target = statement_handle or cursor.hstmt
    set_attribute = Mock(return_value=0)
    check_error = Mock()
    warning = Mock()
    monkeypatch.setattr(ddbc_bindings, "DDBCSQLSetStmtAttr", set_attribute)
    monkeypatch.setattr("mssql_python.cursor.check_error", check_error)
    monkeypatch.setattr(logger, "warning", warning)

    assert cursor._set_timeout(statement_handle) is None

    if timeout:
        set_attribute.assert_called_once_with(
            target, ConstantsDDBC.SQL_ATTR_QUERY_TIMEOUT.value, timeout
        )
        check_error.assert_called_once_with(ConstantsDDBC.SQL_HANDLE_STMT.value, target, 0)
    else:
        set_attribute.assert_not_called()
        check_error.assert_not_called()
    warning.assert_not_called()


@pytest.mark.parametrize("metadata_handle", [False, True], ids=["default", "metadata"])
def test_set_timeout_failure_reports_target_diagnostics(
    db_connection, monkeypatch, metadata_handle
):
    with db_connection.cursor() as cursor:
        cursor._timeout = 2
        if metadata_handle:
            cursor._tvp_metadata_hstmt = db_connection._conn.alloc_statement_handle()
        statement_handle = cursor._tvp_metadata_hstmt
        target = statement_handle or cursor.hstmt
        real_set_attribute = ddbc_bindings.DDBCSQLSetStmtAttr
        real_check_error = ddbc_bindings.DDBCSQLCheckError

        def fail_attribute(handle, attribute, value):
            # An invalid identifier produces real driver diagnostics on this handle.
            return real_set_attribute(handle, 999999, value)

        set_attribute = Mock(side_effect=fail_attribute)
        check_error = Mock(wraps=real_check_error)
        warning = Mock()
        with monkeypatch.context() as patch:
            patch.setattr(ddbc_bindings, "DDBCSQLSetStmtAttr", set_attribute)
            patch.setattr(ddbc_bindings, "DDBCSQLCheckError", check_error)
            patch.setattr(logger, "warning", warning)
            assert cursor._set_timeout(statement_handle) is None

        set_attribute.assert_called_once_with(target, ConstantsDDBC.SQL_ATTR_QUERY_TIMEOUT.value, 2)
        check_error.assert_called_once_with(ConstantsDDBC.SQL_HANDLE_STMT.value, target, -1)
        assert check_error.call_args.args[1] is target
        error_info = real_check_error(ConstantsDDBC.SQL_HANDLE_STMT.value, target, -1)
        assert error_info.sqlState == "HY092"
        assert error_info.ddbcErrorMsg
        warning.assert_called_once()
        assert warning.call_args.args[0] == "Failed to set query timeout: %s"
        assert "Invalid attribute/option identifier" in warning.call_args.args[1]
        assert cursor.execute("SELECT 1").fetchone()[0] == 1
