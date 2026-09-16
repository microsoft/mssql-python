from typing import Any, cast

import mssql_py_core
import pytest

from mssql_python import exceptions as public_exceptions
from mssql_python.async_query import AsyncConnection
from mssql_python.async_query.exception_translator import (
    translate_py_core_exception,
    translate_py_core_exceptions,
)

EXCEPTION_NAMES = (
    "Warning",
    "Error",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
)


@pytest.mark.parametrize("name", EXCEPTION_NAMES)
def test_translates_each_native_dbapi_exception(name):
    native_type = getattr(mssql_py_core, name)
    native_error = native_type("native failure")

    translated = cast(Any, translate_py_core_exception(native_error))

    assert isinstance(translated, getattr(public_exceptions, name))
    assert translated.driver_error == "Async operation failed"
    assert translated.ddbc_error == "native failure"


def test_translation_normalizes_native_detail_as_backend_error():
    native_error = getattr(mssql_py_core, "OperationalError")(
        "[Microsoft][ODBC Driver 18 for SQL Server]connection failed"
    )

    translated = cast(Any, translate_py_core_exception(native_error))

    assert translated.driver_error == "Async operation failed"
    assert translated.ddbc_error == "[Microsoft]connection failed"


def test_translation_preserves_native_diagnostic_attributes():
    native_error = getattr(mssql_py_core, "DatabaseError")("query failed")
    native_error.sql_errors = [{"number": 50001}]
    native_error.info_messages = [{"message": "notice"}]

    translated = cast(Any, translate_py_core_exception(native_error))

    assert translated.sql_errors == native_error.sql_errors
    assert translated.info_messages == native_error.info_messages


def test_non_py_core_exception_is_not_translated():
    error = RuntimeError("unrelated failure")

    assert translate_py_core_exception(error) is error


def test_translation_context_preserves_native_error_as_cause():
    native_error = getattr(mssql_py_core, "OperationalError")("connection lost")

    with pytest.raises(public_exceptions.OperationalError) as caught:
        with translate_py_core_exceptions():
            raise native_error

    assert caught.value.__cause__ is native_error


@pytest.mark.asyncio
async def test_sql_error_translation_preserves_server_diagnostics(async_connection):
    cursor = async_connection.cursor()
    try:
        await cursor.execute("SELECT 1 / 0")

        with pytest.raises(public_exceptions.DatabaseError) as caught:
            await cursor.fetchone()

        assert type(caught.value.__cause__) is getattr(mssql_py_core, "DatabaseError")
        assert getattr(caught.value, "sql_errors")[0]["number"] == 8134
    finally:
        await cursor.close()


def test_async_connection_exposes_public_exception_classes():
    for name in EXCEPTION_NAMES:
        assert getattr(AsyncConnection, name) is getattr(public_exceptions, name)
