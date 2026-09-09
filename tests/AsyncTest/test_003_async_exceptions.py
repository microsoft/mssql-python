import asyncio

import pytest

from mssql_python import exceptions as public_exceptions
from mssql_python.async_query import AsyncConnection
from mssql_python.async_query.exception_translator import translate_py_core_exception

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


def native_exception(name):
    return type(name, (Exception,), {"__module__": "mssql_py_core"})


@pytest.mark.parametrize("name", EXCEPTION_NAMES)
def test_translates_each_native_dbapi_exception(name):
    native_error = native_exception(name)("native failure")

    translated = translate_py_core_exception(native_error)

    assert isinstance(translated, getattr(public_exceptions, name))
    assert translated.driver_error == "Async operation failed"
    assert translated.ddbc_error == "native failure"


def test_translation_normalizes_native_detail_as_backend_error():
    native_error = native_exception("OperationalError")(
        "[Microsoft][ODBC Driver 18 for SQL Server]connection failed"
    )

    translated = translate_py_core_exception(native_error)

    assert translated.driver_error == "Async operation failed"
    assert translated.ddbc_error == "[Microsoft]connection failed"


def test_translation_preserves_sql_diagnostics():
    native_error = native_exception("DatabaseError")("query failed")
    native_error.sql_errors = [{"number": 50001}]
    native_error.info_messages = [{"message": "notice"}]

    translated = translate_py_core_exception(native_error)

    assert translated.sql_errors == native_error.sql_errors
    assert translated.info_messages == native_error.info_messages


def test_non_py_core_exception_is_not_translated():
    error = RuntimeError("unrelated failure")

    assert translate_py_core_exception(error) is error


def test_async_connection_translates_native_error_and_preserves_cause():
    native_error = native_exception("OperationalError")("connection lost")

    class FailingNativeConnection:
        async def commit(self):
            raise native_error

    connection = AsyncConnection(FailingNativeConnection())

    with pytest.raises(public_exceptions.OperationalError) as caught:
        asyncio.run(connection.commit())

    assert caught.value.__cause__ is native_error


def test_async_connection_exposes_public_exception_classes():
    for name in EXCEPTION_NAMES:
        assert getattr(AsyncConnection, name) is getattr(public_exceptions, name)
