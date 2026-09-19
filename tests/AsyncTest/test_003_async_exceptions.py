from typing import Any, cast
from uuid import uuid4

import pytest

mssql_py_core = pytest.importorskip("mssql_py_core", exc_type=ImportError)

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


@pytest.mark.parametrize(
    "number, public_type",
    (
        (8134, public_exceptions.DataError),
        (2627, public_exceptions.IntegrityError),
        (156, public_exceptions.ProgrammingError),
        (50001, public_exceptions.DatabaseError),
    ),
)
def test_translation_classifies_sql_server_diagnostics(number, public_type):
    native_error = getattr(mssql_py_core, "DatabaseError")("query failed")
    native_error.sql_errors = [{"number": number}]

    translated = translate_py_core_exception(native_error)

    assert type(translated) is public_type
    assert cast(Any, translated).sql_errors == native_error.sql_errors


def test_non_py_core_exception_is_not_translated():
    error = RuntimeError("unrelated failure")

    assert translate_py_core_exception(error) is error


@pytest.mark.parametrize(
    "native_error, public_type",
    (
        (RuntimeError("Cursor is closed"), public_exceptions.ProgrammingError),
        (RuntimeError("Connection is closed"), public_exceptions.InterfaceError),
        (RuntimeError("Connection is closing"), public_exceptions.InterfaceError),
        (
            RuntimeError("Connection is busy with another cursor operation"),
            public_exceptions.OperationalError,
        ),
        (RuntimeError("Connection is broken"), public_exceptions.OperationalError),
        (
            TypeError("The SQL contains 2 parameter markers, but 1 parameters were supplied"),
            public_exceptions.ProgrammingError,
        ),
    ),
)
def test_translates_known_py_core_builtin_errors(native_error, public_type):
    translated = translate_py_core_exception(native_error)

    assert isinstance(translated, public_type)


def test_translation_context_preserves_native_error_as_cause():
    native_error = getattr(mssql_py_core, "OperationalError")("connection lost")

    with pytest.raises(public_exceptions.OperationalError) as caught:
        with translate_py_core_exceptions():
            raise native_error

    assert caught.value.__cause__ is native_error


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fetch",
    (
        lambda cursor: cursor.fetchone(),
        lambda cursor: cursor.fetchall(),
        lambda cursor: cursor.fetchmany(),
    ),
    ids=("fetchone", "fetchall", "fetchmany"),
)
async def test_fetch_data_error_preserves_server_diagnostics(async_connection, fetch):
    cursor = async_connection.cursor()
    try:
        await cursor.execute("SELECT 1 / 0")

        with pytest.raises(public_exceptions.DataError) as caught:
            await fetch(cursor)

        assert type(caught.value.__cause__) is getattr(mssql_py_core, "DatabaseError")
        assert getattr(caught.value, "sql_errors")[0]["number"] == 8134
    finally:
        await cursor.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "operation",
    (
        lambda cursor: cursor.execute("SELECT 1"),
        lambda cursor: cursor.executemany("SELECT ?", [(1,)]),
        lambda cursor: cursor.fetchone(),
        lambda cursor: cursor.fetchall(),
        lambda cursor: cursor.fetchmany(),
        lambda cursor: cursor.fetchmany(0),
        lambda cursor: cursor.fetchmany("invalid"),
    ),
    ids=(
        "execute",
        "executemany",
        "fetchone",
        "fetchall",
        "fetchmany",
        "fetchmany-zero",
        "fetchmany-invalid",
    ),
)
async def test_closed_cursor_operations_raise_programming_error(async_connection, operation):
    cursor = async_connection.cursor()
    await cursor.close()

    with pytest.raises(public_exceptions.ProgrammingError) as caught:
        await operation(cursor)

    assert isinstance(caught.value.__cause__, RuntimeError)


@pytest.mark.asyncio
async def test_closed_cursor_executemany_checks_state_before_parameters(async_connection):
    cursor = async_connection.cursor()
    await cursor.close()

    with pytest.raises(public_exceptions.ProgrammingError) as caught:
        await cursor.executemany("SELECT ?", iter([(1,)]))

    assert isinstance(caught.value.__cause__, RuntimeError)


@pytest.mark.asyncio
async def test_connection_close_invalidates_cursor_fetchmany_fast_path(async_connection_string):
    connection = await AsyncConnection.connect(async_connection_string)
    cursor = connection.cursor()
    await connection.close()

    with pytest.raises(public_exceptions.InterfaceError) as caught:
        await cursor.fetchmany(0)

    assert isinstance(caught.value.__cause__, RuntimeError)


@pytest.mark.asyncio
async def test_execute_parameter_count_error_is_programming_error_and_cursor_is_reusable(
    async_cursor,
):
    with pytest.raises(public_exceptions.ProgrammingError) as caught:
        await async_cursor.execute("SELECT ?, ?", 1)

    assert isinstance(caught.value.__cause__, TypeError)
    assert await async_cursor.execute("SELECT 1") is async_cursor


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fetch",
    (
        lambda cursor: cursor.fetchone(),
        lambda cursor: cursor.fetchall(),
        lambda cursor: cursor.fetchmany(),
    ),
    ids=("fetchone", "fetchall", "fetchmany"),
)
async def test_fetch_without_result_set_raises_programming_error(async_cursor, fetch):
    with pytest.raises(public_exceptions.ProgrammingError) as caught:
        await fetch(async_cursor)

    assert type(caught.value.__cause__) is getattr(mssql_py_core, "ProgrammingError")
    assert str(caught.value.__cause__) == "No active result set"


@pytest.mark.asyncio
async def test_client_validation_errors_remain_native_python_errors(async_cursor):
    with pytest.raises(KeyError):
        await async_cursor.execute("SELECT %(missing)s", {"other": 1})

    with pytest.raises(TypeError):
        await async_cursor.executemany("SELECT %(value)s", [{"value": 1}, (2,)])

    await async_cursor.execute("SELECT 1")
    with pytest.raises(TypeError):
        await async_cursor.fetchmany("invalid")


@pytest.mark.asyncio
async def test_execute_programming_error_preserves_diagnostics_and_cursor_is_reusable(
    async_cursor,
):
    with pytest.raises(public_exceptions.ProgrammingError) as caught:
        await async_cursor.execute("SELECT FROM")

    assert getattr(caught.value, "sql_errors")[0]["number"] == 156
    assert await async_cursor.execute("SELECT 1") is async_cursor


@pytest.mark.asyncio
async def test_executemany_integrity_error_reports_row_and_preserves_partial_progress(
    async_connection_string,
):
    connection = await AsyncConnection.connect(async_connection_string, autocommit=True)
    cursor = connection.cursor()
    table_name = f"async_exception_{uuid4().hex}"
    try:
        await cursor.execute(f"CREATE TABLE {table_name} (id INT PRIMARY KEY)")

        with pytest.raises(public_exceptions.IntegrityError) as caught:
            await cursor.executemany(
                f"INSERT INTO {table_name} VALUES (?)",
                [(1,), (1,), (2,)],
            )

        assert "parameter row 1" in str(caught.value)
        assert getattr(caught.value, "sql_errors")[0]["number"] == 2627
        await cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row = await cursor.fetchone()
        assert row is not None
        assert row[0] == 1
    finally:
        await cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        await cursor.close()
        await connection.close()


@pytest.mark.asyncio
async def test_timeout_is_operational_error_and_cursor_is_reusable(async_connection_string):
    connection = await AsyncConnection.connect(async_connection_string)
    connection.timeout = 1
    cursor = connection.cursor()
    try:
        with pytest.raises(public_exceptions.OperationalError) as caught:
            await cursor.execute("WAITFOR DELAY '00:00:03'; SELECT 1")

        assert type(caught.value.__cause__) is getattr(mssql_py_core, "OperationalError")
        assert await cursor.execute("SELECT 1") is cursor
    finally:
        await cursor.close()
        await connection.close()


@pytest.mark.asyncio
async def test_busy_connection_is_operational_error(async_connection):
    owning_cursor = async_connection.cursor()
    blocked_cursor = async_connection.cursor()
    try:
        await owning_cursor.execute("SELECT 1 UNION ALL SELECT 2")

        with pytest.raises(public_exceptions.OperationalError) as caught:
            await blocked_cursor.execute("SELECT 3")

        assert isinstance(caught.value.__cause__, RuntimeError)
        await owning_cursor.close()
        assert await blocked_cursor.execute("SELECT 3") is blocked_cursor
    finally:
        await owning_cursor.close()
        await blocked_cursor.close()


def test_async_connection_exposes_public_exception_classes():
    for name in EXCEPTION_NAMES:
        assert getattr(AsyncConnection, name) is getattr(public_exceptions, name)
