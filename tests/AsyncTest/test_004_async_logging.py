import pytest
mssql_py_core = pytest.importorskip("mssql_py_core", exc_type=ImportError)

from mssql_python import OperationalError, setup_logging
from mssql_python.async_query import AsyncConnection
from mssql_python.async_query import exception_translator
from mssql_python.logging import logger


def read_log(log_path):
    for handler in logger.handlers:
        handler.flush()
    return log_path.read_text(encoding="utf-8")


def enable_file_logging(tmp_path, name):
    log_path = tmp_path / name
    setup_logging(output="file", log_file_path=str(log_path))
    return log_path


@pytest.mark.asyncio
async def test_connect_logging_does_not_include_client_context(
    async_connection_string,
    tmp_path,
):
    log_path = enable_file_logging(tmp_path, "async-connect.log")

    connection = await AsyncConnection.connect(async_connection_string, autocommit=True)
    await connection.close()

    messages = read_log(log_path)
    assert "AsyncConnection.connect: starting" in messages
    assert "AsyncConnection.connect: connected" in messages
    assert "PWD=" not in messages
    assert "password" not in messages.lower()
    assert "client_context" not in messages


@pytest.mark.asyncio
async def test_connection_lifecycle_logs_important_boundaries(
    async_connection,
    tmp_path,
):
    log_path = enable_file_logging(tmp_path, "async-lifecycle.log")

    cursor = async_connection.cursor()
    async_connection.timeout = 10
    await cursor.close()
    await async_connection.commit()
    await async_connection.rollback()
    await async_connection.close()

    messages = read_log(log_path)
    expected_messages = (
        "AsyncConnection.cursor: cursor created",
        "AsyncConnection.timeout: updated",
        "AsyncCursor.close: starting",
        "AsyncCursor.close: completed",
        "AsyncConnection.commit: starting",
        "AsyncConnection.commit: completed",
        "AsyncConnection.rollback: starting",
        "AsyncConnection.rollback: completed",
        "AsyncConnection.close: starting",
        "AsyncConnection.close: completed",
    )
    for expected in expected_messages:
        assert expected in messages


@pytest.mark.asyncio
async def test_context_logging_records_error_presence_without_error_details(
    async_connection_string,
    tmp_path,
):
    connection = await AsyncConnection.connect(async_connection_string)
    log_path = enable_file_logging(tmp_path, "async-context.log")
    secret_message = "sensitive user exception"

    try:
        async with connection:
            raise ValueError(secret_message)
    except ValueError:
        pass

    messages = read_log(log_path)
    assert "block_error=True" in messages
    assert secret_message not in messages


def test_exception_translation_logs_classes_without_error_message(tmp_path):
    log_path = enable_file_logging(tmp_path, "async-exception.log")
    native_error = getattr(mssql_py_core, "OperationalError")("secret native failure")

    translated = exception_translator.translate_py_core_exception(native_error)

    messages = read_log(log_path)
    assert isinstance(translated, OperationalError)
    assert "Async exception translation: OperationalError -> OperationalError" in messages
    assert "secret native failure" not in messages
