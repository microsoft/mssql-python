import asyncio
from types import ModuleType
from unittest.mock import call, patch

from mssql_python import OperationalError
from mssql_python.AsyncQuery import AsyncConnection
from mssql_python.AsyncQuery import async_connection
from mssql_python.AsyncQuery import exception_translator


class FakeNativeConnection:
    def __init__(self):
        self.timeout = 0
        self.closed = False

    def cursor(self):
        return object()

    async def commit(self):
        return None

    async def rollback(self):
        return None

    async def close(self):
        self.closed = True

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        self.closed = True
        return None


def native_exception(name):
    return type(name, (Exception,), {"__module__": "mssql_py_core"})


def test_connect_logging_does_not_include_client_context(monkeypatch):
    native_connection = FakeNativeConnection()

    class FakePyAsyncConnection:
        @classmethod
        async def connect(cls, context, python_logger=None, autocommit=False):
            return native_connection

    py_core = ModuleType("mssql_py_core")
    py_core.PyAsyncConnection = FakePyAsyncConnection
    py_core.PyAsyncCursor = object
    monkeypatch.setattr(async_connection, "load_py_core", lambda: py_core)
    context = {
        "server": "secret-server",
        "user_name": "secret-user",
        "password": "secret-password",
        "access_token": "secret-token",
    }

    with patch.object(async_connection.logger, "debug") as debug:
        asyncio.run(AsyncConnection.connect(context, autocommit=True))

    messages = str(debug.call_args_list)
    assert "AsyncConnection.connect: starting" in messages
    assert "AsyncConnection.connect: connected" in messages
    assert all(value not in messages for value in context.values())


def test_connection_lifecycle_logs_important_boundaries():
    connection = AsyncConnection(FakeNativeConnection())

    with patch.object(async_connection.logger, "debug") as debug:
        connection.cursor()
        connection.timeout = 10
        asyncio.run(connection.commit())
        asyncio.run(connection.rollback())
        asyncio.run(connection.close())

    assert debug.call_args_list == [
        call("AsyncConnection.cursor: cursor created"),
        call("AsyncConnection.timeout: updated"),
        call("AsyncConnection.commit: starting"),
        call("AsyncConnection.commit: completed"),
        call("AsyncConnection.rollback: starting"),
        call("AsyncConnection.rollback: completed"),
        call("AsyncConnection.close: starting"),
        call("AsyncConnection.close: completed"),
    ]


def test_context_logging_records_error_presence_without_error_details():
    connection = AsyncConnection(FakeNativeConnection())
    secret_message = "sensitive user exception"

    async def use_connection():
        try:
            async with connection:
                raise ValueError(secret_message)
        except ValueError:
            pass

    with patch.object(async_connection.logger, "debug") as debug:
        asyncio.run(use_connection())

    messages = [str(item) for item in debug.call_args_list]
    assert any("block_error=%s" in message for message in messages)
    assert all(secret_message not in message for message in messages)


def test_exception_translation_logs_classes_without_error_message():
    native_error = native_exception("OperationalError")("secret native failure")

    with patch.object(exception_translator.logger, "debug") as debug:
        translated = exception_translator.translate_py_core_exception(native_error)

    assert isinstance(translated, OperationalError)
    debug.assert_called_once_with(
        "Async exception translation: %s -> %s",
        "OperationalError",
        "OperationalError",
    )
    assert "secret native failure" not in str(debug.call_args)
