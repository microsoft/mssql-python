import asyncio
from types import ModuleType

import pytest

from mssql_python import ConnectionStringParseError, InterfaceError, NotSupportedError
from mssql_python.async_query import AsyncConnection, AsyncCursor
from mssql_python.async_query import async_connection
from mssql_python.async_query._connection_context import build_async_connection_context
from mssql_python.helpers import connstr_to_pycore_params


class FakeNativeConnection:
    def __init__(self):
        self.timeout = 0
        self.autocommit = True
        self.closed = False
        self.calls = []
        self.native_cursor = object()

    def _ensure_open(self):
        if self.closed:
            raise RuntimeError("Connection is closed")

    def cursor(self):
        self._ensure_open()
        self.calls.append(("cursor",))
        return self.native_cursor

    async def commit(self):
        self._ensure_open()
        self.calls.append(("commit",))

    async def rollback(self):
        self._ensure_open()
        self.calls.append(("rollback",))

    async def close(self):
        self.calls.append(("close",))
        self.closed = True

    async def __aenter__(self):
        self._ensure_open()
        self.calls.append(("__aenter__",))
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        self.calls.append(("__aexit__", exc_type, exc_value, traceback))
        self.closed = True
        return None

    def is_connected(self):
        self.calls.append(("is_connected",))
        return not self.closed


def test_connect_delegates_directly_to_py_core(monkeypatch):
    native_connection = FakeNativeConnection()
    captured = {}

    class FakePyAsyncConnection:
        @classmethod
        async def connect(cls, context, python_logger=None, autocommit=False):
            captured.update(
                context=context,
                python_logger=python_logger,
                autocommit=autocommit,
            )
            return native_connection

    py_core = ModuleType("mssql_py_core")
    py_core.PyAsyncConnection = FakePyAsyncConnection
    py_core.PyAsyncCursor = object
    monkeypatch.setattr(async_connection, "load_py_core", lambda: py_core)
    connection_str = (
        "Addr=localhost;Database={db;name};"
        "UID=test-user;PWD={p}}ass;word};Encrypt=Yes"
    )
    logger = object()

    connection = asyncio.run(
        AsyncConnection.connect(
            connection_str,
            autocommit=True,
            timeout=12,
            python_logger=logger,
        )
    )

    assert connection._native_connection is native_connection
    assert captured == {
        "context": {
            "server": "test-server.example.invalid",
            "database": "db;name",
            "user_name": "test-user",
            "password": "p}ass;word",
            "encryption": "Yes",
            "connect_timeout": 12,
        },
        "python_logger": logger,
        "autocommit": True,
    }


def test_connect_defaults_autocommit_to_false(monkeypatch):
    native_connection = FakeNativeConnection()
    captured = {}

    class FakePyAsyncConnection:
        @classmethod
        async def connect(cls, context, python_logger=None, autocommit=False):
            captured["autocommit"] = autocommit
            return native_connection

    py_core = ModuleType("mssql_py_core")
    py_core.PyAsyncConnection = FakePyAsyncConnection
    py_core.PyAsyncCursor = object
    monkeypatch.setattr(async_connection, "load_py_core", lambda: py_core)

    connection = asyncio.run(AsyncConnection.connect("Server=test-server.example.invalid"))

    assert connection._native_connection is native_connection
    assert captured["autocommit"] is False


def test_connect_preserves_native_error(monkeypatch):
    class FakePyAsyncConnection:
        @classmethod
        async def connect(cls, context, python_logger=None, autocommit=False):
            raise RuntimeError("native connect failed")

    py_core = ModuleType("mssql_py_core")
    py_core.PyAsyncConnection = FakePyAsyncConnection
    py_core.PyAsyncCursor = object
    monkeypatch.setattr(async_connection, "load_py_core", lambda: py_core)

    with pytest.raises(RuntimeError, match="native connect failed"):
        asyncio.run(AsyncConnection.connect("Server=invalid"))


def test_sql_password_authentication_is_forwarded_to_py_core():
    context = build_async_connection_context(
        "Server=test-server.example.invalid;Authentication=SqlPassword;UID=user;PWD=password",
        0,
    )

    assert context == {
        "server": "test-server.example.invalid",
        "authentication": "SqlPassword",
        "user_name": "user",
        "password": "password",
    }


def test_trusted_connection_is_forwarded_to_py_core():
    context = build_async_connection_context(
        "Server=test-server.example.invalid;Trusted_Connection=Yes",
        0,
    )

    assert context == {
        "server": "test-server.example.invalid",
        "trusted_connection": "Yes",
    }


@pytest.mark.parametrize(
    "connection_str",
    (
        "Server=test-server.example.invalid;Unknown=value",
        "Server=test-server.example.invalid;Driver=custom",
        "Server=test-server.example.invalid;Server=duplicate",
    ),
)
def test_connection_string_parser_rejects_invalid_keywords(connection_str):
    with pytest.raises(ConnectionStringParseError):
        build_async_connection_context(connection_str, 0)


def test_async_connection_rejects_invalid_numeric_option():
    with pytest.raises(ValueError, match="packetsize.*integer"):
        build_async_connection_context(
            "Server=test-server.example.invalid;PacketSize=invalid",
            0,
        )


def test_async_connection_rejects_embedded_nul_with_interface_error():
    with pytest.raises(InterfaceError) as exc_info:
        build_async_connection_context(
            "Server=test-server.example.invalid\x00;Database=test",
            0,
        )

    assert (
        exc_info.value.driver_error == "Connection string must not contain a NUL (\\x00) character."
    )
    assert exc_info.value.ddbc_error == "Embedded NUL in connection string."


@pytest.mark.parametrize(
    ("connection_str", "key", "expected"),
    (
        ("Addr=first;Server=second", "server", "first"),
        ("Server=first;Addr=second", "server", "first"),
        (
            "Server=test;Trust_Server_Certificate=No;TrustServerCertificate=Yes",
            "trust_server_certificate",
            "No",
        ),
        (
            "Server=test;TrustServerCertificate=No;Trust_Server_Certificate=Yes",
            "trust_server_certificate",
            "No",
        ),
        ("Server=test;Packet Size=4096;PacketSize=8192", "packet_size", 4096),
        ("Server=test;PacketSize=4096;Packet Size=8192", "packet_size", 4096),
    ),
)
def test_async_connection_preserves_first_synonym(connection_str, key, expected):
    context = build_async_connection_context(connection_str, 0)

    assert context[key] == expected


def test_existing_pycore_conversion_remains_permissive_for_bcp():
    context = connstr_to_pycore_params(
        {
            "server": "test-server.example.invalid",
            "packetsize": "invalid",
            "unsupported": "ignored",
        }
    )

    assert context == {"server": "test-server.example.invalid"}


def test_bcp_conversion_does_not_fall_through_invalid_first_synonym():
    context = connstr_to_pycore_params(
        {
            "packet size": "invalid",
            "packetsize": "8192",
        }
    )

    assert context == {}


def test_async_connection_rejects_entra_authentication():
    with pytest.raises(NotSupportedError, match="Async Entra authentication is not supported"):
        build_async_connection_context(
            "Server=test-server.example.invalid;Authentication=ActiveDirectoryDefault",
            0,
        )


@pytest.mark.parametrize("timeout", (True, 1.5, "10"))
def test_async_connection_rejects_non_integer_login_timeout(timeout):
    with pytest.raises(TypeError, match="Login timeout must be an integer"):
        build_async_connection_context("Server=test-server.example.invalid", timeout)


def test_async_connection_rejects_missing_server():
    with pytest.raises(ValueError, match="SERVER parameter is required"):
        build_async_connection_context("Database=test", 0)


def test_connection_delegates_complete_native_surface():
    native_connection = FakeNativeConnection()
    connection = AsyncConnection(native_connection)

    cursor = connection.cursor()
    assert isinstance(cursor, AsyncCursor)
    assert cursor._native_cursor is native_connection.native_cursor
    assert connection.timeout == 0
    connection.timeout = 12
    assert native_connection.timeout == 12
    assert connection.autocommit is True
    assert connection.closed is False
    with pytest.raises(AttributeError):
        connection.autocommit = False
    with pytest.raises(AttributeError):
        connection.closed = True
    assert connection.is_connected() is True
    assert repr(connection) == "AsyncConnection(connected)"

    assert asyncio.run(connection.commit()) is None
    assert asyncio.run(connection.rollback()) is None
    assert asyncio.run(connection.close()) is None

    assert connection.closed is True
    assert connection.is_connected() is False
    assert repr(connection) == "AsyncConnection(closed)"
    assert native_connection.calls == [
        ("cursor",),
        ("is_connected",),
        ("commit",),
        ("rollback",),
        ("close",),
        ("is_connected",),
    ]


def test_async_context_manager_returns_wrapper_and_delegates_exit():
    native_connection = FakeNativeConnection()
    connection = AsyncConnection(native_connection)
    error = ValueError("failure")

    async def use_connection():
        entered = await connection.__aenter__()
        assert entered is connection
        result = await connection.__aexit__(ValueError, error, None)
        assert result is None

    asyncio.run(use_connection())

    assert native_connection.calls == [
        ("__aenter__",),
        ("__aexit__", ValueError, error, None),
    ]


def test_close_can_be_called_repeatedly_and_delegates_to_native():
    native_connection = FakeNativeConnection()
    connection = AsyncConnection(native_connection)

    asyncio.run(connection.close())
    asyncio.run(connection.close())

    assert connection.closed is True
    assert connection.is_connected() is False
    assert native_connection.calls == [
        ("close",),
        ("close",),
        ("is_connected",),
    ]


def test_operations_after_close_preserve_native_errors():
    connection = AsyncConnection(FakeNativeConnection())
    asyncio.run(connection.close())

    with pytest.raises(RuntimeError, match="Connection is closed"):
        connection.cursor()
    with pytest.raises(RuntimeError, match="Connection is closed"):
        asyncio.run(connection.commit())
    with pytest.raises(RuntimeError, match="Connection is closed"):
        asyncio.run(connection.rollback())
    with pytest.raises(RuntimeError, match="Connection is closed"):
        asyncio.run(connection.__aenter__())


def test_context_manager_closes_connection_on_clean_exit():
    connection = AsyncConnection(FakeNativeConnection())

    async def use_connection():
        async with connection as entered:
            assert entered is connection
            assert connection.closed is False

    asyncio.run(use_connection())

    assert connection.closed is True
    assert connection.is_connected() is False


def test_context_manager_preserves_block_exception_and_closes_connection():
    native_connection = FakeNativeConnection()
    connection = AsyncConnection(native_connection)

    async def use_connection():
        async with connection:
            raise ValueError("user_error_42")

    with pytest.raises(ValueError, match="user_error_42"):
        asyncio.run(use_connection())

    assert connection.closed is True
    exit_call = native_connection.calls[-1]
    assert exit_call[0] == "__aexit__"
    assert exit_call[1] is ValueError
    assert str(exit_call[2]) == "user_error_42"
