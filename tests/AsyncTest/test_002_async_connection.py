import pytest

pytest.importorskip("mssql_py_core", exc_type=ImportError)

from mssql_python import ConnectionStringParseError, InterfaceError, NotSupportedError
from mssql_python.async_query import AsyncConnection, AsyncCursor
from mssql_python.async_query._connection_context import build_async_connection_context
from mssql_python.helpers import connstr_to_pycore_params


@pytest.mark.asyncio
async def test_connect_accepts_login_options_with_real_py_core(async_connection_string):
    connection = await AsyncConnection.connect(
        async_connection_string,
        autocommit=True,
        timeout=12,
    )
    try:
        assert connection.timeout == 0
        assert connection.autocommit is True
        assert connection.closed is False
        assert connection.is_connected() is True
        assert repr(connection) == "AsyncConnection(connected)"
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_connect_defaults_autocommit_to_false(async_connection_string):
    connection = await AsyncConnection.connect(async_connection_string)
    try:
        assert connection.autocommit is False
    finally:
        await connection.close()


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


@pytest.mark.parametrize(
    ("option", "value", "expected_range"),
    (
        ("ConnectRetryCount", -1, "0 and 255"),
        ("ConnectRetryCount", 256, "0 and 255"),
        ("ConnectRetryInterval", -1, "1 and 60"),
        ("ConnectRetryInterval", 0, "1 and 60"),
        ("ConnectRetryInterval", 61, "1 and 60"),
    ),
)
def test_async_connection_rejects_retry_options_outside_sync_ranges(
    option,
    value,
    expected_range,
):
    with pytest.raises(ValueError, match=expected_range):
        build_async_connection_context(
            f"Server=test-server.example.invalid;{option}={value}",
            0,
        )


@pytest.mark.parametrize(
    ("option", "value", "context_key"),
    (
        ("ConnectRetryCount", 0, "connect_retry_count"),
        ("ConnectRetryCount", 255, "connect_retry_count"),
        ("ConnectRetryInterval", 1, "connect_retry_interval"),
        ("ConnectRetryInterval", 60, "connect_retry_interval"),
    ),
)
def test_async_connection_accepts_retry_option_boundaries(option, value, context_key):
    context = build_async_connection_context(
        f"Server=test-server.example.invalid;{option}={value}",
        0,
    )

    assert context[context_key] == value


def test_async_connection_converts_keep_alive_seconds_to_milliseconds():
    context = build_async_connection_context(
        "Server=test-server.example.invalid;KeepAlive=30;KeepAliveInterval=1",
        0,
    )

    assert context["keep_alive"] == 30_000
    assert context["keep_alive_interval"] == 1_000


@pytest.mark.parametrize(
    ("option", "context_key"),
    (
        ("KeepAlive", "keep_alive"),
        ("KeepAliveInterval", "keep_alive_interval"),
    ),
)
def test_async_connection_saturates_keep_alive_milliseconds(option, context_key):
    context = build_async_connection_context(
        f"Server=test-server.example.invalid;{option}={2**32 - 1}",
        0,
    )

    assert context[context_key] == 2**32 - 1


@pytest.mark.parametrize("option", ("KeepAlive", "KeepAliveInterval"))
@pytest.mark.parametrize("value", (-1, 2**32))
def test_async_connection_rejects_keep_alive_outside_unsigned_integer_range(option, value):
    with pytest.raises(ValueError, match="0 and 4294967295"):
        build_async_connection_context(
            f"Server=test-server.example.invalid;{option}={value}",
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
            "connectretrycount": "256",
            "connectretryinterval": "0",
            "keepalive": "30",
            "keepaliveinterval": "1",
            "unsupported": "ignored",
        }
    )

    assert context == {
        "server": "test-server.example.invalid",
        "connect_retry_count": 256,
        "connect_retry_interval": 0,
        "keep_alive": 30_000,
        "keep_alive_interval": 1_000,
    }


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


@pytest.mark.asyncio
async def test_connection_exposes_complete_native_surface(async_connection):
    cursor = async_connection.cursor()
    assert isinstance(cursor, AsyncCursor)
    assert async_connection.timeout == 0
    async_connection.timeout = 12
    assert async_connection.timeout == 12
    assert async_connection.autocommit is False
    assert async_connection.closed is False
    with pytest.raises(AttributeError):
        setattr(async_connection, "autocommit", True)
    with pytest.raises(AttributeError):
        setattr(async_connection, "closed", True)
    assert async_connection.is_connected() is True
    assert repr(async_connection) == "AsyncConnection(connected)"

    await cursor.close()
    assert await async_connection.commit() is None
    assert await async_connection.rollback() is None


@pytest.mark.asyncio
async def test_async_context_manager_returns_wrapper_and_closes_connection(
    async_connection_string,
):
    connection = await AsyncConnection.connect(async_connection_string)

    async with connection as entered:
        assert entered is connection
        assert connection.closed is False

    assert connection.closed is True
    assert connection.is_connected() is False


@pytest.mark.asyncio
async def test_close_can_be_called_repeatedly(async_connection_string):
    connection = await AsyncConnection.connect(async_connection_string)

    await connection.close()
    await connection.close()

    assert connection.closed is True
    assert connection.is_connected() is False
    assert repr(connection) == "AsyncConnection(closed)"


@pytest.mark.asyncio
async def test_operations_after_close_preserve_native_errors(async_connection_string):
    connection = await AsyncConnection.connect(async_connection_string)
    await connection.close()

    with pytest.raises(RuntimeError, match="Connection is closed"):
        connection.cursor()
    with pytest.raises(RuntimeError, match="Connection is closed"):
        await connection.commit()
    with pytest.raises(RuntimeError, match="Connection is closed"):
        await connection.rollback()
    with pytest.raises(RuntimeError, match="Connection is closed"):
        await connection.__aenter__()


@pytest.mark.asyncio
async def test_context_manager_preserves_block_exception_and_closes_connection(
    async_connection_string,
):
    connection = await AsyncConnection.connect(async_connection_string)

    with pytest.raises(ValueError, match="user_error_42"):
        async with connection:
            raise ValueError("user_error_42")

    assert connection.closed is True
    assert connection.is_connected() is False
