"""Async equivalents of the non-pooling contracts in test_008_auth.

ODBC attribute encoding, native Windows ODBC login, bulk copy, and pool identity/
checkout tests do not apply to these individual TDS connections.

The live integration test uses the same DB_CONNECTION_STRING-driven fixture as
the other async integration tests. An Entra connection string exercises real
Entra login; SQL-password jobs exercise their configured authentication instead.
Missing configuration or failed login fails the test rather than skipping it.
"""

import asyncio
import builtins
import threading
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest

from mssql_python import (
    ConnectionStringParseError,
    InterfaceError,
    NotSupportedError,
    OperationalError,
)
from mssql_python.auth import _SQL_SCOPE, _credential_cache, _credential_cache_lock
from mssql_python.async_query import _AsyncConnection
from mssql_python.async_query import _auth as async_auth
from mssql_python.async_query import async_connection as connection_module
from mssql_python.async_query._connection_context import build_async_connection_context
from mssql_python.async_query._native import load_py_core

TOKEN = "test-header.test-payload.test-signature"
SERVER = "Server=localhost;Database=testdb"
AUTH_MODES = (
    ("ActiveDirectoryDefault", "DefaultAzureCredential"),
    ("ActiveDirectoryDeviceCode", "DeviceCodeCredential"),
    ("ActiveDirectoryInteractive", "InteractiveBrowserCredential"),
    ("ActiveDirectoryMsi", "ManagedIdentityCredential"),
)


@pytest.fixture(autouse=True)
def clear_async_credentials():
    with async_auth._credential_cache_lock:
        async_auth._credential_cache.clear()
    yield
    with async_auth._credential_cache_lock:
        async_auth._credential_cache.clear()


@pytest.fixture
def fake_core(monkeypatch):
    def make_connection(*args, **kwargs):
        cursor = SimpleNamespace(
            execute=AsyncMock(),
            fetchone=AsyncMock(return_value=(1,)),
            close=AsyncMock(),
            description=None,
            rowcount=-1,
        )
        return SimpleNamespace(
            cursor=Mock(return_value=cursor),
            commit=AsyncMock(),
            rollback=AsyncMock(),
            close=AsyncMock(),
            closed=False,
        )

    connect = AsyncMock(side_effect=make_connection)
    core = SimpleNamespace(PyAsyncConnection=SimpleNamespace(connect=connect))
    monkeypatch.setattr(connection_module, "load_py_core", lambda: core)
    return connect


@pytest.fixture
def credentials(monkeypatch):
    import azure.identity

    constructors = {}
    for _, name in AUTH_MODES:
        credential = MagicMock()
        credential.get_token.return_value = SimpleNamespace(
            token=TOKEN, expires_on=int(time.time()) + 3600
        )
        constructor = Mock(return_value=credential)
        constructor.__name__ = name
        monkeypatch.setattr(azure.identity, name, constructor)
        constructors[name] = constructor
    return constructors


def provider(token=TOKEN, **kwargs):
    credential = Mock()
    credential.get_token.return_value = SimpleNamespace(token=token, **kwargs)
    return credential


@pytest.mark.parametrize(
    "mode", [mode for mode, _ in AUTH_MODES] + ["ActiveDirectoryServicePrincipal"]
)
def test_context_accepts_entra_modes_without_token_acquisition(mode):
    context = build_async_connection_context(f"{SERVER};Authentication={mode}", 12)

    assert context == {
        "server": "localhost",
        "database": "testdb",
        "authentication": mode,
        "connect_timeout": 12,
    }


@pytest.mark.parametrize(
    "mode",
    ("ActiveDirectoryPassword", "ActiveDirectoryIntegrated", "TypoMode"),
)
@pytest.mark.asyncio
async def test_unsupported_auth_fails_before_native_connect(mode, fake_core):
    with pytest.raises(NotSupportedError, match="not supported"):
        await _AsyncConnection.connect(f"{SERVER};Authentication={mode}")
    fake_core.assert_not_awaited()


@pytest.mark.parametrize(("mode", "class_name"), AUTH_MODES)
@pytest.mark.asyncio
async def test_builtin_credential_mapping_scope_and_raw_token(
    mode, class_name, credentials, fake_core
):
    await _AsyncConnection.connect(
        f"{SERVER};Authentication={mode}",
        timeout=12,
        autocommit=True,
    )

    credentials[class_name].assert_called_once_with()
    credentials[class_name].return_value.get_token.assert_called_once_with(_SQL_SCOPE)
    assert fake_core.await_args.args[0] == {
        "server": "localhost",
        "database": "testdb",
        "connect_timeout": 12,
        "access_token": TOKEN,
    }
    assert fake_core.await_args.kwargs == {"python_logger": None, "autocommit": True}


@pytest.mark.parametrize(
    "mode", ("activedirectorydefault", "ACTIVEDIRECTORYDEFAULT", " ActiveDirectoryDefault ")
)
@pytest.mark.asyncio
async def test_authentication_is_case_insensitive(mode, credentials, fake_core):
    await _AsyncConnection.connect(f"{SERVER};Authentication={{{mode}}}")
    credentials["DefaultAzureCredential"].assert_called_once_with()
    assert fake_core.await_args.args[0]["access_token"] == TOKEN


@pytest.mark.parametrize("system", ("Windows", "Linux", "Darwin"))
@pytest.mark.asyncio
async def test_interactive_uses_python_on_every_platform(
    system, monkeypatch, credentials, fake_core
):
    monkeypatch.setattr("platform.system", lambda: system)
    await _AsyncConnection.connect(f"{SERVER};Authentication=ActiveDirectoryInteractive")
    credentials["InteractiveBrowserCredential"].assert_called_once_with()
    assert fake_core.await_args.args[0]["access_token"] == TOKEN


@pytest.mark.asyncio
async def test_system_assigned_msi(credentials, fake_core):
    await _AsyncConnection.connect(f"{SERVER};Authentication=ActiveDirectoryMsi")
    credentials["ManagedIdentityCredential"].assert_called_once_with()


@pytest.mark.parametrize(
    ("uid", "client_id"),
    (("client-id", "client-id"), ("{client-id}", "client-id"), ("{client;id}", "client;id")),
)
@pytest.mark.asyncio
async def test_user_assigned_msi_preserves_parsed_uid(uid, client_id, credentials, fake_core):
    await _AsyncConnection.connect(f"{SERVER};Authentication=ActiveDirectoryMsi;UID={uid}")
    credentials["ManagedIdentityCredential"].assert_called_once_with(client_id=client_id)
    assert "user_name" not in fake_core.await_args.args[0]


@pytest.mark.asyncio
async def test_msi_caches_separately_for_each_identity(credentials, fake_core):
    for uid in ("", ";UID=first", ";UID=second", ";UID=first"):
        await _AsyncConnection.connect(f"{SERVER};Authentication=ActiveDirectoryMsi{uid}")
    constructor = credentials["ManagedIdentityCredential"]
    assert constructor.call_count == 3
    assert constructor.return_value.get_token.call_count == 4
    assert fake_core.await_count == 4


@pytest.mark.asyncio
async def test_credentials_reused_but_each_connect_acquires_and_opens(credentials, fake_core):
    credential = credentials["DefaultAzureCredential"].return_value
    credential.get_token.side_effect = [
        SimpleNamespace(token="first", expires_on=1),
        SimpleNamespace(token="rotated", expires_on=int(time.time()) + 3600),
    ]
    first = await _AsyncConnection.connect(f"{SERVER};Authentication=ActiveDirectoryDefault")
    second = await _AsyncConnection.connect(f"{SERVER};Authentication=ActiveDirectoryDefault")

    credentials["DefaultAzureCredential"].assert_called_once_with()
    assert credential.get_token.call_count == 2
    assert [call.args[0]["access_token"] for call in fake_core.await_args_list] == [
        "first",
        "rotated",
    ]
    assert first._py_core_async_connection is not second._py_core_async_connection
    await first.close()
    await second.close()


@pytest.mark.asyncio
async def test_different_modes_have_separate_credential_instances(credentials, fake_core):
    for mode, _ in AUTH_MODES:
        await _AsyncConnection.connect(f"{SERVER};Authentication={mode}")
    for constructor in credentials.values():
        constructor.assert_called_once_with()


@pytest.mark.asyncio
async def test_concurrent_connects_construct_one_credential(credentials, fake_core):
    await asyncio.gather(
        *(
            _AsyncConnection.connect(f"{SERVER};Authentication=ActiveDirectoryDefault")
            for _ in range(10)
        )
    )
    credentials["DefaultAzureCredential"].assert_called_once_with()
    assert credentials["DefaultAzureCredential"].return_value.get_token.call_count == 10
    assert fake_core.await_count == 10


@pytest.mark.asyncio
async def test_failed_credential_initialization_does_not_poison_cache(credentials, fake_core):
    constructor = credentials["DefaultAzureCredential"]
    failure = ValueError("invalid credential configuration")
    constructor.side_effect = failure
    with pytest.raises(OperationalError, match="Failed to create") as caught:
        await _AsyncConnection.connect(f"{SERVER};Authentication=ActiveDirectoryDefault")
    assert caught.value.__cause__ is failure
    assert async_auth._credential_cache == {}
    fake_core.assert_not_awaited()

    constructor.side_effect = None
    await _AsyncConnection.connect(f"{SERVER};Authentication=ActiveDirectoryDefault")
    assert constructor.call_count == 2


@pytest.mark.asyncio
async def test_missing_azure_identity_reports_dependency_error(monkeypatch, fake_core):
    original_import = builtins.__import__

    def missing_identity(name, *args, **kwargs):
        if name == "azure.identity":
            raise ImportError("azure.identity is unavailable")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", missing_identity)
    with pytest.raises(OperationalError, match="Install azure-identity"):
        await _AsyncConnection.connect(f"{SERVER};Authentication=ActiveDirectoryDefault")
    fake_core.assert_not_awaited()


@pytest.mark.parametrize("class_name", ("InteractiveBrowserCredential", "DeviceCodeCredential"))
@pytest.mark.asyncio
async def test_interactive_sdk_is_silent_first_and_prompts_only_when_required(
    class_name, monkeypatch, fake_core
):
    import azure.identity
    from azure.core.credentials import AccessTokenInfo
    from azure.identity import AuthenticationRequiredError
    from azure.identity._internal import interactive

    credential_class = getattr(azure.identity, class_name)
    credential = credential_class()
    silent = Mock(
        side_effect=[
            AuthenticationRequiredError(scopes=[_SQL_SCOPE]),
            AccessTokenInfo(TOKEN, int(time.time()) + 3600),
        ]
    )
    prompt = Mock(return_value={"access_token": TOKEN, "expires_in": 3600})
    monkeypatch.setattr(credential, "_acquire_token_silent", silent)
    monkeypatch.setattr(credential, "_request_token", prompt)
    monkeypatch.setattr(interactive, "_build_auth_record", Mock())
    monkeypatch.setattr(azure.identity, class_name, Mock(return_value=credential))
    mode = dict((name, mode) for mode, name in AUTH_MODES)[class_name]
    try:
        await _AsyncConnection.connect(f"{SERVER};Authentication={mode}")
        await _AsyncConnection.connect(f"{SERVER};Authentication={mode}")
    finally:
        credential.close()

    assert silent.call_count == 2
    prompt.assert_called_once()
    assert fake_core.await_count == 2


@pytest.mark.parametrize(("mode", "class_name"), AUTH_MODES)
@pytest.mark.asyncio
async def test_builtin_authentication_failure_never_falls_back(
    mode, class_name, credentials, fake_core
):
    from azure.core.exceptions import ClientAuthenticationError

    failure = ClientAuthenticationError("sign-in failed or cancelled")
    credentials[class_name].return_value.get_token.side_effect = failure
    with pytest.raises(OperationalError) as caught:
        await _AsyncConnection.connect(f"{SERVER};Authentication={mode}")
    assert caught.value.__cause__ is failure
    fake_core.assert_not_awaited()


@pytest.mark.asyncio
async def test_token_acquisition_error_keeps_reusable_credential(credentials, fake_core):
    credential = credentials["ManagedIdentityCredential"].return_value
    credential.get_token.side_effect = [
        RuntimeError("temporary failure"),
        SimpleNamespace(token=TOKEN),
    ]
    with pytest.raises(OperationalError):
        await _AsyncConnection.connect(f"{SERVER};Authentication=ActiveDirectoryMsi")
    await _AsyncConnection.connect(f"{SERVER};Authentication=ActiveDirectoryMsi")
    credentials["ManagedIdentityCredential"].assert_called_once_with()
    fake_core.assert_awaited_once()


@pytest.mark.asyncio
async def test_builtin_token_auth_removes_conflicting_fields(credentials, fake_core):
    await _AsyncConnection.connect(
        f"{SERVER};Authentication=ActiveDirectoryDefault;UID=user;PWD=dummy;"
        "Trusted_Connection=Yes;Encrypt=Yes"
    )
    assert fake_core.await_args.args[0] == {
        "server": "localhost",
        "database": "testdb",
        "encryption": "Yes",
        "access_token": TOKEN,
    }


@pytest.mark.asyncio
async def test_custom_provider_accepts_minimal_get_token_and_missing_expiry(fake_core):
    class Credential:
        def get_token(self, scope):
            assert scope == _SQL_SCOPE
            return SimpleNamespace(token=TOKEN)

    await _AsyncConnection.connect(SERVER, token_provider=Credential())
    assert fake_core.await_args.args[0]["access_token"] == TOKEN


@pytest.mark.parametrize("token", (" ", "e" + "A" * 1500 + ".sig", TOKEN))
@pytest.mark.asyncio
async def test_provider_raw_token_is_forwarded_without_odbc_encoding(token, fake_core, monkeypatch):
    def reject_encoding(*args):
        pytest.fail("Async authentication must not construct an ODBC access-token struct")

    monkeypatch.setattr("mssql_python.auth.AADAuth.get_token_struct", reject_encoding)
    await _AsyncConnection.connect(SERVER, token_provider=provider(token))
    assert fake_core.await_args.args[0]["access_token"] == token


@pytest.mark.parametrize("invalid", ("", None, b"bytes", 123, False, [], {}))
@pytest.mark.asyncio
async def test_invalid_provider_token_fails_before_login(invalid, fake_core):
    with pytest.raises(InterfaceError, match="non-empty"):
        await _AsyncConnection.connect(SERVER, token_provider=provider(invalid))
    fake_core.assert_not_awaited()


@pytest.mark.parametrize("result", (None, "raw-string", SimpleNamespace()))
@pytest.mark.asyncio
async def test_provider_must_return_token_object(result, fake_core):
    credential = provider()
    credential.get_token.return_value = result
    with pytest.raises(InterfaceError, match="non-empty"):
        await _AsyncConnection.connect(SERVER, token_provider=credential)
    fake_core.assert_not_awaited()


@pytest.mark.parametrize(
    "credential", ("not-a-credential", object(), SimpleNamespace(get_token=None))
)
@pytest.mark.asyncio
async def test_provider_requires_callable_get_token(credential, fake_core):
    with pytest.raises(InterfaceError, match="callable"):
        await _AsyncConnection.connect(SERVER, token_provider=credential)
    fake_core.assert_not_awaited()


@pytest.mark.parametrize("authentication", ("SqlPassword", "ActiveDirectoryDefault"))
@pytest.mark.asyncio
async def test_provider_and_authentication_are_mutually_exclusive(authentication, fake_core):
    credential = provider()
    with pytest.raises(InterfaceError, match="Cannot specify both"):
        await _AsyncConnection.connect(
            f"{SERVER};Authentication={authentication}", token_provider=credential
        )
    credential.get_token.assert_not_called()
    fake_core.assert_not_awaited()


@pytest.mark.asyncio
async def test_empty_authentication_fails_parser_before_acquisition(fake_core):
    credential = provider()
    with pytest.raises(ConnectionStringParseError, match="Empty value"):
        await _AsyncConnection.connect(f"{SERVER};Authentication=", token_provider=credential)
    credential.get_token.assert_not_called()
    fake_core.assert_not_awaited()


@pytest.mark.asyncio
async def test_ignored_provider_credentials_warn_and_are_removed(fake_core):
    with pytest.warns(UserWarning, match="ignored") as recorded:
        await _AsyncConnection.connect(
            f"{SERVER};UID=user;PWD=dummy;Trusted_Connection=Yes",
            token_provider=provider(),
        )
    assert recorded[0].filename == __file__
    assert fake_core.await_args.args[0] == {
        "server": "localhost",
        "database": "testdb",
        "access_token": TOKEN,
    }


@pytest.mark.asyncio
async def test_provider_scope_signature_error_is_interface_error(fake_core):
    class Credential:
        def get_token(self):
            return SimpleNamespace(token=TOKEN)

    with pytest.raises(InterfaceError, match="scope positional"):
        await _AsyncConnection.connect(SERVER, token_provider=Credential())
    fake_core.assert_not_awaited()


@pytest.mark.parametrize(
    "failure", (TypeError("body failure"), ValueError("body failure"), RuntimeError("body failure"))
)
@pytest.mark.asyncio
async def test_provider_body_errors_preserve_cause(failure, fake_core):
    credential = provider()
    credential.get_token.side_effect = failure
    with pytest.raises(OperationalError) as caught:
        await _AsyncConnection.connect(SERVER, token_provider=credential)
    assert caught.value.__cause__ is failure
    fake_core.assert_not_awaited()


@pytest.mark.asyncio
async def test_async_credential_is_rejected_and_coroutine_closed(fake_core):
    async def acquire(scope):
        return SimpleNamespace(token=TOKEN)

    coroutine = acquire(_SQL_SCOPE)
    credential = provider()
    credential.get_token.return_value = coroutine
    with pytest.raises(InterfaceError, match="async credential"):
        await _AsyncConnection.connect(SERVER, token_provider=credential)
    assert coroutine.cr_frame is None
    fake_core.assert_not_awaited()


@pytest.mark.parametrize("expires_on", (None, True, "unknown", int(time.time()) + 3600))
@pytest.mark.asyncio
async def test_optional_expiry_does_not_require_pooling(expires_on, fake_core):
    await _AsyncConnection.connect(SERVER, token_provider=provider(expires_on=expires_on))
    assert fake_core.await_args.args[0]["access_token"] == TOKEN


@pytest.mark.asyncio
async def test_expired_provider_token_logs_warning(fake_core, monkeypatch):
    log = Mock()
    monkeypatch.setattr("mssql_python.auth.logger", log)
    await _AsyncConnection.connect(SERVER, token_provider=provider(expires_on=1))
    assert "already expired" in log.warning.call_args.args[0]


@pytest.mark.asyncio
async def test_token_is_not_logged_or_exposed_in_connection_repr(fake_core, monkeypatch):
    log = Mock(is_debug_enabled=False)
    monkeypatch.setattr("mssql_python.auth.logger", log)
    monkeypatch.setattr(async_auth, "logger", log)
    monkeypatch.setattr(connection_module, "logger", log)
    connection = await _AsyncConnection.connect(SERVER, token_provider=provider())
    assert TOKEN not in str(log.mock_calls)
    assert TOKEN not in repr(connection)


@pytest.mark.asyncio
async def test_input_context_is_not_mutated():
    original = {"server": "localhost", "database": "testdb"}
    first = await async_auth.prepare_async_authentication(original, provider("first"))
    second = await async_auth.prepare_async_authentication(original, provider("second"))
    assert original == {"server": "localhost", "database": "testdb"}
    assert first["access_token"] == "first"
    assert second["access_token"] == "second"
    assert first is not second


@pytest.mark.parametrize("builtin", (False, True))
@pytest.mark.asyncio
async def test_acquisition_does_not_block_loop_and_cancellation_prevents_login(
    builtin, credentials, fake_core
):
    loop = asyncio.get_running_loop()
    started = asyncio.Event()
    release = threading.Event()
    finished = threading.Event()
    event_loop_thread = threading.get_ident()

    def blocking_get_token(scope):
        assert threading.get_ident() != event_loop_thread
        loop.call_soon_threadsafe(started.set)
        try:
            assert release.wait(5), "Test did not release credential worker"
            return SimpleNamespace(token=TOKEN)
        finally:
            finished.set()

    credential = provider()
    if builtin:
        credential = credentials["DefaultAzureCredential"].return_value
    credential.get_token.side_effect = blocking_get_token
    task = asyncio.create_task(
        _AsyncConnection.connect(f"{SERVER};Authentication=ActiveDirectoryDefault")
        if builtin
        else _AsyncConnection.connect(SERVER, token_provider=credential)
    )
    try:
        await asyncio.wait_for(started.wait(), 2)
        await asyncio.sleep(0)
        assert not task.done()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        fake_core.assert_not_awaited()
    finally:
        release.set()
        await asyncio.to_thread(finished.wait, 2)
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    fake_core.assert_not_awaited()


@pytest.mark.parametrize(
    ("connection_str", "timeout"),
    (
        ("Database=testdb", 0),
        (SERVER + "\x00", 0),
        (SERVER, -1),
        (SERVER, True),
        (SERVER, "10"),
        (SERVER + ";PacketSize=invalid", 0),
    ),
)
@pytest.mark.asyncio
async def test_invalid_options_fail_before_token_acquisition(connection_str, timeout, fake_core):
    credential = provider()
    with pytest.raises((InterfaceError, ValueError, TypeError)):
        await _AsyncConnection.connect(connection_str, timeout=timeout, token_provider=credential)
    credential.get_token.assert_not_called()
    fake_core.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_native_dependency_does_not_trigger_sign_in(monkeypatch):
    credential = provider()

    def unavailable():
        raise ImportError("Missing py-core")

    monkeypatch.setattr(connection_module, "load_py_core", unavailable)
    with pytest.raises(ImportError, match="Missing py-core"):
        await _AsyncConnection.connect(SERVER, token_provider=credential)
    credential.get_token.assert_not_called()


@pytest.mark.parametrize(
    "suffix",
    (
        ";UID=user;PWD=dummy",
        ";Trusted_Connection=Yes",
        ";Authentication=SqlPassword;UID=user;PWD=dummy",
    ),
)
@pytest.mark.asyncio
async def test_non_entra_flows_are_unchanged(suffix, fake_core, credentials):
    expected = build_async_connection_context(SERVER + suffix, 0)
    await _AsyncConnection.connect(SERVER + suffix, token_provider=None)
    assert fake_core.await_args.args[0] == expected
    for constructor in credentials.values():
        constructor.assert_not_called()


@pytest.mark.asyncio
async def test_queries_transactions_and_close_do_not_reacquire_token(fake_core):
    credential = provider()
    connection = await _AsyncConnection.connect(SERVER, token_provider=credential)
    cursor = connection.cursor()
    await cursor.execute("SELECT 1")
    await connection.commit()
    await connection.rollback()
    await cursor.close()
    await connection.close()
    credential.get_token.assert_called_once_with(_SQL_SCOPE)
    fake_core.assert_awaited_once()


@pytest.mark.asyncio
async def test_native_login_failure_is_not_retried(fake_core):
    failure = RuntimeError("native login failed")
    fake_core.side_effect = failure
    credential = provider()
    with pytest.raises(RuntimeError, match="native login failed"):
        await _AsyncConnection.connect(SERVER, token_provider=credential)
    fake_core.assert_awaited_once()
    credential.get_token.assert_called_once_with(_SQL_SCOPE)


@pytest.fixture
def service_principal_sdk(monkeypatch):
    import azure.identity

    credential = provider()
    constructor = Mock(return_value=credential)
    monkeypatch.setattr(azure.identity, "ClientSecretCredential", constructor)
    # This cache is owned by the reused, transport-independent SP factory.
    with _credential_cache_lock:
        saved = dict(_credential_cache)
        _credential_cache.clear()
    yield constructor
    with _credential_cache_lock:
        _credential_cache.clear()
        _credential_cache.update(saved)


async def connect_service_principal(fake_core, uid="client-id", secret="dummy-secret"):
    credentials = (f";UID={uid}" if uid else "") + (f";PWD={secret}" if secret else "")
    await _AsyncConnection.connect(
        f"{SERVER};Authentication=ActiveDirectoryServicePrincipal{credentials}"
    )
    return fake_core.await_args.args[0]["entra_id_token_factory"]


@pytest.mark.parametrize(("uid", "secret"), (("", "dummy"), ("client", ""), ("", "")))
@pytest.mark.asyncio
async def test_service_principal_requires_client_id_and_secret(uid, secret, fake_core):
    with pytest.raises(InterfaceError, match="requires UID"):
        await connect_service_principal(fake_core, uid, secret)
    fake_core.assert_not_awaited()


@pytest.mark.asyncio
async def test_service_principal_context_preserves_auth_method_for_native_resolver(fake_core):
    factory = await connect_service_principal(fake_core)
    assert callable(factory)
    assert fake_core.await_args.args[0] == {
        "server": "localhost",
        "database": "testdb",
        "authentication": "ActiveDirectoryServicePrincipal",
        "user_name": "client-id",
        "password": "dummy-secret",
        "entra_id_token_factory": factory,
    }


@pytest.mark.parametrize(
    ("sts_url", "tenant"),
    (
        ("https://login.microsoftonline.com/tenant-guid/", "tenant-guid"),
        ("https://login.microsoftonline.com/tenant-guid", "tenant-guid"),
        ("https://login.microsoftonline.com/contoso.onmicrosoft.com/", "contoso.onmicrosoft.com"),
        ("https://login.microsoftonline.com/tenant-guid/?query=value", "tenant-guid"),
        ("https://login.microsoftonline.com/tenant-guid/oauth2/authorize", "tenant-guid"),
    ),
)
@pytest.mark.parametrize("spn", ("https://database.windows.net/", _SQL_SCOPE))
@pytest.mark.asyncio
async def test_service_principal_callback_scope_tenant_and_wire_bytes(
    sts_url, tenant, spn, fake_core, service_principal_sdk
):
    factory = await connect_service_principal(fake_core)
    result = await asyncio.to_thread(factory, spn, sts_url, "activedirectoryserviceprincipal")
    assert result == TOKEN.encode("utf-16-le")
    kwargs = service_principal_sdk.call_args.kwargs
    assert kwargs["tenant_id"] == tenant
    assert kwargs["client_id"] == "client-id"
    assert kwargs["client_secret"] == "dummy-secret"
    service_principal_sdk.return_value.get_token.assert_called_once_with(_SQL_SCOPE)


@pytest.mark.parametrize(
    "sts_url",
    (
        "",
        "tenant-guid",
        "/tenant-guid/oauth2",
        "https://login.microsoftonline.com/",
        "http://login.microsoftonline.com/tenant/",
        "https://login.microsoftonline.com/common/",
        "https://login.microsoftonline.com/organizations/",
        "https://login.microsoftonline.com/consumers/",
        "https://login.microsoftonline.com/COMMON/",
    ),
)
@pytest.mark.asyncio
async def test_service_principal_rejects_invalid_tenant(sts_url, fake_core, service_principal_sdk):
    factory = await connect_service_principal(fake_core)
    with pytest.raises(RuntimeError, match="tenant_id"):
        await asyncio.to_thread(factory, _SQL_SCOPE, sts_url, "activedirectoryserviceprincipal")
    service_principal_sdk.assert_not_called()


@pytest.mark.asyncio
async def test_service_principal_rejects_empty_spn(fake_core, service_principal_sdk):
    factory = await connect_service_principal(fake_core)
    with pytest.raises(RuntimeError, match="empty SPN"):
        await asyncio.to_thread(
            factory,
            "",
            "https://login.microsoftonline.com/tenant/",
            "activedirectoryserviceprincipal",
        )
    service_principal_sdk.assert_not_called()


@pytest.mark.asyncio
async def test_service_principal_credential_cache_reuse_and_secret_rotation(
    fake_core, service_principal_sdk
):
    for secret in ("first-secret", "first-secret", "rotated-secret"):
        factory = await connect_service_principal(fake_core, secret=secret)
        await asyncio.to_thread(
            factory,
            _SQL_SCOPE,
            "https://login.microsoftonline.com/tenant/",
            "activedirectoryserviceprincipal",
        )
    assert service_principal_sdk.call_count == 2
    assert service_principal_sdk.return_value.get_token.call_count == 3
    assert service_principal_sdk.call_args.kwargs["client_secret"] == "rotated-secret"
    assert len(_credential_cache) == 1


@pytest.mark.asyncio
async def test_service_principal_failure_is_explicit(fake_core, service_principal_sdk):
    from azure.core.exceptions import ClientAuthenticationError

    service_principal_sdk.return_value.get_token.side_effect = ClientAuthenticationError("denied")
    factory = await connect_service_principal(fake_core)
    with pytest.raises(RuntimeError, match="ServicePrincipal authentication failed"):
        await asyncio.to_thread(
            factory,
            _SQL_SCOPE,
            "https://login.microsoftonline.com/tenant/",
            "activedirectoryserviceprincipal",
        )


@pytest.mark.asyncio
async def test_installed_native_callback_bridge_runs_python_off_loop(
    fake_core, service_principal_sdk
):
    core = load_py_core()
    if not hasattr(core, "_invoke_entra_id_token_factory"):
        pytest.skip("Installed py-core does not expose its callback test hook")
    loop_thread = threading.get_ident()
    credential = service_principal_sdk.return_value

    def get_token(scope):
        assert threading.get_ident() != loop_thread
        return SimpleNamespace(token=TOKEN)

    credential.get_token.side_effect = get_token
    factory = await connect_service_principal(fake_core)
    result = await asyncio.to_thread(
        core._invoke_entra_id_token_factory,
        factory,
        _SQL_SCOPE,
        "https://login.microsoftonline.com/tenant/",
    )
    assert bytes(result) == TOKEN.encode("utf-16-le")


@pytest.mark.parametrize("service_principal", (False, True))
@pytest.mark.asyncio
async def test_installed_async_native_accepts_entra_context_before_network(service_principal):
    core = load_py_core()
    context = {"server": "127.0.0.1,1", "connect_timeout": 1, "connect_retry_count": 0}
    if service_principal:
        context.update(
            authentication="ActiveDirectoryServicePrincipal",
            user_name="dummy-client",
            password="dummy-secret",
        )
        context = await async_auth.prepare_async_authentication(context)
    else:
        context = await async_auth.prepare_async_authentication(context, provider())
    with pytest.raises(RuntimeError, match="Failed to connect to SQL Server"):
        await core.PyAsyncConnection.connect(context)


@pytest.mark.asyncio
async def test_live_configured_authentication_login_and_query(async_connection):
    assert async_connection.is_connected()
    cursor = async_connection.cursor()
    try:
        await cursor.execute("SELECT CAST(? AS INT)", 1)
        assert tuple(await cursor.fetchone()) == (1,)
    finally:
        await cursor.close()
