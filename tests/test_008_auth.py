"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
Tests for the auth module.
"""

import pytest
import platform
import sys
import threading
from unittest.mock import patch, MagicMock
from mssql_python.auth import (
    AADAuth,
    ServicePrincipalAuth,
    _parse_tenant_id,
    process_auth_parameters,
    remove_sensitive_params,
    get_auth_token,
    extract_auth_type,
    _credential_cache,
    _credential_cache_lock,
)
from mssql_python.constants import AuthType, ConstantsDDBC
import secrets

SAMPLE_TOKEN = secrets.token_hex(44)


@pytest.fixture(autouse=True)
def setup_azure_identity():
    """Setup mock azure.identity module"""

    class MockToken:
        token = SAMPLE_TOKEN

    class MockDefaultAzureCredential:
        def get_token(self, scope):
            return MockToken()

    class MockDeviceCodeCredential:
        def get_token(self, scope):
            return MockToken()

    class MockInteractiveBrowserCredential:
        def get_token(self, scope):
            return MockToken()

    class MockClientSecretCredential:
        # Captures construction kwargs and get_token args so ServicePrincipal
        # tests can assert the right tenant/client_id/secret/scope flowed
        # through from the connection string + STS URL.
        last_init_kwargs = None
        last_scope = None

        def __init__(self, **kwargs):
            MockClientSecretCredential.last_init_kwargs = kwargs

        def get_token(self, scope):
            MockClientSecretCredential.last_scope = scope
            return MockToken()

    class MockManagedIdentityCredential:
        # Captures construction kwargs so user-assigned MSI tests can assert
        # client_id was forwarded correctly.
        last_init_kwargs = None

        def __init__(self, **kwargs):
            MockManagedIdentityCredential.last_init_kwargs = kwargs

        def get_token(self, scope):
            return MockToken()

    class MockRequestsTransport:
        # Captures construction kwargs so the SP factory's timeout config
        # can be asserted.
        last_init_kwargs = None

        def __init__(self, **kwargs):
            MockRequestsTransport.last_init_kwargs = kwargs

    # Mock ClientAuthenticationError
    class MockClientAuthenticationError(Exception):
        pass

    class MockIdentity:
        DefaultAzureCredential = MockDefaultAzureCredential
        DeviceCodeCredential = MockDeviceCodeCredential
        InteractiveBrowserCredential = MockInteractiveBrowserCredential
        ClientSecretCredential = MockClientSecretCredential
        ManagedIdentityCredential = MockManagedIdentityCredential

    class MockCore:
        class exceptions:
            ClientAuthenticationError = MockClientAuthenticationError

        class pipeline:
            class transport:
                RequestsTransport = MockRequestsTransport

    # Create mock azure module if it doesn't exist
    if "azure" not in sys.modules:
        sys.modules["azure"] = type("MockAzure", (), {})()

    # Add identity and core modules to azure
    sys.modules["azure.identity"] = MockIdentity()
    sys.modules["azure.core"] = MockCore()
    sys.modules["azure.core.exceptions"] = MockCore.exceptions()
    sys.modules["azure.core.pipeline"] = MockCore.pipeline()
    sys.modules["azure.core.pipeline.transport"] = MockCore.pipeline.transport()

    yield

    # Cleanup
    for module in [
        "azure.identity",
        "azure.core",
        "azure.core.exceptions",
        "azure.core.pipeline",
        "azure.core.pipeline.transport",
    ]:
        if module in sys.modules:
            del sys.modules[module]


@pytest.fixture(autouse=True)
def clear_credential_cache():
    """Clear the module-level credential cache between tests."""
    _credential_cache.clear()
    yield
    _credential_cache.clear()


class TestAuthType:
    def test_auth_type_constants(self):
        assert AuthType.INTERACTIVE.value == "activedirectoryinteractive"
        assert AuthType.DEVICE_CODE.value == "activedirectorydevicecode"
        assert AuthType.DEFAULT.value == "activedirectorydefault"
        assert AuthType.MSI.value == "activedirectorymsi"
        assert AuthType.SERVICE_PRINCIPAL.value == "activedirectoryserviceprincipal"


class TestAADAuth:
    def test_get_token_struct(self):
        token_struct = AADAuth.get_token_struct(SAMPLE_TOKEN)
        assert isinstance(token_struct, bytes)
        assert len(token_struct) > 4

    def test_get_raw_token_default(self):
        raw_token = AADAuth.get_raw_token("default")
        assert isinstance(raw_token, str)
        assert raw_token == SAMPLE_TOKEN

    def test_get_token_default(self):
        token_struct = AADAuth.get_token("default")
        assert isinstance(token_struct, bytes)

    def test_get_token_device_code(self):
        token_struct = AADAuth.get_token("devicecode")
        assert isinstance(token_struct, bytes)

    def test_get_token_interactive(self):
        token_struct = AADAuth.get_token("interactive")
        assert isinstance(token_struct, bytes)

    def test_get_token_credential_mapping(self):
        # Test that all supported auth types work
        supported_types = ["default", "devicecode", "interactive"]
        for auth_type in supported_types:
            token_struct = AADAuth.get_token(auth_type)
            assert isinstance(token_struct, bytes)
            assert len(token_struct) > 4

    def test_get_token_client_authentication_error(self):
        """Test that ClientAuthenticationError is properly handled"""
        from azure.core.exceptions import ClientAuthenticationError

        # Create a mock credential that raises ClientAuthenticationError
        class MockFailingCredential:
            def get_token(self, scope):
                raise ClientAuthenticationError("Mock authentication failed")

        # Use monkeypatch to mock the credential creation
        def mock_get_token_failing(auth_type):
            from azure.core.exceptions import ClientAuthenticationError

            if auth_type == "default":
                try:
                    credential = MockFailingCredential()
                    token = credential.get_token("https://database.windows.net/.default").token
                    return AADAuth.get_token_struct(token)
                except ClientAuthenticationError as e:
                    raise RuntimeError(
                        f"Azure AD authentication failed for MockFailingCredential: {e}. "
                        f"This could be due to invalid credentials, missing environment variables, "
                        f"user cancellation, network issues, or unsupported configuration."
                    ) from e
            else:
                return AADAuth.get_token(auth_type)

        with pytest.raises(RuntimeError, match="Azure AD authentication failed"):
            mock_get_token_failing("default")

    def test_get_token_general_exception_handling_init_error(self):
        """Test general Exception handling during credential initialization (Lines 52-56)."""

        # Test by modifying the mock credential classes to raise exceptions
        import sys

        # Get the current azure.identity module (which is mocked)
        azure_identity = sys.modules["azure.identity"]

        # Store original credentials
        original_default = azure_identity.DefaultAzureCredential
        original_device = azure_identity.DeviceCodeCredential
        original_interactive = azure_identity.InteractiveBrowserCredential

        # Create a mock credential that raises exceptions during initialization
        class MockCredentialWithInitError:
            def __init__(self):
                raise ValueError("Mock credential initialization failed")

            def get_token(self, scope):
                pass  # Won't be reached

        try:
            # Test DefaultAzureCredential initialization error
            azure_identity.DefaultAzureCredential = MockCredentialWithInitError

            with pytest.raises(RuntimeError) as exc_info:
                AADAuth.get_token("default")

            # Verify the error message format (lines 54-56)
            error_message = str(exc_info.value)
            assert "Failed to create MockCredentialWithInitError" in error_message
            assert "Mock credential initialization failed" in error_message

            # Verify exception chaining is preserved (from e)
            assert exc_info.value.__cause__ is not None
            assert isinstance(exc_info.value.__cause__, ValueError)

            # Test different exception types
            class MockCredentialWithTypeError:
                def __init__(self):
                    raise TypeError("Invalid argument type passed")

            azure_identity.DeviceCodeCredential = MockCredentialWithTypeError

            with pytest.raises(RuntimeError) as exc_info:
                AADAuth.get_token("devicecode")

            assert "Failed to create MockCredentialWithTypeError" in str(exc_info.value)
            assert "Invalid argument type passed" in str(exc_info.value)
            assert isinstance(exc_info.value.__cause__, TypeError)

        finally:
            # Restore original credentials
            azure_identity.DefaultAzureCredential = original_default
            azure_identity.DeviceCodeCredential = original_device
            azure_identity.InteractiveBrowserCredential = original_interactive

    def test_get_token_general_exception_handling_token_error(self):
        """Test general Exception handling during token retrieval (Lines 52-56)."""

        import sys

        azure_identity = sys.modules["azure.identity"]

        # Store original credentials
        original_interactive = azure_identity.InteractiveBrowserCredential

        # Create a credential that fails during get_token call
        class MockCredentialWithTokenError:
            def __init__(self):
                pass  # Successful initialization

            def get_token(self, scope):
                raise OSError("Network connection failed during token retrieval")

        try:
            azure_identity.InteractiveBrowserCredential = MockCredentialWithTokenError

            with pytest.raises(RuntimeError) as exc_info:
                AADAuth.get_token("interactive")

            # Verify the error message format (lines 54-56)
            error_message = str(exc_info.value)
            assert "Failed to create MockCredentialWithTokenError" in error_message
            assert "Network connection failed during token retrieval" in error_message

            # Verify exception chaining
            assert exc_info.value.__cause__ is not None
            assert isinstance(exc_info.value.__cause__, OSError)

        finally:
            # Restore original credential
            azure_identity.InteractiveBrowserCredential = original_interactive

    def test_get_token_various_exception_types_coverage(self):
        """Test coverage of different exception types (Lines 52-56)."""

        import sys

        azure_identity = sys.modules["azure.identity"]

        # Store original credential
        original_default = azure_identity.DefaultAzureCredential

        # Test different exception types that could occur
        exception_test_cases = [
            (ImportError, "Required dependency missing"),
            (AttributeError, "Missing required attribute"),
            (RuntimeError, "Custom runtime error"),
        ]

        for exception_type, exception_message in exception_test_cases:

            class MockCredentialWithCustomError:
                def __init__(self):
                    raise exception_type(exception_message)

            try:
                azure_identity.DefaultAzureCredential = MockCredentialWithCustomError

                with pytest.raises(RuntimeError) as exc_info:
                    AADAuth.get_token("default")

                # Verify the error message format (lines 54-56)
                error_message = str(exc_info.value)
                assert "Failed to create MockCredentialWithCustomError" in error_message
                assert exception_message in error_message

                # Verify exception chaining is preserved
                assert exc_info.value.__cause__ is not None
                assert isinstance(exc_info.value.__cause__, exception_type)

            finally:
                # Restore for next iteration
                azure_identity.DefaultAzureCredential = original_default


class TestProcessAuthParameters:
    def test_empty_parameters(self):
        auth_type = process_auth_parameters({})
        assert auth_type is None

    def test_interactive_auth_windows(self, monkeypatch):
        monkeypatch.setattr(platform, "system", lambda: "Windows")
        params = {"Authentication": "ActiveDirectoryInteractive", "Server": "test"}
        auth_type = process_auth_parameters(params)
        assert auth_type is None

    def test_interactive_auth_non_windows(self, monkeypatch):
        monkeypatch.setattr(platform, "system", lambda: "Darwin")
        params = {"Authentication": "ActiveDirectoryInteractive", "Server": "test"}
        auth_type = process_auth_parameters(params)
        assert auth_type == "interactive"

    def test_device_code_auth(self):
        params = {"Authentication": "ActiveDirectoryDeviceCode", "Server": "test"}
        auth_type = process_auth_parameters(params)
        assert auth_type == "devicecode"

    def test_default_auth(self):
        params = {"Authentication": "ActiveDirectoryDefault", "Server": "test"}
        auth_type = process_auth_parameters(params)
        assert auth_type == "default"

    def test_service_principal_auth_leaves_odbc_path_alone(self):
        """ServicePrincipal is handled natively by ODBC. process_auth_parameters
        must return None so the ODBC path doesn't pre-acquire a token (which
        would require tenant_id we don't have client-side). Bulkcopy still
        gets "serviceprincipal" from extract_auth_type."""
        params = {"Authentication": "ActiveDirectoryServicePrincipal", "Server": "test"}
        auth_type = process_auth_parameters(params)
        assert auth_type is None

    def test_service_principal_auth_case_insensitive(self):
        params = {"Authentication": "activedirectoryserviceprincipal", "Server": "test"}
        auth_type = process_auth_parameters(params)
        assert auth_type is None

    def test_msi_auth(self):
        params = {"Authentication": "ActiveDirectoryMSI", "Server": "test"}
        auth_type = process_auth_parameters(params)
        assert auth_type == "msi"

    def test_msi_auth_case_insensitive(self):
        params = {"Authentication": "activedirectorymsi", "Server": "test"}
        auth_type = process_auth_parameters(params)
        assert auth_type == "msi"


class TestRemoveSensitiveParams:
    def test_remove_sensitive_parameters(self):
        params = {
            "Server": "test",
            "UID": "user",
            "PWD": "password",
            "Encrypt": "yes",
            "TrustServerCertificate": "yes",
            "Authentication": "ActiveDirectoryDefault",
            "Trusted_Connection": "yes",
            "Database": "testdb",
        }
        filtered_params = remove_sensitive_params(params)
        assert "Server" in filtered_params
        assert "Database" in filtered_params
        assert "UID" not in filtered_params
        assert "PWD" not in filtered_params
        assert "Encrypt" in filtered_params
        assert "TrustServerCertificate" in filtered_params
        assert "Trusted_Connection" not in filtered_params
        assert "Authentication" not in filtered_params


class TestExtractAuthType:
    def test_interactive(self):
        assert (
            extract_auth_type({"Server": "test", "Authentication": "ActiveDirectoryInteractive"})
            == "interactive"
        )

    def test_default(self):
        assert (
            extract_auth_type({"Server": "test", "Authentication": "ActiveDirectoryDefault"})
            == "default"
        )

    def test_devicecode(self):
        assert (
            extract_auth_type({"Server": "test", "Authentication": "ActiveDirectoryDeviceCode"})
            == "devicecode"
        )

    def test_serviceprincipal(self):
        assert (
            extract_auth_type(
                {"Server": "test", "Authentication": "ActiveDirectoryServicePrincipal"}
            )
            == "serviceprincipal"
        )

    def test_msi(self):
        assert (
            extract_auth_type({"Server": "test", "Authentication": "ActiveDirectoryMSI"}) == "msi"
        )

    def test_no_auth(self):
        assert extract_auth_type({"Server": "test", "Database": "db"}) is None

    def test_unsupported_auth(self):
        assert extract_auth_type({"Server": "test", "Authentication": "SqlPassword"}) is None


class TestManagedIdentity:
    """Tests for ActiveDirectoryMSI support (system- and user-assigned)."""

    def test_get_token_system_assigned_msi(self):
        """System-assigned MSI: ManagedIdentityCredential() constructed with no kwargs."""
        az = sys.modules["azure.identity"]

        az.ManagedIdentityCredential.last_init_kwargs = None
        token_struct = AADAuth.get_token("msi")
        assert isinstance(token_struct, bytes)
        assert az.ManagedIdentityCredential.last_init_kwargs == {}

    def test_get_raw_token_system_assigned_msi(self):
        raw_token = AADAuth.get_raw_token("msi")
        assert raw_token == SAMPLE_TOKEN

    def test_get_token_user_assigned_msi(self):
        """User-assigned MSI: client_id is forwarded to the credential constructor."""
        az = sys.modules["azure.identity"]

        az.ManagedIdentityCredential.last_init_kwargs = None
        client_id = "11111111-2222-3333-4444-555555555555"
        token_struct = AADAuth.get_token("msi", {"client_id": client_id})
        assert isinstance(token_struct, bytes)
        assert az.ManagedIdentityCredential.last_init_kwargs == {"client_id": client_id}

    def test_msi_separate_cache_entries_per_client_id(self):
        """System-assigned and user-assigned MSI must not share a cached credential."""
        AADAuth.get_token("msi")  # system-assigned
        AADAuth.get_token("msi", {"client_id": "abc"})
        AADAuth.get_token("msi", {"client_id": "def"})

        # System-assigned uses the bare string key; user-assigned uses tuples.
        assert "msi" in _credential_cache
        assert ("msi", (("client_id", "abc"),)) in _credential_cache
        assert ("msi", (("client_id", "def"),)) in _credential_cache
        assert _credential_cache["msi"] is not _credential_cache[("msi", (("client_id", "abc"),))]

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_msi_auth_type_stored_on_connection(self, mock_ddbc_conn):
        """MSI with UID: Connection stores auth_type and credential_kwargs."""
        mock_ddbc_conn.return_value = MagicMock()
        from mssql_python import connect

        az = sys.modules["azure.identity"]
        az.ManagedIdentityCredential.last_init_kwargs = None

        conn = connect(
            "Server=test;Database=testdb;Authentication=ActiveDirectoryMSI;"
            "UID=11111111-2222-3333-4444-555555555555"
        )
        assert conn._auth_type == "msi"
        assert conn._credential_kwargs == {"client_id": "11111111-2222-3333-4444-555555555555"}
        # UID must be stripped from the sanitized connection string
        assert "UID=" not in conn.connection_str
        conn.close()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_msi_system_assigned_no_credential_kwargs(self, mock_ddbc_conn):
        """System-assigned MSI: no UID -> credential_kwargs is None."""
        mock_ddbc_conn.return_value = MagicMock()
        from mssql_python import connect

        conn = connect("Server=test;Database=testdb;Authentication=ActiveDirectoryMSI")
        assert conn._auth_type == "msi"
        assert conn._credential_kwargs is None
        conn.close()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_msi_braced_uid_value_is_unwrapped(self, mock_ddbc_conn):
        """A braced UID value (UID={hello=world}) must be unwrapped by the
        canonical _ConnectionStringParser; the inner '=' must NOT split."""
        mock_ddbc_conn.return_value = MagicMock()
        from mssql_python import connect

        conn = connect(
            "Server=test;Authentication=ActiveDirectoryMSI;" "UID={hello=world};Database=testdb"
        )
        assert conn._auth_type == "msi"
        assert conn._credential_kwargs == {"client_id": "hello=world"}
        conn.close()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_msi_braced_uid_with_semicolon_is_preserved(self, mock_ddbc_conn):
        """A braced UID value containing a semicolon (legal under ODBC) must
        be returned intact, not truncated at the inner ';'."""
        mock_ddbc_conn.return_value = MagicMock()
        from mssql_python import connect

        conn = connect(
            "Server=test;Authentication=ActiveDirectoryMSI;" "UID={abc;def;ghi};Database=testdb"
        )
        assert conn._auth_type == "msi"
        assert conn._credential_kwargs == {"client_id": "abc;def;ghi"}
        conn.close()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_msi_without_uid_is_system_assigned(self, mock_ddbc_conn):
        """MSI without UID at all should be treated as system-assigned."""
        mock_ddbc_conn.return_value = MagicMock()
        from mssql_python import connect

        conn = connect("Server=test;Authentication=ActiveDirectoryMSI;Database=testdb")
        assert conn._auth_type == "msi"
        assert conn._credential_kwargs is None
        # UID should not appear in the connection string
        assert "UID=" not in conn.connection_str
        conn.close()

    def test_bulkcopy_path_preserves_user_assigned_msi_client_id(self):
        """Regression test (cursor.bulkcopy() end-to-end) for the silent
        system-assigned fallback: the bulkcopy fresh-token code path must
        forward Connection._credential_kwargs to AADAuth.get_raw_token,
        not re-parse the (now UID-stripped) connection_str.

        Fails if cursor.py is reverted to call extract_credential_kwargs on
        self.connection.connection_str, OR if Connection stops persisting
        _credential_kwargs."""
        from mssql_python.cursor import Cursor

        client_id = "11111111-2222-3333-4444-555555555555"

        # Mock Connection holding what Connection.__init__ would store after
        # process_connection_string strips UID from the user-supplied string.
        mock_conn = MagicMock()
        # Post-sanitization string: NO UID. If cursor re-parses this, the
        # forwarded kwargs will be {} and the assert below will fail.
        mock_conn.connection_str = "Server=tcp:test.database.windows.net;Database=testdb;"
        mock_conn._auth_type = "msi"
        mock_conn._credential_kwargs = {"client_id": client_id}
        mock_conn._is_connected = True

        cursor = Cursor.__new__(Cursor)
        cursor._connection = mock_conn
        cursor._timeout = 0
        cursor.closed = False
        cursor.hstmt = None

        captured = {}

        def fake_get_raw_token(auth_type, credential_kwargs=None):
            captured["auth_type"] = auth_type
            captured["credential_kwargs"] = credential_kwargs
            return SAMPLE_TOKEN

        mock_pycore_cursor = MagicMock()
        mock_pycore_cursor.bulkcopy.return_value = {
            "rows_copied": 1,
            "batch_count": 1,
            "elapsed_time": 0.1,
        }
        mock_pycore_conn = MagicMock()
        mock_pycore_conn.cursor.return_value = mock_pycore_cursor
        mock_pycore_module = MagicMock()
        mock_pycore_module.PyCoreConnection = lambda ctx, **kwargs: mock_pycore_conn

        with (
            patch.dict("sys.modules", {"mssql_py_core": mock_pycore_module}),
            patch("mssql_python.auth.AADAuth.get_raw_token", side_effect=fake_get_raw_token),
        ):
            cursor.bulkcopy("dbo.test_table", [(1, "row")], timeout=10)

        assert captured["auth_type"] == "msi"
        assert captured["credential_kwargs"] == {"client_id": client_id}, (
            f"bulkcopy must forward Connection._credential_kwargs verbatim; "
            f"got {captured['credential_kwargs']!r}. If this is {{}} or None, "
            f"the cursor likely re-parses the (UID-stripped) connection_str."
        )


class TestCredentialInstanceCache:
    """Tests for the credential instance caching behavior."""

    def test_credential_reused_across_calls(self):
        """The same credential instance should be returned for repeated calls."""
        AADAuth.get_token("default")
        assert "default" in _credential_cache
        first_instance = _credential_cache["default"]

        AADAuth.get_token("default")
        assert _credential_cache["default"] is first_instance

    def test_different_auth_types_get_separate_instances(self):
        """Each auth type should have its own cached credential."""
        AADAuth.get_token("default")
        AADAuth.get_token("devicecode")

        assert "default" in _credential_cache
        assert "devicecode" in _credential_cache
        assert _credential_cache["default"] is not _credential_cache["devicecode"]

    def test_get_raw_token_uses_cached_credential(self):
        """get_raw_token should also use the cached credential instance."""
        AADAuth.get_token("default")
        cached = _credential_cache["default"]

        AADAuth.get_raw_token("default")
        assert _credential_cache["default"] is cached

    def test_cache_starts_empty(self):
        """Cache should be empty at the start due to the clear_credential_cache fixture."""
        assert len(_credential_cache) == 0

    def test_cached_credential_refreshes_token_after_expiry(self):
        """Verify that the cached credential instance returns fresh tokens on each call.

        This simulates what happens when Azure Identity SDK refreshes an expired
        token internally: because we cache the credential (not the token), each
        _acquire_token() call invokes get_token() on the same instance, giving
        the SDK the opportunity to return a refreshed token when the old one has
        expired.
        """
        import sys

        azure_identity = sys.modules["azure.identity"]
        original = azure_identity.DefaultAzureCredential

        call_count = 0
        tokens = ["initial_token_abc123", "refreshed_token_xyz789"]

        class MockCredentialWithRefresh:
            def get_token(self, scope):
                nonlocal call_count
                idx = min(call_count, len(tokens) - 1)
                call_count += 1

                class Token:
                    token = tokens[idx]

                return Token()

        try:
            azure_identity.DefaultAzureCredential = MockCredentialWithRefresh

            # First call — gets initial token
            _, raw_token_1 = AADAuth._acquire_token("default")
            assert raw_token_1 == "initial_token_abc123"
            assert call_count == 1

            # Same credential instance is cached
            cached = _credential_cache["default"]
            assert isinstance(cached, MockCredentialWithRefresh)

            # Second call — same credential instance, but SDK returns refreshed token
            # (simulating post-expiry refresh)
            _, raw_token_2 = AADAuth._acquire_token("default")
            assert raw_token_2 == "refreshed_token_xyz789"
            assert call_count == 2

            # Credential instance is still the same (not recreated)
            assert _credential_cache["default"] is cached
        finally:
            azure_identity.DefaultAzureCredential = original


class TestAcquireTokenImportError:
    """Test the ImportError path when azure-identity is not installed."""

    def test_import_error_raises_runtime_error(self):
        """_acquire_token raises RuntimeError when azure.identity is missing."""
        import sys

        # Temporarily remove the mocked azure modules
        saved = {}
        for mod_name in list(sys.modules):
            if mod_name == "azure" or mod_name.startswith("azure."):
                saved[mod_name] = sys.modules.pop(mod_name)

        # Make the import fail
        import builtins

        real_import = builtins.__import__

        def blocked_import(name, *args, **kwargs):
            if name.startswith("azure"):
                raise ImportError("No module named 'azure'")
            return real_import(name, *args, **kwargs)

        builtins.__import__ = blocked_import
        try:
            with pytest.raises(
                RuntimeError, match="Azure authentication libraries are not installed"
            ):
                AADAuth._acquire_token("default")
        finally:
            builtins.__import__ = real_import
            sys.modules.update(saved)


class TestAcquireTokenClientAuthError:
    """Test the ClientAuthenticationError path inside _acquire_token."""

    def test_client_auth_error_in_acquire_token(self):
        """ClientAuthenticationError during get_token is wrapped in RuntimeError."""
        import sys

        azure_identity = sys.modules["azure.identity"]
        original = azure_identity.DefaultAzureCredential

        from azure.core.exceptions import ClientAuthenticationError

        class FailingCredential:
            def get_token(self, scope):
                raise ClientAuthenticationError("token request denied")

        try:
            azure_identity.DefaultAzureCredential = FailingCredential
            with pytest.raises(RuntimeError, match="Azure AD authentication failed"):
                AADAuth._acquire_token("default")
        finally:
            azure_identity.DefaultAzureCredential = original


class TestProcessAuthParametersEdgeCases:
    """Cover edge cases for dict-based process_auth_parameters."""

    def test_no_authentication_key(self):
        params = {"Server": "test", "Database": "db"}
        auth_type = process_auth_parameters(params)
        assert auth_type is None

    def test_empty_authentication_value(self):
        params = {"Server": "test", "Authentication": "", "Database": "db"}
        auth_type = process_auth_parameters(params)
        assert auth_type is None


class TestTokenFailureFallthrough:
    """Verify that connect() succeeds without a token when credential creation fails."""

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_connect_proceeds_without_token_on_credential_failure(self, mock_ddbc_conn):
        """When auth type is detected but token acquisition fails,
        the connection should still be attempted (just without a token)."""
        mock_ddbc_conn.return_value = MagicMock()
        import sys

        azure_identity = sys.modules["azure.identity"]
        original = azure_identity.DefaultAzureCredential

        class CredentialThatAlwaysFails:
            def __init__(self):
                raise RuntimeError("cannot create credential")

        try:
            azure_identity.DefaultAzureCredential = CredentialThatAlwaysFails
            from mssql_python import connect

            conn = connect("Server=test;Authentication=ActiveDirectoryDefault;Database=testdb")
            assert conn._auth_type == "default"
            # Token should not be in attrs_before since acquisition failed
            assert ConstantsDDBC.SQL_COPT_SS_ACCESS_TOKEN.value not in conn._attrs_before
            conn.close()
        finally:
            azure_identity.DefaultAzureCredential = original


class TestGetAuthTokenEdgeCases:
    """Cover the Windows-interactive and token-failure branches."""

    def test_no_auth_type_returns_none(self):
        result = get_auth_token(None)
        assert result is None

    def test_empty_auth_type_returns_none(self):
        result = get_auth_token("")
        assert result is None

    def test_windows_interactive_returns_none(self, monkeypatch):
        monkeypatch.setattr(platform, "system", lambda: "Windows")
        result = get_auth_token("interactive")
        assert result is None

    def test_token_acquisition_failure_returns_none(self):
        """When AADAuth.get_token raises, get_auth_token returns None."""
        import sys

        azure_identity = sys.modules["azure.identity"]
        original = azure_identity.DefaultAzureCredential

        class FailingCredential:
            def __init__(self):
                raise RuntimeError("credential creation exploded")

        try:
            azure_identity.DefaultAzureCredential = FailingCredential
            result = get_auth_token("default")
            assert result is None
        finally:
            azure_identity.DefaultAzureCredential = original


def test_acquire_token_unsupported_auth_type():
    with pytest.raises(ValueError, match="Unsupported auth_type 'bogus'"):
        AADAuth._acquire_token("bogus")


class TestConnectionAuthType:
    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_auth_type_stored_on_connection(self, mock_ddbc_conn):
        mock_ddbc_conn.return_value = MagicMock()
        from mssql_python import connect

        conn = connect("Server=test;Database=testdb;Authentication=ActiveDirectoryDefault")
        assert conn._auth_type == "default"
        conn.close()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_credential_kwargs_persisted_for_user_assigned_msi(self, mock_ddbc_conn):
        """Connection.__init__ must capture MSI client_id BEFORE
        remove_sensitive_params strips UID, and persist it on
        self._credential_kwargs so cursor.bulkcopy() can use it later."""
        mock_ddbc_conn.return_value = MagicMock()
        from mssql_python import connect

        client_id = "11111111-2222-3333-4444-555555555555"
        conn = connect(
            f"Server=test;Database=testdb;Authentication=ActiveDirectoryMSI;UID={client_id}"
        )
        assert conn._auth_type == "msi"
        assert conn._credential_kwargs == {"client_id": client_id}
        # And the connection_str on the Connection should NOT contain UID
        # (this is what makes _credential_kwargs the source of truth).
        assert "UID=" not in conn.connection_str
        conn.close()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_credential_kwargs_none_for_system_assigned_msi(self, mock_ddbc_conn):
        """System-assigned MSI: no UID → _credential_kwargs stays None."""
        mock_ddbc_conn.return_value = MagicMock()
        from mssql_python import connect

        conn = connect("Server=test;Database=testdb;Authentication=ActiveDirectoryMSI")
        assert conn._auth_type == "msi"
        assert conn._credential_kwargs is None
        conn.close()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_credential_kwargs_none_for_non_msi_auth(self, mock_ddbc_conn):
        """Non-MSI auth types must not pick up credential_kwargs even if
        UID is present (e.g. SQL auth UID)."""
        mock_ddbc_conn.return_value = MagicMock()
        from mssql_python import connect

        conn = connect(
            "Server=test;Database=testdb;Authentication=ActiveDirectoryDefault;UID=user@x"
        )
        assert conn._auth_type == "default"
        assert conn._credential_kwargs is None
        conn.close()


class TestCredentialCacheThreadSafety:
    """Verify thread-safe behavior of credential instance cache."""

    def test_concurrent_access_creates_only_one_instance(self):
        """Multiple threads calling get_token concurrently should result in
        exactly one credential instance per auth type in the cache."""
        import sys

        azure_identity = sys.modules["azure.identity"]
        original = azure_identity.DefaultAzureCredential

        instances_created = []

        class TrackingCredential:
            def __init__(self):
                instances_created.append(self)

            def get_token(self, scope):
                class Token:
                    token = SAMPLE_TOKEN

                return Token()

        try:
            azure_identity.DefaultAzureCredential = TrackingCredential

            errors = []
            barrier = threading.Barrier(10)

            def worker():
                try:
                    barrier.wait(timeout=5)
                    AADAuth.get_token("default")
                except Exception as e:
                    errors.append(e)

            threads = [threading.Thread(target=worker) for _ in range(10)]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=10)

            assert not errors, f"Threads raised errors: {errors}"
            # Only one credential instance should exist in the cache
            assert "default" in _credential_cache
            # All threads should use the same cached instance
            cached = _credential_cache["default"]
            assert isinstance(cached, TrackingCredential)
            # Due to the lock, only one instance should have been created
            assert len(instances_created) == 1
        finally:
            azure_identity.DefaultAzureCredential = original


class TestCacheStateAfterErrors:
    """Verify credential cache state after various error scenarios."""

    def test_client_auth_error_leaves_credential_in_cache(self):
        """When get_token raises ClientAuthenticationError, the credential
        instance should still remain in the cache since it was created
        successfully — only the token acquisition failed."""
        import sys

        azure_identity = sys.modules["azure.identity"]
        original = azure_identity.DefaultAzureCredential
        from azure.core.exceptions import ClientAuthenticationError

        class CredentialThatFailsGetToken:
            def get_token(self, scope):
                raise ClientAuthenticationError("token denied")

        try:
            azure_identity.DefaultAzureCredential = CredentialThatFailsGetToken

            with pytest.raises(RuntimeError, match="Azure AD authentication failed"):
                AADAuth._acquire_token("default")

            # Credential was created and cached before get_token failed
            assert "default" in _credential_cache
            assert isinstance(_credential_cache["default"], CredentialThatFailsGetToken)
        finally:
            azure_identity.DefaultAzureCredential = original

    def test_init_error_does_not_leave_stale_entry_in_cache(self):
        """When credential_class() raises during __init__, no entry should
        be left in _credential_cache since the dict assignment never completes."""
        import sys

        azure_identity = sys.modules["azure.identity"]
        original = azure_identity.DefaultAzureCredential

        class CredentialThatFailsInit:
            def __init__(self):
                raise ValueError("init exploded")

        try:
            azure_identity.DefaultAzureCredential = CredentialThatFailsInit

            with pytest.raises(RuntimeError, match="Failed to create"):
                AADAuth.get_token("default")

            # The cache should NOT contain a stale entry
            assert "default" not in _credential_cache
        finally:
            azure_identity.DefaultAzureCredential = original


class TestCacheOutputCorrectness:
    """Verify the returned token bytes are correct on both cache-miss and cache-hit."""

    def test_token_output_correct_on_cache_miss_and_hit(self):
        """get_token should return correct token bytes on both
        the initial (cache-miss) and subsequent (cache-hit) calls."""
        # First call — cache miss
        token_1 = AADAuth.get_token("default")
        assert isinstance(token_1, bytes)
        assert len(token_1) > 4
        expected = AADAuth.get_token_struct(SAMPLE_TOKEN)
        assert token_1 == expected

        # Second call — cache hit
        token_2 = AADAuth.get_token("default")
        assert isinstance(token_2, bytes)
        assert token_2 == expected

        # Same credential instance for both
        assert "default" in _credential_cache


class TestParseTenantId:
    def test_guid_tenant(self):
        url = "https://login.microsoftonline.com/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/"
        assert _parse_tenant_id(url) == "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

    def test_guid_tenant_no_trailing_slash(self):
        url = "https://login.microsoftonline.com/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        assert _parse_tenant_id(url) == "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

    def test_domain_tenant(self):
        url = "https://login.microsoftonline.com/contoso.onmicrosoft.com/"
        assert _parse_tenant_id(url) == "contoso.onmicrosoft.com"

    def test_tenant_with_query_string(self):
        url = "https://login.microsoftonline.com/tenant-guid/?foo=bar"
        assert _parse_tenant_id(url) == "tenant-guid"

    def test_extra_path_segments_after_tenant(self):
        url = "https://login.microsoftonline.com/tenant-guid/oauth2/authorize"
        assert _parse_tenant_id(url) == "tenant-guid"

    def test_empty_string(self):
        assert _parse_tenant_id("") is None

    def test_no_path(self):
        assert _parse_tenant_id("https://login.microsoftonline.com/") is None

    def test_rejects_bare_string_without_scheme(self):
        # urlparse puts a bare string into path; without a scheme/netloc check
        # this would be silently treated as a tenant id.
        assert _parse_tenant_id("tenant-guid") is None

    def test_rejects_path_only_url(self):
        assert _parse_tenant_id("/tenant-guid/oauth2") is None

    def test_rejects_http_scheme(self):
        # Azure AD STS URLs are always https. Reject http to avoid trusting
        # a downgraded URL.
        assert _parse_tenant_id("http://login.microsoftonline.com/tenant/") is None

    def test_rejects_common_alias(self):
        # Multi-tenant alias — confidential clients (SP) cannot auth against
        # it. Reject up front so the error surfaced is ours, not AADSTS50194.
        assert _parse_tenant_id("https://login.microsoftonline.com/common/") is None

    def test_rejects_organizations_alias(self):
        assert _parse_tenant_id("https://login.microsoftonline.com/organizations/") is None

    def test_rejects_consumers_alias(self):
        assert _parse_tenant_id("https://login.microsoftonline.com/consumers/") is None

    def test_rejects_reserved_alias_case_insensitive(self):
        # Defensive: AAD treats these as case-insensitive; we should too.
        assert _parse_tenant_id("https://login.microsoftonline.com/Common/") is None
        assert _parse_tenant_id("https://login.microsoftonline.com/COMMON/") is None


class TestServicePrincipalAuth:
    """Tests for the ActiveDirectoryServicePrincipal token factory."""

    def test_make_token_factory_returns_callable(self):
        factory = ServicePrincipalAuth.make_token_factory("client-id", "client-secret")
        assert callable(factory)

    def test_factory_requires_client_id(self):
        with pytest.raises(ValueError, match="client_id"):
            ServicePrincipalAuth.make_token_factory("", "client-secret")

    def test_factory_requires_client_secret(self):
        with pytest.raises(ValueError, match="client_secret"):
            ServicePrincipalAuth.make_token_factory("client-id", "")

    def test_factory_returns_utf16le_bytes(self):
        factory = ServicePrincipalAuth.make_token_factory("client-id", "client-secret")
        result = factory(
            "https://database.windows.net/",
            "https://login.microsoftonline.com/tenant-guid/",
            "activedirectoryserviceprincipal",
        )
        assert isinstance(result, bytes)
        # SAMPLE_TOKEN is hex chars (ASCII). UTF-16LE encoding doubles each byte
        # and inserts a 0x00 high byte after each ASCII char.
        assert result == SAMPLE_TOKEN.encode("utf-16-le")
        assert len(result) == len(SAMPLE_TOKEN) * 2

    def test_factory_forwards_credentials_to_ClientSecretCredential(self):
        az = sys.modules["azure.identity"]
        az.ClientSecretCredential.last_init_kwargs = None
        az.ClientSecretCredential.last_scope = None

        factory = ServicePrincipalAuth.make_token_factory(
            "11111111-2222-3333-4444-555555555555", "my-secret"
        )
        factory(
            "https://database.windows.net/",
            "https://login.microsoftonline.com/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/",
            "activedirectoryserviceprincipal",
        )

        kwargs = az.ClientSecretCredential.last_init_kwargs
        # tenant/client/secret must match — transport is asserted separately.
        assert kwargs["tenant_id"] == "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        assert kwargs["client_id"] == "11111111-2222-3333-4444-555555555555"
        assert kwargs["client_secret"] == "my-secret"

    def test_factory_passes_transport_with_explicit_timeouts(self):
        # Without explicit timeouts, azure-identity defaults can block the
        # mssql-py-core blocking-pool worker for tens of seconds on a slow
        # AAD endpoint. The factory must pass a bounded RequestsTransport.
        from azure.core.pipeline.transport import RequestsTransport

        RequestsTransport.last_init_kwargs = None
        az = sys.modules["azure.identity"]
        az.ClientSecretCredential.last_init_kwargs = None

        factory = ServicePrincipalAuth.make_token_factory("cid", "secret")
        factory(
            "https://database.windows.net/",
            "https://login.microsoftonline.com/tenant-guid/",
            "activedirectoryserviceprincipal",
        )

        # Transport is constructed with finite connection + read timeouts.
        t_kwargs = RequestsTransport.last_init_kwargs
        assert t_kwargs is not None, "RequestsTransport was never constructed"
        assert "connection_timeout" in t_kwargs
        assert "read_timeout" in t_kwargs
        assert isinstance(t_kwargs["connection_timeout"], (int, float))
        assert isinstance(t_kwargs["read_timeout"], (int, float))
        assert 0 < t_kwargs["connection_timeout"] <= 30
        assert 0 < t_kwargs["read_timeout"] <= 60

        # Credential receives the transport.
        cred_kwargs = az.ClientSecretCredential.last_init_kwargs
        assert "transport" in cred_kwargs

    def test_factory_builds_scope_from_spn(self):
        az = sys.modules["azure.identity"]
        az.ClientSecretCredential.last_scope = None

        factory = ServicePrincipalAuth.make_token_factory("cid", "secret")
        factory(
            "https://database.windows.net/",
            "https://login.microsoftonline.com/tenant/",
            "activedirectoryserviceprincipal",
        )
        assert az.ClientSecretCredential.last_scope == "https://database.windows.net/.default"

    def test_factory_keeps_existing_default_suffix(self):
        az = sys.modules["azure.identity"]
        az.ClientSecretCredential.last_scope = None

        factory = ServicePrincipalAuth.make_token_factory("cid", "secret")
        factory(
            "https://database.windows.net/.default",
            "https://login.microsoftonline.com/tenant/",
            "activedirectoryserviceprincipal",
        )
        assert az.ClientSecretCredential.last_scope == "https://database.windows.net/.default"

    def test_factory_errors_on_unparseable_sts_url(self):
        factory = ServicePrincipalAuth.make_token_factory("cid", "secret")
        with pytest.raises(RuntimeError, match="Could not extract tenant_id"):
            factory(
                "https://database.windows.net/",
                "https://login.microsoftonline.com/",  # no tenant segment
                "activedirectoryserviceprincipal",
            )

    def test_factory_propagates_authentication_error(self):
        from azure.core.exceptions import ClientAuthenticationError

        class FailingCred:
            def __init__(self, **kwargs):
                pass

            def get_token(self, scope):
                raise ClientAuthenticationError("AADSTS7000215: Invalid client secret")

        original = sys.modules["azure.identity"].ClientSecretCredential
        sys.modules["azure.identity"].ClientSecretCredential = FailingCred
        try:
            factory = ServicePrincipalAuth.make_token_factory("cid", "secret")
            with pytest.raises(RuntimeError, match="ServicePrincipal authentication failed"):
                factory(
                    "https://database.windows.net/",
                    "https://login.microsoftonline.com/tenant-guid/",
                    "activedirectoryserviceprincipal",
                )
        finally:
            sys.modules["azure.identity"].ClientSecretCredential = original

    def test_factory_does_not_leak_provider_message_in_runtime_error(self):
        """The user-facing RuntimeError must not echo the provider message
        (which can carry tenant ids, claims, or other sensitive context).
        Provider detail is preserved in debug logs only."""
        from azure.core.exceptions import ClientAuthenticationError

        secret_marker = "AADSTS7000215_SECRET_MARKER_in_provider_message"

        class FailingCred:
            def __init__(self, **kwargs):
                pass

            def get_token(self, scope):
                raise ClientAuthenticationError(secret_marker)

        original = sys.modules["azure.identity"].ClientSecretCredential
        sys.modules["azure.identity"].ClientSecretCredential = FailingCred
        try:
            factory = ServicePrincipalAuth.make_token_factory("cid", "secret")
            try:
                factory(
                    "https://database.windows.net/",
                    "https://login.microsoftonline.com/tenant-guid/",
                    "activedirectoryserviceprincipal",
                )
            except RuntimeError as e:
                full_chain = str(e)
                cause = e.__cause__
                while cause is not None:
                    full_chain += " || " + str(cause)
                    cause = getattr(cause, "__cause__", None)
                assert (
                    secret_marker not in full_chain
                ), f"Provider message leaked into surfaced exception chain: {full_chain}"
        finally:
            sys.modules["azure.identity"].ClientSecretCredential = original

    def test_factory_rejects_empty_spn(self):
        factory = ServicePrincipalAuth.make_token_factory("cid", "secret")
        with pytest.raises(RuntimeError, match="empty SPN"):
            factory(
                "",
                "https://login.microsoftonline.com/tenant-guid/",
                "activedirectoryserviceprincipal",
            )

    def test_factory_caches_credential_per_tenant(self):
        """ClientSecretCredential must be reused across calls for the same
        tenant so azure-identity's per-instance token cache actually works."""
        az = sys.modules["azure.identity"]
        construction_count = {"n": 0}

        original = az.ClientSecretCredential

        class _Tok:
            token = SAMPLE_TOKEN

        class CountingCred:
            def __init__(self, **kwargs):
                construction_count["n"] += 1

            def get_token(self, scope):
                return _Tok()

        az.ClientSecretCredential = CountingCred
        try:
            factory = ServicePrincipalAuth.make_token_factory("cid", "secret")
            sts = "https://login.microsoftonline.com/tenant-guid/"
            for _ in range(3):
                factory("https://database.windows.net/", sts, "activedirectoryserviceprincipal")
            assert construction_count["n"] == 1, (
                f"Expected 1 ClientSecretCredential construction across 3 calls, "
                f"got {construction_count['n']}"
            )
            # A different tenant should produce a second instance.
            factory(
                "https://database.windows.net/",
                "https://login.microsoftonline.com/other-tenant/",
                "activedirectoryserviceprincipal",
            )
            assert construction_count["n"] == 2
        finally:
            az.ClientSecretCredential = original

    def test_factory_rotates_credential_when_secret_changes(self):
        """A new client_secret for the same tenant+client_id MUST produce a new
        ClientSecretCredential instance. Without this, an external secret
        rotation would not invalidate the cached credential: azure-identity's
        internal token cache would keep returning the previously-issued token
        (good for up to ~1 hour) until expiry, masking the rotation."""
        az = sys.modules["azure.identity"]
        construction_count = {"n": 0}

        original = az.ClientSecretCredential

        class _Tok:
            token = SAMPLE_TOKEN

        class CountingCred:
            def __init__(self, **kwargs):
                construction_count["n"] += 1

            def get_token(self, scope):
                return _Tok()

        az.ClientSecretCredential = CountingCred
        try:
            sts = "https://login.microsoftonline.com/tenant-guid/"
            spn = "https://database.windows.net/"

            # Old secret, two calls -> 1 construction (cached)
            factory_old = ServicePrincipalAuth.make_token_factory("cid", "old-secret")
            factory_old(spn, sts, "activedirectoryserviceprincipal")
            factory_old(spn, sts, "activedirectoryserviceprincipal")
            assert construction_count["n"] == 1

            # Rotate the secret. Same tenant + client_id, different secret.
            # MUST produce a fresh ClientSecretCredential so azure-identity
            # cannot serve a stale token from its internal cache.
            factory_new = ServicePrincipalAuth.make_token_factory("cid", "new-secret")
            factory_new(spn, sts, "activedirectoryserviceprincipal")
            assert construction_count["n"] == 2, (
                f"Expected 2 ClientSecretCredential constructions after secret rotation, "
                f"got {construction_count['n']}. A rotated secret was silently ignored."
            )

            # Calling the new factory again should hit cache (1 more = 2 total)
            factory_new(spn, sts, "activedirectoryserviceprincipal")
            assert construction_count["n"] == 2

            # Calling the OLD factory again: the stale entry was evicted when
            # new-secret was inserted, so this reconstructs (3 total). This is
            # the correct behavior: after rotation, the old secret should not
            # linger in the cache.
            factory_old(spn, sts, "activedirectoryserviceprincipal")
            assert construction_count["n"] == 3, (
                f"Expected 3 constructions (old evicted on rotation), "
                f"got {construction_count['n']}"
            )
        finally:
            az.ClientSecretCredential = original

    def test_factory_cache_key_does_not_contain_raw_secret(self):
        """The cache key must hash the secret, never store it raw. Otherwise
        the secret is visible in process memory as part of the dict key."""
        from mssql_python.auth import _credential_cache

        secret_marker = "RAW_SECRET_MARKER_must_not_appear_in_cache_key"
        factory = ServicePrincipalAuth.make_token_factory("cid", secret_marker)
        factory(
            "https://database.windows.net/",
            "https://login.microsoftonline.com/tenant-guid/",
            "activedirectoryserviceprincipal",
        )
        for key in _credential_cache.keys():
            assert secret_marker not in repr(key), f"Raw secret leaked into cache key: {key!r}"
