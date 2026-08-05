"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
Tests for the auth module.
"""

import pytest
import collections
import platform
import sys
import threading
import warnings
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch, MagicMock
from mssql_python.auth import (
    AADAuth,
    ServicePrincipalAuth,
    TokenInfo,
    _parse_tenant_id,
    process_auth_parameters,
    remove_sensitive_params,
    get_auth_token_info,
    compute_identity_key,
    extract_auth_type,
    _credential_cache,
    acquire_token_from_credential,
    acquire_raw_token_from_credential,
    _SQL_SCOPE,
    _credential_cache_lock,
    _account_id_cache,
)
from azure.core.credentials import TokenCredential
from mssql_python.constants import AuthType, ConstantsDDBC
from mssql_python.exceptions import InterfaceError, OperationalError
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

    class MockAuthenticationRecord:
        # Minimal stand-in for azure.identity.AuthenticationRecord: only the
        # public home_account_id is consumed by the pooling key logic.
        home_account_id = "mock-home-account-id"

    class MockDeviceCodeCredential:
        def __init__(self, **kwargs):
            # Accept disable_automatic_authentication (and any future kwargs)
            # the same way real azure-identity credentials do.
            self._init_kwargs = kwargs

        def authenticate(self, **kwargs):
            return MockAuthenticationRecord()

        def get_token(self, scope):
            return MockToken()

    class MockInteractiveBrowserCredential:
        def __init__(self, **kwargs):
            self._init_kwargs = kwargs

        def authenticate(self, **kwargs):
            return MockAuthenticationRecord()

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

    # Prefer the real AuthenticationRequiredError so production code exercises
    # the exact exception type it will see in production; fall back to a local
    # subclass only when azure-identity is not installed (the scenario this mock
    # exists to cover). Either way the mocked module exposes the same class
    # object that production imports, so silent-first's except-clause matches.
    try:
        from azure.identity import AuthenticationRequiredError as MockAuthenticationRequiredError
    except ImportError:

        class MockAuthenticationRequiredError(MockClientAuthenticationError):
            """Local stand-in when azure-identity is unavailable."""

    class MockIdentity:
        DefaultAzureCredential = MockDefaultAzureCredential
        DeviceCodeCredential = MockDeviceCodeCredential
        InteractiveBrowserCredential = MockInteractiveBrowserCredential
        ClientSecretCredential = MockClientSecretCredential
        ManagedIdentityCredential = MockManagedIdentityCredential
        AuthenticationRequiredError = MockAuthenticationRequiredError

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
    """Clear the module-level credential and account-id caches between tests."""
    _credential_cache.clear()
    _account_id_cache.clear()
    yield
    _credential_cache.clear()
    _account_id_cache.clear()


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
                def __init__(self, **kwargs):
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
            def __init__(self, **kwargs):
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


class TestSilentFirstInteractive:
    """Regression tests for the silent-first pattern (interactive / device-code).

    Contract: ``_acquire_token`` must try ``get_token()`` silently first and
    invoke ``authenticate()`` ONLY when the SDK raises
    ``AuthenticationRequiredError`` — never on every acquisition. This guards
    against reverting to the old "authenticate() on every checkout" behavior,
    which would prompt the user (browser / device code) on each pooled reconnect.
    """

    def test_silent_success_never_calls_authenticate(self):
        """When get_token() succeeds silently, authenticate() is never invoked."""
        az = sys.modules["azure.identity"]
        original = az.InteractiveBrowserCredential

        class SilentCred:
            def __init__(self, **kwargs):
                self.authenticate_calls = 0

            def authenticate(self, **kwargs):
                self.authenticate_calls += 1

                class R:
                    home_account_id = "acct"

                return R()

            def get_token(self, scope):
                class T:
                    token = SAMPLE_TOKEN

                return T()

        try:
            az.InteractiveBrowserCredential = SilentCred
            AADAuth._acquire_token("interactive")
            cred = _credential_cache["interactive"]
            assert cred.authenticate_calls == 0, "authenticate() must not run on a silent success"
        finally:
            az.InteractiveBrowserCredential = original

    def test_authentication_required_triggers_authenticate_once(self):
        """AuthenticationRequiredError from the silent get_token() falls back to
        authenticate() exactly once, then retries get_token() successfully."""
        az = sys.modules["azure.identity"]
        original = az.DeviceCodeCredential
        auth_required = az.AuthenticationRequiredError

        class ChallengeCred:
            def __init__(self, **kwargs):
                self.authenticate_calls = 0
                self.get_token_calls = 0

            def authenticate(self, **kwargs):
                self.authenticate_calls += 1

                class R:
                    home_account_id = "device-acct"

                return R()

            def get_token(self, scope):
                self.get_token_calls += 1
                if self.get_token_calls == 1:
                    raise auth_required("interactive authentication required")

                class T:
                    token = SAMPLE_TOKEN

                return T()

        try:
            az.DeviceCodeCredential = ChallengeCred
            _, _, _, home_account_id = AADAuth._acquire_token("devicecode")
            cred = _credential_cache["devicecode"]
            # Exactly one interactive step, and get_token tried twice
            # (silent attempt that raised, then the post-authenticate retry).
            assert cred.authenticate_calls == 1
            assert cred.get_token_calls == 2
            assert home_account_id == "device-acct"
        finally:
            az.DeviceCodeCredential = original

    def test_subsequent_acquisitions_are_silent(self):
        """After the first interactive authenticate(), later acquisitions for
        the same cached credential acquire silently and never re-prompt."""
        az = sys.modules["azure.identity"]
        original = az.InteractiveBrowserCredential
        auth_required = az.AuthenticationRequiredError

        class OnceChallengeCred:
            def __init__(self, **kwargs):
                self.authenticate_calls = 0
                self.challenged = False

            def authenticate(self, **kwargs):
                self.authenticate_calls += 1

                class R:
                    home_account_id = "acct"

                return R()

            def get_token(self, scope):
                if not self.challenged:
                    self.challenged = True
                    raise auth_required("need interactive")

                class T:
                    token = SAMPLE_TOKEN

                return T()

        try:
            az.InteractiveBrowserCredential = OnceChallengeCred
            AADAuth._acquire_token("interactive")  # challenge -> authenticate()
            AADAuth._acquire_token("interactive")  # silent
            AADAuth._acquire_token("interactive")  # silent
            cred = _credential_cache["interactive"]
            assert cred.authenticate_calls == 1, "authenticate() must run only on the first prompt"
        finally:
            az.InteractiveBrowserCredential = original

    def test_home_account_id_is_cached_and_reused(self):
        """The home_account_id resolved by the interactive step is cached in
        _account_id_cache and surfaced on subsequent silent acquisitions."""
        az = sys.modules["azure.identity"]
        original = az.InteractiveBrowserCredential
        auth_required = az.AuthenticationRequiredError

        class OnceChallengeCred:
            def __init__(self, **kwargs):
                self.challenged = False

            def authenticate(self, **kwargs):
                class R:
                    home_account_id = "stable-acct"

                return R()

            def get_token(self, scope):
                if not self.challenged:
                    self.challenged = True
                    raise auth_required("need interactive")

                class T:
                    token = SAMPLE_TOKEN

                return T()

        try:
            az.InteractiveBrowserCredential = OnceChallengeCred
            _, _, _, hid1 = AADAuth._acquire_token("interactive")
            # The cache now holds the account id for the interactive cache key.
            assert "stable-acct" in _account_id_cache.values()
            _, _, _, hid2 = AADAuth._acquire_token("interactive")
            assert hid1 == "stable-acct"
            assert hid2 == "stable-acct"
        finally:
            az.InteractiveBrowserCredential = original


class TestAuthenticateInteractive:
    """Direct branch coverage for the _authenticate_interactive fallback helper."""

    def test_returns_none_when_credential_has_no_authenticate(self):
        """A credential/test double lacking authenticate() yields None so the
        caller falls back to the token-hash pool key."""

        class NoAuth:
            pass

        assert AADAuth._authenticate_interactive(NoAuth()) is None

    def test_uses_scopes_keyword_when_supported(self):
        captured = {}

        class Cred:
            def authenticate(self, **kwargs):
                captured.update(kwargs)

                class R:
                    home_account_id = "acct-1"

                return R()

        assert AADAuth._authenticate_interactive(Cred()) == "acct-1"
        assert "scopes" in captured

    def test_falls_back_to_no_args_when_scopes_rejected(self):
        """Older/mocked authenticate() signatures that reject the scopes
        keyword are retried with no arguments."""
        calls = {"with_scopes": 0, "no_args": 0}

        class Cred:
            def authenticate(self, *args, **kwargs):
                if kwargs:
                    calls["with_scopes"] += 1
                    raise TypeError("unexpected keyword argument 'scopes'")
                calls["no_args"] += 1

                class R:
                    home_account_id = "acct-2"

                return R()

        assert AADAuth._authenticate_interactive(Cred()) == "acct-2"
        assert calls["with_scopes"] == 1
        assert calls["no_args"] == 1

    def test_returns_none_when_record_lacks_home_account_id(self):
        class Cred:
            def authenticate(self, **kwargs):
                return object()  # no home_account_id attribute

        assert AADAuth._authenticate_interactive(Cred()) is None


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
        mock_conn._token_provider = None
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
            _, raw_token_1, _, _ = AADAuth._acquire_token("default")
            assert raw_token_1 == "initial_token_abc123"
            assert call_count == 1

            # Same credential instance is cached
            cached = _credential_cache["default"]
            assert isinstance(cached, MockCredentialWithRefresh)

            # Second call — same credential instance, but SDK returns refreshed token
            # (simulating post-expiry refresh)
            _, raw_token_2, _, _ = AADAuth._acquire_token("default")
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
    """Token acquisition must fail *closed*: when a token-backed auth type is
    detected but the credential cannot be created / the token cannot be
    acquired, connect() must raise rather than silently opening a connection
    with Authentication= stripped and no token attached."""

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_connect_raises_on_credential_failure(self, mock_ddbc_conn):
        """When auth type is detected but token acquisition fails, connect()
        must surface a clear error instead of proceeding without a token.

        The underlying Azure failure (RuntimeError from AADAuth) is re-wrapped
        at the DB-API boundary as InterfaceError so callers get a consistent,
        catchable driver exception.
        """
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

            with pytest.raises(InterfaceError):
                connect("Server=test;Authentication=ActiveDirectoryDefault;Database=testdb")
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


# ── Custom token_provider= parameter tests ──


class TestAcquireTokenFromCredential:
    """Tests for the acquire_token_from_credential helper."""

    def test_happy_path(self):
        """acquire_token_from_credential returns a token struct and expiry."""
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MagicMock(token=SAMPLE_TOKEN, expires_on=1893456000)
        token_struct, expires_on = acquire_token_from_credential(mock_cred)
        assert isinstance(token_struct, bytes)
        assert len(token_struct) > 4
        assert expires_on == 1893456000
        mock_cred.get_token.assert_called_once_with("https://database.windows.net/.default")

    def test_credential_raises_exception(self):
        """acquire_token_from_credential wraps credential errors in OperationalError."""
        mock_cred = MagicMock()
        mock_cred.get_token.side_effect = Exception("auth failed")
        with pytest.raises(OperationalError, match="Failed to acquire token from credential"):
            acquire_token_from_credential(mock_cred)

    def test_missing_token_attribute_raises_interface_error(self):
        """Token provider must return an object exposing a non-empty string .token."""
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = object()
        with pytest.raises(InterfaceError, match="non-empty"):
            acquire_token_from_credential(mock_cred)

    def test_non_string_token_raises_interface_error(self):
        """Token provider must return a .token value of type str."""
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MagicMock(token=123)
        with pytest.raises(InterfaceError, match="non-empty"):
            acquire_token_from_credential(mock_cred)

    def test_acquire_token_requests_commercial_cloud_scope(self):
        """The scope is hard-coded to the Azure commercial-cloud audience."""
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MagicMock(token=SAMPLE_TOKEN)
        acquire_token_from_credential(mock_cred)
        mock_cred.get_token.assert_called_once_with("https://database.windows.net/.default")

    def test_missing_expires_on_returns_none(self):
        """A token object without .expires_on yields expires_on=None (not an error)."""

        class MinimalToken:
            token = SAMPLE_TOKEN  # no expires_on attribute

        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MinimalToken()
        token_struct, expires_on = acquire_token_from_credential(mock_cred)
        assert isinstance(token_struct, bytes)
        assert expires_on is None

    def test_bytes_token_raises_interface_error(self):
        """A bytes .token (not str) is rejected just like other non-str values."""
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MagicMock(token=b"not_a_str_token")
        with pytest.raises(InterfaceError, match="non-empty"):
            acquire_token_from_credential(mock_cred)

    def test_whitespace_only_token_is_accepted(self):
        """Documents current behavior: a non-empty whitespace token passes the
        client-side check (validity is enforced server-side at login)."""
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MagicMock(token="   ", expires_on=None)
        token_struct, _ = acquire_token_from_credential(mock_cred)
        assert isinstance(token_struct, bytes)

    def test_credential_exception_preserved_as_cause(self):
        """The original credential error is chained as __cause__ for callers
        that want to catch the underlying azure-identity exception."""

        class ClientAuthenticationError(Exception):
            """Stand-in for azure.core.exceptions.ClientAuthenticationError."""

        original = ClientAuthenticationError("AADSTS700016")
        mock_cred = MagicMock()
        mock_cred.get_token.side_effect = original
        with pytest.raises(OperationalError) as exc_info:
            acquire_token_from_credential(mock_cred)
        assert exc_info.value.__cause__ is original

    def test_get_token_returns_none_raises_interface_error(self):
        """A credential whose get_token returns None is rejected clearly."""
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = None
        with pytest.raises(InterfaceError, match="non-empty"):
            acquire_token_from_credential(mock_cred)

    def test_realistic_length_jwt_round_trips(self):
        """A realistic ~1.5 KB JWT is encoded into the ODBC token struct without
        truncation (length prefix + UTF-16-LE body)."""
        big_jwt = "e" + "A" * 1500 + ".sig"
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MagicMock(token=big_jwt, expires_on=None)
        token_struct, _ = acquire_token_from_credential(mock_cred)
        # struct = 4-byte little-endian length prefix + UTF-16-LE token bytes.
        expected_body = big_jwt.encode("utf-16-le")
        assert token_struct[:4] == len(expected_body).to_bytes(4, "little")
        assert token_struct[4:] == expected_body


class TestAcquireRawTokenFromCredential:
    """Tests for the acquire_raw_token_from_credential helper."""

    def test_happy_path(self):
        """acquire_raw_token_from_credential returns the raw JWT string and expiry."""
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MagicMock(token=SAMPLE_TOKEN, expires_on=1893456000)
        raw_token, expires_on = acquire_raw_token_from_credential(mock_cred)
        assert raw_token == SAMPLE_TOKEN
        assert expires_on == 1893456000
        mock_cred.get_token.assert_called_once_with("https://database.windows.net/.default")

    def test_credential_raises_exception(self):
        """acquire_raw_token_from_credential wraps credential errors in OperationalError."""
        mock_cred = MagicMock()
        mock_cred.get_token.side_effect = Exception("auth failed")
        with pytest.raises(OperationalError, match="Failed to acquire token from credential"):
            acquire_raw_token_from_credential(mock_cred)

    def test_empty_string_token_raises_interface_error(self):
        """Empty token values are rejected as invalid provider output."""
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MagicMock(token="")
        with pytest.raises(InterfaceError, match="non-empty"):
            acquire_raw_token_from_credential(mock_cred)


class TestCustomTokenProviderConnect:
    """Tests for the token_provider= parameter on connect()."""

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_token_provider_happy_path(self, mock_ddbc_conn):
        """token_provider= validates the credential up front, captures the
        expiry, and stores the acquired token in attrs_before; the pool is keyed
        on the token hash (no deferred factory)."""
        mock_ddbc_conn.return_value = MagicMock()
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MagicMock(token=SAMPLE_TOKEN, expires_on=1893456000)
        from mssql_python import connect

        conn = connect("Server=test;Database=testdb", token_provider=mock_cred)
        assert conn._token_provider is mock_cred
        # Eager validate-once captures the expiry at connect().
        assert conn._token_expires_on == 1893456000
        # The token is keyed on its hash, not the provider object, so it is
        # eagerly placed in attrs_before and no factory is wired.
        assert conn._token_factory is None
        assert conn._attrs_before[ConstantsDDBC.SQL_COPT_SS_ACCESS_TOKEN.value] is not None
        # Existing auth_type should be None (no Authentication= in conn str)
        assert conn._auth_type is None
        conn.close()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_token_provider_plus_authentication_raises_interface_error(self, mock_ddbc_conn):
        """token_provider= + Authentication= raises InterfaceError."""
        mock_ddbc_conn.return_value = MagicMock()
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MagicMock(token=SAMPLE_TOKEN)
        from mssql_python import connect

        with pytest.raises(InterfaceError, match="Cannot specify both"):
            connect(
                "Server=test;Database=testdb;Authentication=ActiveDirectoryDefault",
                token_provider=mock_cred,
            )
        mock_cred.get_token.assert_not_called()
        mock_ddbc_conn.assert_not_called()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_token_provider_plus_authentication_via_kwargs_raises_interface_error(
        self, mock_ddbc_conn
    ):
        """token_provider= + Authentication via kwargs raises InterfaceError."""
        mock_ddbc_conn.return_value = MagicMock()
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MagicMock(token=SAMPLE_TOKEN)
        from mssql_python import connect

        with pytest.raises(InterfaceError, match="Cannot specify both"):
            connect(
                "Server=test;Database=testdb",
                token_provider=mock_cred,
                Authentication="ActiveDirectoryDefault",
            )
        mock_cred.get_token.assert_not_called()
        mock_ddbc_conn.assert_not_called()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_token_provider_plus_attrs_before_access_token_raises_interface_error(
        self, mock_ddbc_conn
    ):
        """token_provider= + manual attrs_before token is ambiguous and rejected."""
        mock_ddbc_conn.return_value = MagicMock()
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MagicMock(token=SAMPLE_TOKEN)
        from mssql_python import connect

        with pytest.raises(InterfaceError, match="SQL_COPT_SS_ACCESS_TOKEN"):
            connect(
                "Server=test;Database=testdb",
                token_provider=mock_cred,
                attrs_before={ConstantsDDBC.SQL_COPT_SS_ACCESS_TOKEN.value: b"existing_token"},
            )
        mock_cred.get_token.assert_not_called()
        mock_ddbc_conn.assert_not_called()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_token_provider_without_get_token_raises_interface_error(self, mock_ddbc_conn):
        """Passing an object without .get_token() raises InterfaceError."""
        mock_ddbc_conn.return_value = MagicMock()
        from mssql_python import connect

        with pytest.raises(InterfaceError, match="token_provider must have a .get_token"):
            connect("Server=test;Database=testdb", token_provider="not_a_credential")

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_token_provider_none_uses_existing_flow(self, mock_ddbc_conn):
        """token_provider=None (default) uses existing auth flow, no change."""
        mock_ddbc_conn.return_value = MagicMock()
        from mssql_python import connect

        conn = connect("Server=test;Database=testdb;Authentication=ActiveDirectoryDefault")
        assert conn._token_provider is None
        assert conn._auth_type == "default"
        conn.close()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_token_provider_with_non_auth_attrs_before(self, mock_ddbc_conn):
        """token_provider= works alongside non-auth attrs_before: the
        user-supplied attr is preserved alongside the eagerly-stored token."""
        mock_ddbc_conn.return_value = MagicMock()
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MagicMock(token=SAMPLE_TOKEN)
        from mssql_python import connect

        login_timeout_attr = 113  # SQL_ATTR_LOGIN_TIMEOUT
        conn = connect(
            "Server=test;Database=testdb",
            token_provider=mock_cred,
            attrs_before={login_timeout_attr: 30},
        )
        # The non-auth attr is preserved alongside the eagerly-stored token.
        assert conn._attrs_before[login_timeout_attr] == 30
        assert ConstantsDDBC.SQL_COPT_SS_ACCESS_TOKEN.value in conn._attrs_before
        conn.close()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_token_provider_get_token_failure_raises_operational_error(self, mock_ddbc_conn):
        """If token_provider.get_token() fails, connect() raises OperationalError."""
        mock_ddbc_conn.return_value = MagicMock()
        mock_cred = MagicMock()
        mock_cred.get_token.side_effect = Exception("token acquisition failed")
        from mssql_python import connect

        with pytest.raises(OperationalError, match="Failed to acquire token from credential"):
            connect("Server=test;Database=testdb", token_provider=mock_cred)

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_token_provider_with_non_callable_get_token_raises_interface_error(
        self, mock_ddbc_conn
    ):
        """Object with .get_token as a non-callable attribute raises InterfaceError."""
        mock_ddbc_conn.return_value = MagicMock()
        from mssql_python import connect

        class BadCredential:
            get_token = "not_a_method"

        with pytest.raises(InterfaceError, match="token_provider must have a .get_token"):
            connect("Server=test;Database=testdb", token_provider=BadCredential())

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_multiple_connections_share_same_token_provider(self, mock_ddbc_conn):
        """Two connections can share the same token provider object safely."""
        mock_ddbc_conn.return_value = MagicMock()
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MagicMock(token=SAMPLE_TOKEN)
        from mssql_python import connect

        conn1 = connect("Server=test1;Database=db1", token_provider=mock_cred)
        conn2 = connect("Server=test2;Database=db2", token_provider=mock_cred)
        assert conn1._token_provider is conn2._token_provider
        assert mock_cred.get_token.call_count == 2
        conn1.close()
        conn2.close()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_concurrent_connections_with_same_token_provider(self, mock_ddbc_conn):
        """Concurrent connect() calls with one token provider should succeed."""
        mock_ddbc_conn.return_value = MagicMock()
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MagicMock(token=SAMPLE_TOKEN)
        from mssql_python import connect

        def _open_and_close(i):
            conn = connect(f"Server=test{i};Database=testdb", token_provider=mock_cred)
            conn.close()

        with ThreadPoolExecutor(max_workers=8) as executor:
            list(executor.map(_open_and_close, range(20)))

        assert mock_cred.get_token.call_count == 20


class TestTokenProviderValidation:
    """Tests for token_provider get_token arity validation and the dropped-credential warning."""

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_connect_requests_commercial_cloud_scope(self, mock_ddbc_conn):
        """connect() requests the fixed commercial-cloud scope from the credential."""
        mock_ddbc_conn.return_value = MagicMock()
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MagicMock(token=SAMPLE_TOKEN)
        from mssql_python import connect

        conn = connect("Server=test;Database=testdb", token_provider=mock_cred)
        mock_cred.get_token.assert_called_once_with("https://database.windows.net/.default")
        conn.close()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_get_token_wrong_arity_raises_interface_error(self, mock_ddbc_conn):
        """A get_token() that cannot accept a scope argument is rejected up-front."""
        mock_ddbc_conn.return_value = MagicMock()
        from mssql_python import connect

        class ZeroArgCredential:
            def get_token(self):  # missing scope parameter
                return MagicMock(token=SAMPLE_TOKEN)

        # No up-front signature inspection: the call-time validation raises.
        with pytest.raises(InterfaceError, match="must accept a scope"):
            connect("Server=test;Database=testdb", token_provider=ZeroArgCredential())
        mock_ddbc_conn.assert_not_called()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_get_token_internal_typeerror_raises_operational_error(self, mock_ddbc_conn):
        """A TypeError raised *inside* get_token (a real bug in the credential, not
        an arity mismatch) is not blamed on the scope argument: it surfaces as
        OperationalError carrying the credential's own message, so the user sees
        their real bug instead of being told to fix the scope."""
        mock_ddbc_conn.return_value = MagicMock()
        from mssql_python import connect

        class BuggyCredential:
            def get_token(self, scope):
                # Real bug inside the body -- not an arity/binding problem.
                return "not-a-number" + 1

        with pytest.raises(OperationalError) as exc_info:
            connect("Server=test;Database=testdb", token_provider=BuggyCredential())
        # The scope-arity message must NOT be used for an internal bug.
        assert "must accept a scope" not in str(exc_info.value)
        mock_ddbc_conn.assert_not_called()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_get_token_with_scope_param_accepted(self, mock_ddbc_conn):
        """A well-formed get_token(scope) passes arity validation."""
        mock_ddbc_conn.return_value = MagicMock()
        from mssql_python import connect

        class GoodCredential:
            def get_token(self, scope):
                return MagicMock(token=SAMPLE_TOKEN, expires_on=1893456000)

        conn = connect("Server=test;Database=testdb", token_provider=GoodCredential())
        assert conn._token_expires_on == 1893456000
        conn.close()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_uninspectable_get_token_skips_validation(self, mock_ddbc_conn):
        """A get_token whose signature can't be introspected still works (no signature
        inspection happens; the real call is the source of truth)."""
        mock_ddbc_conn.return_value = MagicMock()
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MagicMock(token=SAMPLE_TOKEN, expires_on=1893456000)
        from mssql_python import connect

        conn = connect("Server=test;Database=testdb", token_provider=mock_cred)
        assert conn._token_expires_on == 1893456000
        conn.close()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_dropped_uid_pwd_emits_warning(self, mock_ddbc_conn):
        """UID/PWD in the connection string trigger a warning when token_provider is set."""
        mock_ddbc_conn.return_value = MagicMock()
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MagicMock(token=SAMPLE_TOKEN)
        from mssql_python import connect

        with pytest.warns(UserWarning, match="credential\\(s\\) are ignored"):
            conn = connect(
                "Server=test;Database=testdb;UID=user@test.com;PWD=secret",
                token_provider=mock_cred,
            )
        conn.close()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_no_warning_without_dropped_credentials(self, mock_ddbc_conn):
        """No 'ignored credentials' warning when the connection string has no UID/PWD."""
        mock_ddbc_conn.return_value = MagicMock()
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MagicMock(token=SAMPLE_TOKEN)
        from mssql_python import connect

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            conn = connect("Server=test;Database=testdb", token_provider=mock_cred)
        assert not any("are ignored" in str(w.message) for w in caught)
        conn.close()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_real_azure_style_signature_accepted(self, mock_ddbc_conn):
        """get_token(self, *scopes, **kwargs) — the real azure-identity shape —
        passes arity validation."""
        mock_ddbc_conn.return_value = MagicMock()
        from mssql_python import connect

        class AzureStyleCredential:
            def get_token(self, *scopes, **kwargs):
                return MagicMock(token=SAMPLE_TOKEN, expires_on=1893456000)

        conn = connect("Server=test;Database=testdb", token_provider=AzureStyleCredential())
        assert conn._token_expires_on == 1893456000
        conn.close()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_connection_string_sanitized_of_uid_pwd(self, mock_ddbc_conn):
        """UID/PWD are stripped from connection_str when token_provider is used."""
        mock_ddbc_conn.return_value = MagicMock()
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MagicMock(token=SAMPLE_TOKEN)
        from mssql_python import connect

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            conn = connect(
                "Server=test;Database=testdb;UID=user@test.com;PWD=secret",
                token_provider=mock_cred,
            )
        assert "UID=" not in conn.connection_str
        assert "PWD=" not in conn.connection_str
        assert "secret" not in conn.connection_str
        conn.close()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_missing_expires_on_sets_none(self, mock_ddbc_conn):
        """A credential whose token lacks .expires_on leaves _token_expires_on None."""
        mock_ddbc_conn.return_value = MagicMock()
        from mssql_python import connect

        class MinimalToken:
            token = SAMPLE_TOKEN  # no expires_on

        class MinimalCredential:
            def get_token(self, scope):
                return MinimalToken()

        conn = connect("Server=test;Database=testdb", token_provider=MinimalCredential())
        assert conn._token_expires_on is None
        # Token is eagerly stored in attrs_before (no deferred factory).
        assert ConstantsDDBC.SQL_COPT_SS_ACCESS_TOKEN.value in conn._attrs_before
        conn.close()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_dropped_trusted_connection_emits_warning(self, mock_ddbc_conn):
        """Trusted_Connection alone also triggers the dropped-credential warning."""
        mock_ddbc_conn.return_value = MagicMock()
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MagicMock(token=SAMPLE_TOKEN)
        from mssql_python import connect

        with pytest.warns(UserWarning, match="credential\\(s\\) are ignored"):
            conn = connect(
                "Server=test;Database=testdb;Trusted_Connection=yes",
                token_provider=mock_cred,
            )
        conn.close()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_async_credential_coroutine_rejected(self, mock_ddbc_conn):
        """An async credential returns a coroutine from a synchronous get_token()
        call and is rejected with a clear, async-specific InterfaceError (no
        un-awaited-coroutine warning, since the coroutine is closed)."""
        mock_ddbc_conn.return_value = MagicMock()
        from mssql_python import connect

        class AsyncCredential:
            async def get_token(self, scope):  # azure.identity.aio shape
                return MagicMock(token=SAMPLE_TOKEN)

        cred = AsyncCredential()
        with pytest.raises(InterfaceError, match="async credential"):
            connect("Server=test;Database=testdb", token_provider=cred)

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_hard_to_introspect_signature_not_warned_or_blocked(self, mock_ddbc_conn):
        """A credential with a hard-to-introspect signature (partial/decorated) is
        never rejected or warned at connect time — the real call is the source of
        truth, so it just succeeds when the call works."""
        mock_ddbc_conn.return_value = MagicMock()
        from mssql_python import connect

        class WorkingCredential:
            def get_token(self, scope):  # genuinely accepts a scope
                return MagicMock(token=SAMPLE_TOKEN, expires_on=1893456000)

        cred = WorkingCredential()
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            conn = connect("Server=test;Database=testdb", token_provider=cred)
        assert not any("does not appear to accept" in str(w.message) for w in caught)
        # Not blocked: the connection succeeded, captured the expiry, and stored
        # the token eagerly in attrs_before.
        assert conn._token_expires_on == 1893456000
        assert conn._token_factory is None
        assert ConstantsDDBC.SQL_COPT_SS_ACCESS_TOKEN.value in conn._attrs_before
        conn.close()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_keyword_only_scope_rejected(self, mock_ddbc_conn):
        """get_token(self, *, scope) can't take scope positionally and is rejected."""
        mock_ddbc_conn.return_value = MagicMock()
        from mssql_python import connect

        class KeywordOnlyCredential:
            def get_token(self, *, scope):
                return MagicMock(token=SAMPLE_TOKEN)

        # No up-front signature inspection: the call-time validation raises.
        with pytest.raises(InterfaceError, match="must accept a scope"):
            connect("Server=test;Database=testdb", token_provider=KeywordOnlyCredential())
        mock_ddbc_conn.assert_not_called()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_caller_attrs_before_dict_not_mutated(self, mock_ddbc_conn):
        """connect() must not inject the access token into the caller's own
        attrs_before dict (it would leak the secret and break dict reuse)."""
        mock_ddbc_conn.return_value = MagicMock()
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MagicMock(token=SAMPLE_TOKEN)
        from mssql_python import connect

        login_timeout_attr = 113  # SQL_ATTR_LOGIN_TIMEOUT
        caller_opts = {login_timeout_attr: 30}
        conn = connect(
            "Server=test;Database=testdb",
            token_provider=mock_cred,
            attrs_before=caller_opts,
        )
        # The caller's dict is untouched: no access token leaked in.
        assert caller_opts == {login_timeout_attr: 30}
        assert ConstantsDDBC.SQL_COPT_SS_ACCESS_TOKEN.value not in caller_opts
        # The token is stored in the connection's own attrs_before copy, which
        # is independent of the caller's dict.
        assert ConstantsDDBC.SQL_COPT_SS_ACCESS_TOKEN.value in conn._attrs_before
        assert caller_opts == {login_timeout_attr: 30}
        conn.close()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_reusing_attrs_before_across_connections_succeeds(self, mock_ddbc_conn):
        """The same attrs_before dict can be reused for a second connection with
        a different provider — proves the dict isn't polluted by the first."""
        mock_ddbc_conn.return_value = MagicMock()
        cred_a = MagicMock()
        cred_a.get_token.return_value = MagicMock(token=SAMPLE_TOKEN)
        cred_b = MagicMock()
        cred_b.get_token.return_value = MagicMock(token=SAMPLE_TOKEN)
        from mssql_python import connect

        shared_opts = {113: 30}  # SQL_ATTR_LOGIN_TIMEOUT
        c1 = connect("Server=s;Database=d", token_provider=cred_a, attrs_before=shared_opts)
        # Without the copy fix this raises "Cannot specify both ... access token".
        c2 = connect("Server=s;Database=d", token_provider=cred_b, attrs_before=shared_opts)
        # Each connection has its own attrs_before copy and its own factory, and
        # the shared caller dict was never polluted with a token.
        assert c1._attrs_before is not c2._attrs_before
        assert c1._token_factory is None
        assert c2._token_factory is None
        assert ConstantsDDBC.SQL_COPT_SS_ACCESS_TOKEN.value in c1._attrs_before
        assert ConstantsDDBC.SQL_COPT_SS_ACCESS_TOKEN.value in c2._attrs_before
        assert ConstantsDDBC.SQL_COPT_SS_ACCESS_TOKEN.value not in shared_opts
        c1.close()
        c2.close()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_expired_expires_on_warns_but_is_accepted(self, mock_ddbc_conn):
        """An already-expired expires_on is still accepted (the server enforces
        expiry), but a warning is logged (not raised as a Python warning, so it
        is never promoted to an exception under ``-W error``)."""
        mock_ddbc_conn.return_value = MagicMock()
        past = 1  # 1970 — long expired
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MagicMock(token=SAMPLE_TOKEN, expires_on=past)
        from mssql_python import connect
        import mssql_python.auth as auth_mod

        with patch.object(auth_mod.logger, "warning") as mock_warning:
            conn = connect("Server=test;Database=testdb", token_provider=mock_cred)
        assert any("already expired" in str(call.args[0]) for call in mock_warning.call_args_list)
        assert conn._token_expires_on == past
        # Token stored eagerly in attrs_before.
        assert ConstantsDDBC.SQL_COPT_SS_ACCESS_TOKEN.value in conn._attrs_before
        conn.close()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_token_value_not_in_exception_message(self, mock_ddbc_conn):
        """A provider failure must not leak the acquired token in the error."""
        mock_ddbc_conn.return_value = MagicMock()
        mock_cred = MagicMock()
        mock_cred.get_token.side_effect = Exception("auth failed")
        from mssql_python import connect

        with pytest.raises(OperationalError) as exc_info:
            connect("Server=test;Database=testdb", token_provider=mock_cred)
        assert SAMPLE_TOKEN not in str(exc_info.value)

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_token_value_not_in_logs(self, mock_ddbc_conn, caplog):
        """The raw JWT must never be written to logs (only its length)."""
        import logging

        mock_ddbc_conn.return_value = MagicMock()
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MagicMock(token=SAMPLE_TOKEN)
        from mssql_python import connect

        with caplog.at_level(logging.DEBUG):
            conn = connect("Server=test;Database=testdb", token_provider=mock_cred)
        assert SAMPLE_TOKEN not in caplog.text
        conn.close()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_empty_connection_string_with_token_provider(self, mock_ddbc_conn):
        """An empty connection string with token_provider should not crash the
        validation path; the credential is still validated and the token stored."""
        mock_ddbc_conn.return_value = MagicMock()
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MagicMock(token=SAMPLE_TOKEN)
        from mssql_python import connect

        conn = connect("", token_provider=mock_cred)
        assert ConstantsDDBC.SQL_COPT_SS_ACCESS_TOKEN.value in conn._attrs_before
        conn.close()


class TestTokenProviderProtocol:
    """Tests for the runtime_checkable azure.core TokenCredential protocol."""

    def test_object_with_get_token_is_instance(self):
        """An object exposing get_token satisfies the Protocol at runtime."""

        class Cred:
            def get_token(self, *scopes, **kwargs):
                return MagicMock(token=SAMPLE_TOKEN)

        assert isinstance(Cred(), TokenCredential)

    def test_object_without_get_token_is_not_instance(self):
        """An object missing get_token does not satisfy the Protocol."""

        class NotCred:
            def something_else(self):
                return None

        assert not isinstance(NotCred(), TokenCredential)

    def test_database_scope_is_commercial_cloud_constant(self):
        """The shared scope constant points at the Azure commercial-cloud audience."""
        assert _SQL_SCOPE == "https://database.windows.net/.default"


class TestTokenProviderPooling:
    """Pins pooling behavior for access-token connections.

    Access-token connections are pooled with an identity-aware pool key: the
    sanitized connection string plus a per-identity suffix (``msi:``/``acct:``/
    ``tok:``). Two different principals that share the same Server/Database land
    in distinct pool buckets and are never handed each other's authenticated
    connection, while same-identity reuse still benefits from pooling. These
    tests pin that contract for every access-token path (raw
    SQL_COPT_SS_ACCESS_TOKEN, built-in Authentication=ActiveDirectory*, and
    token_provider=).
    """

    @staticmethod
    def _pooling_arg(mock_ddbc_conn):
        """Return the `pooling` positional arg passed to ddbc_bindings.Connection."""
        # ddbc_bindings.Connection(connection_str, pooling, attrs_before)
        return mock_ddbc_conn.call_args.args[1]

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_token_provider_allows_pooling_with_identity_key(self, mock_ddbc_conn):
        """token_provider= connections are pooled, keyed on the token hash
        (``tok:``) so distinct tokens never share a pooled connection. The token
        is stored eagerly in attrs_before with no deferred factory."""
        mock_ddbc_conn.return_value = MagicMock()
        from mssql_python import connect
        from mssql_python.pooling import PoolingManager

        PoolingManager._reset_for_testing()
        cred = MagicMock()
        cred.get_token.return_value = MagicMock(token=SAMPLE_TOKEN, expires_on=None)

        conn = connect("Server=s;Database=d", token_provider=cred)
        assert self._pooling_arg(mock_ddbc_conn) is True
        assert conn._pooling is True
        assert conn._pool_key.startswith(conn.connection_str + "\x00tok:")
        assert conn._token_factory is None
        assert ConstantsDDBC.SQL_COPT_SS_ACCESS_TOKEN.value in conn._attrs_before
        conn.close()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_raw_access_token_in_attrs_before_allows_pooling_with_identity_key(
        self, mock_ddbc_conn
    ):
        """A raw SQL_COPT_SS_ACCESS_TOKEN supplied directly in attrs_before (the
        pyodbc-style path) is pooled with a token-hash identity key."""
        mock_ddbc_conn.return_value = MagicMock()
        from mssql_python import connect
        from mssql_python.constants import ConstantsDDBC
        from mssql_python.pooling import PoolingManager

        PoolingManager._reset_for_testing()
        token_struct = b"\x04\x00\x00\x00test"
        conn = connect(
            "Server=s;Database=d",
            attrs_before={ConstantsDDBC.SQL_COPT_SS_ACCESS_TOKEN.value: token_struct},
        )
        assert self._pooling_arg(mock_ddbc_conn) is True
        assert conn._pooling is True
        assert conn._pool_key.startswith(conn.connection_str + "\x00tok:")
        conn.close()

    @patch("mssql_python.connection.get_auth_token_info")
    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_builtin_entra_auth_allows_pooling_with_identity_key(
        self, mock_ddbc_conn, mock_get_token_info
    ):
        """Built-in Authentication=ActiveDirectory* auth that injects a token into
        attrs_before (e.g. ActiveDirectoryDefault) is pooled with a token-hash
        identity key. (Driver-native paths such as ServicePrincipal keep
        credentials in the connection string and remain poolable; see
        test_builtin_driver_native_auth_keeps_pooling.)"""
        mock_ddbc_conn.return_value = MagicMock()
        mock_get_token_info.return_value = TokenInfo(
            token_struct=b"\x04\x00\x00\x00test", expires_on=None
        )
        from mssql_python import connect
        from mssql_python.pooling import PoolingManager

        PoolingManager._reset_for_testing()
        conn = connect("Server=s;Database=d;Authentication=ActiveDirectoryDefault")
        assert self._pooling_arg(mock_ddbc_conn) is True
        assert conn._pooling is True
        assert conn._pool_key.startswith(conn.connection_str + "\x00tok:")
        conn.close()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_builtin_driver_native_auth_keeps_pooling(self, mock_ddbc_conn):
        """Driver-native Entra auth (ServicePrincipal) keeps UID/PWD in the
        connection string, so the pool key already distinguishes principals and
        pooling stays enabled — no token is injected into attrs_before."""
        mock_ddbc_conn.return_value = MagicMock()
        from mssql_python import connect
        from mssql_python.constants import ConstantsDDBC
        from mssql_python.pooling import PoolingManager

        PoolingManager._reset_for_testing()
        conn = connect(
            "Server=s;Database=d;Authentication=ActiveDirectoryServicePrincipal;"
            "UID=app-id;PWD=app-secret"
        )
        # No access token was injected, so pooling is left enabled.
        assert ConstantsDDBC.SQL_COPT_SS_ACCESS_TOKEN.value not in conn._attrs_before
        assert self._pooling_arg(mock_ddbc_conn) is True
        assert conn._pooling is True
        conn.close()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_non_token_connection_keeps_pooling_enabled(self, mock_ddbc_conn):
        """A plain connection (no access token) is still eligible for pooling —
        the fix must not regress normal SQL/Windows-auth pooling."""
        mock_ddbc_conn.return_value = MagicMock()
        from mssql_python import connect
        from mssql_python.pooling import PoolingManager

        PoolingManager._reset_for_testing()
        conn = connect("Server=s;Database=d;UID=sa;PWD=secret")
        assert self._pooling_arg(mock_ddbc_conn) is True
        assert conn._pooling is True
        conn.close()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_same_token_from_different_providers_shares_pool_key(self, mock_ddbc_conn):
        """Two DIFFERENT provider objects that mint the SAME token resolve to the
        SAME token-hash pool key. This is the documented weaker-but-safe reuse:
        the pool is keyed on the token, not the provider object, so a mutable
        credential can never hand a caller a connection authenticated as a stale
        principal (a different principal always mints a different token)."""
        mock_ddbc_conn.return_value = MagicMock()
        from mssql_python import connect
        from mssql_python.pooling import PoolingManager

        PoolingManager._reset_for_testing()
        cred_a = MagicMock()
        cred_a.get_token.return_value = MagicMock(token=SAMPLE_TOKEN, expires_on=None)
        cred_b = MagicMock()
        cred_b.get_token.return_value = MagicMock(token=SAMPLE_TOKEN, expires_on=None)

        c1 = connect("Server=s;Database=d", token_provider=cred_a)
        c2 = connect("Server=s;Database=d", token_provider=cred_b)
        assert c1.connection_str == c2.connection_str
        # Same token => same tok: identity => same pool bucket.
        assert c1._pool_key == c2._pool_key
        assert c1._pool_key.startswith(c1.connection_str + "\x00tok:")
        c1.close()
        c2.close()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_different_tokens_from_providers_get_distinct_pool_keys(self, mock_ddbc_conn):
        """Two providers that mint DIFFERENT tokens get DISTINCT pool keys, so
        distinct principals are never handed each other's pooled connection."""
        mock_ddbc_conn.return_value = MagicMock()
        from mssql_python import connect
        from mssql_python.pooling import PoolingManager

        PoolingManager._reset_for_testing()
        cred_a = MagicMock()
        cred_a.get_token.return_value = MagicMock(token=SAMPLE_TOKEN, expires_on=None)
        cred_b = MagicMock()
        cred_b.get_token.return_value = MagicMock(token=SAMPLE_TOKEN + "-B", expires_on=None)

        c1 = connect("Server=s;Database=d", token_provider=cred_a)
        c2 = connect("Server=s;Database=d", token_provider=cred_b)
        assert c1.connection_str == c2.connection_str
        assert c1._pool_key != c2._pool_key
        assert c1._pool_key.startswith(c1.connection_str + "\x00tok:")
        assert c2._pool_key.startswith(c2.connection_str + "\x00tok:")
        c1.close()
        c2.close()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_token_not_refreshed_after_connect(self, mock_ddbc_conn):
        """The credential is acquired exactly once up front at connect(),
        regardless of the token's expiry. The token is a pre-connect ODBC
        attribute and is never re-acquired on this connection."""
        mock_ddbc_conn.return_value = MagicMock()
        mock_cred = MagicMock()
        mock_cred.get_token.return_value = MagicMock(token=SAMPLE_TOKEN, expires_on=1)
        from mssql_python import connect

        # expires_on=1 is in the past (the expired-token diagnostic is logged);
        # the point of this test is that the token is acquired exactly once.
        conn = connect("Server=s;Database=d", token_provider=mock_cred)
        # Even though expires_on is in the past, nothing re-acquires the token
        # (the mocked native never invokes the factory).
        assert mock_cred.get_token.call_count == 1
        conn.close()
        assert mock_cred.get_token.call_count == 1


# --- Faithful azure-identity stand-ins -------------------------------------
# These mirror the real azure.core.credentials API so the token_provider path
# is exercised exactly as it would be with a live `azure-identity` install,
# without taking a dependency on the package or making network calls.

# azure.core.credentials.AccessToken is a NamedTuple(token: str, expires_on: int).
_AccessToken = collections.namedtuple("AccessToken", ["token", "expires_on"])


class _FakeDefaultAzureCredential:
    """Mirrors azure.identity.DefaultAzureCredential.

    Real signature:
        get_token(self, *scopes, claims=None, tenant_id=None,
                  enable_cae=False, **kwargs) -> AccessToken
    The SDK caches internally and hands back the same AccessToken until it is
    near expiry, so repeated calls are cheap and return a stable token.
    """

    def __init__(self, token=SAMPLE_TOKEN, expires_on=1893456000):
        self._cached = _AccessToken(token, expires_on)
        self.calls = []

    def get_token(self, *scopes, claims=None, tenant_id=None, enable_cae=False, **kwargs):
        self.calls.append(scopes)
        return self._cached


class _FakeClientSecretCredential:
    """Mirrors azure.identity.ClientSecretCredential (service principal)."""

    def __init__(self, tenant_id, client_id, client_secret, token=SAMPLE_TOKEN):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self._secret = client_secret
        self._token = token
        self.calls = 0

    def get_token(self, *scopes, **kwargs):
        self.calls += 1
        return _AccessToken(self._token, 1893456000)


class _FakeManagedIdentityCredential:
    """Mirrors azure.identity.ManagedIdentityCredential (App Service / VM)."""

    def __init__(self, client_id=None, token=SAMPLE_TOKEN):
        self.client_id = client_id
        self._token = token

    def get_token(self, *scopes, **kwargs):
        return _AccessToken(self._token, 1893456000)


class _FakeInteractiveBrowserCredential:
    """Mirrors azure.identity.InteractiveBrowserCredential.

    The first call performs an interactive sign-in (slow / may block); after
    that the token is cached. We model that the first get_token is the one that
    "logs in" and subsequent calls return the cached value.
    """

    def __init__(self, token=SAMPLE_TOKEN):
        self._token = token
        self.login_count = 0

    def get_token(self, *scopes, claims=None, tenant_id=None, enable_cae=False, **kwargs):
        if self.login_count == 0:
            self.login_count += 1  # "interactive sign-in" happens here
        return _AccessToken(self._token, 1893456000)


class TestTokenProviderRealWorld:
    """End-to-end checks against faithful azure-identity credential stand-ins.

    Validates that the token_provider= fix behaves correctly with the real
    Azure SDK API shapes (AccessToken namedtuple, *scopes/**kwargs signatures)
    and the real usage patterns library consumers actually write.
    """

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_default_azure_credential_end_to_end(self, mock_ddbc_conn):
        """The canonical `connect(conn_str, token_provider=DefaultAzureCredential())`."""
        mock_ddbc_conn.return_value = MagicMock()
        cred = _FakeDefaultAzureCredential()
        from mssql_python import connect

        conn = connect("Server=myserver.database.windows.net;Database=mydb", token_provider=cred)
        # Token acquired with the commercial-cloud database scope, once.
        assert cred.calls == [(_SQL_SCOPE,)]
        assert conn._token_provider is cred
        assert conn._token_expires_on == 1893456000
        # Token is stored eagerly in attrs_before (no deferred factory).
        assert ConstantsDDBC.SQL_COPT_SS_ACCESS_TOKEN.value in conn._attrs_before
        conn.close()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_client_secret_credential_service_principal(self, mock_ddbc_conn):
        """Service-principal pattern: ClientSecretCredential(tenant, id, secret)."""
        mock_ddbc_conn.return_value = MagicMock()
        cred = _FakeClientSecretCredential(
            tenant_id="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            client_id="11111111-2222-3333-4444-555555555555",
            client_secret="super-secret",
        )
        from mssql_python import connect

        conn = connect("Server=s.database.windows.net;Database=d", token_provider=cred)
        assert cred.calls == 1
        assert conn._token_provider is cred
        # The client secret must never end up in the (sanitized) connection string.
        assert "super-secret" not in conn.connection_str
        conn.close()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_managed_identity_credential_app_service(self, mock_ddbc_conn):
        """App Service / VM pattern: ManagedIdentityCredential(client_id=...)."""
        mock_ddbc_conn.return_value = MagicMock()
        cred = _FakeManagedIdentityCredential(client_id="user-assigned-mi-client-id")
        from mssql_python import connect

        conn = connect("Server=s.database.windows.net;Database=d", token_provider=cred)
        assert conn._token_provider is cred
        assert ConstantsDDBC.SQL_COPT_SS_ACCESS_TOKEN.value in conn._attrs_before
        conn.close()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_interactive_browser_credential_signs_in_once(self, mock_ddbc_conn):
        """Interactive credential: first connect triggers the single sign-in."""
        mock_ddbc_conn.return_value = MagicMock()
        cred = _FakeInteractiveBrowserCredential()
        from mssql_python import connect

        conn = connect("Server=s.database.windows.net;Database=d", token_provider=cred)
        assert cred.login_count == 1
        conn.close()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_access_token_namedtuple_round_trips(self, mock_ddbc_conn):
        """A real AccessToken namedtuple flows through .token / .expires_on access."""
        mock_ddbc_conn.return_value = MagicMock()
        cred = _FakeDefaultAzureCredential(expires_on=1999999999)
        from mssql_python import connect

        conn = connect("Server=s;Database=d", token_provider=cred)
        assert conn._token_expires_on == 1999999999
        # The token is stored as a UTF-16-LE struct, not the raw JWT.
        token_struct = conn._attrs_before[ConstantsDDBC.SQL_COPT_SS_ACCESS_TOKEN.value]
        body = SAMPLE_TOKEN.encode("UTF-16-LE")
        assert token_struct[:4] == len(body).to_bytes(4, "little")
        assert token_struct[4:] == body
        conn.close()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_one_credential_reused_across_a_connection_pool(self, mock_ddbc_conn):
        """The real pattern: build the credential once, reuse for every connect.

        Each connect() acquires a fresh token from the (internally-cached)
        credential, and connections never share an attrs_before dict.
        """
        mock_ddbc_conn.return_value = MagicMock()
        cred = _FakeDefaultAzureCredential()
        from mssql_python import connect

        conns = [connect(f"Server=s{i};Database=d", token_provider=cred) for i in range(5)]
        assert len(cred.calls) == 5
        # No two connections alias the same attrs_before dict (regression guard
        # for the caller-dict-mutation bug).
        ids = {id(c._attrs_before) for c in conns}
        assert len(ids) == 5
        for c in conns:
            c.close()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_shared_app_config_dict_reused_for_every_connection(self, mock_ddbc_conn):
        """Real-world bug-fix scenario: an app holds ONE options dict (e.g. a
        login timeout) and passes it to every connect() alongside a credential.

        Before the fix the first connect() injected the access token into this
        shared dict, so the second connect() raised "Cannot specify both ...".
        """
        mock_ddbc_conn.return_value = MagicMock()
        cred = _FakeDefaultAzureCredential()
        from mssql_python import connect

        SQL_ATTR_LOGIN_TIMEOUT = 113
        app_attrs = {SQL_ATTR_LOGIN_TIMEOUT: 30}  # built once, reused everywhere

        c1 = connect("Server=s1;Database=d", token_provider=cred, attrs_before=app_attrs)
        c2 = connect("Server=s2;Database=d", token_provider=cred, attrs_before=app_attrs)

        # The shared dict is untouched: only the login timeout, no access token.
        assert app_attrs == {SQL_ATTR_LOGIN_TIMEOUT: 30}
        assert ConstantsDDBC.SQL_COPT_SS_ACCESS_TOKEN.value not in app_attrs
        # Each connection keeps the app's login timeout and stores its own token
        # in its private attrs_before copy (the shared dict is never polluted).
        for c in (c1, c2):
            assert c._attrs_before[SQL_ATTR_LOGIN_TIMEOUT] == 30
            assert ConstantsDDBC.SQL_COPT_SS_ACCESS_TOKEN.value in c._attrs_before
        c1.close()
        c2.close()


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


class TestTokenExpiryCapture:
    """Foundation for identity-aware pooling: the acquisition
    path must surface the token expiry alongside the ODBC struct."""

    def test_get_token_info_returns_tokeninfo(self):
        info = AADAuth.get_token_info("default")
        assert isinstance(info, TokenInfo)
        assert isinstance(info.token_struct, bytes)
        assert len(info.token_struct) > 4

    def test_get_token_info_expiry_none_when_absent(self):
        # The default test double's token object has no expires_on attribute,
        # so capture must fall back to None rather than raising.
        info = AADAuth.get_token_info("default")
        assert info.expires_on is None

    def test_get_token_info_captures_expiry_when_present(self):
        class MockTokenWithExpiry:
            token = SAMPLE_TOKEN
            expires_on = 1_900_000_000

        class MockCredWithExpiry:
            def get_token(self, scope):
                return MockTokenWithExpiry()

        import sys

        azure_identity = sys.modules["azure.identity"]
        original = azure_identity.DefaultAzureCredential
        azure_identity.DefaultAzureCredential = MockCredWithExpiry
        try:
            info = AADAuth.get_token_info("default")
            assert info.expires_on == 1_900_000_000
        finally:
            azure_identity.DefaultAzureCredential = original

    def test_get_auth_token_info_returns_none_without_auth_type(self):
        assert get_auth_token_info(None) is None
        assert get_auth_token_info("") is None

    def test_get_auth_token_info_default(self):
        info = get_auth_token_info("default")
        assert isinstance(info, TokenInfo)
        assert isinstance(info.token_struct, bytes)

    def test_get_auth_token_info_windows_interactive_returns_none(self):
        # Windows Interactive auth is handled natively by the driver, so the
        # Python layer must not acquire a token (auth.py line 478).
        with patch("mssql_python.auth.platform.system", return_value="Windows"):
            assert get_auth_token_info("interactive") is None

    def test_get_auth_token_info_raises_on_runtime_error(self):
        # A token-acquisition RuntimeError must propagate (fail closed) so the
        # caller never falls through to a token-less connect.
        with patch(
            "mssql_python.auth.AADAuth.get_token_info",
            side_effect=RuntimeError("token endpoint unreachable"),
        ):
            with pytest.raises(RuntimeError, match="token endpoint unreachable"):
                get_auth_token_info("default")

    def test_get_auth_token_info_raises_on_value_error(self):
        # Same fail-closed contract for ValueError (unsupported auth type, etc.).
        with patch(
            "mssql_python.auth.AADAuth.get_token_info",
            side_effect=ValueError("invalid credential kwargs"),
        ):
            with pytest.raises(ValueError, match="invalid credential kwargs"):
                get_auth_token_info("default")


class TestDefaultCredentialPoolingWarning:
    """The one-time DefaultAzureCredential pooling warning uses double-checked
    locking so concurrent callers emit it at most once."""

    def test_inner_double_check_returns_without_warning(self, monkeypatch):
        # Simulate a race: the outer check sees the flag unset, but another
        # thread sets it while we wait on the lock, so the inner check returns
        # (auth.py line 96) and no warning is emitted by this caller.
        import mssql_python.auth as auth_mod

        monkeypatch.setattr(auth_mod, "_dac_pooling_warned", False, raising=False)
        mock_logger = MagicMock()
        monkeypatch.setattr(auth_mod, "logger", mock_logger)

        class RacingLock:
            def __enter__(self):
                auth_mod._dac_pooling_warned = True
                return self

            def __exit__(self, *exc):
                return False

        monkeypatch.setattr(auth_mod, "_dac_pooling_warned_lock", RacingLock())

        auth_mod._warn_default_credential_pooling()

        mock_logger.warning.assert_not_called()

    def test_warning_emitted_once_when_not_yet_warned(self, monkeypatch):
        # Baseline: with the flag unset and a normal lock, the warning fires.
        import threading
        import mssql_python.auth as auth_mod

        monkeypatch.setattr(auth_mod, "_dac_pooling_warned", False, raising=False)
        monkeypatch.setattr(auth_mod, "_dac_pooling_warned_lock", threading.Lock())
        mock_logger = MagicMock()
        monkeypatch.setattr(auth_mod, "logger", mock_logger)

        auth_mod._warn_default_credential_pooling()
        auth_mod._warn_default_credential_pooling()

        assert mock_logger.warning.call_count == 1


class TestPublicApiReexports:
    """The public token_provider typing surface must be importable from the
    package root so callers can annotate against it."""

    def test_token_provider_is_reexported(self):
        import mssql_python
        from mssql_python import TokenProvider

        assert TokenProvider is mssql_python.connection.TokenProvider
        assert "TokenProvider" in mssql_python.__all__


class TestComputeIdentityKey:
    """The per-identity discriminator that makes the native pool key
    identity-aware."""

    def test_none_when_no_auth_type(self):
        # No token auth => pool key stays the bare connection string.
        assert compute_identity_key(None) is None
        assert compute_identity_key("") is None

    def test_msi_user_assigned_uses_client_id(self):
        key = compute_identity_key("msi", {"client_id": "abc-123"})
        assert key == "msi:client:abc-123"

    def test_msi_system_assigned(self):
        assert compute_identity_key("msi") == "msi:system"
        assert compute_identity_key("msi", {}) == "msi:system"

    def test_msi_key_derived_without_token(self):
        # No token_struct supplied, yet MSI still yields a key -> enables
        # skipping token acquisition on a pool hit.
        assert compute_identity_key("msi", {"client_id": "cid"}) == "msi:client:cid"

    def test_msi_user_and_system_encodings_are_disjoint(self):
        # A user-assigned MSI whose client_id is literally "system" must NOT
        # collide with a system-assigned MSI: the client id lives under the
        # ``msi:client:`` prefix so the two encodings can never overlap.
        user = compute_identity_key("msi", {"client_id": "system"})
        system = compute_identity_key("msi")
        assert user == "msi:client:system"
        assert system == "msi:system"
        assert user != system

    def test_msi_client_id_is_normalized(self):
        # The same managed identity written with different GUID casing or with
        # optional surrounding {braces} must map to a single pool bucket.
        canonical = "abcd1234-5678-90ab-cdef-1234567890ab"
        assert (
            compute_identity_key("msi", {"client_id": canonical.upper()})
            == f"msi:client:{canonical}"
        )
        assert (
            compute_identity_key("msi", {"client_id": "{" + canonical.upper() + "}"})
            == f"msi:client:{canonical}"
        )
        assert (
            compute_identity_key("msi", {"client_id": "  " + canonical + "  "})
            == f"msi:client:{canonical}"
        )

    def test_token_hash_fallback_for_default(self):
        token = b"\x00\x01sometokenbytes"
        key = compute_identity_key("default", token_struct=token)
        assert key is not None and key.startswith("tok:")
        assert len(key) == len("tok:") + 64  # sha256 hex

    def test_different_tokens_produce_different_keys(self):
        k1 = compute_identity_key("default", token_struct=b"token-a")
        k2 = compute_identity_key("default", token_struct=b"token-b")
        assert k1 != k2

    def test_same_token_produces_stable_key(self):
        k1 = compute_identity_key("default", token_struct=b"same")
        k2 = compute_identity_key("default", token_struct=b"same")
        assert k1 == k2

    def test_none_when_token_required_but_absent(self):
        # default/interactive/devicecode need the token to form the key;
        # without it, signal the caller to acquire then recompute.
        assert compute_identity_key("default") is None
        assert compute_identity_key("interactive") is None
        assert compute_identity_key("devicecode") is None

    def test_interactive_uses_home_account_id(self):
        # A stable signed-in account id keys the pool on the account, so a
        # silent refresh reuses it while a different account gets its own pool.
        assert compute_identity_key("interactive", home_account_id="acct-xyz") == "acct:acct-xyz"

    def test_devicecode_uses_home_account_id(self):
        assert compute_identity_key("devicecode", home_account_id="dev-acct") == "acct:dev-acct"

    def test_account_id_preferred_over_token_hash(self):
        # When both are available the account id wins (stable across refreshes).
        key = compute_identity_key("interactive", token_struct=b"tok", home_account_id="acct-1")
        assert key == "acct:acct-1"

    def test_compute_token_identity_hashes_token(self):
        # compute_token_identity is the raw-token helper: it derives a stable
        # ``tok:`` identity straight from the token struct, independent of any
        # auth_type, and matches the plain "default" token hash.
        from mssql_python.auth import compute_token_identity

        token = b"\x04\x00\x00\x00rawtoken"
        identity = compute_token_identity(token)
        assert identity is not None and identity.startswith("tok:")
        assert identity == compute_identity_key("default", token_struct=token)

    def test_compute_token_identity_without_token_is_none(self):
        # compute_token_identity needs a token struct; a falsy value yields None
        # so the caller does not build a bare-connection-string key for a token
        # connection.
        from mssql_python.auth import compute_token_identity

        assert compute_token_identity(None) is None
        assert compute_token_identity(b"") is None


class TestConnectionPoolKey:
    """Part B: Connection.__init__ must build an identity-aware pool key and
    forward it to the native Connection so distinct Entra identities never
    share a pooled connection, while non-token auth keeps the legacy
    bare-connection-string key (backward compatible)."""

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_no_pool_key_for_non_token_auth(self, mock_ddbc_conn):
        # Plain SQL auth (no Authentication=) => identity key None => pool key
        # stays empty so the native layer keys on the connection string.
        mock_ddbc_conn.return_value = MagicMock()
        from mssql_python import connect

        conn = connect("Server=test;Database=testdb;UID=sa;PWD=secret")
        assert conn._pool_key == ""
        # Native Connection received the empty pool key as the 4th positional arg.
        args, _ = mock_ddbc_conn.call_args
        assert args[3] == ""
        conn.close()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_system_assigned_msi_pool_key(self, mock_ddbc_conn):
        mock_ddbc_conn.return_value = MagicMock()
        from mssql_python import connect

        conn = connect("Server=test;Database=testdb;Authentication=ActiveDirectoryMSI")
        assert conn._pool_key == conn.connection_str + "\x00msi:system"
        args, _ = mock_ddbc_conn.call_args
        assert args[3] == conn._pool_key
        conn.close()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_user_assigned_msi_pool_key_uses_client_id(self, mock_ddbc_conn):
        mock_ddbc_conn.return_value = MagicMock()
        from mssql_python import connect

        client_id = "11111111-2222-3333-4444-555555555555"
        conn = connect(
            f"Server=test;Database=testdb;Authentication=ActiveDirectoryMSI;UID={client_id}"
        )
        assert conn._pool_key == conn.connection_str + f"\x00msi:client:{client_id}"
        conn.close()

    @patch("mssql_python.connection.get_auth_token_info")
    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_default_auth_pool_key_hashes_token(self, mock_ddbc_conn, mock_get_token_info):
        # When a token is present, the pool key must include its hash so that
        # two different tokens (identities) never collide on the same pool.
        mock_ddbc_conn.return_value = MagicMock()
        mock_get_token_info.return_value = TokenInfo(
            token_struct=b"\x00\x01fake-token-bytes", expires_on=None
        )
        from mssql_python import connect

        conn = connect("Server=test;Database=testdb;Authentication=ActiveDirectoryDefault")
        assert conn._pool_key.startswith(conn.connection_str + "\x00tok:")
        args, _ = mock_ddbc_conn.call_args
        assert args[3] == conn._pool_key
        conn.close()

    @patch("mssql_python.connection.get_auth_token_info")
    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_different_tokens_yield_different_pool_keys(self, mock_ddbc_conn, mock_get_token_info):
        mock_ddbc_conn.return_value = MagicMock()
        from mssql_python import connect

        cs = "Server=test;Database=testdb;Authentication=ActiveDirectoryDefault"

        mock_get_token_info.return_value = TokenInfo(
            token_struct=b"identity-A-token", expires_on=None
        )
        conn_a = connect(cs)
        key_a = conn_a._pool_key
        conn_a.close()

        mock_get_token_info.return_value = TokenInfo(
            token_struct=b"identity-B-token", expires_on=None
        )
        conn_b = connect(cs)
        key_b = conn_b._pool_key
        conn_b.close()

        # Same connection string, different identity => different pool key.
        assert key_a != key_b
        assert conn_a.connection_str == conn_b.connection_str

    @patch("mssql_python.connection.get_auth_token_info")
    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_default_auth_no_token_fails_closed(self, mock_ddbc_conn, mock_get_token_info):
        # Token acquisition yielded nothing usable: there is no identity to key
        # on and no token to attach, so connect() must fail closed rather than
        # open a token-less connection on the bare connection-string key.
        mock_ddbc_conn.return_value = MagicMock()
        mock_get_token_info.return_value = None
        from mssql_python import connect

        with pytest.raises(InterfaceError):
            connect("Server=test;Database=testdb;Authentication=ActiveDirectoryDefault")


class TestTokenFactoryLazyAcquisition:
    """Part C: when the pool key is known *without* a token (MSI, whose
    identity is the client id), token acquisition is deferred to a callback the
    native layer invokes only when it opens a physical connection. A same-
    identity pool hit therefore never pays for a token. This is an internal
    ``token_factory`` (5th positional arg to native Connection) and is distinct
    from the public ``token_provider=`` credential API."""

    _TOKEN_ATTR = ConstantsDDBC.SQL_COPT_SS_ACCESS_TOKEN.value

    @patch("mssql_python.connection.get_auth_token_info")
    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_msi_defers_token_acquisition(self, mock_ddbc_conn, mock_get_token_info):
        # MSI identity is known from the client id, so the token must NOT be
        # acquired during __init__ — it is deferred to the token_factory.
        mock_ddbc_conn.return_value = MagicMock()
        from mssql_python import connect

        conn = connect("Server=test;Database=testdb;Authentication=ActiveDirectoryMSI")
        try:
            mock_get_token_info.assert_not_called()
            # token_factory is passed as the 5th positional arg and is callable.
            args, _ = mock_ddbc_conn.call_args
            assert callable(args[4])
            # The token is not eagerly placed into attrs_before either.
            assert self._TOKEN_ATTR not in conn._attrs_before
        finally:
            conn.close()

    @patch("mssql_python.connection.get_auth_token_info")
    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_token_factory_materializes_token_on_call(self, mock_ddbc_conn, mock_get_token_info):
        # Invoking the factory (as native does on a pool miss) acquires the
        # token and returns ``(attrs, expires_on)`` with the access token merged
        # into attrs.
        mock_ddbc_conn.return_value = MagicMock()
        mock_get_token_info.return_value = TokenInfo(
            token_struct=b"materialized-token-bytes", expires_on=1893456000
        )
        from mssql_python import connect

        conn = connect("Server=test;Database=testdb;Authentication=ActiveDirectoryMSI")
        try:
            attrs, expires_on = conn._token_factory()
            mock_get_token_info.assert_called_once()
            assert attrs[self._TOKEN_ATTR] == b"materialized-token-bytes"
            assert expires_on == 1893456000
        finally:
            conn.close()

    @patch("mssql_python.connection.get_auth_token_info")
    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_token_dependent_auth_has_no_factory(self, mock_ddbc_conn, mock_get_token_info):
        # DefaultAzureCredential keys on the token hash, so the token must be
        # acquired eagerly (to compute the key) and there is no deferred factory.
        mock_ddbc_conn.return_value = MagicMock()
        mock_get_token_info.return_value = TokenInfo(
            token_struct=b"eager-token-bytes", expires_on=None
        )
        from mssql_python import connect

        conn = connect("Server=test;Database=testdb;Authentication=ActiveDirectoryDefault")
        try:
            mock_get_token_info.assert_called_once()
            assert conn._token_factory is None
            args, _ = mock_ddbc_conn.call_args
            assert args[4] is None
            assert conn._attrs_before[self._TOKEN_ATTR] == b"eager-token-bytes"
        finally:
            conn.close()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_non_token_auth_has_no_factory(self, mock_ddbc_conn):
        # Plain SQL auth: no identity, no deferred token.
        mock_ddbc_conn.return_value = MagicMock()
        from mssql_python import connect

        conn = connect("Server=test;Database=testdb;UID=sa;PWD=secret")
        try:
            assert conn._token_factory is None
            args, _ = mock_ddbc_conn.call_args
            assert args[4] is None
        finally:
            conn.close()

    @patch("mssql_python.connection.get_auth_token_info")
    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_account_keyed_auth_defers_to_factory(self, mock_ddbc_conn, mock_get_token_info):
        # Device-code yields a stable home_account_id, so the pool key is
        # ``acct:<id>`` and token acquisition is deferred to the factory rather
        # than pinned into attrs_before.
        mock_ddbc_conn.return_value = MagicMock()
        mock_get_token_info.return_value = TokenInfo(
            token_struct=b"dc-token-bytes",
            expires_on=1893456000,
            home_account_id="dc-acct-1",
        )
        from mssql_python import connect

        conn = connect("Server=test;Database=testdb;Authentication=ActiveDirectoryDeviceCode")
        try:
            assert conn._pool_key.endswith("\x00acct:dc-acct-1")
            assert conn._token_factory is not None
            assert self._TOKEN_ATTR not in conn._attrs_before
            args, _ = mock_ddbc_conn.call_args
            assert callable(args[4])
        finally:
            conn.close()

    @patch("mssql_python.connection.get_auth_token_info")
    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_factory_raises_when_token_unavailable(self, mock_ddbc_conn, mock_get_token_info):
        # If the deferred token acquisition yields nothing, the factory must
        # fail closed (raise) so native never opens a pooled connection with
        # Authentication= stripped and no token attached.
        mock_ddbc_conn.return_value = MagicMock()
        mock_get_token_info.return_value = TokenInfo(
            token_struct=b"dc-token-bytes",
            expires_on=1893456000,
            home_account_id="dc-acct-2",
        )
        from mssql_python import connect

        conn = connect("Server=test;Database=testdb;Authentication=ActiveDirectoryDeviceCode")
        try:
            # Simulate the provider returning nothing on the deferred call.
            mock_get_token_info.return_value = None
            with pytest.raises(InterfaceError):
                conn._token_factory()
        finally:
            conn.close()

    @patch("mssql_python.connection.get_auth_token_info")
    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_account_factory_rejects_changed_account(self, mock_ddbc_conn, mock_get_token_info):
        # The acct: pool bucket is bound to the home_account_id the pool was
        # keyed on. If the deferred token acquisition later yields a DIFFERENT
        # account (e.g. the signed-in principal changed between pooling and the
        # physical connect), the factory must fail closed rather than open a
        # connection authenticated as a different account in this pool.
        mock_ddbc_conn.return_value = MagicMock()
        mock_get_token_info.return_value = TokenInfo(
            token_struct=b"dc-token-bytes",
            expires_on=1893456000,
            home_account_id="dc-acct-1",
        )
        from mssql_python import connect

        conn = connect("Server=test;Database=testdb;Authentication=ActiveDirectoryDeviceCode")
        try:
            assert conn._pool_key.endswith("\x00acct:dc-acct-1")
            # The signed-in account changed under the same pool bucket.
            mock_get_token_info.return_value = TokenInfo(
                token_struct=b"other-token-bytes",
                expires_on=1893456000,
                home_account_id="dc-acct-2",
            )
            with pytest.raises(InterfaceError):
                conn._token_factory()
        finally:
            conn.close()


class TestRawAccessTokenPoolKey:
    """A raw SQL_COPT_SS_ACCESS_TOKEN in attrs_before (no ``Authentication=``
    keyword — the documented msodbcsql raw-token / token_provider pattern) must
    still isolate the pool per token, or two callers with different tokens
    against the same server would share a pooled connection."""

    _TOKEN_ATTR = ConstantsDDBC.SQL_COPT_SS_ACCESS_TOKEN.value

    @pytest.mark.parametrize(
        "bad_token",
        [
            "a-str-token",  # the exact str bypass called out in review
            12345,  # int
            memoryview(b"\x04\x00\x00\x00tok"),  # memoryview is not bytes/bytearray
            ["not", "bytes"],  # list
        ],
        ids=["str", "int", "memoryview", "list"],
    )
    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_non_binary_access_token_fails_closed(self, mock_ddbc_conn, bad_token):
        """A non-bytes access token is rejected up front with InterfaceError.

        Adversarial case: a caller supplies attr 1256 as a ``str`` (or any
        non-binary type). The Python identity-key logic only hashes
        bytes/bytearray, so without this guard a str token would fall through
        with the bare connStr pool key and two callers with different str tokens
        against the same server could share a pooled connection. The guard fails
        closed *before* any physical connect, so nothing is ever pooled under a
        shared key. The native ``setAttribute`` enforces the same rule.
        """
        from mssql_python import connect

        with pytest.raises(InterfaceError, match="SQL_COPT_SS_ACCESS_TOKEN"):
            connect(
                "Server=test;Database=testdb;UID=sa;PWD=secret",
                attrs_before={self._TOKEN_ATTR: bad_token},
            )
        # Fail closed: the guard raises before the native Connection is built.
        mock_ddbc_conn.assert_not_called()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_raw_token_sets_token_hash_pool_key(self, mock_ddbc_conn):
        import hashlib

        mock_ddbc_conn.return_value = MagicMock()
        from mssql_python import connect

        token = b"raw-access-token-bytes"
        conn = connect(
            "Server=test;Database=testdb;UID=sa;PWD=secret",
            attrs_before={self._TOKEN_ATTR: token},
        )
        try:
            expected = "tok:" + hashlib.sha256(token).hexdigest()
            assert conn._pool_key.endswith("\x00" + expected)
            assert conn._pool_key != ""
        finally:
            conn.close()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_different_raw_tokens_get_distinct_pool_keys(self, mock_ddbc_conn):
        mock_ddbc_conn.return_value = MagicMock()
        from mssql_python import connect

        conn_a = connect(
            "Server=test;Database=testdb;UID=sa;PWD=secret",
            attrs_before={self._TOKEN_ATTR: b"token-A"},
        )
        conn_b = connect(
            "Server=test;Database=testdb;UID=sa;PWD=secret",
            attrs_before={self._TOKEN_ATTR: b"token-B"},
        )
        try:
            assert conn_a._pool_key != conn_b._pool_key
        finally:
            conn_a.close()
            conn_b.close()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_same_raw_token_gets_stable_pool_key(self, mock_ddbc_conn):
        mock_ddbc_conn.return_value = MagicMock()
        from mssql_python import connect

        conn_a = connect(
            "Server=test;Database=testdb;UID=sa;PWD=secret",
            attrs_before={self._TOKEN_ATTR: b"same-token"},
        )
        conn_b = connect(
            "Server=test;Database=testdb;UID=sa;PWD=secret",
            attrs_before={self._TOKEN_ATTR: b"same-token"},
        )
        try:
            assert conn_a._pool_key == conn_b._pool_key
        finally:
            conn_a.close()
            conn_b.close()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_bytearray_raw_token_is_handled(self, mock_ddbc_conn):
        import hashlib

        mock_ddbc_conn.return_value = MagicMock()
        from mssql_python import connect

        token = bytearray(b"bytearray-token")
        conn = connect(
            "Server=test;Database=testdb;UID=sa;PWD=secret",
            attrs_before={self._TOKEN_ATTR: token},
        )
        try:
            expected = "tok:" + hashlib.sha256(bytes(token)).hexdigest()
            assert conn._pool_key.endswith("\x00" + expected)
            # TOCTOU fix: the mutable bytearray is frozen to immutable bytes once
            # and stored back, so the value the pool hash saw and the value the
            # native connect reads are the same object and can't be mutated
            # between hashing and connect.
            stored = conn._attrs_before[self._TOKEN_ATTR]
            assert isinstance(stored, bytes)
            assert not isinstance(stored, bytearray)
            assert stored == b"bytearray-token"
        finally:
            conn.close()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_no_token_leaves_pool_key_empty(self, mock_ddbc_conn):
        # Plain SQL auth with no token: pool key stays empty (legacy connStr
        # keying), so the raw-token guard does not disturb existing behavior.
        mock_ddbc_conn.return_value = MagicMock()
        from mssql_python import connect

        conn = connect(
            "Server=test;Database=testdb;UID=sa;PWD=secret",
            attrs_before={1256000: 30},  # some unrelated non-token attribute
        )
        try:
            assert conn._pool_key == ""
        finally:
            conn.close()


class TestConnectionStringNulRejection:
    """The identity-aware pool key joins the sanitized connection string and the
    per-identity discriminator with a ``\\x00`` separator, and the ODBC layer
    terminates a connection string at the first NUL (SQL_NTS). A caller-supplied
    NUL in the connection string (or any string kwarg) could forge/collide a
    pool key or silently truncate the connection string, so it is rejected at
    the Python boundary before any pool key is built or a connection opened."""

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_nul_in_connection_string_is_rejected(self, mock_ddbc_conn):
        mock_ddbc_conn.return_value = MagicMock()
        from mssql_python import connect

        with pytest.raises(InterfaceError, match="NUL"):
            connect("Server=test;Database=t\x00est;UID=sa;PWD=secret")
        # Fail closed: rejected before any native Connection is built.
        mock_ddbc_conn.assert_not_called()

    @patch("mssql_python.connection.ddbc_bindings.Connection")
    def test_nul_in_string_kwarg_is_rejected(self, mock_ddbc_conn):
        mock_ddbc_conn.return_value = MagicMock()
        from mssql_python import connect

        with pytest.raises(InterfaceError, match="NUL"):
            connect("Server=test;Database=testdb", database="te\x00st")
        mock_ddbc_conn.assert_not_called()
