"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
Tests for the auth module.
"""

import pytest
import platform
import sys
from mssql_python.auth import (
    AADAuth,
    process_auth_parameters,
    remove_sensitive_params,
    get_auth_token,
    process_connection_string
)
from mssql_python.constants import AuthType
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

    # Mock ClientAuthenticationError
    class MockClientAuthenticationError(Exception):
        pass

    class MockIdentity:
        DefaultAzureCredential = MockDefaultAzureCredential
        DeviceCodeCredential = MockDeviceCodeCredential
        InteractiveBrowserCredential = MockInteractiveBrowserCredential

    class MockCore:
        class exceptions:
            ClientAuthenticationError = MockClientAuthenticationError

    # Create mock azure module if it doesn't exist
    if 'azure' not in sys.modules:
        sys.modules['azure'] = type('MockAzure', (), {})()
    
    # Add identity and core modules to azure
    sys.modules['azure.identity'] = MockIdentity()
    sys.modules['azure.core'] = MockCore()
    sys.modules['azure.core.exceptions'] = MockCore.exceptions()
    
    yield
    
    # Cleanup
    for module in ['azure.identity', 'azure.core', 'azure.core.exceptions']:
        if module in sys.modules:
            del sys.modules[module]

class TestAuthType:
    def test_auth_type_constants(self):
        assert AuthType.INTERACTIVE.value == "activedirectoryinteractive"
        assert AuthType.DEVICE_CODE.value == "activedirectorydevicecode"
        assert AuthType.DEFAULT.value == "activedirectorydefault"

class TestAADAuth:
    def test_get_token_struct(self):
        token_struct = AADAuth.get_token_struct(SAMPLE_TOKEN)
        assert isinstance(token_struct, bytes)
        assert len(token_struct) > 4

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

class TestProcessAuthParameters:
    def test_empty_parameters(self):
        modified_params, auth_type = process_auth_parameters([])
        assert modified_params == []
        assert auth_type is None

    def test_interactive_auth_windows(self, monkeypatch):
        monkeypatch.setattr(platform, "system", lambda: "Windows")
        params = ["Authentication=ActiveDirectoryInteractive", "Server=test"]
        modified_params, auth_type = process_auth_parameters(params)
        assert "Authentication=ActiveDirectoryInteractive" in modified_params
        assert auth_type == None

    def test_interactive_auth_non_windows(self, monkeypatch):
        monkeypatch.setattr(platform, "system", lambda: "Darwin")
        params = ["Authentication=ActiveDirectoryInteractive", "Server=test"]
        _, auth_type = process_auth_parameters(params)
        assert auth_type == "interactive"

    def test_device_code_auth(self):
        params = ["Authentication=ActiveDirectoryDeviceCode", "Server=test"]
        _, auth_type = process_auth_parameters(params)
        assert auth_type == "devicecode"

    def test_default_auth(self):
        params = ["Authentication=ActiveDirectoryDefault", "Server=test"]
        _, auth_type = process_auth_parameters(params)
        assert auth_type == "default"

class TestRemoveSensitiveParams:
    def test_remove_sensitive_parameters(self):
        params = [
            "Server=test",
            "UID=user",
            "PWD=password",
            "Encrypt=yes",
            "TrustServerCertificate=yes",
            "Authentication=ActiveDirectoryDefault",
            "Database=testdb"
        ]
        filtered_params = remove_sensitive_params(params)
        assert "Server=test" in filtered_params
        assert "Database=testdb" in filtered_params
        assert "UID=user" not in filtered_params
        assert "PWD=password" not in filtered_params
        assert "Encrypt=yes" not in filtered_params
        assert "TrustServerCertificate=yes" not in filtered_params
        assert "Authentication=ActiveDirectoryDefault" not in filtered_params

class TestProcessConnectionString:
    def test_process_connection_string_with_default_auth(self):
        conn_str = "Server=test;Authentication=ActiveDirectoryDefault;Database=testdb"
        result_str, attrs = process_connection_string(conn_str)
        
        assert "Server=test" in result_str
        assert "Database=testdb" in result_str
        assert attrs is not None
        assert 1256 in attrs
        assert isinstance(attrs[1256], bytes)

    def test_process_connection_string_no_auth(self):
        conn_str = "Server=test;Database=testdb;UID=user;PWD=password"
        result_str, attrs = process_connection_string(conn_str)
        
        assert "Server=test" in result_str
        assert "Database=testdb" in result_str
        assert "UID=user" in result_str
        assert "PWD=password" in result_str
        assert attrs is None

    def test_process_connection_string_interactive_non_windows(self, monkeypatch):
        monkeypatch.setattr(platform, "system", lambda: "Darwin")
        conn_str = "Server=test;Authentication=ActiveDirectoryInteractive;Database=testdb"
        result_str, attrs = process_connection_string(conn_str)
        
        assert "Server=test" in result_str
        assert "Database=testdb" in result_str
        assert attrs is not None
        assert 1256 in attrs
        assert isinstance(attrs[1256], bytes)

def test_error_handling():
    # Empty string should raise ValueError
    with pytest.raises(ValueError, match="Connection string cannot be empty"):
        process_connection_string("")

    # Invalid connection string should raise ValueError
    with pytest.raises(ValueError, match="Invalid connection string format"):
        process_connection_string("InvalidConnectionString")

    # Test non-string input
    with pytest.raises(ValueError, match="Connection string must be a string"):
        process_connection_string(None)


def test_short_access_token_protection_blocks_short_tokens():
    """
    Test protection against ODBC driver crashes with malformed access tokens.
    
    Microsoft ODBC Driver 18 has a bug where it crashes (segfault on macOS/Linux,
    access violation on Windows) when given malformed access tokens. This test
    verifies that our defensive validation properly rejects invalid tokens before
    they reach the ODBC driver.
    
    The validation is implemented in Connection::setAttribute() in connection.cpp
    and checks:
    1. Minimum size (4 bytes for ACCESSTOKEN header)
    2. Structure integrity (declared size matches actual size)
    3. Non-empty data (not all zeros)
    
    This test runs in a subprocess to isolate potential crashes.
    """
    import os
    import subprocess
    
    # Get connection string and remove UID/Pwd to force token-only mode
    conn_str = os.getenv("DB_CONNECTION_STRING")
    if not conn_str:
        pytest.skip("DB_CONNECTION_STRING environment variable not set")
    
    # Remove authentication to force pure token mode
    conn_str_no_auth = conn_str
    for remove_param in ["UID=", "Pwd=", "uid=", "pwd="]:
        if remove_param in conn_str_no_auth:
            parts = conn_str_no_auth.split(";")
            parts = [p for p in parts if not p.lower().startswith(remove_param.lower())]
            conn_str_no_auth = ";".join(parts)
    
    # Escape connection string for embedding in subprocess code
    escaped_conn_str = conn_str_no_auth.replace('\\', '\\\\').replace('"', '\\"')
    
    # Test cases for problematic tokens
    test_cases = [
        (b"", "empty token"),
        (b"x" * 3, "too small (< 4 bytes)"),
        (b"\x00\x00\x00\x00", "header only, no data"),
        (b"\x10\x00\x00\x00" + b"\x00" * 16, "size mismatch (declares 16, total 20)"),
        (b"\x10\x00\x00\x00" + b"\x00" * 12, "size mismatch (declares 16, has 12)"),
        (b"\x08\x00\x00\x00" + b"\x00" * 8, "all zeros data"),
    ]
    
    for token, description in test_cases:
        # Convert bytes to hex string for safe embedding in subprocess code
        token_hex = token.hex()
        
        code = f"""
import sys
from mssql_python import connect

conn_str = "{escaped_conn_str}"
fake_token = bytes.fromhex("{token_hex}")
attrs_before = {{1256: fake_token}}  # SQL_COPT_SS_ACCESS_TOKEN = 1256

try:
    connect(conn_str, attrs_before=attrs_before)
    print("ERROR: Should have raised exception for {description}")
    sys.exit(1)
except Exception as e:
    error_msg = str(e)
    # Check for our validation error messages
    if "Invalid access token" in error_msg:
        print(f"PASS: Got expected validation error for {description}")
        sys.exit(0)
    else:
        print(f"ERROR: Got unexpected error for {description}: {{error_msg}}")
        sys.exit(1)
"""
        
        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True
        )
        
        # Should not crash (exit code 139 on Linux, 134 on macOS, -11 on some systems)
        assert result.returncode not in [134, 139, -11], \
            f"Crash detected for {description}! STDERR: {result.stderr}"
        
        # Should exit cleanly with our validation error
        assert result.returncode == 0, \
            f"Expected validation error for {description}. Exit code: {result.returncode}\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}"
        
        assert "PASS" in result.stdout, \
            f"Expected PASS message for {description}, got: {result.stdout}"


def test_short_access_token_protection_allows_valid_tokens():
    """
    Test that properly formatted access tokens are NOT blocked by validation.
    
    This verifies that our defensive validation only blocks malformed tokens,
    and allows properly structured tokens to proceed (even though they may fail
    authentication if the token is invalid, which is expected behavior).
    
    Runs in separate subprocess to avoid ODBC driver state pollution from earlier tests.
    """
    import os
    import subprocess
    import struct
    
    # Get connection string and remove UID/Pwd to force token-only mode
    conn_str = os.getenv("DB_CONNECTION_STRING")
    if not conn_str:
        pytest.skip("DB_CONNECTION_STRING environment variable not set")
    
    # Remove authentication to force pure token mode
    conn_str_no_auth = conn_str
    for remove_param in ["UID=", "Pwd=", "uid=", "pwd="]:
        if remove_param in conn_str_no_auth:
            parts = conn_str_no_auth.split(";")
            parts = [p for p in parts if not p.lower().startswith(remove_param.lower())]
            conn_str_no_auth = ";".join(parts)
    
    # Escape connection string for embedding in subprocess code
    escaped_conn_str = conn_str_no_auth.replace('\\', '\\\\').replace('"', '\\"')
    
    # Test that properly formatted tokens don't get blocked (but will fail auth)
    # Create a properly formatted UTF-16LE encoded ACCESSTOKEN structure
    code = f"""
import sys
import struct
from mssql_python import connect

conn_str = "{escaped_conn_str}"

# Create properly formatted ACCESSTOKEN with UTF-16LE encoded data
# Use a fake JWT-like string that encodes properly
fake_jwt = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"  # Base64-like JWT header
token_data = fake_jwt.encode('utf-16-le')  # Properly encode as UTF-16LE
token_struct = struct.pack(f'<I{{len(token_data)}}s', len(token_data), token_data)

attrs_before = {{1256: token_struct}}

try:
    connect(conn_str, attrs_before=attrs_before)
    print("ERROR: Should have failed authentication")
    sys.exit(1)
except Exception as e:
    error_msg = str(e)
    # Should NOT get our validation errors
    if "Invalid access token" in error_msg:
        print(f"ERROR: Valid token structure was incorrectly blocked: {{error_msg}}")
        sys.exit(1)
    # Should get an authentication/connection error instead
    elif any(keyword in error_msg.lower() for keyword in ["login", "auth", "tcp", "connect", "token"]):
        print(f"PASS: Valid token structure not blocked, got expected connection/auth error")
        sys.exit(0)
    else:
        print(f"WARN: Got unexpected error (but structure passed validation): {{error_msg}}")
        sys.exit(0)  # Still pass - structure validation worked
"""
    
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True
    )
    
    # Should not crash
    assert result.returncode not in [134, 139, -11], \
        f"Segfault detected for legitimate token! STDERR: {result.stderr}"
    
    # Should pass the test
    assert result.returncode == 0, \
        f"Legitimate token test failed. Exit code: {result.returncode}\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}"
    
    assert "PASS" in result.stdout, \
        f"Expected PASS message for legitimate token, got: {result.stdout}"
