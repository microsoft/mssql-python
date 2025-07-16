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

    class MockIdentity:
        DefaultAzureCredential = MockDefaultAzureCredential
        DeviceCodeCredential = MockDeviceCodeCredential
        InteractiveBrowserCredential = MockInteractiveBrowserCredential

    # Create mock azure module if it doesn't exist
    if 'azure' not in sys.modules:
        sys.modules['azure'] = type('MockAzure', (), {})()
    
    # Add identity module to azure
    sys.modules['azure.identity'] = MockIdentity()
    
    yield
    
    # Cleanup
    if 'azure.identity' in sys.modules:
        del sys.modules['azure.identity']

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

    def test_get_default_token(self):
        token_struct = AADAuth.get_default_token()
        assert isinstance(token_struct, bytes)

    def test_get_device_code_token(self):
        token_struct = AADAuth.get_device_code_token()
        assert isinstance(token_struct, bytes)

    def test_get_interactive_token(self):
        token_struct = AADAuth.get_interactive_token()
        assert isinstance(token_struct, bytes)

class TestProcessAuthParameters:
    def test_empty_parameters(self):
        modified_params, auth_type = process_auth_parameters([])
        assert modified_params == []
        assert auth_type is None

    def test_interactive_auth_windows(self, monkeypatch):
        monkeypatch.setattr(platform, "system", lambda: "Windows")
        params = ["Authentication=ActiveDirectoryInteractive", "Server=test"]
        modified_params, auth_type = process_auth_parameters(params)
        assert "Authentication=ActiveDirectoryInteractive" not in modified_params
        assert auth_type == None

    def test_interactive_auth_non_windows(self, monkeypatch):
        monkeypatch.setattr(platform, "system", lambda: "Darwin")
        params = ["Authentication=ActiveDirectoryInteractive", "Server=test"]
        modified_params, auth_type = process_auth_parameters(params)
        assert "Authentication=ActiveDirectoryInteractive" in modified_params
        assert auth_type == "interactive"

    def test_device_code_auth(self):
        params = ["Authentication=ActiveDirectoryDeviceCode", "Server=test"]
        modified_params, auth_type = process_auth_parameters(params)
        assert auth_type == "devicecode"

    def test_default_auth(self):
        params = ["Authentication=ActiveDirectoryDefault", "Server=test"]
        modified_params, auth_type = process_auth_parameters(params)
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