"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
This module handles authentication for the mssql_python package.
"""

import platform
import struct
from typing import Tuple, Dict, Optional, Union
from mssql_python.constants import AuthType

def validate_access_token_struct(token_struct: bytes) -> None:
    """
    Validate ACCESSTOKEN structure to prevent ODBC driver crashes.
    
    The ODBC driver crashes (segfault on macOS/Linux, access violation on Windows)
    when given malformed access tokens. This function validates the structure
    before passing to the driver.
    
    ACCESSTOKEN structure: typedef struct { DWORD dataSize; BYTE data[]; } ACCESSTOKEN;
    
    Args:
        token_struct (bytes): The ACCESSTOKEN structure to validate
        
    Raises:
        ValueError: If the token structure is invalid
    """
    # Check minimum size (4-byte header + data)
    if len(token_struct) < 4:
        raise ValueError(
            f"Invalid access token: minimum 4 bytes required for ACCESSTOKEN structure, got {len(token_struct)} bytes"
        )
    
    # Extract declared size from first 4 bytes
    declared_size = struct.unpack('<I', token_struct[:4])[0]
    
    # Validate structure integrity
    total_size = len(token_struct)
    expected_size = declared_size + 4
    if expected_size != total_size:
        raise ValueError(
            f"Invalid access token: size mismatch in ACCESSTOKEN structure. "
            f"Header declares {declared_size} bytes, but structure has {total_size - 4} bytes of data"
        )
    
    # Validate token data is not empty/all zeros
    token_data = token_struct[4:]
    if not any(token_data):
        raise ValueError("Invalid access token: token data is empty or all zeros")
    
    # Validate UTF-16LE encoding (ODBC driver requirement)
    # JWT tokens in UTF-16LE have null bytes interleaved with ASCII characters
    if declared_size % 2 != 0:
        raise ValueError(
            f"Invalid access token: must be UTF-16LE encoded (got odd byte length {declared_size})"
        )
    
    # Check for UTF-16LE pattern: ASCII characters with interleaved null bytes
    # Real JWTs start with "eyJ" in UTF-16LE: 65 00 79 00 4A 00
    if declared_size >= 6:
        has_utf16_pattern = all([
            0x20 <= token_data[0] <= 0x7E and token_data[1] == 0,  # First char
            0x20 <= token_data[2] <= 0x7E and token_data[3] == 0,  # Second char
            0x20 <= token_data[4] <= 0x7E and token_data[5] == 0   # Third char
        ])
        
        if not has_utf16_pattern:
            raise ValueError(
                "Invalid access token: must be UTF-16LE encoded JWT. "
                "Expected alternating ASCII and null bytes (e.g., 'e\\x00y\\x00J\\x00' for 'eyJ')"
            )

class AADAuth:
    """Handles Azure Active Directory authentication"""
    
    @staticmethod
    def get_token_struct(token: str) -> bytes:
        """Convert token to SQL Server compatible format"""
        token_bytes = token.encode("UTF-16-LE")
        token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
        
        # Validate before returning to catch any encoding issues early
        validate_access_token_struct(token_struct)
        
        return token_struct

    @staticmethod
    def get_token(auth_type: str) -> bytes:
        """Get token using the specified authentication type"""
        from azure.identity import (
            DefaultAzureCredential, 
            DeviceCodeCredential, 
            InteractiveBrowserCredential
        )
        from azure.core.exceptions import ClientAuthenticationError
        
        # Mapping of auth types to credential classes
        credential_map = {
            "default": DefaultAzureCredential,
            "devicecode": DeviceCodeCredential,
            "interactive": InteractiveBrowserCredential,
        }
        
        credential_class = credential_map[auth_type]
        
        try:
            credential = credential_class()
            token = credential.get_token("https://database.windows.net/.default").token
            return AADAuth.get_token_struct(token)
        except ClientAuthenticationError as e:
            # Re-raise with more specific context about Azure AD authentication failure
            raise RuntimeError(
                f"Azure AD authentication failed for {credential_class.__name__}: {e}. "
                f"This could be due to invalid credentials, missing environment variables, "
                f"user cancellation, network issues, or unsupported configuration."
            ) from e
        except Exception as e:
            # Catch any other unexpected exceptions
            raise RuntimeError(f"Failed to create {credential_class.__name__}: {e}") from e

def process_auth_parameters(parameters: list) -> Tuple[list, Optional[str]]:
    """
    Process connection parameters and extract authentication type.
    
    Args:
        parameters: List of connection string parameters
        
    Returns:
        Tuple[list, Optional[str]]: Modified parameters and authentication type
        
    Raises:
        ValueError: If an invalid authentication type is provided
    """
    modified_parameters = []
    auth_type = None

    for param in parameters:
        param = param.strip()
        if not param:
            continue

        if "=" not in param:
            modified_parameters.append(param)
            continue

        key, value = param.split("=", 1)
        key_lower = key.lower()
        value_lower = value.lower()

        if key_lower == "authentication":
            # Check for supported authentication types and set auth_type accordingly
            if value_lower == AuthType.INTERACTIVE.value:
                auth_type = "interactive"
                # Interactive authentication (browser-based); only append parameter for non-Windows
                if platform.system().lower() == "windows":
                    auth_type = None  # Let Windows handle AADInteractive natively
                
            elif value_lower == AuthType.DEVICE_CODE.value:
                # Device code authentication (for devices without browser)
                auth_type = "devicecode"
            elif value_lower == AuthType.DEFAULT.value:
                # Default authentication (uses DefaultAzureCredential)
                auth_type = "default"
        modified_parameters.append(param)

    return modified_parameters, auth_type

def remove_sensitive_params(parameters: list) -> list:
    """Remove sensitive parameters from connection string"""
    exclude_keys = [
        "uid=", "pwd=", "encrypt=", "trustservercertificate=", "authentication="
    ]
    return [
        param for param in parameters
        if not any(param.lower().startswith(exclude) for exclude in exclude_keys)
    ]

def get_auth_token(auth_type: str) -> Optional[bytes]:
    """Get authentication token based on auth type"""
    if not auth_type:
        return None
        
    # Handle platform-specific logic for interactive auth
    if auth_type == "interactive" and platform.system().lower() == "windows":
        return None  # Let Windows handle AADInteractive natively
        
    try:
        return AADAuth.get_token(auth_type)
    except (ValueError, RuntimeError):
        return None

def process_connection_string(connection_string: str) -> Tuple[str, Optional[Dict]]:
    """
    Process connection string and handle authentication.
    
    Args:
        connection_string: The connection string to process
        
    Returns:
        Tuple[str, Optional[Dict]]: Processed connection string and attrs_before dict if needed
        
    Raises:
        ValueError: If the connection string is invalid or empty
    """
    # Check type first
    if not isinstance(connection_string, str):
        raise ValueError("Connection string must be a string")

    # Then check if empty
    if not connection_string:
        raise ValueError("Connection string cannot be empty")

    parameters = connection_string.split(";")
    
    # Validate that there's at least one valid parameter
    if not any('=' in param for param in parameters):
        raise ValueError("Invalid connection string format")

    modified_parameters, auth_type = process_auth_parameters(parameters)

    if auth_type:
        modified_parameters = remove_sensitive_params(modified_parameters)
        token_struct = get_auth_token(auth_type)
        if token_struct:
            return ";".join(modified_parameters) + ";", {1256: token_struct}

    return ";".join(modified_parameters) + ";", None