"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
This module handles authentication for the mssql_python package.
"""

import platform
import struct
from typing import Tuple, Dict, Optional, Union
from mssql_python.constants import AuthType

class AADAuth:
    """Handles Azure Active Directory authentication"""
    
    @staticmethod
    def get_token_struct(token: str) -> bytes:
        """Convert token to SQL Server compatible format"""
        token_bytes = token.encode("UTF-16-LE")
        return struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)

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