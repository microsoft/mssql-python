"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
This module handles authentication for the mssql_python package.
"""

import platform
import struct
from typing import Tuple, Dict, Optional, List

from mssql_python.logging import logger
from mssql_python.constants import AuthType, ConstantsDDBC


class AADAuth:
    """Handles Azure Active Directory authentication"""

    @staticmethod
    def get_token_struct(token: str) -> bytes:
        """Convert token to SQL Server compatible format"""
        logger.debug(
            "get_token_struct: Converting token to SQL Server format - token_length=%d chars",
            len(token),
        )
        token_bytes = token.encode("UTF-16-LE")
        logger.debug(
            "get_token_struct: Token encoded to UTF-16-LE - byte_length=%d", len(token_bytes)
        )
        return struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)

    @staticmethod
    def get_token(auth_type: str) -> bytes:
        """Get DDBC token struct for the specified authentication type."""
        token_struct, _ = AADAuth._acquire_token(auth_type)
        return token_struct

    @staticmethod
    def get_raw_token(auth_type: str) -> str:
        """Acquire a fresh raw JWT for the mssql-py-core connection (bulk copy).

        This deliberately does NOT cache the credential or token — each call
        creates a new Azure Identity credential instance and requests a token.
        A fresh acquisition avoids expired-token errors when bulkcopy() is
        called long after the original DDBC connect().
        """
        _, raw_token = AADAuth._acquire_token(auth_type)
        return raw_token

    @staticmethod
    def _acquire_token(auth_type: str) -> Tuple[bytes, str]:
        """Internal: acquire token and return (ddbc_struct, raw_jwt)."""
        # Import Azure libraries inside method to support test mocking
        # pylint: disable=import-outside-toplevel
        try:
            from azure.identity import (
                DefaultAzureCredential,
                DeviceCodeCredential,
                InteractiveBrowserCredential,
            )
            from azure.core.exceptions import ClientAuthenticationError
        except ImportError as e:
            raise RuntimeError(
                "Azure authentication libraries are not installed. "
                "Please install with: pip install azure-identity azure-core"
            ) from e

        # Mapping of auth types to credential classes
        credential_map = {
            "default": DefaultAzureCredential,
            "devicecode": DeviceCodeCredential,
            "interactive": InteractiveBrowserCredential,
        }

        credential_class = credential_map.get(auth_type)
        if not credential_class:
            raise ValueError(
                f"Unsupported auth_type '{auth_type}'. " f"Supported: {', '.join(credential_map)}"
            )
        logger.info(
            "get_token: Starting Azure AD authentication - auth_type=%s, credential_class=%s",
            auth_type,
            credential_class.__name__,
        )

        try:
            credential = credential_class()
            raw_token = credential.get_token("https://database.windows.net/.default").token
            logger.info(
                "get_token: Azure AD token acquired successfully - token_length=%d chars",
                len(raw_token),
            )
            token_struct = AADAuth.get_token_struct(raw_token)
            return token_struct, raw_token
        except ClientAuthenticationError as e:
            logger.error(
                "get_token: Azure AD authentication failed - credential_class=%s, error=%s",
                credential_class.__name__,
                str(e),
            )
            raise RuntimeError(
                f"Azure AD authentication failed for {credential_class.__name__}: {e}. "
                f"This could be due to invalid credentials, missing environment variables, "
                f"user cancellation, network issues, or unsupported configuration."
            ) from e
        except Exception as e:
            logger.error(
                "get_token: Unexpected error during credential creation - credential_class=%s, error=%s",
                credential_class.__name__,
                str(e),
            )
            raise RuntimeError(f"Failed to create {credential_class.__name__}: {e}") from e


def process_auth_parameters(parameters: List[str]) -> Tuple[List[str], Optional[str]]:
    """
    Process connection parameters and extract authentication type.

    Args:
        parameters: List of connection string parameters

    Returns:
        Tuple[list, Optional[str]]: Modified parameters and authentication type

    Raises:
        ValueError: If an invalid authentication type is provided
    """
    logger.debug("process_auth_parameters: Processing %d connection parameters", len(parameters))
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
                logger.debug("process_auth_parameters: Interactive authentication detected")
                # Interactive authentication (browser-based); only append parameter for non-Windows
                if platform.system().lower() == "windows":
                    logger.debug(
                        "process_auth_parameters: Windows platform - using native AADInteractive"
                    )
                    auth_type = None  # Let Windows handle AADInteractive natively

            elif value_lower == AuthType.DEVICE_CODE.value:
                # Device code authentication (for devices without browser)
                logger.debug("process_auth_parameters: Device code authentication detected")
                auth_type = "devicecode"
            elif value_lower == AuthType.DEFAULT.value:
                # Default authentication (uses DefaultAzureCredential)
                logger.debug("process_auth_parameters: Default Azure authentication detected")
                auth_type = "default"
        modified_parameters.append(param)

    logger.debug(
        "process_auth_parameters: Processing complete - auth_type=%s, param_count=%d",
        auth_type,
        len(modified_parameters),
    )
    return modified_parameters, auth_type


def remove_sensitive_params(parameters: List[str]) -> List[str]:
    """Remove sensitive parameters from connection string"""
    logger.debug(
        "remove_sensitive_params: Removing sensitive parameters - input_count=%d", len(parameters)
    )
    exclude_keys = [
        "uid=",
        "pwd=",
        "trusted_connection=",
        "authentication=",
    ]
    result = [
        param
        for param in parameters
        if not any(param.lower().startswith(exclude) for exclude in exclude_keys)
    ]
    logger.debug(
        "remove_sensitive_params: Sensitive parameters removed - output_count=%d", len(result)
    )
    return result


def get_auth_token(auth_type: str) -> Optional[bytes]:
    """Get DDBC authentication token struct based on auth type."""
    logger.debug("get_auth_token: Starting - auth_type=%s", auth_type)
    if not auth_type:
        logger.debug("get_auth_token: No auth_type specified, returning None")
        return None

    # Handle platform-specific logic for interactive auth
    if auth_type == "interactive" and platform.system().lower() == "windows":
        logger.debug("get_auth_token: Windows interactive auth - delegating to native handler")
        return None  # Let Windows handle AADInteractive natively

    try:
        token = AADAuth.get_token(auth_type)
        logger.info("get_auth_token: Token acquired successfully - auth_type=%s", auth_type)
        return token
    except (ValueError, RuntimeError) as e:
        logger.warning(
            "get_auth_token: Token acquisition failed - auth_type=%s, error=%s", auth_type, str(e)
        )
        return None


def extract_auth_type(connection_string: str) -> Optional[str]:
    """Extract Entra ID auth type from a connection string.

    Used as a fallback when process_connection_string does not propagate
    auth_type (e.g. Windows Interactive where DDBC handles auth natively).
    Bulkcopy still needs the auth type to acquire a token via Azure Identity.
    """
    auth_map = {
        AuthType.INTERACTIVE.value: "interactive",
        AuthType.DEVICE_CODE.value: "devicecode",
        AuthType.DEFAULT.value: "default",
    }
    for part in connection_string.split(";"):
        key, _, value = part.strip().partition("=")
        if key.strip().lower() == "authentication":
            return auth_map.get(value.strip().lower())
    return None


def process_connection_string(
    connection_string: str,
) -> Tuple[str, Optional[Dict[int, bytes]], Optional[str]]:
    """
    Process connection string and handle authentication.

    Args:
        connection_string: The connection string to process

    Returns:
        Tuple[str, Optional[Dict], Optional[str]]: Processed connection string,
            attrs_before dict if needed, and auth_type string for bulk copy token acquisition

    Raises:
        ValueError: If the connection string is invalid or empty
    """
    logger.debug(
        "process_connection_string: Starting - conn_str_length=%d",
        len(connection_string) if isinstance(connection_string, str) else 0,
    )
    # Check type first
    if not isinstance(connection_string, str):
        logger.error(
            "process_connection_string: Invalid type - expected str, got %s",
            type(connection_string).__name__,
        )
        raise ValueError("Connection string must be a string")

    # Then check if empty
    if not connection_string:
        logger.error("process_connection_string: Connection string is empty")
        raise ValueError("Connection string cannot be empty")

    parameters = connection_string.split(";")
    logger.debug(
        "process_connection_string: Split connection string - parameter_count=%d", len(parameters)
    )

    # Validate that there's at least one valid parameter
    if not any("=" in param for param in parameters):
        logger.error(
            "process_connection_string: Invalid connection string format - no key=value pairs found"
        )
        raise ValueError("Invalid connection string format")

    modified_parameters, auth_type = process_auth_parameters(parameters)

    if auth_type:
        logger.info(
            "process_connection_string: Authentication type detected - auth_type=%s", auth_type
        )
        modified_parameters = remove_sensitive_params(modified_parameters)
        token_struct = get_auth_token(auth_type)
        if token_struct:
            logger.info(
                "process_connection_string: Token authentication configured successfully - auth_type=%s",
                auth_type,
            )
            return (
                ";".join(modified_parameters) + ";",
                {ConstantsDDBC.SQL_COPT_SS_ACCESS_TOKEN.value: token_struct},
                auth_type,
            )
        else:
            logger.warning(
                "process_connection_string: Token acquisition failed, proceeding without token"
            )

    logger.debug(
        "process_connection_string: Connection string processing complete - has_auth=%s",
        bool(auth_type),
    )
    return ";".join(modified_parameters) + ";", None, auth_type
