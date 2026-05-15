"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
This module handles authentication for the mssql_python package.
"""

import platform
import struct
import threading
from typing import Tuple, Dict, Optional, List

from mssql_python.logging import logger
from mssql_python.constants import AuthType, ConstantsDDBC
from mssql_python.connection_string_parser import _ConnectionStringParser

# Module-level credential instance cache.
# Reusing credential objects allows the Azure Identity SDK's built-in
# in-memory token cache to work, avoiding redundant token acquisitions.
# See: https://github.com/Azure/azure-sdk-for-python/blob/main/sdk/identity/azure-identity/TOKEN_CACHING.md
#
# Cache is keyed on (auth_type, sorted credential_kwargs), which is
# bounded by the distinct credentials a single process ever uses.
_credential_cache: Dict[object, object] = {}
_credential_cache_lock = threading.Lock()


def _credential_cache_key(auth_type: str, credential_kwargs: Optional[Dict[str, str]]):
    """Build a hashable cache key from auth_type and optional credential kwargs.

    Returns the plain auth_type string when no kwargs are provided so that
    callers caching by string (the original behavior) keep working. When
    kwargs are present (e.g. user-assigned MSI client_id), the key is a
    tuple of ``(auth_type, sorted_kwargs_items)`` so different kwargs map
    to different cached credentials.
    """
    if not credential_kwargs:
        return auth_type
    return (auth_type, tuple(sorted(credential_kwargs.items())))


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
    def get_token(auth_type: str, credential_kwargs: Optional[Dict[str, str]] = None) -> bytes:
        """Get DDBC token struct for the specified authentication type."""
        token_struct, _ = AADAuth._acquire_token(auth_type, credential_kwargs)
        return token_struct

    @staticmethod
    def get_raw_token(auth_type: str, credential_kwargs: Optional[Dict[str, str]] = None) -> str:
        """Acquire a raw JWT for the mssql-py-core connection (bulk copy).

        Uses the cached credential instance so the Azure Identity SDK's
        built-in token cache can serve a valid token without a round-trip
        when the previous token has not yet expired.
        """
        _, raw_token = AADAuth._acquire_token(auth_type, credential_kwargs)
        return raw_token

    @staticmethod
    def _acquire_token(
        auth_type: str, credential_kwargs: Optional[Dict[str, str]] = None
    ) -> Tuple[bytes, str]:
        """Internal: acquire token and return (ddbc_struct, raw_jwt)."""
        # Import Azure libraries inside method to support test mocking
        # pylint: disable=import-outside-toplevel
        try:
            from azure.identity import (
                DefaultAzureCredential,
                DeviceCodeCredential,
                InteractiveBrowserCredential,
                ManagedIdentityCredential,
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
            "msi": ManagedIdentityCredential,
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

        kwargs = credential_kwargs or {}
        cache_key = _credential_cache_key(auth_type, kwargs)
        try:
            with _credential_cache_lock:
                if cache_key not in _credential_cache:
                    logger.debug(
                        "get_token: Creating new credential instance for auth_type=%s",
                        auth_type,
                    )
                    _credential_cache[cache_key] = credential_class(**kwargs)
                else:
                    logger.debug(
                        "get_token: Reusing cached credential instance for auth_type=%s",
                        auth_type,
                    )
                credential = _credential_cache[cache_key]
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


def _extract_msi_client_id(connection_string: str) -> Optional[str]:
    """Pull UID out of a connection string for user-assigned MSI.

    For ActiveDirectoryMSI, UID (when present) carries the user-assigned
    identity's ``client_id``. Returns None for system-assigned MSI.

    Uses the canonical ``_ConnectionStringParser`` so braced ODBC values
    are handled correctly: a ``UID={hello=world}`` resolves to the value
    ``hello=world`` (no surrounding braces, no false split on the inner
    ``=``), and a semicolon inside a legitimate braced value (e.g.
    ``Database={foo;uid=victim;bar}``) cannot spoof a top-level ``UID=``.
    """
    # Connection.__init__ already parsed the same string through
    # _ConnectionStringParser via _construct_connection_string, so by the
    # time we get here the input is guaranteed parseable. No defensive
    # try/except: a parse failure now means a real bug upstream and should
    # propagate, not silently degrade user-assigned MSI to system-assigned.
    parsed = _ConnectionStringParser(validate_keywords=False)._parse(connection_string)
    uid = (parsed.get("uid") or "").strip()
    return uid or None


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
            elif value_lower == AuthType.MSI.value:
                # Managed identity authentication (system- or user-assigned)
                logger.debug("process_auth_parameters: Managed identity authentication detected")
                auth_type = "msi"
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


def get_auth_token(
    auth_type: str, credential_kwargs: Optional[Dict[str, str]] = None
) -> Optional[bytes]:
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
        token = AADAuth.get_token(auth_type, credential_kwargs)
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
        AuthType.MSI.value: "msi",
    }
    for part in connection_string.split(";"):
        key, _, value = part.strip().partition("=")
        if key.strip().lower() == "authentication":
            return auth_map.get(value.strip().lower())
    return None


def process_connection_string(
    connection_string: str,
) -> Tuple[str, Optional[Dict[int, bytes]], Optional[str], Optional[Dict[str, str]]]:
    """
    Process connection string and handle authentication.

    NOTE: Returns a 4-tuple. Callers must unpack all four elements.
    Destructuring with three names raises ``ValueError: too many values
    to unpack``. The fourth element (``credential_kwargs``) is needed by
    Connection.__init__ to persist credential constructor args (e.g. the
    user-assigned MSI ``client_id``) for the bulkcopy fresh-token path,
    since UID is stripped from the sanitized connection string.

    Args:
        connection_string: The connection string to process

    Returns:
        Tuple[str, Optional[Dict], Optional[str], Optional[Dict[str, str]]]:
            Processed connection string, attrs_before dict if needed, auth_type
            string for bulk copy token acquisition, and credential constructor
            kwargs (e.g. user-assigned MSI ``client_id``) to be persisted on
            the Connection so bulkcopy can re-use them when acquiring a fresh
            token after sanitization has stripped UID from the connection
            string.

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

    # Capture credential kwargs (e.g. user-assigned MSI client_id) before
    # remove_sensitive_params strips UID from the parameter list. Pass the
    # original connection_string (not modified_parameters) so the helper can
    # use the canonical _ConnectionStringParser — handles braced values like
    # UID={hello=world} correctly.
    credential_kwargs: Dict[str, str] = {}
    if auth_type == "msi":
        client_id = _extract_msi_client_id(connection_string)
        if client_id:
            credential_kwargs["client_id"] = client_id
            logger.debug(
                "process_connection_string: ActiveDirectoryMSI with UID — "
                "user-assigned managed identity selected (client_id length=%d)",
                len(client_id),
            )
        else:
            logger.debug(
                "process_connection_string: ActiveDirectoryMSI without UID — "
                "system-assigned managed identity selected"
            )

    if auth_type:
        logger.info(
            "process_connection_string: Authentication type detected - auth_type=%s", auth_type
        )
        modified_parameters = remove_sensitive_params(modified_parameters)
        token_struct = get_auth_token(auth_type, credential_kwargs or None)
        if token_struct:
            logger.info(
                "process_connection_string: Token authentication configured successfully - auth_type=%s",
                auth_type,
            )
            return (
                ";".join(modified_parameters) + ";",
                {ConstantsDDBC.SQL_COPT_SS_ACCESS_TOKEN.value: token_struct},
                auth_type,
                credential_kwargs or None,
            )
        else:
            logger.warning(
                "process_connection_string: Token acquisition failed, proceeding without token"
            )

    logger.debug(
        "process_connection_string: Connection string processing complete - has_auth=%s",
        bool(auth_type),
    )
    return (
        ";".join(modified_parameters) + ";",
        None,
        auth_type,
        credential_kwargs or None,
    )
