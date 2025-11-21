"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
This module provides helper functions for the mssql_python package.
"""

import re
import threading
import locale
from typing import Any, Union, Tuple, Optional
from mssql_python import ddbc_bindings
from mssql_python.exceptions import raise_exception
from mssql_python.logging import logger
from mssql_python.constants import ConstantsDDBC

# normalize_architecture import removed as it's unused


def add_driver_to_connection_str(connection_str: str) -> str:
    """
    Add the DDBC driver to the connection string if not present.

    Args:
        connection_str (str): The original connection string.


    Returns:
        str: The connection string with the DDBC driver added.

    Raises:
        Exception: If the connection string is invalid.
    """
    logger.debug(
        "add_driver_to_connection_str: Processing connection string (length=%d)",
        len(connection_str),
    )
    driver_name = "Driver={ODBC Driver 18 for SQL Server}"
    try:
        # Strip any leading or trailing whitespace from the connection string
        connection_str = connection_str.strip()
        connection_str = add_driver_name_to_app_parameter(connection_str)

        # Split the connection string into individual attributes
        connection_attributes = connection_str.split(";")
        final_connection_attributes = []

        # Iterate through the attributes and exclude any existing driver attribute
        driver_found = False
        for attribute in connection_attributes:
            if attribute.lower().split("=")[0] == "driver":
                driver_found = True
                logger.debug(
                    "add_driver_to_connection_str: Existing driver attribute found, removing"
                )
                continue
            final_connection_attributes.append(attribute)

        # Join the remaining attributes back into a connection string
        connection_str = ";".join(final_connection_attributes)

        # Insert the driver attribute at the beginning of the connection string
        final_connection_attributes.insert(0, driver_name)
        connection_str = ";".join(final_connection_attributes)
        logger.debug(
            "add_driver_to_connection_str: Driver added (had_existing=%s, attr_count=%d)",
            str(driver_found),
            len(final_connection_attributes),
        )

    except Exception as e:
        logger.debug(
            "add_driver_to_connection_str: Failed to process connection string - %s", str(e)
        )
        raise ValueError(
            "Invalid connection string, Please follow the format: "
            "Server=server_name;Database=database_name;UID=user_name;PWD=password"
        ) from e

    return connection_str


def check_error(handle_type: int, handle: Any, ret: int) -> None:
    """
    Check for errors and raise an exception if an error is found.

    Args:
        handle_type: The type of the handle (e.g., SQL_HANDLE_ENV, SQL_HANDLE_DBC).
        handle: The SqlHandle object associated with the operation.
        ret: The return code from the DDBC function call.

    Raises:
        RuntimeError: If an error is found.
    """
    if ret < 0:
        logger.debug(
            "check_error: Error detected - handle_type=%d, return_code=%d", handle_type, ret
        )
        error_info = ddbc_bindings.DDBCSQLCheckError(handle_type, handle, ret)
        logger.error("Error: %s", error_info.ddbcErrorMsg)
        logger.debug("check_error: SQL state=%s", error_info.sqlState)
        raise_exception(error_info.sqlState, error_info.ddbcErrorMsg)


def add_driver_name_to_app_parameter(connection_string: str) -> str:
    """
    Modifies the input connection string by appending the APP name.

    Args:
        connection_string (str): The input connection string.

    Returns:
        str: The modified connection string.
    """
    logger.debug("add_driver_name_to_app_parameter: Processing connection string")
    # Split the input string into key-value pairs
    parameters = connection_string.split(";")

    # Initialize variables
    app_found = False
    modified_parameters = []

    # Iterate through the key-value pairs
    for param in parameters:
        if param.lower().startswith("app="):
            # Overwrite the value with 'MSSQL-Python'
            app_found = True
            key, _ = param.split("=", 1)
            modified_parameters.append(f"{key}=MSSQL-Python")
            logger.debug("add_driver_name_to_app_parameter: Existing APP parameter overwritten")
        else:
            # Keep other parameters as is
            modified_parameters.append(param)

    # If APP key is not found, append it
    if not app_found:
        modified_parameters.append("APP=MSSQL-Python")
        logger.debug("add_driver_name_to_app_parameter: APP parameter added")

    # Join the parameters back into a connection string
    return ";".join(modified_parameters) + ";"


def sanitize_connection_string(conn_str: str) -> str:
    """
    Sanitize the connection string by removing sensitive information.
    Args:
        conn_str (str): The connection string to sanitize.
    Returns:
        str: The sanitized connection string.
    """
    logger.debug(
        "sanitize_connection_string: Sanitizing connection string (length=%d)", len(conn_str)
    )
    # Remove sensitive information from the connection string, Pwd section
    # Replace Pwd=...; or Pwd=... (end of string) with Pwd=***;
    sanitized = re.sub(r"(Pwd\s*=\s*)[^;]*", r"\1***", conn_str, flags=re.IGNORECASE)
    logger.debug("sanitize_connection_string: Password fields masked")
    return sanitized


def sanitize_user_input(user_input: str, max_length: int = 50) -> str:
    """
    Sanitize user input for safe logging by removing control characters,
    limiting length, and ensuring safe characters only.

    Args:
        user_input (str): The user input to sanitize.
        max_length (int): Maximum length of the sanitized output.

    Returns:
        str: The sanitized string safe for logging.
    """
    logger.debug(
        "sanitize_user_input: Sanitizing input (type=%s, length=%d)",
        type(user_input).__name__,
        len(user_input) if isinstance(user_input, str) else 0,
    )
    if not isinstance(user_input, str):
        logger.debug("sanitize_user_input: Non-string input detected")
        return "<non-string>"

    # Remove control characters and non-printable characters
    # Allow alphanumeric, dash, underscore, and dot (common in encoding names)
    sanitized = re.sub(r"[^\w\-\.]", "", user_input)

    # Limit length to prevent log flooding
    was_truncated = False
    if len(sanitized) > max_length:
        sanitized = sanitized[:max_length] + "..."
        was_truncated = True

    # Return placeholder if nothing remains after sanitization
    result = sanitized if sanitized else "<invalid>"
    logger.debug(
        "sanitize_user_input: Result length=%d, truncated=%s", len(result), str(was_truncated)
    )
    return result


def validate_attribute_value(
    attribute: Union[int, str],
    value: Union[int, str, bytes, bytearray],
    is_connected: bool = True,
    sanitize_logs: bool = True,
    max_log_length: int = 50,
) -> Tuple[bool, Optional[str], str, str]:
    """
    Validates attribute and value pairs for connection attributes.

    Performs basic type checking and validation of ODBC connection attributes.

    Args:
        attribute (int): The connection attribute to validate (SQL_ATTR_*)
        value: The value to set for the attribute (int, str, bytes, or bytearray)
        is_connected (bool): Whether the connection is already established
        sanitize_logs (bool): Whether to include sanitized versions for logging
        max_log_length (int): Maximum length of sanitized output for logging

    Returns:
        tuple: (is_valid, error_message, sanitized_attribute, sanitized_value)
    """
    logger.debug(
        "validate_attribute_value: Validating attribute=%s, value_type=%s, is_connected=%s",
        str(attribute),
        type(value).__name__,
        str(is_connected),
    )

    # Sanitize a value for logging
    def _sanitize_for_logging(input_val: Any, max_length: int = max_log_length) -> str:
        if not isinstance(input_val, str):
            try:
                input_val = str(input_val)
            except (TypeError, ValueError):
                return "<non-string>"

        # Allow alphanumeric, dash, underscore, and dot
        sanitized = re.sub(r"[^\w\-\.]", "", input_val)

        # Limit length
        if len(sanitized) > max_length:
            sanitized = sanitized[:max_length] + "..."

        return sanitized if sanitized else "<invalid>"

    # Create sanitized versions for logging
    sanitized_attr = _sanitize_for_logging(attribute) if sanitize_logs else str(attribute)
    sanitized_val = _sanitize_for_logging(value) if sanitize_logs else str(value)

    # Basic attribute validation - must be an integer
    if not isinstance(attribute, int):
        logger.debug(
            "validate_attribute_value: Attribute not an integer - type=%s", type(attribute).__name__
        )
        return (
            False,
            f"Attribute must be an integer, got {type(attribute).__name__}",
            sanitized_attr,
            sanitized_val,
        )

    # Define driver-level attributes that are supported
    supported_attributes = [
        ConstantsDDBC.SQL_ATTR_ACCESS_MODE.value,
        ConstantsDDBC.SQL_ATTR_CONNECTION_TIMEOUT.value,
        ConstantsDDBC.SQL_ATTR_CURRENT_CATALOG.value,
        ConstantsDDBC.SQL_ATTR_LOGIN_TIMEOUT.value,
        ConstantsDDBC.SQL_ATTR_PACKET_SIZE.value,
        ConstantsDDBC.SQL_ATTR_TXN_ISOLATION.value,
    ]

    # Check if attribute is supported
    if attribute not in supported_attributes:
        logger.debug("validate_attribute_value: Unsupported attribute - attr=%d", attribute)
        return (
            False,
            f"Unsupported attribute: {attribute}",
            sanitized_attr,
            sanitized_val,
        )

    # Check timing constraints for these specific attributes
    before_only_attributes = [
        ConstantsDDBC.SQL_ATTR_LOGIN_TIMEOUT.value,
        ConstantsDDBC.SQL_ATTR_PACKET_SIZE.value,
    ]

    # Check if attribute can be set at the current connection state
    if is_connected and attribute in before_only_attributes:
        logger.debug(
            "validate_attribute_value: Timing violation - attr=%d cannot be set after connection",
            attribute,
        )
        return (
            False,
            (
                f"Attribute {attribute} must be set before connection establishment. "
                "Use the attrs_before parameter when creating the connection."
            ),
            sanitized_attr,
            sanitized_val,
        )

    # Basic value type validation
    if isinstance(value, int):
        # For integer values, check if negative (login timeout can be -1 for default)
        if value < 0 and attribute != ConstantsDDBC.SQL_ATTR_LOGIN_TIMEOUT.value:
            return (
                False,
                f"Integer value cannot be negative: {value}",
                sanitized_attr,
                sanitized_val,
            )

    elif isinstance(value, str):
        # Basic string length check
        max_string_size = 8192  # 8KB maximum
        if len(value) > max_string_size:
            return (
                False,
                f"String value too large: {len(value)} bytes (max {max_string_size})",
                sanitized_attr,
                sanitized_val,
            )

    elif isinstance(value, (bytes, bytearray)):
        # Basic binary length check
        max_binary_size = 32768  # 32KB maximum
        if len(value) > max_binary_size:
            return (
                False,
                f"Binary value too large: {len(value)} bytes (max {max_binary_size})",
                sanitized_attr,
                sanitized_val,
            )

    else:
        # Reject unsupported value types
        return (
            False,
            f"Unsupported attribute value type: {type(value).__name__}",
            sanitized_attr,
            sanitized_val,
        )

    # All basic validations passed
    logger.debug(
        "validate_attribute_value: Validation passed - attr=%d, value_type=%s",
        attribute,
        type(value).__name__,
    )
    return True, None, sanitized_attr, sanitized_val


# Settings functionality moved here to avoid circular imports

# Initialize the locale setting only once at module import time
# This avoids thread-safety issues with locale
_default_decimal_separator: str = "."
try:
    # Get the locale setting once during module initialization
    locale_separator = locale.localeconv()["decimal_point"]
    if locale_separator and len(locale_separator) == 1:
        _default_decimal_separator = locale_separator
except (AttributeError, KeyError, TypeError, ValueError):
    pass  # Keep the default "." if locale access fails


class Settings:
    """
    Settings class for mssql_python package configuration.

    This class holds global settings that affect the behavior of the package,
    including lowercase column names, decimal separator.
    """

    def __init__(self) -> None:
        self.lowercase: bool = False
        # Use the pre-determined separator - no locale access here
        self.decimal_separator: str = _default_decimal_separator


# Global settings instance
_settings: Settings = Settings()
_settings_lock: threading.Lock = threading.Lock()


def get_settings() -> Settings:
    """Return the global settings object"""
    with _settings_lock:
        return _settings
