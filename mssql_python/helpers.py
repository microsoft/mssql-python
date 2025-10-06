"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
This module provides helper functions for the mssql_python package.
"""

from mssql_python import ddbc_bindings
from mssql_python.exceptions import raise_exception
from mssql_python.logging_config import get_logger
import re
from mssql_python.constants import ConstantsDDBC
from mssql_python.ddbc_bindings import normalize_architecture

logger = get_logger()


def add_driver_to_connection_str(connection_str):
    """
    Add the DDBC driver to the connection string if not present.

    Args:
        connection_str (str): The original connection string.

    Returns:
        str: The connection string with the DDBC driver added.

    Raises:
        Exception: If the connection string is invalid.
    """
    driver_name = "Driver={ODBC Driver 18 for SQL Server}"
    try:
        # Strip any leading or trailing whitespace from the connection string
        connection_str = connection_str.strip()
        connection_str = add_driver_name_to_app_parameter(connection_str)

        # Split the connection string into individual attributes
        connection_attributes = connection_str.split(";")
        final_connection_attributes = []

        # Iterate through the attributes and exclude any existing driver attribute
        for attribute in connection_attributes:
            if attribute.lower().split("=")[0] == "driver":
                continue
            final_connection_attributes.append(attribute)

        # Join the remaining attributes back into a connection string
        connection_str = ";".join(final_connection_attributes)

        # Insert the driver attribute at the beginning of the connection string
        final_connection_attributes.insert(0, driver_name)
        connection_str = ";".join(final_connection_attributes)

    except Exception as e:
        raise Exception(
            "Invalid connection string, Please follow the format: "
            "Server=server_name;Database=database_name;UID=user_name;PWD=password"
        ) from e

    return connection_str


def check_error(handle_type, handle, ret):
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
        error_info = ddbc_bindings.DDBCSQLCheckError(handle_type, handle, ret)
        if logger:
            logger.error("Error: %s", error_info.ddbcErrorMsg)
        raise_exception(error_info.sqlState, error_info.ddbcErrorMsg)


def add_driver_name_to_app_parameter(connection_string):
    """
    Modifies the input connection string by appending the APP name.

    Args:
        connection_string (str): The input connection string.

    Returns:
        str: The modified connection string.
    """
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
        else:
            # Keep other parameters as is
            modified_parameters.append(param)

    # If APP key is not found, append it
    if not app_found:
        modified_parameters.append("APP=MSSQL-Python")

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
    # Remove sensitive information from the connection string, Pwd section
    # Replace Pwd=...; or Pwd=... (end of string) with Pwd=***;
    import re
    return re.sub(r"(Pwd\s*=\s*)[^;]*", r"\1***", conn_str, flags=re.IGNORECASE)


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
    if not isinstance(user_input, str):
        return "<non-string>"
    
    # Remove control characters and non-printable characters
    import re
    # Allow alphanumeric, dash, underscore, and dot (common in encoding names)
    sanitized = re.sub(r'[^\w\-\.]', '', user_input)
    
    # Limit length to prevent log flooding
    if len(sanitized) > max_length:
        sanitized = sanitized[:max_length] + "..."
    
    # Return placeholder if nothing remains after sanitization
    return sanitized if sanitized else "<invalid>"

def validate_attribute_value(attribute, value, is_connected=True, sanitize_logs=True, max_log_length=50):
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
    # Sanitize a value for logging
    def _sanitize_for_logging(input_val, max_length=max_log_length):
        if not isinstance(input_val, str):
            try:
                input_val = str(input_val)
            except:
                return "<non-string>"
        
        # Allow alphanumeric, dash, underscore, and dot
        sanitized = re.sub(r'[^\w\-\.]', '', input_val)
        
        # Limit length
        if len(sanitized) > max_length:
            sanitized = sanitized[:max_length] + "..."
        
        return sanitized if sanitized else "<invalid>"
    
    # Create sanitized versions for logging
    sanitized_attr = _sanitize_for_logging(attribute) if sanitize_logs else str(attribute)
    sanitized_val = _sanitize_for_logging(value) if sanitize_logs else str(value)
    
    # Basic attribute validation - must be an integer
    if not isinstance(attribute, int):
        return False, f"Attribute must be an integer, got {type(attribute).__name__}", sanitized_attr, sanitized_val
    
    # Define driver-level attributes that are supported
    SUPPORTED_ATTRIBUTES = [
        ConstantsDDBC.SQL_ATTR_ACCESS_MODE.value,
        ConstantsDDBC.SQL_ATTR_CONNECTION_TIMEOUT.value,
        ConstantsDDBC.SQL_ATTR_CURRENT_CATALOG.value,
        ConstantsDDBC.SQL_ATTR_LOGIN_TIMEOUT.value,
        ConstantsDDBC.SQL_ATTR_PACKET_SIZE.value,
        ConstantsDDBC.SQL_ATTR_TXN_ISOLATION.value
    ]
    
    # Check if attribute is supported
    if attribute not in SUPPORTED_ATTRIBUTES:
        return False, f"Unsupported attribute: {attribute}", sanitized_attr, sanitized_val
    
    # Check timing constraints for these specific attributes
    BEFORE_ONLY_ATTRIBUTES = [
        ConstantsDDBC.SQL_ATTR_LOGIN_TIMEOUT.value,
        ConstantsDDBC.SQL_ATTR_PACKET_SIZE.value
    ]
    
    # Check if attribute can be set at the current connection state
    if is_connected and attribute in BEFORE_ONLY_ATTRIBUTES:
        return False, (f"Attribute {attribute} must be set before connection establishment. "
                      "Use the attrs_before parameter when creating the connection."), sanitized_attr, sanitized_val
    
    # Basic value type validation
    if isinstance(value, int):
        # For integer values, check if negative (login timeout can be -1 for default)
        if value < 0 and attribute != ConstantsDDBC.SQL_ATTR_LOGIN_TIMEOUT.value:
            return False, f"Integer value cannot be negative: {value}", sanitized_attr, sanitized_val
    
    elif isinstance(value, str):
        # Basic string length check
        MAX_STRING_SIZE = 8192  # 8KB maximum
        if len(value) > MAX_STRING_SIZE:
            return False, f"String value too large: {len(value)} bytes (max {MAX_STRING_SIZE})", sanitized_attr, sanitized_val
    
    elif isinstance(value, (bytes, bytearray)):
        # Basic binary length check
        MAX_BINARY_SIZE = 32768  # 32KB maximum
        if len(value) > MAX_BINARY_SIZE:
            return False, f"Binary value too large: {len(value)} bytes (max {MAX_BINARY_SIZE})", sanitized_attr, sanitized_val
    
    else:
        # Reject unsupported value types
        return False, f"Unsupported attribute value type: {type(value).__name__}", sanitized_attr, sanitized_val
    
    # All basic validations passed
    return True, None, sanitized_attr, sanitized_val

def log(level: str, message: str, *args) -> None:
    """
    Universal logging helper that gets a fresh logger instance.
    
    Args:
        level: Log level ('debug', 'info', 'warning', 'error')
        message: Log message with optional format placeholders
        *args: Arguments for message formatting
    """
    logger = get_logger()
    if logger:
        getattr(logger, level)(message, *args)