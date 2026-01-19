"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
This module initializes the mssql_python package.
"""

import atexit
import sys
import threading
import types
import weakref
from typing import Dict

# Import settings from helpers to avoid circular imports
from .helpers import Settings, get_settings, _settings, _settings_lock

# Driver version
__version__ = "1.2.0"

# Exceptions
# https://www.python.org/dev/peps/pep-0249/#exceptions

# Import necessary modules
from .exceptions import (
    Warning,
    Error,
    InterfaceError,
    DatabaseError,
    DataError,
    OperationalError,
    IntegrityError,
    InternalError,
    ProgrammingError,
    NotSupportedError,
    ConnectionStringParseError,
)

# Type Objects
from .type import (
    Date,
    Time,
    Timestamp,
    DateFromTicks,
    TimeFromTicks,
    TimestampFromTicks,
    Binary,
    STRING,
    BINARY,
    NUMBER,
    DATETIME,
    ROWID,
)

# Connection Objects
from .db_connection import connect, Connection

# Connection String Handling
from .connection_string_parser import _ConnectionStringParser
from .connection_string_builder import _ConnectionStringBuilder

# Cursor Objects
from .cursor import Cursor

# Logging Configuration (Simplified single-level DEBUG system)
from .logging import logger, setup_logging, driver_logger

# Constants
from .constants import ConstantsDDBC, GetInfoConstants

# Pooling
from .pooling import PoolingManager

# Global registry for tracking active connections (using weak references)
_active_connections = weakref.WeakSet()
_connections_lock = threading.Lock()


def _register_connection(conn):
    """Register a connection for cleanup before shutdown."""
    with _connections_lock:
        _active_connections.add(conn)


def _cleanup_connections():
    """
    Cleanup function called by atexit to close all active connections.

    This prevents resource leaks during interpreter shutdown by ensuring
    all ODBC handles are freed in the correct order before Python finalizes.
    """
    # Make a copy of the connections to avoid modification during iteration
    with _connections_lock:
        connections_to_close = list(_active_connections)

    for conn in connections_to_close:
        try:
            # Check if connection is still valid and not closed
            if hasattr(conn, "_closed") and not conn._closed:
                # Close will handle both cursors and the connection
                conn.close()
        except Exception as e:
            # Log errors during shutdown cleanup for debugging
            # We're prioritizing crash prevention over error propagation
            try:
                driver_logger.error(
                    f"Error during connection cleanup at shutdown: {type(e).__name__}: {e}"
                )
            except Exception:
                # If logging fails during shutdown, silently ignore
                pass


# Register cleanup function to run before Python exits
atexit.register(_cleanup_connections)

# GLOBALS
# Read-Only
apilevel: str = "2.0"
paramstyle: str = "pyformat"
threadsafety: int = 1

# Set the initial decimal separator in C++
try:
    from .ddbc_bindings import DDBCSetDecimalSeparator

    DDBCSetDecimalSeparator(_settings.decimal_separator)
except ImportError:
    # Handle case where ddbc_bindings is not available
    DDBCSetDecimalSeparator = None


# New functions for decimal separator control
def setDecimalSeparator(separator: str) -> None:
    """
    Sets the decimal separator character used when parsing NUMERIC/DECIMAL values
    from the database, e.g. the "." in "1,234.56".

    The default is to use the current locale's "decimal_point" value when the module
    was first imported, or "." if the locale is not available. This function overrides
    the default.

    Args:
        separator (str): The character to use as decimal separator

    Raises:
        ValueError: If the separator is not a single character string
    """
    # Type validation
    if not isinstance(separator, str):
        raise ValueError("Decimal separator must be a string")

    # Length validation
    if len(separator) == 0:
        raise ValueError("Decimal separator cannot be empty")

    if len(separator) > 1:
        raise ValueError("Decimal separator must be a single character")

    # Character validation
    if separator.isspace():
        raise ValueError("Whitespace characters are not allowed as decimal separators")

    # Check for specific disallowed characters
    if separator in ["\t", "\n", "\r", "\v", "\f"]:
        raise ValueError(
            f"Control character '{repr(separator)}' is not allowed as a decimal separator"
        )

    # Set in Python side settings
    _settings.decimal_separator = separator

    # Update the C++ side
    if DDBCSetDecimalSeparator is not None:
        DDBCSetDecimalSeparator(separator)


def getDecimalSeparator() -> str:
    """
    Returns the decimal separator character used when parsing NUMERIC/DECIMAL values
    from the database.

    Returns:
        str: The current decimal separator character
    """
    return _settings.decimal_separator


# Export specific constants for setencoding()
SQL_CHAR: int = ConstantsDDBC.SQL_CHAR.value
SQL_WCHAR: int = ConstantsDDBC.SQL_WCHAR.value
SQL_WMETADATA: int = -99

# Export connection attribute constants for set_attr()
# Only include driver-level attributes that the SQL Server ODBC driver can handle directly

# Core driver-level attributes
SQL_ATTR_ACCESS_MODE: int = ConstantsDDBC.SQL_ATTR_ACCESS_MODE.value
SQL_ATTR_CONNECTION_TIMEOUT: int = ConstantsDDBC.SQL_ATTR_CONNECTION_TIMEOUT.value
SQL_ATTR_CURRENT_CATALOG: int = ConstantsDDBC.SQL_ATTR_CURRENT_CATALOG.value
SQL_ATTR_LOGIN_TIMEOUT: int = ConstantsDDBC.SQL_ATTR_LOGIN_TIMEOUT.value
SQL_ATTR_PACKET_SIZE: int = ConstantsDDBC.SQL_ATTR_PACKET_SIZE.value
SQL_ATTR_TXN_ISOLATION: int = ConstantsDDBC.SQL_ATTR_TXN_ISOLATION.value

# Transaction Isolation Level Constants
SQL_TXN_READ_UNCOMMITTED: int = ConstantsDDBC.SQL_TXN_READ_UNCOMMITTED.value
SQL_TXN_READ_COMMITTED: int = ConstantsDDBC.SQL_TXN_READ_COMMITTED.value
SQL_TXN_REPEATABLE_READ: int = ConstantsDDBC.SQL_TXN_REPEATABLE_READ.value
SQL_TXN_SERIALIZABLE: int = ConstantsDDBC.SQL_TXN_SERIALIZABLE.value

# Access Mode Constants
SQL_MODE_READ_WRITE: int = ConstantsDDBC.SQL_MODE_READ_WRITE.value
SQL_MODE_READ_ONLY: int = ConstantsDDBC.SQL_MODE_READ_ONLY.value


def pooling(max_size: int = 100, idle_timeout: int = 600, enabled: bool = True) -> None:
    """
    Enable connection pooling with the specified parameters.
    By default:
        - If not explicitly called, pooling will be auto-enabled with default values.

    Args:
        max_size (int): Maximum number of connections in the pool.
        idle_timeout (int): Time in seconds before idle connections are closed.
        enabled (bool): Whether to enable or disable pooling.

    Returns:
        None
    """
    if not enabled:
        PoolingManager.disable()
    else:
        PoolingManager.enable(max_size, idle_timeout)


_original_module_setattr = sys.modules[__name__].__setattr__


def _custom_setattr(name, value):
    if name == "lowercase":
        with _settings_lock:
            _settings.lowercase = bool(value)
            # Update the module's lowercase variable
            _original_module_setattr(name, _settings.lowercase)
    else:
        _original_module_setattr(name, value)


# Replace the module's __setattr__ with our custom version
sys.modules[__name__].__setattr__ = _custom_setattr


# Export SQL constants at module level
SQL_VARCHAR: int = ConstantsDDBC.SQL_VARCHAR.value
SQL_LONGVARCHAR: int = ConstantsDDBC.SQL_LONGVARCHAR.value
SQL_WVARCHAR: int = ConstantsDDBC.SQL_WVARCHAR.value
SQL_WLONGVARCHAR: int = ConstantsDDBC.SQL_WLONGVARCHAR.value
SQL_DECIMAL: int = ConstantsDDBC.SQL_DECIMAL.value
SQL_NUMERIC: int = ConstantsDDBC.SQL_NUMERIC.value
SQL_BIT: int = ConstantsDDBC.SQL_BIT.value
SQL_TINYINT: int = ConstantsDDBC.SQL_TINYINT.value
SQL_SMALLINT: int = ConstantsDDBC.SQL_SMALLINT.value
SQL_INTEGER: int = ConstantsDDBC.SQL_INTEGER.value
SQL_BIGINT: int = ConstantsDDBC.SQL_BIGINT.value
SQL_REAL: int = ConstantsDDBC.SQL_REAL.value
SQL_FLOAT: int = ConstantsDDBC.SQL_FLOAT.value
SQL_DOUBLE: int = ConstantsDDBC.SQL_DOUBLE.value
SQL_BINARY: int = ConstantsDDBC.SQL_BINARY.value
SQL_VARBINARY: int = ConstantsDDBC.SQL_VARBINARY.value
SQL_LONGVARBINARY: int = ConstantsDDBC.SQL_LONGVARBINARY.value
SQL_DATE: int = ConstantsDDBC.SQL_DATE.value
SQL_TIME: int = ConstantsDDBC.SQL_TIME.value
SQL_TIMESTAMP: int = ConstantsDDBC.SQL_TIMESTAMP.value

# Export GetInfo constants at module level
# Driver and database information
SQL_DRIVER_NAME: int = GetInfoConstants.SQL_DRIVER_NAME.value
SQL_DRIVER_VER: int = GetInfoConstants.SQL_DRIVER_VER.value
SQL_DRIVER_ODBC_VER: int = GetInfoConstants.SQL_DRIVER_ODBC_VER.value
SQL_DATA_SOURCE_NAME: int = GetInfoConstants.SQL_DATA_SOURCE_NAME.value
SQL_DATABASE_NAME: int = GetInfoConstants.SQL_DATABASE_NAME.value
SQL_SERVER_NAME: int = GetInfoConstants.SQL_SERVER_NAME.value
SQL_USER_NAME: int = GetInfoConstants.SQL_USER_NAME.value

# SQL conformance and support
SQL_SQL_CONFORMANCE: int = GetInfoConstants.SQL_SQL_CONFORMANCE.value
SQL_KEYWORDS: int = GetInfoConstants.SQL_KEYWORDS.value
SQL_IDENTIFIER_QUOTE_CHAR: int = GetInfoConstants.SQL_IDENTIFIER_QUOTE_CHAR.value
SQL_SEARCH_PATTERN_ESCAPE: int = GetInfoConstants.SQL_SEARCH_PATTERN_ESCAPE.value

# Catalog and schema support
SQL_CATALOG_TERM: int = GetInfoConstants.SQL_CATALOG_TERM.value
SQL_SCHEMA_TERM: int = GetInfoConstants.SQL_SCHEMA_TERM.value
SQL_TABLE_TERM: int = GetInfoConstants.SQL_TABLE_TERM.value
SQL_PROCEDURE_TERM: int = GetInfoConstants.SQL_PROCEDURE_TERM.value

# Transaction support
SQL_TXN_CAPABLE: int = GetInfoConstants.SQL_TXN_CAPABLE.value
SQL_DEFAULT_TXN_ISOLATION: int = GetInfoConstants.SQL_DEFAULT_TXN_ISOLATION.value

# Data type support
SQL_NUMERIC_FUNCTIONS: int = GetInfoConstants.SQL_NUMERIC_FUNCTIONS.value
SQL_STRING_FUNCTIONS: int = GetInfoConstants.SQL_STRING_FUNCTIONS.value
SQL_DATETIME_FUNCTIONS: int = GetInfoConstants.SQL_DATETIME_FUNCTIONS.value

# Limits
SQL_MAX_COLUMN_NAME_LEN: int = GetInfoConstants.SQL_MAX_COLUMN_NAME_LEN.value
SQL_MAX_TABLE_NAME_LEN: int = GetInfoConstants.SQL_MAX_TABLE_NAME_LEN.value
SQL_MAX_SCHEMA_NAME_LEN: int = GetInfoConstants.SQL_MAX_SCHEMA_NAME_LEN.value
SQL_MAX_CATALOG_NAME_LEN: int = GetInfoConstants.SQL_MAX_CATALOG_NAME_LEN.value
SQL_MAX_IDENTIFIER_LEN: int = GetInfoConstants.SQL_MAX_IDENTIFIER_LEN.value


# Also provide a function to get all constants
def get_info_constants() -> Dict[str, int]:
    """
    Returns a dictionary of all available GetInfo constants.

    This provides all SQLGetInfo constants that can be used with the Connection.getinfo() method
    to retrieve metadata about the database server and driver.

    Returns:
        dict: Dictionary mapping constant names to their integer values
    """
    return {name: member.value for name, member in GetInfoConstants.__members__.items()}


# Create a custom module class that uses properties instead of __setattr__
class _MSSQLModule(types.ModuleType):
    @property
    def lowercase(self) -> bool:
        """Get the lowercase setting."""
        return _settings.lowercase

    @lowercase.setter
    def lowercase(self, value: bool) -> None:
        """Set the lowercase setting."""
        if not isinstance(value, bool):
            raise ValueError("lowercase must be a boolean value")
        with _settings_lock:
            _settings.lowercase = value


# Replace the current module with our custom module class
old_module: types.ModuleType = sys.modules[__name__]
new_module: _MSSQLModule = _MSSQLModule(__name__)

# Copy all existing attributes to the new module
for attr_name in dir(old_module):
    if attr_name != "__class__":
        try:
            setattr(new_module, attr_name, getattr(old_module, attr_name))
        except AttributeError:
            pass

# Replace the module in sys.modules
sys.modules[__name__] = new_module

# Initialize property values
lowercase: bool = _settings.lowercase
