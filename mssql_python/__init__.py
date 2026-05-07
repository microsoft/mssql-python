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

# Import settings from helpers module
from .helpers import Settings, get_settings, _settings, _settings_lock

# Driver version
__version__ = "1.6.0"

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

# Row Objects
from .row import Row

# Logging Configuration (Simplified single-level DEBUG system)
from .logging import logger, setup_logging, driver_logger

# Constants
from .constants import ConstantsDDBC, GetInfoConstants, get_info_constants

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

# Create decimal separator control functions bound to our settings
from .decimal_config import create_decimal_separator_functions

setDecimalSeparator, getDecimalSeparator = create_decimal_separator_functions(_settings)

# Import module-level constants from constants module
from .constants import (  # noqa: F401
    # Enum classes
    AuthType,
    SQLTypes,
    # Helper function
    get_info_constants,
    # SQL Type constants (from ConstantsDDBC)
    SQL_CHAR,
    SQL_VARCHAR,
    SQL_LONGVARCHAR,
    SQL_WCHAR,
    SQL_WVARCHAR,
    SQL_WLONGVARCHAR,
    SQL_DECIMAL,
    SQL_NUMERIC,
    SQL_BIT,
    SQL_TINYINT,
    SQL_SMALLINT,
    SQL_INTEGER,
    SQL_BIGINT,
    SQL_REAL,
    SQL_FLOAT,
    SQL_DOUBLE,
    SQL_BINARY,
    SQL_VARBINARY,
    SQL_LONGVARBINARY,
    SQL_DATE,
    SQL_TIME,
    SQL_TIMESTAMP,
    SQL_TYPE_DATE,
    SQL_TYPE_TIME,
    SQL_TYPE_TIMESTAMP,
    SQL_GUID,
    SQL_XML,
    # Connection attribute constants
    SQL_ATTR_ACCESS_MODE,
    SQL_ATTR_CONNECTION_TIMEOUT,
    SQL_ATTR_CURRENT_CATALOG,
    SQL_ATTR_LOGIN_TIMEOUT,
    SQL_ATTR_PACKET_SIZE,
    SQL_ATTR_TXN_ISOLATION,
    # Transaction isolation levels
    SQL_TXN_READ_UNCOMMITTED,
    SQL_TXN_READ_COMMITTED,
    SQL_TXN_REPEATABLE_READ,
    SQL_TXN_SERIALIZABLE,
    # Access modes
    SQL_MODE_READ_WRITE,
    SQL_MODE_READ_ONLY,
    # Special constants
    SQL_WMETADATA,
    # GetInfoConstants (all exported as module-level constants)
    SQL_DRIVER_NAME,
    SQL_DRIVER_VER,
    SQL_DRIVER_ODBC_VER,
    SQL_DRIVER_HLIB,
    SQL_DRIVER_HENV,
    SQL_DRIVER_HDBC,
    SQL_DATA_SOURCE_NAME,
    SQL_DATABASE_NAME,
    SQL_SERVER_NAME,
    SQL_USER_NAME,
    SQL_SQL_CONFORMANCE,
    SQL_KEYWORDS,
    SQL_IDENTIFIER_CASE,
    SQL_IDENTIFIER_QUOTE_CHAR,
    SQL_SPECIAL_CHARACTERS,
    SQL_SQL92_ENTRY_SQL,
    SQL_SQL92_INTERMEDIATE_SQL,
    SQL_SQL92_FULL_SQL,
    SQL_SUBQUERIES,
    SQL_EXPRESSIONS_IN_ORDERBY,
    SQL_CORRELATION_NAME,
    SQL_SEARCH_PATTERN_ESCAPE,
    SQL_CATALOG_TERM,
    SQL_CATALOG_NAME_SEPARATOR,
    SQL_SCHEMA_TERM,
    SQL_TABLE_TERM,
    SQL_PROCEDURES,
    SQL_ACCESSIBLE_TABLES,
    SQL_ACCESSIBLE_PROCEDURES,
    SQL_CATALOG_NAME,
    SQL_CATALOG_USAGE,
    SQL_SCHEMA_USAGE,
    SQL_COLUMN_ALIAS,
    SQL_DESCRIBE_PARAMETER,
    SQL_TXN_CAPABLE,
    SQL_TXN_ISOLATION_OPTION,
    SQL_DEFAULT_TXN_ISOLATION,
    SQL_MULTIPLE_ACTIVE_TXN,
    SQL_TXN_ISOLATION_LEVEL,
    SQL_NUMERIC_FUNCTIONS,
    SQL_STRING_FUNCTIONS,
    SQL_DATETIME_FUNCTIONS,
    SQL_SYSTEM_FUNCTIONS,
    SQL_CONVERT_FUNCTIONS,
    SQL_LIKE_ESCAPE_CLAUSE,
    SQL_MAX_COLUMN_NAME_LEN,
    SQL_MAX_TABLE_NAME_LEN,
    SQL_MAX_SCHEMA_NAME_LEN,
    SQL_MAX_CATALOG_NAME_LEN,
    SQL_MAX_IDENTIFIER_LEN,
    SQL_MAX_STATEMENT_LEN,
    SQL_MAX_CHAR_LITERAL_LEN,
    SQL_MAX_BINARY_LITERAL_LEN,
    SQL_MAX_COLUMNS_IN_TABLE,
    SQL_MAX_COLUMNS_IN_SELECT,
    SQL_MAX_COLUMNS_IN_GROUP_BY,
    SQL_MAX_COLUMNS_IN_ORDER_BY,
    SQL_MAX_COLUMNS_IN_INDEX,
    SQL_MAX_TABLES_IN_SELECT,
    SQL_MAX_CONCURRENT_ACTIVITIES,
    SQL_MAX_DRIVER_CONNECTIONS,
    SQL_MAX_ROW_SIZE,
    SQL_MAX_USER_NAME_LEN,
    SQL_ACTIVE_CONNECTIONS,
    SQL_ACTIVE_STATEMENTS,
    SQL_DATA_SOURCE_READ_ONLY,
    SQL_NEED_LONG_DATA_LEN,
    SQL_GETDATA_EXTENSIONS,
    SQL_CURSOR_COMMIT_BEHAVIOR,
    SQL_CURSOR_ROLLBACK_BEHAVIOR,
    SQL_CURSOR_SENSITIVITY,
    SQL_BOOKMARK_PERSISTENCE,
    SQL_DYNAMIC_CURSOR_ATTRIBUTES1,
    SQL_DYNAMIC_CURSOR_ATTRIBUTES2,
    SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1,
    SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2,
    SQL_STATIC_CURSOR_ATTRIBUTES1,
    SQL_STATIC_CURSOR_ATTRIBUTES2,
    SQL_KEYSET_CURSOR_ATTRIBUTES1,
    SQL_KEYSET_CURSOR_ATTRIBUTES2,
    SQL_SCROLL_OPTIONS,
    SQL_SCROLL_CONCURRENCY,
    SQL_FETCH_DIRECTION,
    SQL_ROWSET_SIZE,
    SQL_CONCURRENCY,
    SQL_ROW_NUMBER,
    SQL_STATIC_SENSITIVITY,
    SQL_BATCH_SUPPORT,
    SQL_BATCH_ROW_COUNT,
    SQL_PARAM_ARRAY_ROW_COUNTS,
    SQL_PARAM_ARRAY_SELECTS,
    SQL_PROCEDURE_TERM,
    SQL_POSITIONED_STATEMENTS,
    SQL_GROUP_BY,
    SQL_OJ_CAPABILITIES,
    SQL_ORDER_BY_COLUMNS_IN_SELECT,
    SQL_OUTER_JOINS,
    SQL_QUOTED_IDENTIFIER_CASE,
    SQL_CONCAT_NULL_BEHAVIOR,
    SQL_NULL_COLLATION,
    SQL_ALTER_TABLE,
    SQL_UNION,
    SQL_DDL_INDEX,
    SQL_MULT_RESULT_SETS,
    SQL_OWNER_USAGE,
    SQL_QUALIFIER_USAGE,
    SQL_TIMEDATE_ADD_INTERVALS,
    SQL_TIMEDATE_DIFF_INTERVALS,
    SQL_IC_UPPER,
    SQL_IC_LOWER,
    SQL_IC_SENSITIVE,
    SQL_IC_MIXED,
)

__all__ = [
    # Exception classes
    "Warning",
    "Error",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
    "ConnectionStringParseError",
    # Type objects and functions
    "Date",
    "Time",
    "Timestamp",
    "DateFromTicks",
    "TimeFromTicks",
    "TimestampFromTicks",
    "Binary",
    "STRING",
    "BINARY",
    "NUMBER",
    "DATETIME",
    "ROWID",
    # Connection and cursor classes
    "connect",
    "Connection",
    "Cursor",
    "Row",
    # Settings
    "Settings",
    "get_settings",
    # Logging
    "logger",
    "setup_logging",
    "driver_logger",
    # Decimal functions
    "setDecimalSeparator",
    "getDecimalSeparator",
    # Pooling
    "pooling",
    "PoolingManager",
    # Constants - Enum classes
    "AuthType",
    "SQLTypes",
    "get_info_constants",
    # SQL Type constants
    "SQL_CHAR",
    "SQL_VARCHAR",
    "SQL_LONGVARCHAR",
    "SQL_WCHAR",
    "SQL_WVARCHAR",
    "SQL_WLONGVARCHAR",
    "SQL_DECIMAL",
    "SQL_NUMERIC",
    "SQL_BIT",
    "SQL_TINYINT",
    "SQL_SMALLINT",
    "SQL_INTEGER",
    "SQL_BIGINT",
    "SQL_REAL",
    "SQL_FLOAT",
    "SQL_DOUBLE",
    "SQL_BINARY",
    "SQL_VARBINARY",
    "SQL_LONGVARBINARY",
    "SQL_DATE",
    "SQL_TIME",
    "SQL_TIMESTAMP",
    "SQL_TYPE_DATE",
    "SQL_TYPE_TIME",
    "SQL_TYPE_TIMESTAMP",
    "SQL_GUID",
    "SQL_XML",
    # Connection attribute constants
    "SQL_ATTR_ACCESS_MODE",
    "SQL_ATTR_CONNECTION_TIMEOUT",
    "SQL_ATTR_CURRENT_CATALOG",
    "SQL_ATTR_LOGIN_TIMEOUT",
    "SQL_ATTR_PACKET_SIZE",
    "SQL_ATTR_TXN_ISOLATION",
    # Transaction isolation levels
    "SQL_TXN_READ_UNCOMMITTED",
    "SQL_TXN_READ_COMMITTED",
    "SQL_TXN_REPEATABLE_READ",
    "SQL_TXN_SERIALIZABLE",
    # Access modes
    "SQL_MODE_READ_WRITE",
    "SQL_MODE_READ_ONLY",
    # Special constants
    "SQL_WMETADATA",
    # GetInfo constants
    "SQL_DRIVER_NAME",
    "SQL_DRIVER_VER",
    "SQL_DRIVER_ODBC_VER",
    "SQL_DRIVER_HLIB",
    "SQL_DRIVER_HENV",
    "SQL_DRIVER_HDBC",
    "SQL_DATA_SOURCE_NAME",
    "SQL_DATABASE_NAME",
    "SQL_SERVER_NAME",
    "SQL_USER_NAME",
    "SQL_SQL_CONFORMANCE",
    "SQL_KEYWORDS",
    "SQL_IDENTIFIER_CASE",
    "SQL_IDENTIFIER_QUOTE_CHAR",
    "SQL_SPECIAL_CHARACTERS",
    "SQL_SQL92_ENTRY_SQL",
    "SQL_SQL92_INTERMEDIATE_SQL",
    "SQL_SQL92_FULL_SQL",
    "SQL_SUBQUERIES",
    "SQL_EXPRESSIONS_IN_ORDERBY",
    "SQL_CORRELATION_NAME",
    "SQL_SEARCH_PATTERN_ESCAPE",
    "SQL_CATALOG_TERM",
    "SQL_CATALOG_NAME_SEPARATOR",
    "SQL_SCHEMA_TERM",
    "SQL_TABLE_TERM",
    "SQL_PROCEDURES",
    "SQL_ACCESSIBLE_TABLES",
    "SQL_ACCESSIBLE_PROCEDURES",
    "SQL_CATALOG_NAME",
    "SQL_CATALOG_USAGE",
    "SQL_SCHEMA_USAGE",
    "SQL_COLUMN_ALIAS",
    "SQL_DESCRIBE_PARAMETER",
    "SQL_TXN_CAPABLE",
    "SQL_TXN_ISOLATION_OPTION",
    "SQL_DEFAULT_TXN_ISOLATION",
    "SQL_MULTIPLE_ACTIVE_TXN",
    "SQL_TXN_ISOLATION_LEVEL",
    "SQL_NUMERIC_FUNCTIONS",
    "SQL_STRING_FUNCTIONS",
    "SQL_DATETIME_FUNCTIONS",
    "SQL_SYSTEM_FUNCTIONS",
    "SQL_CONVERT_FUNCTIONS",
    "SQL_LIKE_ESCAPE_CLAUSE",
    "SQL_MAX_COLUMN_NAME_LEN",
    "SQL_MAX_TABLE_NAME_LEN",
    "SQL_MAX_SCHEMA_NAME_LEN",
    "SQL_MAX_CATALOG_NAME_LEN",
    "SQL_MAX_IDENTIFIER_LEN",
    "SQL_MAX_STATEMENT_LEN",
    "SQL_MAX_CHAR_LITERAL_LEN",
    "SQL_MAX_BINARY_LITERAL_LEN",
    "SQL_MAX_COLUMNS_IN_TABLE",
    "SQL_MAX_COLUMNS_IN_SELECT",
    "SQL_MAX_COLUMNS_IN_GROUP_BY",
    "SQL_MAX_COLUMNS_IN_ORDER_BY",
    "SQL_MAX_COLUMNS_IN_INDEX",
    "SQL_MAX_TABLES_IN_SELECT",
    "SQL_MAX_CONCURRENT_ACTIVITIES",
    "SQL_MAX_DRIVER_CONNECTIONS",
    "SQL_MAX_ROW_SIZE",
    "SQL_MAX_USER_NAME_LEN",
    "SQL_ACTIVE_CONNECTIONS",
    "SQL_ACTIVE_STATEMENTS",
    "SQL_DATA_SOURCE_READ_ONLY",
    "SQL_NEED_LONG_DATA_LEN",
    "SQL_GETDATA_EXTENSIONS",
    "SQL_CURSOR_COMMIT_BEHAVIOR",
    "SQL_CURSOR_ROLLBACK_BEHAVIOR",
    "SQL_CURSOR_SENSITIVITY",
    "SQL_BOOKMARK_PERSISTENCE",
    "SQL_DYNAMIC_CURSOR_ATTRIBUTES1",
    "SQL_DYNAMIC_CURSOR_ATTRIBUTES2",
    "SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1",
    "SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2",
    "SQL_STATIC_CURSOR_ATTRIBUTES1",
    "SQL_STATIC_CURSOR_ATTRIBUTES2",
    "SQL_KEYSET_CURSOR_ATTRIBUTES1",
    "SQL_KEYSET_CURSOR_ATTRIBUTES2",
    "SQL_SCROLL_OPTIONS",
    "SQL_SCROLL_CONCURRENCY",
    "SQL_FETCH_DIRECTION",
    "SQL_ROWSET_SIZE",
    "SQL_CONCURRENCY",
    "SQL_ROW_NUMBER",
    "SQL_STATIC_SENSITIVITY",
    "SQL_BATCH_SUPPORT",
    "SQL_BATCH_ROW_COUNT",
    "SQL_PARAM_ARRAY_ROW_COUNTS",
    "SQL_PARAM_ARRAY_SELECTS",
    "SQL_PROCEDURE_TERM",
    "SQL_POSITIONED_STATEMENTS",
    "SQL_GROUP_BY",
    "SQL_OJ_CAPABILITIES",
    "SQL_ORDER_BY_COLUMNS_IN_SELECT",
    "SQL_OUTER_JOINS",
    "SQL_QUOTED_IDENTIFIER_CASE",
    "SQL_CONCAT_NULL_BEHAVIOR",
    "SQL_NULL_COLLATION",
    "SQL_ALTER_TABLE",
    "SQL_UNION",
    "SQL_DDL_INDEX",
    "SQL_MULT_RESULT_SETS",
    "SQL_OWNER_USAGE",
    "SQL_QUALIFIER_USAGE",
    "SQL_TIMEDATE_ADD_INTERVALS",
    "SQL_TIMEDATE_DIFF_INTERVALS",
    "SQL_IC_UPPER",
    "SQL_IC_LOWER",
    "SQL_IC_SENSITIVE",
    "SQL_IC_MIXED",
    # API level globals
    "apilevel",
    "paramstyle",
    "threadsafety",
    # Module properties
    "lowercase",
    "native_uuid",
]


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

    @property
    def native_uuid(self) -> bool:
        """Get the native_uuid setting.

        Controls whether UNIQUEIDENTIFIER columns return uuid.UUID objects (True)
        or str (False). Default is True.
        Set to False to return str for pyodbc-compatible migration.
        """
        return _settings.native_uuid

    @native_uuid.setter
    def native_uuid(self, value: bool) -> None:
        """Set the native_uuid setting."""
        if not isinstance(value, bool):
            raise ValueError("native_uuid must be a boolean value")
        with _settings_lock:
            _settings.native_uuid = value


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
native_uuid: bool = _settings.native_uuid
