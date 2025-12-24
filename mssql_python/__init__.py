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

# Import settings from helpers module
from .helpers import Settings, get_settings, _settings, _settings_lock

# Driver version
__version__ = "1.1.0"

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

# Import all module-level constants from constants module
from .constants import *  # noqa: F401, F403


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
