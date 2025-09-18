"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
This module initializes the mssql_python package.
"""
import threading
import locale

# Exceptions
# https://www.python.org/dev/peps/pep-0249/#exceptions

# GLOBALS
# Read-Only
apilevel = "2.0"
paramstyle = "qmark"
threadsafety = 1

# Initialize the locale setting only once at module import time
# This avoids thread-safety issues with locale
_DEFAULT_DECIMAL_SEPARATOR = "."
try:
    # Get the locale setting once during module initialization
    _locale_separator = locale.localeconv()['decimal_point']
    if _locale_separator and len(_locale_separator) == 1:
        _DEFAULT_DECIMAL_SEPARATOR = _locale_separator
except (AttributeError, KeyError, TypeError, ValueError):
    pass  # Keep the default "." if locale access fails

class Settings:
    def __init__(self):
        self.lowercase = False
        # Use the pre-determined separator - no locale access here
        self.decimal_separator = _DEFAULT_DECIMAL_SEPARATOR

# Global settings instance
_settings = Settings()
_settings_lock = threading.Lock()

def get_settings():
    """Return the global settings object"""
    with _settings_lock:
        _settings.lowercase = lowercase
        return _settings

lowercase = _settings.lowercase  # Default is False

# Set the initial decimal separator in C++
from .ddbc_bindings import DDBCSetDecimalSeparator
DDBCSetDecimalSeparator(_settings.decimal_separator)

# New functions for decimal separator control
def setDecimalSeparator(separator):
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
    if separator in ['\t', '\n', '\r', '\v', '\f']:
        raise ValueError(f"Control character '{repr(separator)}' is not allowed as a decimal separator")
    
    # Set in Python side settings
    _settings.decimal_separator = separator
    
    # Update the C++ side
    from .ddbc_bindings import DDBCSetDecimalSeparator
    DDBCSetDecimalSeparator(separator)

def getDecimalSeparator():
    """
    Returns the decimal separator character used when parsing NUMERIC/DECIMAL values
    from the database.
    
    Returns:
        str: The current decimal separator character
    """
    return _settings.decimal_separator

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

# Cursor Objects
from .cursor import Cursor

# Logging Configuration
from .logging_config import setup_logging, get_logger

# Constants
from .constants import ConstantsDDBC

# Export specific constants for setencoding()
SQL_CHAR = ConstantsDDBC.SQL_CHAR.value
SQL_WCHAR = ConstantsDDBC.SQL_WCHAR.value
SQL_WMETADATA = -99

from .pooling import PoolingManager
def pooling(max_size=100, idle_timeout=600, enabled=True):
#     """
#     Enable connection pooling with the specified parameters.
#     By default:
#         - If not explicitly called, pooling will be auto-enabled with default values.

#     Args:
#         max_size (int): Maximum number of connections in the pool.
#         idle_timeout (int): Time in seconds before idle connections are closed.
    
#     Returns:
#         None
#     """
    if not enabled:
        PoolingManager.disable()
    else:
        PoolingManager.enable(max_size, idle_timeout)

import sys
_original_module_setattr = sys.modules[__name__].__setattr__

def _custom_setattr(name, value):
    if name == 'lowercase':
        with _settings_lock:
            _settings.lowercase = bool(value)
            # Update the module's lowercase variable
            _original_module_setattr(name, _settings.lowercase)
    else:
        _original_module_setattr(name, value)

# Replace the module's __setattr__ with our custom version
sys.modules[__name__].__setattr__ = _custom_setattr
