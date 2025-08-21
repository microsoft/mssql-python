"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
This module initializes the mssql_python package.
"""

# Exceptions
# https://www.python.org/dev/peps/pep-0249/#exceptions

# GLOBALS
# Read-Only
apilevel = "2.0"
paramstyle = "qmark"
threadsafety = 1

class Settings:
    def __init__(self):
        self.lowercase = False
        self.decimal_separator = "."

# Global settings instance
_settings = Settings()

def get_settings():
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
    
    The default is "." (period). This function overrides the default.
    
    Args:
        separator (str): The character to use as decimal separator
    """
    if not isinstance(separator, str) or len(separator) != 1:
        raise ValueError("Decimal separator must be a single character string")
    
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
