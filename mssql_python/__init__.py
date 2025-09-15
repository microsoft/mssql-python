"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
This module initializes the mssql_python package.
"""
import threading
# Exceptions
# https://www.python.org/dev/peps/pep-0249/#exceptions

# GLOBALS
# Read-Only
apilevel = "2.0"
paramstyle = "qmark"
threadsafety = 1

_settings_lock = threading.Lock()

# Create a settings object to hold configuration
class Settings:
    def __init__(self):
        self.lowercase = False

# Create a global settings instance
_settings = Settings()

# Define the get_settings function for internal use
def get_settings():
    """Return the global settings object"""
    with _settings_lock:
        _settings.lowercase = lowercase
        return _settings

# Expose lowercase as a regular module variable that users can access and set
lowercase = _settings.lowercase

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
