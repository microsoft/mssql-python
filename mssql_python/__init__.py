"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
This module initializes the mssql_python package.
"""

# Exceptions
# https://www.python.org/dev/peps/pep-0249/#exceptions
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

# Export connection attribute constants for set_attr()
# NOTE: Some attributes are only supported when using an ODBC Driver Manager.
# Attributes marked with [NO-OP] are not supported directly by the SQL Server ODBC driver
# and will have no effect in this implementation.

SQL_ATTR_ACCESS_MODE = ConstantsDDBC.SQL_ATTR_ACCESS_MODE.value
SQL_ATTR_AUTOCOMMIT = ConstantsDDBC.SQL_ATTR_AUTOCOMMIT.value
SQL_ATTR_CONNECTION_TIMEOUT = ConstantsDDBC.SQL_ATTR_CONNECTION_TIMEOUT.value
SQL_ATTR_CURRENT_CATALOG = ConstantsDDBC.SQL_ATTR_CURRENT_CATALOG.value
SQL_ATTR_LOGIN_TIMEOUT = ConstantsDDBC.SQL_ATTR_LOGIN_TIMEOUT.value
SQL_ATTR_ODBC_CURSORS = ConstantsDDBC.SQL_ATTR_ODBC_CURSORS.value
SQL_ATTR_PACKET_SIZE = ConstantsDDBC.SQL_ATTR_PACKET_SIZE.value
SQL_ATTR_TXN_ISOLATION = ConstantsDDBC.SQL_ATTR_TXN_ISOLATION.value

# The following attributes are [NO-OP] in this implementation (require Driver Manager):
# SQL_ATTR_QUIET_MODE
# SQL_ATTR_TRACE
# SQL_ATTR_TRACEFILE
# SQL_ATTR_TRANSLATE_LIB
# SQL_ATTR_TRANSLATE_OPTION
# SQL_ATTR_CONNECTION_POOLING
# SQL_ATTR_CP_MATCH
# SQL_ATTR_ASYNC_ENABLE
# SQL_ATTR_ENLIST_IN_DTC
# SQL_ATTR_ENLIST_IN_XA
# SQL_ATTR_CONNECTION_DEAD
# SQL_ATTR_ASYNC_DBC_FUNCTIONS_ENABLE
# SQL_ATTR_ASYNC_DBC_EVENT
# SQL_ATTR_SERVER_NAME
# SQL_ATTR_RESET_CONNECTION
# SQL_RESET_CONNECTION_YES

# Transaction Isolation Level Constants
SQL_TXN_READ_UNCOMMITTED = ConstantsDDBC.SQL_TXN_READ_UNCOMMITTED.value
SQL_TXN_READ_COMMITTED = ConstantsDDBC.SQL_TXN_READ_COMMITTED.value
SQL_TXN_REPEATABLE_READ = ConstantsDDBC.SQL_TXN_REPEATABLE_READ.value
SQL_TXN_SERIALIZABLE = ConstantsDDBC.SQL_TXN_SERIALIZABLE.value

# Access Mode Constants
SQL_MODE_READ_WRITE = ConstantsDDBC.SQL_MODE_READ_WRITE.value
SQL_MODE_READ_ONLY = ConstantsDDBC.SQL_MODE_READ_ONLY.value

# ODBC Cursors Constants
SQL_CUR_USE_IF_NEEDED = ConstantsDDBC.SQL_CUR_USE_IF_NEEDED.value
SQL_CUR_USE_ODBC = ConstantsDDBC.SQL_CUR_USE_ODBC.value
SQL_CUR_USE_DRIVER = ConstantsDDBC.SQL_CUR_USE_DRIVER.value


# GLOBALS
# Read-Only
apilevel = "2.0"
paramstyle = "qmark"
threadsafety = 1

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
