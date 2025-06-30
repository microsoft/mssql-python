"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
This module initializes the mssql_python package.
"""

# Import for pooling functionality
from .pooling import PoolingManager

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

# BCP
from .bcp_options import BCPOptions, ColumnFormat
from .bcp_main import BCPClient

# Logging Configuration
from .logging_config import setup_logging, get_logger

# Constants
from .constants import ConstantsDDBC, BCPControlOptions, BCPDataTypes

# BCP
from .bcp_options import BCPOptions, ColumnFormat

# GLOBALS
# Read-Only - PEP-249 mandates these names
apilevel = "2.0"
paramstyle = "qmark"
threadsafety = 1

# Create direct variables for easier access to BCP data type constants Read-only
# Character/string types
SQLTEXT = BCPDataTypes.SQLTEXT.value
SQLVARCHAR = BCPDataTypes.SQLVARCHAR.value
SQLCHARACTER = BCPDataTypes.SQLCHARACTER.value
SQLBIGCHAR = BCPDataTypes.SQLBIGCHAR.value
SQLBIGVARCHAR = BCPDataTypes.SQLBIGVARCHAR.value
SQLNCHAR = BCPDataTypes.SQLNCHAR.value
SQLNVARCHAR = BCPDataTypes.SQLNVARCHAR.value
SQLNTEXT = BCPDataTypes.SQLNTEXT.value

# Binary types
SQLBINARY = BCPDataTypes.SQLBINARY.value
SQLVARBINARY = BCPDataTypes.SQLVARBINARY.value
SQLBIGBINARY = BCPDataTypes.SQLBIGBINARY.value
SQLBIGVARBINARY = BCPDataTypes.SQLBIGVARBINARY.value
SQLIMAGE = BCPDataTypes.SQLIMAGE.value

# Integer types
SQLBIT = BCPDataTypes.SQLBIT.value
SQLBITN = BCPDataTypes.SQLBITN.value
SQLINT1 = BCPDataTypes.SQLINT1.value
SQLINT2 = BCPDataTypes.SQLINT2.value
SQLINT4 = BCPDataTypes.SQLINT4.value
SQLINT8 = BCPDataTypes.SQLINT8.value
SQLINTN = BCPDataTypes.SQLINTN.value

# Floating point types
SQLFLT4 = BCPDataTypes.SQLFLT4.value
SQLFLT8 = BCPDataTypes.SQLFLT8.value
SQLFLTN = BCPDataTypes.SQLFLTN.value

# Decimal/numeric types
SQLDECIMAL = BCPDataTypes.SQLDECIMAL.value
SQLNUMERIC = BCPDataTypes.SQLNUMERIC.value
SQLDECIMALN = BCPDataTypes.SQLDECIMALN.value
SQLNUMERICN = BCPDataTypes.SQLNUMERICN.value

# Money types
SQLMONEY = BCPDataTypes.SQLMONEY.value
SQLMONEY4 = BCPDataTypes.SQLMONEY4.value
SQLMONEYN = BCPDataTypes.SQLMONEYN.value

# Date/time types
SQLDATETIME = BCPDataTypes.SQLDATETIME.value
SQLDATETIM4 = BCPDataTypes.SQLDATETIM4.value
SQLDATETIMN = BCPDataTypes.SQLDATETIMN.value
SQLDATEN = BCPDataTypes.SQLDATEN.value
SQLTIMEN = BCPDataTypes.SQLTIMEN.value
SQLDATETIME2N = BCPDataTypes.SQLDATETIME2N.value
SQLDATETIMEOFFSETN = BCPDataTypes.SQLDATETIMEOFFSETN.value

# Special types
SQLUNIQUEID = BCPDataTypes.SQLUNIQUEID.value
SQLVARIANT = BCPDataTypes.SQLVARIANT.value
SQLUDT = BCPDataTypes.SQLUDT.value
SQLXML = BCPDataTypes.SQLXML.value
SQLTABLE = BCPDataTypes.SQLTABLE.value

# BCP special values
SQL_VARLEN_DATA = BCPDataTypes.SQL_VARLEN_DATA.value
SQL_NULL_DATA = BCPDataTypes.SQL_NULL_DATA.value


def pooling(max_size=100, idle_timeout=600):
    """
    Enable connection pooling with the specified parameters.

    Args:
        max_size (int): Maximum number of connections in the pool.
        idle_timeout (int): Time in seconds before idle connections are closed.

    Returns:
        None
    """
    PoolingManager.enable(max_size, idle_timeout)
