"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
Type stubs for mssql_python package - based on actual public API
"""

from typing import Any, Dict, List, Optional, Union, Tuple, Sequence, Callable, Iterator
import datetime
import logging

# GLOBALS - DB-API 2.0 Required Module Globals
# https://www.python.org/dev/peps/pep-0249/#module-interface
apilevel: str  # "2.0"
paramstyle: str  # "qmark"
threadsafety: int  # 1

# Module Settings - Properties that can be get/set at module level
lowercase: bool  # Controls column name case behavior
native_uuid: bool  # Controls UUID type handling

# Settings Class
class Settings:
    lowercase: bool
    decimal_separator: str
    native_uuid: bool
    def __init__(self) -> None: ...

# Module-level Configuration Functions
def get_settings() -> Settings: ...
def setDecimalSeparator(separator: str) -> None: ...
def getDecimalSeparator() -> str: ...
def pooling(max_size: int = 100, idle_timeout: int = 600, enabled: bool = True) -> None: ...
def get_info_constants() -> Dict[str, int]: ...

# Logging Functions
def setup_logging(mode: str = "file", log_level: int = logging.DEBUG) -> None: ...
def get_logger() -> Optional[logging.Logger]: ...

# DB-API 2.0 Type Objects
# https://www.python.org/dev/peps/pep-0249/#type-objects
class STRING:
    """Type object for string-based database columns (e.g. CHAR, VARCHAR)."""

    ...

class BINARY:
    """Type object for binary database columns (e.g. BINARY, VARBINARY)."""

    ...

class NUMBER:
    """Type object for numeric database columns (e.g. INT, DECIMAL)."""

    ...

class DATETIME:
    """Type object for date/time database columns (e.g. DATE, TIMESTAMP)."""

    ...

class ROWID:
    """Type object for row identifier columns."""

    ...

# DB-API 2.0 Type Constructors
# https://www.python.org/dev/peps/pep-0249/#type-constructors
def Date(year: int, month: int, day: int) -> datetime.date: ...
def Time(hour: int, minute: int, second: int) -> datetime.time: ...
def Timestamp(
    year: int,
    month: int,
    day: int,
    hour: int,
    minute: int,
    second: int,
    microsecond: int,
) -> datetime.datetime: ...
def DateFromTicks(ticks: int) -> datetime.date: ...
def TimeFromTicks(ticks: int) -> datetime.time: ...
def TimestampFromTicks(ticks: int) -> datetime.datetime: ...
def Binary(value: Union[str, bytes, bytearray]) -> bytes: ...

# DB-API 2.0 Exception Hierarchy
# https://www.python.org/dev/peps/pep-0249/#exceptions
class Warning(Exception):
    def __init__(self, driver_error: str, ddbc_error: str) -> None: ...
    driver_error: str
    ddbc_error: str
    message: str

class Error(Exception):
    def __init__(self, driver_error: str, ddbc_error: str) -> None: ...
    driver_error: str
    ddbc_error: str
    message: str

class InterfaceError(Error):
    def __init__(self, driver_error: str, ddbc_error: str) -> None: ...

class DatabaseError(Error):
    def __init__(self, driver_error: str, ddbc_error: str) -> None: ...

class DataError(DatabaseError):
    def __init__(self, driver_error: str, ddbc_error: str) -> None: ...

class OperationalError(DatabaseError):
    def __init__(self, driver_error: str, ddbc_error: str) -> None: ...

class IntegrityError(DatabaseError):
    def __init__(self, driver_error: str, ddbc_error: str) -> None: ...

class InternalError(DatabaseError):
    def __init__(self, driver_error: str, ddbc_error: str) -> None: ...

class ProgrammingError(DatabaseError):
    def __init__(self, driver_error: str, ddbc_error: str) -> None: ...

class NotSupportedError(DatabaseError):
    def __init__(self, driver_error: str, ddbc_error: str) -> None: ...

# Row Object
class Row:
    """
    Represents a database result row.

    Supports both index-based and name-based column access.
    """

    def __init__(
        self,
        cursor: "Cursor",
        description: List[
            Tuple[
                str,
                Any,
                Optional[int],
                Optional[int],
                Optional[int],
                Optional[int],
                Optional[bool],
            ]
        ],
        values: List[Any],
        column_map: Optional[Dict[str, int]] = None,
        settings_snapshot: Optional[Dict[str, Any]] = None,
    ) -> None: ...
    def __getitem__(self, index: int) -> Any: ...
    def __getattr__(self, name: str) -> Any: ...
    def __eq__(self, other: Any) -> bool: ...
    def __len__(self) -> int: ...
    def __iter__(self) -> Iterator[Any]: ...
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...

# DB-API 2.0 Cursor Object
# https://www.python.org/dev/peps/pep-0249/#cursor-objects
class Cursor:
    """
    Database cursor for executing SQL operations and fetching results.

    This class should not be instantiated directly. Use Connection.cursor() instead.
    """

    # DB-API 2.0 Required Attributes
    description: Optional[
        List[
            Tuple[
                str,
                Any,
                Optional[int],
                Optional[int],
                Optional[int],
                Optional[int],
                Optional[bool],
            ]
        ]
    ]
    rowcount: int
    arraysize: int

    # Extension Attributes
    closed: bool
    messages: List[str]

    @property
    def rownumber(self) -> int: ...
    @property
    def connection(self) -> "Connection": ...
    def __init__(self, connection: "Connection", timeout: int = 0) -> None: ...

    # DB-API 2.0 Required Methods
    def callproc(
        self, procname: str, parameters: Optional[Sequence[Any]] = None
    ) -> Optional[Sequence[Any]]: ...
    def close(self) -> None: ...
    def execute(
        self,
        operation: str,
        *parameters: Any,
        use_prepare: bool = True,
        reset_cursor: bool = True,
    ) -> "Cursor": ...
    def executemany(self, operation: str, seq_of_parameters: List[Sequence[Any]]) -> None: ...
    def fetchone(self) -> Optional[Row]: ...
    def fetchmany(self, size: Optional[int] = None) -> List[Row]: ...
    def fetchall(self) -> List[Row]: ...
    def nextset(self) -> Optional[bool]: ...
    def setinputsizes(self, sizes: List[Union[int, Tuple[Any, ...]]]) -> None: ...
    def setoutputsize(self, size: int, column: Optional[int] = None) -> None: ...

# DB-API 2.0 Connection Object
# https://www.python.org/dev/peps/pep-0249/#connection-objects
class Connection:
    """
    Database connection object.

    This class should not be instantiated directly. Use the connect() function instead.
    """

    # DB-API 2.0 Exception Attributes
    Warning: type[Warning]
    Error: type[Error]
    InterfaceError: type[InterfaceError]
    DatabaseError: type[DatabaseError]
    DataError: type[DataError]
    OperationalError: type[OperationalError]
    IntegrityError: type[IntegrityError]
    InternalError: type[InternalError]
    ProgrammingError: type[ProgrammingError]
    NotSupportedError: type[NotSupportedError]

    # Connection Properties
    @property
    def timeout(self) -> int: ...
    @timeout.setter
    def timeout(self, value: int) -> None: ...
    @property
    def autocommit(self) -> bool: ...
    @autocommit.setter
    def autocommit(self, value: bool) -> None: ...
    @property
    def searchescape(self) -> str: ...
    def __init__(
        self,
        connection_str: str = "",
        autocommit: bool = False,
        attrs_before: Optional[Dict[int, Union[int, str, bytes]]] = None,
        timeout: int = 0,
        **kwargs: Any,
    ) -> None: ...

    # DB-API 2.0 Required Methods
    def cursor(self) -> Cursor: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...
    def close(self) -> None: ...

    # Extension Methods
    def setautocommit(self, value: bool = False) -> None: ...
    def setencoding(self, encoding: Optional[str] = None, ctype: Optional[int] = None) -> None: ...
    def getencoding(self) -> Dict[str, Union[str, int]]: ...
    def setdecoding(
        self, sqltype: int, encoding: Optional[str] = None, ctype: Optional[int] = None
    ) -> None: ...
    def getdecoding(self, sqltype: int) -> Dict[str, Union[str, int]]: ...
    def set_attr(self, attribute: int, value: Union[int, str, bytes, bytearray]) -> None: ...
    def add_output_converter(self, sqltype: int, func: Callable[[Any], Any]) -> None: ...
    def get_output_converter(self, sqltype: Union[int, type]) -> Optional[Callable[[Any], Any]]: ...
    def remove_output_converter(self, sqltype: Union[int, type]) -> None: ...
    def clear_output_converters(self) -> None: ...
    def execute(self, sql: str, *args: Any) -> Cursor: ...
    def batch_execute(
        self,
        statements: List[str],
        params: Optional[List[Union[None, Any, Tuple[Any, ...], List[Any]]]] = None,
        reuse_cursor: Optional[Cursor] = None,
        auto_close: bool = False,
    ) -> Tuple[List[Union[List[Row], int]], Cursor]: ...
    def getinfo(self, info_type: int) -> Union[str, int, bool, None]: ...

    # Context Manager Support
    def __enter__(self) -> "Connection": ...
    def __exit__(self, *args: Any) -> None: ...

# Module Connection Function
def connect(
    connection_str: str = "",
    autocommit: bool = False,
    attrs_before: Optional[Dict[int, Union[int, str, bytes]]] = None,
    timeout: int = 0,
    **kwargs: Any,
) -> Connection: ...

# SQL Type Constants
SQL_CHAR: int
SQL_VARCHAR: int
SQL_LONGVARCHAR: int
SQL_WCHAR: int
SQL_WVARCHAR: int
SQL_WLONGVARCHAR: int
SQL_DECIMAL: int
SQL_NUMERIC: int
SQL_BIT: int
SQL_TINYINT: int
SQL_SMALLINT: int
SQL_INTEGER: int
SQL_BIGINT: int
SQL_REAL: int
SQL_FLOAT: int
SQL_DOUBLE: int
SQL_BINARY: int
SQL_VARBINARY: int
SQL_LONGVARBINARY: int
SQL_DATE: int
SQL_TIME: int
SQL_TIMESTAMP: int
SQL_WMETADATA: int

# Connection Attribute Constants
SQL_ATTR_ACCESS_MODE: int
SQL_ATTR_CONNECTION_TIMEOUT: int
SQL_ATTR_CURRENT_CATALOG: int
SQL_ATTR_LOGIN_TIMEOUT: int
SQL_ATTR_PACKET_SIZE: int
SQL_ATTR_TXN_ISOLATION: int

# Transaction Isolation Level Constants
SQL_TXN_READ_UNCOMMITTED: int
SQL_TXN_READ_COMMITTED: int
SQL_TXN_REPEATABLE_READ: int
SQL_TXN_SERIALIZABLE: int

# Access Mode Constants
SQL_MODE_READ_WRITE: int
SQL_MODE_READ_ONLY: int

# GetInfo Constants for Connection.getinfo()
SQL_DRIVER_NAME: int
SQL_DRIVER_VER: int
SQL_DRIVER_ODBC_VER: int
SQL_DATA_SOURCE_NAME: int
SQL_DATABASE_NAME: int
SQL_SERVER_NAME: int
SQL_USER_NAME: int
SQL_SQL_CONFORMANCE: int
SQL_KEYWORDS: int
SQL_IDENTIFIER_QUOTE_CHAR: int
SQL_SEARCH_PATTERN_ESCAPE: int
SQL_CATALOG_TERM: int
SQL_SCHEMA_TERM: int
SQL_TABLE_TERM: int
SQL_PROCEDURE_TERM: int
SQL_TXN_CAPABLE: int
SQL_DEFAULT_TXN_ISOLATION: int
SQL_NUMERIC_FUNCTIONS: int
SQL_STRING_FUNCTIONS: int
SQL_DATETIME_FUNCTIONS: int
SQL_MAX_COLUMN_NAME_LEN: int
SQL_MAX_TABLE_NAME_LEN: int
SQL_MAX_SCHEMA_NAME_LEN: int
SQL_MAX_CATALOG_NAME_LEN: int
SQL_MAX_IDENTIFIER_LEN: int
