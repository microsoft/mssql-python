"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
"""

from typing import Final, Union
import datetime

# GLOBALS
# Read-Only
apilevel: Final[str] = "2.0"
paramstyle: Final[str] = "pyformat"
threadsafety: Final[int] = 1

# Type Objects
# https://www.python.org/dev/peps/pep-0249/#type-objects
class STRING:
    """
    This type object is used to describe columns in a database that are string-based (e.g. CHAR).
    """

    def __init__(self) -> None: ...

class BINARY:
    """
    This type object is used to describe (long) 
    binary columns in a database (e.g. LONG, RAW, BLOBs).
    """

    def __init__(self) -> None: ...

class NUMBER:
    """
    This type object is used to describe numeric columns in a database.
    """

    def __init__(self) -> None: ...

class DATETIME:
    """
    This type object is used to describe date/time columns in a database.
    """

    def __init__(self) -> None: ...

class ROWID:
    """
    This type object is used to describe the “Row ID” column in a database.
    """

    def __init__(self) -> None: ...

# Type Constructors
def Date(year: int, month: int, day: int) -> datetime.date: ...
def Time(hour: int, minute: int, second: int) -> datetime.time: ...
def Timestamp(
    year: int, month: int, day: int, hour: int, minute: int, second: int, microsecond: int
) -> datetime.datetime: ...
def DateFromTicks(ticks: int) -> datetime.date: ...
def TimeFromTicks(ticks: int) -> datetime.time: ...
def TimestampFromTicks(ticks: int) -> datetime.datetime: ...
def Binary(string: str) -> bytes: ...

# Exceptions
# https://www.python.org/dev/peps/pep-0249/#exceptions
class Warning(Exception): ...
class Error(Exception): ...
class InterfaceError(Error): ...
class DatabaseError(Error): ...
class DataError(DatabaseError): ...
class OperationalError(DatabaseError): ...
class IntegrityError(DatabaseError): ...
class InternalError(DatabaseError): ...
class ProgrammingError(DatabaseError): ...
class NotSupportedError(DatabaseError): ...

# Connection Objects
class Connection:
    """
    Connection object for interacting with the database.

    https://www.python.org/dev/peps/pep-0249/#connection-objects

    This class should not be instantiated directly, instead call global connect() method to
    create a Connection object.
    """

    def cursor(self) -> "Cursor":
        """
        Return a new Cursor object using the connection.
        """
        ...

    def commit(self) -> None:
        """
        Commit the current transaction.
        """
        ...

    def rollback(self) -> None:
        """
        Roll back the current transaction.
        """
        ...

    def close(self) -> None:
        """
        Close the connection now.
        """
        ...

# Cursor Objects
class Cursor:
    """
    Cursor object for executing SQL queries and fetching results.

    https://www.python.org/dev/peps/pep-0249/#cursor-objects

    This class should not be instantiated directly, instead call cursor() from a Connection
    object to create a Cursor object.
    """

    def callproc(
        self, procname: str, parameters: Union[None, list] = None
    ) -> Union[None, list]:
        """
        Call a stored database procedure with the given name.
        """
        ...

    def close(self) -> None:
        """
        Close the cursor now.
        """
        ...

    def execute(
        self, operation: str, parameters: Union[None, list, dict] = None
    ) -> None:
        """
        Prepare and execute a database operation (query or command).
        """
        ...

    def executemany(self, operation: str, seq_of_parameters: list) -> None:
        """
        Prepare a database operation and execute it against all parameter sequences.
        """
        ...

    def fetchone(self) -> Union[None, tuple]:
        """
        Fetch the next row of a query result set.
        """
        ...

    def fetchmany(self, size: int = None) -> list:
        """
        Fetch the next set of rows of a query result.
        """
        ...

    def fetchall(self) -> list:
        """
        Fetch all (remaining) rows of a query result.
        """
        ...

    def nextset(self) -> Union[None, bool]:
        """
        Skip to the next available result set.
        """
        ...

    def setinputsizes(self, sizes: list) -> None:
        """
        Predefine memory areas for the operation’s parameters.
        """
        ...

    def setoutputsize(self, size: int, column: int = None) -> None:
        """
        Set a column buffer size for fetches of large columns.
        """
        ...

# Module Functions
def connect(connection_str: str) -> Connection:
    """
    Constructor for creating a connection to the database.
    """
    ...
