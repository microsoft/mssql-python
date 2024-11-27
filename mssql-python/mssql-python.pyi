from typing import (
    Any, Callable, Dict, Final, Generator, Iterable, Iterator,
    List, Optional, Sequence, Tuple, Union,
)

# GLOBALS
# Read-Only
apilevel: Final[str] = '2.0'
paramstyle: Final[str] = 'qmark'
threadsafety: Final[int] = 1

# Type Objects
STRING: str
BINARY: str
NUMBER: str
DATETIME: str
ROWID: str

# Type Constructors
def Date(year: int, month: int, day: int) -> str: ...
def Time(hour: int, minute: int, second: int) -> str: ...
def Timestamp(year: int, month: int, day: int, hour: int, minute: int, second: int) -> str: ...
def DateFromTicks(ticks: int) -> str: ...
def TimeFromTicks(ticks: int) -> str: ...
def TimestampFromTicks(ticks: int) -> str: ...
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

    This class should not be instantiated directly, instead call pyodbc.connect() to
    create a Connection object.
    """

    def cursor(self) -> 'Cursor':
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

    def callproc(self, procname: str, parameters: Union[None, list] = None) -> Union[None, list]:
        """
        Call a stored database procedure with the given name.
        """
        ...

    def close(self) -> None:
        """
        Close the cursor now.
        """
        ...

    def execute(self, operation: str, parameters: Union[None, list, dict] = None) -> None:
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
def connect(database: str) -> Connection:
    """
    Constructor for creating a connection to the database.
    """
    ...