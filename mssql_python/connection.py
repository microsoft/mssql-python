"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
This module defines the Connection class, which is used to manage a connection to a database.
The class provides methods to establish a connection, create cursors, commit transactions, 
roll back transactions, and close the connection.
Resource Management:
- All cursors created from this connection are tracked internally.
- When close() is called on the connection, all open cursors are automatically closed.
- Do not use any cursor after the connection is closed; doing so will raise an exception.
- Cursors are also cleaned up automatically when no longer referenced, to prevent memory leaks.
"""
import weakref
import re
import codecs
from mssql_python.cursor import Cursor
from mssql_python.helpers import add_driver_to_connection_str, sanitize_connection_string, sanitize_user_input, log
from mssql_python import ddbc_bindings
from mssql_python.pooling import PoolingManager
from mssql_python.exceptions import InterfaceError, ProgrammingError
from mssql_python.auth import process_connection_string
from mssql_python.constants import ConstantsDDBC

# Add SQL_WMETADATA constant for metadata decoding configuration
SQL_WMETADATA = -99  # Special flag for column name decoding

# UTF-16 encoding variants that should use SQL_WCHAR by default
UTF16_ENCODINGS = frozenset([
    'utf-16',
    'utf-16le', 
    'utf-16be'
])

def _validate_encoding(encoding: str) -> bool:
    """
    Cached encoding validation using codecs.lookup().
    
    Args:
        encoding (str): The encoding name to validate.
        
    Returns:
        bool: True if encoding is valid, False otherwise.
        
    Note:
        Uses LRU cache to avoid repeated expensive codecs.lookup() calls.
        Cache size is limited to 128 entries which should cover most use cases.
    """
    try:
        codecs.lookup(encoding)
        return True
    except LookupError:
        return False

# Import all DB-API 2.0 exception classes for Connection attributes
from mssql_python.exceptions import (
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


class Connection:
    """
    A class to manage a connection to a database, compliant with DB-API 2.0 specifications.

    This class provides methods to establish a connection to a database, create cursors,
    commit transactions, roll back transactions, and close the connection. It is designed
    to be used in a context where database operations are required, such as executing queries
    and fetching results.

    The Connection class supports the Python context manager protocol (with statement).
    When used as a context manager, it will automatically close the connection when
    exiting the context, ensuring proper resource cleanup.

    Example usage:
        with connect(connection_string) as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO table VALUES (?)", [value])
        # Connection is automatically closed when exiting the with block
        
    For long-lived connections, use without context manager:
        conn = connect(connection_string)
        try:
            # Multiple operations...
        finally:
            conn.close()

    Methods:
        __init__(database: str) -> None:
        connect_to_db() -> None:
        cursor() -> Cursor:
        commit() -> None:
        rollback() -> None:
        close() -> None:
        __enter__() -> Connection:
        __exit__() -> None:
        setencoding(encoding=None, ctype=None) -> None:
        setdecoding(sqltype, encoding=None, ctype=None) -> None:
        getdecoding(sqltype) -> dict:
    """

    # DB-API 2.0 Exception attributes
    # These allow users to catch exceptions using connection.Error, connection.ProgrammingError, etc.
    Warning = Warning
    Error = Error
    InterfaceError = InterfaceError
    DatabaseError = DatabaseError
    DataError = DataError
    OperationalError = OperationalError
    IntegrityError = IntegrityError
    InternalError = InternalError
    ProgrammingError = ProgrammingError
    NotSupportedError = NotSupportedError

    def __init__(self, connection_str: str = "", autocommit: bool = False, attrs_before: dict = None, **kwargs) -> None:
        """
        Initialize the connection object with the specified connection string and parameters.

        Args:
            - connection_str (str): The connection string to connect to.
            - autocommit (bool): If True, causes a commit to be performed after each SQL statement.
            **kwargs: Additional key/value pairs for the connection string.
            Not including below properties since we are driver doesn't support this:

        Returns:
            None

        Raises:
            ValueError: If the connection string is invalid or connection fails.

        This method sets up the initial state for the connection object,
        preparing it for further operations such as connecting to the 
        database, executing queries, etc.
        """
        self.connection_str = self._construct_connection_string(
            connection_str, **kwargs
        )
        self._attrs_before = attrs_before or {}

        # Initialize encoding settings with defaults for Python 3
        # Python 3 only has str (which is Unicode), so we use utf-16le by default
        self._encoding_settings = {
            'encoding': 'utf-16le',
            'ctype': ConstantsDDBC.SQL_WCHAR.value
        }

        # Initialize decoding settings with Python 3 defaults
        self._decoding_settings = {
            ConstantsDDBC.SQL_CHAR.value: {
                'encoding': 'utf-8',
                'ctype': ConstantsDDBC.SQL_CHAR.value
            },
            ConstantsDDBC.SQL_WCHAR.value: {
                'encoding': 'utf-16le',
                'ctype': ConstantsDDBC.SQL_WCHAR.value
            },
            SQL_WMETADATA: {
                'encoding': 'utf-16le',
                'ctype': ConstantsDDBC.SQL_WCHAR.value
            }
        }

        # Check if the connection string contains authentication parameters
        # This is important for processing the connection string correctly.
        # If authentication is specified, it will be processed to handle
        # different authentication types like interactive, device code, etc.
        if re.search(r"authentication", self.connection_str, re.IGNORECASE):
            connection_result = process_connection_string(self.connection_str)
            self.connection_str = connection_result[0]
            if connection_result[1]:
                self._attrs_before.update(connection_result[1])
        
        self._closed = False
        
        # Using WeakSet which automatically removes cursors when they are no longer in use
        # It is a set that holds weak references to its elements.
        # When an object is only weakly referenced, it can be garbage collected even if it's still in the set.
        # It prevents memory leaks by ensuring that cursors are cleaned up when no longer in use without requiring explicit deletion.
        # TODO: Think and implement scenarios for multi-threaded access to cursors
        self._cursors = weakref.WeakSet()

        # Auto-enable pooling if user never called
        if not PoolingManager.is_initialized():
            PoolingManager.enable()
        self._pooling = PoolingManager.is_enabled()
        self._conn = ddbc_bindings.Connection(self.connection_str, self._pooling, self._attrs_before)
        self.setautocommit(autocommit)

    def _construct_connection_string(self, connection_str: str = "", **kwargs) -> str:
        """
        Construct the connection string by concatenating the connection string 
        with key/value pairs from kwargs.

        Args:
            connection_str (str): The base connection string.
            **kwargs: Additional key/value pairs for the connection string.

        Returns:
            str: The constructed connection string.
        """
        # Add the driver attribute to the connection string
        conn_str = add_driver_to_connection_str(connection_str)

        # Add additional key-value pairs to the connection string
        for key, value in kwargs.items():
            if key.lower() == "host" or key.lower() == "server":
                key = "Server"
            elif key.lower() == "user" or key.lower() == "uid":
                key = "Uid"
            elif key.lower() == "password" or key.lower() == "pwd":
                key = "Pwd"
            elif key.lower() == "database":
                key = "Database"
            elif key.lower() == "encrypt":
                key = "Encrypt"
            elif key.lower() == "trust_server_certificate":
                key = "TrustServerCertificate"
            else:
                continue
            conn_str += f"{key}={value};"

        log('info', "Final connection string: %s", sanitize_connection_string(conn_str))

        return conn_str
    
    @property
    def autocommit(self) -> bool:
        """
        Return the current autocommit mode of the connection.
        Returns:
            bool: True if autocommit is enabled, False otherwise.
        """
        return self._conn.get_autocommit()

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        """
        Set the autocommit mode of the connection.
        Args:
            value (bool): True to enable autocommit, False to disable it.
        Returns:
            None
        """
        self.setautocommit(value)
        log('info', "Autocommit mode set to %s.", value)

    def setautocommit(self, value: bool = False) -> None:
        """
        Set the autocommit mode of the connection.
        Args:
            value (bool): True to enable autocommit, False to disable it.
        Returns:
            None
        Raises:
            DatabaseError: If there is an error while setting the autocommit mode.
        """
        self._conn.set_autocommit(value)

    def setencoding(self, encoding=None, ctype=None):
        """
        Sets the text encoding for SQL statements and text parameters.
        
        Since Python 3 only has str (which is Unicode), this method configures
        how text is encoded when sending to the database.
        
        Args:
            encoding (str, optional): The encoding to use. This must be a valid Python 
                encoding that converts text to bytes. If None, defaults to 'utf-16le'.
            ctype (int, optional): The C data type to use when passing data: 
                SQL_CHAR or SQL_WCHAR. If not provided, SQL_WCHAR is used for 
                UTF-16 variants (see UTF16_ENCODINGS constant). SQL_CHAR is used for all other encodings.
        
        Returns:
            None
            
        Raises:
            ProgrammingError: If the encoding is not valid or not supported.
            InterfaceError: If the connection is closed.
            
        Example:
            # For databases that only communicate with UTF-8
            cnxn.setencoding(encoding='utf-8')
            
            # For explicitly using SQL_CHAR
            cnxn.setencoding(encoding='utf-8', ctype=mssql_python.SQL_CHAR)
        """
        if self._closed:
            raise InterfaceError(
                driver_error="Connection is closed",
                ddbc_error="Connection is closed",
            )
        
        # Set default encoding if not provided
        if encoding is None:
            encoding = 'utf-16le'
            
        # Validate encoding using cached validation for better performance
        if not _validate_encoding(encoding):
            # Log the sanitized encoding for security
            log('warning', "Invalid encoding attempted: %s", sanitize_user_input(str(encoding)))
            raise ProgrammingError(
                driver_error=f"Unsupported encoding: {encoding}",
                ddbc_error=f"The encoding '{encoding}' is not supported by Python",
            )
        
        # Normalize encoding to casefold for more robust Unicode handling
        encoding = encoding.casefold()
        
        # Set default ctype based on encoding if not provided
        if ctype is None:
            if encoding in UTF16_ENCODINGS:
                ctype = ConstantsDDBC.SQL_WCHAR.value
            else:
                ctype = ConstantsDDBC.SQL_CHAR.value
        
        # Validate ctype
        valid_ctypes = [ConstantsDDBC.SQL_CHAR.value, ConstantsDDBC.SQL_WCHAR.value]
        if ctype not in valid_ctypes:
            # Log the sanitized ctype for security  
            log('warning', "Invalid ctype attempted: %s", sanitize_user_input(str(ctype)))
            raise ProgrammingError(
                driver_error=f"Invalid ctype: {ctype}",
                ddbc_error=f"ctype must be SQL_CHAR ({ConstantsDDBC.SQL_CHAR.value}) or SQL_WCHAR ({ConstantsDDBC.SQL_WCHAR.value})",
            )
        
        # Store the encoding settings
        self._encoding_settings = {
            'encoding': encoding,
            'ctype': ctype
        }
        
        # Log with sanitized values for security
        log('info', "Text encoding set to %s with ctype %s", 
            sanitize_user_input(encoding), sanitize_user_input(str(ctype)))

    def getencoding(self):
        """
        Gets the current text encoding settings.
        
        Returns:
            dict: A dictionary containing 'encoding' and 'ctype' keys.
            
        Raises:
            InterfaceError: If the connection is closed.
            
        Example:
            settings = cnxn.getencoding()
            print(f"Current encoding: {settings['encoding']}")
            print(f"Current ctype: {settings['ctype']}")
        """
        if self._closed:
            raise InterfaceError(
                driver_error="Connection is closed",
                ddbc_error="Connection is closed",
            )
        
        return self._encoding_settings.copy()

    def setdecoding(self, sqltype, encoding=None, ctype=None):
        """
        Sets the text decoding used when reading SQL_CHAR and SQL_WCHAR from the database.
        
        This method configures how text data is decoded when reading from the database.
        In Python 3, all text is Unicode (str), so this primarily affects the encoding
        used to decode bytes from the database.
        
        Args:
            sqltype (int): The SQL type being configured: SQL_CHAR, SQL_WCHAR, or SQL_WMETADATA.
                SQL_WMETADATA is a special flag for configuring column name decoding.
            encoding (str, optional): The Python encoding to use when decoding the data.
                If None, uses default encoding based on sqltype.
            ctype (int, optional): The C data type to request from SQLGetData: 
                SQL_CHAR or SQL_WCHAR. If None, uses default based on encoding.
        
        Returns:
            None
            
        Raises:
            ProgrammingError: If the sqltype, encoding, or ctype is invalid.
            InterfaceError: If the connection is closed.
            
        Example:
            # Configure SQL_CHAR to use UTF-8 decoding
            cnxn.setdecoding(mssql_python.SQL_CHAR, encoding='utf-8')
            
            # Configure column metadata decoding
            cnxn.setdecoding(mssql_python.SQL_WMETADATA, encoding='utf-16le')
            
            # Use explicit ctype
            cnxn.setdecoding(mssql_python.SQL_WCHAR, encoding='utf-16le', ctype=mssql_python.SQL_WCHAR)
        """
        if self._closed:
            raise InterfaceError(
                driver_error="Connection is closed",
                ddbc_error="Connection is closed",
            )
        
        # Validate sqltype
        valid_sqltypes = [
            ConstantsDDBC.SQL_CHAR.value,
            ConstantsDDBC.SQL_WCHAR.value,
            SQL_WMETADATA
        ]
        if sqltype not in valid_sqltypes:
            log('warning', "Invalid sqltype attempted: %s", sanitize_user_input(str(sqltype)))
            raise ProgrammingError(
                driver_error=f"Invalid sqltype: {sqltype}",
                ddbc_error=f"sqltype must be SQL_CHAR ({ConstantsDDBC.SQL_CHAR.value}), SQL_WCHAR ({ConstantsDDBC.SQL_WCHAR.value}), or SQL_WMETADATA ({SQL_WMETADATA})",
            )
        
        # Set default encoding based on sqltype if not provided
        if encoding is None:
            if sqltype == ConstantsDDBC.SQL_CHAR.value:
                encoding = 'utf-8'  # Default for SQL_CHAR in Python 3
            else:  # SQL_WCHAR or SQL_WMETADATA
                encoding = 'utf-16le'  # Default for SQL_WCHAR in Python 3
        
        # Validate encoding using cached validation for better performance
        if not _validate_encoding(encoding):
            log('warning', "Invalid encoding attempted: %s", sanitize_user_input(str(encoding)))
            raise ProgrammingError(
                driver_error=f"Unsupported encoding: {encoding}",
                ddbc_error=f"The encoding '{encoding}' is not supported by Python",
            )
        
        # Normalize encoding to lowercase for consistency
        encoding = encoding.lower()
        
        # Set default ctype based on encoding if not provided
        if ctype is None:
            if encoding in UTF16_ENCODINGS:
                ctype = ConstantsDDBC.SQL_WCHAR.value
            else:
                ctype = ConstantsDDBC.SQL_CHAR.value
        
        # Validate ctype
        valid_ctypes = [ConstantsDDBC.SQL_CHAR.value, ConstantsDDBC.SQL_WCHAR.value]
        if ctype not in valid_ctypes:
            log('warning', "Invalid ctype attempted: %s", sanitize_user_input(str(ctype)))
            raise ProgrammingError(
                driver_error=f"Invalid ctype: {ctype}",
                ddbc_error=f"ctype must be SQL_CHAR ({ConstantsDDBC.SQL_CHAR.value}) or SQL_WCHAR ({ConstantsDDBC.SQL_WCHAR.value})",
            )
        
        # Store the decoding settings for the specified sqltype
        self._decoding_settings[sqltype] = {
            'encoding': encoding,
            'ctype': ctype
        }
        
        # Log with sanitized values for security
        sqltype_name = {
            ConstantsDDBC.SQL_CHAR.value: "SQL_CHAR",
            ConstantsDDBC.SQL_WCHAR.value: "SQL_WCHAR", 
            SQL_WMETADATA: "SQL_WMETADATA"
        }.get(sqltype, str(sqltype))
        
        log('info', "Text decoding set for %s to %s with ctype %s", 
            sqltype_name, sanitize_user_input(encoding), sanitize_user_input(str(ctype)))

    def getdecoding(self, sqltype):
        """
        Gets the current text decoding settings for the specified SQL type.
        
        Args:
            sqltype (int): The SQL type to get settings for: SQL_CHAR, SQL_WCHAR, or SQL_WMETADATA.
        
        Returns:
            dict: A dictionary containing 'encoding' and 'ctype' keys for the specified sqltype.
            
        Raises:
            ProgrammingError: If the sqltype is invalid.
            InterfaceError: If the connection is closed.
            
        Example:
            settings = cnxn.getdecoding(mssql_python.SQL_CHAR)
            print(f"SQL_CHAR encoding: {settings['encoding']}")
            print(f"SQL_CHAR ctype: {settings['ctype']}")
        """
        if self._closed:
            raise InterfaceError(
                driver_error="Connection is closed",
                ddbc_error="Connection is closed",
            )
        
        # Validate sqltype
        valid_sqltypes = [
            ConstantsDDBC.SQL_CHAR.value,
            ConstantsDDBC.SQL_WCHAR.value,
            SQL_WMETADATA
        ]
        if sqltype not in valid_sqltypes:
            raise ProgrammingError(
                driver_error=f"Invalid sqltype: {sqltype}",
                ddbc_error=f"sqltype must be SQL_CHAR ({ConstantsDDBC.SQL_CHAR.value}), SQL_WCHAR ({ConstantsDDBC.SQL_WCHAR.value}), or SQL_WMETADATA ({SQL_WMETADATA})",
            )
        
        return self._decoding_settings[sqltype].copy()

    def cursor(self) -> Cursor:
        """
        Return a new Cursor object using the connection.

        This method creates and returns a new cursor object that can be used to
        execute SQL queries and fetch results. The cursor is associated with the
        current connection and allows interaction with the database.

        Returns:
            Cursor: A new cursor object for executing SQL queries.

        Raises:
            DatabaseError: If there is an error while creating the cursor.
            InterfaceError: If there is an error related to the database interface.
        """
        """Return a new Cursor object using the connection."""
        if self._closed:
            # raise InterfaceError
            raise InterfaceError(
                driver_error="Cannot create cursor on closed connection",
                ddbc_error="Cannot create cursor on closed connection",
            )

        cursor = Cursor(self)
        self._cursors.add(cursor)  # Track the cursor
        return cursor

    def commit(self) -> None:
        """
        Commit the current transaction.

        This method commits the current transaction to the database, making all
        changes made during the transaction permanent. It should be called after
        executing a series of SQL statements that modify the database to ensure
        that the changes are saved.

        Raises:
            DatabaseError: If there is an error while committing the transaction.
        """
        # Commit the current transaction
        self._conn.commit()
        log('info', "Transaction committed successfully.")

    def rollback(self) -> None:
        """
        Roll back the current transaction.

        This method rolls back the current transaction, undoing all changes made
        during the transaction. It should be called if an error occurs during the
        transaction or if the changes should not be saved.

        Raises:
            DatabaseError: If there is an error while rolling back the transaction.
        """
        # Roll back the current transaction
        self._conn.rollback()
        log('info', "Transaction rolled back successfully.")

    def close(self) -> None:
        """
        Close the connection now (rather than whenever .__del__() is called).

        This method closes the connection to the database, releasing any resources
        associated with it. After calling this method, the connection object should
        not be used for any further operations. The same applies to all cursor objects
        trying to use the connection. Note that closing a connection without committing
        the changes first will cause an implicit rollback to be performed.

        Raises:
            DatabaseError: If there is an error while closing the connection.
        """
        # Close the connection
        if self._closed:
            return
        
        # Close all cursors first, but don't let one failure stop the others
        if hasattr(self, '_cursors'):
            # Convert to list to avoid modification during iteration
            cursors_to_close = list(self._cursors)
            close_errors = []
            
            for cursor in cursors_to_close:
                try:
                    if not cursor.closed:
                        cursor.close()
                except Exception as e:
                    # Collect errors but continue closing other cursors
                    close_errors.append(f"Error closing cursor: {e}")
                    log('warning', f"Error closing cursor: {e}")
            
            # If there were errors closing cursors, log them but continue
            if close_errors:
                log('warning', f"Encountered {len(close_errors)} errors while closing cursors")

            # Clear the cursor set explicitly to release any internal references
            self._cursors.clear()

        # Close the connection even if cursor cleanup had issues
        try:
            if self._conn:
                if not self.autocommit:
                    # If autocommit is disabled, rollback any uncommitted changes
                    # This is important to ensure no partial transactions remain
                    # For autocommit True, this is not necessary as each statement is committed immediately
                    log('info', "Rolling back uncommitted changes before closing connection.")
                    self._conn.rollback()
                # TODO: Check potential race conditions in case of multithreaded scenarios
                # Close the connection
                self._conn.close()
                self._conn = None
        except Exception as e:
            log('error', f"Error closing database connection: {e}")
            # Re-raise the connection close error as it's more critical
            raise
        finally:
            # Always mark as closed, even if there were errors
            self._closed = True
        
        log('info', "Connection closed successfully.")

    def __enter__(self) -> 'Connection':
        """
        Enter the context manager.
        
        This method enables the Connection to be used with the 'with' statement.
        When entering the context, it simply returns the connection object itself.
        
        Returns:
            Connection: The connection object itself.
            
        Example:
            with connect(connection_string) as conn:
                cursor = conn.cursor()
                cursor.execute("INSERT INTO table VALUES (?)", [value])
                # Transaction will be committed automatically when exiting
        """
        log('info', "Entering connection context manager.")
        return self

    def __exit__(self, *args) -> None:
        """
        Exit the context manager.
        
        Closes the connection when exiting the context, ensuring proper resource cleanup.
        This follows the modern standard used by most database libraries.
        """
        if not self._closed:
            self.close()

    def __del__(self):
        """
        Destructor to ensure the connection is closed when the connection object is no longer needed.
        This is a safety net to ensure resources are cleaned up
        even if close() was not called explicitly.
        """
        if "_closed" not in self.__dict__ or not self._closed:
            try:
                self.close()
            except Exception as e:
                # Dont raise exceptions from __del__ to avoid issues during garbage collection
                log('error', f"Error during connection cleanup: {e}")