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
from mssql_python.cursor import Cursor
from mssql_python.logging_config import get_logger, ENABLE_LOGGING
from mssql_python.constants import ConstantsDDBC as ddbc_sql_const
from mssql_python.helpers import add_driver_to_connection_str, check_error
from mssql_python import ddbc_bindings
from mssql_python.pooling import PoolingManager
from mssql_python.exceptions import DatabaseError, InterfaceError

logger = get_logger()


class Connection:
    """
    A class to manage a connection to a database, compliant with DB-API 2.0 specifications.

    This class provides methods to establish a connection to a database, create cursors,
    commit transactions, roll back transactions, and close the connection. It is designed
    to be used in a context where database operations are required, such as executing queries
    and fetching results.

    Methods:
        __init__(database: str) -> None:
        connect_to_db() -> None:
        cursor() -> Cursor:
        commit() -> None:
        rollback() -> None:
        close() -> None:
    """

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

        if ENABLE_LOGGING:
            logger.info("Final connection string: %s", conn_str)

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
        if ENABLE_LOGGING:
            logger.info("Autocommit mode set to %s.", value)

    def setautocommit(self, value: bool = True) -> None:
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
        if ENABLE_LOGGING:
            logger.info("Transaction committed successfully.")

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
        if ENABLE_LOGGING:
            logger.info("Transaction rolled back successfully.")

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
                    if ENABLE_LOGGING:
                        logger.warning(f"Error closing cursor: {e}")
            
            # If there were errors closing cursors, log them but continue
            if close_errors and ENABLE_LOGGING:
                logger.warning(f"Encountered {len(close_errors)} errors while closing cursors")

            # Clear the cursor set explicitly to release any internal references
            self._cursors.clear()

        # Close the connection even if cursor cleanup had issues
        try:
            if self._conn:
                self._conn.close()
                self._conn = None
        except Exception as e:
            if ENABLE_LOGGING:
                logger.error(f"Error closing database connection: {e}")
            # Re-raise the connection close error as it's more critical
            raise
        finally:
            # Always mark as closed, even if there were errors
            self._closed = True
        
        if ENABLE_LOGGING:
            logger.info("Connection closed successfully.")
