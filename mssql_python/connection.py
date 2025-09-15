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
from typing import Any
from mssql_python.cursor import Cursor
from mssql_python.helpers import add_driver_to_connection_str, sanitize_connection_string, log
from mssql_python import ddbc_bindings
from mssql_python.pooling import PoolingManager
from mssql_python.exceptions import InterfaceError
from mssql_python.auth import process_connection_string


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

    def execute(self, sql: str, *args: Any) -> Cursor:
        """
        Creates a new Cursor object, calls its execute method, and returns the new cursor.
        
        This is a convenience method that is not part of the DB API. Since a new Cursor
        is allocated by each call, this should not be used if more than one SQL statement
        needs to be executed on the connection.
        
        Note on cursor lifecycle management:
        - Each call creates a new cursor that is tracked by the connection's internal WeakSet
        - Cursors are automatically dereferenced/closed when they go out of scope
        - For long-running applications or loops, explicitly call cursor.close() when done
          to release resources immediately rather than waiting for garbage collection
        
        Args:
            sql (str): The SQL query to execute.
            *args: Parameters to be passed to the query.
            
        Returns:
            Cursor: A new cursor with the executed query.
            
        Raises:
            DatabaseError: If there is an error executing the query.
            InterfaceError: If the connection is closed.
    
        Example:
            # Automatic cleanup (cursor goes out of scope after the operation)
            row = connection.execute("SELECT name FROM users WHERE id = ?", 123).fetchone()
            
            # Manual cleanup for more explicit resource management
            cursor = connection.execute("SELECT * FROM large_table")
            try:
                # Use cursor...
                rows = cursor.fetchall()
            finally:
                cursor.close()  # Explicitly release resources
        """
        cursor = self.cursor()
        cursor.execute(sql, *args)
        return cursor

    def batch_execute(self, statements, params=None, reuse_cursor=None, auto_close=False):
        """
        Execute multiple SQL statements efficiently using a single cursor.
        
        This method allows executing multiple SQL statements in sequence using a single
        cursor, which is more efficient than creating a new cursor for each statement.
        
        Args:
            statements (list): List of SQL statements to execute
            params (list, optional): List of parameter sets corresponding to statements.
                Each item can be None, a single parameter, or a sequence of parameters.
                If None, no parameters will be used for any statement.
            reuse_cursor (Cursor, optional): Existing cursor to reuse instead of creating a new one.
                If None, a new cursor will be created.
            auto_close (bool): Whether to close the cursor after execution if a new one was created.
                Defaults to False. Has no effect if reuse_cursor is provided.
            
        Returns:
            tuple: (results, cursor) where:
                - results is a list of execution results, one for each statement
                - cursor is the cursor used for execution (useful if you want to keep using it)
            
        Raises:
            TypeError: If statements is not a list or if params is provided but not a list
            ValueError: If params is provided but has different length than statements
            DatabaseError: If there is an error executing any of the statements
            InterfaceError: If the connection is closed
            
        Example:
            # Execute multiple statements with a single cursor
            results, _ = conn.batch_execute([
                "INSERT INTO users VALUES (?, ?)",
                "UPDATE stats SET count = count + 1",
                "SELECT * FROM users"
            ], [
                (1, "user1"),
                None,
                None
            ])
            
            # Last result contains the SELECT results
            for row in results[-1]:
                print(row)
                
            # Reuse an existing cursor
            my_cursor = conn.cursor()
            results, _ = conn.batch_execute([
                "SELECT * FROM table1",
                "SELECT * FROM table2"
            ], reuse_cursor=my_cursor)
            
            # Cursor remains open for further use
            my_cursor.execute("SELECT * FROM table3")
        """
        # Validate inputs
        if not isinstance(statements, list):
            raise TypeError("statements must be a list of SQL statements")
        
        if params is not None:
            if not isinstance(params, list):
                raise TypeError("params must be a list of parameter sets")
            if len(params) != len(statements):
                raise ValueError("params list must have the same length as statements list")
        else:
            # Create a list of None values with the same length as statements
            params = [None] * len(statements)
        
        # Determine which cursor to use
        is_new_cursor = reuse_cursor is None
        cursor = self.cursor() if is_new_cursor else reuse_cursor
        
        # Execute statements and collect results
        results = []
        try:
            for i, (stmt, param) in enumerate(zip(statements, params)):
                try:
                    # Execute the statement with parameters if provided
                    if param is not None:
                        cursor.execute(stmt, param)
                    else:
                        cursor.execute(stmt)
                    
                    # For SELECT statements, fetch all rows
                    # For other statements, get the row count
                    if cursor.description is not None:
                        # This is a SELECT statement or similar that returns rows
                        results.append(cursor.fetchall())
                    else:
                        # This is an INSERT, UPDATE, DELETE or similar that doesn't return rows
                        results.append(cursor.rowcount)
                    
                    log('debug', f"Executed batch statement {i+1}/{len(statements)}")
                
                except Exception as e:
                    # If a statement fails, include statement context in the error
                    log('error', f"Error executing statement {i+1}/{len(statements)}: {e}")
                    raise
                    
        except Exception as e:
            # If an error occurs and auto_close is True, close the cursor
            if auto_close:
                try:
                    # Close the cursor regardless of whether it's reused or new
                    cursor.close()
                    log('debug', "Automatically closed cursor after batch execution error")
                except Exception as close_err:
                    log('warning', f"Error closing cursor after execution failure: {close_err}")
            # Re-raise the original exception
            raise
        
        # Close the cursor if requested and we created a new one
        if is_new_cursor and auto_close:
            cursor.close()
            log('debug', "Automatically closed cursor after batch execution")
        
        return results, cursor

    def commit(self) -> None:
        """
        Commit the current transaction.

        This method commits the current transaction to the database, making all
        changes made during the transaction permanent. It should be called after
        executing a series of SQL statements that modify the database to ensure
        that the changes are saved.

        Raises:
            InterfaceError: If the connection is closed.
            DatabaseError: If there is an error while committing the transaction.
        """
        # Check if connection is closed
        if self._closed or self._conn is None:
            raise InterfaceError(
                driver_error="Cannot commit on a closed connection",
                ddbc_error="Cannot commit on a closed connection",
            )
    
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
            InterfaceError: If the connection is closed.
            DatabaseError: If there is an error while rolling back the transaction.
        """
        # Check if connection is closed
        if self._closed or self._conn is None:
            raise InterfaceError(
                driver_error="Cannot rollback on a closed connection",
                ddbc_error="Cannot rollback on a closed connection",
            )
    
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