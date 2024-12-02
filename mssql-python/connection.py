import ctypes
from cursor import Cursor
from helper import add_driver_to_connection_str
from logging_config import setup_logging
from utils import ODBCInitializer
from constants import ConstantsODBC as const
import logging

# Setting up logging
setup_logging()

class Connection:
    """
    A class to manage a connection to a database, compliant with DB-API 2.0 specifications.

    This class provides methods to establish a connection to a database, create cursors,
    commit transactions, roll back transactions, and close the connection. It is designed
    to be used in a context where database operations are required, such as executing queries
    and fetching results.
    
    Methods:
        __init__(database: str) -> None:
        _connect_to_db() -> None:
        cursor() -> Cursor:
        commit() -> None:
        rollback() -> None:
        close() -> None:
    """
  
    def __init__(self, connection_str: str) -> None:
        """
        Initialize the connection object with the specified connection string.

        Args:
            connection_str (str): The connection_str to connect to.

        Returns:
            None

        Raises:
            ValueError: If the connection_str is invalid or connection fails.

        This method sets up the initial state for the connection object, 
        preparing it for further operations such as connecting to the database, executing queries, etc.
        """
        self.odbc_initializer = ODBCInitializer()
        self.connection_str = add_driver_to_connection_str(connection_str)
        

    def _connect_to_db(self) -> None:
        """
        Establish a connection to the database.

        This method is responsible for creating a connection to the specified database.
        It does not take any arguments and does not return any value. The connection
        details such as database name, user credentials, host, and port should be
        configured within the class or passed during the class instantiation.

        """
        try:
            converted_connection_string = ctypes.c_wchar_p(self.connection_str)
            out_connection_string = ctypes.create_unicode_buffer(1024)
            out_connection_string_length = ctypes.c_short()
            ret = self.odbc_initializer.odbc.SQLDriverConnectW(
                self.odbc_initializer.hdbc,
                None,
                converted_connection_string,
                len(self.connection_str),
                out_connection_string,
                1024,
                ctypes.byref(out_connection_string_length),
                const.SQL_DRIVER_NOPROMPT.value
            )
            self.odbc_initializer._check_ret(ret, const.SQL_HANDLE_DBC.value, self.hdbc)
            logging.info("Connection established successfully.")
        except Exception as e:
            logging.error("An error occurred while connecting to the database: %s", e)
            raise

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
        try:
            return Cursor(self.connection_str)
        except Exception as e:
            logging.error("An error occurred while creating the cursor: %s", e)
            raise Exception("DatabaseError: Failed to create the cursor") from e

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
        try:
            # Commit the current transaction
            ret = self.odbc_initializer.odbc.SQLEndTran(const.SQL_HANDLE_DBC.value, self.odbc_initializer.hdbc, const.SQL_COMMIT.value)
            self.odbc_initializer._check_ret(ret, const.SQL_HANDLE_DBC.value, self.odbc_initializer.hdbc)
            logging.info("Transaction committed successfully.")
        except Exception as e:
            logging.error("An error occurred while committing the transaction: %s", e)
            raise Exception("DatabaseError: Failed to commit the transaction") from e

    def rollback(self) -> None:
        """
        Roll back the current transaction.

        This method rolls back the current transaction, undoing all changes made
        during the transaction. It should be called if an error occurs during the
        transaction or if the changes should not be saved.

        Raises:
            DatabaseError: If there is an error while rolling back the transaction.
        """
        try:
            # Roll back the current transaction
            ret = self.odbc_initializer.odbc.SQLEndTran(const.SQL_HANDLE_DBC.value, self.odbc_initializer.hdbc, const.SQL_ROLLBACK.value)
            self.odbc_initializer._check_ret(ret, const.SQL_HANDLE_DBC.value, self.odbc_initializer.hdbc)
            logging.info("Transaction rolled back successfully.")
        except Exception as e:
            logging.error("An error occurred while rolling back the transaction: %s", e)
            raise Exception("DatabaseError: Failed to roll back the transaction") from e

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
        try:
            # Disconnect from the database
            ret = self.odbc_initializer.odbc.SQLDisconnect(self.odbc_initializer.hdbc)
            self.odbc_initializer._check_ret(ret, const.SQL_HANDLE_DBC.value, self.odbc_initializer.hdbc)
            
            # Free the connection handle
            ret = self.odbc_initializer.odbc.SQLFreeHandle(const.SQL_HANDLE_DBC.value, self.odbc_initializer.hdbc)
            self.odbc_initializer._check_ret(ret, const.SQL_HANDLE_DBC.value, self.odbc_initializer.hdbc)
            
            # Free the environment handle
            ret = self.odbc_initializer.odbc.SQLFreeHandle(const.SQL_HANDLE_ENV.value, self.odbc_initializer.henv)
            self.odbc_initializer._check_ret(ret, const.SQL_HANDLE_ENV.value, self.odbc_initializer.henv)
            
            logging.info("Connection closed successfully.")
        except Exception as e:
            logging.error("An error occurred while closing the connection: %s", e)
            raise Exception("DatabaseError: Failed to close the connection") from e