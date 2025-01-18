import ctypes
from mssql_python.cursor import Cursor
from mssql_python.logging_config import setup_logging
from mssql_python.constants import ConstantsODBC as odbc_sql_const
from mssql_python.helpers import add_driver_to_connection_str, check_error
import logging
import os

# Change the current working directory to the directory of the script to import ddbc_bindings
os.chdir(os.path.dirname(__file__))

from mssql_python import ddbc_bindings

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
        connect_to_db() -> None:
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
        self.henv = ctypes.c_void_p()
        self.hdbc = ctypes.c_void_p()
        self.connection_str = add_driver_to_connection_str(connection_str)
        self._initializer()

    def _initializer(self) -> None:
        """
        Initialize the ODBC environment and connection handles.

        This method is responsible for setting up the ODBC environment and connection
        handles, allocating memory for them, and setting the necessary attributes.
        It should be called before establishing a connection to the database.
        """
        self._allocate_environment_handle()
        self._set_environment_attributes()
        self._allocate_connection_handle()
        self._connect_to_db()
        
    def _allocate_environment_handle(self):
        """
        Allocate the ODBC environment handle.
        """
        ret = ddbc_bindings.SQLAllocHandle(
            odbc_sql_const.SQL_HANDLE_ENV.value, #SQL environment handle type
            0, # SQL input handle
            ctypes.cast(ctypes.pointer(self.henv), ctypes.c_void_p).value # SQL output handle pointer
        )
        check_error(odbc_sql_const.SQL_HANDLE_ENV.value, self.henv.value, ret)

    def _set_environment_attributes(self):
        """
        Set the ODBC environment attributes.
        """
        ret = ddbc_bindings.SQLSetEnvAttr(
            self.henv.value,  # Environment handle
            odbc_sql_const.SQL_ATTR_ODBC_VERSION.value,  # Attribute
            odbc_sql_const.SQL_OV_ODBC3_80.value, # String Length
            0 # Null-terminated string
        )
        check_error(odbc_sql_const.SQL_HANDLE_ENV.value, self.henv.value, ret)

    def _allocate_connection_handle(self):
        """
        Allocate the ODBC connection handle.
        """
        ret = ddbc_bindings.SQLAllocHandle(
            odbc_sql_const.SQL_HANDLE_DBC.value, # SQL connection handle type
            self.henv.value, # SQL environment handle
            ctypes.cast(ctypes.pointer(self.hdbc), ctypes.c_void_p).value # SQL output handle pointer
        )
        check_error(odbc_sql_const.SQL_HANDLE_DBC.value, self.hdbc.value, ret)

    def _connect_to_db(self) -> None:
        """
        Establish a connection to the database.

        This method is responsible for creating a connection to the specified database.
        It does not take any arguments and does not return any value. The connection
        details such as database name, user credentials, host, and port should be
        configured within the class or passed during the class instantiation.

        Raises:
            DatabaseError: If there is an error while trying to connect to the database.
            InterfaceError: If there is an error related to the database interface.
        """
        logging.info("Connecting to the database")
        try:
            ret = ddbc_bindings.SQLDriverConnect(
                self.hdbc.value, # Connection handle
                0, # Window handle
                self.connection_str # Connection string
            )
            check_error(odbc_sql_const.SQL_HANDLE_DBC.value, self.hdbc.value, ret)
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
            return Cursor(self)
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
            ret = ddbc_bindings.SQLEndTran(
                odbc_sql_const.SQL_HANDLE_DBC.value, # Handle type
                self.hdbc.value, # Connection handle
                odbc_sql_const.SQL_COMMIT.value # Commit the transaction
            )
            check_error(odbc_sql_const.SQL_HANDLE_DBC.value, self.hdbc.value, ret)
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
            ret = ddbc_bindings.SQLEndTran(
                odbc_sql_const.SQL_HANDLE_DBC.value,  # Handle type
                self.hdbc.value, # Connection handle
                odbc_sql_const.SQL_ROLLBACK.value # Roll back the transaction
            )
            check_error(odbc_sql_const.SQL_HANDLE_DBC.value, self.hdbc.value, ret)
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
            ret = ddbc_bindings.SQLDisconnect(self.hdbc.value)
            check_error(odbc_sql_const.SQL_HANDLE_DBC.value, self.hdbc.value, ret)
            
            # Free the connection handle
            ret = ddbc_bindings.SQLFreeHandle(odbc_sql_const.SQL_HANDLE_DBC.value, self.hdbc.value)
            check_error(odbc_sql_const.SQL_HANDLE_DBC.value, self.hdbc.value, ret)
            
            
            logging.info("Connection closed successfully.")
        except Exception as e:
            logging.error("An error occurred while closing the connection: %s", e)
            raise Exception("DatabaseError: Failed to close the connection") from e
