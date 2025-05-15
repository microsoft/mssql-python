"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
This module defines the Connection class, which is used to manage a connection to a database.
The class provides methods to establish a connection, create cursors, commit transactions, 
roll back transactions, and close the connection.
"""
import ctypes
from mssql_python.cursor import Cursor
from mssql_python.logging_config import get_logger, ENABLE_LOGGING
from mssql_python.constants import ConstantsDDBC as ddbc_sql_const
from mssql_python.helpers import add_driver_to_connection_str, check_error
from mssql_python import ddbc_bindings

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
        self.henv = None
        self.hdbc = None
        self.connection_str = self._construct_connection_string(
            connection_str, **kwargs
        )
        self._attrs_before = attrs_before
        self._autocommit = autocommit  # Initialize _autocommit before calling _initializer
        self._initializer()
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

    def _is_closed(self) -> bool:
        """
        Check if the connection is closed.

        Returns:
            bool: True if the connection is closed, False otherwise.
        """
        return self.hdbc is None
    
    def _initializer(self) -> None:
        """
        Initialize the environment and connection handles.

        This method is responsible for setting up the environment and connection
        handles, allocating memory for them, and setting the necessary attributes.
        It should be called before establishing a connection to the database.
        """
        self._allocate_environment_handle()
        self._set_environment_attributes()
        self._allocate_connection_handle()
        if self._attrs_before != {}:
            self._apply_attrs_before()  # Apply pre-connection attributes
        if self._autocommit:
            self._set_connection_attributes(
                ddbc_sql_const.SQL_ATTR_AUTOCOMMIT.value,
                ddbc_sql_const.SQL_AUTOCOMMIT_ON.value,
            )
        self._connect_to_db()

    def _apply_attrs_before(self):
        """
        Apply specific pre-connection attributes.
        Currently, this method only processes an attribute with key 1256 (e.g., SQL_COPT_SS_ACCESS_TOKEN)
        if present in `self._attrs_before`. Other attributes are ignored.

        Returns:
            bool: True.
        """

        if ENABLE_LOGGING:
            logger.info("Attempting to apply pre-connection attributes (attrs_before): %s", self._attrs_before)

        if not isinstance(self._attrs_before, dict):
            if self._attrs_before is not None and ENABLE_LOGGING:
                logger.warning(
                    f"_attrs_before is of type {type(self._attrs_before).__name__}, "
                    f"expected dict. Skipping attribute application."
                )
            elif self._attrs_before is None and ENABLE_LOGGING:
                 logger.debug("_attrs_before is None. No pre-connection attributes to apply.")
            return True # Exit if _attrs_before is not a dictionary or is None

        for key, value in self._attrs_before.items():
            ikey = None
            if isinstance(key, int):
                ikey = key
            elif isinstance(key, str) and key.isdigit():
                try:
                    ikey = int(key)
                except ValueError:
                    if ENABLE_LOGGING:
                        logger.debug(
                            f"Skipping attribute with key '{key}' in attrs_before: "
                            f"could not convert string to int."
                        )
                    continue # Skip if string key is not a valid integer
            else:
                if ENABLE_LOGGING:
                    logger.debug(
                        f"Skipping attribute with key '{key}' in attrs_before due to "
                        f"unsupported key type: {type(key).__name__}. Expected int or string representation of an int."
                    )
                continue  # Skip keys that are not int or string representation of an int

            if ikey == ddbc_sql_const.SQL_COPT_SS_ACCESS_TOKEN.value:
                if ENABLE_LOGGING:
                    logger.info(
                        f"Found attribute {ddbc_sql_const.SQL_COPT_SS_ACCESS_TOKEN.value}. Attempting to set it."
                    )
                self._set_connection_attributes(ikey, value)
                if ENABLE_LOGGING:
                    logger.info(
                        f"Call to set attribute {ddbc_sql_const.SQL_COPT_SS_ACCESS_TOKEN.value} with value '{value}' completed."
                    )
                # If you expect only one such key, you could add 'break' here.
            else:
                if ENABLE_LOGGING:
                    logger.debug(
                        f"Ignoring attribute with key '{key}' (resolved to {ikey}) in attrs_before "
                        f"as it is not the target attribute ({ddbc_sql_const.SQL_COPT_SS_ACCESS_TOKEN.value})."
                    )
        return True

    def _allocate_environment_handle(self):
        """
        Allocate the environment handle.
        """
        ret, handle = ddbc_bindings.DDBCSQLAllocHandle(
            ddbc_sql_const.SQL_HANDLE_ENV.value,  # SQL environment handle type
            None
        )
        check_error(ddbc_sql_const.SQL_HANDLE_ENV.value, handle, ret)
        self.henv = handle

    def _set_environment_attributes(self):
        """
        Set the environment attributes.
        """
        ret = ddbc_bindings.DDBCSQLSetEnvAttr(
            self.henv,  # Use the wrapper class
            ddbc_sql_const.SQL_ATTR_DDBC_VERSION.value,  # Attribute
            ddbc_sql_const.SQL_OV_DDBC3_80.value,  # String Length
            0,  # Null-terminated string
        )
        check_error(ddbc_sql_const.SQL_HANDLE_ENV.value, self.henv, ret)

    def _allocate_connection_handle(self):
        """
        Allocate the connection handle.
        """
        ret, handle = ddbc_bindings.DDBCSQLAllocHandle(
            ddbc_sql_const.SQL_HANDLE_DBC.value,  # SQL connection handle type
            self.henv
        )
        check_error(ddbc_sql_const.SQL_HANDLE_DBC.value, handle, ret)
        self.hdbc = handle

    def _set_connection_attributes(self, ikey: int, ivalue: any) -> None:
        """
        Set the connection attributes before connecting.

        Args:
            ikey (int): The attribute key to set.
            ivalue (Any): The value to set for the attribute. Can be bytes, bytearray, int, or unicode.
            vallen (int): The length of the value.

        Raises:
            DatabaseError: If there is an error while setting the connection attribute.
        """

        ret = ddbc_bindings.DDBCSQLSetConnectAttr(
            self.hdbc,  # Connection handle
            ikey,  # Attribute
            ivalue,  # Value
        )
        check_error(ddbc_sql_const.SQL_HANDLE_DBC.value, self.hdbc, ret)

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
        if ENABLE_LOGGING:
            logger.info("Connecting to the database")
        ret = ddbc_bindings.DDBCSQLDriverConnect(
            self.hdbc,  # Connection handle (wrapper)
            0,  # Window handle
            self.connection_str,  # Connection string
        )
        check_error(ddbc_sql_const.SQL_HANDLE_DBC.value, self.hdbc, ret)
        if ENABLE_LOGGING:
            logger.info("Connection established successfully.")

    @property
    def autocommit(self) -> bool:
        """
        Return the current autocommit mode of the connection.
        Returns:
            bool: True if autocommit is enabled, False otherwise.
        """
        autocommit_mode = ddbc_bindings.DDBCSQLGetConnectionAttr(
            self.hdbc,  # Connection handle (wrapper)
            ddbc_sql_const.SQL_ATTR_AUTOCOMMIT.value,  # Attribute
        )
        check_error(
            ddbc_sql_const.SQL_HANDLE_DBC.value, self.hdbc, autocommit_mode
        )
        return autocommit_mode == ddbc_sql_const.SQL_AUTOCOMMIT_ON.value

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        """
        Set the autocommit mode of the connection.
        Args:
            value (bool): True to enable autocommit, False to disable it.
        Returns:
            None
        Raises:
            DatabaseError: If there is an error while setting the autocommit mode.
        """
        ret = ddbc_bindings.DDBCSQLSetConnectAttr(
            self.hdbc,  # Connection handle
            ddbc_sql_const.SQL_ATTR_AUTOCOMMIT.value,  # Attribute
            (
                ddbc_sql_const.SQL_AUTOCOMMIT_ON.value
                if value
                else ddbc_sql_const.SQL_AUTOCOMMIT_OFF.value
            ),  # Value
        )
        check_error(ddbc_sql_const.SQL_HANDLE_DBC.value, self.hdbc, ret)
        self._autocommit = value
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
        self.autocommit = value

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
        if self._is_closed():
            # Cannot create a cursor if the connection is closed
            raise Exception("Connection is closed. Cannot create cursor.")
        return Cursor(self)

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
        if self._is_closed():
            # Cannot commit if the connection is closed
            raise Exception("Connection is closed. Cannot commit.")

        # Commit the current transaction
        ret = ddbc_bindings.DDBCSQLEndTran(
            ddbc_sql_const.SQL_HANDLE_DBC.value,  # Handle type
            self.hdbc,  # Connection handle (wrapper)
            ddbc_sql_const.SQL_COMMIT.value,  # Commit the transaction
        )
        check_error(ddbc_sql_const.SQL_HANDLE_DBC.value, self.hdbc, ret)
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
        if self._is_closed():
            # Cannot roll back if the connection is closed
            raise Exception("Connection is closed. Cannot roll back.")

        # Roll back the current transaction
        ret = ddbc_bindings.DDBCSQLEndTran(
            ddbc_sql_const.SQL_HANDLE_DBC.value,  # Handle type
            self.hdbc,  # Connection handle (wrapper)
            ddbc_sql_const.SQL_ROLLBACK.value,  # Roll back the transaction
        )
        check_error(ddbc_sql_const.SQL_HANDLE_DBC.value, self.hdbc, ret)
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
        if self._is_closed():
            # Connection is already closed
            return
        # Disconnect from the database
        ret = ddbc_bindings.DDBCSQLDisconnect(self.hdbc)
        check_error(ddbc_sql_const.SQL_HANDLE_DBC.value, self.hdbc, ret)

        # Set the reference to None to trigger destructor
        self.hdbc.free()
        self.hdbc = None

        if ENABLE_LOGGING:
            logger.info("Connection closed successfully.")
