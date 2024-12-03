from connection import Connection 
import ctypes
from helper import get_dll_path
from constants import ConstantsODBC as odbc_sql_const, ConstantsSQLSTATE as sqlstate
from exceptions import DatabaseError, InterfaceError
import logging
from logging_config import setup_logging

# Setting up logging
setup_logging()

def connect(connection_str: str) -> Connection:
        """
        Constructor for creating a connection to the database.

        Args:
            connection_str (str): The connection_str to connect to.

        Returns:
            Connection: A new connection object to interact with the database.

        Raises:
            DatabaseError: If there is an error while trying to connect to the database.
            InterfaceError: If there is an error related to the database interface.

        This function provides a way to create a new connection object, which can then
        be used to perform database operations such as executing queries, committing
        transactions, and closing the connection.
        """
        try:
            conn = Connection(connection_str)
            conn._connect_to_db()
            return conn
        except DatabaseError as e:
            logging.error(f"Database error occurred while connecting to the database: {e}")
            raise DatabaseError(f"Database error occurred while connecting to the database: {e}")
        except InterfaceError as e:
            logging.error(f"Interface error occurred while connecting to the database: {e}")
            raise InterfaceError(f"Interface error occurred while connecting to the database: {e}")
        except Exception as e:
            logging.error(f"An error occurred while connecting to the database: {e}")
            raise Exception(f"An error occurred while connecting to the database: {e}")
        

class ODBCInitializer:
    """
    A class to manage the initialization of the ODBC environment and connection handles.
    This class provides methods to allocate the environment and connection handles,
    set environment attributes, and check the return codes of ODBC function calls.
    """
    def __init__(self):
        self.odbc = ctypes.windll.LoadLibrary(get_dll_path("msodbcsql18.dll"))
        self.henv = ctypes.c_void_p()
        self.hdbc = ctypes.c_void_p()
        self.buffer_length = 1024
        self.buffer = ctypes.create_string_buffer(self.buffer_length)
        self.indicator = ctypes.c_long()
        self._allocate_environment_handle()
        self._set_environment_attributes()
        self._allocate_connection_handle()

    def _check_ret(self, return_code, handle_type, handle):
        """
        Check the return code from an ODBC function call and handle any errors.

        Args:
            return_code (int): The return code from the ODBC function call.
            handle_type (int): The type of handle (e.g., SQL_HANDLE_ENV, SQL_HANDLE_DBC).
            handle (ctypes.c_void_p): The handle to check for errors.

        Raises:
            Exception: If an error is detected in the ODBC function call.
        """
        if return_code not in (
            odbc_sql_const.SQL_SUCCESS.value,
            odbc_sql_const.SQL_SUCCESS_WITH_INFO.value,
            odbc_sql_const.SQL_STILL_EXECUTING.value,
            odbc_sql_const.SQL_NO_DATA.value
        ):
            sql_state = ctypes.create_unicode_buffer(6)
            native_error = ctypes.c_int()
            message_text = ctypes.create_unicode_buffer(2048)
            text_length = ctypes.c_short()

            diag_return_code = self.odbc.SQLGetDiagRecW(
                handle_type,
                handle,
                self.odbc.SQLSMALLINT(1),
                sql_state,
                ctypes.byref(native_error),
                message_text,
                ctypes.sizeof(message_text),
                ctypes.byref(text_length)
            )

            if diag_return_code in (odbc_sql_const.SQL_SUCCESS.value, odbc_sql_const.SQL_SUCCESS_WITH_INFO.value):
                if sql_state.value == sql_state.GENERAL_WARNING.value:
                    logging.info("General notification: %s", message_text.value)
                    return
                raise Exception(f"ODBC error: {message_text.value} (SQL State: {sql_state.value})")
            else:
                logging.error("SQLGetDiagRecW failed with return code: %d", diag_return_code)
                raise Exception(f"Failed to retrieve diagnostic information. Return code: {diag_return_code}, ODBC return code: {return_code}")

    def _allocate_environment_handle(self):
        """
        Allocate the ODBC environment handle.
        """
        ret = self.odbc.SQLAllocHandle(odbc_sql_const.SQL_HANDLE_ENV.value, None, ctypes.byref(self.henv))
        self._check_ret(ret, odbc_sql_const.SQL_HANDLE_ENV, self.henv)

    def _set_environment_attributes(self):
        """
        Set the ODBC environment attributes.
        """
        ret = self.odbc.SQLSetEnvAttr(self.henv, odbc_sql_const.SQL_ATTR_ODBC_VERSION.value, ctypes.c_void_p(odbc_sql_const.SQL_OV_ODBC3_80.value), len(odbc_sql_const.SQL_OV_ODBC3_80.value))
        self._check_ret(ret, odbc_sql_const.SQL_HANDLE_ENV.value, self.henv)

    def _allocate_connection_handle(self):
        """
        Allocate the ODBC connection handle.
        """
        ret = self.odbc.SQLAllocHandle(odbc_sql_const.SQL_HANDLE_DBC.value, self.henv, ctypes.byref(self.hdbc))
        self._check_ret(ret, odbc_sql_const.SQL_HANDLE_DBC.value, self.hdbc)