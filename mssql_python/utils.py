from connection import Connection 
import ctypes
import os
from constants import ConstantsODBC as odbc_sql_const, ConstantsSQLSTATE as sqlstate
from exceptions import DatabaseError, InterfaceError
import logging
from logging_config import setup_logging
from mssql_python import odbc

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

def get_odbc_dll_path(dll_name):
    """
    Get the full path to the specified ODBC DLL.
    
    Args:   
        dll_name (str): The name of the ODBC DLL to locate.
        
    Returns:
        str: The full path to the specified ODBC DLL.
    
    Raises:
        FileNotFoundError: If the specified DLL is not found.
    """
    try:
        # Get the directory of the current script
        current_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Construct the path to the DLLs directory
        dlls_dir = os.path.join(current_dir, "msodbcsql_dlls")
        
        # Construct the full path to the DLL
        dll_path = os.path.join(dlls_dir, dll_name)
        
        # Check if the DLL exists
        if not os.path.exists(dll_path):
            raise FileNotFoundError(f"The specified DLL '{dll_name}' was not found in '{dlls_dir}'")
        
        return dll_path
    except Exception as e:
        raise Exception(f"An error occurred while getting the DLL path: {e}")

def add_driver_to_connection_str(connection_str):
    """
    Add the ODBC driver to the connection string if not present.

    Args:
        connection_str (str): The original connection string.

    Returns:
        str: The connection string with the ODBC driver added.

    Raises:
        Exception: If the connection string is invalid.
    """
    driver_name = 'Driver={ODBC Driver 18 for SQL Server}'
    try:
        # Strip any leading or trailing whitespace from the connection string
        connection_str = connection_str.strip()
        
        # Split the connection string into individual attributes
        connection_attributes = connection_str.split(';')
        final_connection_attributes = []
        
        # Iterate through the attributes and exclude any existing driver attribute
        for attribute in connection_attributes:
            if attribute.split('=').lower() == 'driver':
                continue
            final_connection_attributes.append(attribute)
        
        # Join the remaining attributes back into a connection string
        connection_str = ';'.join(final_connection_attributes)
        
        # Insert the driver attribute at the beginning of the connection string
        final_connection_attributes.insert(0, driver_name)
        connection_str = ';'.join(final_connection_attributes)
    except Exception as e:
        raise Exception(("Invalid connection string, Please follow the format: "
                         "Server=server_name;Database=database_name;UID=user_name;PWD=password"))
    
    return connection_str     

def check_ret(self, return_code, handle_type, handle):
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

        diag_return_code = odbc.SQLGetDiagRecW(
            handle_type,
            handle,
            odbc.SQLSMALLINT(1),
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