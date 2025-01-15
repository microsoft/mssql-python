from mssql_python.constants import ConstantsODBC
from mssql_python import ddbc_bindings
    
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
            if attribute.lower().split('=')[0] == 'driver':
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

def check_error(handle_type, handle, ret):
    """
    Check for errors and raise an exception if an error is found.

    Args:
        handle_type: The type of the handle (e.g., SQL_HANDLE_ENV, SQL_HANDLE_DBC).
        handle: The handle to check for errors.
        ret: The return code from the ODBC function call.

    Raises:
        RuntimeError: If an error is found.
    """
    if ret == ConstantsODBC.SQL_ERROR.value:
        e = ddbc_bindings.CheckError(handle_type, handle, ret)
        raise RuntimeError(f"Failed to allocate SQL connection handle. Error code: {e}")