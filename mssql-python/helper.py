import os

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