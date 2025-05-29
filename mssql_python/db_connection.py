"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
This module provides a way to create a new connection object to interact with the database.
"""
import platform
if platform.system() == 'Windows':
    from mssql_python.connection import Connection
else:
    from mssql_python.connection_mac import Connection

def connect(connection_str: str = "", autocommit: bool = True, attrs_before: dict = None, **kwargs) -> Connection:
    """
    Constructor for creating a connection to the database.

    Args:
        connection_str (str): The connection string to connect to.
        autocommit (bool): If True, causes a commit to be performed after each SQL statement.
    TODO: Add the following parameters to the function signature:
        timeout (int): The timeout for the connection attempt, in seconds.
        readonly (bool): If True, the connection is set to read-only.
        attrs_before (dict): A dictionary of connection attributes to set before connecting.
    Keyword Args:
        **kwargs: Additional key/value pairs for the connection string.
    Below attributes are not implemented in the internal driver:
    - encoding (str): The encoding for the connection string.
    - ansi (bool): If True, indicates the driver does not support Unicode.

    Returns:
        Connection: A new connection object to interact with the database.

    Raises:
        DatabaseError: If there is an error while trying to connect to the database.
        InterfaceError: If there is an error related to the database interface.

    This function provides a way to create a new connection object, which can then
    be used to perform database operations such as executing queries, committing
    transactions, and closing the connection.
    """
    conn = Connection(connection_str, autocommit=autocommit, attrs_before=attrs_before, **kwargs)
    return conn
