"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
This module provides a way to create a new connection object to interact with the database.
"""

from typing import Any, Dict, Optional, Union

from mssql_python.connection import Connection


def connect(
    connection_str: str = "",
    autocommit: bool = False,
    attrs_before: Optional[Dict[int, Union[int, str, bytes]]] = None,
    timeout: int = 0,
    native_uuid: Optional[bool] = None,
    **kwargs: Any,
) -> Connection:
    """
    Constructor for creating a connection to the database.

    Args:
        connection_str (str): The connection string to connect to.
        autocommit (bool): If True, causes a commit to be performed after each SQL statement.
        attrs_before (dict, optional): A dictionary of connection attributes to set before
                                      connecting.
        timeout (int): The timeout for the connection attempt, in seconds.
        native_uuid (bool, optional): Controls whether UNIQUEIDENTIFIER columns return
            uuid.UUID objects (True) or str (False) for this connection.
            - True: UNIQUEIDENTIFIER columns return uuid.UUID objects.
            - False: UNIQUEIDENTIFIER columns return str (pyodbc-compatible).
            - None (default): Uses the module-level ``mssql_python.native_uuid`` setting (True).

            This per-connection override is useful for migration from pyodbc:
            connections that need string UUIDs can pass native_uuid=False, while the default (True)
            returns native uuid.UUID objects.
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
    conn = Connection(
        connection_str,
        autocommit=autocommit,
        attrs_before=attrs_before,
        timeout=timeout,
        native_uuid=native_uuid,
        **kwargs,
    )
    return conn
