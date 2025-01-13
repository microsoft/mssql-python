
import logging
from mssql_python.logging_config import setup_logging
from mssql_python.exceptions import DatabaseError, InterfaceError
from mssql_python.connection import Connection

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
        logging.info(f"Connecting to the database")
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