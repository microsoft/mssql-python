from connection import Connection 

def connect(connection_str: str) -> 'Connection':
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
        conn = Connection(connection_str)
        conn._connect_to_db()
        return conn