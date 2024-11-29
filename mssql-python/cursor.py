from typing import Union

class Cursor:
    """
    Represents a database cursor, which is used to manage the context of a fetch operation.

    Attributes:
        connection: Database connection object.
        description: Sequence of 7-item sequences describing one result column.
        rowcount: Number of rows produced or affected by the last execute operation.
        arraysize: Number of rows to fetch at a time with fetchmany().

    Methods:
        __init__(connection) -> None.
        callproc(procname, parameters=None) -> Modified copy of the input sequence with output parameters.
        close() -> None.
        execute(operation, parameters=None) -> None.
        executemany(operation, seq_of_parameters) -> None.
        fetchone() -> Single sequence or None if no more data is available.
        fetchmany(size=None) -> Sequence of sequences (e.g. list of tuples).
        fetchall() -> Sequence of sequences (e.g. list of tuples).
        nextset() -> True if there is another result set, None otherwise.
        setinputsizes(sizes) -> None.
        setoutputsize(size, column=None) -> None.
    """

    def __init__(self, connection_str: str) -> None:
        """
        Initialize the cursor with a database connection.
        
        Args:
            connection_str (str): Database connection object.
        """
        self.connection = connection_str
        self.description = None
        self.rowcount = -1
        self.arraysize = 1

    def callproc(self, procname: str, parameters: Union[None, list] = None) -> Union[None, list]:
        """
        Call a stored database procedure with the given name.
        
        Args:
            procname (str): Name of the stored procedure.
            parameters: Sequence of parameters for the procedure.
        
        Returns:
            Modified copy of the input sequence with output parameters.
        """
        pass

    def close(self) -> None:
        """
        Close the cursor now (rather than whenever __del__ is called).
        
        Raises:
            Error: If any operation is attempted with the cursor after it is closed.
        """
        pass

    def execute(self, operation: str, parameters: Union[None, list, dict] = None) -> None:
        """
        Prepare and execute a database operation (query or command).
        
        Args:
            operation (str): SQL query or command.
            parameters: Sequence or mapping of parameters.
        
        Raises:
            Error: If the operation fails.
        """
        pass

    def executemany(self, operation: str, seq_of_parameters: list) -> None:
        """
        Prepare a database operation and execute it against all parameter sequences.
        
        Args:
            operation: SQL query or command.
            seq_of_parameters: Sequence of sequences or mappings of parameters.
        
        Raises:
            Error: If the operation fails.
        """
        pass

    def fetchone(self) -> Union[None, tuple]:
        """
        Fetch the next row of a query result set.
        
        Returns:
            Single sequence or None if no more data is available.
        
        Raises:
            Error: If the previous call to execute did not produce any result set.
        """
        pass

    def fetchmany(self, size: int =None) -> list:
        """
        Fetch the next set of rows of a query result.
        
        Args:
            size: Number of rows to fetch per call.
        
        Returns:
            Sequence of sequences (e.g. list of tuples).
        
        Raises:
            Error: If the previous call to execute did not produce any result set.
        """
        pass

    def fetchall(self) -> list:
        """
        Fetch all (remaining) rows of a query result.
        
        Returns:
            Sequence of sequences (e.g. list of tuples).
        
        Raises:
            Error: If the previous call to execute did not produce any result set.
        """
        pass

    def nextset(self) -> Union[None, bool]:
        """
        Skip to the next available result set.
        
        Returns:
            True if there is another result set, None otherwise.
        
        Raises:
            Error: If the previous call to execute did not produce any result set.
        """
        pass

    def setinputsizes(self, sizes: list) -> None:
        """
        Predefine memory areas for the operation’s parameters.
        
        Args:
            sizes: Sequence of Type Objects or integers specifying maximum length of string parameters.
        """
        pass

    def setoutputsize(self, size: int, column: int=None) -> None:
        """
        Set a column buffer size for fetches of large columns.
        
        Args:
            size: Buffer size.
            column: Index of the column in the result sequence.
        """
        pass