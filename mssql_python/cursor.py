"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
This module contains the Cursor class, which represents a database cursor.
"""
import ctypes
import decimal
import uuid
import datetime
from typing import List, Union, Optional, Tuple, Any, Sequence
from mssql_python.constants import ConstantsDDBC as ddbc_sql_const
from mssql_python.helpers import check_error
from mssql_python.logging_config import get_logger, ENABLE_LOGGING
from mssql_python import ddbc_bindings
from .row import Row

logger = get_logger()


class Cursor:
    """
    Represents a database cursor, which is used to manage the context of a fetch operation.

    Attributes:
        connection: Database connection object.
        description: Sequence of 7-item sequences describing one result column.
        rowcount: Number of rows produced or affected by the last execute operation.
        arraysize: Number of rows to fetch at a time with fetchmany().

    Methods:
        __init__(connection_str) -> None.
        callproc(procname, parameters=None) -> 
            Modified copy of the input sequence with output parameters.
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

    def __init__(self, connection) -> None:
        """
        Initialize the cursor with a database connection.

        Args:
            connection: Database connection object.
        """
        self.connection = connection
        # Create the C++ cursor object
        self._cursor = ddbc_bindings.Cursor(self.connection._conn)
        self.closed = False

    def _is_unicode_string(self, param):
        """
        Check if a string contains non-ASCII characters.

        Args:
            param: The string to check.

        Returns:
            True if the string contains non-ASCII characters, False otherwise.
        """
        try:
            param.encode("ascii")
            return False  # Can be encoded to ASCII, so not Unicode
        except UnicodeEncodeError:
            return True  # Contains non-ASCII characters, so treat as Unicode

    def _parse_date(self, param):
        """
        Attempt to parse a string as a date.

        Args:
            param: The string to parse.

        Returns:
            A datetime.date object if parsing is successful, else None.
        """
        formats = ["%Y-%m-%d"]
        for fmt in formats:
            try:
                return datetime.datetime.strptime(param, fmt).date()
            except ValueError:
                continue
        return None

    def _parse_datetime(self, param):
        """
        Attempt to parse a string as a datetime, smalldatetime, datetime2, timestamp.

        Args:
            param: The string to parse.

        Returns:
            A datetime.datetime object if parsing is successful, else None.
        """
        formats = [
            "%Y-%m-%dT%H:%M:%S.%f",  # ISO 8601 datetime with fractional seconds
            "%Y-%m-%dT%H:%M:%S",  # ISO 8601 datetime
            "%Y-%m-%d %H:%M:%S.%f",  # Datetime with fractional seconds
            "%Y-%m-%d %H:%M:%S",  # Datetime without fractional seconds
        ]
        for fmt in formats:
            try:
                return datetime.datetime.strptime(param, fmt)  # Valid datetime
            except ValueError:
                continue  # Try next format

        return None  # If all formats fail, return None

    def _parse_time(self, param):
        """
        Attempt to parse a string as a time.

        Args:
            param: The string to parse.

        Returns:
            A datetime.time object if parsing is successful, else None.
        """
        formats = [
            "%H:%M:%S",  # Time only
            "%H:%M:%S.%f",  # Time with fractional seconds
        ]
        for fmt in formats:
            try:
                return datetime.datetime.strptime(param, fmt).time()
            except ValueError:
                continue
        return None
    
    def _get_numeric_data(self, param):
        """
        Get the data for a numeric parameter.

        Args:
            param: The numeric parameter.

        Returns:
            numeric_data: A NumericData struct containing 
            the numeric data.
        """
        decimal_as_tuple = param.as_tuple()
        num_digits = len(decimal_as_tuple.digits)
        exponent = decimal_as_tuple.exponent

        # Calculate the SQL precision & scale
        #   precision = no. of significant digits
        #   scale     = no. digits after decimal point
        if exponent >= 0:
            # digits=314, exp=2 ---> '31400' --> precision=5, scale=0
            precision = num_digits + exponent
            scale = 0
        elif (-1 * exponent) <= num_digits:
            # digits=3140, exp=-3 ---> '3.140' --> precision=4, scale=3
            precision = num_digits
            scale = exponent * -1
        else:
            # digits=3140, exp=-5 ---> '0.03140' --> precision=5, scale=5
            # TODO: double check the precision calculation here with SQL documentation
            precision = exponent * -1
            scale = exponent * -1

        # TODO: Revisit this check, do we want this restriction?
        if precision > 15:
            raise ValueError(
                "Precision of the numeric value is too high - "
                + str(param)
                + ". Should be less than or equal to 15"
            )
        Numeric_Data = ddbc_bindings.NumericData
        numeric_data = Numeric_Data()
        numeric_data.scale = scale
        numeric_data.precision = precision
        numeric_data.sign = 1 if decimal_as_tuple.sign == 0 else 0
        # strip decimal point from param & convert the significant digits to integer
        # Ex: 12.34 ---> 1234
        val = str(param)
        if "." in val or "-" in val:
            val = val.replace(".", "")
            val = val.replace("-", "")
        val = int(val)
        numeric_data.val = val
        return numeric_data

    def _map_sql_type(self, param, parameters_list, i):
        """
        Map a Python data type to the corresponding SQL type, 
        C type, Column size, and Decimal digits.
        Takes:
            - param: The parameter to map.
            - parameters_list: The list of parameters to bind.
            - i: The index of the parameter in the list.
        Returns:
            - A tuple containing the SQL type, C type, column size, and decimal digits.
        """
        if param is None:
            return (
                ddbc_sql_const.SQL_VARCHAR.value, # TODO: Add SQLDescribeParam to get correct type
                ddbc_sql_const.SQL_C_DEFAULT.value,
                1,
                0,
            )

        if isinstance(param, bool):
            return ddbc_sql_const.SQL_BIT.value, ddbc_sql_const.SQL_C_BIT.value, 1, 0

        if isinstance(param, int):
            if 0 <= param <= 255:
                return (
                    ddbc_sql_const.SQL_TINYINT.value,
                    ddbc_sql_const.SQL_C_TINYINT.value,
                    3,
                    0,
                )
            if -32768 <= param <= 32767:
                return (
                    ddbc_sql_const.SQL_SMALLINT.value,
                    ddbc_sql_const.SQL_C_SHORT.value,
                    5,
                    0,
                )
            if -2147483648 <= param <= 2147483647:
                return (
                    ddbc_sql_const.SQL_INTEGER.value,
                    ddbc_sql_const.SQL_C_LONG.value,
                    10,
                    0,
                )
            return (
                ddbc_sql_const.SQL_BIGINT.value,
                ddbc_sql_const.SQL_C_SBIGINT.value,
                19,
                0,
            )

        if isinstance(param, float):
            return (
                ddbc_sql_const.SQL_DOUBLE.value,
                ddbc_sql_const.SQL_C_DOUBLE.value,
                15,
                0,
            )

        if isinstance(param, decimal.Decimal):
            parameters_list[i] = self._get_numeric_data(
                param
            )  # Replace the parameter with the dictionary
            return (
                ddbc_sql_const.SQL_NUMERIC.value,
                ddbc_sql_const.SQL_C_NUMERIC.value,
                parameters_list[i].precision,
                parameters_list[i].scale,
            )

        if isinstance(param, str):
            if (
                param.startswith("POINT")
                or param.startswith("LINESTRING")
                or param.startswith("POLYGON")
            ):
                return (
                    ddbc_sql_const.SQL_WVARCHAR.value,
                    ddbc_sql_const.SQL_C_WCHAR.value,
                    len(param),
                    0,
                )

            # Attempt to parse as date, datetime, datetime2, timestamp, smalldatetime or time
            if self._parse_date(param):
                parameters_list[i] = self._parse_date(
                    param
                )  # Replace the parameter with the date object
                return (
                    ddbc_sql_const.SQL_DATE.value,
                    ddbc_sql_const.SQL_C_TYPE_DATE.value,
                    10,
                    0,
                )
            if self._parse_datetime(param):
                parameters_list[i] = self._parse_datetime(param)
                return (
                    ddbc_sql_const.SQL_TIMESTAMP.value,
                    ddbc_sql_const.SQL_C_TYPE_TIMESTAMP.value,
                    26,
                    6,
                )
            if self._parse_time(param):
                parameters_list[i] = self._parse_time(param)
                return (
                    ddbc_sql_const.SQL_TIME.value,
                    ddbc_sql_const.SQL_C_TYPE_TIME.value,
                    8,
                    0,
                )

            # String mapping logic here
            is_unicode = self._is_unicode_string(param)
            # TODO: revisit
            if len(param) > 4000:  # Long strings
                if is_unicode:
                    return (
                        ddbc_sql_const.SQL_WLONGVARCHAR.value,
                        ddbc_sql_const.SQL_C_WCHAR.value,
                        len(param),
                        0,
                    )
                return (
                    ddbc_sql_const.SQL_LONGVARCHAR.value,
                    ddbc_sql_const.SQL_C_CHAR.value,
                    len(param),
                    0,
                )
            if is_unicode:  # Short Unicode strings
                return (
                    ddbc_sql_const.SQL_WVARCHAR.value,
                    ddbc_sql_const.SQL_C_WCHAR.value,
                    len(param),
                    0,
                )
            return (
                ddbc_sql_const.SQL_VARCHAR.value,
                ddbc_sql_const.SQL_C_CHAR.value,
                len(param),
                0,
            )

        if isinstance(param, bytes):
            if len(param) > 8000:  # Assuming VARBINARY(MAX) for long byte arrays
                return (
                    ddbc_sql_const.SQL_VARBINARY.value,
                    ddbc_sql_const.SQL_C_BINARY.value,
                    len(param),
                    0,
                )
            return (
                ddbc_sql_const.SQL_BINARY.value,
                ddbc_sql_const.SQL_C_BINARY.value,
                len(param),
                0,
            )

        if isinstance(param, bytearray):
            if len(param) > 8000:  # Assuming VARBINARY(MAX) for long byte arrays
                return (
                    ddbc_sql_const.SQL_VARBINARY.value,
                    ddbc_sql_const.SQL_C_BINARY.value,
                    len(param),
                    0,
                )
            return (
                ddbc_sql_const.SQL_BINARY.value,
                ddbc_sql_const.SQL_C_BINARY.value,
                len(param),
                0,
            )

        if isinstance(param, datetime.datetime):
            return (
                ddbc_sql_const.SQL_TIMESTAMP.value,
                ddbc_sql_const.SQL_C_TYPE_TIMESTAMP.value,
                26,
                6,
            )

        if isinstance(param, datetime.date):
            return (
                ddbc_sql_const.SQL_DATE.value,
                ddbc_sql_const.SQL_C_TYPE_DATE.value,
                10,
                0,
            )

        if isinstance(param, datetime.time):
            return (
                ddbc_sql_const.SQL_TIME.value,
                ddbc_sql_const.SQL_C_TYPE_TIME.value,
                8,
                0,
            )

        return (
            ddbc_sql_const.SQL_VARCHAR.value,
            ddbc_sql_const.SQL_C_CHAR.value,
            len(str(param)),
            0,
        )

    def __del__(self):
        """
        Destructor to ensure proper cleanup of resources when the cursor is garbage collected.
        This prevents segfaults when cursors are not explicitly closed.
        """
        try:
            if hasattr(self, 'closed') and not self.closed and hasattr(self, '_cursor'):
                # Check if ddbc_bindings module is still available
                import sys
                if 'mssql_python.ddbc_bindings' in sys.modules:
                    self.close()
        except (AttributeError, ImportError, TypeError):
            # During Python shutdown, modules may be None or unavailable
            # Just ignore cleanup errors to prevent exceptions during garbage collection
            pass
        except Exception:
            # Catch any other exceptions during cleanup
            pass
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures cursor is always closed."""
        self.close()
        return False

    def close(self) -> None:
        """
        Close the cursor now (rather than whenever __del__ is called).

        Raises:
            Error: If any operation is attempted with the cursor after it is closed.
        """
        if self.closed:
            return
        
        if hasattr(self, '_cursor'):
            self._cursor.close()
            
        self.closed = True

    def _check_closed(self):
        """
        Check if the cursor is closed and raise an exception if it is.

        Raises:
            Error: If the cursor is closed.
        """
        if self.closed:
            raise Exception("Operation cannot be performed: the cursor is closed.")

    # _create_parameter_types_list method removed as it's no longer needed with C++ implementation

    def _initialize_description(self):
        """
        Initialize the description attribute using the C++ implementation.
        
        This method is kept for backward compatibility but now uses
        the C++ cursor implementation to get the description.
        """
        if hasattr(self, '_description_cache'):
            self._description_cache = None
        
        # The description will be lazily loaded from the C++ implementation
        # when the description property is accessed

    def _map_data_type(self, sql_type):
        """
        Map SQL data type to Python data type.

        Args:
            sql_type: SQL data type.

        Returns:
            Corresponding Python data type.
        """
        sql_to_python_type = {
            ddbc_sql_const.SQL_INTEGER.value: int,
            ddbc_sql_const.SQL_VARCHAR.value: str,
            ddbc_sql_const.SQL_WVARCHAR.value: str,
            ddbc_sql_const.SQL_CHAR.value: str,
            ddbc_sql_const.SQL_WCHAR.value: str,
            ddbc_sql_const.SQL_FLOAT.value: float,
            ddbc_sql_const.SQL_DOUBLE.value: float,
            ddbc_sql_const.SQL_DECIMAL.value: decimal.Decimal,
            ddbc_sql_const.SQL_NUMERIC.value: decimal.Decimal,
            ddbc_sql_const.SQL_DATE.value: datetime.date,
            ddbc_sql_const.SQL_TIMESTAMP.value: datetime.datetime,
            ddbc_sql_const.SQL_TIME.value: datetime.time,
            ddbc_sql_const.SQL_BIT.value: bool,
            ddbc_sql_const.SQL_TINYINT.value: int,
            ddbc_sql_const.SQL_SMALLINT.value: int,
            ddbc_sql_const.SQL_BIGINT.value: int,
            ddbc_sql_const.SQL_BINARY.value: bytes,
            ddbc_sql_const.SQL_VARBINARY.value: bytes,
            ddbc_sql_const.SQL_LONGVARBINARY.value: bytes,
            ddbc_sql_const.SQL_GUID.value: uuid.UUID,
            # Add more mappings as needed
        }
        return sql_to_python_type.get(sql_type, str)

    def execute(
        self,
        operation: str,
        parameters=None,
        use_prepare: bool = True,
        reset_cursor: bool = True
    ) -> None:
        """
        Prepare and execute a database operation (query or command).

        Args:
            operation: SQL query or command.
            parameters: Sequence of parameters to bind.
            use_prepare: Whether to use SQLPrepareW (default) or SQLExecDirectW.
            reset_cursor: Whether to reset the cursor before execution.
        """
        self._check_closed()  # Check if the cursor is closed
        
        # Clear any cached description
        if hasattr(self, '_description_cache'):
            self._description_cache = None
            
        # Prepare parameters if needed
        if parameters is None:
            parameters = []
            
        # Flatten parameters if a single tuple or list is passed
        if len(parameters) == 1 and isinstance(parameters[0], (tuple, list)):
            parameters = parameters[0]

        parameters = list(parameters)
        
        if ENABLE_LOGGING:
            logger.debug("Executing query: %s", operation)
            for i, param in enumerate(parameters):
                logger.debug(
                    "Parameter number: %s, Parameter: %s, Param Python Type: %s",
                    i + 1,
                    param,
                    str(type(param))
                )
                
        # Use the C++ implementation to execute the query
        self._cursor.execute(operation, parameters)
        
        # Clear cached description
        if hasattr(self, '_description_cache'):
            self._description_cache = None
            
        # Description and rowcount are now automatically handled by the C++ implementation

    def executemany(self, operation: str, seq_of_parameters: list) -> None:
        """
        Prepare a database operation and execute it against all parameter sequences.

        Args:
            operation: SQL query or command.
            seq_of_parameters: Sequence of sequences or mappings of parameters.

        Raises:
            Error: If the operation fails.
        """
        self._check_closed()  # Check if the cursor is closed
        
        # Clear any cached description
        if hasattr(self, '_description_cache'):
            self._description_cache = None
            
        # Use the C++ implementation
        self._cursor.executemany(operation, seq_of_parameters)

    def fetchone(self) -> Union[None, Row]:
        """
        Fetch the next row of a query result set.
        
        Returns:
            Single Row object or None if no more data is available.
        """
        self._check_closed()  # Check if the cursor is closed
        
        # Use the C++ implementation to get raw data
        row_data = self._cursor.fetchone()
        
        # Convert raw data to Row object if it's not None
        if row_data is not None:
            return Row(row_data, self.description)
        return None

    def fetchmany(self, size: int = None) -> List[Row]:
        """
        Fetch the next set of rows of a query result.
        
        Args:
            size: Number of rows to fetch at a time.
        
        Returns:
            List of Row objects.
        """
        self._check_closed()  # Check if the cursor is closed

        if size is None:
            size = self.arraysize

        # Use the C++ implementation to get raw data
        rows_data = self._cursor.fetchmany(size)
        
        # Convert raw data to Row objects
        return [Row(row_data, self.description) for row_data in rows_data]

    def fetchall(self) -> List[Row]:
        """
        Fetch all (remaining) rows of a query result.
        
        Returns:
            List of Row objects.
        """
        self._check_closed()  # Check if the cursor is closed
        
        # Use the C++ implementation
        rows_data = self._cursor.fetchall()
        
        # Convert raw data to Row objects
        return [Row(row_data, self.description) for row_data in rows_data]

    def nextset(self) -> Union[bool, None]:
        """
        Skip to the next available result set.

        Returns:
            True if there is another result set, None otherwise.

        Raises:
            Error: If the previous call to execute did not produce any result set.
        """
        self._check_closed()  # Check if the cursor is closed
        
        # Use the C++ implementation
        return self._cursor.nextset()
        
    def setinputsizes(self, sizes) -> None:
        """
        Set sizes of input parameters.
        
        This method is optional since the database interface should adapt
        to input parameters as needed. If implemented, it should be called
        before execute() or executemany().
        
        Args:
            sizes: A sequence of type information for input parameters.
        """
        self._check_closed()  # Check if the cursor is closed
        
        # Use the C++ implementation
        self._cursor.setinputsizes(sizes)
        
    def setoutputsize(self, size, column=None) -> None:
        """
        Set a column buffer size for fetching large data items.
        
        This method is optional since the interface should adapt as needed.
        
        Args:
            size: The buffer size.
            column: The column index (starting at 0). If None, applies to all columns.
        """
        self._check_closed()  # Check if the cursor is closed
        
        # Use the C++ implementation
        col_idx = -1 if column is None else column
        self._cursor.setoutputsize(size, col_idx)
        
    @property
    def description(self):
        """
        Read-only attribute providing information about each result column.

        Returns:
            Sequence of 7-item sequences describing one result column, or None.
            The 7 items are: (name, type_code, display_size, internal_size, precision, scale, null_ok)
        """
        if not hasattr(self, '_description_cache') or self._description_cache is None:
            if not self.closed and hasattr(self, '_cursor'):
                self._description_cache = self._cursor.getDescription()
            else:
                self._description_cache = None
        return self._description_cache
        
    @property
    def rowcount(self):
        """
        Read-only attribute specifying the number of rows affected by the last operation.

        Returns:
            Number of rows affected by the last execute call, or -1 when not determined.
        """
        if self.closed:
            return -1
        return self._cursor.getRowCount()
        
    @property
    def arraysize(self):
        """
        Read/write attribute specifying the number of rows to fetch at a time with fetchmany().

        Returns:
            The current arraysize.
        """
        if not hasattr(self, '_arraysize'):
            self._arraysize = 1
        return self._arraysize
        
    @arraysize.setter
    def arraysize(self, value):
        """
        Set the number of rows to fetch at a time with fetchmany().
        
        Args:
            value: The new arraysize.
        """
        if value <= 0:
            raise ValueError("arraysize must be positive")
        self._arraysize = value
        if hasattr(self, '_cursor') and not self.closed:
            self._cursor.setArraySize(value)