import ctypes
import decimal, uuid
from typing import List, Union
from mssql_python.constants import ConstantsODBC as odbc_sql_const
from mssql_python.helpers import check_error
from mssql_python.logging_config import get_logger, ENABLE_LOGGING
import datetime
import decimal
import uuid
import os
from mssql_python.exceptions import raise_exception
from mssql_python import ddbc_bindings

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

    def __init__(self, connection) -> None:
        """
        Initialize the cursor with a database connection.
        
        Args:
            connection: Database connection object.
        """
        self.connection = connection
        # self.connection.autocommit = False
        self.hstmt = ctypes.c_void_p()
        self._initialize_cursor()
        self.description = None
        self.rowcount = -1
        self.arraysize = 1 # Default number of rows to fetch at a time is 1, user can change it
        self.buffer_length = 1024  # Default buffer length for string data
        self.closed = False  # Flag to indicate if the cursor is closed
        self.last_executed_stmt = "" # Stores the last statement executed by this cursor
        self.is_stmt_prepared = [False] # Indicates if last_executed_stmt was prepared by ddbc shim.
                                        # Is a list instead of a bool coz bools in Python are immutable.
                                        # Hence, we can't pass around bools by reference & modify them.
                                        # Therefore, it must be a list with exactly one bool element.

    def _is_unicode_string(self, param):
        """
        Check if a string contains non-ASCII characters.
        
        Args:
            param: The string to check.
        
        Returns:
            True if the string contains non-ASCII characters, False otherwise.
        """
        try:
            param.encode('ascii')
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
        Attempt to parse a string as a datetime.
        
        Args:
            param: The string to parse.
        
        Returns:
            A datetime.datetime object if parsing is successful, else None.
        """
        formats = [
            "%Y-%m-%dT%H:%M:%S.%f",     # ISO 8601 datetime with fractional seconds
            "%Y-%m-%dT%H:%M:%S",        # ISO 8601 datetime
            "%Y-%m-%d %H:%M:%S.%f",     # Datetime with fractional seconds (up to 3 digits)
            "%Y-%m-%d %H:%M:%S",        # Datetime without fractional seconds
        ]
        for fmt in formats:
            try:
                dt = datetime.datetime.strptime(param, fmt)

                # If there are fractional seconds, ensure they do not exceed 7 digits
                if fmt in ["%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S.%f"]:
                    fractional_part = param.split('.')[-1]
                    # If the fractional part is more than 3 digits, truncate to 3 digits
                    if len(fractional_part) > 3:
                        fractional_part = fractional_part[:3]
                        # Convert to microseconds
                        dt = dt.replace(microsecond=int(fractional_part.ljust(3, "0")) * 1000)
                return dt  # Valid datetime
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
            "%H:%M:%S",                 # Time only
            "%H:%M:%S.%f",              # Time with fractional seconds
        ]
        for fmt in formats:
            try:
                return datetime.datetime.strptime(param, fmt).time()
            except ValueError:
                continue
        return None
    
    def _parse_timestamptz(self, param):
        """
        Attempt to parse a string as a timestamp with time zone (timestamptz).
        
        Args:
            param: The string to parse.
        
        Returns:
            A datetime.datetime object if parsing is successful, else None.
        """
        formats = [
            "%Y-%m-%dT%H:%M:%S%z",      # ISO 8601 datetime with timezone offset
            "%Y-%m-%d %H:%M:%S.%f%z",   # Datetime with fractional seconds and timezone offset
        ]
        for fmt in formats:
            try:
                return datetime.datetime.strptime(param, fmt)
            except ValueError:
                continue
        return None

    def _parse_smalldatetime(self, param):
        """
        Attempt to parse a string as a smalldatetime.
        
        Args:
            param: The string to parse.
        
        Returns:
            A datetime.datetime object if parsing is successful, else None.
        """
        formats = [
            "%Y-%m-%d %H:%M:%S",        # Standard datetime
        ]
        for fmt in formats:
            try:
                return datetime.datetime.strptime(param, fmt)
            except ValueError:
                continue
        return None

    def _parse_datetime2(self, param):
        """
        Attempt to parse a string as a datetime2.
        
        Args:
            param: The string to parse.
        
        Returns:
            A datetime.datetime object if parsing is successful, else None.
        """
        formats = [
            "%Y-%m-%d %H:%M:%S.%f",     # Datetime with fractional seconds (up to 6 digits)
        ]
        for fmt in formats:
            try:
                dt = datetime.datetime.strptime(param, fmt)
                if fmt == "%Y-%m-%d %H:%M:%S.%f" and len(param.split('.')[-1]) > 3:
                    return dt
            except ValueError:
                continue
        return None

    def _get_numeric_data(self, param):
        """
        Get the data for a numeric parameter.
        
        Args:
            param: The numeric parameter. 
        
        Returns:
            A NumericData struct containing the numeric data.
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
            raise ValueError("Precision of the numeric value is too high - " + str(param) +
                             ". Should be less than or equal to 15")
        NumericData = ddbc_bindings.NumericData
        numeric_data = NumericData()
        numeric_data.scale = scale
        numeric_data.precision = precision
        numeric_data.sign = 1 if decimal_as_tuple.sign == 0 else 0
        # strip decimal point from param & convert the significant digits to integer
        # Ex: 12.34 ---> 1234
        val = str(param)
        if '.' in val or '-' in val:
            val = val.replace('.', '')
            val = val.replace('-', '')
        val = int(val)
        numeric_data.val = val
        return numeric_data

    def _map_sql_type(self, param, parameters_list, i):
        """
        Map a Python data type to the corresponding SQL type,C type,Columnsize and Decimal digits.
        Takes:
            - param: The parameter to map.
            - parameters_list: The list of parameters to bind.
            - i: The index of the parameter in the list.
        Returns:
            - A tuple containing the SQL type, C type, column size, and decimal digits.
        """
        if param is None:
            return odbc_sql_const.SQL_NULL_DATA.value, odbc_sql_const.SQL_C_DEFAULT.value, 1, 0

        elif isinstance(param, bool):
            return odbc_sql_const.SQL_BIT.value, odbc_sql_const.SQL_C_BIT.value, 1, 0

        elif isinstance(param, int):
            if 0 <= param <= 255:
                return odbc_sql_const.SQL_TINYINT.value, odbc_sql_const.SQL_C_TINYINT.value, 3, 0
            elif -32768 <= param <= 32767:
                return odbc_sql_const.SQL_SMALLINT.value, odbc_sql_const.SQL_C_SHORT.value, 5, 0
            elif -2147483648 <= param <= 2147483647:
                return odbc_sql_const.SQL_INTEGER.value, odbc_sql_const.SQL_C_LONG.value, 10, 0
            else:
                return odbc_sql_const.SQL_BIGINT.value, odbc_sql_const.SQL_C_SBIGINT.value, 19, 0

        elif isinstance(param, float):
            return odbc_sql_const.SQL_DOUBLE.value, odbc_sql_const.SQL_C_DOUBLE.value, 15, 0

        elif isinstance(param, decimal.Decimal):
            # TODO: Support for other numeric types (smallmoney, money etc.)
            # if param.as_tuple().exponent == -4:  # Scale is 4
            #     if -214748.3648 <= param <= 214748.3647:
            #         return odbc_sql_const.SQL_SMALLMONEY.value, odbc_sql_const.SQL_C_NUMERIC.value, 10, 4
            #     elif -922337203685477.5808 <= param <= 922337203685477.5807:
            #         return odbc_sql_const.SQL_MONEY.value, odbc_sql_const.SQL_C_NUMERIC.value, 19, 4
            parameters_list[i] = self._get_numeric_data(param)  # Replace the parameter with the dictionary
            return odbc_sql_const.SQL_NUMERIC.value, odbc_sql_const.SQL_C_NUMERIC.value, parameters_list[i].precision, parameters_list[i].scale

        elif isinstance(param, str):
            # Check for Well-Known Text (WKT) format for geography/geometry
            if param.startswith("POINT") or param.startswith("LINESTRING") or param.startswith("POLYGON"):
                return odbc_sql_const.SQL_WVARCHAR.value, odbc_sql_const.SQL_C_WCHAR.value, len(param), 0

            # Attempt to parse as date, datetime or time
            if self._parse_date(param):
                parameters_list[i] = self._parse_date(param)  # Replace the parameter with the date object
                return odbc_sql_const.SQL_DATE.value, odbc_sql_const.SQL_C_TYPE_DATE.value, 10, 0
            elif self._parse_datetime(param):
                parameters_list[i] = self._parse_datetime(param)
                return odbc_sql_const.SQL_TIMESTAMP.value, odbc_sql_const.SQL_C_TYPE_TIMESTAMP.value, 23, 3
            elif self._parse_time(param):
                parameters_list[i] = self._parse_time(param)
                return odbc_sql_const.SQL_TIME.value, odbc_sql_const.SQL_C_TYPE_TIME.value, 8, 0
            # TODO: Support for other types (Timestampoffset etc.)
            # elif self._parse_timestamptz(param):
            #     return odbc_sql_const.SQL_TIMESTAMPOFFSET.value, odbc_sql_const.SQL_C_TYPE_TIMESTAMP.value, 34, 7
            # elif self._parse_smalldatetime(param):
            #     return odbc_sql_const.SQL_SMALLDATETIME.value, odbc_sql_const.SQL_C_TYPE_TIMESTAMP.value, 16, 0
            # elif self._parse_datetime2(param):
            #     return odbc_sql_const.SQL_DATETIME2.value, odbc_sql_const.SQL_C_TYPE_TIMESTAMP.value, 27, 7
            

            # String mapping logic here
            is_unicode = self._is_unicode_string(param)
            if len(param) > 4000:  # Long strings
                if is_unicode:
                    return odbc_sql_const.SQL_WLONGVARCHAR.value, odbc_sql_const.SQL_C_WCHAR.value, len(param), 0
                else:
                    return odbc_sql_const.SQL_LONGVARCHAR.value, odbc_sql_const.SQL_C_CHAR.value, len(param), 0
            elif is_unicode:  # Short Unicode strings
                return odbc_sql_const.SQL_WVARCHAR.value, odbc_sql_const.SQL_C_WCHAR.value, len(param), 0
            else:  # Short non-Unicode strings
                return odbc_sql_const.SQL_VARCHAR.value, odbc_sql_const.SQL_C_CHAR.value, len(param), 0

        elif isinstance(param, bytes):
            if len(param) > 8000:  # Assuming VARBINARY(MAX) for long byte arrays
                return odbc_sql_const.SQL_VARBINARY.value, odbc_sql_const.SQL_C_BINARY.value, len(param), 0
            else:
                return odbc_sql_const.SQL_BINARY.value, odbc_sql_const.SQL_C_BINARY.value, len(param), 0
        
        elif isinstance(param, bytearray):
            if len(param) > 8000:  # Assuming VARBINARY(MAX) for long byte arrays
                return odbc_sql_const.SQL_VARBINARY.value, odbc_sql_const.SQL_C_BINARY.value, len(param), 0
            else:
                return odbc_sql_const.SQL_BINARY.value, odbc_sql_const.SQL_C_BINARY.value, len(param), 0
        
        elif isinstance(param, uuid.UUID):  # Handle uniqueidentifier
            return odbc_sql_const.SQL_GUID.value, odbc_sql_const.SQL_C_GUID.value, 36, 0
        
        elif isinstance(param, datetime.datetime):
            # Always keep datetime.datetime check before datetime.date check since datetime.datetime is a subclass of datetime (isinstance(datetime.datetime, datetime.date) returns True)
            return odbc_sql_const.SQL_TIMESTAMP.value, odbc_sql_const.SQL_C_TYPE_TIMESTAMP.value, 23, 3
        
        elif isinstance(param, datetime.date):
            return odbc_sql_const.SQL_DATE.value, odbc_sql_const.SQL_C_TYPE_DATE.value, 10, 0
        
        elif isinstance(param, datetime.time):
            return odbc_sql_const.SQL_TIME.value, odbc_sql_const.SQL_C_TYPE_TIME.value, 8, 0
        
        else:
            # Fallback to VARCHAR for unsupported types
            return odbc_sql_const.SQL_VARCHAR.value, odbc_sql_const.SQL_C_CHAR.value, len(str(param)), 0
 
    def _initialize_cursor(self) -> None:
        """
        Initialize the ODBC statement handle.
        """
        self._allocate_statement_handle()
    
    def _allocate_statement_handle(self):
            """
            Allocate the ODBC statement handle.
            """
            ret = ddbc_bindings.DDBCSQLAllocHandle(
                odbc_sql_const.SQL_HANDLE_STMT.value,
                self.connection.hdbc.value,
                ctypes.cast(ctypes.pointer(self.hstmt), ctypes.c_void_p).value
            )
            check_error(odbc_sql_const.SQL_HANDLE_STMT.value, self.hstmt.value, ret)
    
    def _reset_cursor(self) -> None:
        """
        Reset the ODBC statement handle.
        """
        # Free the existing statement handle
        if self.hstmt.value:
            ddbc_bindings.DDBCSQLFreeHandle(odbc_sql_const.SQL_HANDLE_STMT.value, self.hstmt.value)
        # Reinitialize the statement handle
        self._initialize_cursor()
    
    def close(self) -> None:
        """
        Close the cursor now (rather than whenever __del__ is called).
        
        Raises:
            Error: If any operation is attempted with the cursor after it is closed.
        """
        if self.closed:
            raise Exception("Cursor is already closed.")
        
        # Free the statement handle
        if self.hstmt.value:
            ret = ddbc_bindings.DDBCSQLFreeHandle(odbc_sql_const.SQL_HANDLE_STMT.value, self.hstmt.value)
            check_error(odbc_sql_const.SQL_HANDLE_STMT.value, self.hstmt.value, ret)
            self.hstmt.value = None
        
        self.closed = True

    def _check_closed(self):
        """
        Check if the cursor is closed and raise an exception if it is.
        
        Raises:
            Error: If the cursor is closed.
        """
        if self.closed:
            raise Exception("Operation cannot be performed: the cursor is closed.")
    
    def _create_parameter_types_list(self, parameter, ParamInfo, parameters_list, i):
        """
        Maps parameter types for the given parameter.
        
        Args:
            parameter: parameter to bind.
        
        Returns:
            paraminfo.
        """
        paraminfo = ParamInfo()
        sql_type, c_type, column_size, decimal_digits = self._map_sql_type(parameter, parameters_list, i)
        paraminfo.paramCType = c_type
        paraminfo.paramSQLType = sql_type
        paraminfo.inputOutputType = odbc_sql_const.SQL_PARAM_INPUT.value
        paraminfo.columnSize = column_size
        paraminfo.decimalDigits = decimal_digits
        return paraminfo

    def _initialize_description(self):
        """
        Initialize the description attribute using SQLDescribeCol.
        """
        col_metadata = []
        ret = ddbc_bindings.DDBCSQLDescribeCol(self.hstmt.value, col_metadata)
        check_error(odbc_sql_const.SQL_HANDLE_STMT.value, self.hstmt.value, ret)

        self.description = [
            (
                col["ColumnName"],
                self._map_data_type(col["DataType"]),
                None,
                col["ColumnSize"],
                col["ColumnSize"],
                col["DecimalDigits"],
                col["Nullable"] == odbc_sql_const.SQL_NULLABLE.value,
            )
            for col in col_metadata
        ]

    def _map_data_type(self, sql_type):
        """
        Map SQL data type to Python data type.
        
        Args:
            sql_type: SQL data type.
        
        Returns:
            Corresponding Python data type.
        """
        sql_to_python_type = {
            odbc_sql_const.SQL_INTEGER.value: int,
            odbc_sql_const.SQL_VARCHAR.value: str,
            odbc_sql_const.SQL_WVARCHAR.value: str,
            odbc_sql_const.SQL_CHAR.value: str,
            odbc_sql_const.SQL_WCHAR.value: str,
            odbc_sql_const.SQL_FLOAT.value: float,
            odbc_sql_const.SQL_DOUBLE.value: float,
            odbc_sql_const.SQL_DECIMAL.value: decimal.Decimal,
            odbc_sql_const.SQL_NUMERIC.value: decimal.Decimal,
            odbc_sql_const.SQL_DATE.value: datetime.date,
            odbc_sql_const.SQL_TIMESTAMP.value: datetime.datetime,
            odbc_sql_const.SQL_TIME.value: datetime.time,
            odbc_sql_const.SQL_BIT.value: bool,
            odbc_sql_const.SQL_TINYINT.value: int,
            odbc_sql_const.SQL_SMALLINT.value: int,
            odbc_sql_const.SQL_BIGINT.value: int,
            odbc_sql_const.SQL_BINARY.value: bytes,
            odbc_sql_const.SQL_VARBINARY.value: bytes,
            odbc_sql_const.SQL_LONGVARBINARY.value: bytes,
            odbc_sql_const.SQL_GUID.value: uuid.UUID,
            # Add more mappings as needed
        }
        return sql_to_python_type.get(sql_type, str)

    def execute(self, operation: str, *parameters, use_prepare: bool = True, reset_cursor: bool = True):
        """
        Prepare and execute a database operation (query or command).
        
        Args:
            operation: SQL query or command.
            parameters: Sequence of parameters to bind.
            use_prepare: Whether to use SQLPrepareW (default) or SQLExecDirectW.
            reset_cursor: Whether to reset the cursor before execution.
        """
        self._check_closed()  # Check if the cursor is closed

        if reset_cursor:
            self._reset_cursor()

        ParamInfo = ddbc_bindings.ParamInfo
        parameters_type = []

        # Flatten parameters if a single tuple or list is passed
        if len(parameters) == 1 and isinstance(parameters[0], (tuple, list)):
            parameters = parameters[0]

        parameters = list(parameters)

        if len(parameters):
            for i, param in enumerate(parameters):
                paraminfo = self._create_parameter_types_list(param, ParamInfo, parameters, i)
                parameters_type.append(paraminfo)

        # TODO: Use a more sophisticated string compare that handles redundant spaces etc.
        #       Also consider storing last query's hash instead of full query string. This will help
        #       in low-memory conditions (Ex: huge number of parallel queries with huge query string sizes)
        if operation != self.last_executed_stmt:
            # Executing a new statement. Reset is_stmt_prepared to false
            self.is_stmt_prepared = [False]
        '''
        Execute SQL Statement - (SQLExecute)
        '''
        # TODO - Need to evaluate encrypted logs for query parameters
        if ENABLE_LOGGING:
            logger.debug("Executing query: %s", operation)
            for i, param in enumerate(parameters):
                logger.debug(
                    "Parameter number: %s, Parameter: %s, Param Python Type: %s, ParamInfo: %s, %s, %s, %s, %s",
                    i+1,
                    param,
                    str(type(param)),
                    parameters_type[i].paramSQLType,
                    parameters_type[i].paramCType,
                    parameters_type[i].columnSize,
                    parameters_type[i].decimalDigits,
                    parameters_type[i].inputOutputType
                )

        ret = ddbc_bindings.DDBCSQLExecute(self.hstmt.value, operation, parameters, parameters_type,
                                            self.is_stmt_prepared, use_prepare)
        check_error(odbc_sql_const.SQL_HANDLE_STMT.value, self.hstmt.value, ret)
        self.last_executed_stmt = operation

        # Update rowcount after execution
        self.rowcount = ddbc_bindings.DDBCSQLRowCount(self.hstmt.value)

        # Initialize description after execution
        self._initialize_description()

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
        
        # Reset the cursor once before the loop
        self._reset_cursor()

        try:
            first_execution = True
            total_rowcount = 0
            for parameters in seq_of_parameters:
                # Execute the operation with the current set of parameters without 
                # Converting the parameters to a list
                parameters = list(parameters)
                if ENABLE_LOGGING:
                    logger.info("Executing query with parameters: %s", parameters)
                # Prepare the statement only during first execution. From second time
                # onwards, skip preparing and directly execute. This helps avoid
                # unnecessary 'prepare' network calls.
                if first_execution:
                    prepare_stmt = True
                    first_execution = False
                else:
                    prepare_stmt = False
                # Execute statement with one parameter set
                self.execute(operation, parameters, use_prepare=prepare_stmt, reset_cursor=False)
                if self.rowcount != -1:
                    # Rowcount would get updated inside execute method, add it to the current rowcount
                    total_rowcount += self.rowcount
                else:
                    total_rowcount = -1
            # Update the rowcount after all executions
            self.rowcount = total_rowcount
        except Exception as e:
            if ENABLE_LOGGING:
                logger.info("Executing query with parameters: %s", parameters)
            # Prepare the statement only during first execution. From second time
            # onwards, skip preparing and directly execute. This helps avoid
            # unnecessary 'prepare' network calls.
            if first_execution:
                prepare_stmt = True
                first_execution = False
            else:
                prepare_stmt = False
            # Execute statement with one parameter set
            self.execute(operation, parameters, use_prepare=prepare_stmt, reset_cursor=False)

    def fetchone(self) -> Union[None, tuple]:
        """
        Fetch the next row of a query result set.
        
        Returns:
            Single sequence or None if no more data is available.
        
        Raises:
            Error: If the previous call to execute did not produce any result set.
        """
        self._check_closed()  # Check if the cursor is closed
        
        # Fetch the next row
        row = []
        ret = ddbc_bindings.DDBCSQLFetchOne(self.hstmt.value, row)
        check_error(odbc_sql_const.SQL_HANDLE_STMT.value, self.hstmt.value, ret)
        if ret == odbc_sql_const.SQL_NO_DATA.value:
            return None
        return list(row)

    def fetchmany(self, size: int = None) -> List[tuple]:
        """
        Fetch the next set of rows of a query result.
        
        Args:
            size: Number of rows to fetch at a time.
        
        Returns:
            Sequence of sequences (e.g. list of tuples).
        
        Raises:
            Error: If the previous call to execute did not produce any result set.
        """
        self._check_closed()  # Check if the cursor is closed

        if size is None:
            size = self.arraysize

        # Fetch the next set of rows
        rows = []
        ret = ddbc_bindings.DDBCSQLFetchMany(self.hstmt.value, rows, size)
        if ret == odbc_sql_const.SQL_NO_DATA.value:
            return []
        return rows

    def fetchall(self) -> List[tuple]:
        """
        Fetch all (remaining) rows of a query result.
        
        Returns:
            Sequence of sequences (e.g. list of tuples).
        
        Raises:
            Error: If the previous call to execute did not produce any result set.
        """
        self._check_closed()  # Check if the cursor is closed

        # Fetch all remaining rows
        rows = []
        ret = ddbc_bindings.DDBCSQLFetchAll(self.hstmt.value, rows)
        if ret != odbc_sql_const.SQL_NO_DATA.value:
            return []
        return list(rows)

    def nextset(self) -> Union[bool, None]:
        """
        Skip to the next available result set.
        
        Returns:
            True if there is another result set, None otherwise.
        
        Raises:
            Error: If the previous call to execute did not produce any result set.
        """
        self._check_closed()  # Check if the cursor is closed

        # Skip to the next result set
        ret = ddbc_bindings.DDBCSQLMoreResults(self.hstmt.value)
        if ret == odbc_sql_const.SQL_NO_DATA.value:
            return False
        return True
