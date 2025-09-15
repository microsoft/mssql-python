"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
This module contains the Cursor class, which represents a database cursor.
Resource Management:
- Cursors are tracked by their parent connection.
- Closing the connection will automatically close all open cursors.
- Do not use a cursor after it is closed, or after its parent connection is closed.
- Use close() to release resources held by the cursor as soon as it is no longer needed.
"""
import decimal
import uuid
import datetime
import warnings
from typing import List, Union, Any
from mssql_python.constants import ConstantsDDBC as ddbc_sql_const
from mssql_python.helpers import check_error, log
from mssql_python import ddbc_bindings
from mssql_python.exceptions import InterfaceError, ProgrammingError
from mssql_python.row import Row
from mssql_python import get_settings


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

    # TODO(jathakkar): Thread safety considerations
    # The cursor class contains methods that are not thread-safe due to:
    #  1. Methods that mutate cursor state (_reset_cursor, self.description, etc.)
    #  2. Methods that call ODBC functions with shared handles (self.hstmt)
    # 
    # These methods should be properly synchronized or redesigned when implementing 
    # async functionality to prevent race conditions and data corruption.
    # Consider using locks, redesigning for immutability, or ensuring 
    # cursor objects are never shared across threads.

    def __init__(self, connection, timeout: int = 0) -> None:
        """
        Initialize the cursor with a database connection.

        Args:
            connection: Database connection object.
        """
        self.connection = connection
        self._timeout = timeout
        self._inputsizes = None
        # self.connection.autocommit = False
        self.hstmt = None
        self._initialize_cursor()
        self.description = None
        self.rowcount = -1
        self.arraysize = (
            1  # Default number of rows to fetch at a time is 1, user can change it
        )
        self.buffer_length = 1024  # Default buffer length for string data
        self.closed = False
        self._result_set_empty = False  # Add this initialization
        self.last_executed_stmt = (
            ""  # Stores the last statement executed by this cursor
        )
        self.is_stmt_prepared = [
            False
        ]  # Indicates if last_executed_stmt was prepared by ddbc shim.
        # Is a list instead of a bool coz bools in Python are immutable.
        # Hence, we can't pass around bools by reference & modify them.
        # Therefore, it must be a list with exactly one bool element.
        
        self.lowercase = get_settings().lowercase

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

    def _initialize_cursor(self) -> None:
        """
        Initialize the DDBC statement handle.
        """
        self._allocate_statement_handle()

    def _allocate_statement_handle(self):
        """
        Allocate the DDBC statement handle.
        """
        self.hstmt = self.connection._conn.alloc_statement_handle()

    def _reset_cursor(self) -> None:
        """
        Reset the DDBC statement handle.
        """
        if self.hstmt:
            self.hstmt.free()
            self.hstmt = None
            log('debug', "SQLFreeHandle succeeded")     
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

        if self.hstmt:
            self.hstmt.free()
            self.hstmt = None
            log('debug', "SQLFreeHandle succeeded")
        self.closed = True

    def _check_closed(self):
        """
        Check if the cursor is closed and raise an exception if it is.

        Raises:
            Error: If the cursor is closed.
        """
        if self.closed:
            raise Exception("Operation cannot be performed: the cursor is closed.")

    def setinputsizes(self, sizes: List[Union[int, tuple]]) -> None:
        """
        Sets the type information to be used for parameters in execute and executemany.
        
        This method can be used to explicitly declare the types and sizes of query parameters.
        For example:
        
        sql = "INSERT INTO product (item, price) VALUES (?, ?)"
        params = [('bicycle', 499.99), ('ham', 17.95)]
        # specify that parameters are for NVARCHAR(50) and DECIMAL(18,4) columns
        cursor.setinputsizes([(SQL_WVARCHAR, 50, 0), (SQL_DECIMAL, 18, 4)])
        cursor.executemany(sql, params)
        
        Args:
            sizes: A sequence of tuples, one for each parameter. Each tuple contains
                   (sql_type, size, decimal_digits) where size and decimal_digits are optional.
        """
        self._inputsizes = []
        
        if sizes:
            for size_info in sizes:
                if isinstance(size_info, tuple):
                    # Handle tuple format (sql_type, size, decimal_digits)
                    if len(size_info) == 1:
                        self._inputsizes.append((size_info[0], 0, 0))
                    elif len(size_info) == 2:
                        self._inputsizes.append((size_info[0], size_info[1], 0))
                    elif len(size_info) >= 3:
                        self._inputsizes.append((size_info[0], size_info[1], size_info[2]))
                else:
                    # Handle single value (just sql_type)
                    self._inputsizes.append((size_info, 0, 0))
    
    def _reset_inputsizes(self):
        """Reset input sizes after execution"""
        self._inputsizes = None

    def _get_c_type_for_sql_type(self, sql_type: int) -> int:
        """Map SQL type to appropriate C type for parameter binding"""
        sql_to_c_type = {
            ddbc_sql_const.SQL_CHAR.value: ddbc_sql_const.SQL_C_CHAR.value,
            ddbc_sql_const.SQL_VARCHAR.value: ddbc_sql_const.SQL_C_CHAR.value,
            ddbc_sql_const.SQL_LONGVARCHAR.value: ddbc_sql_const.SQL_C_CHAR.value,
            ddbc_sql_const.SQL_WCHAR.value: ddbc_sql_const.SQL_C_WCHAR.value,
            ddbc_sql_const.SQL_WVARCHAR.value: ddbc_sql_const.SQL_C_WCHAR.value,
            ddbc_sql_const.SQL_WLONGVARCHAR.value: ddbc_sql_const.SQL_C_WCHAR.value,
            ddbc_sql_const.SQL_DECIMAL.value: ddbc_sql_const.SQL_C_NUMERIC.value,
            ddbc_sql_const.SQL_NUMERIC.value: ddbc_sql_const.SQL_C_NUMERIC.value,
            ddbc_sql_const.SQL_BIT.value: ddbc_sql_const.SQL_C_BIT.value,
            ddbc_sql_const.SQL_TINYINT.value: ddbc_sql_const.SQL_C_TINYINT.value,
            ddbc_sql_const.SQL_SMALLINT.value: ddbc_sql_const.SQL_C_SHORT.value,
            ddbc_sql_const.SQL_INTEGER.value: ddbc_sql_const.SQL_C_LONG.value,
            ddbc_sql_const.SQL_BIGINT.value: ddbc_sql_const.SQL_C_SBIGINT.value,
            ddbc_sql_const.SQL_REAL.value: ddbc_sql_const.SQL_C_FLOAT.value,
            ddbc_sql_const.SQL_FLOAT.value: ddbc_sql_const.SQL_C_DOUBLE.value,
            ddbc_sql_const.SQL_DOUBLE.value: ddbc_sql_const.SQL_C_DOUBLE.value,
            ddbc_sql_const.SQL_BINARY.value: ddbc_sql_const.SQL_C_BINARY.value,
            ddbc_sql_const.SQL_VARBINARY.value: ddbc_sql_const.SQL_C_BINARY.value,
            ddbc_sql_const.SQL_LONGVARBINARY.value: ddbc_sql_const.SQL_C_BINARY.value,
            ddbc_sql_const.SQL_DATE.value: ddbc_sql_const.SQL_C_TYPE_DATE.value,
            ddbc_sql_const.SQL_TIME.value: ddbc_sql_const.SQL_C_TYPE_TIME.value,
            ddbc_sql_const.SQL_TIMESTAMP.value: ddbc_sql_const.SQL_C_TYPE_TIMESTAMP.value,
        }
        return sql_to_c_type.get(sql_type, ddbc_sql_const.SQL_C_DEFAULT.value)

    def _create_parameter_types_list(self, parameter: Any, param_info, parameters_list, i: int) -> 'param_info':
        """
        Maps parameter types for the given parameter.

        Args:
            parameter: parameter to bind.

        Returns:
            paraminfo.
        """
        paraminfo = param_info()
        
        # Check if we have explicit type information from setinputsizes
        if self._inputsizes and i < len(self._inputsizes):
            # Use explicit type information
            sql_type, column_size, decimal_digits = self._inputsizes[i]
            
            if parameter is None:
                # For NULL parameters, use SQL_C_DEFAULT instead of a specific C type
                # This allows NULL to be properly sent for any SQL type
                c_type = ddbc_sql_const.SQL_C_DEFAULT.value
            else:
                # For non-NULL parameters, determine the appropriate C type based on SQL type
                c_type = self._get_c_type_for_sql_type(sql_type)

            # Sanitize precision/scale for numeric types
            if sql_type in (ddbc_sql_const.SQL_DECIMAL.value, ddbc_sql_const.SQL_NUMERIC.value):
                column_size = max(1, min(int(column_size) if column_size > 0 else 18, 38))
                decimal_digits = min(max(0, decimal_digits), column_size)
    
        else:
            # Fall back to automatic type inference
            sql_type, c_type, column_size, decimal_digits = self._map_sql_type(
                parameter, parameters_list, i
            )

        paraminfo.paramCType = c_type
        paraminfo.paramSQLType = sql_type
        paraminfo.inputOutputType = ddbc_sql_const.SQL_PARAM_INPUT.value
        paraminfo.columnSize = column_size
        paraminfo.decimalDigits = decimal_digits
        return paraminfo

    def _initialize_description(self, column_metadata=None):
        """Initialize the description attribute from column metadata."""
        if not column_metadata:
            self.description = None
            return
        import mssql_python

        description = []
        for i, col in enumerate(column_metadata):
            # Get column name - lowercase it if the lowercase flag is set
            column_name = col["ColumnName"]
            
            if mssql_python.lowercase:
                column_name = column_name.lower()
                
            # Add to description tuple (7 elements as per PEP-249)
            description.append((
                column_name,                           # name 
                self._map_data_type(col["DataType"]),  # type_code
                None,                                  # display_size
                col["ColumnSize"],                     # internal_size
                col["ColumnSize"],                     # precision - should match ColumnSize
                col["DecimalDigits"],                  # scale
                col["Nullable"] == ddbc_sql_const.SQL_NULLABLE.value, # null_ok
            ))
        self.description = description

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
        *parameters,
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

        # Restore original fetch methods if they exist
        if hasattr(self, '_original_fetchone'):
            self.fetchone = self._original_fetchone
            self.fetchmany = self._original_fetchmany
            self.fetchall = self._original_fetchall
            del self._original_fetchone
            del self._original_fetchmany
            del self._original_fetchall

        self._check_closed()  # Check if the cursor is closed
        if reset_cursor:
            self._reset_cursor()

        # Apply timeout if set (non-zero)
        if self._timeout > 0:
            try:
                timeout_value = int(self._timeout) 
                ret = ddbc_bindings.DDBCSQLSetStmtAttr(
                    self.hstmt,
                    ddbc_sql_const.SQL_ATTR_QUERY_TIMEOUT.value,
                    timeout_value
                )
                check_error(ddbc_sql_const.SQL_HANDLE_STMT.value, self.hstmt, ret)
                log('debug', f"Set query timeout to {timeout_value} seconds")
            except Exception as e:
                log('warning', f"Failed to set query timeout: {e}")

        param_info = ddbc_bindings.ParamInfo
        parameters_type = []

        # Flatten parameters if a single tuple or list is passed
        if len(parameters) == 1 and isinstance(parameters[0], (tuple, list)):
            parameters = parameters[0]

        parameters = list(parameters)

        # Validate that inputsizes matches parameter count if both are present
        if parameters and self._inputsizes:
            if len(self._inputsizes) != len(parameters):

                warnings.warn(
                    f"Number of input sizes ({len(self._inputsizes)}) does not match "
                    f"number of parameters ({len(parameters)}). This may lead to unexpected behavior.",
                    Warning
                )

        if parameters:
            for i, param in enumerate(parameters):
                paraminfo = self._create_parameter_types_list(
                    param, param_info, parameters, i
                )
                parameters_type.append(paraminfo)

        # TODO: Use a more sophisticated string compare that handles redundant spaces etc.
        #       Also consider storing last query's hash instead of full query string. This will help
        #       in low-memory conditions
        #       (Ex: huge number of parallel queries with huge query string sizes)
        if operation != self.last_executed_stmt:
# Executing a new statement. Reset is_stmt_prepared to false
            self.is_stmt_prepared = [False]

        log('debug', "Executing query: %s", operation)
        for i, param in enumerate(parameters):
            log('debug',
                """Parameter number: %s, Parameter: %s,
                Param Python Type: %s, ParamInfo: %s, %s, %s, %s, %s""",
                i + 1,
                param,
                str(type(param)),
                    parameters_type[i].paramSQLType,
                    parameters_type[i].paramCType,
                    parameters_type[i].columnSize,
                    parameters_type[i].decimalDigits,
                    parameters_type[i].inputOutputType,
                )

        ret = ddbc_bindings.DDBCSQLExecute(
            self.hstmt,
            operation,
            parameters,
            parameters_type,
            self.is_stmt_prepared,
            use_prepare,
        )
        check_error(ddbc_sql_const.SQL_HANDLE_STMT.value, self.hstmt, ret)
        self.last_executed_stmt = operation

        # Update rowcount after execution
        # TODO: rowcount return code from SQL needs to be handled
        self.rowcount = ddbc_bindings.DDBCSQLRowCount(self.hstmt)

        # Initialize description after execution
        # After successful execution, initialize description if there are results
        column_metadata = []
        try:
            ddbc_bindings.DDBCSQLDescribeCol(self.hstmt, column_metadata)
            self._initialize_description(column_metadata)
        except Exception as e:
            # If describe fails, it's likely there are no results (e.g., for INSERT)
            self.description = None
        
        self._reset_inputsizes()  # Reset input sizes after execution

    def getTypeInfo(self, sqlType=None):
        """
        Executes SQLGetTypeInfo and creates a result set with information about 
        the specified data type or all data types supported by the ODBC driver if not specified.
        """
        self._check_closed()
        
        # Always reset the cursor first to ensure clean state
        self._reset_cursor()
        
        # SQL_ALL_TYPES = 0
        sql_all_types = 0
        
        try:
            if sqlType is None:
                # Get information about all data types
                ret = ddbc_bindings.DDBCSQLGetTypeInfo(self.hstmt, sql_all_types)
            else:
                # Get information about specified data type
                ret = ddbc_bindings.DDBCSQLGetTypeInfo(self.hstmt, sqlType)
    
            check_error(ddbc_sql_const.SQL_HANDLE_STMT.value, self.hstmt, ret)
    
            # Initialize the description based on result set metadata
            column_metadata = []
            ddbc_bindings.DDBCSQLDescribeCol(self.hstmt, column_metadata)

            # Initialize the description attribute with the column metadata
            self._initialize_description(column_metadata)
                
            # Define column names in ODBC standard order
            self._column_map = {}
            for i, (name, *_) in enumerate(self.description):
                # Add standard name
                self._column_map[name] = i
                # Add lowercase alias
                self._column_map[name.lower()] = i

            # Remember original fetch methods (store only once)
            if not hasattr(self, '_original_fetchone'):
                self._original_fetchone = self.fetchone
                self._original_fetchmany = self.fetchmany
                self._original_fetchall = self.fetchall

                # Create wrapper fetch methods that add column mappings
                def fetchone_with_mapping():
                    row = self._original_fetchone()
                    if row is not None:
                        row._column_map = self._column_map
                    return row

                def fetchmany_with_mapping(size=None):
                    rows = self._original_fetchmany(size)
                    for row in rows:
                        row._column_map = self._column_map
                    return rows

                def fetchall_with_mapping():
                    rows = self._original_fetchall()
                    for row in rows:
                        row._column_map = self._column_map
                    return rows

                # Replace fetch methods
                self.fetchone = fetchone_with_mapping
                self.fetchmany = fetchmany_with_mapping
                self.fetchall = fetchall_with_mapping

            # Return the cursor itself
            return self
        except Exception as e:
            # Always reset the cursor on exception
            self._reset_cursor()
            raise e
        
    def procedures(self, procedure=None, catalog=None, schema=None):
        """
        Executes SQLProcedures and creates a result set of information about procedures in the data source.
        
        Args:
            procedure (str, optional): Procedure name pattern. Default is None (all procedures).
            catalog (str, optional): Catalog name pattern. Default is None (current catalog).
            schema (str, optional): Schema name pattern. Default is None (all schemas).
            
        Returns:
            List of Row objects, each containing procedure information with these columns:
            - procedure_cat (str): The catalog name
            - procedure_schem (str): The schema name
            - procedure_name (str): The procedure name
            - num_input_params (int): Number of input parameters
            - num_output_params (int): Number of output parameters
            - num_result_sets (int): Number of result sets
            - remarks (str): Comments about the procedure
            - procedure_type (int): Type of procedure (1=procedure, 2=function)
        """
        self._check_closed()
        
        # Always reset the cursor first to ensure clean state
        self._reset_cursor()
        
        # Call the SQLProcedures function
        retcode = ddbc_bindings.DDBCSQLProcedures(self.hstmt, catalog, schema, procedure)
        check_error(ddbc_sql_const.SQL_HANDLE_STMT.value, self.hstmt, retcode)
        
        # Create column metadata and initialize description
        column_metadata = []
        try:
            ddbc_bindings.DDBCSQLDescribeCol(self.hstmt, column_metadata)
            self._initialize_description(column_metadata)

        except InterfaceError as e:
            log('error', f"Driver interface error during metadata retrieval: {e}")

        except Exception as e:
            # Log the exception with appropriate context
            log('error', f"Failed to retrieve column metadata: {e}. Using standard ODBC column definitions instead.")

        if not self.description:
            # If describe fails, create a manual description
            column_types = [str, str, str, int, int, int, str, int]
            self.description = [
                ("procedure_cat", column_types[0], None, 128, 128, 0, True),
                ("procedure_schem", column_types[1], None, 128, 128, 0, True),
                ("procedure_name", column_types[2], None, 128, 128, 0, False),
                ("num_input_params", column_types[3], None, 10, 10, 0, True),
                ("num_output_params", column_types[4], None, 10, 10, 0, True),
                ("num_result_sets", column_types[5], None, 10, 10, 0, True),
                ("remarks", column_types[6], None, 254, 254, 0, True),
                ("procedure_type", column_types[7], None, 10, 10, 0, False)
            ]
        
        # Define column names in ODBC standard order
        self._column_map = {}
        for i, (name, *_) in enumerate(self.description):
            # Add standard name
            self._column_map[name] = i
            # Add lowercase alias
            self._column_map[name.lower()] = i

        # Remember original fetch methods (store only once)
        if not hasattr(self, '_original_fetchone'):
            self._original_fetchone = self.fetchone
            self._original_fetchmany = self.fetchmany
            self._original_fetchall = self.fetchall

            # Create wrapper fetch methods that add column mappings
            def fetchone_with_mapping():
                row = self._original_fetchone()
                if row is not None:
                    row._column_map = self._column_map
                return row

            def fetchmany_with_mapping(size=None):
                rows = self._original_fetchmany(size)
                for row in rows:
                    row._column_map = self._column_map
                return rows

            def fetchall_with_mapping():
                rows = self._original_fetchall()
                for row in rows:
                    row._column_map = self._column_map
                return rows

            # Replace fetch methods
            self.fetchone = fetchone_with_mapping
            self.fetchmany = fetchmany_with_mapping
            self.fetchall = fetchall_with_mapping

        # Return the cursor itself
        return self
    
    def primaryKeys(self, table, catalog=None, schema=None):
        """
        Creates a result set of column names that make up the primary key for a table
        by executing the SQLPrimaryKeys function.
        
        Args:
            table (str): The name of the table
            catalog (str, optional): The catalog name (database). Defaults to None.
            schema (str, optional): The schema name. Defaults to None.
        
        Returns:
            list: A list of rows with the following columns:
                - table_cat: Catalog name
                - table_schem: Schema name
                - table_name: Table name
                - column_name: Column name that is part of the primary key
                - key_seq: Column sequence number in the primary key (starting with 1)
                - pk_name: Primary key name
        
        Raises:
            ProgrammingError: If the cursor is closed
        """
        self._check_closed()
        
        # Always reset the cursor first to ensure clean state
        self._reset_cursor()
        
        if not table:
            raise ProgrammingError("Table name must be specified", "HY000")
        
        # Call the SQLPrimaryKeys function
        retcode = ddbc_bindings.DDBCSQLPrimaryKeys(
            self.hstmt,
            catalog,
            schema,
            table
        )
        check_error(ddbc_sql_const.SQL_HANDLE_STMT.value, self.hstmt, retcode)
        
        # Initialize description from column metadata
        column_metadata = []
        try:
            ddbc_bindings.DDBCSQLDescribeCol(self.hstmt, column_metadata)
            self._initialize_description(column_metadata)
        except InterfaceError as e:
            log('error', f"Driver interface error during metadata retrieval: {e}")

        except Exception as e:
            # Log the exception with appropriate context
            log('error', f"Failed to retrieve column metadata: {e}. Using standard ODBC column definitions instead.")

        if not self.description:
            # If describe fails, create a manual description for the standard columns
            column_types = [str, str, str, str, int, str]
            self.description = [
                ("table_cat", column_types[0], None, 128, 128, 0, True),
                ("table_schem", column_types[1], None, 128, 128, 0, True),
                ("table_name", column_types[2], None, 128, 128, 0, False),
                ("column_name", column_types[3], None, 128, 128, 0, False),
                ("key_seq", column_types[4], None, 10, 10, 0, False),
                ("pk_name", column_types[5], None, 128, 128, 0, True)
            ]
        
        # Define column names in ODBC standard order
        self._column_map = {}
        for i, (name, *_) in enumerate(self.description):
            # Add standard name
            self._column_map[name] = i
            # Add lowercase alias
            self._column_map[name.lower()] = i

        # Remember original fetch methods (store only once)
        if not hasattr(self, '_original_fetchone'):
            self._original_fetchone = self.fetchone
            self._original_fetchmany = self.fetchmany
            self._original_fetchall = self.fetchall

            # Create wrapper fetch methods that add column mappings
            def fetchone_with_mapping():
                row = self._original_fetchone()
                if row is not None:
                    row._column_map = self._column_map
                return row

            def fetchmany_with_mapping(size=None):
                rows = self._original_fetchmany(size)
                for row in rows:
                    row._column_map = self._column_map
                return rows

            def fetchall_with_mapping():
                rows = self._original_fetchall()
                for row in rows:
                    row._column_map = self._column_map
                return rows

            # Replace fetch methods
            self.fetchone = fetchone_with_mapping
            self.fetchmany = fetchmany_with_mapping
            self.fetchall = fetchall_with_mapping

        # Return the cursor itself
        return self

    def foreignKeys(self, table=None, catalog=None, schema=None, foreignTable=None, foreignCatalog=None, foreignSchema=None):
        """
        Executes the SQLForeignKeys function and creates a result set of column names that are foreign keys.
        
        This function returns:
        1. Foreign keys in the specified table that reference primary keys in other tables, OR
        2. Foreign keys in other tables that reference the primary key in the specified table
        
        Args:
            table (str, optional): The table containing the foreign key columns
            catalog (str, optional): The catalog containing table
            schema (str, optional): The schema containing table
            foreignTable (str, optional): The table containing the primary key columns
            foreignCatalog (str, optional): The catalog containing foreignTable
            foreignSchema (str, optional): The schema containing foreignTable
                
        Returns:
            List of Row objects, each containing foreign key information with these columns:
            - pktable_cat (str): Primary key table catalog name
            - pktable_schem (str): Primary key table schema name
            - pktable_name (str): Primary key table name
            - pkcolumn_name (str): Primary key column name
            - fktable_cat (str): Foreign key table catalog name
            - fktable_schem (str): Foreign key table schema name
            - fktable_name (str): Foreign key table name
            - fkcolumn_name (str): Foreign key column name
            - key_seq (int): Sequence number of the column in the foreign key
            - update_rule (int): Action for update (CASCADE, SET NULL, etc.)
            - delete_rule (int): Action for delete (CASCADE, SET NULL, etc.)
            - fk_name (str): Foreign key name
            - pk_name (str): Primary key name
            - deferrability (int): Indicates if constraint checking can be deferred
        """
        self._check_closed()
        
        # Always reset the cursor first to ensure clean state
        self._reset_cursor()
        
        # Check if we have at least one table specified - mimic pyodbc behavior
        if table is None and foreignTable is None:
            raise ProgrammingError("Either table or foreignTable must be specified", "HY000")
        
        # Call the SQLForeignKeys function
        retcode = ddbc_bindings.DDBCSQLForeignKeys(
            self.hstmt, 
            foreignCatalog, foreignSchema, foreignTable,
            catalog, schema, table
        )
        check_error(ddbc_sql_const.SQL_HANDLE_STMT.value, self.hstmt, retcode)
        
        # Initialize description from column metadata
        column_metadata = []
        try:
            ddbc_bindings.DDBCSQLDescribeCol(self.hstmt, column_metadata)
            self._initialize_description(column_metadata)

        except InterfaceError as e:
            log('error', f"Driver interface error during metadata retrieval: {e}")

        except Exception as e:
            # Log the exception with appropriate context
            log('error', f"Failed to retrieve column metadata: {e}. Using standard ODBC column definitions instead.")

        if not self.description:
            # If describe fails, create a manual description for the standard columns
            column_types = [str, str, str, str, str, str, str, str, int, int, int, str, str, int]
            self.description = [
                ("pktable_cat", column_types[0], None, 128, 128, 0, True),
                ("pktable_schem", column_types[1], None, 128, 128, 0, True),
                ("pktable_name", column_types[2], None, 128, 128, 0, False),
                ("pkcolumn_name", column_types[3], None, 128, 128, 0, False),
                ("fktable_cat", column_types[4], None, 128, 128, 0, True),
                ("fktable_schem", column_types[5], None, 128, 128, 0, True),
                ("fktable_name", column_types[6], None, 128, 128, 0, False),
                ("fkcolumn_name", column_types[7], None, 128, 128, 0, False),
                ("key_seq", column_types[8], None, 10, 10, 0, False),
                ("update_rule", column_types[9], None, 10, 10, 0, False),
                ("delete_rule", column_types[10], None, 10, 10, 0, False),
                ("fk_name", column_types[11], None, 128, 128, 0, True),
                ("pk_name", column_types[12], None, 128, 128, 0, True),
                ("deferrability", column_types[13], None, 10, 10, 0, False)
            ]
        
        # Define column names in ODBC standard order
        self._column_map = {}
        for i, (name, *_) in enumerate(self.description):
            # Add standard name
            self._column_map[name] = i
            # Add lowercase alias
            self._column_map[name.lower()] = i

        # Remember original fetch methods (store only once)
        if not hasattr(self, '_original_fetchone'):
            self._original_fetchone = self.fetchone
            self._original_fetchmany = self.fetchmany
            self._original_fetchall = self.fetchall

            # Create wrapper fetch methods that add column mappings
            def fetchone_with_mapping():
                row = self._original_fetchone()
                if row is not None:
                    row._column_map = self._column_map
                return row

            def fetchmany_with_mapping(size=None):
                rows = self._original_fetchmany(size)
                for row in rows:
                    row._column_map = self._column_map
                return rows

            def fetchall_with_mapping():
                rows = self._original_fetchall()
                for row in rows:
                    row._column_map = self._column_map
                return rows

            # Replace fetch methods
            self.fetchone = fetchone_with_mapping
            self.fetchmany = fetchmany_with_mapping
            self.fetchall = fetchall_with_mapping

        # Return the cursor itself
        return self
    
    def rowIdColumns(self, table, catalog=None, schema=None, nullable=True):
        """
        Executes SQLSpecialColumns with SQL_BEST_ROWID which creates a result set of 
        columns that uniquely identify a row.
        
        Args:
            table (str): The table name
            catalog (str, optional): The catalog name (database). Defaults to None.
            schema (str, optional): The schema name. Defaults to None.
            nullable (bool, optional): Whether to include nullable columns. Defaults to True.
        
        Returns:
            list: A list of rows with the following columns:
                - scope: One of SQL_SCOPE_CURROW, SQL_SCOPE_TRANSACTION, or SQL_SCOPE_SESSION
                - column_name: Column name
                - data_type: The ODBC SQL data type constant (e.g. SQL_CHAR)
                - type_name: Type name
                - column_size: Column size
                - buffer_length: Buffer length
                - decimal_digits: Decimal digits
                - pseudo_column: One of SQL_PC_UNKNOWN, SQL_PC_NOT_PSEUDO, SQL_PC_PSEUDO
        """
        self._check_closed()
        
        # Always reset the cursor first to ensure clean state
        self._reset_cursor()
        
        # Convert None values to empty strings as required by ODBC API
        if not table:
            raise ProgrammingError("Table name must be specified", "HY000")
        
        # Set the identifier type to SQL_BEST_ROWID (1)
        identifier_type = ddbc_sql_const.SQL_BEST_ROWID.value
        
        # Set scope to SQL_SCOPE_CURROW (0) - default scope
        scope = ddbc_sql_const.SQL_SCOPE_CURROW.value
        
        # Set nullable flag
        nullable_flag = ddbc_sql_const.SQL_NULLABLE.value if nullable else ddbc_sql_const.SQL_NO_NULLS.value
        
        # Call the SQLSpecialColumns function
        retcode = ddbc_bindings.DDBCSQLSpecialColumns(
            self.hstmt,
            identifier_type,
            catalog,
            schema,
            table,
            scope,
            nullable_flag
        )
        check_error(ddbc_sql_const.SQL_HANDLE_STMT.value, self.hstmt, retcode)
        
        # Initialize description from column metadata
        column_metadata = []
        try:
            ddbc_bindings.DDBCSQLDescribeCol(self.hstmt, column_metadata)
            self._initialize_description(column_metadata)

        except InterfaceError as e:
            log('error', f"Driver interface error during metadata retrieval: {e}")

        except Exception as e:
            # Log the exception with appropriate context
            log('error', f"Failed to retrieve column metadata: {e}. Using standard ODBC column definitions instead.")

        if not self.description:
            # If describe fails, create a manual description for the standard columns
            column_types = [int, str, int, str, int, int, int, int]
            self.description = [
                ("scope", column_types[0], None, 10, 10, 0, False),
                ("column_name", column_types[1], None, 128, 128, 0, False),
                ("data_type", column_types[2], None, 10, 10, 0, False),
                ("type_name", column_types[3], None, 128, 128, 0, False),
                ("column_size", column_types[4], None, 10, 10, 0, False),
                ("buffer_length", column_types[5], None, 10, 10, 0, False),
                ("decimal_digits", column_types[6], None, 10, 10, 0, True),
                ("pseudo_column", column_types[7], None, 10, 10, 0, False)
            ]
        
        # Create a column map with both ODBC standard names and lowercase aliases
        self._column_map = {}
        for i, (name, *_) in enumerate(self.description):
            # Add standard name
            self._column_map[name] = i
            # Add lowercase alias
            self._column_map[name.lower()] = i

        # Remember original fetch methods (store only once)
        if not hasattr(self, '_original_fetchone'):
            self._original_fetchone = self.fetchone
            self._original_fetchmany = self.fetchmany
            self._original_fetchall = self.fetchall

            # Create wrapper fetch methods that add column mappings
            def fetchone_with_mapping():
                row = self._original_fetchone()
                if row is not None:
                    row._column_map = self._column_map
                return row

            def fetchmany_with_mapping(size=None):
                rows = self._original_fetchmany(size)
                for row in rows:
                    row._column_map = self._column_map
                return rows

            def fetchall_with_mapping():
                rows = self._original_fetchall()
                for row in rows:
                    row._column_map = self._column_map
                return rows

            # Replace fetch methods
            self.fetchone = fetchone_with_mapping
            self.fetchmany = fetchmany_with_mapping
            self.fetchall = fetchall_with_mapping

        # Return the cursor itself
        return self
    
    def rowVerColumns(self, table, catalog=None, schema=None, nullable=True):
        """
        Executes SQLSpecialColumns with SQL_ROWVER which creates a result set of
        columns that are automatically updated when any value in the row is updated.
        
        Args:
            table (str): The table name
            catalog (str, optional): The catalog name (database). Defaults to None.
            schema (str, optional): The schema name. Defaults to None.
            nullable (bool, optional): Whether to include nullable columns. Defaults to True.
        
        Returns:
            list: A list of rows with the following columns:
                - scope: One of SQL_SCOPE_CURROW, SQL_SCOPE_TRANSACTION, or SQL_SCOPE_SESSION
                - column_name: Column name
                - data_type: The ODBC SQL data type constant (e.g. SQL_CHAR)
                - type_name: Type name
                - column_size: Column size
                - buffer_length: Buffer length
                - decimal_digits: Decimal digits
                - pseudo_column: One of SQL_PC_UNKNOWN, SQL_PC_NOT_PSEUDO, SQL_PC_PSEUDO
        """
        self._check_closed()
        
        # Always reset the cursor first to ensure clean state
        self._reset_cursor()
        
        if not table:
            raise ProgrammingError("Table name must be specified", "HY000")
        
        # Set the identifier type to SQL_ROWVER (2)
        identifier_type = ddbc_sql_const.SQL_ROWVER.value
        
        # Set scope to SQL_SCOPE_CURROW (0) - default scope
        scope = ddbc_sql_const.SQL_SCOPE_CURROW.value
        
        # Set nullable flag
        nullable_flag = ddbc_sql_const.SQL_NULLABLE.value if nullable else ddbc_sql_const.SQL_NO_NULLS.value
        
        # Call the SQLSpecialColumns function
        retcode = ddbc_bindings.DDBCSQLSpecialColumns(
            self.hstmt,
            identifier_type,
            catalog,
            schema,
            table,
            scope,
            nullable_flag
        )
        check_error(ddbc_sql_const.SQL_HANDLE_STMT.value, self.hstmt, retcode)
        
        # Initialize description from column metadata
        column_metadata = []
        try:
            ddbc_bindings.DDBCSQLDescribeCol(self.hstmt, column_metadata)
            self._initialize_description(column_metadata)
        
        except InterfaceError as e:
            log('error', f"Driver interface error during metadata retrieval: {e}")

        except Exception as e:
            # Log the exception with appropriate context
            log('error', f"Failed to retrieve column metadata: {e}. Using standard ODBC column definitions instead.")

        if not self.description:
            # If describe fails, create a manual description for the standard columns
            column_types = [int, str, int, str, int, int, int, int]
            self.description = [
                ("scope", column_types[0], None, 10, 10, 0, False),
                ("column_name", column_types[1], None, 128, 128, 0, False),
                ("data_type", column_types[2], None, 10, 10, 0, False),
                ("type_name", column_types[3], None, 128, 128, 0, False),
                ("column_size", column_types[4], None, 10, 10, 0, False),
                ("buffer_length", column_types[5], None, 10, 10, 0, False),
                ("decimal_digits", column_types[6], None, 10, 10, 0, True),
                ("pseudo_column", column_types[7], None, 10, 10, 0, False)
            ]
        
        # Create a column map with both ODBC standard names and lowercase aliases
        self._column_map = {}
        for i, (name, *_) in enumerate(self.description):
            # Add standard name
            self._column_map[name] = i
            # Add lowercase alias
            self._column_map[name.lower()] = i

        # Remember original fetch methods (store only once)
        if not hasattr(self, '_original_fetchone'):
            self._original_fetchone = self.fetchone
            self._original_fetchmany = self.fetchmany
            self._original_fetchall = self.fetchall

            # Create wrapper fetch methods that add column mappings
            def fetchone_with_mapping():
                row = self._original_fetchone()
                if row is not None:
                    row._column_map = self._column_map
                return row

            def fetchmany_with_mapping(size=None):
                rows = self._original_fetchmany(size)
                for row in rows:
                    row._column_map = self._column_map
                return rows

            def fetchall_with_mapping():
                rows = self._original_fetchall()
                for row in rows:
                    row._column_map = self._column_map
                return rows

            # Replace fetch methods
            self.fetchone = fetchone_with_mapping
            self.fetchmany = fetchmany_with_mapping
            self.fetchall = fetchall_with_mapping

        # Return the cursor itself
        return self

    def statistics(self, table: str, catalog: str = None, schema: str = None, unique: bool = False, quick: bool = True) -> 'Cursor':
        """
        Creates a result set of statistics about a single table and the indexes associated 
        with the table by executing SQLStatistics.
        
        Args:
            table (str): The name of the table.
            catalog (str, optional): The catalog name. Defaults to None.
            schema (str, optional): The schema name. Defaults to None.
            unique (bool, optional): If True, only unique indexes are returned. 
                                    If False, all indexes are returned. Defaults to False.
            quick (bool, optional): If True, CARDINALITY and PAGES are returned only 
                                    if readily available. Defaults to True.
        
        Returns:
            cursor: The cursor itself, containing the result set. Use fetchone(), fetchmany(),
                   or fetchall() to retrieve the results.
            
        Example:
            # Get statistics for the 'Customers' table
            stats_cursor = cursor.statistics(table='Customers')
            
            # Fetch rows as needed
            first_stat = stats_cursor.fetchone()
            next_10_stats = stats_cursor.fetchmany(10)
            all_remaining = stats_cursor.fetchall()
        """
        self._check_closed()
        
        # Always reset the cursor first to ensure clean state
        self._reset_cursor()

        # Table name is required
        if not table:
            raise ProgrammingError("Table name is required", "HY000")
        
        # Set unique flag (SQL_INDEX_UNIQUE = 0, SQL_INDEX_ALL = 1)
        unique_option = ddbc_sql_const.SQL_INDEX_UNIQUE.value if unique else ddbc_sql_const.SQL_INDEX_ALL.value
        
        # Set quick flag (SQL_QUICK = 0, SQL_ENSURE = 1)
        reserved_option = ddbc_sql_const.SQL_QUICK.value if quick else ddbc_sql_const.SQL_ENSURE.value
        
        # Call the SQLStatistics function
        retcode = ddbc_bindings.DDBCSQLStatistics(
            self.hstmt,
            catalog,
            schema,
            table,
            unique_option,
            reserved_option
        )
        check_error(ddbc_sql_const.SQL_HANDLE_STMT.value, self.hstmt, retcode)
        
        # Initialize description from column metadata
        column_metadata = []
        try:
            ddbc_bindings.DDBCSQLDescribeCol(self.hstmt, column_metadata)
            self._initialize_description(column_metadata)
        except InterfaceError as e:
            log('error', f"Driver interface error during metadata retrieval: {e}")

        except Exception as e:
            # Log the exception with appropriate context
            log('error', f"Failed to retrieve column metadata: {e}. Using standard ODBC column definitions instead.")

        if not self.description:
            # If describe fails, create a manual description for the standard columns
            column_types = [str, str, str, bool, str, str, int, int, str, str, int, int, str]
            self.description = [
                ("table_cat", column_types[0], None, 128, 128, 0, True),
                ("table_schem", column_types[1], None, 128, 128, 0, True),
                ("table_name", column_types[2], None, 128, 128, 0, False),
                ("non_unique", column_types[3], None, 1, 1, 0, False),
                ("index_qualifier", column_types[4], None, 128, 128, 0, True),
                ("index_name", column_types[5], None, 128, 128, 0, True),
                ("type", column_types[6], None, 10, 10, 0, False),
                ("ordinal_position", column_types[7], None, 10, 10, 0, False),
                ("column_name", column_types[8], None, 128, 128, 0, True),
                ("asc_or_desc", column_types[9], None, 1, 1, 0, True),
                ("cardinality", column_types[10], None, 20, 20, 0, True),
                ("pages", column_types[11], None, 20, 20, 0, True),
                ("filter_condition", column_types[12], None, 128, 128, 0, True)
            ]
        
        # Create a column map with both ODBC standard names and lowercase aliases
        self._column_map = {}
        for i, (name, *_) in enumerate(self.description):
            # Add standard name
            self._column_map[name] = i
            # Add lowercase alias
            self._column_map[name.lower()] = i
        
        # Remember original fetch methods (store only once)
        if not hasattr(self, '_original_fetchone'):
            self._original_fetchone = self.fetchone
            self._original_fetchmany = self.fetchmany
            self._original_fetchall = self.fetchall
    
            # Create wrapper fetch methods that add column mappings
            def fetchone_with_mapping():
                row = self._original_fetchone()
                if row is not None:
                    row._column_map = self._column_map
                return row
                
            def fetchmany_with_mapping(size=None):
                rows = self._original_fetchmany(size)
                for row in rows:
                    row._column_map = self._column_map
                return rows
                
            def fetchall_with_mapping():
                rows = self._original_fetchall()
                for row in rows:
                    row._column_map = self._column_map
                return rows
                
            # Replace fetch methods
            self.fetchone = fetchone_with_mapping
            self.fetchmany = fetchmany_with_mapping
            self.fetchall = fetchall_with_mapping
        
        return self
    
    def columns(self, table=None, catalog=None, schema=None, column=None):
        """
        Creates a result set of column information in the specified tables 
        using the SQLColumns function.
        
        Args:
            table (str, optional): The table name pattern. Default is None (all tables).
            catalog (str, optional): The catalog name. Default is None (current catalog).
            schema (str, optional): The schema name pattern. Default is None (all schemas).
            column (str, optional): The column name pattern. Default is None (all columns).
        
        Returns:
            cursor: The cursor itself, containing the result set. Use fetchone(), fetchmany(),
                or fetchall() to retrieve the results.

                Each row contains the following columns:
                - table_cat (str): Catalog name
                - table_schem (str): Schema name
                - table_name (str): Table name
                - column_name (str): Column name
                - data_type (int): The ODBC SQL data type constant (e.g. SQL_CHAR)
                - type_name (str): Data source dependent type name
                - column_size (int): Column size
                - buffer_length (int): Length of the column in bytes
                - decimal_digits (int): Number of fractional digits
                - num_prec_radix (int): Radix (typically 10 or 2)
                - nullable (int): One of SQL_NO_NULLS, SQL_NULLABLE, SQL_NULLABLE_UNKNOWN
                - remarks (str): Comments about the column
                - column_def (str): Default value for the column
                - sql_data_type (int): The SQL data type from java.sql.Types
                - sql_datetime_sub (int): Subcode for datetime types
                - char_octet_length (int): Maximum length in bytes for char types
                - ordinal_position (int): Column position in the table (starting at 1)
                - is_nullable (str): "YES", "NO", or "" (unknown)

        Warning:
            Calling this method without any filters (all parameters as None) will enumerate 
            EVERY column in EVERY table in the database. This can be extremely expensive in 
            large databases, potentially causing high memory usage, slow execution times, 
            and in extreme cases, timeout errors. Always use filters (catalog, schema, table, 
            or column) whenever possible to limit the result set.
    
        Example:
            # Get all columns in table 'Customers'
            columns = cursor.columns(table='Customers')
            
            # Get all columns in table 'Customers' in schema 'dbo'
            columns = cursor.columns(table='Customers', schema='dbo')
            
            # Get column named 'CustomerID' in any table
            columns = cursor.columns(column='CustomerID')
        """
        self._check_closed()
        
        # Always reset the cursor first to ensure clean state
        self._reset_cursor()
        
        # Call the SQLColumns function
        retcode = ddbc_bindings.DDBCSQLColumns(
            self.hstmt,
            catalog,
            schema,
            table,
            column
        )
        check_error(ddbc_sql_const.SQL_HANDLE_STMT.value, self.hstmt, retcode)
        
        # Initialize description from column metadata
        column_metadata = []
        try:
            ddbc_bindings.DDBCSQLDescribeCol(self.hstmt, column_metadata)
            self._initialize_description(column_metadata)
        except InterfaceError as e:
            log('error', f"Driver interface error during metadata retrieval: {e}")

        except Exception as e:
            # Log the exception with appropriate context
            log('error', f"Failed to retrieve column metadata: {e}. Using standard ODBC column definitions instead.")

        if not self.description:
            # If describe fails, create a manual description for the standard columns
            column_types = [str, str, str, str, int, str, int, int, int, int, int, str, str, int, int, int, int, str]
            self.description = [
                ("table_cat", column_types[0], None, 128, 128, 0, True),
                ("table_schem", column_types[1], None, 128, 128, 0, True),
                ("table_name", column_types[2], None, 128, 128, 0, False),
                ("column_name", column_types[3], None, 128, 128, 0, False),
                ("data_type", column_types[4], None, 10, 10, 0, False),
                ("type_name", column_types[5], None, 128, 128, 0, False),
                ("column_size", column_types[6], None, 10, 10, 0, True),
                ("buffer_length", column_types[7], None, 10, 10, 0, True),
                ("decimal_digits", column_types[8], None, 10, 10, 0, True),
                ("num_prec_radix", column_types[9], None, 10, 10, 0, True),
                ("nullable", column_types[10], None, 10, 10, 0, False),
                ("remarks", column_types[11], None, 254, 254, 0, True),
                ("column_def", column_types[12], None, 254, 254, 0, True),
                ("sql_data_type", column_types[13], None, 10, 10, 0, False),
                ("sql_datetime_sub", column_types[14], None, 10, 10, 0, True),
                ("char_octet_length", column_types[15], None, 10, 10, 0, True),
                ("ordinal_position", column_types[16], None, 10, 10, 0, False),
                ("is_nullable", column_types[17], None, 254, 254, 0, True)
            ]

        # Store the column mappings for this specific columns() call
        column_names = [desc[0] for desc in self.description]
        
        # Create a specialized column map for this result set
        columns_map = {}
        for i, name in enumerate(column_names):
            columns_map[name] = i
            columns_map[name.lower()] = i
        
        # Define wrapped fetch methods that preserve existing column mapping
        # but add our specialized mapping just for column results
        def fetchone_with_columns_mapping():
            row = self._original_fetchone()
            if row is not None:
                # Create a merged map with columns result taking precedence
                merged_map = getattr(row, '_column_map', {}).copy()
                merged_map.update(columns_map)
                row._column_map = merged_map
            return row
            
        def fetchmany_with_columns_mapping(size=None):
            rows = self._original_fetchmany(size)
            for row in rows:
                # Create a merged map with columns result taking precedence
                merged_map = getattr(row, '_column_map', {}).copy()
                merged_map.update(columns_map)
                row._column_map = merged_map
            return rows
            
        def fetchall_with_columns_mapping():
            rows = self._original_fetchall()
            for row in rows:
                # Create a merged map with columns result taking precedence
                merged_map = getattr(row, '_column_map', {}).copy()
                merged_map.update(columns_map)
                row._column_map = merged_map
            return rows
        
        # Save original fetch methods
        if not hasattr(self, '_original_fetchone'):
            self._original_fetchone = self.fetchone
            self._original_fetchmany = self.fetchmany
            self._original_fetchall = self.fetchall

        # Override fetch methods with our wrapped versions
        self.fetchone = fetchone_with_columns_mapping
        self.fetchmany = fetchmany_with_columns_mapping
        self.fetchall = fetchall_with_columns_mapping
            
      return self

    @staticmethod
    def _select_best_sample_value(column):
        """
        Selects the most representative non-null value from a column for type inference.

        This is used during executemany() to infer SQL/C types based on actual data,
        preferring a non-null value that is not the first row to avoid bias from placeholder defaults.

        Args:
            column: List of values in the column.
        """
        non_nulls = [v for v in column if v is not None]
        if not non_nulls:
            return None
        if all(isinstance(v, int) for v in non_nulls):
            # Pick the value with the widest range (min/max)
            return max(non_nulls, key=lambda v: abs(v))
        if all(isinstance(v, float) for v in non_nulls):
            return 0.0
        if all(isinstance(v, decimal.Decimal) for v in non_nulls):
            return max(non_nulls, key=lambda d: len(d.as_tuple().digits))
        if all(isinstance(v, str) for v in non_nulls):
            return max(non_nulls, key=lambda s: len(str(s)))
        if all(isinstance(v, datetime.datetime) for v in non_nulls):
            return datetime.datetime.now()
        if all(isinstance(v, datetime.date) for v in non_nulls):
            return datetime.date.today()
        return non_nulls[0]  # fallback

    def _transpose_rowwise_to_columnwise(self, seq_of_parameters: list) -> tuple[list, int]:
        """
        Convert sequence of rows (row-wise) into list of columns (column-wise),
        for array binding via ODBC. Works with both iterables and generators.
        
        Args:
            seq_of_parameters: Sequence of sequences or mappings of parameters.
            
        Returns:
            tuple: (columnwise_data, row_count)
        """
        columnwise = []
        first_row = True
        row_count = 0
        
        for row in seq_of_parameters:
            row_count += 1
            if first_row:
                # Initialize columnwise lists based on first row
                num_params = len(row)
                columnwise = [[] for _ in range(num_params)]
                first_row = False
            else:
                # Validate row size consistency
                if len(row) != num_params:
                    raise ValueError("Inconsistent parameter row size in executemany()")
        
            # Add each value to its column list
            for i, val in enumerate(row):
                columnwise[i].append(val)
        
        return columnwise, row_count

    def executemany(self, operation: str, seq_of_parameters: list) -> None:
        """
        Prepare a database operation and execute it against all parameter sequences.
        This version uses column-wise parameter binding and a single batched SQLExecute().
        
        Args:
            operation: SQL query or command.
            seq_of_parameters: Sequence of sequences or mappings of parameters.

        Raises:
            Error: If the operation fails.
        """
        self._check_closed()
        self._reset_cursor()

        if not seq_of_parameters:
            self.rowcount = 0
            return

        # Apply timeout if set (non-zero)
        if self._timeout > 0:
            try:
                timeout_value = int(self._timeout)
                ret = ddbc_bindings.DDBCSQLSetStmtAttr(
                    self.hstmt,
                    ddbc_sql_const.SQL_ATTR_QUERY_TIMEOUT.value,
                    timeout_value
                )
                check_error(ddbc_sql_const.SQL_HANDLE_STMT.value, self.hstmt, ret)
                log('debug', f"Set query timeout to {timeout_value} seconds")
            except Exception as e:
                log('warning', f"Failed to set query timeout: {e}")

        # Get sample row for parameter type detection and validation
        sample_row = seq_of_parameters[0] if hasattr(seq_of_parameters, '__getitem__') else next(iter(seq_of_parameters))
        param_count = len(sample_row)
        param_info = ddbc_bindings.ParamInfo
        parameters_type = []
        
        # Check if we have explicit input sizes set
        if self._inputsizes:
            # Validate input sizes match parameter count
            if len(self._inputsizes) != param_count:
                warnings.warn(
                    f"Number of input sizes ({len(self._inputsizes)}) does not match "
                    f"number of parameters ({param_count}). This may lead to unexpected behavior.",
                    Warning
                )

        # Prepare parameter type information
        for col_index in range(param_count):
            if self._inputsizes and col_index < len(self._inputsizes):
                # Use explicitly set input sizes
                sql_type, column_size, decimal_digits = self._inputsizes[col_index]
                c_type = self._get_c_type_for_sql_type(sql_type)
                
                paraminfo = param_info()
                paraminfo.paramCType = c_type
                paraminfo.paramSQLType = sql_type
                paraminfo.inputOutputType = ddbc_sql_const.SQL_PARAM_INPUT.value
                paraminfo.columnSize = column_size
                paraminfo.decimalDigits = decimal_digits
                parameters_type.append(paraminfo)
            else:
                # Use auto-detection for columns without explicit types
                column = [row[col_index] for row in seq_of_parameters] if hasattr(seq_of_parameters, '__getitem__') else []
                if not column:
                    # For generators, use the sample row for inference
                    sample_value = sample_row[col_index]
                else:
                    sample_value = self._select_best_sample_value(column)
                
                dummy_row = list(sample_row)
                parameters_type.append(
                    self._create_parameter_types_list(sample_value, param_info, dummy_row, col_index)
                )

        # Process parameters into column-wise format with possible type conversions
        # First, convert any Decimal types as needed for NUMERIC/DECIMAL columns
        processed_parameters = []
        for row in seq_of_parameters:
            processed_row = list(row)
            for i, val in enumerate(processed_row):
                if (parameters_type[i].paramSQLType in 
                    (ddbc_sql_const.SQL_DECIMAL.value, ddbc_sql_const.SQL_NUMERIC.value) and
                    not isinstance(val, decimal.Decimal) and val is not None):
                    try:
                        processed_row[i] = decimal.Decimal(str(val))
                    except:
                        pass  # Keep original value if conversion fails
            processed_parameters.append(processed_row)
        
        # Now transpose the processed parameters
        columnwise_params, row_count = self._transpose_rowwise_to_columnwise(processed_parameters)
        
        # Execute batched statement
        ret = ddbc_bindings.SQLExecuteMany(
            self.hstmt,
            operation,
            columnwise_params,
            parameters_type,
            row_count
        )
        
        try:
            check_error(ddbc_sql_const.SQL_HANDLE_STMT.value, self.hstmt, ret)
            self.rowcount = ddbc_bindings.DDBCSQLRowCount(self.hstmt)
            self.last_executed_stmt = operation
            self._initialize_description()
        finally:
            # Reset input sizes after execution
            self._reset_inputsizes()
        

    def fetchone(self) -> Union[None, Row]:
        """
        Fetch the next row of a query result set.
        
        Returns:
            Single Row object or None if no more data is available.
        """
        self._check_closed()  # Check if the cursor is closed

        # Fetch raw data
        row_data = []
        ret = ddbc_bindings.DDBCSQLFetchOne(self.hstmt, row_data)
        
        if ret == ddbc_sql_const.SQL_NO_DATA.value:
            return None
        
        # Create and return a Row object
        return Row(self, self.description, row_data)

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

        if size <= 0:
            return []
        
        # Fetch raw data
        rows_data = []
        ret = ddbc_bindings.DDBCSQLFetchMany(self.hstmt, rows_data, size)
        
        # Convert raw data to Row objects
        return [Row(self, self.description, row_data) for row_data in rows_data]

    def fetchall(self) -> List[Row]:
        """
        Fetch all (remaining) rows of a query result.
        
        Returns:
            List of Row objects.
        """
        self._check_closed()  # Check if the cursor is closed

        # Fetch raw data
        rows_data = []
        ret = ddbc_bindings.DDBCSQLFetchAll(self.hstmt, rows_data)
        
        # Convert raw data to Row objects
        return [Row(self, self.description, row_data) for row_data in rows_data]

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
        ret = ddbc_bindings.DDBCSQLMoreResults(self.hstmt)
        check_error(ddbc_sql_const.SQL_HANDLE_STMT.value, self.hstmt, ret)
        if ret == ddbc_sql_const.SQL_NO_DATA.value:
            return False
        return True

    def __del__(self):
        """
        Destructor to ensure the cursor is closed when it is no longer needed.
        This is a safety net to ensure resources are cleaned up
        even if close() was not called explicitly.
        """
        if "_closed" not in self.__dict__ or not self._closed:
            try:
                self.close()
            except Exception as e:
                # Don't raise an exception in __del__, just log it
                log('error', "Error during cursor cleanup in __del__: %s", e)