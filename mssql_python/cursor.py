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
import ctypes
import decimal
import uuid
import datetime
from typing import List, Union
from mssql_python.constants import ConstantsDDBC as ddbc_sql_const
from mssql_python.helpers import check_error, log
from mssql_python import ddbc_bindings
from mssql_python.exceptions import InterfaceError
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

    def setinputsizes(self, sizes):
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

    def _get_c_type_for_sql_type(self, sql_type):
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

    def _create_parameter_types_list(self, parameter, param_info, parameters_list, i):
        """
        Maps parameter types for the given parameter.

        Args:
            parameter: parameter to bind.

        Returns:
            paraminfo.
        """
        paraminfo = param_info()
        
        # Check if we have explicit type information from setinputsizes
        if hasattr(self, '_inputsizes') and self._inputsizes and i < len(self._inputsizes):
            # Use explicit type information
            sql_type, column_size, decimal_digits = self._inputsizes[i]
            
            # Determine the appropriate C type based on SQL type
            c_type = self._get_c_type_for_sql_type(sql_type)
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
                
            # Fetch all rows first
            rows_data = []
            ret = ddbc_bindings.DDBCSQLFetchAll(self.hstmt, rows_data)
            
            # If we have no rows, return an empty list
            if not rows_data:
                return []
                
            # Create a custom column map for our Row objects
            column_map = {
                'type_name': 0,
                'data_type': 1,
                'column_size': 2,
                'literal_prefix': 3,
                'literal_suffix': 4,
                'create_params': 5,
                'nullable': 6,
                'case_sensitive': 7,
                'searchable': 8,
                'unsigned_attribute': 9,
                'fixed_prec_scale': 10,
                'auto_unique_value': 11,
                'local_type_name': 12,
                'minimum_scale': 13,
                'maximum_scale': 14,
                'sql_data_type': 15,
                'sql_datetime_sub': 16,
                'num_prec_radix': 17,
                'interval_precision': 18
            }
            
            # Create result rows with the custom column map
            result_rows = []
            for row_data in rows_data:
                row = Row(self, self.description, row_data)
                # Manually add the column map
                row._column_map = column_map
                result_rows.append(row)
                
            return result_rows
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
        
        # Check if we're looking for temporary procedures (which start with #)
        # The ODBC SQLProcedures doesn't return temp procedures, so we need to handle them separately
        if procedure and procedure.startswith('#'):
            # Use direct SQL query to find temporary procedures in tempdb
            # SQL Server adds unique identifiers to temp procedure names in tempdb
            sql = """
            SELECT 
                DB_NAME() AS procedure_cat,
                USER_NAME(p.schema_id) AS procedure_schem, 
                ? AS procedure_name,  -- Use original name for consistency
                (SELECT COUNT(*) FROM tempdb.sys.parameters 
                 WHERE object_id = p.object_id AND is_output = 0) AS num_input_params,
                (SELECT COUNT(*) FROM tempdb.sys.parameters 
                 WHERE object_id = p.object_id AND is_output = 1) AS num_output_params,
                0 AS num_result_sets,
                CONVERT(VARCHAR(254), p.create_date) AS remarks,
                1 AS procedure_type
            FROM tempdb.sys.procedures p
            WHERE p.name LIKE ?
            """
            
            # The % wildcard will match any characters after the procedure name
            # This handles SQL Server's unique suffixes on temp procedure names
            like_pattern = procedure + '%'
            self.execute(sql, [procedure, like_pattern])
            rows = self.fetchall()
            
            # Set up the column map for attribute access
            column_names = [
                "procedure_cat", "procedure_schem", "procedure_name",
                "num_input_params", "num_output_params", "num_result_sets",
                "remarks", "procedure_type"
            ]
            column_map = {name: i for i, name in enumerate(column_names)}
            
            # Apply the column map to each row
            for row in rows:
                row._column_map = column_map
                
            return rows
        
        # For non-temporary procedures, use the SQLProcedures API
        # Convert parameters to empty strings if None
        catalog_str = "" if catalog is None else catalog
        schema_str = "" if schema is None else schema
        procedure_str = "" if procedure is None else procedure
        
        # Call the SQLProcedures function
        retcode = ddbc_bindings.DDBCSQLProcedures(self.hstmt, catalog_str, schema_str, procedure_str)
        check_error(ddbc_sql_const.SQL_HANDLE_STMT.value, self.hstmt, retcode)
        
        # Create column metadata and initialize description
        column_metadata = []
        try:
            ddbc_bindings.DDBCSQLDescribeCol(self.hstmt, column_metadata)
            self._initialize_description(column_metadata)
        except Exception as e:
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
        column_names = [
            "procedure_cat", "procedure_schem", "procedure_name",
            "num_input_params", "num_output_params", "num_result_sets",
            "remarks", "procedure_type"
        ]
        
        # Fetch all rows and create a custom column map
        rows_data = []
        ddbc_bindings.DDBCSQLFetchAll(self.hstmt, rows_data)
        
        # Create a column map for attribute access
        column_map = {name: i for i, name in enumerate(column_names)}
        
        # Create Row objects with the column map
        result_rows = []
        for row_data in rows_data:
            row = Row(self, self.description, row_data)
            row._column_map = column_map
            
            # Fix procedure name by removing semicolon and number if present
            # The ODBC driver may return names in format "procedure_name;1"
            if hasattr(row, 'procedure_name') and row.procedure_name and ';' in row.procedure_name:
                proc_name_parts = row.procedure_name.split(';')
                row._values[column_map["procedure_name"]] = proc_name_parts[0]
            
            result_rows.append(row)
        
        return result_rows

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

    def _transpose_rowwise_to_columnwise(self, seq_of_parameters: list) -> list:
        """
        Convert list of rows (row-wise) into list of columns (column-wise),
        for array binding via ODBC.
        Args:
            seq_of_parameters: Sequence of sequences or mappings of parameters.
        """
        if not seq_of_parameters:
            return []

        num_params = len(seq_of_parameters[0])
        columnwise = [[] for _ in range(num_params)]
        for row in seq_of_parameters:
            if len(row) != num_params:
                raise ValueError("Inconsistent parameter row size in executemany()")
            for i, val in enumerate(row):
                columnwise[i].append(val)
        return columnwise

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

        param_info = ddbc_bindings.ParamInfo
        param_count = len(seq_of_parameters[0])
        parameters_type = []
        
        # Make a copy of the parameters for potential transformation
        processed_parameters = [list(params) for params in seq_of_parameters]

        # Check if we have explicit input sizes set
        if hasattr(self, '_inputsizes') and self._inputsizes:
            # Use the explicitly set input sizes
            for col_index in range(param_count):
                if col_index < len(self._inputsizes):
                    sql_type, column_size, decimal_digits = self._inputsizes[col_index]
                    c_type = self._get_c_type_for_sql_type(sql_type)

                    # If using SQL_DECIMAL/NUMERIC, we need to ensure the Python values
                    # are properly converted for the driver
                    if sql_type in (ddbc_sql_const.SQL_DECIMAL.value, ddbc_sql_const.SQL_NUMERIC.value):
                        # Make sure all values in this column are Decimal objects
                        for row_idx, row in enumerate(processed_parameters):
                            if not isinstance(row[col_index], decimal.Decimal):
                                # Convert to Decimal if it's not already
                                processed_parameters[row_idx][col_index] = decimal.Decimal(str(row[col_index]))

                    paraminfo = param_info()
                    paraminfo.paramCType = c_type
                    paraminfo.paramSQLType = sql_type
                    paraminfo.inputOutputType = ddbc_sql_const.SQL_PARAM_INPUT.value
                    paraminfo.columnSize = column_size
                    paraminfo.decimalDigits = decimal_digits
                    parameters_type.append(paraminfo)
                else:
                    # Fall back to auto-detect for any parameters beyond those specified
                    column = [row[col_index] for row in seq_of_parameters]
                    sample_value = self._select_best_sample_value(column)
                    dummy_row = list(seq_of_parameters[0])
                    parameters_type.append(
                        self._create_parameter_types_list(sample_value, param_info, dummy_row, col_index)
                    )
        else:
            # No input sizes set, use auto-detection
            for col_index in range(param_count):
                column = [row[col_index] for row in seq_of_parameters]
                sample_value = self._select_best_sample_value(column)
                dummy_row = list(seq_of_parameters[0])
                parameters_type.append(
                    self._create_parameter_types_list(sample_value, param_info, dummy_row, col_index)
                )


        columnwise_params = self._transpose_rowwise_to_columnwise(processed_parameters)
        
        log('info', "Executing batch query with %d parameter sets:\n%s",
            len(seq_of_parameters), "\n".join(f"  {i+1}: {tuple(p) if isinstance(p, (list, tuple)) else p}" for i, p in enumerate(seq_of_parameters))
        )

        # Execute batched statement
        ret = ddbc_bindings.SQLExecuteMany(
            self.hstmt,
            operation,
            columnwise_params,
            parameters_type,
            len(seq_of_parameters)
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