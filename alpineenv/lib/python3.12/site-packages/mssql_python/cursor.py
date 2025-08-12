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
from .row import Row


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

    def _create_parameter_types_list(self, parameter, param_info, parameters_list, i):
        """
        Maps parameter types for the given parameter.

        Args:
            parameter: parameter to bind.

        Returns:
            paraminfo.
        """
        paraminfo = param_info()
        sql_type, c_type, column_size, decimal_digits = self._map_sql_type(
            parameter, parameters_list, i
        )
        paraminfo.paramCType = c_type
        paraminfo.paramSQLType = sql_type
        paraminfo.inputOutputType = ddbc_sql_const.SQL_PARAM_INPUT.value
        paraminfo.columnSize = column_size
        paraminfo.decimalDigits = decimal_digits
        return paraminfo

    def _initialize_description(self):
        """
        Initialize the description attribute using SQLDescribeCol.
        """
        col_metadata = []
        ret = ddbc_bindings.DDBCSQLDescribeCol(self.hstmt, col_metadata)
        check_error(ddbc_sql_const.SQL_HANDLE_STMT.value, self.hstmt, ret)

        self.description = [
            (
                col["ColumnName"],
                self._map_data_type(col["DataType"]),
                None,
                col["ColumnSize"],
                col["ColumnSize"],
                col["DecimalDigits"],
                col["Nullable"] == ddbc_sql_const.SQL_NULLABLE.value,
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
        self._initialize_description()

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

        param_info = ddbc_bindings.ParamInfo
        param_count = len(seq_of_parameters[0])
        parameters_type = []

        for col_index in range(param_count):
            column = [row[col_index] for row in seq_of_parameters]
            sample_value = self._select_best_sample_value(column)
            dummy_row = list(seq_of_parameters[0])
            parameters_type.append(
                self._create_parameter_types_list(sample_value, param_info, dummy_row, col_index)
            )

        columnwise_params = self._transpose_rowwise_to_columnwise(seq_of_parameters)
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
        check_error(ddbc_sql_const.SQL_HANDLE_STMT.value, self.hstmt, ret)

        self.rowcount = ddbc_bindings.DDBCSQLRowCount(self.hstmt)
        self.last_executed_stmt = operation
        self._initialize_description()

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
        return Row(row_data, self.description)

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
        return [Row(row_data, self.description) for row_data in rows_data]

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