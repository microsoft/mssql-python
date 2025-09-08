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
from typing import List, Union
from mssql_python.constants import ConstantsDDBC as ddbc_sql_const
from mssql_python.helpers import check_error, log
from mssql_python import ddbc_bindings
from mssql_python.exceptions import InterfaceError, NotSupportedError, ProgrammingError
from .row import Row

# Constants for string handling
MAX_INLINE_CHAR = 4000  # NVARCHAR/VARCHAR inline limit; this triggers NVARCHAR(MAX)/VARCHAR(MAX) + DAE

class Cursor:
    """
    Represents a database cursor, which is used to manage the context of a fetch operation.

    Attributes:
        connection: Database connection object.
        description: Sequence of 7-item sequences describing one result column.
        rowcount: Number of rows produced or affected by the last execute operation.
        arraysize: Number of rows to fetch at a time with fetchmany().
        rownumber: Track the current row index in the result set.

    Methods:
        __init__(connection_str) -> None.
        callproc(procname, parameters=None) -> 
            Modified copy of the input sequence with output parameters.
        close() -> None.
        execute(operation, parameters=None) -> Cursor.
        executemany(operation, seq_of_parameters) -> None.
        fetchone() -> Single sequence or None if no more data is available.
        fetchmany(size=None) -> Sequence of sequences (e.g. list of tuples).
        fetchall() -> Sequence of sequences (e.g. list of tuples).
        nextset() -> True if there is another result set, None otherwise.
        next() -> Fetch the next row from the cursor.
        setinputsizes(sizes) -> None.
        setoutputsize(size, column=None) -> None.
    """

    def __init__(self, connection) -> None:
        """
        Initialize the cursor with a database connection.

        Args:
            connection: Database connection object.
        """
        self._connection = connection  # Store as private attribute
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
        
        # rownumber attribute
        self._rownumber = -1  # DB-API extension: last returned row index, -1 before first
        self._next_row_index = 0  # internal: index of the next row the driver will return (0-based)
        self._has_result_set = False  # Track if we have an active result set
        self._skip_increment_for_next_fetch = False  # Track if we need to skip incrementing the row index

        self.messages = []  # Store diagnostic messages

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
                ddbc_sql_const.SQL_VARCHAR.value,
                ddbc_sql_const.SQL_C_DEFAULT.value,
                1,
                0,
                False,
            )

        if isinstance(param, bool):
            return ddbc_sql_const.SQL_BIT.value, ddbc_sql_const.SQL_C_BIT.value, 1, 0, False

        if isinstance(param, int):
            if 0 <= param <= 255:
                return (
                    ddbc_sql_const.SQL_TINYINT.value,
                    ddbc_sql_const.SQL_C_TINYINT.value,
                    3,
                    0,
                    False,
                )
            if -32768 <= param <= 32767:
                return (
                    ddbc_sql_const.SQL_SMALLINT.value,
                    ddbc_sql_const.SQL_C_SHORT.value,
                    5,
                    0,
                    False,
                )
            if -2147483648 <= param <= 2147483647:
                return (
                    ddbc_sql_const.SQL_INTEGER.value,
                    ddbc_sql_const.SQL_C_LONG.value,
                    10,
                    0,
                    False,
                )
            return (
                ddbc_sql_const.SQL_BIGINT.value,
                ddbc_sql_const.SQL_C_SBIGINT.value,
                19,
                0,
                False,
            )

        if isinstance(param, float):
            return (
                ddbc_sql_const.SQL_DOUBLE.value,
                ddbc_sql_const.SQL_C_DOUBLE.value,
                15,
                0,
                False,
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
                False,
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
                    False,
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
                    False,
                )
            if self._parse_datetime(param):
                parameters_list[i] = self._parse_datetime(param)
                return (
                    ddbc_sql_const.SQL_TIMESTAMP.value,
                    ddbc_sql_const.SQL_C_TYPE_TIMESTAMP.value,
                    26,
                    6,
                    False,
                )
            if self._parse_time(param):
                parameters_list[i] = self._parse_time(param)
                return (
                    ddbc_sql_const.SQL_TIME.value,
                    ddbc_sql_const.SQL_C_TYPE_TIME.value,
                    8,
                    0,
                    False,
                )

            # String mapping logic here
            is_unicode = self._is_unicode_string(param)

            # Computes UTF-16 code units (handles surrogate pairs)
            utf16_len = sum(2 if ord(c) > 0xFFFF else 1 for c in param)
            if utf16_len > MAX_INLINE_CHAR:  # Long strings -> DAE
                if is_unicode:
                    return (
                        ddbc_sql_const.SQL_WLONGVARCHAR.value,
                        ddbc_sql_const.SQL_C_WCHAR.value,
                        utf16_len,
                        0,
                        True,
                    )
                return (
                    ddbc_sql_const.SQL_LONGVARCHAR.value,
                    ddbc_sql_const.SQL_C_CHAR.value,
                    len(param),
                    0,
                    True,
                )

            # Short strings
            if is_unicode:
                return (
                    ddbc_sql_const.SQL_WVARCHAR.value,
                    ddbc_sql_const.SQL_C_WCHAR.value,
                    utf16_len,
                    0,
                    False,
                )
            return (
                ddbc_sql_const.SQL_VARCHAR.value,
                ddbc_sql_const.SQL_C_CHAR.value,
                len(param),
                0,
                False,
            )
        
        if isinstance(param, bytes):
            # Use VARBINARY for Python bytes/bytearray since they are variable-length by nature.
            # This avoids storage waste from BINARY's zero-padding and matches Python's semantics.
            return (
                ddbc_sql_const.SQL_VARBINARY.value,
                ddbc_sql_const.SQL_C_BINARY.value,
                len(param),
                0,
                False,
            )

        if isinstance(param, bytearray):
            # Use VARBINARY for Python bytes/bytearray since they are variable-length by nature.
            # This avoids storage waste from BINARY's zero-padding and matches Python's semantics.
            return (
                ddbc_sql_const.SQL_VARBINARY.value,
                ddbc_sql_const.SQL_C_BINARY.value,
                len(param),
                0,
                False,
            )

        if isinstance(param, datetime.datetime):
            return (
                ddbc_sql_const.SQL_TIMESTAMP.value,
                ddbc_sql_const.SQL_C_TYPE_TIMESTAMP.value,
                26,
                6,
                False,
            )

        if isinstance(param, datetime.date):
            return (
                ddbc_sql_const.SQL_DATE.value,
                ddbc_sql_const.SQL_C_TYPE_DATE.value,
                10,
                0,
                False,
            )

        if isinstance(param, datetime.time):
            return (
                ddbc_sql_const.SQL_TIME.value,
                ddbc_sql_const.SQL_C_TYPE_TIME.value,
                8,
                0,
                False,
            )

        # For safety: unknown/unhandled Python types should not silently go to SQL
        raise TypeError("Unsupported parameter type: The driver cannot safely convert it to a SQL type.")

    def _initialize_cursor(self) -> None:
        """
        Initialize the DDBC statement handle.
        """
        self._allocate_statement_handle()

    def _allocate_statement_handle(self):
        """
        Allocate the DDBC statement handle.
        """
        self.hstmt = self._connection._conn.alloc_statement_handle()

    def _reset_cursor(self) -> None:
        """
        Reset the DDBC statement handle.
        """
        if self.hstmt:
            self.hstmt.free()
            self.hstmt = None
            log('debug', "SQLFreeHandle succeeded")     
        
        self._clear_rownumber()
        
        # Reinitialize the statement handle
        self._initialize_cursor()

    def close(self) -> None:
        """
        Close the connection now (rather than whenever .__del__() is called).
        Idempotent: subsequent calls have no effect and will be no-ops.

        The cursor will be unusable from this point forward; an InterfaceError
        will be raised if any operation (other than close) is attempted with the cursor.
        This is a deviation from pyodbc, which raises an exception if the cursor is already closed.
        """
        if self.closed:
            # Do nothing - not calling _check_closed() here since we want this to be idempotent
            return

        # Clear messages per DBAPI
        self.messages = []
        
        if self.hstmt:
            self.hstmt.free()
            self.hstmt = None
            log('debug', "SQLFreeHandle succeeded")
        self._clear_rownumber()
        self.closed = True

    def _check_closed(self):
        """
        Check if the cursor is closed and raise an exception if it is.

        Raises:
            ProgrammingError: If the cursor is closed.
        """
        if self.closed:
            raise ProgrammingError(
                driver_error="Operation cannot be performed: The cursor is closed.",
                ddbc_error=""
            )

    def _create_parameter_types_list(self, parameter, param_info, parameters_list, i):
        """
        Maps parameter types for the given parameter.

        Args:
            parameter: parameter to bind.

        Returns:
            paraminfo.
        """
        paraminfo = param_info()
        sql_type, c_type, column_size, decimal_digits, is_dae = self._map_sql_type(
            parameter, parameters_list, i
        )
        paraminfo.paramCType = c_type
        paraminfo.paramSQLType = sql_type
        paraminfo.inputOutputType = ddbc_sql_const.SQL_PARAM_INPUT.value
        paraminfo.columnSize = column_size
        paraminfo.decimalDigits = decimal_digits
        paraminfo.isDAE = is_dae

        if is_dae:
            paraminfo.dataPtr = parameter  # Will be converted to py::object* in C++

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
    
    @property
    def rownumber(self):
        """
        DB-API extension: Current 0-based index of the cursor in the result set.
        
        Returns:
            int or None: The current 0-based index of the cursor in the result set,
                        or None if no row has been fetched yet or the index cannot be determined.
        
        Note:
            - Returns -1 before the first successful fetch
            - Returns 0 after fetching the first row
            - Returns -1 for empty result sets (since no rows can be fetched)
        
        Warning:
            This is a DB-API extension and may not be portable across different
            database modules.
        """
        # Use mssql_python logging system instead of standard warnings
        log('warning', "DB-API extension cursor.rownumber used")

        # Return None if cursor is closed or no result set is available
        if self.closed or not self._has_result_set:
            return -1
        
        return self._rownumber  # Will be None until first fetch, then 0, 1, 2, etc.

    @property
    def connection(self):
        """
        DB-API 2.0 attribute: Connection object that created this cursor.
        
        This is a read-only reference to the Connection object that was used to create
        this cursor. This attribute is useful for polymorphic code that needs access
        to connection-level functionality.
        
        Returns:
            Connection: The connection object that created this cursor.
            
        Note:
            This attribute is read-only as specified by DB-API 2.0. Attempting to
            assign to this attribute will raise an AttributeError.
        """
        return self._connection
    
    def _reset_rownumber(self):
        """Reset the rownumber tracking when starting a new result set."""
        self._rownumber = -1
        self._next_row_index = 0
        self._has_result_set = True
        self._skip_increment_for_next_fetch = False

    def _increment_rownumber(self):
        """
        Called after a successful fetch from the driver. Keep both counters consistent.
        """
        if self._has_result_set:
            # driver returned one row, so the next row index increments by 1
            self._next_row_index += 1
            # rownumber is last returned row index
            self._rownumber = self._next_row_index - 1
        else:
            raise InterfaceError("Cannot increment rownumber: no active result set.", "No active result set.")
        
    # Will be used when we add support for scrollable cursors
    def _decrement_rownumber(self):
        """
        Decrement the rownumber by 1.
        
        This could be used for error recovery or cursor positioning operations.
        """
        if self._has_result_set and self._rownumber >= 0:
            if self._rownumber > 0:
                self._rownumber -= 1
            else:
                self._rownumber = -1
        else:
            raise InterfaceError("Cannot decrement rownumber: no active result set.", "No active result set.")

    def _clear_rownumber(self):
        """
        Clear the rownumber tracking.
        
        This should be called when the result set is cleared or when the cursor is reset.
        """
        self._rownumber = -1
        self._has_result_set = False
        self._skip_increment_for_next_fetch = False

    def __iter__(self):
        """
        Return the cursor itself as an iterator.
        
        This allows direct iteration over the cursor after execute():
        
        for row in cursor.execute("SELECT * FROM table"):
            print(row)
        """
        self._check_closed()
        return self
    
    def __next__(self):
        """
        Fetch the next row when iterating over the cursor.
        
        Returns:
            The next Row object.
            
        Raises:
            StopIteration: When no more rows are available.
        """
        self._check_closed()
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row
    
    def next(self):
        """
        Fetch the next row from the cursor.
        
        This is an alias for __next__() to maintain compatibility with older code.
        
        Returns:
            The next Row object.
            
        Raises:
            StopIteration: When no more rows are available.
        """
        return self.__next__()

    def execute(
        self,
        operation: str,
        *parameters,
        use_prepare: bool = True,
        reset_cursor: bool = True
    ) -> 'Cursor':
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

        # Clear any previous messages
        self.messages = []

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
        # Check return code
        try:
            
        # Check for errors but don't raise exceptions for info/warning messages
            check_error(ddbc_sql_const.SQL_HANDLE_STMT.value, self.hstmt, ret)
        except Exception as e:
            log('warning', "Execute failed, resetting cursor: %s", e)
            self._reset_cursor()
            raise

        
        # Capture any diagnostic messages (SQL_SUCCESS_WITH_INFO, etc.)
        if self.hstmt:
            self.messages.extend(ddbc_bindings.DDBCSQLGetAllDiagRecords(self.hstmt))
    
        self.last_executed_stmt = operation

        # Update rowcount after execution
        # TODO: rowcount return code from SQL needs to be handled
        self.rowcount = ddbc_bindings.DDBCSQLRowCount(self.hstmt)

        # Initialize description after execution
        self._initialize_description()
        
        # Reset rownumber for new result set (only for SELECT statements)
        if self.description:  # If we have column descriptions, it's likely a SELECT
            self.rowcount = -1
            self._reset_rownumber()
        else:
            self.rowcount = ddbc_bindings.DDBCSQLRowCount(self.hstmt)
            self._clear_rownumber()

        # Return self for method chaining
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
        if all(isinstance(v, (bytes, bytearray)) for v in non_nulls):
            return max(non_nulls, key=lambda b: len(b))
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
        
        # Clear any previous messages
        self.messages = []
        
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

        # Capture any diagnostic messages after execution
        if self.hstmt:
            self.messages.extend(ddbc_bindings.DDBCSQLGetAllDiagRecords(self.hstmt))
    
        self.rowcount = ddbc_bindings.DDBCSQLRowCount(self.hstmt)
        self.last_executed_stmt = operation
        self._initialize_description()
        
        if self.description:
            self.rowcount = -1
            self._reset_rownumber()
        else:
            self.rowcount = ddbc_bindings.DDBCSQLRowCount(self.hstmt)
            self._clear_rownumber()

    def fetchone(self) -> Union[None, Row]:
        """
        Fetch the next row of a query result set.
        
        Returns:
            Single Row object or None if no more data is available.
        """
        self._check_closed()  # Check if the cursor is closed

        # Fetch raw data
        row_data = []
        try:
            ret = ddbc_bindings.DDBCSQLFetchOne(self.hstmt, row_data)
            
            if self.hstmt:
                self.messages.extend(ddbc_bindings.DDBCSQLGetAllDiagRecords(self.hstmt))
            
            if ret == ddbc_sql_const.SQL_NO_DATA.value:
                return None
            
            # Update internal position after successful fetch
            if self._skip_increment_for_next_fetch:
                self._skip_increment_for_next_fetch = False
                self._next_row_index += 1
            else:
                self._increment_rownumber()
            
            # Create and return a Row object, passing column name map if available
            column_map = getattr(self, '_column_name_map', None)
            return Row(row_data, self.description, column_map)
        except Exception as e:
            # On error, don't increment rownumber - rethrow the error
            raise e

    def fetchmany(self, size: int = None) -> List[Row]:
        """
        Fetch the next set of rows of a query result.
        
        Args:
            size: Number of rows to fetch at a time.
        
        Returns:
            List of Row objects.
        """
        self._check_closed()  # Check if the cursor is closed
        if not self._has_result_set and self.description:
            self._reset_rownumber()

        if size is None:
            size = self.arraysize

        if size <= 0:
            return []
        
        # Fetch raw data
        rows_data = []
        try:
            ret = ddbc_bindings.DDBCSQLFetchMany(self.hstmt, rows_data, size)

            if self.hstmt:
                self.messages.extend(ddbc_bindings.DDBCSQLGetAllDiagRecords(self.hstmt))
            
            
            # Update rownumber for the number of rows actually fetched
            if rows_data and self._has_result_set:
                # advance counters by number of rows actually returned
                self._next_row_index += len(rows_data)
                self._rownumber = self._next_row_index - 1
            
            # Convert raw data to Row objects
            column_map = getattr(self, '_column_name_map', None)
            return [Row(row_data, self.description, column_map) for row_data in rows_data]
        except Exception as e:
            # On error, don't increment rownumber - rethrow the error
            raise e

    def fetchall(self) -> List[Row]:
        """
        Fetch all (remaining) rows of a query result.
        
        Returns:
            List of Row objects.
        """
        self._check_closed()  # Check if the cursor is closed
        if not self._has_result_set and self.description:
            self._reset_rownumber()

        # Fetch raw data
        rows_data = []
        try:
            ret = ddbc_bindings.DDBCSQLFetchAll(self.hstmt, rows_data)

            if self.hstmt:
                self.messages.extend(ddbc_bindings.DDBCSQLGetAllDiagRecords(self.hstmt))
            
            
            # Update rownumber for the number of rows actually fetched
            if rows_data and self._has_result_set:
                self._next_row_index += len(rows_data)
                self._rownumber = self._next_row_index - 1
            
            # Convert raw data to Row objects
            column_map = getattr(self, '_column_name_map', None)
            return [Row(row_data, self.description, column_map) for row_data in rows_data]
        except Exception as e:
            # On error, don't increment rownumber - rethrow the error
            raise e

    def nextset(self) -> Union[bool, None]:
        """
        Skip to the next available result set.

        Returns:
            True if there is another result set, None otherwise.

        Raises:
            Error: If the previous call to execute did not produce any result set.
        """
        self._check_closed()  # Check if the cursor is closed

        # Clear messages per DBAPI
        self.messages = []
        
        # Skip to the next result set
        ret = ddbc_bindings.DDBCSQLMoreResults(self.hstmt)
        check_error(ddbc_sql_const.SQL_HANDLE_STMT.value, self.hstmt, ret)
        
        if ret == ddbc_sql_const.SQL_NO_DATA.value:
            self._clear_rownumber()
            return False

        self._reset_rownumber()

        return True

    def __enter__(self):
        """
        Enter the runtime context for the cursor.
        
        Returns:
            The cursor instance itself.
        """
        self._check_closed()
        return self
    
    def __exit__(self, *args):
        """Closes the cursor when exiting the context, ensuring proper resource cleanup."""
        if not self.closed:
            self.close()
        return None

    def fetchval(self):
        """
        Fetch the first column of the first row if there are results.
        
        This is a convenience method for queries that return a single value,
        such as SELECT COUNT(*) FROM table, SELECT MAX(id) FROM table, etc.
        
        Returns:
            The value of the first column of the first row, or None if no rows
            are available or the first column value is NULL.
            
        Raises:
            Exception: If the cursor is closed.
            
        Example:
            >>> count = cursor.execute('SELECT COUNT(*) FROM users').fetchval()
            >>> max_id = cursor.execute('SELECT MAX(id) FROM products').fetchval()
            >>> name = cursor.execute('SELECT name FROM users WHERE id = ?', user_id).fetchval()
            
        Note:
            This is a convenience extension beyond the DB-API 2.0 specification.
            After calling fetchval(), the cursor position advances by one row,
            just like fetchone().
        """
        self._check_closed()  # Check if the cursor is closed
        
        # Check if this is a result-producing statement
        if not self.description:
            # Non-result-set statement (INSERT, UPDATE, DELETE, etc.)
            return None
        
        # Fetch the first row
        row = self.fetchone()
        
        return None if row is None else row[0]

    def commit(self):
        """
        Commit all SQL statements executed on the connection that created this cursor.
        
        This is a convenience method that calls commit() on the underlying connection.
        It affects all cursors created by the same connection since the last commit/rollback.
        
        The benefit is that many uses can now just use the cursor and not have to track
        the connection object.
        
        Raises:
            Exception: If the cursor is closed or if the commit operation fails.
            
        Example:
            >>> cursor.execute("INSERT INTO users (name) VALUES (?)", "John")
            >>> cursor.commit()  # Commits the INSERT
            
        Note:
            This is equivalent to calling connection.commit() but provides convenience
            for code that only has access to the cursor object.
        """
        self._check_closed()  # Check if the cursor is closed
        
        # Clear messages per DBAPI
        self.messages = []
        
        # Delegate to the connection's commit method
        self._connection.commit()

    def rollback(self):
        """
        Roll back all SQL statements executed on the connection that created this cursor.
        
        This is a convenience method that calls rollback() on the underlying connection.
        It affects all cursors created by the same connection since the last commit/rollback.
        
        The benefit is that many uses can now just use the cursor and not have to track
        the connection object.
        
        Raises:
            Exception: If the cursor is closed or if the rollback operation fails.
            
        Example:
            >>> cursor.execute("INSERT INTO users (name) VALUES (?)", "John")
            >>> cursor.rollback()  # Rolls back the INSERT
            
        Note:
            This is equivalent to calling connection.rollback() but provides convenience
            for code that only has access to the cursor object.
        """
        self._check_closed()  # Check if the cursor is closed
        
        # Clear messages per DBAPI
        self.messages = []
        
        # Delegate to the connection's rollback method
        self._connection.rollback()

    def __del__(self):
        """
        Destructor to ensure the cursor is closed when it is no longer needed.
        This is a safety net to ensure resources are cleaned up
        even if close() was not called explicitly.
        If the cursor is already closed, it will not raise an exception during cleanup.
        """
        if "closed" not in self.__dict__ or not self.closed:
            try:
                self.close()
            except Exception as e:
                # Don't raise an exception in __del__, just log it
                # If interpreter is shutting down, we might not have logging set up
                import sys
                if sys and sys._is_finalizing():
                    # Suppress logging during interpreter shutdown
                    return
                log('debug', "Exception during cursor cleanup in __del__: %s", e)

    def scroll(self, value: int, mode: str = 'relative') -> None:
        """
        Scroll using SQLFetchScroll only, matching test semantics:
          - relative(N>0): consume N rows; rownumber = previous + N; next fetch returns the following row.
          - absolute(-1): before first (rownumber = -1), no data consumed.
          - absolute(0): position so next fetch returns first row; rownumber stays 0 even after that fetch.
          - absolute(k>0): next fetch returns row index k (0-based); rownumber == k after scroll.
        """
        self._check_closed()
        
        # Clear messages per DBAPI
        self.messages = []
        
        if mode not in ('relative', 'absolute'):
            raise ProgrammingError("Invalid scroll mode",
                                   f"mode must be 'relative' or 'absolute', got '{mode}'")
        if not self._has_result_set:
            raise ProgrammingError("No active result set",
                                   "Cannot scroll: no result set available. Execute a query first.")
        if not isinstance(value, int):
            raise ProgrammingError("Invalid scroll value type",
                                   f"scroll value must be an integer, got {type(value).__name__}")
    
        # Relative backward not supported
        if mode == 'relative' and value < 0:
            raise NotSupportedError("Backward scrolling not supported",
                                    f"Cannot move backward by {value} rows on a forward-only cursor")
    
        row_data: list = []
    
        # Absolute special cases
        if mode == 'absolute':
            if value == -1:
                # Before first
                ddbc_bindings.DDBCSQLFetchScroll(self.hstmt,
                                                 ddbc_sql_const.SQL_FETCH_ABSOLUTE.value,
                                                 0, row_data)
                self._rownumber = -1
                self._next_row_index = 0
                return
            if value == 0:
                # Before first, but tests want rownumber==0 pre and post the next fetch
                ddbc_bindings.DDBCSQLFetchScroll(self.hstmt,
                                                 ddbc_sql_const.SQL_FETCH_ABSOLUTE.value,
                                                 0, row_data)
                self._rownumber = 0
                self._next_row_index = 0
                self._skip_increment_for_next_fetch = True
                return
    
        try:
            if mode == 'relative':
                if value == 0:
                    return
                ret = ddbc_bindings.DDBCSQLFetchScroll(self.hstmt,
                                                       ddbc_sql_const.SQL_FETCH_RELATIVE.value,
                                                       value, row_data)
                if ret == ddbc_sql_const.SQL_NO_DATA.value:
                    raise IndexError("Cannot scroll to specified position: end of result set reached")
                # Consume N rows; last-returned index advances by N
                self._rownumber = self._rownumber + value
                self._next_row_index = self._rownumber + 1
                return
    
            # absolute(k>0): map Python k (0-based next row) to ODBC ABSOLUTE k (1-based),
            # intentionally passing k so ODBC fetches row #k (1-based), i.e., 0-based (k-1),
            # leaving the NEXT fetch to return 0-based index k.
            ret = ddbc_bindings.DDBCSQLFetchScroll(self.hstmt,
                                                   ddbc_sql_const.SQL_FETCH_ABSOLUTE.value,
                                                   value, row_data)
            if ret == ddbc_sql_const.SQL_NO_DATA.value:
                raise IndexError(f"Cannot scroll to position {value}: end of result set reached")
    
            # Tests expect rownumber == value after absolute(value)
            # Next fetch should return row index 'value'
            self._rownumber = value
            self._next_row_index = value
    
        except Exception as e:
            if isinstance(e, (IndexError, NotSupportedError)):
                raise
            raise IndexError(f"Scroll operation failed: {e}") from e
            
    def skip(self, count: int) -> None:
        """
        Skip the next count records in the query result set.
        
        Args:
            count: Number of records to skip.
            
        Raises:
            IndexError: If attempting to skip past the end of the result set.
            ProgrammingError: If count is not an integer.
            NotSupportedError: If attempting to skip backwards.
        """
        from mssql_python.exceptions import ProgrammingError, NotSupportedError
    
        self._check_closed()
        
        # Clear messages
        self.messages = []
        
        # Simply delegate to the scroll method with 'relative' mode
        self.scroll(count, 'relative')

    def _execute_tables(self, stmt_handle, catalog_name=None, schema_name=None, table_name=None, 
                  table_type=None, search_escape=None):
        """
        Execute SQLTables ODBC function to retrieve table metadata.
        
        Args:
            stmt_handle: ODBC statement handle
            catalog_name: The catalog name pattern
            schema_name: The schema name pattern
            table_name: The table name pattern
            table_type: The table type filter
            search_escape: The escape character for pattern matching
        """
        # Convert None values to empty strings for ODBC
        catalog = "" if catalog_name is None else catalog_name
        schema = "" if schema_name is None else schema_name
        table = "" if table_name is None else table_name
        types = "" if table_type is None else table_type
        
        # Call the ODBC SQLTables function
        retcode = ddbc_bindings.DDBCSQLTables(
            stmt_handle,
            catalog, 
            schema,
            table,
            types
        )
        
        # Check return code and handle errors
        check_error(ddbc_sql_const.SQL_HANDLE_STMT.value, stmt_handle, retcode)
        
        # Capture any diagnostic messages
        if stmt_handle:
            self.messages.extend(ddbc_bindings.DDBCSQLGetAllDiagRecords(stmt_handle))

    def tables(self, table=None, catalog=None, schema=None, tableType=None):
        """
        Returns information about tables in the database that match the given criteria using
        the SQLTables ODBC function.
        
        Args:
            table (str, optional): The table name pattern. Default is None (all tables).
            catalog (str, optional): The catalog name. Default is None.
            schema (str, optional): The schema name pattern. Default is None.
            tableType (str or list, optional): The table type filter. Default is None.
                                              Example: "TABLE" or ["TABLE", "VIEW"]
        
        Returns:
            list: A list of Row objects containing table information with these columns:
                  - table_cat: Catalog name
                  - table_schem: Schema name
                  - table_name: Table name
                  - table_type: Table type (e.g., "TABLE", "VIEW")
                  - remarks: Comments about the table
        
        Notes:
            This method only processes the standard five columns as defined in the ODBC
            specification. Any additional columns that might be returned by specific ODBC
            drivers are not included in the result set.
        
        Example:
            # Get all tables in the database
            tables = cursor.tables()
            
            # Get all tables in schema 'dbo'
            tables = cursor.tables(schema='dbo')
            
            # Get table named 'Customers'
            tables = cursor.tables(table='Customers')
            
            # Get all views
            tables = cursor.tables(tableType='VIEW')
        """
        self._check_closed()
        
        # Clear messages
        self.messages = []
        
        # Always reset the cursor first to ensure clean state
        self._reset_cursor()
        
        # Format table_type parameter - SQLTables expects comma-separated string
        table_type_str = None
        if tableType is not None:
            if isinstance(tableType, (list, tuple)):
                table_type_str = ",".join(tableType)
            else:
                table_type_str = str(tableType)
        
        # Call SQLTables via the helper method
        self._execute_tables(
            self.hstmt,
            catalog_name=catalog,
            schema_name=schema,
            table_name=table,
            table_type=table_type_str
        )
        
        # Initialize description from column metadata
        column_metadata = []
        try:
            ddbc_bindings.DDBCSQLDescribeCol(self.hstmt, column_metadata)
            self._initialize_description(column_metadata)
        except Exception:
            # If describe fails, create a manual description for the standard columns
            column_types = [str, str, str, str, str]
            self.description = [
                ("table_cat", column_types[0], None, 128, 128, 0, True),
                ("table_schem", column_types[1], None, 128, 128, 0, True),
                ("table_name", column_types[2], None, 128, 128, 0, False),
                ("table_type", column_types[3], None, 128, 128, 0, False),
                ("remarks", column_types[4], None, 254, 254, 0, True)
            ]
        
        # Define column names in ODBC standard order
        column_names = [
            "table_cat", "table_schem", "table_name", "table_type", "remarks"
        ]
        
        # Fetch all rows
        rows_data = []
        ddbc_bindings.DDBCSQLFetchAll(self.hstmt, rows_data)
        
        # Create a column map for attribute access
        column_map = {name: i for i, name in enumerate(column_names)}
        
        # Create Row objects with the column map
        result_rows = []
        for row_data in rows_data:
            row = Row(row_data, self.description, column_map)
            result_rows.append(row)
        
        return result_rows
