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
from mssql_python.constants import ConstantsDDBC as ddbc_sql_const, SQLTypes
from mssql_python.helpers import check_error, log
from mssql_python import ddbc_bindings
from mssql_python.exceptions import InterfaceError, NotSupportedError, ProgrammingError
from mssql_python.row import Row
from mssql_python import get_settings

# Constants for string handling
MAX_INLINE_CHAR = 4000  # NVARCHAR/VARCHAR inline limit; this triggers NVARCHAR(MAX)/VARCHAR(MAX) + DAE
SMALLMONEY_MIN = decimal.Decimal('-214748.3648')
SMALLMONEY_MAX = decimal.Decimal('214748.3647')
MONEY_MIN = decimal.Decimal('-922337203685477.5808')
MONEY_MAX = decimal.Decimal('922337203685477.5807')


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
        self._connection = connection  # Store as private attribute
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

    def _map_sql_type(self, param, parameters_list, i, min_val=None, max_val=None):
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
            # Use min_val/max_val if available
            value_to_check = max_val if max_val is not None else param
            min_to_check = min_val if min_val is not None else param

            if 0 <= min_to_check and value_to_check <= 255:
                return (
                    ddbc_sql_const.SQL_TINYINT.value,
                    ddbc_sql_const.SQL_C_TINYINT.value,
                    3,
                    0,
                    False,
                )
            if -32768 <= min_to_check and value_to_check <= 32767:
                return (
                    ddbc_sql_const.SQL_SMALLINT.value,
                    ddbc_sql_const.SQL_C_SHORT.value,
                    5,
                    0,
                    False,
                )
            if -2147483648 <= min_to_check and value_to_check <= 2147483647:
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
        # Detect MONEY / SMALLMONEY range
            if SMALLMONEY_MIN  <= param <= SMALLMONEY_MAX:
                # smallmoney
                parameters_list[i] = str(param)
                return (
                    ddbc_sql_const.SQL_VARCHAR.value,
                    ddbc_sql_const.SQL_C_CHAR.value,
                    len(parameters_list[i]),
                    0,
                    False,
                )
            elif MONEY_MIN <= param <= MONEY_MAX:
                # money
                parameters_list[i] = str(param)
                return (
                    ddbc_sql_const.SQL_VARCHAR.value,
                    ddbc_sql_const.SQL_C_CHAR.value,
                    len(parameters_list[i]),
                    0,
                    False,
                )
            else:
                # fallback to generic numeric binding
                parameters_list[i] = self._get_numeric_data(param)
                return (
                    ddbc_sql_const.SQL_NUMERIC.value,
                    ddbc_sql_const.SQL_C_NUMERIC.value,
                    parameters_list[i].precision,
                    parameters_list[i].scale,
                    False,
                )
            
        if isinstance(param, uuid.UUID):
            parameters_list[i] = param.bytes_le
            return (
                ddbc_sql_const.SQL_GUID.value,
                ddbc_sql_const.SQL_C_GUID.value,
                16,
                0,
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
                
            try:
                val = uuid.UUID(param)
                parameters_list[i] = val.bytes_le
                return (
                    ddbc_sql_const.SQL_GUID.value,
                    ddbc_sql_const.SQL_C_GUID.value,
                    16,
                    0,
                    False
                )
            except ValueError:
                pass

            # String mapping logic here
            is_unicode = self._is_unicode_string(param)

            # Computes UTF-16 code units (handles surrogate pairs)
            utf16_len = sum(2 if ord(c) > 0xFFFF else 1 for c in param)
            if utf16_len > MAX_INLINE_CHAR:  # Long strings -> DAE
                if is_unicode:
                    return (
                        ddbc_sql_const.SQL_WVARCHAR.value,
                        ddbc_sql_const.SQL_C_WCHAR.value,
                        0,
                        0,
                        True,
                    )
                return (
                    ddbc_sql_const.SQL_VARCHAR.value,
                    ddbc_sql_const.SQL_C_CHAR.value,
                    0,
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
        
        if isinstance(param, (bytes, bytearray)):
            length = len(param)
            if length > 8000:  # Use VARBINARY(MAX) for large blobs
                return (
                    ddbc_sql_const.SQL_VARBINARY.value,
                    ddbc_sql_const.SQL_C_BINARY.value,
                    0,
                    0,
                    True
                )
            else:  # Small blobs → direct binding
                return (
                    ddbc_sql_const.SQL_VARBINARY.value,
                    ddbc_sql_const.SQL_C_BINARY.value,
                    max(length, 1),
                    0,
                    False
                )

        if isinstance(param, datetime.datetime):
            if param.tzinfo is not None:
                # Timezone-aware datetime -> DATETIMEOFFSET
                return (
                    ddbc_sql_const.SQL_DATETIMEOFFSET.value,
                    ddbc_sql_const.SQL_C_SS_TIMESTAMPOFFSET.value,
                    34,
                    7,
                    False,
                )
            else:
                # Naive datetime -> TIMESTAMP
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
        
        # Remove this cursor from the connection's tracking
        if hasattr(self, 'connection') and self.connection and hasattr(self.connection, '_cursors'):
            try:
                self.connection._cursors.discard(self)
            except Exception as e:
                log('warning', "Error removing cursor from connection tracking: %s", e)

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
        
        # Get valid SQL types from centralized constants
        valid_sql_types = SQLTypes.get_valid_types()
        
        self._inputsizes = []
        
        if sizes:
            for size_info in sizes:
                if isinstance(size_info, tuple):
                    # Handle tuple format (sql_type, size, decimal_digits)
                    if len(size_info) == 1:
                        sql_type = size_info[0]
                        column_size = 0
                        decimal_digits = 0
                    elif len(size_info) == 2:
                        sql_type, column_size = size_info
                        decimal_digits = 0
                    elif len(size_info) >= 3:
                        sql_type, column_size, decimal_digits = size_info
                    
                    # Validate SQL type
                    if not isinstance(sql_type, int) or sql_type not in valid_sql_types:
                        raise ValueError(f"Invalid SQL type: {sql_type}. Must be a valid SQL type constant.")
                    
                    # Validate size and precision
                    if not isinstance(column_size, int) or column_size < 0:
                        raise ValueError(f"Invalid column size: {column_size}. Must be a non-negative integer.")
                    
                    if not isinstance(decimal_digits, int) or decimal_digits < 0:
                        raise ValueError(f"Invalid decimal digits: {decimal_digits}. Must be a non-negative integer.")
                    
                    self._inputsizes.append((sql_type, column_size, decimal_digits))
                else:
                    # Handle single value (just sql_type)
                    sql_type = size_info
                    
                    # Validate SQL type
                    if not isinstance(sql_type, int) or sql_type not in valid_sql_types:
                        raise ValueError(f"Invalid SQL type: {sql_type}. Must be a valid SQL type constant.")
                    
                    self._inputsizes.append((sql_type, 0, 0))
    
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

    def _create_parameter_types_list(self, parameter, param_info, parameters_list, i, min_val=None, max_val=None):
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
            
            # Default is_dae to False for explicit types, but set to True for large strings/binary
            is_dae = False
            
            if parameter is None:
                # For NULL parameters, always use SQL_C_DEFAULT regardless of SQL type
                c_type = ddbc_sql_const.SQL_C_DEFAULT.value
            else:
                # For non-NULL parameters, determine the appropriate C type based on SQL type
                c_type = self._get_c_type_for_sql_type(sql_type)
                
                # Check if this should be a DAE (data at execution) parameter
                # For string types with large column sizes
                if isinstance(parameter, str) and column_size > MAX_INLINE_CHAR:
                    is_dae = True
                # For binary types with large column sizes
                elif isinstance(parameter, (bytes, bytearray)) and column_size > 8000:
                    is_dae = True
    
            # Sanitize precision/scale for numeric types
            if sql_type in (ddbc_sql_const.SQL_DECIMAL.value, ddbc_sql_const.SQL_NUMERIC.value):
                column_size = max(1, min(int(column_size) if column_size > 0 else 18, 38))
                decimal_digits = min(max(0, decimal_digits), column_size)
        
        else:
            # Fall back to automatic type inference
            sql_type, c_type, column_size, decimal_digits, is_dae = self._map_sql_type(
                parameter, parameters_list, i, min_val=min_val, max_val=max_val
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

    def _initialize_description(self, column_metadata=None):
        """Initialize the description attribute from column metadata."""
        if not column_metadata:
            self.description = None
            return

        description = []
        for i, col in enumerate(column_metadata):
            # Get column name - lowercase it if the lowercase flag is set
            column_name = col["ColumnName"]
            
            # Use the current global setting to ensure tests pass correctly
            if get_settings().lowercase:
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

        # Clear any previous messages
        self.messages = []

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
        # After successful execution, initialize description if there are results
        column_metadata = []
        try:
            ddbc_bindings.DDBCSQLDescribeCol(self.hstmt, column_metadata)
            self._initialize_description(column_metadata)
        except Exception as e:
            # If describe fails, it's likely there are no results (e.g., for INSERT)
            self.description = None
        
        # Reset rownumber for new result set (only for SELECT statements)
        if self.description:  # If we have column descriptions, it's likely a SELECT
            self.rowcount = -1
            self._reset_rownumber()
        else:
            self.rowcount = ddbc_bindings.DDBCSQLRowCount(self.hstmt)
            self._clear_rownumber()

        # After successful execution, initialize description if there are results
        column_metadata = []
        try:
            ddbc_bindings.DDBCSQLDescribeCol(self.hstmt, column_metadata)
            self._initialize_description(column_metadata)
        except Exception as e:
            # If describe fails, it's likely there are no results (e.g., for INSERT)
            self.description = None
        
        self._reset_inputsizes()  # Reset input sizes after execution
        # Return self for method chaining
        return self

    def _prepare_metadata_result_set(self, column_metadata=None, fallback_description=None, specialized_mapping=None):
        """
        Prepares a metadata result set by:
        1. Retrieving column metadata if not provided
        2. Initializing the description attribute
        3. Setting up column name mappings
        4. Creating wrapper fetch methods with column mapping support
        
        Args:
            column_metadata (list, optional): Pre-fetched column metadata. 
                                             If None, it will be retrieved.
            fallback_description (list, optional): Fallback description to use if 
                                                  metadata retrieval fails.
            specialized_mapping (dict, optional): Custom column mapping for special cases.
        
        Returns:
            Cursor: Self, for method chaining
        """
        # Retrieve column metadata if not provided
        if column_metadata is None:
            column_metadata = []
            try:
                ddbc_bindings.DDBCSQLDescribeCol(self.hstmt, column_metadata)
            except InterfaceError as e:
                log('error', f"Driver interface error during metadata retrieval: {e}")
            except Exception as e:
                # Log the exception with appropriate context
                log('error', f"Failed to retrieve column metadata: {e}. Using standard ODBC column definitions instead.")
    
        # Initialize the description attribute with the column metadata
        self._initialize_description(column_metadata)
        
        # Use fallback description if provided and current description is empty
        if not self.description and fallback_description:
            self.description = fallback_description
        
        # Define column names in ODBC standard order
        self._column_map = {}
        for i, (name, *_) in enumerate(self.description):
            # Add standard name
            self._column_map[name] = i
            # Add lowercase alias
            self._column_map[name.lower()] = i
    
        # If specialized mapping is provided, handle it differently
        if specialized_mapping:
            # Define specialized fetch methods that use the custom mapping
            def fetchone_with_specialized_mapping():
                row = self._original_fetchone()
                if row is not None:
                    merged_map = getattr(row, '_column_map', {}).copy()
                    merged_map.update(specialized_mapping)
                    row._column_map = merged_map
                return row
                
            def fetchmany_with_specialized_mapping(size=None):
                rows = self._original_fetchmany(size)
                for row in rows:
                    merged_map = getattr(row, '_column_map', {}).copy()
                    merged_map.update(specialized_mapping)
                    row._column_map = merged_map
                return rows
                
            def fetchall_with_specialized_mapping():
                rows = self._original_fetchall()
                for row in rows:
                    merged_map = getattr(row, '_column_map', {}).copy()
                    merged_map.update(specialized_mapping)
                    row._column_map = merged_map
                return rows
            
            # Save original fetch methods
            if not hasattr(self, '_original_fetchone'):
                self._original_fetchone = self.fetchone
                self._original_fetchmany = self.fetchmany
                self._original_fetchall = self.fetchall
    
            # Use specialized mapping methods
            self.fetchone = fetchone_with_specialized_mapping
            self.fetchmany = fetchmany_with_specialized_mapping
            self.fetchall = fetchall_with_specialized_mapping
        else:
            # Standard column mapping
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
    
        # Return the cursor itself for method chaining
        return self

    def getTypeInfo(self, sqlType=None):
        """
        Executes SQLGetTypeInfo and creates a result set with information about 
        the specified data type or all data types supported by the ODBC driver if not specified.
        """
        self._check_closed()
        self._reset_cursor()
        
        sql_all_types = 0  # SQL_ALL_TYPES = 0
        
        try:
            # Get information about data types
            ret = ddbc_bindings.DDBCSQLGetTypeInfo(
                self.hstmt, 
                sqlType if sqlType is not None else sql_all_types
            )
            check_error(ddbc_sql_const.SQL_HANDLE_STMT.value, self.hstmt, ret)
            
            # Use the helper method to prepare the result set
            return self._prepare_metadata_result_set()
        except Exception as e:
            self._reset_cursor()
            raise e

    def procedures(self, procedure=None, catalog=None, schema=None):
        """
        Executes SQLProcedures and creates a result set of information about procedures in the data source.
        
        Args:
            procedure (str, optional): Procedure name pattern. Default is None (all procedures).
            catalog (str, optional): Catalog name pattern. Default is None (current catalog).
            schema (str, optional): Schema name pattern. Default is None (all schemas).
        """
        self._check_closed()
        self._reset_cursor()
        
        # Call the SQLProcedures function
        retcode = ddbc_bindings.DDBCSQLProcedures(self.hstmt, catalog, schema, procedure)
        check_error(ddbc_sql_const.SQL_HANDLE_STMT.value, self.hstmt, retcode)
        
        # Define fallback description for procedures
        fallback_description = [
            ("procedure_cat", str, None, 128, 128, 0, True),
            ("procedure_schem", str, None, 128, 128, 0, True),
            ("procedure_name", str, None, 128, 128, 0, False),
            ("num_input_params", int, None, 10, 10, 0, True),
            ("num_output_params", int, None, 10, 10, 0, True),
            ("num_result_sets", int, None, 10, 10, 0, True),
            ("remarks", str, None, 254, 254, 0, True),
            ("procedure_type", int, None, 10, 10, 0, False)
        ]
        
        # Use the helper method to prepare the result set
        return self._prepare_metadata_result_set(fallback_description=fallback_description)

    def primaryKeys(self, table, catalog=None, schema=None):
        """
        Creates a result set of column names that make up the primary key for a table
        by executing the SQLPrimaryKeys function.
        
        Args:
            table (str): The name of the table
            catalog (str, optional): The catalog name (database). Defaults to None.
            schema (str, optional): The schema name. Defaults to None.
        """
        self._check_closed()
        self._reset_cursor()
        
        if not table:
            raise ProgrammingError("Table name must be specified", "HY000")
        
        # Call the SQLPrimaryKeys function
        retcode = ddbc_bindings.DDBCSQLPrimaryKeys(self.hstmt, catalog, schema, table)
        check_error(ddbc_sql_const.SQL_HANDLE_STMT.value, self.hstmt, retcode)
        
        # Define fallback description for primary keys
        fallback_description = [
            ("table_cat", str, None, 128, 128, 0, True),
            ("table_schem", str, None, 128, 128, 0, True),
            ("table_name", str, None, 128, 128, 0, False),
            ("column_name", str, None, 128, 128, 0, False),
            ("key_seq", int, None, 10, 10, 0, False),
            ("pk_name", str, None, 128, 128, 0, True)
        ]
        
        # Use the helper method to prepare the result set
        return self._prepare_metadata_result_set(fallback_description=fallback_description)

    def foreignKeys(self, table=None, catalog=None, schema=None, foreignTable=None, foreignCatalog=None, foreignSchema=None):
        """
        Executes the SQLForeignKeys function and creates a result set of column names that are foreign keys.
        
        This function returns:
        1. Foreign keys in the specified table that reference primary keys in other tables, OR
        2. Foreign keys in other tables that reference the primary key in the specified table
        """
        self._check_closed()
        self._reset_cursor()
        
        # Check if we have at least one table specified
        if table is None and foreignTable is None:
            raise ProgrammingError("Either table or foreignTable must be specified", "HY000")
        
        # Call the SQLForeignKeys function
        retcode = ddbc_bindings.DDBCSQLForeignKeys(
            self.hstmt, 
            foreignCatalog, foreignSchema, foreignTable,
            catalog, schema, table
        )
        check_error(ddbc_sql_const.SQL_HANDLE_STMT.value, self.hstmt, retcode)
        
        # Define fallback description for foreign keys
        fallback_description = [
            ("pktable_cat", str, None, 128, 128, 0, True),
            ("pktable_schem", str, None, 128, 128, 0, True),
            ("pktable_name", str, None, 128, 128, 0, False),
            ("pkcolumn_name", str, None, 128, 128, 0, False),
            ("fktable_cat", str, None, 128, 128, 0, True),
            ("fktable_schem", str, None, 128, 128, 0, True),
            ("fktable_name", str, None, 128, 128, 0, False),
            ("fkcolumn_name", str, None, 128, 128, 0, False),
            ("key_seq", int, None, 10, 10, 0, False),
            ("update_rule", int, None, 10, 10, 0, False),
            ("delete_rule", int, None, 10, 10, 0, False),
            ("fk_name", str, None, 128, 128, 0, True),
            ("pk_name", str, None, 128, 128, 0, True),
            ("deferrability", int, None, 10, 10, 0, False)
        ]
        
        # Use the helper method to prepare the result set
        return self._prepare_metadata_result_set(fallback_description=fallback_description)

    def rowIdColumns(self, table, catalog=None, schema=None, nullable=True):
        """
        Executes SQLSpecialColumns with SQL_BEST_ROWID which creates a result set of 
        columns that uniquely identify a row.
        """
        self._check_closed()
        self._reset_cursor()
        
        if not table:
            raise ProgrammingError("Table name must be specified", "HY000")
        
        # Set the identifier type and options
        identifier_type = ddbc_sql_const.SQL_BEST_ROWID.value
        scope = ddbc_sql_const.SQL_SCOPE_CURROW.value
        nullable_flag = ddbc_sql_const.SQL_NULLABLE.value if nullable else ddbc_sql_const.SQL_NO_NULLS.value
        
        # Call the SQLSpecialColumns function
        retcode = ddbc_bindings.DDBCSQLSpecialColumns(
            self.hstmt, identifier_type, catalog, schema, table, scope, nullable_flag
        )
        check_error(ddbc_sql_const.SQL_HANDLE_STMT.value, self.hstmt, retcode)
        
        # Define fallback description for special columns
        fallback_description = [
            ("scope", int, None, 10, 10, 0, False),
            ("column_name", str, None, 128, 128, 0, False),
            ("data_type", int, None, 10, 10, 0, False),
            ("type_name", str, None, 128, 128, 0, False),
            ("column_size", int, None, 10, 10, 0, False),
            ("buffer_length", int, None, 10, 10, 0, False),
            ("decimal_digits", int, None, 10, 10, 0, True),
            ("pseudo_column", int, None, 10, 10, 0, False)
        ]
        
        # Use the helper method to prepare the result set
        return self._prepare_metadata_result_set(fallback_description=fallback_description)

    def rowVerColumns(self, table, catalog=None, schema=None, nullable=True):
        """
        Executes SQLSpecialColumns with SQL_ROWVER which creates a result set of
        columns that are automatically updated when any value in the row is updated.
        """
        self._check_closed()
        self._reset_cursor()
        
        if not table:
            raise ProgrammingError("Table name must be specified", "HY000")
        
        # Set the identifier type and options
        identifier_type = ddbc_sql_const.SQL_ROWVER.value
        scope = ddbc_sql_const.SQL_SCOPE_CURROW.value
        nullable_flag = ddbc_sql_const.SQL_NULLABLE.value if nullable else ddbc_sql_const.SQL_NO_NULLS.value
        
        # Call the SQLSpecialColumns function
        retcode = ddbc_bindings.DDBCSQLSpecialColumns(
            self.hstmt, identifier_type, catalog, schema, table, scope, nullable_flag
        )
        check_error(ddbc_sql_const.SQL_HANDLE_STMT.value, self.hstmt, retcode)
        
        # Same fallback description as rowIdColumns
        fallback_description = [
            ("scope", int, None, 10, 10, 0, False),
            ("column_name", str, None, 128, 128, 0, False),
            ("data_type", int, None, 10, 10, 0, False),
            ("type_name", str, None, 128, 128, 0, False),
            ("column_size", int, None, 10, 10, 0, False),
            ("buffer_length", int, None, 10, 10, 0, False),
            ("decimal_digits", int, None, 10, 10, 0, True),
            ("pseudo_column", int, None, 10, 10, 0, False)
        ]
        
        # Use the helper method to prepare the result set
        return self._prepare_metadata_result_set(fallback_description=fallback_description)

    def statistics(self, table: str, catalog: str = None, schema: str = None, unique: bool = False, quick: bool = True) -> 'Cursor':
        """
        Creates a result set of statistics about a single table and the indexes associated 
        with the table by executing SQLStatistics.
        """
        self._check_closed()
        self._reset_cursor()

        if not table:
            raise ProgrammingError("Table name is required", "HY000")
        
        # Set unique and quick flags
        unique_option = ddbc_sql_const.SQL_INDEX_UNIQUE.value if unique else ddbc_sql_const.SQL_INDEX_ALL.value
        reserved_option = ddbc_sql_const.SQL_QUICK.value if quick else ddbc_sql_const.SQL_ENSURE.value
        
        # Call the SQLStatistics function
        retcode = ddbc_bindings.DDBCSQLStatistics(
            self.hstmt, catalog, schema, table, unique_option, reserved_option
        )
        check_error(ddbc_sql_const.SQL_HANDLE_STMT.value, self.hstmt, retcode)
        
        # Define fallback description for statistics
        fallback_description = [
            ("table_cat", str, None, 128, 128, 0, True),
            ("table_schem", str, None, 128, 128, 0, True),
            ("table_name", str, None, 128, 128, 0, False),
            ("non_unique", bool, None, 1, 1, 0, False),
            ("index_qualifier", str, None, 128, 128, 0, True),
            ("index_name", str, None, 128, 128, 0, True),
            ("type", int, None, 10, 10, 0, False),
            ("ordinal_position", int, None, 10, 10, 0, False),
            ("column_name", str, None, 128, 128, 0, True),
            ("asc_or_desc", str, None, 1, 1, 0, True),
            ("cardinality", int, None, 20, 20, 0, True),
            ("pages", int, None, 20, 20, 0, True),
            ("filter_condition", str, None, 128, 128, 0, True)
        ]
        
        # Use the helper method to prepare the result set
        return self._prepare_metadata_result_set(fallback_description=fallback_description)

    def columns(self, table=None, catalog=None, schema=None, column=None):
        """
        Creates a result set of column information in the specified tables 
        using the SQLColumns function.
        """
        self._check_closed()
        self._reset_cursor()
        
        # Call the SQLColumns function
        retcode = ddbc_bindings.DDBCSQLColumns(
            self.hstmt, catalog, schema, table, column
        )
        check_error(ddbc_sql_const.SQL_HANDLE_STMT.value, self.hstmt, retcode)
        
        # Define fallback description for columns
        fallback_description = [
            ("table_cat", str, None, 128, 128, 0, True),
            ("table_schem", str, None, 128, 128, 0, True),
            ("table_name", str, None, 128, 128, 0, False),
            ("column_name", str, None, 128, 128, 0, False),
            ("data_type", int, None, 10, 10, 0, False),
            ("type_name", str, None, 128, 128, 0, False),
            ("column_size", int, None, 10, 10, 0, True),
            ("buffer_length", int, None, 10, 10, 0, True),
            ("decimal_digits", int, None, 10, 10, 0, True),
            ("num_prec_radix", int, None, 10, 10, 0, True),
            ("nullable", int, None, 10, 10, 0, False),
            ("remarks", str, None, 254, 254, 0, True),
            ("column_def", str, None, 254, 254, 0, True),
            ("sql_data_type", int, None, 10, 10, 0, False),
            ("sql_datetime_sub", int, None, 10, 10, 0, True),
            ("char_octet_length", int, None, 10, 10, 0, True),
            ("ordinal_position", int, None, 10, 10, 0, False),
            ("is_nullable", str, None, 254, 254, 0, True)
        ]
        
        # Use the helper method to prepare the result set
        return self._prepare_metadata_result_set(fallback_description=fallback_description)

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
    
    def _compute_column_type(self, column):
        """
        Determine representative value and integer min/max for a column.
        
        Returns:
            sample_value: Representative value for type inference and modified_row.
            min_val: Minimum for integers (None otherwise).
            max_val: Maximum for integers (None otherwise).
        """
        non_nulls = [v for v in column if v is not None]
        if not non_nulls:
            return None, None, None

        int_values = [v for v in non_nulls if isinstance(v, int)]
        if int_values:
            min_val, max_val = min(int_values), max(int_values)
            sample_value = max(int_values, key=abs)
            return sample_value, min_val, max_val

        sample_value = None
        for v in non_nulls:
            if not sample_value or (hasattr(v, '__len__') and len(v) > len(sample_value)):
                sample_value = v

        return sample_value, None, None
    
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
        self.messages = []

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
                log('debug', f"Set query timeout to {self._timeout} seconds")
            except Exception as e:
                log('warning', f"Failed to set query timeout: {e}")

        # Get sample row for parameter type detection and validation
        sample_row = seq_of_parameters[0] if hasattr(seq_of_parameters, '__getitem__') else next(iter(seq_of_parameters))
        param_count = len(sample_row)
        param_info = ddbc_bindings.ParamInfo
        parameters_type = []
        any_dae = False

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
            column = [row[col_index] for row in seq_of_parameters] if hasattr(seq_of_parameters, '__getitem__') else []
            sample_value, min_val, max_val = self._compute_column_type(column)
            
            if self._inputsizes and col_index < len(self._inputsizes):
                # Use explicitly set input sizes
                sql_type, column_size, decimal_digits = self._inputsizes[col_index]
                
                # Default is_dae to False
                is_dae = False
                
                # Determine appropriate C type based on SQL type
                c_type = self._get_c_type_for_sql_type(sql_type)
                
                # Check if this should be a DAE (data at execution) parameter based on column size
                if sample_value is not None:
                    if isinstance(sample_value, str) and column_size > MAX_INLINE_CHAR:
                        is_dae = True
                    elif isinstance(sample_value, (bytes, bytearray)) and column_size > 8000:
                        is_dae = True
                
                # Sanitize precision/scale for numeric types
                if sql_type in (ddbc_sql_const.SQL_DECIMAL.value, ddbc_sql_const.SQL_NUMERIC.value):
                    column_size = max(1, min(int(column_size) if column_size > 0 else 18, 38))
                    decimal_digits = min(max(0, decimal_digits), column_size)

                # For binary data columns with mixed content, we need to find max size
                if sql_type in (ddbc_sql_const.SQL_BINARY.value, ddbc_sql_const.SQL_VARBINARY.value,
                            ddbc_sql_const.SQL_LONGVARBINARY.value):
                    # Find the maximum size needed for any row's binary data
                    max_binary_size = 0
                    for row in seq_of_parameters:
                        value = row[col_index]
                        if value is not None and isinstance(value, (bytes, bytearray)):
                            max_binary_size = max(max_binary_size, len(value))
                    
                    # For SQL Server VARBINARY(MAX), we need to use large object binding
                    if column_size > 8000 or max_binary_size > 8000:
                        sql_type = ddbc_sql_const.SQL_LONGVARBINARY.value
                        is_dae = True
                    
                    # Update column_size to actual maximum size if it's larger
                    # Always ensure at least a minimum size of 1 for empty strings
                    column_size = max(max_binary_size, 1)
                
                paraminfo = param_info()
                paraminfo.paramCType = c_type
                paraminfo.paramSQLType = sql_type
                paraminfo.inputOutputType = ddbc_sql_const.SQL_PARAM_INPUT.value
                paraminfo.columnSize = column_size
                paraminfo.decimalDigits = decimal_digits
                paraminfo.isDAE = is_dae
                
                # Ensure we never have SQL_C_DEFAULT (0) for C-type
                if paraminfo.paramCType == 0:
                    paraminfo.paramCType = ddbc_sql_const.SQL_C_DEFAULT.value
                    
                parameters_type.append(paraminfo)
            else:
                # Use auto-detection for columns without explicit types
                column = [row[col_index] for row in seq_of_parameters] if hasattr(seq_of_parameters, '__getitem__') else []
                sample_value, min_val, max_val = self._compute_column_type(column)

                dummy_row = list(sample_row)
                paraminfo = self._create_parameter_types_list(
                    sample_value, param_info, dummy_row, col_index, min_val=min_val, max_val=max_val
                )
                # Special handling for binary data in auto-detected types
                if paraminfo.paramSQLType in (ddbc_sql_const.SQL_BINARY.value, ddbc_sql_const.SQL_VARBINARY.value,
                                        ddbc_sql_const.SQL_LONGVARBINARY.value):
                    # Find the maximum size needed for any row's binary data
                    max_binary_size = 0
                    for row in seq_of_parameters:
                        value = row[col_index]
                        if value is not None and isinstance(value, (bytes, bytearray)):
                            max_binary_size = max(max_binary_size, len(value))
                    
                    # For SQL Server VARBINARY(MAX), we need to use large object binding
                    if max_binary_size > 8000:
                        paraminfo.paramSQLType = ddbc_sql_const.SQL_LONGVARBINARY.value
                        paraminfo.isDAE = True
                    
                    # Update column_size to actual maximum size
                    # Always ensure at least a minimum size of 1 for empty strings
                    paraminfo.columnSize = max(max_binary_size, 1)
                
                parameters_type.append(paraminfo)
                if paraminfo.isDAE:
                    any_dae = True
        
        if any_dae:
            log('debug', "DAE parameters detected. Falling back to row-by-row execution with streaming.")
            for row in seq_of_parameters:
                self.execute(operation, row)
            return
        
        # Process parameters into column-wise format with possible type conversions
        # First, convert any Decimal types as needed for NUMERIC/DECIMAL columns
        processed_parameters = []
        for row in seq_of_parameters:
            processed_row = list(row)
            for i, val in enumerate(processed_row):
                if val is None:
                    continue
                # Convert Decimals for money/smallmoney to string
                if isinstance(val, decimal.Decimal) and parameters_type[i].paramSQLType == ddbc_sql_const.SQL_VARCHAR.value:
                    processed_row[i] = str(val)
                # Existing numeric conversion
                elif (parameters_type[i].paramSQLType in 
                    (ddbc_sql_const.SQL_DECIMAL.value, ddbc_sql_const.SQL_NUMERIC.value) and
                    not isinstance(val, decimal.Decimal)):
                    try:
                        processed_row[i] = decimal.Decimal(str(val))
                    except Exception as e:
                        raise ValueError(
                            f"Failed to convert parameter at row {row}, column {i} to Decimal: {e}"
                        )
            processed_parameters.append(processed_row)

        
        # Now transpose the processed parameters
        columnwise_params, row_count = self._transpose_rowwise_to_columnwise(processed_parameters)
        
        # Add debug logging
        log('debug', "Executing batch query with %d parameter sets:\n%s",
            len(seq_of_parameters), "\n".join(f"  {i+1}: {tuple(p) if isinstance(p, (list, tuple)) else p}" for i, p in enumerate(seq_of_parameters[:5]))  # Limit to first 5 rows for large batches
        )

        ret = ddbc_bindings.SQLExecuteMany(
            self.hstmt,
            operation,
            columnwise_params,
            parameters_type,
            row_count
        )
        
        # Capture any diagnostic messages after execution
        if self.hstmt:
            self.messages.extend(ddbc_bindings.DDBCSQLGetAllDiagRecords(self.hstmt))
        
        try:
            check_error(ddbc_sql_const.SQL_HANDLE_STMT.value, self.hstmt, ret)
            self.rowcount = ddbc_bindings.DDBCSQLRowCount(self.hstmt)
            self.last_executed_stmt = operation
            self._initialize_description()
            
            if self.description:
                self.rowcount = -1
                self._reset_rownumber()
            else:
                self.rowcount = ddbc_bindings.DDBCSQLRowCount(self.hstmt)
                self._clear_rownumber()
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
        try:
            ret = ddbc_bindings.DDBCSQLFetchOne(self.hstmt, row_data)
            
            if self.hstmt:
                self.messages.extend(ddbc_bindings.DDBCSQLGetAllDiagRecords(self.hstmt))
            
            if ret == ddbc_sql_const.SQL_NO_DATA.value:
                # No more data available
                if self._next_row_index == 0 and self.description is not None:
                    # This is an empty result set, set rowcount to 0
                    self.rowcount = 0
                return None
            
            # Update internal position after successful fetch
            if self._skip_increment_for_next_fetch:
                self._skip_increment_for_next_fetch = False
                self._next_row_index += 1
            else:
                self._increment_rownumber()

            self.rowcount = self._next_row_index
            
            # Create and return a Row object, passing column name map if available
            column_map = getattr(self, '_column_name_map', None)
            return Row(self, self.description, row_data, column_map)
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

            # Centralize rowcount assignment after fetch
            if len(rows_data) == 0 and self._next_row_index == 0:
                self.rowcount = 0
            else:
                self.rowcount = self._next_row_index
            
            # Convert raw data to Row objects
            column_map = getattr(self, '_column_name_map', None)
            return [Row(self, self.description, row_data, column_map) for row_data in rows_data]
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

            # Centralize rowcount assignment after fetch
            if len(rows_data) == 0 and self._next_row_index == 0:
                self.rowcount = 0
            else:
                self.rowcount = self._next_row_index
            
            # Convert raw data to Row objects
            column_map = getattr(self, '_column_name_map', None)
            return [Row(self, self.description, row_data, column_map) for row_data in rows_data]
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
            >>> max_id = cursor.execute('SELECT MAX(id) FROM users').fetchval()
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
            Cursor: The cursor object itself for method chaining with fetch methods.
        """
        self._check_closed()
        self._reset_cursor()
        
        # Format table_type parameter - SQLTables expects comma-separated string
        table_type_str = None
        if tableType is not None:
            if isinstance(tableType, (list, tuple)):
                table_type_str = ",".join(tableType)
            else:
                table_type_str = str(tableType)
        
        try:
            # Call SQLTables via the helper method
            self._execute_tables(
                self.hstmt,
                catalog_name=catalog,
                schema_name=schema,
                table_name=table,
                table_type=table_type_str
            )
            
            # Define fallback description for tables
            fallback_description = [
                ("table_cat", str, None, 128, 128, 0, True),
                ("table_schem", str, None, 128, 128, 0, True),
                ("table_name", str, None, 128, 128, 0, False),
                ("table_type", str, None, 128, 128, 0, False),
                ("remarks", str, None, 254, 254, 0, True)
            ]
            
            # Use the helper method to prepare the result set
            return self._prepare_metadata_result_set(fallback_description=fallback_description)
        
        except Exception as e:
            # Log the error and re-raise
            log('error', f"Error executing tables query: {e}")
            raise