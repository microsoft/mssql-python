"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
This module contains custom exception classes for the mssql_python package.
These classes are used to raise exceptions when an error occurs while executing a query.
"""
from mssql_python.logging_config import get_logger, ENABLE_LOGGING

logger = get_logger()


class Exception(Exception):
    """
    Base class for all DB API 2.0 exceptions.
    """

    def __init__(self, driver_error, ddbc_error) -> None:
        self.driver_error = driver_error
        self.ddbc_error = truncate_error_message(ddbc_error)
        self.message = (
            f"Driver Error: {self.driver_error}; DDBC Error: {self.ddbc_error}"
        )
        super().__init__(self.message)


class Warning(Exception):
    """
    Exception raised for important warnings like data truncations while inserting, etc.
    """

    def __init__(self, driver_error, ddbc_error) -> None:
        super().__init__(driver_error, ddbc_error)


class Error(Exception):
    """
    Base class for errors.
    """

    def __init__(self, driver_error, ddbc_error) -> None:
        super().__init__(driver_error, ddbc_error)


class InterfaceError(Error):
    """
    Exception raised for errors that are related to the database
    interface rather than the database itself.
    """

    def __init__(self, driver_error, ddbc_error) -> None:
        super().__init__(driver_error, ddbc_error)


class DatabaseError(Error):
    """
    Exception raised for errors that are related to the database.
    """

    def __init__(self, driver_error, ddbc_error) -> None:
        super().__init__(driver_error, ddbc_error)


class DataError(DatabaseError):
    """
    Exception raised for errors that are due to problems with the
    processed data like division by zero, numeric value out of range, etc.
    """

    def __init__(self, driver_error, ddbc_error) -> None:
        super().__init__(driver_error, ddbc_error)


class OperationalError(DatabaseError):
    """
    Exception raised for errors that are related to the database's operation
    and not necessarily under the control of the programmer.
    """

    def __init__(self, driver_error, ddbc_error) -> None:
        super().__init__(driver_error, ddbc_error)


class IntegrityError(DatabaseError):
    """
    Exception raised when the relational integrity of the database is affected,
    e.g., a foreign key check fails.
    """

    def __init__(self, driver_error, ddbc_error) -> None:
        super().__init__(driver_error, ddbc_error)


class InternalError(DatabaseError):
    """
    Exception raised when the database encounters an internal error,
    e.g., the cursor is not valid anymore, the transaction is out of sync, etc.
    """

    def __init__(self, driver_error, ddbc_error) -> None:
        super().__init__(driver_error, ddbc_error)


class ProgrammingError(DatabaseError):
    """
    Exception raised for programming errors,
    e.g., table not found or already exists, syntax error in the SQL statement,
    wrong number of parameters specified, etc.
    """

    def __init__(self, driver_error, ddbc_error) -> None:
        super().__init__(driver_error, ddbc_error)


class NotSupportedError(DatabaseError):
    """
    Exception raised in case a method or database API was used which
    is not supported by the database, e.g., requesting a .rollback()
    on a connection that does not support transaction or has transactions turned off.
    """

    def __init__(self, driver_error, ddbc_error) -> None:
        super().__init__(driver_error, ddbc_error)


# Mapping SQLSTATE codes to custom exception classes
def sqlstate_to_exception(sqlstate: str, ddbc_error: str) -> Exception:
    """
    Map an SQLSTATE code to a custom exception class.
    This function maps an SQLSTATE code to a custom exception class based on the code.
    If the code is not found in the mapping, a generic DatabaseError class is returned
    to handle the error.
    Args:
        sqlstate (str): The SQLSTATE code to map to a custom exception class.
        Returns:
        mapping[str, Exception]: A mapping of SQLSTATE codes to custom exception classes.
    """
    mapping = {
        "01000": Warning(
            driver_error="General warning",
            ddbc_error=ddbc_error
        ),  # General warning
        "01001": OperationalError(
            driver_error="Cursor operation conflict",
            ddbc_error=ddbc_error
        ),  # Cursor operation conflict
        "01002": OperationalError(
            driver_error="Disconnect error",
            ddbc_error=ddbc_error
        ),  # Disconnect error
        "01003": DataError(
            driver_error="NULL value eliminated in set function",
            ddbc_error=ddbc_error
        ),  # NULL value eliminated in set function
        "01004": DataError(
            driver_error="String data, right-truncated",
            ddbc_error=ddbc_error
        ),  # String data, right-truncated
        "01006": OperationalError(
            driver_error="Privilege not revoked",
            ddbc_error=ddbc_error
        ),  # Privilege not revoked
        "01007": OperationalError(
            driver_error="Privilege not granted",
            ddbc_error=ddbc_error
        ),  # Privilege not granted
        "01S00": ProgrammingError(
            driver_error="Invalid connection string attribute",
            ddbc_error=ddbc_error
        ),  # Invalid connection string attribute
        "01S01": DataError(
            driver_error="Error in row",
            ddbc_error=ddbc_error
        ),  # Error in row
        "01S02": Warning(
            driver_error="Option value changed",
            ddbc_error=ddbc_error
        ),  # Option value changed
        "01S06": OperationalError(
            driver_error="Attempt to fetch before the result set returned the first rowset",
            ddbc_error=ddbc_error,
        ),  # Attempt to fetch before the result set returned the first rowset
        "01S07": DataError(
            driver_error="Fractional truncation",
            ddbc_error=ddbc_error
        ),  # Fractional truncation
        "01S08": OperationalError(
            driver_error="Error saving File DSN",
            ddbc_error=ddbc_error
        ),  # Error saving File DSN
        "01S09": ProgrammingError(
            driver_error="Invalid keyword",
            ddbc_error=ddbc_error
        ),  # Invalid keyword
        "07001": ProgrammingError(
            driver_error="Wrong number of parameters",
            ddbc_error=ddbc_error
        ),  # Wrong number of parameters
        "07002": ProgrammingError(
            driver_error="COUNT field incorrect",
            ddbc_error=ddbc_error
        ),  # COUNT field incorrect
        "07005": ProgrammingError(
            driver_error="Prepared statement not a cursor-specification",
            ddbc_error=ddbc_error,
        ),  # Prepared statement not a cursor-specification
        "07006": ProgrammingError(
            driver_error="Restricted data type attribute violation",
            ddbc_error=ddbc_error,
        ),  # Restricted data type attribute violation
        "07009": ProgrammingError(
            driver_error="Invalid descriptor index",
            ddbc_error=ddbc_error
        ),  # Invalid descriptor index
        "07S01": ProgrammingError(
            driver_error="Invalid use of default parameter",
            ddbc_error=ddbc_error
        ),  # Invalid use of default parameter
        "08001": OperationalError(
            driver_error="Client unable to establish connection",
            ddbc_error=ddbc_error
        ),  # Client unable to establish connection
        "08002": OperationalError(
            driver_error="Connection name in use",
            ddbc_error=ddbc_error
        ),  # Connection name in use
        "08003": OperationalError(
            driver_error="Connection not open",
            ddbc_error=ddbc_error
        ),  # Connection not open
        "08004": OperationalError(
            driver_error="Server rejected the connection",
            ddbc_error=ddbc_error
        ),  # Server rejected the connection
        "08007": OperationalError(
            driver_error="Connection failure during transaction",
            ddbc_error=ddbc_error
        ),  # Connection failure during transaction
        "08S01": OperationalError(
            driver_error="Communication link failure",
            ddbc_error=ddbc_error
        ),  # Communication link failure
        "21S01": ProgrammingError(
            driver_error="Insert value list does not match column list",
            ddbc_error=ddbc_error,
        ),  # Insert value list does not match column list
        "21S02": ProgrammingError(
            driver_error="Degree of derived table does not match column list",
            ddbc_error=ddbc_error,
        ),  # Degree of derived table does not match column list
        "22001": DataError(
            driver_error="String data, right-truncated",
            ddbc_error=ddbc_error
        ),  # String data, right-truncated
        "22002": DataError(
            driver_error="Indicator variable required but not supplied",
            ddbc_error=ddbc_error,
        ),  # Indicator variable required but not supplied
        "22003": DataError(
            driver_error="Numeric value out of range",
            ddbc_error=ddbc_error
        ),  # Numeric value out of range
        "22007": DataError(
            driver_error="Invalid datetime format",
            ddbc_error=ddbc_error
        ),  # Invalid datetime format
        "22008": DataError(
            driver_error="Datetime field overflow",
            ddbc_error=ddbc_error
        ),  # Datetime field overflow
        "22012": DataError(
            driver_error="Division by zero",
            ddbc_error=ddbc_error
        ),  # Division by zero
        "22015": DataError(
            driver_error="Interval field overflow",
            ddbc_error=ddbc_error
        ),  # Interval field overflow
        "22018": DataError(
            driver_error="Invalid character value for cast specification",
            ddbc_error=ddbc_error,
        ),  # Invalid character value for cast specification
        "22019": ProgrammingError(
            driver_error="Invalid escape character",
            ddbc_error=ddbc_error
        ),  # Invalid escape character
        "22025": ProgrammingError(
            driver_error="Invalid escape sequence",
            ddbc_error=ddbc_error
        ),  # Invalid escape sequence
        "22026": DataError(
            driver_error="String data, length mismatch",
            ddbc_error=ddbc_error
        ),  # String data, length mismatch
        "23000": IntegrityError(
            driver_error="Integrity constraint violation",
            ddbc_error=ddbc_error
        ),  # Integrity constraint violation
        "24000": ProgrammingError(
            driver_error="Invalid cursor state",
            ddbc_error=ddbc_error
        ),  # Invalid cursor state
        "25000": OperationalError(
            driver_error="Invalid transaction state",
            ddbc_error=ddbc_error
        ),  # Invalid transaction state
        "25S01": OperationalError(
            driver_error="Transaction state",
            ddbc_error=ddbc_error
        ),  # Transaction state
        "25S02": OperationalError(
            driver_error="Transaction is still active",
            ddbc_error=ddbc_error
        ),  # Transaction is still active
        "25S03": OperationalError(
            driver_error="Transaction is rolled back",
            ddbc_error=ddbc_error
        ),  # Transaction is rolled back
        "28000": OperationalError(
            driver_error="Invalid authorization specification",
            ddbc_error=ddbc_error
        ),  # Invalid authorization specification
        "34000": ProgrammingError(
            driver_error="Invalid cursor name",
            ddbc_error=ddbc_error
        ),  # Invalid cursor name
        "3C000": ProgrammingError(
            driver_error="Duplicate cursor name",
            ddbc_error=ddbc_error
        ),  # Duplicate cursor name
        "3D000": ProgrammingError(
            driver_error="Invalid catalog name",
            ddbc_error=ddbc_error
        ),  # Invalid catalog name
        "3F000": ProgrammingError(
            driver_error="Invalid schema name",
            ddbc_error=ddbc_error
        ),  # Invalid schema name
        "40001": OperationalError(
            driver_error="Serialization failure",
            ddbc_error=ddbc_error
        ),  # Serialization failure
        "40002": IntegrityError(
            driver_error="Integrity constraint violation",
            ddbc_error=ddbc_error
        ),  # Integrity constraint violation
        "40003": OperationalError(
            driver_error="Statement completion unknown",
            ddbc_error=ddbc_error
        ),  # Statement completion unknown
        "42000": ProgrammingError(
            driver_error="Syntax error or access violation",
            ddbc_error=ddbc_error
        ),  # Syntax error or access violation
        "42S01": ProgrammingError(
            driver_error="Base table or view already exists",
            ddbc_error=ddbc_error
        ),  # Base table or view already exists
        "42S02": ProgrammingError(
            driver_error="Base table or view not found",
            ddbc_error=ddbc_error
        ),  # Base table or view not found
        "42S11": ProgrammingError(
            driver_error="Index already exists",
            ddbc_error=ddbc_error
        ),  # Index already exists
        "42S12": ProgrammingError(
            driver_error="Index not found",
            ddbc_error=ddbc_error
        ),  # Index not found
        "42S21": ProgrammingError(
            driver_error="Column already exists",
            ddbc_error=ddbc_error
        ),  # Column already exists
        "42S22": ProgrammingError(
            driver_error="Column not found",
            ddbc_error=ddbc_error
        ),  # Column not found
        "44000": IntegrityError(
            driver_error="WITH CHECK OPTION violation",
            ddbc_error=ddbc_error
        ),  # WITH CHECK OPTION violation
        "HY000": OperationalError(
            driver_error="General error",
            ddbc_error=ddbc_error
        ),  # General error
        "HY001": OperationalError(
            driver_error="Memory allocation error",
            ddbc_error=ddbc_error
        ),  # Memory allocation error
        "HY003": ProgrammingError(
            driver_error="Invalid application buffer type",
            ddbc_error=ddbc_error
        ),  # Invalid application buffer type
        "HY004": ProgrammingError(
            driver_error="Invalid SQL data type",
            ddbc_error=ddbc_error
        ),  # Invalid SQL data type
        "HY007": ProgrammingError(
            driver_error="Associated statement is not prepared",
            ddbc_error=ddbc_error
        ),  # Associated statement is not prepared
        "HY008": OperationalError(
            driver_error="Operation canceled",
            ddbc_error=ddbc_error
        ),  # Operation canceled
        "HY009": ProgrammingError(
            driver_error="Invalid use of null pointer",
            ddbc_error=ddbc_error
        ),  # Invalid use of null pointer
        "HY010": ProgrammingError(
            driver_error="Function sequence error",
            ddbc_error=ddbc_error
        ),  # Function sequence error
        "HY011": ProgrammingError(
            driver_error="Attribute cannot be set now",
            ddbc_error=ddbc_error
        ),  # Attribute cannot be set now
        "HY012": ProgrammingError(
            driver_error="Invalid transaction operation code",
            ddbc_error=ddbc_error
        ),  # Invalid transaction operation code
        "HY013": OperationalError(
            driver_error="Memory management error",
            ddbc_error=ddbc_error
        ),  # Memory management error
        "HY014": OperationalError(
            driver_error="Limit on the number of handles exceeded",
            ddbc_error=ddbc_error,
        ),  # Limit on the number of handles exceeded
        "HY015": ProgrammingError(
            driver_error="No cursor name available",
            ddbc_error=ddbc_error
        ),  # No cursor name available
        "HY016": ProgrammingError(
            driver_error="Cannot modify an implementation row descriptor",
            ddbc_error=ddbc_error,
        ),  # Cannot modify an implementation row descriptor
        "HY017": ProgrammingError(
            driver_error="Invalid use of an automatically allocated descriptor handle",
            ddbc_error=ddbc_error,
        ),  # Invalid use of an automatically allocated descriptor handle
        "HY018": OperationalError(
            driver_error="Server declined cancel request",
            ddbc_error=ddbc_error
        ),  # Server declined cancel request
        "HY019": DataError(
            driver_error="Non-character and non-binary data sent in pieces",
            ddbc_error=ddbc_error,
        ),  # Non-character and non-binary data sent in pieces
        "HY020": DataError(
            driver_error="Attempt to concatenate a null value",
            ddbc_error=ddbc_error
        ),  # Attempt to concatenate a null value
        "HY021": ProgrammingError(
            driver_error="Inconsistent descriptor information",
            ddbc_error=ddbc_error
        ),  # Inconsistent descriptor information
        "HY024": ProgrammingError(
            driver_error="Invalid attribute value",
            ddbc_error=ddbc_error
        ),  # Invalid attribute value
        "HY090": ProgrammingError(
            driver_error="Invalid string or buffer length",
            ddbc_error=ddbc_error
        ),  # Invalid string or buffer length
        "HY091": ProgrammingError(
            driver_error="Invalid descriptor field identifier",
            ddbc_error=ddbc_error
        ),  # Invalid descriptor field identifier
        "HY092": ProgrammingError(
            driver_error="Invalid attribute/option identifier",
            ddbc_error=ddbc_error
        ),  # Invalid attribute/option identifier
        "HY095": ProgrammingError(
            driver_error="Function type out of range",
            ddbc_error=ddbc_error
        ),  # Function type out of range
        "HY096": ProgrammingError(
            driver_error="Invalid information type",
            ddbc_error=ddbc_error
        ),  # Invalid information type
        "HY097": ProgrammingError(
            driver_error="Column type out of range",
            ddbc_error=ddbc_error
        ),  # Column type out of range
        "HY098": ProgrammingError(
            driver_error="Scope type out of range",
            ddbc_error=ddbc_error
        ),  # Scope type out of range
        "HY099": ProgrammingError(
            driver_error="Nullable type out of range",
            ddbc_error=ddbc_error
        ),  # Nullable type out of range
        "HY100": ProgrammingError(
            driver_error="Uniqueness option type out of range",
            ddbc_error=ddbc_error
        ),  # Uniqueness option type out of range
        "HY101": ProgrammingError(
            driver_error="Accuracy option type out of range",
            ddbc_error=ddbc_error
        ),  # Accuracy option type out of range
        "HY103": ProgrammingError(
            driver_error="Invalid retrieval code",
            ddbc_error=ddbc_error
        ),  # Invalid retrieval code
        "HY104": ProgrammingError(
            driver_error="Invalid precision or scale value",
            ddbc_error=ddbc_error
        ),  # Invalid precision or scale value
        "HY105": ProgrammingError(
            driver_error="Invalid parameter type",
            ddbc_error=ddbc_error
        ),  # Invalid parameter type
        "HY106": ProgrammingError(
            driver_error="Fetch type out of range",
            ddbc_error=ddbc_error
        ),  # Fetch type out of range
        "HY107": ProgrammingError(
            driver_error="Row value out of range",
            ddbc_error=ddbc_error
        ),  # Row value out of range
        "HY109": ProgrammingError(
            driver_error="Invalid cursor position",
            ddbc_error=ddbc_error
        ),  # Invalid cursor position
        "HY110": ProgrammingError(
            driver_error="Invalid driver completion",
            ddbc_error=ddbc_error
        ),  # Invalid driver completion
        "HY111": ProgrammingError(
            driver_error="Invalid bookmark value",
            ddbc_error=ddbc_error
        ),  # Invalid bookmark value
        "HYC00": NotSupportedError(
            driver_error="Optional feature not implemented",
            ddbc_error=ddbc_error
        ),  # Optional feature not implemented
        "HYT00": OperationalError(
            driver_error="Timeout expired",
            ddbc_error=ddbc_error
        ),  # Timeout expired
        "HYT01": OperationalError(
            driver_error="Connection timeout expired",
            ddbc_error=ddbc_error
        ),  # Connection timeout expired
        "IM001": NotSupportedError(
            driver_error="Driver does not support this function",
            ddbc_error=ddbc_error
        ),  # Driver does not support this function
        "IM002": OperationalError(
            driver_error="Data source name not found and no default driver specified",
            ddbc_error=ddbc_error,
        ),  # Data source name not found and no default driver specified
        "IM003": OperationalError(
            driver_error="Specified driver could not be loaded",
            ddbc_error=ddbc_error
        ),  # Specified driver could not be loaded
        "IM004": OperationalError(
            driver_error="Driver's SQLAllocHandle on SQL_HANDLE_ENV failed",
            ddbc_error=ddbc_error,
        ),  # Driver's SQLAllocHandle on SQL_HANDLE_ENV failed
        "IM005": OperationalError(
            driver_error="Driver's SQLAllocHandle on SQL_HANDLE_DBC failed",
            ddbc_error=ddbc_error,
        ),  # Driver's SQLAllocHandle on SQL_HANDLE_DBC failed
        "IM006": OperationalError(
            driver_error="Driver's SQLSetConnectAttr failed",
            ddbc_error=ddbc_error
        ),  # Driver's SQLSetConnectAttr failed
        "IM007": OperationalError(
            driver_error="No data source or driver specified; dialog prohibited",
            ddbc_error=ddbc_error,
        ),  # No data source or driver specified; dialog prohibited
        "IM008": OperationalError(
            driver_error="Dialog failed",
            ddbc_error=ddbc_error
        ),  # Dialog failed
        "IM009": OperationalError(
            driver_error="Unable to load translation DLL",
            ddbc_error=ddbc_error
        ),  # Unable to load translation DLL
        "IM010": OperationalError(
            driver_error="Data source name too long",
            ddbc_error=ddbc_error
        ),  # Data source name too long
        "IM011": OperationalError(
            driver_error="Driver name too long",
            ddbc_error=ddbc_error
        ),  # Driver name too long
        "IM012": OperationalError(
            driver_error="DRIVER keyword syntax error",
            ddbc_error=ddbc_error
        ),  # DRIVER keyword syntax error
        "IM013": OperationalError(
            driver_error="Trace file error",
            ddbc_error=ddbc_error
        ),  # Trace file error
        "IM014": OperationalError(
            driver_error="Invalid name of File DSN",
            ddbc_error=ddbc_error
        ),  # Invalid name of File DSN
        "IM015": OperationalError(
            driver_error="Corrupt file data source",
            ddbc_error=ddbc_error
        ),  # Corrupt file data source
    }
    return mapping.get(sqlstate, None)


def truncate_error_message(error_message: str) -> str:
    """
    - The Driver Error message is the message that is returned by the Internal driver.
    - This section will always be at the start of the message.
    """
    try:
        if not error_message.startswith("[Microsoft]"):
            # The message is not from the driver, so no need to truncate
            return error_message
        string_first = error_message[: error_message.index("]") + 1]
        string_second = error_message[error_message.index("]") + 1 :]
        string_third = string_second[string_second.index("]") + 1 :]
        return string_first + string_third
    except Exception as e:
        if ENABLE_LOGGING:
            logger.error("Error while truncating error message: %s",e)
        return error_message


def raise_exception(sqlstate: str, ddbc_error: str) -> None:
    """
    Raise a custom exception based on the given SQLSTATE code.
    This function raises a custom exception based on the provided SQLSTATE code.
    If the code is not found in the mapping, a generic DatabaseError is raised.

    Args:
        sqlstate (str): The SQLSTATE code to map to a custom exception.
        ddbc_error (str): The DDBC error message.

    Raises:
        DatabaseError: If the SQLSTATE code is not found in the mapping.
    """
    exception_class = sqlstate_to_exception(sqlstate, ddbc_error)
    if exception_class:
        if ENABLE_LOGGING:
            logger.error(exception_class)
        raise exception_class
    raise DatabaseError(
        driver_error=f"An error occurred with SQLSTATE code: {sqlstate}",
        ddbc_error=f"{ddbc_error}" if ddbc_error else f"Unknown DDBC error",
    )
