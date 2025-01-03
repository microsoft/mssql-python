class Exception(Exception):
    """
    Base class for all exceptions.
    This is the base class for all custom exceptions in this module. 
    It can be used to catch any exception raised by the database operations.
    """
    def __init__(self, message="An exception occurred", status_code=500) -> None:
        self.message = message
        self.status_code = status_code
        super().__init__(self.message)

class Warning(Exception):
    """
    Base class for warnings.
    This class is used to represent warnings that do not necessarily stop the execution 
    but indicate that something unexpected happened.
    """
    def __init__(self, message="A warning occurred", status_code=400) -> None:
        self.message = message
        self.status_code = status_code
        super().__init__(self.message)

class Error(Exception):
    """
    Base class for errors.
    This is the base class for all error-related exceptions. It is a subclass of the 
    general Exception class and serves as a parent for more specific error types.
    """
    def __init__(self, message="An error occurred", status_code=500) -> None:
        self.message = message
        self.status_code = status_code
        super().__init__(self.message)

class InterfaceError(Error):
    """
    Error related to the database interface.
    This exception is raised for errors that are related to the database interface, 
    such as connection issues or invalid queries.
    """
    def __init__(self, message="An interface error occurred", status_code=500) -> None:
        self.message = message
        self.status_code = status_code
        super().__init__(self.message)

class DatabaseError(Error):
    """
    Base class for database errors.
    This is the base class for all database-related errors. It serves as a parent 
    for more specific database error types.
    """
    def __init__(self, message="A database error occurred", status_code=500) -> None:
        self.message = message
        self.status_code = status_code
        super().__init__(self.message)

class DataError(DatabaseError):
    """
    Error related to problems with the processed data.
    This exception is raised for errors that occur due to issues with the data being 
    processed, such as data type mismatches or data integrity problems.
    """
    def __init__(self, message="A data error occurred", status_code=422) -> None:
        self.message = message
        self.status_code = status_code
        super().__init__(self.message)

class OperationalError(DatabaseError):
    """
    Error related to the database's operation.
    This exception is raised for errors that occur during the operation of the database, 
    such as connection failures, transaction errors, or other operational issues.
    """
    def __init__(self, message="An operational error occurred", status_code=503) -> None:
        self.message = message
        self.status_code = status_code
        super().__init__(self.message)

class IntegrityError(DatabaseError):
    """
    Error related to database integrity.
    This exception is raised for errors that occur due to integrity constraints being 
    violated, such as unique key violations or foreign key constraints.
    """
    def __init__(self, message="An integrity error occurred", status_code=409) -> None:
        self.message = message
        self.status_code = status_code
        super().__init__(self.message)

class InternalError(DatabaseError):
    """
    Error related to the database's internal operation.
    This exception is raised for errors that occur within the database's internal 
    operations, such as internal consistency checks or unexpected conditions.
    """
    def __init__(self, message="An internal error occurred", status_code=500) -> None:
        self.message = message
        self.status_code = status_code
        super().__init__(self.message)

class ProgrammingError(DatabaseError):
    """
    Error related to programming errors.
    This exception is raised for errors that occur due to mistakes in the database 
    programming, such as syntax errors in SQL queries or incorrect API usage.
    """
    def __init__(self, message="A programming error occurred", status_code=400) -> None:
        self.message = message
        self.status_code = status_code
        super().__init__(self.message)

class NotSupportedError(DatabaseError):
    """
    Error related to unsupported operations.
    This exception is raised for errors that occur when an unsupported operation is 
    attempted, such as using a feature that is not available in the database.
    """
    def __init__(self, message="A not supported error occurred", status_code=501) -> None:
        self.message = message
        self.status_code = status_code
        super().__init__(self.message)


# Mapping SQLSTATE codes to custom exception classes
sqlstate_to_exception = {
    '01000': Warning("General warning", 400),  # General warning
    '01001': OperationalError("Cursor operation conflict", 500),  # Cursor operation conflict
    '01002': OperationalError("Disconnect error", 500),  # Disconnect error
    '01003': DataError("NULL value eliminated in set function", 422),  # NULL value eliminated in set function
    '01004': DataError("String data, right-truncated", 422),  # String data, right-truncated
    '01006': OperationalError("Privilege not revoked", 500),  # Privilege not revoked
    '01007': OperationalError("Privilege not granted", 500),  # Privilege not granted
    '01S00': ProgrammingError("Invalid connection string attribute", 422),  # Invalid connection string attribute
    '01S01': DataError("Error in row", 422),  # Error in row
    '01S02': Warning("Option value changed", 400),  # Option value changed
    '01S06': OperationalError("Attempt to fetch before the result set returned the first rowset", 500),  # Attempt to fetch before the result set returned the first rowset
    '01S07': DataError("Fractional truncation", 422),  # Fractional truncation
    '01S08': OperationalError("Error saving File DSN", 500),  # Error saving File DSN
    '01S09': ProgrammingError("Invalid keyword", 422),  # Invalid keyword
    '07001': ProgrammingError("Wrong number of parameters", 422),  # Wrong number of parameters
    '07002': ProgrammingError("COUNT field incorrect", 422),  # COUNT field incorrect
    '07005': ProgrammingError("Prepared statement not a cursor-specification", 422),  # Prepared statement not a cursor-specification
    '07006': ProgrammingError("Restricted data type attribute violation", 422),  # Restricted data type attribute violation
    '07009': ProgrammingError("Invalid descriptor index", 422),  # Invalid descriptor index
    '07S01': ProgrammingError("Invalid use of default parameter", 422),  # Invalid use of default parameter
    '08001': OperationalError("Client unable to establish connection", 500),  # Client unable to establish connection
    '08002': OperationalError("Connection name in use", 500),  # Connection name in use
    '08003': OperationalError("Connection not open", 500),  # Connection not open
    '08004': OperationalError("Server rejected the connection", 500),  # Server rejected the connection
    '08007': OperationalError("Connection failure during transaction", 500),  # Connection failure during transaction
    '08S01': OperationalError("Communication link failure", 500),  # Communication link failure
    '21S01': ProgrammingError("Insert value list does not match column list", 422),  # Insert value list does not match column list
    '21S02': ProgrammingError("Degree of derived table does not match column list", 422),  # Degree of derived table does not match column list
    '22001': DataError("String data, right-truncated", 422),  # String data, right-truncated
    '22002': DataError("Indicator variable required but not supplied", 422),  # Indicator variable required but not supplied
    '22003': DataError("Numeric value out of range", 422),  # Numeric value out of range
    '22007': DataError("Invalid datetime format", 422),  # Invalid datetime format
    '22008': DataError("Datetime field overflow", 422),  # Datetime field overflow
    '22012': DataError("Division by zero", 422),  # Division by zero
    '22015': DataError("Interval field overflow", 422),  # Interval field overflow
    '22018': DataError("Invalid character value for cast specification", 422),  # Invalid character value for cast specification
    '22019': ProgrammingError("Invalid escape character", 422),  # Invalid escape character
    '22025': ProgrammingError("Invalid escape sequence", 422),  # Invalid escape sequence
    '22026': DataError("String data, length mismatch", 422),  # String data, length mismatch
    '23000': IntegrityError("Integrity constraint violation", 422),  # Integrity constraint violation
    '24000': ProgrammingError("Invalid cursor state", 422),  # Invalid cursor state
    '25000': OperationalError("Invalid transaction state", 500),  # Invalid transaction state
    '25S01': OperationalError("Transaction state", 500),  # Transaction state
    '25S02': OperationalError("Transaction is still active", 500),  # Transaction is still active
    '25S03': OperationalError("Transaction is rolled back", 500),  # Transaction is rolled back
    '28000': OperationalError("Invalid authorization specification", 500),  # Invalid authorization specification
    '34000': ProgrammingError("Invalid cursor name", 422),  # Invalid cursor name
    '3C000': ProgrammingError("Duplicate cursor name", 422),  # Duplicate cursor name
    '3D000': ProgrammingError("Invalid catalog name", 422),  # Invalid catalog name
    '3F000': ProgrammingError("Invalid schema name", 422),  # Invalid schema name
    '40001': OperationalError("Serialization failure", 500),  # Serialization failure
    '40002': IntegrityError("Integrity constraint violation", 422),  # Integrity constraint violation
    '40003': OperationalError("Statement completion unknown", 500),  # Statement completion unknown
    '42000': ProgrammingError("Syntax error or access violation", 422),  # Syntax error or access violation
    '42S01': ProgrammingError("Base table or view already exists", 422),  # Base table or view already exists
    '42S02': ProgrammingError("Base table or view not found", 422),  # Base table or view not found
    '42S11': ProgrammingError("Index already exists", 422),  # Index already exists
    '42S12': ProgrammingError("Index not found", 422),  # Index not found
    '42S21': ProgrammingError("Column already exists", 422),  # Column already exists
    '42S22': ProgrammingError("Column not found", 422),  # Column not found
    '44000': IntegrityError("WITH CHECK OPTION violation", 422),  # WITH CHECK OPTION violation
    'HY000': OperationalError("General error", 500),  # General error
    'HY001': OperationalError("Memory allocation error", 500),  # Memory allocation error
    'HY003': ProgrammingError("Invalid application buffer type", 422),  # Invalid application buffer type
    'HY004': ProgrammingError("Invalid SQL data type", 422),  # Invalid SQL data type
    'HY007': ProgrammingError("Associated statement is not prepared", 422),  # Associated statement is not prepared
    'HY008': OperationalError("Operation canceled", 500),  # Operation canceled
    'HY009': ProgrammingError("Invalid use of null pointer", 422),  # Invalid use of null pointer
    'HY010': ProgrammingError("Function sequence error", 422),  # Function sequence error
    'HY011': ProgrammingError("Attribute cannot be set now", 422),  # Attribute cannot be set now
    'HY012': ProgrammingError("Invalid transaction operation code", 422),  # Invalid transaction operation code
    'HY013': OperationalError("Memory management error", 500),  # Memory management error
    'HY014': OperationalError("Limit on the number of handles exceeded", 500),  # Limit on the number of handles exceeded
    'HY015': ProgrammingError("No cursor name available", 422),  # No cursor name available
    'HY016': ProgrammingError("Cannot modify an implementation row descriptor", 422),  # Cannot modify an implementation row descriptor
    'HY017': ProgrammingError("Invalid use of an automatically allocated descriptor handle", 422),  # Invalid use of an automatically allocated descriptor handle
    'HY018': OperationalError("Server declined cancel request", 500),  # Server declined cancel request
    'HY019': DataError("Non-character and non-binary data sent in pieces", 422),  # Non-character and non-binary data sent in pieces
    'HY020': DataError("Attempt to concatenate a null value", 422),  # Attempt to concatenate a null value
    'HY021': ProgrammingError("Inconsistent descriptor information", 422),  # Inconsistent descriptor information
    'HY024': ProgrammingError("Invalid attribute value", 422),  # Invalid attribute value
    'HY090': ProgrammingError("Invalid string or buffer length", 422),  # Invalid string or buffer length
    'HY091': ProgrammingError("Invalid descriptor field identifier", 422),  # Invalid descriptor field identifier
    'HY092': ProgrammingError("Invalid attribute/option identifier", 422),  # Invalid attribute/option identifier
    'HY095': ProgrammingError("Function type out of range", 422),  # Function type out of range
    'HY096': ProgrammingError("Invalid information type", 422),  # Invalid information type
    'HY097': ProgrammingError("Column type out of range", 422),  # Column type out of range
    'HY098': ProgrammingError("Scope type out of range", 422),  # Scope type out of range
    'HY099': ProgrammingError("Nullable type out of range", 422),  # Nullable type out of range
    'HY100': ProgrammingError("Uniqueness option type out of range", 422),  # Uniqueness option type out of range
    'HY101': ProgrammingError("Accuracy option type out of range", 422),  # Accuracy option type out of range
    'HY103': ProgrammingError("Invalid retrieval code", 422),  # Invalid retrieval code
    'HY104': ProgrammingError("Invalid precision or scale value", 422),  # Invalid precision or scale value
    'HY105': ProgrammingError("Invalid parameter type", 422),  # Invalid parameter type
    'HY106': ProgrammingError("Fetch type out of range", 422),  # Fetch type out of range
    'HY107': ProgrammingError("Row value out of range", 422),  # Row value out of range
    'HY109': ProgrammingError("Invalid cursor position", 422),  # Invalid cursor position
    'HY110': ProgrammingError("Invalid driver completion", 422),  # Invalid driver completion
    'HY111': ProgrammingError("Invalid bookmark value", 422),  # Invalid bookmark value
    'HYC00': NotSupportedError("Optional feature not implemented", 501),  # Optional feature not implemented
    'HYT00': OperationalError("Timeout expired", 500),  # Timeout expired
    'HYT01': OperationalError("Connection timeout expired", 500),  # Connection timeout expired
    'IM001': NotSupportedError("Driver does not support this function", 501),  # Driver does not support this function
    'IM002': OperationalError("Data source name not found and no default driver specified", 500),  # Data source name not found and no default driver specified
    'IM003': OperationalError("Specified driver could not be loaded", 500),  # Specified driver could not be loaded
    'IM004': OperationalError("Driver's SQLAllocHandle on SQL_HANDLE_ENV failed", 500),  # Driver's SQLAllocHandle on SQL_HANDLE_ENV failed
    'IM005': OperationalError("Driver's SQLAllocHandle on SQL_HANDLE_DBC failed", 500),  # Driver's SQLAllocHandle on SQL_HANDLE_DBC failed
    'IM006': OperationalError("Driver's SQLSetConnectAttr failed", 500),  # Driver's SQLSetConnectAttr failed
    'IM007': OperationalError("No data source or driver specified; dialog prohibited", 500),  # No data source or driver specified; dialog prohibited
    'IM008': OperationalError("Dialog failed", 500),  # Dialog failed
    'IM009': OperationalError("Unable to load translation DLL", 500),  # Unable to load translation DLL
    'IM010': OperationalError("Data source name too long", 500),  # Data source name too long
    'IM011': OperationalError("Driver name too long", 500),  # Driver name too long
    'IM012': OperationalError("DRIVER keyword syntax error", 500),  # DRIVER keyword syntax error
    'IM013': OperationalError("Trace file error", 500),  # Trace file error
    'IM014': OperationalError("Invalid name of File DSN", 500),  # Invalid name of File DSN
    'IM015': OperationalError("Corrupt file data source", 500),  # Corrupt file data source
}

def raise_exception(sqlstate: str) -> None:
    """
    Raise a custom exception based on the given SQLSTATE code.
    This function raises a custom exception based on the provided SQLSTATE code.
    If the code is not found in the mapping, a generic DatabaseError is raised.
    Args:
        sqlstate (str): The SQLSTATE code to map to a custom exception.
    Raises:
        DatabaseError: If the SQLSTATE code is not found in the mapping.
    """
    if sqlstate in sqlstate_to_exception:
        exception = sqlstate_to_exception[sqlstate]
        if exception:
            raise exception
    raise DatabaseError(f"An error occurred with SQLSTATE code {sqlstate}")