"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
This module defines the Connection class, which is used to manage a connection to a database.
The class provides methods to establish a connection, create cursors, commit transactions,
roll back transactions, and close the connection.
Resource Management:
- All cursors created from this connection are tracked internally.
- When close() is called on the connection, all open cursors are automatically closed.
- Do not use any cursor after the connection is closed; doing so will raise an exception.
- Cursors are also cleaned up automatically when no longer referenced, to prevent memory leaks.
"""

import weakref
import re
import codecs
from typing import Any, Dict, Optional, Union, List, Tuple, Callable, TYPE_CHECKING
import threading

import mssql_python
from mssql_python.cursor import Cursor
from mssql_python.helpers import (
    add_driver_to_connection_str,
    sanitize_connection_string,
    sanitize_user_input,
    validate_attribute_value,
)
from mssql_python.logging import logger
from mssql_python import ddbc_bindings
from mssql_python.pooling import PoolingManager
from mssql_python.exceptions import (
    Warning,  # pylint: disable=redefined-builtin
    Error,
    InterfaceError,
    DatabaseError,
    DataError,
    OperationalError,
    IntegrityError,
    InternalError,
    ProgrammingError,
    NotSupportedError,
)
from mssql_python.auth import process_connection_string
from mssql_python.constants import ConstantsDDBC, GetInfoConstants
from mssql_python.connection_string_parser import _ConnectionStringParser
from mssql_python.connection_string_builder import _ConnectionStringBuilder
from mssql_python.constants import _RESERVED_PARAMETERS

if TYPE_CHECKING:
    from mssql_python.row import Row

# Add SQL_WMETADATA constant for metadata decoding configuration
SQL_WMETADATA: int = -99  # Special flag for column name decoding
# Threshold to determine if an info type is string-based
INFO_TYPE_STRING_THRESHOLD: int = 10000

# UTF-16 encoding variants that should use SQL_WCHAR by default
# Note: "utf-16" with BOM is NOT included as it's problematic for SQL_WCHAR
UTF16_ENCODINGS: frozenset[str] = frozenset(["utf-16le", "utf-16be"])


def _validate_utf16_wchar_compatibility(
    encoding: str, wchar_type: int, context: str = "SQL_WCHAR"
) -> None:
    """
    Validates UTF-16 encoding compatibility with SQL_WCHAR.

    Centralizes the validation logic to eliminate duplication across setencoding/setdecoding.

    Args:
        encoding: The encoding string (already normalized to lowercase)
        wchar_type: The SQL_WCHAR constant value to check against
        context: Context string for error messages ('SQL_WCHAR', 'SQL_WCHAR ctype', etc.)

    Raises:
        ProgrammingError: If encoding is incompatible with SQL_WCHAR
    """
    if encoding == "utf-16":
        # UTF-16 with BOM is rejected due to byte order ambiguity
        logger.warning("utf-16 with BOM rejected for %s", context)
        raise ProgrammingError(
            driver_error="UTF-16 with Byte Order Mark not supported for SQL_WCHAR",
            ddbc_error=(
                "Cannot use 'utf-16' encoding with SQL_WCHAR due to Byte Order Mark ambiguity. "
                "Use 'utf-16le' or 'utf-16be' instead for explicit byte order."
            ),
        )
    elif encoding not in UTF16_ENCODINGS:
        # Non-UTF-16 encodings are not supported with SQL_WCHAR
        logger.warning(
            "Non-UTF-16 encoding %s attempted with %s", sanitize_user_input(encoding), context
        )

        # Generate context-appropriate error messages
        if "ctype" in context:
            driver_error = f"SQL_WCHAR ctype only supports UTF-16 encodings"
            ddbc_context = "SQL_WCHAR ctype"
        else:
            driver_error = f"SQL_WCHAR only supports UTF-16 encodings"
            ddbc_context = "SQL_WCHAR"

        raise ProgrammingError(
            driver_error=driver_error,
            ddbc_error=(
                f"Cannot use encoding '{encoding}' with {ddbc_context}. "
                f"SQL_WCHAR requires UTF-16 encodings (utf-16le, utf-16be)"
            ),
        )


def _validate_encoding(encoding: str) -> bool:
    """
    Cached encoding validation using codecs.lookup().

    Args:
        encoding (str): The encoding name to validate.

    Returns:
        bool: True if encoding is valid, False otherwise.

    Note:
        Uses LRU cache to avoid repeated expensive codecs.lookup() calls.
        Cache size is limited to 128 entries which should cover most use cases.
        Also validates that encoding name only contains safe characters.
    """
    # Basic security checks - prevent obvious attacks
    if not encoding or not isinstance(encoding, str):
        return False

    # Check length limit (prevent DOS)
    if len(encoding) > 100:
        return False

    # Prevent null bytes and control characters that could cause issues
    if "\x00" in encoding or any(ord(c) < 32 and c not in "\t\n\r" for c in encoding):
        return False

    # Then check if it's a valid Python codec
    try:
        codecs.lookup(encoding)
        return True
    except LookupError:
        return False


class Connection:
    """
    A class to manage a connection to a database, compliant with DB-API 2.0 specifications.

    This class provides methods to establish a connection to a database, create cursors,
    commit transactions, roll back transactions, and close the connection. It is designed
    to be used in a context where database operations are required, such as executing queries
    and fetching results.

    The Connection class supports the Python context manager protocol (with statement).
    When used as a context manager, it will automatically close the connection when
    exiting the context, ensuring proper resource cleanup.

    Example usage:
        with connect(connection_string) as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO table VALUES (?)", [value])
        # Connection is automatically closed when exiting the with block

    For long-lived connections, use without context manager:
        conn = connect(connection_string)
        try:
            # Multiple operations...
        finally:
            conn.close()

    Methods:
        __init__(database: str) -> None:
        connect_to_db() -> None:
        cursor() -> Cursor:
        commit() -> None:
        rollback() -> None:
        close() -> None:
        __enter__() -> Connection:
        __exit__() -> None:
        setencoding(encoding=None, ctype=None) -> None:
        setdecoding(sqltype, encoding=None, ctype=None) -> None:
        getdecoding(sqltype) -> dict:
        set_attr(attribute, value) -> None:
    """

    # DB-API 2.0 Exception attributes
    # These allow users to catch exceptions using connection.Error,
    # connection.ProgrammingError, etc.
    Warning = Warning
    Error = Error
    InterfaceError = InterfaceError
    DatabaseError = DatabaseError
    DataError = DataError
    OperationalError = OperationalError
    IntegrityError = IntegrityError
    InternalError = InternalError
    ProgrammingError = ProgrammingError
    NotSupportedError = NotSupportedError

    def __init__(
        self,
        connection_str: str = "",
        autocommit: bool = False,
        attrs_before: Optional[Dict[int, Union[int, str, bytes]]] = None,
        timeout: int = 0,
        **kwargs: Any,
    ) -> None:
        """
        Initialize the connection object with the specified connection string and parameters.

        Args:
            connection_str (str): The connection string to connect to.
            autocommit (bool): If True, causes a commit to be performed after
                              each SQL statement.
            attrs_before (dict, optional): Dictionary of connection attributes to set before
                                          connection establishment. Keys are SQL_ATTR_* constants,
                                          and values are their corresponding settings.
                                          Use this for attributes that must be set before
                                          connecting, such as SQL_ATTR_LOGIN_TIMEOUT,
                                          SQL_ATTR_ODBC_CURSORS, and SQL_ATTR_PACKET_SIZE.
            timeout (int): Login timeout in seconds. 0 means no timeout.
            **kwargs: Additional key/value pairs for the connection string.

        Returns:
            None

        Raises:
            ValueError: If the connection string is invalid or connection fails.

        This method sets up the initial state for the connection object,
        preparing it for further operations such as connecting to the
        database, executing queries, etc.

        Example:
            >>> # Setting login timeout using attrs_before
            >>> import mssql_python as ms
            >>> conn = ms.connect("Server=myserver;Database=mydb",
            ...                   attrs_before={ms.SQL_ATTR_LOGIN_TIMEOUT: 30})
        """
        self.connection_str = self._construct_connection_string(connection_str, **kwargs)
        self._attrs_before = attrs_before or {}

        # Initialize encoding settings with defaults for Python 3
        # Python 3 only has str (which is Unicode), so we use utf-16le by default
        self._encoding_settings = {
            "encoding": "utf-16le",
            "ctype": ConstantsDDBC.SQL_WCHAR.value,
        }

        # Initialize decoding settings with Python 3 defaults
        self._decoding_settings = {
            ConstantsDDBC.SQL_CHAR.value: {
                "encoding": "utf-8",
                "ctype": ConstantsDDBC.SQL_CHAR.value,
            },
            ConstantsDDBC.SQL_WCHAR.value: {
                "encoding": "utf-16le",
                "ctype": ConstantsDDBC.SQL_WCHAR.value,
            },
            SQL_WMETADATA: {
                "encoding": "utf-16le",
                "ctype": ConstantsDDBC.SQL_WCHAR.value,
            },
        }

        # Check if the connection string contains authentication parameters
        # This is important for processing the connection string correctly.
        # If authentication is specified, it will be processed to handle
        # different authentication types like interactive, device code, etc.
        if re.search(r"authentication", self.connection_str, re.IGNORECASE):
            connection_result = process_connection_string(self.connection_str)
            self.connection_str = connection_result[0]
            if connection_result[1]:
                self._attrs_before.update(connection_result[1])

        self._closed = False
        self._timeout = timeout

        # Using WeakSet which automatically removes cursors when they are no
        # longer in use
        # It is a set that holds weak references to its elements.
        # When an object is only weakly referenced, it can be garbage
        # collected even if it's still in the set.
        # It prevents memory leaks by ensuring that cursors are cleaned up
        # when no longer in use without requiring explicit deletion.
        # TODO: Think and implement scenarios for multi-threaded access
        # to cursors
        self._cursors = weakref.WeakSet()

        # Initialize output converters dictionary and its lock for thread safety
        self._output_converters = {}
        self._converters_lock = threading.Lock()

        # Initialize encoding/decoding settings lock for thread safety
        # This lock protects both _encoding_settings and _decoding_settings dictionaries
        # from concurrent modification. We use a simple Lock (not RLock) because:
        # - Write operations (setencoding/setdecoding) replace the entire dict atomically
        # - Read operations (getencoding/getdecoding) return a copy, so they're safe
        # - No recursive locking is needed in our usage pattern
        # This is more performant than RLock for the multiple-readers-single-writer pattern
        self._encoding_lock = threading.Lock()

        # Initialize search escape character
        self._searchescape = None

        # Auto-enable pooling if user never called
        if not PoolingManager.is_initialized():
            PoolingManager.enable()
        self._pooling = PoolingManager.is_enabled()
        self._conn = ddbc_bindings.Connection(
            self.connection_str, self._pooling, self._attrs_before
        )
        self.setautocommit(autocommit)

        # Register this connection for cleanup before Python shutdown
        # This ensures ODBC handles are freed in correct order, preventing leaks
        try:
            if hasattr(mssql_python, "_register_connection"):
                mssql_python._register_connection(self)
        except AttributeError as e:
            # If registration fails, continue - cleanup will still happen via __del__
            logger.warning(
                f"Failed to register connection for shutdown cleanup: {type(e).__name__}: {e}"
            )
        except Exception as e:
            # Catch any other unexpected errors during registration
            logger.error(
                f"Unexpected error during connection registration: {type(e).__name__}: {e}"
            )

    def _construct_connection_string(self, connection_str: str = "", **kwargs: Any) -> str:
        """
        Construct the connection string by parsing, validating, and merging parameters.

        This method performs a 6-step process:
        1. Parse and validate the base connection_str (validates against allowlist)
        2. Normalize parameter names (e.g., addr/address -> Server, uid -> UID)
        3. Merge kwargs (which override connection_str params after normalization)
        4. Build connection string from normalized, merged params
        5. Add Driver and APP parameters (always controlled by the driver)
        6. Return the final connection string

        Args:
            connection_str (str): The base connection string.
            **kwargs: Additional key/value pairs for the connection string.

        Returns:
            str: The constructed and validated connection string.
        """

        # Step 1: Parse base connection string with allowlist validation
        # The parser validates everything: unknown params, reserved params, duplicates, syntax
        parser = _ConnectionStringParser(validate_keywords=True)
        parsed_params = parser._parse(connection_str)

        # Step 2: Normalize parameter names (e.g., addr/address -> Server, uid -> UID)
        # This handles synonym mapping and deduplication via normalized keys
        normalized_params = _ConnectionStringParser._normalize_params(
            parsed_params, warn_rejected=False
        )

        # Step 3: Process kwargs and merge with normalized_params
        # kwargs override connection string values (processed after, so they take precedence)
        for key, value in kwargs.items():
            normalized_key = _ConnectionStringParser.normalize_key(key)
            if normalized_key:
                # Driver and APP are reserved - raise error if user tries to set them
                if normalized_key in _RESERVED_PARAMETERS:
                    raise ValueError(
                        f"Connection parameter '{key}' is reserved and controlled by the driver. "
                        f"It cannot be set by the user."
                    )
                # kwargs override any existing values from connection string
                normalized_params[normalized_key] = str(value)
            else:
                logger.warning(f"Ignoring unknown connection parameter from kwargs: {key}")

        # Step 4: Build connection string with merged params
        builder = _ConnectionStringBuilder(normalized_params)

        # Step 5: Add Driver and APP parameters (always controlled by the driver)
        # These maintain existing behavior: Driver is always hardcoded, APP is always MSSQL-Python
        builder.add_param("Driver", "ODBC Driver 18 for SQL Server")
        builder.add_param("APP", "MSSQL-Python")

        # Step 6: Build final string
        conn_str = builder.build()

        logger.info("Final connection string: %s", sanitize_connection_string(conn_str))

        return conn_str

    @property
    def timeout(self) -> int:
        """
        Get the current query timeout setting in seconds.

        Returns:
            int: The timeout value in seconds. Zero means no timeout (wait indefinitely).
        """
        return self._timeout

    @timeout.setter
    def timeout(self, value: int) -> None:
        """
        Set the query timeout for all operations performed by this connection.

        Args:
            value (int): The timeout value in seconds. Zero means no timeout.

        Returns:
            None

        Note:
            This timeout applies to all cursors created from this connection.
            It cannot be changed for individual cursors or SQL statements.
            If a query timeout occurs, an OperationalError exception will be raised.
        """
        if not isinstance(value, int):
            raise TypeError("Timeout must be an integer")
        if value < 0:
            raise ValueError("Timeout cannot be negative")
        self._timeout = value
        logger.info(f"Query timeout set to {value} seconds")

    @property
    def autocommit(self) -> bool:
        """
        Return the current autocommit mode of the connection.
        Returns:
            bool: True if autocommit is enabled, False otherwise.
        """
        return self._conn.get_autocommit()

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        """
        Set the autocommit mode of the connection.
        Args:
            value (bool): True to enable autocommit, False to disable it.
        Returns:
            None
        """
        self.setautocommit(value)
        logger.info("Autocommit mode set to %s.", value)

    @property
    def closed(self) -> bool:
        """
        Returns True if the connection is closed, False otherwise.

        This property indicates whether close() was explicitly called on
        the connection. Note that this does not indicate whether the
        connection is healthy/alive - if a timeout or network issue breaks
        the connection, closed would still be False until close() is
        explicitly called.

        Returns:
            bool: True if the connection is closed, False otherwise.
        """
        return self._closed

    def setautocommit(self, value: bool = False) -> None:
        """
        Set the autocommit mode of the connection.
        Args:
            value (bool): True to enable autocommit, False to disable it.
        Returns:
            None
        Raises:
            DatabaseError: If there is an error while setting the autocommit mode.
        """
        self._conn.set_autocommit(value)

    def setencoding(self, encoding: Optional[str] = None, ctype: Optional[int] = None) -> None:
        """
        Sets the text encoding for SQL statements and text parameters.

        Since Python 3 only has str (which is Unicode), this method configures
        how text is encoded when sending to the database.

        Args:
            encoding (str, optional): The encoding to use. This must be a valid Python
                encoding that converts text to bytes. If None, defaults to 'utf-16le'.
            ctype (int, optional): The C data type to use when passing data:
                SQL_CHAR or SQL_WCHAR. If not provided, SQL_WCHAR is used for
                UTF-16 variants (see UTF16_ENCODINGS constant). SQL_CHAR is used
                for all other encodings.

        Returns:
            None

        Raises:
            ProgrammingError: If the encoding is not valid or not supported.
            InterfaceError: If the connection is closed.

        Example:
            # For databases that only communicate with UTF-8
            cnxn.setencoding(encoding='utf-8')

            # For explicitly using SQL_CHAR
            cnxn.setencoding(encoding='utf-8', ctype=mssql_python.SQL_CHAR)
        """
        logger.debug(
            "setencoding: Configuring encoding=%s, ctype=%s",
            str(encoding) if encoding else "default",
            str(ctype) if ctype else "auto",
        )
        if self._closed:
            logger.debug("setencoding: Connection is closed")
            raise InterfaceError(
                driver_error="Connection is closed",
                ddbc_error="Connection is closed",
            )

        # Set default encoding if not provided
        if encoding is None:
            encoding = "utf-16le"
            logger.debug("setencoding: Using default encoding=utf-16le")

        # Validate encoding using cached validation for better performance
        if not _validate_encoding(encoding):
            # Log the sanitized encoding for security
            logger.warning(
                "Invalid encoding attempted: %s",
                sanitize_user_input(str(encoding)),
            )
            raise ProgrammingError(
                driver_error=f"Unsupported encoding: {encoding}",
                ddbc_error=f"The encoding '{encoding}' is not supported by Python",
            )

        # Normalize encoding to casefold for more robust Unicode handling
        encoding = encoding.casefold()
        logger.debug("setencoding: Encoding normalized to %s", encoding)

        # Early validation if ctype is already specified as SQL_WCHAR
        if ctype == ConstantsDDBC.SQL_WCHAR.value:
            _validate_utf16_wchar_compatibility(encoding, ctype, "SQL_WCHAR")

        # Set default ctype based on encoding if not provided
        if ctype is None:
            if encoding in UTF16_ENCODINGS:
                ctype = ConstantsDDBC.SQL_WCHAR.value
                logger.debug("setencoding: Auto-selected SQL_WCHAR for UTF-16")
            else:
                ctype = ConstantsDDBC.SQL_CHAR.value
                logger.debug("setencoding: Auto-selected SQL_CHAR for non-UTF-16")

        # Validate ctype
        valid_ctypes = [ConstantsDDBC.SQL_CHAR.value, ConstantsDDBC.SQL_WCHAR.value]
        if ctype not in valid_ctypes:
            # Log the sanitized ctype for security
            logger.warning(
                "Invalid ctype attempted: %s",
                sanitize_user_input(str(ctype)),
            )
            raise ProgrammingError(
                driver_error=f"Invalid ctype: {ctype}",
                ddbc_error=(
                    f"ctype must be SQL_CHAR ({ConstantsDDBC.SQL_CHAR.value}) or "
                    f"SQL_WCHAR ({ConstantsDDBC.SQL_WCHAR.value})"
                ),
            )

        # Final validation: SQL_WCHAR ctype only supports UTF-16 encodings (without BOM)
        if ctype == ConstantsDDBC.SQL_WCHAR.value:
            _validate_utf16_wchar_compatibility(encoding, ctype, "SQL_WCHAR")

        # Store the encoding settings (thread-safe with lock)
        with self._encoding_lock:
            self._encoding_settings = {"encoding": encoding, "ctype": ctype}

        # Log with sanitized values for security
        logger.info(
            "Text encoding set to %s with ctype %s",
            sanitize_user_input(encoding),
            sanitize_user_input(str(ctype)),
        )

    def getencoding(self) -> Dict[str, Union[str, int]]:
        """
        Gets the current text encoding settings (thread-safe).

        Returns:
            dict: A dictionary containing 'encoding' and 'ctype' keys.

        Raises:
            InterfaceError: If the connection is closed.

        Example:
            settings = cnxn.getencoding()
            print(f"Current encoding: {settings['encoding']}")
            print(f"Current ctype: {settings['ctype']}")

        Note:
            This method is thread-safe and can be called from multiple threads concurrently.
            Returns a copy of the settings to prevent external modification.
        """
        if self._closed:
            raise InterfaceError(
                driver_error="Connection is closed",
                ddbc_error="Connection is closed",
            )

        # Thread-safe read with lock to prevent race conditions
        with self._encoding_lock:
            return self._encoding_settings.copy()

    def setdecoding(
        self, sqltype: int, encoding: Optional[str] = None, ctype: Optional[int] = None
    ) -> None:
        """
        Sets the text decoding used when reading SQL_CHAR and SQL_WCHAR from the database.

        This method configures how text data is decoded when reading from the database.
        In Python 3, all text is Unicode (str), so this primarily affects the encoding
        used to decode bytes from the database.

        Args:
            sqltype (int): The SQL type being configured: SQL_CHAR, SQL_WCHAR, or SQL_WMETADATA.
                SQL_WMETADATA is a special flag for configuring column name decoding.
            encoding (str, optional): The Python encoding to use when decoding the data.
                If None, uses default encoding based on sqltype.
            ctype (int, optional): The C data type to request from SQLGetData:
                SQL_CHAR or SQL_WCHAR. If None, uses default based on encoding.

        Returns:
            None

        Raises:
            ProgrammingError: If the sqltype, encoding, or ctype is invalid.
            InterfaceError: If the connection is closed.

        Example:
            # Configure SQL_CHAR to use UTF-8 decoding
            cnxn.setdecoding(mssql_python.SQL_CHAR, encoding='utf-8')

            # Configure column metadata decoding
            cnxn.setdecoding(mssql_python.SQL_WMETADATA, encoding='utf-16le')

            # Use explicit ctype
            cnxn.setdecoding(mssql_python.SQL_WCHAR, encoding='utf-16le',
                           ctype=mssql_python.SQL_WCHAR)
        """
        if self._closed:
            raise InterfaceError(
                driver_error="Connection is closed",
                ddbc_error="Connection is closed",
            )

        # Validate sqltype
        valid_sqltypes = [
            ConstantsDDBC.SQL_CHAR.value,
            ConstantsDDBC.SQL_WCHAR.value,
            SQL_WMETADATA,
        ]
        if sqltype not in valid_sqltypes:
            logger.warning(
                "Invalid sqltype attempted: %s",
                sanitize_user_input(str(sqltype)),
            )
            raise ProgrammingError(
                driver_error=f"Invalid sqltype: {sqltype}",
                ddbc_error=(
                    f"sqltype must be SQL_CHAR ({ConstantsDDBC.SQL_CHAR.value}), "
                    f"SQL_WCHAR ({ConstantsDDBC.SQL_WCHAR.value}), or "
                    f"SQL_WMETADATA ({SQL_WMETADATA})"
                ),
            )

        # Set default encoding based on sqltype if not provided
        if encoding is None:
            if sqltype == ConstantsDDBC.SQL_CHAR.value:
                encoding = "utf-8"  # Default for SQL_CHAR in Python 3
            else:  # SQL_WCHAR or SQL_WMETADATA
                encoding = "utf-16le"  # Default for SQL_WCHAR in Python 3

        # Validate encoding using cached validation for better performance
        if not _validate_encoding(encoding):
            logger.warning(
                "Invalid encoding attempted: %s",
                sanitize_user_input(str(encoding)),
            )
            raise ProgrammingError(
                driver_error=f"Unsupported encoding: {encoding}",
                ddbc_error=f"The encoding '{encoding}' is not supported by Python",
            )

        # Normalize encoding to lowercase for consistency
        encoding = encoding.lower()

        # Validate SQL_WCHAR encoding compatibility
        if sqltype == ConstantsDDBC.SQL_WCHAR.value:
            _validate_utf16_wchar_compatibility(encoding, sqltype, "SQL_WCHAR sqltype")

        # SQL_WMETADATA can use any valid encoding (UTF-8, UTF-16, etc.)
        # No restriction needed here - let users configure as needed

        # Set default ctype based on encoding if not provided
        if ctype is None:
            if encoding in UTF16_ENCODINGS:
                ctype = ConstantsDDBC.SQL_WCHAR.value
            else:
                ctype = ConstantsDDBC.SQL_CHAR.value

        # Validate ctype
        valid_ctypes = [ConstantsDDBC.SQL_CHAR.value, ConstantsDDBC.SQL_WCHAR.value]
        if ctype not in valid_ctypes:
            logger.warning(
                "Invalid ctype attempted: %s",
                sanitize_user_input(str(ctype)),
            )
            raise ProgrammingError(
                driver_error=f"Invalid ctype: {ctype}",
                ddbc_error=(
                    f"ctype must be SQL_CHAR ({ConstantsDDBC.SQL_CHAR.value}) or "
                    f"SQL_WCHAR ({ConstantsDDBC.SQL_WCHAR.value})"
                ),
            )

        # Validate SQL_WCHAR ctype encoding compatibility
        if ctype == ConstantsDDBC.SQL_WCHAR.value:
            _validate_utf16_wchar_compatibility(encoding, ctype, "SQL_WCHAR ctype")

        # Store the decoding settings for the specified sqltype (thread-safe with lock)
        with self._encoding_lock:
            self._decoding_settings[sqltype] = {"encoding": encoding, "ctype": ctype}

        # Log with sanitized values for security
        sqltype_name = {
            ConstantsDDBC.SQL_CHAR.value: "SQL_CHAR",
            ConstantsDDBC.SQL_WCHAR.value: "SQL_WCHAR",
            SQL_WMETADATA: "SQL_WMETADATA",
        }.get(sqltype, str(sqltype))

        logger.info(
            "Text decoding set for %s to %s with ctype %s",
            sqltype_name,
            sanitize_user_input(encoding),
            sanitize_user_input(str(ctype)),
        )

    def getdecoding(self, sqltype: int) -> Dict[str, Union[str, int]]:
        """
        Gets the current text decoding settings for the specified SQL type (thread-safe).

        Args:
            sqltype (int): The SQL type to get settings for: SQL_CHAR, SQL_WCHAR, or SQL_WMETADATA.

        Returns:
            dict: A dictionary containing 'encoding' and 'ctype' keys for the specified sqltype.

        Raises:
            ProgrammingError: If the sqltype is invalid.
            InterfaceError: If the connection is closed.

        Example:
            settings = cnxn.getdecoding(mssql_python.SQL_CHAR)
            print(f"SQL_CHAR encoding: {settings['encoding']}")
            print(f"SQL_CHAR ctype: {settings['ctype']}")

        Note:
            This method is thread-safe and can be called from multiple threads concurrently.
            Returns a copy of the settings to prevent external modification.
        """
        if self._closed:
            raise InterfaceError(
                driver_error="Connection is closed",
                ddbc_error="Connection is closed",
            )

        # Validate sqltype
        valid_sqltypes = [
            ConstantsDDBC.SQL_CHAR.value,
            ConstantsDDBC.SQL_WCHAR.value,
            SQL_WMETADATA,
        ]
        if sqltype not in valid_sqltypes:
            raise ProgrammingError(
                driver_error=f"Invalid sqltype: {sqltype}",
                ddbc_error=(
                    f"sqltype must be SQL_CHAR ({ConstantsDDBC.SQL_CHAR.value}), "
                    f"SQL_WCHAR ({ConstantsDDBC.SQL_WCHAR.value}), or "
                    f"SQL_WMETADATA ({SQL_WMETADATA})"
                ),
            )

        # Thread-safe read with lock to prevent race conditions
        with self._encoding_lock:
            return self._decoding_settings[sqltype].copy()

    def set_attr(self, attribute: int, value: Union[int, str, bytes, bytearray]) -> None:
        """
        Set a connection attribute.

        This method sets a connection attribute using SQLSetConnectAttr.
        It provides pyodbc-compatible functionality for configuring connection
        behavior such as autocommit mode, transaction isolation level, and
        connection timeouts.

        Args:
            attribute (int): The connection attribute to set. Should be one of the
                           SQL_ATTR_* constants (e.g., SQL_ATTR_AUTOCOMMIT,
                           SQL_ATTR_TXN_ISOLATION).
            value: The value to set for the attribute. Can be an integer, string,
                   bytes, or bytearray depending on the attribute type.

        Raises:
            InterfaceError: If the connection is closed or attribute is invalid.
            ProgrammingError: If the value type or range is invalid.
            ProgrammingError: If the attribute cannot be set after connection.

        Example:
            >>> conn.set_attr(SQL_ATTR_TXN_ISOLATION, SQL_TXN_READ_COMMITTED)

        Note:
            Some attributes (like SQL_ATTR_LOGIN_TIMEOUT, SQL_ATTR_ODBC_CURSORS, and
            SQL_ATTR_PACKET_SIZE) can only be set before connection establishment and
            must be provided in the attrs_before parameter when creating the connection.
            Attempting to set these attributes after connection will raise a ProgrammingError.
        """
        if self._closed:
            raise InterfaceError(
                "Cannot set attribute on closed connection", "Connection is closed"
            )

        # Use the integrated validation helper function with connection state
        is_valid, error_message, sanitized_attr, sanitized_val = validate_attribute_value(
            attribute, value, is_connected=True
        )

        if not is_valid:
            # Use the already sanitized values for logging
            logger.debug(
                "warning",
                f"Invalid attribute or value: {sanitized_attr}={sanitized_val}, {error_message}",
            )
            raise ProgrammingError(
                driver_error=f"Invalid attribute or value: {error_message}",
                ddbc_error=error_message,
            )

        # Log with sanitized values
        logger.debug(f"Setting connection attribute: {sanitized_attr}={sanitized_val}")

        try:
            # Call the underlying C++ method
            self._conn.set_attr(attribute, value)
            logger.info(f"Connection attribute {sanitized_attr} set successfully")

        except Exception as e:
            error_msg = f"Failed to set connection attribute {sanitized_attr}: {str(e)}"

            # Determine appropriate exception type based on error content
            error_str = str(e).lower()
            if "invalid" in error_str or "unsupported" in error_str or "cast" in error_str:
                logger.error(error_msg)
                raise InterfaceError(error_msg, str(e)) from e
            logger.error(error_msg)
            raise ProgrammingError(error_msg, str(e)) from e

    @property
    def searchescape(self) -> str:
        """
        The ODBC search pattern escape character, as returned by
        SQLGetInfo(SQL_SEARCH_PATTERN_ESCAPE), used to escape special characters
        such as '%' and '_' in LIKE clauses. These are driver specific.

        Returns:
            str: The search pattern escape character (usually '\' or another character)
        """
        if not hasattr(self, "_searchescape") or self._searchescape is None:
            try:
                escape_char = self.getinfo(GetInfoConstants.SQL_SEARCH_PATTERN_ESCAPE.value)
                # Some drivers might return this as an integer memory address
                # or other non-string format, so ensure we have a string
                if not isinstance(escape_char, str):
                    # Default to backslash if not a string
                    escape_char = "\\"
                self._searchescape = escape_char
            except Exception as e:
                # Log the exception for debugging, but do not expose sensitive info
                logger.debug(
                    "warning",
                    "Failed to retrieve search escape character, using default '\\'. "
                    "Exception: %s",
                    type(e).__name__,
                )
                self._searchescape = "\\"
        return self._searchescape

    def cursor(self) -> Cursor:
        """
        Return a new Cursor object using the connection.

        This method creates and returns a new cursor object that can be used to
        execute SQL queries and fetch results. The cursor is associated with the
        current connection and allows interaction with the database.

        Returns:
            Cursor: A new cursor object for executing SQL queries.

        Raises:
            DatabaseError: If there is an error while creating the cursor.
            InterfaceError: If there is an error related to the database interface.
        """
        logger.debug(
            "cursor: Creating new cursor - timeout=%d, total_cursors=%d",
            self._timeout,
            len(self._cursors),
        )
        if self._closed:
            logger.error("cursor: Cannot create cursor on closed connection")
            # raise InterfaceError
            raise InterfaceError(
                driver_error="Cannot create cursor on closed connection",
                ddbc_error="Cannot create cursor on closed connection",
            )

        cursor = Cursor(self, timeout=self._timeout)
        self._cursors.add(cursor)  # Track the cursor
        logger.debug("cursor: Cursor created successfully - total_cursors=%d", len(self._cursors))
        return cursor

    def add_output_converter(self, sqltype: int, func: Callable[[Any], Any]) -> None:
        """
        Register an output converter function that will be called whenever a value
        with the given SQL type is read from the database.

        Thread-safe implementation that protects the converters dictionary with a lock.

        ⚠️ WARNING: Registering an output converter will cause the supplied Python function
        to be executed on every matching database value. Do not register converters from
        untrusted sources, as this can result in arbitrary code execution and security
        vulnerabilities. This API should never be exposed to untrusted or external input.

        Args:
            sqltype (int): The integer SQL type value to convert, which can be one of the
                          defined standard constants (e.g. SQL_VARCHAR) or a database-specific
                          value (e.g. -151 for the SQL Server 2008 geometry data type).
            func (callable): The converter function which will be called with a single parameter,
                            the value, and should return the converted value. If the value is NULL
                            then the parameter passed to the function will be None, otherwise it
                            will be a bytes object.

        Returns:
            None
        """
        with self._converters_lock:
            self._output_converters[sqltype] = func
            # Pass to the underlying connection if native implementation supports it
            if hasattr(self._conn, "add_output_converter"):
                self._conn.add_output_converter(sqltype, func)
        logger.info(f"Added output converter for SQL type {sqltype}")

    def get_output_converter(self, sqltype: Union[int, type]) -> Optional[Callable[[Any], Any]]:
        """
        Get the output converter function for the specified SQL type.

        Thread-safe implementation that protects the converters dictionary with a lock.

        Args:
            sqltype (int or type): The SQL type value or Python type to get the converter for

        Returns:
            callable or None: The converter function or None if no converter is registered

        Note:
            ⚠️ The returned converter function will be executed on database values. Only use
            converters from trusted sources.
        """
        with self._converters_lock:
            return self._output_converters.get(sqltype)

    def remove_output_converter(self, sqltype: Union[int, type]) -> None:
        """
        Remove the output converter function for the specified SQL type.

        Thread-safe implementation that protects the converters dictionary with a lock.

        Args:
            sqltype (int or type): The SQL type value to remove the converter for

        Returns:
            None
        """
        with self._converters_lock:
            if sqltype in self._output_converters:
                del self._output_converters[sqltype]
                # Pass to the underlying connection if native implementation supports it
                if hasattr(self._conn, "remove_output_converter"):
                    self._conn.remove_output_converter(sqltype)
        logger.info(f"Removed output converter for SQL type {sqltype}")

    def clear_output_converters(self) -> None:
        """
        Remove all output converter functions.

        Thread-safe implementation that protects the converters dictionary with a lock.

        Returns:
            None
        """
        with self._converters_lock:
            self._output_converters.clear()
            # Pass to the underlying connection if native implementation supports it
            if hasattr(self._conn, "clear_output_converters"):
                self._conn.clear_output_converters()
        logger.info("Cleared all output converters")

    def execute(self, sql: str, *args: Any) -> Cursor:
        """
        Creates a new Cursor object, calls its execute method, and returns the new cursor.

        This is a convenience method that is not part of the DB API. Since a new Cursor
        is allocated by each call, this should not be used if more than one SQL statement
        needs to be executed on the connection.

        Note on cursor lifecycle management:
        - Each call creates a new cursor that is tracked by the connection's internal WeakSet
        - Cursors are automatically dereferenced/closed when they go out of scope
        - For long-running applications or loops, explicitly call cursor.close() when done
          to release resources immediately rather than waiting for garbage collection

        Args:
            sql (str): The SQL query to execute.
            *args: Parameters to be passed to the query.

        Returns:
            Cursor: A new cursor with the executed query.

        Raises:
            DatabaseError: If there is an error executing the query.
            InterfaceError: If the connection is closed.

        Example:
            # Automatic cleanup (cursor goes out of scope after the operation)
            row = connection.execute("SELECT name FROM users WHERE id = ?", 123).fetchone()

            # Manual cleanup for more explicit resource management
            cursor = connection.execute("SELECT * FROM large_table")
            try:
                # Use cursor...
                rows = cursor.fetchall()
            finally:
                cursor.close()  # Explicitly release resources
        """
        cursor = self.cursor()
        try:
            # Add the cursor to our tracking set BEFORE execution
            # This ensures it's tracked even if execution fails
            self._cursors.add(cursor)

            # Now execute the query
            cursor.execute(sql, *args)
            return cursor
        except Exception:
            # If execution fails, close the cursor to avoid leaking resources
            cursor.close()
            raise

    def batch_execute(
        self,
        statements: List[str],
        params: Optional[List[Union[None, Any, Tuple[Any, ...], List[Any]]]] = None,
        reuse_cursor: Optional[Cursor] = None,
        auto_close: bool = False,
    ) -> Tuple[List[Union[List["Row"], int]], Cursor]:
        """
        Execute multiple SQL statements efficiently using a single cursor.

        This method allows executing multiple SQL statements in sequence using a single
        cursor, which is more efficient than creating a new cursor for each statement.

        Args:
            statements (list): List of SQL statements to execute
            params (list, optional): List of parameter sets corresponding to statements.
                Each item can be None, a single parameter, or a sequence of parameters.
                If None, no parameters will be used for any statement.
            reuse_cursor (Cursor, optional): Existing cursor to reuse instead of creating a new one.
                If None, a new cursor will be created.
            auto_close (bool): Whether to close the cursor after execution if a new one was created.
                Defaults to False. Has no effect if reuse_cursor is provided.

        Returns:
            tuple: (results, cursor) where:
                - results is a list of execution results, one for each statement
                - cursor is the cursor used for execution (useful if you want to keep using it)

        Raises:
            TypeError: If statements is not a list or if params is provided but not a list
            ValueError: If params is provided but has different length than statements
            DatabaseError: If there is an error executing any of the statements
            InterfaceError: If the connection is closed

        Example:
            # Execute multiple statements with a single cursor
            results, _ = conn.batch_execute([
                "INSERT INTO users VALUES (?, ?)",
                "UPDATE stats SET count = count + 1",
                "SELECT * FROM users"
            ], [
                (1, "user1"),
                None,
                None
            ])

            # Last result contains the SELECT results
            for row in results[-1]:
                print(row)

            # Reuse an existing cursor
            my_cursor = conn.cursor()
            results, _ = conn.batch_execute([
                "SELECT * FROM table1",
                "SELECT * FROM table2"
            ], reuse_cursor=my_cursor)

            # Cursor remains open for further use
            my_cursor.execute("SELECT * FROM table3")
        """
        # Validate inputs
        if not isinstance(statements, list):
            raise TypeError("statements must be a list of SQL statements")

        if params is not None:
            if not isinstance(params, list):
                raise TypeError("params must be a list of parameter sets")
            if len(params) != len(statements):
                raise ValueError("params list must have the same length as statements list")
        else:
            # Create a list of None values with the same length as statements
            params = [None] * len(statements)

        # Determine which cursor to use
        is_new_cursor = reuse_cursor is None
        cursor = self.cursor() if is_new_cursor else reuse_cursor

        # Execute statements and collect results
        results = []
        try:
            for i, (stmt, param) in enumerate(zip(statements, params)):
                try:
                    # Execute the statement with parameters if provided
                    if param is not None:
                        cursor.execute(stmt, param)
                    else:
                        cursor.execute(stmt)

                    # For SELECT statements, fetch all rows
                    # For other statements, get the row count
                    if cursor.description is not None:
                        # This is a SELECT statement or similar that returns rows
                        results.append(cursor.fetchall())
                    else:
                        # This is an INSERT, UPDATE, DELETE or similar that doesn't return rows
                        results.append(cursor.rowcount)

                    logger.debug(f"Executed batch statement {i+1}/{len(statements)}")

                except Exception as e:
                    # If a statement fails, include statement context in the error
                    logger.debug(
                        "error",
                        f"Error executing statement {i+1}/{len(statements)}: {e}",
                    )
                    raise

        except Exception:
            # If an error occurs and auto_close is True, close the cursor
            if auto_close:
                try:
                    # Close the cursor regardless of whether it's reused or new
                    cursor.close()
                    logger.debug(
                        "debug",
                        "Automatically closed cursor after batch execution error",
                    )
                except Exception as close_err:
                    logger.debug(
                        "warning",
                        f"Error closing cursor after execution failure: {close_err}",
                    )
            # Re-raise the original exception
            raise

        # Close the cursor if requested and we created a new one
        if is_new_cursor and auto_close:
            cursor.close()
            logger.debug("Automatically closed cursor after batch execution")

        return results, cursor

    def getinfo(self, info_type: int) -> Union[str, int, bool, None]:
        """
        Return general information about the driver and data source.

        Args:
            info_type (int): The type of information to return. See the ODBC
                             SQLGetInfo documentation for the supported values.

        Returns:
            The requested information. The type of the returned value depends
            on the information requested. It will be a string, integer, or boolean.

        Raises:
            DatabaseError: If there is an error retrieving the information.
            InterfaceError: If the connection is closed.
        """
        if self._closed:
            raise InterfaceError(
                driver_error="Cannot get info on closed connection",
                ddbc_error="Cannot get info on closed connection",
            )

        # Check that info_type is an integer
        if not isinstance(info_type, int):
            raise ValueError(f"info_type must be an integer, got {type(info_type).__name__}")

        # Check for invalid info_type values
        if info_type < 0:
            logger.debug(
                "warning",
                f"Invalid info_type: {info_type}. Must be a positive integer.",
            )
            return None

        # Get the raw result from the C++ layer
        try:
            raw_result = self._conn.get_info(info_type)
        except Exception as e:  # pylint: disable=broad-exception-caught
            # Log the error and return None for invalid info types
            logger.warning(f"getinfo({info_type}) failed: {e}")
            return None

        if raw_result is None:
            return None

        # Check if the result is already a simple type
        if isinstance(raw_result, (str, int, bool)):
            return raw_result

        # If it's a dictionary with data and metadata
        if isinstance(raw_result, dict) and "data" in raw_result:
            # Extract data and metadata from the raw result
            data = raw_result["data"]
            length = raw_result["length"]

            # Debug logging to understand the issue better
            logger.debug(
                "debug",
                f"getinfo: info_type={info_type}, length={length}, data_type={type(data)}",
            )

            # Define constants for different return types
            # String types - these return strings in pyodbc
            string_type_constants = {
                GetInfoConstants.SQL_DATA_SOURCE_NAME.value,
                GetInfoConstants.SQL_DRIVER_NAME.value,
                GetInfoConstants.SQL_DRIVER_VER.value,
                GetInfoConstants.SQL_SERVER_NAME.value,
                GetInfoConstants.SQL_USER_NAME.value,
                GetInfoConstants.SQL_DRIVER_ODBC_VER.value,
                GetInfoConstants.SQL_IDENTIFIER_QUOTE_CHAR.value,
                GetInfoConstants.SQL_CATALOG_NAME_SEPARATOR.value,
                GetInfoConstants.SQL_CATALOG_TERM.value,
                GetInfoConstants.SQL_SCHEMA_TERM.value,
                GetInfoConstants.SQL_TABLE_TERM.value,
                GetInfoConstants.SQL_KEYWORDS.value,
                GetInfoConstants.SQL_PROCEDURE_TERM.value,
                GetInfoConstants.SQL_SPECIAL_CHARACTERS.value,
                GetInfoConstants.SQL_SEARCH_PATTERN_ESCAPE.value,
            }

            # Boolean 'Y'/'N' types
            yn_type_constants = {
                GetInfoConstants.SQL_ACCESSIBLE_PROCEDURES.value,
                GetInfoConstants.SQL_ACCESSIBLE_TABLES.value,
                GetInfoConstants.SQL_DATA_SOURCE_READ_ONLY.value,
                GetInfoConstants.SQL_EXPRESSIONS_IN_ORDERBY.value,
                GetInfoConstants.SQL_LIKE_ESCAPE_CLAUSE.value,
                GetInfoConstants.SQL_MULTIPLE_ACTIVE_TXN.value,
                GetInfoConstants.SQL_NEED_LONG_DATA_LEN.value,
                GetInfoConstants.SQL_PROCEDURES.value,
            }

            # Numeric type constants that return integers
            numeric_type_constants = {
                GetInfoConstants.SQL_MAX_COLUMN_NAME_LEN.value,
                GetInfoConstants.SQL_MAX_TABLE_NAME_LEN.value,
                GetInfoConstants.SQL_MAX_SCHEMA_NAME_LEN.value,
                GetInfoConstants.SQL_MAX_CATALOG_NAME_LEN.value,
                GetInfoConstants.SQL_MAX_IDENTIFIER_LEN.value,
                GetInfoConstants.SQL_MAX_STATEMENT_LEN.value,
                GetInfoConstants.SQL_MAX_DRIVER_CONNECTIONS.value,
                GetInfoConstants.SQL_NUMERIC_FUNCTIONS.value,
                GetInfoConstants.SQL_STRING_FUNCTIONS.value,
                GetInfoConstants.SQL_DATETIME_FUNCTIONS.value,
                GetInfoConstants.SQL_TXN_CAPABLE.value,
                GetInfoConstants.SQL_DEFAULT_TXN_ISOLATION.value,
                GetInfoConstants.SQL_CURSOR_COMMIT_BEHAVIOR.value,
            }

            # Determine the type of information we're dealing with
            is_string_type = (
                info_type > INFO_TYPE_STRING_THRESHOLD or info_type in string_type_constants
            )
            is_yn_type = info_type in yn_type_constants
            is_numeric_type = info_type in numeric_type_constants

            # Process the data based on type
            if is_string_type:
                # For string data, ensure we properly handle the byte array
                if isinstance(data, bytes):
                    # Make sure we use the correct amount of data based on length
                    actual_data = data[:length]

                    # SQLGetInfoW returns UTF-16LE encoded strings (wide-character ODBC API)
                    # Try UTF-16LE first (expected), then UTF-8 as fallback
                    for encoding in ("utf-16-le", "utf-8"):
                        try:
                            return actual_data.decode(encoding).rstrip("\0")
                        except UnicodeDecodeError:
                            continue

                    # All decodings failed
                    logger.debug(
                        "Failed to decode string in getinfo (info_type=%d) with supported encodings. "
                        "Returning None to avoid silent corruption.",
                        info_type,
                    )
                    return None
                else:
                    # If it's not bytes, return as is
                    return data
            elif is_yn_type:
                # For Y/N types, pyodbc returns a string 'Y' or 'N'
                if isinstance(data, bytes) and length >= 1:
                    byte_val = data[0]
                    if byte_val in (b"Y"[0], b"y"[0], 1):
                        return "Y"
                    return "N"
                # If it's not a byte or we can't determine, default to 'N'
                return "N"
            elif is_numeric_type:
                # Handle numeric types based on length
                if isinstance(data, bytes):
                    # Map byte length → signed int size
                    int_sizes = {
                        1: lambda d: int(d[0]),
                        2: lambda d: int.from_bytes(d[:2], "little", signed=True),
                        4: lambda d: int.from_bytes(d[:4], "little", signed=True),
                        8: lambda d: int.from_bytes(d[:8], "little", signed=True),
                    }

                    # Direct numeric conversion if supported length
                    if length in int_sizes:
                        result = int_sizes[length](data)
                        return int(result)

                    # Helper: check if all chars are digits
                    def is_digit_bytes(b: bytes) -> bool:
                        return all(c in b"0123456789" for c in b)

                    # Helper: check if bytes are ASCII-printable or NUL padded
                    def is_printable_bytes(b: bytes) -> bool:
                        return all(32 <= c <= 126 or c == 0 for c in b)

                    chunk = data[:length]

                    # Try interpret as integer string
                    if is_digit_bytes(chunk):
                        return int(chunk)

                    # Try decode as ASCII/UTF-8 string
                    if is_printable_bytes(chunk):
                        str_val = chunk.decode("utf-8", errors="replace").rstrip("\0")
                        return int(str_val) if str_val.isdigit() else str_val

                    # For 16-bit values that might be returned for max lengths
                    if length == 2:
                        return int.from_bytes(data[:2], "little", signed=True)

                    # For 32-bit values (common for bitwise flags)
                    if length == 4:
                        return int.from_bytes(data[:4], "little", signed=True)

                    # Fallback: try to convert to int if possible
                    try:
                        if length <= 8:
                            return int.from_bytes(data[:length], "little", signed=True)
                    except Exception:
                        pass

                    # Last resort: return as integer if all else fails
                    try:
                        return int.from_bytes(data[: min(length, 8)], "little", signed=True)
                    except Exception:
                        return 0
                elif isinstance(data, (int, float)):
                    # Already numeric
                    return int(data)
                else:
                    # Try to convert to int if it's a string
                    try:
                        if isinstance(data, str) and data.isdigit():
                            return int(data)
                    except Exception:
                        pass

                    # Return as is if we can't convert
                    return data

            # For other types, try to determine the most appropriate type
            if isinstance(data, bytes):
                # Try to convert to string first
                try:
                    return data[:length].decode("utf-8").rstrip("\0")
                except UnicodeDecodeError:
                    pass

                # Try to convert to int for short binary data
                try:
                    if length <= 8:
                        return int.from_bytes(data[:length], "little", signed=True)
                except Exception:  # pylint: disable=broad-exception-caught
                    pass

                # Return as is if we can't determine
                return data

            return data

        return raw_result  # Return as-is

    def commit(self) -> None:
        """
        Commit the current transaction.

        This method commits the current transaction to the database, making all
        changes made during the transaction permanent. It should be called after
        executing a series of SQL statements that modify the database to ensure
        that the changes are saved.

        Raises:
            InterfaceError: If the connection is closed.
            DatabaseError: If there is an error while committing the transaction.
        """
        # Check if connection is closed
        if self._closed or self._conn is None:
            raise InterfaceError(
                driver_error="Cannot commit on a closed connection",
                ddbc_error="Cannot commit on a closed connection",
            )

        # Commit the current transaction
        self._conn.commit()
        logger.info("Transaction committed successfully.")

    def rollback(self) -> None:
        """
        Roll back the current transaction.

        This method rolls back the current transaction, undoing all changes made
        during the transaction. It should be called if an error occurs during the
        transaction or if the changes should not be saved.

        Raises:
            InterfaceError: If the connection is closed.
            DatabaseError: If there is an error while rolling back the transaction.
        """
        # Check if connection is closed
        if self._closed or self._conn is None:
            raise InterfaceError(
                driver_error="Cannot rollback on a closed connection",
                ddbc_error="Cannot rollback on a closed connection",
            )

        # Roll back the current transaction
        self._conn.rollback()
        logger.info("Transaction rolled back successfully.")

    def close(self) -> None:
        """
        Close the connection now (rather than whenever .__del__() is called).

        This method closes the connection to the database, releasing any resources
        associated with it. After calling this method, the connection object should
        not be used for any further operations. The same applies to all cursor objects
        trying to use the connection. Note that closing a connection without committing
        the changes first will cause an implicit rollback to be performed.

        Raises:
            DatabaseError: If there is an error while closing the connection.
        """
        # Close the connection
        if self._closed:
            return

        # Close all cursors first, but don't let one failure stop the others
        if hasattr(self, "_cursors"):
            # Convert to list to avoid modification during iteration
            cursors_to_close = list(self._cursors)
            close_errors = []

            for cursor in cursors_to_close:
                try:
                    if not cursor.closed:
                        cursor.close()
                except Exception as e:  # pylint: disable=broad-exception-caught
                    # Collect errors but continue closing other cursors
                    close_errors.append(f"Error closing cursor: {e}")
                    logger.warning(f"Error closing cursor: {e}")

            # If there were errors closing cursors, log them but continue
            if close_errors:
                logger.debug(
                    "warning",
                    "Encountered %d errors while closing cursors",
                    len(close_errors),
                )

            # Clear the cursor set explicitly to release any internal
            # references
            self._cursors.clear()

        # Close the connection even if cursor cleanup had issues
        try:
            if self._conn:
                if not self.autocommit:
                    # If autocommit is disabled, rollback any uncommitted changes
                    # This is important to ensure no partial transactions remain
                    # For autocommit True, this is not necessary as each statement is
                    # committed immediately
                    logger.debug("Rolling back uncommitted changes before closing connection.")
                    self._conn.rollback()
                # TODO: Check potential race conditions in case of multithreaded scenarios
                # Close the connection
                self._conn.close()
                self._conn = None
        except Exception as e:
            logger.error(f"Error closing database connection: {e}")
            # Re-raise the connection close error as it's more critical
            raise
        finally:
            # Always mark as closed, even if there were errors
            self._closed = True

        logger.info("Connection closed successfully.")

    def _remove_cursor(self, cursor: Cursor) -> None:
        """
        Remove a cursor from the connection's tracking.

        This method is called when a cursor is closed to ensure proper cleanup.

        Args:
            cursor: The cursor to remove from tracking.
        """
        if hasattr(self, "_cursors"):
            try:
                self._cursors.discard(cursor)
            except Exception:
                pass  # Ignore errors during cleanup

    def __enter__(self) -> "Connection":
        """
        Enter the context manager.

        This method enables the Connection to be used with the 'with' statement.
        When entering the context, it simply returns the connection object itself.

        Returns:
            Connection: The connection object itself.

        Example:
            with connect(connection_string) as conn:
                cursor = conn.cursor()
                cursor.execute("INSERT INTO table VALUES (?)", [value])
                # Transaction will be committed automatically when exiting
        """
        logger.info("Entering connection context manager.")
        return self

    def __exit__(self, *args: Any) -> None:
        """
        Exit the context manager.

        Closes the connection when exiting the context, ensuring proper
        resource cleanup. This follows the modern standard used by most
        database libraries.
        """
        if not self._closed:
            self.close()

    def __del__(self) -> None:
        """
        Destructor to ensure the connection is closed when the connection object
        is no longer needed.
        This is a safety net to ensure resources are cleaned up
        even if close() was not called explicitly.
        """
        if "_closed" not in self.__dict__ or not self._closed:
            try:
                self.close()
            except Exception as e:
                # Dont raise exceptions from __del__ to avoid issues during garbage collection
                logger.warning(f"Error during connection cleanup: {e}")
