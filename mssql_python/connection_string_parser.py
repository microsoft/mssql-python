"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.

ODBC connection string parser for mssql-python.

Handles ODBC-specific syntax per MS-ODBCSTR specification:
- Semicolon-separated key=value pairs
- Braced values: {value}
- Escaped braces: }} → } (only closing braces need escaping)

Parser behavior:
- Validates all key=value pairs
- Raises exceptions for malformed syntax (missing values, unknown keywords, duplicates)
- Collects all errors and reports them together
"""

from typing import Dict, Tuple, Optional
from mssql_python.exceptions import ConnectionStringParseError
from mssql_python.constants import _ALLOWED_CONNECTION_STRING_PARAMS, _RESERVED_PARAMETERS
from mssql_python.helpers import sanitize_user_input
from mssql_python.logging import logger


class _ConnectionStringParser:
    """
    Internal parser for ODBC connection strings. Not part of public API.

    Implements the ODBC Connection String format as specified in MS-ODBCSTR.
    Handles braced values, escaped characters, and proper tokenization.

    Validates connection strings and raises errors for:
    - Unknown/unrecognized keywords
    - Duplicate keywords
    - Incomplete specifications (keyword with no value)

    Reference: https://learn.microsoft.com/en-us/openspecs/sql_server_protocols/ms-odbcstr/55953f0e-2d30-4ad4-8e56-b4207e491409
    """

    def __init__(self, validate_keywords: bool = False) -> None:
        """
        Initialize the parser.

        Args:
            validate_keywords: Whether to validate keywords against the allow-list.
                             If False, pure parsing without validation is performed.
                             This is useful for testing parsing logic independently
                             or when validation is handled separately.
        """
        self._validate_keywords = validate_keywords

    @classmethod
    def normalize_key(cls, key: str) -> Optional[str]:
        """
        Normalize a parameter key to its canonical form.

        Args:
            key: Parameter key from connection string (case-insensitive)

        Returns:
            Canonical parameter name if allowed, None otherwise

        Examples:
            >>> _ConnectionStringParser.normalize_key('SERVER')
            'Server'
            >>> _ConnectionStringParser.normalize_key('uid')
            'UID'
            >>> _ConnectionStringParser.normalize_key('UnsupportedParam')
            None
        """
        key_lower = key.lower().strip()
        return _ALLOWED_CONNECTION_STRING_PARAMS.get(key_lower)

    @staticmethod
    def _normalize_params(params: Dict[str, str], warn_rejected: bool = True) -> Dict[str, str]:
        """
        Normalize and filter parameters against the allow-list (internal use only).

        This method performs several operations:
        - Normalizes parameter names (e.g., addr/address → Server, uid → UID)
        - Filters out parameters not in the allow-list
        - Removes reserved parameters (Driver, APP)
        - Deduplicates via normalized keys

        Args:
            params: Dictionary of connection string parameters (keys should be lowercase)
            warn_rejected: Whether to log warnings for rejected parameters

        Returns:
            Dictionary containing only allowed parameters with normalized keys

        Note:
            Driver and APP parameters are filtered here but will be set by
            the driver in _construct_connection_string to maintain control.
        """
        filtered = {}

        # The rejected list should ideally be empty when used in the normal connection
        # flow, since the parser validates against the allowlist first and raises
        # errors for unknown parameters. This filtering is primarily a safety net.
        rejected = []

        for key, value in params.items():
            normalized_key = _ConnectionStringParser.normalize_key(key)

            if normalized_key:
                # Skip Driver and APP - these are controlled by the driver
                if normalized_key in _RESERVED_PARAMETERS:
                    continue

                # Parameter is allowed
                filtered[normalized_key] = value
            else:
                # Parameter is not in allow-list
                # Note: In normal flow, this should be empty since parser validates first
                rejected.append(key)

        # Log all rejected parameters together if any were found
        if rejected and warn_rejected:
            safe_keys = [sanitize_user_input(key) for key in rejected]
            logger.debug(
                f"Connection string parameters not in allow-list and will be ignored: {', '.join(safe_keys)}"
            )

        return filtered

    def _parse(self, connection_str: str) -> Dict[str, str]:
        """
        Parse a connection string into a dictionary of parameters.

        Validates the connection string and raises ConnectionStringParseError
        if any issues are found (unknown keywords, duplicates, missing values).

        Args:
            connection_str: ODBC-format connection string

        Returns:
            Dictionary mapping parameter names (lowercase) to values

        Raises:
            ConnectionStringParseError: If validation errors are found

        Examples:
            >>> parser = _ConnectionStringParser()
            >>> result = parser._parse("Server=localhost;Database=mydb")
            {'server': 'localhost', 'database': 'mydb'}

            >>> parser._parse("Server={;local;};PWD={p}}w{{d}")
            {'server': ';local;', 'pwd': 'p}w{d'}

            >>> parser._parse("Server=localhost;Server=other")
            ConnectionStringParseError: Duplicate keyword 'server'
        """
        if not connection_str:
            return {}

        connection_str = connection_str.strip()
        if not connection_str:
            return {}

        # Collect all errors for batch reporting
        errors = []

        # Dictionary to store parsed key=value pairs
        params = {}

        # Track which keys we've seen to detect duplicates
        seen_keys = {}  # Maps normalized key -> first occurrence position

        # Track current position in the string
        current_pos = 0
        str_len = len(connection_str)

        # Main parsing loop
        while current_pos < str_len:
            # Skip leading whitespace and semicolons
            while current_pos < str_len and connection_str[current_pos] in " \t;":
                current_pos += 1

            if current_pos >= str_len:
                break

            # Parse the key
            key_start = current_pos

            # Advance until we hit '=', ';', or end of string
            while current_pos < str_len and connection_str[current_pos] not in "=;":
                current_pos += 1

            # Check if we found a valid '=' separator
            if current_pos >= str_len or connection_str[current_pos] != "=":
                # ERROR: No '=' found - incomplete specification
                incomplete_text = connection_str[key_start:current_pos].strip()
                if incomplete_text:
                    errors.append(
                        f"Incomplete specification: keyword '{incomplete_text}' has no value (missing '=')"
                    )
                # Skip to next semicolon
                while current_pos < str_len and connection_str[current_pos] != ";":
                    current_pos += 1
                continue

            # Extract and normalize the key
            key = connection_str[key_start:current_pos].strip().lower()

            # ERROR: Empty key
            if not key:
                errors.append("Empty keyword found (format: =value)")
                current_pos += 1  # Skip the '='
                # Skip to next semicolon
                while current_pos < str_len and connection_str[current_pos] != ";":
                    current_pos += 1
                continue

            # Move past the '='
            current_pos += 1

            # Parse the value
            try:
                value, current_pos = self._parse_value(connection_str, current_pos)

                # ERROR: Empty value
                if not value:
                    errors.append(
                        f"Empty value for keyword '{key}' (all connection string parameters must have non-empty values)"
                    )

                # Check for duplicates
                if key in seen_keys:
                    errors.append(f"Duplicate keyword '{key}' found")
                else:
                    seen_keys[key] = True
                    params[key] = value

            except ValueError as e:
                errors.append(f"Error parsing value for keyword '{key}': {e}")
                # Skip to next semicolon
                while current_pos < str_len and connection_str[current_pos] != ";":
                    current_pos += 1

        # Validate keywords against allowlist if validation is enabled
        if self._validate_keywords:
            unknown_keys = []
            reserved_keys = []

            for key in params.keys():
                # Check if this key can be normalized (i.e., it's known)
                normalized_key = _ConnectionStringParser.normalize_key(key)

                if normalized_key is None:
                    # Unknown keyword
                    unknown_keys.append(key)
                elif normalized_key in _RESERVED_PARAMETERS:
                    # Reserved keyword - user cannot set these
                    reserved_keys.append(key)

            if reserved_keys:
                for key in reserved_keys:
                    errors.append(
                        f"Reserved keyword '{key}' is controlled by the driver and cannot be specified by the user"
                    )

            if unknown_keys:
                for key in unknown_keys:
                    errors.append(f"Unknown keyword '{key}' is not recognized")

        # If we collected any errors, raise them all together
        if errors:
            raise ConnectionStringParseError(errors)

        return params

    def _parse_value(self, connection_str: str, start_pos: int) -> Tuple[str, int]:
        """
        Parse a parameter value from the connection string.

        Handles both simple values and braced values with escaping.

        Args:
            connection_str: The connection string
            start_pos: Starting position of the value

        Returns:
            Tuple of (parsed_value, new_position)

        Raises:
            ValueError: If braced value is not properly closed
        """
        str_len = len(connection_str)

        # Skip leading whitespace before the value
        while start_pos < str_len and connection_str[start_pos] in " \t":
            start_pos += 1

        # If we've consumed the entire string or reached a semicolon, return empty value
        if start_pos >= str_len:
            return "", start_pos

        # Determine if this is a braced value or simple value
        if connection_str[start_pos] == "{":
            return self._parse_braced_value(connection_str, start_pos)
        else:
            return self._parse_simple_value(connection_str, start_pos)

    def _parse_simple_value(self, connection_str: str, start_pos: int) -> Tuple[str, int]:
        """
        Parse a simple (non-braced) value up to the next semicolon.

        Args:
            connection_str: The connection string
            start_pos: Starting position of the value

        Returns:
            Tuple of (parsed_value, new_position)
        """
        str_len = len(connection_str)
        value_start = start_pos

        # Read characters until we hit a semicolon or end of string
        while start_pos < str_len and connection_str[start_pos] != ";":
            start_pos += 1

        # Extract the value and strip trailing whitespace
        value = connection_str[value_start:start_pos].rstrip()
        return value, start_pos

    def _parse_braced_value(self, connection_str: str, start_pos: int) -> Tuple[str, int]:
        """
        Parse a braced value with proper handling of escaped braces.

        Braced values:
        - Start with '{' and end with '}'
        - '}' inside the value is escaped as '}}'
        - '{' inside the value does not need escaping
        - Can contain semicolons and other special characters

        Args:
            connection_str: The connection string
            start_pos: Starting position (should point to opening '{')

        Returns:
            Tuple of (parsed_value, new_position)

        Raises:
            ValueError: If the braced value is not closed (missing '}')
        """
        str_len = len(connection_str)
        brace_start_pos = start_pos

        # Skip the opening '{'
        start_pos += 1

        # Build the value character by character
        value = []

        while start_pos < str_len:
            ch = connection_str[start_pos]

            if ch == "}":
                # Check if next character is also '}' (escaped brace)
                if start_pos + 1 < str_len and connection_str[start_pos + 1] == "}":
                    # Escaped right brace: '}}' → '}'
                    value.append("}")
                    start_pos += 2
                else:
                    # Single '}' means end of braced value
                    start_pos += 1
                    return "".join(value), start_pos
            else:
                # Regular character (including '{' which doesn't need escaping per ODBC spec)
                value.append(ch)
                start_pos += 1

        # Reached end without finding closing '}'
        raise ValueError(f"Unclosed braced value starting at position {brace_start_pos}")
