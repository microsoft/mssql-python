"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.

Connection string builder for mssql-python.

Reconstructs ODBC connection strings from parameter dictionaries
with proper escaping and formatting per MS-ODBCSTR specification.
"""

from typing import Dict, Optional
from mssql_python.constants import _CONNECTION_STRING_DRIVER_KEY


class _ConnectionStringBuilder:
    """
    Internal builder for ODBC connection strings. Not part of public API.

    Handles proper escaping of special characters and reconstructs
    connection strings in ODBC format.
    """

    def __init__(self, initial_params: Optional[Dict[str, str]] = None):
        """
        Initialize the builder with optional initial parameters.

        Args:
            initial_params: Dictionary of initial connection parameters
        """
        self._params: Dict[str, str] = initial_params.copy() if initial_params else {}

    def add_param(self, key: str, value: str) -> "_ConnectionStringBuilder":
        """
        Add or update a connection parameter.

        Args:
            key: Parameter name (should be normalized canonical name)
            value: Parameter value

        Returns:
            Self for method chaining
        """
        self._params[key] = str(value)
        return self

    def build(self) -> str:
        """
        Build the final connection string.

        Returns:
            ODBC-formatted connection string with proper escaping

        Note:
            - Driver parameter is placed first
            - Other parameters are sorted for consistency
            - Values are escaped if they contain special characters
        """
        parts = []

        # Build in specific order: Driver first, then others
        if _CONNECTION_STRING_DRIVER_KEY in self._params:
            parts.append(f"Driver={self._escape_value(self._params['Driver'])}")

        # Add other parameters (sorted for consistency)
        for key in sorted(self._params.keys()):
            if key == "Driver":
                continue  # Already added

            value = self._params[key]
            escaped_value = self._escape_value(value)
            parts.append(f"{key}={escaped_value}")

        # Join with semicolons
        return ";".join(parts)

    def _escape_value(self, value: str) -> str:
        """
        Escape a parameter value if it contains special characters.

        - Values containing ';', '{', '}', '=', or spaces should be braced for safety
        - '}' inside braced values is escaped as '}}'
        - '{' does not need to be escaped

        Args:
            value: Parameter value to escape

        Returns:
            Escaped value (possibly wrapped in braces)

        Examples:
            >>> builder = _ConnectionStringBuilder()
            >>> builder._escape_value("localhost")
            'localhost'
            >>> builder._escape_value("local;host")
            '{local;host}'
            >>> builder._escape_value("p}w{d")
            '{p}}w{d}'
            >>> builder._escape_value("ODBC Driver 18 for SQL Server")
            '{ODBC Driver 18 for SQL Server}'
        """
        if not value:
            return value

        # Check if value contains special characters that require bracing
        # Include spaces and = for safety, even though technically not always required
        needs_braces = any(ch in value for ch in ";{}= ")

        if needs_braces:
            # Escape closing braces by doubling them (ODBC requirement)
            # Opening braces do not need to be escaped
            escaped = value.replace("}", "}}")
            return f"{{{escaped}}}"
        else:
            return value
