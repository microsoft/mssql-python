"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
This module contains the Row class, which represents a single row of data
from a cursor fetch operation.
"""

import decimal
from typing import Any
from mssql_python.helpers import get_settings
from mssql_python.logging import logger


class Row:
    """
    A row of data from a cursor fetch operation. Provides both tuple-like indexing
    and attribute access to column values.

    Column attribute access behavior depends on the global 'lowercase' setting:
    - When enabled: Case-insensitive attribute access
    - When disabled (default): Case-sensitive attribute access matching original column names

    Example:
        row = cursor.fetchone()
        print(row[0])           # Access by index
        print(row.column_name)  # Access by column name (case sensitivity varies)
    """

    def __init__(self, values, column_map, cursor=None, converter_map=None):
        """
        Initialize a Row object with values and pre-built column map.
        Args:
            values: List of values for this row
            column_map: Pre-built column name to index mapping (shared across rows)
            cursor: Optional cursor reference (for backward compatibility and lowercase access)
            converter_map: Pre-computed converter map (shared across rows for performance)
        """
        # Apply output converters if available using pre-computed converter map
        if converter_map:
            self._values = self._apply_output_converters_optimized(values, converter_map)
        elif (
            cursor
            and hasattr(cursor.connection, "_output_converters")
            and cursor.connection._output_converters
        ):
            # Fallback to original method for backward compatibility
            self._values = self._apply_output_converters(values, cursor)
        else:
            self._values = values

        self._column_map = column_map
        self._cursor = cursor

    def _apply_output_converters(self, values, cursor):
        """
        Apply output converters to raw values.

        Args:
            values: Raw values from the database
            cursor: Cursor object with connection and description

        Returns:
            List of converted values
        """
        if not cursor.description:
            return values

        converted_values = list(values)

        for i, (value, desc) in enumerate(zip(values, cursor.description)):
            if desc is None or value is None:
                continue

            # Get SQL type from description
            sql_type = desc[1]  # type_code is at index 1 in description tuple

            # Try to get a converter for this type
            converter = cursor.connection.get_output_converter(sql_type)

            # If no converter found for the SQL type but the value is a string or bytes,
            # try the WVARCHAR converter as a fallback
            if converter is None and isinstance(value, (str, bytes)):
                from mssql_python.constants import ConstantsDDBC

                converter = cursor.connection.get_output_converter(ConstantsDDBC.SQL_WVARCHAR.value)

            # If we found a converter, apply it
            if converter:
                try:
                    # If value is already a Python type (str, int, etc.),
                    # we need to convert it to bytes for our converters
                    if isinstance(value, str):
                        # Encode as UTF-16LE for string values (SQL_WVARCHAR format)
                        value_bytes = value.encode("utf-16-le")
                        converted_values[i] = converter(value_bytes)
                    else:
                        converted_values[i] = converter(value)
                except Exception:
                    logger.debug("Exception occurred in output converter", exc_info=True)
                    # If conversion fails, keep the original value
                    pass

        return converted_values

    def _apply_output_converters_optimized(self, values, converter_map):
        """
        Apply output converters using pre-computed converter map for optimal performance.

        Args:
            values: Raw values from the database
            converter_map: Pre-computed list of converters (one per column, None if no converter)

        Returns:
            List of converted values
        """
        converted_values = list(values)

        for i, (value, converter) in enumerate(zip(values, converter_map)):
            if converter and value is not None:
                try:
                    if isinstance(value, str):
                        value_bytes = value.encode("utf-16-le")
                        converted_values[i] = converter(value_bytes)
                    else:
                        converted_values[i] = converter(value)
                except Exception:
                    pass

        return converted_values

    def __getitem__(self, index: int) -> Any:
        """Allow accessing by numeric index: row[0]"""
        return self._values[index]

    def __getattr__(self, name: str) -> Any:
        """
        Allow accessing by column name as attribute: row.column_name

        Note: Case sensitivity depends on the global 'lowercase' setting:
        - When lowercase=True: Column names are stored in lowercase, enabling
          case-insensitive attribute access (e.g., row.NAME, row.name, row.Name all work).
        - When lowercase=False (default): Column names preserve original casing,
          requiring exact case matching for attribute access.
        """
        # Handle lowercase attribute access - if lowercase is enabled,
        # try to match attribute names case-insensitively
        if name in self._column_map:
            return self._values[self._column_map[name]]

        # If lowercase is enabled on the cursor, try case-insensitive lookup
        if hasattr(self._cursor, "lowercase") and self._cursor.lowercase:
            name_lower = name.lower()
            for col_name in self._column_map:
                if col_name.lower() == name_lower:
                    return self._values[self._column_map[col_name]]

        raise AttributeError(f"Row has no attribute '{name}'")

    def __eq__(self, other: Any) -> bool:
        """
        Support comparison with lists for test compatibility.
        This is the key change needed to fix the tests.
        """
        if isinstance(other, list):
            return self._values == other
        if isinstance(other, Row):
            return self._values == other._values
        return super().__eq__(other)

    def __len__(self) -> int:
        """Return the number of values in the row"""
        return len(self._values)

    def __iter__(self) -> Any:
        """Allow iteration through values"""
        return iter(self._values)

    def __str__(self) -> str:
        """Return string representation of the row"""
        # Local import to avoid circular dependency
        from mssql_python import getDecimalSeparator

        parts = []
        for value in self:
            if isinstance(value, decimal.Decimal):
                # Apply custom decimal separator for display
                sep = getDecimalSeparator()
                if sep != "." and value is not None:
                    s = str(value)
                    if "." in s:
                        s = s.replace(".", sep)
                    parts.append(s)
                else:
                    parts.append(str(value))
            else:
                parts.append(repr(value))

        return "(" + ", ".join(parts) + ")"

    def __repr__(self) -> str:
        """Return a detailed string representation for debugging"""
        return repr(tuple(self._values))
