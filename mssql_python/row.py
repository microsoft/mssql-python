"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
This module contains the Row class, which represents a single row of data 
from a cursor fetch operation.
"""
import decimal
import uuid
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

from mssql_python.constants import ConstantsDDBC
from mssql_python.helpers import get_settings

if TYPE_CHECKING:
    from mssql_python.cursor import Cursor


class Row:
    """
    A row of data from a cursor fetch operation.
    """

    def __init__(
        self,
        cursor: "Cursor",
        description: List[
            Tuple[
                str,
                Any,
                Optional[int],
                Optional[int],
                Optional[int],
                Optional[int],
                Optional[bool],
            ]
        ],
        values: List[Any],
        column_map: Optional[Dict[str, int]] = None,
        settings_snapshot: Optional[Dict[str, Any]] = None,
        converter_map: Optional[Dict[int, Dict[str, Any]]] = None,
    ) -> None:
        """
        Initialize a Row object with values and description.

        Args:
            cursor: The cursor object
            description: The cursor description containing column metadata
            values: List of values for this row
            column_map: Optional pre-built column map (for optimization)
            settings_snapshot: Settings snapshot from cursor to ensure consistency
            converter_map: Optional pre-built converter map (for optimization)
        """
        self._cursor = cursor
        self._description = description
        self._converter_map = converter_map

        # Use pre-computed settings and column map for performance
        self._settings = settings_snapshot or {
            "lowercase": get_settings().lowercase,
            "native_uuid": get_settings().native_uuid,
        }
        self._column_map = column_map or self._build_column_map(description)

        # First make a mutable copy of values
        processed_values = list(values)

        # Apply output converters if available (use shared converter map for efficiency)
        if converter_map:
            processed_values = self._apply_output_converters_optimized(processed_values)
        elif (
            hasattr(cursor.connection, "_output_converters")
            and cursor.connection._output_converters
        ):
            processed_values = self._apply_output_converters(processed_values)

        # Process UUID values using the snapshotted setting
        self._values = self._process_uuid_values(processed_values, description)

    def _build_column_map(self, description):
        """Build column name to index mapping (fallback when not pre-computed)."""
        column_map = {}
        for i, col_desc in enumerate(description):
            if col_desc:  # Ensure column description exists
                col_name = col_desc[0]  # Name is first item in description tuple
                if self._settings.get("lowercase"):
                    col_name = col_name.lower()
                column_map[col_name] = i
        return column_map

    def _process_uuid_values(
        self,
        values: List[Any],
        description: List[
            Tuple[
                str,
                Any,
                Optional[int],
                Optional[int],
                Optional[int],
                Optional[int],
                Optional[bool],
            ]
        ],
    ) -> List[Any]:
        """
        Convert string UUIDs to uuid.UUID objects if native_uuid setting is True,
        or ensure UUIDs are returned as strings if False.
        """

        # Use the snapshot setting for native_uuid
        native_uuid = self._settings.get("native_uuid")

        # Early return if no conversion needed
        if not native_uuid and not any(isinstance(v, uuid.UUID) for v in values):
            return values

        # Get pre-identified UUID indices from cursor if available
        uuid_indices = getattr(self._cursor, "_uuid_indices", None)
        
        # Fast path: use pre-computed UUID indices
        if uuid_indices is not None and native_uuid:
            processed_values = list(values)  # Create a copy to modify
            for i in uuid_indices:
                if i < len(processed_values) and processed_values[i] is not None:
                    value = processed_values[i]
                    if isinstance(value, str):
                        try:
                            # Remove braces if present
                            clean_value = value.strip("{}")
                            processed_values[i] = uuid.UUID(clean_value)
                        except (ValueError, AttributeError):
                            pass  # Keep original if conversion fails
        # Slow path: scan all columns (fallback)
        elif native_uuid:
            processed_values = list(values)  # Create a copy to modify
            for i, value in enumerate(processed_values):
                if value is None:
                    continue

                if i < len(description) and description[i]:
                    # Check SQL type for UNIQUEIDENTIFIER (-11)
                    sql_type = description[i][1]
                    if sql_type == -11:  # SQL_GUID
                        if isinstance(value, str):
                            try:
                                processed_values[i] = uuid.UUID(value.strip("{}"))
                            except (ValueError, AttributeError):
                                pass
        else:
            processed_values = list(values)  # Create a copy to modify

        # When native_uuid is False, convert UUID objects to strings  
        if not native_uuid:
            for i, value in enumerate(processed_values):
                if isinstance(value, uuid.UUID):
                    processed_values[i] = str(value)

        return processed_values

    def _apply_output_converters_optimized(self, values: List[Any]) -> List[Any]:
        """
        Apply pre-computed output converters using shared converter map for performance.
        
        Args:
            values: Raw values from the database
            
        Returns:
            List of converted values
        """
        if not self._converter_map:
            return values
            
        converted_values = list(values)
        
        # Map SQL type codes to appropriate byte sizes (cached for performance)
        int_size_map = {
            -6: 1,   # SQL_TINYINT
            5: 2,    # SQL_SMALLINT
            4: 4,    # SQL_INTEGER
            -5: 8,   # SQL_BIGINT
        }
        
        # Apply converters only to columns that have them pre-computed
        for col_idx, converter_info in self._converter_map.items():
            if col_idx >= len(values) or values[col_idx] is None:
                continue
                
            converter = converter_info['converter']
            sql_type = converter_info['sql_type']
            value = values[col_idx]
            
            try:
                # Handle different value types efficiently
                if isinstance(value, str):
                    # Encode as UTF-16LE for string values (SQL_WVARCHAR format)
                    value_bytes = value.encode("utf-16-le")
                    converted_values[col_idx] = converter(value_bytes)
                elif isinstance(value, int):
                    # Get appropriate byte size for this integer type
                    byte_size = int_size_map.get(sql_type, 8)
                    try:
                        # Use signed=True to properly handle negative values
                        value_bytes = value.to_bytes(
                            byte_size, byteorder="little", signed=True
                        )
                        converted_values[col_idx] = converter(value_bytes)
                    except OverflowError:
                        # Keep original value on overflow
                        pass
                else:
                    # Pass the value directly for other types
                    converted_values[col_idx] = converter(value)
            except Exception:
                # If conversion fails, keep the original value
                pass
        
        return converted_values

    def _apply_output_converters(self, values: List[Any]) -> List[Any]:
        """
        Apply output converters to raw values.

        Args:
            values: Raw values from the database

        Returns:
            List of converted values
        """
        if not self._description:
            return values

        converted_values = list(values)

        # Map SQL type codes to appropriate byte sizes
        int_size_map = {
            # SQL_TINYINT
            ConstantsDDBC.SQL_TINYINT.value: 1,
            # SQL_SMALLINT
            ConstantsDDBC.SQL_SMALLINT.value: 2,
            # SQL_INTEGER
            ConstantsDDBC.SQL_INTEGER.value: 4,
            # SQL_BIGINT
            ConstantsDDBC.SQL_BIGINT.value: 8,
        }

        for i, (value, desc) in enumerate(zip(values, self._description)):
            if desc is None or value is None:
                continue

            # Get SQL type from description
            sql_type = desc[1]  # type_code is at index 1 in description tuple

            # Try to get a converter for this type
            converter = self._cursor.connection.get_output_converter(sql_type)

            # If no converter found for the SQL type but the value is a string or bytes,
            # try the WVARCHAR converter as a fallback
            if converter is None and isinstance(value, (str, bytes)):
                converter = self._cursor.connection.get_output_converter(
                    ConstantsDDBC.SQL_WVARCHAR.value
                )

            # If we found a converter, apply it
            if converter:
                try:
                    # If value is already a Python type (str, int, etc.),
                    # we need to handle it appropriately
                    if isinstance(value, str):
                        # Encode as UTF-16LE for string values (SQL_WVARCHAR format)
                        value_bytes = value.encode("utf-16-le")
                        converted_values[i] = converter(value_bytes)
                    elif isinstance(value, int):
                        # Get appropriate byte size for this integer type
                        byte_size = int_size_map.get(sql_type, 8)
                        try:
                            # Use signed=True to properly handle negative values
                            value_bytes = value.to_bytes(
                                byte_size, byteorder="little", signed=True
                            )
                            converted_values[i] = converter(value_bytes)
                        except OverflowError:
                            # Log specific overflow error with details to help diagnose the issue
                            if hasattr(self._cursor, "log"):
                                self._cursor.log(
                                    "warning",
                                    f"Integer overflow: value {value} does not fit in "
                                    f"{byte_size} bytes for SQL type {sql_type}",
                                )
                            # Keep the original value in this case
                    else:
                        # Pass the value directly for other types
                        converted_values[i] = converter(value)
                except Exception as e:
                    # Log the exception for debugging without leaking sensitive data
                    if hasattr(self._cursor, "log"):
                        self._cursor.log(
                            "warning",
                            f"Exception in output converter: {type(e).__name__} "
                            f"for SQL type {sql_type}",
                        )
                    # If conversion fails, keep the original value

        return converted_values

    def __getitem__(self, index: int) -> Any:
        """Allow accessing by numeric index: row[0]"""
        return self._values[index]

    def __getattr__(self, name: str) -> Any:
        """
        Allow accessing by column name as attribute: row.column_name
        """
        # _column_map should already be set in __init__, but check to be safe
        if not hasattr(self, "_column_map"):
            self._column_map = {}

        # Try direct lookup first
        if name in self._column_map:
            return self._values[self._column_map[name]]

        # Use the snapshot lowercase setting instead of global
        if self._settings.get("lowercase"):
            # If lowercase is enabled, try case-insensitive lookup
            name_lower = name.lower()
            if name_lower in self._column_map:
                return self._values[self._column_map[name_lower]]

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