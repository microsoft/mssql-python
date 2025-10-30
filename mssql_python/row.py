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
        values: List[Any],
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
        column_map: Optional[Dict[str, int]] = None,
        converter_map: Optional[List[Optional[Any]]] = None,
        settings_snapshot: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Initialize a Row object with values and description.

        Args:
            values: List of values for this row
            cursor: The cursor object
            description: The cursor description containing column metadata
            column_map: Optional pre-built column map (for optimization)
            converter_map: Pre-computed converter map (shared across rows for performance)
            settings_snapshot: Settings snapshot from cursor to ensure consistency
        """
        self._cursor = cursor
        self._description = description

        # Store pre-built column map
        self._column_map = column_map or {}
        self._settings = settings_snapshot or {
            "lowercase": get_settings().lowercase,
            "native_uuid": get_settings().native_uuid,
        }

        # Apply output converters using pre-built converter map if available
        if converter_map:
            processed_values = self._apply_output_converters(values, converter_map)
        else:
            # Fallback to no conversion
            processed_values = list(values)

        # Process UUID values using the snapshotted setting
        self._values = self._process_uuid_values(processed_values, description)

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
        processed_values = list(values)  # Create a copy to modify

        # Process only UUID columns when native_uuid is True
        if native_uuid:
            # If we have pre-identified UUID columns
            if uuid_indices is not None:
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
            # Fallback to scanning all columns if indices weren't pre-identified
            else:
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
        # When native_uuid is False, convert UUID objects to strings
        else:
            for i, value in enumerate(processed_values):
                if isinstance(value, uuid.UUID):
                    processed_values[i] = str(value)

        return processed_values

    def _apply_output_converters(self, values, converter_map):
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
                        value_bytes = value.encode('utf-16-le')
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
        parts = []
        for value in self:
            if isinstance(value, decimal.Decimal):
                try:
                    # Apply custom decimal separator for display with safety checks
                    # Local import to avoid circular dependency
                    from mssql_python import getDecimalSeparator
                    sep = getDecimalSeparator()
                    if sep and sep != "." and value is not None:
                        s = str(value)
                        if "." in s:
                            s = s.replace(".", sep)
                        parts.append(s)
                    else:
                        parts.append(str(value))
                except Exception:
                    parts.append(str(value))
            else:
                parts.append(repr(value))

        return "(" + ", ".join(parts) + ")"

    def __repr__(self) -> str:
        """Return a detailed string representation for debugging"""
        return repr(tuple(self._values))
