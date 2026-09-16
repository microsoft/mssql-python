"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
This module contains the Row class, which represents a single row of data
from a cursor fetch operation.
"""

import decimal
import uuid as _uuid
from collections.abc import Mapping
from typing import Any
from mssql_python.logging import logger


class Row:
    """
    A row of data from a cursor fetch operation. Provides both tuple-like indexing
    and attribute access to column values.

    For dict-like access, use the read-only ``row._mapping`` view (a
    ``collections.abc.Mapping`` of column name -> value). Iterating the Row itself
    (for x in row) yields values, not keys — consistent with pyodbc.Row and
    sqlite3.Row; iterate ``row._mapping`` to get column names.

    Column attribute access behavior depends on the global 'lowercase' setting:
    - When enabled: Case-insensitive attribute access
    - When disabled (default): Case-sensitive attribute access matching original column names

    Example:
        row = cursor.fetchone()
        print(row[0])                  # Access by index
        print(row.column_name)         # Access by column name
        print(dict(row._mapping))      # Convert to a plain dict
        print(row._mapping["col"])     # Access a value by column name via the mapping
        for name in row._mapping:      # Iterate column names
            print(name, row._mapping[name])
        for value in row:              # Iterating the Row yields values, not keys
            print(value)
    """

    def __init__(
        self,
        values,
        column_map,
        cursor=None,
        converter_map=None,
        uuid_str_indices=None,
        column_map_lower=None,
        column_names=None,
    ):
        """
        Initialize a Row object with values and pre-built column map.
        Args:
            values: List of values for this row
            column_map: Pre-built column name to index mapping (shared across rows)
            cursor: Optional cursor reference (for backward compatibility and lowercase access)
            converter_map: Pre-computed converter map (shared across rows for performance)
            uuid_str_indices: Tuple of column indices whose uuid.UUID values should be
                converted to str. Pre-computed once per result set when native_uuid=False.
                None means no conversion (native_uuid=True, the default).
            column_map_lower: Pre-built lowercase column map for O(1) case-insensitive
                lookups. Built once per result set in the cursor when lowercase is enabled;
                None when lowercase is off (the default). Shared across all rows.
            column_names: Canonical, order- and duplicate-preserving column names for
                the result set, snapshotted once by the cursor and shared by reference
                across all rows. Backs ``row._mapping``. None for rows built without a
                cursor snapshot; ``_mapping_keys()`` then reconstructs names from
                ``column_map``.
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

        # Convert UUID columns to str when native_uuid=False.
        # uuid_str_indices is pre-computed once at execute() time, so this is
        # O(num_uuid_columns) per row — zero cost when native_uuid=True (the default).
        if uuid_str_indices:
            self._stringify_uuids(uuid_str_indices)

        self._column_map = column_map
        self._cursor = cursor
        # Lowercase map is pre-built once per result set in the cursor and shared
        # across all rows. None when lowercase is off (the default) — zero cost.
        self._column_map_lower = column_map_lower
        # Canonical column names for this row's result set, snapshotted once by the
        # cursor (order- and duplicate-preserving) and shared by reference across every
        # row. None only for rows built without a cursor snapshot (e.g. some direct or
        # test constructions); _mapping_keys() then reconstructs names from _column_map.
        self._column_names = column_names

    def _stringify_uuids(self, indices):
        """
        Convert uuid.UUID values at the given column indices to uppercase str in-place.

        This is only called when native_uuid=False. It operates directly on
        self._values to avoid creating an extra list copy.
        """
        vals = self._values
        # If values are still the original list (no converters), we need a mutable copy
        if not isinstance(vals, list):
            vals = list(vals)
            self._values = vals

        for i in indices:
            v = vals[i]
            if v is not None and isinstance(v, _uuid.UUID):
                vals[i] = str(v).upper()

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

    def __getitem__(self, index) -> Any:
        """Allow accessing by numeric index (row[0]) or column name (row["col"])."""
        if isinstance(index, str):
            if index in self._column_map:
                return self._values[self._column_map[index]]
            # O(1) case-insensitive lookup when lowercase is enabled
            if self._column_map_lower is not None:
                idx = self._column_map_lower.get(index.lower())
                if idx is not None:
                    return self._values[idx]
            raise KeyError(f"Row has no column '{index}'")
        if isinstance(index, (int, slice)):
            return self._values[index]
        raise TypeError(
            f"Row indices must be integers, slices, or strings, not {type(index).__name__}"
        )

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

        # O(1) case-insensitive lookup when lowercase is enabled
        if self._column_map_lower is not None:
            idx = self._column_map_lower.get(name.lower())
            if idx is not None:
                return self._values[idx]

        raise AttributeError(f"Row has no attribute '{name}'")

    @property
    def _mapping(self) -> "RowMapping":
        """Read-only dict-like view (column name -> value) over this row.

        Returns a ``collections.abc.Mapping``; use ``dict(row._mapping)`` for a plain
        dict, ``row._mapping.items()`` for name/value pairs, and ``iter(row._mapping)``
        for column names. Names are order-preserving and de-duplicated (last column
        wins for a repeated name, matching subscript and attribute access).
        """
        return RowMapping(self)

    def _mapping_keys(self) -> tuple:
        """Canonical, order-preserving column names backing ``_mapping``.

        Prefers the names snapshotted once by the cursor for the result set. When a
        row was built without that snapshot, reconstructs names from ``_column_map``
        (one name per column index); returns ``()`` when neither is available.
        """
        if self._column_names is not None:
            return self._column_names
        if self._column_map:
            idx_to_name: dict = {}
            for name, idx in self._column_map.items():
                idx_to_name.setdefault(idx, name)
            return tuple(idx_to_name[i] for i in sorted(idx_to_name))
        return ()

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


class RowMapping(Mapping):
    """Read-only ``Mapping`` view over a :class:`Row` (column name -> value).

    Created via :attr:`Row._mapping`. Keys are the row's column names, order-
    preserving and de-duplicated (last column wins for a repeated name, matching
    ``row[name]`` / ``row.name``). The view reflects the row it wraps and copies
    no values.
    """

    __slots__ = ("_row",)

    def __init__(self, row: "Row") -> None:
        self._row = row

    def __getitem__(self, key: str) -> Any:
        if isinstance(key, str):
            try:
                return self._row[key]
            except KeyError:
                raise KeyError(key) from None
        raise KeyError(key)

    def __iter__(self):
        seen = set()
        for name in self._row._mapping_keys():
            if name not in seen:
                seen.add(name)
                yield name

    def __len__(self) -> int:
        return sum(1 for _ in self)

    def __repr__(self) -> str:
        return f"RowMapping({dict(self)!r})"
