"""Explicit, transport-neutral input values for SQL Server table types."""

from collections.abc import AsyncIterable, Iterator
from datetime import date, datetime, time
from decimal import Decimal
from uuid import UUID

from .constants import ConstantsDDBC as SQL


class TableValuedParameter:
    """A table type, its ordered column declarations, and an unconsumed row source.

    Columns are ``(name, sql_type[, size_or_precision[, scale]])`` tuples.
    Variable-length columns require a size (0 means MAX); DECIMAL/NUMERIC
    require precision and scale. Temporal scale defaults to 7.
    ``type_name`` accepts an unquoted ``schema.Type`` or a type name with the
    separate ``schema`` keyword (default ``dbo``). Binding is positional.
    """

    __slots__ = ("_type_name", "_schema", "_columns", "_rows")

    def __init__(self, type_name, columns, rows, *, schema=None):
        if not isinstance(type_name, str):
            raise TypeError("TVP type_name must be a string")
        if "." in type_name:
            if schema is not None or type_name.count(".") != 1:
                raise ValueError("Specify TVP schema once: schema.Type or schema=...")
            schema, type_name = type_name.split(".")
        schema = "dbo" if schema is None else schema
        for name in (type_name, schema):
            if not isinstance(name, str):
                raise TypeError("TVP schema and type name must be strings")
            if (
                not name.strip()
                or any(char in name for char in '\x00.[]"')
                or len(name.encode("utf-16le")) > 256
            ):
                raise ValueError("TVP names must be unquoted identifiers of 1–128 UTF-16 units")
        if not isinstance(columns, (list, tuple)) or not columns:
            raise TypeError("TVP columns must be a non-empty list or tuple")
        normalized = []
        for column in columns:
            if not isinstance(column, tuple) or not 2 <= len(column) <= 4:
                raise TypeError("TVP columns use (name, sql_type[, size_or_precision[, scale]])")
            name, sql_type = column[:2]
            if not isinstance(name, str) or not name:
                raise TypeError("TVP column names must be non-empty strings")
            if type(sql_type) is not int:
                raise TypeError(f"TVP column {name}: sql_type must be an SQL_* integer constant")
            sql_type = {9: 91, 10: 92, 11: 93}.get(sql_type, sql_type)
            if sql_type not in self._types:
                raise ValueError(f"TVP column {name}: unsupported SQL type {sql_type}")
            variable = sql_type in self._variable
            numeric = sql_type in (SQL.SQL_DECIMAL.value, SQL.SQL_NUMERIC.value)
            temporal = sql_type in (92, 93, -154, -155)
            if (variable and len(column) < 3) or (numeric and len(column) != 4):
                raise ValueError(f"TVP column {name}: explicit size or precision/scale is required")
            size = column[2] if len(column) >= 3 else self._sizes.get(sql_type, 0)
            scale = column[3] if len(column) == 4 else (7 if temporal else 0)
            if type(size) is not int or type(scale) is not int:
                raise TypeError(f"TVP column {name}: size and scale must be integers")
            if not 0 <= size <= 2147483647 or scale < 0:
                raise ValueError(f"TVP column {name}: invalid size or scale")
            if numeric and not (1 <= size <= 38 and 0 <= scale <= size):
                raise ValueError(
                    f"TVP column {name}: require 1 <= precision <= 38 and scale <= precision"
                )
            if temporal and scale > 7:
                raise ValueError(f"TVP column {name}: temporal scale must be between 0 and 7")
            if not numeric and not temporal and scale:
                raise ValueError(
                    f"TVP column {name}: scale is only valid for numeric/temporal types"
                )
            if (
                sql_type in (SQL.SQL_CHAR.value, SQL.SQL_WCHAR.value, SQL.SQL_BINARY.value)
                and not size
            ):
                raise ValueError(f"TVP column {name}: fixed-length columns require a positive size")
            normalized.append((name, sql_type, size, scale))
        if not isinstance(rows, (list, tuple, Iterator, AsyncIterable)):
            raise TypeError("TVP rows must be a list, tuple, or iterator; use [] for an empty TVP")
        self._type_name, self._schema = type_name, schema
        self._columns, self._rows = tuple(normalized), rows

    @property
    def type_name(self):
        return self._type_name

    @property
    def schema(self):
        return self._schema

    @property
    def columns(self):
        return self._columns

    @property
    def rows(self):
        return self._rows

    # The existing setinputsizes map owns SQL-to-C mappings. TVP time keeps
    # fractional seconds, and decimals use the scalar exact-text conversion.
    def _odbc_columns(self):
        from .cursor import Cursor

        mapping = Cursor._get_sql_to_c_type_map()
        return [
            (
                {-1: 12, -10: -9, -4: -3}.get(sql_type, sql_type),
                SQL.SQL_C_WCHAR.value if sql_type in (2, 3, 92, -154) else mapping[sql_type],
                size,
                scale,
            )
            for _, sql_type, size, scale in self.columns
        ]

    def _validate_row(self, row, index):
        if not isinstance(row, (list, tuple)):
            raise TypeError(f"TVP row {index} must be a list or tuple")
        if len(row) != len(self.columns):
            raise ValueError(f"TVP row {index} has {len(row)} cells; expected {len(self.columns)}")
        for (name, sql_type, _, _), value in zip(self.columns, row):
            if value is not None and not isinstance(value, self._types[sql_type]):
                raise TypeError(f"TVP row {index}, column {name}: incompatible Python value")
            if value is None:
                continue
            if sql_type in self._integer_ranges:
                lower, upper = self._integer_ranges[sql_type]
                if not lower <= value <= upper:
                    raise ValueError(f"TVP row {index}, column {name}: integer out of range")
            if sql_type in (2, 3) and not value.is_finite():
                raise ValueError(f"TVP row {index}, column {name}: non-finite Decimal")
        return row

    _variable = frozenset((1, 12, -1, -8, -9, -10, -2, -3, -4))
    _types = {
        **dict.fromkeys((1, 12, -1, -8, -9, -10), str),
        **dict.fromkeys((-2, -3, -4), (bytes, bytearray)),
        **dict.fromkeys((-6, 5, 4, -5), int),
        **dict.fromkeys((6, 7, 8), float),
        **dict.fromkeys((2, 3), Decimal),
        -7: bool,
        91: date,
        92: time,
        -154: time,
        93: datetime,
        -155: datetime,
        -11: UUID,
    }
    _sizes = {
        -7: 1,
        -6: 3,
        5: 5,
        4: 10,
        -5: 19,
        6: 15,
        7: 7,
        8: 15,
        91: 10,
        92: 16,
        -154: 16,
        93: 27,
        -155: 34,
        -11: 36,
    }
    _integer_ranges = {
        -6: (0, 255),
        5: (-(2**15), 2**15 - 1),
        4: (-(2**31), 2**31 - 1),
        -5: (-(2**63), 2**63 - 1),
    }
