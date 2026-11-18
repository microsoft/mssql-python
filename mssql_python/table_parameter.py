"""Explicit table-valued parameter schema and immutable row snapshots."""

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from .constants import SQLTypes, ConstantsDDBC
from .vector import _check_shape, _vector_from_hint

_NUMERIC_TYPES = (ConstantsDDBC.SQL_DECIMAL.value, ConstantsDDBC.SQL_NUMERIC.value)


def _decimal_cell(value: Any, row: int, column: int) -> Any:
    if value is None or isinstance(value, Decimal):
        return value
    # Match executemany(): convert through str() and never echo the rejected value.
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        pass
    raise ValueError(
        f"Failed to convert TVP value to Decimal at row {row}, column {column} "
        f"(value type: {type(value).__name__})"
    )


@dataclass(frozen=True, init=False)
class TableValuedParameter:
    """A READONLY TVP.

    columns contains (sql_type, size, scale) hints, as for setinputsizes().
    For vector columns, size is the dimension count and scale is 0 (FLOAT32)
    or 1 (FLOAT16). Vector cells accept Vector, list, or None; lists are
    snapshotted as Vectors. DECIMAL/NUMERIC cells are snapshotted as Decimal,
    converting other values through str() as executemany() does. Explicit
    schema also supports empty/all-NULL tables.
    """

    type_name: str
    columns: tuple[tuple[int, int, int], ...]
    rows: tuple[tuple[Any, ...], ...]

    def __init__(
        self,
        type_name: str,
        rows: Iterable[Sequence[Any]],
        columns: Sequence[tuple[int, int, int]],
    ) -> None:
        if not isinstance(type_name, str) or not type_name or "\0" in type_name:
            raise ValueError("type_name must be a non-empty SQL table type name")
        parts = type_name.split(".")
        if len(parts) not in (1, 2) or any(
            not part or not all(c.isalnum() or c in "_@" for c in part) for part in parts
        ):
            raise ValueError("type_name must be an unquoted type or schema.type name")
        schema = tuple(tuple(column) for column in columns)
        if not schema:
            raise ValueError("columns must contain at least one column")
        valid = SQLTypes.get_valid_types()
        for column in schema:
            if len(column) != 3 or any(
                isinstance(item, bool) or not isinstance(item, int) for item in column
            ):
                raise ValueError("columns must contain (sql_type, size, scale) integer tuples")
            sql_type, size, scale = column
            if sql_type not in valid or size < 0 or scale < 0:
                raise ValueError("invalid TVP column type, size or scale")
            if sql_type in (
                ConstantsDDBC.SQL_SS_UDT.value,
                ConstantsDDBC.SQL_SS_VARIANT.value,
            ):
                raise ValueError("UDT and sql_variant TVP columns are not supported")
            if sql_type == ConstantsDDBC.SQL_SS_VECTOR.value:
                if scale not in (0, 1):
                    raise ValueError("vector subtype must be 0 (FLOAT32) or 1 (FLOAT16)")
                _check_shape(size, "float16" if scale else "float32")
        snapshot = []
        for row_index, row in enumerate(rows):
            values = list(row)
            if len(values) != len(schema):
                raise ValueError("TVP rows must match the number of columns")
            for index, (sql_type, size, scale) in enumerate(schema):
                if sql_type == ConstantsDDBC.SQL_SS_VECTOR.value:
                    values[index] = _vector_from_hint(values[index], size, scale)
                elif sql_type in _NUMERIC_TYPES:
                    values[index] = _decimal_cell(values[index], row_index, index)
            snapshot.append(tuple(values))
        object.__setattr__(self, "type_name", type_name)
        object.__setattr__(self, "columns", schema)
        object.__setattr__(self, "rows", tuple(snapshot))
