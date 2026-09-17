"""Asynchronous result fetching through mssql-py-core."""

import uuid
from typing import TYPE_CHECKING, Any

from ..helpers import get_settings
from ..logging import logger
from ..row import Row
from .exception_translator import translate_py_core_exceptions

if TYPE_CHECKING:
    from .async_cursor import AsyncCursor


def _get_py_core_async_cursor(cursor: "AsyncCursor") -> Any:
    return cursor._py_core_async_cursor  # pyright: ignore[reportPrivateUsage]


def _wrap_row(cursor: "AsyncCursor", values: tuple[Any, ...]) -> Row:
    description = cursor.description or ()
    column_map = {column[0]: index for index, column in enumerate(description)}
    column_map_lower = (
        {name.lower(): index for name, index in column_map.items()}
        if get_settings().lowercase
        else None
    )
    uuid_str_indices = (
        tuple(index for index, column in enumerate(description) if column[1] is uuid.UUID)
        if not get_settings().native_uuid
        else None
    )
    return Row(
        values,
        column_map,
        uuid_str_indices=uuid_str_indices,
        column_map_lower=column_map_lower,
    )


async def fetchone(cursor: "AsyncCursor") -> Row | None:
    """Fetch the next row through the py-core async cursor."""
    logger.debug("AsyncCursor.fetchone: starting")
    with translate_py_core_exceptions():
        row = await _get_py_core_async_cursor(cursor).fetchone()
    cursor._record_fetch(row is not None, row is None)  # pyright: ignore[reportPrivateUsage]
    logger.debug(
        "AsyncCursor.fetchone: completed; row_found=%s; rowcount=%d",
        row is not None,
        cursor.rowcount,
    )
    return None if row is None else _wrap_row(cursor, row)


async def fetchmany(cursor: "AsyncCursor", size: int | None = None) -> list[Row]:
    """Fetch up to size rows, using cursor arraysize when size is omitted."""
    requested_size = cursor.arraysize if size is None else size
    logger.debug("AsyncCursor.fetchmany: starting; requested_size=%s", requested_size)
    with translate_py_core_exceptions():
        if size is None:
            rows = await _get_py_core_async_cursor(cursor).fetchmany()
        else:
            rows = await _get_py_core_async_cursor(cursor).fetchmany(size)
    if size is None or size > 0:
        cursor._record_fetch(len(rows), not rows)  # pyright: ignore[reportPrivateUsage]
    logger.debug(
        "AsyncCursor.fetchmany: completed; row_count=%d; rowcount=%d",
        len(rows),
        cursor.rowcount,
    )
    return [_wrap_row(cursor, row) for row in rows]


async def fetchall(cursor: "AsyncCursor") -> list[Row]:
    """Fetch all remaining rows through the py-core async cursor."""
    logger.debug("AsyncCursor.fetchall: starting")
    with translate_py_core_exceptions():
        rows = await _get_py_core_async_cursor(cursor).fetchall()
    cursor._record_fetch(len(rows), not rows)  # pyright: ignore[reportPrivateUsage]
    logger.debug(
        "AsyncCursor.fetchall: completed; row_count=%d; rowcount=%d",
        len(rows),
        cursor.rowcount,
    )
    return [_wrap_row(cursor, row) for row in rows]
