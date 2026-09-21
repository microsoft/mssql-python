"""Asynchronous result fetching through mssql-py-core."""

from typing import TYPE_CHECKING, Any

from ..logging import logger
from ..row import Row
from .exception_translator import translate_py_core_exceptions

if TYPE_CHECKING:
    from .async_cursor import AsyncCursor


def _get_py_core_async_cursor(cursor: "AsyncCursor") -> Any:
    return cursor._py_core_async_cursor  # pyright: ignore[reportPrivateUsage]


def _wrap_row(cursor: "AsyncCursor", values: tuple[Any, ...]) -> Row:
    return Row(
        list(values),
        cursor._column_map,  # pyright: ignore[reportPrivateUsage]
        uuid_str_indices=cursor._uuid_str_indices,  # pyright: ignore[reportPrivateUsage]
        column_map_lower=cursor._column_map_lower,  # pyright: ignore[reportPrivateUsage]
        column_names=cursor._column_names,  # pyright: ignore[reportPrivateUsage]
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
    cursor._check_closed()  # pyright: ignore[reportPrivateUsage]
    requested_size = cursor.arraysize if size is None else size
    logger.debug("AsyncCursor.fetchmany: starting; requested_size=%s", requested_size)
    if requested_size <= 0:
        logger.debug("AsyncCursor.fetchmany: completed; row_count=0; rowcount=%d", cursor.rowcount)
        return []
    with translate_py_core_exceptions():
        if size is None:
            rows = await _get_py_core_async_cursor(cursor).fetchmany()
        else:
            rows = await _get_py_core_async_cursor(cursor).fetchmany(size)
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
