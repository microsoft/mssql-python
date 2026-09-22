"""Asynchronous result fetching through mssql-py-core."""

from typing import TYPE_CHECKING, Any

from ..exceptions import OperationalError, ProgrammingError
from ..logging import logger
from ..row import Row
from .exception_translator import translate_py_core_exceptions

if TYPE_CHECKING:
    from .async_cursor import AsyncCursor

_ResultSnapshot = tuple[
    int,
    dict[str, int],
    tuple[int, ...] | None,
    dict[str, int] | None,
    tuple[str, ...] | None,
]


def _get_py_core_async_cursor(cursor: "AsyncCursor") -> Any:
    return cursor._py_core_async_cursor  # pyright: ignore[reportPrivateUsage]


def _snapshot_result(cursor: "AsyncCursor") -> _ResultSnapshot:
    return (
        cursor._result_generation,  # pyright: ignore[reportPrivateUsage]
        cursor._column_map,  # pyright: ignore[reportPrivateUsage]
        cursor._uuid_str_indices,  # pyright: ignore[reportPrivateUsage]
        cursor._column_map_lower,  # pyright: ignore[reportPrivateUsage]
        cursor._column_names,  # pyright: ignore[reportPrivateUsage]
    )


def _reconcile_failed_fetch(cursor: "AsyncCursor", operation: str, error: Exception) -> None:
    if isinstance(error, OperationalError) and str(error.__cause__).startswith(
        "Connection is busy"
    ):
        return
    logger.debug("AsyncCursor.%s: invalidating result state after fetch failure", operation)
    cursor._reset_fetch_tracking()  # pyright: ignore[reportPrivateUsage]
    cursor._clear_result_metadata()  # pyright: ignore[reportPrivateUsage]


def _wrap_row(snapshot: _ResultSnapshot, values: tuple[Any, ...]) -> Row:
    _, column_map, uuid_str_indices, column_map_lower, column_names = snapshot
    return Row(
        list(values),
        column_map,
        uuid_str_indices=uuid_str_indices,
        column_map_lower=column_map_lower,
        column_names=column_names,
    )


async def fetchone(cursor: "AsyncCursor") -> Row | None:
    """Fetch the next row through the py-core async cursor."""
    logger.debug("AsyncCursor.fetchone: starting")
    snapshot = _snapshot_result(cursor)
    try:
        with translate_py_core_exceptions():
            row = await _get_py_core_async_cursor(cursor).fetchone()
    except Exception as error:
        _reconcile_failed_fetch(cursor, "fetchone", error)
        raise
    cursor._record_fetch(  # pyright: ignore[reportPrivateUsage]
        snapshot[0], row is not None, row is None
    )
    logger.debug(
        "AsyncCursor.fetchone: completed; row_found=%s; rowcount=%d",
        row is not None,
        cursor.rowcount,
    )
    return None if row is None else _wrap_row(snapshot, row)


async def fetchmany(cursor: "AsyncCursor", size: int | None = None) -> list[Row]:
    """Fetch up to size rows, using cursor arraysize when size is omitted."""
    cursor._check_closed()  # pyright: ignore[reportPrivateUsage]
    requested_size = cursor.arraysize if size is None else size
    logger.debug("AsyncCursor.fetchmany: starting; requested_size=%s", requested_size)
    if requested_size <= 0:
        if cursor.description is None:
            raise ProgrammingError("Async operation failed", "No active result set")
        logger.debug("AsyncCursor.fetchmany: completed; row_count=0; rowcount=%d", cursor.rowcount)
        return []
    snapshot = _snapshot_result(cursor)
    try:
        with translate_py_core_exceptions():
            if size is None:
                rows = await _get_py_core_async_cursor(cursor).fetchmany()
            else:
                rows = await _get_py_core_async_cursor(cursor).fetchmany(size)
    except Exception as error:
        _reconcile_failed_fetch(cursor, "fetchmany", error)
        raise
    cursor._record_fetch(snapshot[0], len(rows), not rows)  # pyright: ignore[reportPrivateUsage]
    logger.debug(
        "AsyncCursor.fetchmany: completed; row_count=%d; rowcount=%d",
        len(rows),
        cursor.rowcount,
    )
    return [_wrap_row(snapshot, row) for row in rows]


async def fetchall(cursor: "AsyncCursor") -> list[Row]:
    """Fetch all remaining rows through the py-core async cursor."""
    logger.debug("AsyncCursor.fetchall: starting")
    snapshot = _snapshot_result(cursor)
    try:
        with translate_py_core_exceptions():
            rows = await _get_py_core_async_cursor(cursor).fetchall()
    except Exception as error:
        _reconcile_failed_fetch(cursor, "fetchall", error)
        raise
    cursor._record_fetch(snapshot[0], len(rows), not rows)  # pyright: ignore[reportPrivateUsage]
    logger.debug(
        "AsyncCursor.fetchall: completed; row_count=%d; rowcount=%d",
        len(rows),
        cursor.rowcount,
    )
    return [_wrap_row(snapshot, row) for row in rows]
