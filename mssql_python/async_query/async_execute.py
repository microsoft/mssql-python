"""Asynchronous statement execution through mssql-py-core."""

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

from ..logging import logger
from ..row import Row
from .exception_translator import translate_py_core_exceptions

if TYPE_CHECKING:
    from .async_cursor import AsyncCursor


def _get_py_core_async_cursor(cursor: "AsyncCursor") -> Any:
    return cursor._py_core_async_cursor  # pyright: ignore[reportPrivateUsage]


def _native_result_state(cursor: "AsyncCursor") -> tuple[Any, int]:
    py_core_cursor = _get_py_core_async_cursor(cursor)
    return py_core_cursor.description, py_core_cursor.rowcount


def _reconcile_failed_execution(
    cursor: "AsyncCursor", previous_native_state: tuple[Any, int]
) -> None:
    if _native_result_state(cursor) == previous_native_state:
        return
    cursor._reset_fetch_tracking()  # pyright: ignore[reportPrivateUsage]
    cursor._clear_result_metadata()  # pyright: ignore[reportPrivateUsage]
    cursor._initialize_result_metadata()  # pyright: ignore[reportPrivateUsage]


async def execute(
    cursor: "AsyncCursor",
    operation: str,
    *parameters: Any,
    use_prepare: bool = True,
    reset_cursor: bool = True,
) -> "AsyncCursor":
    """Execute a statement using the py-core async cursor."""
    if len(parameters) == 1 and isinstance(parameters[0], (tuple, list, Row)):
        parameters = tuple(parameters[0])

    previous_native_state = _native_result_state(cursor)
    logger.debug(
        "AsyncCursor.execute: starting; param_count=%d; use_prepare=%s; reset_cursor=%s",
        len(parameters),
        use_prepare,
        reset_cursor,
    )
    try:
        with translate_py_core_exceptions():
            await _get_py_core_async_cursor(cursor).execute(
                operation,
                *parameters,
                use_prepare=use_prepare,
                reset_cursor=reset_cursor,
            )
    except Exception:
        _reconcile_failed_execution(cursor, previous_native_state)
        raise
    cursor._reset_fetch_tracking()  # pyright: ignore[reportPrivateUsage]
    cursor._clear_result_metadata()  # pyright: ignore[reportPrivateUsage]
    cursor._initialize_result_metadata()  # pyright: ignore[reportPrivateUsage]
    description = cursor.description
    logger.debug(
        "AsyncCursor.execute: completed; rowcount=%d; column_count=%d; has_result_set=%s",
        cursor.rowcount,
        len(description) if description is not None else 0,
        description is not None,
    )
    return cursor


async def executemany(
    cursor: "AsyncCursor",
    operation: str,
    seq_of_parameters: Sequence[Sequence[Any]] | Sequence[Mapping[str, Any]],
    *,
    use_prepare: bool = True,
) -> None:
    """Execute a statement for every parameter row using the py-core async cursor."""
    cursor._check_closed()  # pyright: ignore[reportPrivateUsage]
    batch_count = len(seq_of_parameters)
    previous_native_state = _native_result_state(cursor)
    logger.debug(
        "AsyncCursor.executemany: starting; batch_count=%d; use_prepare=%s",
        batch_count,
        use_prepare,
    )
    try:
        with translate_py_core_exceptions():
            await _get_py_core_async_cursor(cursor).executemany(
                operation,
                seq_of_parameters,
                use_prepare=use_prepare,
            )
    except Exception:
        _reconcile_failed_execution(cursor, previous_native_state)
        raise
    cursor._reset_fetch_tracking()  # pyright: ignore[reportPrivateUsage]
    cursor._clear_result_metadata()  # pyright: ignore[reportPrivateUsage]
    cursor._initialize_result_metadata()  # pyright: ignore[reportPrivateUsage]
    logger.debug("AsyncCursor.executemany: completed; rowcount=%d", cursor.rowcount)
