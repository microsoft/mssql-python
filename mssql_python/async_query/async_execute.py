"""Asynchronous statement execution through mssql-py-core."""

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

from ..logging import logger
from .exception_translator import translate_py_core_exceptions

if TYPE_CHECKING:
    from .async_cursor import AsyncCursor


def _get_py_core_async_cursor(cursor: "AsyncCursor") -> Any:
    return cursor._py_core_async_cursor  # pyright: ignore[reportPrivateUsage]


async def execute(
    cursor: "AsyncCursor",
    operation: str,
    *parameters: Any,
    use_prepare: bool = True,
    reset_cursor: bool = True,
) -> "AsyncCursor":
    """Execute a statement using the py-core async cursor."""
    cursor._reset_fetch_tracking()  # pyright: ignore[reportPrivateUsage]
    if len(parameters) == 1 and isinstance(parameters[0], (tuple, list)):
        parameters = tuple(parameters[0])

    logger.debug(
        "AsyncCursor.execute: starting; param_count=%d; use_prepare=%s; reset_cursor=%s",
        len(parameters),
        use_prepare,
        reset_cursor,
    )
    with translate_py_core_exceptions():
        await _get_py_core_async_cursor(cursor).execute(
            operation,
            *parameters,
            use_prepare=use_prepare,
            reset_cursor=reset_cursor,
        )
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
) -> None:
    """Execute a statement for every parameter row using the py-core async cursor."""
    batch_count = len(seq_of_parameters)
    cursor._reset_fetch_tracking()  # pyright: ignore[reportPrivateUsage]
    logger.debug("AsyncCursor.executemany: starting; batch_count=%d", batch_count)
    with translate_py_core_exceptions():
        await _get_py_core_async_cursor(cursor).executemany(
            operation,
            seq_of_parameters,
        )
    logger.debug("AsyncCursor.executemany: completed; rowcount=%d", cursor.rowcount)
