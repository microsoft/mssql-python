"""Asynchronous statement execution through mssql-py-core."""

import asyncio
from collections.abc import Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any

from ..exceptions import OperationalError, ProgrammingError
from ..logging import logger
from ..row import Row
from .exception_translator import translate_py_core_exceptions

if TYPE_CHECKING:
    from .async_cursor import _AsyncCursor  # pyright: ignore[reportPrivateUsage]


def _get_py_core_async_cursor(cursor: "_AsyncCursor") -> Any:
    return cursor._py_core_async_cursor  # pyright: ignore[reportPrivateUsage]


def _is_non_mutating_rejection(error: BaseException) -> bool:
    """Return whether py-core guarantees rejection before result-state mutation."""
    if isinstance(error, OperationalError) and str(error.__cause__).startswith(
        "Connection is busy"
    ):
        return True
    return isinstance(error, (TypeError, KeyError)) or (
        isinstance(error, ProgrammingError) and isinstance(error.__cause__, TypeError)
    )


def _reconcile_failed_execution(cursor: "_AsyncCursor", error: BaseException) -> None:
    if _is_non_mutating_rejection(error):
        return
    cursor._reset_fetch_tracking()  # pyright: ignore[reportPrivateUsage]
    cursor._clear_result_metadata()  # pyright: ignore[reportPrivateUsage]
    try:
        cursor._initialize_result_metadata()  # pyright: ignore[reportPrivateUsage]
    except Exception as error:
        logger.debug("Async execution metadata recovery failed: %s", error)


async def execute(
    cursor: "_AsyncCursor",
    operation: str,
    *parameters: Any,
    use_prepare: bool = True,
    reset_cursor: bool = True,
) -> "_AsyncCursor":
    """Execute a statement using the py-core async cursor."""
    if len(parameters) == 1 and isinstance(parameters[0], (tuple, list, Row)):
        parameters = tuple(parameters[0])

    logger.debug(
        "AsyncCursor.execute: starting; param_count=%d; use_prepare=%s; reset_cursor=%s",
        len(parameters),
        use_prepare,
        reset_cursor,
    )
    with translate_py_core_exceptions():
        execute_awaitable = _get_py_core_async_cursor(cursor).execute(
            operation,
            *parameters,
            use_prepare=use_prepare,
            reset_cursor=reset_cursor,
        )
    try:
        with translate_py_core_exceptions():
            await execute_awaitable
    except (Exception, asyncio.CancelledError) as error:
        _reconcile_failed_execution(cursor, error)
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
    cursor: "_AsyncCursor",
    operation: str,
    seq_of_parameters: Iterable[Sequence[Any] | Mapping[str, Any]],
    *,
    use_prepare: bool = True,
) -> None:
    """Execute parameter rows from a synchronous iterable through py-core.

    Py-core consumes and validates the iterable before dispatch; this is not
    streaming execution. Asynchronous iterables are not supported.
    """
    cursor._check_closed()  # pyright: ignore[reportPrivateUsage]
    iteration_failed = False

    def parameter_rows():
        nonlocal iteration_failed
        try:
            yield from seq_of_parameters
        except BaseException:
            iteration_failed = True
            raise

    logger.debug(
        "AsyncCursor.executemany: starting; use_prepare=%s",
        use_prepare,
    )
    with translate_py_core_exceptions():
        executemany_awaitable = _get_py_core_async_cursor(cursor).executemany(
            operation,
            parameter_rows(),
            use_prepare=use_prepare,
        )
    try:
        with translate_py_core_exceptions():
            await executemany_awaitable
    except (Exception, asyncio.CancelledError) as error:
        if not iteration_failed:
            _reconcile_failed_execution(cursor, error)
        raise
    cursor._reset_fetch_tracking()  # pyright: ignore[reportPrivateUsage]
    cursor._clear_result_metadata()  # pyright: ignore[reportPrivateUsage]
    cursor._initialize_result_metadata()  # pyright: ignore[reportPrivateUsage]
    logger.debug("AsyncCursor.executemany: completed; rowcount=%d", cursor.rowcount)
