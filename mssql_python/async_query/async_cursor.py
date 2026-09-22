"""Public asynchronous cursor backed directly by mssql-py-core.

Warning:
    Async query execution APIs are under active development and are not intended
    for production use. Their signatures, behavior, error handling, and compatibility
    may change without notice.
"""

import asyncio
from collections.abc import Mapping, Sequence
from contextlib import asynccontextmanager
from typing import Any, Optional
import uuid

from ..exceptions import OperationalError
from ..helpers import get_settings
from ..logging import logger
from ..row import Row
from . import async_execute, async_fetch
from .exception_translator import translate_py_core_exceptions


class AsyncCursor:
    """Thin Python wrapper over ``mssql_py_core.PyAsyncCursor``.

    Warning:
        This preview API is under active development and is not intended for production use.
        Its signatures, behavior, error handling, and compatibility may change without notice.
    """

    def __init__(self, py_core_async_cursor: Any, connection: Any = None) -> None:
        self._py_core_async_cursor = py_core_async_cursor
        self._connection = connection
        self._closed = False
        self._result_transition_lock = asyncio.Lock()
        self._result_ready = asyncio.Event()
        self._result_ready.set()
        self._result_generation = 0
        self._fetched_row_count = 0
        self._fetch_rowcount: int | None = None
        self._description: list[tuple[Any, ...]] | None = None
        self._column_map: dict[str, int] = {}
        self._column_map_lower: dict[str, int] | None = None
        self._column_names: tuple[str, ...] | None = None
        self._uuid_str_indices: tuple[int, ...] | None = None

    def _clear_result_metadata(self) -> None:
        self._result_generation += 1
        self._description = None
        self._column_map = {}
        self._column_map_lower = None
        self._column_names = None
        self._uuid_str_indices = None

    def _initialize_result_metadata(self) -> None:
        with translate_py_core_exceptions():
            description = self._py_core_async_cursor.description
        if description is None:
            self._clear_result_metadata()
            return

        settings = get_settings()
        self._description = [
            ((column[0].lower() if settings.lowercase else column[0]), *column[1:])
            for column in description
        ]
        self._column_names = tuple(column[0] for column in self._description)
        self._column_map = {column[0]: index for index, column in enumerate(self._description)}
        self._column_map_lower = (
            {name.lower(): index for name, index in self._column_map.items()}
            if settings.lowercase
            else None
        )
        self._uuid_str_indices = (
            tuple(index for index, column in enumerate(self._description) if column[1] is uuid.UUID)
            if not settings.native_uuid
            else None
        )

    def _reset_fetch_tracking(self) -> None:
        self._fetched_row_count = 0
        self._fetch_rowcount = None

    @asynccontextmanager
    async def _result_transition(self):
        async with self._result_transition_lock:
            self._result_ready.clear()
            try:
                yield
            finally:
                self._result_ready.set()

    async def _wait_for_result_publication(self) -> None:
        await self._result_ready.wait()

    def _reconcile_failed_result_operation(self, operation: str, error: BaseException) -> None:
        if isinstance(error, OperationalError) and str(error.__cause__).startswith(
            "Connection is busy"
        ):
            return
        self._reset_fetch_tracking()
        self._clear_result_metadata()
        try:
            self._initialize_result_metadata()
        except Exception as error:
            logger.debug("AsyncCursor.%s: metadata recovery failed: %s", operation, error)

    def _check_closed(self) -> None:
        if self._closed or (self._connection is not None and self._connection.closed):
            message = "Cursor is closed" if self._closed else "Connection is closed"
            with translate_py_core_exceptions():
                raise RuntimeError(message)

    def _record_fetch(self, generation: int, count: int, exhausted: bool) -> None:
        if generation != self._result_generation:
            return
        if count:
            self._fetched_row_count += count
            self._fetch_rowcount = self._fetched_row_count
        elif exhausted and self._fetched_row_count == 0:
            self._fetch_rowcount = 0

    async def execute(
        self,
        operation: str,
        *parameters: Any,
        use_prepare: bool = True,
        reset_cursor: bool = True,
    ) -> "AsyncCursor":
        async with self._result_transition():
            return await async_execute.execute(
                self,
                operation,
                *parameters,
                use_prepare=use_prepare,
                reset_cursor=reset_cursor,
            )

    async def executemany(
        self,
        operation: str,
        seq_of_parameters: Sequence[Sequence[Any]] | Sequence[Mapping[str, Any]],
        *,
        use_prepare: bool = True,
    ) -> None:
        async with self._result_transition():
            await async_execute.executemany(
                self,
                operation,
                seq_of_parameters,
                use_prepare=use_prepare,
            )

    async def fetchone(self) -> Row | None:
        return await async_fetch.fetchone(self)

    async def fetchmany(self, size: Optional[int] = None) -> list[Row]:
        return await async_fetch.fetchmany(self, size)

    async def fetchall(self) -> list[Row]:
        return await async_fetch.fetchall(self)

    async def nextset(self) -> bool:
        async with self._result_transition():
            try:
                with translate_py_core_exceptions():
                    has_next = await self._py_core_async_cursor.nextset()
            except (Exception, asyncio.CancelledError) as error:
                self._reconcile_failed_result_operation("nextset", error)
                raise
            self._reset_fetch_tracking()
            self._clear_result_metadata()
            if has_next:
                self._initialize_result_metadata()
            return has_next

    async def close(self) -> None:
        logger.debug("AsyncCursor.close: starting")
        async with self._result_transition():
            with translate_py_core_exceptions():
                await self._py_core_async_cursor.close()
            self._closed = True
            self._reset_fetch_tracking()
            self._clear_result_metadata()
        logger.debug("AsyncCursor.close: completed")

    def setinputsizes(self, sizes: Any) -> None:
        with translate_py_core_exceptions():
            self._py_core_async_cursor.setinputsizes(sizes)

    @property
    def timeout(self) -> int:
        with translate_py_core_exceptions():
            return self._py_core_async_cursor.timeout

    @property
    def description(self) -> Any:
        return self._description

    @property
    def rowcount(self) -> int:
        if self._fetch_rowcount is not None:
            return self._fetch_rowcount
        with translate_py_core_exceptions():
            return self._py_core_async_cursor.rowcount

    @property
    def arraysize(self) -> int:
        with translate_py_core_exceptions():
            return self._py_core_async_cursor.arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        with translate_py_core_exceptions():
            self._py_core_async_cursor.arraysize = value
