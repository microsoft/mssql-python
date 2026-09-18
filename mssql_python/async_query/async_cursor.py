"""Public asynchronous cursor backed directly by mssql-py-core.

Warning:
    Async query execution APIs are under active development and are not intended
    for production use. Their signatures, behavior, error handling, and compatibility
    may change without notice.
"""

from collections.abc import Mapping, Sequence
from typing import Any, Optional
import uuid

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
        self._fetched_row_count = 0
        self._fetch_rowcount: int | None = None
        self._description: list[tuple[Any, ...]] | None = None
        self._column_map: dict[str, int] = {}
        self._column_map_lower: dict[str, int] | None = None
        self._uuid_str_indices: tuple[int, ...] | None = None

    def _clear_result_metadata(self) -> None:
        self._description = None
        self._column_map = {}
        self._column_map_lower = None
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

    def _check_closed(self) -> None:
        if self._closed or (self._connection is not None and self._connection.closed):
            message = "Cursor is closed" if self._closed else "Connection is closed"
            with translate_py_core_exceptions():
                raise RuntimeError(message)

    def _record_fetch(self, count: int, exhausted: bool) -> None:
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
        self._reset_fetch_tracking()
        self._clear_result_metadata()
        with translate_py_core_exceptions():
            has_next = await self._py_core_async_cursor.nextset()
        if has_next:
            self._initialize_result_metadata()
        return has_next

    async def close(self) -> None:
        logger.debug("AsyncCursor.close: starting")
        with translate_py_core_exceptions():
            await self._py_core_async_cursor.close()
        self._closed = True
        self._reset_fetch_tracking()
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
