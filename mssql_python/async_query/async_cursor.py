"""Public asynchronous cursor backed directly by mssql-py-core.

Warning:
    Async query execution APIs are under active development and are not intended
    for production use. Their signatures, behavior, error handling, and compatibility
    may change without notice.
"""

from collections.abc import Mapping, Sequence
from typing import Any, Optional

from ..logging import logger
from . import async_execute
from .exception_translator import translate_py_core_exceptions


class AsyncCursor:
    """Thin Python wrapper over ``mssql_py_core.PyAsyncCursor``.

    Warning:
        This preview API is under active development and is not intended for production use.
        Its signatures, behavior, error handling, and compatibility may change without notice.
    """

    def __init__(self, py_core_async_cursor: Any) -> None:
        self._py_core_async_cursor = py_core_async_cursor

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
    ) -> None:
        await async_execute.executemany(
            self,
            operation,
            seq_of_parameters,
        )

    async def fetchone(self) -> Any:
        with translate_py_core_exceptions():
            return await self._py_core_async_cursor.fetchone()

    async def fetchmany(self, size: Optional[int] = None) -> Any:
        with translate_py_core_exceptions():
            if size is None:
                return await self._py_core_async_cursor.fetchmany()
            return await self._py_core_async_cursor.fetchmany(size)

    async def fetchall(self) -> Any:
        with translate_py_core_exceptions():
            return await self._py_core_async_cursor.fetchall()

    async def nextset(self) -> bool:
        with translate_py_core_exceptions():
            return await self._py_core_async_cursor.nextset()

    async def close(self) -> None:
        logger.debug("AsyncCursor.close: starting")
        with translate_py_core_exceptions():
            await self._py_core_async_cursor.close()
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
        with translate_py_core_exceptions():
            return self._py_core_async_cursor.description

    @property
    def rowcount(self) -> int:
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
