"""Public asynchronous cursor backed directly by mssql-py-core."""

from typing import Any, Optional

from ..logging import logger
from .exception_translator import translate_py_core_exceptions


class AsyncCursor:
    """Thin Python wrapper over ``mssql_py_core.PyAsyncCursor``."""

    def __init__(self, native_cursor: Any) -> None:
        self._native_cursor = native_cursor

    async def execute(
        self,
        operation: str,
        *parameters: Any,
        use_prepare: bool = True,
        reset_cursor: bool = True,
    ) -> "AsyncCursor":
        logger.debug("AsyncCursor.execute: starting")
        with translate_py_core_exceptions():
            await self._native_cursor.execute(
                operation,
                *parameters,
                use_prepare=use_prepare,
                reset_cursor=reset_cursor,
            )
        logger.debug("AsyncCursor.execute: completed")
        return self

    async def executemany(
        self,
        operation: str,
        seq_of_parameters: Any,
        *,
        use_prepare: bool = True,
    ) -> "AsyncCursor":
        logger.debug("AsyncCursor.executemany: starting")
        with translate_py_core_exceptions():
            await self._native_cursor.executemany(
                operation,
                seq_of_parameters,
                use_prepare=use_prepare,
            )
        logger.debug("AsyncCursor.executemany: completed")
        return self

    async def fetchone(self) -> Any:
        with translate_py_core_exceptions():
            return await self._native_cursor.fetchone()

    async def fetchmany(self, size: Optional[int] = None) -> Any:
        with translate_py_core_exceptions():
            if size is None:
                return await self._native_cursor.fetchmany()
            return await self._native_cursor.fetchmany(size)

    async def fetchall(self) -> Any:
        with translate_py_core_exceptions():
            return await self._native_cursor.fetchall()

    async def nextset(self) -> bool:
        with translate_py_core_exceptions():
            return await self._native_cursor.nextset()

    async def close(self) -> None:
        logger.debug("AsyncCursor.close: starting")
        with translate_py_core_exceptions():
            await self._native_cursor.close()
        logger.debug("AsyncCursor.close: completed")

    def setinputsizes(self, sizes: Any) -> None:
        with translate_py_core_exceptions():
            self._native_cursor.setinputsizes(sizes)

    @property
    def timeout(self) -> int:
        with translate_py_core_exceptions():
            return self._native_cursor.timeout

    @property
    def description(self) -> Any:
        with translate_py_core_exceptions():
            return self._native_cursor.description

    @property
    def rowcount(self) -> int:
        with translate_py_core_exceptions():
            return self._native_cursor.rowcount

    @property
    def arraysize(self) -> int:
        with translate_py_core_exceptions():
            return self._native_cursor.arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        with translate_py_core_exceptions():
            self._native_cursor.arraysize = value
