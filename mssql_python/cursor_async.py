"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.

Async cursor for the mssql-py-core backend.

Wraps the Rust ``mssql_py_core.PyCoreAsyncCursor`` type in a small Python
class so callers get proper coroutine methods (``async def foo(...)``) that
work with ``asyncio.create_task`` / ``gather`` / ``wait_for``.

The Rust ``execute_async`` / ``fetchone_async`` methods return a PyO3
awaitable, not a coroutine — hence the ``async def foo(...): return await
self._core.foo_async(...)`` pattern used here.

POC scope: only ``execute`` and ``fetchone`` are async. Parameters are not
yet supported.
"""

from __future__ import annotations

from typing import Any, Optional

from mssql_python.exceptions import InterfaceError


class AsyncCursor:
    """Awaitable cursor.

    Not thread-safe — bind one to each asyncio task. Instances are created
    via :meth:`mssql_python.connection_async.AsyncConnection.cursor`;
    construct directly only in tests.
    """

    __slots__ = ("_core", "_closed")

    def __init__(self, core_cursor: Any) -> None:
        self._core = core_cursor
        self._closed = False

    async def execute(
        self,
        operation: str,
        timeout_sec: int = 30,
    ) -> "AsyncCursor":
        """Execute a T-SQL batch. POC: parameters are not supported."""
        if self._closed:
            raise InterfaceError(
                driver_error="Cursor is closed",
                ddbc_error="AsyncCursor.execute called after close",
            )
        if not isinstance(operation, str) or not operation:
            raise ValueError("operation must be a non-empty str")
        await self._core.execute_async(operation, timeout_sec)
        return self

    async def fetchone(self) -> Optional[tuple]:
        """Return the next row as a tuple, or None when the result set is exhausted."""
        if self._closed:
            raise InterfaceError(
                driver_error="Cursor is closed",
                ddbc_error="AsyncCursor.fetchone called after close",
            )
        return await self._core.fetchone_async()

    def cancel(self) -> None:
        """Dispatch a TDS attention to abort the currently in-flight operation.

        Non-blocking. The awaiting coroutine resumes with an exception when
        the server acknowledges the cancel.
        """
        if not self._closed:
            self._core.cancel()

    async def close(self) -> None:
        """Idempotent close. Cancels any in-flight operation."""
        if not self._closed:
            self._core.close()
            self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed

    async def __aenter__(self) -> "AsyncCursor":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()


__all__ = ["AsyncCursor"]
