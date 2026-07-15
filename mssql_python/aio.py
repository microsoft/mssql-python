"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.

Async surface for the mssql-py-core backend.

Exposes :class:`AsyncConnection` and :class:`AsyncCursor` alongside the module
factory :func:`connect_async`. These are thin wrappers over the Rust
``mssql_py_core.PyCoreAsyncConnection`` / ``PyCoreAsyncCursor`` classes.

POC scope (Phase 4) — only two DB operations are exposed asynchronously:

* :meth:`AsyncCursor.execute` — an awaitable form of ``execute``.
* :meth:`AsyncCursor.fetchone` — an awaitable form of ``fetchone``.

Everything else (parameterised execution, ``fetchmany``/``fetchall``, async
connect, async pool, bulkcopy) is intentionally out of scope for the POC.

Design notes
------------
* The Rust ``execute_async`` / ``fetchone_async`` methods return a *PyO3
  awaitable*, not a Python coroutine. ``asyncio.create_task(f)`` requires a
  coroutine, so every wrapper method here is defined as ``async def foo()``
  and simply ``return await self._core.foo_async(...)``. This lets callers
  freely use ``create_task``, ``gather``, ``wait_for``, etc.

* Cancellation is *cooperative*. Call :meth:`AsyncCursor.cancel` from another
  task to send a TDS attention to the server; the awaited coroutine will then
  resume with an exception mapped from the Rust ``OperationCancelledError``.

* Concurrency contract — one physical TDS session, one in-flight batch. Two
  concurrent ``await`` calls on the same :class:`AsyncConnection` (or the
  same cursor) will *serialise* on the underlying ``Arc<Mutex<TdsClient>>``.
  For true parallelism use multiple :class:`AsyncConnection` instances.
"""

from __future__ import annotations

from typing import Any, Optional

import mssql_py_core

from mssql_python.connection_string_parser import _ConnectionStringParser
from mssql_python.exceptions import InterfaceError
from mssql_python.helpers import connstr_to_pycore_params


class AsyncCursor:
    """Awaitable cursor.

    Not thread-safe — bind one to each asyncio task. Instances are created
    via :meth:`AsyncConnection.cursor`; construct directly only in tests.
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


class AsyncConnection:
    """Awaitable connection.

    Constructing this object synchronously connects to the server on the
    shared Tokio runtime. See :func:`connect_async` for a factory that
    matches the awaitable idiom.
    """

    __slots__ = ("_core", "_closed")

    def __init__(self, connection_str: str) -> None:
        if not connection_str:
            raise ValueError("connection_str must be a non-empty str")
        parser = _ConnectionStringParser(validate_keywords=False)
        params = parser._parse(connection_str)
        pycore_ctx = connstr_to_pycore_params(params)
        self._core = mssql_py_core.PyCoreAsyncConnection(pycore_ctx)
        self._closed = False

    @classmethod
    async def connect(cls, connection_str: str) -> "AsyncConnection":
        """Awaitable factory. Currently connects synchronously under the hood.

        The method is ``async`` so callers can migrate to a truly-async
        connect path (planned) without changing their code.
        """
        return cls(connection_str)

    def cursor(self) -> AsyncCursor:
        if self._closed:
            raise InterfaceError(
                driver_error="Connection is closed",
                ddbc_error="AsyncConnection.cursor called after close",
            )
        return AsyncCursor(self._core.cursor())

    async def close(self) -> None:
        if not self._closed:
            self._core.close()
            self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed

    async def __aenter__(self) -> "AsyncConnection":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()


async def connect_async(connection_str: str) -> AsyncConnection:
    """Awaitable factory for an :class:`AsyncConnection`.

    Example::

        async with await connect_async(conn_str) as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1")
                print(await cur.fetchone())
    """
    return await AsyncConnection.connect(connection_str)


__all__ = ["AsyncConnection", "AsyncCursor", "connect_async"]
