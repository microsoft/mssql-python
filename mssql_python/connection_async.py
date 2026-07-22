"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.

Async connection for the mssql-py-core backend.

Exposes :class:`AsyncConnection` and the awaitable factory
:func:`connect_async`. Under the hood it drives
``mssql_py_core.PyCoreAsyncConnection`` on a process-wide Tokio runtime.

Design notes
------------
* Constructing :class:`AsyncConnection` currently connects *synchronously*
  under the shared Tokio runtime. The method is exposed as an awaitable
  factory (:meth:`AsyncConnection.connect`, :func:`connect_async`) so
  callers can migrate to a truly-async connect path (planned) without
  changing their code.

* Concurrency contract — one physical TDS session, one in-flight batch.
  Two concurrent ``await`` calls on the same :class:`AsyncConnection` (or
  the same cursor) will *serialise* on the underlying
  ``Arc<Mutex<TdsClient>>``. For true parallelism use multiple
  :class:`AsyncConnection` instances.
"""

from __future__ import annotations

import mssql_py_core

from mssql_python.connection_string_parser import _ConnectionStringParser
from mssql_python.cursor_async import AsyncCursor
from mssql_python.exceptions import InterfaceError
from mssql_python.helpers import connstr_to_pycore_params


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


__all__ = ["AsyncConnection", "connect_async"]
