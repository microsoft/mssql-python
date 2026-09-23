"""Public asynchronous connection backed directly by mssql-py-core.

Warning:
    Async query execution APIs are under active development and are not intended
    for production use. Their signatures, behavior, error handling, and compatibility
    may change without notice.
"""

from typing import Any, Optional

from ..logging import logger
from ._native import load_py_core
from ._connection_context import build_async_connection_context
from .async_cursor import AsyncCursor
from .exception_translator import (
    DataError,
    DatabaseError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Warning,
    translate_py_core_exceptions,
)


class AsyncConnection:
    """Thin Python wrapper over ``mssql_py_core.PyAsyncConnection``.

    Warning:
        This preview API is under active development and is not intended for production use.
        Its signatures, behavior, error handling, and compatibility may change without notice.
    """

    Warning = Warning
    Error = Error
    InterfaceError = InterfaceError
    DatabaseError = DatabaseError
    DataError = DataError
    OperationalError = OperationalError
    IntegrityError = IntegrityError
    InternalError = InternalError
    ProgrammingError = ProgrammingError
    NotSupportedError = NotSupportedError

    def __init__(self, py_core_async_connection: Any) -> None:
        self._py_core_async_connection = py_core_async_connection

    @classmethod
    async def connect(
        cls,
        connection_str: str = "",
        autocommit: bool = False,
        timeout: int = 0,
        python_logger: Optional[Any] = None,
    ) -> "AsyncConnection":
        """Establish an asynchronous connection from an ODBC connection string."""
        logger_bridge = python_logger
        if logger_bridge is None and logger.is_debug_enabled:
            logger_bridge = logger
        logger.debug(
            "AsyncConnection.connect: starting; autocommit=%s; logger_source=%s",
            autocommit,
            (
                "custom"
                if python_logger is not None
                else "mssql_python" if logger_bridge is not None else "disabled"
            ),
        )
        with translate_py_core_exceptions():
            client_context_dict = build_async_connection_context(connection_str, timeout)
            py_core = load_py_core()
            py_core_async_connection = await py_core.PyAsyncConnection.connect(
                client_context_dict,
                python_logger=logger_bridge,
                autocommit=autocommit,
            )
        logger.debug("AsyncConnection.connect: connected")
        return cls(py_core_async_connection)

    def cursor(self) -> AsyncCursor:
        """Create a public asynchronous cursor sharing this connection."""
        with translate_py_core_exceptions():
            py_core_async_cursor = self._py_core_async_connection.cursor()
        logger.debug("AsyncConnection.cursor: cursor created")
        return AsyncCursor(py_core_async_cursor, self)

    async def commit(self) -> None:
        """Commit the active transaction, if any."""
        logger.debug("AsyncConnection.commit: starting")
        with translate_py_core_exceptions():
            await self._py_core_async_connection.commit()
        logger.debug("AsyncConnection.commit: completed")

    async def rollback(self) -> None:
        """Roll back the active transaction, if any."""
        logger.debug("AsyncConnection.rollback: starting")
        with translate_py_core_exceptions():
            await self._py_core_async_connection.rollback()
        logger.debug("AsyncConnection.rollback: completed")

    async def close(self) -> None:
        """Close the py-core async connection."""
        logger.debug("AsyncConnection.close: starting")
        with translate_py_core_exceptions():
            await self._py_core_async_connection.close()
        logger.debug("AsyncConnection.close: completed")

    async def __aenter__(self) -> "AsyncConnection":
        logger.debug("AsyncConnection.__aenter__: entering context")
        with translate_py_core_exceptions():
            await self._py_core_async_connection.__aenter__()
        logger.debug("AsyncConnection.__aenter__: context entered")
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> Any:
        logger.debug(
            "AsyncConnection.__aexit__: exiting context; block_error=%s",
            exc_type is not None,
        )
        with translate_py_core_exceptions():
            result = await self._py_core_async_connection.__aexit__(exc_type, exc_value, traceback)
        logger.debug("AsyncConnection.__aexit__: context exited")
        return result

    @property
    def timeout(self) -> int:
        """Default query timeout inherited by subsequently created cursors."""
        with translate_py_core_exceptions():
            return self._py_core_async_connection.timeout

    @timeout.setter
    def timeout(self, value: int) -> None:
        with translate_py_core_exceptions():
            self._py_core_async_connection.timeout = value
        logger.debug("AsyncConnection.timeout: updated")

    @property
    def autocommit(self) -> bool:
        """Whether the connection was opened in autocommit mode."""
        with translate_py_core_exceptions():
            return self._py_core_async_connection.autocommit

    @property
    def closed(self) -> bool:
        """Whether close has been initiated on the py-core async connection."""
        with translate_py_core_exceptions():
            return self._py_core_async_connection.closed

    def is_connected(self) -> bool:
        """Return whether the py-core async connection remains open."""
        with translate_py_core_exceptions():
            return self._py_core_async_connection.is_connected()

    def __repr__(self) -> str:
        state = "closed" if self.closed else "connected"
        return f"AsyncConnection({state})"
