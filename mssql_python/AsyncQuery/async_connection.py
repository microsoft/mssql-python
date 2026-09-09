"""Public asynchronous connection backed directly by mssql-py-core."""

from typing import Any, Optional

from ..logging import logger
from ._native import load_py_core
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
    """Thin Python wrapper over ``mssql_py_core.PyAsyncConnection``."""

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

    def __init__(self, native_connection: Any) -> None:
        self._native_connection = native_connection

    @classmethod
    async def connect(
        cls,
        client_context_dict: dict,
        python_logger: Optional[Any] = None,
        autocommit: bool = False,
    ) -> "AsyncConnection":
        """Establish a direct asynchronous TDS connection through py-core."""
        logger.debug(
            "AsyncConnection.connect: starting; autocommit=%s; custom_logger=%s",
            autocommit,
            python_logger is not None,
        )
        with translate_py_core_exceptions():
            py_core = load_py_core()
            native_connection = await py_core.PyAsyncConnection.connect(
                client_context_dict,
                python_logger=python_logger,
                autocommit=autocommit,
            )
        logger.debug("AsyncConnection.connect: connected")
        return cls(native_connection)

    def cursor(self) -> Any:
        """Create a native asynchronous cursor sharing this connection."""
        with translate_py_core_exceptions():
            cursor = self._native_connection.cursor()
        logger.debug("AsyncConnection.cursor: cursor created")
        return cursor

    async def commit(self) -> None:
        """Commit the active transaction, if any."""
        logger.debug("AsyncConnection.commit: starting")
        with translate_py_core_exceptions():
            await self._native_connection.commit()
        logger.debug("AsyncConnection.commit: completed")

    async def rollback(self) -> None:
        """Roll back the active transaction, if any."""
        logger.debug("AsyncConnection.rollback: starting")
        with translate_py_core_exceptions():
            await self._native_connection.rollback()
        logger.debug("AsyncConnection.rollback: completed")

    async def close(self) -> None:
        """Close the native connection."""
        logger.debug("AsyncConnection.close: starting")
        with translate_py_core_exceptions():
            await self._native_connection.close()
        logger.debug("AsyncConnection.close: completed")

    async def __aenter__(self) -> "AsyncConnection":
        logger.debug("AsyncConnection.__aenter__: entering context")
        with translate_py_core_exceptions():
            await self._native_connection.__aenter__()
        logger.debug("AsyncConnection.__aenter__: context entered")
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> Any:
        logger.debug(
            "AsyncConnection.__aexit__: exiting context; block_error=%s",
            exc_type is not None,
        )
        with translate_py_core_exceptions():
            result = await self._native_connection.__aexit__(exc_type, exc_value, traceback)
        logger.debug("AsyncConnection.__aexit__: context exited")
        return result

    @property
    def timeout(self) -> int:
        """Default query timeout inherited by subsequently created cursors."""
        with translate_py_core_exceptions():
            return self._native_connection.timeout

    @timeout.setter
    def timeout(self, value: int) -> None:
        with translate_py_core_exceptions():
            self._native_connection.timeout = value
        logger.debug("AsyncConnection.timeout: updated")

    @property
    def autocommit(self) -> bool:
        """Whether the connection was opened in autocommit mode."""
        with translate_py_core_exceptions():
            return self._native_connection.autocommit

    @property
    def closed(self) -> bool:
        """Whether close has been initiated on the native connection."""
        with translate_py_core_exceptions():
            return self._native_connection.closed

    def is_connected(self) -> bool:
        """Return whether the native connection remains open."""
        with translate_py_core_exceptions():
            return self._native_connection.is_connected()

    def __repr__(self) -> str:
        state = "closed" if self.closed else "connected"
        return f"AsyncConnection({state})"
