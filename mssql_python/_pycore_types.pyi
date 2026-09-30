"""Structural contracts for the dynamically loaded Rust extension."""

from collections.abc import Callable, Iterable, Mapping, Sequence
from types import TracebackType
from typing import Any, Protocol, TypedDict

CoreContext = dict[str, str | int | Callable[[str, str, str], bytes]]

class BulkCopyResult(TypedDict):
    rows_copied: int
    batch_count: int
    elapsed_time: float
    rows_per_second: float

class CoreCursor(Protocol):
    def close(self) -> None: ...
    def bulkcopy(
        self,
        table_name: str,
        data_source: Iterable[tuple[Any, ...]],
        batch_size: int = 0,
        timeout: int = 30,
        column_mappings: list[str] | list[tuple[int, str]] | None = None,
        keep_identity: bool = False,
        check_constraints: bool = False,
        table_lock: bool = False,
        keep_nulls: bool = False,
        fire_triggers: bool = False,
        use_internal_transaction: bool = False,
        python_logger: object = None,
    ) -> BulkCopyResult: ...
    def bulkcopy_arrow(
        self,
        table_name: str,
        source: object,
        batch_size: int = 0,
        timeout: int = 30,
        column_mappings: list[str] | list[tuple[int, str]] | None = None,
        keep_identity: bool = False,
        check_constraints: bool = False,
        table_lock: bool = False,
        keep_nulls: bool = False,
        fire_triggers: bool = False,
        use_internal_transaction: bool = False,
        python_logger: object = None,
    ) -> BulkCopyResult: ...

class CoreConnection(Protocol):
    def __init__(
        self, client_context_dict: Mapping[str, object], python_logger: object = None
    ) -> None: ...
    def cursor(self) -> CoreCursor: ...
    def close(self) -> None: ...

class AsyncCoreCursor(Protocol):
    arraysize: int
    @property
    def timeout(self) -> int: ...
    @property
    def rowcount(self) -> int: ...
    @property
    def description(self) -> list[tuple[Any, ...]] | None: ...
    async def execute(
        self, operation: str, *parameters: Any, use_prepare: bool = True, reset_cursor: bool = True
    ) -> object: ...
    async def executemany(
        self,
        operation: str,
        seq_of_parameters: Sequence[Sequence[Any]] | Sequence[Mapping[str, Any]],
        *,
        use_prepare: bool = True,
    ) -> None: ...
    def setinputsizes(self, sizes: Sequence[int | tuple[int, ...]]) -> None: ...
    async def fetchone(self) -> tuple[Any, ...] | None: ...
    async def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]: ...
    async def fetchall(self) -> list[tuple[Any, ...]]: ...
    async def nextset(self) -> bool: ...
    async def close(self) -> None: ...

class AsyncCoreConnection(Protocol):
    timeout: int
    @property
    def autocommit(self) -> bool: ...
    @property
    def closed(self) -> bool: ...
    @classmethod
    async def connect(
        cls,
        client_context_dict: Mapping[str, object],
        python_logger: object = None,
        autocommit: bool = False,
    ) -> AsyncCoreConnection: ...
    def cursor(self) -> AsyncCoreCursor: ...
    async def commit(self) -> None: ...
    async def rollback(self) -> None: ...
    async def close(self) -> None: ...
    async def __aenter__(self) -> AsyncCoreConnection: ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None: ...
    def is_connected(self) -> bool: ...

class PyCoreModule(Protocol):
    __name__: str
    PyCoreConnection: type[CoreConnection]
    PyCoreCursor: type[CoreCursor]
    PyAsyncConnection: type[AsyncCoreConnection]
    PyAsyncCursor: type[AsyncCoreCursor]
