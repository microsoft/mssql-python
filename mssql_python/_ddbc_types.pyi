"""Native declarations; kept separate so mypy also checks the Python loader."""

from collections.abc import Callable, Mapping, Sequence
from typing import Any, NoReturn, TypedDict, overload

from .row import Row

__all__ = [
    "ARCHITECTURE",
    "Connection",
    "DDBCSQLCheckError",
    "DDBCSQLColumns",
    "DDBCSQLDescribeCol",
    "DDBCSQLExecDirect",
    "DDBCSQLExecute",
    "DDBCSQLFetch",
    "DDBCSQLFetchAll",
    "DDBCSQLFetchArrowBatch",
    "DDBCSQLFetchMany",
    "DDBCSQLFetchOne",
    "DDBCSQLFetchScroll",
    "DDBCSQLForeignKeys",
    "DDBCSQLFreeHandle",
    "DDBCSQLGetAllDiagRecords",
    "DDBCSQLGetData",
    "DDBCSQLGetTypeInfo",
    "DDBCSQLMoreResults",
    "DDBCSQLNumResultCols",
    "DDBCSQLPrimaryKeys",
    "DDBCSQLProcedures",
    "DDBCSQLResetStmt",
    "DDBCSQLRowCount",
    "DDBCSQLSetStmtAttr",
    "DDBCSQLSpecialColumns",
    "DDBCSQLStatistics",
    "DDBCSQLTables",
    "DDBCSetDecimalSeparator",
    "ErrorInfo",
    "GetDriverPathCpp",
    "NumericData",
    "ParamInfo",
    "SQLExecuteMany",
    "SQL_NO_TOTAL",
    "SqlHandle",
    "ThrowStdException",
    "close_pooling",
    "construct_rows",
    "disable_pooling",
    "enable_pooling",
    "update_log_level",
]

class EncodingSettings(TypedDict):
    encoding: str
    ctype: int

class ColumnMetadata(TypedDict):
    ColumnName: str
    DataType: int
    ColumnSize: int
    DecimalDigits: int
    Nullable: int

class InfoResult(TypedDict):
    data: bytes
    length: int
    info_type: int

class SqlHandle:
    def free(self) -> None: ...
    def _close_cursor(self) -> None: ...
    def _cancel(self) -> None: ...

class Connection:
    def __init__(
        self,
        conn_str: str,
        use_pool: bool,
        attrs_before: dict[int, int | str | bytes] = ...,
        pool_key: str = "",
        token_factory: Callable[[], tuple[dict[int, int | str | bytes], int | None]] | None = None,
    ) -> None: ...
    def alloc_statement_handle(self) -> SqlHandle: ...
    def close(self, transaction_already_rolled_back: bool = False) -> None: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...
    def get_autocommit(self) -> bool: ...
    def set_autocommit(self, value: bool) -> None: ...
    def set_attr(self, attribute: int, value: int | str | bytes | bytearray) -> None: ...
    def get_info(self, info_type: int) -> InfoResult | None: ...

class ParamInfo:
    inputOutputType: int
    paramCType: int
    paramSQLType: int
    columnSize: int
    decimalDigits: int
    strLenOrInd: int
    dataPtr: object
    isDAE: bool
    def __init__(self) -> None: ...

class NumericData:
    precision: int
    scale: int
    sign: int
    val: str | bytes
    @overload
    def __init__(self) -> None: ...
    @overload
    def __init__(self, precision: int, scale: int, sign: int, val: str | bytes) -> None: ...

class ErrorInfo:
    sqlState: str
    ddbcErrorMsg: str

ARCHITECTURE: str
SQL_NO_TOTAL: int

def _get_odbc_driver_path(base_dir: str, provider: str) -> str: ...
def _set_odbc_provider(provider: str) -> None: ...
def GetDriverPathCpp(base_dir: str) -> str: ...
def ThrowStdException(message: str) -> NoReturn: ...
def enable_pooling(max_size: int, idle_timeout: int) -> None: ...
def disable_pooling() -> None: ...
def close_pooling() -> None: ...
def update_log_level(level: int) -> None: ...
def DDBCSetDecimalSeparator(separator: str) -> None: ...
def DDBCSQLCheckError(handle_type: int, handle: SqlHandle | None, ret: int) -> ErrorInfo: ...
def DDBCSQLGetAllDiagRecords(handle: SqlHandle | None) -> list[tuple[str, str]]: ...
def DDBCSQLExecDirect(handle: SqlHandle | None, query: str) -> int: ...
def DDBCSQLExecute(
    statementHandle: SqlHandle | None,
    query: str,
    params: list[Any],
    inputSizes: list[tuple[int, int, int, int]] | None,
    isStmtPrepared: list[bool],
    usePrepare: bool,
    encodingSettings: EncodingSettings,
) -> int: ...
def SQLExecuteMany(
    statementHandle: SqlHandle | None,
    query: str,
    columnwise_params: list[list[Any]],
    paramInfos: Sequence[ParamInfo],
    paramSetSize: int,
    encodingSettings: EncodingSettings,
) -> int: ...
def DDBCSQLRowCount(handle: SqlHandle | None) -> int: ...
def DDBCSQLFetch(handle: SqlHandle | None) -> int: ...
def DDBCSQLNumResultCols(
    statementHandle: SqlHandle | None, messages: list[tuple[str, str]] | None = None
) -> int: ...
def DDBCSQLDescribeCol(
    StatementHandle: SqlHandle | None,
    ColumnMetadata: list[ColumnMetadata],
    messages: list[tuple[str, str]] | None = None,
) -> int: ...
def DDBCSQLGetData(
    StatementHandle: SqlHandle | None,
    colCount: int,
    row: list[Any],
    charEncoding: str,
    wcharEncoding: str,
    charCtype: int,
    messages: list[tuple[str, str]] | None = None,
) -> int: ...
def DDBCSQLMoreResults(handle: SqlHandle | None) -> int: ...
def DDBCSQLFetchOne(
    StatementHandle: SqlHandle | None,
    row: list[Any],
    charEncoding: str = "utf-16le",
    wcharEncoding: str = "utf-16le",
    charCtype: int = -8,
    messages: list[tuple[str, str]] | None = None,
) -> int: ...
def DDBCSQLFetchMany(
    StatementHandle: SqlHandle | None,
    rows: list[list[Any]],
    fetchSize: int,
    charEncoding: str = "utf-16le",
    wcharEncoding: str = "utf-16le",
    charCtype: int = -8,
    messages: list[tuple[str, str]] | None = None,
) -> int: ...
def DDBCSQLFetchAll(
    StatementHandle: SqlHandle | None,
    rows: list[list[Any]],
    charEncoding: str = "utf-16le",
    wcharEncoding: str = "utf-16le",
    charCtype: int = -8,
    messages: list[tuple[str, str]] | None = None,
) -> int: ...
def DDBCSQLFetchArrowBatch(
    StatementHandle: SqlHandle | None,
    capsules: list[object],
    arrowBatchSize: int,
    charCtype: int,
    messages: list[tuple[str, str]] | None = None,
) -> int: ...
def DDBCSQLFreeHandle(handle_type: int, handle: SqlHandle | None) -> int: ...
def DDBCSQLResetStmt(handle: SqlHandle | None) -> int: ...
def DDBCSQLSetStmtAttr(handle: SqlHandle | None, attribute: int, value: object) -> int: ...
def DDBCSQLTables(
    StatementHandle: SqlHandle | None,
    catalog: str = "",
    schema: str = "",
    table: str = "",
    tableType: str = "",
) -> int: ...
def DDBCSQLFetchScroll(
    handle: SqlHandle | None, orientation: int, offset: int, row: list[Any]
) -> int: ...
def DDBCSQLGetTypeInfo(StatementHandle: SqlHandle | None, DataType: int) -> int: ...
def DDBCSQLProcedures(
    handle: SqlHandle | None, catalog: str | None, schema: str | None, procedure: str | None
) -> int: ...
def DDBCSQLForeignKeys(
    handle: SqlHandle | None,
    pk_catalog: str | None,
    pk_schema: str | None,
    pk_table: str | None,
    fk_catalog: str | None,
    fk_schema: str | None,
    fk_table: str | None,
) -> int: ...
def DDBCSQLPrimaryKeys(
    handle: SqlHandle | None, catalog: str | None, schema: str | None, table: str
) -> int: ...
def DDBCSQLSpecialColumns(
    handle: SqlHandle | None,
    identifier: int,
    catalog: str | None,
    schema: str | None,
    table: str,
    scope: int,
    nullable: int,
) -> int: ...
def DDBCSQLStatistics(
    handle: SqlHandle | None,
    catalog: str | None,
    schema: str | None,
    table: str,
    unique: int,
    accuracy: int,
) -> int: ...
def DDBCSQLColumns(
    handle: SqlHandle | None,
    catalog: str | None,
    schema: str | None,
    table: str | None,
    column: str | None,
) -> int: ...
def construct_rows(
    rows_data: list[list[Any]],
    row_class: type[Row],
    column_map: Mapping[str, int] | None,
    cursor: object,
    column_map_lower: Mapping[str, int] | None = None,
    column_names: tuple[str, ...] | None = None,
) -> list[Row]: ...
