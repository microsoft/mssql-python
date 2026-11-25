"""Type-check only mssql_python source and stubs without database operations."""

from pathlib import Path
import subprocess
import sys

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE_DIR = "mssql_python"
pytestmark = pytest.mark.typing


def _check_typing(tmp_path: Path, *targets: str) -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "mypy",
            "--config-file=",
            "--strict",
            "--explicit-package-bases",
            "--exclude",
            r"(^|/)build/",
            "--no-incremental",
            "--cache-dir",
            str(tmp_path / "mypy"),
            *targets,
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=300,
    )
    if result.returncode != 0:
        pytest.fail(
            f"mypy failed for {', '.join(targets)} (exit {result.returncode}):\n"
            f"{result.stdout}\n{result.stderr}",
            pytrace=False,
        )


def test_typing(tmp_path: Path) -> None:
    _check_typing(tmp_path, SOURCE_DIR)


@pytest.mark.parametrize("module", ["mssql_python", "mssql_python.mssql_python"])
def test_cursor_consumer_call_shapes(tmp_path: Path, module: str) -> None:
    usage = tmp_path / "cursor_consumer.py"
    usage.write_text(
        f"""\
from {module} import Cursor, Json, SQL_JSON, SQL_SS_JSON, SQL_INTEGER, TableValuedParameter

def supported_calls(cursor: Cursor) -> None:
    cursor.json_output = "objects"
    cursor.execute("SELECT CAST(? AS JSON)", Json({{"items": [1, None]}}))
    cursor.execute("SELECT ?", Json('{{"items":[1,null]}}'))
    cursor.setinputsizes([SQL_JSON])
    cursor.setinputsizes([SQL_SS_JSON])
    cursor.setinputsizes(None)
    cursor.executemany("INSERT INTO example VALUES (?, ?)", [(1, "one"), (2, "two")])
    cursor.executemany("INSERT INTO example VALUES (?, ?)", [[1, "one"], [2, "two"]])
    cursor.executemany(
        "INSERT INTO example VALUES (%(id)s, %(name)s)",
        [{{"id": 1, "name": "one"}}, {{"id": 2, "name": "two"}}],
    )
    rows: list[tuple[int]] = [(1,), (2,)]
    TableValuedParameter("dbo.IntValues", [("value", SQL_INTEGER)], rows)
""",
        encoding="utf-8",
    )
    _check_typing(tmp_path, str(usage))


@pytest.mark.parametrize(
    "module", ["mssql_python", "mssql_python.mssql_python", "mssql_python.exceptions"]
)
def test_exception_consumer_call_shapes(tmp_path: Path, module: str) -> None:
    usage = tmp_path / "exception_consumer.py"
    usage.write_text(
        f"""\
from {module} import (
    Warning, Error, InterfaceError, DatabaseError, DataError, OperationalError,
    IntegrityError, InternalError, ProgrammingError, NotSupportedError,
)

def supported_calls() -> None:
    for exception_type in (
        Warning, Error, InterfaceError, DatabaseError, DataError, OperationalError,
        IntegrityError, InternalError, ProgrammingError, NotSupportedError,
    ):
        legacy = exception_type("driver msg", "ddbc msg")
        diagnostic = exception_type(
            driver_error="driver msg", ddbc_error="ddbc msg", sqlstate="42000",
            native_error=208,
        )
        state: str | None = diagnostic.sqlstate
        legacy_state: str | None = legacy.sqlstate
        native_error: int | None = diagnostic.native_error
        legacy_native_error: int | None = legacy.native_error
""",
        encoding="utf-8",
    )
    _check_typing(tmp_path, str(usage))


def test_native_api_call_shapes(tmp_path: Path) -> None:
    usage = tmp_path / "native_api_usage.py"
    usage.write_text(
        """\
from typing import Any
from mssql_python import ddbc_bindings
from mssql_python._pycore_types import AsyncCoreCursor
from mssql_python.async_query.async_cursor import _AsyncCursor
from mssql_python.row import OutputConverter

def reset_inputsizes(wrapper: _AsyncCursor, native: AsyncCoreCursor) -> None:
    wrapper.setinputsizes(None)
    native.setinputsizes(None)

def diagnostic_metadata(error: ddbc_bindings.NativeDatabaseError) -> None:
    info = ddbc_bindings.DDBCSQLCheckError(3, None, -2)
    native_number: int | None = info.nativeError
    state: str | None = error.sqlstate
    number: int | None = error.native_error
    message: str = error.ddbc_error

def fetch_with_converters(
    handle: ddbc_bindings.SqlHandle,
    row: list[Any],
    converter: OutputConverter,
) -> None:
    ddbc_bindings.DDBCSQLGetData(
        handle, 1, row, "utf-16le", "utf-16le", -8, None, [converter]
    )
    ddbc_bindings.DDBCSQLGetData(
        handle, 1, row, "utf-16le", "utf-16le", -8, converters=[converter]
    )
""",
        encoding="utf-8",
    )
    _check_typing(tmp_path, str(usage))


@pytest.mark.parametrize("module", ["mssql_python", "mssql_python.mssql_python"])
def test_vector_consumer_call_shapes(tmp_path: Path, module: str) -> None:
    usage = tmp_path / "vector_consumer.py"
    usage.write_text(
        f"""\
from {module} import (
    Cursor, Vector, OutputParameter, InputOutputParameter, SQL_SS_VECTOR,
)

def supported_calls(cursor: Cursor) -> None:
    vector = Vector([0.1, 0.2, 0.3], base_type="float16")
    values: list[float] = list(vector)
    dimensions: int = vector.dimensions
    base_type: str = vector.base_type
    output = OutputParameter(SQL_SS_VECTOR, 3, 1)
    inout = InputOutputParameter(vector)
    cursor.execute("SELECT ?", [vector])
    cursor.setinputsizes([(SQL_SS_VECTOR, 3, 1)])
    cursor.executemany("INSERT INTO v VALUES (?)", [([1, 2, 3],), (None,), (vector,)])
    cursor.callproc("[dbo].[p]", [vector, output, inout])
""",
        encoding="utf-8",
    )
    _check_typing(tmp_path, str(usage))
