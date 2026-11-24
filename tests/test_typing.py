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
from {module} import Cursor, Json, SQL_JSON, SQL_SS_JSON

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
