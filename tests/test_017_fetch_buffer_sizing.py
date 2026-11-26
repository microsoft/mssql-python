# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Observe live rowset bindings in isolated processes, including checked metadata."""

import ctypes as ct
import os
from pathlib import Path
import subprocess
import sys

import pytest

import mssql_python as ms
from mssql_python import ddbc_bindings

_POINTER_NAMES = (
    "SQLBindCol_ptr",
    "SQLSetStmtAttr_ptr",
    "SQLFreeStmt_ptr",
    "SQLFetchScroll_ptr",
    "SQLDescribeCol_ptr",
)


@pytest.mark.parametrize(
    "case",
    [
        "empty",
        "small",
        "initial-boundary",
        "growth",
        "wide",
        "unicode",
        "narrow",
        "mixed",
        "zero-width",
        "oversized-row",
        "overflow-cell",
        "overflow-row",
        "growth-failure",
        "unbind-failure",
        "bind-failure",
        "lob",
    ],
)
def test_fetchall_buffer_contract(conn_str, case):
    if not conn_str:
        pytest.skip("DB_CONNECTION_STRING is required")
    addresses = getattr(ddbc_bindings.module, "_arrow_test_pointer_addresses", {})
    if sys.platform == "win32" and not all(name in addresses for name in _POINTER_NAMES):
        if os.getenv("MSSQL_ARROW_TEST_INTERPOSITION", "").upper() in ("1", "ON"):
            pytest.fail("explicitly requested ODBC pointer controls are missing")
        pytest.skip("requires MSSQL_ARROW_TEST_INTERPOSITION=1 on Windows")
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            f"import runpy; runpy.run_path({str(Path(__file__).resolve())!r}, run_name='__main__')",
            case,
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def _run_case(case):
    addresses = getattr(ddbc_bindings.module, "_arrow_test_pointer_addresses", {})
    library = ct.CDLL(str(Path(ddbc_bindings.module.__file__).resolve()))
    pointers = {
        name: (
            ct.c_void_p.from_address(addresses[name])
            if name in addresses
            else ct.c_void_p.in_dll(library, name)
        )
        for name in _POINTER_NAMES
    }
    original_addresses = {}
    callbacks = []
    bindings = []
    rowsets = []
    fetches = []
    events = []
    current_size = 1
    sql_len = ct.c_ssize_t
    sql_ulen = ct.c_size_t
    budget = 8 * 1024 * 1024
    count = {"empty": 0, "small": 3, "initial-boundary": 100}.get(case, 1207)
    if case == "oversized-row":
        count = 2
    if case in ("overflow-cell", "overflow-row", "lob"):
        count = 3

    def install(name, args, handler):
        pointer = pointers[name]
        original_addresses[name] = pointer.value
        signature = ct.CFUNCTYPE(ct.c_short, *args)
        original = signature(pointer.value)
        callback = signature(lambda *values: handler(original, *values))
        callbacks.append(callback)
        pointer.value = ct.cast(callback, ct.c_void_p).value

    def bind(original, handle, col, ctype, data, length, indicator):
        bindings.append((col, ctype, length))
        events.append("bind")
        if case == "bind-failure" and fetches:
            return -2
        return original(handle, col, ctype, data, length, indicator)

    def attr(original, handle, attribute, value, length):
        nonlocal current_size
        if attribute == 27:  # SQL_ATTR_ROW_ARRAY_SIZE
            current_size = value
            rowsets.append(value)
            events.append("rowset")
            if case == "growth-failure" and value > 100:
                # SQL_INVALID_HANDLE has no diagnostic record; it still must surface.
                return -2
        return original(handle, attribute, value, length)

    def free(original, handle, option):
        if option == 2:  # SQL_UNBIND
            events.append("unbind")
            if case == "unbind-failure" and events.count("unbind") == 1:
                return -2
        return original(handle, option)

    def fetch(original, handle, orientation, offset):
        fetches.append(current_size)
        events.append("fetch")
        return original(handle, orientation, offset)

    def describe(original, handle, col, name, capacity, actual, dtype, size, scale, nullable):
        result = original(handle, col, name, capacity, actual, dtype, size, scale, nullable)
        if result in (0, 1) and case == "zero-width" and col == 2:
            dtype[0] = 1
            size[0] = 0
        elif result in (0, 1) and case.startswith("overflow-"):
            dtype[0] = 1  # SQL_CHAR is bound, not routed through LOB streaming.
            if case == "overflow-cell":
                size[0] = sql_ulen(-1).value
            else:
                expansion = 1 if sys.platform == "win32" else 4
                size[0] = (sys.maxsize // 2 - ct.sizeof(sql_len) - 1) // expansion
        return result

    ms.pooling(enabled=False)
    with ms.connect(os.environ["DB_CONNECTION_STRING"]) as connection:
        if case in ("narrow", "overflow-row"):
            connection.setdecoding(ms.SQL_CHAR, encoding="cp1252", ctype=ms.SQL_CHAR)
        columns = ["CAST('x' AS varchar(32))"]
        expected = ["x"]
        if case in ("wide", "narrow"):
            columns = [
                "CAST(REPLICATE(CAST(NCHAR(233) AS nvarchar(max)), 8000) "
                "COLLATE Latin1_General_100_BIN2 AS varchar(8000))"
            ] * 8
            expected = ["\u00e9" * 8000] * 8
        elif case == "oversized-row":
            columns = ["CAST('x' AS varchar(8000))"] * 525
            expected = ["x"] * 525
        elif case == "unicode":
            columns = [
                "CAST(REPLICATE(N'x', 3998) + CAST(0x3DD842DE AS nvarchar(2)) AS nvarchar(4000))"
            ] * 8
            expected = ["x" * 3998 + "\U0001f642"] * 8
        elif case == "mixed":
            columns = [
                "CAST(NCHAR(233) AS varchar(8000))",
                "CAST(0x3DD842DE AS nvarchar(4000))",
                "CAST(NULL AS nvarchar(4000))",
                "CAST(0x0001FF AS varbinary(8000))",
                "CAST(12.50 AS decimal(10,2))",
                "CAST('2026-01-02T03:04:05+05:30' AS datetimeoffset)",
            ]
            from datetime import datetime
            from decimal import Decimal

            expected = [
                "\u00e9",
                "\U0001f642",
                None,
                b"\x00\x01\xff",
                Decimal("12.50"),
                datetime.fromisoformat("2026-01-02T03:04:05+05:30"),
            ]
        elif case == "overflow-row":
            columns *= 3
            expected *= 3
        elif case == "lob":
            columns += ["CAST(REPLICATE(CAST(N'y' AS nvarchar(max)), 12000) AS nvarchar(max))"]
            expected += ["y" * 12000]
        sql = (
            "WITH d(n) AS (SELECT n FROM (VALUES (0),(1),(2),(3),(4),"
            "(5),(6),(7),(8),(9)) AS v(n)) "
            f"SELECT a.n+10*b.n+100*c.n+1000*e.n AS n, {', '.join(columns)} "
            "FROM d a CROSS JOIN d b CROSS JOIN d c CROSS JOIN d e "
            f"WHERE a.n+10*b.n+100*c.n+1000*e.n < {count} ORDER BY n"
        )
        with connection.cursor() as cursor:
            cursor.execute(sql)
            try:
                install(
                    "SQLBindCol_ptr",
                    [ct.c_void_p, ct.c_ushort, ct.c_short, ct.c_void_p, sql_len, ct.c_void_p],
                    bind,
                )
                install(
                    "SQLSetStmtAttr_ptr",
                    [ct.c_void_p, ct.c_int, ct.c_void_p, ct.c_int],
                    attr,
                )
                install("SQLFreeStmt_ptr", [ct.c_void_p, ct.c_ushort], free)
                install("SQLFetchScroll_ptr", [ct.c_void_p, ct.c_short, sql_len], fetch)
                install(
                    "SQLDescribeCol_ptr",
                    [
                        ct.c_void_p,
                        ct.c_ushort,
                        ct.c_void_p,
                        ct.c_short,
                        ct.c_void_p,
                        ct.POINTER(ct.c_short),
                        ct.POINTER(sql_ulen),
                        ct.c_void_p,
                        ct.c_void_p,
                    ],
                    describe,
                )
                if case.startswith("overflow-"):
                    message = (
                        "Fetch buffer size exceeds supported limits"
                        if case == "overflow-cell"
                        else "Fetch row buffer size exceeds supported limits"
                    )
                    with pytest.raises(ValueError, match=f"^{message}$"):
                        cursor.fetchall()
                    assert not bindings
                    assert not fetches
                elif case.endswith("-failure"):
                    error = RuntimeError if case == "bind-failure" else ms.Error
                    with pytest.raises(error):
                        cursor.fetchall()
                    assert fetches == [100]
                else:
                    rows = cursor.fetchall()
                    assert [tuple(row) for row in rows] == [(i, *expected) for i in range(count)]
                    assert cursor.fetchall() == []
            finally:
                for name, address in original_addresses.items():
                    pointers[name].value = address

            if not case.startswith("overflow-") and case != "lob" and not case.endswith("-failure"):
                ncols = len(columns) + 1
                first = bindings[:ncols]
                row_bytes = sum(length + ct.sizeof(sql_len) for _, _, length in first)
                target = max(1, min(1000, budget // row_bytes))
                initial = min(100, target)
                # An exhausted second fetchall may bind again; examine the first run.
                assert rowsets[0] == initial
                expected_sizes = [initial]
                if count >= initial and target > initial:
                    expected_sizes.append(target)
                    first_fetch = events.index("fetch")
                    second_bind = events.index("bind", first_fetch + 1)
                    assert "unbind" in events[first_fetch + 1 : second_bind]
                    assert bindings[ncols : 2 * ncols] == first
                assert rowsets[: len(expected_sizes)] == expected_sizes
                batches = 1 if count == 0 else 2 + max(0, (count - initial + target - 1) // target)
                subsequent = target if count >= initial else initial
                assert fetches[:batches] == [initial] + [subsequent] * (batches - 1)
                assert len(fetches) == batches + 1
                assert target * row_bytes <= budget or target == 1
                if case == "wide":
                    assert first[1][1:] == (-8, 16002)  # SQL_C_WCHAR, including terminator.
                elif case == "narrow":
                    width = 32001 if sys.platform != "win32" else 8001
                    assert first[1][1:] == (1, width)
                elif case == "oversized-row":
                    assert row_bytes > budget and initial == target == 1
            elif case == "lob":
                assert not bindings and not fetches
            cursor.execute("SELECT 42")
            assert tuple(cursor.fetchone()) == (42,)


if __name__ == "__main__":
    _run_case(sys.argv[1])
