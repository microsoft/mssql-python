# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Observe live rowset bindings in isolated processes, including checked metadata."""

import ctypes as ct
import gc
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
    "SQLFreeHandle_ptr",
    "SQLFetch_ptr",
    "SQLGetData_ptr",
)

_RETAINED_CASES = (
    "persistent-unbind-failure",
    "persistent-rows-fetched-failure",
    "rowset-reset-failure",
    "partial-bind-cleanup-failure",
    "fetchmany-cleanup-failure",
    "arrow-cleanup-failure",
    "parent-cleanup-failure",
)


@pytest.mark.parametrize("item_size,terminator", [(0, 0), (1, sys.maxsize + 1)])
def test_checked_fetch_buffer_preconditions(item_size, terminator):
    checked_bytes = getattr(ddbc_bindings.module, "_test_checked_fetch_buffer_bytes", None)
    if checked_bytes is None:
        if os.getenv("MSSQL_ARROW_TEST_INTERPOSITION", "").upper() in ("1", "ON"):
            pytest.fail("explicitly requested native sizing controls are missing")
        pytest.skip("requires MSSQL_ARROW_TEST_INTERPOSITION=1")
    with pytest.raises(ValueError, match="^Fetch buffer size exceeds supported limits$"):
        checked_bytes(0, item_size, terminator)
    assert checked_bytes(0, 1, sys.maxsize) == sys.maxsize


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
        "partial-bind-failure",
        "persistent-unbind-failure",
        "persistent-rows-fetched-failure",
        "rowset-reset-failure",
        "partial-bind-cleanup-failure",
        "fetchmany-cleanup-failure",
        "arrow-cleanup-failure",
        "parent-cleanup-failure",
        "repeat-cleanup",
        "lob",
        "compact-wide",
        "compact-medium",
        "compact-small-dense",
        "compact-warning",
        "compact-dense-rebind-failure",
        "compact-no-total",
        "compact-invalid-length",
        "compact-truncated",
        "compact-invalid-scalar",
        "compact-fetch-error",
        "compact-odd-wide-length",
        "compact-zero-rows",
        "compact-excess-rows",
        "compact-dense-truncated",
    ],
)
def test_fetchall_buffer_contract(conn_str, case):
    if not conn_str:
        pytest.skip("DB_CONNECTION_STRING is required")
    if case == "arrow-cleanup-failure":
        pytest.importorskip("pyarrow")
    addresses = getattr(ddbc_bindings.module, "_arrow_test_pointer_addresses", {})
    if (case in _RETAINED_CASES or case == "repeat-cleanup") and not hasattr(
        ddbc_bindings.module, "_test_retained_binding_storage"
    ):
        if os.getenv("MSSQL_ARROW_TEST_INTERPOSITION", "").upper() in ("1", "ON"):
            pytest.fail("explicitly requested native lifetime controls are missing")
        pytest.skip("native lifetime checks require MSSQL_ARROW_TEST_INTERPOSITION=1")
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
    bound_cells = {}
    text_reads = []
    length_probes = []
    events = []
    current_size = 1
    statement = None
    fetched_pointer = None
    lifetime = None
    persistent = case in _RETAINED_CASES
    sql_len = ct.c_ssize_t
    sql_ulen = ct.c_size_t
    budget = 8 * 1024 * 1024
    count = {"empty": 0, "small": 3, "initial-boundary": 100}.get(case, 1207)
    if case == "oversized-row":
        count = 2
    if case in ("overflow-cell", "overflow-row", "lob") or case.startswith("compact-"):
        count = 3
    if case in (
        "compact-wide",
        "compact-medium",
        "compact-dense-truncated",
        "compact-dense-rebind-failure",
    ):
        count = 1207
    if case == "compact-small-dense":
        count = 1

    def install(name, args, handler):
        pointer = pointers[name]
        original_addresses[name] = pointer.value
        signature = ct.CFUNCTYPE(ct.c_short, *args)
        original = signature(pointer.value)
        callback = signature(lambda *values: handler(original, *values))
        callbacks.append(callback)
        pointer.value = ct.cast(callback, ct.c_void_p).value

    def bind(original, handle, col, ctype, data, length, indicator):
        nonlocal statement
        statement = handle
        bindings.append((col, ctype, length))
        bound_cells[col] = (length, indicator, data)
        events.append("bind")
        if case == "bind-failure" and fetches:
            return -2
        if (
            case
            in (
                "partial-bind-failure",
                "partial-bind-cleanup-failure",
                "compact-dense-rebind-failure",
            )
            and fetches
            and col == 2
        ):
            return -2
        return original(handle, col, ctype, data, length, indicator)

    def attr(original, handle, attribute, value, length):
        nonlocal current_size, fetched_pointer
        if attribute == 26 and value:
            fetched_pointer = value
        if (
            case in ("persistent-rows-fetched-failure", "arrow-cleanup-failure")
            and attribute == 26
            and not value
        ):
            return -2  # Leave SQL_ATTR_ROWS_FETCHED_PTR installed.
        if attribute == 27:  # SQL_ATTR_ROW_ARRAY_SIZE
            current_size = value
            rowsets.append(value)
            events.append("rowset")
            if case == "rowset-reset-failure" and value == 1:
                return -2
            if case == "growth-failure" and value > 100:
                # SQL_INVALID_HANDLE has no diagnostic record; it still must surface.
                return -2
        return original(handle, attribute, value, length)

    def free(original, handle, option):
        if option == 2:  # SQL_UNBIND
            events.append("unbind")
            if case == "unbind-failure" and events.count("unbind") == 1:
                return -2
            if case in (
                "persistent-unbind-failure",
                "fetchmany-cleanup-failure",
                "parent-cleanup-failure",
            ):
                return -2
            if case == "partial-bind-cleanup-failure" and events.count("unbind") > 1:
                return -2
        return original(handle, option)

    def free_handle(original, kind, handle):
        if case == "parent-cleanup-failure" and kind == 3 and handle == statement:
            events.append("failed-free")
            return -2
        return original(kind, handle)

    def fetch(original, handle, orientation, offset):
        fetches.append(current_size)
        events.append("fetch")
        result = original(handle, orientation, offset)
        if result in (0, 1):
            if case == "compact-warning" and len(fetches) == 1:
                return 1  # Valid bound values with SQL_SUCCESS_WITH_INFO.
            if case in ("compact-zero-rows", "compact-excess-rows"):
                output_count = ct.cast(fetched_pointer, ct.POINTER(sql_ulen))
                output_count[0] = 0 if case == "compact-zero-rows" else current_size + 1
            if case == "compact-fetch-error":
                return -2
            column = 1 if case == "compact-invalid-scalar" else 2
            capacity, indicator, _ = bound_cells[column]
            output_length = ct.cast(indicator, ct.POINTER(sql_len))
            if case == "compact-no-total":
                output_length[0] = -4
            elif case in ("compact-invalid-length", "compact-invalid-scalar"):
                output_length[0] = -99
            elif case == "compact-truncated":
                output_length[0] = capacity + 2
            elif case == "compact-odd-wide-length":
                output_length[0] = 1
            elif case == "compact-dense-truncated" and len(fetches) > 1:
                output_length[0] = capacity + 2
        return result

    def arrow_fetch(original, handle):
        fetches.append(current_size)
        return original(handle)

    def getdata(original, handle, col, ctype, data, length, indicator):
        if ctype == -2 and length == 0:
            length_probes.append(col)
        if ctype in (1, -8) and length:
            text_reads.append((ctype, data, length))
        return original(handle, col, ctype, data, length, indicator)

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
        if case in ("wide", "narrow", "compact-dense-truncated", "compact-dense-rebind-failure"):
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
        elif case == "compact-wide":
            columns = ["CAST('x' AS varchar(8000))"] * 8
            expected = ["x"] * 8
        elif case == "compact-medium":
            columns = ["CAST('x' AS varchar(257))"] * 8
            expected = ["x"] * 8
        elif case == "compact-small-dense":
            columns = ["CAST(REPLICATE(N'x', 4000) AS nvarchar(4000))"]
            expected = ["x" * 4000]
        elif case.startswith("compact-"):
            columns = ["CAST('x' AS varchar(8000))"]
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
                install("SQLFreeHandle_ptr", [ct.c_short, ct.c_void_p], free_handle)
                if case == "arrow-cleanup-failure":
                    install("SQLFetch_ptr", [ct.c_void_p], arrow_fetch)
                install("SQLFetchScroll_ptr", [ct.c_void_p, ct.c_short, sql_len], fetch)
                install(
                    "SQLGetData_ptr",
                    [ct.c_void_p, ct.c_ushort, ct.c_short, ct.c_void_p, sql_len, ct.c_void_p],
                    getdata,
                )
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
                elif case in (
                    "compact-no-total",
                    "compact-invalid-length",
                    "compact-truncated",
                    "compact-invalid-scalar",
                    "compact-odd-wide-length",
                    "compact-dense-truncated",
                ):
                    with pytest.raises(RuntimeError, match="Invalid bounded fetch length"):
                        cursor.fetchall()
                    if case == "compact-dense-truncated":
                        assert fetches == rowsets[:2]
                    else:
                        assert fetches == [rowsets[0]]
                elif case in ("compact-zero-rows", "compact-excess-rows"):
                    with pytest.raises(RuntimeError, match="Invalid bounded fetch row count"):
                        cursor.fetchall()
                    assert fetches == [rowsets[0]]
                elif case == "compact-fetch-error":
                    with pytest.raises(ms.Error):
                        cursor.fetchall()
                    assert fetches == [rowsets[0]]
                elif case.endswith("-failure"):
                    error = (
                        RuntimeError
                        if case
                        in (
                            "bind-failure",
                            "partial-bind-failure",
                            "partial-bind-cleanup-failure",
                            "compact-dense-rebind-failure",
                        )
                        else ms.Error
                    )
                    with pytest.raises(error) as failure:
                        if case == "fetchmany-cleanup-failure":
                            cursor.fetchmany(100)
                        elif case == "arrow-cleanup-failure":
                            cursor.arrow_batch(64)
                        else:
                            cursor.fetchall()
                    if error is RuntimeError:
                        assert "Failed to bind column" in str(failure.value)
                    if case == "arrow-cleanup-failure":
                        assert fetches == [64]
                    elif case == "compact-dense-rebind-failure":
                        assert fetches == [rowsets[0]]
                        assert bindings[len(columns) + 2][0] == 2
                    elif case not in ("persistent-rows-fetched-failure", "rowset-reset-failure"):
                        assert fetches == [100]
                    if case in ("partial-bind-failure", "partial-bind-cleanup-failure"):
                        assert bindings[3][0] == 2
                    if persistent:
                        with pytest.raises(ms.ProgrammingError, match="failed binding cleanup"):
                            ddbc_bindings.DDBCSQLNumResultCols(cursor.hstmt)
                        with pytest.raises(ms.ProgrammingError, match="failed binding cleanup"):
                            cursor.fetchone()
                        lifetime = ddbc_bindings.module._test_retained_binding_storage(cursor.hstmt)
                        gc.collect()
                        assert lifetime(), "ODBC still holds pointers into this native storage"
                    if case == "parent-cleanup-failure":
                        cursor.close()
                        gc.collect()
                        assert "failed-free" in events
                        assert lifetime(), "failed statement free must retain its bound storage"
                        connection.close()
                        assert not lifetime(), "successful disconnect must release the storage"
                else:
                    rows = cursor.fetchall()
                    assert [tuple(row) for row in rows] == [(i, *expected) for i in range(count)]
                    assert cursor.fetchall() == []
            finally:
                for name, address in original_addresses.items():
                    pointers[name].value = address

            if persistent:
                cursor.close()
                assert not lifetime()
                if case != "parent-cleanup-failure":
                    with connection.cursor() as sibling:
                        sibling.execute("SELECT 42")
                        assert tuple(sibling.fetchone()) == (42,)
                return

            if case.startswith("compact-") or case in (
                "wide",
                "narrow",
                "unicode",
                "mixed",
                "oversized-row",
            ):
                assert not text_reads and not length_probes
                ncols = len(columns) + 1
                first = bindings[:ncols]
                row_bytes = sum(length + ct.sizeof(sql_len) for _, _, length in first)
                initial = max(1, min(100, 1024 * 1024 // row_bytes))
                assert rowsets[0] == initial
                assert initial * row_bytes <= 1024 * 1024 or initial == 1
                sparse_narrow = case == "narrow" and sys.platform != "win32"
                if case in (
                    "compact-wide",
                    "compact-small-dense",
                    "compact-warning",
                    "mixed",
                    "oversized-row",
                ) or (sparse_narrow):
                    assert all(size == initial for size in fetches)
                    assert len(fetches) == (count + initial - 1) // initial + 2
                    assert len(bindings) == 2 * ncols  # Only the two fetchall invocations.
                elif case in ("wide", "narrow", "unicode", "compact-medium"):
                    target_budget = 1024 * 1024 if case == "compact-medium" else budget
                    target = max(1, min(1000, target_budget // row_bytes))
                    assert fetches[0] == initial
                    assert all(size == target for size in fetches[1:-1])
                    assert fetches[-1] == initial  # Exhausted second fetchall.
                    assert rowsets[1] == target
                    assert bindings[ncols : 2 * ncols] == first
            elif (
                not case.startswith("overflow-") and case != "lob" and not case.endswith("-failure")
            ):
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
            elif case == "lob":
                assert not bindings and not fetches
            cursor.execute("SELECT 42")
            assert tuple(cursor.fetchone()) == (42,)


if __name__ == "__main__":
    if sys.argv[1] == "repeat-cleanup":
        for _ in range(10):
            for failure in (
                "small",
                "partial-bind-failure",
                "persistent-unbind-failure",
                "parent-cleanup-failure",
            ):
                _run_case(failure)
    else:
        _run_case(sys.argv[1])
