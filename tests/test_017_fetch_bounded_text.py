# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Bounded text payload fidelity, including row-wise routing beside a MAX column.

The MAX value here is only a routing control. Actual MAX text BOM/NUL fidelity
belongs to the separate LOB decoder and is not covered by this regression.
"""

import os
import subprocess
import sys
import textwrap

import pytest

from mssql_python import SQL_CHAR, SQL_WCHAR, ddbc_bindings as ddbc


@pytest.fixture(scope="module")
def utf8_collation(db_connection):
    with db_connection.cursor() as cursor:
        cursor.execute(
            "SELECT name FROM sys.fn_helpcollations() "
            "WHERE name = 'Latin1_General_100_BIN2_UTF8'"
        )
        row = cursor.fetchone()
    if row is None:
        pytest.skip("VARCHAR BOM payloads require SQL Server UTF-8 collation support")
    return row[0]


def _fetch_rows(cursor, method):
    if method == "fetchall":
        rows = cursor.fetchall()
    elif method == "fetchmany":
        rows = []
        while batch := cursor.fetchmany(2):
            rows.extend(batch)
    else:
        rows = []
        while (row := cursor.fetchone()) is not None:
            rows.append(row)
    assert cursor.fetchone() is None
    return [tuple(row) for row in rows]


def _assert_payloads(connection, expressions, expected, method, forced_max, storage_encoding):
    values = ", ".join(f"({index}, {expression})" for index, expression in enumerate(expressions))
    relation = f"FROM (VALUES {values}) AS p(row_id, payload)"
    with connection.cursor() as cursor:
        # Fetch SQL evidence separately so text conversion cannot mask stored data.
        cursor.execute(
            "SELECT row_id, UNICODE(payload), DATALENGTH(payload), "
            f"CAST(payload AS varbinary(128)) {relation} ORDER BY row_id"
        )
        evidence = [tuple(row) for row in cursor.fetchall()]
        wanted_evidence = []
        for index, payload in enumerate(expected):
            raw = None if payload is None else payload.encode(storage_encoding)
            wanted_evidence.append(
                (
                    index,
                    ord(payload[0]) if payload else None,
                    None if raw is None else len(raw),
                    raw,
                )
            )
        assert evidence == wanted_evidence

        extra = ", CAST(N'route' AS nvarchar(max)) AS force_max" if forced_max else ""
        cursor.execute(
            f"SELECT r.repeat_id, p.row_id, p.payload {extra} {relation} "
            "CROSS JOIN (VALUES (1), (2)) AS r(repeat_id) ORDER BY r.repeat_id, p.row_id"
        )
        wanted = [
            (repeat_id, index, payload) + (("route",) if forced_max else ())
            for repeat_id in (1, 2)
            for index, payload in enumerate(expected)
        ]
        actual = _fetch_rows(cursor, method)
        assert actual == wanted
        assert all(row[2] is None or type(row[2]) is str for row in actual)


@pytest.mark.parametrize("prefix", [0xFEFF, 0xFFFE], ids=["feff", "fffe"])
@pytest.mark.parametrize("method", ["fetchone", "fetchmany", "fetchall"])
@pytest.mark.parametrize("forced_max", [False, True], ids=["bounded", "beside-max"])
def test_bounded_nvarchar_bom_payload(db_connection, prefix, method, forced_max):
    _assert_payloads(
        db_connection,
        [f"CAST(NCHAR({prefix}) + N'BOM' AS nvarchar(64))"],
        [chr(prefix) + "BOM"],
        method,
        forced_max,
        "utf-16le",
    )


@pytest.mark.parametrize("prefix", [0xFEFF, 0xFFFE], ids=["feff", "fffe"])
@pytest.mark.parametrize("method", ["fetchone", "fetchmany", "fetchall"])
@pytest.mark.parametrize("forced_max", [False, True], ids=["bounded", "beside-max"])
def test_bounded_varchar_bom_payload(db_connection, utf8_collation, prefix, method, forced_max):
    assert db_connection.getdecoding(SQL_CHAR)["ctype"] == SQL_WCHAR
    _assert_payloads(
        db_connection,
        [f"CAST((NCHAR({prefix}) + N'BOM') COLLATE {utf8_collation} AS varchar(64))"],
        [chr(prefix) + "BOM"],
        method,
        forced_max,
        "utf-8",
    )


@pytest.mark.parametrize("method", ["fetchone", "fetchmany", "fetchall"])
@pytest.mark.parametrize("forced_max", [False, True], ids=["bounded", "beside-max"])
def test_bounded_nvarchar_unicode_and_lengths(db_connection, method, forced_max):
    expected = [
        None,
        "",
        "plain ASCII",
        "caf\u00e9 \u4e2d\u6587",
        "A\U0001f642Z",
        "A\0B",
        "A\0",
        "\0",
        "A\ufeff\ufffeZ",
        "x" * 62 + "\U0001f642",
    ]
    expressions = [
        (
            "CAST(NULL AS nvarchar(64))"
            if value is None
            else f"CAST(0x{value.encode('utf-16le').hex()} AS nvarchar(64))"
        )
        for value in expected
    ]
    _assert_payloads(db_connection, expressions, expected, method, forced_max, "utf-16le")


@pytest.mark.parametrize(
    "encoding, ctype", [("utf-16le", SQL_WCHAR), ("latin-1", SQL_CHAR)], ids=["wide", "narrow"]
)
@pytest.mark.parametrize("method", ["fetchone", "fetchmany", "fetchall"])
@pytest.mark.parametrize("forced_max", [False, True], ids=["bounded", "beside-max"])
def test_bounded_varchar_decoding_controls(db_connection, encoding, ctype, method, forced_max):
    expected = [None, "", "ASCII", "caf\u00e9", "A\0B", "A\0", "\0", "x" * 64]
    expressions = [
        (
            "CAST(NULL AS varchar(64))"
            if value is None
            else (
                f"CAST(CAST(0x{value.encode('utf-16le').hex()} AS nvarchar(64)) "
                "COLLATE Latin1_General_100_BIN2 AS varchar(64))"
            )
        )
        for value in expected
    ]
    original = db_connection.getdecoding(SQL_CHAR)
    try:
        db_connection.setdecoding(SQL_CHAR, encoding=encoding, ctype=ctype)
        _assert_payloads(db_connection, expressions, expected, method, forced_max, "latin-1")
    finally:
        db_connection.setdecoding(SQL_CHAR, encoding=original["encoding"], ctype=original["ctype"])


@pytest.mark.parametrize("raw", ["00D8", "00DC"], ids=["unpaired-high", "unpaired-low"])
@pytest.mark.parametrize(
    "method, forced_max",
    [("fetchone", False), ("fetchone", True), ("fetchmany", True), ("fetchall", True)],
    ids=["bounded-fetchone", "beside-max-fetchone", "beside-max-fetchmany", "beside-max-fetchall"],
)
def test_bounded_nvarchar_strict_decode_error(db_connection, raw, method, forced_max):
    extra = ", CAST(N'route' AS nvarchar(max)) AS force_max" if forced_max else ""
    with db_connection.cursor() as cursor:
        cursor.execute(f"SELECT CAST(0x{raw} AS nvarchar(64)) {extra}")
        with pytest.raises(UnicodeDecodeError):
            _fetch_rows(cursor, method)
        cursor.execute("SELECT CAST(N'recovered' AS nvarchar(64))")
        assert cursor.fetchone()[0] == "recovered"


@pytest.mark.parametrize("raw", ["00D8", "00DC"], ids=["unpaired-high", "unpaired-low"])
@pytest.mark.parametrize("method", ["fetchmany", "fetchall"])
def test_bounded_nvarchar_batch_malformed_fallback(db_connection, raw, method):
    """Preserve the existing platform-specific batch behavior for unpaired surrogates."""
    expression = f"CAST(0x{raw} AS nvarchar(64))"
    raw_bytes = bytes.fromhex(raw)
    with db_connection.cursor() as cursor:
        cursor.execute(f"SELECT DATALENGTH({expression}), CAST({expression} AS varbinary(64))")
        assert tuple(cursor.fetchone()) == (len(raw_bytes), raw_bytes)
        cursor.execute(f"SELECT {expression}")
        expected = (
            raw_bytes.decode("utf-16le", errors="surrogatepass") if sys.platform == "win32" else ""
        )
        assert _fetch_rows(cursor, method) == [(expected,)]
        cursor.execute("SELECT CAST(N'recovered' AS nvarchar(64))")
        assert cursor.fetchone()[0] == "recovered"


@pytest.mark.parametrize(
    "width", [10, 63, 64, 4000], ids=["small", "inline-edge", "heap-edge", "large"]
)
@pytest.mark.parametrize("method", ["fetchone", "fetchval"])
def test_bounded_nvarchar_single_row_buffer_boundary(db_connection, width, method):
    expected = [None, "", "A\0B", "\ufeff\ufffe", "x" * (width - 2) + "\U0001f642"]
    values = ", ".join(
        f"({index}, CAST("
        + ("NULL" if value is None else f"0x{value.encode('utf-16le').hex()}")
        + f" AS nvarchar({width})))"
        for index, value in enumerate(expected)
    )
    with db_connection.cursor() as cursor:
        cursor.execute(f"SELECT payload FROM (VALUES {values}) AS v(n, payload) ORDER BY n")
        for value in expected:
            row = getattr(cursor, method)()
            actual = row[0] if method == "fetchone" else row
            assert actual == value
            assert actual is None or type(actual) is str
        assert getattr(cursor, method)() is None
        assert getattr(cursor, method)() is None
        cursor.execute("SELECT CAST(N'reused' AS nvarchar(10))")
        assert cursor.fetchone()[0] == "reused"


@pytest.mark.parametrize("width", [63, 64], ids=["inline-edge", "heap-edge"])
@pytest.mark.parametrize("method", ["fetchone", "fetchval"])
@pytest.mark.parametrize("raw", ["00D8", "00DC"], ids=["unpaired-high", "unpaired-low"])
def test_bounded_nvarchar_buffer_boundary_strict_error(db_connection, width, method, raw):
    with db_connection.cursor() as cursor:
        cursor.execute(f"SELECT CAST(0x{raw} AS nvarchar({width}))")
        with pytest.raises(UnicodeDecodeError):
            getattr(cursor, method)()
        cursor.execute("SELECT CAST(N'recovered' AS nvarchar(10))")
        row = getattr(cursor, method)()
        assert (row[0] if method == "fetchone" else row) == "recovered"


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Windows does not export the native driver function-pointer globals",
)
@pytest.mark.parametrize("width", [63, 64], ids=["inline-edge", "heap-edge"])
def test_bounded_nvarchar_getdata_buffer_contract(conn_str, width):
    import os
    import subprocess
    import textwrap

    script = textwrap.dedent("""
        import ctypes as c
        import os
        import sys
        import mssql_python
        from mssql_python import ddbc_bindings as ddbc

        width = int(sys.argv[1])
        pointer, short, length = c.c_void_p, c.c_short, c.c_ssize_t
        get_type = c.CFUNCTYPE(short, pointer, c.c_ushort, short, pointer, length, pointer)
        library = c.CDLL(ddbc.module.__file__)
        slot = pointer.in_dll(library, "SQLGetData_ptr")
        calls, errors = [], []

        @get_type
        def getdata(handle, column, ctype, buffer, capacity, indicator):
            try:
                assert column == 1 and ctype == -8  # SQL_C_WCHAR
                assert capacity == (width + 1) * 2
                assert c.string_at(buffer, capacity) == bytes(capacity)
                calls.append(capacity)
                return original(handle, column, ctype, buffer, capacity, indicator)
            except BaseException as error:
                errors.append(repr(error))
                return -1

        with mssql_python.connect(os.environ["DB_CONNECTION_STRING"]) as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"SELECT CAST(REPLICATE(N'x', {width}) AS nvarchar({width}))")
                saved = slot.value
                assert saved
                original = get_type(saved)
                slot.value = c.cast(getdata, pointer).value
                try:
                    assert cursor.fetchone()[0] == "x" * width
                    assert cursor.fetchone() is None
                finally:
                    slot.value = saved
                assert calls == [(width + 1) * 2] and not errors, (calls, errors)
                cursor.execute("SELECT CAST(N'recovered' AS nvarchar(10))")
                assert cursor.fetchval() == "recovered"
        """)
    result = subprocess.run(
        [sys.executable, "-c", script, str(width)],
        env={**os.environ, "DB_CONNECTION_STRING": conn_str},
        capture_output=True,
        text=True,
        timeout=45,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def _run_fetch_script(conn_str, script, *arguments, timeout=45):
    result = subprocess.run(
        [sys.executable, "-c", script, *arguments],
        env={**os.environ, "DB_CONNECTION_STRING": conn_str},
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("size", (1, 2))
def test_getdata_decode_error_keeps_preexisting_and_completed_cells(cursor, size):
    cursor.execute("SELECT 7, CAST(0x00D8 AS NVARCHAR(1))")
    assert ddbc.DDBCSQLFetch(cursor.hstmt) == 0
    marker = object()
    row = [marker]
    if size == 1:
        assert ddbc.DDBCSQLGetData(cursor.hstmt, size, row, "utf-16le", "utf-16le", -8) == 0
    else:
        with pytest.raises(UnicodeDecodeError):
            ddbc.DDBCSQLGetData(cursor.hstmt, size, row, "utf-16le", "utf-16le", -8)
    assert row == [marker, 7]
    cursor.execute("SELECT 42")
    assert cursor.fetchval() == 42


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Windows does not export the native driver function-pointer globals",
)
@pytest.mark.parametrize("payload", ("utf8", "empty", "null", "invalid", "warning"))
def test_narrow_getdata_decoding_in_subprocess(conn_str, payload):
    script = textwrap.dedent("""
        import ctypes as c
        import os
        import sys
        import mssql_python
        from mssql_python import ddbc_bindings as ddbc

        mode = sys.argv[1]
        data = b"\\xff" if mode == "invalid" else (
            b"" if mode == "empty" else "\\ufeffA\\x00\\U0001f600".encode("utf-8")
        )
        pointer, short, length = c.c_void_p, c.c_short, c.c_ssize_t
        get_type = c.CFUNCTYPE(short, pointer, c.c_ushort, short, pointer, length, pointer)
        diag_type = c.CFUNCTYPE(
            short, short, pointer, short, pointer, pointer, pointer, short, pointer
        )
        library = c.CDLL(ddbc.module.__file__)
        slot = pointer.in_dll(library, "SQLGetData_ptr")
        diag_slot = pointer.in_dll(library, "SQLGetDiagRec_ptr")
        calls, errors = [], []

        @diag_type
        def diagnostic(handle_type, handle, record, state, native, message, capacity, size):
            try:
                if record > 1:
                    return 100
                text = "getdata warning".encode("utf-16le")
                assert capacity > len(text) // 2
                c.memmove(state, "01000\\0".encode("utf-16le"), 12)
                c.memmove(message, text + b"\\0\\0", len(text) + 2)
                c.cast(native, c.POINTER(c.c_int))[0] = 0
                c.cast(size, c.POINTER(short))[0] = len(text) // 2
                return 0
            except BaseException as error:
                errors.append(type(error).__name__)
                return -1

        @get_type
        def getdata(handle, column, ctype, buffer, capacity, indicator):
            try:
                assert ctype == 1  # SQL_C_CHAR
                calls.append(column)
                result = original(handle, column, ctype, buffer, capacity, indicator)
                assert result == 0 and capacity > len(data)
                c.memmove(buffer, data + b"\\0", len(data) + 1)
                c.cast(indicator, c.POINTER(length))[0] = -1 if mode == "null" else len(data)
                return 1 if mode == "warning" else result
            except BaseException as error:
                errors.append(type(error).__name__)
                return -1

        with mssql_python.connect(os.environ["DB_CONNECTION_STRING"]) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT CAST('text' AS VARCHAR(30))")
                saved, saved_diag = slot.value, diag_slot.value
                assert saved and saved_diag
                original = get_type(saved)
                assert ddbc.DDBCSQLFetch(cursor.hstmt) == 0
                marker = object()
                row = [marker]
                slot.value = c.cast(getdata, pointer).value
                if mode == "warning":
                    diag_slot.value = c.cast(diagnostic, pointer).value
                try:
                    ret = ddbc.DDBCSQLGetData(
                        cursor.hstmt, 1, row, "utf-8", "utf-16le", 1, cursor.messages
                    )
                    assert ret == (1 if mode == "warning" else 0)
                finally:
                    slot.value, diag_slot.value = saved, saved_diag
                expected = None if mode == "null" else (
                    data if mode == "invalid" else data.decode("utf-8")
                )
                assert row == [marker, expected]
                assert type(row[1]) is type(expected)
                assert calls == [1] and not errors, (calls, errors)
                assert cursor.messages == (
                    [("[01000] (0)", "getdata warning")] if mode == "warning" else []
                )
                cursor.execute("SELECT 42")
                assert cursor.fetchval() == 42
        """)
    _run_fetch_script(conn_str, script, payload)
