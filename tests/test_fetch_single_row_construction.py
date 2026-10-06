"""Count reuse and checked row-wise construction; driver faults stay in subprocesses."""

import datetime
import decimal
import gc
import os
import subprocess
import sys
import textwrap
import weakref

import pytest

from mssql_python import ddbc_bindings as ddbc


def test_getdata_appends_to_existing_list_without_python_append(cursor):
    class Destination(list):
        def append(self, value):
            raise AssertionError("native append must not dispatch to list overrides")

    class Marker:
        pass

    marker = Marker()
    reference = weakref.ref(marker)
    destination = Destination([marker])
    alias = destination
    cursor.execute(
        "SELECT CAST(-2147483648 AS INT), CAST(-32768 AS SMALLINT), "
        "CAST(-9223372036854775808 AS BIGINT), CAST(255 AS TINYINT), "
        "CAST(1 AS BIT), CAST(-1.25 AS REAL), CAST(1.5 AS FLOAT), "
        "CAST(NULL AS INT), CAST(NULL AS SMALLINT), CAST(NULL AS BIGINT), "
        "CAST(NULL AS TINYINT), CAST(NULL AS BIT), CAST(NULL AS REAL), "
        "CAST(NULL AS FLOAT), CAST(12.50 AS DECIMAL(5,2)), "
        "CAST('2024-02-29' AS DATE), CAST(0x0001FF AS VARBINARY(3))"
    )
    assert ddbc.DDBCSQLFetch(cursor.hstmt) == 0
    assert ddbc.DDBCSQLGetData(cursor.hstmt, 17, destination, "utf-16le", "utf-16le", -8) == 0
    expected = [
        -2147483648,
        -32768,
        -9223372036854775808,
        255,
        True,
        -1.25,
        1.5,
        *([None] * 7),
        decimal.Decimal("12.50"),
        datetime.date(2024, 2, 29),
        b"\x00\x01\xff",
    ]
    assert destination is alias and destination[0] is marker
    assert destination[1:] == expected
    assert list(map(type, destination[1:])) == list(map(type, expected))
    del marker, destination, alias
    gc.collect()
    assert reference() is None
    cursor.execute("SELECT 42")
    assert cursor.fetchval() == 42


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
@pytest.mark.parametrize(
    "mode",
    (
        "numeric",
        "mixed",
        "lob",
        "prefix",
        "generation",
        "count_error",
        "nextset",
        "warning",
        "decode_error",
    ),
)
def test_fetchmany_one_full_count_calls_in_subprocess(conn_str, mode):
    script = textwrap.dedent("""
        import ctypes as c
        import os
        import sys
        import mssql_python
        from mssql_python import ddbc_bindings as ddbc

        mode = sys.argv[1]
        pointer, short, ushort = c.c_void_p, c.c_short, c.c_ushort
        count_type = c.CFUNCTYPE(short, pointer, c.POINTER(short))
        diag_type = c.CFUNCTYPE(
            short, short, pointer, short, pointer, pointer, pointer, short, pointer
        )
        library = c.CDLL(ddbc.module.__file__)
        count_slot = pointer.in_dll(library, "SQLNumResultCols_ptr")
        diag_slot = pointer.in_dll(library, "SQLGetDiagRec_ptr")
        calls, errors, injected = [], [], []
        warning_pending = False

        @count_type
        def counted(handle, count):
            global warning_pending
            try:
                calls.append(handle)
                if mode == "count_error" and not injected:
                    injected.append(True)
                    return -1
                result = original_count(handle, count)
                if mode == "generation" and not injected:
                    injected.append(True)
                    assert ddbc.DDBCSQLSetStmtAttr(cursor.hstmt, 0, 0) == 0
                if mode == "warning" and not injected:
                    injected.append(True)
                    warning_pending = True
                    return 1
                return result
            except BaseException as error:
                errors.append(type(error).__name__)
                return -1

        @diag_type
        def diagnostic(handle_type, handle, record, state, native, message, capacity, size):
            global warning_pending
            try:
                if not warning_pending:
                    return original_diag(
                        handle_type, handle, record, state, native, message, capacity, size
                    )
                if record > 1:
                    warning_pending = False
                    return 100
                text = "count warning".encode("utf-16le")
                assert capacity > len(text) // 2
                c.memmove(state, "01000\\0".encode("utf-16le"), 12)
                c.memmove(message, text + b"\\0\\0", len(text) + 2)
                c.cast(native, c.POINTER(c.c_int))[0] = 0
                c.cast(size, c.POINTER(short))[0] = len(text) // 2
                return 0
            except BaseException as error:
                errors.append(type(error).__name__)
                return -1

        with mssql_python.connect(os.environ["DB_CONNECTION_STRING"]) as connection:
            with connection.cursor() as cursor:
                value_sql = (
                    "CASE WHEN n = 2 THEN CAST(0x00D8 AS NVARCHAR(10)) "
                    "ELSE CAST(N'text' AS NVARCHAR(10)) END" if mode == "decode_error" else
                    "CAST(N'text' AS NVARCHAR(MAX))" if mode == "lob" else
                    "CAST(N'text' AS NVARCHAR(10))" if mode in ("mixed", "prefix") else
                    "n + 10"
                )
                query = (
                    f"SELECT n AS a, {value_sql} AS b FROM "
                    "(VALUES (1),(2),(3),(4),(5),(6)) v(n) ORDER BY n"
                )
                cursor.execute(query)
                saved_count, saved_diag = count_slot.value, diag_slot.value
                assert saved_count and saved_diag
                original_count, original_diag = count_type(saved_count), diag_type(saved_diag)
                count_slot.value = c.cast(counted, pointer).value
                diag_slot.value = c.cast(diagnostic, pointer).value
                try:
                    expected = 1
                    if mode == "prefix":
                        assert ddbc.DDBCSQLFetch(cursor.hstmt) == 0
                        prefix = []
                        assert ddbc.DDBCSQLGetData(
                            cursor.hstmt, 1, prefix, "utf-16le", "utf-16le", -8
                        ) == 0
                        assert prefix == [1] and calls == []
                        expected = 2
                    if mode == "count_error":
                        try:
                            cursor.fetchmany(1)
                        except mssql_python.DatabaseError:
                            pass
                        else:
                            raise AssertionError("count failure was not propagated")
                        assert len(calls) == 1
                        calls.clear()
                    first = cursor.fetchmany(1)[0]
                    assert first[0] == expected and len(first) == 2
                    assert first[1] == (
                        "text" if mode in ("mixed", "prefix", "lob", "decode_error")
                        else expected + 10
                    )
                    # Cold eager count and DescribeColumns' independent count.
                    assert len(calls) == 2, (mode, calls)
                    if mode == "decode_error":
                        try:
                            cursor.fetchone()
                        except UnicodeDecodeError:
                            pass
                        else:
                            raise AssertionError("invalid UTF-16 must fail GetData decoding")
                        assert len(calls) == 2
                        assert tuple(cursor.fetchmany(1)[0]) == (3, "text")
                        assert len(calls) == 4  # failure invalidated count and metadata
                        expected = 3
                    cold = len(calls)
                    assert cursor.fetchmany(1)[0][0] == expected + 1
                    assert len(calls) == cold + (1 if mode == "generation" else 0)
                    warm = len(calls)
                    assert cursor.fetchval() == expected + 2
                    assert cursor.fetchmany(1)[0][0] == expected + 3
                    assert len(calls) == warm
                    assert ddbc.DDBCSQLNumResultCols(cursor.hstmt) == 2
                    assert ddbc.DDBCSQLNumResultCols(cursor.hstmt) == 2
                    assert len(calls) == warm + 2  # public API is still uncached
                    assert not errors, errors
                    if mode == "warning":
                        assert cursor.messages == [("[01000] (0)", "count warning")]
                    else:
                        assert not cursor.messages
                    while cursor.fetchmany(1):
                        pass
                    assert len(calls) == warm + 2  # EOF does not reacquire the count
                    if mode == "nextset":
                        cursor.execute("SELECT 7 AS a; SELECT 8 AS a, 9 AS b")
                        calls.clear()
                        assert tuple(cursor.fetchmany(1)[0]) == (7,)
                        assert len(calls) == 2
                        assert cursor.nextset()
                        calls.clear()
                        assert tuple(cursor.fetchmany(1)[0]) == (8, 9)
                        assert len(calls) == 2
                        assert cursor.fetchmany(1) == []
                        assert len(calls) == 2
                finally:
                    count_slot.value, diag_slot.value = saved_count, saved_diag
                assert not errors, errors
        """)
    result = subprocess.run(
        [sys.executable, "-c", script, mode],
        env={**os.environ, "DB_CONNECTION_STRING": conn_str},
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


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
    result = subprocess.run(
        [sys.executable, "-c", script, payload],
        env={**os.environ, "DB_CONNECTION_STRING": conn_str},
        capture_output=True,
        text=True,
        timeout=45,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.skipif(
    sys.platform != "win32",
    reason="Unix narrow fetches intentionally decode the driver's UTF-8, not a custom codec",
)
@pytest.mark.parametrize("failure", ("memory", "codec", "embedded_nul", "len_error", "len_quiet"))
def test_narrow_custom_codec_failure_in_subprocess(conn_str, failure):
    script = textwrap.dedent("""
        import codecs
        import os
        import sys
        import mssql_python
        from mssql_python import ddbc_bindings as ddbc
        from mssql_python.logging import logger

        calls = []
        length_calls = []
        saved_level = logger._cached_level

        class Decoded(str):
            def __len__(self):
                length_calls.append(True)
                raise ValueError("injected decoded length failure")

        def decode(data, errors="strict"):
            calls.append(bytes(data))
            if sys.argv[1] == "memory":
                raise MemoryError("injected codec allocation failure")
            if sys.argv[1] in ("len_error", "len_quiet"):
                return Decoded("text"), len(data)
            raise ValueError("injected codec failure")

        def search(name):
            if name == "mssql_fetch_test_codec":
                return codecs.CodecInfo(name=name, encode=None, decode=decode)
            return None

        codecs.register(search)
        try:
            with mssql_python.connect(os.environ["DB_CONNECTION_STRING"]) as connection:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT 7, CAST('text' AS VARCHAR(10))")
                    assert ddbc.DDBCSQLFetch(cursor.hstmt) == 0
                    marker = object()
                    row = [marker]
                    encoding = (
                        "utf-8\\0suffix" if sys.argv[1] == "embedded_nul"
                        else "mssql_fetch_test_codec"
                    )
                    if sys.argv[1] in ("len_error", "len_quiet"):
                        # Native LOG arguments are evaluated only at DEBUG.
                        ddbc.update_log_level(10 if sys.argv[1] == "len_error" else 50)
                    try:
                        result = ddbc.DDBCSQLGetData(
                            cursor.hstmt, 2, row, encoding, "utf-16le", 1
                        )
                    except MemoryError:
                        assert sys.argv[1] == "memory"
                        assert row == [marker, 7]
                    else:
                        assert result == 0
                        if sys.argv[1] in ("len_error", "len_quiet"):
                            assert type(row[2]) is Decoded
                            assert row[:3] == [marker, 7, "text"]
                            assert row[3:] == ([b"text"] if sys.argv[1] == "len_error" else [])
                            assert length_calls == ([True] if sys.argv[1] == "len_error" else [])
                        else:
                            assert sys.argv[1] in ("codec", "embedded_nul")
                            assert row == [marker, 7, b"text"]
                            assert type(row[2]) is bytes
                    assert calls == ([] if sys.argv[1] == "embedded_nul" else [b"text"])
                    cursor.execute("SELECT 42")
                    assert cursor.fetchval() == 42
        finally:
            ddbc.update_log_level(saved_level)
            codecs.unregister(search)
        """)
    result = subprocess.run(
        [sys.executable, "-c", script, failure],
        env={**os.environ, "DB_CONNECTION_STRING": conn_str},
        capture_output=True,
        text=True,
        timeout=45,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
