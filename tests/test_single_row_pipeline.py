"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.

Single-row native fetch parity, ownership, and operation-count regressions.
"""

import datetime
import decimal
from unittest.mock import Mock, patch
import uuid
import weakref

import pytest

import mssql_python
from mssql_python import ddbc_bindings
from mssql_python.cursor import Cursor
from mssql_python.row import Row

SINGLE_METHODS = ("fetchone", "iterator", "fetchval", "fetchmany_one", "fetchmany_default")
UUID_TEXT = "00112233-4455-6677-8899-AABBCCDDEEFF"


@pytest.fixture
def connection(conn_str):
    try:
        conn = mssql_python.connect(conn_str, timeout=5)
    except mssql_python.Error as error:
        pytest.fail(
            f"Connection failed: {type(error).__name__}; connection details withheld",
            pytrace=False,
        )
    try:
        yield conn
    finally:
        conn.close()


def read_one(cursor, method):
    if method == "iterator":
        return next(cursor, None)
    if method.startswith("fetchmany"):
        batch = cursor.fetchmany(1) if method == "fetchmany_one" else cursor.fetchmany()
        assert type(batch) is list
        assert len(batch) <= 1
        return batch[0] if batch else None
    return getattr(cursor, method)()


@pytest.mark.parametrize("method", SINGLE_METHODS)
@pytest.mark.parametrize(
    ("sql_type", "literal", "value"),
    (
        ("INT", "-42", -42),
        ("BIGINT", "9223372036854775807", 9223372036854775807),
        ("SMALLINT", "-32768", -32768),
        ("TINYINT", "255", 255),
        ("BIT", "1", True),
        ("REAL", "1.25", 1.25),
        ("FLOAT", "1.25", 1.25),
        ("DECIMAL(20,4)", "12345678.1250", decimal.Decimal("12345678.1250")),
        ("VARCHAR(10)", "'abc'", "abc"),
        ("NVARCHAR(10)", "NCHAR(233)", "\u00e9"),
        ("VARBINARY(10)", "0x0102", b"\x01\x02"),
        ("UNIQUEIDENTIFIER", f"'{UUID_TEXT}'", uuid.UUID(UUID_TEXT)),
        ("DATE", "'20260102'", datetime.date(2026, 1, 2)),
        ("DATETIME2", "'2026-01-02T03:04:05.125'", datetime.datetime(2026, 1, 2, 3, 4, 5, 125000)),
        ("TIME", "'03:04:05.125'", datetime.time(3, 4, 5, 125000)),
        ("XML", "'<value>abc</value>'", "<value>abc</value>"),
    ),
)
def test_single_row_types_nulls_and_position(connection, method, sql_type, literal, value):
    with connection.cursor() as cursor:
        assert cursor.arraysize == 1
        cursor.execute(
            f"SELECT CAST(CASE WHEN n = 2 THEN NULL ELSE {literal} END AS {sql_type}) AS value "
            "FROM (VALUES (1), (2), (3)) AS v(n) ORDER BY n"
        )
        for index, expected in enumerate((value, None, value)):
            row = read_one(cursor, method)
            if method != "fetchval":
                assert type(row) is Row
                actual = row[0]
            else:
                actual = row
            assert actual == expected
            assert type(actual) is type(expected)
            assert cursor.rowcount == index + 1
            assert cursor.rownumber == index
        assert read_one(cursor, method) is None
        assert read_one(cursor, method) is None
        assert cursor.rowcount == 3
        assert cursor.rownumber == 2
        assert not cursor.messages
        cursor.execute("SELECT 7 AS replacement WHERE 1 = 0")
        assert read_one(cursor, method) is None
        assert cursor.rowcount == 0
        assert cursor.rownumber == -1


@pytest.mark.parametrize("width", (1, 2, 32, 128))
@pytest.mark.parametrize("method", SINGLE_METHODS)
def test_single_row_interleaves_batches_and_keeps_row_ownership(connection, width, method):
    columns = ", ".join(
        f"n AS c{i}" if i % 2 == 0 else f"CAST(N'payload' AS NVARCHAR(20)) AS c{i}"
        for i in range(width)
    )
    with connection.cursor() as cursor:
        cursor.execute(
            f"SELECT {columns} FROM (VALUES (1), (2), (3), (4), (5), (6), (7)) AS v(n) ORDER BY n"
        )
        first = read_one(cursor, method)
        assert (first if method == "fetchval" else first[0]) == 1
        assert [row[0] for row in cursor.fetchmany(2)] == [2, 3]
        fourth = read_one(cursor, method)
        assert (fourth if method == "fetchval" else fourth[0]) == 4
        remaining = cursor.fetchall()
        assert [row[0] for row in remaining] == [5, 6, 7]
        assert cursor.rowcount == 7
        assert cursor.rownumber == 6
        assert cursor.fetchmany(1) == []
        cursor.execute("SELECT 99 AS replacement; SELECT 100 AS following")
        assert cursor.fetchone()[0] == 99
        assert cursor.nextset()
        assert cursor.fetchmany(1)[0][0] == 100
    if method != "fetchval":
        assert list(first) == [1 if i % 2 == 0 else "payload" for i in range(width)]
        assert first._column_names == tuple(f"c{i}" for i in range(width))
        assert first._values is not fourth._values
        assert first._values is not remaining[0]._values
        reference = weakref.ref(first)
        del first
        assert reference() is None


@pytest.mark.parametrize("method", ("fetchone", "fetchmany_one"))
def test_partial_native_metadata_is_not_a_result_column_count(connection, method):
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT n, CAST(N'abc' AS NVARCHAR(10)) AS text, CAST(1.25 AS DECIMAL(5,2)) AS amount "
            "FROM (VALUES (1), (2), (3)) AS v(n) ORDER BY n"
        )
        assert ddbc_bindings.DDBCSQLFetch(cursor.hstmt) == 0
        prefix = []
        assert (
            ddbc_bindings.DDBCSQLGetData(
                cursor.hstmt, 1, prefix, "utf-16le", "utf-16le", mssql_python.SQL_WCHAR
            )
            == 0
        )
        assert prefix == [1]
        for expected in (2, 3):
            row = read_one(cursor, method)
            assert list(row) == [expected, "abc", decimal.Decimal("1.25")]


@pytest.mark.parametrize("method", SINGLE_METHODS)
def test_single_row_converter_refresh_failure_keeps_consumed_position(connection, method):
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT CAST(n AS NVARCHAR(10)) AS value "
            "FROM (VALUES (1), (2), (3)) AS v(n) ORDER BY n"
        )
        first = read_one(cursor, method)
        assert (first if method == "fetchval" else first[0]) == "1"
        converter = Mock(side_effect=lambda raw: "converted:" + raw.decode("utf-16-le"))
        connection.add_output_converter(mssql_python.SQL_WVARCHAR, converter)
        with patch.object(
            connection, "get_output_converter", side_effect=RuntimeError("injected map failure")
        ):
            with pytest.raises(RuntimeError, match="injected map failure"):
                read_one(cursor, method)
        assert cursor.rowcount == 2
        assert cursor.rownumber == 1
        final = read_one(cursor, method)
        assert (final if method == "fetchval" else final[0]) == "converted:3"
        converter.assert_called_once_with(b"3\x00")


@pytest.mark.parametrize("method", SINGLE_METHODS)
def test_single_row_decoding_refresh_failure_does_not_consume(connection, method):
    with connection.cursor() as cursor:
        cursor.execute("SELECT CONVERT(VARCHAR(1), 0xE9) AS value FROM (VALUES (1), (2)) AS v(n)")
        connection.setdecoding(mssql_python.SQL_CHAR, encoding="latin-1")
        with patch.object(connection, "getdecoding", side_effect=RuntimeError("injected decoding")):
            with pytest.raises(RuntimeError, match="injected decoding"):
                read_one(cursor, method)
        assert cursor.rownumber == -1
        for index in range(2):
            row = read_one(cursor, method)
            assert (row if method == "fetchval" else row[0]) == "\u00e9"
            assert cursor.rowcount == index + 1


@pytest.mark.parametrize("method", ("iterator", "fetchval"))
@pytest.mark.parametrize("override", ("instance", "subclass"))
def test_single_row_keeps_fetchone_override_dispatch(connection, method, override):
    calls = []

    class CustomCursor(Cursor):
        def fetchone(self):
            calls.append("subclass")
            return super().fetchone()

    cursor_class = CustomCursor if override == "subclass" else Cursor
    with cursor_class(connection) as cursor:
        cursor.execute("SELECT 42 AS value")
        if override == "instance":
            original = cursor.fetchone

            def fetchone():
                calls.append("instance")
                return original()

            cursor.fetchone = fetchone
        row = read_one(cursor, method)
        assert (row if method == "fetchval" else row[0]) == 42
        assert read_one(cursor, method) is None
        assert calls == [override, override]


@pytest.mark.parametrize("method", SINGLE_METHODS)
def test_single_row_factory_failure_keeps_consumed_position(connection, method):
    with connection.cursor() as cursor:
        cursor.execute("SELECT n FROM (VALUES (1), (2)) AS v(n) ORDER BY n")
        target, name = (
            (ddbc_bindings, "construct_rows")
            if method.startswith("fetchmany")
            else (Row, "_fast_create")
        )
        with patch.object(target, name, side_effect=RuntimeError("injected row factory")):
            with pytest.raises(RuntimeError, match="injected row factory"):
                read_one(cursor, method)
        assert cursor.rowcount == 1
        assert cursor.rownumber == 0
        row = read_one(cursor, method)
        assert (row if method == "fetchval" else row[0]) == 2
        assert cursor.rowcount == 2


@pytest.mark.parametrize("method", SINGLE_METHODS)
def test_single_row_closed_and_cancelled_cursor(connection, method):
    with connection.cursor() as cursor:
        cursor.execute("SELECT n FROM (VALUES (1), (2)) AS v(n)")
        assert read_one(cursor, method) is not None
        cursor.hstmt._cancel()
        cursor.execute("SELECT 42 AS replacement")
        row = read_one(cursor, method)
        assert (row if method == "fetchval" else row[0]) == 42
    with pytest.raises(mssql_python.ProgrammingError, match="closed"):
        read_one(cursor, method)


@pytest.mark.parametrize("method", SINGLE_METHODS)
def test_single_row_reuses_unbound_state_without_batch_setup(connection, method):
    profiling = getattr(ddbc_bindings, "profiling", None)
    if profiling is None:
        pytest.skip("Operation counts require an ENABLE_PROFILING=ON diagnostic build")
    with connection.cursor() as cursor:
        cursor.execute("SELECT n FROM (VALUES (1), (2), (3)) AS v(n) ORDER BY n")
        profiling.reset()
        profiling.enable()
        try:
            for expected in (1, 2, 3):
                row = read_one(cursor, method)
                assert (row if method == "fetchval" else row[0]) == expected
            assert read_one(cursor, method) is None
        finally:
            profiling.disable()
        counts = {name: stats["calls"] for name, stats in profiling.get_stats().items()}
        assert counts["ddbc::FetchSingleRow"] == 4
        assert counts["ddbc::FetchSingleRow::SQL_UNBIND"] == 1
        assert counts.get("ddbc::SQLNumResultCols_wrap", 0) <= 1
        assert counts.get("ddbc::SQLBindColums", 0) == 0
        assert counts.get("ddbc::FetchBatchData", 0) == 0
