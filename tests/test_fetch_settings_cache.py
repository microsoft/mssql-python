"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.

Regression and operation-count tests for fetch settings and diagnostic preservation.
Read-only fetch regressions; native fault injection runs in isolated child processes.
"""

import datetime
import decimal
import gc
import os
from pathlib import Path
import subprocess
import sys
import textwrap
import uuid
import weakref
from unittest.mock import Mock, patch

import pytest
import mssql_python
from mssql_python import ddbc_bindings as ddbc
from mssql_python.constants import ConstantsDDBC
from mssql_python.cursor import Cursor
from mssql_python.row import Row

FETCH_METHODS = ("fetchone", "fetchmany", "fetchall")
SQL_WVARCHAR = ConstantsDDBC.SQL_WVARCHAR.value
SQL_SS_VARIANT = ConstantsDDBC.SQL_SS_VARIANT.value
UUID_TEXT = "00112233-4455-6677-8899-AABBCCDDEEFF"
MIXED_SELECT = (
    "SELECT CAST('abc' AS VARCHAR(10)) AS narrow, "
    "CAST(N'def' AS NVARCHAR(10)) AS wide, CAST(42 AS INT) AS number, "
    f"CAST('{UUID_TEXT}' AS UNIQUEIDENTIFIER) AS id, "
    "CAST(0x0102 AS VARBINARY(2)) AS binary_value, CAST(NULL AS NVARCHAR(10)) AS empty_value"
)
MIXED_CONVERTER_INPUTS = [b"a\x00b\x00c\x00", b"d\x00e\x00f\x00", b"\x01\x02"]
VARIANT_SELECT = (
    "SELECT value FROM (VALUES "
    "(1, CAST(42 AS SQL_VARIANT)), "
    "(2, CAST(CAST('20260102' AS DATE) AS SQL_VARIANT)), "
    f"(3, CAST(CAST('{UUID_TEXT}' AS UNIQUEIDENTIFIER) AS SQL_VARIANT)), "
    "(4, CAST(CAST(N'abc' AS NVARCHAR(10)) AS SQL_VARIANT)), "
    "(5, CAST(CAST(0x0102 AS VARBINARY(2)) AS SQL_VARIANT)), "
    "(6, CAST(NULL AS SQL_VARIANT))) AS v(n, value) ORDER BY n"
)
VARIANT_VALUES = [
    42,
    datetime.date(2026, 1, 2),
    uuid.UUID(UUID_TEXT),
    "abc",
    b"\x01\x02",
    None,
]


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


def fetch_rows(cursor, method):
    if method == "fetchone":
        row = cursor.fetchone()
        return [] if row is None else [row]
    if method == "fetchmany":
        return cursor.fetchmany(10)
    return cursor.fetchall()


@pytest.mark.parametrize("method", FETCH_METHODS)
@pytest.mark.parametrize("lob", (False, True), ids=("bound", "lob"))
@pytest.mark.parametrize(
    ("sql_type", "literal", "expected"),
    (
        ("INT", "42", 42),
        ("SMALLINT", "42", 42),
        ("BIGINT", "42", 42),
        ("TINYINT", "42", 42),
        ("BIT", "1", True),
        ("REAL", "1.25", 1.25),
        ("FLOAT", "1.25", 1.25),
        ("DATE", "'20260102'", datetime.date(2026, 1, 2)),
        ("DATETIME", "'20260102'", datetime.datetime(2026, 1, 2)),
        ("DATETIME2", "'20260102'", datetime.datetime(2026, 1, 2)),
        ("SMALLDATETIME", "'20260102'", datetime.datetime(2026, 1, 2)),
    ),
)
def test_fixed_width_null_fetch(connection, method, lob, sql_type, literal, expected):
    prefix = "CAST(N'payload' AS NVARCHAR(MAX)), " if lob else ""
    with connection.cursor() as cursor:
        cursor.execute(
            f"SELECT {prefix}CAST(CASE WHEN n = 2 THEN NULL ELSE {literal} END AS {sql_type}) "
            "AS value FROM (VALUES (1), (2)) AS v(n) ORDER BY n"
        )
        rows = fetch_rows(cursor, method)
        if method == "fetchone":
            rows.extend(fetch_rows(cursor, method))
        assert len(rows) == 2
        assert rows[0][-1] == expected
        assert type(rows[0][-1]) is type(expected)
        assert rows[1][-1] is None
        assert not cursor.messages
        assert fetch_rows(cursor, method) == []
        cursor.execute(f"SELECT CAST({literal} AS {sql_type})")
        assert cursor.fetchval() == expected
        assert not cursor.messages


@pytest.mark.parametrize("expression", ("NULL", "OBJECT_ID('tempdb..#missing_fetch_null_table')"))
def test_fetchval_null_expression(connection, expression):
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT {expression}")
        assert cursor.fetchval() is None
        assert not cursor.messages
        cursor.execute("SELECT 42")
        assert cursor.fetchval() == 42


@pytest.mark.parametrize("method", FETCH_METHODS)
@pytest.mark.parametrize("lob", (False, True), ids=("bound", "lob"))
@pytest.mark.parametrize("wide", (False, True), ids=("char", "wchar"))
@pytest.mark.parametrize("when", ("before_execute", "after_execute", "between_fetches"))
def test_decoding_changes_on_existing_cursor(connection, method, lob, wide, when):
    sqltype = mssql_python.SQL_WCHAR if wide else mssql_python.SQL_CHAR
    if wide:
        # Native WCHAR decoding is fixed in this revision; these are value parity controls.
        connection.setdecoding(sqltype, encoding="utf-16be")
    size = "MAX" if lob else "1"
    expression = (
        f"CAST(NCHAR(233) AS NVARCHAR({size}))" if wide else f"CONVERT(VARCHAR({size}), 0xE9)"
    )
    encoding = "utf-16le" if wide else "latin-1"

    with connection.cursor() as cursor:
        if when == "before_execute":
            connection.setdecoding(sqltype, encoding=encoding)
        cursor.execute(f"SELECT {expression} AS txt FROM (VALUES (1), (2)) AS v(n)")
        if when == "between_fetches":
            assert cursor.fetchone() is not None
        if when != "before_execute":
            connection.setdecoding(sqltype, encoding=encoding)
        rows = fetch_rows(cursor, method)
        assert rows
        assert all(row.txt == "\u00e9" for row in rows)


@pytest.mark.parametrize(
    ("method", "bridge_name"),
    (
        ("fetchone", "DDBCSQLFetchOne"),
        ("fetchmany", "DDBCSQLFetchMany"),
        ("fetchall", "DDBCSQLFetchAll"),
    ),
)
def test_wchar_decoding_forwarded_to_live_fetch_bridge(connection, method, bridge_name):
    bridge = getattr(mssql_python.ddbc_bindings, bridge_name)
    with (
        patch.object(connection, "getdecoding", wraps=connection.getdecoding) as reads,
        patch.object(mssql_python.ddbc_bindings, bridge_name, wraps=bridge) as fetch,
        connection.cursor() as cursor,
    ):
        assert reads.call_count == 2
        previous_encoding = "utf-16le"
        expected_reads = 2
        encodings = ("utf-16le", "utf-16be", "utf-16be", "utf-16le", "utf-16le")
        for index, encoding in enumerate(encodings, 1):
            cursor.execute("SELECT CAST(NCHAR(233) AS NVARCHAR(1)) AS txt")
            if encoding != previous_encoding:
                connection.setdecoding(mssql_python.SQL_WCHAR, encoding=encoding)
                expected_reads += 2
            assert fetch_rows(cursor, method)[0].txt == "\u00e9"
            assert fetch.call_count == index
            assert fetch.call_args.args[-4:-1] == ("utf-16le", encoding, mssql_python.SQL_WCHAR)
            assert fetch.call_args.args[-1] is cursor.messages
            assert fetch.call_args.kwargs == {}
            assert reads.call_count == expected_reads
            previous_encoding = encoding
        assert [call.args[0] for call in reads.call_args_list] == [
            mssql_python.SQL_CHAR,
            mssql_python.SQL_WCHAR,
        ] * 3


def test_decoding_cache_reuse_and_multiple_cursors(connection):
    with (
        patch.object(connection, "getdecoding", wraps=connection.getdecoding) as reads,
        patch.object(ddbc, "_FetchOptions", wraps=ddbc._FetchOptions) as options,
        patch.object(ddbc, "_fetchone_with_options", wraps=ddbc._fetchone_with_options) as one,
        patch.object(ddbc, "_fetchmany_with_options", wraps=ddbc._fetchmany_with_options) as many,
    ):
        with connection.cursor() as first, connection.cursor() as second:
            assert reads.call_count == 4
            assert options.call_count == 0
            for cursor in (first, second):
                cursor.execute("SELECT 42")
                assert cursor.fetchall()[0][0] == 42
                assert cursor._cached_fetch_options is None
                cursor.execute("SELECT n FROM (VALUES (1), (2), (3)) AS v(n) ORDER BY n")
                assert cursor.fetchone()[0] == 1
                snapshot = cursor._cached_fetch_options
                assert cursor.fetchmany(1)[0][0] == 2
                assert cursor.fetchall()[0][0] == 3
                assert one.call_args.args[-2] is many.call_args.args[-2] is snapshot
                assert one.call_args.args[-1] is many.call_args.args[-1] is cursor.messages
                assert cursor._cached_fetch_options is snapshot
            assert reads.call_count == 4
            assert options.call_count == one.call_count == many.call_count == 2

            connection.setdecoding(mssql_python.SQL_CHAR, encoding="latin-1")
            for index, cursor in enumerate((first, second), 1):
                snapshot = cursor._cached_fetch_options
                cursor.execute(
                    "SELECT CONVERT(VARCHAR(1), 0xE9) AS txt FROM (VALUES (1), (2), (3)) AS v(n)"
                )
                assert cursor.fetchone().txt == "\u00e9"
                assert cursor.fetchmany(1)[0].txt == "\u00e9"
                assert cursor.fetchall()[0].txt == "\u00e9"
                assert reads.call_count == 4 + 2 * index
                assert options.call_count == 2 + index
                assert cursor._cached_fetch_options is not snapshot


@pytest.mark.parametrize("method", ("fetchone", "fetchmany", "fetchval"))
def test_native_fetch_options_refresh_failure_is_retried(connection, method):
    with connection.cursor() as cursor:
        cursor.execute("SELECT 42")
        assert cursor.fetchone()[0] == 42
        assert cursor._cached_fetch_options is not None
        cursor.execute("SELECT CONVERT(VARCHAR(1), 0xE9) AS txt")
        connection.setdecoding(mssql_python.SQL_CHAR, encoding="latin-1")

        def fetch():
            return cursor.fetchmany(1) if method == "fetchmany" else getattr(cursor, method)()

        with patch.object(ddbc, "_FetchOptions", side_effect=MemoryError("options allocation")):
            with pytest.raises(MemoryError, match="options allocation"):
                fetch()
        assert cursor._cached_decoding_generation == connection._decoding_generation
        assert cursor._cached_fetch_options is None
        assert cursor._next_row_index == 0
        value = fetch()
        actual = (
            value[0][0] if method == "fetchmany" else value if method == "fetchval" else value[0]
        )
        assert actual == "\u00e9"
        assert cursor._cached_fetch_options is not None
        assert cursor.rowcount == 1


@pytest.mark.parametrize("method", FETCH_METHODS)
@pytest.mark.parametrize("native_uuid", (True, False))
@pytest.mark.parametrize("mutation", ("add", "replace", "remove", "clear"))
def test_converter_changes_after_execute(connection, method, native_uuid, mutation, monkeypatch):
    monkeypatch.setattr(mssql_python, "native_uuid", native_uuid)
    original = Mock(side_effect=lambda raw: "original:" + raw.decode("utf-16-le"))
    replacement = Mock(side_effect=lambda raw: "converted:" + raw.decode("utf-16-le"))
    if mutation != "add":
        connection.add_output_converter(SQL_WVARCHAR, original)

    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT CAST(N'abc' AS NVARCHAR(10)) AS txt, "
            f"CAST('{UUID_TEXT}' AS UNIQUEIDENTIFIER) AS id, "
            "CAST(NULL AS NVARCHAR(10)) AS empty_value"
        )
        if mutation in ("add", "replace"):
            connection.add_output_converter(SQL_WVARCHAR, replacement)
        elif mutation == "remove":
            connection.remove_output_converter(SQL_WVARCHAR)
        else:
            connection.clear_output_converters()

        row = fetch_rows(cursor, method)[0]
        assert row.txt == ("converted:abc" if mutation in ("add", "replace") else "abc")
        assert row.id == (uuid.UUID(UUID_TEXT) if native_uuid else UUID_TEXT)
        assert row.empty_value is None
        assert original.call_count == 0
        if mutation in ("add", "replace"):
            replacement.assert_called_once_with(b"a\x00b\x00c\x00")
        else:
            replacement.assert_not_called()


def test_converter_cache_reuse_between_fetches(connection):
    converter = Mock(side_effect=lambda raw: "converted:" + raw.decode("utf-16-le"))
    replacement = Mock(side_effect=lambda raw: "new:" + raw.decode("utf-16-le"))
    with connection.cursor() as cursor:
        with (
            patch.object(
                cursor, "_build_converter_map", wraps=cursor._build_converter_map
            ) as builds,
            patch.object(
                connection, "get_output_converter", wraps=connection.get_output_converter
            ) as lookups,
            patch.object(Row, "_fast_create", wraps=Row._fast_create) as fast_one,
            patch.object(
                mssql_python.ddbc_bindings,
                "construct_rows",
                wraps=mssql_python.ddbc_bindings.construct_rows,
            ) as fast_batch,
        ):
            cursor.execute(
                "SELECT CAST(N'abc' AS NVARCHAR(10)) AS txt "
                "FROM (VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9)) AS v(n)"
            )
            assert builds.call_count == 1
            assert lookups.call_count == 0
            assert cursor.fetchone().txt == "abc"
            assert fast_one.call_count == 1

            connection.add_output_converter(SQL_WVARCHAR, converter)
            assert cursor.fetchone().txt == "converted:abc"
            assert [row.txt for row in cursor.fetchmany(2)] == ["converted:abc"] * 2
            assert converter.call_count == 3
            assert builds.call_count == 2
            assert lookups.call_count == 1

            connection.add_output_converter(SQL_WVARCHAR, replacement)
            assert cursor.fetchone().txt == "new:abc"
            assert builds.call_count == 3
            assert lookups.call_count == 2
            assert replacement.call_count == 1

            connection.remove_output_converter(SQL_WVARCHAR)
            assert cursor.fetchmany(1)[0].txt == "abc"
            assert builds.call_count == 4
            assert lookups.call_count == 2
            assert fast_batch.call_count == 1

            connection.add_output_converter(SQL_WVARCHAR, converter)
            assert cursor.fetchone().txt == "converted:abc"
            assert builds.call_count == 5
            assert lookups.call_count == 3
            connection.clear_output_converters()
            assert [row.txt for row in cursor.fetchall()] == ["abc", "abc"]
            assert builds.call_count == 6
            assert lookups.call_count == 3
            assert fast_batch.call_count == 2


@pytest.mark.parametrize("method", FETCH_METHODS)
@pytest.mark.parametrize("native_uuid", (True, False))
@pytest.mark.parametrize("mutation", ("add", "replace", "remove", "clear"))
def test_late_converter_mixed_values(connection, method, native_uuid, mutation, monkeypatch):
    monkeypatch.setattr(mssql_python, "native_uuid", native_uuid)
    original = Mock(return_value="original")
    replacement = Mock(return_value="converted")
    if mutation != "add":
        connection.add_output_converter(SQL_WVARCHAR, original)

    with connection.cursor() as cursor:
        cursor.execute(MIXED_SELECT)
        if mutation in ("add", "replace"):
            connection.add_output_converter(SQL_WVARCHAR, replacement)
        elif mutation == "remove":
            connection.remove_output_converter(SQL_WVARCHAR)
        else:
            connection.clear_output_converters()
        row = fetch_rows(cursor, method)[0]
        converted = mutation in ("add", "replace")
        assert list(row) == [
            "converted" if converted else "abc",
            "converted" if converted else "def",
            42,
            uuid.UUID(UUID_TEXT) if native_uuid else UUID_TEXT,
            "converted" if converted else b"\x01\x02",
            None,
        ]
        original.assert_not_called()
        assert [call.args[0] for call in replacement.call_args_list] == (
            MIXED_CONVERTER_INPUTS if converted else []
        )


@pytest.mark.parametrize("native_uuid", (True, False))
def test_late_converter_mixed_values_between_fetches(connection, native_uuid, monkeypatch):
    monkeypatch.setattr(mssql_python, "native_uuid", native_uuid)
    converter = Mock(return_value="converted")
    replacement = Mock(return_value="new")
    with connection.cursor() as cursor:
        cursor.execute(MIXED_SELECT + " FROM (VALUES (1), (2), (3), (4), (5)) AS v(n)")
        assert cursor.fetchone().number == 42
        connection.add_output_converter(SQL_WVARCHAR, converter)
        first = cursor.fetchone()
        connection.add_output_converter(SQL_WVARCHAR, replacement)
        second = cursor.fetchmany(1)[0]
        connection.remove_output_converter(SQL_WVARCHAR)
        third = cursor.fetchone()
        connection.add_output_converter(SQL_WVARCHAR, converter)
        connection.clear_output_converters()
        fourth = cursor.fetchall()[0]
        for row, text in ((first, "converted"), (second, "new"), (third, "abc"), (fourth, "abc")):
            assert row.narrow == text
            assert row.number == 42
            assert row.id == (uuid.UUID(UUID_TEXT) if native_uuid else UUID_TEXT)
            assert row.empty_value is None
        assert [call.args[0] for call in converter.call_args_list] == MIXED_CONVERTER_INPUTS
        assert [call.args[0] for call in replacement.call_args_list] == MIXED_CONVERTER_INPUTS


def test_preconfigured_converter_keeps_existing_fallback_semantics(connection):
    converter = Mock(return_value="converted")
    connection.add_output_converter(SQL_WVARCHAR, converter)
    with connection.cursor() as cursor:
        cursor.execute(MIXED_SELECT)
        assert list(cursor.fetchone()) == [
            "converted",
            "converted",
            42,
            uuid.UUID(UUID_TEXT),
            "converted",
            None,
        ]
        assert [call.args[0] for call in converter.call_args_list] == MIXED_CONVERTER_INPUTS


@pytest.mark.parametrize("method", FETCH_METHODS)
@pytest.mark.parametrize("when", ("before_execute", "after_execute", "between_fetches"))
def test_variant_string_fallback_is_value_gated(connection, method, when):
    converter = Mock(return_value="converted")
    with (
        connection.cursor() as cursor,
        patch.object(cursor, "_build_converter_map", wraps=cursor._build_converter_map) as builds,
        patch.object(
            connection, "get_output_converter", wraps=connection.get_output_converter
        ) as lookups,
    ):
        if when == "before_execute":
            connection.add_output_converter(SQL_WVARCHAR, converter)
        cursor.execute(VARIANT_SELECT)
        expected = VARIANT_VALUES[:3] + ["converted", "converted", None]
        if when == "between_fetches":
            assert cursor.fetchone()[0] == 42
            expected = expected[1:]
        if when != "before_execute":
            connection.add_output_converter(SQL_WVARCHAR, converter)
        rows = []
        while batch := fetch_rows(cursor, method):
            rows.extend(batch)
        assert [row[0] for row in rows] == expected
        assert [type(row[0]) for row in rows] == [type(value) for value in expected]
        assert [call.args[0] for call in converter.call_args_list] == [
            b"a\x00b\x00c\x00",
            b"\x01\x02",
        ]
        assert builds.call_count == (1 if when == "before_execute" else 2)
        assert lookups.call_count == 3


@pytest.mark.parametrize("method", FETCH_METHODS)
@pytest.mark.parametrize("explicit_type", ("sql", "python"))
def test_late_variant_explicit_converter_precedence(connection, method, explicit_type):
    fallback = Mock(return_value="fallback")
    python_converter = Mock(return_value="python")
    sql_converter = Mock(return_value="sql")
    with connection.cursor() as cursor:
        cursor.execute(VARIANT_SELECT)
        connection.add_output_converter(SQL_WVARCHAR, fallback)
        connection.add_output_converter(str, python_converter)
        if explicit_type == "sql":
            connection.add_output_converter(SQL_SS_VARIANT, sql_converter)
        rows = []
        while batch := fetch_rows(cursor, method):
            rows.extend(batch)
        assert [row[0] for row in rows] == [explicit_type] * 5 + [None]
        selected = sql_converter if explicit_type == "sql" else python_converter
        assert [call.args[0] for call in selected.call_args_list] == (
            VARIANT_VALUES[:3] + [b"a\x00b\x00c\x00", b"\x01\x02"]
        )
        fallback.assert_not_called()
        if explicit_type == "sql":
            python_converter.assert_not_called()
        else:
            sql_converter.assert_not_called()


def test_unknown_sql_type_string_fallback_is_value_gated(connection):
    converter = Mock(return_value="converted")
    connection.add_output_converter(SQL_WVARCHAR, converter)
    with connection.cursor() as cursor:
        cursor._initialize_description(
            [
                {
                    "ColumnName": "value",
                    "DataType": 123456,
                    "ColumnSize": 100,
                    "DecimalDigits": 0,
                    "Nullable": ConstantsDDBC.SQL_NULLABLE.value,
                }
            ]
        )
        assert cursor.description[0][1] is str
        converter_map = cursor._build_converter_map()
        rows = [Row([value], {"value": 0}, converter_map=converter_map) for value in VARIANT_VALUES]
        assert [row[0] for row in rows] == VARIANT_VALUES[:3] + ["converted", "converted", None]
        assert [call.args[0] for call in converter.call_args_list] == [
            b"a\x00b\x00c\x00",
            b"\x01\x02",
        ]


def test_converter_cache_multiple_cursors_and_result_shapes(connection):
    converter = Mock(side_effect=lambda raw: "converted:" + raw.decode("utf-16-le"))
    with connection.cursor() as first, connection.cursor() as second:
        for cursor in (first, second):
            cursor.execute("SELECT CAST(N'abc' AS NVARCHAR(10)) AS txt")
            assert cursor.fetchone().txt == "abc"
        connection.add_output_converter(SQL_WVARCHAR, converter)
        for cursor in (first, second):
            with patch.object(
                cursor, "_build_converter_map", wraps=cursor._build_converter_map
            ) as builds:
                assert cursor.fetchall() == []
                builds.assert_called_once_with()
            cursor.execute(
                "SELECT CAST(NULL AS INT) AS empty_value, CAST(N'def' AS NVARCHAR(10)) AS txt; "
                "SELECT CAST(N'ghi' AS NVARCHAR(10)) AS renamed"
            )
            row = cursor.fetchone()
            assert list(row) == [None, "converted:def"]
            assert cursor.nextset()
            assert cursor.fetchone().renamed == "converted:ghi"
        assert converter.call_count == 4


@pytest.mark.parametrize("stringify_uuid", (False, True))
def test_direct_row_converter_fallback(connection, stringify_uuid):
    converter = Mock(side_effect=lambda raw: "converted:" + raw.decode("utf-16-le"))
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT CAST(N'abc' AS NVARCHAR(10)) AS txt, "
            f"CAST('{UUID_TEXT}' AS UNIQUEIDENTIFIER) AS id"
        )
        values = list(cursor.fetchone())
        connection.add_output_converter(SQL_WVARCHAR, converter)
        row = Row(
            values,
            {"txt": 0, "id": 1},
            cursor=cursor,
            uuid_str_indices=(1,) if stringify_uuid else None,
        )
        assert row.txt == "converted:abc"
        assert row.id == (UUID_TEXT if stringify_uuid else uuid.UUID(UUID_TEXT))
        converter.assert_called_once_with(b"a\x00b\x00c\x00")


def test_direct_row_without_converters_is_zero_copy():
    values = [1, "abc", None]
    row = Row(values, {"number": 0, "txt": 1, "empty_value": 2})
    assert row._values is values


@pytest.mark.parametrize("container", (list, tuple))
def test_converter_leaf_inputs_and_copy(container):
    text = "A\0\u00e9\U0001f600"
    values = container(
        [
            text,
            b"\0\xff",
            42,
            decimal.Decimal("1.25"),
            datetime.date(2026, 1, 2),
            uuid.UUID(UUID_TEXT),
            None,
        ]
    )
    seen = []

    def convert(value):
        seen.append(value)
        return value

    with patch.object(
        ddbc, "_apply_output_converters", wraps=ddbc._apply_output_converters
    ) as leaf:
        row = Row(values, {}, converter_map=[convert] * len(values), uuid_str_indices=(5,))
    expected = [b"A\0\0\0\xe9\0\x3d\xd8\0\xde", *values[1:-1]]
    assert seen == expected
    assert [type(value) for value in seen] == [type(value) for value in expected]
    assert row._values is not values
    assert list(row) == [*expected[:5], UUID_TEXT, None]
    assert isinstance(values[5], uuid.UUID)
    assert leaf.call_count == int(container is list)
    if container is list:
        values[0] = "changed"
        assert row[0] == expected[0]


@pytest.mark.parametrize("container", (list, tuple))
@pytest.mark.parametrize("map_container", (list, tuple))
@pytest.mark.parametrize("map_length", (1, 4))
def test_converter_leaf_map_length(container, map_container, map_length):
    calls = []
    converters = map_container([lambda value: calls.append(value)] * map_length)
    row = Row(container([1, 2]), {}, converter_map=converters)
    assert calls == ([1] if map_length == 1 else [1, 2])
    assert list(row) == ([None, 2] if map_length == 1 else [None, None])


@pytest.mark.parametrize("container", (list, tuple))
def test_converter_leaf_dynamic_encode(container):
    events = []
    token = object()

    class Text(str):
        def encode(self, encoding):
            events.append((str(self), encoding))
            Text.encode = lambda self, encoding: token
            return b"first"

    class StringProxy:
        __class__ = property(lambda self: str)

        def encode(self, encoding):
            events.append(("proxy", encoding))
            return token

    values = container([Text("one"), Text("two"), StringProxy()])
    row = Row(values, {}, converter_map=[lambda value: value] * 3)
    assert events == [("one", "utf-16-le"), ("proxy", "utf-16-le")]
    assert row[0] == b"first"
    assert row[1] is row[2] is token


def test_converter_leaf_releases_owned_references():
    references = []

    class Value:
        pass

    class Text(str):
        def encode(self, encoding):
            value = Value()
            references.append(weakref.ref(value))
            return value

    class Converter:
        def __call__(self, value):
            assert value is references[-1]()
            result = Value()
            references.append(weakref.ref(result))
            return result

    values, converters = [Text("text")], [Converter()]
    references.extend([weakref.ref(values[0]), weakref.ref(converters[0])])
    row = Row(values, {}, converter_map=converters)
    assert references[2]() is None
    assert references[3]() is row[0]
    del values, converters, row
    gc.collect()
    assert all(reference() is None for reference in references)


@pytest.mark.parametrize("container", (list, tuple))
@pytest.mark.parametrize("stage", ("bool", "encode", "call"))
@pytest.mark.parametrize("error_type", (ValueError, KeyboardInterrupt))
def test_converter_leaf_exception_boundaries(container, stage, error_type):
    class Text(str):
        def encode(self, encoding):
            if stage == "encode":
                raise error_type("converter failure")
            return super().encode(encoding)

    class Converter:
        def __bool__(self):
            if stage == "bool":
                raise error_type("converter failure")
            return True

        def __call__(self, value):
            raise error_type("converter failure")

    values = container([Text("text")])
    if stage == "bool" or error_type is KeyboardInterrupt:
        with pytest.raises(error_type, match="converter failure"):
            Row(values, {}, converter_map=[Converter()])
    else:
        assert Row(values, {}, converter_map=[Converter()])[0] is values[0]


@pytest.mark.parametrize("native", (False, True))
@pytest.mark.parametrize("mutation", ("replace", "grow", "shrink"))
def test_converter_leaf_live_lists(native, mutation):
    events = []
    values = [1, 2, None]

    class Values(list):
        pass

    if not native:
        values = Values(values)

    class Converter:
        def __bool__(self):
            events.append("bool")
            return True

        def __call__(self, value):
            events.append(value)
            if value == 1:
                if mutation == "replace":
                    values[1] = 20
                    converters[1] = lambda value: value + 100
                elif mutation == "grow":
                    values.append(4)
                    converters.append(self)
                else:
                    values.clear()
                    converters.clear()
            return value + 10

    converters = [Converter()] * 3
    row = Row(values, {}, converter_map=converters)
    assert (
        list(row)
        == {
            "replace": [11, 120, None],
            "grow": [11, 12, None],
            "shrink": [11, 2, None],
        }[mutation]
    )
    assert (
        events
        == {
            "replace": ["bool", 1, "bool"],
            "grow": ["bool", 1, "bool", 2, "bool", "bool", 4],
            "shrink": ["bool", 1],
        }[mutation]
    )


def test_converter_leaf_self_replacement_finalizer_order():
    def run(container):
        events = []

        class Converter:
            def __call__(self, value):
                converters[0] = None
                events.append(value)
                return value

            def __del__(self):
                events.append("released")

        converters = [Converter(), events.append, events.append]
        row = Row(container([1, 2, 3]), {}, converter_map=converters)
        return events, list(row)

    assert run(list) == run(tuple)


def test_converter_leaf_codec_lookup_in_subprocess():
    script = textwrap.dedent("""
        import codecs
        import encodings
        from mssql_python.row import Row

        events = []
        def encode(value, errors="strict"):
            events.append((value, errors))
            return b"custom", len(value)
        def search(name):
            if name == "utf_16_le":
                return codecs.CodecInfo(name=name, encode=encode, decode=None)
        codecs.unregister(encodings.search_function)
        codecs.register(search)
        codecs.register(encodings.search_function)
        values = ["A\\0\\u00e9", "\\U0001f600"]
        expected = [value.encode("utf-16-le") for value in values]
        expected_events = events[:]
        events.clear()
        row = Row(values, {}, converter_map=[lambda value: value] * 2)
        assert list(row) == expected
        assert events == expected_events
        """)
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=30
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("method", FETCH_METHODS)
def test_converter_leaf_registry_changes_during_rows(connection, method):
    calls = []

    def replacement(value):
        return value + 100

    def original(value):
        calls.append(value)
        connection.add_output_converter(mssql_python.SQL_INTEGER, replacement)
        return value + 10

    connection.add_output_converter(mssql_python.SQL_INTEGER, original)
    with connection.cursor() as cursor:
        cursor.execute("SELECT n AS a, n + 10 AS b FROM (VALUES (1), (2)) AS v(n) ORDER BY n")
        rows = fetch_rows(cursor, method)
        count = 1 if method == "fetchone" else 2
        assert calls == [value for n in range(1, count + 1) for value in (n, n + 10)]
        assert [list(row) for row in rows] == [[n + 10, n + 20] for n in range(1, count + 1)]
        if method == "fetchone":
            assert list(cursor.fetchone()) == [102, 112]
        cursor.execute("SELECT 3 AS renamed")
        assert cursor.fetchval() == 103
    assert list(rows[0]) == [11, 21]
    assert dict(rows[0]._mapping) == {"a": 11, "b": 21}


@pytest.mark.parametrize("method", ("fetchone", "fetchval"))
def test_converter_leaf_native_error_precedes_callbacks(connection, method):
    calls = []
    connection.add_output_converter(mssql_python.SQL_INTEGER, lambda value: calls.append(value))
    with connection.cursor() as cursor:
        cursor.execute("SELECT 1 AS a, CAST(0x00D8 AS NVARCHAR(10)) AS invalid_utf16")
        with pytest.raises(UnicodeDecodeError):
            getattr(cursor, method)()
        assert calls == []


def test_decoding_cache_refresh_failure_is_retried(connection):
    with connection.cursor() as cursor:
        cursor.execute("SELECT CONVERT(VARCHAR(1), 0xE9) AS txt")
        generation = cursor._cached_decoding_generation
        connection.setdecoding(mssql_python.SQL_CHAR, encoding="latin-1")
        read_settings = connection.getdecoding

        def fail_wchar(sqltype):
            if sqltype == mssql_python.SQL_WCHAR:
                raise RuntimeError("injected settings failure")
            return read_settings(sqltype)

        with patch.object(connection, "getdecoding", side_effect=fail_wchar):
            with pytest.raises(RuntimeError, match="injected settings failure"):
                cursor.fetchone()
        assert cursor._cached_decoding_generation == generation
        assert cursor.fetchone().txt == "\u00e9"


def test_converter_cache_refresh_failure_is_retried(connection):
    with connection.cursor() as cursor:
        cursor.execute("SELECT CAST(N'abc' AS NVARCHAR(10)) AS txt FROM (VALUES (1), (2)) AS v(n)")
        generation = cursor._cached_converters_generation
        connection.add_output_converter(SQL_WVARCHAR, lambda raw: "converted")
        with patch.object(
            connection, "get_output_converter", side_effect=RuntimeError("injected map failure")
        ):
            with pytest.raises(RuntimeError, match="injected map failure"):
                cursor.fetchone()
        assert cursor._cached_converters_generation == generation
        assert cursor.fetchone().txt == "converted"


@pytest.mark.parametrize("method", FETCH_METHODS)
@pytest.mark.parametrize("lowercase", (False, True))
@pytest.mark.parametrize("processing", ("none", "converter", "uuid"))
def test_fetch_preserves_column_name_maps(connection, method, lowercase, processing, monkeypatch):
    monkeypatch.setattr(mssql_python, "lowercase", lowercase)
    monkeypatch.setattr(mssql_python, "native_uuid", processing != "uuid")
    if processing == "converter":
        connection.add_output_converter(SQL_WVARCHAR, lambda raw: "converted")
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT CAST(N'abc' AS NVARCHAR(10)) AS MixedName, "
            f"CAST('{UUID_TEXT}' AS UNIQUEIDENTIFIER) AS MixedId "
            "FROM (VALUES (1), (2)) AS v(n)"
        )
        rows = fetch_rows(cursor, method)
        if method == "fetchone":
            rows.extend(fetch_rows(cursor, method))
        row = rows[0]
        name = "mixedname" if lowercase else "MixedName"
        expected = "converted" if processing == "converter" else "abc"
        assert row[name] == getattr(row, name) == expected
        if lowercase:
            assert row["MIXEDNAME"] == row.MIXEDNAME == expected
        else:
            with pytest.raises(KeyError):
                row["MIXEDNAME"]
            with pytest.raises(AttributeError):
                row.MIXEDNAME
        assert row._column_map_lower is cursor._cached_column_map_lower
        assert row[1] == (UUID_TEXT if processing == "uuid" else uuid.UUID(UUID_TEXT))
        id_name = "mixedid" if lowercase else "MixedId"
        names = cursor._cached_result_columns
        assert names == (name, id_name)
        assert len(rows) == 2
        assert all(item._column_names is names for item in rows)
        expected_mapping = {name: expected, id_name: row[1]}
        assert all(dict(item._mapping) == expected_mapping for item in rows)
        cursor.execute("SELECT 42 AS replacement")
        replacement = fetch_rows(cursor, method)[0]
        assert replacement._column_names is cursor._cached_result_columns
        assert replacement._column_names is not names
        assert dict(replacement._mapping) == {"replacement": 42}
    assert all(dict(item._mapping) == expected_mapping for item in rows)


@pytest.mark.parametrize("native", (False, True))
def test_fast_row_without_column_snapshot_mapping(native):
    values = [1, "abc"]
    column_map = {"number": 0, "text": 1}
    if native:
        row = mssql_python.ddbc_bindings.construct_rows([values], Row, column_map, None)[0]
    else:
        row = Row._fast_create(values, column_map, None)
    assert row._values is values
    assert row._column_names is None
    assert dict(row._mapping) == {"number": 1, "text": "abc"}


def assert_row_instance_capabilities(row):
    row.metadata = "initial"
    assert vars(row)["metadata"] == "initial"
    vars(row)["metadata"] = "updated"
    assert row.metadata == "updated"
    del row.metadata
    assert not hasattr(row, "metadata")
    assert row.number == row["number"] == row[0] == 42
    assert dict(row._mapping) == {"number": 42}
    reference = weakref.ref(row)
    assert reference() is row
    return reference


@pytest.mark.parametrize("construction", ("direct", "python_fast", "native", "native_single"))
def test_constructed_row_preserves_instance_capabilities(construction):
    values = [42]
    column_map = {"number": 0}
    if construction == "direct":
        row = Row(values, column_map)
    elif construction == "python_fast":
        row = Row._fast_create(values, column_map, None)
    elif construction == "native_single":
        row = mssql_python.ddbc_bindings.construct_row(values, Row, column_map, None)
    else:
        row = mssql_python.ddbc_bindings.construct_rows([values], Row, column_map, None)[0]
    assert row._values is values
    reference = assert_row_instance_capabilities(row)
    del row
    assert reference() is None


@pytest.mark.parametrize("method", FETCH_METHODS)
def test_fetched_row_preserves_instance_capabilities(connection, method):
    with connection.cursor() as cursor:
        cursor.execute("SELECT 42 AS number")
        row = fetch_rows(cursor, method)[0]
    reference = assert_row_instance_capabilities(row)
    del row
    assert reference() is None


@pytest.mark.parametrize(
    ("size", "single"),
    ((0, False), (1, False), (3, False), (1, True)),
    ids=("0", "1", "3", "native-single"),
)
def test_construct_rows_repeated_calls_release_references(size, single):
    values = [[index] for index in range(size)]
    column_map = {"Number": 0}
    column_map_lower = {"number": 0}
    column_names = ("Number",)
    cursor = object()
    tracked = (values, column_map, column_map_lower, column_names, cursor, *values)
    references = [sys.getrefcount(value) for value in tracked]
    for _ in range(10):
        if single:
            rows = [
                mssql_python.ddbc_bindings.construct_row(
                    values[0], Row, column_map, cursor, column_map_lower, column_names
                )
            ]
        else:
            rows = mssql_python.ddbc_bindings.construct_rows(
                values, Row, column_map, cursor, column_map_lower, column_names
            )
        assert len(rows) == size
        assert all(row._values is values[index] for index, row in enumerate(rows))
        assert all(row._column_map is column_map for row in rows)
        assert all(row._column_map_lower is column_map_lower for row in rows)
        assert all(row._column_names is column_names for row in rows)
        assert all(row._cursor is cursor for row in rows)
        del rows
        assert [sys.getrefcount(value) for value in tracked] == references


def test_construct_rows_releases_partial_batch_on_attribute_error():
    _assert_construct_row_attribute_failure(single=False)


def test_construct_single_row_releases_references_on_attribute_error():
    _assert_construct_row_attribute_failure(single=True)


def _assert_construct_row_attribute_failure(single):
    class FailingRow(Row):
        __slots__ = ()

        @property
        def _column_names(self):
            return None

        @_column_names.setter
        def _column_names(self, names):
            if self._values[0] == 2:
                raise RuntimeError("injected slot assignment failure")

    values = [[1], [2]]
    column_map = {"number": 0}
    column_names = ("number",)
    cursor = object()
    tracked = (values, column_map, column_names, cursor, *values)
    references = [sys.getrefcount(value) for value in tracked]
    for _ in range(10):
        with pytest.raises(RuntimeError, match="injected slot assignment failure"):
            if single:
                mssql_python.ddbc_bindings.construct_row(
                    values[-1], FailingRow, column_map, cursor, None, column_names
                )
            else:
                mssql_python.ddbc_bindings.construct_rows(
                    values, FailingRow, column_map, cursor, None, column_names
                )
        assert [sys.getrefcount(value) for value in tracked] == references


def test_construct_rows_accepts_row_subclasses():
    class CustomRow(Row):
        __slots__ = ()

    values = [42]
    row = mssql_python.ddbc_bindings.construct_rows([values], CustomRow, {"number": 0}, None)[0]
    assert type(row) is CustomRow
    assert row._values is values
    reference = assert_row_instance_capabilities(row)
    del row
    assert reference() is None


@pytest.mark.parametrize(
    ("method", "bridge_name"),
    (
        ("fetchone", "DDBCSQLFetchOne"),
        ("fetchmany", "DDBCSQLFetchMany"),
        ("fetchall", "DDBCSQLFetchAll"),
    ),
)
def test_char_decoding_ctype_refresh(connection, method, bridge_name):
    bridge = getattr(mssql_python.ddbc_bindings, bridge_name)
    with (
        patch.object(connection, "getdecoding", wraps=connection.getdecoding) as reads,
        patch.object(mssql_python.ddbc_bindings, bridge_name, wraps=bridge) as fetch,
        connection.cursor() as cursor,
    ):
        for encoding, ctype in (
            ("utf-16le", mssql_python.SQL_WCHAR),
            ("latin-1", mssql_python.SQL_CHAR),
            ("utf-16le", mssql_python.SQL_WCHAR),
        ):
            cursor.execute("SELECT CONVERT(VARCHAR(1), 0xE9) AS txt")
            connection.setdecoding(mssql_python.SQL_CHAR, encoding=encoding, ctype=ctype)
            assert fetch_rows(cursor, method)[0].txt == "\u00e9"
            assert fetch.call_args.args[-4:-1] == (encoding, "utf-16le", ctype)
            assert fetch.call_args.args[-1] is cursor.messages
            assert fetch.call_args.kwargs == {}
        assert reads.call_count == 8


@pytest.mark.parametrize("invalid_type", ("None", "object()", "42", "'Row'"))
@pytest.mark.parametrize("rows", ("[]", "[[1]]"))
def test_construct_rows_rejects_non_types_in_subprocess(invalid_type, rows):
    code = f"""
import sys
if sys.platform == "win32":
    import ctypes
    ctypes.windll.kernel32.SetErrorMode(0x0001 | 0x0002)
from mssql_python import ddbc_bindings
try:
    ddbc_bindings.construct_rows({rows}, {invalid_type}, {{}}, None)
except TypeError as error:
    assert str(error) == "row_class must be a type", str(error)
else:
    raise AssertionError("Expected TypeError")
"""
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, timeout=30
    )
    assert result.returncode == 0, (result.returncode, result.stdout, result.stderr)


@pytest.mark.parametrize(
    "unrelated_type",
    ("types.FunctionType", "types.CodeType", "types.SimpleNamespace", "object", "int", "dict"),
)
@pytest.mark.parametrize("rows", ("[]", "[[1]]"))
def test_construct_rows_rejects_unrelated_types_in_subprocess(unrelated_type, rows):
    code = f"""
import sys
import types
if sys.platform == "win32":
    import ctypes
    ctypes.windll.kernel32.SetErrorMode(0x0001 | 0x0002)
from mssql_python import ddbc_bindings
try:
    ddbc_bindings.construct_rows({rows}, {unrelated_type}, {{}}, None)
except TypeError as error:
    assert str(error) == "row_class must be Row or a Row subclass", str(error)
else:
    raise AssertionError("Expected TypeError")
"""
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, timeout=30
    )
    assert result.returncode == 0, (result.returncode, result.stdout, result.stderr)


@pytest.mark.parametrize("method", FETCH_METHODS)
@pytest.mark.parametrize("stringify_uuid", (False, True))
def test_unrelated_converter_preserves_zero_copy_fast_path(
    connection, method, stringify_uuid, monkeypatch
):
    monkeypatch.setattr(mssql_python, "native_uuid", not stringify_uuid)
    converter = Mock(return_value="unexpected")
    connection.add_output_converter(ConstantsDDBC.SQL_INTEGER.value, converter)
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT CAST(N'abc' AS NVARCHAR(10)) AS txt, "
            f"CAST('{UUID_TEXT}' AS UNIQUEIDENTIFIER) AS id"
        )
        with (
            patch.object(Row, "_fast_create", wraps=Row._fast_create) as fast_one,
            patch.object(
                mssql_python.ddbc_bindings,
                "construct_rows",
                wraps=mssql_python.ddbc_bindings.construct_rows,
            ) as fast_batch,
            patch.object(
                Row, "_apply_output_converters", side_effect=AssertionError("per-row lookup")
            ),
            patch.object(
                Row,
                "_apply_output_converters_optimized",
                side_effect=AssertionError("unnecessary row copy"),
            ),
            patch.object(
                connection, "get_output_converter", wraps=connection.get_output_converter
            ) as lookups,
        ):
            row = fetch_rows(cursor, method)[0]
            assert row.txt == "abc"
            assert row.id == (UUID_TEXT if stringify_uuid else uuid.UUID(UUID_TEXT))
            assert lookups.call_count == 0
            assert fast_one.call_count == int(not stringify_uuid and method == "fetchone")
            assert fast_batch.call_count == int(not stringify_uuid and method != "fetchone")
        converter.assert_not_called()


@pytest.mark.parametrize(
    ("method", "bridge_name"),
    (
        ("fetchone", "DDBCSQLFetchOne"),
        ("fetchmany", "DDBCSQLFetchMany"),
        ("fetchall", "DDBCSQLFetchAll"),
    ),
)
@pytest.mark.parametrize(
    "status",
    (
        ConstantsDDBC.SQL_SUCCESS.value,
        ConstantsDDBC.SQL_SUCCESS_WITH_INFO.value,
        ConstantsDDBC.SQL_NO_DATA.value,
    ),
)
def test_fetch_drains_diagnostics_independent_of_final_status(
    connection, method, bridge_name, status
):
    bridge = getattr(mssql_python.ddbc_bindings, bridge_name)
    warning = ("[01000] (0)", "injected fetch warning")
    with connection.cursor() as cursor:
        cursor.execute("SELECT CAST(N'abc' AS NVARCHAR(MAX)) AS txt")

        def fetch_with_final_status(*args):
            bridge(*args)
            assert args[-1] is cursor.messages
            args[-1].append(warning)
            return status

        with (
            patch.object(
                mssql_python.ddbc_bindings, bridge_name, side_effect=fetch_with_final_status
            ),
            patch.object(
                mssql_python.ddbc_bindings, "DDBCSQLGetAllDiagRecords", return_value=[warning]
            ) as diagnostics,
        ):
            fetch_rows(cursor, method)
            diagnostics.assert_not_called()
            assert cursor.messages == [warning]


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Windows does not export the native diagnostic function-pointer globals",
)
@pytest.mark.parametrize("mode", ("available", "missing", "error", "info", "unterminated"))
def test_native_mixed_fetch_diagnostics(conn_str, mode):
    if not conn_str:
        pytest.skip("DB_CONNECTION_STRING is required")
    # Driver pointers are process-global: never replace them in the pytest process.
    code = (
        "import runpy, sys; "
        "runpy.run_path(sys.argv[1])['_check_native_mixed_fetch_diagnostics']"
        "(sys.argv[2], sys.argv[3])"
    )
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            code,
            str(Path(__file__).resolve()),
            mode,
            str(Path(mssql_python.ddbc_bindings.module.__file__).resolve()),
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, (result.returncode, result.stdout, result.stderr)


def _check_native_mixed_fetch_diagnostics(mode, expected_native):
    import ctypes
    import os

    native = Path(mssql_python.ddbc_bindings.module.__file__).resolve()
    assert native == Path(expected_native)
    library = ctypes.CDLL(str(native))
    rec_pointer = ctypes.c_void_p.in_dll(library, "SQLGetDiagRec_ptr")
    field_pointer = ctypes.c_void_p.in_dll(library, "SQLGetDiagField_ptr")
    smallint = ctypes.c_short
    wchar_pointer = ctypes.POINTER(ctypes.c_uint16)
    smallint_pointer = ctypes.POINTER(smallint)
    rec_type = ctypes.CFUNCTYPE(
        smallint,
        smallint,
        ctypes.c_void_p,
        smallint,
        wchar_pointer,
        ctypes.POINTER(ctypes.c_int32),
        wchar_pointer,
        smallint,
        smallint_pointer,
    )
    field_type = ctypes.CFUNCTYPE(
        smallint,
        smallint,
        ctypes.c_void_p,
        smallint,
        smallint,
        ctypes.c_void_p,
        smallint,
        smallint_pointer,
    )
    success = ConstantsDDBC.SQL_SUCCESS.value
    info = ConstantsDDBC.SQL_SUCCESS_WITH_INFO.value
    no_data = ConstantsDDBC.SQL_NO_DATA.value
    error = ConstantsDDBC.SQL_ERROR.value
    records = [
        ("01000", 11, "before truncation \u00e9"),
        ("01004", 0, "internal first chunk"),
        ("01S07", 22, "between truncations"),
        ("01004", 0, "internal second chunk"),
        ("01000", 33, "after truncation"),
    ]
    all_records = [(f"[{state}] ({number})", message) for state, number, message in records]
    wanted = [all_records[index] for index in (0, 2, 4)]
    rec_calls, field_calls, callback_errors = [], [], []
    observed_handles, delegated_records = [], []
    phase, target_handle = "observe", None

    def guarded(callback_type):
        def decorate(function):
            def boundary(*args):
                try:
                    return function(*args)
                except BaseException as failure:
                    # ctypes otherwise prints and suppresses exceptions crossing the C boundary.
                    callback_errors.append(type(failure).__name__)
                    return error

            return callback_type(boundary)

        return decorate

    @guarded(rec_type)
    def read_record(handle_type, handle, number, state, native_error, message, capacity, length):
        if phase == "observe":
            observed_handles.append((handle_type, handle))
        if phase != "inject" or (handle_type, handle) != target_handle:
            delegated_records.append((handle_type, handle, number))
            return original_read_record(
                handle_type, handle, number, state, native_error, message, capacity, length
            )
        rec_calls.append(number)
        if not handle or handle_type != ConstantsDDBC.SQL_HANDLE_STMT.value or number < 1:
            callback_errors.append("invalid record lookup")
            return error
        if number > len(records):
            return no_data
        sqlstate, code, text = records[number - 1]
        encoded = text.encode("utf-16le")
        if (
            not state
            or not native_error
            or not message
            or not length
            or capacity <= len(encoded) // 2
        ):
            callback_errors.append("invalid record output buffer")
            return error
        ctypes.memmove(state, (sqlstate + "\0").encode("utf-16le"), 12)
        ctypes.memmove(message, encoded + b"\0\0", len(encoded) + 2)
        native_error[0] = code
        length[0] = len(encoded) // 2
        return success

    @guarded(field_type)
    def read_state(handle_type, handle, number, identifier, output, capacity, length):
        if phase != "inject" or (handle_type, handle) != target_handle:
            if original_read_state is None:
                return error
            return original_read_state(
                handle_type, handle, number, identifier, output, capacity, length
            )
        field_calls.append(number)
        # SQL_DIAG_SQLSTATE uses bytes, including the sixth SQLWCHAR terminator.
        if (
            not handle
            or handle_type != ConstantsDDBC.SQL_HANDLE_STMT.value
            or number < 1
            or identifier != 4
            or not output
            or capacity != 12
        ):
            callback_errors.append("invalid SQLSTATE lookup or byte capacity")
            return error
        if mode == "error":
            return error
        if number > len(records):
            return no_data
        sqlstate = records[number - 1][0] + "\0"
        if mode == "info":
            sqlstate = "01004\0"
        elif mode == "unterminated":
            sqlstate = "01004X"
        ctypes.memmove(output, sqlstate.encode("utf-16le"), 12)
        return info if mode == "info" else success

    try:
        connection = mssql_python.connect(os.environ["DB_CONNECTION_STRING"], timeout=5)
    except mssql_python.Error as failure:
        raise AssertionError(
            f"Connection failed: {type(failure).__name__}; connection details withheld"
        ) from None
    with connection, connection.cursor() as cursor, connection.cursor() as other_cursor:
        cursor.execute("SELECT CAST(REPLICATE(CAST('x' AS VARCHAR(MAX)), 8193) AS VARBINARY(MAX))")
        original_rec, original_field = rec_pointer.value, field_pointer.value
        assert original_rec, "Driver diagnostic records must be available"
        original_read_record = rec_type(original_rec)
        original_read_state = field_type(original_field) if original_field else None
        try:
            rec_pointer.value = ctypes.cast(read_record, ctypes.c_void_p).value
            # Learn this cursor's raw handle while forwarding the diagnostic call unchanged.
            assert mssql_python.ddbc_bindings.DDBCSQLGetAllDiagRecords(cursor.hstmt) == []
            assert not callback_errors, callback_errors
            assert len(observed_handles) == 1
            target_handle = observed_handles[0]
            assert target_handle[0] == ConstantsDDBC.SQL_HANDLE_STMT.value and target_handle[1]
            phase = "inject"
            field_pointer.value = (
                None if mode == "missing" else ctypes.cast(read_state, ctypes.c_void_p).value
            )
            delegated_records.clear()
            assert mssql_python.ddbc_bindings.DDBCSQLGetAllDiagRecords(other_cursor.hstmt) == []
            assert len(delegated_records) == 1
            assert delegated_records[0][1] != target_handle[1]
            # One real SQLGetData continuation runs the compiled native mixed-record filter.
            assert tuple(cursor.fetchone()) == (b"x" * 8193,)
            assert not callback_errors, callback_errors
            assert cursor.messages == wanted
            assert field_calls == ([] if mode == "missing" else [1, 2, 3, 4, 5, 6])
            if mode == "available":
                assert rec_calls == [1, 3, 5]
            else:
                assert rec_calls == list(range(1, 7 if mode in ("missing", "error") else 6))

            rec_calls.clear()
            field_calls.clear()
            assert mssql_python.ddbc_bindings.DDBCSQLGetAllDiagRecords(cursor.hstmt) == all_records
            assert not callback_errors, callback_errors
            assert rec_calls == [1, 2, 3, 4, 5, 6]
            assert field_calls == []
            delegated_records.clear()
            phase = "delegate"
            assert cursor.fetchone() is None
            assert not callback_errors, callback_errors
            assert delegated_records == [(*target_handle, 1)]
            assert cursor.messages == wanted
        finally:
            rec_pointer.value, field_pointer.value = original_rec, original_field
        cursor.execute("SELECT 42")
        assert tuple(cursor.fetchone()) == (42,)


@pytest.mark.parametrize(
    ("method", "bridge_name"),
    (
        ("fetchone", "DDBCSQLFetchOne"),
        ("fetchmany", "DDBCSQLFetchMany"),
        ("fetchall", "DDBCSQLFetchAll"),
    ),
)
def test_fetch_error_is_raised_before_wrapping_rows(connection, method, bridge_name):
    from types import SimpleNamespace

    with connection.cursor() as cursor:
        cursor.execute("SELECT 1 AS number")
        position = cursor._next_row_index
        with (
            patch.object(
                mssql_python.ddbc_bindings,
                bridge_name,
                return_value=ConstantsDDBC.SQL_ERROR.value,
            ),
            patch.object(
                mssql_python.ddbc_bindings,
                "DDBCSQLCheckError",
                return_value=SimpleNamespace(sqlState="HY000", ddbcErrorMsg="injected fetch error"),
            ),
        ):
            with pytest.raises(mssql_python.DatabaseError, match="injected fetch error"):
                fetch_rows(cursor, method)
        assert cursor._next_row_index == position
        assert tuple(fetch_rows(cursor, method)[0]) == (1,)


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Windows does not export the native driver function-pointer globals",
)
@pytest.mark.parametrize(
    "mode",
    (
        "warm",
        "prefix",
        "shapes",
        "nextset",
        "fetch-error",
        "count-error",
        "decode-error",
        "generation",
        "mixed",
    ),
)
def test_native_fetchone_full_column_count_cache(conn_str, mode):
    if not conn_str:
        pytest.skip("DB_CONNECTION_STRING is required")
    code = (
        "import runpy, sys; "
        "runpy.run_path(sys.argv[1])['_check_native_fetchone_full_column_count_cache']"
        "(sys.argv[2], sys.argv[3])"
    )
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            code,
            str(Path(__file__).resolve()),
            mode,
            str(Path(mssql_python.ddbc_bindings.module.__file__).resolve()),
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, (result.returncode, result.stdout, result.stderr)


def _check_native_fetchone_full_column_count_cache(mode, expected_native):
    import ctypes
    import os

    native = Path(mssql_python.ddbc_bindings.module.__file__).resolve()
    assert native == Path(expected_native)
    library = ctypes.CDLL(str(native))
    count_pointer = ctypes.c_void_p.in_dll(library, "SQLNumResultCols_ptr")
    fetch_pointer = ctypes.c_void_p.in_dll(library, "SQLFetch_ptr")
    count_type = ctypes.CFUNCTYPE(ctypes.c_short, ctypes.c_void_p, ctypes.POINTER(ctypes.c_short))
    fetch_type = ctypes.CFUNCTYPE(ctypes.c_short, ctypes.c_void_p)
    success = ConstantsDDBC.SQL_SUCCESS.value
    error = ConstantsDDBC.SQL_ERROR.value
    ddbc = mssql_python.ddbc_bindings
    count_calls, callback_errors = [], []
    fail_fetch = fail_count = invalidate_count = False

    @count_type
    def count_columns(handle, count):
        nonlocal fail_count, invalidate_count
        try:
            count_calls.append(handle)
            if fail_count:
                fail_count = False
                return error
            result = original_count(handle, count)
            if invalidate_count:
                invalidate_count = False
                status = ddbc.DDBCSQLSetStmtAttr(
                    cursor.hstmt, ConstantsDDBC.SQL_ATTR_QUERY_TIMEOUT.value, 0
                )
                if status != success:
                    callback_errors.append("Failed to invalidate the metadata generation")
                    return error
            return result
        except BaseException as failure:
            callback_errors.append(type(failure).__name__)
            return error

    @fetch_type
    def fetch_row(handle):
        nonlocal fail_fetch
        try:
            if fail_fetch:
                fail_fetch = False
                return error
            return original_fetch(handle)
        except BaseException as failure:
            callback_errors.append(type(failure).__name__)
            return error

    try:
        connection = mssql_python.connect(os.environ["DB_CONNECTION_STRING"], timeout=5)
    except mssql_python.Error as failure:
        raise AssertionError(
            f"Connection failed: {type(failure).__name__}; connection details withheld"
        ) from None
    with connection, connection.cursor() as cursor:
        query = (
            "SELECT n AS number, CAST(N'text' AS NVARCHAR(10)) AS txt "
            "FROM (VALUES (1), (2), (3), (4), (5), (6)) AS v(n) ORDER BY n"
        )
        cursor.execute(query)
        saved_count, saved_fetch = count_pointer.value, fetch_pointer.value
        assert saved_count and saved_fetch
        original_count, original_fetch = count_type(saved_count), fetch_type(saved_fetch)
        try:
            count_pointer.value = ctypes.cast(count_columns, ctypes.c_void_p).value
            fetch_pointer.value = ctypes.cast(fetch_row, ctypes.c_void_p).value
            if mode == "prefix":
                assert ddbc.DDBCSQLFetch(cursor.hstmt) == success
                prefix = []
                assert (
                    ddbc.DDBCSQLGetData(
                        cursor.hstmt,
                        1,
                        prefix,
                        "utf-16le",
                        "utf-16le",
                        ConstantsDDBC.SQL_C_WCHAR.value,
                    )
                    == success
                )
                assert prefix == [1]
                assert count_calls == []
                assert tuple(cursor.fetchone()) == (2, "text")
                assert len(count_calls) == 1
                assert tuple(cursor.fetchone()) == (3, "text")
                assert len(count_calls) == 1
            elif mode == "shapes":
                for sql, expected in (
                    ("SELECT 7 AS number", (7,)),
                    ("SELECT CAST(N'new' AS NVARCHAR(10)) AS txt", ("new",)),
                    ("SELECT CAST(NULL AS INT) AS empty_value, 9 AS number", (None, 9)),
                ):
                    cursor.execute(sql + " FROM (VALUES (1), (2)) AS v(n)")
                    count_calls.clear()
                    for _ in range(2):
                        row = tuple(cursor.fetchone())
                        assert row == expected
                        assert tuple(map(type, row)) == tuple(map(type, expected))
                    assert len(count_calls) == 1
                    assert cursor.fetchone() is None
                    assert len(count_calls) == 1
            elif mode == "nextset":
                cursor.execute(
                    "SELECT 1 AS number FROM (VALUES (1), (2)) AS v(n); "
                    "SELECT CAST(N'changed' AS NVARCHAR(10)) AS txt "
                    "FROM (VALUES (1), (2)) AS v(n)"
                )
                count_calls.clear()
                assert [cursor.fetchone()[0] for _ in range(2)] == [1, 1]
                assert len(count_calls) == 1
                assert cursor.nextset()
                count_calls.clear()
                assert [cursor.fetchone()[0] for _ in range(2)] == ["changed", "changed"]
                assert len(count_calls) == 1
            elif mode == "fetch-error":
                assert tuple(cursor.fetchone()) == (1, "text")
                assert len(count_calls) == 1
                fail_fetch = True
                row = []
                assert ddbc.DDBCSQLFetchOne(cursor.hstmt, row) == error
                assert row == []
                assert len(count_calls) == 1
                assert tuple(cursor.fetchone()) == (2, "text")
                assert len(count_calls) == 2
                assert tuple(cursor.fetchone()) == (3, "text")
                assert len(count_calls) == 2
            elif mode == "count-error":
                fail_count = True
                with pytest.raises(mssql_python.DatabaseError):
                    ddbc.DDBCSQLFetchOne(cursor.hstmt, [])
                assert len(count_calls) == 1
                assert tuple(cursor.fetchone()) == (2, "text")
                assert len(count_calls) == 2
                assert tuple(cursor.fetchone()) == (3, "text")
                assert len(count_calls) == 2
            elif mode == "decode-error":
                cursor.execute(
                    "SELECT CASE WHEN n = 2 THEN CAST(0x00D8 AS NVARCHAR(10)) "
                    "ELSE CAST(N'ok' AS NVARCHAR(10)) END AS txt "
                    "FROM (VALUES (1), (2), (3), (4)) AS v(n) ORDER BY n"
                )
                count_calls.clear()
                assert cursor.fetchone()[0] == "ok"
                with pytest.raises(UnicodeDecodeError):
                    cursor.fetchone()
                assert len(count_calls) == 1
                assert [cursor.fetchone()[0] for _ in range(2)] == ["ok", "ok"]
                assert len(count_calls) == 2
            elif mode == "generation":
                invalidate_count = True
                assert tuple(cursor.fetchone()) == (1, "text")
                assert len(count_calls) == 1
                assert tuple(cursor.fetchone()) == (2, "text")
                assert len(count_calls) == 2
                assert tuple(cursor.fetchone()) == (3, "text")
                assert len(count_calls) == 2
            elif mode == "mixed":
                assert [tuple(row) for row in cursor.fetchmany(2)] == [(1, "text"), (2, "text")]
                count_calls.clear()
                assert tuple(cursor.fetchone()) == (3, "text")
                assert len(count_calls) == 1
                assert tuple(cursor.fetchmany(1)[0]) == (4, "text")
                count_calls.clear()
                assert tuple(cursor.fetchone()) == (5, "text")
                assert count_calls == []
                assert [tuple(row) for row in cursor.fetchall()] == [(6, "text")]
                count_calls.clear()
                assert cursor.fetchone() is None
                assert count_calls == []
                cursor.execute(query)
                try:
                    other_connection = mssql_python.connect(
                        os.environ["DB_CONNECTION_STRING"], timeout=5
                    )
                except mssql_python.Error as failure:
                    raise AssertionError(
                        f"Connection failed: {type(failure).__name__}; connection details withheld"
                    ) from None
                with other_connection, other_connection.cursor() as other:
                    other.execute("SELECT CAST(N'other' AS NVARCHAR(10))")
                    count_calls.clear()
                    assert cursor.fetchone()[0] == 1
                    assert other.fetchone()[0] == "other"
                    assert cursor.fetchone()[0] == 2
                    assert len(count_calls) == 2
            else:
                assert mode == "warm"
                assert [tuple(cursor.fetchone()) for _ in range(6)] == [
                    (n, "text") for n in range(1, 7)
                ]
                assert len(count_calls) == 1
                assert cursor.fetchone() is None
                assert len(count_calls) == 1
                # Direct callers still perform a real query on every invocation.
                assert ddbc.DDBCSQLNumResultCols(cursor.hstmt) == 2
                assert ddbc.DDBCSQLNumResultCols(cursor.hstmt) == 2
                assert len(count_calls) == 3
            assert not callback_errors, callback_errors
            assert not cursor.messages
        finally:
            count_pointer.value, fetch_pointer.value = saved_count, saved_fetch
        cursor.execute("SELECT 42")
        assert cursor.fetchone()[0] == 42


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


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Windows does not export the native driver function-pointer globals",
)
@pytest.mark.parametrize("method", ["fetchone", "fetchmany", "fetchval"])
def test_first_numeric_getdata_error_stops_before_second_column(conn_str, method):
    script = textwrap.dedent("""
        import ctypes as c
        import os
        import sys
        from types import SimpleNamespace
        from unittest.mock import patch
        import mssql_python
        from mssql_python import ddbc_bindings as ddbc

        pointer, short, length = c.c_void_p, c.c_short, c.c_ssize_t
        get_type = c.CFUNCTYPE(short, pointer, c.c_ushort, short, pointer, length, pointer)
        library = c.CDLL(ddbc.module.__file__)
        slot = pointer.in_dll(library, "SQLGetData_ptr")
        calls = []

        @get_type
        def getdata(handle, column, ctype, buffer, capacity, indicator):
            calls.append(column)
            if column == 1:
                return -1
            return original(handle, column, ctype, buffer, capacity, indicator)

        with mssql_python.connect(os.environ["DB_CONNECTION_STRING"]) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT CAST(7 AS INT), CAST(8 AS INT)")
                position = cursor._next_row_index
                saved = slot.value
                assert saved
                original = get_type(saved)
                diagnostic = SimpleNamespace(sqlState="HY000", ddbcErrorMsg="first column failed")
                with patch.object(ddbc, "DDBCSQLCheckError", return_value=diagnostic):
                    slot.value = c.cast(getdata, pointer).value
                    try:
                        method = sys.argv[1]
                        try:
                            getattr(cursor, method)(*([1] if method == "fetchmany" else []))
                        except mssql_python.DatabaseError as error:
                            assert "first column failed" in str(error)
                        else:
                            raise AssertionError("first-column SQL_ERROR was masked")
                    finally:
                        slot.value = saved
                assert calls == [1], calls
                assert cursor._next_row_index == position
                cursor.execute("SELECT 42, 43")
                assert tuple(cursor.fetchone()) == (42, 43)
        """)
    _run_fetch_script(conn_str, script, method)


@pytest.mark.parametrize(
    ("sql_type", "literal", "expected"),
    (
        ("INT", "-2147483648", -2147483648),
        ("INT", "2147483647", 2147483647),
        ("SMALLINT", "-32768", -32768),
        ("SMALLINT", "32767", 32767),
        ("BIGINT", "-9223372036854775808", -9223372036854775808),
        ("BIGINT", "9223372036854775807", 9223372036854775807),
        ("TINYINT", "255", 255),
        ("BIT", "0", False),
        ("REAL", "-1.25", -1.25),
        ("FLOAT", "1.7976931348623157E308", 1.7976931348623157e308),
        ("FLOAT", "-2.2250738585072014E-308", -2.2250738585072014e-308),
        ("FLOAT", "-0.0", 0.0),
    ),
    ids=(
        "int_min",
        "int_max",
        "smallint_min",
        "smallint_max",
        "bigint_min",
        "bigint_max",
        "tinyint_max",
        "bit_zero",
        "real_negative",
        "float_max",
        "float_tiny",
        "float_zero",
    ),
)
def test_single_numeric_row_bound_path_parity(cursor, sql_type, literal, expected):
    query = f"SELECT CAST({literal} AS {sql_type}) AS a, CAST(NULL AS {sql_type}) AS b"
    cursor.execute(query)
    bound = cursor.fetchmany(2)[0]
    cursor.execute(query)
    single = cursor.fetchmany(1)[0]
    assert tuple(single) == tuple(bound) == (expected, None)
    assert type(single[0]) is type(bound[0]) is type(expected)


def test_single_row_converters_run_in_column_order(cursor):
    events = []

    def convert(value):
        events.append(value)
        if value == 2:
            raise ValueError("keep original second column")
        return value + 10

    cursor.connection.add_output_converter(mssql_python.SQL_INTEGER, convert)
    try:
        cursor.execute("SELECT 1 AS a, 2 AS b, CAST(NULL AS INT) AS c")
        assert cursor.fetchval() == 11
        assert events == [1, 2]
        events.clear()
        cursor.execute("SELECT 1 AS a, 2 AS b, CAST(NULL AS INT) AS c")
        assert tuple(cursor.fetchmany(1)[0]) == (11, 2, None)
        assert events == [1, 2]
    finally:
        cursor.connection.remove_output_converter(mssql_python.SQL_INTEGER)


def test_single_row_wrapper_does_not_enter_batch_factory(cursor):
    from mssql_python import ddbc_bindings

    cursor.execute(
        "SELECT n AS a, CAST(N'text' AS NVARCHAR(10)) AS b "
        "FROM (VALUES (1),(2),(3),(4)) AS v(n) ORDER BY n"
    )
    with (
        patch.object(ddbc_bindings, "construct_rows", wraps=ddbc_bindings.construct_rows) as batch,
        patch.object(
            ddbc_bindings, "DDBCSQLFetchRow", wraps=ddbc_bindings.DDBCSQLFetchRow
        ) as fused,
    ):
        retained = cursor.fetchmany(1)[0]
        assert tuple(retained) == (1, "text")
        batch.assert_not_called()
        assert [row[0] for row in cursor.fetchmany(2)] == [2, 3]
        batch.assert_called_once()
        assert cursor.fetchmany(2)[0][0] == 4
        assert batch.call_count == 2
        assert tuple(retained) == (1, "text")
        fused.assert_not_called()


@pytest.mark.parametrize("override_fast_create", (False, True))
def test_fetchmany_preserves_substituted_row_class(cursor, override_fast_create):
    import importlib
    from mssql_python.row import Row

    class DerivedRow(Row):
        pass

    def forbidden(*args):
        raise AssertionError("batch wrapping must not call a substituted Row's factory")

    if override_fast_create:
        DerivedRow._fast_create = staticmethod(forbidden)
    cursor_module = importlib.import_module("mssql_python.cursor")
    cursor.execute("SELECT 1 AS a")
    with patch.object(cursor_module, "Row", DerivedRow):
        row = cursor.fetchmany(1)[0]
    assert type(row) is DerivedRow
    assert row.a == 1


def test_fetchmany_preserves_replaced_fast_factory(cursor):
    from mssql_python.row import Row

    cursor.execute("SELECT 1 AS a")
    with patch.object(Row, "_fast_create", side_effect=AssertionError("must use batch factory")):
        assert cursor.fetchmany(1)[0].a == 1


@pytest.mark.parametrize("via_arraysize", (False, True))
@pytest.mark.parametrize("raises", (False, True))
def test_fetchmany_size_subclass_equality_is_not_called(cursor, via_arraysize, raises):
    from mssql_python import ddbc_bindings

    calls = []

    class Size(int):
        def __eq__(self, other):
            calls.append(other)
            if raises:
                raise AssertionError("size equality must not run after native fetch")
            return super().__eq__(other)

    cursor.execute("SELECT 1 AS a UNION ALL SELECT 2")
    with patch.object(ddbc_bindings, "construct_rows", wraps=ddbc_bindings.construct_rows) as batch:
        if via_arraysize:
            cursor.arraysize = Size(1)
            result = cursor.fetchmany()
        else:
            result = cursor.fetchmany(Size(1))
        assert result[0][0] == 1
        batch.assert_called_once()
    assert calls == []
    assert cursor.fetchone()[0] == 2


def test_real_subclass_and_instance_fetchone_overrides(cursor):
    calls = []

    class DerivedCursor(Cursor):
        def fetchone(self):
            calls.append("derived")
            return (71, 72)

    with DerivedCursor(cursor.connection) as derived:
        derived.execute("SELECT 1 AS a UNION ALL SELECT 2")
        assert derived.fetchval() == 71
        assert next(derived) == (71, 72)
        assert calls == ["derived", "derived"]
        # fetchmany must not acquire fetchone's Python override semantics.
        assert derived.fetchmany(1)[0][0] == 1
        assert calls == ["derived", "derived"]
        with patch.object(derived, "fetchone", return_value=(81, 82)) as override:
            assert derived.fetchval() == 81
            assert next(derived) == (81, 82)
            assert derived.fetchmany(1)[0][0] == 2
            assert override.call_count == 2


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Windows does not export the native driver function-pointer globals",
)
@pytest.mark.parametrize("first_fetch", ("fetchone", "fetchmany"))
def test_count_generation_change_with_unbound_marker(conn_str, first_fetch):
    """A count obtained across invalidation cannot authorize either cache."""
    script = textwrap.dedent("""
        import ctypes as c
        import os
        import sys
        import mssql_python
        from mssql_python import ddbc_bindings as ddbc

        library = c.CDLL(ddbc.module.__file__)
        pointer = c.c_void_p
        count_type = c.CFUNCTYPE(c.c_short, pointer, c.POINTER(c.c_short))
        unbind_type = c.CFUNCTYPE(c.c_short, pointer, c.c_ushort)
        count_slot = pointer.in_dll(library, "SQLNumResultCols_ptr")
        unbind_slot = pointer.in_dll(library, "SQLFreeStmt_ptr")
        counts, unbinds, invalidations, callback_errors = [], [], [], []

        @count_type
        def counted(handle, value):
            try:
                counts.append(handle)
                ret = original_count(handle, value)
                if not invalidations:
                    invalidations.append(True)
                    # Change only the cache generation, not the result shape.
                    assert ddbc.DDBCSQLSetStmtAttr(cursor.hstmt, 0, 0) == 0
                return ret
            except BaseException as error:
                callback_errors.append(type(error).__name__)
                return -1

        @unbind_type
        def unbound(handle, option):
            try:
                if option == 2:
                    unbinds.append(handle)
                return original_unbind(handle, option)
            except BaseException as error:
                callback_errors.append(type(error).__name__)
                return -1

        with mssql_python.connect(os.environ["DB_CONNECTION_STRING"]) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT n AS a, n + 10 AS b FROM (VALUES (1),(2),(3),(4)) v(n) ORDER BY n"
                )
                saved_count, saved_unbind = count_slot.value, unbind_slot.value
                assert saved_count and saved_unbind
                original_count = count_type(saved_count)
                original_unbind = unbind_type(saved_unbind)
                count_slot.value = c.cast(counted, pointer).value
                unbind_slot.value = c.cast(unbound, pointer).value
                try:
                    first = (
                        cursor.fetchmany(1)[0] if sys.argv[1] == "fetchmany"
                        else cursor.fetchone()
                    )
                    assert tuple(first) == (1, 11)
                    assert invalidations == [True]
                    before_count, before_unbind = len(counts), len(unbinds)
                    assert tuple(cursor.fetchone()) == (2, 12)
                    assert len(counts) == before_count + 1
                    assert len(unbinds) == before_unbind + 1
                    before_count, before_unbind = len(counts), len(unbinds)
                    assert cursor.fetchval() == 3
                    assert tuple(next(cursor)) == (4, 14)
                    assert cursor.fetchone() is None
                    assert len(counts) == before_count
                    assert len(unbinds) == before_unbind
                    # Direct count calls remain uncached even after a warm fetch.
                    assert ddbc.DDBCSQLNumResultCols(cursor.hstmt) == 2
                    assert ddbc.DDBCSQLNumResultCols(cursor.hstmt) == 2
                    assert len(counts) == before_count + 2
                    assert tuple(first) == (1, 11)
                    assert not callback_errors, callback_errors
                    assert not cursor.messages
                finally:
                    count_slot.value, unbind_slot.value = saved_count, saved_unbind
                cursor.execute("SELECT 42")
                assert cursor.fetchval() == 42
        """)
    _run_fetch_script(conn_str, script, first_fetch)


@pytest.mark.parametrize("mutation", ("new", "setattr", "code", "abstract", "descriptor"))
def test_fast_row_inplace_customization(monkeypatch, mutation):
    import weakref
    from mssql_python import ddbc_bindings
    from mssql_python.row import Row

    factory = Row._fast_create
    events = []

    def custom_new(cls):
        events.append("new")
        return object.__new__(cls)

    def custom_setattr(self, name, value):
        events.append(name)
        object.__setattr__(self, name, value)

    def custom_factory(values, column_map, cursor, column_map_lower=None, column_names=None):
        raise RuntimeError("customized factory code")

    def fail_descriptor(self, value):
        events.append(weakref.ref(self))
        raise RuntimeError("customized descriptor")

    if mutation == "new":
        monkeypatch.setattr(Row, "__new__", staticmethod(custom_new))
    elif mutation == "setattr":
        monkeypatch.setattr(Row, "__setattr__", custom_setattr)
    elif mutation == "code":
        monkeypatch.setattr(factory, "__code__", custom_factory.__code__)
    elif mutation == "abstract":
        monkeypatch.setattr(Row, "__abstractmethods__", frozenset({"required"}), raising=False)
    else:
        monkeypatch.setattr(Row, "_column_names", property(fset=fail_descriptor))

    assert Row._fast_create is factory
    with patch.object(ddbc_bindings, "construct_row", wraps=ddbc_bindings.construct_row) as native:
        if mutation in ("code", "descriptor"):
            with pytest.raises(RuntimeError, match="customized"):
                factory([42], {"number": 0}, None)
        elif mutation == "abstract":
            with pytest.raises(TypeError, match="abstract"):
                factory([42], {"number": 0}, None)
        else:
            assert factory([42], {"number": 0}, None).number == 42
        native.assert_not_called()
    if mutation == "new":
        assert events == ["new"]
    elif mutation == "setattr":
        assert events == ["_values", "_column_map", "_cursor", "_column_map_lower", "_column_names"]
    elif mutation == "descriptor":
        assert len(events) == 1 and events[0]() is None


@pytest.mark.parametrize("method", ("fetchone", "fetchmany", "fetchval"))
@pytest.mark.parametrize("failure", ("maps", "factory"))
def test_single_row_construction_failure_keeps_fetch_position(cursor, method, failure):
    from mssql_python import ddbc_bindings

    cursor.execute("SELECT n AS number FROM (VALUES (1), (2)) AS v(n) ORDER BY n")
    from mssql_python.row import Row

    bridge_name = "DDBCSQLFetchMany" if method == "fetchmany" else "DDBCSQLFetchOne"
    bridge = getattr(ddbc_bindings, bridge_name)

    def fetch():
        value = cursor.fetchmany(1) if method == "fetchmany" else getattr(cursor, method)()
        return value[0][0] if method == "fetchmany" else value if method == "fetchval" else value[0]

    def fail(*args):
        assert cursor.rowcount == 1
        assert cursor.rownumber == 0
        assert cursor._next_row_index == 1
        raise RuntimeError("injected post-fetch failure")

    failed_stage = Mock(side_effect=fail)
    failure_patch = (
        patch.object(cursor, "_get_column_and_converter_maps", failed_stage)
        if failure == "maps"
        else patch.object(Row, "_column_names", property(fset=failed_stage))
    )
    with patch.object(ddbc_bindings, bridge_name, wraps=bridge) as native_fetch, failure_patch:
        with pytest.raises(RuntimeError, match="injected post-fetch failure"):
            fetch()
        native_fetch.assert_called_once()
        failed_stage.assert_called_once()
    assert fetch() == 2
    assert cursor.rowcount == 2
    assert cursor.rownumber == 1


@pytest.mark.parametrize("target", ("__new__", "__setattr__"))
def test_native_row_guard_does_not_invoke_descriptors(monkeypatch, target):
    from mssql_python.cursor import _native_row_eligible
    from mssql_python.row import Row

    events = []

    class Descriptor:
        def __get__(self, instance, owner):
            events.append("lookup")
            if target == "__new__":
                if len(events) > 1:
                    raise RuntimeError("duplicate allocator lookup")

                def allocate(cls):
                    events.append("allocate")
                    return object.__new__(cls)

                return allocate
            return lambda name, value: object.__setattr__(instance, name, value)

    monkeypatch.setattr(Row, target, Descriptor())
    assert not _native_row_eligible(Row)
    assert events == []
    row = Row._fast_create([42], {"number": 0}, None)
    assert row.number == 42
    assert events == (["lookup", "allocate"] if target == "__new__" else ["lookup"] * 5)


def test_native_row_guard_does_not_invoke_metaclass_hooks():
    from mssql_python.cursor import _native_row_eligible
    from mssql_python.row import Row

    class Meta(type):
        def __getattribute__(cls, name):
            raise AssertionError("guard must not inspect substituted class through metaclass")

    class CustomRow(Row, metaclass=Meta):
        pass

    assert not _native_row_eligible(CustomRow)


@pytest.mark.parametrize("method", ("fetchone", "fetchmany", "fetchval"))
@pytest.mark.parametrize("when", ("maps", "final_argument"))
def test_single_row_fusion_handles_post_fetch_factory_change(cursor, method, when):
    from mssql_python import ddbc_bindings
    from mssql_python.row import Row

    original_maps = cursor._get_column_and_converter_maps
    factory = Row._fast_create

    def replacement(values, column_map, cursor, column_map_lower=None, column_names=None):
        raise RuntimeError("factory changed after native advancement")

    def maps():
        factory.__code__ = replacement.__code__
        return original_maps()

    def names(self):
        factory.__code__ = replacement.__code__
        return self.__dict__["_cached_result_columns"]

    cursor.execute("SELECT n AS number FROM (VALUES (1), (2)) AS v(n) ORDER BY n")
    cursor._get_column_and_converter_maps()
    change = (
        patch.object(cursor, "_get_column_and_converter_maps", side_effect=maps)
        if when == "maps"
        else patch.object(type(cursor), "_cached_result_columns", property(names), create=True)
    )
    old_code = factory.__code__
    try:
        with (
            change,
            patch.object(
                ddbc_bindings, "DDBCSQLFetchRow", wraps=ddbc_bindings.DDBCSQLFetchRow
            ) as fused,
        ):
            with pytest.raises(RuntimeError, match="factory changed after native advancement"):
                cursor.fetchmany(1) if method == "fetchmany" else getattr(cursor, method)()
            fused.assert_not_called()
            assert Row._fast_create is factory
            assert cursor.rowcount == 1 and cursor.rownumber == 0
    finally:
        factory.__code__ = old_code
    assert cursor.fetchone().number == 2


@pytest.mark.parametrize("method", ("fetchone", "fetchmany", "fetchval"))
def test_single_row_late_allocator_preserves_factory_global_lookup(cursor, method):
    from mssql_python import ddbc_bindings
    from mssql_python.row import Row

    original_row = Row

    class ChangedRow(original_row):
        pass

    events = []
    factory = original_row._fast_create
    assert "__new__" not in vars(original_row)

    class Allocator:
        def __get__(self, instance, owner):
            events.append("new_lookup")
            factory.__globals__["Row"] = ChangedRow

            def allocate(cls):
                events.append("allocate:" + cls.__name__)
                return object.__new__(cls)

            return allocate

    def names(self):
        events.append("names_lookup")
        original_row.__new__ = Allocator()
        return self.__dict__["_cached_result_columns"]

    cursor.execute("SELECT n AS number FROM (VALUES (1), (2)) AS v(n) ORDER BY n")
    cursor._get_column_and_converter_maps()
    try:
        with (
            patch.object(type(cursor), "_cached_result_columns", property(names), create=True),
            patch.object(
                ddbc_bindings, "DDBCSQLFetchRow", wraps=ddbc_bindings.DDBCSQLFetchRow
            ) as fused,
        ):
            result = cursor.fetchmany(1) if method == "fetchmany" else getattr(cursor, method)()
            fused.assert_not_called()
        if method == "fetchval":
            assert result == 1
        else:
            row = result[0] if method == "fetchmany" else result
            assert type(row) is ChangedRow
            assert row.number == 1
        assert events == ["names_lookup", "new_lookup", "allocate:ChangedRow"]
        assert cursor.rowcount == 1 and cursor.rownumber == 0
    finally:
        factory.__globals__["Row"] = original_row
        if "__new__" in vars(original_row):
            delattr(original_row, "__new__")
    assert cursor.fetchone().number == 2


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
    _run_fetch_script(conn_str, script, mode, timeout=60)
