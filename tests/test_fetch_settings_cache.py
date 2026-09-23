"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.

Regression and operation-count tests for connection settings cached by fetch APIs.
All integration queries are read-only and each test owns its connection.
"""

import datetime
import subprocess
import sys
import uuid
import weakref
from unittest.mock import Mock, patch

import pytest
import mssql_python
from mssql_python.constants import ConstantsDDBC
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
    with patch.object(connection, "getdecoding", wraps=connection.getdecoding) as reads:
        with connection.cursor() as first, connection.cursor() as second:
            assert reads.call_count == 4
            for cursor in (first, second):
                cursor.execute("SELECT n FROM (VALUES (1), (2), (3)) AS v(n) ORDER BY n")
                assert cursor.fetchone()[0] == 1
                assert cursor.fetchmany(1)[0][0] == 2
                assert cursor.fetchall()[0][0] == 3
            assert reads.call_count == 4

            connection.setdecoding(mssql_python.SQL_CHAR, encoding="latin-1")
            for index, cursor in enumerate((first, second), 1):
                cursor.execute(
                    "SELECT CONVERT(VARCHAR(1), 0xE9) AS txt FROM (VALUES (1), (2), (3)) AS v(n)"
                )
                assert cursor.fetchone().txt == "\u00e9"
                assert cursor.fetchmany(1)[0].txt == "\u00e9"
                assert cursor.fetchall()[0].txt == "\u00e9"
                assert reads.call_count == 4 + 2 * index


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


@pytest.mark.parametrize("construction", ("direct", "python_fast", "native"))
def test_constructed_row_preserves_instance_capabilities(construction):
    values = [42]
    column_map = {"number": 0}
    if construction == "direct":
        row = Row(values, column_map)
    elif construction == "python_fast":
        row = Row._fast_create(values, column_map, None)
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


@pytest.mark.parametrize("size", (0, 1, 3))
def test_construct_rows_repeated_calls_release_references(size):
    values = [[index] for index in range(size)]
    column_map = {"Number": 0}
    column_map_lower = {"number": 0}
    column_names = ("Number",)
    cursor = object()
    tracked = (values, column_map, column_map_lower, column_names, cursor, *values)
    references = [sys.getrefcount(value) for value in tracked]
    for _ in range(10):
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
