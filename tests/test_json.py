# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.
"""Native JSON tests. Live cases require an explicitly qualified preview runtime."""

import json
import os
import asyncio
from dataclasses import FrozenInstanceError
from datetime import date
from decimal import Decimal
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import pytest

from mssql_python import Cursor, DataError, Json, NotSupportedError, SQL_JSON, SQL_SS_JSON, connect
from mssql_python.json import _decode_json, _reject_json_parameters


@pytest.mark.parametrize(
    "value",
    [
        None,
        True,
        1,
        1.5,
        b"{}",
        (),
        {1: "value"},
        {"nested": {None: 1}},
        {"v": (1, 2)},
        {"v": {1, 2}},
        {"v": object()},
    ],
)
def test_json_rejects_unsupported_input(value):
    with pytest.raises(TypeError):
        Json(value)


@pytest.mark.parametrize(
    "text",
    [' \n{"key":1,"key":2,"escaped":"\\u0000\\u03bb"}\t', "[]", "", "text", '"text"', "null"],
    ids=["object", "array", "empty", "malformed", "scalar", "null"],
)
def test_json_serialized_text_is_not_transformed(text, monkeypatch):
    dumps = Mock(side_effect=AssertionError("Text must not be serialized"))
    loads = Mock(side_effect=AssertionError("Text must not be parsed"))
    monkeypatch.setattr(json, "dumps", dumps)
    monkeypatch.setattr(json, "loads", loads)
    parameter = Json(text)
    assert parameter._text is text
    assert repr(parameter) == "Json(<serialized>)"
    with pytest.raises(FrozenInstanceError):
        parameter._text = "changed"
    dumps.assert_not_called()
    loads.assert_not_called()


@pytest.mark.parametrize("base", [dict, list, str])
def test_json_rejects_root_subclasses(base):
    class Custom(base):
        pass

    with pytest.raises(TypeError):
        Json(Custom())


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -float("inf")])
def test_json_rejects_nonfinite_numbers(value):
    with pytest.raises(ValueError):
        Json({"nested": [value]})


@pytest.mark.parametrize(
    "value", [{"v": "\ud800"}, {"\udfff": 1}, ["\ud800\udc00"], '{"v":"\ud800"}', "\udfff"]
)
def test_json_rejects_surrogate_strings(value):
    with pytest.raises(UnicodeError):
        Json(value)


def test_json_snapshot_cycles_and_shared_children():
    child = [None, True, 1.5, "\U0001f600", "\x00"]
    value = {"first": child, "second": child}
    parameter = Json(value)
    original = json.loads(parameter._text)
    child.append("later")
    assert json.loads(parameter._text) == original
    with pytest.raises(FrozenInstanceError):
        parameter._text = "{}"
    child.append(value)
    with pytest.raises(ValueError, match="circular"):
        Json(value)


@pytest.mark.parametrize("value", [None, 0, True, "object", [], {}])
def test_json_output_validation(value):
    cursor = Cursor.__new__(Cursor)
    with pytest.raises(ValueError):
        cursor.json_output = value


@pytest.mark.parametrize("value", ["{", "null", "1", '"text"'])
def test_json_decode_errors_are_not_text_fallbacks(value):
    with pytest.raises(DataError):
        _decode_json(value)


def test_deferred_backends_reject_json():
    for backend in ("async execution", "bulk copy"):
        with pytest.raises(NotSupportedError, match=backend):
            _reject_json_parameters([Json({})], backend)
        _reject_json_parameters([None, "{}", [1, 2]], backend)


@pytest.mark.parametrize("parameters", [(Json({}),), ([Json({})],), ({"doc": Json({})},)])
def test_async_json_is_rejected_before_core_execute(parameters):
    from mssql_python.async_query.async_execute import execute

    core = Mock()
    cursor = SimpleNamespace(_py_core_async_cursor=core)
    with pytest.raises(NotSupportedError, match="async"):
        asyncio.run(execute(cursor, "SELECT ?", *parameters))
    core.execute.assert_not_called()


@pytest.mark.parametrize("sizes", [[SQL_SS_JSON], [(SQL_SS_JSON, 0, 0)]])
def test_async_json_hint_is_rejected_before_core_call(sizes):
    from mssql_python.async_query.async_cursor import _AsyncCursor

    core = Mock()
    cursor = SimpleNamespace(_py_core_async_cursor=core)
    with pytest.raises(NotSupportedError, match="async"):
        _AsyncCursor.setinputsizes(cursor, sizes)
    core.setinputsizes.assert_not_called()
    _AsyncCursor.setinputsizes(cursor, None)
    core.setinputsizes.assert_called_once_with(None)


def test_sync_json_name_does_not_replace_async_symbol(monkeypatch):
    from mssql_python import async_query
    from mssql_python.async_query.async_cursor import _AsyncCursor

    py_core = ModuleType("mssql_py_core")
    py_core.SQL_JSON = 244
    monkeypatch.setattr(async_query, "load_py_core", lambda: py_core)
    core = Mock()
    cursor = SimpleNamespace(_py_core_async_cursor=core)
    _AsyncCursor.setinputsizes(cursor, [async_query.SQL_JSON])
    core.setinputsizes.assert_called_once_with([244])
    with pytest.raises(NotSupportedError, match="async"):
        _AsyncCursor.setinputsizes(cursor, [SQL_JSON])


def test_async_json_batch_guard_preserves_iterator_boundary():
    from mssql_python.async_query.async_execute import executemany

    consumed = []

    def rows():
        consumed.append("first")
        yield (Json({}),)
        consumed.append("tail")
        yield (1,)

    async def consume(_operation, parameters, **_kwargs):
        list(parameters)

    core = SimpleNamespace(executemany=consume)
    cursor = SimpleNamespace(_check_closed=lambda: None, _py_core_async_cursor=core)
    with pytest.raises(NotSupportedError, match="async"):
        asyncio.run(executemany(cursor, "SELECT ?", rows()))
    assert consumed == ["first"]


@pytest.fixture
def json_connection(conn_str):
    if os.environ.get("MSSQL_TEST_NATIVE_JSON") != "1":
        pytest.skip("Set MSSQL_TEST_NATIVE_JSON=1 with a qualified JSON provider and server")
    with connect(conn_str) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT CAST(N'{}' AS JSON)")
            assert cursor._column_sql_types == [SQL_SS_JSON], "Provider must report native JSON"
        yield connection


@pytest.mark.parametrize("fetch", ["fetchone", "fetchmany", "fetchall", "fetchval", "iteration"])
@pytest.mark.parametrize("mode", ["text", "objects"])
@pytest.mark.parametrize("serialized", [False, True])
def test_json_fetch_modes(json_connection, fetch, mode, serialized):
    expected = {"nested": [None, True, 1.25, "\U0001f600", "\x00"]}
    value = json.dumps(expected, ensure_ascii=False) if serialized else expected
    with json_connection.cursor() as cursor:
        cursor.json_output = mode
        cursor.execute("SELECT ? AS doc", Json(value))
        assert cursor._column_sql_types == [SQL_SS_JSON]
        assert cursor.description[0][1] is str
        if fetch == "fetchval":
            actual = cursor.fetchval()
        elif fetch == "iteration":
            actual = next(cursor)[0]
        elif fetch in ("fetchmany", "fetchall"):
            actual = getattr(cursor, fetch)()[0][0]
        else:
            actual = cursor.fetchone()[0]
        assert (json.loads(actual) if mode == "text" else actual) == expected


@pytest.mark.parametrize("size", [10, 4000, 4001, 70000])
def test_json_large_array_binding_and_nulls(json_connection, size):
    document = {"payload": "\U0001f600" * size}
    with json_connection.cursor() as cursor:
        cursor.execute("CREATE TABLE #json_batch (id INT, doc JSON NULL)")
        rows = [(0, None), (1, Json(document)), (2, Json([]))]
        cursor.executemany("INSERT INTO #json_batch VALUES (?, ?)", rows)
        cursor.setinputsizes([4, SQL_JSON])
        cursor.executemany("INSERT INTO #json_batch VALUES (?, ?)", [(3, None), (4, None)])
        cursor.setinputsizes([4, SQL_JSON])
        cursor.execute("INSERT INTO #json_batch VALUES (?, ?)", 5, None)
        cursor.json_output = "objects"
        cursor.execute("SELECT doc FROM #json_batch ORDER BY id")
        assert [row[0] for row in cursor.fetchall()] == [None, document, [], None, None, None]
        assert isinstance(rows[1][1], Json)


def test_json_array_native_binding_contract(json_connection, monkeypatch):
    from mssql_python import ddbc_bindings

    execute_many = ddbc_bindings.SQLExecuteMany
    observed = []

    def bind(handle, sql, columns, infos, count, encoding):
        observed.append([(info.paramSQLType, info.paramCType) for info in infos])
        return execute_many(handle, sql, columns, infos, count, encoding)

    monkeypatch.setattr(ddbc_bindings, "SQLExecuteMany", bind)
    with json_connection.cursor() as cursor:
        cursor.execute("CREATE TABLE #json_contract (doc JSON NULL)")
        text = ' \n{"u":"\\u0000\\u03bb"}\t'
        parameter = Json(text)
        cursor.executemany(
            "INSERT INTO #json_contract VALUES (?)",
            [(None,), (Json({}),), (Json([]),), (parameter,)],
        )
        assert observed == [[(SQL_SS_JSON, -8)]]
        assert parameter._text is text
        cursor.json_output = "objects"
        cursor.execute("SELECT doc FROM #json_contract")
        assert [row[0] for row in cursor.fetchall()] == [None, {}, [], {"u": "\x00\u03bb"}]


@pytest.mark.parametrize(
    "text",
    ["", "text", "{", "null", '"text"', "1", "true", '{}\x00{"extra":1}'],
    ids=["empty", "malformed", "incomplete", "null", "string", "number", "boolean", "nul"],
)
def test_json_text_server_rejection_and_prepared_recovery(json_connection, text):
    from mssql_python import DatabaseError

    parameter = Json(text)
    with json_connection.cursor() as cursor:
        cursor.json_output = "objects"
        assert cursor.execute("SELECT ?", Json("{}"), reset_cursor=False).fetchval() == {}
        with pytest.raises(DatabaseError) as error:
            cursor.execute("SELECT ?", parameter, reset_cursor=False).fetchall()
        assert error.value.args
        assert cursor.execute("SELECT ?", Json("[]"), reset_cursor=False).fetchval() == []
        assert cursor._column_sql_types == [SQL_JSON]


def test_json_text_duplicate_keys_reach_server_unchanged(json_connection):
    text = ' \n{"key":1,"key":2,"escaped":"\\u0000\\u03bb"}\t'
    parameter = Json(text)
    with json_connection.cursor() as cursor:
        cursor.execute(
            "SELECT ? AS doc, JSON_VALUE(?, '$.key') AS first_value", parameter, parameter
        )
        assert cursor._column_sql_types[0] == SQL_JSON
        row = cursor.fetchone()
        assert row.first_value == "1"
        assert json.loads(row.doc)["escaped"] == "\x00\u03bb"
        assert parameter._text is text


@pytest.mark.parametrize("streamed", [False, True])
def test_json_text_mixed_wrappers_hints_and_nulls(json_connection, streamed):
    expected = [None, {"d": []}, [1], {"u": "\x00\u03bb\U0001f600"}, None]
    values = [
        None,
        Json(expected[1]),
        Json(expected[2]),
        Json(' {"u":"\\u0000\u03bb\U0001f600"} '),
        None,
    ]
    companion = "x" * (12000 if streamed else 1)
    with json_connection.cursor() as cursor:
        cursor.execute("CREATE TABLE #json_sources (id INT, doc JSON, txt NVARCHAR(MAX))")
        cursor.setinputsizes([4, SQL_JSON, (-10 if streamed else -9, len(companion), 0)])
        cursor.executemany(
            "INSERT INTO #json_sources VALUES (?, ?, ?)",
            [(i, value, companion) for i, value in enumerate(values)],
        )
        cursor.json_output = "objects"
        cursor.execute("SELECT doc, LEN(txt) FROM #json_sources ORDER BY id")
        assert [tuple(row) for row in cursor.fetchall()] == [
            (value, len(companion)) for value in expected
        ]


@pytest.mark.parametrize("streamed", [False, True])
def test_json_invalid_text_batch_explicit_rollback(json_connection, streamed):
    from mssql_python import DatabaseError

    companion = "x" * (12000 if streamed else 1)
    with json_connection.cursor() as cursor:
        cursor.execute("CREATE TABLE #json_transaction (doc JSON, txt NVARCHAR(MAX))")
        json_connection.commit()
        with pytest.raises(DatabaseError):
            cursor.executemany(
                "INSERT INTO #json_transaction VALUES (?, ?)",
                [(Json("{}"), companion), (Json("invalid"), companion)],
            )
        json_connection.rollback()
        assert cursor.execute("SELECT COUNT(*) FROM #json_transaction").fetchval() == 0
        cursor.json_output = "objects"
        assert cursor.execute("SELECT ?", Json('{"recovered":true}')).fetchval() == {
            "recovered": True
        }


@pytest.mark.parametrize("mode", ["text", "objects"])
def test_json_result_reinsertion_in_either_mode(json_connection, mode):
    with json_connection.cursor() as cursor:
        cursor.execute("CREATE TABLE #json_reinsert (id INT, doc JSON)")
        cursor.json_output = mode
        sources = [Json('{"v":[1,null]}'), Json('["\\u03bb"]'), None]
        for i, source in enumerate(sources):
            cursor.setinputsizes([SQL_JSON])
            row = cursor.execute("SELECT ? AS payload", source).fetchone()
            parameter = None if row.payload is None else Json(row.payload)
            cursor.setinputsizes([4, SQL_JSON])
            cursor.execute("INSERT INTO #json_reinsert VALUES (?, ?)", i, parameter)
        cursor.json_output = "objects"
        cursor.execute("SELECT doc FROM #json_reinsert ORDER BY id")
        assert [row[0] for row in cursor.fetchall()] == [{"v": [1, None]}, ["\u03bb"], None]


def test_json_provider_rejection_does_not_switch(json_connection, monkeypatch):
    from mssql_python.odbc_provider import ProviderManager

    monkeypatch.setattr(ProviderManager, "effective", lambda: "mssql-odbc")
    with json_connection.cursor() as cursor:
        with pytest.raises(NotSupportedError, match="msodbcsql18"):
            cursor.execute("SELECT ?", Json({}))
        cursor.execute("SELECT 42")
        assert cursor.fetchval() == 42


@pytest.mark.parametrize("batch", [False, True])
@pytest.mark.parametrize(
    "text",
    ["{}", "[]", '{}\x00{"extra":1}', '{"v":"' + "x" * 12000 + '"}'],
    ids=["object", "array", "nul", "large"],
)
def test_json_hint_rejects_serialized_strings_before_insert(json_connection, batch, text):
    with json_connection.cursor() as cursor:
        cursor.execute("CREATE TABLE #json_text_reject (doc JSON)")
        cursor.setinputsizes([SQL_JSON])
        with pytest.raises(TypeError, match="SQL_JSON requires Json or None"):
            if batch:
                cursor.executemany("INSERT INTO #json_text_reject VALUES (?)", [(text,)])
            else:
                cursor.execute("INSERT INTO #json_text_reject VALUES (?)", text)
        cursor.setinputsizes(None)
        cursor.execute("SELECT COUNT(*) FROM #json_text_reject")
        assert cursor.fetchval() == 0


@pytest.mark.parametrize("hinted", [False, True])
@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("streamed_companion", [False, True])
@pytest.mark.parametrize("serialized", [False, True])
def test_json_mixed_string_batch_rejects_before_dispatch(
    json_connection, hinted, reverse, streamed_companion, serialized
):
    with json_connection.cursor() as cursor:
        cursor.execute("CREATE TABLE #json_mixed (doc JSON NULL, txt NVARCHAR(MAX))")
        text = "x" * (12000 if streamed_companion else 1)
        rows = [(Json("{}" if serialized else {}), text), ("[]", text)]
        if reverse:
            rows.reverse()
        if hinted:
            cursor.setinputsizes([SQL_JSON, (-9, 0, 0)])
        with pytest.raises(TypeError, match="SQL_JSON requires Json or None"):
            cursor.executemany("INSERT INTO #json_mixed VALUES (?, ?)", rows)
        cursor.setinputsizes(None)
        cursor.execute("SELECT COUNT(*) FROM #json_mixed")
        assert cursor.fetchval() == 0


@pytest.mark.parametrize("batch", [False, True])
def test_unhinted_string_json_inserts_remain_supported(json_connection, batch):
    values = ["{}", '["\\u03bb"]', '{"v":"' + "x" * 12000 + '"}']
    with json_connection.cursor() as cursor:
        cursor.execute("CREATE TABLE #json_legacy (id INT, doc JSON)")
        rows = list(enumerate(values))
        if batch:
            cursor.executemany("INSERT INTO #json_legacy VALUES (?, ?)", rows)
        else:
            for row in rows:
                cursor.execute("INSERT INTO #json_legacy VALUES (?, ?)", row)
        cursor.json_output = "objects"
        cursor.execute("SELECT doc FROM #json_legacy ORDER BY id")
        assert [row[0] for row in cursor.fetchall()] == [json.loads(value) for value in values]


def test_json_semantic_hint_accepts_wrappers_and_nulls(json_connection):
    with json_connection.cursor() as cursor:
        cursor.execute("CREATE TABLE #json_hint (id INT, doc JSON NULL)")
        cursor.setinputsizes([4, SQL_JSON])
        cursor.execute("INSERT INTO #json_hint VALUES (?, ?)", 0, Json({}))
        cursor.setinputsizes([4, SQL_JSON])
        cursor.executemany("INSERT INTO #json_hint VALUES (?, ?)", [(1, Json([])), (2, None)])
        cursor.json_output = "objects"
        cursor.execute("SELECT doc FROM #json_hint ORDER BY id")
        assert [row[0] for row in cursor.fetchall()] == [{}, [], None]


def test_json_execution_snapshot_and_nextset(json_connection):
    with json_connection.cursor() as cursor:
        cursor.json_output = "objects"
        cursor.execute("SELECT CAST(N'[]' AS JSON); SELECT CAST(N'{}' AS JSON)")
        cursor.json_output = "text"
        assert cursor.fetchone()[0] == []
        assert cursor.nextset()
        assert cursor.fetchone()[0] == {}
        cursor.execute("SELECT CAST(N'[]' AS JSON)")
        assert isinstance(cursor.fetchone()[0], str)


@pytest.mark.parametrize("method", ["fetchone", "fetchmany", "fetchval", "iteration"])
def test_json_warm_fetch_and_mode_changes(json_connection, method):
    def read(cursor):
        if method == "fetchval":
            return cursor.fetchval()
        if method == "iteration":
            return next(cursor)[0]
        if method == "fetchmany":
            return cursor.fetchmany(1)[0][0]
        return cursor.fetchone()[0]

    with json_connection.cursor() as cursor:
        query = "SELECT CAST(N'{}' AS JSON) FROM (VALUES(1),(2),(3),(4),(5)) t(id)"
        cursor.execute(query)
        assert [read(cursor) for _ in range(3)] == ["{}"] * 3
        cursor.json_output = "objects"
        assert [read(cursor) for _ in range(2)] == ["{}"] * 2
        cursor.execute(query)
        assert [read(cursor) for _ in range(5)] == [{}] * 5
        cursor.json_output = "text"
        cursor.execute(query)
        assert [read(cursor) for _ in range(5)] == ["{}"] * 5


def test_json_converters_nulls_and_text_columns(json_connection):
    with json_connection.cursor() as cursor:
        cursor.json_output = "objects"
        cursor.execute(
            "SELECT CAST(N'{}' AS JSON), CAST(N'[]' AS NVARCHAR(MAX)), CAST(NULL AS JSON)"
        )
        assert tuple(cursor.fetchone()) == ({}, "[]", None)
        converter = Mock(return_value="custom")
        json_connection.add_output_converter(SQL_SS_JSON, converter)
        cursor.execute("SELECT CAST(N'{}' AS JSON), CAST(NULL AS JSON)")
        assert tuple(cursor.fetchone()) == ("custom", None)
        converter.assert_called_once_with("{}".encode("utf-16-le"))
        json_connection.remove_output_converter(SQL_SS_JSON)
        cursor.execute("SELECT CAST(N'{}' AS JSON)")
        assert cursor.fetchone()[0] == {}


def test_json_parse_once_and_independent_rows(json_connection, monkeypatch):
    import mssql_python.cursor as cursor_module

    decode = Mock(wraps=_decode_json)
    monkeypatch.setattr(cursor_module, "_decode_json", decode)
    with json_connection.cursor() as cursor:
        cursor.json_output = "objects"
        cursor.execute("SELECT CAST(N'{\"v\":[]}' AS JSON) FROM (VALUES(1),(2),(3)) t(id)")
        rows = [cursor.fetchone(), *cursor.fetchmany(2)]
        assert decode.call_count == 3
        rows[0][0]["v"].append(1)
        assert rows[1][0] == rows[2][0] == {"v": []}


def test_json_parameter_styles_and_streamed_companion(json_connection):
    with json_connection.cursor() as cursor:
        cursor.execute("CREATE TABLE #json_stream (doc JSON NULL, txt NVARCHAR(MAX))")
        cursor.executemany(
            "INSERT INTO #json_stream VALUES (%(doc)s, %(txt)s)",
            [{"doc": None, "txt": "x" * 12000}, {"doc": Json([1]), "txt": "y" * 12000}],
        )
        cursor.json_output = "objects"
        cursor.execute("SELECT doc, LEN(txt) FROM #json_stream ORDER BY txt")
        assert [tuple(row) for row in cursor.fetchall()] == [(None, 12000), ([1], 12000)]
        cursor.execute("SELECT CAST(%(doc)s AS JSON)", {"doc": Json({"v": 1})})
        assert cursor.fetchval() == {"v": 1}
        cursor.execute("SELECT ?, ?", [1, 2])
        assert tuple(cursor.fetchone()) == (1, 2)


def test_json_large_scalar_and_cursor_reuse(json_connection):
    with json_connection.cursor() as cursor:
        cursor.json_output = "objects"
        for value in [{}, {"v": "\U0001f600" * 70000}, []]:
            cursor.execute("SELECT CAST(? AS JSON)", Json(value))
            assert cursor.fetchval() == value
        cursor.setinputsizes([SQL_JSON])
        cursor.execute("SELECT CAST(? AS JSON)", None)
        assert cursor.fetchval() is None


@pytest.mark.parametrize("method", ["fetchone", "fetchmany", "fetchall"])
def test_json_parse_errors_propagate_from_fetch(json_connection, monkeypatch, method):
    from mssql_python import ddbc_bindings

    def invalid_result(handle, rows, *args):
        rows.extend(["{"] if method == "fetchone" else [["{"]])
        return 0

    with json_connection.cursor() as cursor:
        cursor.json_output = "objects"
        cursor.execute("SELECT CAST(N'{}' AS JSON)")
        native_method = {
            "fetchone": "DDBCSQLFetchOne",
            "fetchmany": "DDBCSQLFetchMany",
            "fetchall": "DDBCSQLFetchAll",
        }[method]
        monkeypatch.setattr(ddbc_bindings, native_method, invalid_result)
        with pytest.raises(DataError, match="Invalid native JSON"):
            getattr(cursor, method)()


@pytest.mark.parametrize("batch", [False, True])
def test_json_rejects_conflicting_hints_before_execution(json_connection, batch):
    with json_connection.cursor() as cursor:
        cursor.execute("CREATE TABLE #json_reject (doc JSON)")
        cursor.setinputsizes([12])
        with pytest.raises(TypeError, match="SQL_JSON"):
            if batch:
                cursor.executemany("INSERT INTO #json_reject VALUES (?)", [(Json({}),)])
            else:
                cursor.execute("INSERT INTO #json_reject VALUES (?)", Json({}))
        cursor.setinputsizes(None)
        cursor.execute("SELECT COUNT(*) FROM #json_reject")
        assert cursor.fetchval() == 0


@pytest.mark.parametrize("method", ["arrow", "arrow_batch", "arrow_reader"])
def test_json_arrow_boundary_does_not_consume_rows(json_connection, method):
    with json_connection.cursor() as cursor:
        cursor.execute("SELECT CAST(N'{}' AS JSON)")
        with pytest.raises(NotSupportedError, match="Arrow"):
            getattr(cursor, method)()
        assert json.loads(cursor.fetchone()[0]) == {}


@pytest.mark.parametrize("base", [dict, list, str, int, float])
def test_json_rejects_nested_subclasses(base):
    class Custom(base):
        pass

    with pytest.raises(TypeError):
        Json({"nested": [Custom()]})


def test_json_snapshot_serialized_once_across_reuse(json_connection, monkeypatch):
    dumps = Mock(wraps=json.dumps)
    monkeypatch.setattr(json, "dumps", dumps)
    original = {"child": [1, "\U0001f600"]}
    parameter = Json(original)
    original["child"].append("changed")
    with json_connection.cursor() as cursor:
        cursor.execute("CREATE TABLE #json_snapshot (doc JSON)")
        cursor.execute("INSERT INTO #json_snapshot VALUES (?)", parameter)
        cursor.executemany("INSERT INTO #json_snapshot VALUES (?)", [(parameter,)] * 3)
        cursor.json_output = "objects"
        cursor.execute("SELECT doc FROM #json_snapshot")
        assert [row[0] for row in cursor.fetchall()] == [{"child": [1, "\U0001f600"]}] * 4
    assert dumps.call_count == 1


@pytest.mark.parametrize("units", [3999, 4000, 4001, 4097, 4098, 4099, 65534, 65535, 65536])
@pytest.mark.parametrize("batch", [False, True])
@pytest.mark.parametrize("serialized", [False, True])
def test_json_exact_utf16_boundaries(json_connection, units, batch, serialized):
    # Eight units belong to {"v":""}; at 4098 the surrogate pair crosses the
    # 8192-byte SQLGetData buffer's 4095-unit payload boundary.
    value = {"v": "a" * (units - 10) + "\U0001f600"}
    parameter = Json(
        json.dumps(value, ensure_ascii=False, separators=(",", ":")) if serialized else value
    )
    assert len(parameter._text.encode("utf-16-le")) // 2 == units
    with json_connection.cursor() as cursor:
        cursor.execute("CREATE TABLE #json_boundary (id INT, doc JSON)")
        if batch:
            cursor.executemany(
                "INSERT INTO #json_boundary VALUES (?, ?)",
                [(0, Json({})), (1, parameter), (2, Json([]))],
            )
        else:
            for i, item in enumerate([Json({}), parameter, Json([])]):
                cursor.execute("INSERT INTO #json_boundary VALUES (?, ?)", i, item)
        cursor.json_output = "objects"
        cursor.execute("SELECT doc FROM #json_boundary ORDER BY id")
        assert [row[0] for row in cursor.fetchall()] == [{}, value, []]


@pytest.mark.parametrize("batch", [False, True])
@pytest.mark.parametrize("serialized", [False, True])
def test_json_multiple_columns_unicode_and_large_lob(json_connection, batch, serialized):
    value = {
        '\x00\n"\\e\u0301\u4e2d\U0001f600': [
            '\x00\b\f\n\r\t"\\',
            "a" * 32758 + "\U0001f600",
            "\U0001f600\u4e2d" * 350000,
        ]
    }
    expected = [(value, [], "ordinary"), ({}, None, "companion")]
    with json_connection.cursor() as cursor:
        cursor.execute("CREATE TABLE #json_columns (id INT, a JSON, b JSON NULL, c NVARCHAR(20))")
        parameter = Json(json.dumps(value, ensure_ascii=False) if serialized else value)
        rows = [(0, parameter, Json([]), "ordinary"), (1, Json({}), None, "companion")]
        if batch:
            cursor.executemany("INSERT INTO #json_columns VALUES (?, ?, ?, ?)", rows)
        else:
            for row in rows:
                cursor.setinputsizes([4, SQL_JSON, SQL_JSON, -9])
                cursor.execute("INSERT INTO #json_columns VALUES (?, ?, ?, ?)", row)
        cursor.json_output = "objects"
        cursor.execute("SELECT a,b,c FROM #json_columns ORDER BY id")
        assert [tuple(row) for row in cursor.fetchall()] == expected


@pytest.mark.parametrize("method", ["fetchone", "fetchmany", "fetchall"])
def test_json_converter_changes_between_fetches(json_connection, method):
    def read(cursor):
        if method == "fetchone":
            return [cursor.fetchone()[0]]
        if method == "fetchmany":
            return [row[0] for row in cursor.fetchmany(1)]
        return [row[0] for row in cursor.fetchall()]

    query = "SELECT CAST(N'{}' AS JSON) FROM (VALUES(1),(2),(3),(4)) t(id)"
    with json_connection.cursor() as cursor:
        cursor.json_output = "objects"
        cursor.execute(query)
        assert cursor.fetchone()[0] == {}
        json_connection.add_output_converter(str, lambda raw: "python-type")
        assert cursor.fetchone()[0] == "python-type"
        json_connection.add_output_converter(SQL_JSON, lambda raw: "native-type")
        assert cursor.fetchone()[0] == "native-type"
        json_connection.remove_output_converter(SQL_JSON)
        json_connection.remove_output_converter(str)
        assert read(cursor) == [{}]
        cursor.execute(query)
        json_connection.add_output_converter(SQL_JSON, lambda raw: "first")
        assert cursor.fetchone()[0] == "first"
        json_connection.add_output_converter(SQL_JSON, lambda raw: "replacement")
        assert read(cursor) == ["replacement"] * (3 if method == "fetchall" else 1)


@pytest.mark.parametrize("method", ["fetchmany", "fetchall"])
@pytest.mark.parametrize("action", ["remove", "replace"])
def test_json_converter_mutation_preserves_batch_snapshot(json_connection, method, action):
    calls = []

    def converter(raw):
        calls.append(raw)
        if action == "remove":
            json_connection.remove_output_converter(SQL_JSON)
        else:
            json_connection.add_output_converter(SQL_JSON, lambda value: "replacement")
        return "original"

    json_connection.add_output_converter(SQL_JSON, converter)
    with json_connection.cursor() as cursor:
        cursor.json_output = "objects"
        cursor.execute("SELECT CAST(N'{}' AS JSON) FROM (VALUES(1),(2),(3),(4)) t(id)")
        rows = cursor.fetchmany(3) if method == "fetchmany" else cursor.fetchall()
        assert [row[0] for row in rows] == ["original"] * len(rows)
        assert len(calls) == (3 if method == "fetchmany" else 4)
        if method == "fetchall":
            cursor.execute("SELECT CAST(N'{}' AS JSON)")
        assert cursor.fetchval() == ({} if action == "remove" else "replacement")


def test_json_failed_batch_preserves_prepared_execution(json_connection):
    with json_connection.cursor() as cursor:
        cursor.execute("CREATE TABLE #json_prepared (doc JSON)")
        cursor.json_output = "objects"
        cursor.execute("SELECT ?", Json({}), reset_cursor=False)
        assert cursor.fetchval() == {}
        with pytest.raises(TypeError, match="SQL_JSON"):
            cursor.executemany("INSERT INTO #json_prepared VALUES (?)", [(Json({}),), ("{}",)])
        for value in [Json([]), None, Json({"recovered": True})]:
            cursor.setinputsizes([SQL_JSON])
            cursor.execute("SELECT ?", value, reset_cursor=False)
            assert cursor._column_sql_types == [SQL_JSON]
            assert cursor.fetchval() == (None if value is None else json.loads(value._text))
        assert cursor.execute("SELECT COUNT(*) FROM #json_prepared").fetchval() == 0


@pytest.mark.parametrize("method", ["fetchone", "fetchmany", "fetchall"])
def test_json_empty_result_rowcount_matches_text(json_connection, method):
    counts = []
    with json_connection.cursor() as cursor:
        cursor.json_output = "objects"
        for sql_type in ["JSON", "NVARCHAR(MAX)"]:
            cursor.execute(f"SELECT CAST(N'{{}}' AS {sql_type}) WHERE 1=0")
            assert getattr(cursor, method)() == (None if method == "fetchone" else [])
            counts.append(cursor.rowcount)
    assert counts[0] == counts[1]


@pytest.mark.parametrize("method", ["skip", "scroll"])
def test_json_relative_positioning_preserves_row_values(json_connection, method, monkeypatch):
    import mssql_python.cursor as cursor_module

    decode = Mock(wraps=_decode_json)
    monkeypatch.setattr(cursor_module, "_decode_json", decode)
    with json_connection.cursor() as cursor:
        cursor.json_output = "objects"
        cursor.execute(
            "SELECT CAST(CONCAT(N'{\"id\":', id, N'}') AS JSON) "
            "FROM (VALUES(1),(2),(3),(4)) t(id) ORDER BY id"
        )
        getattr(cursor, method)(1)
        assert cursor.fetchval() == {"id": 2}
        getattr(cursor, method)(1)
        assert cursor.fetchval() == {"id": 4}
        assert cursor.fetchone() is None
        assert decode.call_count == 2


def test_json_cursor_isolation_empty_schema_and_no_text_parsing(json_connection, monkeypatch):
    import mssql_python.cursor as cursor_module

    decode = Mock(wraps=_decode_json)
    monkeypatch.setattr(cursor_module, "_decode_json", decode)
    with json_connection.cursor() as objects, json_connection.cursor() as text:
        objects.json_output = "objects"
        objects.execute("SELECT CAST(N'{\"v\":[]}' AS JSON)")
        retained = objects.fetchval()
        assert objects.fetchone() is None
        text.execute("SELECT CAST(N'{\"v\":[]}' AS JSON)")
        assert json.loads(text.fetchval()) == retained
        assert text.fetchone() is None
        assert decode.call_count == 1
        objects.execute("SELECT CAST(N'{}' AS JSON) WHERE 1=0")
        assert objects.fetchall() == []
        objects.execute("SELECT N'{}', CAST(NULL AS JSON)")
        assert tuple(objects.fetchone()) == ("{}", None)
        assert decode.call_count == 1
    retained["v"].append("still independent after close")


def test_json_validation_failure_and_server_error_recovery(json_connection):
    from mssql_python import DatabaseError

    with json_connection.cursor() as cursor:
        cursor.execute("CREATE TABLE #json_recovery (id INT PRIMARY KEY, doc JSON)")
        cursor.execute("INSERT INTO #json_recovery VALUES (?, ?)", 1, Json({}))
        for parameter in ["{}", 1, [1], {"not": "wrapped"}]:
            cursor.setinputsizes([4, SQL_JSON])
            with pytest.raises(TypeError, match="SQL_JSON"):
                cursor.execute("INSERT INTO #json_recovery VALUES (?, ?)", 2, parameter)
        cursor.setinputsizes(None)
        with pytest.raises(DatabaseError):
            cursor.execute("INSERT INTO #json_recovery VALUES (?, ?)", 1, Json([]))
        cursor.execute("INSERT INTO #json_recovery VALUES (?, ?)", 2, Json([]))
        cursor.json_output = "objects"
        cursor.execute("SELECT doc FROM #json_recovery ORDER BY id")
        assert [row[0] for row in cursor.fetchall()] == [{}, []]


@pytest.mark.parametrize("use_prepare", [False, True])
def test_json_binding_type_switch_and_null_reuse(json_connection, use_prepare):
    with json_connection.cursor() as cursor:
        cursor.json_output = "objects"
        cursor.execute("SELECT ?", Json({}))
        assert cursor.fetchval() == {}
        for value in [None, Json({}), Json({"v": "x" * 12000}), Json([]), None]:
            cursor.setinputsizes([SQL_JSON])
            cursor.execute("SELECT ?", value, use_prepare=use_prepare, reset_cursor=False)
            assert cursor.fetchval() == (None if value is None else json.loads(value._text))
        for value in [42, "ordinary", Json({}), "ordinary again", Json([])]:
            cursor.execute("SELECT ?", value, use_prepare=use_prepare, reset_cursor=False)
            assert cursor.fetchval() == (
                json.loads(value._text) if isinstance(value, Json) else value
            )


def test_json_late_batch_mismatch_never_dispatches(json_connection, monkeypatch):
    from mssql_python import ddbc_bindings

    with json_connection.cursor() as cursor:
        cursor.execute("CREATE TABLE #json_late (a JSON, b JSON)")
        dispatch = Mock(wraps=ddbc_bindings.SQLExecuteMany)
        monkeypatch.setattr(ddbc_bindings, "SQLExecuteMany", dispatch)
        rows = [(Json({}), Json([]))] * 32 + [(Json({}), "[]")]
        with pytest.raises(TypeError, match="SQL_JSON"):
            cursor.executemany("INSERT INTO #json_late VALUES (?, ?)", rows)
        cursor.executemany("INSERT INTO #json_late VALUES (?, ?)", [])
        dispatch.assert_not_called()
        cursor.execute("SELECT COUNT(*) FROM #json_late")
        assert cursor.fetchval() == 0


@pytest.mark.parametrize("numeric", ["integer", "decimal"])
def test_ordinary_numeric_batch_checks_json_per_column_not_per_cell(
    json_connection, monkeypatch, numeric
):
    import builtins
    import mssql_python.cursor as cursor_module

    checks = []

    def counted_isinstance(value, classes):
        if classes is Json:
            checks.append(type(value))
        return builtins.isinstance(value, classes)

    with json_connection.cursor() as cursor:
        cursor.execute("CREATE TABLE #json_check_counts (a DECIMAL(12,2), b DECIMAL(12,2), c BIT)")
        values = [i if numeric == "integer" else Decimal(i) / 10 for i in range(64)]
        monkeypatch.setattr(cursor_module, "isinstance", counted_isinstance, raising=False)
        cursor.executemany(
            "INSERT INTO #json_check_counts VALUES (?, ?, ?)",
            [(value, value + 1, bool(i % 2)) for i, value in enumerate(values)],
        )
        assert len(checks) == 3
        assert cursor.execute("SELECT COUNT(*) FROM #json_check_counts").fetchval() == 64


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("streamed", [False, True])
@pytest.mark.parametrize("ordinary", [1, True, 1.5, "{}", b"{}", Decimal("1.5"), date(2025, 1, 1)])
def test_json_mixed_batch_with_null_prefix_rejected_before_dispatch(
    json_connection, monkeypatch, reverse, streamed, ordinary
):
    from mssql_python import ddbc_bindings

    text = "x" * (12000 if streamed else 1)
    rows = [(Json({}), text), (ordinary, text)]
    if reverse:
        rows.reverse()
    rows.insert(0, (None, text))
    with json_connection.cursor() as cursor:
        cursor.execute("CREATE TABLE #json_mixed (doc JSON, companion NVARCHAR(MAX))")
        scalar = Mock(wraps=ddbc_bindings.DDBCSQLExecute)
        batch = Mock(wraps=ddbc_bindings.SQLExecuteMany)
        monkeypatch.setattr(ddbc_bindings, "DDBCSQLExecute", scalar)
        monkeypatch.setattr(ddbc_bindings, "SQLExecuteMany", batch)
        with pytest.raises(TypeError, match="SQL_JSON"):
            cursor.executemany("INSERT INTO #json_mixed VALUES (?, ?)", rows)
        scalar.assert_not_called()
        batch.assert_not_called()
        assert cursor.execute("SELECT COUNT(*) FROM #json_mixed").fetchval() == 0


def test_json_streamed_batch_row_shape_rejected_before_writes(json_connection):
    with json_connection.cursor() as cursor:
        cursor.execute("CREATE TABLE #json_shape (doc JSON, txt NVARCHAR(MAX))")
        rows = [(Json({}), "x" * 12000), (Json([]), "y" * 12000, "extra")]
        with pytest.raises(ValueError, match="Inconsistent parameter row size"):
            cursor.executemany("INSERT INTO #json_shape VALUES (?, ?)", rows)
        cursor.execute("SELECT COUNT(*) FROM #json_shape")
        assert cursor.fetchval() == 0


def test_json_bulk_guard_preserves_iterator_boundary(json_connection, monkeypatch):
    consumed = []

    def rows():
        consumed.append("first")
        yield (Json({}),)
        consumed.append("tail")
        yield (1,)

    def consume(_table, parameters, **_kwargs):
        list(parameters)

    core = Mock()
    native_cursor = core.PyCoreConnection.return_value.cursor.return_value
    native_cursor.bulkcopy.side_effect = consume
    with json_connection.cursor() as cursor:
        monkeypatch.setattr(cursor, "_bulkcopy_core_and_validate", lambda *args: core)
        monkeypatch.setattr(cursor, "_build_pycore_context", dict)
        with pytest.raises(NotSupportedError, match="bulk copy"):
            cursor.bulkcopy("unused_json_guard", rows())
    assert consumed == ["first"]
    native_cursor.close.assert_called_once()
    core.PyCoreConnection.return_value.close.assert_called_once()
