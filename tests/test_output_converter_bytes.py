"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.

Output converter byte representation, fallback, ordering and ownership.
"""

import decimal
import importlib
import inspect
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import pytest
import mssql_python
from mssql_python import ddbc_bindings
from mssql_python.row import Row

METHODS = ("fetchone", "fetchmany", "fetchall")
TEXT = "\ufeffA\x00\u00e9\u03a9\U0001f642\ufffe"


def _fetch(cursor, method):
    if method == "fetchall":
        return cursor.fetchall()
    rows = []
    if method == "fetchone":
        while (row := cursor.fetchone()) is not None:
            rows.append(row)
    else:
        while batch := cursor.fetchmany(1000):
            rows.extend(batch)
    return rows


@pytest.fixture
def connection(conn_str):
    connection = mssql_python.connect(conn_str)
    try:
        yield connection
    finally:
        connection.close()


@pytest.mark.parametrize("method", METHODS)
@pytest.mark.parametrize("width", (1, 32))
def test_converter_native_bytes_and_row_major_order(connection, method, width):
    inputs = []
    raw_values = []
    native_apply = ddbc_bindings.apply_output_converters
    bridge_name = {
        "fetchone": "DDBCSQLFetchOne",
        "fetchmany": "DDBCSQLFetchMany",
        "fetchall": "DDBCSQLFetchAll",
    }[method]
    bridge = getattr(ddbc_bindings, bridge_name)

    def converter(raw):
        assert type(raw) is bytes
        inputs.append(raw)
        return raw.decode("utf-16-le") + "!"

    def observe(*args):
        status = bridge(*args)
        if method == "fetchone":
            raw_values.extend(args[1])
        else:
            for values in args[1]:
                raw_values.extend(values)
        return status

    connection.add_output_converter(mssql_python.SQL_WVARCHAR, converter)
    columns = ", ".join(f"CAST(? AS NVARCHAR(100)) AS c{i}" for i in range(width))
    expected = [TEXT + str(i) for i in range(width)]
    with connection.cursor() as cursor:
        cursor.execute(
            f"SELECT {columns} FROM (VALUES (1), (2), (3)) AS v(n) ORDER BY n",
            expected,
        )
        with patch.object(ddbc_bindings, bridge_name, side_effect=observe):
            rows = _fetch(cursor, method)
    assert [list(row) for row in rows] == [[value + "!" for value in expected]] * 3
    assert inputs == [value.encode("utf-16-le") for value in expected] * 3
    assert all(type(value).__name__ == "PyCapsule" for value in raw_values)
    # The ODBC statement/buffers are gone; retained cells still own the same bytes.
    assert native_apply(raw_values, [lambda raw: raw] * len(raw_values)) == inputs


@pytest.mark.parametrize("method", METHODS)
def test_converter_empty_nul_and_bom_values(connection, method):
    expected = ["", "\x00", "\x00\x00", "\ufeff", "\ufffe", "\U0001f642"]
    calls = []
    connection.add_output_converter(str, lambda raw: calls.append(raw) or raw)
    with connection.cursor() as cursor:
        for value in expected:
            cursor.execute("SELECT CAST(? AS NVARCHAR(10))", [value])
            assert _fetch(cursor, method)[0][0] == value.encode("utf-16-le")
    assert calls == [value.encode("utf-16-le") for value in expected]


@pytest.mark.parametrize("method", METHODS)
@pytest.mark.parametrize("fail", (False, True))
def test_converter_releases_owned_payload_after_wrapping(connection, method, fail):
    inputs = []

    def converter(raw):
        inputs.append(raw)
        if fail:
            raise ValueError("keep original")
        return raw.decode("utf-16-le")

    connection.add_output_converter(mssql_python.SQL_WVARCHAR, converter)
    with connection.cursor() as cursor:
        cursor.execute("SELECT CAST(N'abc' AS NVARCHAR(10)) FROM (VALUES (1), (2), (3)) AS v(n)")
        rows = _fetch(cursor, method)
    assert [row[0] for row in rows] == ["abc"] * 3
    assert all(type(row[0]) is str for row in rows)
    assert len(inputs) == 3
    # Only the callback's list owns these non-interned bytes after fetch returns.
    # Measure outside pytest's rewritten assertion, which retains its operands.
    first_refs = sys.getrefcount(inputs[0])
    second_refs = sys.getrefcount(inputs[1])
    third_refs = sys.getrefcount(inputs[2])
    assert (first_refs, second_refs, third_refs) == (2, 2, 2)


@pytest.mark.parametrize("method", METHODS)
@pytest.mark.parametrize("sqltype", ("VARCHAR(100)", "NVARCHAR(MAX)", "XML"))
def test_converter_failures_keep_original_text(connection, method, sqltype):
    observed = []

    def converter(raw):
        observed.append(raw)
        raise ValueError("converter failure")

    connection.add_output_converter(str, converter)
    value = "<a>text</a>" if sqltype == "XML" else "abc"
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT CAST(? AS {sqltype})", [value])
        row = _fetch(cursor, method)[0]
    assert type(row[0]) is str
    assert row[0] == value
    assert observed == [value.encode("utf-16-le")]


@pytest.mark.parametrize("method", METHODS)
def test_converter_null_bypass_and_arbitrary_results(connection, method):
    inputs = []
    result = object()

    def converter(value):
        inputs.append(value)
        return result

    for sqltype in (
        mssql_python.SQL_WVARCHAR,
        mssql_python.SQL_VARBINARY,
        mssql_python.SQL_GUID,
        mssql_python.SQL_SS_XML,
        mssql_python.SQL_DECIMAL,
    ):
        connection.add_output_converter(sqltype, converter)
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT CAST(NULL AS NVARCHAR(10)), CAST(NULL AS VARBINARY(10)), "
            "CAST(NULL AS UNIQUEIDENTIFIER), CAST(NULL AS XML), CAST(NULL AS DECIMAL(10,2)), "
            "CAST(N'abc' AS NVARCHAR(10)), CAST(0x0102 AS VARBINARY(2)), "
            "CAST(1.25 AS DECIMAL(10,2))"
        )
        row = _fetch(cursor, method)[0]
    assert list(row[:5]) == [None] * 5
    assert all(value is result for value in row[5:])
    assert inputs == [b"a\x00b\x00c\x00", b"\x01\x02", decimal.Decimal("1.25")]


@pytest.mark.parametrize("method", METHODS)
@pytest.mark.parametrize("mutation", ("replace", "remove", "clear"))
def test_converter_mutation_during_fetch_preserves_batch_snapshot(connection, method, mutation):
    inputs = []

    def original(raw):
        inputs.append(raw)
        if mutation == "replace":
            connection.add_output_converter(mssql_python.SQL_WVARCHAR, lambda value: "new")
        elif mutation == "remove":
            connection.remove_output_converter(mssql_python.SQL_WVARCHAR)
        else:
            connection.clear_output_converters()
        return "old"

    connection.add_output_converter(mssql_python.SQL_WVARCHAR, original)
    with connection.cursor() as cursor:
        cursor.execute("SELECT CAST(N'abc' AS NVARCHAR(10)) FROM (VALUES (1), (2), (3)) AS v(n)")
        rows = _fetch(cursor, method)
        expected = ["old"] * 3
        if method == "fetchone":
            expected = ["old"] + (["new"] if mutation == "replace" else ["abc"]) * 2
        assert [row[0] for row in rows] == expected
        assert len(inputs) == (1 if method == "fetchone" else 3)
        connection.clear_output_converters()
        cursor.execute("SELECT CAST(N'reused' AS NVARCHAR(10))")
        assert cursor.fetchone()[0] == "reused"


@pytest.mark.parametrize("method", METHODS)
@pytest.mark.parametrize("hook", ("constructor", "converter"))
def test_custom_row_hooks_receive_original_values(connection, method, hook, monkeypatch):
    cursor_module = importlib.import_module("mssql_python.cursor")
    seen = []

    class CustomRow(Row):
        def __init__(self, values, *args, **kwargs):
            seen.append(list(values))
            super().__init__(values, *args, **kwargs)

    original = Row._apply_output_converters_optimized

    def custom_convert(self, values, converters):
        seen.append(list(values))
        return original(self, values, converters)

    if hook == "constructor":
        monkeypatch.setattr(cursor_module, "Row", CustomRow)
    else:
        monkeypatch.setattr(Row, "_apply_output_converters_optimized", custom_convert)
    connection.add_output_converter(mssql_python.SQL_WVARCHAR, lambda raw: raw)
    with connection.cursor() as cursor:
        cursor.execute("SELECT CAST(N'abc' AS NVARCHAR(10))")
        assert _fetch(cursor, method)[0][0] == b"a\x00b\x00c\x00"
    assert seen == [["abc"]]


@pytest.mark.parametrize("method", ("fetchmany", "fetchall"))
def test_converter_can_change_row_constructor_between_rows(connection, method, monkeypatch):
    cursor_module = importlib.import_module("mssql_python.cursor")
    seen = []

    class CustomRow(Row):
        def __init__(self, values, *args, **kwargs):
            seen.append(list(values))
            super().__init__(values, *args, **kwargs)

    def converter(raw):
        monkeypatch.setattr(cursor_module, "Row", CustomRow)
        return raw

    connection.add_output_converter(mssql_python.SQL_WVARCHAR, converter)
    with connection.cursor() as cursor:
        cursor.execute("SELECT CAST(N'abc' AS NVARCHAR(10)) FROM (VALUES (1), (2)) AS v(n)")
        assert [row[0] for row in _fetch(cursor, method)] == [b"a\x00b\x00c\x00"] * 2
    assert seen == [["abc"]]


@pytest.mark.parametrize("method", ("fetchmany", "fetchall"))
def test_converter_can_clear_cached_map_between_rows(connection, method):
    calls = []
    with connection.cursor() as cursor:

        def converter(raw):
            calls.append(raw)
            cursor._cached_converter_map.clear()
            return raw

        connection.add_output_converter(mssql_python.SQL_WVARCHAR, converter)
        cursor.execute(
            "SELECT CAST(N'abc' AS NVARCHAR(10)), CAST(N'def' AS NVARCHAR(10)) "
            "FROM (VALUES (1), (2)) AS v(n)"
        )
        assert [list(row) for row in _fetch(cursor, method)] == [
            [b"a\x00b\x00c\x00", "def"],
            ["abc", "def"],
        ]
    assert calls == [b"a\x00b\x00c\x00"]


@pytest.mark.parametrize("method", METHODS)
def test_converter_builtin_row_wrapping_avoids_python_dispatch(connection, method):
    calls = []
    connection.add_output_converter(
        mssql_python.SQL_WVARCHAR, lambda raw: calls.append(raw) or raw.decode("utf-16-le")
    )
    python_row_calls = []
    encodes = []
    original_init = Row.__init__.__code__
    original_apply = Row._apply_output_converters_optimized.__code__

    def profile(frame, event, argument):
        if event == "call" and frame.f_code in (original_init, original_apply):
            python_row_calls.append(frame.f_code.co_name)
        if event == "c_call" and getattr(argument, "__name__", None) == "encode":
            encodes.append(argument)

    with connection.cursor() as cursor:
        cursor.execute("SELECT CAST(N'abc' AS NVARCHAR(10)) FROM (VALUES (1), (2), (3)) AS v(n)")
        previous = sys.getprofile()
        try:
            sys.setprofile(profile)
            rows = _fetch(cursor, method)
        finally:
            sys.setprofile(previous)
    assert [row[0] for row in rows] == ["abc"] * 3
    assert calls == [b"a\x00b\x00c\x00"] * 3
    assert python_row_calls == []
    assert encodes == []


@pytest.mark.parametrize("method", METHODS)
@pytest.mark.parametrize("hook", ("__init__", "__new__", "__getattribute__", "__setattr__"))
def test_converter_row_type_mutation_invalidates_guard(connection, method, hook, monkeypatch):
    if hook == "__new__":
        # Deleting a patched __new__ does not restore CPython's original tp_new.
        # Isolate the native slot state, not just the Python attribute.
        code = (
            "import runpy, sys; "
            "runpy.run_path(sys.argv[1])['_check_converter_new_mutation']"
            "(sys.argv[2], sys.argv[3])"
        )
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                code,
                str(Path(__file__).resolve()),
                method,
                str(Path(ddbc_bindings.module.__file__).resolve()),
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 0, (result.returncode, result.stdout, result.stderr)
    else:
        _check_converter_row_type_mutation(connection, method, hook, monkeypatch)

    cursor_module = importlib.import_module("mssql_python.cursor")
    assert cursor_module._CONVERTER_ROW_BUILDER.matches(Row)
    assert Row(["abc"], {}, converter_map=[lambda raw: raw])[0] == b"a\x00b\x00c\x00"


def _check_converter_new_mutation(method, expected_native):
    import os

    assert Path(ddbc_bindings.module.__file__).resolve() == Path(expected_native)
    try:
        connection = mssql_python.connect(os.environ["DB_CONNECTION_STRING"], timeout=5)
    except mssql_python.Error as failure:
        raise AssertionError(
            f"Connection failed: {type(failure).__name__}; connection details withheld"
        ) from None
    with connection, pytest.MonkeyPatch.context() as monkeypatch:
        _check_converter_row_type_mutation(connection, method, "__new__", monkeypatch)


def _check_converter_row_type_mutation(connection, method, hook, monkeypatch):
    cursor_module = importlib.import_module("mssql_python.cursor")
    builder = cursor_module._CONVERTER_ROW_BUILDER
    assert builder.matches(Row)
    observed = []
    original = getattr(Row, hook)

    if hook == "__init__":

        def replacement(self, values, *args, **kwargs):
            observed.append(list(values))
            original(self, values, *args, **kwargs)

    elif hook == "__new__":

        def replacement(cls, *args, **kwargs):
            observed.append(list(args[0]))
            return original(cls)

        replacement = staticmethod(replacement)
    elif hook == "__getattribute__":

        def replacement(self, name):
            observed.append(name)
            return original(self, name)

    else:

        def replacement(self, name, value):
            observed.append(name)
            original(self, name, value)

    connection.add_output_converter(mssql_python.SQL_WVARCHAR, lambda raw: raw)
    with connection.cursor() as cursor:
        cursor.execute("SELECT CAST(N'abc' AS NVARCHAR(10))")
        with monkeypatch.context() as change:
            change.setattr(Row, hook, replacement)
            assert not builder.matches(Row)
            assert _fetch(cursor, method)[0][0] == b"a\x00b\x00c\x00"
    assert observed
    assert builder.matches(Row)


@pytest.mark.parametrize("method", ("fetchmany", "fetchall"))
def test_converter_changed_helper_between_rows_uses_python_fallback(
    connection, method, monkeypatch
):
    calls = []
    original_apply = Row._apply_output_converters_optimized

    def replacement(self, values, converters):
        calls.append(list(values))
        return original_apply(self, values, converters)

    def converter(raw):
        monkeypatch.setattr(Row, "_apply_output_converters_optimized", replacement)
        return raw

    connection.add_output_converter(mssql_python.SQL_WVARCHAR, converter)
    with connection.cursor() as cursor:
        cursor.execute("SELECT CAST(N'abc' AS NVARCHAR(10)) FROM (VALUES (1), (2)) AS v(n)")
        assert [row[0] for row in _fetch(cursor, method)] == [b"a\x00b\x00c\x00"] * 2
    assert calls == [["abc"]]


@pytest.mark.parametrize("method", METHODS)
def test_converter_native_helper_override_is_not_bypassed(connection, method):
    connection.add_output_converter(mssql_python.SQL_WVARCHAR, lambda raw: raw)
    calls = []
    original_apply = ddbc_bindings.apply_output_converters

    def replacement(values, converters):
        calls.append(list(values))
        return original_apply(values, converters)

    with connection.cursor() as cursor:
        cursor.execute("SELECT CAST(N'abc' AS NVARCHAR(10))")
        with patch.object(ddbc_bindings, "apply_output_converters", replacement):
            assert _fetch(cursor, method)[0][0] == b"a\x00b\x00c\x00"
    assert calls == [["abc"]]


@pytest.mark.parametrize("method", METHODS)
def test_converter_changed_function_code_invalidates_guard(connection, method, monkeypatch):
    cursor_module = importlib.import_module("mssql_python.cursor")
    builder = cursor_module._CONVERTER_ROW_BUILDER
    assert builder.matches(Row)

    def replacement(self, values, converters):
        return ["patched"] * len(values)

    connection.add_output_converter(mssql_python.SQL_WVARCHAR, lambda raw: raw)
    with connection.cursor() as cursor:
        cursor.execute("SELECT CAST(N'abc' AS NVARCHAR(10))")
        with monkeypatch.context() as change:
            change.setattr(Row._apply_output_converters_optimized, "__code__", replacement.__code__)
            assert not builder.matches(Row)
            assert _fetch(cursor, method)[0][0] == "patched"
    assert builder.matches(Row)


def test_converter_builder_releases_python_references():
    tracked = (Row, ddbc_bindings, Row.__init__, Row._apply_output_converters_optimized)
    before = [sys.getrefcount(value) for value in tracked]
    for _ in range(10):
        builder = ddbc_bindings._ConverterRowBuilder(Row, ddbc_bindings)
        assert builder.matches(Row)
        del builder
    after = [sys.getrefcount(value) for value in tracked]
    assert after == before


def test_converter_builder_rejects_unrelated_initializers():
    class CustomRow(Row):
        pass

    with pytest.raises(TypeError, match="canonical Row"):
        ddbc_bindings._ConverterRowBuilder(CustomRow, ddbc_bindings)
    with pytest.raises(TypeError, match="Row type"):
        ddbc_bindings._ConverterRowBuilder(None, ddbc_bindings)


@pytest.mark.parametrize("method", ("fetchmany", "fetchall"))
def test_converter_column_names_are_read_between_callbacks(connection, method):
    names = ("replacement",)
    calls = []
    with connection.cursor() as cursor:

        def converter(raw):
            calls.append(raw)
            cursor._cached_result_columns = names
            return raw

        connection.add_output_converter(mssql_python.SQL_WVARCHAR, converter)
        cursor.execute(
            "SELECT CAST(N'abc' AS NVARCHAR(10)) AS original FROM (VALUES (1), (2)) AS v(n)"
        )
        rows = _fetch(cursor, method)
    assert rows[0]._column_names == ("original",)
    assert rows[0]._column_names is not names
    assert rows[1]._column_names is names
    assert calls == [b"a\x00b\x00c\x00"] * 2


@pytest.mark.parametrize("method", METHODS)
def test_converter_removed_during_fetch_keeps_no_converter_row_path(connection, method):
    bridge_name = {
        "fetchone": "DDBCSQLFetchOne",
        "fetchmany": "DDBCSQLFetchMany",
        "fetchall": "DDBCSQLFetchAll",
    }[method]
    bridge = getattr(ddbc_bindings, bridge_name)
    connection.add_output_converter(mssql_python.SQL_WVARCHAR, lambda raw: raw)

    def remove_converter(*args):
        status = bridge(*args)
        if args[1]:
            connection.remove_output_converter(mssql_python.SQL_WVARCHAR)
        return status

    with connection.cursor() as cursor:
        cursor.execute("SELECT CAST(N'abc' AS NVARCHAR(10))")
        with (
            patch.object(ddbc_bindings, bridge_name, remove_converter),
            patch.object(Row, "_fast_create", wraps=Row._fast_create) as fast_one,
            patch.object(
                ddbc_bindings, "construct_rows", wraps=ddbc_bindings.construct_rows
            ) as fast_many,
        ):
            assert _fetch(cursor, method)[0][0] == "abc"
    assert fast_one.call_count == int(method == "fetchone")
    expected_batches = {"fetchone": [], "fetchmany": [1, 0], "fetchall": [1]}
    assert [len(call.args[0]) for call in fast_many.call_args_list] == expected_batches[method]


@pytest.mark.parametrize("method", ("fetchmany", "fetchall"))
def test_converter_row_field_descriptor_failure_releases_partial_batch(
    connection, method, monkeypatch
):
    import gc
    import weakref

    references = []
    descriptor = inspect.getattr_static(Row, "_column_map")

    class FailingColumnMap:
        def __get__(self, instance, owner):
            return descriptor.__get__(instance, owner)

        def __set__(self, instance, value):
            references.append(weakref.ref(instance))
            if len(references) == 2:
                raise ValueError("injected column-map failure")
            descriptor.__set__(instance, value)

    connection.add_output_converter(mssql_python.SQL_WVARCHAR, lambda raw: raw)
    with connection.cursor() as cursor:
        cursor.execute("SELECT CAST(N'abc' AS NVARCHAR(10)) FROM (VALUES (1), (2), (3)) AS v(n)")
        with monkeypatch.context() as change:
            change.setattr(Row, "_column_map", FailingColumnMap())
            with pytest.raises(ValueError, match="injected column-map failure"):
                _fetch(cursor, method)
        gc.collect()
        assert len(references) == 2
        assert all(reference() is None for reference in references)
        cursor.execute("SELECT CAST(N'reuse' AS NVARCHAR(10))")
        assert cursor.fetchone()[0] == "reuse".encode("utf-16-le")


@pytest.mark.parametrize("method", METHODS)
def test_converter_bridge_retained_inputs_are_not_mutated(connection, method):
    bridge_name = {
        "fetchone": "DDBCSQLFetchOne",
        "fetchmany": "DDBCSQLFetchMany",
        "fetchall": "DDBCSQLFetchAll",
    }[method]
    bridge = getattr(ddbc_bindings, bridge_name)
    retained = []
    connection.add_output_converter(mssql_python.SQL_WVARCHAR, lambda raw: "converted")

    def observe(*args):
        status = bridge(*args)
        if method == "fetchone":
            if args[1]:
                retained.append(args[1])
        else:
            retained.extend(args[1])
        return status

    with connection.cursor() as cursor:
        cursor.execute("SELECT CAST(N'abc' AS NVARCHAR(10)) FROM (VALUES (1), (2)) AS v(n)")
        with patch.object(ddbc_bindings, bridge_name, observe):
            assert [row[0] for row in _fetch(cursor, method)] == ["converted"] * 2
    assert len(retained) == 2
    for values in retained:
        assert type(values[0]).__name__ == "PyCapsule"
        assert ddbc_bindings.restore_output_converter_text(values) == ["abc"]


@pytest.mark.parametrize("method", METHODS)
def test_no_converter_does_not_defer_text(connection, method):
    with connection.cursor() as cursor:
        cursor.execute("SELECT CAST(? AS NVARCHAR(100))", [TEXT])
        with patch.object(
            ddbc_bindings,
            "apply_output_converters",
            side_effect=AssertionError("unexpected converter dispatch"),
        ):
            assert _fetch(cursor, method)[0][0] == TEXT


@pytest.mark.parametrize("method", METHODS)
def test_converter_respects_narrow_decoding_settings(connection, method):
    observed = []
    connection.setdecoding(mssql_python.SQL_CHAR, encoding="latin-1")
    connection.setencoding(encoding="utf-8")
    connection.add_output_converter(str, lambda raw: observed.append(raw) or raw)
    with connection.cursor() as cursor:
        cursor.execute("SELECT CONVERT(VARCHAR(1), 0xE9)")
        row = _fetch(cursor, method)[0]
    assert observed == ["\u00e9".encode("utf-16-le")]
    assert row[0] == observed[0]


@pytest.mark.parametrize("method", METHODS)
@pytest.mark.parametrize("raw_utf16", ("0x00D8", "0x00DC", "0x00D84100"))
def test_malformed_text_retains_legacy_fetch_behavior(connection, method, raw_utf16):
    outcomes = []
    for native in (False, True):
        calls = []
        connection.add_output_converter(
            mssql_python.SQL_WVARCHAR, lambda raw: calls.append(raw) or raw
        )
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT CAST({raw_utf16} AS NVARCHAR(10))")
            with patch("mssql_python.cursor._native_converter_row", return_value=native):
                try:
                    values = [tuple(row) for row in _fetch(cursor, method)]
                except UnicodeError as error:
                    values = type(error)
        outcomes.append((values, calls))
    assert outcomes[0] == outcomes[1]


@pytest.mark.parametrize(
    ("values_type", "converters_type"),
    ((list, list), (tuple, list), (list, tuple)),
    ids=("native-lists", "tuple-values", "tuple-converters"),
)
def test_native_converter_preserves_input_and_exception_semantics(values_type, converters_type):
    expected = ["abc", b"\x01", 42, None, "\ud800"]
    values = values_type(expected)
    calls = []

    def converter(value):
        calls.append(value)
        raise OverflowError("keep original")

    row = Row(values, {}, converter_map=converters_type([converter] * len(values)))
    assert row._values == expected
    assert row._values is not values
    assert calls == [b"a\x00b\x00c\x00", b"\x01", 42]
    assert values == values_type(expected)


@pytest.mark.parametrize("result", (None, []), ids=("none", "empty-list"))
def test_native_converter_preserves_falsey_results(result):
    assert Row(["abc"], {}, converter_map=[lambda raw: result])[0] is result


def test_native_converter_preserves_falsey_and_null_truth_checks():
    events = []

    class FalseConverter:
        def __bool__(self):
            events.append("bool")
            return False

        def __call__(self, value):
            pytest.fail("falsey converter was called")

    converter = FalseConverter()
    assert Row(["abc", None], {}, converter_map=[converter, converter])._values == ["abc", None]
    assert events == ["bool", "bool"]

    class RaisingConverter:
        def __bool__(self):
            raise ValueError("converter truth failure")

    with pytest.raises(ValueError, match="converter truth failure"):
        Row([None], {}, converter_map=[RaisingConverter()])


def test_native_converter_preserves_string_subclass_encoding():
    class Text(str):
        def encode(self, encoding):
            assert encoding == "utf-16-le"
            return b"custom"

    assert Row([Text("abc")], {}, converter_map=[lambda raw: raw])[0] == b"custom"


def test_native_converter_does_not_swallow_baseexception():
    def converter(raw):
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        Row(["abc"], {}, converter_map=[converter])


def test_native_converter_list_mutation_keeps_original_fallback_snapshot():
    values = ["abc", "def"]
    converters = []

    def first(raw):
        if len(values) == 2:
            values[1] = "changed"
            values.append("new")
            converters.append(first)
        return raw

    def failing(raw):
        raise ValueError("original fallback")

    converters.extend((first, failing))
    assert Row(values, {}, converter_map=converters)._values == [b"a\x00b\x00c\x00", "def"]


def test_native_converter_reference_cleanup_in_subprocess():
    source = """
import gc
import sys
from mssql_python.row import Row

class Value:
    pass

value = Value()
def converter(raw):
    return value

refs = sys.getrefcount(value)
for _ in range(1000):
    row = Row(["abc", None, 42], {}, converter_map=[converter] * 3)
    assert row[0] is value and row[1] is None and row[2] is value
    del row
gc.collect()
assert sys.getrefcount(value) == refs
"""
    result = subprocess.run(
        [sys.executable, "-c", source], capture_output=True, text=True, timeout=30
    )
    assert result.returncode == 0, result.stdout + result.stderr
