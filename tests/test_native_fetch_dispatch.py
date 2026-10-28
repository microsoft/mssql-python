# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Mutation, lifetime and mechanism coverage for the native fetch prototype."""

import cProfile
from contextlib import contextmanager, nullcontext
import gc
from pathlib import Path
import subprocess
import sys
from unittest.mock import patch
import weakref

import pytest

import mssql_python
from mssql_python import ddbc_bindings, perf_timer
from mssql_python.cursor import Cursor
from mssql_python.row import Row

METHODS = ("fetchone", "fetchmany", "fetchval", "iterator")
SELECT_FIVE = "SELECT n FROM (VALUES (1), (2), (3), (4), (5)) AS v(n) ORDER BY n"


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


def read(cursor, method):
    if method == "fetchmany":
        rows = cursor.fetchmany(1)
        assert type(rows) is list
        return rows[0] if rows else None
    if method == "iterator":
        return next(cursor, None)
    return getattr(cursor, method)()


def value(row, method):
    return row if method == "fetchval" or row is None else row[0]


def warm(cursor, method):
    cursor.execute(SELECT_FIVE)
    assert value(read(cursor, method), method) == 1
    assert value(read(cursor, method), method) == 2


@contextmanager
def _native_fetch_counters(enabled=True):
    profiling = getattr(ddbc_bindings, "profiling", None) if enabled else None
    if profiling is not None:
        assert not profiling.is_enabled()
        profiling.reset()
        profiling.enable()
    try:
        yield profiling
    finally:
        if profiling is not None:
            profiling.disable()


@contextmanager
def _fetchone_window(profiling, enabled=True):
    if not enabled:
        yield None
        return
    if profiling is not None:
        assert profiling.is_enabled()
    before = profiling.get_stats() if profiling is not None else None
    profile = cProfile.Profile()
    observation = {}
    profile.enable()
    try:
        yield observation
    finally:
        profile.disable()
        observation["python"] = {
            entry.code: entry.callcount
            for entry in profile.getstats()
            if not isinstance(entry.code, str)
        }
        after = profiling.get_stats() if profiling is not None else None
        observation["native"] = (
            {
                name: after.get(name, {}).get("calls", 0) - before.get(name, {}).get("calls", 0)
                for name in before.keys() | after.keys()
            }
            if profiling is not None
            else None
        )
        if profiling is not None:
            assert profiling.is_enabled()
            assert all(count >= 0 for count in observation["native"].values())


def _assert_fetchone_counts(observation, *, prepare, native, fallback, sql):
    from mssql_python.cursor import _NATIVE_FETCH_RUNTIME

    calls = observation["python"]
    assert calls.get(type(_NATIVE_FETCH_RUNTIME[2]).__call__.__code__, 0) == prepare
    assert calls.get(_NATIVE_FETCH_RUNTIME[3][0].__code__, 0) == fallback
    if observation["native"] is not None:
        for name, expected in (
            ("NativeCursor::prepare", prepare),
            ("NativeCursor::fetch", native),
            ("NativeCursor::fallback", fallback),
            ("NativeCursor::completion_fallback", 0),
            ("FetchOne_wrap", fallback),
            ("FetchSingleRow", sql),
        ):
            assert observation["native"].get("ddbc::" + name, 0) == expected, name


def test_repeated_negative_fetchone_skips_prepare(connection):
    if ddbc_bindings._fetch_observer is None:
        pytest.skip("Requires supported CPython watchers")
    calls = []

    def convert(raw):
        calls.append(raw)
        return raw * 10

    connection.add_output_converter(mssql_python.SQL_INTEGER, convert)
    try:
        with _native_fetch_counters() as profiling, connection.cursor() as cursor:
            cursor.execute(SELECT_FIVE)
            assert cursor.fetchone()[0] == 10
            assert calls == [1]
            with _fetchone_window(profiling) as repeated:
                rows = [cursor.fetchone() for _ in range(5)]
            assert [value(row, "fetchone") for row in rows] == [20, 30, 40, 50, None]
            assert calls == [1, 2, 3, 4, 5]
            assert repeated["python"].get(convert.__code__, 0) == 4
            assert repeated["python"].get(Cursor._complete_fetchone.__code__, 0) == 0
            assert repeated["python"].get(Cursor._wrap_fetchone.__code__, 0) == 0
            _assert_fetchone_counts(repeated, prepare=0, native=0, fallback=5, sql=5)
            assert cursor.rowcount == 5
            assert cursor.rownumber == 4
    finally:
        connection.remove_output_converter(mssql_python.SQL_INTEGER)


@pytest.mark.parametrize("method", METHODS)
@pytest.mark.parametrize("python_profiling", (False, True))
def test_warm_native_dispatch_removes_python_control_calls(
    connection, method, python_profiling, request
):
    if ddbc_bindings._fetch_observer is None:
        pytest.skip("Direct native dispatch requires GIL-enabled CPython 3.12+ watchers")
    assert not perf_timer.is_enabled()
    if python_profiling:
        request.addfinalizer(perf_timer.disable)
        perf_timer.enable()
    with _native_fetch_counters() as profiling, connection.cursor() as cursor:
        cursor._get_sql_to_c_type_map()
        cursor.setinputsizes([(mssql_python.SQL_INTEGER, 0, 0)])
        with pytest.warns(Warning, match="Number of input sizes"):
            cursor.execute("SELECT ?, ?", 1, 2)
        warm(cursor, method)
        with _fetchone_window(profiling) as observation:
            rows = [read(cursor, method) for _ in range(4)]
        assert [value(row, method) for row in rows] == [3, 4, 5, None]
        assert cursor.rowcount == 5
        assert cursor.rownumber == 4
        forbidden = {
            Cursor._get_column_and_converter_maps.__code__,
            Cursor._increment_rownumber.__code__,
            Cursor._complete_fetchone.__code__,
            Cursor._complete_fetchmany.__code__,
        }
        from mssql_python.cursor import _NATIVE_FETCH_RUNTIME

        forbidden.update(function.__code__ for function in _NATIVE_FETCH_RUNTIME[3])
        calls = observation["python"]
        assert not forbidden.intersection(calls)
        assert calls.get(Row._fast_create.__code__, 0) == (3 if method == "iterator" else 0)
        if observation["native"] is not None:
            counts = observation["native"]
            assert counts["ddbc::NativeCursor::fetch"] == 4
            assert counts["ddbc::FetchSingleRow"] == 4
            for name in (
                "NativeCursor::prepare",
                "NativeCursor::fallback",
                "NativeCursor::completion_fallback",
                "FetchOne_wrap",
                "FetchMany_wrap",
                "FetchBatchData",
                "SQLBindColums",
            ):
                assert counts.get("ddbc::" + name, 0) == 0


@pytest.mark.parametrize("method", METHODS)
@pytest.mark.parametrize("scope", ("instance", "class"))
def test_warm_map_override_and_restoration(connection, method, scope):
    with connection.cursor() as cursor:
        warm(cursor, method)
        target = cursor if scope == "instance" else Cursor
        with patch.object(
            target, "_get_column_and_converter_maps", side_effect=RuntimeError("map override")
        ):
            with pytest.raises(RuntimeError, match="map override"):
                read(cursor, method)
        assert cursor.rowcount == 3
        assert cursor.rownumber == 2
        assert value(read(cursor, method), method) == 4
        assert value(read(cursor, method), method) == 5

    if method == "fetchone" and scope == "instance" and ddbc_bindings._fetch_observer is not None:
        connection.add_output_converter(mssql_python.SQL_INTEGER, lambda raw: raw)
        try:
            with _native_fetch_counters() as profiling, connection.cursor() as cursor:
                warm(cursor, "fetchone")
                with patch.object(
                    cursor,
                    "_get_column_and_converter_maps",
                    side_effect=RuntimeError("negative map override"),
                ):
                    with _fetchone_window(profiling) as failed:
                        with pytest.raises(RuntimeError, match="negative map override"):
                            cursor.fetchone()
                _assert_fetchone_counts(failed, prepare=0, native=0, fallback=1, sql=1)
                assert cursor.rowcount == 3
                assert cursor.rownumber == 2
                assert cursor.fetchone()[0] == 4
                assert cursor.fetchone()[0] == 5
        finally:
            connection.remove_output_converter(mssql_python.SQL_INTEGER)


@pytest.mark.parametrize("method", METHODS)
def test_warm_factory_override_keeps_consumed_position(connection, method):
    with connection.cursor() as cursor:
        warm(cursor, method)
        target, name = (
            (ddbc_bindings, "construct_rows") if method == "fetchmany" else (Row, "_fast_create")
        )
        with patch.object(target, name, side_effect=RuntimeError("factory override")):
            with pytest.raises(RuntimeError, match="factory override"):
                read(cursor, method)
        assert cursor.rowcount == 3
        assert cursor.rownumber == 2
        assert value(read(cursor, method), method) == 4


@pytest.mark.parametrize("method", ("fetchval", "iterator"))
def test_warm_fetchone_override_is_not_bypassed(connection, method):
    with connection.cursor() as cursor:
        warm(cursor, method)
        original = cursor.fetchone
        with patch.object(cursor, "fetchone", wraps=original) as override:
            assert value(read(cursor, method), method) == 3
            override.assert_called_once_with()
        assert value(read(cursor, method), method) == 4


@pytest.mark.parametrize("method", METHODS)
def test_warm_dictionary_replacement_and_direct_counter_mutation(connection, method):
    with connection.cursor() as cursor:
        warm(cursor, method)
        old = cursor.__dict__
        cursor.__dict__ = old.copy()
        assert value(read(cursor, method), method) == 3
        assert old["rowcount"] == 2
        assert cursor.rowcount == 3
        cursor.__dict__["_next_row_index"] = 20
        assert value(read(cursor, method), method) == 4
        assert cursor.rowcount == 21
        assert cursor.rownumber == 20
        connection.__dict__ = connection.__dict__.copy()
        assert value(read(cursor, method), method) == 5
        assert cursor.rowcount == 22

    if method == "fetchone" and ddbc_bindings._fetch_observer is not None:
        connection.add_output_converter(mssql_python.SQL_INTEGER, lambda raw: raw)
        try:
            with _native_fetch_counters() as profiling, connection.cursor() as cursor:
                warm(cursor, "fetchone")
                old = cursor.__dict__
                cursor.__dict__ = old.copy()
                with _fetchone_window(profiling) as replacement:
                    assert cursor.fetchone()[0] == 3
                _assert_fetchone_counts(replacement, prepare=1, native=0, fallback=1, sql=1)
                assert old["rowcount"] == 2
                assert cursor.rowcount == 3
                assert cursor.rownumber == 2
        finally:
            connection.remove_output_converter(mssql_python.SQL_INTEGER)


@pytest.mark.parametrize("method", METHODS)
@pytest.mark.parametrize("target", ("cursor", "connection"))
def test_warm_dictionary_subclass_uses_python_fallback(connection, method, target):
    class CustomDict(dict):
        pass

    with connection.cursor() as cursor:
        warm(cursor, method)
        owner = cursor if target == "cursor" else connection
        owner.__dict__ = CustomDict(owner.__dict__)
        assert value(read(cursor, method), method) == 3
        assert cursor.rowcount == 3
        assert cursor.rownumber == 2
        assert value(read(cursor, method), method) == 4
        owner.__dict__ = dict(owner.__dict__)
        assert value(read(cursor, method), method) == 5
        assert cursor.rowcount == 5
        assert cursor.rownumber == 4
        assert read(cursor, method) is None


def test_negative_fetchone_uncertain_dictionary_uses_full_policy(connection):
    if ddbc_bindings._fetch_observer is None:
        pytest.skip("Requires supported CPython watchers")
    calls = []
    armed = False

    class Key:
        def __hash__(self):
            calls.append("hash")
            return hash("_connection")

        def __eq__(self, other):
            calls.append("eq")
            if armed:
                raise RuntimeError("uncertain dictionary policy")
            return False

    class TextKey(str):
        __hash__ = Key.__hash__
        __eq__ = Key.__eq__

    class CustomDict(dict):
        def get(self, *args, **kwargs):
            calls.append("get")
            if armed:
                raise RuntimeError("uncertain dictionary policy")
            return super().get(*args, **kwargs)

    connection.add_output_converter(mssql_python.SQL_INTEGER, lambda raw: raw)
    try:
        for shape in ("custom-key", "late-text-key", "dict-subclass"):
            with _native_fetch_counters() as profiling, connection.cursor() as cursor:
                if shape == "dict-subclass":
                    cursor.__dict__ = CustomDict(cursor.__dict__)
                warm(cursor, "fetchone")
                data = cursor.__dict__
                original = dict(data)
                try:
                    if shape != "dict-subclass":
                        key = Key() if shape == "custom-key" else TextKey("_connection")
                        del data["_connection"]
                        data[key] = connection
                        assert list(data)[-1] is key
                    calls.clear()
                    armed = True
                    with _fetchone_window(profiling) as uncertain:
                        with pytest.raises(RuntimeError, match="uncertain dictionary policy"):
                            cursor.fetchone()
                    assert cursor.__dict__ is data
                    assert calls == (["get"] if shape == "dict-subclass" else ["eq"])
                    _assert_fetchone_counts(uncertain, prepare=1, native=0, fallback=0, sql=0)
                finally:
                    armed = False
                    cursor.__dict__ = original
                assert cursor.rowcount == 2
                assert cursor.rownumber == 1
                assert cursor.fetchone()[0] == 3
    finally:
        connection.remove_output_converter(mssql_python.SQL_INTEGER)


def test_conversion_prefilter_declines_callback_capable_dictionary_shapes():
    if ddbc_bindings._fetch_observer is None:
        pytest.skip("Conversion prefilter requires supported CPython watchers")
    calls = []

    class Key:
        def __hash__(self):
            calls.append("hash")
            return hash("_cached_converter_map")

        def __eq__(self, other):
            calls.append("eq")
            return False

    class TextKey(str):
        __hash__ = Key.__hash__
        __eq__ = Key.__eq__

    class CustomDict(dict):
        def get(self, *args, **kwargs):
            calls.append("get")
            raise AssertionError("Custom get must not run in the prefilter")

    key = Key()
    text_key = TextKey("unrelated")
    custom_key_data = {key: None, "_cached_converter_map": []}
    late_key_data = {"_cached_converter_map": [], text_key: None}
    subclass_data = CustomDict({"_cached_converter_map": []})
    assert list(custom_key_data)[0] is key
    assert list(late_key_data)[-1] is text_key
    helper = ddbc_bindings._fetch_conversion_ineligible
    for data in (custom_key_data, late_key_data, subclass_data):
        calls.clear()
        assert helper(data) is False
        assert calls == []

    distractors = {
        name[:-2] + character + name[-1]: (0,)
        for name in ("_cached_converter_map", "_uuid_str_indices")
        for character in ("x", "\u00e9", "\0")
    }
    assert len(distractors) == 6
    assert all(type(key) is str and len(key) in (17, 21) for key in distractors)
    data = {"_cached_converter_map": (), "_uuid_str_indices": None, **distractors}
    assert helper(data) is False
    data["_cached_converter_map"] = (0,)
    assert helper(data) is True
    data["_cached_converter_map"] = []
    assert helper(data) is True
    data["_cached_converter_map"] = ()
    data["_uuid_str_indices"] = ()
    assert helper(data) is True
    data["_uuid_str_indices"] = None
    assert helper(data) is False
    del data["_cached_converter_map"]
    assert helper(data) is True


@pytest.mark.parametrize("method", METHODS)
@pytest.mark.parametrize("write", ("attribute", "object_setattr"))
def test_warm_attribute_writes_invalidate_native_position(connection, method, write):
    with connection.cursor() as cursor:
        for _ in range(12):
            warm(cursor, method)
            if write == "attribute":
                cursor._next_row_index = 40
            else:
                object.__setattr__(cursor, "_next_row_index", 40)
            assert value(read(cursor, method), method) == 3
            assert cursor.rowcount == 41
            assert cursor.rownumber == 40


@pytest.mark.parametrize("method", ("fetchone", "fetchmany", "iterator"))
def test_warm_in_place_column_map_clear_uses_legacy_fallback(connection, method):
    with connection.cursor() as cursor:
        warm(cursor, method)
        legacy = {"legacy": 0}
        cursor._column_name_map = legacy
        assert value(read(cursor, method), method) == 3
        cursor._cached_column_map.clear()
        row = read(cursor, method)
        assert row[0] == 4
        assert row._column_map is legacy


@pytest.mark.parametrize("method", METHODS)
def test_warm_function_code_mutation_and_restoration(connection, method):
    def fail_map(self):
        raise RuntimeError("code mutation")

    with connection.cursor() as cursor:
        warm(cursor, method)
        function = Cursor._get_column_and_converter_maps
        original = function.__code__
        try:
            function.__code__ = fail_map.__code__
            helper_patch = nullcontext()
            if method == "fetchone" and ddbc_bindings._fetch_observer is not None:
                helper_patch = patch.object(
                    ddbc_bindings,
                    "_fetch_conversion_ineligible",
                    side_effect=AssertionError("substituted conversion predicate"),
                )
            with helper_patch as helper_trap:
                with pytest.raises(RuntimeError, match="code mutation"):
                    read(cursor, method)
                if helper_trap is not None:
                    helper_trap.assert_not_called()
        finally:
            function.__code__ = original
        assert cursor.rowcount == 3
        assert value(read(cursor, method), method) == 4

    if method == "fetchone" and ddbc_bindings._fetch_observer is not None:
        connection.add_output_converter(mssql_python.SQL_INTEGER, lambda raw: raw)
        try:
            with _native_fetch_counters() as profiling, connection.cursor() as cursor:
                warm(cursor, "fetchone")
                try:
                    function.__code__ = fail_map.__code__
                    with patch.object(
                        ddbc_bindings,
                        "_fetch_conversion_ineligible",
                        side_effect=AssertionError("negative substituted conversion predicate"),
                    ) as helper_trap:
                        with _fetchone_window(profiling) as failed:
                            with pytest.raises(RuntimeError, match="code mutation"):
                                cursor.fetchone()
                        helper_trap.assert_not_called()
                    _assert_fetchone_counts(failed, prepare=0, native=0, fallback=1, sql=1)
                finally:
                    function.__code__ = original
                assert cursor.rowcount == 3
                assert cursor.rownumber == 2
                assert cursor.fetchone()[0] == 4
        finally:
            connection.remove_output_converter(mssql_python.SQL_INTEGER)


@pytest.mark.parametrize("method", METHODS)
def test_converter_callback_mutation_retains_row_stage(connection, method):
    calls = []

    def convert(raw):
        calls.append(raw)
        connection.remove_output_converter(mssql_python.SQL_INTEGER)
        return raw * 10

    tracked = method == "fetchone" and ddbc_bindings._fetch_observer is not None
    with _native_fetch_counters(tracked) as profiling, connection.cursor() as cursor:
        warm(cursor, method)
        connection.add_output_converter(mssql_python.SQL_INTEGER, convert)
        assert value(read(cursor, method), method) == 30
        assert calls == [3]
        with _fetchone_window(profiling, tracked) as refresh:
            assert value(read(cursor, method), method) == 4
        if tracked:
            _assert_fetchone_counts(refresh, prepare=0, native=0, fallback=1, sql=1)
            assert cursor._cached_converter_map == ()
            assert cursor._cached_converters_generation == connection._converters_generation
        with _fetchone_window(profiling, tracked) as reenabled:
            assert value(read(cursor, method), method) == 5
        if tracked:
            _assert_fetchone_counts(reenabled, prepare=1, native=1, fallback=0, sql=1)
            assert refresh["python"].get(convert.__code__, 0) == 0
            assert reenabled["python"].get(convert.__code__, 0) == 0
        assert cursor.rowcount == 5

    if tracked:
        connection.add_output_converter(mssql_python.SQL_INTEGER, lambda raw: raw)
        try:
            with _native_fetch_counters() as profiling, connection.cursor() as cursor:
                warm(cursor, "fetchone")
                connection.add_output_converter(mssql_python.SQL_INTEGER, lambda raw: raw * 10)
                with patch.object(
                    connection,
                    "get_output_converter",
                    side_effect=RuntimeError("negative converter refresh"),
                ) as converter_hook:
                    with _fetchone_window(profiling) as failed:
                        with pytest.raises(RuntimeError, match="negative converter refresh"):
                            cursor.fetchone()
                    converter_hook.assert_called_once_with(mssql_python.SQL_INTEGER)
                _assert_fetchone_counts(failed, prepare=0, native=0, fallback=1, sql=1)
                assert cursor.rowcount == 3
                assert cursor.rownumber == 2
                assert cursor.fetchone()[0] == 40
        finally:
            connection.remove_output_converter(mssql_python.SQL_INTEGER)


@pytest.mark.parametrize("method", METHODS)
def test_warm_native_result_transitions_and_closure(connection, method):
    with connection.cursor() as cursor:
        warm(cursor, method)
        assert [row[0] for row in cursor.fetchmany(2)] == [3, 4]
        assert value(read(cursor, method), method) == 5
        cursor.execute("SELECT 11 AS first; SELECT 12 AS second")
        assert value(read(cursor, method), method) == 11
        assert cursor.nextset()
        assert value(read(cursor, method), method) == 12
        cursor.execute("SELECT n FROM (VALUES (1), (2), (3)) AS v(n) WHERE 1 = 0")
        assert read(cursor, method) is None
        assert cursor.rowcount == 0
        cursor.execute(SELECT_FIVE)
        assert value(read(cursor, method), method) == 1
        connection.close()
        with pytest.raises(mssql_python.ProgrammingError, match="closed"):
            read(cursor, method)


def test_native_retained_rows_and_cycles_are_collectible(connection):
    cursor = connection.cursor()
    warm(cursor, "fetchone")
    row = cursor.fetchone()
    mapping = row._mapping
    cursor.saved_row = row
    cursor_reference, row_reference = weakref.ref(cursor), weakref.ref(row)
    cursor.close()
    assert list(mapping.values()) == [3]
    del mapping, row, cursor
    gc.collect()
    assert cursor_reference() is None
    assert row_reference() is None


def test_native_fetchmany_argument_contract(connection):
    with connection.cursor() as cursor:
        warm(cursor, "fetchmany")
        assert cursor.fetchmany(size=1)[0][0] == 3
        assert cursor.fetchmany(None)[0][0] == 4
        assert cursor.fetchmany(True)[0][0] == 5
        assert cursor.fetchmany(0) == []
        for call in (
            lambda: cursor.fetchmany(1, 2),
            lambda: cursor.fetchmany(1, size=1),
            lambda: cursor.fetchmany(unknown=1),
        ):
            with pytest.raises(TypeError):
                call()


def test_native_fetchmany_keyword_subclass_preserves_binding_error(connection):
    calls = []

    class Keyword(str):
        __hash__ = str.__hash__

        def __eq__(self, other):
            calls.append(other)
            raise RuntimeError("keyword equality sentinel")

    keywords = {Keyword("size"): 1}
    with _native_fetch_counters() as profiling, connection.cursor() as cursor:
        warm(cursor, "fetchmany")
        before = profiling.get_stats() if profiling is not None else {}
        with pytest.raises(RuntimeError, match="keyword equality sentinel"):
            cursor.fetchmany(**keywords)
        assert calls
        assert cursor.rowcount == 2
        assert cursor.rownumber == 1
        if profiling is not None:
            after = profiling.get_stats()
            for name in ("ddbc::NativeCursor::fetch", "ddbc::FetchSingleRow"):
                assert after.get(name, {}).get("calls", 0) == before.get(name, {}).get("calls", 0)
        assert cursor.fetchmany(size=1)[0][0] == 3


def test_native_fetchmany_oversized_int_preserves_closed_cursor_error():
    if ddbc_bindings._fetch_observer is None:
        pytest.skip("Native size prefilter requires supported CPython watchers")
    cursor = Cursor.__new__(Cursor)
    cursor.closed = True
    with pytest.raises(mssql_python.ProgrammingError, match="cursor is closed"):
        cursor.fetchmany(1 << 100)


def test_native_unbound_keyword_self_preserves_python_signature(connection):
    with connection.cursor() as cursor:
        warm(cursor, "fetchone")
        assert Cursor.fetchone(self=cursor)[0] == 3
        assert Cursor.fetchmany(self=cursor, size=1)[0][0] == 4
        assert Cursor.fetchval(self=cursor) == 5
        with pytest.raises(StopIteration):
            Cursor.__next__(self=cursor)


def test_cursor_subclass_object_allocator_and_fetch_override(connection):
    class CustomCursor(Cursor):
        def __new__(cls, *args, **kwargs):
            return object.__new__(cls)

        def fetchone(self):
            row = super().fetchone()
            self.fetch_calls += 1
            return row

    with CustomCursor(connection) as cursor:
        cursor.fetch_calls = 0
        cursor.execute(SELECT_FIVE)
        assert cursor.fetchval() == 1
        assert next(cursor)[0] == 2
        assert cursor.fetch_calls == 2


def test_native_arrow_interleave(connection):
    pytest.importorskip("pyarrow")
    with connection.cursor() as cursor:
        warm(cursor, "fetchone")
        assert cursor.arrow_batch(2).column(0).to_pylist() == [3, 4]
        assert cursor.fetchone()[0] == 5
        assert cursor.rowcount == 5
        assert cursor.rownumber == 4


def test_native_executemany_and_catalog_transitions(connection):
    with connection.cursor() as cursor:
        warm(cursor, "fetchone")
        cursor.execute("CREATE TABLE #native_fetch_transition (value INT)")
        try:
            cursor.executemany(
                "INSERT INTO #native_fetch_transition (value) VALUES (?)", [(7,), (8,), (9,)]
            )
            assert cursor.fetchval() is None
            cursor.execute("SELECT value FROM #native_fetch_transition ORDER BY value")
            assert cursor.fetchone()[0] == 7
            assert cursor.fetchone()[0] == 8
            retained = cursor.fetchone()
            cursor.columns(table="__native_fetch_no_such_table__")
            assert cursor.fetchmany(1) == []
            assert cursor.rowcount == 0
            assert retained[0] == 9
            cursor.execute("SELECT 10 AS replacement")
            assert cursor.fetchval() == 10
        finally:
            cursor.execute("DROP TABLE #native_fetch_transition")


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Windows does not export the native SQLFetch function-pointer global",
)
@pytest.mark.parametrize("method", METHODS)
@pytest.mark.parametrize("mutation", ("override", "dictionary", "converter"))
def test_mutation_during_native_fetch_completes_once(conn_str, method, mutation):
    if not conn_str:
        pytest.skip("DB_CONNECTION_STRING is required")
    if ddbc_bindings._fetch_observer is None:
        pytest.skip("Requires supported CPython watchers")
    # Pointer interception and its callback lifetime are confined to a subprocess.
    code = (
        "import runpy, sys; "
        "runpy.run_path(sys.argv[1])['_mutate_during_fetch'](sys.argv[2], sys.argv[3])"
    )
    result = subprocess.run(
        [sys.executable, "-c", code, str(Path(__file__).resolve()), method, mutation],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, (result.returncode, result.stdout, result.stderr)


def _mutate_during_fetch(method, mutation):
    import ctypes
    import os

    with mssql_python.connect(os.environ["DB_CONNECTION_STRING"], timeout=5) as connection:
        with connection.cursor() as cursor:
            warm(cursor, method)
            library = ctypes.CDLL(str(Path(ddbc_bindings.module.__file__).resolve()))
            pointer = ctypes.c_void_p.in_dll(library, "SQLFetch_ptr")
            original = pointer.value
            fetch_type = ctypes.CFUNCTYPE(ctypes.c_short, ctypes.c_void_p)
            fetch = fetch_type(original)
            callback_calls, errors = [], []

            @fetch_type
            def intercepted(handle):
                try:
                    callback_calls.append(handle)
                    if mutation == "dictionary":
                        cursor.__dict__ = cursor.__dict__.copy()
                        cursor.__dict__["_next_row_index"] = 40
                    elif mutation == "converter":
                        connection.add_output_converter(
                            mssql_python.SQL_INTEGER, lambda raw: raw * 10
                        )
                    else:

                        def fail_map():
                            raise RuntimeError("mutation inside SQLFetch")

                        cursor._get_column_and_converter_maps = fail_map
                    return fetch(handle)
                except BaseException as error:
                    errors.append(type(error).__name__)
                    return -1

            pointer.value = ctypes.cast(intercepted, ctypes.c_void_p).value
            try:
                if mutation == "override":
                    with pytest.raises(RuntimeError, match="mutation inside SQLFetch"):
                        read(cursor, method)
                    assert cursor.rowcount == 3
                    del cursor._get_column_and_converter_maps
                else:
                    assert value(read(cursor, method), method) == (
                        30 if mutation == "converter" else 3
                    )
                    assert cursor.rowcount == (41 if mutation == "dictionary" else 3)
            finally:
                pointer.value = original
            assert not errors, errors
            assert len(callback_calls) == 1
            connection.clear_output_converters()
            assert value(read(cursor, method), method) == 4


def _redesign_subprocess(function, *arguments):
    code = "import runpy, sys; runpy.run_path(sys.argv[1])[sys.argv[2]](*sys.argv[3:])"
    result = subprocess.run(
        [sys.executable, "-c", code, str(Path(__file__).resolve()), function, *arguments],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, (result.returncode, result.stdout, result.stderr)


def _redesign_fault_bindings():
    _redesign_runtime()
    if not hasattr(ddbc_bindings, "_native_fetch_test_fault_schema"):
        pytest.skip("Requires separate MSSQL_NATIVE_FETCH_TEST_FAULTS=ON build")
    assert ddbc_bindings._native_fetch_test_fault_schema == 1
    assert hasattr(ddbc_bindings, "profiling")
    return ddbc_bindings._native_fetch_test_arm, ddbc_bindings._native_fetch_test_snapshot


def _redesign_position(data):
    return tuple(data[name] for name in ("_next_row_index", "_rownumber", "rowcount"))


def _redesign_delta(after, before, name):
    return after[name] - before[name]


@pytest.mark.parametrize("stage", (1, 2, 3, 4))
def test_redesign_allocation_failure_contract(conn_str, stage):
    _redesign_fault_bindings()
    _redesign_subprocess("_redesign_allocation_failure", str(stage), "absent")


def test_redesign_copied_state_publication_failure_contract(conn_str):
    _redesign_fault_bindings()
    _redesign_subprocess("_redesign_allocation_failure", "4", "copied")


def _redesign_allocation_failure(stage, key_mode):
    import os

    stage = int(stage)
    arm, snapshot = _redesign_fault_bindings()
    gc.collect()
    origin = snapshot(None)
    assert origin["schema"] == 1 and origin["active"] == 0
    with (
        mssql_python.connect(os.environ["DB_CONNECTION_STRING"], timeout=5) as connection,
        _native_fetch_counters() as profiling,
    ):
        assert profiling is not None

        def scenario():
            cursor = connection.cursor()
            old_dict = old_state = None
            try:
                if key_mode == "copied":
                    warm(cursor, "fetchone")
                    old_dict = cursor.__dict__
                    old_state = old_dict["_native_fetch_state"]
                    old_position = _redesign_position(old_dict)
                    assert snapshot(cursor)["positive_plan"]
                    cursor.__dict__ = old_dict.copy()
                else:
                    cursor.execute(SELECT_FIVE)
                    assert "_native_fetch_state" not in cursor.__dict__
                data = cursor.__dict__
                position = _redesign_position(data)
                before = snapshot(cursor)
                arm(stage)
                exception_name = None
                with _fetchone_window(profiling) as observation:
                    try:
                        cursor.fetchone()
                    except MemoryError as error:
                        exception_name = type(error).__name__
                assert exception_name == "MemoryError"
                _assert_fetchone_counts(observation, prepare=0, native=0, fallback=0, sql=0)
                after = snapshot(cursor)
                assert after["armed"] == 0 and after["hit"] == stage
                assert _redesign_delta(after, before, "hits") == 1
                visits = tuple(a - b for a, b in zip(after["visits"], before["visits"]))
                assert (
                    visits
                    == {
                        1: (1, 0, 0, 0, 0),
                        2: (1, 1, 0, 0, 0),
                        3: (1, 1, 1, 0, 0),
                        4: (1, 1, 1, 1, 1),
                    }[stage]
                )
                assert after["active"] == 0 and not after["receiver_active"]
                for name in ("states_allocated", "states_cleared", "states_deallocated"):
                    assert _redesign_delta(after, before, name) == int(stage >= 3), name
                for name in ("holders_allocated", "holders_deleted"):
                    assert _redesign_delta(after, before, name) == int(stage == 4), name
                assert _redesign_delta(after, before, "publications") == 0
                assert cursor.__dict__ is data and _redesign_position(data) == position
                if key_mode == "copied":
                    assert data["_native_fetch_state"] is old_state
                    assert old_dict["_native_fetch_state"] is old_state
                    assert _redesign_position(old_dict) == old_position
                else:
                    assert "_native_fetch_state" not in data
                assert not after["associated"]

                # The except target is gone; no exception/traceback owns the receiver.
                reference = weakref.ref(cursor)
                with _fetchone_window(profiling) as recovered:
                    row = cursor.fetchone()
                expected = 3 if key_mode == "copied" else 1
                assert row[0] == expected
                _assert_fetchone_counts(
                    recovered,
                    prepare=1,
                    native=int(key_mode == "copied"),
                    fallback=int(key_mode != "copied"),
                    sql=1,
                )
                with _fetchone_window(profiling) as native:
                    next_row = cursor.fetchone()
                assert next_row[0] == expected + 1
                _assert_fetchone_counts(
                    native, prepare=int(key_mode != "copied"), native=1, fallback=0, sql=1
                )
                recovered_state = snapshot(cursor)
                assert recovered_state["active"] == 0
                assert recovered_state["associated"] and recovered_state["positive_plan"]
                assert recovered_state["hits"] == after["hits"]
                if key_mode == "copied":
                    assert data["_native_fetch_state"] is not old_state
                    assert old_dict["_native_fetch_state"] is old_state
                    assert _redesign_position(old_dict) == old_position
                return reference, weakref.ref(row), weakref.ref(next_row)
            finally:
                arm(0)
                cursor.close()

        references = scenario()
        gc.collect()
        assert all(reference() is None for reference in references)
        end = snapshot(None)
        assert end["active"] == 0 and end["armed"] == 0
        assert _redesign_delta(end, origin, "states_allocated") == _redesign_delta(
            end, origin, "states_cleared"
        )
        assert _redesign_delta(end, origin, "holders_allocated") == _redesign_delta(
            end, origin, "holders_deleted"
        )


@pytest.mark.parametrize("boundary", ("prepublication", "last_copied_state"))
def test_redesign_publication_callback_boundaries(conn_str, boundary):
    _redesign_fault_bindings()
    _redesign_subprocess("_redesign_publication_callback", boundary)


def _redesign_publication_callback(boundary):
    import os

    arm, snapshot = _redesign_fault_bindings()
    callbacks, errors = [], []
    conflict = object()
    with (
        mssql_python.connect(os.environ["DB_CONNECTION_STRING"], timeout=5) as connection,
        _native_fetch_counters() as profiling,
        connection.cursor() as cursor,
    ):
        assert profiling is not None
        reference = weakref.ref(cursor)

        def callback():
            target = reference()
            observed = snapshot(target)
            assert observed["active"] == 1 and observed["receiver_active"]
            if boundary == "prepublication":
                assert observed["armed"] == 0 and observed["hit"] == 5
                assert _redesign_delta(observed, before, "hits") == 1
                assert _redesign_delta(observed, before, "publications") == 0
                assert not observed["associated"]
                target.__dict__ = target.__dict__.copy()
            else:
                assert _redesign_delta(observed, before, "publications") == 1
                assert _redesign_delta(observed, before, "states_deallocated") == 1
                assert _redesign_delta(observed, before, "positive_clears") == 1
                assert _redesign_delta(observed, before, "guarded_positive_clears") == 1
                assert observed["associated"] and not observed["positive_plan"]
            target.__dict__["_native_fetch_state"] = conflict
            callbacks.append(target.fetchone()[0])

        if boundary == "prepublication":
            cursor.execute(SELECT_FIVE)
            old_dict = cursor.__dict__
            old_position = _redesign_position(old_dict)
            cursor._native_fetch_test_callback = callback
        else:
            warm(cursor, "fetchone")
            assert snapshot(cursor)["positive_plan"]

            class OnRelease:
                def __del__(self):
                    try:
                        callback()
                    except BaseException as error:
                        errors.append(type(error).__name__)

            cursor.messages.append(OnRelease())
            old_dict = cursor.__dict__
            cursor.__dict__ = old_dict.copy()
            cursor.messages = []
            # Break the old State->dict->State cycle. The copied key is now its
            # only external owner; do not retain a State, Plan or messages alias.
            del old_dict["_native_fetch_state"]
            del old_dict
        before = snapshot(cursor)
        if boundary == "prepublication":
            arm(5)
        try:
            with _fetchone_window(profiling) as observation:
                row = cursor.fetchone()
        finally:
            arm(0)
        expected = 2 if boundary == "prepublication" else 4
        assert row[0] == expected and callbacks == [expected - 1]
        assert not errors, errors
        _assert_fetchone_counts(observation, prepare=0, native=0, fallback=2, sql=2)
        assert _redesign_position(cursor.__dict__) == (expected, expected - 1, expected)
        assert cursor.__dict__["_native_fetch_state"] is conflict
        after = snapshot(cursor)
        assert after["active"] == 0 and not after["associated"]
        visits = tuple(a - b for a, b in zip(after["visits"], before["visits"]))
        assert visits == (1, 1, 1, int(boundary == "last_copied_state"), 1)
        if boundary == "prepublication":
            assert _redesign_position(old_dict) == old_position
            assert "_native_fetch_state" not in old_dict
            assert _redesign_delta(after, before, "states_deallocated") == 1
            assert _redesign_delta(after, before, "holders_deleted") == 1
            del cursor._native_fetch_test_callback
            del old_dict
        else:
            assert _redesign_delta(after, before, "states_deallocated") == 2
            assert _redesign_delta(after, before, "holders_deleted") == 2
        del cursor._native_fetch_state
        with _fetchone_window(profiling) as recovered:
            assert cursor.fetchone()[0] == expected + 1
        _assert_fetchone_counts(recovered, prepare=1, native=1, fallback=0, sql=1)
        assert snapshot(cursor)["positive_plan"] and snapshot(cursor)["active"] == 0


def _redesign_runtime():
    from mssql_python.cursor import _NATIVE_FETCH_RUNTIME

    if ddbc_bindings._fetch_observer is None:
        pytest.skip("Requires supported CPython watchers")
    assert ddbc_bindings._native_fetch_schema == 2
    assert type(_NATIVE_FETCH_RUNTIME) is tuple
    assert len(_NATIVE_FETCH_RUNTIME) == 7
    assert _NATIVE_FETCH_RUNTIME[6] is ddbc_bindings._native_fetch_context
    assert Cursor.__bases__ == (object,)
    return _NATIVE_FETCH_RUNTIME


def test_redesign_state_is_lazy_and_gc_visible(connection):
    runtime = _redesign_runtime()
    with connection.cursor() as cursor:
        assert "_native_fetch_state" not in cursor.__dict__
        ddbc_bindings._invalidate_native_fetch(cursor)
        assert "_native_fetch_state" not in cursor.__dict__
        warm(cursor, "fetchone")
        state = cursor.__dict__["_native_fetch_state"]
        assert gc.is_tracked(state)
        assert gc.is_tracked(runtime[6])
        referents = gc.get_referents(state)
        assert any(item is cursor.__dict__ for item in referents)
        assert any(item is runtime[6] for item in referents)
        assert any(type(item) is weakref.ReferenceType and item() is cursor for item in referents)
        with pytest.raises(TypeError, match="created internally"):
            type(state)()


@pytest.mark.parametrize("mutation", ("delete", "copy", "none", "conflict"))
def test_redesign_state_entry_mutation_preserves_position(connection, mutation):
    _redesign_runtime()
    with connection.cursor() as cursor:
        warm(cursor, "fetchone")
        old_dict = cursor.__dict__
        old_state = old_dict["_native_fetch_state"]
        replacement = object() if mutation == "conflict" else None
        if mutation == "delete":
            del cursor._native_fetch_state
        elif mutation == "copy":
            cursor.__dict__ = old_dict.copy()
        else:
            cursor.__dict__["_native_fetch_state"] = replacement
        assert cursor.fetchone()[0] == 3
        assert cursor.rowcount == 3
        assert cursor.rownumber == 2
        if mutation in ("delete", "copy"):
            assert cursor.__dict__["_native_fetch_state"] is not old_state
        else:
            assert cursor.__dict__["_native_fetch_state"] is replacement
        if mutation == "copy":
            assert old_dict["_native_fetch_state"] is old_state
            assert old_dict["rowcount"] == 2
        assert cursor.fetchone()[0] == 4


def test_redesign_foreign_state_is_not_adopted(connection, conn_str):
    _redesign_runtime()
    with (
        mssql_python.connect(conn_str, timeout=5) as other,
        connection.cursor() as left,
        other.cursor() as right,
    ):
        warm(left, "fetchone")
        right.execute("SELECT n FROM (VALUES (101), (102), (103), (104)) AS v(n) ORDER BY n")
        assert right.fetchone()[0] == 101
        assert right.fetchone()[0] == 102
        foreign = right.__dict__["_native_fetch_state"]
        left.__dict__["_native_fetch_state"] = foreign
        assert left.fetchone()[0] == 3
        assert left.__dict__["_native_fetch_state"] is foreign
        assert right.rowcount == 2
        assert right.fetchone()[0] == 103
        assert left.rowcount == right.rowcount == 3


def test_redesign_dictionary_deletion_does_not_create_state_or_consume(connection):
    _redesign_runtime()
    with connection.cursor() as cursor:
        warm(cursor, "fetchone")
        original = cursor.__dict__
        try:
            del cursor.__dict__
            with pytest.raises(AttributeError, match="closed"):
                cursor.fetchone()
            assert "_native_fetch_state" not in cursor.__dict__
        finally:
            cursor.__dict__ = original
        assert cursor.rowcount == 2
        assert cursor.fetchone()[0] == 3


def test_redesign_state_key_acquisition_does_not_invoke_equality(connection):
    _redesign_runtime()
    calls = []
    armed = False

    class Key:
        def __hash__(self):
            if armed:
                calls.append("hash")
                raise AssertionError("State acquisition hashed a user key")
            return hash("_native_fetch_state")

        def __eq__(self, other):
            if armed:
                calls.append("eq")
                raise AssertionError("State acquisition compared a user key")
            return False

    with connection.cursor() as cursor:
        warm(cursor, "fetchone")
        del cursor._native_fetch_state
        key = Key()
        cursor.__dict__[key] = "unrelated"
        try:
            armed = True
            assert cursor.fetchone()[0] == 3
            assert calls == []
            assert all(
                type(item) is not str or item != "_native_fetch_state" for item in cursor.__dict__
            )
        finally:
            armed = False
            del cursor.__dict__[key]
        assert cursor.fetchone()[0] == 4
        assert "_native_fetch_state" in cursor.__dict__


@pytest.mark.parametrize("hook", ("__getattribute__", "__setattr__"))
def test_redesign_cold_state_respects_cursor_attribute_hooks(connection, hook):
    _redesign_runtime()
    calls = []
    original = getattr(object, hook)

    def custom(self, name, *args):
        calls.append(name)
        return original(self, name, *args)

    with connection.cursor() as cursor:
        cursor.execute(SELECT_FIVE)
        with patch.object(Cursor, hook, custom):
            assert cursor.fetchone()[0] == 1
            assert cursor.fetchone()[0] == 2
            assert "_native_fetch_state" not in object.__getattribute__(cursor, "__dict__")
        assert calls
        assert "_native_fetch_state" not in calls
        assert cursor.fetchone()[0] == 3
        assert "_native_fetch_state" in cursor.__dict__


def test_redesign_partial_initialization_keeps_first_assignments():
    assignments = []

    class RecordingCursor(Cursor):
        def __setattr__(self, name, value):
            assignments.append((name, value))
            object.__setattr__(self, name, value)

        def _initialize_cursor(self):
            raise RuntimeError("redesign initialization sentinel")

    cursor = object.__new__(RecordingCursor)
    try:
        with pytest.raises(RuntimeError, match="redesign initialization sentinel"):
            cursor.__init__(None)
        assert assignments[:2] == [("closed", False), ("hstmt", None)]
        assert "_native_fetch_state" not in cursor.__dict__
        ddbc_bindings._invalidate_native_fetch(cursor)
        assert "_native_fetch_state" not in cursor.__dict__
    finally:
        cursor.closed = True


def test_redesign_reinitialization_does_not_reuse_old_plan(connection):
    _redesign_runtime()
    cursor = connection.cursor()
    try:
        warm(cursor, "fetchone")
        row = cursor.fetchone()
        old_handle = cursor.hstmt
        cursor.close()
        Cursor.__init__(cursor, connection)
        assert cursor.hstmt is not old_handle
        cursor.execute("SELECT n FROM (VALUES (99), (100), (101)) AS v(n) ORDER BY n")
        assert [cursor.fetchone()[0] for _ in range(3)] == [99, 100, 101]
        assert list(row) == [3]
        assert cursor.rowcount == 3
    finally:
        cursor.close()


def test_redesign_plan_failure_leaves_state_recoverable_before_sql(connection):
    runtime = _redesign_runtime()
    with connection.cursor() as cursor:
        cursor.execute(SELECT_FIVE)
        before = (cursor.rowcount, cursor.rownumber)
        with patch.object(type(runtime[2]), "__call__", side_effect=MemoryError("plan sentinel")):
            with pytest.raises(MemoryError, match="plan sentinel"):
                cursor.fetchone()
        assert (cursor.rowcount, cursor.rownumber) == before
        assert gc.is_tracked(cursor.__dict__["_native_fetch_state"])
        assert cursor.fetchone()[0] == 1
        assert cursor.fetchone()[0] == 2


def test_redesign_last_plan_release_reenters_before_outer_sql(connection):
    _redesign_runtime()
    callbacks, errors = [], []
    with _native_fetch_counters() as profiling, connection.cursor() as cursor:
        warm(cursor, "fetchone")
        reference = weakref.ref(cursor)

        class OnRelease:
            def __del__(self):
                try:
                    callbacks.append(reference().fetchone()[0])
                except BaseException as error:
                    errors.append(type(error).__name__)

        cursor.messages.append(OnRelease())
        cursor.messages = []
        with _fetchone_window(profiling) as observation:
            assert cursor.fetchone()[0] == 4
        _assert_fetchone_counts(observation, prepare=1, native=0, fallback=2, sql=2)
        assert callbacks == [3]
        assert not errors
        assert cursor.rowcount == 4
        assert cursor.rownumber == 3
        assert cursor.fetchone()[0] == 5


def test_redesign_external_dictionary_alias_then_unrooted_cycle(conn_str):
    _redesign_runtime()
    _redesign_subprocess("_redesign_live_plan_cycle")


def _redesign_live_plan_cycle():
    import os

    _redesign_runtime()
    finalized, closed, completed, errors = [], [], [], []
    original_close = Cursor.close

    def close(self):
        closed.append((self.closed is False, self.hstmt is not None))
        original_close(self)
        completed.append((self.closed is True, self.hstmt is None))

    def unraisable(args):
        errors.append(type(args.exc_value).__name__)

    with (
        mssql_python.connect(os.environ["DB_CONNECTION_STRING"], timeout=5) as connection,
        _native_fetch_counters() as profiling,
    ):
        cursor = connection.cursor()
        warm(cursor, "fetchone")
        with _fetchone_window(profiling) as observation:
            row = cursor.fetchone()
        _assert_fetchone_counts(observation, prepare=0, native=1, fallback=0, sql=1)
        assert row[0] == 3 and row._cursor is cursor
        cursor.back_edge = lambda saved=row: saved
        assert cursor.back_edge() is row
        assert not cursor.closed and "_native_fetch_state" in cursor.__dict__
        snapshot = getattr(ddbc_bindings, "_native_fetch_test_snapshot", None)
        if snapshot is not None:
            assert snapshot(cursor)["positive_plan"]
        alias = cursor.__dict__
        reference, row_reference = weakref.ref(cursor), weakref.ref(row)
        weakref.finalize(cursor, finalized.append, "cursor")
        weakref.finalize(row, finalized.append, "row")
        del cursor, row
        with patch.object(Cursor, "close", close), patch.object(sys, "unraisablehook", unraisable):
            gc.collect()
            assert reference() is not None and row_reference()[0] == 3
            assert not finalized and not closed and not completed
            if snapshot is not None:
                assert snapshot(reference())["positive_plan"]
            del alias
            gc.collect()
        assert reference() is None and row_reference() is None
        assert sorted(finalized) == ["cursor", "row"]
        assert closed == [(True, True)]
        assert completed == [(True, True)]
        assert not errors
        assert _redesign_runtime()[6] is ddbc_bindings._native_fetch_context


@pytest.mark.skipif(sys.platform == "win32", reason="Requires exported SQLFetch pointer")
@pytest.mark.parametrize("mutation", ("dictionary", "delete", "conflict"))
def test_redesign_reentry_survives_state_replacement(conn_str, mutation):
    _redesign_runtime()
    code = (
        "import runpy, sys; "
        "runpy.run_path(sys.argv[1])['_redesign_reenter_during_fetch'](sys.argv[2])"
    )
    result = subprocess.run(
        [sys.executable, "-c", code, str(Path(__file__).resolve()), mutation],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, (result.returncode, result.stdout, result.stderr)


def _redesign_reenter_during_fetch(mutation):
    import ctypes
    import os

    runtime = _redesign_runtime()
    with mssql_python.connect(os.environ["DB_CONNECTION_STRING"], timeout=5) as connection:
        with _native_fetch_counters() as profiling, connection.cursor() as cursor:
            warm(cursor, "fetchone")
            old = cursor.__dict__
            conflict = object()
            library = ctypes.CDLL(str(Path(ddbc_bindings.module.__file__).resolve()))
            pointer = ctypes.c_void_p.in_dll(library, "SQLFetch_ptr")
            original = pointer.value
            fetch_type = ctypes.CFUNCTYPE(ctypes.c_short, ctypes.c_void_p)
            fetch = fetch_type(original)
            calls, nested, errors = [], [], []

            @fetch_type
            def intercepted(handle):
                try:
                    calls.append(handle)
                    pointer.value = original
                    if mutation == "dictionary":
                        cursor.__dict__ = old.copy()
                    elif mutation == "delete":
                        del cursor._native_fetch_state
                    else:
                        cursor._native_fetch_state = conflict
                    nested.append(cursor.fetchone()[0])
                    return fetch(handle)
                except BaseException as error:
                    errors.append(type(error).__name__)
                    return -1

            pointer.value = ctypes.cast(intercepted, ctypes.c_void_p).value
            try:
                with _fetchone_window(profiling) as observation:
                    row = cursor.fetchone()
            finally:
                pointer.value = original
            assert not errors, errors
            assert len(calls) == 1
            assert nested == [3]
            assert row[0] == 4
            assert cursor.rowcount == 4
            assert cursor.rownumber == 3
            python = observation["python"]
            assert python.get(type(runtime[2]).__call__.__code__, 0) == 0
            assert python.get(runtime[3][0].__code__, 0) == 1
            assert python.get(Cursor._complete_fetchone.__code__, 0) == 1
            assert python.get(Cursor._wrap_fetchone.__code__, 0) == 1
            if observation["native"] is not None:
                for name, expected in (
                    ("NativeCursor::prepare", 0),
                    ("NativeCursor::fetch", 1),
                    ("NativeCursor::fallback", 1),
                    ("NativeCursor::completion_fallback", 1),
                    ("FetchSingleRow", 2),
                ):
                    assert observation["native"].get("ddbc::" + name, 0) == expected
            if mutation == "dictionary":
                assert old["rowcount"] == 2
            elif mutation == "conflict":
                assert cursor._native_fetch_state is conflict
            assert cursor.fetchone()[0] == 5


@pytest.mark.skipif(sys.platform == "win32", reason="Requires exported SQLFetch pointer")
def test_redesign_interleaved_calls_can_exit_out_of_order(conn_str):
    _redesign_runtime()
    code = "import runpy, sys; " "runpy.run_path(sys.argv[1])['_redesign_interleave_calls']()"
    result = subprocess.run(
        [sys.executable, "-c", code, str(Path(__file__).resolve())],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, (result.returncode, result.stdout, result.stderr)


def _redesign_interleave_calls():
    import ctypes
    import os
    import threading

    runtime = _redesign_runtime()
    with (
        mssql_python.connect(os.environ["DB_CONNECTION_STRING"], timeout=5) as first,
        mssql_python.connect(os.environ["DB_CONNECTION_STRING"], timeout=5) as second,
        first.cursor() as left,
        second.cursor() as right,
    ):
        warm(left, "fetchone")
        warm(right, "fetchone")
        library = ctypes.CDLL(str(Path(ddbc_bindings.module.__file__).resolve()))
        pointer = ctypes.c_void_p.in_dll(library, "SQLFetch_ptr")
        original = pointer.value
        fetch_type = ctypes.CFUNCTYPE(ctypes.c_short, ctypes.c_void_p)
        fetch = fetch_type(original)
        entered = {name: threading.Event() for name in ("left", "right")}
        release = {name: threading.Event() for name in entered}
        calls, results, errors = {}, {}, []

        @fetch_type
        def intercepted(handle):
            try:
                name = threading.current_thread().name
                calls[name] = calls.get(name, 0) + 1
                if calls[name] == 1:
                    entered[name].set()
                    assert release[name].wait(15), name
                    if name == "right":
                        with _fetchone_window(None) as nested:
                            assert right.fetchone()[0] == 3
                        assert nested["python"].get(runtime[3][0].__code__, 0) == 1
                        assert nested["python"].get(type(runtime[2]).__call__.__code__, 0) == 0
                return fetch(handle)
            except BaseException as error:
                errors.append(type(error).__name__)
                return -1

        def run(name, cursor):
            try:
                results[name] = cursor.fetchone()[0]
            except BaseException as error:
                errors.append(type(error).__name__)

        a = threading.Thread(target=run, args=("left", left), name="left")
        b = threading.Thread(target=run, args=("right", right), name="right")
        pointer.value = ctypes.cast(intercepted, ctypes.c_void_p).value
        try:
            a.start()
            assert entered["left"].wait(15)
            b.start()
            assert entered["right"].wait(15)
            release["left"].set()
            a.join(15)
            assert not a.is_alive()
            release["right"].set()
            b.join(15)
            assert not b.is_alive()
        finally:
            release["left"].set()
            release["right"].set()
            if a.ident is not None:
                a.join(15)
            if b.ident is not None:
                b.join(15)
            pointer.value = original
        assert not errors, errors
        assert results == {"left": 3, "right": 4}
        assert calls == {"left": 1, "right": 2}
        assert left.fetchone()[0] == 4
        assert right.fetchone()[0] == 5


@pytest.mark.parametrize("phase", ("method", "policy", "activation", "rollback"))
def test_redesign_installation_failure_restores_originals(phase):
    _redesign_runtime()
    code = r"""
import importlib.abc
import importlib.machinery
import sys

phase = sys.argv[1]
saved = {"restores": [], "terminal": []}

class Loader:
    def __init__(self, original):
        self.original = original

    def create_module(self, spec):
        return self.original.create_module(spec)

    def exec_module(self, module):
        self.original.exec_module(module)
        bindings = module.ddbc_bindings
        begin = bindings._begin_native_fetch_install
        activate = bindings._activate_native_fetch_install
        abort = bindings._abort_native_fetch_install
        disable = bindings._disable_native_fetch_install
        policy = module._Policy
        saved.update(module=module, bindings=bindings, begin=begin, activate=activate,
                     policy=policy, abort=abort, disable=disable)

        def capture(cursor_type, functions):
            namespace = functions[0].__globals__
            saved.update(cursor_type=cursor_type, functions=functions, namespace=namespace,
                         row_type=namespace["Row"])
            return begin(cursor_type, functions)

        def assign(target, name, value):
            if phase == "method" and name == "fetchmany" and value is bindings._native_fetch_methods[1]:
                raise RuntimeError("installation sentinel")
            names = ("fetchone", "fetchmany", "fetchval", "__next__")
            if value is saved["functions"][names.index(name)]:
                saved["restores"].append(name)
                if phase == "rollback" and name == "fetchone":
                    raise RuntimeError("secondary restoration sentinel")
            setattr(target, name, value)

        def terminal(name, function):
            def invoke():
                saved["terminal"].append(name)
                return function()
            return invoke

        def fail_policy(cursor_type, row_type, namespace, functions):
            temporary = cursor_type.__new__(cursor_type)
            temporary.closed = True
            try:
                temporary.fetchone()
            except namespace["ProgrammingError"] as error:
                assert "cursor is closed" in str(error)
            else:
                raise AssertionError("Unpublished entry did not retain ordinary errors")
            assert "_native_fetch_state" not in temporary.__dict__
            policy(cursor_type, row_type, namespace, functions)
            raise MemoryError("installation sentinel")

        def reject_runtime(runtime):
            return activate(runtime[:6])

        bindings._begin_native_fetch_install = capture
        bindings._abort_native_fetch_install = terminal("abort", abort)
        bindings._disable_native_fetch_install = terminal("disable", disable)
        module.setattr = assign
        if phase in ("policy", "rollback"):
            module._Policy = fail_policy
        elif phase == "activation":
            bindings._activate_native_fetch_install = reject_runtime

class Finder(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if fullname != "mssql_python._native_fetch":
            return None
        spec = importlib.machinery.PathFinder.find_spec(fullname, path)
        assert spec is not None
        spec.loader = Loader(spec.loader)
        return spec

finder = Finder()
sys.meta_path.insert(0, finder)
expected = {"method": RuntimeError, "policy": MemoryError,
            "activation": TypeError, "rollback": RuntimeError}[phase]
try:
    try:
        import mssql_python
    except expected as error:
        if phase == "rollback":
            assert type(error) is saved["module"]._InstallRollbackError
            assert type(error.installation_error) is MemoryError
            assert str(error.installation_error) == "installation sentinel"
            assert error.__cause__ is error.installation_error
            assert len(error.cleanup_errors) == 1
            name, secondary = error.cleanup_errors[0]
            assert name == "fetchone" and type(secondary) is RuntimeError
            assert str(secondary) == "secondary restoration sentinel"
            assert "installation sentinel" in str(error)
            assert "secondary restoration sentinel" in str(error)
            del secondary
    else:
        raise AssertionError("Installation failure was swallowed")
finally:
    sys.meta_path.remove(finder)

module = saved["module"]
bindings = saved["bindings"]
bindings._begin_native_fetch_install = saved["begin"]
bindings._activate_native_fetch_install = saved["activate"]
bindings._abort_native_fetch_install = saved["abort"]
bindings._disable_native_fetch_install = saved["disable"]
module._Policy = saved["policy"]
del module.setattr
cursor_type = saved["cursor_type"]
names = ("fetchone", "fetchmany", "fetchval", "__next__")
if phase == "rollback":
    assert saved["restores"] == list(names)
    assert saved["terminal"] == ["disable"]
    assert vars(cursor_type)["fetchone"] is bindings._native_fetch_methods[0]
    assert all(vars(cursor_type)[name] is function
               for name, function in zip(names[1:], saved["functions"][1:]))
    profiling = getattr(bindings, "profiling", None)
    if profiling is not None:
        profiling.reset()
        profiling.enable()
    temporary = object.__new__(cursor_type)
    temporary.closed = True
    for entry in bindings._native_fetch_methods:
        try:
            entry.__get__(temporary, cursor_type)()
        except saved["namespace"]["ProgrammingError"] as error:
            assert "cursor is closed" in str(error)
        else:
            raise AssertionError("Disabled descriptor lost ordinary fallback")
        assert "_native_fetch_state" not in temporary.__dict__
    if profiling is not None:
        profiling.disable()
        stats = profiling.get_stats()
        for name in ("NativeCursor::prepare", "NativeCursor::fetch", "NativeCursor::fallback",
                     "FetchOne_wrap", "FetchSingleRow"):
            assert stats.get("ddbc::" + name, {}).get("calls", 0) == 0
    try:
        module.install(cursor_type, saved["row_type"], saved["namespace"])
    except RuntimeError as error:
        assert "disabled after incomplete rollback" in str(error)
    else:
        raise AssertionError("Disabled context allowed reinstallation")
    assert vars(cursor_type)["fetchone"] is bindings._native_fetch_methods[0]
    raise SystemExit(0)
assert saved["terminal"] == ["abort"]
assert all(vars(cursor_type)[name] is function for name, function in zip(names, saved["functions"]))
runtime = module.install(cursor_type, saved["row_type"], saved["namespace"])
assert len(runtime) == 7 and runtime[6] is bindings._native_fetch_context
assert all(vars(cursor_type)[name] is entry for name, entry in zip(names, bindings._native_fetch_methods))
"""
    result = subprocess.run(
        [sys.executable, "-c", code, phase],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, (result.returncode, result.stdout, result.stderr)
