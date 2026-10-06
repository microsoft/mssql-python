"""Shared single-row fetch contracts; native lifetime checks run in a child process."""

import os
import subprocess
import sys
import textwrap
from unittest.mock import Mock, patch

import pytest

import mssql_python
from mssql_python.cursor import Cursor

NUMERIC_TYPES = ("INT", "SMALLINT", "BIGINT", "TINYINT", "BIT", "REAL", "FLOAT")


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


@pytest.mark.parametrize("sql_type", NUMERIC_TYPES)
@pytest.mark.parametrize("method", ("fetchone", "iterator", "fetchmany", "fetchval"))
def test_numeric_single_row_values_and_eof(cursor, sql_type, method):
    cursor.execute(
        f"SELECT CAST(CASE WHEN n = 2 THEN NULL ELSE 1 END AS {sql_type}) AS value "
        "FROM (VALUES (1), (2), (3)) AS v(n) ORDER BY n"
    )

    def fetch():
        if method == "fetchmany":
            rows = cursor.fetchmany(1)
            return rows[0][0] if rows else "EOF"
        if method == "fetchval":
            return cursor.fetchval()
        row = next(cursor, None) if method == "iterator" else cursor.fetchone()
        return row[0] if row is not None else "EOF"

    values = [fetch(), fetch(), fetch()]
    expected = True if sql_type == "BIT" else 1.0 if sql_type in ("REAL", "FLOAT") else 1
    assert values == [expected, None, expected]
    assert type(values[0]) is type(expected)
    assert cursor.rowcount == 3
    assert cursor.rownumber == 2
    assert fetch() == (None if method == "fetchval" else "EOF")
    assert cursor.rowcount == 3
    assert not cursor.messages


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
    with patch.object(ddbc_bindings, "construct_rows", wraps=ddbc_bindings.construct_rows) as batch:
        retained = cursor.fetchmany(1)[0]
        assert tuple(retained) == (1, "text")
        batch.assert_not_called()
        assert [row[0] for row in cursor.fetchmany(2)] == [2, 3]
        batch.assert_called_once()
        assert cursor.fetchmany(2)[0][0] == 4
        assert batch.call_count == 2
        assert tuple(retained) == (1, "text")


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


@pytest.mark.parametrize("enabled", (False, True))
@pytest.mark.parametrize("method", ("fetchone", "fetchmany", "fetchval", "iterator"))
def test_single_row_python_phase_counts(cursor, enabled, method):
    from mssql_python import perf_timer

    perf_timer.disable()
    perf_timer.reset()
    if enabled:
        perf_timer.enable()
    try:
        cursor.execute("SELECT 1 AS a")
        perf_timer.reset()
        if method == "fetchmany":
            assert cursor.fetchmany(1)[0][0] == 1
        elif method == "fetchval":
            assert cursor.fetchval() == 1
        elif method == "iterator":
            assert next(cursor)[0] == 1
        else:
            assert cursor.fetchone()[0] == 1
        stats = perf_timer.get_stats()
        prefix = "py::fetchmany" if method == "fetchmany" else "py::fetchone"
        assert {name: item["calls"] for name, item in stats.items()} == (
            {prefix + "::cpp_call": 1, prefix + "::row_wrap": 1} if enabled else {}
        )
    finally:
        perf_timer.disable()
        perf_timer.reset()


def test_single_row_native_transitions_in_subprocess(conn_str):
    """Counters describe actual native work, not mocked Python bridge calls."""
    from mssql_python import ddbc_bindings

    if not hasattr(ddbc_bindings, "profiling"):
        pytest.skip("requires a profiling-enabled native build")
    script = textwrap.dedent("""
        import os
        import mssql_python
        from mssql_python import ddbc_bindings as ddbc

        p = ddbc.profiling
        assert hasattr(p, "enable")
        with mssql_python.connect(os.environ["DB_CONNECTION_STRING"]) as connection:
            with connection.cursor() as cursor:
                query = (
                    "SELECT n AS a, n + 10 AS b FROM "
                    "(VALUES (1),(2),(3),(4),(5),(6),(7),(8)) AS v(n) ORDER BY n"
                )
                cursor.execute(query)
                p.reset()
                p.enable()
                retained = cursor.fetchone()
                assert retained[0] == 1
                assert next(cursor)[0] == 2
                assert cursor.fetchval() == 3
                assert p.get_stats()["ddbc::FetchSingleRow::SQL_UNBIND"]["calls"] == 1
                assert cursor.fetchmany(1)[0][0] == 4
                assert "ddbc::SQLBindColums" not in p.get_stats()
                assert p.get_stats()["ddbc::FetchMany::single_numeric_row"]["calls"] == 1
                # Cleanup deliberately forgets the unbound marker.
                assert cursor.fetchone()[0] == 5
                assert p.get_stats()["ddbc::FetchSingleRow::SQL_UNBIND"]["calls"] == 2
                assert [r[0] for r in cursor.fetchmany(2)] == [6, 7]
                assert p.get_stats()["ddbc::SQLBindColums"]["calls"] == 1
                assert cursor.fetchone()[0] == 8
                assert cursor.fetchone() is None
                assert cursor.fetchone() is None
                assert p.get_stats()["ddbc::FetchSingleRow::SQL_UNBIND"]["calls"] == 3
                assert tuple(retained) == (1, 11)
                assert retained.a == 1
                assert not cursor.messages

                cursor.execute(query)
                assert cursor.fetchone()[0] == 1
                assert p.get_stats()["ddbc::FetchSingleRow::SQL_UNBIND"]["calls"] == 4
                # Public statement-attribute changes must invalidate the marker.
                assert ddbc.DDBCSQLSetStmtAttr(cursor.hstmt, 0, 0) == 0  # SQL_ATTR_QUERY_TIMEOUT
                assert cursor.fetchone()[0] == 2
                assert p.get_stats()["ddbc::FetchSingleRow::SQL_UNBIND"]["calls"] == 5
                cursor.execute("SELECT 21 AS a; SELECT 22 AS a")
                assert cursor.fetchone()[0] == 21
                assert cursor.nextset()
                assert cursor.fetchone()[0] == 22
                assert p.get_stats()["ddbc::FetchSingleRow::SQL_UNBIND"]["calls"] == 7

                cursor.execute("SELECT CAST(N'text' AS NVARCHAR(10)) AS a")
                assert cursor.fetchmany(1)[0][0] == "text"
                assert p.get_stats()["ddbc::SQLBindColums"]["calls"] == 2
                cursor.execute(query)
                assert cursor.fetchone()[0] == 1
                cursor.skip(1)
                before = p.get_stats()["ddbc::FetchSingleRow::SQL_UNBIND"]["calls"]
                assert cursor.fetchone()[0] == 3
                assert p.get_stats()["ddbc::FetchSingleRow::SQL_UNBIND"]["calls"] == before + 1
                batch = cursor.arrow_batch(2)
                assert batch.to_pydict() == {"a": [4, 5], "b": [14, 15]}
                before = p.get_stats()["ddbc::FetchSingleRow::SQL_UNBIND"]["calls"]
                assert cursor.fetchone()[0] == 6
                assert p.get_stats()["ddbc::FetchSingleRow::SQL_UNBIND"]["calls"] == before + 1
                assert batch.to_pydict() == {"a": [4, 5], "b": [14, 15]}
                # Cancel invalidates the result generation; execute recovers the handle.
                cursor.hstmt._cancel()
                cursor.execute(query)
                before = p.get_stats()["ddbc::FetchSingleRow::SQL_UNBIND"]["calls"]
                assert cursor.fetchone()[0] == 1
                assert p.get_stats()["ddbc::FetchSingleRow::SQL_UNBIND"]["calls"] == before + 1
                cursor.close()
                with connection.cursor() as replacement:
                    replacement.execute(query)
                    before = p.get_stats()["ddbc::FetchSingleRow::SQL_UNBIND"]["calls"]
                    assert replacement.fetchone()[0] == 1
                    assert p.get_stats()["ddbc::FetchSingleRow::SQL_UNBIND"]["calls"] == before + 1
                p.disable()
        """)
    environment = os.environ.copy()
    environment["DB_CONNECTION_STRING"] = conn_str
    result = subprocess.run(
        [sys.executable, "-c", script],
        env=environment,
        capture_output=True,
        text=True,
        timeout=45,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


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
    environment = os.environ.copy()
    environment["DB_CONNECTION_STRING"] = conn_str
    result = subprocess.run(
        [sys.executable, "-c", script, first_fetch],
        env=environment,
        capture_output=True,
        text=True,
        timeout=45,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize(
    "failure",
    (
        "unbind",
        "unbind_warning",
        "partial_bind",
        "configure",
        "cleanup",
        "rows_fetched_cleanup",
        "column_count",
        "column_name",
        "fetch",
        "getdata",
    ),
)
def test_single_row_native_failure_recovery_in_subprocess(conn_str, failure):
    """Inject one native failure; restore the entry point before recovery."""
    from mssql_python import ddbc_bindings

    if sys.platform == "win32":
        pytest.skip("native function-pointer globals are not exported on Windows")
    if not hasattr(ddbc_bindings, "profiling"):
        pytest.skip("requires a profiling-enabled native build")
    script = textwrap.dedent("""
        import ctypes as c
        import os
        import sys
        import mssql_python
        from mssql_python import ddbc_bindings as ddbc

        failure = sys.argv[1]
        library = c.CDLL(ddbc.module.__file__)
        short, ushort, pointer, length = c.c_short, c.c_ushort, c.c_void_p, c.c_ssize_t
        signatures = {
            "unbind": ("SQLFreeStmt_ptr", (pointer, ushort)),
            "unbind_warning": ("SQLFreeStmt_ptr", (pointer, ushort)),
            "partial_bind": ("SQLBindCol_ptr", (pointer, ushort, short, pointer, length, pointer)),
            "configure": ("SQLSetStmtAttr_ptr", (pointer, c.c_int, pointer, c.c_int)),
            "cleanup": ("SQLSetStmtAttr_ptr", (pointer, c.c_int, pointer, c.c_int)),
            "rows_fetched_cleanup": ("SQLSetStmtAttr_ptr", (pointer, c.c_int, pointer, c.c_int)),
            "column_count": ("SQLNumResultCols_ptr", (pointer, pointer)),
            "fetch": ("SQLFetchScroll_ptr", (pointer, short, length)),
            "getdata": ("SQLGetData_ptr", (pointer, ushort, short, pointer, length, pointer)),
            "column_name": ("SQLDescribeCol_ptr",
                            (pointer, ushort, pointer, short, pointer, pointer,
                             pointer, pointer, pointer)),
        }
        symbol, arguments = signatures[failure]
        prototype = c.CFUNCTYPE(short, *arguments)
        slot = pointer.in_dll(library, symbol)
        calls = []
        fired = []
        callback_errors = []

        def inject(*args):
            calls.append(args)
            hit = not fired
            if failure == "partial_bind":
                hit = hit and args[1] == 2
            elif failure in ("unbind", "unbind_warning"):
                hit = hit and args[1] == 2  # SQL_UNBIND
            elif failure == "rows_fetched_cleanup":
                matching = [item for item in calls if item[1] == 26]
                hit = hit and args[1] == 26 and len(matching) == 2
            elif failure in ("configure", "cleanup"):
                matching = [item for item in calls if item[1] == 27]
                hit = hit and args[1] == 27 and len(matching) == (
                    1 if failure == "configure" else 2
                )
            if hit:
                fired.append(True)
                if failure == "unbind_warning":
                    result = original(*args)
                    assert result == 0
                    return 1  # SQL_SUCCESS_WITH_INFO after a real successful unbind
                if failure == "column_name":
                    result = original(*args)
                    # Invalid UTF-16 must fail eager name validation before fetch.
                    c.cast(args[2], c.POINTER(ushort))[0] = 0xD800
                    c.cast(args[4], c.POINTER(short))[0] = 1
                    return result
                return -1  # SQL_ERROR
            return original(*args)

        @prototype
        def injected(*args):
            try:
                return inject(*args)
            except BaseException as error:
                callback_errors.append(type(error).__name__)
                return -1

        diag_slot = pointer.in_dll(library, "SQLGetDiagRec_ptr")
        diag_type = c.CFUNCTYPE(
            short, short, pointer, short, pointer, pointer, pointer, short, pointer
        )

        @diag_type
        def warning(handle_type, handle, record, state, native, message, capacity, size):
            try:
                if record > 1:
                    return 100
                text = "unbind warning".encode("utf-16le")
                assert capacity > len(text) // 2
                c.memmove(state, "01000\\0".encode("utf-16le"), 12)
                c.memmove(message, text + b"\\0\\0", len(text) + 2)
                c.cast(native, c.POINTER(c.c_int))[0] = 0
                c.cast(size, c.POINTER(short))[0] = len(text) // 2
                return 0
            except BaseException as error:
                callback_errors.append(type(error).__name__)
                return -1

        with mssql_python.connect(os.environ["DB_CONNECTION_STRING"]) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT n AS a, n + 10 AS b FROM (VALUES (1),(2),(3),(4)) v(n) ORDER BY n"
                )
                # The bundled driver resolves function pointers lazily on connect.
                saved, saved_diag = slot.value, diag_slot.value
                assert saved and saved_diag
                original = prototype(saved)
                ddbc.profiling.reset()
                ddbc.profiling.enable()
                slot.value = c.cast(injected, pointer).value
                if failure == "unbind_warning":
                    diag_slot.value = c.cast(warning, pointer).value
                try:
                    if failure == "unbind_warning":
                        assert cursor.fetchone()[0] == 1
                        assert cursor.messages == [("[01000] (0)", "unbind warning")]
                        assert cursor.fetchone()[0] == 2
                        assert len(calls) == 1
                        assert cursor.messages == [("[01000] (0)", "unbind warning")]
                    else:
                        try:
                            if failure == "unbind":
                                cursor.fetchone()
                            else:
                                cursor.fetchmany(2 if failure == "partial_bind" else 1)
                        except (mssql_python.Error, UnicodeError, RuntimeError) as error:
                            if failure == "partial_bind":
                                assert type(error) is RuntimeError
                                assert str(error) == (
                                    "Failed to bind column - b, Type - 4, column ID - 2"
                                )
                            elif failure == "column_name":
                                assert isinstance(error, UnicodeError)
                            else:
                                assert isinstance(error, mssql_python.Error)
                        else:
                            raise AssertionError("injected failure was not propagated")
                finally:
                    slot.value = saved
                    diag_slot.value = saved_diag
                assert fired == [True], (failure, fired)
                assert not callback_errors, callback_errors
                # GetData and cleanup failures happen after consuming the row.
                expected = (
                    3 if failure == "unbind_warning" else
                    2 if failure in ("cleanup", "rows_fetched_cleanup", "getdata") else 1
                )
                assert cursor.fetchone()[0] == expected, failure
                before = ddbc.profiling.get_stats()["ddbc::FetchSingleRow::SQL_UNBIND"]["calls"]
                assert cursor.fetchone()[0] == expected + 1
                assert ddbc.profiling.get_stats()["ddbc::FetchSingleRow::SQL_UNBIND"]["calls"] == before
                ddbc.profiling.disable()
        """)
    environment = os.environ.copy()
    environment["DB_CONNECTION_STRING"] = conn_str
    result = subprocess.run(
        [sys.executable, "-c", script, failure],
        env=environment,
        capture_output=True,
        text=True,
        timeout=45,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


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

    bridge_name = "DDBCSQLFetchRow"
    bridge = ddbc_bindings.DDBCSQLFetchRow

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
            assert fused.call_count == (1 if method == "fetchmany" else 0)
            assert Row._fast_create is factory
            assert cursor.rowcount == 1 and cursor.rownumber == 0
    finally:
        factory.__code__ = old_code
    assert cursor.fetchone().number == 2


@pytest.mark.parametrize("method", ("fetchone", "fetchmany", "fetchval"))
def test_single_row_fusion_uses_native_constructor(cursor, method):
    from mssql_python import ddbc_bindings, perf_timer
    from mssql_python.row import Row

    cursor.execute("SELECT 42 AS number, CAST(N'text' AS NVARCHAR(10)) AS label")
    factory_code = Row._fast_create.__code__
    calls = []
    completion = cursor._finish_fetchmany if method == "fetchmany" else cursor._finish_fetchone
    native_flags = []

    def finish(ret, data, native=False):
        native_flags.append(native)
        return completion(ret, data, native)

    previous_profile = sys.getprofile()
    phases_enabled = perf_timer.is_enabled()

    def profile(frame, event, arg):
        if event == "call" and frame.f_code is factory_code:
            calls.append(event)

    perf_timer.disable()
    try:
        with (
            patch.object(
                ddbc_bindings, "DDBCSQLFetchRow", wraps=ddbc_bindings.DDBCSQLFetchRow
            ) as fused,
            patch.object(
                cursor,
                "_finish_fetchmany" if method == "fetchmany" else "_finish_fetchone",
                side_effect=finish,
            ),
        ):
            sys.setprofile(profile)
            try:
                result = cursor.fetchmany(1) if method == "fetchmany" else getattr(cursor, method)()
            finally:
                sys.setprofile(previous_profile)
            assert fused.call_count == (1 if method == "fetchmany" else 0)
        if method == "fetchval":
            assert result == 42
        else:
            row = result[0] if method == "fetchmany" else result
            assert type(row) is Row
            assert tuple(row) == (42, "text")
        assert calls == ([] if method == "fetchmany" else ["call"])
        assert native_flags == [method == "fetchmany"]
        assert cursor.rowcount == 1 and cursor.rownumber == 0
    finally:
        if phases_enabled:
            perf_timer.enable()


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
            assert fused.call_count == (1 if method == "fetchmany" else 0)
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


@pytest.mark.parametrize("method", ("fetchone", "fetchmany", "fetchval"))
def test_single_row_fusion_native_counters_in_subprocess(conn_str, method):
    from mssql_python import ddbc_bindings

    if not hasattr(ddbc_bindings, "profiling"):
        pytest.skip("requires a profiling-enabled native build; Python phases remain disabled")
    script = textwrap.dedent("""
        import os
        import sys
        import mssql_python
        from mssql_python import ddbc_bindings as ddbc, perf_timer

        method = sys.argv[1]
        p = ddbc.profiling
        perf_timer.disable()
        perf_timer.reset()
        p.disable()
        query = (
            "SELECT n AS a, n + 10 AS b, CAST(N'text' AS NVARCHAR(10)) AS label "
            "FROM (VALUES (1), (2)) AS v(n) ORDER BY n"
        )
        fetch_timer = "ddbc::FetchMany_wrap" if method == "fetchmany" else "ddbc::FetchOne_wrap"

        def calls(name):
            return p.get_stats().get(name, {}).get("calls", 0)

        with mssql_python.connect(os.environ["DB_CONNECTION_STRING"]) as connection:
            with connection.cursor() as cursor:
                def fetch():
                    if method == "fetchval":
                        return cursor.fetchval()
                    result = cursor.fetchmany(1) if method == "fetchmany" else cursor.fetchone()
                    row = result[0] if result and method == "fetchmany" else result
                    return tuple(row) if row else None

                try:
                    cursor.execute(query)
                    p.reset()
                    p.enable()
                    assert fetch() == (1 if method == "fetchval" else (1, 11, "text"))
                    assert calls("ddbc::FetchRow::construct_row") == (1 if method == "fetchmany" else 0)
                    assert calls(fetch_timer) == 1
                    assert fetch() == (2 if method == "fetchval" else (2, 12, "text"))
                    assert fetch() is None
                    assert fetch() is None
                    assert calls("ddbc::FetchRow::construct_row") == (2 if method == "fetchmany" else 0)
                    assert calls(fetch_timer) == 4
                    assert perf_timer.get_stats() == {}
                    assert cursor.rowcount == 2 and cursor.rownumber == 1

                    p.disable()
                    converted = []
                    def convert(value):
                        converted.append(value)
                        return value + 100
                    connection.add_output_converter(mssql_python.SQL_INTEGER, convert)
                    cursor.execute(query)
                    p.reset()
                    p.enable()
                    assert fetch() == (101 if method == "fetchval" else (101, 111, "text"))
                    assert converted == [1, 11]
                    assert calls("ddbc::FetchRow::construct_row") == 0
                    assert calls(fetch_timer) == 1
                    assert perf_timer.get_stats() == {}
                finally:
                    p.disable()
        """)
    environment = os.environ.copy()
    environment["DB_CONNECTION_STRING"] = conn_str
    result = subprocess.run(
        [sys.executable, "-c", script, method],
        env=environment,
        capture_output=True,
        text=True,
        timeout=45,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
