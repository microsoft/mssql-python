"""Bounded fetchmany storage must survive reuse, transitions, and failed conversion.

Attribute/unbind counters cover retained-plan helpers, not every legacy ODBC call.
Their absence on another route is not evidence of zero driver work.
"""

import os
from pathlib import Path
import subprocess
import sys
import textwrap
from decimal import Decimal

import pytest

import mssql_python as db
from mssql_python import ddbc_bindings as native


@pytest.fixture
def reuse_cursor(conn_str):
    with db.connect(conn_str) as connection:
        with connection.cursor() as cursor:
            yield cursor


def _query(count, width=2):
    columns = ",".join(f"CAST(id AS INT) AS c{i}" for i in range(width))
    return (
        f"WITH n AS (SELECT TOP({count}) ROW_NUMBER() OVER "
        "(ORDER BY a.object_id,b.object_id) AS id "
        f"FROM sys.all_objects a CROSS JOIN sys.all_objects b) SELECT {columns} FROM n ORDER BY id"
    )


def _tuples(rows):
    return [tuple(row) for row in rows]


def _isolated(script, tmp_path):
    environment = dict(os.environ)
    root = str(Path(db.__file__).resolve().parent.parent)
    environment["PYTHONPATH"] = os.pathsep.join([root, environment.get("PYTHONPATH", "")])
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(script)],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("size", [1, 2, 1000])
def test_repeated_size_partial_batch_and_eof(reuse_cursor, size):
    cursor = reuse_cursor
    count = size * 2 + 1
    cursor.execute(_query(count))
    assert _tuples(cursor.fetchmany(size)) == [(i, i) for i in range(1, size + 1)]
    assert cursor.fetchmany(0) == []
    assert cursor.fetchmany(-1) == []
    assert _tuples(cursor.fetchmany(size)) == [(i, i) for i in range(size + 1, size * 2 + 1)]
    assert _tuples(cursor.fetchmany(size)) == [(count, count)]
    assert cursor.fetchmany(size) == []
    assert cursor.fetchmany(size) == []
    assert cursor.fetchone() is None
    assert cursor.fetchall() == []


def test_size_changes_and_arraysize_do_not_skip_rows(reuse_cursor):
    cursor = reuse_cursor
    cursor.execute(_query(2015))
    consumed = 0
    for size in (1, 1, 2, 2, 1000, 1000, 2, 1, 1):
        batch = cursor.fetchmany(size)
        assert _tuples(batch) == [(i, i) for i in range(consumed + 1, consumed + len(batch) + 1)]
        consumed += len(batch)
    cursor.arraysize = 2
    while batch := cursor.fetchmany():
        assert _tuples(batch) == [(i, i) for i in range(consumed + 1, consumed + len(batch) + 1)]
        consumed += len(batch)
    assert consumed == 2015


def test_nulls_and_variable_values_overwrite_old_rows(reuse_cursor):
    cursor = reuse_cursor
    cursor.execute(
        "SELECT id, CASE WHEN id%2=0 THEN CAST(NULL AS INT) ELSE id END AS n,"
        "CASE WHEN id%2=0 THEN CAST(NULL AS NVARCHAR(30)) "
        "ELSE REPLICATE(N'x',id) END AS s,"
        "CASE WHEN id%2=0 THEN CAST(NULL AS VARBINARY(10)) ELSE 0x0102 END AS b,"
        "CASE WHEN id%2=0 THEN CAST(NULL AS DECIMAL(10,2)) "
        "ELSE CAST(id AS DECIMAL(10,2)) END AS d "
        "FROM (VALUES(1),(2),(3),(4),(5)) source(id) ORDER BY id"
    )
    rows = []
    while batch := cursor.fetchmany(2):
        rows.extend(batch)
    assert _tuples(rows) == [
        (i, None, None, None, None) if i % 2 == 0 else (i, i, "x" * i, b"\x01\x02", Decimal(i))
        for i in range(1, 6)
    ]


@pytest.mark.parametrize("mode", ["one", "scroll", "direct", "all", "arrow", "arrow_schema"])
def test_many_transitions_preserve_cursor_position(reuse_cursor, mode):
    cursor = reuse_cursor
    if mode.startswith("arrow"):
        pytest.importorskip("pyarrow")
    cursor.execute(_query(9))
    assert _tuples(cursor.fetchmany(2)) == [(1, 1), (2, 2)]
    assert _tuples(cursor.fetchmany(2)) == [(3, 3), (4, 4)]
    if mode == "one":
        assert tuple(cursor.fetchone()) == (5, 5)
    elif mode == "scroll":
        cursor.scroll(1)
    elif mode == "direct":
        assert native.DDBCSQLFetch(cursor.hstmt) in (0, 1)
        row = []
        assert native.DDBCSQLGetData(
            cursor.hstmt, 2, row, "utf-16le", "utf-16le", db.SQL_WCHAR
        ) in (0, 1)
        assert row == [5, 5]
    elif mode == "all":
        assert _tuples(cursor.fetchall()) == [(i, i) for i in range(5, 10)]
        return
    elif mode == "arrow":
        assert cursor.arrow_batch(1).to_pydict() == {"c0": [5], "c1": [5]}
    else:
        assert cursor.arrow_batch(0).num_rows == 0
        assert _tuples(cursor.fetchmany(1)) == [(5, 5)]
    assert _tuples(cursor.fetchmany(2)) == [(6, 6), (7, 7)]
    assert _tuples(cursor.fetchall()) == [(8, 8), (9, 9)]


@pytest.mark.parametrize("distance", [1, 3])
def test_scroll_cleanup_exception_preserves_public_position(reuse_cursor, monkeypatch, distance):
    """Mock the bridge exception, not an actual ODBC cleanup failure."""
    cursor = reuse_cursor
    cursor.execute(_query(8))
    assert _tuples(cursor.fetchmany(2)) == [(1, 1), (2, 2)]
    position = (cursor._rownumber, cursor._next_row_index, cursor.rowcount)
    statement = cursor.hstmt
    failure = RuntimeError("SQLSTATE:HY010:Detaching retained fetch buffers before scroll: unbind")
    calls = []

    def fail_cleanup(*arguments):
        calls.append(arguments)
        raise failure

    with monkeypatch.context() as patch:
        patch.setattr(native, "DDBCSQLFetchScroll", fail_cleanup)
        with pytest.raises(IndexError, match="SQLSTATE:HY010:") as raised:
            cursor.scroll(distance)
        assert raised.value.__cause__ is failure

    assert len(calls) == 1
    assert cursor.hstmt is statement
    assert (cursor._rownumber, cursor._next_row_index, cursor.rowcount) == position
    assert _tuples(cursor.fetchmany(1)) == [(3, 3)]


def test_nextset_same_width_changes_type_and_size(reuse_cursor):
    cursor = reuse_cursor
    cursor.execute(
        "SELECT id, CAST('a' AS VARCHAR(1)) AS text_value "
        "FROM (VALUES(1),(2),(3)) source(id) ORDER BY id;"
        "SELECT CAST(id+0.25 AS DECIMAL(9,2)) AS amount,"
        "CAST(REPLICATE(N'z',30) AS NVARCHAR(40)) AS long_value "
        "FROM (VALUES(4),(5),(6)) source(id) ORDER BY id"
    )
    assert _tuples(cursor.fetchmany(1)) == [(1, "a")]
    assert _tuples(cursor.fetchmany(1)) == [(2, "a")]
    assert cursor.nextset()
    assert _tuples(cursor.fetchmany(1)) == [(Decimal("4.25"), "z" * 30)]
    assert _tuples(cursor.fetchmany(1)) == [(Decimal("5.25"), "z" * 30)]
    assert _tuples(cursor.fetchmany(1)) == [(Decimal("6.25"), "z" * 30)]
    assert cursor.fetchmany(1) == []


@pytest.mark.parametrize("reset_cursor", [True, False])
def test_prepared_handle_reexecution_replaces_plan(reuse_cursor, reset_cursor):
    cursor = reuse_cursor
    query = "SELECT CAST(? AS INT)+id AS n FROM (VALUES(1),(2),(3)) source(id) ORDER BY id"
    cursor.execute(query, 10)
    statement = cursor.hstmt
    assert _tuples(cursor.fetchmany(1)) == [(11,)]
    assert _tuples(cursor.fetchmany(1)) == [(12,)]
    cursor.execute(query, 20, reset_cursor=reset_cursor)
    assert cursor.hstmt is statement
    assert _tuples(cursor.fetchmany(1)) == [(21,)]
    assert _tuples(cursor.fetchmany(1)) == [(22,)]
    assert _tuples(cursor.fetchall()) == [(23,)]


def test_converter_failure_preserves_fallback_and_position(reuse_cursor):
    cursor = reuse_cursor
    cursor.execute(_query(4, 1))
    assert _tuples(cursor.fetchmany(1)) == [(1,)]
    calls = []

    def fail(value):
        calls.append(value)
        raise LookupError(f"converter rejected {value}")

    cursor.connection.add_output_converter(db.SQL_INTEGER, fail)
    assert _tuples(cursor.fetchmany(1)) == [(2,)]
    assert calls == [2]
    cursor.connection.remove_output_converter(db.SQL_INTEGER)
    assert _tuples(cursor.fetchmany(1)) == [(3,)]
    assert _tuples(cursor.fetchmany(1)) == [(4,)]


def test_native_conversion_failure_recovers_without_transition(tmp_path):
    """A cached constructor raises inside native conversion on only the middle row."""
    _isolated(
        """
        import datetime
        import os

        original_date = datetime.date
        calls = []

        class ConstructorFailure(Exception):
            pass

        failure = ConstructorFailure("middle-row date conversion")

        class ValueCheckedDate(original_date):
            def __new__(cls, year, month, day):
                calls.append(year)
                if year == 2002:
                    raise failure
                return original_date.__new__(cls, year, month, day)

        datetime.date = ValueCheckedDate
        import mssql_python as db

        with db.connect(os.environ["DB_CONNECTION_STRING"]) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT CAST(value AS DATE) FROM "
                    "(VALUES(1,'2001-01-01'),(2,'2002-01-01'),(3,'2003-01-01')) "
                    "source(id,value) ORDER BY id"
                )
                statement = cursor.hstmt
                calls.clear()
                assert cursor.fetchmany(1)[0][0].year == 2001
                try:
                    cursor.fetchmany(1)
                except ConstructorFailure as error:
                    assert error is failure
                else:
                    raise AssertionError("Native constructor failure was swallowed")
                assert cursor.hstmt is statement
                assert cursor.fetchmany(1)[0][0].year == 2003
                assert calls == [2001, 2002, 2003]
                assert cursor.fetchmany(1) == []
        """,
        tmp_path,
    )


def test_decoding_change_on_live_bound_cursor(reuse_cursor):
    cursor = reuse_cursor
    cursor.execute(
        "SELECT CAST('value' AS VARCHAR(20)) AS s "
        "FROM (VALUES(1),(2),(3),(4)) source(id) ORDER BY id"
    )
    assert _tuples(cursor.fetchmany(1)) == [("value",)]
    assert _tuples(cursor.fetchmany(1)) == [("value",)]
    cursor.connection.setdecoding(db.SQL_CHAR, encoding="utf-8", ctype=db.SQL_CHAR)
    assert _tuples(cursor.fetchmany(1)) == [("value",)]
    assert _tuples(cursor.fetchmany(1)) == [("value",)]


def test_external_statement_attribute_detaches_before_update(reuse_cursor):
    cursor = reuse_cursor
    cursor.execute(_query(7))
    assert _tuples(cursor.fetchmany(2)) == [(1, 1), (2, 2)]
    # SQL_ATTR_QUERY_TIMEOUT does not change layout, but is an external mutation.
    assert native.DDBCSQLSetStmtAttr(cursor.hstmt, 0, 0) in (0, 1)
    assert _tuples(cursor.fetchmany(2)) == [(3, 3), (4, 4)]
    assert _tuples(cursor.fetchmany(2)) == [(5, 5), (6, 6)]
    assert _tuples(cursor.fetchmany(2)) == [(7, 7)]


@pytest.mark.parametrize("operation", ["commit", "rollback", "autocommit"])
def test_transaction_transition_with_live_bindings(reuse_cursor, operation):
    cursor = reuse_cursor
    connection = cursor.connection
    info = (
        db.SQL_CURSOR_ROLLBACK_BEHAVIOR
        if operation == "rollback"
        else db.SQL_CURSOR_COMMIT_BEHAVIOR
    )
    if connection.getinfo(info) != 2:
        pytest.skip("SQL_CB_PRESERVE is required for the live-cursor transition")
    cursor.execute(_query(5))
    assert _tuples(cursor.fetchmany(2)) == [(1, 1), (2, 2)]
    if operation == "autocommit":
        connection.autocommit = True
    else:
        getattr(connection, operation)()
    assert _tuples(cursor.fetchmany(2)) == [(3, 3), (4, 4)]
    assert _tuples(cursor.fetchmany(2)) == [(5, 5)]


@pytest.mark.parametrize("close_mode", ["statement", "connection", "gc"])
def test_bound_statement_lifetime_isolated(tmp_path, close_mode):
    _isolated(
        f"""
        import gc
        import os
        import mssql_python as db
        from mssql_python import ddbc_bindings as native
        db.pooling(enabled=False)
        for _ in range(5):
            connection = db.connect(os.environ["DB_CONNECTION_STRING"], autocommit=True)
            statement = connection._conn.alloc_statement_handle()
            assert native.DDBCSQLExecDirect(
                statement, "SELECT id FROM (VALUES(1),(2),(3),(4)) s(id) ORDER BY id"
            ) in (0, 1)
            for expected in (1, 2):
                rows = []
                assert native.DDBCSQLFetchMany(statement, rows, 1) in (0, 1)
                assert rows == [[expected]]
            if {close_mode!r} == "statement":
                statement.free()
                connection.close()
            elif {close_mode!r} == "connection":
                connection._conn.close()
                statement.free()
                connection._conn = None
                connection.close()
            else:
                del statement
                gc.collect()
                connection.close()
        """,
        tmp_path,
    )


@pytest.mark.skipif(not hasattr(native, "profiling"), reason="requires native operation counters")
@pytest.mark.parametrize("size,width", [(1, 24), (2, 3), (1000, 24)])
def test_mechanism_actual_binding_and_allocation_counts(tmp_path, size, width):
    count = 10000 if size == 1 else size * 2 + 1
    query = _query(count, width)
    _isolated(
        f"""
        import os
        import mssql_python as db
        from mssql_python import ddbc_bindings as native
        with db.connect(os.environ["DB_CONNECTION_STRING"]) as connection:
            with connection.cursor() as cursor:
                cursor.execute({query!r})
                native.profiling.reset()
                native.profiling.enable()
                try:
                    consumed = 0
                    while batch := cursor.fetchmany({size}):
                        assert [tuple(row) for row in batch] == [
                            (i,) * {width} for i in range(consumed+1, consumed+len(batch)+1)
                        ]
                        consumed += len(batch)
                    assert consumed == {count}
                finally:
                    native.profiling.disable()
                stats = native.profiling.get_stats()
                def calls(name):
                    return stats.get("ddbc::" + name, {{}}).get("calls", 0)
                assert calls("fetch_bindings::plan_allocation") == 1, stats
                assert calls("fetch_bindings::column_buffer_allocation") == {width}, stats
                assert calls("fetch_bindings::SQLBindCol") == {width}, stats
                assert calls("fetch_bindings::SQL_UNBIND") == 1, stats
                assert calls("fetch_bindings::SQLSetStmtAttr::ROW_ARRAY_SIZE") == 2, stats
                assert calls("fetch_bindings::SQLSetStmtAttr::ROWS_FETCHED_PTR") == 2, stats
                assert calls("fetch_bindings::SQLGetStmtAttr") == 1, stats
                expected_fetches = {((count + size - 1) // size) + 1}
                assert calls("FetchBatchData::SQLFetchScroll_call") == expected_fetches, stats
                assert calls("SQLDescribeCol::driver_call") == {width}, stats
        """,
        tmp_path,
    )


@pytest.mark.skipif(not hasattr(native, "profiling"), reason="requires native operation counters")
def test_mechanism_size_and_mode_changes_rebind_only_when_needed(tmp_path):
    _isolated(
        f"""
        import os
        import mssql_python as db
        from mssql_python import ddbc_bindings as native
        with db.connect(os.environ["DB_CONNECTION_STRING"]) as connection:
            with connection.cursor() as cursor:
                cursor.execute({_query(12)!r})
                native.profiling.reset()
                native.profiling.enable()
                try:
                    for size, first in ((1,1),(1,2),(2,3),(2,5)):
                        assert [tuple(r) for r in cursor.fetchmany(size)] == [
                            (i,i) for i in range(first,first+size)
                        ]
                    assert tuple(cursor.fetchone()) == (7,7)
                    assert [tuple(r) for r in cursor.fetchmany(2)] == [(8,8),(9,9)]
                    assert [tuple(r) for r in cursor.fetchmany(2)] == [(10,10),(11,11)]
                finally:
                    native.profiling.disable()
                stats = native.profiling.get_stats()
                assert stats["ddbc::fetch_bindings::plan_allocation"]["calls"] == 3, stats
                assert stats["ddbc::fetch_bindings::SQLBindCol"]["calls"] == 6, stats
                assert stats["ddbc::fetch_bindings::SQL_UNBIND"]["calls"] == 2, stats
                assert stats["ddbc::fetch_bindings::SQLSetStmtAttr::ROW_ARRAY_SIZE"]["calls"] == 5
                assert stats["ddbc::fetch_bindings::SQLSetStmtAttr::ROWS_FETCHED_PTR"]["calls"] == 5
        """,
        tmp_path,
    )


@pytest.mark.skipif(not hasattr(native, "profiling"), reason="requires native operation counters")
@pytest.mark.parametrize("kind", ["lob", "variant"])
def test_mechanism_fallback_does_not_retain_a_bound_plan(tmp_path, kind):
    expression = (
        "CAST(REPLICATE(N'x',30) AS NVARCHAR(MAX))"
        if kind == "lob"
        else "CAST(CASE WHEN id=2 THEN NULL ELSE id END AS SQL_VARIANT)"
    )
    query = f"SELECT {expression} FROM (VALUES(1),(2),(3)) source(id) ORDER BY id"
    expected = [("x" * 30,)] * 3 if kind == "lob" else [(1,), (None,), (3,)]
    _isolated(
        f"""
        import os
        import mssql_python as db
        from mssql_python import ddbc_bindings as native
        with db.connect(os.environ["DB_CONNECTION_STRING"]) as connection:
            with connection.cursor() as cursor:
                cursor.execute({query!r})
                native.profiling.reset()
                native.profiling.enable()
                try:
                    rows = []
                    while batch := cursor.fetchmany(2):
                        rows.extend(tuple(row) for row in batch)
                    assert rows == {expected!r}
                finally:
                    native.profiling.disable()
                stats = native.profiling.get_stats()
                assert stats.get("ddbc::fetch_bindings::plan_allocation", {{}}).get("calls", 0) == 0
                assert stats.get("ddbc::fetch_bindings::SQLBindCol", {{}}).get("calls", 0) == 0
        """,
        tmp_path,
    )
