"""Repeated execute reuses native bindings, not Python values or raw handle keys.

The existing native logger observes the actual allocation/bind call sites. These
tests therefore check the optimization's contract as well as returned values,
without a test-only API or a special build.
"""

import datetime
import decimal
import gc
import logging
import os
import subprocess
import sys
import textwrap
import uuid
import weakref
from concurrent.futures import ThreadPoolExecutor

import pytest

import mssql_python
from mssql_python import ddbc_bindings
from mssql_python.constants import ConstantsDDBC as SQL
from mssql_python.logging import logger


@pytest.fixture
def cursor(db_connection):
    current = db_connection.cursor()
    try:
        yield current
    finally:
        current.close()


@pytest.fixture
def binding_events(caplog):
    old_level = logger.level
    caplog.set_level(logging.DEBUG, logger="mssql_python")
    native_logger = logging.getLogger("mssql_python")
    native_logger.addHandler(caplog.handler)
    ddbc_bindings.update_log_level(logging.DEBUG)
    try:
        yield caplog
    finally:
        ddbc_bindings.update_log_level(old_level)
        native_logger.removeHandler(caplog.handler)


def counts(events):
    messages = [record.getMessage() for record in events.records]
    return (
        sum("BindParameters: SQLBindParameter param[" in message for message in messages),
        sum("BindParameters: Reusing " in message for message in messages),
        sum("AllocateParamBuffer: New owned buffer" in message for message in messages),
    )


@pytest.mark.parametrize("reset_cursor", [True, False])
@pytest.mark.parametrize(
    "first,second",
    [
        ([12], [34]),
        ([32768], [32769]),
        ([2**40], [2**40 + 1]),
        ([True], [False]),
        ([1.25], [-2.5]),
        (["abc"], ["def"]),
        (["a" * 1000], ["b" * 1000]),
        (["你好"], ["世界"]),
        (["你" * 1000], ["界" * 1000]),
        (["😀x"], ["🚀y"]),
        (["a\0b"], ["c\0d"]),
        ([""], [""]),
        ([b""], [b""]),
        ([b"\x00\x01"], [b"\xff\x00"]),
        ([b"a" * 1000], [b"b" * 1000]),
        ([bytearray(b"ab")], [bytearray(b"cd")]),
        ([12, "abc", 1.25, True], [34, "def", -2.5, False]),
    ],
)
def test_same_shape_reuses_bound_buffers(cursor, binding_events, reset_cursor, first, second):
    query = "SELECT " + ", ".join("?" for _ in first)
    cursor.execute(query, first)
    assert tuple(cursor.fetchone()) == tuple(first)
    handle = cursor.hstmt
    assert counts(binding_events)[0] == len(first)
    for values in (second, first, second):
        binding_events.clear()
        cursor.execute(query, values, reset_cursor=reset_cursor)
        assert tuple(cursor.fetchone()) == tuple(values)
        assert cursor.hstmt is handle
        assert counts(binding_events) == (0, 1, 0)


@pytest.mark.parametrize(
    "first,second",
    [
        (12, 32768),
        (12, True),
        (12, 1.25),
        ("abc", "longer string"),
        ("abc", "x"),
        ("abc", "世界"),
        ("abc", b"abc"),
        ("", "x"),
        (b"", b"x"),
        ("abc", None),
        (None, "abc"),
    ],
)
def test_shape_changes_rebind(cursor, binding_events, first, second):
    cursor.execute("SELECT ?", [first]).fetchone()
    binding_events.clear()
    cursor.execute("SELECT ?", [second])
    assert cursor.fetchone()[0] == second
    assert counts(binding_events)[0:2] == (1, 0)


def test_input_sizes_and_actual_encoded_length(cursor, binding_events):
    for value, reused in [("😀", False), ("🚀", True), ("ab", True), ("中", False), ("文", True)]:
        cursor.setinputsizes([(SQL.SQL_WVARCHAR.value, 100, 0)])
        binding_events.clear()
        cursor.execute("SELECT ?", [value])
        assert cursor.fetchone()[0] == value
        assert counts(binding_events)[0:2] == ((0, 1) if reused else (1, 0))
    cursor.setinputsizes([(SQL.SQL_WVARCHAR.value, 200, 0)])
    binding_events.clear()
    assert cursor.execute("SELECT ?", ["字"]).fetchone()[0] == "字"
    assert counts(binding_events)[0:2] == (1, 0)
    cursor.setinputsizes(None)


def test_encoding_changes_rebind(cursor, db_connection, binding_events):
    try:
        for encoding, value, reuse in [
            ("utf-8", "abc", False),
            ("utf-8", "def", True),
            ("ascii", "ghi", False),
            ("ascii", "jkl", True),
        ]:
            cursor.setinputsizes([(SQL.SQL_VARCHAR.value, 100, 0)])
            db_connection.setencoding(encoding, ctype=mssql_python.SQL_CHAR)
            binding_events.clear()
            assert cursor.execute("SELECT ?", [value]).fetchone()[0] == value
            assert counts(binding_events)[0:2] == ((0, 1) if reuse else (1, 0))
    finally:
        db_connection.setencoding()
        cursor.setinputsizes(None)


@pytest.mark.parametrize(
    "value",
    [
        None,
        decimal.Decimal("123.45"),
        datetime.date(2024, 1, 2),
        datetime.datetime(2024, 1, 2, 3, 4, 5),
        uuid.UUID("12345678-1234-5678-1234-567812345678"),
        "x" * 9000,
        "😀" * 4500,
        b"\0" * 9000,
        bytearray(b"x" * 9000),
    ],
    ids=[
        "none",
        "decimal",
        "date",
        "datetime",
        "uuid",
        "long-ascii",
        "long-emoji",
        "long-null-bytes",
        "long-bytearray",
    ],
)
def test_uncached_shapes_fall_back_and_recover(cursor, binding_events, value):
    cursor.execute("SELECT ?", [12]).fetchone()
    for _ in range(2):
        binding_events.clear()
        cursor.execute("SELECT ?", [value])
        result = cursor.fetchone()[0]
        if isinstance(value, uuid.UUID):
            assert str(result).lower() == str(value)
        else:
            assert result == value
        assert counts(binding_events)[0:2] == (1, 0)
    cursor.execute("SELECT ?", [34]).fetchone()
    binding_events.clear()
    assert cursor.execute("SELECT ?", [56]).fetchone()[0] == 56
    assert counts(binding_events) == (0, 1, 0)


def test_changed_sql_direct_and_parameter_count(cursor, binding_events):
    for sql, values in [
        ("SELECT ?", [12]),
        ("SELECT ? + 1", [12]),
        ("SELECT ?, ?", [12, 13]),
        ("SELECT 12", []),
        ("SELECT ?", [14]),
    ]:
        binding_events.clear()
        cursor.execute(sql, values).fetchone()
        assert counts(binding_events)[0:2] == (len(values), 0)


@pytest.mark.parametrize("fast", [False, True])
def test_executemany_invalidates(cursor, binding_events, fast):
    cursor.execute("DROP TABLE IF EXISTS #cached_bindings")
    cursor.execute("CREATE TABLE #cached_bindings (value int)")
    sql = "INSERT INTO #cached_bindings VALUES (?)"
    cursor.execute(sql, [12])
    cursor.execute(sql, [13])
    cursor.fast_executemany = fast
    cursor.executemany(sql, [[14], [15]])
    binding_events.clear()
    cursor.execute(sql, [16])
    assert counts(binding_events)[0:2] == (1, 0)
    cursor.execute("SELECT value FROM #cached_bindings ORDER BY value")
    assert [row[0] for row in cursor.fetchall()] == [12, 13, 14, 15, 16]
    cursor.execute("DROP TABLE #cached_bindings")


def test_execution_failure_diagnostics_and_recovery(cursor, binding_events):
    sql = "SELECT 10 / ?"
    assert cursor.execute(sql, [2]).fetchone()[0] == 5
    with pytest.raises(mssql_python.DatabaseError, match="(?i)divide by zero"):
        cursor.execute(sql, [0]).fetchone()
    binding_events.clear()
    assert cursor.execute(sql, [5]).fetchone()[0] == 2
    assert counts(binding_events)[0:2] == (1, 0)


def test_validation_failure_invalidates_without_stale_values(cursor, binding_events):
    cursor.execute("SELECT ?, ?", [12, "abc"]).fetchone()
    with pytest.raises((TypeError, RuntimeError, mssql_python.DatabaseError)):
        cursor.execute("SELECT ?, ?", [13, object()])
    binding_events.clear()
    assert tuple(cursor.execute("SELECT ?, ?", [14, "def"]).fetchone()) == (14, "def")
    assert counts(binding_events)[0:2] == (2, 0)


def test_conversion_failure_keeps_bound_storage_alive(cursor, binding_events):
    sizes = [(SQL.SQL_INTEGER.value, 10, 0), (SQL.SQL_SMALLINT.value, 5, 0)]
    cursor.setinputsizes(sizes)
    cursor.execute("SELECT ?, ?", [12, 13]).fetchone()
    cursor.setinputsizes(sizes)
    with pytest.raises((RuntimeError, OverflowError, mssql_python.DatabaseError)):
        cursor.execute("SELECT ?, ?", [14, 2**40])
    binding_events.clear()
    assert tuple(cursor.execute("SELECT ?, ?", [15, 16]).fetchone()) == (15, 16)
    assert counts(binding_events)[0:2] == (2, 0)
    cursor.setinputsizes(None)


def test_explicit_reset_and_close_lifetimes(db_connection, binding_events):
    cursor = db_connection.cursor()
    cursor.execute("SELECT ?", [12]).fetchone()
    handle = cursor.hstmt
    assert ddbc_bindings.DDBCSQLResetStmt(handle) == SQL.SQL_SUCCESS.value
    binding_events.clear()
    assert cursor.execute("SELECT ?", [13]).fetchone()[0] == 13
    assert counts(binding_events)[0:2] == (1, 0)
    cursor.close()
    assert ddbc_bindings.DDBCSQLResetStmt(handle) == SQL.SQL_INVALID_HANDLE.value
    other = db_connection.cursor()
    try:
        binding_events.clear()
        assert other.execute("SELECT ?", [14]).fetchone()[0] == 14
        assert counts(binding_events)[0:2] == (1, 0)
    finally:
        other.close()


def test_multiple_cursors_and_sequential_thread_handoff(db_connection, binding_events):
    first, second = db_connection.cursor(), db_connection.cursor()
    try:
        assert first.execute("SELECT ?", [12]).fetchall()[0][0] == 12
        assert second.execute("SELECT ?", [34]).fetchall()[0][0] == 34
        binding_events.clear()
        # No simultaneous connection/cursor use: this only checks that storage
        # belongs to the handle, rather than a thread-local raw-handle map.
        with ThreadPoolExecutor(max_workers=1) as executor:
            result = executor.submit(lambda: first.execute("SELECT ?", [56]).fetchall()[0][0])
            assert result.result() == 56
        assert second.execute("SELECT ?", [78]).fetchall()[0][0] == 78
        assert counts(binding_events) == (0, 2, 0)
    finally:
        first.close()
        second.close()


def test_connection_close_with_retained_statement(conn_str):
    connection = mssql_python.connect(conn_str)
    cursor = connection.cursor()
    cursor.execute("SELECT ?", ["owned"]).fetchone()
    handle = cursor.hstmt
    connection.close()
    assert ddbc_bindings.DDBCSQLResetStmt(handle) == SQL.SQL_INVALID_HANDLE.value
    handle.free()
    cursor.close()


def test_native_char_encoding_and_codec_failure(cursor, binding_events):
    # SQL_C_CHAR in the Python constants is historically -8. Use the actual
    # native C type (1) through the existing normalized input-size representation.
    sizes = [(SQL.SQL_VARCHAR.value, 1, 100, 0)]
    for value, reused in [("abc", False), ("def", True), ("longer", False), ("short!", True)]:
        cursor._inputsizes = sizes
        binding_events.clear()
        assert cursor.execute("SELECT ?", [value]).fetchone()[0] == value
        assert counts(binding_events)[0:2] == ((0, 1) if reused else (1, 0))

    class FailingString(str):
        def encode(self, *args, **kwargs):
            raise UnicodeError("test codec failure")

    cursor._inputsizes = sizes
    with pytest.raises(RuntimeError, match="test codec failure"):
        cursor.execute("SELECT ?", [FailingString("failed")])
    cursor._inputsizes = sizes
    binding_events.clear()
    assert cursor.execute("SELECT ?", ["latest"]).fetchone()[0] == "latest"
    assert counts(binding_events)[0:2] == (1, 0)


def test_numeric_text_precision_and_scale_changes(cursor, binding_events):
    for precision, scale, reused in [(10, 2, False), (10, 2, True), (12, 3, False), (12, 3, True)]:
        cursor.setinputsizes([(SQL.SQL_DECIMAL.value, precision, scale)])
        binding_events.clear()
        result = cursor.execute("SELECT ?", [decimal.Decimal("12.34")]).fetchone()[0]
        assert result == decimal.Decimal("12.34")
        assert counts(binding_events)[0:2] == ((0, 1) if reused else (1, 0))


@pytest.mark.parametrize("operation", ["catalog", "direct", "attribute"])
def test_native_invalidation_surfaces(cursor, binding_events, operation):
    cursor.execute("SELECT ?", [12]).fetchall()
    handle = cursor.hstmt
    handle._close_cursor()
    if operation == "catalog":
        rc = ddbc_bindings.DDBCSQLGetTypeInfo(handle, SQL.SQL_INTEGER.value)
    elif operation == "direct":
        rc = ddbc_bindings.DDBCSQLExecDirect(handle, "SELECT 99")
    else:
        rc = ddbc_bindings.DDBCSQLSetStmtAttr(handle, SQL.SQL_ATTR_QUERY_TIMEOUT.value, 0)
    assert rc in (SQL.SQL_SUCCESS.value, SQL.SQL_SUCCESS_WITH_INFO.value)
    handle._close_cursor()
    cursor.is_stmt_prepared = [False]
    binding_events.clear()
    assert cursor.execute("SELECT ?", [34]).fetchone()[0] == 34
    assert counts(binding_events)[0:2] == (1, 0)


def test_raw_free_and_shutdown_with_cached_bindings(conn_str):
    code = textwrap.dedent("""
        import os
        import mssql_python
        from mssql_python import ddbc_bindings as native
        from mssql_python.constants import ConstantsDDBC as SQL

        connection = mssql_python.connect(os.environ["DB_CONNECTION_STRING"])
        cursor = connection.cursor()
        cursor.execute("SELECT ?", [12]).fetchall()
        handle = cursor.hstmt
        assert native.DDBCSQLFreeHandle(SQL.SQL_HANDLE_STMT.value, handle) == 0
        cursor.close()
        other = connection.cursor()
        for value in [34, 56, 78]:
            assert other.execute("SELECT ?", [value]).fetchall()[0][0] == value
        # Exercise atexit cleanup while native bindings and Python handles live.
        """)
    result = subprocess.run(
        [sys.executable, "-c", code],
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr


def test_cache_owns_no_python_values(cursor, binding_events):
    class Text(str):
        pass

    for value in ("abc", "def"):
        text = Text(value)
        reference = weakref.ref(text)
        binding_events.clear()
        cursor.execute("SELECT ?", [text])
        del text
        gc.collect()
        assert reference() is None
        assert cursor.fetchone()[0] == value
        if value == "def":
            assert counts(binding_events) == (0, 1, 0)


def test_native_array_execution_invalidates_same_handle(cursor, binding_events):
    cursor.execute("CREATE TABLE #cached_native_array (value int)")
    sql = "INSERT INTO #cached_native_array VALUES (?)"
    cursor.execute(sql, [12])
    handle = cursor.hstmt
    handle._close_cursor()
    info = ddbc_bindings.ParamInfo()
    info.inputOutputType = 1
    info.paramCType = 4  # SQL_C_LONG
    info.paramSQLType = 4  # SQL_INTEGER
    info.columnSize = 10
    info.decimalDigits = 0
    rc = ddbc_bindings.SQLExecuteMany(handle, sql, [[34, 56]], [info], 2, {})
    assert rc in (SQL.SQL_SUCCESS.value, SQL.SQL_SUCCESS_WITH_INFO.value)
    binding_events.clear()
    cursor.execute(sql, [78])
    assert cursor.hstmt is handle
    assert counts(binding_events)[0:2] == (1, 0)
    cursor.execute("SELECT value FROM #cached_native_array ORDER BY value")
    assert [row[0] for row in cursor.fetchall()] == [12, 34, 56, 78]
    cursor.execute("DROP TABLE #cached_native_array")
