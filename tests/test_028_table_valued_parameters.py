"""Live SQL Server coverage for explicit TVPs and synchronous ODBC streaming."""

import datetime
import os
import subprocess
import sys
import threading
import uuid
import weakref
from decimal import Decimal, localcontext

import pytest

from mssql_python import (
    DatabaseError,
    NotSupportedError,
    TableValuedParameter,
    connect,
    SQL_INTEGER,
    SQL_WVARCHAR,
    SQL_VARBINARY,
    SQL_DECIMAL,
    SQL_BIT,
    SQL_TINYINT,
    SQL_SMALLINT,
    SQL_BIGINT,
    SQL_DOUBLE,
    SQL_TYPE_DATE,
    SQL_TYPE_TIME,
    SQL_TYPE_TIMESTAMP,
    SQL_GUID,
)
from mssql_python.constants import ConstantsDDBC
from test_023_ssh_tunnel_gil_release import (
    WATCHDOG_SECONDS,
    _parse_server,
    _replace_server,
    _start_forwarder,
)


@pytest.fixture
def tvp_schema(cursor, db_connection):
    suffix = uuid.uuid4().hex
    schema = f"pytest_tvp_{suffix}"
    cursor.execute(f"CREATE SCHEMA [{schema}]")
    cursor.execute(f"CREATE TYPE [{schema}].Items AS TABLE (id int, label nvarchar(max))")
    cursor.execute(f"CREATE TABLE [{schema}].Calls (n int)")
    cursor.execute(f"""
        CREATE PROCEDURE [{schema}].Echo @prefix int, @items [{schema}].Items READONLY,
                                       @suffix nvarchar(max)
        AS BEGIN
            SET NOCOUNT ON;
            INSERT INTO [{schema}].Calls VALUES (1);
            SELECT id + @prefix AS id, label + @suffix AS label FROM @items ORDER BY id;
        END
        """)
    cursor.execute(f"""
        CREATE PROCEDURE [{schema}].Pair @left [{schema}].Items READONLY,
                                       @right [{schema}].Items READONLY
        AS BEGIN
            SET NOCOUNT ON;
            SELECT id, label FROM @left UNION ALL SELECT id, label FROM @right;
        END
        """)
    cursor.execute(f"""
        CREATE PROCEDURE [{schema}].CountRows @items [{schema}].Items READONLY
        AS BEGIN
            SET NOCOUNT ON;
            INSERT INTO [{schema}].Calls VALUES (1);
            SELECT COUNT_BIG(*), SUM(CONVERT(bigint,id)) FROM @items;
        END
        """)
    db_connection.commit()
    try:
        yield schema
    finally:
        cursor.execute(f"DROP PROCEDURE [{schema}].Echo")
        cursor.execute(f"DROP PROCEDURE [{schema}].Pair")
        cursor.execute(f"DROP PROCEDURE [{schema}].CountRows")
        cursor.execute(f"DROP TYPE [{schema}].Items")
        cursor.execute(f"DROP TABLE [{schema}].Calls")
        cursor.execute(f"DROP SCHEMA [{schema}]")
        db_connection.commit()


def items(schema, rows):
    return TableValuedParameter(
        "Items", [("id", SQL_INTEGER), ("label", SQL_WVARCHAR, 0)], rows, schema=schema
    )


@pytest.mark.parametrize("rows", [[], [(1, "Keyboard"), (2, None)], ((3, ""),)])
@pytest.mark.parametrize("named", [False, True])
def test_rows_named_reuse_and_one_invocation(cursor, tvp_schema, rows, named):
    value = items(tvp_schema, rows)
    sql = f"EXEC [{tvp_schema}].Echo ?, ?, ?"
    params = (10, value, "✓")
    if named:
        sql = f"EXEC [{tvp_schema}].Echo %(prefix)s, %(items)s, %(suffix)s"
        params = {"prefix": 10, "items": value, "suffix": "✓"}
    for _ in range(2):
        cursor.execute(sql, params)
        result = cursor.fetchall()
        assert [tuple(row) for row in result] == [
            (row[0] + 10, None if row[1] is None else row[1] + "✓") for row in rows
        ]
    cursor.execute(f"SELECT COUNT(*) FROM [{tvp_schema}].Calls")
    assert cursor.fetchone()[0] == 2


@pytest.mark.parametrize("left,right", [([], []), ([], [(2, "right")]), ([(1, None)], [(2, "")])])
def test_multiple_tvps(cursor, tvp_schema, left, right):
    cursor.execute(
        f"EXEC [{tvp_schema}].Pair ?, ?",
        items(tvp_schema, left),
        items(tvp_schema, right),
    )
    result = cursor.fetchall()
    assert sorted(tuple(row) for row in result) == sorted(left + right)


def test_null_scalars_with_tvp(cursor, tvp_schema):
    cursor.execute(
        f"EXEC [{tvp_schema}].Echo ?, ?, ?",
        None,
        items(tvp_schema, [(1, "one")]),
        None,
    )
    assert tuple(cursor.fetchone()) == (None, None)


def test_explicit_type_in_parameterized_batch(cursor, tvp_schema):
    cursor.execute("SELECT id, label FROM ?", items(tvp_schema, iter([(1, "one")])))
    assert tuple(cursor.fetchone()) == (1, "one")


def test_reuse_with_empty_and_changing_sources(cursor, tvp_schema):
    sql = f"EXEC [{tvp_schema}].Echo ?, ?, ?"
    for rows in ([], [(1, None)], [(2, "two"), (3, "")], []):
        value = TableValuedParameter(
            "Items",
            [("diagnostic_id", SQL_INTEGER), ("diagnostic_label", SQL_WVARCHAR, 100)],
            iter(rows),
            schema=tvp_schema,
        )
        cursor.execute(sql, 0, value, "")
        assert [tuple(row) for row in cursor.fetchall()] == rows


def test_declared_text_size_failure_recovers(cursor, tvp_schema):
    value = TableValuedParameter(
        "Items",
        [("id", SQL_INTEGER), ("label", SQL_WVARCHAR, 3)],
        iter([(1, "too long")]),
        schema=tvp_schema,
    )
    with pytest.raises((DatabaseError, ValueError)):
        cursor.execute(f"EXEC [{tvp_schema}].CountRows ?", value)
    assert cursor.execute("SELECT 42").fetchone()[0] == 42


def test_typed_cells(cursor, db_connection):
    suffix = uuid.uuid4().hex
    name, proc = f"Typed_{suffix}", f"TypedProc_{suffix}"
    declarations = [
        ("bit_value", SQL_BIT, "bit"),
        ("tiny_value", SQL_TINYINT, "tinyint"),
        ("small_value", SQL_SMALLINT, "smallint"),
        ("int_value", SQL_INTEGER, "int"),
        ("big_value", SQL_BIGINT, "bigint"),
        ("float_value", SQL_DOUBLE, "float"),
        ("decimal_value", SQL_DECIMAL, "decimal(18,10)", 18, 10),
        ("text_value", SQL_WVARCHAR, "nvarchar(max)", 0),
        ("binary_value", SQL_VARBINARY, "varbinary(max)", 0),
        ("date_value", SQL_TYPE_DATE, "date"),
        ("time_value", SQL_TYPE_TIME, "time(7)"),
        ("datetime_value", SQL_TYPE_TIMESTAMP, "datetime2(7)"),
        ("offset_value", ConstantsDDBC.SQL_DATETIMEOFFSET.value, "datetimeoffset(7)"),
        ("guid_value", SQL_GUID, "uniqueidentifier"),
        ("all_null_binary", SQL_VARBINARY, "varbinary(16)", 16),
    ]
    columns = [(d[0], d[1], *d[3:]) for d in declarations]
    row = (
        True,
        255,
        -32768,
        2147483647,
        -(2**63),
        1.25,
        Decimal("12345678.1234567890"),
        "long🚀漢字\x00" * 2000,
        b"\x00\xff" * 10000,
        datetime.date(2020, 2, 29),
        datetime.time(1, 2, 3, 123456),
        datetime.datetime(2021, 3, 4, 1, 2, 3, 123456),
        datetime.datetime(
            2021,
            3,
            4,
            1,
            2,
            3,
            123456,
            tzinfo=datetime.timezone(datetime.timedelta(hours=-7, minutes=-30)),
        ),
        uuid.uuid4(),
        None,
    )
    cursor.execute(
        f"CREATE TYPE dbo.[{name}] AS TABLE ("
        + ",".join(f"[{d[0]}] {d[2]} NULL" for d in declarations)
        + ")"
    )
    projection = ",".join(
        "CONVERT(varbinary(max),text_value) AS text_value" if d[0] == "text_value" else d[0]
        for d in declarations
    )
    cursor.execute(
        f"CREATE PROCEDURE dbo.[{proc}] @items dbo.[{name}] READONLY AS "
        f"SELECT {projection} FROM @items"
    )
    db_connection.commit()
    try:
        with localcontext() as ctx:
            ctx.prec = 5
            source = [list(row), [None] * len(row)]
            value = TableValuedParameter(f"dbo.{name}", columns, source)
            cursor.execute(f"EXEC dbo.[{proc}] ?", value)
        results = [tuple(r) for r in cursor.fetchall()]
        assert len(results) == 2
        actual = next(r for r in results if r[0] is not None)
        expected = list(row)
        # Verify every UTF-16 byte stored by SQL Server, including embedded NULs,
        # independently of the result decoder's text-terminator handling.
        expected[7] = row[7].encode("utf-16le")
        assert actual == tuple(expected)
        assert (None,) * len(row) in results
        assert source == [list(row), [None] * len(row)]
    finally:
        cursor.execute(f"DROP PROCEDURE dbo.[{proc}]")
        cursor.execute(f"DROP TYPE dbo.[{name}]")
        db_connection.commit()


def test_date_range(cursor, db_connection):
    name = f"Dates_{uuid.uuid4().hex}"
    rows = [
        (None,),
        (datetime.date.min,),
        (datetime.date(1752, 12, 31),),
        (datetime.date(1753, 1, 1),),
        (datetime.date.max,),
    ]
    cursor.execute(f"CREATE TYPE dbo.[{name}] AS TABLE (value date NULL)")
    db_connection.commit()
    try:
        value = TableValuedParameter(f"dbo.{name}", [("value", SQL_TYPE_DATE)], rows)
        cursor.execute("SELECT value FROM ? ORDER BY value", value)
        assert [tuple(row) for row in cursor.fetchall()] == rows
    finally:
        cursor.execute(f"DROP TYPE dbo.[{name}]")
        db_connection.commit()


def test_generator_is_lazy_bounded_and_invokes_once(cursor, tvp_schema):
    seen = []
    live = weakref.WeakSet()
    peak = 0

    class LiveRow(list):
        __hash__ = object.__hash__

    def rows():
        nonlocal peak
        for i in range(5000):
            row = LiveRow((i, "x" * 1000))
            live.add(row)
            peak = max(peak, len(live))
            assert len(live) <= 2, "The driver retained earlier generator rows"
            seen.append(i)
            yield row

    value = items(tvp_schema, rows())
    assert seen == []
    cursor.execute(f"EXEC [{tvp_schema}].CountRows ?", value)
    assert tuple(cursor.fetchone()) == (5000, 4999 * 5000 // 2)
    assert len(seen) == 5000
    assert peak <= 2
    assert len(live) == 0
    cursor.execute(f"SELECT COUNT(*) FROM [{tvp_schema}].Calls")
    assert cursor.fetchone()[0] == 1


@pytest.mark.parametrize("failure", [ValueError("source failed"), KeyboardInterrupt("interrupted")])
def test_generator_failure_preserves_error_and_recovers(cursor, tvp_schema, failure):
    def rows():
        yield 1, "first"
        raise failure

    with pytest.raises(type(failure)) as error:
        cursor.execute(f"EXEC [{tvp_schema}].Echo ?, ?, ?", 0, items(tvp_schema, rows()), "")
    assert error.value is failure
    assert cursor.execute("SELECT 42").fetchone()[0] == 42
    cursor.execute(f"SELECT COUNT(*) FROM [{tvp_schema}].Calls")
    assert cursor.fetchone()[0] == 0
    cursor.execute(f"EXEC [{tvp_schema}].Echo ?, ?, ?", 0, items(tvp_schema, [(2, "ok")]), "")
    assert tuple(cursor.fetchone()) == (2, "ok")


@pytest.mark.parametrize("rows", [[], [(1, None)], [(1, ""), (2, "🚀" * 10000)]])
def test_streaming_empty_null_and_large_cells(cursor, tvp_schema, rows):
    cursor.execute(f"EXEC [{tvp_schema}].Echo ?, ?, ?", 0, items(tvp_schema, iter(rows)), "")
    assert [tuple(row) for row in cursor.fetchall()] == rows


@pytest.mark.parametrize("bad", [[(1,)], [(1, "a", "extra")], [1], [(1, object())]])
def test_bad_stream_row_recovers(cursor, tvp_schema, bad):
    with pytest.raises((TypeError, ValueError)):
        cursor.execute(f"EXEC [{tvp_schema}].CountRows ?", items(tvp_schema, iter(bad)))
    assert cursor.execute("SELECT 1").fetchone()[0] == 1


def test_implicit_nested_sequences_are_not_tvps(cursor):
    for value in ([], [(1, "a")], ((1, "a"),)):
        with pytest.raises(TypeError, match="Unsupported parameter type"):
            cursor.execute("SELECT ?", (value,))
    assert cursor.execute("SELECT 1").fetchone()[0] == 1


@pytest.mark.parametrize(
    "kwargs,error",
    [
        ({"rows": None}, TypeError),
        ({"columns": []}, TypeError),
        ({"columns": [("x", SQL_WVARCHAR)]}, ValueError),
        ({"columns": [("x", SQL_DECIMAL, 39, 2)]}, ValueError),
        ({"columns": [("x", SQL_DECIMAL, 4, 5)]}, ValueError),
        ({"columns": [("x", 9999)]}, ValueError),
        ({"type_name": "db.dbo.Items"}, ValueError),
        ({"type_name": "dbo.Items", "schema": "dbo"}, ValueError),
    ],
)
def test_explicit_contract_validation(kwargs, error):
    arguments = {"type_name": "Items", "columns": [("id", SQL_INTEGER)], "rows": []}
    arguments.update(kwargs)
    with pytest.raises(error):
        TableValuedParameter(**arguments)


@pytest.mark.parametrize("bad", [2**31, -(2**31) - 1])
def test_explicit_integer_width_does_not_truncate(cursor, tvp_schema, bad):
    with pytest.raises(ValueError, match="out of range"):
        cursor.execute(f"EXEC [{tvp_schema}].CountRows ?", items(tvp_schema, [(bad, None)]))
    assert cursor.execute("SELECT 1").fetchone()[0] == 1


def test_precise_decimal_streaming(cursor, db_connection):
    name = f"Decimal_{uuid.uuid4().hex}"
    proc = f"Proc_{name}"
    cursor.execute(f"CREATE TYPE dbo.[{name}] AS TABLE (amount decimal(38,6))")
    cursor.execute(
        f"CREATE PROC dbo.[{proc}] @items dbo.[{name}] READONLY AS " "SELECT amount FROM @items"
    )
    db_connection.commit()
    values = [
        Decimal("12345678901234567890123456789012.123456"),
        None,
        Decimal("0.000001"),
        Decimal("-99999999999999999999999999999999.999999"),
    ]
    try:
        with localcontext() as context:
            context.prec = 3
            value = TableValuedParameter(
                name, [("amount", SQL_DECIMAL, 38, 6)], ((v,) for v in values)
            )
            cursor.execute(f"EXEC dbo.[{proc}] ?", value)
        assert [r[0] for r in cursor.fetchall()] == values
        for value in (Decimal("NaN"), Decimal("Infinity")):
            with pytest.raises(ValueError, match="non-finite"):
                cursor.execute(
                    f"EXEC dbo.[{proc}] ?",
                    TableValuedParameter(name, [("amount", SQL_DECIMAL, 38, 6)], [(value,)]),
                )
            assert cursor.execute("SELECT 1").fetchone()[0] == 1
    finally:
        cursor.execute(f"DROP PROC dbo.[{proc}]")
        cursor.execute(f"DROP TYPE dbo.[{name}]")
        db_connection.commit()


def test_sync_temporal_scale_six(cursor, db_connection):
    name = f"Times_{uuid.uuid4().hex}"
    proc = f"Proc_{name}"
    cursor.execute(f"CREATE TYPE dbo.[{name}] AS TABLE (t time(6), d datetime2(6))")
    cursor.execute(f"CREATE PROC dbo.[{proc}] @items dbo.[{name}] READONLY AS SELECT * FROM @items")
    db_connection.commit()
    rows = [
        (datetime.time(1, 2, 3, 123456), datetime.datetime(2024, 2, 29, 1, 2, 3, 123456)),
        (None, None),
    ]
    try:
        value = TableValuedParameter(
            name, [("t", SQL_TYPE_TIME, 16, 6), ("d", SQL_TYPE_TIMESTAMP, 26, 6)], iter(rows)
        )
        cursor.execute(f"EXEC dbo.[{proc}] ?", value)
        assert [tuple(row) for row in cursor.fetchall()] == rows
    finally:
        cursor.execute(f"DROP PROC dbo.[{proc}]")
        cursor.execute(f"DROP TYPE dbo.[{name}]")
        db_connection.commit()


def test_schema_mismatch_and_server_error_recovery(cursor, tvp_schema):
    with pytest.raises(DatabaseError):
        cursor.execute(
            f"EXEC [{tvp_schema}].CountRows ?",
            TableValuedParameter("DoesNotExist", [("id", SQL_INTEGER)], [], schema=tvp_schema),
        )
    assert cursor.execute("SELECT 42").fetchone()[0] == 42
    with pytest.raises(DatabaseError):
        cursor.execute(
            f"EXEC [{tvp_schema}].CountRows ?",
            TableValuedParameter(
                "Items", [("only_column", SQL_INTEGER)], [(1,)], schema=tvp_schema
            ),
        )
    assert cursor.execute("SELECT 43").fetchone()[0] == 43


def test_multiple_generator_tvps_and_large_scalar(cursor, tvp_schema):
    cursor.execute(
        f"EXEC [{tvp_schema}].Pair ?, ?",
        items(tvp_schema, iter([(1, "left"), (2, None)])),
        items(tvp_schema, iter([(3, "right")])),
    )
    assert sorted(tuple(row) for row in cursor.fetchall()) == [(1, "left"), (2, None), (3, "right")]
    suffix = "🚀" * 8000
    cursor.execute(
        f"EXEC [{tvp_schema}].Echo ?, ?, ?", 0, items(tvp_schema, iter([(1, "head")])), suffix
    )
    assert tuple(cursor.fetchone()) == (1, "head" + suffix)


def test_executemany_and_inputsizes_reject_tvps_before_consumption(cursor, tvp_schema):
    def rows():
        pytest.fail("Unsupported operation consumed a TVP row source")
        yield 1, "never"

    value = items(tvp_schema, rows())
    with pytest.raises(NotSupportedError, match="executemany"):
        cursor.executemany(f"EXEC [{tvp_schema}].CountRows ?", [(value,)])
    cursor.setinputsizes([SQL_INTEGER])
    try:
        with pytest.raises(NotSupportedError, match="setinputsizes"):
            cursor.execute(f"EXEC [{tvp_schema}].CountRows ?", value)
    finally:
        cursor.setinputsizes([])
    assert cursor.execute("SELECT 1").fetchone()[0] == 1


@pytest.mark.parametrize("operation", ["execute", "close"])
def test_generator_cannot_reenter_its_cursor(cursor, tvp_schema, operation):
    def rows():
        yield 1, "first"
        if operation == "execute":
            cursor.execute("SELECT 99")
        else:
            cursor.close()
        yield 2, "second"

    with pytest.raises(DatabaseError, match="while it is executing"):
        cursor.execute(f"EXEC [{tvp_schema}].CountRows ?", items(tvp_schema, rows()))
    assert cursor.execute("SELECT 42").fetchone()[0] == 42


def _run_isolated_stream_checks(mode, schema):
    connection_string = os.environ["DB_CONNECTION_STRING"]
    if mode == "forwarder":
        target = _parse_server(connection_string)
        assert target is not None
        connection_string = _replace_server(connection_string, *_start_forwarder(target))
    connection = connect(connection_string, autocommit=True)
    cursor = connection.cursor()
    try:
        if mode == "disconnect":
            original = ValueError("original source failure after disconnect")
            source_errors = []

            def rows():
                yield 1, "first"
                try:
                    connection.close()
                except DatabaseError as error:
                    source_errors.append(error)
                    raise
                source_errors.append(original)
                raise original

            try:
                cursor.execute(f"EXEC [{schema}].CountRows ?", items(schema, rows()))
            except (ValueError, DatabaseError) as error:
                assert error is source_errors[0]
            else:
                raise AssertionError("Lost the original iterator error")
            print("OK TVP streaming")
            return
        elif mode == "cancel":
            waiting = threading.Event()
            resume = threading.Event()
            errors = []

            def rows():
                yield 1, "first"
                waiting.set()
                assert resume.wait(10)
                yield 2, "second"

            def execute():
                try:
                    cursor.execute(f"EXEC [{schema}].CountRows ?", items(schema, rows()))
                except BaseException as error:
                    errors.append(error)

            worker = threading.Thread(target=execute)
            worker.start()
            assert waiting.wait(10)
            try:
                cursor.hstmt._cancel()
            finally:
                resume.set()
            worker.join(10)
            assert not worker.is_alive()
            assert len(errors) == 1 and isinstance(errors[0], DatabaseError)
        else:
            count = 20000 if mode == "memory" else 1000
            if mode == "memory":
                import resource

                before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            cursor.execute(
                f"EXEC [{schema}].CountRows ?",
                items(schema, ((i, str(i).ljust(4096, "x")) for i in range(count))),
            )
            assert tuple(cursor.fetchone()) == (count, count * (count - 1) // 2)
            if mode == "memory":
                growth = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss - before
                if sys.platform != "darwin":
                    growth *= 1024
                # Input exceeds 80MB of Python text / 160MB UTF-16. Additional
                # native and Python memory must stay below a table-sized copy.
                assert growth < 64 * 1024 * 1024, growth
                print(f"peak RSS growth: {growth} bytes")
        assert cursor.execute("SELECT 42").fetchone()[0] == 42
    finally:
        cursor.close()
        connection.close()
    print("OK TVP streaming")


@pytest.mark.parametrize(
    "mode",
    [
        "cancel",
        "forwarder",
        "disconnect",
        pytest.param(
            "memory",
            marks=[
                pytest.mark.stress,
                pytest.mark.skipif(sys.platform == "win32", reason="POSIX resource accounting"),
            ],
        ),
    ],
)
def test_isolated_stream_lifecycle(conn_str, tvp_schema, mode):
    if mode == "forwarder":
        target = _parse_server(conn_str)
        if target is None or "\\" in target[0]:
            pytest.skip("TCP forwarding requires a host, not a LocalDB or named-pipe instance")
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(sys.path)
    # The memory stress case sends 160MB, unlike the short deadlock probes.
    # Keep the same row count and RSS bound, but allow slower transport.
    timeout = 120 if mode == "memory" else WATCHDOG_SECONDS
    try:
        result = subprocess.run(
            [
                sys.executable,
                "-u",
                "-c",
                "import faulthandler;"
                f"faulthandler.dump_traceback_later({timeout - 5});"
                "from test_028_table_valued_parameters import _run_isolated_stream_checks;"
                f"_run_isolated_stream_checks({mode!r}, {tvp_schema!r})",
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as error:
        output = "".join(
            chunk.decode(errors="replace") if isinstance(chunk, bytes) else chunk or ""
            for chunk in (error.stdout, error.stderr)
        )
        pytest.fail(f"TVP {mode} subprocess exceeded {timeout}s:\n{output}")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "OK TVP streaming" in result.stdout
    print(result.stdout, end="")
