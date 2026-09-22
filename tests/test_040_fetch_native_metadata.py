"""Native fetch metadata must preserve public descriptions and result-set state."""

import datetime as dt
import os
from pathlib import Path
import subprocess
import sys
import textwrap
from decimal import Decimal
from uuid import UUID

import pytest

import mssql_python
from mssql_python import ddbc_bindings


@pytest.fixture
def metadata_cursor(conn_str):
    with mssql_python.connect(conn_str) as connection:
        with connection.cursor() as cursor:
            yield cursor


def _query(columns, count=20):
    values = ",".join(f"({i})" for i in range(1, 21))
    return (
        f"SELECT {','.join(columns)} FROM (VALUES {values}) AS source(id) "
        f"WHERE id<={count} ORDER BY id"
    )


def _assert_rows(rows, expected):
    assert [tuple(row) for row in rows] == expected
    assert [[type(value) for value in row] for row in rows] == [
        [type(value) for value in row] for row in expected
    ]


def _describe(cursor):
    result = []
    assert ddbc_bindings.DDBCSQLDescribeCol(cursor.hstmt, result) == 0
    for column in result:
        assert set(column) == {"ColumnName", "DataType", "ColumnSize", "DecimalDigits", "Nullable"}
        assert type(column["ColumnName"]) is str
        for key in ("DataType", "ColumnSize", "DecimalDigits", "Nullable"):
            assert type(column[key]) is int
    return result


@pytest.mark.parametrize("width", [3, 24])
@pytest.mark.parametrize("size", [None, 1, 10, 1000, "varied"])
@pytest.mark.parametrize("count", [0, 20])
def test_fetchmany_shape_sizes_and_eof(metadata_cursor, width, size, count):
    cursor = metadata_cursor
    expressions = ["id", "CONVERT(NVARCHAR(30),N'row')", "CONVERT(FLOAT,id)*0.25"]
    columns = [f"{expressions[i % 3]} AS c{i}" for i in range(width)]
    cursor.execute(_query(columns, count))
    assert cursor.arraysize == 1
    description = cursor.description
    assert all(len(column) == 7 for column in description)
    assert [column[0] for column in description] == [f"c{i}" for i in range(width)]
    metadata = _describe(cursor)
    output = []
    iteration = 0
    while True:
        fetch_size = (1, 10, 3, 1000)[iteration % 4] if size == "varied" else size
        batch = cursor.fetchmany() if fetch_size is None else cursor.fetchmany(fetch_size)
        assert cursor.description == description
        if not batch:
            break
        output.extend(batch)
        iteration += 1
    _assert_rows(output, [(i, "row", i * 0.25) * (width // 3) for i in range(1, count + 1)])
    assert _describe(cursor) == metadata
    assert cursor.fetchmany(1) == []
    assert cursor.fetchone() is None
    assert cursor.fetchall() == []


@pytest.mark.parametrize("method", ["fetchmany", "fetchall", "arrow_batch"])
def test_public_metadata_names_and_fields(metadata_cursor, method):
    cursor = metadata_cursor
    names = [
        "duplicate",
        "duplicate",
        "\u03a9\u540d",
        "emoji_\U0001f600",
        "bracket]name",
        "x" * 128,
    ]
    columns = [f"CONVERT(INT,id) AS [{name.replace(']', ']]')}]" for name in names]
    cursor.execute(_query(columns, 1))
    metadata = _describe(cursor)
    assert metadata == [
        {"ColumnName": name, "DataType": 4, "ColumnSize": 10, "DecimalDigits": 0, "Nullable": 1}
        for name in names
    ]
    assert [column[0] for column in cursor.description] == names
    if method == "arrow_batch":
        pytest.importorskip("pyarrow")
        batch = cursor.arrow_batch(1)
        assert batch.schema.names == names
        assert [column.to_pylist() for column in batch.columns] == [[1]] * len(names)
    else:
        rows = cursor.fetchmany(1) if method == "fetchmany" else cursor.fetchall()
        _assert_rows(rows, [(1,) * len(names)])
    assert _describe(cursor) == metadata


_TYPES = [
    ("INT", "7", 7),
    ("SMALLINT", "-7", -7),
    ("BIGINT", "2147483649", 2147483649),
    ("TINYINT", "255", 255),
    ("BIT", "1", True),
    ("REAL", "1.5", 1.5),
    ("FLOAT", "2.25", 2.25),
    ("DECIMAL(20,4)", "123.4500", Decimal("123.4500")),
    ("NUMERIC(28,8)", "-0.125", Decimal("-0.125")),
    ("MONEY", "4.25", Decimal("4.25")),
    ("DATE", "'2001-02-03'", dt.date(2001, 2, 3)),
    ("TIME(7)", "'12:34:56.1234567'", dt.time(12, 34, 56, 123456)),
    ("DATETIME2(7)", "'2001-02-03T12:34:56.1234567'", dt.datetime(2001, 2, 3, 12, 34, 56, 123456)),
    ("DATETIME", "'2001-02-03T12:34:56'", dt.datetime(2001, 2, 3, 12, 34, 56)),
    (
        "DATETIMEOFFSET(7)",
        "'2001-02-03T12:34:56.1234567+05:30'",
        dt.datetime(2001, 2, 3, 12, 34, 56, 123456, dt.timezone(dt.timedelta(minutes=330))),
    ),
    (
        "UNIQUEIDENTIFIER",
        "'12345678-1234-5678-1234-567812345678'",
        UUID("12345678-1234-5678-1234-567812345678"),
    ),
    ("VARCHAR(20)", "'ascii'", "ascii"),
    ("CHAR(8)", "'ascii'", "ascii   "),
    ("NVARCHAR(30)", "N'\u03a9\U0001f600'", "\u03a9\U0001f600"),
    ("NCHAR(5)", "N'\u03a9'", "\u03a9    "),
    ("VARBINARY(10)", "0x00010200", b"\x00\x01\x02\x00"),
    ("BINARY(4)", "0x00010203", b"\x00\x01\x02\x03"),
    ("VARCHAR(1)", "''", ""),
    ("NVARCHAR(1)", "N''", ""),
]


@pytest.mark.parametrize("size", [1, 10, 1000])
def test_fetchmany_typed_nulls_and_values(metadata_cursor, size):
    columns = [
        f"CASE WHEN id%3=0 THEN CAST(NULL AS {sqltype}) "
        f"ELSE CAST({literal} AS {sqltype}) END AS c{i}"
        for i, (sqltype, literal, _) in enumerate(_TYPES)
    ]
    cursor = metadata_cursor
    cursor.execute(_query(columns))
    description = cursor.description
    metadata = _describe(cursor)
    assert len(metadata) == 24
    output = []
    while batch := cursor.fetchmany(size):
        output.extend(batch)
    values = tuple(value for _, _, value in _TYPES)
    _assert_rows(output, [(None,) * 24 if i % 3 == 0 else values for i in range(1, 21)])
    assert cursor.description == description
    assert _describe(cursor) == metadata


def test_reexecute_and_nextset_change_shape(metadata_cursor):
    cursor = metadata_cursor
    for _ in range(3):
        cursor.execute("SELECT 1 AS first_name; SELECT N'new' AS second_name, 2 AS extra")
        _assert_rows(cursor.fetchmany(1), [(1,)])
        assert cursor.nextset()
        assert [col[0] for col in cursor.description] == ["second_name", "extra"]
        _assert_rows(cursor.fetchmany(10), [("new", 2)])
        assert not cursor.nextset()
        cursor.execute("SELECT CAST(3.5 AS DECIMAL(6,2)) AS replacement")
        assert _describe(cursor)[0]["ColumnName"] == "replacement"
        _assert_rows(cursor.fetchmany(), [(Decimal("3.50"),)])


def test_converter_changes_on_execute_and_live_decoding(metadata_cursor):
    cursor = metadata_cursor
    connection = cursor.connection
    cursor.execute(_query(["id", "CAST('ascii' AS VARCHAR(12)) AS txt"], 4))
    _assert_rows(cursor.fetchmany(1), [(1, "ascii")])
    calls = []

    def convert(value):
        calls.append(value)
        return value + 100

    connection.add_output_converter(mssql_python.SQL_INTEGER, convert)
    connection.setdecoding(mssql_python.SQL_CHAR, "utf-8", mssql_python.SQL_CHAR)
    cursor.execute(_query(["id", "CAST('ascii' AS VARCHAR(12)) AS txt"], 4))
    _assert_rows(cursor.fetchmany(1), [(101, "ascii")])
    assert calls == [1]
    connection.setdecoding(mssql_python.SQL_CHAR, "latin1", mssql_python.SQL_CHAR)
    _assert_rows(cursor.fetchmany(1), [(102, "ascii")])
    assert calls == [1, 2]
    connection.remove_output_converter(mssql_python.SQL_INTEGER)
    cursor.execute(_query(["id", "CAST('ascii' AS VARCHAR(12)) AS txt"], 1))
    _assert_rows(cursor.fetchmany(1), [(1, "ascii")])
    connection.setdecoding(mssql_python.SQL_CHAR)
    cursor.execute(_query(["id", "CAST('ascii' AS VARCHAR(12)) AS txt"], 1))
    _assert_rows(cursor.fetchmany(10), [(1, "ascii")])


@pytest.mark.parametrize("size", [1, 10])
def test_fetchmany_lob_and_xml_typed_nulls(metadata_cursor, size):
    cursor = metadata_cursor
    columns = [
        "CASE WHEN id%2=0 THEN CAST(NULL AS NVARCHAR(MAX)) ELSE "
        "REPLICATE(CAST(N'x' AS NVARCHAR(MAX)),9001) END AS txt",
        "CASE WHEN id%2=0 THEN CAST(NULL AS VARBINARY(MAX)) ELSE "
        "CAST(REPLICATE(CAST('a' AS VARCHAR(MAX)),10003) AS VARBINARY(MAX)) END AS bin",
        "CASE WHEN id%2=0 THEN CAST(NULL AS XML) ELSE CAST('<r>value</r>' AS XML) END AS xml",
    ]
    cursor.execute(_query(columns, 4))
    metadata = _describe(cursor)
    rows = []
    while batch := cursor.fetchmany(size):
        rows.extend(batch)
    values = ("x" * 9001, b"a" * 10003, "<r>value</r>")
    _assert_rows(rows, [values, (None, None, None), values, (None, None, None)])
    assert _describe(cursor) == metadata


def _isolated(script, tmp_path):
    environment = dict(os.environ)
    root = str(Path(mssql_python.__file__).resolve().parent.parent)
    environment["PYTHONPATH"] = os.pathsep.join([root, environment.get("PYTHONPATH", "")])
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(script)],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        timeout=45,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_interleaving_movement_and_variant_freshness(tmp_path):
    _isolated(
        """
        import os
        import gc
        import mssql_python as db
        with db.connect(os.environ["DB_CONNECTION_STRING"]) as connection:
            with connection.cursor() as cursor:
                query = "SELECT id FROM (VALUES(1),(2),(3),(4),(5),(6),(7)) s(id) ORDER BY id"
                for _ in range(4):
                    cursor.execute(query)
                    assert cursor.fetchmany(1)[0][0] == 1
                    gc.collect()
                    assert cursor.fetchone()[0] == 2
                    cursor.scroll(1)
                    assert cursor.fetchmany(1)[0][0] == 4
                    cursor.skip(1)
                    assert [tuple(row) for row in cursor.fetchall()] == [(6,), (7,)]
                cursor.execute("CREATE TABLE #metadata_variant(id INT, v SQL_VARIANT)")
                cursor.execute(
                    "INSERT INTO #metadata_variant VALUES "
                    "(1,CAST('abc' AS VARCHAR(3))),"
                    "(2,CAST('abcdefgh' AS VARCHAR(8))),"
                    "(3,CAST(REPLICATE('x',30) AS VARCHAR(30)))"
                )
                for method in ("fetchmany", "fetchall"):
                    cursor.execute("SELECT v FROM #metadata_variant ORDER BY id")
                    assert cursor.fetchone()[0] == "abc"
                    if method == "fetchmany":
                        assert cursor.fetchmany(1)[0][0] == "abcdefgh"
                        assert cursor.fetchmany(1)[0][0] == "x"*30
                    else:
                        assert [r[0] for r in cursor.fetchall()] == ["abcdefgh", "x"*30]
        """,
        tmp_path,
    )


def test_closure_and_error_recovery(tmp_path):
    _isolated(
        """
        import os
        import mssql_python as db
        from mssql_python import Cursor, InterfaceError, ProgrammingError, DatabaseError
        with db.connect(os.environ["DB_CONNECTION_STRING"]) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1 AS c")
                assert cursor.fetchmany(0) == []
                assert cursor.fetchmany(-1) == []
                assert cursor.fetchmany(1)[0][0] == 1
                try:
                    cursor.execute("SELECT invalid_column FROM (VALUES(1)) t(c)")
                except DatabaseError:
                    pass
                else:
                    raise AssertionError("invalid query did not raise")
                cursor.execute("SELECT 2 AS changed")
                assert cursor.fetchmany(1)[0][0] == 2
            try:
                cursor.fetchmany(1)
            except ProgrammingError:
                pass
            else:
                raise AssertionError("closed cursor did not raise")
        connection = db.connect(os.environ["DB_CONNECTION_STRING"])
        cursor = Cursor(connection)
        cursor.execute("SELECT 1")
        connection.close()
        try:
            cursor.fetchmany(1)
        except (InterfaceError, ProgrammingError):
            pass
        else:
            raise AssertionError("closed connection did not raise")
        cursor.close()
        """,
        tmp_path,
    )


def test_malformed_column_name_fails_before_fetch(tmp_path):
    _isolated(
        """
        import os
        import mssql_python as db
        from mssql_python import ddbc_bindings as native
        with db.connect(os.environ["DB_CONNECTION_STRING"]) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "DECLARE @s NVARCHAR(200) = N'SELECT 1 AS [' + "
                    "CAST(0x00D8 AS NVARCHAR(1)) + N']'; EXEC(@s)"
                )
                for operation in (
                    lambda: native.DDBCSQLDescribeCol(cursor.hstmt, []),
                    lambda: cursor.fetchmany(1),
                ):
                    try:
                        operation()
                    except UnicodeDecodeError:
                        pass
                    else:
                        raise AssertionError("malformed UTF-16 column name did not raise")
                assert native.DDBCSQLFetch(cursor.hstmt) == 0
                assert native.DDBCSQLFetch(cursor.hstmt) == 100
        """,
        tmp_path,
    )


@pytest.mark.skipif(
    not hasattr(ddbc_bindings, "profiling"), reason="requires native profiling instrumentation"
)
def test_fetchmany_avoids_python_description_roundtrip(tmp_path):
    _isolated(
        """
        import os
        import mssql_python as db
        from mssql_python import ddbc_bindings as native
        with db.connect(os.environ["DB_CONNECTION_STRING"]) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT id FROM (VALUES(1),(2)) s(id) ORDER BY id")
                native.profiling.reset()
                native.profiling.enable()
                try:
                    metadata = []
                    native.DDBCSQLDescribeCol(cursor.hstmt, metadata)
                finally:
                    native.profiling.disable()
                assert native.profiling.get_stats()["ddbc::SQLDescribeCol_wrap"]["calls"] == 1
                assert len(metadata) == 1
                native.profiling.reset()
                native.profiling.enable()
                try:
                    assert cursor.fetchmany(1)[0][0] == 1
                    assert cursor.fetchmany(1)[0][0] == 2
                    assert cursor.fetchmany(1) == []
                finally:
                    native.profiling.disable()
                stats = native.profiling.get_stats()
                assert stats["ddbc::FetchMany_wrap"]["calls"] == 3
                assert stats.get("ddbc::SQLDescribeCol_wrap", {}).get("calls", 0) == 0
        """,
        tmp_path,
    )


@pytest.mark.parametrize("method", ["one", "many"])
def test_result_metadata_prepared_reexecution(metadata_cursor, method):
    cursor = metadata_cursor
    statement = cursor.hstmt
    query = "SELECT CAST(? AS INT) AS n, CAST(? AS NVARCHAR(30)) AS text_value"
    for value in range(4):
        cursor.execute(query, (value, f"value-{value}"))
        assert cursor.hstmt is statement
        assert cursor.is_stmt_prepared[0]
        profiling = hasattr(ddbc_bindings, "profiling")
        if profiling:
            ddbc_bindings.profiling.reset()
            ddbc_bindings.profiling.enable()
        try:
            rows = [cursor.fetchone()] if method == "one" else cursor.fetchmany()
        finally:
            if profiling:
                ddbc_bindings.profiling.disable()
        _assert_rows(rows, [(value, f"value-{value}")])
        if profiling:
            assert (
                ddbc_bindings.profiling.get_stats()["ddbc::SQLDescribeCol::driver_call"]["calls"]
                == 2
            )
        assert cursor.fetchone() is None
    cursor.execute("SELECT CAST(? AS DECIMAL(8,2)) AS amount", (Decimal("3.25"),))
    _assert_rows(cursor.fetchall(), [(Decimal("3.25"),)])


def test_result_metadata_catalog_replacement(metadata_cursor):
    cursor = metadata_cursor
    for _ in range(2):
        cursor.execute("SELECT 42 AS previous_column")
        _assert_rows(cursor.fetchmany(), [(42,)])
        cursor.getTypeInfo(mssql_python.SQL_INTEGER)
        description = cursor.description
        assert len(description) > 1
        row = cursor.fetchone()
        assert row is not None and len(row) == len(description)
        assert row[1] == mssql_python.SQL_INTEGER
        cursor.fetchall()
        cursor.execute("SELECT N'replaced' AS new_column, 5 AS extra")
        _assert_rows(cursor.fetchmany(), [("replaced", 5)])


def test_result_metadata_native_reset_and_replacement(tmp_path):
    _isolated(
        """
        import os
        import mssql_python as db
        from mssql_python import ddbc_bindings as native
        with db.connect(os.environ["DB_CONNECTION_STRING"]) as connection:
            stmt = connection._conn.alloc_statement_handle()
            try:
                for _ in range(3):
                    assert native.DDBCSQLExecDirect(stmt, "SELECT 1 AS a") in (0, 1)
                    rows = []
                    assert native.DDBCSQLFetchMany(stmt, rows, 1) in (0, 1)
                    assert rows == [[1]]
                    assert native.DDBCSQLResetStmt(stmt) in (0, 1)
                    assert native.DDBCSQLExecDirect(
                        stmt, "SELECT CAST(2 AS BIGINT) AS b, N'new' AS c"
                    ) in (0, 1)
                    row = []
                    assert native.DDBCSQLFetchOne(stmt, row) in (0, 1)
                    assert row == [2, "new"]
                    stmt._close_cursor()
            finally:
                stmt.free()
        """,
        tmp_path,
    )


@pytest.mark.parametrize("operation", ["commit", "rollback", "autocommit"])
def test_result_metadata_transaction_recovery(metadata_cursor, operation):
    cursor = metadata_cursor
    connection = cursor.connection
    cursor.execute(_query(["id"], 3))
    _assert_rows(cursor.fetchmany(), [(1,)])
    if operation == "autocommit":
        connection.autocommit = True
    else:
        getattr(connection, operation)()
    cursor.execute("SELECT CAST(5.75 AS DECIMAL(8,2)) AS changed, N'text' AS extra")
    _assert_rows(cursor.fetchall(), [(Decimal("5.75"), "text")])


@pytest.mark.parametrize("operation", ["commit", "rollback", "autocommit"])
def test_result_metadata_transaction_preserved_cursor(metadata_cursor, operation):
    cursor = metadata_cursor
    connection = cursor.connection
    info = (
        mssql_python.SQL_CURSOR_ROLLBACK_BEHAVIOR
        if operation == "rollback"
        else mssql_python.SQL_CURSOR_COMMIT_BEHAVIOR
    )
    if connection.getinfo(info) != 2:  # SQL_CB_PRESERVE
        pytest.skip("Driver does not preserve cursors; cache/helper coverage is in tests/native")
    cursor.execute(_query(["id"], 3))
    _assert_rows([cursor.fetchone()], [(1,)])
    if operation == "autocommit":
        connection.autocommit = True
    else:
        getattr(connection, operation)()
    profiling = hasattr(ddbc_bindings, "profiling")
    if profiling:
        ddbc_bindings.profiling.reset()
        ddbc_bindings.profiling.enable()
    try:
        _assert_rows([cursor.fetchone()], [(2,)])
        _assert_rows(cursor.fetchmany(), [(3,)])
    finally:
        if profiling:
            ddbc_bindings.profiling.disable()
    if profiling:
        assert (
            ddbc_bindings.profiling.get_stats()["ddbc::SQLDescribeCol::driver_call"]["calls"] == 1
        )


def test_result_metadata_arrow_interleave(tmp_path):
    pytest.importorskip("pyarrow")
    _isolated(
        """
        import gc
        import os
        import mssql_python as db
        with db.connect(os.environ["DB_CONNECTION_STRING"]) as connection:
            with connection.cursor() as cursor:
                query = ("SELECT id, CAST(id AS BIGINT) AS big FROM "
                         "(VALUES(1),(2),(3),(4),(5),(6)) s(id) ORDER BY id")
                for _ in range(3):
                    cursor.execute(query)
                    assert tuple(cursor.fetchone()) == (1, 1)
                    assert [tuple(r) for r in cursor.fetchmany(2)] == [(2, 2), (3, 3)]
                    batch = cursor.arrow_batch(1)
                    assert [c.to_pylist() for c in batch.columns] == [[4], [4]]
                    gc.collect()
                    assert tuple(cursor.fetchone()) == (5, 5)
                    assert [tuple(r) for r in cursor.fetchall()] == [(6, 6)]
                    assert cursor.fetchmany() == []
        """,
        tmp_path,
    )


@pytest.mark.parametrize("method", ["one", "many", "all"])
def test_result_metadata_variant_type_and_size_changes(metadata_cursor, method):
    cursor = metadata_cursor
    cursor.execute(
        "CREATE TABLE #metadata_mixed_variant (id INT, v SQL_VARIANT, txt NVARCHAR(MAX))"
    )
    cursor.execute(
        "INSERT INTO #metadata_mixed_variant VALUES "
        "(1,CAST(NULL AS SQL_VARIANT),N'first'),"
        "(2,CAST(CAST('abc' AS VARCHAR(3)) AS SQL_VARIANT),NULL),"
        "(3,CAST(CAST('abcdefgh' AS VARCHAR(8)) AS SQL_VARIANT),N'third'),"
        "(4,CAST(CAST(17 AS INT) AS SQL_VARIANT),NULL),"
        "(5,CAST(CAST(3.25 AS DECIMAL(8,2)) AS SQL_VARIANT),N'fifth'),"
        "(6,CAST(NULL AS SQL_VARIANT),NULL),"
        "(7,CAST(CAST(0x010200 AS VARBINARY(3)) AS SQL_VARIANT),N'last')"
    )
    cursor.execute("SELECT v, txt FROM #metadata_mixed_variant ORDER BY id")
    if method == "one":
        rows = list(cursor)
    elif method == "many":
        rows = []
        while batch := cursor.fetchmany():
            rows.extend(batch)
    else:
        rows = cursor.fetchall()
    _assert_rows(
        rows,
        [
            (None, "first"),
            ("abc", None),
            ("abcdefgh", "third"),
            (17, None),
            (Decimal("3.25"), "fifth"),
            (None, None),
            (b"\x01\x02\x00", "last"),
        ],
    )


def test_result_metadata_one_then_malformed_name_many_does_not_advance(tmp_path):
    _isolated(
        """
        import os
        import mssql_python as db
        from mssql_python import ddbc_bindings as native
        with db.connect(os.environ["DB_CONNECTION_STRING"]) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "DECLARE @s NVARCHAR(300) = N'SELECT id AS [' + "
                    "CAST(0x00D8 AS NVARCHAR(1)) + "
                    "N'] FROM (VALUES(1),(2),(3)) s(id) ORDER BY id'; EXEC(@s)"
                )
                # The low-level row path never decoded a supported column's name.
                row = []
                assert native.DDBCSQLFetchOne(cursor.hstmt, row) in (0, 1)
                assert row == [1]
                for _ in range(2):
                    try:
                        native.DDBCSQLFetchMany(cursor.hstmt, [], 1)
                    except UnicodeDecodeError:
                        pass
                    else:
                        raise AssertionError("many accepted the malformed column name")
                row = []
                profiling = hasattr(native, "profiling")
                if profiling:
                    native.profiling.reset()
                    native.profiling.enable()
                try:
                    assert native.DDBCSQLFetchOne(cursor.hstmt, row) in (0, 1)
                finally:
                    if profiling:
                        native.profiling.disable()
                assert row == [2]
                if profiling:
                    assert native.profiling.get_stats()["ddbc::SQLDescribeCol::driver_call"]["calls"] == 1
                cursor.execute("SELECT 4 AS valid_name, N'recovered' AS text_value")
                assert tuple(cursor.fetchone()) == (4, "recovered")
                assert cursor.fetchmany() == []
        """,
        tmp_path,
    )


@pytest.mark.skipif(
    not hasattr(ddbc_bindings, "profiling"), reason="requires actual ODBC call instrumentation"
)
@pytest.mark.parametrize("method", ["one", "many"])
def test_result_metadata_actual_description_counts(tmp_path, method):
    _isolated(
        f"""
        import os
        from decimal import Decimal
        import mssql_python as db
        from mssql_python import ddbc_bindings as native
        with db.connect(os.environ["DB_CONNECTION_STRING"]) as connection:
            with connection.cursor() as cursor:
                columns = ",".join(f"id AS c{{i}}" for i in range(24))
                query = ("WITH n AS (SELECT TOP(10000) ROW_NUMBER() OVER "
                         "(ORDER BY a.object_id,b.object_id) AS id "
                         "FROM sys.all_objects a CROSS JOIN sys.all_objects b) "
                         f"SELECT {{columns}} FROM n ORDER BY id")
                cursor.execute(query)
                native.profiling.reset()
                native.profiling.enable()
                try:
                    for value in range(1, 10001):
                        row = cursor.fetchone() if {method!r} == "one" else cursor.fetchmany()[0]
                        assert tuple(row) == (value,) * 24
                    assert cursor.fetchone() is None
                    assert cursor.fetchmany() == []
                finally:
                    native.profiling.disable()
                stats = native.profiling.get_stats()
                assert stats["ddbc::SQLDescribeCol::driver_call"]["calls"] == 24, stats
                assert stats.get("ddbc::SQLDescribeCol_wrap", {{}}).get("calls", 0) == 0
                native.profiling.reset()
                native.profiling.enable()
                try:
                    metadata = []
                    assert native.DDBCSQLDescribeCol(cursor.hstmt, metadata) in (0, 1)
                finally:
                    native.profiling.disable()
                assert len(metadata) == 24
                assert native.profiling.get_stats()["ddbc::SQLDescribeCol::driver_call"]["calls"] == 24
                cursor.execute(
                    "SELECT CAST(3 AS INT) AS changed, CAST(N'x' AS NVARCHAR(1)) AS text_value; "
                    "SELECT CAST(7.25 AS DECIMAL(8,2)) AS amount, "
                    "CAST(N'next long value' AS NVARCHAR(40)) AS name"
                )
                assert tuple(cursor.fetchone()) == (3, "x")
                assert cursor.nextset()
                native.profiling.reset()
                native.profiling.enable()
                try:
                    assert tuple(cursor.fetchmany()[0]) == (Decimal("7.25"), "next long value")
                    assert cursor.fetchmany() == []
                finally:
                    native.profiling.disable()
                assert native.profiling.get_stats()["ddbc::SQLDescribeCol::driver_call"]["calls"] == 2
        """,
        tmp_path,
    )


@pytest.mark.skipif(
    not hasattr(ddbc_bindings, "profiling"), reason="requires actual ODBC call instrumentation"
)
@pytest.mark.parametrize("method", ["one", "many", "all"])
def test_result_metadata_variant_descriptions_remain_per_row(tmp_path, method):
    _isolated(
        f"""
        import os
        import mssql_python as db
        from mssql_python import ddbc_bindings as native
        with db.connect(os.environ["DB_CONNECTION_STRING"]) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT id, v FROM (VALUES "
                    "(1,CAST(NULL AS SQL_VARIANT)),"
                    "(2,CAST('abc' AS SQL_VARIANT)),"
                    "(3,CAST(17 AS SQL_VARIANT))) s(id,v) ORDER BY id"
                )
                native.profiling.reset()
                native.profiling.enable()
                try:
                    if {method!r} == "one":
                        rows = list(cursor)
                    elif {method!r} == "all":
                        rows = cursor.fetchall()
                    else:
                        rows = []
                        while batch := cursor.fetchmany():
                            rows.extend(batch)
                finally:
                    native.profiling.disable()
                assert [tuple(row) for row in rows] == [(1,None),(2,"abc"),(3,17)]
                stats = native.profiling.get_stats()
                expected = 4 if {method!r} == "one" else 5
                assert stats["ddbc::SQLDescribeCol::driver_call"]["calls"] == expected, stats
                assert stats["ddbc::sql_variant::null_probe"]["calls"] == 3, stats
                assert stats["ddbc::sql_variant::subtype"]["calls"] == 2, stats
        """,
        tmp_path,
    )


@pytest.mark.parametrize("method", ["one", "many", "all"])
def test_result_metadata_all_null_rows(tmp_path, method):
    _isolated(
        f"""
        import os
        import mssql_python as db
        from mssql_python import ddbc_bindings as native
        with db.connect(os.environ["DB_CONNECTION_STRING"]) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT CAST(NULL AS INT) AS scalar_null")
                scalar_null = []
                scalar_status = native.DDBCSQLFetchOne(cursor.hstmt, scalar_null)
                diagnostics = native.DDBCSQLGetAllDiagRecords(cursor.hstmt)
                assert scalar_status == 0, (scalar_status, diagnostics)
                assert scalar_null == [None]
                assert diagnostics == []
                values = ",".join(f"({{i}})" for i in range(1, 16))
                cursor.execute(
                    "SELECT CASE WHEN id%7=0 THEN NULL ELSE id END AS c0,"
                    "CASE WHEN id%7=0 THEN CAST(NULL AS SQL_VARIANT) "
                    "WHEN id%3=0 THEN CAST(id AS SQL_VARIANT) "
                    "WHEN id%3=1 THEN CAST(N'row-'+CONVERT(NVARCHAR(12),id) AS SQL_VARIANT) "
                    "ELSE CAST(CONVERT(FLOAT,id)*0.25 AS SQL_VARIANT) END AS c1 "
                    f"FROM (VALUES{{values}}) s(id) ORDER BY id"
                )
                profiling = hasattr(native, "profiling")
                if profiling:
                    native.profiling.reset()
                    native.profiling.enable()
                try:
                    if {method!r} == "one":
                        rows = list(cursor)
                    elif {method!r} == "all":
                        rows = cursor.fetchall()
                    else:
                        rows = []
                        while batch := cursor.fetchmany():
                            rows.extend(batch)
                finally:
                    if profiling:
                        native.profiling.disable()
                expected = [
                    (None,None) if i%7==0 else (i,(i,f"row-{{i}}",i*0.25)[i%3])
                    for i in range(1,16)
                ]
                assert [tuple(row) for row in rows] == expected
                assert [[type(value) for value in row] for row in rows] == [
                    [type(value) for value in row] for row in expected
                ]
                if profiling:
                    stats = native.profiling.get_stats()
                    expected_describes = 16 if {method!r} == "one" else 17
                    assert stats["ddbc::SQLDescribeCol::driver_call"]["calls"] == expected_describes, stats
                    assert stats["ddbc::sql_variant::null_probe"]["calls"] == 15
                    assert stats["ddbc::sql_variant::subtype"]["calls"] == 13
        """,
        tmp_path,
    )


def test_result_metadata_odbc_error_invalidates(tmp_path):
    _isolated(
        """
        import os
        import mssql_python as db
        from mssql_python import ddbc_bindings as native
        with db.connect(os.environ["DB_CONNECTION_STRING"]) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT id FROM (VALUES(1),(2),(3)) s(id) ORDER BY id")
                row = []
                assert native.DDBCSQLFetchOne(cursor.hstmt, row) in (0, 1)
                assert row == [1]
                assert native.DDBCSQLGetData(
                    cursor.hstmt, 2, [], "utf-16le", "utf-16le", db.SQL_WCHAR
                ) == -1
                diagnostics = native.DDBCSQLGetAllDiagRecords(cursor.hstmt)
                assert any("07009" in state for state, _ in diagnostics), diagnostics
                profiling = hasattr(native, "profiling")
                if profiling:
                    native.profiling.reset()
                    native.profiling.enable()
                try:
                    row = []
                    assert native.DDBCSQLFetchOne(cursor.hstmt, row) in (0, 1)
                    assert row == [2]
                finally:
                    if profiling:
                        native.profiling.disable()
                if profiling:
                    assert native.profiling.get_stats()["ddbc::SQLDescribeCol::driver_call"]["calls"] == 1
        """,
        tmp_path,
    )
