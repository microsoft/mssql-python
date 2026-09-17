"""Call-local fetch metadata must preserve public descriptions and fetch state."""

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
