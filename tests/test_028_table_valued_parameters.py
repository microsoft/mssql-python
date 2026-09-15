"""
Integration coverage for SQL Server table-valued parameters.
"""

import datetime
import os
import subprocess
import sys
import uuid
from decimal import Decimal

import pytest

from mssql_python import (
    DatabaseError,
    NotSupportedError,
    ProgrammingError,
    SQL_INTEGER,
    SQL_WVARCHAR,
    ddbc_bindings,
)
from mssql_python.odbc_provider import ProviderManager
from test_023_ssh_tunnel_gil_release import (
    WATCHDOG_SECONDS,
    _parse_server,
    _replace_server,
    _start_forwarder,
)


@pytest.fixture
def tvp_procedure(cursor, db_connection):
    suffix = uuid.uuid4().hex
    type_name = f"pytest_tvp_type_{suffix}"
    wildcard_match_type_name = type_name.replace("_", "X")
    procedure_name = f"pytest_tvp_proc_{suffix}"

    try:
        cursor.execute(
            f"CREATE TYPE dbo.[{type_name}] AS TABLE " "(id int NOT NULL, label nvarchar(100) NULL)"
        )
        cursor.execute(
            f"CREATE TYPE dbo.[{wildcard_match_type_name}] AS TABLE " "(decoy_id bigint NOT NULL)"
        )
        cursor.execute(f"""
            CREATE PROCEDURE dbo.[{procedure_name}]
                @prefix int,
                @items dbo.[{type_name}] READONLY,
                @suffix nvarchar(20)
            AS
            BEGIN
                SET NOCOUNT ON;
                SELECT id + @prefix, label + @suffix
                FROM @items
                ORDER BY id;
            END
            """)
        db_connection.commit()
        yield type_name, procedure_name
    finally:
        cursor.execute(f"DROP PROCEDURE IF EXISTS dbo.[{procedure_name}]")
        cursor.execute(f"DROP TYPE IF EXISTS dbo.[{type_name}]")
        cursor.execute(f"DROP TYPE IF EXISTS dbo.[{wildcard_match_type_name}]")
        db_connection.commit()


@pytest.fixture
def typed_tvp_procedure(cursor, db_connection):
    suffix = uuid.uuid4().hex
    type_name = f"pytest_typed_tvp_type_{suffix}"
    procedure_name = f"pytest_typed_tvp_proc_{suffix}"

    try:
        cursor.execute(f"""
            CREATE TYPE dbo.[{type_name}] AS TABLE (
                bit_value bit NULL,
                tiny_value tinyint NULL,
                small_value smallint NULL,
                int_value int NULL,
                big_value bigint NULL,
                float_value float NULL,
                decimal_value decimal(20, 6) NULL,
                text_value nvarchar(100) NULL,
                binary_value varbinary(100) NULL,
                date_value date NULL,
                time_value time(6) NULL,
                datetime_value datetime2(6) NULL,
                offset_value datetimeoffset(6) NULL,
                guid_value uniqueidentifier NULL,
                all_null_binary varbinary(16) NULL
            )
            """)
        cursor.execute(f"""
            CREATE PROCEDURE dbo.[{procedure_name}]
                @items dbo.[{type_name}] READONLY
            AS
            BEGIN
                SET NOCOUNT ON;
                SELECT *
                FROM @items
                ORDER BY int_value;
            END
            """)
        db_connection.commit()
        yield procedure_name
    finally:
        cursor.execute(f"DROP PROCEDURE IF EXISTS dbo.[{procedure_name}]")
        cursor.execute(f"DROP TYPE IF EXISTS dbo.[{type_name}]")
        db_connection.commit()


@pytest.fixture
def multiple_tvp_procedure(cursor, db_connection):
    suffix = uuid.uuid4().hex
    type_name = f"pytest_multi_tvp_type_{suffix}"
    procedure_name = f"pytest_multi_tvp_proc_{suffix}"

    try:
        cursor.execute(f"CREATE TYPE dbo.[{type_name}] AS TABLE (id int NOT NULL)")
        cursor.execute(f"""
            CREATE PROCEDURE dbo.[{procedure_name}]
                @left_rows dbo.[{type_name}] READONLY,
                @offset int,
                @right_rows dbo.[{type_name}] READONLY
            AS
            BEGIN
                SET NOCOUNT ON;
                SELECT side, id + @offset
                FROM (
                    SELECT 'left' AS side, id FROM @left_rows
                    UNION ALL
                    SELECT 'right', id FROM @right_rows
                ) AS combined
                ORDER BY side, id;
            END
            """)
        db_connection.commit()
        yield procedure_name
    finally:
        cursor.execute(f"DROP PROCEDURE IF EXISTS dbo.[{procedure_name}]")
        cursor.execute(f"DROP TYPE IF EXISTS dbo.[{type_name}]")
        db_connection.commit()


@pytest.fixture
def schema_qualified_large_tvp(cursor, db_connection):
    suffix = uuid.uuid4().hex
    schema_name = f"pytest_tvp_schema_{suffix}"
    type_name = f"pytest_large_tvp_type_{suffix}"
    procedure_name = f"pytest_large_tvp_proc_{suffix}"

    try:
        cursor.execute(f"CREATE SCHEMA [{schema_name}]")
        cursor.execute(f"""
            CREATE TYPE [{schema_name}].[{type_name}] AS TABLE (
                text_value nvarchar(max) NULL,
                binary_value varbinary(max) NULL
            )
            """)
        cursor.execute(f"""
            CREATE PROCEDURE [{schema_name}].[{procedure_name}]
                @items [{schema_name}].[{type_name}] READONLY
            AS
            BEGIN
                SET NOCOUNT ON;
                SELECT text_value, binary_value FROM @items;
            END
            """)
        db_connection.commit()
        yield schema_name, procedure_name
    finally:
        cursor.execute(f"DROP PROCEDURE IF EXISTS [{schema_name}].[{procedure_name}]")
        cursor.execute(f"DROP TYPE IF EXISTS [{schema_name}].[{type_name}]")
        cursor.execute(f"DROP SCHEMA IF EXISTS [{schema_name}]")
        db_connection.commit()


def test_tvp_positional_named_empty_and_statement_reuse(cursor, tvp_procedure):
    _, procedure_name = tvp_procedure
    sql = f"EXEC dbo.[{procedure_name}] ?, ?, ?"

    cursor.execute(sql, (10, [(1, "Keyboard"), (2, None)], "!"))
    assert [tuple(row) for row in cursor.fetchall()] == [(11, "Keyboard!"), (12, None)]

    cursor.execute(sql, (10, [], "!"))
    assert cursor.fetchall() == []

    cursor.execute(sql, (None, [(3, "Nullable")], ""))
    assert tuple(cursor.fetchone()) == (None, "Nullable")

    cursor.execute(
        f"EXEC dbo.[{procedure_name}] %(prefix)s, %(items)s, %(suffix)s",
        {"prefix": 0, "items": [(4, "Mouse")], "suffix": "?"},
    )
    assert tuple(cursor.fetchone()) == (4, "Mouse?")


def test_tvp_empty_first_and_all_null_column(cursor, tvp_procedure):
    _, procedure_name = tvp_procedure
    sql = f"EXEC dbo.[{procedure_name}] ?, ?, ?"

    cursor.execute(sql, (0, [], ""))
    assert cursor.fetchall() == []

    cursor.execute(sql, (0, [(1, None), (2, None)], ""))
    assert [tuple(row) for row in cursor.fetchall()] == [(1, None), (2, None)]


def test_tvp_binds_supported_cell_types(cursor, typed_tvp_procedure):
    first_guid = uuid.uuid4()
    second_guid = uuid.uuid4()
    offset = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
    rows = [
        (
            True,
            255,
            -32768,
            -2147483648,
            -(2**63),
            1.25,
            Decimal("-12345678901234.567890"),
            "Grüße 😀",
            b"\x00\xff",
            datetime.date(2026, 9, 15),
            datetime.time(12, 34, 56, 123456),
            datetime.datetime(2026, 9, 15, 12, 34, 56, 123456),
            datetime.datetime(2026, 9, 15, 12, 34, 56, 123456, tzinfo=offset),
            first_guid,
            None,
        ),
        (
            False,
            0,
            32767,
            2147483647,
            2**63 - 1,
            -2.5,
            Decimal("12.345"),
            "later row is longer",
            b"\x01\x02\x03",
            datetime.date(2026, 9, 16),
            datetime.time(1, 2, 3, 4),
            datetime.datetime(2026, 9, 16, 1, 2, 3, 4),
            datetime.datetime(2026, 9, 16, 1, 2, 3, 4, tzinfo=datetime.timezone.utc),
            second_guid,
            None,
        ),
    ]

    cursor.execute(f"EXEC dbo.[{typed_tvp_procedure}] ?", (rows,))
    actual = [tuple(row) for row in cursor.fetchall()]

    assert actual[0][:9] == rows[0][:9]
    assert actual[1][:9] == rows[1][:9]
    assert actual[0][9:13] == rows[0][9:13]
    assert actual[1][9:13] == rows[1][9:13]
    assert str(actual[0][13]) == str(first_guid)
    assert str(actual[1][13]) == str(second_guid)
    assert actual[0][14] is None
    assert actual[1][14] is None

    invalid_row = list(rows[0])
    invalid_row[6] = Decimal("NaN")
    with pytest.raises(ValueError, match="non-finite Decimal"):
        cursor.execute(
            f"EXEC dbo.[{typed_tvp_procedure}] ?",
            ([tuple(invalid_row)],),
        )


def test_multiple_tvps_with_interleaved_scalar(cursor, multiple_tvp_procedure):
    cursor.execute(
        f"EXEC dbo.[{multiple_tvp_procedure}] ?, ?, ?",
        ([(1,), (2,)], 10, [(3,), (4,)]),
    )
    assert [tuple(row) for row in cursor.fetchall()] == [
        ("left", 11),
        ("left", 12),
        ("right", 13),
        ("right", 14),
    ]


def test_schema_qualified_tvp_with_large_and_embedded_null_values(
    cursor, schema_qualified_large_tvp
):
    schema_name, procedure_name = schema_qualified_large_tvp
    large_text = "prefix\x00suffix" + "😀" * 2500
    large_binary = b"\x00\xff" * 4501
    rows = ((large_text, large_binary), (None, None))

    cursor.execute(f"EXEC [{schema_name}].[{procedure_name}] ?", (rows,))
    actual = [tuple(row) for row in cursor.fetchall()]

    assert actual == [(large_text, large_binary), (None, None)]


def test_failed_tvp_does_not_poison_cursor(cursor, tvp_procedure):
    _, procedure_name = tvp_procedure
    sql = f"EXEC dbo.[{procedure_name}] ?, ?, ?"

    with pytest.raises(DatabaseError):
        cursor.execute(sql, (0, [(None, "invalid")], ""))

    cursor.execute("SELECT ?", 42)
    assert cursor.fetchone()[0] == 42

    cursor.execute(sql, (0, [(1, "valid")], ""))
    assert tuple(cursor.fetchone()) == (1, "valid")


def test_tvp_rejects_invalid_shape_and_cells(cursor, tvp_procedure):
    _, procedure_name = tvp_procedure
    sql = f"EXEC dbo.[{procedure_name}] ?, ?, ?"

    with pytest.raises(ValueError, match="same number"):
        cursor.execute(sql, (0, [(1, "valid"), (2,)], ""))
    with pytest.raises(TypeError, match="incompatible Python types"):
        cursor.execute(sql, (0, [(1, "valid"), ("2", "invalid")], ""))
    with pytest.raises(TypeError, match="only be bound"):
        cursor.execute("SELECT CAST(? AS int)", ([(1,)],))


def test_tvp_rejected_by_executemany_before_first_write(cursor, tvp_procedure):
    cursor.execute("CREATE TABLE #pytest_tvp_executemany (id int)")

    with pytest.raises(NotSupportedError):
        cursor.executemany(
            "INSERT INTO #pytest_tvp_executemany VALUES (?)",
            [(1,), ([(2, "label")],)],
        )

    cursor.execute("SELECT COUNT(*) FROM #pytest_tvp_executemany")
    assert cursor.fetchone()[0] == 0


def test_tvp_rejected_with_setinputsizes(cursor, tvp_procedure):
    _, procedure_name = tvp_procedure
    cursor.setinputsizes(
        [
            (SQL_INTEGER, 10, 0),
            (SQL_WVARCHAR, 100, 0),
            (SQL_WVARCHAR, 20, 0),
        ]
    )
    try:
        with pytest.raises(NotSupportedError):
            cursor.execute(
                f"EXEC dbo.[{procedure_name}] ?, ?, ?",
                (0, [(1, "label")], ""),
            )
    finally:
        cursor.setinputsizes(None)


def test_tvp_rejected_by_mssql_odbc_provider(cursor, tvp_procedure, monkeypatch):
    _, procedure_name = tvp_procedure
    monkeypatch.setattr(ProviderManager, "effective", lambda *_: "mssql-odbc")

    with pytest.raises(NotSupportedError, match="mssql-odbc provider"):
        cursor.execute(
            f"EXEC dbo.[{procedure_name}] ?, ?, ?",
            (0, [(1, "label")], ""),
        )


def test_tvp_metadata_handle_inherits_query_timeout(db_connection, tvp_procedure, monkeypatch):
    _, procedure_name = tvp_procedure
    real_set_stmt_attr = ddbc_bindings.DDBCSQLSetStmtAttr
    timeout_handles = []

    def record_timeout(statement_handle, attribute, value):
        timeout_handles.append(statement_handle)
        return real_set_stmt_attr(statement_handle, attribute, value)

    db_connection.timeout = 2
    monkeypatch.setattr(
        ddbc_bindings,
        "DDBCSQLSetStmtAttr",
        record_timeout,
    )
    try:
        with db_connection.cursor() as timeout_cursor:
            timeout_cursor.execute(
                f"EXEC dbo.[{procedure_name}] ?, ?, ?",
                (0, [(1, "label")], ""),
            )
            assert tuple(timeout_cursor.fetchone()) == (1, "label")
    finally:
        db_connection.timeout = 0

    assert len(timeout_handles) == 2
    assert timeout_handles[0] is not timeout_handles[1]


@pytest.fixture
def conversion_tvp(cursor, db_connection, request):
    suffix = uuid.uuid4().hex
    type_name = f"pytest_conversion_type_{suffix}"
    procedure_name = f"pytest_conversion_proc_{suffix}"
    try:
        cursor.execute(
            f"CREATE TYPE dbo.[{type_name}] AS TABLE (position int, value {request.param} NULL)"
        )
        cursor.execute(
            f"CREATE PROCEDURE dbo.[{procedure_name}] @items dbo.[{type_name}] READONLY AS "
            "SET NOCOUNT ON; SELECT value FROM @items ORDER BY position"
        )
        db_connection.commit()
        yield f"EXEC dbo.[{procedure_name}] ?"
    finally:
        cursor.execute(f"DROP PROCEDURE IF EXISTS dbo.[{procedure_name}]")
        cursor.execute(f"DROP TYPE IF EXISTS dbo.[{type_name}]")
        db_connection.commit()


@pytest.mark.parametrize(
    ("conversion_tvp", "values", "expected"),
    [
        (
            "float",
            [Decimal("1.5"), Decimal("123.45"), None, Decimal("1.50"), Decimal("-0.125")],
            [1.5, 123.45, None, 1.5, -0.125],
        ),
        ("int", [Decimal("1.5"), Decimal("-123.45"), None], [1, -123, None]),
        ("nvarchar(40)", [Decimal("1.5"), None], ["1.5", None]),
        (
            "nvarchar(40)",
            [Decimal("1.5"), Decimal("123.45"), None, Decimal("-0.125")],
            ["1.500", "123.450", None, "-0.125"],
        ),
        (
            "decimal(5,2)",
            [Decimal("1.239"), None, Decimal("-1.235"), Decimal("9.999")],
            [Decimal("1.24"), None, Decimal("-1.24"), Decimal("10.00")],
        ),
        (
            "decimal(8,2)",
            ["000000000000000001.23", None, "-0000000000000001.239"],
            [Decimal("1.23"), None, Decimal("-1.24")],
        ),
        (
            "datetime",
            [datetime.datetime(2026, 9, 15, 12, 34, 56, 456789), None],
            [datetime.datetime(2026, 9, 15, 12, 34, 56, 457000), None],
        ),
        (
            "datetime2(3)",
            [datetime.datetime(2026, 9, 15, 23, 59, 59, 999999), None],
            [datetime.datetime(2026, 9, 16), None],
        ),
        (
            "time(3)",
            [datetime.time(12, 34, 56, 456789), None],
            [datetime.time(12, 34, 56, 457000), None],
        ),
        (
            "datetimeoffset(3)",
            [
                datetime.datetime(2026, 9, 15, 23, 59, 59, 999999, tzinfo=datetime.timezone.utc),
                None,
            ],
            [datetime.datetime(2026, 9, 16, tzinfo=datetime.timezone.utc), None],
        ),
    ],
    indirect=["conversion_tvp"],
)
def test_tvp_source_precision_survives_destination_conversion(
    cursor, conversion_tvp, values, expected
):
    rows = list(enumerate(values))
    for _ in range(2):
        cursor.execute(conversion_tvp, (rows,))
        assert [row[0] for row in cursor.fetchall()] == expected
        assert rows == list(enumerate(values))


@pytest.mark.parametrize(
    ("conversion_tvp", "value", "recovery"),
    [
        ("nvarchar(3)", "x" * 5000, "ok"),
        ("nvarchar(3)", "\u0100" * 5000, "ok"),
        ("varchar(8000)", "x" * 8001, "ok"),
        ("varchar(8000) COLLATE Latin1_General_100_CI_AS", "\u00e9" * 8001, "ok"),
        ("varbinary(8000)", b"x" * 8001, b"ok"),
    ],
    ids=["short-ascii", "short-unicode", "long-ascii", "long-unicode", "long-binary"],
    indirect=["conversion_tvp"],
)
def test_tvp_destination_limits_leave_cursor_reusable(cursor, conversion_tvp, value, recovery):
    with pytest.raises(ProgrammingError, match="would be truncated") as raised:
        cursor.execute(conversion_tvp, ([(0, value)],))
    assert raised.value.driver_error == "Syntax error or access violation"
    cursor.execute("SELECT ?", 42)
    assert cursor.fetchone()[0] == 42
    cursor.execute(conversion_tvp, ([(0, recovery)],))
    assert cursor.fetchone()[0] == recovery


@pytest.mark.parametrize("conversion_tvp", ["decimal(38,0)"], indirect=True)
@pytest.mark.parametrize(
    "value", [Decimal("NaN"), Decimal("Infinity"), Decimal("1E+39"), Decimal("1E-39")]
)
def test_tvp_decimal_validation_matches_scalar(cursor, conversion_tvp, value):
    with pytest.raises(ValueError):
        cursor.execute(conversion_tvp, ([(0, value)],))
    cursor.execute(conversion_tvp, ([(0, Decimal("1E+37")), (1, None)],))
    assert [row[0] for row in cursor.fetchall()] == [Decimal("1E+37"), None]


@pytest.mark.parametrize("conversion_tvp", ["bigint"], indirect=True)
def test_tvp_integer_widening_and_nulls(cursor, conversion_tvp):
    edges = [0, 255, -32768, 32767, -(2**31), 2**31 - 1, -(2**63), 2**63 - 1]
    for ordered in (edges, list(reversed(edges))):
        values = [None, *ordered, None]
        cursor.execute(conversion_tvp, (list(enumerate(values)),))
        assert [row[0] for row in cursor.fetchall()] == values


@pytest.mark.parametrize(
    ("conversion_tvp", "values"),
    [
        ("nvarchar(max)", [None, "ascii first", "\U0001f600" * 4001, "a\x00b", ""]),
        ("nvarchar(max)", ["\U0001f600", "later ascii", None]),
        ("varchar(max)", [None, "x" * 8001, "a\x00b", ""]),
        ("varbinary(max)", [b"", None, b"\x00\xff" * 4501, bytearray(b"last")]),
    ],
    indirect=["conversion_tvp"],
)
def test_tvp_source_buffers_cover_every_row(cursor, conversion_tvp, values):
    cursor.execute(conversion_tvp, (list(enumerate(values)),))
    assert [row[0] for row in cursor.fetchall()] == values


@pytest.mark.parametrize(
    "conversion_tvp", ["varchar(8000) COLLATE Latin1_General_100_CI_AS"], indirect=True
)
@pytest.mark.parametrize("length", [4000, 4001, 8000])
def test_tvp_unicode_source_fits_bounded_varchar(cursor, conversion_tvp, length):
    values = [None, "ascii first", "\u00e9" * length, ""]
    for ordered in (values, list(reversed(values))):
        cursor.execute(conversion_tvp, (list(enumerate(ordered)),))
        assert [row[0] for row in cursor.fetchall()] == ordered


def test_tvp_all_null_columns_use_declared_types(cursor, typed_tvp_procedure):
    cursor.execute(f"EXEC dbo.[{typed_tvp_procedure}] ?", ([tuple([None] * 15)],))
    assert tuple(cursor.fetchone()) == tuple([None] * 15)


@pytest.mark.parametrize("conversion_tvp", ["float"], indirect=True)
def test_tvp_rejects_unrepresentable_common_decimal_precision(cursor, conversion_tvp):
    with pytest.raises(ValueError, match="precision greater than 38"):
        cursor.execute(conversion_tvp, ([(0, Decimal("1E+37")), (1, Decimal("0.1"))],))
    cursor.execute(conversion_tvp, ([(0, Decimal("0.1"))],))
    assert cursor.fetchone()[0] == 0.1


@pytest.mark.parametrize("conversion_tvp", ["decimal(5,2)"], indirect=True)
def test_tvp_decimal_wire_format_is_independent_of_text_encoding(conn_str, conversion_tvp):
    from mssql_python import SQL_CHAR, connect

    with connect(conn_str) as connection:
        connection.setencoding("utf-16le", ctype=SQL_CHAR)
        with connection.cursor() as encoded_cursor:
            encoded_cursor.execute(conversion_tvp, ([(0, Decimal("-1.239")), (1, None)],))
            assert [row[0] for row in encoded_cursor.fetchall()] == [Decimal("-1.24"), None]


@pytest.mark.parametrize("conversion_tvp", ["nvarchar(40)"], indirect=True)
@pytest.mark.parametrize(
    ("input_sizes", "error", "message"),
    [
        (None, RuntimeError, "metadata requires a valid statement handle"),
        ([(SQL_INTEGER, 10, 0)], TypeError, "setinputsizes cannot override"),
    ],
)
def test_tvp_native_entry_point_validates_metadata_and_overrides(
    db_connection, conversion_tvp, input_sizes, error, message
):
    with db_connection.cursor() as native_cursor:
        with pytest.raises(error, match=message):
            ddbc_bindings.DDBCSQLExecute(
                native_cursor.hstmt,
                None,
                conversion_tvp,
                [[(0, "ok")]],
                input_sizes,
                [False],
                True,
                {},
            )
        native_cursor.execute(conversion_tvp, ([(0, "ok")],))
        assert native_cursor.fetchone()[0] == "ok"


@pytest.mark.parametrize(
    ("rows", "error", "message"),
    [
        ([1], TypeError, "rows must be list or tuple"),
        ([()], ValueError, "at least one column"),
        ([(1,)], ValueError, "declared table type requires"),
        ([(1, "ok"), 2], TypeError, "rows must be list or tuple"),
        ([(1, [2])], TypeError, "nested row sequences"),
        ([(1, Decimal("1")), (2, "2")], TypeError, "incompatible Python types"),
    ],
)
def test_tvp_shape_and_type_errors_leave_cursor_reusable(
    cursor, tvp_procedure, rows, error, message
):
    _, procedure_name = tvp_procedure
    sql = f"EXEC dbo.[{procedure_name}] ?, ?, ?"
    with pytest.raises(error, match=message):
        cursor.execute(sql, (0, rows, ""))
    cursor.execute(sql, (0, [(1, "ok")], ""))
    assert tuple(cursor.fetchone()) == (1, "ok")


@pytest.fixture
def wide_tvp_procedure(cursor, db_connection):
    suffix = uuid.uuid4().hex
    type_name = f"pytest_wide_tvp_type_{suffix}"
    procedure_name = f"pytest_wide_tvp_proc_{suffix}"
    # Long column names make metadata span multiple network packets.
    columns = ", ".join(["id int"] + [f"c{i}_{'x' * 100} int" for i in range(1, 1024)])
    try:
        cursor.execute(f"CREATE TYPE dbo.[{type_name}] AS TABLE ({columns})")
        cursor.execute(
            f"CREATE PROCEDURE dbo.[{procedure_name}] @items dbo.[{type_name}] READONLY AS "
            "SET NOCOUNT ON; SELECT id FROM @items ORDER BY id"
        )
        db_connection.commit()
        yield procedure_name
    finally:
        cursor.execute(f"DROP PROCEDURE IF EXISTS dbo.[{procedure_name}]")
        cursor.execute(f"DROP TYPE IF EXISTS dbo.[{type_name}]")
        db_connection.commit()


def _run_forwarded_tvp():
    import mssql_python

    base = os.environ["DB_CONNECTION_STRING"]
    target = _parse_server(base)
    assert target is not None, "Could not parse Server=host,port"
    host, port = _start_forwarder(target)
    mssql_python.pooling(enabled=False)
    with mssql_python.connect(_replace_server(base, host, port)) as connection:
        with connection.cursor() as cursor:
            sql = f"EXEC dbo.[{os.environ['TVP_GIL_PROCEDURE']}] ?"
            padding = (None,) * 1023
            if os.environ["TVP_GIL_METADATA_ERROR"] == "1":
                import ctypes

                library = ctypes.CDLL(sys.modules["ddbc_bindings"].__file__)
                slot = ctypes.c_void_p.in_dll(library, "SQLGetData_ptr")
                original = slot.value
                get_data_type = ctypes.CFUNCTYPE(
                    ctypes.c_short,
                    ctypes.c_void_p,
                    ctypes.c_ushort,
                    ctypes.c_short,
                    ctypes.c_void_p,
                    ctypes.c_ssize_t,
                    ctypes.POINTER(ctypes.c_ssize_t),
                )
                get_data = get_data_type(original)

                @get_data_type
                def invalid_metadata_column(handle, column, ctype, value, size, indicator):
                    # Produce a real driver error before metadata is drained. Do not
                    # wrap SQLFreeStmt: its original GIL behavior is what we exercise.
                    slot.value = original
                    return get_data(handle, 999, ctype, value, size, indicator)

                slot.value = ctypes.cast(invalid_metadata_column, ctypes.c_void_p).value
                try:
                    with pytest.raises(ProgrammingError, match="(?i)invalid descriptor index"):
                        cursor.execute(sql, ([(1,) + padding],))
                finally:
                    slot.value = original
                cursor.execute("SELECT ?", 42)
                assert [tuple(row) for row in cursor.fetchall()] == [(42,)]

            # No pointer overrides on the normal path, including after error recovery.
            for ids in ([1, 2], [3], []):
                cursor.execute(sql, ([(i,) + padding for i in ids],))
                assert [tuple(row) for row in cursor.fetchall()] == [(i,) for i in ids]
    print("OK forwarded TVP metadata", flush=True)


@pytest.mark.parametrize(
    "metadata_error",
    [
        pytest.param(False, id="metadata-reads"),
        pytest.param(
            True,
            id="metadata-error-cleanup",
            marks=pytest.mark.skipif(
                sys.platform == "win32", reason="Native function-pointer injection is Unix-only"
            ),
        ),
    ],
)
def test_tvp_metadata_through_python_forwarder_does_not_deadlock(
    conn_str, wide_tvp_procedure, metadata_error
):
    if not conn_str or _parse_server(conn_str) is None:
        pytest.skip("Requires DB_CONNECTION_STRING with Server=host,port")
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(sys.path)
    env["TVP_GIL_PROCEDURE"] = wide_tvp_procedure
    env["TVP_GIL_METADATA_ERROR"] = str(int(metadata_error))
    try:
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "from test_028_table_valued_parameters import _run_forwarded_tvp; "
                "_run_forwarded_tvp()",
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=WATCHDOG_SECONDS,
        )
    except subprocess.TimeoutExpired:
        pytest.fail(
            f"TVP metadata through the Python forwarder deadlocked after {WATCHDOG_SECONDS}s"
        )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "OK forwarded TVP metadata" in result.stdout
