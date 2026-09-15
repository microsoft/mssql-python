"""
Integration coverage for SQL Server table-valued parameters.
"""

import datetime
import uuid
from decimal import Decimal

import pytest

from mssql_python import (
    DatabaseError,
    NotSupportedError,
    SQL_INTEGER,
    SQL_WVARCHAR,
    ddbc_bindings,
)
from mssql_python.odbc_provider import ProviderManager


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
