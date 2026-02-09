"""
Tests for Arrow (pyarrow) integration with the Cursor class.

These tests require pyarrow to be installed; they are skipped otherwise.
"""

import pytest
import decimal
import io
from datetime import datetime, date, time, timezone

import mssql_python

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    pa = None
    pq = None

# Skip the entire module if pyarrow is not available
pytestmark = pytest.mark.skipif(pa is None, reason="pyarrow is not installed")


def get_arrow_test_data(include_lobs: bool, batch_length: int):
    arrow_test_data = [
        (pa.uint8(), "tinyint", [1, 2, None, 4, 5, 0, 2**8 - 1]),
        (pa.int16(), "smallint", [1, 2, None, 4, 5, -(2**15), 2**15 - 1]),
        (pa.int32(), "int", [1, 2, None, 4, 5, 0, -(2**31), 2**31 - 1]),
        (pa.int64(), "bigint", [1, 2, None, 4, 5, 0, -(2**63), 2**63 - 1]),
        (pa.float64(), "float", [1.0, 2.5, None, 4.25, 5.125]),
        (pa.float32(), "real", [1.0, 2.5, None, 4.25, 5.125]),
        (
            pa.decimal128(precision=10, scale=2),
            "decimal(10, 2)",
            [
                decimal.Decimal("1.23"),
                None,
                decimal.Decimal("0.25"),
                decimal.Decimal("-99999999.99"),
                decimal.Decimal("99999999.99"),
            ],
        ),
        (
            pa.decimal128(precision=38, scale=10),
            "decimal(38, 10)",
            [
                decimal.Decimal("1.1234567890"),
                None,
                decimal.Decimal("0"),
                decimal.Decimal("1.0000000001"),
                decimal.Decimal("-9999999999999999999999999999.9999999999"),
                decimal.Decimal("9999999999999999999999999999.9999999999"),
            ],
        ),
        (
            pa.decimal128(precision=38, scale=0),
            "decimal(38, 0)",
            [
                decimal.Decimal(str(2**63)),
                decimal.Decimal(str(-(2**63))),
                decimal.Decimal(str(2**64)),
                decimal.Decimal(str(-(2**64))),
                decimal.Decimal(str(2**64 - 1)),
                decimal.Decimal(str(-(2**64 - 1))),
                decimal.Decimal(str(2**64 + 1)),
                decimal.Decimal(str(-(2**64 + 1))),
                decimal.Decimal(str(2**96)),
                decimal.Decimal(str(-(2**96))),
            ],
        ),
        (pa.bool_(), "bit", [True, None, False]),
        (pa.large_binary(), "binary(9)", [b"asdfghjkl", None, b"lkjhgfdsa"]),
        (pa.large_string(), "varchar(100)", ["asdfghjkl", None, "lkjhgfdsa"]),
        (pa.large_string(), "nvarchar(100)", ["asdfghjkl", None, "lkjhgfdsa"]),
        (pa.large_string(), "uniqueidentifier", ["58185E0D-3A91-44D8-BC46-7107217E0A6D", None]),
        (
            pa.date32(),
            "date",
            [
                date(1, 1, 1),
                None,
                date(2345, 12, 31),
                date(9999, 12, 31),
                date(1970, 1, 1),
                date(1969, 12, 31),
                date(2000, 2, 29),
                date(2001, 2, 28),
            ],
        ),
        (
            pa.time32("s"),
            "time(0)",
            [time(12, 0, 5, 0), None, time(23, 59, 59, 0), time(0, 0, 0, 0)],
        ),
        (
            pa.time32("s"),
            "time(7)",
            [time(12, 0, 5, 0), None, time(23, 59, 59, 0), time(0, 0, 0, 0)],
        ),
        (
            pa.timestamp("us"),
            "datetime2(0)",
            [datetime(2025, 1, 1, 12, 0, 5, 0), None, datetime(2345, 12, 31, 23, 59, 59, 0)],
        ),
        (
            pa.timestamp("us"),
            "datetime2(3)",
            [datetime(2025, 1, 1, 12, 0, 5, 123_000), None, datetime(2345, 12, 31, 23, 59, 59, 0)],
        ),
        (
            pa.timestamp("us"),
            "datetime2(6)",
            [datetime(2025, 1, 1, 12, 0, 5, 123_456), None, datetime(2345, 12, 31, 23, 59, 59, 0)],
        ),
        (
            pa.timestamp("us"),
            "datetime2(7)",
            [datetime(2025, 1, 1, 12, 0, 5, 123_456), None, datetime(2145, 12, 31, 23, 59, 59, 0)],
        ),
        (
            pa.timestamp("us"),
            "datetime2(2)",
            [datetime(2025, 1, 1, 12, 0, 5, 0), None, datetime(2145, 12, 31, 23, 59, 59, 0)],
        ),
    ]

    if include_lobs:
        arrow_test_data += [
            (pa.large_string(), "nvarchar(max)", ["hey", None, "ho"]),
            (pa.large_string(), "varchar(max)", ["hey", None, "ho"]),
            (pa.large_binary(), "varbinary(max)", [b"hey", None, b"ho"]),
        ]

    for ix in range(len(arrow_test_data)):
        while True:
            T, sql_type, vals = arrow_test_data[ix]
            if len(vals) >= batch_length:
                arrow_test_data[ix] = (T, sql_type, vals[:batch_length])
                break
            arrow_test_data[ix] = (T, sql_type, vals + vals)

    return arrow_test_data


def _test_arrow_test_data(cursor: mssql_python.Cursor, arrow_test_data, fetch_length=500):
    cols = []
    for i_col, (pa_type, sql_type, values) in enumerate(arrow_test_data):
        rows = []
        for value in values:
            if type(value) is bool:
                value = int(value)
            if type(value) is bytes:
                value = value.decode()
            if value is None:
                value = "null"
            else:
                value = f"'{value}'"
            rows.append(f"col_{i_col} = cast({value} as {sql_type})")
        cols.append(rows)

    selects = []
    for row in zip(*cols):
        selects.append(f"select {', '.join(col for col in row)}")
    full_query = "\nunion all\n".join(selects)
    ret = cursor.execute(full_query).arrow_batch(fetch_length)
    for i_col, col in enumerate(ret):
        expected_data = arrow_test_data[i_col][2][:fetch_length]
        for i_row, (v_expected, v_actual) in enumerate(
            zip(expected_data, col.to_pylist(), strict=True)
        ):
            assert (
                v_expected == v_actual
            ), f"Mismatch in column {i_col}, row {i_row}: expected {v_expected}, got {v_actual}"
        # check that null counts match
        expected_null_count = sum(1 for v in expected_data if v is None)
        actual_null_count = col.null_count
        assert expected_null_count == actual_null_count, (expected_null_count, actual_null_count)
    for i_col, (pa_type, sql_type, values) in enumerate(arrow_test_data):
        field = ret.schema.field(i_col)
        assert (
            field.name == f"col_{i_col}"
        ), f"Column {i_col} name mismatch: expected col_{i_col}, got {field.name}"
        assert field.type.equals(
            pa_type
        ), f"Column {i_col} type mismatch: expected {pa_type}, got {field.type}"

    # Validate that Parquet serialization/deserialization does not detect any issues
    tbl = pa.Table.from_batches([ret])
    # for some reason parquet converts seconds to milliseconds in time32
    for i_col, col in enumerate(tbl.columns):
        if col.type == pa.time32("s"):
            tbl = tbl.set_column(
                i_col,
                tbl.schema.field(i_col).name,
                col.cast(pa.time32("ms")),
            )
    buffer = io.BytesIO()
    pq.write_table(tbl, buffer)
    buffer.seek(0)
    read_table = pq.read_table(buffer)
    assert read_table.equals(tbl)


def test_arrow_lob_wide(cursor: mssql_python.Cursor):
    "Take the SQLGetData branch for a wide table."
    arrow_test_data = get_arrow_test_data(include_lobs=True, batch_length=123)
    _test_arrow_test_data(cursor, arrow_test_data)


def test_arrow_nolob_wide(cursor: mssql_python.Cursor):
    "Test the SQLBindData branch for a wide table."
    arrow_test_data = get_arrow_test_data(include_lobs=False, batch_length=123)
    _test_arrow_test_data(cursor, arrow_test_data)


def test_arrow_single_column(cursor: mssql_python.Cursor):
    "Test each datatype as a single column fetch."
    arrow_test_data = get_arrow_test_data(include_lobs=True, batch_length=123)
    for col_data in arrow_test_data:
        _test_arrow_test_data(cursor, [col_data])


def test_arrow_empty_fetch(cursor: mssql_python.Cursor):
    "Test each datatype as a single column fetch of length 0."
    arrow_test_data = get_arrow_test_data(include_lobs=True, batch_length=123)
    for col_data in arrow_test_data:
        _test_arrow_test_data(cursor, [col_data], fetch_length=0)


def test_arrow_table_batchsize_negative(cursor: mssql_python.Cursor):
    tbl = cursor.execute("select 1 a").arrow(batch_size=-42)
    assert type(tbl) is pa.Table
    assert tbl.num_rows == 0
    assert tbl.num_columns == 1
    assert cursor.fetchone()[0] == 1


def test_arrow_empty_result_set(cursor: mssql_python.Cursor):
    "Test fetching from an empty result set."
    cursor.execute("select 1 where 1 = 0")
    batch = cursor.arrow_batch(10)
    assert batch.num_rows == 0
    assert batch.num_columns == 1
    cursor.execute("select cast(N'' as nvarchar(max)) where 1 = 0")
    batch = cursor.arrow_batch(10)
    assert batch.num_rows == 0
    assert batch.num_columns == 1
    cursor.execute("select 1, cast(N'' as nvarchar(max)) where 1 = 0")
    batch = cursor.arrow_batch(10)
    assert batch.num_rows == 0
    assert batch.num_columns == 2


def test_arrow_no_result_set(cursor: mssql_python.Cursor):
    "Test fetching when there is no result set."
    cursor.execute("declare @a int")
    with pytest.raises(Exception, match=".*No active result set.*"):
        cursor.arrow_batch(10)


def test_arrow_datetimeoffset(cursor: mssql_python.Cursor):
    "Datetimeoffset converts correctly to utc"
    for force_sqlgetdata in (False, True):
        str_val = "cast('asdf' as nvarchar(max))" if force_sqlgetdata else "'asdf'"
        cursor.execute(
            "declare @dt datetimeoffset(0) = '2345-02-03 12:34:56 +00:00';\n"
            f"select {str_val}, @dt, @dt at time zone 'Pacific Standard Time';\n"
        )
        batch = cursor.arrow_batch(10)
        assert batch.num_rows == 1
        assert batch.num_columns == 3
        for col in batch.columns[1:]:
            assert pa.types.is_timestamp(col.type)
            assert col.type.tz == "+00:00", col.type.tz
            assert col.to_pylist() == [
                datetime(2345, 2, 3, 12, 34, 56, tzinfo=timezone.utc),
            ]


def test_arrow_schema_nullable(cursor: mssql_python.Cursor):
    "Test that the schema is nullable."
    cursor.execute("select 1 a, null b")
    batch = cursor.arrow_batch(10)
    assert batch.num_rows == 1
    assert batch.num_columns == 2
    assert not batch.schema.field(0).nullable
    assert batch.schema.field(1).nullable
    assert batch.schema.field(0).name == "a"
    assert batch.schema.field(1).name == "b"


def test_arrow_table(cursor: mssql_python.Cursor):
    tbl = cursor.execute("select top 11 1 a from sys.objects").arrow(batch_size=5)
    assert type(tbl) is pa.Table
    assert tbl.num_rows == 11
    assert tbl.num_columns == 1
    assert [len(b) for b in tbl.to_batches()] == [5, 5, 1]


def test_arrow_reader(cursor: mssql_python.Cursor):
    reader = cursor.execute("select top 11 1 a from sys.objects").arrow_reader(batch_size=4)
    assert type(reader) is pa.RecordBatchReader
    batches = list(reader)
    assert [len(b) for b in batches] == [4, 4, 3]
    assert sum(len(b) for b in batches) == 11


def test_arrow_long_string(cursor: mssql_python.Cursor):
    "Make sure resizing the data buffer works"
    long_string = "A" * 100000  # 100k characters
    cursor.execute("select cast(? as nvarchar(max))", (long_string,))
    batch = cursor.arrow_batch(10)
    assert batch.num_rows == 1
    assert batch.num_columns == 1
    assert batch.column(0).to_pylist() == [long_string]


def test_rownumber_arrow_batch_interleaved_fetchmany(cursor: mssql_python.Cursor):
    """Verify that arrow_batch and fetchmany can be interleaved
    on the same result set with correct rownumber tracking and values."""
    N = 20
    unions = " union all ".join(f"select {i} as val" for i in range(1, N + 1))
    cursor.execute(f"select val from ({unions}) t order by val")

    collected = []

    batch = cursor.arrow_batch(3)
    assert batch.num_rows == 3
    vals = batch.column(0).to_pylist()
    assert vals == [1, 2, 3]
    assert cursor.rownumber == 2  # 0-based: last row index = 2
    collected.extend(vals)

    rows = cursor.fetchmany(5)
    assert len(rows) == 5
    vals = [r[0] for r in rows]
    assert vals == [4, 5, 6, 7, 8]
    assert cursor.rownumber == 7
    collected.extend(vals)

    batch = cursor.arrow_batch(4)
    assert batch.num_rows == 4
    vals = batch.column(0).to_pylist()
    assert vals == [9, 10, 11, 12]
    assert cursor.rownumber == 11
    collected.extend(vals)

    batch = cursor.arrow_batch(100)
    assert batch.num_rows == 8
    vals = batch.column(0).to_pylist()
    assert vals == [13, 14, 15, 16, 17, 18, 19, 20]
    assert cursor.rownumber == 19
    collected.extend(vals)

    assert collected == list(range(1, N + 1))
