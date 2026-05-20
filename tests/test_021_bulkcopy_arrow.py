# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for the Arrow-input bulkcopy path (Cursor.bulkcopy_arrow)."""

import os

import pytest

mssql_py_core = pytest.importorskip(
    "mssql_py_core", reason="mssql_py_core not loadable (glibc too old?)"
)
pa = pytest.importorskip("pyarrow")


# ── unit-style tests: do not require a live database ──────────────────────────


def test_pycore_cursor_exposes_bulkcopy_arrow():
    assert hasattr(mssql_py_core.PyCoreCursor, "bulkcopy_arrow")


def test_cursor_class_exposes_bulkcopy_arrow():
    # Importing the package may pull in ddbc_bindings; skip if not built.
    pytest.importorskip("mssql_python.ddbc_bindings", exc_type=ImportError)
    from mssql_python.cursor import Cursor

    assert callable(getattr(Cursor, "bulkcopy_arrow", None))
    assert callable(getattr(Cursor, "_looks_like_arrow_source", None))


def test_looks_like_arrow_source_detection():
    pytest.importorskip("mssql_python.ddbc_bindings", exc_type=ImportError)
    from mssql_python.cursor import Cursor

    assert Cursor._looks_like_arrow_source(pa.table({"a": [1, 2, 3]})) is True
    assert (
        Cursor._looks_like_arrow_source(
            pa.record_batch([pa.array([1, 2])], names=["a"])
        )
        is True
    )
    reader = pa.RecordBatchReader.from_batches(
        pa.schema([("a", pa.int32())]),
        [pa.record_batch([pa.array([1], type=pa.int32())], names=["a"])],
    )
    assert Cursor._looks_like_arrow_source(reader) is True
    assert Cursor._looks_like_arrow_source([(1,), (2,)]) is False
    assert Cursor._looks_like_arrow_source(None) is False
    assert Cursor._looks_like_arrow_source("abc") is False


# ── live-DB integration tests ────────────────────────────────────────────────

LIVE_DB = pytest.mark.skipif(
    not os.getenv("DB_CONNECTION_STRING"),
    reason="DB_CONNECTION_STRING not set",
)


@LIVE_DB
def test_bulkcopy_arrow_table(cursor):
    """Round-trip a small pyarrow.Table via bulkcopy_arrow."""
    table_name = "mssql_python_test_bulkcopy_arrow"

    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"CREATE TABLE {table_name} (id INT NOT NULL, name NVARCHAR(50) NULL, score FLOAT NULL)"
    )
    cursor.connection.commit()

    arrow_table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int32()),
            "name": pa.array(["Alice", None, "Charlie"]),
            "score": pa.array([1.5, 2.5, None], type=pa.float64()),
        }
    )

    result = cursor.bulkcopy_arrow(table_name, arrow_table)
    assert result["rows_copied"] == 3

    cursor.execute(f"SELECT id, name, score FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert [r[0] for r in rows] == [1, 2, 3]
    assert rows[1][1] is None
    assert rows[2][2] is None

    cursor.execute(f"DROP TABLE {table_name}")


def test_bulkcopy_rejects_arrow_input():
    """`bulkcopy(...)` should refuse Arrow inputs and direct callers to bulkcopy_arrow."""
    from mssql_python.cursor import Cursor

    arrow_table = pa.table({"id": pa.array([10, 20, 30], type=pa.int32())})

    # We don't need a live DB or a real cursor for this — bulkcopy validates
    # the input shape before touching the connection.
    sentinel = Cursor.__new__(Cursor)
    with pytest.raises(TypeError, match="bulkcopy_arrow"):
        Cursor.bulkcopy(sentinel, "any_table", arrow_table)


@LIVE_DB
def test_bulkcopy_arrow_record_batch_reader(cursor):
    table_name = "mssql_python_test_bulkcopy_arrow_reader"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT NOT NULL, txt NVARCHAR(20) NULL)")
    cursor.connection.commit()

    schema = pa.schema([("id", pa.int32()), ("txt", pa.string())])
    batches = [
        pa.record_batch([pa.array([1, 2], type=pa.int32()), pa.array(["a", "b"])], schema=schema),
        pa.record_batch([pa.array([3], type=pa.int32()), pa.array([None])], schema=schema),
    ]
    reader = pa.RecordBatchReader.from_batches(schema, batches)

    result = cursor.bulkcopy_arrow(table_name, reader)
    assert result["rows_copied"] == 3

    cursor.execute(f"DROP TABLE {table_name}")
