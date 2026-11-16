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
            pa.time64("ns"),
            "time(0)",
            [time(12, 0, 5, 0), None, time(23, 59, 59, 0), time(0, 0, 0, 0)],
        ),
        (
            pa.time64("ns"),
            "time(7)",
            [time(12, 0, 5, 0), None, time(23, 59, 59, 0), time(0, 0, 0, 0)],
        ),
        (
            pa.time64("ns"),
            "time(7)",
            [time(12, 0, 5, 123456), None, time(23, 59, 59, 123456), time(0, 0, 0, 0)],
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
    # arrow_reader returns a RecordBatchReader-compatible wrapper (not the
    # raw pyarrow.RecordBatchReader) so that .close() can actually stop
    # fetching and release the server-side cursor.  Verify duck-typed
    # compatibility instead of exact identity.
    assert hasattr(reader, "schema")
    assert hasattr(reader, "read_next_batch")
    assert hasattr(reader, "close")
    batches = list(reader)
    assert [len(b) for b in batches] == [4, 4, 3]
    assert sum(len(b) for b in batches) == 11


def test_arrow_reader_read_all_returns_table(cursor: mssql_python.Cursor):
    """Regression: ``reader.read_all()`` is the idiomatic pyarrow way to
    drain a reader into a Table and must work through the wrapper.  Was
    silently missing when the class hand-enumerated the pyarrow surface;
    covered here to lock the ``__getattr__`` delegation contract in place.
    """
    reader = cursor.execute("select top 11 1 a from sys.objects").arrow_reader(batch_size=4)
    tbl = reader.read_all()
    assert type(tbl) is pa.Table
    assert tbl.num_rows == 11
    assert tbl.num_columns == 1
    # After a successful drain the reader is empty; close() must still be
    # safe (idempotency + generator finally).
    reader.close()
    assert reader.closed is True
    # Parent cursor stays usable.
    cursor.execute("select 42")
    assert cursor.fetchone()[0] == 42


def test_arrow_reader_read_pandas_returns_dataframe(cursor: mssql_python.Cursor):
    """Regression: ``reader.read_pandas()`` must delegate to the inner
    reader and return a pandas ``DataFrame``.  Skipped if pandas isn't
    importable in the test env.
    """
    pd = pytest.importorskip("pandas")
    reader = cursor.execute("select top 7 1 a from sys.objects").arrow_reader(batch_size=3)
    df = reader.read_pandas()
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 7
    assert list(df.columns) == ["a"]
    reader.close()


def test_arrow_reader_cast_delegates(cursor: mssql_python.Cursor):
    """Regression: ``reader.cast(target_schema)`` must delegate to the
    inner reader.  ``cast`` returns a new pyarrow reader that projects the
    stream to a different schema — we're not exercising the projection
    logic itself, only that the delegation path exists and returns
    something reader-shaped.
    """
    reader = cursor.execute("select top 5 1 a from sys.objects").arrow_reader(batch_size=2)
    # Same schema — the cast should succeed (identity projection).
    casted = reader.cast(reader.schema)
    assert casted is not None
    # ``cast`` returns a real pyarrow RecordBatchReader (not another wrapper),
    # which is fine — the consumer just wants a reader-like object.
    assert hasattr(casted, "read_next_batch")
    reader.close()


def test_arrow_reader_delegates_unknown_pyarrow_method(cursor: mssql_python.Cursor):
    """Guard against future regression: any public method the inner
    pyarrow reader exposes must be reachable through the wrapper without
    the class having to enumerate it explicitly.  Probes ``schema`` (a
    property that used to be hand-written) and ``read_next_batch`` (a
    method that used to be hand-written) via ``__getattr__``.
    """
    reader = cursor.execute("select top 3 1 a from sys.objects").arrow_reader(batch_size=2)
    # schema now goes through __getattr__ delegation.
    schema = reader.schema
    assert schema.field(0).name == "a"
    # read_next_batch also goes through __getattr__ (except when reached
    # via the iteration protocol, which uses __next__).
    batch = reader.read_next_batch()
    assert batch.num_rows > 0
    reader.close()


def test_arrow_reader_delegated_method_raises_after_close(cursor: mssql_python.Cursor):
    """Post-close access via ``__getattr__`` must raise ``ArrowInvalid``
    to match the explicit ``__next__`` / ``__arrow_c_stream__`` semantics —
    a delegated call must not silently succeed on a reader the user has
    already closed.
    """
    reader = cursor.execute("select top 5 1 a from sys.objects").arrow_reader(batch_size=2)
    reader.close()
    with pytest.raises(pa.ArrowInvalid):
        reader.read_all()
    with pytest.raises(pa.ArrowInvalid):
        _ = reader.schema


def test_arrow_reader_pycapsule_protocol(cursor: mssql_python.Cursor):
    """The wrapper implements the Arrow PyCapsule Protocol via
    ``__arrow_c_stream__``, so Arrow-aware consumers can accept it
    without an ``isinstance(x, pa.RecordBatchReader)`` check.

    Regression guard for the alternative to subclassing
    ``pyarrow.RecordBatchReader`` (which is a Cython extension type and
    can't carry our extra state).
    """
    if not hasattr(pa.RecordBatchReader, "from_stream"):
        pytest.skip("pyarrow>=15 required for RecordBatchReader.from_stream")

    reader = cursor.execute("select top 11 1 a from sys.objects").arrow_reader(batch_size=4)
    assert hasattr(reader, "__arrow_c_stream__")
    assert callable(reader.__arrow_c_stream__)

    # Consume the wrapper via the PyCapsule Protocol.  ``from_stream`` calls
    # ``__arrow_c_stream__`` internally, which transfers the C stream out of
    # the inner reader — after this the wrapper is effectively drained but
    # still safely closeable.
    native = pa.RecordBatchReader.from_stream(reader)
    assert isinstance(native, pa.RecordBatchReader)
    total_rows = sum(b.num_rows for b in native)
    assert total_rows == 11

    # Wrapper close() must still be safe after the stream has been
    # transferred out (idempotent + generator finally handles teardown).
    reader.close()
    assert reader.closed is True

    # Parent cursor remains usable.
    cursor.execute("select 42")
    assert cursor.fetchone()[0] == 42


def test_arrow_reader_pycapsule_protocol_raises_after_close(cursor: mssql_python.Cursor):
    """``__arrow_c_stream__`` must refuse export after ``close()`` — matches
    the ``read_next_batch()`` / ``schema`` post-close semantics."""
    reader = cursor.execute("select top 5 1 a from sys.objects").arrow_reader(batch_size=2)
    reader.close()
    with pytest.raises(pa.ArrowInvalid):
        reader.__arrow_c_stream__()


def test_arrow_reader_close_semantics(cursor: mssql_python.Cursor):
    """``reader.close()`` must stop fetching, mark the reader closed, leave
    the parent Cursor usable, be idempotent, and work as a context manager."""
    reader = cursor.execute("select top 1000 1 a from sys.objects o1, sys.objects o2").arrow_reader(
        batch_size=10
    )

    # Drain one batch then close mid-iteration.
    first = reader.read_next_batch()
    assert first.num_rows > 0
    assert reader.closed is False

    reader.close()
    assert reader.closed is True

    # Further reads raise (pyarrow.ArrowInvalid expected).
    with pytest.raises(pa.ArrowInvalid):
        reader.read_next_batch()
    with pytest.raises(pa.ArrowInvalid):
        next(iter(reader))

    # close() is idempotent.
    reader.close()
    reader.close()

    # Parent cursor must still be usable after the reader was closed.
    cursor.execute("select 42")
    row = cursor.fetchone()
    assert row[0] == 42


def test_arrow_reader_context_manager(cursor: mssql_python.Cursor):
    """Using the reader as a context manager closes it on exit."""
    with cursor.execute("select top 5 1 a from sys.objects").arrow_reader(batch_size=2) as reader:
        assert reader.closed is False
        _ = reader.read_next_batch()
    assert reader.closed is True
    # Cursor remains usable.
    cursor.execute("select 7")
    assert cursor.fetchone()[0] == 7


def test_arrow_reader_gc_cleanup(cursor: mssql_python.Cursor):
    """Dropping the reader without calling close() must still release the
    server-side cursor — the try/finally in the batch generator runs on GC."""
    import gc

    reader = cursor.execute("select top 100 1 a from sys.objects").arrow_reader(batch_size=10)
    _ = reader.read_next_batch()  # partial consume

    # Drop the only strong reference and force collection. The generator's
    # finally block must run, releasing the cursor so the next execute()
    # succeeds without ProgrammingError("connection busy") etc.
    del reader
    gc.collect()

    cursor.execute("select 5")
    assert cursor.fetchone()[0] == 5


@pytest.mark.stress  # timing fuzz supplements the deterministic cancellation-boundary tests
def test_arrow_reader_cancel_from_other_thread():
    _run_arrow_probe("_arrow_stress_close_probe")


def _arrow_stress_close_probe():
    import os

    with mssql_python.connect(os.environ["DB_CONNECTION_STRING"]) as connection:
        with connection.cursor() as cursor:
            _arrow_cancel_from_other_thread(cursor)


def _arrow_cancel_from_other_thread(cursor):
    """Concurrent close must stop streaming and leave the parent Cursor reusable."""
    import threading
    import time

    # Big enough cross-join that streaming will not finish in <100ms.
    reader = cursor.execute(
        "select top 1000000 1 a from sys.objects o1, sys.objects o2, sys.objects o3"
    ).arrow_reader(batch_size=64)

    closer_done = threading.Event()
    closer_exc = []

    def closer():
        try:
            time.sleep(0.05)  # exercise varying close/fetch interleavings
            reader.close()
        except Exception as e:  # pragma: no cover - reported to main thread
            closer_exc.append(e)
        finally:
            closer_done.set()

    t = threading.Thread(target=closer, daemon=True)
    t.start()

    # Iterate; the cancel from the other thread must terminate the loop
    # (either by exhausting cleanly or by raising) within a couple seconds.
    rows = 0
    try:
        for batch in reader:
            rows += batch.num_rows
            if rows > 2_000_000:  # safety net — should never reach this
                pytest.fail("reader was not cancelled by the other thread")
    except pa.ArrowInvalid:
        pass  # acceptable: reader was closed mid-iteration
    except mssql_python.OperationalError as e:
        # Acceptable: SQLCancel landed while the driver was inside the fetch,
        # so the fetch returned SQLSTATE HY008 (ODBC-specified behaviour).
        # HY008 is the only SQLSTATE mapped to driver_error "Operation canceled";
        # any other OperationalError is a real failure.
        if getattr(e, "driver_error", None) != "Operation canceled":
            raise

    closer_done.wait(timeout=5)
    t.join(timeout=5)
    # Fail loudly if the closer thread did not actually finish — otherwise a
    # deadlock in close() would silently masquerade as a downstream failure
    # (or, worse, hang the interpreter while the daemon thread holds the
    # HSTMT).
    assert (
        closer_done.is_set()
    ), "closer thread did not signal completion within 5s — close() may be deadlocked"
    assert (
        not t.is_alive()
    ), "closer thread is still alive after join(timeout=5) — close() may be deadlocked"
    assert not closer_exc, f"closer thread raised: {closer_exc[0]!r}"
    assert reader.closed is True

    # Parent cursor must still work after the cross-thread cancel.
    cursor.execute("select 99")
    assert cursor.fetchone()[0] == 99


@pytest.mark.parametrize("consumer", ["iteration", "delegated", "c_stream"])
@pytest.mark.parametrize("fetch_error", [False, True])
def test_arrow_reader_close_waits_for_batch(consumer, fetch_error):
    if consumer == "c_stream" and not hasattr(pa.RecordBatchReader, "from_stream"):
        pytest.skip("RecordBatchReader.from_stream requires pyarrow>=15")
    _run_arrow_probe("_arrow_close_waits_probe", consumer, str(int(fetch_error)))


def _arrow_close_waits_probe(consumer, fetch_error):
    import os

    with mssql_python.connect(os.environ["DB_CONNECTION_STRING"]) as connection:
        with connection.cursor() as cursor, pytest.MonkeyPatch.context() as monkeypatch:
            _arrow_close_waits_for_batch(cursor, monkeypatch, consumer, bool(int(fetch_error)))


def _arrow_close_waits_for_batch(cursor, monkeypatch, consumer, fetch_error):
    """Close must fence a batch already admitted by any Arrow consumption path."""
    import threading

    reader = cursor.execute("select top 10 1 a from sys.objects").arrow_reader(batch_size=128)
    stream = pa.RecordBatchReader.from_stream(reader) if consumer == "c_stream" else reader
    read = next if consumer == "iteration" else lambda r: r.read_next_batch()
    entered = threading.Event()
    release = threading.Event()
    requested = threading.Event()
    close_done = threading.Event()
    errors = []
    close_errors = []
    fetch = cursor._fetch_arrow_batch
    state = reader._close_requested

    class ObservedClose:
        def request_close(self):
            state.request_close()
            requested.set()

    reader._close_requested = ObservedClose()

    def paused_fetch(*args):
        entered.set()
        assert release.wait(5), "batch was not released"
        if fetch_error:
            raise mssql_python.ProgrammingError("unrelated fetch failure", "synthetic diagnostic")
        return fetch(*args)

    monkeypatch.setattr(cursor, "_fetch_arrow_batch", paused_fetch)

    def consume():
        try:
            read(stream)
        except StopIteration:
            pass
        except Exception as error:
            errors.append(error)

    def close():
        try:
            reader.close()
        except Exception as error:
            close_errors.append(error)
        finally:
            close_done.set()

    worker = threading.Thread(target=consume, daemon=True)
    closer = threading.Thread(target=close, daemon=True)
    worker.start()
    try:
        assert entered.wait(5), "consumer did not enter the batch"
        closer.start()
        assert requested.wait(5), "close request was not published"
        assert not close_done.is_set(), "close returned before the active batch completed"
    finally:
        release.set()
        worker.join(5)
        if closer.ident is not None:
            closer.join(5)
        reader.close()
        if consumer == "c_stream":
            stream.close()

    assert not worker.is_alive() and not closer.is_alive()
    assert not close_errors
    if fetch_error:
        assert len(errors) == 1
        # Arrow's C stream translates the Python exception to ArrowInvalid.
        assert "unrelated fetch failure" in str(errors[0])
        if consumer != "c_stream":
            assert isinstance(errors[0], mssql_python.ProgrammingError)
    else:
        assert not errors
    assert reader.closed
    cursor.execute("select 99")
    assert cursor.fetchone()[0] == 99


def test_arrow_reader_idle_close_request_does_not_cancel(cursor):
    """An idle request must not close the statement before iterator cleanup."""
    from mssql_python import ddbc_bindings

    reader = cursor.execute("select top 10 1 a from sys.objects").arrow_reader(batch_size=2)
    try:
        assert next(reader).num_rows == 2
        reader._close_requested.request_close()
        assert ddbc_bindings.DDBCSQLNumResultCols(cursor.hstmt) == 1
        with pytest.raises(StopIteration):
            reader.read_next_batch()
    finally:
        reader.close()
    cursor.execute("select 99")
    assert cursor.fetchone()[0] == 99


def test_arrow_reader_close_before_first_batch_releases_cursor(cursor, monkeypatch):
    """The generator's unstarted state must not skip server-side cleanup."""
    reader = cursor.execute("select 1 a").arrow_reader(batch_size=2)
    handle_type = type(cursor.hstmt)
    close_cursor = handle_type._close_cursor
    calls = []

    def observed_close(handle):
        calls.append(handle)
        close_cursor(handle)

    monkeypatch.setattr(handle_type, "_close_cursor", observed_close)
    reader.close()
    reader.close()
    assert calls == [cursor.hstmt]
    assert cursor.rowcount == -1
    cursor.execute("select 99")
    assert cursor.fetchone()[0] == 99


@pytest.mark.parametrize("started", [False, True])
@pytest.mark.parametrize("action", ["close", "gc", "read"])
def test_arrow_reader_does_not_touch_reexecuted_result(cursor, started, action):
    import gc

    statement = "select ? as value"
    reader = cursor.execute(statement, 1).arrow_reader(batch_size=1)
    if started:
        assert next(reader).column(0).to_pylist() == [1]
    cursor.execute(statement, 2)
    rowcount = cursor.rowcount
    if action == "gc":
        del reader
        gc.collect()
    else:
        if action == "read":
            with pytest.raises(StopIteration):
                reader.read_next_batch()
        reader.close()
    assert cursor.rowcount == rowcount
    assert cursor.fetchone()[0] == 2


def test_arrow_reader_does_not_close_next_result(cursor):
    reader = cursor.execute("select 1 as value; select 2 as value").arrow_reader(batch_size=1)
    assert cursor.nextset()
    reader.close()
    assert cursor.fetchone()[0] == 2


@pytest.mark.parametrize("action", ["close", "read"])
def test_arrow_reader_ignores_replacement_statement(cursor, monkeypatch, action):
    reader = cursor.execute("select 1 as value").arrow_reader()
    with pytest.raises(mssql_python.ProgrammingError):
        cursor.execute("select * from #arrow_missing_table")
    handle_type = type(cursor.hstmt)
    calls = []
    close_cursor = handle_type._close_cursor

    def observed_close(handle):
        calls.append(handle)
        close_cursor(handle)

    monkeypatch.setattr(handle_type, "_close_cursor", observed_close)
    try:
        if action == "read":
            with pytest.raises(StopIteration):
                next(reader)
    finally:
        reader.close()
    assert calls == [], "stale reader closed the replacement statement"
    cursor.execute("select 2 as value")
    assert cursor.fetchone()[0] == 2


def test_arrow_reader_preserves_reexecuted_update_rowcount(cursor):
    reader = cursor.execute("select 1 as value").arrow_reader()
    cursor.execute("declare @rows table (value int); update @rows set value = 2")
    assert cursor.rowcount == 0
    reader.close()
    assert cursor.rowcount == 0


def test_arrow_reader_reentrant_close():
    _run_arrow_probe("_arrow_reentrant_close_probe", timeout=10)


def _run_arrow_probe(name, *args, timeout=30):
    import subprocess
    import sys
    from pathlib import Path

    code = "import runpy, sys; runpy.run_path(sys.argv[1])[sys.argv[2]](*sys.argv[3:])"
    result = subprocess.run(
        [sys.executable, "-c", code, str(Path(__file__).resolve()), name, *args],
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    assert result.returncode == 0, (result.stdout, result.stderr)


def _arrow_reentrant_close_probe():
    import os
    import signal

    with mssql_python.connect(os.environ["DB_CONNECTION_STRING"]) as connection:
        with connection.cursor() as cursor:
            reader = cursor.execute("select 1 as value").arrow_reader(batch_size=1)
            fetch = cursor._fetch_arrow_batch
            previous = signal.signal(signal.SIGINT, lambda *_: reader.close())

            def interrupted_fetch(*args):
                signal.raise_signal(signal.SIGINT)
                return fetch(*args)

            cursor._fetch_arrow_batch = interrupted_fetch
            try:
                with pytest.raises(StopIteration):
                    next(reader)
            finally:
                signal.signal(signal.SIGINT, previous)
                cursor._fetch_arrow_batch = fetch
            assert reader.closed
            assert reader._generator.gi_frame is None
            assert reader._cursor is None
            assert cursor.rowcount == -1
            cursor.execute("select 2 as value")
            reader.close()
            assert cursor.fetchone()[0] == 2


@pytest.mark.parametrize(
    "outcome",
    [
        "partial_cancel",
        "partial_cancel_warning_first",
        "closing_cancel",
        "closing_hy007",
        "closing_hy010",
        "closing_24000",
        "closing_error",
        "closing_mixed",
        "closing_mixed_cancel_first",
    ],
)
def test_arrow_reader_native_cancel_boundary(outcome):
    """Interpose native ODBC calls in an isolated process, never shared fixtures."""
    _require_arrow_interposition()
    _run_arrow_probe("_arrow_native_cancel_probe", outcome)


@pytest.mark.parametrize("requested", [False, True])
def test_arrow_interposition_requires_compiled_controls(monkeypatch, requested):
    import ctypes
    from mssql_python import ddbc_bindings

    monkeypatch.delattr(ddbc_bindings.module, "_arrow_test_pointer_addresses", raising=False)
    if requested:
        monkeypatch.setenv("MSSQL_ARROW_TEST_INTERPOSITION", "1")
    else:
        monkeypatch.delenv("MSSQL_ARROW_TEST_INTERPOSITION", raising=False)

    def unexpected_library_lookup(*args):
        pytest.fail("Exported ODBC globals must not bypass compiled test controls")

    monkeypatch.setattr(ctypes, "CDLL", unexpected_library_lookup)
    expected = pytest.fail.Exception if requested else pytest.skip.Exception
    with pytest.raises(expected, match="native test controls"):
        _require_arrow_interposition()


def _require_arrow_interposition():
    import os
    from mssql_python import ddbc_bindings

    if not hasattr(ddbc_bindings.module, "_arrow_test_pointer_addresses"):
        if os.environ.get("MSSQL_ARROW_TEST_INTERPOSITION", "").upper() in ("1", "ON"):
            pytest.fail("Arrow interposition was requested but native test controls are missing")
        pytest.skip("Requires compiled Arrow native test controls")


def _arrow_odbc_pointers():
    import ctypes
    from mssql_python import ddbc_bindings

    addresses = ddbc_bindings.module._arrow_test_pointer_addresses
    names = ("SQLFetch_ptr", "SQLCancel_ptr", "SQLGetDiagRec_ptr")
    return {name: ctypes.c_void_p.from_address(addresses[name]) for name in names}


@pytest.mark.parametrize("cancel_result", ["driver", "success_with_info"])
def test_arrow_reader_cancels_blocked_driver_fetch(cancel_result):
    _require_arrow_interposition()
    _run_arrow_probe("_arrow_blocked_driver_fetch_probe", cancel_result, timeout=45)


def _arrow_blocked_driver_fetch_probe(cancel_result):
    import ctypes
    import os
    import threading
    import time
    import uuid

    callback = ctypes.WINFUNCTYPE if os.name == "nt" else ctypes.CFUNCTYPE
    odbc_call = callback(ctypes.c_short, ctypes.c_void_p)
    pointers = _arrow_odbc_pointers()
    active = threading.Event()
    cancelled = threading.Event()
    finished = threading.Event()
    errors = []
    cancel_results = []
    fetch_calls = []

    @odbc_call
    def fetch(handle):
        if threading.get_ident() != worker.ident:
            return real_fetch(handle)
        fetch_calls.append(handle)
        active.set()
        try:
            return real_fetch(handle)
        finally:
            active.clear()

    @odbc_call
    def cancel(handle):
        result = real_cancel(handle)
        if cancel_result == "success_with_info" and result in (0, 1):
            result = 1
        cancel_results.append(result)
        cancelled.set()
        return result

    connection_string = os.environ["DB_CONNECTION_STRING"]
    with (
        mssql_python.connect(connection_string, autocommit=True) as locker,
        mssql_python.connect(connection_string, autocommit=True) as connection,
    ):
        with locker.cursor() as control, connection.cursor() as cursor:
            original = {name: pointer.value for name, pointer in pointers.items()}
            real_fetch = odbc_call(original["SQLFetch_ptr"])
            real_cancel = odbc_call(original["SQLCancel_ptr"])
            table = "##arrow_cancel_" + uuid.uuid4().hex
            control.execute(f"create table {table} (value int primary key, payload char(4000))")
            control.execute(
                f"insert into {table} select top (4096) "
                "row_number() over (order by (select null)), replicate('x', 4000) "
                "from sys.all_objects a cross join sys.all_objects b"
            )
            locker_id = control.execute("select @@spid").fetchone()[0]
            reader_id = cursor.execute("select @@spid").fetchone()[0]
            locker.autocommit = False
            control.execute(
                f"update {table} with (rowlock) set payload = 'locked' where value = 4096"
            )
            cursor.execute("set lock_timeout 20000")
            reader = cursor.execute(
                f"select value, payload from {table} with (readcommittedlock, rowlock, index(1)) "
                "order by value option (maxdop 1)"
            ).arrow_reader(batch_size=64)

            def consume():
                try:
                    for _ in reader:
                        pass
                except pa.ArrowInvalid as error:
                    if not reader.closed:
                        errors.append(error)
                except Exception as error:
                    errors.append(error)
                finally:
                    finished.set()

            worker = threading.Thread(target=consume, daemon=True)
            pointers["SQLFetch_ptr"].value = ctypes.cast(fetch, ctypes.c_void_p).value
            pointers["SQLCancel_ptr"].value = ctypes.cast(cancel, ctypes.c_void_p).value
            worker.start()
            try:
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline:
                    fetch_count = len(fetch_calls)
                    was_fetching = active.is_set()
                    blocked = control.execute(
                        "select wait_type from sys.dm_exec_requests "
                        "where session_id = ? and blocking_session_id = ? "
                        "and wait_type like 'LCK_M_%'",
                        reader_id,
                        locker_id,
                    ).fetchone()
                    if (
                        blocked
                        and was_fetching
                        and active.is_set()
                        and len(fetch_calls) == fetch_count
                    ):
                        break
                    assert not finished.is_set(), errors
                    time.sleep(0.01)
                else:
                    pytest.fail(
                        "No outstanding blocked SQLFetch observed; DMV visibility is required: "
                        f"fetches={len(fetch_calls)}, active={active.is_set()}, "
                        f"finished={finished.is_set()}, blocked={blocked}, errors={errors}"
                    )
                assert active.is_set(), "the blocked operation was not SQLFetch"
                reader.close()
                assert finished.wait(5), "real driver fetch did not terminate after SQLCancel"
                assert cancelled.is_set() and len(cancel_results) == 1, cancel_results
                assert cancel_results[0] in (0, 1), cancel_results
                assert not errors, errors
                # The blocking transaction is deliberately still open.
                cursor.execute("select 99")
                assert cursor.fetchone()[0] == 99
            finally:
                locker.rollback()
                worker.join(5)
                assert not worker.is_alive(), "driver fetch remained blocked after rollback"
                for name, pointer in pointers.items():
                    pointer.value = original[name]
                reader.close()


def _arrow_native_cancel_probe(outcome):
    import ctypes
    import os
    import threading
    from mssql_python import ddbc_bindings

    callback = ctypes.WINFUNCTYPE if os.name == "nt" else ctypes.CFUNCTYPE
    fetch_type = callback(ctypes.c_short, ctypes.c_void_p)
    diag_type = callback(
        ctypes.c_short,
        ctypes.c_short,
        ctypes.c_void_p,
        ctypes.c_short,
        ctypes.POINTER(ctypes.c_ushort),
        ctypes.POINTER(ctypes.c_int),
        ctypes.POINTER(ctypes.c_ushort),
        ctypes.c_short,
        ctypes.POINTER(ctypes.c_short),
    )
    pointers = _arrow_odbc_pointers()
    closing = not outcome.startswith("partial_cancel")
    unrelated_error = outcome == "closing_error" or outcome.startswith("closing_mixed")
    entered = threading.Event()
    cancel_entered = threading.Event()
    release_fetch = threading.Event()
    release_cancel = threading.Event()
    cancel_done = threading.Event()
    finished = threading.Event()
    errors = []
    close_errors = []
    violations = []
    calls = []

    @fetch_type
    def fetch(handle):
        calls.append(handle)
        ret = real_fetch(handle)
        if ret != 0:
            violations.append(("unexpected real fetch result", ret))
        entered.set()
        if closing and not release_fetch.wait(10):
            violations.append("fetch release timed out")
        return (
            1
            if not closing or outcome == "closing_cancel" or outcome.startswith("closing_mixed")
            else -1
        )

    @fetch_type
    def cancel(_handle):
        cancel_entered.set()
        if not release_cancel.wait(10):
            violations.append("cancel release timed out")
        cancel_done.set()
        return 0

    @diag_type
    def diag(_kind, _handle, record, state, native, message, capacity, length):
        if closing and not cancel_done.is_set():
            violations.append("diagnostics raced SQLCancel")
        multiple = outcome.startswith("closing_mixed") or outcome == "partial_cancel_warning_first"
        if record > (2 if multiple else 1):
            return 100
        text = {
            "closing_error": "22003",
            "closing_hy007": "HY007",
            "closing_hy010": "HY010",
            "closing_24000": "24000",
            "closing_mixed": "22003" if record == 1 else "HY008",
            "closing_mixed_cancel_first": "HY008" if record == 1 else "22003",
            "partial_cancel_warning_first": "01000" if record == 1 else "HY008",
        }.get(outcome, "HY008")
        for index, character in enumerate(text + "\0"):
            state[index] = ord(character)
        native[0] = 0
        diagnostic = "injected diagnostic " + text
        length[0] = len(diagnostic)
        if capacity:
            copied = min(len(diagnostic), capacity - 1)
            for index in range(copied):
                message[index] = ord(diagnostic[index])
            message[copied] = 0
        return 1 if capacity <= len(diagnostic) else 0

    with mssql_python.connect(os.environ["DB_CONNECTION_STRING"]) as connection:
        with connection.cursor() as cursor:
            reader = cursor.execute(
                "select top 1000 1 a from sys.objects o1, sys.objects o2"
            ).arrow_reader(batch_size=128)
            original = {name: pointer.value for name, pointer in pointers.items()}
            real_fetch = fetch_type(original["SQLFetch_ptr"])

            def consume():
                try:
                    next(reader)
                except StopIteration:
                    pass
                except Exception as error:
                    errors.append(error)
                finally:
                    finished.set()

            def close():
                try:
                    reader.close()
                except Exception as error:
                    close_errors.append(error)

            worker = threading.Thread(target=consume, daemon=True)
            closer = threading.Thread(target=close, daemon=True)
            try:
                pointers["SQLFetch_ptr"].value = ctypes.cast(fetch, ctypes.c_void_p).value
                pointers["SQLCancel_ptr"].value = ctypes.cast(cancel, ctypes.c_void_p).value
                pointers["SQLGetDiagRec_ptr"].value = ctypes.cast(diag, ctypes.c_void_p).value
                worker.start()
                assert entered.wait(5)
                if closing:
                    closer.start()
                    assert cancel_entered.wait(5)
                    release_fetch.set()
                    assert not finished.wait(0.1), "fetch escaped the cancellation fence"
                release_cancel.set()
                worker.join(5)
                if closer.ident is not None:
                    closer.join(5)
                assert not worker.is_alive() and not closer.is_alive()
            finally:
                release_fetch.set()
                release_cancel.set()
                worker.join(5)
                if closer.ident is not None:
                    closer.join(5)
                for name, pointer in pointers.items():
                    pointer.value = original[name]
                reader.close()

            assert not violations, violations
            assert not close_errors, close_errors
            assert len(calls) == 1, "cancelled batch issued another SQLFetch"
            if closing and not unrelated_error:
                assert not errors, errors
            else:
                expected = (
                    mssql_python.DataError if unrelated_error else mssql_python.OperationalError
                )
                assert len(errors) == 1 and isinstance(errors[0], expected), errors
                state = "22003" if unrelated_error else "HY008"
                assert "injected diagnostic " + state in str(errors[0]), errors
            cursor.execute("select 99")
            assert cursor.fetchone()[0] == 99


def test_arrow_reader_diagnostics_drained_on_close(cursor: mssql_python.Cursor):
    """After close(), any diagnostic messages produced server-side end up on
    cursor.messages (not silently dropped)."""
    # Drive a result-producing query, partially read, then close.
    reader = cursor.execute("select top 50 1 a from sys.objects").arrow_reader(batch_size=5)
    _ = reader.read_next_batch()
    # messages is a list of (sqlstate, text) tuples; should at least exist
    # and not raise when the close path tries to extend it.
    assert isinstance(cursor.messages, list)
    reader.close()
    assert isinstance(cursor.messages, list)


def test_arrow_reader_drains_diagnostics_when_close_cursor_succeeds(
    cursor: mssql_python.Cursor, monkeypatch
):
    """SQLFreeStmt(SQL_CLOSE) can return SQL_SUCCESS_WITH_INFO — a success
    code that still pushes warning records onto the HSTMT diag stack.  The
    cleanup path must drain diagnostics *unconditionally* after the close
    attempt, not only when _close_cursor() raises, otherwise those warnings
    would be silently dropped."""
    from mssql_python import cursor as cursor_mod

    reader = cursor.execute("select top 10 1 a from sys.objects").arrow_reader(batch_size=5)
    _ = reader.read_next_batch()

    # Snapshot any pre-close diagnostics already on the cursor so we can
    # detect *new* records pushed by our monkeypatched drain calls.
    pre_existing = list(cursor.messages)

    real_drain = cursor_mod.ddbc_bindings.DDBCSQLGetAllDiagRecords
    call_count = {"n": 0}

    def fake_drain(hstmt):
        call_count["n"] += 1
        records = list(real_drain(hstmt))
        # Inject one synthetic record per call so we can prove both the
        # pre-close drain AND the post-close (success-path) drain ran.
        records.append(("01000", f"synthetic warning #{call_count['n']}"))
        return records

    monkeypatch.setattr(cursor_mod.ddbc_bindings, "DDBCSQLGetAllDiagRecords", fake_drain)

    # _close_cursor() should succeed (no exception); the bug would skip the
    # post-close drain entirely on that success path.
    reader.close()

    # Strip the snapshot to look only at messages added by the cleanup path.
    added = cursor.messages[len(pre_existing) :]
    synthetic_texts = [m[1] for m in added if isinstance(m, tuple) and len(m) >= 2]

    assert (
        "synthetic warning #1" in synthetic_texts
    ), "pre-close drain did not push diagnostics onto cursor.messages"
    assert "synthetic warning #2" in synthetic_texts, (
        "post-close drain was skipped on the SQL_CLOSE success path "
        "(SQL_SUCCESS_WITH_INFO warnings would be lost)"
    )
    assert call_count["n"] == 2

    reader = cursor.execute("SELECT 1 AS a UNION ALL SELECT 2").arrow_reader(batch_size=1)
    call_count["n"] = 0
    assert sum(batch.num_rows for batch in reader) == 2
    assert call_count["n"] == 1, "Natural exhaustion must only drain SQL_CLOSE diagnostics"
    assert cursor.messages == [("01000", "synthetic warning #1")]


def test_arrow_reader_close_retries_after_failed_attempt(cursor: mssql_python.Cursor):
    """If a first close() raises before the generator is released (e.g. another
    thread held it and gen.close() raised), a subsequent close() must retry
    the cleanup rather than silently no-op'ing — otherwise the server-side
    cursor would leak."""
    reader = cursor.execute("select top 10 1 a from sys.objects").arrow_reader(batch_size=2)

    real_gen = reader._generator
    assert real_gen is not None

    class FlakyGen:
        """Generator wrapper: first close() raises and reports gi_frame as
        still-set (simulating 'generator currently executing on another
        thread'); second close() delegates to the real generator."""

        def __init__(self, inner):
            self._inner = inner
            self._closed_calls = 0
            self.gi_frame = object()  # truthy => 'still alive'

        def close(self):
            self._closed_calls += 1
            if self._closed_calls == 1:
                raise ValueError("generator already executing")
            # Second call: pretend the other thread released it, delegate.
            self.gi_frame = None
            self._inner.close()

    flaky = FlakyGen(real_gen)
    reader._generator = flaky

    # First close: should mark reader closed (racing reads must raise) but
    # leave _generator intact so a retry is possible.
    reader.close()
    assert reader.closed is True
    assert reader._generator is flaky, "failed close() must not drop the generator ref"
    assert reader._cursor is not None, "failed close() must not drop the cursor ref"
    assert flaky._closed_calls == 1

    # Second close: must retry and complete cleanup this time.
    reader.close()
    assert flaky._closed_calls == 2
    assert reader._generator is None
    assert reader._cursor is None

    # Third close: now a true no-op (fully cleaned up).
    reader.close()
    assert flaky._closed_calls == 2  # not invoked again

    # Parent cursor still usable after the recovered close.
    cursor.execute("select 7")
    assert cursor.fetchone()[0] == 7


# ── _ArrowReader cancel / cleanup edge cases (coverage-focused) ─────────────
#
# The tests below target the branches of _ArrowReader / arrow_reader() that
# the existing suite doesn't reach — mostly defensive paths on the close /
# cancel side.  They keep the wrapper's public contract locked in so future
# refactors can't silently drop these guarantees:
#
#   * private-attribute lookup on the wrapper raises AttributeError (does not
#     recursively delegate through __getattr__ into self._inner)
#   * re-entering a closed reader as a context manager raises ArrowInvalid
#   * __arrow_c_stream__ fails loudly when the wrapped pyarrow reader lacks
#     the PyCapsule protocol (pyarrow < 14)
#   * __del__ is a no-op during interpreter finalization (module globals may
#     be gone) and never propagates an exception
#   * the batch-generator finally block is symmetric across every teardown
#     path — parent-cursor-already-closed, diag-drain failure, SQL_CLOSE
#     failure, bookkeeping-reset failure — so a single ODBC glitch cannot
#     leak the server-side cursor or crash close()


def test_arrow_reader_propagates_fetch_error_when_parent_cursor_is_closed(conn_str):
    """Closing the parent cursor must not turn the next fetch error into end-of-stream."""
    conn = mssql_python.connect(conn_str)
    try:
        tmp = conn.cursor()
        reader = tmp.execute("select top 5 1 a from sys.objects").arrow_reader(batch_size=2)
        _ = reader.read_next_batch()
        tmp.close()

        with pytest.raises(mssql_python.ProgrammingError, match="cursor is closed"):
            reader.read_next_batch()
    finally:
        conn.close()


def test_arrow_reader_getattr_refuses_private_names(cursor: mssql_python.Cursor):
    """__getattr__ refuses leading-underscore names so a partially-constructed
    instance during __del__ cannot recurse forever trying to resolve its own
    slot names via self._inner."""
    reader = cursor.execute("select 1 a").arrow_reader(batch_size=10)
    try:
        with pytest.raises(AttributeError):
            _ = reader._does_not_exist_anywhere
    finally:
        reader.close()


def test_arrow_reader_enter_after_close_raises(cursor: mssql_python.Cursor):
    """Using a closed reader as a context manager must raise ArrowInvalid;
    __enter__ refuses to hand out a reader that's already been torn down."""
    reader = cursor.execute("select 1 a").arrow_reader(batch_size=10)
    reader.close()
    with pytest.raises(pa.ArrowInvalid):
        with reader:
            pass


def test_arrow_reader_pycapsule_missing_on_inner_raises(cursor: mssql_python.Cursor):
    """If the wrapped pyarrow reader lacks ``__arrow_c_stream__`` (pyarrow < 14),
    the wrapper must raise ``ArrowInvalid`` rather than silently returning
    something invalid.  Simulated here by swapping ``_inner`` for an object
    that does not implement the protocol."""
    reader = cursor.execute("select 1 a").arrow_reader(batch_size=10)
    try:

        class WithoutProtocol:
            pass

        reader._inner = WithoutProtocol()
        with pytest.raises(pa.ArrowInvalid, match=r"pyarrow>=14"):
            reader.__arrow_c_stream__()
    finally:
        # close() only touches _generator / _cursor; the swapped _inner is
        # safe.  It is nulled out at the end of close() anyway.
        reader.close()


def test_arrow_reader_del_skips_during_interpreter_finalization(
    cursor: mssql_python.Cursor, monkeypatch
):
    """``__del__`` must return early when ``sys.is_finalizing()`` is True —
    module globals (pyarrow, ddbc_bindings) may already be torn down and
    touching native code at that point is unsafe.  With the guard active,
    close() is *not* invoked, so ``_generator`` stays set."""
    import sys as _sys

    reader = cursor.execute("select 1 a").arrow_reader(batch_size=10)
    try:
        monkeypatch.setattr(_sys, "is_finalizing", lambda: True)
        reader.__del__()
        assert (
            reader._generator is not None
        ), "__del__ ran close() during simulated interpreter finalization"
    finally:
        monkeypatch.undo()
        reader.close()


def test_arrow_reader_del_swallows_exceptions(cursor: mssql_python.Cursor, monkeypatch):
    """``__del__`` is best-effort — any exception raised inside it (e.g.
    because module globals were already torn down) must be swallowed rather
    than propagating out of a finalizer, where it would only be printed as
    an unraisable warning at best and abort the interpreter at worst."""
    import sys as _sys

    reader = cursor.execute("select 1 a").arrow_reader(batch_size=10)
    try:

        def boom():
            raise RuntimeError("simulated shutdown noise")

        monkeypatch.setattr(_sys, "is_finalizing", boom)
        # Must not propagate.
        reader.__del__()
    finally:
        monkeypatch.undo()
        reader.close()


def test_arrow_reader_cleanup_no_op_when_parent_cursor_already_closed(conn_str):
    """If the parent ``Cursor`` is closed before the reader's cleanup
    generator runs, the ``finally`` block must safely return without
    touching the freed HSTMT — covers the
    ``(cur.closed or cur.hstmt is None)`` early-return.

    Uses a dedicated connection so the shared module-scoped ``cursor``
    fixture is not affected and cannot race an 'active result set' error."""
    conn = mssql_python.connect(conn_str)
    try:
        tmp = conn.cursor()
        reader = tmp.execute("select top 5 1 a from sys.objects").arrow_reader(batch_size=2)
        _ = reader.read_next_batch()
        tmp.close()  # frees hstmt before the reader's finally block runs
        reader.close()  # must not raise
        assert reader.closed is True
    finally:
        conn.close()


def test_arrow_reader_cleanup_survives_diag_drain_failure(conn_str, monkeypatch):
    """If ``DDBCSQLGetAllDiagRecords`` raises (either the pre-close drain or
    the post-close SQL_SUCCESS_WITH_INFO drain), the cleanup path swallows
    the exception and continues rather than propagating and skipping the
    remaining teardown steps."""
    from mssql_python import cursor as cursor_mod

    conn = mssql_python.connect(conn_str)
    try:
        tmp = conn.cursor()
        reader = tmp.execute("select top 5 1 a from sys.objects").arrow_reader(batch_size=2)
        _ = reader.read_next_batch()

        def raise_drain(_hstmt):
            raise RuntimeError("simulated ODBC diag failure")

        monkeypatch.setattr(cursor_mod.ddbc_bindings, "DDBCSQLGetAllDiagRecords", raise_drain)
        reader.close()  # must not propagate
        assert reader.closed is True
        monkeypatch.undo()
        tmp.close()
    finally:
        conn.close()


def test_arrow_reader_cleanup_survives_close_cursor_failure(cursor, monkeypatch):
    """If ``hstmt._close_cursor()`` raises inside the cleanup generator, the
    handler swallows and continues to the bookkeeping-reset step.  A
    ``SQLFreeStmt(SQL_CLOSE)`` failure must be a WARNING, not a crash."""
    from unittest.mock import Mock
    from mssql_python import cursor as cursor_mod

    reader = cursor.execute("select top 5 1 a from sys.objects").arrow_reader(batch_size=2)
    assert reader.read_next_batch().num_rows == 2
    original_handle = cursor.hstmt
    failure = RuntimeError("simulated SQLFreeStmt failure")
    close_attempts = []
    warning = Mock()
    cursor.rowcount = 5
    assert cursor._has_result_set

    def fail_close(handle):
        close_attempts.append(handle)
        raise failure

    with monkeypatch.context() as patch:
        patch.setattr(type(original_handle), "_close_cursor", fail_close)
        patch.setattr(cursor_mod.logger, "warning", warning)
        reader.close()
        reader.close()

    assert cursor.hstmt is original_handle
    assert close_attempts == [original_handle]
    warning.assert_called_once()
    message, error = warning.call_args.args
    assert "arrow_reader cleanup: _close_cursor failed" in message
    assert "server-side cursor may remain open" in message
    assert error is failure
    assert reader.closed
    assert reader._generator is None
    assert cursor.rowcount == -1
    assert cursor._rownumber == -1
    assert not cursor._has_result_set
    cursor.execute("select 99")
    assert cursor.fetchone()[0] == 99


def test_arrow_reader_cleanup_survives_bookkeeping_reset_failure(conn_str, monkeypatch):
    """If ``Cursor._clear_rownumber()`` raises, cleanup swallows and completes.
    A bookkeeping glitch must not leave the reader in an inconsistent
    'closed but generator alive' state."""
    conn = mssql_python.connect(conn_str)
    try:
        tmp = conn.cursor()
        reader = tmp.execute("select top 5 1 a from sys.objects").arrow_reader(batch_size=2)
        _ = reader.read_next_batch()

        def raise_clear():
            raise RuntimeError("simulated bookkeeping failure")

        monkeypatch.setattr(tmp, "_clear_rownumber", raise_clear)
        reader.close()
        assert reader.closed is True
        monkeypatch.undo()
        tmp.close()
    finally:
        conn.close()


def test_arrow_long_string(cursor: mssql_python.Cursor):
    "Make sure resizing the data buffer works"
    long_string = "A" * 100000  # 100k characters
    cursor.execute("select cast(? as nvarchar(max))", (long_string,))
    batch = cursor.arrow_batch(10)
    assert batch.num_rows == 1
    assert batch.num_columns == 1
    assert batch.column(0).to_pylist() == [long_string]


@pytest.mark.parametrize("sql_type", ["char(32)", "varchar(32)"])
@pytest.mark.parametrize("narrow", [True, False])
def test_arrow_char_utf8_collation_unicode(
    cursor: mssql_python.Cursor, sql_type: str, narrow: bool
):
    table = "#t_arrow_char_decode"
    collation = "Latin1_General_100_CI_AS_SC_UTF8"
    expected = [
        "Grüße",
        "你好😀",
        "こんにちは",
        "Привет",
        "Hello 世界",
        "😀😃😄😁",
        "",
        None,
    ]
    if narrow:
        cursor.connection.setdecoding(mssql_python.SQL_CHAR, ctype=mssql_python.SQL_CHAR)

    try:
        cursor.execute(
            f"create table {table} (id int primary key, v {sql_type} collate {collation})"
        )
    except Exception as exc:
        pytest.skip(f"UTF-8 collation '{collation}' not supported: {exc}")

    try:
        for index, value in enumerate(expected, start=1):
            cursor.execute(f"insert into {table} (id, v) values (?, ?)", index, value)
        tbl = cursor.execute(f"select v from {table} order by id").arrow()
        assert tbl.column(0).type.equals(pa.large_string())
        for expected_val, actual_val in zip(expected, tbl.column(0).to_pylist(), strict=True):
            if actual_val is not None:
                actual_val = actual_val.strip()
            assert expected_val == actual_val
    finally:
        cursor.connection.setdecoding(mssql_python.SQL_CHAR)
        cursor.execute(f"drop table if exists {table}")


@pytest.mark.parametrize("sql_type", ["char(32)", "varchar(32)", "text"])
@pytest.mark.parametrize("narrow", [True, False])
def test_arrow_char_cp1252_collation_unicode(
    cursor: mssql_python.Cursor, sql_type: str, narrow: bool
):
    table = "#t_arrow_char_decode"
    collation = "SQL_Latin1_General_CP1_CI_AS"
    expected = [
        "Grüße",
        "café René!",
        "naïve café",
        "Español",
        "Müller-Öztürk",
        "Françoise",
        "",
        None,
    ]
    if narrow:
        cursor.connection.setdecoding(mssql_python.SQL_CHAR, ctype=mssql_python.SQL_CHAR)

    cursor.execute(f"create table {table} (id int primary key, v {sql_type} collate {collation})")

    try:
        for index, value in enumerate(expected, start=1):
            cursor.execute(f"insert into {table} (id, v) values (?, ?)", index, value)
        tbl = cursor.execute(f"select v from {table} order by id").arrow()
        assert tbl.column(0).type.equals(pa.large_string())
        for expected_val, actual_val in zip(expected, tbl.column(0).to_pylist(), strict=True):
            if actual_val is not None:
                actual_val = actual_val.strip()
            assert expected_val == actual_val
    finally:
        cursor.connection.setdecoding(mssql_python.SQL_CHAR)
        cursor.execute(f"drop table if exists {table}")


def test_rownumber_arrow_batch_interleaved_fetchmany(cursor: mssql_python.Cursor):
    """Verify that arrow_batch and fetchmany can be interleaved
    on the same result set with correct rownumber tracking and values."""
    N = 20
    unions = " union all ".join(f"select {i} as val" for i in range(1, N + 1))
    cursor.execute(f"select val from ({unions}) t order by val")

    batch = cursor.arrow_batch(3)
    assert batch.num_rows == 3
    vals = batch.column(0).to_pylist()
    assert vals == [1, 2, 3]
    assert cursor.rownumber == 2  # 0-based: last row index = 2

    rows = cursor.fetchmany(5)
    assert len(rows) == 5
    vals = [r[0] for r in rows]
    assert vals == [4, 5, 6, 7, 8]
    assert cursor.rownumber == 7

    batch = cursor.arrow_batch(4)
    assert batch.num_rows == 4
    vals = batch.column(0).to_pylist()
    assert vals == [9, 10, 11, 12]
    assert cursor.rownumber == 11

    batch = cursor.arrow_batch(100)
    assert batch.num_rows == 8
    vals = batch.column(0).to_pylist()
    assert vals == [13, 14, 15, 16, 17, 18, 19, 20]
    assert cursor.rownumber == 19


def test_rownumber_interleaved_fetchmany_arrow_fetchone_arrow_fetchall(
    cursor: mssql_python.Cursor,
):
    """Verify mixed fetch APIs can be interleaved on the same result set
    with correct rownumber tracking and values."""
    N = 20
    unions = " union all ".join(f"select {i} as val" for i in range(1, N + 1))
    cursor.execute(f"select val from ({unions}) t order by val")

    rows = cursor.fetchmany(4)
    assert len(rows) == 4
    vals = [r[0] for r in rows]
    assert vals == [1, 2, 3, 4]
    assert cursor.rownumber == 3

    batch = cursor.arrow_batch(3)
    assert batch.num_rows == 3
    vals = batch.column(0).to_pylist()
    assert vals == [5, 6, 7]
    assert cursor.rownumber == 6

    row = cursor.fetchone()
    assert row[0] == 8
    assert cursor.rownumber == 7

    batch = cursor.arrow_batch(5)
    assert batch.num_rows == 5
    vals = batch.column(0).to_pylist()
    assert vals == [9, 10, 11, 12, 13]
    assert cursor.rownumber == 12

    rows = cursor.fetchall()
    vals = [r[0] for r in rows]
    assert vals == [14, 15, 16, 17, 18, 19, 20]
    assert cursor.rownumber == 19


def test_rownumber_interleaved_fetchone_arrow_batch(cursor: mssql_python.Cursor):
    """Verify fetchone followed by arrow_batch returns the remaining rows
    with correct rownumber tracking and values."""
    N = 20
    unions = " union all ".join(f"select {i} as val" for i in range(1, N + 1))
    cursor.execute(f"select val from ({unions}) t order by val")

    row = cursor.fetchone()
    assert row[0] == 1
    assert cursor.rownumber == 0

    batch = cursor.arrow_batch(100)
    assert batch.num_rows == 19
    vals = batch.column(0).to_pylist()
    assert vals == list(range(2, N + 1))
    assert cursor.rownumber == 19


def test_arrow_sql_ss_udt_hierarchyid_fetch_bindcol(cursor: mssql_python.Cursor):
    select_sql = """
        SELECT node
        FROM (
            VALUES
                (1, hierarchyid::Parse('/1/')),
                (2, CAST(NULL AS HIERARCHYID)),
                (3, hierarchyid::Parse('/1/2/3/'))
        ) AS v(id, node)
        ORDER BY id
    """

    expected_rows = cursor.execute(select_sql).fetchall()
    batch = cursor.execute(select_sql).arrow_batch(10)
    assert batch.num_rows == 3
    assert batch.num_columns == 1
    udt_col_index = 0
    udt_column = batch.column(udt_col_index)
    assert udt_column.type.equals(pa.large_binary())
    assert udt_column.to_pylist() == [row[udt_col_index] for row in expected_rows]
    assert udt_column.null_count == 1


def test_arrow_sql_ss_udt_hierarchyid_fetch_getdata(cursor: mssql_python.Cursor):
    select_sql = """
        SELECT filler, node
        FROM (
            VALUES
                (1, CONVERT(VARBINARY(MAX), REPLICATE(CAST('A' AS VARCHAR(MAX)), 9001)), hierarchyid::Parse('/1/')),
                (2, CONVERT(VARBINARY(MAX), REPLICATE(CAST('B' AS VARCHAR(MAX)), 9002)), CAST(NULL AS HIERARCHYID)),
                (3, CONVERT(VARBINARY(MAX), REPLICATE(CAST('C' AS VARCHAR(MAX)), 9003)), hierarchyid::Parse('/1/2/3/'))
        ) AS v(id, filler, node)
        ORDER BY id
    """

    expected_rows = cursor.execute(select_sql).fetchall()
    batch = cursor.execute(select_sql).arrow_batch(10)

    assert batch.num_rows == 3
    assert batch.num_columns == 2
    assert batch.column(0).type.equals(pa.large_binary())
    assert batch.column(0).to_pylist() == [row[0] for row in expected_rows]
    udt_col_index = 1
    udt_column = batch.column(udt_col_index)
    assert udt_column.type.equals(pa.large_binary())
    assert udt_column.to_pylist() == [row[udt_col_index] for row in expected_rows]
    assert udt_column.null_count == 1
