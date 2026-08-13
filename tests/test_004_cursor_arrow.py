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
        pytest.skip("pyarrow>=14 required for RecordBatchReader.from_stream")

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


@pytest.mark.stress  # large cross-join + 50ms timing race — flaky under CI CPU contention
def test_arrow_reader_cancel_from_other_thread(cursor: mssql_python.Cursor):
    """close() called from a separate thread must unblock an in-flight fetch
    via SQLCancel and leave the parent Cursor reusable."""
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
            time.sleep(0.05)  # let the consumer get into a fetch
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


@pytest.mark.parametrize(
    ("closed", "has_hstmt"),
    [(True, True), (False, False)],
    ids=["closed-cursor", "missing-hstmt"],
)
def test_arrow_reader_propagates_fetch_error_when_cleanup_is_skipped(closed, has_hstmt):
    """A defensive cleanup guard must not turn a fetch error into end-of-stream."""

    class FakeCursor:
        def __init__(self):
            self.closed = False
            self.hstmt = object()
            self.calls = 0

        def _check_closed(self):
            pass

        def _ensure_pyarrow(self):
            return pa

        def arrow_batch(self, _batch_size):
            self.calls += 1
            if self.calls == 1:
                return pa.record_batch([pa.array([], type=pa.int64())], names=["value"])

            self.closed = closed
            self.hstmt = object() if has_hstmt else None
            raise RuntimeError("fetch failed")

    fake_cursor = FakeCursor()
    reader = mssql_python.Cursor.arrow_reader(fake_cursor, batch_size=1)
    try:
        with pytest.raises(RuntimeError, match="fetch failed"):
            reader.read_next_batch()
    finally:
        reader.close()


def test_arrow_reader_propagates_fetch_error_after_cleanup(monkeypatch):
    """Fetch errors must survive the normal cleanup path, which must still run."""
    from mssql_python import cursor as cursor_mod

    class FakeHstmt:
        def __init__(self):
            self.close_calls = 0

        def _cancel(self):
            pass

        def _close_cursor(self):
            self.close_calls += 1

    class FakeCursor:
        def __init__(self):
            self.closed = False
            self.hstmt = FakeHstmt()
            self.messages = []
            self.rowcount = 1
            self.calls = 0
            self.rownumber_cleared = False

        def _check_closed(self):
            pass

        def _ensure_pyarrow(self):
            return pa

        def _clear_rownumber(self):
            self.rownumber_cleared = True

        def arrow_batch(self, _batch_size):
            self.calls += 1
            if self.calls == 1:
                return pa.record_batch([pa.array([], type=pa.int64())], names=["value"])
            raise RuntimeError("fetch failed")

    monkeypatch.setattr(cursor_mod.ddbc_bindings, "DDBCSQLGetAllDiagRecords", lambda _h: [])
    fake_cursor = FakeCursor()
    reader = mssql_python.Cursor.arrow_reader(fake_cursor, batch_size=1)
    try:
        with pytest.raises(RuntimeError, match="fetch failed"):
            reader.read_next_batch()
        assert fake_cursor.hstmt.close_calls == 1
        assert fake_cursor.rownumber_cleared is True
        assert fake_cursor.rowcount == -1
    finally:
        reader.close()


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


def test_arrow_reader_cleanup_survives_close_cursor_failure(conn_str, monkeypatch):
    """If ``hstmt._close_cursor()`` raises inside the cleanup generator, the
    handler swallows and continues to the bookkeeping-reset step.  A
    ``SQLFreeStmt(SQL_CLOSE)`` failure must be a WARNING, not a crash."""
    from mssql_python import cursor as cursor_mod

    conn = mssql_python.connect(conn_str)
    try:
        tmp = conn.cursor()
        reader = tmp.execute("select top 5 1 a from sys.objects").arrow_reader(batch_size=2)
        _ = reader.read_next_batch()

        real_hstmt = tmp.hstmt

        class FakeHstmt:
            # reader.close() invokes SQLCancel via hstmt._cancel() before it
            # closes the generator; provide a no-op so we exercise the SQL_CLOSE
            # path specifically.
            def _cancel(self_inner):
                pass

            def _close_cursor(self_inner):
                raise RuntimeError("simulated SQLFreeStmt failure")

        tmp.hstmt = FakeHstmt()
        # Neutralize the diag drain — it would crash on our fake hstmt.
        monkeypatch.setattr(cursor_mod.ddbc_bindings, "DDBCSQLGetAllDiagRecords", lambda _h: [])
        try:
            reader.close()
            assert reader.closed is True
        finally:
            # Restore the real hstmt before closing tmp so ODBC teardown is clean.
            tmp.hstmt = real_hstmt
            monkeypatch.undo()
            tmp.close()
    finally:
        conn.close()


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
