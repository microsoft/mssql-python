# Bulk Copy — Arrow Input API Proposal

Proposal for issue [microsoft/mssql-python#551](https://github.com/microsoft/mssql-python/issues/551). Adds an Apache Arrow input path to `Cursor.bulkcopy(...)` so callers with columnar data can hand it to the driver without first materializing a `list[tuple]` in Python.

## Motivation

`Cursor.bulkcopy(table_name, data, ...)` today expects `Iterable[tuple|list]`. Each row is a Python tuple; the Rust extension (`mssql_py_core`) walks it cell-by-cell, calling `is_instance_of::<PyInt>()`, `is_instance_of::<PyDateTime>()` etc. for every value (`mssql-py-core/src/bulkcopy.rs`, `SourcePythonType::detect`). For a 1M × 20 mixed table that is 20M Python `is_instance` calls plus 20M `PyAny` extracts — pure overhead with the GIL held the entire time. The benchmark in #551 puts the resulting throughput well behind a `subprocess(dotnet) + SqlBulkCopy + IDataReader<Arrow>` pipeline once the dataset gets large.

Arrow already round-trips into the driver via `Cursor.arrow() / arrow_batch() / arrow_reader()` (`cursor.py:2629-2715`) on the **read** side. The write side should accept the same shape.

## Public API

Add **one new method** on `Cursor` and accept the same Arrow inputs that the read side returns. The existing `bulkcopy(...)` is unchanged.

```python
def bulkcopy_arrow(
    self,
    table_name: str,
    source: ArrowSource,                       # see "Accepted inputs"
    *,
    batch_size: int = 0,
    timeout: int = 30,
    column_mappings: Optional[Union[List[str], List[Tuple[int, str]]]] = None,
    keep_identity: bool = False,
    check_constraints: bool = False,
    table_lock: bool = False,
    keep_nulls: bool = False,
    fire_triggers: bool = False,
    use_internal_transaction: bool = False,
) -> dict:
    ...
```

Return value matches `bulkcopy(...)`: `{"rows_copied": int, "batch_count": int, ...}`.

### Accepted inputs (`ArrowSource`)

Anything in this union, in priority order:

1. `pyarrow.RecordBatchReader` — the streaming case. Driver pulls one batch at a time; nothing extra is materialized.
2. `pyarrow.Table` — converted internally to `Table.to_reader(max_chunksize=batch_size or default)`.
3. `pyarrow.RecordBatch` — wrapped as a single-batch reader.
4. **Anything implementing `__arrow_c_stream__()`** (Arrow PyCapsule Interface, [arrow.apache.org spec](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html)). Covers `polars.DataFrame`, `pandas.DataFrame.__arrow_c_stream__` (pandas ≥ 2.2), `duckdb` results, `narwhals`, etc.
5. **Anything implementing `__arrow_c_array__()`** for a single-batch source.
6. `Iterable[pyarrow.RecordBatch]` — last-resort, lets callers stitch their own pipeline.

The driver dispatches on the input shape; callers don't have to convert.

### Convenience: detection in `bulkcopy`

`bulkcopy(...)` (the existing tuple/list method) **does not** silently re-route Arrow inputs. If `data` looks like Arrow (`hasattr(data, "__arrow_c_stream__")` or is a `pyarrow.{Table, RecordBatch, RecordBatchReader}`) it raises `TypeError` directing the caller to `bulkcopy_arrow`. Two reasons for the strict split:

1. **Symmetry with the read API.** mssql-python already exposes `fetchall_arrow` / `fetchmany_arrow` / `fetchone_arrow` as distinct methods rather than overloads. `bulkcopy_arrow` mirrors that convention.
2. **Predictability.** `bulkcopy()` and `bulkcopy_arrow()` have different per-row semantics (Python type detection vs Arrow column-plan extraction) and different perf envelopes. Keeping dispatch explicit means the call site documents which path is in use, which matters for both reviews and benchmarks.

Migration is one-line: `cur.bulkcopy(t, df)` → `cur.bulkcopy_arrow(t, df)` for any Arrow source.

### Errors

* `TypeError` — `source` is none of the supported shapes.
* `ImportError` — pyarrow not installed (only when the user supplied a pyarrow object explicitly; the C-stream PyCapsule path needs no pyarrow import).
* `ValueError` — Arrow schema can't be reconciled with the destination columns (unmappable types, fewer source columns than required non-nullable destination columns, etc.).

### Type mapping (Arrow → SQL Server)

| Arrow type | SQL Server target (preferred) | Notes |
|---|---|---|
| `int8/16/32/64`, `uint8/16/32` | `TINYINT/SMALLINT/INT/BIGINT` | width-preserving, narrows where dest column allows |
| `uint64` | `DECIMAL(20,0)` | TDS has no native u64 |
| `float16/32` | `REAL` | `float16` upcast |
| `float64` | `FLOAT` | |
| `bool` | `BIT` | |
| `string`, `large_string`, `string_view` | `NVARCHAR(...)`/`VARCHAR(...)` | UTF-8 buffer copied to UTF-16LE for `NVARCHAR` |
| `binary`, `large_binary`, `binary_view` | `VARBINARY(...)` | zero-copy slice |
| `date32` | `DATE` | days-since-epoch passed through |
| `date64` | `DATE` | ms divided by 86_400_000 |
| `timestamp[s/ms/us/ns]` (no tz) | `DATETIME2(<scale>)` | scale chosen from unit unless dest dictates |
| `timestamp[*, tz]` | `DATETIMEOFFSET(<scale>)` | offset preserved |
| `time32/64` | `TIME(<scale>)` | |
| `decimal128(p,s)` | `DECIMAL(p,s)` | precision/scale must fit destination |
| `fixed_size_binary(16)` | `UNIQUEIDENTIFIER` (when dest column is `uniqueidentifier`) | otherwise `BINARY(16)` |
| `null` | `NULL` (column must be nullable) | |
| `dictionary<...>` | resolved to value type, then mapped as above | |
| `list/struct/map` | not supported | raises `ValueError` |

NULL handling is honored from the Arrow validity buffer; non-nullable destination columns receive the same error today's tuple path raises (`Cannot insert NULL value into non-nullable column ...`).

## Wire-level design

The point of Arrow input is to skip Python entirely once the data is in. Two layers change:

### 1. `mssql-py-core` (Python ↔ Rust bridge)

* New PyO3 method `PyCoreCursor.bulkcopy_arrow(table_name, source, ...)` mirroring the existing `bulkcopy` signature.
* Accepts `&Bound<'_, PyAny>` and resolves it once:
  1. If it has `__arrow_c_stream__` — call it, get the PyCapsule (`arrow_array_stream`), extract the raw `ArrowArrayStream*` and hand to `arrow::ffi_stream::ArrowArrayStreamReader::from_raw` (the capsule transfers ownership of the stream — destructor on capsule release runs the stream's `release` callback exactly once; we move the FFI struct out before the capsule's destructor fires, per the Arrow spec).
  2. Otherwise import `pyarrow`, accept `Table`/`RecordBatchReader`/`RecordBatch`, and use the same `__arrow_c_stream__` path on it.
* Drops the GIL for the entire write loop (`Python::detach`), unlike the tuple path which holds it.

### 2. `mssql-tds` (Arrow → TDS)

A new `ArrowBatchRowAdapter { batch: Arc<RecordBatch>, row_idx: usize, mapping: Arc<Vec<ResolvedColumnMapping>>, dest: Arc<Vec<BulkCopyColumnMetadata>> }` implements `BulkLoadRow` (the existing zero-copy trait at `mssql-tds/src/connection/bulk_copy.rs:1140`). For each column, it reads directly from the Arrow array's primitive buffers — no Python touch, no `Box<dyn Any>` walk:

```rust
async fn write_to_packet(&self, w: &mut StreamingBulkLoadWriter<'_>, col_idx: &mut usize)
    -> TdsResult<()> {
    for (src_col_ord, dest_meta) in self.mapping.iter().zip(self.dest.iter()) {
        let arr = self.batch.column(src_col_ord.source_index);
        // Branch on arr.data_type() once per (column, target type) pair, *not* per row,
        // by caching a column writer at adapter-construction time (see ColumnPlan below).
        ...
    }
}
```

To avoid re-dispatching on `arr.data_type()` for every row, build a `Vec<ColumnPlan>` once per `RecordBatch`:

```rust
enum ColumnPlan {
    Int32        { arr: Int32Array, dest: TdsInt },
    Int64        { arr: Int64Array, dest: TdsBigInt },
    Float64      { arr: Float64Array, dest: TdsFloat },
    Utf8Nvarchar { arr: StringArray, dest_max_chars: u32, collation: SqlCollation },
    Utf8VarChar  { arr: StringArray, dest_max_chars: u32, codepage: u32 },
    Date32       { arr: Date32Array },
    Ts2Micros    { arr: TimestampMicrosecondArray, scale: u8 },
    // ... one variant per (arrow_type, sql_type) pair we support
    Decimal128   { arr: Decimal128Array, dest_p: u8, dest_s: u8 },
    FixedBin16Uuid { arr: FixedSizeBinaryArray },
    Coerced      { boxed: Box<dyn ColumnWriter + Send + Sync> }, // slow path / cast
}
```

Each variant has `write_row(&self, row_idx: usize, w: &mut StreamingBulkLoadWriter<'_>) -> TdsResult<()>` which:

* checks the validity bitmap (if any) — null → `write_column_value(col_idx, &ColumnValues::Null)`,
* otherwise grabs a primitive value or a `&[u8]` slice from the Arrow buffer and calls the existing `TdsValueSerializer` via `write_column_value`. Strings encode UTF-8 → UTF-16LE on the fly into the packet buffer (no intermediate `String`).

Result: per cell, we do (1) a bitmap check, (2) a buffer index, (3) the same TDS serialize step the tuple path uses. The `is_instance_of` chain and `extract::<i64>()` are gone; the GIL is released.

### Streaming and batching

* Outer loop: `while let Some(batch) = stream_reader.next() { ... }`.
* Per batch: build `ColumnPlan` once, then `for row_idx in 0..batch.num_rows() { writer.write_row_zerocopy(&ArrowBatchRowAdapter { ... }).await?; }`.
* `batch_size` keyword still controls the TDS-level commit interval (`StreamingBulkLoadWriter` flushes when this is hit), independent of Arrow batch boundaries — so a caller passing one giant batch still gets BCP commits at the requested cadence.

### Schema reconciliation

Build a `Vec<ResolvedColumnMapping>` once at start:

* If `column_mappings` is provided: use it (names map Arrow field name → SQL column, ordinals map Arrow column index → SQL column).
* Else: zip Arrow schema fields against destination metadata by ordinal — same default as today's tuple path.

If an Arrow field can't safely target the destination SQL type, raise before any data flows.

### Why this should beat the .NET subprocess

The subprocess approach pays:

* Process spin-up (~500 ms cold start, visible in #551's small-N rows).
* Inter-process Arrow IPC serialization (memcpy through a pipe).
* `IDataReader` boxing/unboxing in C# (`object`-typed `GetValue(i)` per cell unless every `Get*` is overridden).

The in-process Rust path pays:

* No subprocess, no pipe, no IPC framing.
* Zero-copy Arrow buffer reads (one bounds check + one load per primitive cell).
* TDS serialization runs on the same memory the producer wrote — no second copy until the network packet.

We get the .NET path's benefit (columnar, no Python-per-cell) without its costs (process boundary, IPC).

## Phasing

1. **Phase 1 — primitives + strings + dates + decimal + uuid + timestamps.** Covers every column type in #551's narrow/mixed/wide benchmarks.
2. **Phase 2 — large variants** (`large_string`, `large_binary`, `string_view`/`binary_view`), `dictionary` resolution, dest-driven Arrow casts (e.g. Arrow `int64` → SQL `int` with overflow check).
3. **Phase 3 — column-major TDS write.** A `write_column_chunk` API in `mssql-tds` that takes an entire Arrow array slice and writes N rows of one column in one pass, for further reduction in branch-prediction churn. Optional; row-major is already enough to beat the .NET path.

## Backwards compatibility

* `bulkcopy(...)` signature unchanged. Existing tuple/list callers behave identically.
* `bulkcopy(data=arrow_object)` is now a `TypeError` with a message pointing at `bulkcopy_arrow(...)` — no silent behavior change for existing tuple callers, and Arrow callers get a one-line, type-checked migration path.
* No new required dependency. `pyarrow` stays optional (already declared in `setup.py:192-194` and `requirements.txt:7`), and the C-stream PyCapsule path doesn't actually import pyarrow.
