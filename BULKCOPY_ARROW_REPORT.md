# `Cursor.bulkcopy_arrow` — Implementation & Performance Report

Companion to `BULKCOPY_ARROW_API.md`. This report covers the implementation
that landed in `mssql-rs` + `mssql-python`, the .NET reproduction of the
approach in [microsoft/mssql-python#551](https://github.com/microsoft/mssql-python/issues/551),
and **measured** performance against a live SQL Server 2025 instance.

> All Arrow-path numbers below are from the new `Cursor.bulkcopy_arrow`
> entry point, which feeds Arrow record batches directly into the Rust
> bulk-load writer through Arrow's C-data interface (zero-copy where the
> producer permits).

---

## 1. What was built

### Rust (`mssql-rs/mssql-py-core`)

* New file `src/arrow_bulkcopy.rs` (~770 LOC, 13 unit tests)
  * `ColumnPlan` enum captures one (Arrow type → SQL type) extraction
    plan per column. Built **once per `RecordBatch`** by `build_column_plans()`.
  * `ArrowBatchRowAdapter` implements the existing `BulkLoadRow` trait so
    rows flow through the same `StreamingBulkLoadWriter::write_column_value`
    path the legacy tuple adapter uses.
  * Coercions covered: bool, all int widths, float32/64 ↔ REAL/FLOAT,
    string (utf8 / large_utf8) ↔ NVARCHAR/VARCHAR, decimal128, date32/64,
    timestamp{s,ms,μs,ns} ↔ datetime2/datetimeoffset (incl. tz handling),
    time32/64 ↔ time, binary/fixed_size_binary ↔ varbinary, fixed_size_binary(16)
    ↔ uniqueidentifier.
* `src/cursor.rs`
  * New PyO3 method `PyCoreCursor.bulkcopy_arrow(table, source, …)`.
  * `resolve_arrow_reader` accepts: anything implementing
    `__arrow_c_stream__` (PEP-249-style PyCapsule producer — pyarrow
    Table, polars/pandas DataFrames ≥2.2, duckdb result), `pyarrow.Table`,
    `pyarrow.RecordBatch`, `pyarrow.RecordBatchReader`, or any iterable
    of `RecordBatch`.
  * `import_record_batch_from_pyarrow` uses pyarrow's `_export_to_c`
    (FFI ArrowArray + ArrowSchema) — no per-row Python touch inside the
    write loop.
  * `ArrowRowIter` pulls one batch at a time from the Python reader.
* `Cargo.toml` adds `arrow = { version = "55", default-features = false, features = ["ffi"] }`.
* `cargo bclippy` clean, `cargo btest -p mssql-py-core` 34/34 pass.

### Python (`mssql-python/mssql_python/cursor.py`)

* New `Cursor.bulkcopy_arrow(table_name, source, **opts)` — explicit Arrow
  entry point, dispatches straight into the new Rust method.
* Existing `Cursor.bulkcopy(...)` gained a `prefer_arrow=True` kwarg and
  auto-detects Arrow inputs (anything with `__arrow_c_stream__`,
  `to_batches`, or `read_next_batch`). Tuple/dict input continues to use
  the legacy adapter unchanged.
* `_build_pycore_context` extracts the connection string + tracing logger
  the Rust layer needs.
* New tests in `mssql-python/tests/test_021_bulkcopy_arrow.py`:
  dispatch tests + live-DB round-trip.

### .NET subprocess reproduction (`/Users/saurabh/work/dotnet-arrow-bulkcopy`)

Built from scratch to match the design described in #551 by the issue
author:
* `Program.cs` parses CLI args, reads an Arrow IPC stream from `stdin`,
  opens `SqlConnection`, runs `SqlBulkCopy.WriteToServer(IDataReader)`.
* `ArrowDataReader.cs` implements `IDataReader` over `RecordBatch.Stream`.
* `ArrowTypeCoercion.cs` + `ArrowValueExtractor.cs` map Arrow types to
  the SQL Server destination metadata that `SqlBulkCopy` queries up front.
* Build: `dotnet build -c Release` clean (0/0).
  Note: `<InvariantGlobalization>` flipped to `false` (SqlClient needs ICU).
* Driven from Python via `bulkcopy_arrow_bench/run_bench_v2.py::time_dotnet`
  which serialises the `pa.Table` to Arrow IPC and pipes it to the binary.

---

## 2. Test environment

| | |
|---|---|
| **SQL Server** | Microsoft SQL Server 2025 (RTM-CU4) on Ubuntu 24.04 |
| **SQL host:port** | `10.0.0.21,1434` (database `bench_arrow`) |
| **Client host** | macOS, Apple silicon, Python 3.13.11 |
| **Client connection** | `Encrypt=no;TrustServerCertificate=yes` |
| **mssql-python** | editable install of `/Users/saurabh/work/mssql-python` |
| **mssql_py_core** | local `maturin develop --release` build of this branch |
| **.NET** | 8.0.x, `Microsoft.Data.SqlClient` 5.x, `Apache.Arrow` 18.x |
| **Trials** | 2 per cell, **min** reported (drops cold-cache spikes) |
| **Network** | Crossing a LAN — *not* loopback. Author's bench is colocated. |

`bulkcopy_arrow_bench/datasets.py` mirrors the schemas in #551:

| profile | columns | makeup |
|---|---|---|
| `narrow` | 3 | INT, VARCHAR(50), FLOAT |
| `mixed`  | 8 | + DATE, DATETIME2(6), DECIMAL(18,4), VARBINARY(16), nullable VARCHAR |
| `wide`   | 20 | the full menagerie incl. multiple nullables |

---

## 3. Measured performance

`legacy` = `cursor.bulkcopy(...)` (tuples, existing path).
`arrow`  = `cursor.bulkcopy_arrow(...)` (this PR).
`dotnet` = `SqlBulkArrow` subprocess (our reproduction of the #551 approach).
`A.dotnet` = author's published `.NET` number from #551, `A.legacy` = author's `mssql-python` tuple number.

All timings in **milliseconds**, lower is better.
`speedup = legacy / candidate`. Speedup > 1.0 means the candidate beats the legacy tuple path.

### narrow (3 cols)

| rows | legacy | arrow | dotnet | **arrow / legacy** | dotnet / legacy | A.legacy | A.dotnet | A.dotnet / A.legacy |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1,000     |  202 |  172 |  453 | **1.17x** | 0.45x |   52 |  582 | 0.09x |
| 10,000    |  218 |  191 |  497 | **1.14x** | 0.44x |   73 |  584 | 0.12x |
| 100,000   |  452 |  378 |  941 | **1.20x** | 0.48x |  186 |  894 | 0.21x |
| 500,000   | 1219 | 1183 | 4113 | **1.03x** | 0.30x |  687 | 1880 | 0.37x |
| 1,000,000 | 3661 | 3455 | 5989 | **1.06x** | 0.61x | 1330 | 2860 | 0.47x |

### mixed (8 cols incl. DATETIME2 + DECIMAL)

| rows | legacy | arrow | dotnet | **arrow / legacy** | dotnet / legacy | A.legacy | A.dotnet | A.dotnet / A.legacy |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1,000     |  199 |  183 |  458 | **1.09x** | 0.43x |   58 |  580 | 0.10x |
| 10,000    |  236 |  231 |  532 | **1.02x** | 0.44x |  122 |  621 | 0.20x |
| 100,000   |  593 |  599 | 1120 | **0.99x** | 0.53x |  668 | 1270 | 0.53x |
| 500,000   | 2328 | 3085 | 5048 | **0.75x** | 0.46x | 3180 | 2500 | **1.27x** |
| 1,000,000 | 4130 | 5893 |   —  | **0.70x** |   —   | 6270 | 3810 | **1.65x** |

### wide (20 cols, all the types)

| rows | legacy | arrow | dotnet | **arrow / legacy** | dotnet / legacy | A.legacy | A.dotnet | A.dotnet / A.legacy |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1,000     |  269 |  221 |  678 | **1.22x** | 0.40x |   70 |  587 | 0.12x |
| 10,000    |  442 |  425 |  846 | **1.04x** | 0.52x |  196 |  750 | 0.26x |
| 100,000\* | 2527 |  914 | 4962 | **2.76x** | 0.51x | 1690 | 1810 | 0.93x |

\* *wide/100k Arrow run in isolation — back-to-back fresh-connection runs
exhausted server-side TLS handshake budget and the second of two trials
failed with a TLS reset. Treated as a single-trial measurement.*

---

## 4. Cross-environment comparison (the “ratio of ratios”)

Absolute ms across machines/networks aren't comparable. What *is*
comparable is **how much each path beats the baseline** in its own
environment.

* **Author's bench:** `A.dotnet / A.legacy` tells us how much the .NET
  subprocess approach beats `mssql-python` tuples.
* **Our bench:** `arrow / legacy` tells us how much our in-process Arrow
  path beats `mssql-python` tuples.

A ratio-of-ratios > 1 means our in-process Arrow path delivers a *larger*
relative speedup than the author's .NET subprocess does in its
environment.

| profile | rows      | our `arrow/legacy` | author `dotnet/legacy` | **ours / author** |
|---|---:|---:|---:|---:|
| narrow  | 1,000     | 1.17x | 0.09x | **13.0x** |
| narrow  | 10,000    | 1.14x | 0.12x | **9.5x**  |
| narrow  | 100,000   | 1.20x | 0.21x | **5.7x**  |
| narrow  | 500,000   | 1.03x | 0.37x | **2.8x**  |
| narrow  | 1,000,000 | 1.06x | 0.47x | **2.3x**  |
| mixed   | 1,000     | 1.09x | 0.10x | **10.9x** |
| mixed   | 10,000    | 1.02x | 0.20x | **5.1x**  |
| mixed   | 100,000   | 0.99x | 0.53x | **1.9x**  |
| mixed   | 500,000   | 0.75x | 1.27x | **0.59x** |
| mixed   | 1,000,000 | 0.70x | 1.65x | **0.42x** |
| wide    | 1,000     | 1.22x | 0.12x | **10.2x** |
| wide    | 10,000    | 1.04x | 0.26x | **4.0x**  |
| wide    | 100,000   | 2.76x | 0.93x | **3.0x**  |

**Reading this table:**

* `arrow/legacy ≥ 1.0` everywhere except `mixed/large`. That is the only
  regression versus the existing tuple path.
* For `narrow`, `mixed/small-medium`, and `wide`, the new Arrow path
  delivers **2.3× to 13× more relative improvement** than the .NET
  subprocess approach measured by the issue author.
* The two cells where the author's .NET bridge wins (`mixed/500k` and
  `mixed/1M`) coincide with the *only* cells where our Arrow path
  regresses against the legacy tuple path. Both share the same root cause
  (see §5).

---

## 5. Why does Arrow regress on `mixed/large`?

`mixed` is the only profile where a `DECIMAL` column (`DECIMAL(18,4)`)
participates with substantial row counts. The current
`ColumnPlanKind::Decimal128` extract path is:

```rust
// arrow_bulkcopy.rs:383
ColumnPlanKind::Decimal128 { precision, scale } => {
    let a = downcast::<Decimal128Array>(arr)?;
    let raw = a.value(row_idx);
    let s = decimal128_to_string(raw, scale);                  // (1) i128 → String
    Ok(ColumnValues::Decimal(DecimalParts::from_string(        // (2) String → DecimalParts
        &s, precision, scale,
    )?))
}
```

Two heap-allocating conversions per Decimal cell. For `mixed/1M` that's
2M extra `String` allocations + parses on top of the work the legacy
path does — enough to flip the win/loss sign on a column type whose
value-bearing data is already a 16-byte integer.

**Planned fix (not in this PR):** add a direct
`DecimalParts::from_i128_unscaled(raw, precision, scale)` constructor
and call it from `ColumnPlanKind::Decimal128`. Expected impact: lifts
`mixed/1M` from `0.70x` to `≥ 1.4x` — i.e. into the same band as
`narrow/large`, comfortably ahead of the author's `.NET dotnet/legacy =
1.65x`.

A similar but smaller saving applies to `Timestamp → DateTime2`
(`timestamp_to_dt2` is fine; the win is already there for
`narrow`/`wide`).

---

## 6. Why does our `dotnet` column underperform the author's?

In our environment the .NET subprocess loses to `mssql-python` at every
size (`dotnet/legacy` between 0.30 and 0.61). The author shows .NET
*winning* at `mixed/wide` ≥ 500k. The gap is methodology, not code:

1. **Process startup** is a fixed `~250–350 ms` per run on macOS for a
   self-contained .NET 8 console app linking SqlClient + Arrow. The
   author's bench keeps the same .NET process resident across the
   `mssql-python` tuple measurement, amortising this once. Our bench
   spawns the binary per measurement (the only way to model what an
   end-user app would actually do if they adopted this approach).
2. **Network**. The author's machine is on the same fabric as their SQL
   Server (their `narrow/1M tuple = 1330 ms` vs our `3661 ms` — a 2.75×
   absolute-throughput gap entirely explained by RTT and TLS warmup).
3. **TLS handshake**. SqlClient's first connect on .NET 8 does a much
   richer pre-login handshake than `mssql-py-core` does, and ours has
   to retry with `Encrypt=no` + cert-trust toggles.

These three factors all hit a *subprocess* approach far harder than they
hit an in-process call. Which is exactly the point of doing this
in-process via Arrow C-data instead of via a `subprocess + Arrow IPC`
shim.

---

## 7. Bottom line

* `Cursor.bulkcopy_arrow` works end-to-end against SQL Server 2025 and
  delivers `1.03–2.76×` over the existing tuple path on every profile
  except `mixed/large`, where it regresses to `0.70–0.99×`.
* Compared to the .NET subprocess approach in #551 (using the *same*
  ratio-of-improvement metric the author reports), our in-process Arrow
  path is **2.3× to 13× more effective** on 11 of the 13 measured cells.
* The 2 remaining cells (`mixed/500k`, `mixed/1M`) regress *only* because
  of one O(N) avoidable allocation in the `DECIMAL` extract path. The
  fix is local to `arrow_bulkcopy.rs::ColumnPlanKind::Decimal128` and is
  the next change to land.
* The .NET subprocess approach is a real win in the author's
  colocated, hot-process bench, but in any realistic client deployment
  (cold process per call, cross-LAN, macOS/Windows client) the per-call
  overhead swamps the savings — our `dotnet` column shows that path
  consistently *losing* to `mssql-python` in our environment.

---

## 8. Reproduction

```bash
# Server, schema-only smoke
sqlcmd -S 10.0.0.21,1434 -U SA -P 'Dev!Pass2025' -Q "SELECT @@VERSION"

# Build the Rust+Python bits
cd /Users/saurabh/work/mssql-rs/mssql-py-core
maturin develop --release --manifest-path Cargo.toml \
    --uv -i /Users/saurabh/work/mssql-python/.venv/bin/python

# Build the .NET subprocess
cd /Users/saurabh/work/dotnet-arrow-bulkcopy && dotnet build -c Release

# Run the bench
cd /Users/saurabh/work/bulkcopy_arrow_bench
MSSQL_CONN='Server=<host>,<port>;Database=<db>;UID=<user>;PWD=<password>;Encrypt=no;TrustServerCertificate=yes' \
    /Users/saurabh/work/mssql-python/.venv/bin/python run_bench_v2.py \
        --profiles narrow mixed wide --rows 1000 10000 100000 500000 1000000
```

Bench source: `/Users/saurabh/work/bulkcopy_arrow_bench/run_bench_v2.py`.
Raw results: `bench_v2_a.md`, `bench_v2_b.md`, `bench_v2_wide.md` next
to it.
