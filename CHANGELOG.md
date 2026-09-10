# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- New feature: Support for macOS and Linux.
- Documentation: Added API documentation in the Wiki.
- New `token_provider=` parameter on `connect()` / `Connection` for Microsoft
  Entra ID authentication with a custom credential object. Accepts any object
  exposing a `.get_token(scope)` method (e.g. any `azure-identity` credential
  such as `DefaultAzureCredential`, `AzureCliCredential`,
  `ManagedIdentityCredential`). Mutually exclusive with `Authentication=` in
  the connection string and with `attrs_before[SQL_COPT_SS_ACCESS_TOKEN]`.
  Bulk copy re-acquires a fresh token from the provider on each operation. The
  token scope is fixed to the Azure commercial cloud; sovereign clouds are out
  of scope (supply a pre-acquired token via `attrs_before` instead).
- **GH-570:** New `Cursor.bulkcopy_arrow(table_name, source)` method for
  high-performance bulk loading directly from Apache Arrow data. Accepts a
  `pyarrow.Table`, `RecordBatch`, or `RecordBatchReader`, any object exposing
  the Arrow C Data Interface (`__arrow_c_stream__` / `__arrow_c_array__` — e.g.
  polars, pandas 2.2+, DuckDB, ADBC results), or an iterable of record batches.
  Data is streamed to the server through the Arrow C Data Interface without
  materializing intermediate Python row objects, and the GIL is released for
  the duration of the network transfer. When the source data already originates
  as Arrow, this avoids the Arrow→tuple conversion the classic `bulkcopy()`
  path requires (measured ~1.4x–2.7x faster end-to-end for such sources).
  `bulkcopy()` now raises `TypeError` steering Arrow inputs to this method.
  Requires `mssql-py-core` 0.1.5+.
- Bulk copy now supports `Authentication=ActiveDirectoryServicePrincipal`
  via an `entra_id_token_factory` callback registered on the mssql-py-core
  connection. The callback is invoked by mssql-tds mid-handshake (FedAuth
  workflow 0x02) so the tenant id can be resolved from the server-supplied
  STS URL. Requires `mssql-py-core` 0.1.5+. Partial fix for #534.
- **Standalone `mssql-python-odbc` package (PRs #663, #664):** the Microsoft
  ODBC Driver 18 for SQL Server binaries are now also published as a separate,
  platform-specific `mssql-python-odbc` package. When it is installed, the
  native driver loader resolves the driver from it; when it is absent or
  incomplete, mssql-python transparently falls back to its own bundled `libs/`.
  This is a non-breaking step toward decoupling driver-binary updates from
  mssql-python releases; a future major version will make the dependency
  explicit and drop the bundled binaries.
- **Opt-in/opt-out native provider selection:** a new `mssql_python.native_provider`
  module property (and `MSSQL_PYTHON_NATIVE_PROVIDER` environment variable, which
  takes precedence) lets a caller select which native ODBC provider is loaded:
  the default `"msodbcsql18"` (Microsoft ODBC Driver 18, unchanged behavior) or
  the opt-in `"mssql-odbc"` (a Rust-based driver, shipped inside the
  `mssql-python-rs` package alongside the Rust TDS core). The selection must be made before the first
  connection; it resolves and freezes then, and a later change is ignored with
  a `RuntimeWarning`; a conflicting property assignment also warns when the
  environment variable takes precedence. Call `mssql_python.get_native_provider_info()`
  for diagnostics (selected id, package, version, driver path, source, and
  whether it's frozen). This PR
  does not change the default provider or ship any Rust driver binaries.

### Changed
- **GH-769 deprecation policy:** The misplaced `GetInfoConstants` members
  `SQL_TXN_ISOLATION_LEVEL`, `SQL_CONCURRENCY`, `SQL_ROWSET_SIZE`, `SQL_ROW_NUMBER`,
  `SQL_IC_UPPER`, `SQL_IC_LOWER`, `SQL_IC_SENSITIVE`, `SQL_IC_MIXED`, and
  `SQL_SQL92_ENTRY_SQL`, `SQL_SQL92_INTERMEDIATE_SQL`, `SQL_SQL92_FULL_SQL`
  remain available with their original values throughout **1.x**. Existing enum
  attribute access, module-level imports, and `get_info_constants()` dictionary
  lookups remain available. Removal is deferred to **2.0 or later**, only after
  maintainer approval and an explicit migration notice; this is not a scheduled removal.
  Deprecation is documented rather than emitting runtime warnings.
  These names are not information-type names, and passing their integers to
  `getinfo()` still requests unrelated information. They cannot be rejected by
  value without also rejecting legitimate information types with the same IDs.
  For migration, use `SQL_ATTR_TXN_ISOLATION` for the connection attribute;
  use `ConstantsDDBC` or module-level names for legacy statement options and
  `SQL_IC_*` response values, not as `getinfo()` requests. To interpret
  `SQL_SQL_CONFORMANCE`, use `SQL_SC_SQL92_ENTRY` (1),
  `SQL_SC_FIPS127_2_TRANSITIONAL` (2), `SQL_SC_SQL92_INTERMEDIATE` (4), and
  `SQL_SC_SQL92_FULL` (8). The deprecated `SQL_SQL92_*_SQL` names retain
  **127/128/129** solely for compatibility; they are not conformance flags.
  For information types whose IDs are corrected, previously persisted value-based
  enum pickles and raw IDs cannot identify their original meaning; rebuild them
  from the intended information-type names. Name-based enum pickles resolve
  retained names to their corrected values.
- Connection strings and string connection parameters that contain a NUL
  (`\x00`) character are now rejected up front with `InterfaceError` instead of
  being silently truncated at the NUL by the underlying driver.
- Improved error handling in the connection module.
- **GH-627 behavioral change:** `NULL` parameters for `VARBINARY`/`BINARY`
  columns on physical tables now succeed silently (previously raised
  `ProgrammingError` when a non-NULL parameter was bound first). For temp
  tables where `SQLDescribeParam` cannot determine column metadata, the
  fallback to `SQL_VARCHAR` still produces the same `ProgrammingError` as
  before; users should call `cursor.setinputsizes()` to work around this.

### Fixed
- **GH-769:** Corrected 11 `GetInfoConstants` IDs for scalar functions, outer
  joins, driver handles, cursor attributes, catalog support, and parameter
  descriptions. Added the ODBC name `SQL_TIMEDATE_FUNCTIONS` as an alias of
  `SQL_DATETIME_FUNCTIONS` at 52, preserving the existing canonical `.name`.
  For registered information types,
  `getinfo()` now uses ODBC return types instead of guessing from the bytes: numeric
  information is decoded as unsigned integers (including
  `SQL_SQL_CONFORMANCE`, `SQL_CURSOR_SENSITIVITY`, and
  `SQL_MAX_IDENTIFIER_LEN`), and character results use the Unicode path.
  An immutable return-type registry enforces the exact ODBC numeric width,
  including pointer-sized handles. Wrong-width or truncated numeric payloads
  raise `DatabaseError` rather than returning a guessed value. Legacy non-byte
  numeric payloads are not truncated or coerced from bool to int; convertible
  decimal strings still return integers.
  Also corrects decoding for the unlisted standard IDs `SQL_DBMS_NAME` (17),
  `SQL_DBMS_VER` (18), `SQL_XOPEN_CLI_YEAR` (10000), `SQL_ASYNC_MODE` (10021), and
  `SQL_CREATE_ASSERTION` (127). Other unlisted raw IDs retain their existing behavior.
  Driver Manager-only handle queries can still be unsupported by the native
  provider; correcting their IDs does not add Driver Manager support. Native
  retrieval errors, including timeout and connection-loss errors, continue to be
  logged and return `None`. Providers may return cached metadata after connection
  loss: `getinfo()` is not a connection-health check.
- **GH-740:** A Python `Decimal` whose value falls in the SQL Server MONEY /
  SMALLMONEY range is now bound as `SQL_NUMERIC` with its own precision and scale
  on both `execute()` paths (native detection, and the legacy path reached when
  `setinputsizes()` covers fewer positions than parameters). Previously it was
  bound as a formatted `VARCHAR` based on the value alone, so comparing it against
  a smaller `numeric`/`decimal` column (`WHERE v = ?`) made SQL Server convert
  `varchar`→`numeric` and raise an arithmetic overflow instead of simply not
  matching. Also fixes a latent binder bug the shortcut was masking: the numeric
  APD descriptor record number was hardcoded to `1`, so a numeric parameter in any
  position other than the first corrupted the parameter bound at position 1.
  **Behavioral change on the wire:** money-range Decimals now arrive as `numeric`
  rather than `varchar` — a bare `SELECT ?` returns `Decimal` instead of `str`,
  `sql_variant` stores them as `numeric`, and because `numeric` outranks
  `money`/`varchar` in data-type precedence, `WHERE money_or_varchar_col = ?` can
  add a `CONVERT_IMPLICIT` on the column side that turns an index seek into a scan.
  `executemany` intentionally keeps its batch `VARCHAR` string binding (GH-503);
  the remaining money-range case there is tracked in #745.
- **GH-725:** The `timeout` parameter of `connect()` / `Connection(...)` now
  correctly sets the **login (connection-attempt) timeout**
  (`SQL_ATTR_LOGIN_TIMEOUT`), matching pyodbc and its own docstring. Previously
  it was silently applied as the per-statement **query** timeout, so
  `connect(timeout=N)` did not bound the connection attempt and instead aborted
  long-running queries. **Behavioral change:** the constructor `timeout` no
  longer affects `Connection.timeout` (the query timeout, still settable via the
  property, default `0`); an explicit `attrs_before[SQL_ATTR_LOGIN_TIMEOUT]`
  takes precedence over the `timeout` kwarg. Timeout values are now validated
  consistently at both entry points: the constructor `timeout` and the
  `Connection.timeout` setter reject negative, non-integer, and `bool` values
  (previously `Connection.timeout = True`/`False` was accepted as `1`/`0`).
- **GH-627:** Fixed `SQLDescribeParam` ordinal remapping bug that caused
  `VARBINARY`/`BINARY` `NULL` bindings to fail when a non-NULL parameter was
  bound first. The driver now pre-resolves unknown NULL parameter types before
  any `SQLBindParameter` calls, avoiding ODBC ordinal confusion.
- **GH-726:** The extension loader now derives the Windows architecture from
  `sysconfig.get_platform()` (the interpreter build) instead of
  `platform.machine()` (the host CPU). An x64 interpreter on a Windows ARM64
  machine previously looked for `ddbc_bindings.cpXY-arm64.pyd`, missed, and
  took the fallback path on every import; it now resolves the `amd64` binary
  the `win_amd64` wheel ships. The fallback notice is now emitted as a
  `RuntimeWarning` instead of being printed to stdout.

## [1.0.0-alpha] - 2025-02-24

### Added
- Initial release of the mssql-python driver for SQL Server.

### Changed
- N/A

### Fixed
- N/A