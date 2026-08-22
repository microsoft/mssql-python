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

### Changed
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
- Bug fix: Resolved issue with connection timeout.
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