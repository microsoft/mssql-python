# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- New feature: Support for macOS and Linux.
- Documentation: Added API documentation in the Wiki.
- Bulk copy now supports `Authentication=ActiveDirectoryServicePrincipal`
  via an `entra_id_token_factory` callback registered on the mssql-py-core
  connection. The callback is invoked by mssql-tds mid-handshake (FedAuth
  workflow 0x02) so the tenant id can be resolved from the server-supplied
  STS URL. Requires `mssql-py-core` 0.1.5+. Partial fix for #534.

### Changed
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

## [1.0.0-alpha] - 2025-02-24

### Added
- Initial release of the mssql-python driver for SQL Server.

### Changed
- N/A

### Fixed
- N/A