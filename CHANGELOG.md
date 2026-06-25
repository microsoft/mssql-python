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

### Fixed
- Bug fix: Resolved issue with connection timeout.

## [1.0.0-alpha] - 2025-02-24

### Added
- Initial release of the mssql-python driver for SQL Server.

### Changed
- N/A

### Fixed
- N/A