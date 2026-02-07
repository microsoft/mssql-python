# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- New feature: Support for macOS and Linux.
- Documentation: Added API documentation in the Wiki.
- Added `SqlTypeCode` class for dual-compatible type codes in `cursor.description`.

### Changed

- Improved error handling in the connection module.
- Enhanced `cursor.description[i][1]` to return `SqlTypeCode` objects that compare equal to both SQL type integers and Python types, improving backwards compatibility while aligning with DB-API 2.0. Note that `SqlTypeCode` instances are intentionally unhashable; code that previously used `cursor.description[i][1]` as a dict or set key should use `int(type_code)` or `type_code.type_code` instead.

### Fixed

- Bug fix: Resolved issue with connection timeout.
- Fixed `cursor.description` type handling for better DB-API 2.0 compliance (Issue #352).

## [1.0.0-alpha] - 2025-02-24

### Added

- Initial release of the mssql-python driver for SQL Server.

### Changed

- N/A

### Fixed

- N/A
