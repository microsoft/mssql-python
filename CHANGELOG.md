# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- New feature: Support for macOS and Linux.
- Documentation: Added API documentation in the Wiki.
- Added support for SQL Server spatial data types (geography, geometry, hierarchyid) via SQL_SS_UDT type handling.
- Added `SQLTypeCode` class for dual-compatible type codes in `cursor.description`.

### Changed

- Improved error handling in the connection module.
- Enhanced `cursor.description[i][1]` to return `SQLTypeCode` objects that compare equal to both SQL type integers and Python types, maintaining full backwards compatibility while aligning with DB-API 2.0.

### Fixed

- Bug fix: Resolved issue with connection timeout.
- Fixed `cursor.description` type handling for better DB-API 2.0 compliance (Issue #352).

### SQLTypeCode Usage

The `type_code` field in `cursor.description` now returns `SQLTypeCode` objects that support both comparison styles:

```python
cursor.execute("SELECT id, name FROM users")
desc = cursor.description

# Style 1: Compare with Python types (backwards compatible with pandas, etc.)
if desc[0][1] == int:
    print("Integer column")

# Style 2: Compare with SQL type codes (DB-API 2.0 compliant)
from mssql_python.constants import ConstantsDDBC as sql_types
if desc[0][1] == sql_types.SQL_INTEGER.value:  # or just == 4
    print("Integer column")

# Get the raw SQL type code
type_code = int(desc[0][1])  # Returns 4 for SQL_INTEGER
```

## [1.0.0-alpha] - 2025-02-24

### Added

- Initial release of the mssql-python driver for SQL Server.

### Changed

- N/A

### Fixed

- N/A
