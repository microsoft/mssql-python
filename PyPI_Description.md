# General Availability Release
 
mssql‑python is now Generally Available (GA) as Microsoft’s official Python driver for SQL Server, Azure SQL, and SQL databases in Fabric. This release delivers a production‑ready, high‑performance, and developer‑friendly experience.
 
## What makes mssql-python different?
 
### Powered by DDBC – Direct Database Connectivity
 
Most Python SQL Server drivers, including pyodbc, route calls through the Driver Manager, which has slightly different implementations across Windows, macOS, and Linux. This results in inconsistent behavior and capabilities across platforms. Additionally, the Driver Manager must be installed separately, creating friction for both new developers and when deploying applications to servers.
 
At the heart of the mssql-python driver is DDBC (Direct Database Connectivity) — a lightweight, high-performance C++ layer that replaces the platform’s Driver Manager.
 
Key Advantages:
 
- Provides a consistent, cross-platform backend that handles connections, statements, and memory directly.
- Interfaces directly with the native SQL Server drivers.
- Integrates with the same TDS core library that powers the ODBC driver.
 
### Why is this architecture important?
 
By simplifying the architecture, DDBC delivers:
 
- Consistency across platforms
- Lower function call overhead
- Zero external dependencies on Windows (`pip install mssql-python` is all you need)
- Full control over connections, memory, and statement handling
 
### Built with PyBind11 + Modern C++ for Performance and Safety
 
To expose the DDBC engine to Python, mssql-python uses PyBind11 – a modern C++ binding library.

PyBind11 provides:
 
- Native-speed execution with automatic type conversions
- Memory-safe bindings
- Clean and Pythonic API, while performance-critical logic remains in robust, maintainable C++.
 
## What's new in v1.8.0

### Enhancements

- **ActiveDirectoryMSI Support for Bulk Copy** - Adds `Authentication=ActiveDirectoryMSI` support to bulk copy, enabling both system-assigned and user-assigned managed identity authentication for Azure-hosted services (#573).
- **Row String-Key Indexing** - Row objects now support accessing values by column name as a string key (e.g., `row["col"]`), in addition to integer index and attribute access. Case-insensitive lookup is supported when the cursor's `lowercase` attribute is enabled (#589).
- **Bundled ODBC Driver Upgrade** - Updated the bundled Microsoft ODBC Driver for SQL Server from 18.5.1.1 to 18.6.2.1 (#569).

### Bug Fixes

- **Deferred Connect-Attribute Use-After-Free** - Fixed a use-after-free in `Connection.setAttribute` for deferred ODBC attributes (e.g., `SQL_COPT_SS_ACCESS_TOKEN`) that caused SIGBUS on macOS arm64 and authentication failures on Windows and Azure SQL (#596).
- **Connection String Parsed Multiple Times in Auth Path** - Refactored authentication handling to use dictionary-based parameter processing instead of repeated string parsing, improving reliability and performance (#590).
- **executemany Type Annotation Regression** - Fixed a typing regression where `Cursor.executemany` rejected valid `list[tuple[...]]` arguments under mypy due to invariant `List` type. The parameter type now uses covariant `Sequence` matching PEP 249 (#586).

For more information, please visit the project link on Github: https://github.com/microsoft/mssql-python
 
If you have any feedback, questions or need support please mail us at mssql-python@microsoft.com.
 
## What's Next
 
As we continue to refine the driver and add new features, you can expect regular updates, optimizations, and bug fixes. We encourage you to contribute, provide feedback and report any issues you encounter, as this will help us improve the driver.
