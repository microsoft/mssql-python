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
 
## What's new in v1.5.0

### Features

- **Apache Arrow Fetch Support** - Added high-performance Arrow-based data fetching via `cursor.arrow()`, `cursor.arrow_batch()`, and `cursor.arrow_reader()`, enabling zero-copy integration with pandas, Polars, and other Arrow-native data frameworks.
- **sql_variant Type Support** - Added support for the `sql_variant` complex SQL Server data type.
- **Native UUID Support** - Added native support for fetching and binding UUID/GUID values without manual string conversion.
- **Row Class Export** - `Row` class is now publicly exported from the top-level `mssql_python` module for easier use in type annotations and downstream code.

### Bug Fixes

- **Qmark False Positive Fix** - Fixed false positive qmark (`?`) detection for `?` appearing inside bracketed identifiers, string literals, and SQL comments.
- **NULL VARBINARY Parameter Fix** - Fixed NULL parameter type mapping for VARBINARY columns.
- **Bulkcopy Auth Fix** - Fixed stale auth fields being retained in `pycore_context` after token acquisition during bulk copy operations.
- **Explicit Module Exports** - Added explicit `__all__` exports from the main library module to prevent import resolution issues.
- **Credential Cache Fix** - Fixed credential instance cache to correctly reuse and invalidate cached credential objects.
- **datetime.time Microseconds Fix** - Fixed stored `datetime.time` values incorrectly having `microseconds` set to zero.
- **Arrow Time Fractional Seconds Fix** - Fixed time handling in Arrow integration to correctly include fractional seconds.
 
For more information, please visit the project link on Github: https://github.com/microsoft/mssql-python
 
If you have any feedback, questions or need support please mail us at mssql-python@microsoft.com.
 
## What's Next
 
As we continue to refine the driver and add new features, you can expect regular updates, optimizations, and bug fixes. We encourage you to contribute, provide feedback and report any issues you encounter, as this will help us improve the driver.
