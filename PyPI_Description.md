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
 
## What's new in v1.15.0

### Enhancements

- **Faster parameterized queries with `setinputsizes()`** - Queries that declare parameter types up front now execute measurably faster (up to ~50%), especially repeated `execute()` workloads. No code changes needed (#736).
- **`memoryview` Support in `Binary()`** - `Binary()` now accepts `memoryview` objects in addition to existing bytes-like inputs (#741).
- **Module-Level SQL Server Type Constants** - SQL Server-specific type constants are now available directly from the `mssql_python` module for simpler API access (#764).

### Bug Fixes

- **Concurrent Logging No Longer Deadlocks** - Logging now avoids GIL and mutex lock-order inversions during concurrent multithreaded use (#678).
- **Correct Rust Core in Windows ARM64 Wheels** - Windows ARM64 wheels now vendor the matching `mssql_py_core` binary, restoring installation and bulk-copy compatibility (#737).
- **Reliable Package-Local DLL Loading on Windows** - Bundled driver and authentication DLLs are resolved from package-local directories for more reliable deployment (#735).
- **Consistent Decimal Parameter Binding** - `Decimal` values are now bound as `SQL_NUMERIC` regardless of their value, preventing inconsistent parameter typing (#742).
- **ODBC 3.x Parameter Types** - Parameter binding now uses ODBC 3.x types instead of obsolete ODBC 2.x types for improved standards compatibility (#758).
- **Database Name Metadata Is Decoded** - `Connection.getinfo(SQL_DATABASE_NAME)` now returns correctly decoded text (#771).
- **Mixed Cursor Cleanup No Longer Crashes at Shutdown** - Cleanup for connections with mixed cursor states no longer causes a process-shutdown crash (#772).

For more information, please visit the project link on Github: https://github.com/microsoft/mssql-python
 
If you have any feedback, questions or need support please mail us at mssql-python@microsoft.com.
 
## What's Next
 
As we continue to refine the driver and add new features, you can expect regular updates, optimizations, and bug fixes. We encourage you to contribute, provide feedback and report any issues you encounter, as this will help us improve the driver.
