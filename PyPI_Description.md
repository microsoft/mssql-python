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
 
## What's new in v1.7.1

### Enhancements

- **Platform Support: manylinux_2_28 Build Targets** - Added build targets for RHEL 8 and glibc 2.28 compatible distributions (#548).
- **Platform Support: macOS universal2 Wheel for Python 3.10** - Now producing a universal2 wheel for Python 3.10 on macOS, enabling native performance on Apple Silicon (#542).
- **Performance: UTF-16 String Handling via simdutf** - UTF-16 string processing now uses `simdutf` and `std::u16string` for significantly faster encoding/decoding (#526).
- **Performance: Optimized execute() Hot Path** - `execute()` gains soft reset, prepare caching, and guarded diagnostics for reduced overhead on repeated statement execution (#528).
- **Documentation: Azure Linux Installation Guide** - Added installation instructions for Azure Linux (#567).

### Bug Fixes

- **Login Failures Now Raise Correct Exception Type** - Authentication failures previously surfaced as `RuntimeError`; they now raise the appropriate `mssql_python` exception type (#562).
- **GIL Release Around Blocking ODBC Calls** - The GIL is now released around blocking `SQLSetConnectAttr` calls (#568), ODBC statement/fetch/transaction calls (#541), preventing thread stalls in multi-threaded workloads.
- **executemany Decimal Sign Change Fix** - Fixed a `RuntimeError` in `executemany` when decimal parameter values change sign between rows (#560).
- **CP1252 VARCHAR Encoding Consistency** - Fixed inconsistent retrieval of CP1252 encoded data in `VARCHAR` columns between Windows and Linux (#495).
- **BulkCopy Empty String in NVARCHAR(MAX)/VARCHAR(MAX)** - Fixed `cursor.bulkcopy()` failing with SQL error 40197/4804 when any row contained an empty string `""` in an `NVARCHAR(MAX)` or `VARCHAR(MAX)` column. Fix ships via `mssql_py_core` 0.1.4 (#559).

For more information, please visit the project link on Github: https://github.com/microsoft/mssql-python
 
If you have any feedback, questions or need support please mail us at mssql-python@microsoft.com.
 
## What's Next
 
As we continue to refine the driver and add new features, you can expect regular updates, optimizations, and bug fixes. We encourage you to contribute, provide feedback and report any issues you encounter, as this will help us improve the driver.
