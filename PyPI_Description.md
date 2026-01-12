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
 
## What's new in v1.1.0
 
### Enhancements
 
- **Thread-Safe Encoding/Decoding** - Mutex-based lock protection prevents race conditions in multi-threaded applications with concurrent connections. Strict validation for SQL_WCHAR types and security measures against encoding-based attacks ensure data integrity and application security.

### Critical Bug Fixes

- **Linux Stability** - Fixed critical double-free issue causing segmentation faults during Python garbage collection, significantly improving stability on Linux platforms.

- **Connection Pooling Isolation Level** - Transaction isolation level now explicitly resets to READ COMMITTED when pooled connections are reused, preventing unexpected transaction behavior and isolation level conflicts.

- **Connection String Escaping** - Fixed parser and builder to correctly handle ODBC curly brace escaping rules, enabling proper handling of special characters in passwords and connection values.

- **UTF-16 String Decoding** - Enhanced `connection.getinfo()` to properly decode UTF-16LE strings from SQL Server, eliminating data corruption when retrieving metadata with non-ASCII characters.

- **NULL Parameter Arrays** - Added support for arrays containing only NULL values in `executemany()`, improving batch operation reliability.

- **Query Timeout Consistency** - Refactored timeout handling to set query timeout during cursor initialization, reducing overhead and ensuring consistent timeout application.

- **IntegrityError Detection** - Fixed error handling in `fetchall()` for INSERT statements with OUTPUT clause and multiple VALUES entries.

- **Sensitive Parameter Filtering** - Corrected authentication parameter filtering to properly exclude Trusted_Connection while preserving encryption settings.
 
For more information, please visit the project link on Github: https://github.com/microsoft/mssql-python
 
If you have any feedback, questions or need support please mail us at mssql-python@microsoft.com.
 
## What's Next
 
As we continue to refine the driver and add new features, you can expect regular updates, optimizations, and bug fixes. We encourage you to contribute, provide feedback and report any issues you encounter, as this will help us improve the driver.
