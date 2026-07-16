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
 
## What's new in v1.11.0

### Bug Fixes

- **SSH-Tunnel / In-Process Forwarder Deadlock** - The driver now releases the GIL around blocking ODBC teardown calls (`SQLFreeHandle`/`SQLFreeStmt`) and `SQLDescribeParam`, preventing deadlocks when the connection is routed through an in-process Python TCP forwarder (#604).
- **BINARY/VARBINARY NULL Parameters in Temp Tables** - Unknown NULL parameter types are now pre-resolved before binding, with actionable `setinputsizes` guidance, fixing errors when inserting NULL binary values into temp tables or table variables (#654).
- **Context Manager Transaction Semantics** - The `Connection` context manager now commits on clean exit and rolls back on exception (with `autocommit=False`), matching the documented behavior (#639).
- **macOS Apple Silicon Import Failure** - Bundled macOS ODBC dylibs are now configured for all shipped architectures, so `import mssql_python` works on Apple Silicon without requiring a separate `brew install unixodbc` (#661).
- **Service Principal Bulk Copy Freeze** - Fixed a GIL-deadlock that froze `bulkcopy` when authenticating with a service principal (#666, via `mssql_py_core` 0.1.6).

For more information, please visit the project link on Github: https://github.com/microsoft/mssql-python
 
If you have any feedback, questions or need support please mail us at mssql-python@microsoft.com.
 
## What's Next
 
As we continue to refine the driver and add new features, you can expect regular updates, optimizations, and bug fixes. We encourage you to contribute, provide feedback and report any issues you encounter, as this will help us improve the driver.
