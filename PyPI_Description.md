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
 
## What's new in v1.19.0

### Enhancements

- **Connection Retry Policy** - Add optional `RetryPolicy` configuration for retrying transient failures during `connect()` and `Connection()` initialization; existing behavior is unchanged without a policy (#751).
- **Table-Valued Parameters** - Add `TableValuedParameter` definitions and synchronous execution through the default ODBC provider, including streamed rows, empty inputs, and nullable cells (#789).
- **Structured Error Diagnostics** - Expose optional `sqlstate` and `native_error` attributes on DB-API exceptions for synchronous ODBC errors while preserving existing exception messages and arguments (#909, #913).
- **Synchronous Query Cancellation** - Add best-effort `Cursor.cancel()` for supported synchronous execute and fetch operations, with operation-scoped requests and checked recovery. Cancellation does not implicitly roll back transactions; wait for the active operation to finish before reuse (#911, #915).
- **POSIX Native Binary Hardening** - Enable stack protection and supported fortification for the native extension, with additional Linux linker hardening (#810).

### Bug Fixes

- **Lossless Batch Parameter Inference** - Preserve Unicode, Decimal, and mixed-type values in `executemany` batches, including embedded and trailing NULs, while retaining explicit input-size overrides (#907).
- **Numeric Binding for Money-Range Decimals** - Bind auto-detected `executemany` Decimal columns as `SQL_NUMERIC` with batch-wide precision and scale, avoiding the money-range VARCHAR shortcut that can cause numeric overflow (#752).
- **Korean VARCHAR Encoding** - Include and verify CP949 conversion modules in manylinux builds to restore Korean VARCHAR conversion fidelity (#910).

For more information, please visit the project link on Github: https://github.com/microsoft/mssql-python
 
If you have any feedback, questions or need support please mail us at mssql-python@microsoft.com.
 
## What's Next
 
As we continue to refine the driver and add new features, you can expect regular updates, optimizations, and bug fixes. We encourage you to contribute, provide feedback and report any issues you encounter, as this will help us improve the driver.
