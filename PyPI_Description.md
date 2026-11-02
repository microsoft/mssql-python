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
 
## What's new in v1.17.0

### Enhancements

- **Reduced Pooled Connection Return Work** - Proven-clean pooled connections avoid redundant sanitation while transaction, failure-discard, capacity, and native handle-lifetime protections remain in place (#820).

### Bug Fixes

- **Reliable Bulk Copy After Upgrades** - Bulk copy and async loading import the native Rust extension directly, so upgrading from 1.15.0 no longer leaves the shared package initializer missing (#860).
- **Bounded Pooled Connection Checkout** - Validation of an unresponsive pooled socket now observes the configured login timeout and opens a replacement instead of probing stale sockets indefinitely (#859).
- **Accurate `nextset()` Row Counts** - Moving to the next result now refreshes `rowcount` for subsequent data-modification statements and resets it for row-producing results (#864).
- **PEP 249 `Timestamp()` Compatibility** - The `microsecond` argument is optional and defaults to zero, so the standard six-argument constructor works as specified (#872).
- **UTF-8 Bulk Copy Encoding** - Narrow values now honor UTF-8 flags and SQL sort IDs when encoded for bulk copy, preventing multibyte data failures against UTF-8 destinations (#875, via `mssql-python-rs`).
- **Bulk Copy Identifier Validation** - Server-qualified four-part destinations are rejected before metadata retrieval or command generation; valid targets remain limited to `database.schema.table` (#875, via `mssql-python-rs`).
- **Preserved Bulk Copy Datetime Precision** - String values targeting `DATETIME2` and `DATETIMEOFFSET` preserve seven fractional digits, trailing `Z`, and supported ISO forms without Python-version-dependent truncation (#875, via `mssql-python-rs`).

For more information, please visit the project link on Github: https://github.com/microsoft/mssql-python
 
If you have any feedback, questions or need support please mail us at mssql-python@microsoft.com.
 
## What's Next
 
As we continue to refine the driver and add new features, you can expect regular updates, optimizations, and bug fixes. We encourage you to contribute, provide feedback and report any issues you encounter, as this will help us improve the driver.
