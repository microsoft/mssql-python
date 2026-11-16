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
 
## What's new in v1.18.0

### Enhancements

- **Custom Application Names** - Connection strings accept `APP` and `Application Name`, sharing the effective name across synchronous, async, and bulk-copy paths. The async and bulk-copy default changes from `mssql-python` to `MSSQL-Python`; exact-name telemetry filters may need updating (#884).
- **Faster Row-to-Dictionary Conversion** - Reuse row-mapping metadata to reduce the overhead of converting fetched rows to dictionaries (#847).
- **Faster Single-Row Fetching** - Reuse cursor-scoped native state for eligible `fetchone()`, `fetchmany(1)`, `fetchval()`, and iteration paths while retaining Python fallbacks (#858).
- **Faster Output-Converter Batching** - Reduce conversion and row-construction overhead when fetching batches with output converters (#852).
- **Faster Size-One Text Fetches** - Avoid repeating text-fetch preparation for `fetchmany(1)` without prefetching rows (#891).
- **Faster Pyformat Conversion** - Convert pyformat placeholders in a single pass and cache converted statements for repeated execution (#888).
- **Prepared Statement Reuse** - Compatible re-executions of the same parameterized SQL retain parameter bindings so drivers can reuse the prepared statement (#886).
- **Faster Rust-Backed Execution** - Reduce SQL-text processing and RPC parameter-serialization overhead in Rust-backed execution paths (#896) *(via `mssql-python-rs`)*.
- **macOS Strict TLS Improvements** - Rust-backed Strict connections can negotiate TLS 1.3, with post-quantum key exchange on supported macOS versions (#896) *(via `mssql-python-rs`)*.
- **Rust ODBC Table-Valued Parameters** - Add table-valued parameter support in the opt-in Rust ODBC provider (#896) *(via `mssql-python-rs`)*.

### Bug Fixes

- **Synapse Pooled Connection Close** - Avoid an unsupported isolation-level reset when closing autocommit-on pooled connections to Azure Synapse dedicated SQL pools (#889).
- **Explicit Pooled Checkout Timeouts** - Respect an explicit positive connection timeout during pooled checkout and restore the original timeout after successful validation (#897).
- **Arrow Reader Cross-Thread Shutdown** - Coordinate reader close and cancellation with batch operations to prevent races and protect subsequent cursor results (#892).
- **Correct Pyformat Placeholder Handling** - Preserve placeholder-like text inside SQL literals, quoted identifiers, and comments, and handle escaped percent signs consistently (#888).
- **Rust-Backed Connection Reliability** - Fix connection-timeout handling, prepared-statement lifetime and cancellation, isolation-change cursor preservation, and connection stack overflow on small stacks (#896) *(via `mssql-python-rs`)*.
- **Rust ODBC Streaming Order** - Stream data-at-execution values in their bound parameter order in the opt-in Rust ODBC provider (#896) *(via `mssql-python-rs`)*.
- **SQL_VARIANT Length Validation** - Reject malformed oversized scalar payload lengths instead of truncating them during Rust-backed decoding (#896) *(via `mssql-python-rs`)*.

For more information, please visit the project link on Github: https://github.com/microsoft/mssql-python
 
If you have any feedback, questions or need support please mail us at mssql-python@microsoft.com.
 
## What's Next
 
As we continue to refine the driver and add new features, you can expect regular updates, optimizations, and bug fixes. We encourage you to contribute, provide feedback and report any issues you encounter, as this will help us improve the driver.
