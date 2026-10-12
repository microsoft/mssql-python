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
 
## What's new in v1.16.0

### Enhancements

- **Python 3.15 Support** - Release wheels and validation now cover Python 3.15 across supported platforms (#817).
- **Dictionary-Style Row Access** - `row._mapping` provides a read-only mapping view with dictionary conversion, keys, values, items, membership checks, and stable column metadata (#613).
- **External Rust Runtime Dependency** - `mssql-python` now depends on the exact compatible `mssql-python-rs` package instead of embedding its files (#768).
- **Optional `mssql-odbc` Provider** - Customers can opt into the Rust-based provider distributed with `mssql-python-rs` by setting `MSSQL_PYTHON_NATIVE_PROVIDER=mssql-odbc` or `mssql_python.native_provider = "mssql-odbc"` before the first connection; Microsoft ODBC Driver 18 (`msodbcsql18`) remains the default (#730, #804, via `mssql-python-rs`).
- **Stable-ABI Rust Runtime Wheels** - The Rust runtime now uses CPython's stable ABI and `mssql-python-rs` 0.3.0, eliminating Python-version-specific wheel variants while retaining separate wheels for each supported OS and architecture (#804, via `mssql-python-rs`).
- **Updated ODBC Runtime Package** - `mssql-python` now pins `mssql-python-odbc==18.6.2.2`, whose Linux wheels separate glibc and musl payloads while retaining the same ODBC Driver 18.6 binaries (#798, #807).
- **Faster Row Construction and Batch Fetching** - Optimized native Row construction and cursor bookkeeping reduce Python-side overhead in `fetchone()`, `fetchmany()`, and `fetchall()` (#558).
- **Faster Temporal Fetch Conversion** - DATE, TIME, and TIMESTAMP values use checked CPython constructors to reduce object-construction overhead (#795).
- **Reusable Result-Set Metadata** - Stable native column metadata is reused within a result set instead of being repeatedly described (#796).
- **Reduced Fetch Diagnostic Overhead** - Fetch diagnostics are captured once and reused instead of being redundantly rescanned after clean fetches (#809).
- **Faster Integer Row Indexing** - Exact integer indexes use a direct fast path while preserving existing behavior for booleans, subclasses, slices, and column names (#822).
- **Lower `fetchval()` Logging Overhead** - Disabled debug logging no longer dispatches unnecessary log calls from `fetchval()` (#823).

### Bug Fixes

- **Reliable Windows Shutdown** - Late ODBC environment cleanup no longer causes process-exit access violations after Python finalization (#783).
- **Preserved Leading BOM-Like Characters** - Bounded UTF-16 text keeps leading U+FEFF and U+FFFE code points as data across fetch paths (#793).
- **Clean Pooled Connection State** - Connections are rolled back and restored to autocommit before returning to the pool, preventing retained empty transactions (#777).
- **Safe Native Logging of Dynamic Metadata** - Server-controlled Arrow column metadata can no longer be interpreted as a native format string (#791).
- **UDT Parameter Binding** - SQL Server UDT parameter identities are resolved before scalar or array binding, including typed NULLs and statement reuse (#818).
- **Visible Unsupported Encoding Requests** - `setencoding()` now warns when a valid encoding request cannot be applied instead of silently ignoring it (#828).
- **Bounded Bulk Copy Timeouts** - Positive sub-second bulk-copy timeouts no longer become infinite and are rounded to one second (#804, via `mssql-python-rs`).
- **Safe Bulk Copy Identifiers** - Bulk-copy destinations, columns, and collations are parsed, quoted, and validated consistently (#804, via `mssql-python-rs`).

For more information, please visit the project link on Github: https://github.com/microsoft/mssql-python
 
If you have any feedback, questions or need support please mail us at mssql-python@microsoft.com.
 
## What's Next
 
As we continue to refine the driver and add new features, you can expect regular updates, optimizations, and bug fixes. We encourage you to contribute, provide feedback and report any issues you encounter, as this will help us improve the driver.
