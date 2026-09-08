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
 
## What's new in v1.14.0

### Enhancements

- **Faster Parameter Detection and Execution** - Parameter type detection and binding now run in a native C++ pipeline, substantially reducing per-parameter overhead and improving throughput for wide and batched `execute()` workloads (#549).

### Bug Fixes

- **Bulk Copy Accepts `timeout=0`** - `bulkcopy()` now treats zero as no timeout, matching the BCP API contract, while continuing to reject negative, non-integer, and boolean values (#698).
- **Arrow Reader Fetch Exceptions Are Preserved** - Defensive cursor cleanup no longer masks the original exception raised while fetching Arrow result batches (#718).
- **Decimal Conversion Errors No Longer Expose Parameter Values** - `executemany()` Decimal conversion failures now report metadata only, preventing parameter rows and sensitive values from leaking through exception messages or chained tracebacks (#719).
- **Arrow View Types Work in Bulk Copy** - Polars `string_view` columns exported through the Arrow C Data Interface now round-trip correctly through `bulkcopy_arrow()` (#717, via `mssql_py_core`).
- **`connect(timeout=)` Sets the Login Timeout** - The constructor timeout now bounds connection attempts as documented instead of setting the per-statement query timeout; timeout validation is consistent across both APIs (#728).
- **Windows Extension Loading Uses Interpreter Architecture** - The loader now selects the native binary using the Python interpreter architecture, fixing x64 Python on Windows ARM64 hosts and avoiding fallback warnings on stdout (#727).

For more information, please visit the project link on Github: https://github.com/microsoft/mssql-python
 
If you have any feedback, questions or need support please mail us at mssql-python@microsoft.com.
 
## What's Next
 
As we continue to refine the driver and add new features, you can expect regular updates, optimizations, and bug fixes. We encourage you to contribute, provide feedback and report any issues you encounter, as this will help us improve the driver.
