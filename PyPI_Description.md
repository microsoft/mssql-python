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
 
## What's new in v1.9.0

### Enhancements

- **Row Objects in Bulk Copy** - `bulkcopy` now accepts `Row` objects (and lists) directly, automatically converting each row to a tuple so data fetched from a query can be bulk-inserted without manual conversion (#615).

### Bug Fixes

- **macOS / Linux Import Failure** - simdutf is now always statically linked via FetchContent, embedding its symbols into the extension and fixing import failures on machines without simdutf installed at the CI build path (#608).
- **Incorrect Type Fallback for NULL Parameters** - `SQLDescribeParam` results are now cached per statement when binding `NULL` parameters, fixing incorrect type fallbacks for all-NULL columns and VARBINARY types while also eliminating redundant server round-trips (#614).
- **executemany Large Decimal Handling** - Fixed a `SQL_C_NUMERIC` type mismatch that caused runtime errors when inserting `Decimal` values outside the SQL Server `MONEY` range via `executemany` (#611).
- **Exception Pickling** - All DB-API exception subclasses and `ConnectionStringParseError` now implement `__reduce__`, so they survive pickle/unpickle round-trips with all attributes preserved (#616).
- **PRINT Messages in nextset()** - Diagnostic messages (e.g., SQL Server PRINT output) from subsequent result sets are now captured correctly when `SQL_SUCCESS_WITH_INFO` is returned during `nextset()` (#618).
- **Row Objects in executemany DAE Path** - `executemany` now converts `Row` objects to tuples in the DAE fallback path, fixing failures when writing to `varchar(max)` columns (#630).
- **Static Type-Checking of Fetch Methods** - `fetchone`, `fetchmany`, and `fetchall` are no longer reassigned as instance attributes, fixing type-checking failures under `ty` and other static type checkers (#631).

For more information, please visit the project link on Github: https://github.com/microsoft/mssql-python
 
If you have any feedback, questions or need support please mail us at mssql-python@microsoft.com.
 
## What's Next
 
As we continue to refine the driver and add new features, you can expect regular updates, optimizations, and bug fixes. We encourage you to contribute, provide feedback and report any issues you encounter, as this will help us improve the driver.
