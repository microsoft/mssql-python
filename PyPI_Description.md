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
 
## What's new in v1.13.0

### Enhancements

- **ODBC Driver Now Ships Exclusively via `mssql-python-odbc`** - The `libs/` fallback introduced in v1.12.0 has been removed. `mssql-python` now hard-depends on `mssql-python-odbc==18.6.2.1`; `pip install mssql-python` still Just Works and transparently pulls the driver package. Wheels are smaller and driver binaries are managed independently (#693).
- **Apache Arrow Bulk Copy** - New `Cursor.bulkcopy_arrow(table_name, source)` method for high-performance bulk loading from `pyarrow.Table`, `RecordBatch`, or any object exposing the Arrow C Data Interface, avoiding Python row materialization. The classic `bulkcopy()` now raises `TypeError` for Arrow inputs and steers users to the new method (#665).
- **`token_provider=` Parameter for Azure Identity** - `connect()` now accepts a `token_provider` object with a `.get_token(scope)` method, enabling `DefaultAzureCredential`, `AzureCliCredential`, `ManagedIdentityCredential`, and any custom credential from `azure-identity`. Bulk copy re-acquires a fresh token per operation. Mutually exclusive with `Authentication=` in the connection string (#603).
- **Identity-Aware Connection Pooling with Token-Expiry Refresh** - The pool now keys on the security context (connection string + identity discriminator) so a connection authenticated as user A can never be handed to user B. Token acquisition is deferred to pool misses, and pooled connections whose token is within 5 minutes of expiry are refreshed automatically (#660).

### Bug Fixes

- **Silent Zero-Row `executemany` Batches on Late NULLs** - Fixed numeric array parameter binding paths (`TINYINT` / `SMALLINT` / `INT` / `FLOAT`) that left indicator slots uninitialized when a NULL appeared partway through the batch, causing the batch to insert zero rows without raising (#702, issue #670).
- **`SQL_WVARCHAR` Output Converter Applied to Non-String Columns** - The legacy `SQL_WVARCHAR` catch-all no longer runs against `INT` / `DECIMAL` / `DATE` columns. Registering a single `SQL_WVARCHAR` converter used to mangle every non-string column value; the fallback is now gated on `str`/`bytes` mapped types, matching `Row._apply_output_converters` (#692, issue #691).
- **Integer-Keyed Output Converters Now Fire** - `Connection.add_output_converter(SQL_DECIMAL, ...)` and any other integer ODBC SQL type code as a key now dispatch correctly. Previously the converter dictionary was keyed only by Python type, so integer-keyed registrations were silently stored but never invoked, diverging from `pyodbc` and from the driver's own documentation. Integer keys take precedence over Python-type keys, and `SQL_DECIMAL` vs `SQL_NUMERIC` are dispatched distinctly (#690, issue #684).
- **`RecordBatchReader.Close()` for Arrow Result Sets** - `Cursor.arrow_reader()` now returns a wrapped reader whose `.close()` stops fetching, releases the server-side cursor, resets cursor state, and leaves the parent cursor usable. Supports idempotent close and context-manager usage (#644, issue #643).
- **`AttributeError` on Partially-Initialized `Cursor` Cleanup** - `Cursor.__init__` now sets `self.closed = False` and `self.hstmt = None` before any code that can raise, `close()` defends with `getattr(self, "closed", True)`, and `__del__` uses the correct `sys.is_finalizing()` guard, so half-constructed cursors no longer emit unraisable exceptions during garbage collection (#646, issue #642).

For more information, please visit the project link on Github: https://github.com/microsoft/mssql-python
 
If you have any feedback, questions or need support please mail us at mssql-python@microsoft.com.
 
## What's Next
 
As we continue to refine the driver and add new features, you can expect regular updates, optimizations, and bug fixes. We encourage you to contribute, provide feedback and report any issues you encounter, as this will help us improve the driver.
