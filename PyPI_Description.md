# mssql-python

mssql-python is a new first-party SQL Server driver for Python that has all of the benefits of a fresh start while preserving a familiar experience for developers.

## What makes mssql-python different?

### Powered by DDBC – Direct Database Connectivity

Most Python SQL Server drivers, including pyodbc, route calls through the Driver Manager, which has slightly different implementations across Windows, macOS, and Linux. This results in inconsistent behavior and capabilities across platforms. Additionally, the Driver Manager must be installed separately, creating friction for both new developers and when deploying applications to servers.

At the heart of the driver is DDBC (Direct Database Connectivity) — a lightweight, high-performance C++ layer that replaces the platform’s Driver Manager.

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

To expose the DDBC engine to Python, mssql-python uses PyBind11 – a modern C++ binding library, instead of ctypes. With ctypes, every call between Python and the ODBC driver involved costly type conversions, manual pointer management, resulting in slow and potentially unsafe code.

PyBind11 provides:

- Native-speed execution with automatic type conversions
- Memory-safe bindings
- Clean and Pythonic API, while performance-critical logic remains in robust, maintainable C++.

## Public Preview Release

We are currently in **Public Preview**.

## What's new in v0.10.0

- **SUSE Linux Support:** Added full support for SUSE and openSUSE distributions alongside existing other Linux distros support, broadening enterprise Linux compatibility.
- **Context Manager Support:** Implemented Python `with` statement support for Connection and Cursor classes with automatic transaction management and resource cleanup.
- **Large Text Streaming:** Added Data At Execution (DAE) support for streaming large text parameters (`NVARCHAR(MAX)`, `VARCHAR(MAX)`), eliminating memory constraints for bulk text `execute()` operations.
  - `VARBINARY(MAX)` support to follow alongwith streaming support for fetch operations.
- **Enhanced Unicode Handling:** Improved emoji and international character support with robust UTF-16 encoding for reliable multilingual data processing.
- **PyODBC Compatibility:** Enhanced API compatibility with pyodbc including:
  - DB-API 2.0 exception classes: `Warning`, `Error`, `InterfaceError`, `DatabaseError`, `DataError`, `OperationalError`, `IntegrityError`, `InternalError`, `ProgrammingError`, `NotSupportedError`
  - Context manager support with `with` statements for Connection and Cursor
  - Encoding configuration APIs: `setencoding()`, `getencoding()`, `setdecoding()`, `getdecoding()`
  - Cursor navigation APIs: `next()`, `__iter__()`, `scroll()`, `skip()`, `fetchval()`
  - Cursor attributes: `rownumber`, `messages`
  - Additional methods: `cursor.commit()`, `cursor.rollback()`, `table()`

For more information, please visit the project link on Github: https://github.com/microsoft/mssql-python

If you have any feedback, questions or need support please mail us at mssql-python@microsoft.com.

## What's Next

As we continue to develop and refine the driver, you can expect regular updates that will introduce new features, optimizations, and bug fixes. We encourage you to contribute, provide feedback and report any issues you encounter, as this will help us improve the driver ahead of General Availability.
