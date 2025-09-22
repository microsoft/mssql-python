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

## What's new in v0.11.0

- **Database Metadata & Introspection:** Added comprehensive `getInfo()` method and extensive catalog APIs (`SQLGetTypeInfo`, `SQLProcedures`, `SQLForeignKeys`, `SQLColumns`) for complete database schema discovery and introspection capabilities.
- **Advanced Parameter Management:** Implemented `setinputsizes()` with SQL type constants and enhanced parameter validation through the `SQLTypes` class for precise type control in parameterized queries.
- **Large Data Streaming Enhancements:** Extended streaming support to VARBINARY(MAX) and VARCHAR(MAX) across all fetch operations (`fetchone`, `fetchmany`, `fetchall`) with improved chunked retrieval for efficient memory usage.
- **Output Data Conversion System:** Introduced flexible output converter framework with `add_output_converter()`, `remove_output_converter()`, and related methods for customizable data transformations during result fetching.
- **Connection-Level Execute:** Added direct `execute()` method to Connection class for streamlined query execution without explicit cursor management.
- **Financial Data Type Support:** Comprehensive support for SQL Server MONEY and SMALLMONEY types with proper boundary value handling and decimal precision.
- **Enhanced Configuration APIs:** Added connection timeout control, decimal separator configuration (`getDecimalSeperator()`, `setDecimalSeperator()`), and improved global variable management.

For more information, please visit the project link on Github: https://github.com/microsoft/mssql-python

If you have any feedback, questions or need support please mail us at mssql-python@microsoft.com.

## What's Next

As we continue to develop and refine the driver, you can expect regular updates that will introduce new features, optimizations, and bug fixes. We encourage you to contribute, provide feedback and report any issues you encounter, as this will help us improve the driver ahead of General Availability.
