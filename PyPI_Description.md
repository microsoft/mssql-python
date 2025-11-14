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

## What's new in v0.14.0

### New Features
- **50-60% Faster Fetching:** Major optimizations including direct UTF-16 decoding, Python C API usage, and cached converters deliver dramatic performance gains for large result sets (100K+ rows), with **1.4-1.7× improvement** for very large datasets.
- **Connection String Validation:** Intelligent parser with allowlist validation, synonym normalization, and clear error messages for malformed strings. **Breaking change:** Unknown parameters now raise errors instead of being silently ignored.
- **Enhanced DECIMAL Precision:** Increased precision support from 15 to 38 digits (SQL Server maximum) with proper binary representation for high-precision calculations.
- **Comprehensive Logging:** Unified Python-C++ logging framework with `setup_logging()` API for detailed diagnostics with zero overhead when disabled.
- **Connection Attribute Control:** New `Connection.set_attr()` method for fine-grained control over ODBC connection attributes, isolation levels, and timeouts (pyodbc-compatible API).
- **XML Data Type:** Comprehensive support for SQL Server `XML` type, including efficient streaming for large documents.
- **DECIMAL Scientific Notation:** Improved handling of decimal values in scientific notation to prevent SQL Server conversion errors.

### Bug Fixes
- **Access Token Management:** Fixed Microsoft Entra ID authentication token handling to eliminate corruption in concurrent scenarios.
- **Decimal executemany Fix:** Resolved type inference issues when batch inserting Decimal values.

⚠️ **Breaking Change:** Connection string validation now raises `ConnectionStringParseError` for unknown/misspelled parameters. Review connection strings before upgrading.

For more information, please visit the project link on Github: https://github.com/microsoft/mssql-python

If you have any feedback, questions or need support please mail us at mssql-python@microsoft.com.

## What's Next

As we continue to develop and refine the driver, you can expect regular updates that will introduce new features, optimizations, and bug fixes. We encourage you to contribute, provide feedback and report any issues you encounter, as this will help us improve the driver ahead of General Availability.
