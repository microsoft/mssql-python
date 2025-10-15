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

## What's new in v0.13.1

- **Authentication Reliability:** Fixed token handling for Microsoft Entra ID authentication to ensure stable and reliable connections.
- **Timezone Preservation:** Removed forced UTC conversion for `datetimeoffset` values, preserving original timezone information in Python `datetime` objects for accurate cross-timezone data handling.
- **Connection Pooling Stability:** Enhanced pool shutdown mechanism with proper tracking to prevent resource leaks and ensure cleanup operations execute reliably.
- **Predictable Type Handling:** Refined UUID string parameter handling to prevent automatic type coercion, ensuring strings are processed as intended.

For more information, please visit the project link on Github: https://github.com/microsoft/mssql-python

If you have any feedback, questions or need support please mail us at mssql-python@microsoft.com.

## What's Next

As we continue to develop and refine the driver, you can expect regular updates that will introduce new features, optimizations, and bug fixes. We encourage you to contribute, provide feedback and report any issues you encounter, as this will help us improve the driver ahead of General Availability.
