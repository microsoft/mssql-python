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
 
## What's new in v1.4.0

### Features

- **Bulk Copy Support** - High-performance bulk data loading API is now publicly available with support for large-scale ETL workloads, configurable batch sizes, column mappings, and identity/constraint handling.
- **Spatial Type Support** - Added support for geography, geometry, and hierarchyid spatial types.
- **mssql-py-core Upgrade** - Upgraded to mssql-py-core version 0.1.0 with enhanced connection string parameter support.
- **Type Annotations** - Added py.typed marker for improved type checking support.
- **Azure SQL Database Testing** - Added Azure SQL Database to PR validation pipeline matrix.

### Bug Fixes

- **VARCHAR Encoding Fix** - Fixed VARCHAR fetch failures when data length equals column size with non-ASCII CP1252 characters.
- **Segmentation Fault Fix** - Fixed segmentation fault when interleaving fetchmany and fetchone calls.
- **Date/Time Type Mappings** - Aligned date/time type code mappings with ODBC 18 driver source.
- **Pipeline Updates** - Updated OneBranch pipelines for new 1ES images and pool selection.
 
For more information, please visit the project link on Github: https://github.com/microsoft/mssql-python
 
If you have any feedback, questions or need support please mail us at mssql-python@microsoft.com.
 
## What's Next
 
As we continue to refine the driver and add new features, you can expect regular updates, optimizations, and bug fixes. We encourage you to contribute, provide feedback and report any issues you encounter, as this will help us improve the driver.
