# MSSQL-Python Driver Knowledge Base

## Overview

The mssql-python driver is a Python library for connecting to Microsoft SQL Server, currently in Alpha phase. It provides a Python Database API Specification (PEP 249) compliant interface, allowing Python applications to interact with SQL Server databases efficiently.

The driver uses the Microsoft ODBC Driver 18 for SQL Server (msodbcsql18.dll) internally and exposes its functionality through Python bindings via pybind11. This approach combines the performance benefits of the native ODBC driver with the ease of use of a Python interface.

## Architecture

The mssql-python driver follows a layered architecture:

1. **ODBC Layer**: The lowest layer interfacing directly with the ODBC18 driver.
2. **C++/Pybind11 Binding Layer**: Exposes ODBC driver functions as Python-callable methods.
3. **Python API Layer**: Implements the Python DB-API 2.0 (PEP 249) interface.

### ODBC Layer

The driver uses the Microsoft ODBC Driver 18 (msodbcsql18.dll), which is included within the package structure. Key functions are loaded dynamically at runtime using the Windows API functions `LoadLibraryW` and `GetProcAddress`.

Key ODBC driver files:
- `msodbcsql18.dll` - The core ODBC driver
- `msodbcdiag18.dll` - Diagnostics utility
- `msodbcsqlr18.rll` - Resources for localization

### C++/Pybind11 Binding Layer

The binding layer, implemented in `ddbc_bindings.cpp`, is the critical bridge between the low-level ODBC driver and the Python interface. This layer is responsible for:

1. **ODBC Driver Management**
   - Dynamic loading of the ODBC driver (msodbcsql18.dll) at runtime
   - Function pointer resolution using Windows API (`GetProcAddress`)
   - Error detection and propagation from the ODBC driver to Python

2. **Data Structure Mapping**
   - Custom structures that mirror ODBC structures but are pybind11-compatible
   - Specialized memory management for parameter binding and result sets
   - Carefully designed buffer management for various SQL data types

3. **Type Conversion System**
   - Bidirectional conversion between Python and SQL Server data types
   - Special handling for complex types like dates, decimals, and Unicode strings
   - SQL NULL value handling via Python's `None`

4. **Parameter Binding**
   - Sophisticated binding logic in `BindParameters` function
   - Type inference and validation for Python parameters
   - Memory allocation and management for parameter data
   - Special handling for various parameter types

5. **Result Set Processing**
   - Column binding via `SQLBindColums` function
   - Batch data fetching for improved performance
   - Buffer management for retrieving data from various column types
   - Conversion of fetched data to appropriate Python types

6. **Memory Management**
   - Smart pointer usage (`std::shared_ptr`) for automatic resource cleanup
   - Buffer reuse strategies to minimize allocations
   - Memory limit considerations for large result sets
   - Templated buffer allocation via `AllocateParamBuffer` function

7. **Error Handling**
   - Diagnostic information retrieval via SQL diagnostic records
   - Error code translation and message formatting
   - Exception propagation to Python with contextual information
   - Logging integration with Python's logging system

#### Key Technical Components

1. **Function Pointer Management**
   - The binding layer defines typedefs for all ODBC functions (e.g., `SQLAllocHandleFunc`)
   - Function pointers are initialized at runtime through dynamic loading
   - Wrapper functions with prefix `_wrap` provide Python-compatible interfaces

2. **Data Structures for Parameter and Result Binding**
   - `ParamInfo`: Stores information about parameters for binding
   - `NumericData`: Custom structure for handling decimal/numeric data
   - `ColumnBuffers`: Complex structure holding buffers for different data types
   - `ErrorInfo`: Structure for returning diagnostic information

3. **Type System and Conversion Logic**
   - `_map_sql_type` function maps Python types to SQL types
   - Type-specific binding logic in `BindParameters` function
   - Specialized conversions for datetime types, Unicode strings, and numeric types

4. **Buffer Management Techniques**
   - Multi-dimensional buffer vectors for batch operations
   - Type-specialized buffer allocation (charBuffers, wcharBuffers, etc.)
   - Memory size calculations and limitations
   - NULL indicator handling

5. **Special Type Handling**
   - Unicode vs. ASCII string differentiation
   - Numeric precision and scale handling
   - Datetime conversion between SQL and Python formats
   - Binary data management

6. **Module Export**
   - Uses pybind11's `PYBIND11_MODULE` to define the module
   - Exposes all necessary structures and functions to Python
   - Provides detailed docstrings for Python intellisense

#### Core Function Implementations

1. **Dynamic Driver Loading**
   ```cpp
   void LoadDriverOrThrowException() {
       // Get module path
       // Look for msodbcsql18.dll in relative path
       // Load the DLL with LoadLibraryW
       // Get function pointers with GetProcAddress
       // Verify all required functions are loaded
   }
   ```

2. **Parameter Binding**
   ```cpp
   SQLRETURN BindParameters(SQLHANDLE hStmt, const py::list& params,
                            const std::vector<ParamInfo>& paramInfos,
                            std::vector<std::shared_ptr<void>>& paramBuffers) {
       // For each parameter:
       //   - Determine the C type and SQL type
       //   - Allocate appropriate buffer
       //   - Convert Python value to C/C++ value
       //   - Bind the parameter using SQLBindParameter
       //   - Handle special cases (NULL, numeric, etc.)
   }
   ```

3. **Result Set Fetching**
   ```cpp
   SQLRETURN FetchBatchData(SQLHSTMT hStmt, ColumnBuffers& buffers, py::list& columnNames,
                            py::list& rows, SQLUSMALLINT numCols, SQLULEN& numRowsFetched) {
       // Fetch rows using SQLFetchScroll
       // For each row and column:
       //   - Check for NULL values
       //   - Extract data from appropriate buffer based on type
       //   - Convert to Python type
       //   - Append to result rows
   }
   ```

4. **Memory Allocation Helper**
   ```cpp
   template <typename ParamType, typename... CtorArgs>
   ParamType* AllocateParamBuffer(std::vector<std::shared_ptr<void>>& paramBuffers,
                                 CtorArgs&&... ctorArgs) {
       // Creates new instance of ParamType
       // Stores in shared_ptr for automatic cleanup
       // Returns raw pointer for ODBC API use
   }
   ```

5. **Python Module Definition**
   ```cpp
   PYBIND11_MODULE(ddbc_bindings, m) {
       // Define module documentation
       // Register C++ classes with Python
       // Expose C++ functions to Python
       // Set up function documentation
   }
   ```

### Python API Layer

The Python API layer provides a PEP 249 compliant interface for Python applications. This includes:

1. **Connection Objects**: Manage connections to the database
2. **Cursor Objects**: Execute queries and fetch results
3. **Type Objects**: Handle SQL-to-Python type conversions
4. **Exception Classes**: Provide standardized error reporting

## Core Components

### Connection Management

The `Connection` class (`connection.py`) implements the PEP 249 connection interface:

- **Initialization**: Sets up environment and connection handles
- **Statement Execution**: Creates cursors for executing SQL statements
- **Transaction Management**: Handles commit and rollback operations
- **Resource Management**: Ensures proper cleanup of resources

### Query Execution and Result Handling

The `Cursor` class (`cursor.py`) implements the PEP 249 cursor interface:

- **Statement Execution**: Handles execution of SQL statements with or without parameters
- **Result Set Navigation**: Methods for fetching rows (fetchone, fetchmany, fetchall)
- **Metadata Access**: Provides information about query results (description)
- **Parameter Binding**: Maps Python values to SQL parameters

### Type System

The driver provides robust support for SQL Server data types, mapping them to appropriate Python types:

| SQL Server Type | Python Type |
|----------------|-------------|
| INTEGER | int |
| VARCHAR/CHAR | str |
| NVARCHAR/NCHAR | str |
| FLOAT/REAL | float |
| DECIMAL/NUMERIC | decimal.Decimal |
| DATE | datetime.date |
| TIME | datetime.time |
| DATETIME/DATETIME2 | datetime.datetime |
| BIT | bool |
| BINARY/VARBINARY | bytes |
| GUID | uuid.UUID |

### Error Handling

Following PEP 249, the driver provides a hierarchy of exception classes:

- **Warning**: For non-fatal issues
- **Error**: Base class for all errors
- **InterfaceError**: For errors in the database interface
- **DatabaseError**: Base class for errors related to the database
- **DataError**: For problems with the data
- **OperationalError**: For errors related to the database operation
- **IntegrityError**: For errors that violate integrity constraints
- **InternalError**: For internal database errors
- **ProgrammingError**: For programming errors like syntax errors
- **NotSupportedError**: For features not supported by the database

## Building the Driver

The mssql-python driver is designed to support multiple architectures (x86, x64, and ARM64) on Windows. The build system uses pybind11 and cibuildwheel to create architecture-specific wheels.

### Build System Overview

The driver uses a combination of two key technologies for building:

1. **Pybind11**: For creating the C++ bindings that expose the ODBC driver functionality to Python.
2. **cibuildwheel**: For automatically building wheels for different architectures and Python versions.

### Local Development Build

For local development, you can build the extension module with:

```bash
# Install development dependencies
pip install pybind11 setuptools wheel

# Build the extension in-place
python setup.py build_ext --inplace
```

This builds the extension module (`ddbc_bindings.pyd`) directly in the mssql_python package directory, using your current architecture.

### Creating Wheels

To build a wheel package for your current architecture:

```bash
python setup.py bdist_wheel
```

### Building for Multiple Architectures

To build for multiple architectures and Python versions, cibuildwheel is used:

```bash
# Install cibuildwheel
pip install cibuildwheel

# Build wheels
python -m cibuildwheel --output-dir wheelhouse
```

The cibuildwheel configuration is defined in `pyproject.toml`, specifying which architectures and Python versions to target.

### Automated Builds with CI/CD

The repository includes an Azure DevOps pipeline configuration (`build-and-release-pipeline.yml`) that:

1. Builds wheels for all supported architectures (x86, x64, ARM64) and Python versions
2. Runs tests on each wheel
3. Publishes the wheels to PyPI when building from a release branch

This automated process ensures that all wheels are built in consistent environments and thoroughly tested before release.

### Architecture-Specific DLLs

The driver includes architecture-specific ODBC driver DLLs organized in the following directories:

- `mssql_python/dlls/win32/` - 32-bit DLLs
- `mssql_python/dlls/win64/` - 64-bit DLLs
- `mssql_python/dlls/winarm64/` - ARM64 DLLs

When building a wheel, only the DLLs for the target architecture are included, keeping the wheel size optimized.

### Runtime Architecture Detection

The C++ binding layer includes architecture detection logic that:

1. Determines the current architecture at runtime
2. Loads the appropriate architecture-specific DLLs
3. Exposes the architecture information to Python code via the `__architecture__` attribute

This enables the same Python code to work seamlessly across different architectures, with the DLL loading handled transparently.

## Requirements

- Python 3.13
- Windows operating system
- CMake (for building from source)

## References

- [Python Database API Specification v2.0 (PEP 249)](https://www.python.org/dev/peps/pep-0249/)
- [Microsoft ODBC Driver for SQL Server](https://docs.microsoft.com/en-us/sql/connect/odbc/microsoft-odbc-driver-for-sql-server)
- [pybind11 Documentation](https://pybind11.readthedocs.io/)