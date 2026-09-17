# mssql-python-odbc

Internal implementation package for [**mssql-python**](https://pypi.org/project/mssql-python/) —
Microsoft's official Python driver for SQL Server, Azure SQL, and SQL databases in Fabric.

This package ships the platform-specific **Microsoft ODBC Driver 18 for SQL Server** binaries
(and the supporting runtime libraries they depend on) as a standalone, pure-data wheel, so that
`mssql-python` does not have to bundle them inside its own wheel.

## Not intended for direct use

Do **not** install this package directly. Install
[**mssql-python**](https://pypi.org/project/mssql-python/) instead — it declares the correct
pinned dependency on `mssql-python-odbc` and loads these binaries automatically:

```
pip install mssql-python
```

## Documentation

For usage, API documentation, and source code, see the
[mssql-python project on GitHub](https://github.com/microsoft/mssql-python).

## License information

This package redistributes proprietary Microsoft binaries under their respective license terms.
The full license text also ships inside every wheel (in the wheel metadata under `.dist-info/licenses/`).

- [Microsoft ODBC Driver for SQL Server License](https://github.com/microsoft/mssql-python/blob/main/mssql_python_odbc/licenses/MICROSOFT_ODBC_DRIVER_FOR_SQL_SERVER_LICENSE.txt)
- [Microsoft Visual C++ Redistributable (Visual Studio) License](https://github.com/microsoft/mssql-python/blob/main/mssql_python_odbc/licenses/MICROSOFT_VISUAL_STUDIO_LICENSE.txt)
