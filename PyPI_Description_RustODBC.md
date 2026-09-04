# mssql-python-rust-odbc

Internal implementation package for [mssql-python](https://pypi.org/project/mssql-python/).

It ships the platform-specific [mssql-odbc](https://github.com/microsoft/mssql-rs) driver
binaries so that `mssql-python` does not have to bundle them in its own wheel. This package
is not intended for direct use.

To use the `mssql-odbc` driver with `mssql-python`, install `mssql-python` and select the
provider:

```python
import mssql_python
mssql_python.native_provider = "mssql-odbc"
```

or set the `MSSQL_PYTHON_NATIVE_PROVIDER` environment variable to `mssql-odbc` before
connecting. Installing `mssql-python-rust-odbc` directly is only required if
`mssql-python` does not already declare it as a dependency for your platform.
