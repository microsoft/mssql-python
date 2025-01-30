# MSSQL-Python

This repository contains the source code for the MSSQL-Python project, which provides Python bindings for the Microsoft SQL Server ODBC driver using `pybind11`. The project includes a C++ extension module that wraps the ODBC API and exposes it to Python, allowing for efficient database interactions.

## Build Status (`main`)

### Python Tests
[![Build Status](https://sqlclientdrivers.visualstudio.com/mssql-python/_apis/build/status%2FPython%20Tests?branchName=main)](https://sqlclientdrivers.visualstudio.com/mssql-python/_build/latest?definitionId=2024&branchName=main)

## Requirements

To build and run this project, you need the following:

- **Python**: Version 3.13

### Installing Requirements

To install the required Python packages, use the following command:

```sh
pip install -r requirements.txt
```

## Running Tests

To run the tests locally, use the following command:

```sh
python -m pytest -v
```

## Building the PYD binary

To build locally, follow steps in the `README.txt` inside `mssql_python\pybind` directory.