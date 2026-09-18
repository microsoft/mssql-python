# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import os
from pathlib import Path

import mssql_python


def main():
    info = mssql_python.get_native_provider_info()
    assert info["id"] == "mssql-odbc", info
    assert info["package"] == "mssql_py_core", info
    assert info["source"] == "environment", info
    driver_path = Path(info["driver_path"])
    assert "mssqlodbc" in driver_path.name.lower(), info
    assert driver_path.is_file(), info

    with mssql_python.connect(os.environ["DB_CONNECTION_STRING"]) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            assert cursor.fetchone()[0] == 1

    loaded_info = mssql_python.get_native_provider_info()
    assert loaded_info["id"] == "mssql-odbc", loaded_info
    assert loaded_info["frozen"] is True, loaded_info
    print("MSSQL_ODBC_PREFLIGHT_OK", loaded_info, flush=True)


if __name__ == "__main__":
    main()
