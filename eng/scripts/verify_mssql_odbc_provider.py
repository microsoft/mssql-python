# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import os
from pathlib import Path

import mssql_python


def require(condition, message, details):
    if not condition:
        raise RuntimeError(f"{message}: {details!r}")


def main():
    info = mssql_python.get_native_provider_info()
    require(info.get("id") == "mssql-odbc", "Unexpected native provider", info)
    require(info.get("package") == "mssql_py_core", "Unexpected provider package", info)
    require(info.get("source") == "environment", "Unexpected provider source", info)
    driver_path = Path(info["driver_path"])
    require("mssqlodbc" in driver_path.name.lower(), "Unexpected driver filename", info)
    require(driver_path.is_file(), "Driver path does not exist", info)

    with mssql_python.connect(os.environ["DB_CONNECTION_STRING"]) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            row = cursor.fetchone()
            require(
                row is not None and row[0] == 1, "Provider query returned an unexpected row", row
            )

    loaded_info = mssql_python.get_native_provider_info()
    require(loaded_info.get("id") == "mssql-odbc", "Loaded provider changed", loaded_info)
    require(loaded_info.get("frozen") is True, "Loaded provider is not frozen", loaded_info)
    print("MSSQL_ODBC_PREFLIGHT_OK", loaded_info, flush=True)


if __name__ == "__main__":
    main()
