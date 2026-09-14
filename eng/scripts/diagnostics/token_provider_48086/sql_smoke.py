"""Actual ARM64 native clients against this job's owned SQL Server 2022 container."""

import concurrent.futures
import json
import os
from pathlib import Path
import sys

from contracts import require, save, sha, configure_source_imports, import_source_package

ROOT = configure_source_imports(__file__)
connect = import_source_package(ROOT, "mssql_python").connect


def main():
    connection = os.environ["DB_CONNECTION_STRING"]
    with connect(connection, autocommit=True) as setup:
        with setup.cursor() as cursor:
            cursor.execute("SELECT CONVERT(int, SERVERPROPERTY('ProductMajorVersion'))")
            major = cursor.fetchone()[0]
            require(major == 16, "Owned SQL service must be SQL Server 2022")
            cursor.execute("CREATE TABLE qualification48086 (id int PRIMARY KEY)")
    try:

        def operation(index):
            conn = connect(connection)
            native = conn._conn
            try:
                require(
                    conn.autocommit is False and native.get_autocommit() is False,
                    "Default native autocommit is not False",
                )
                conn.autocommit = True
                require(
                    conn.autocommit is True and native.get_autocommit() is True,
                    "Native autocommit did not become True",
                )
                conn.autocommit = False
                require(
                    conn.autocommit is False and native.get_autocommit() is False,
                    "Native autocommit did not become False",
                )
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    require(cursor.fetchone()[0] == 1, "Native query failed")
                    cursor.execute("INSERT INTO qualification48086 VALUES (?)", (index,))
                    conn.commit()
                    cursor.execute("INSERT INTO qualification48086 VALUES (?)", (-index - 1,))
            finally:
                conn.close()
            require(conn.closed, "Connection remained open")
            return native

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            natives = list(executor.map(operation, range(20)))
        require(len({id(n) for n in natives}) == 20, "Native connections were reused")
        with connect(connection) as check:
            with check.cursor() as cursor:
                cursor.execute("SELECT id FROM qualification48086 ORDER BY id")
                rows = [row[0] for row in cursor.fetchall()]
        require(rows == list(range(20)), "Commit or rollback-on-close contract failed")
        libraries = sorted(
            {
                line.split()[-1]
                for line in Path("/proc/self/maps").read_text().splitlines()
                if "libmsodbcsql" in line and line.split()[-1].startswith("/")
            }
        )
        require(bool(libraries), "No loaded ODBC driver identity")
        require(
            all(
                Path(path).resolve().is_relative_to(Path.cwd() / "mssql_python_odbc")
                for path in libraries
            ),
            "Loaded an external ODBC driver",
        )
        save(
            sys.argv[1],
            {
                "sql_major": major,
                "operations": 20,
                "workers": 8,
                "distinct_native": 20,
                "committed_ids": rows,
                "negative_ids_after_close": 0,
                "autocommit_transitions": [False, True, False],
                "authentication": "owned local SQL credentials; NOT live Entra authentication",
                "loaded_odbc_libraries": {path: sha(Path(path).read_bytes()) for path in libraries},
            },
        )
    finally:
        with connect(connection, autocommit=True) as cleanup:
            with cleanup.cursor() as cursor:
                cursor.execute("DROP TABLE qualification48086")


if __name__ == "__main__":
    if sys.argv[1:] == ["--verify-source-imports"]:
        package = import_source_package(ROOT, "mssql_python")
        print(json.dumps({"root": str(ROOT), "package_origin": package.__file__}))
    else:
        main()
