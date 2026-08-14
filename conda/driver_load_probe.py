"""DB-less ODBC driver-load proof for the conda test-before-live gate.

Importing ``mssql_python`` triggers the one-time native ODBC driver load
(``std::call_once`` in the C++ binding). To prove the driver payload is present
AND architecture-correct WITHOUT a live SQL Server, we additionally attempt a
connection to an unreachable local port and classify the failure:

* a connection / network failure  -> the native driver loaded and attempted TCP  (PASS)
* a "driver not found" style error -> the repackaged wheel is missing / mis-arch  (FAIL)

This gates on the actual DRIVER, not just the tiny ``mssql_python_odbc`` Python
shim, and needs no ``DB_CONNECTION_STRING`` secret. A real live ``SELECT 1`` still
runs separately whenever a server is wired.

Exit code 0 = driver loaded; non-zero = driver did not load (blocks publish).
"""

import sys

import mssql_python

# Substrings that only appear when the native ODBC driver could NOT be loaded /
# resolved (missing companion, wrong architecture, dangling shared object). A
# genuine connection failure (host unreachable / refused / login timeout) proves
# the opposite -> the driver loaded fine.
_DRIVER_MISSING_MARKERS = (
    "mssql-python-odbc",          # our own "install the driver package" DriverError
    "libmsodbcsql",               # posix driver .so failed to load
    "image not found",            # macOS dlopen failure
    "cannot open shared object",  # linux dlopen failure
    "can't open lib",             # unixODBC could not open the driver
    "no such file or directory",  # driver binary absent
)


def main() -> None:
    # Unreachable endpoint (nothing listens on TCP port 1) -> fast connection
    # refusal AFTER the driver has loaded and attempted the socket.
    conn_str = (
        "Server=127.0.0.1,1;Database=x;Uid=x;Pwd=x;"
        "Encrypt=no;TrustServerCertificate=yes;"
    )
    try:
        mssql_python.connect(conn_str)
    except Exception as exc:  # noqa: BLE001 - classified by message below, on purpose
        msg = str(exc).lower()
        if any(marker in msg for marker in _DRIVER_MISSING_MARKERS):
            sys.exit("DRIVER DID NOT LOAD / wrong arch: " + str(exc))
        print("DRIVER_LOADED (expected connection failure):", str(exc)[:200])
        return
    # Reaching a real server on 127.0.0.1:1 is not expected, but a successful
    # connect still proves the driver loaded.
    print("DRIVER_LOADED (unexpected connect success)")


if __name__ == "__main__":
    main()
