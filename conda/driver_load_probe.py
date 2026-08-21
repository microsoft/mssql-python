"""DB-less ODBC driver-load proof for the conda test-before-live gate.

Importing ``mssql_python`` and issuing the first ``connect()`` triggers the
one-time native ODBC driver load (``std::call_once`` in the C++ binding). To
prove the driver payload is present AND architecture-correct WITHOUT a live SQL
Server, we attempt a connection to an unreachable local port and classify the
failure.

FAIL-CLOSED classification (this is the whole point of the probe):

* We treat the outcome as PASS **only** when there is positive proof the native
  driver loaded -- either a clean connect, or a *connection-stage* diagnostic
  that only the loaded ``msodbcsql`` driver can emit (its ``[Microsoft][ODBC
  Driver 18 for SQL Server]`` branding, a SQL Server network provider error, a
  TLS handshake error, or a login / auth outcome). See ``_DRIVER_LOADED_MARKERS``.
* Every other exception is treated as a load failure -> non-zero exit. This
  includes the C++ ``LoadDriverOrThrowException`` family
  ("Failed to load the driver...", "Failed to load library: <path>",
  "Failed to load required function pointers...", "ODBC driver not found...",
  the ``mssql-auth.dll`` errors) and the macOS ``dlopen`` / ``dlerror`` detail --
  none of which contain a loaded-driver marker, so a broken / missing /
  mis-architecture driver can never report PASS.

This gates on the actual DRIVER, not just the tiny ``mssql_python_odbc`` Python
shim, and needs no ``DB_CONNECTION_STRING`` secret. A real live ``SELECT 1`` still
runs separately whenever a server is wired.

Exit code 0 = driver loaded; non-zero = driver did not load (blocks publish).
"""

import sys

# Positive signals: the native ODBC driver LOADED and reached the network / TLS
# / auth stage (or connected). These are the ONLY outcomes that count as PASS.
# All markers are matched case-insensitively.
_DRIVER_LOADED_MARKERS = (
    # The loaded msodbcsql driver brands every diagnostic it emits; a driver
    # that failed to load / link / resolve its symbols never gets far enough to
    # print this, so it is the strongest single proof of a successful load.
    "odbc driver 18 for sql server",
    "microsoft][odbc",
    # SQL Server network / transport providers -- reached only after load.
    "tcp provider",
    "named pipes provider",
    "shared memory provider",
    "sql server network interfaces",
    # Connection / login outcomes that prove the handshake was attempted.
    "login timeout expired",
    "a network-related or instance-specific error",
    "server was not found",
    "server is not found",
    "actively refused",  # Windows WSAECONNREFUSED (target port closed)
    "connection refused",  # posix ECONNREFUSED (target port closed)
    "communication link failure",
    "unable to establish",
    "login failed for user",  # authentication stage reached
    "cannot open database",  # server reached, database validation
    # TLS handshake reached -> both the driver and its crypto backend loaded.
    "ssl provider",
    "ssl security error",
    "certificate",
)

# Negative signals: the native driver did NOT load / link / resolve. Listed only
# to produce a clearer FAIL message -- classification is allowlist-based, so an
# unrecognized exception still fails closed even if it matches nothing here.
_DRIVER_LOAD_FAILURE_MARKERS = (
    "failed to load the driver",
    "failed to load library",
    "failed to load required function pointers",
    "odbc driver not found",
    "mssql-auth.dll",
    "mssql-python-odbc",
    "cannot open shared object",  # linux dlopen failure
    "image not found",  # macOS dlopen failure
    "no such file or directory",  # driver binary absent
    "can't open lib",  # unixODBC could not open the driver
    "unsupported architecture",
    "unsupported platform",
)


def driver_loaded(exc):
    """FAIL-CLOSED classifier for the connect outcome.

    Returns ``True`` only when there is positive proof the native ODBC driver
    loaded: a clean connect (``exc is None``) or a connection-stage diagnostic
    that the loaded driver alone can emit. Every other exception -- including the
    C++ "Failed to load the driver..." family and anything unrecognized --
    returns ``False`` so the probe exits non-zero.
    """
    if exc is None:
        return True
    msg = str(exc).lower()
    return any(marker in msg for marker in _DRIVER_LOADED_MARKERS)


def describe(exc):
    """Short, human-readable reason string for the probe's stdout / exit line."""
    if exc is None:
        return "clean connect"
    msg = str(exc)
    low = msg.lower()
    for marker in _DRIVER_LOAD_FAILURE_MARKERS:
        if marker in low:
            return "driver load failure -> " + msg[:300]
    return msg[:300]


def main():
    # Deferred so this module can be imported (and ``driver_loaded`` unit-tested)
    # WITHOUT triggering the native ``mssql_python`` import, which needs the
    # compiled extension + driver payload.
    import mssql_python

    # Unreachable endpoint (nothing listens on TCP port 1) -> the driver loads,
    # attempts the socket, and fails fast at the network stage. The loopback:1 is a
    # dummy DB-less probe target, never a live endpoint.
    conn_str = "Server=127.0.0.1,1;Database=x;Uid=x;Pwd=x;Encrypt=no;TrustServerCertificate=yes;"  # DevSkim: ignore DS162092
    outcome = None
    try:
        conn = mssql_python.connect(conn_str)
        # Reaching a real server on 127.0.0.1:1 is not expected, but a successful
        # connect still proves the driver loaded. Close it and pass.
        try:
            conn.close()
        except Exception:  # noqa: BLE001 - best-effort cleanup only
            pass
    except Exception as exc:  # noqa: BLE001 - deliberately classified below
        outcome = exc

    if driver_loaded(outcome):
        print("DRIVER_LOADED (" + describe(outcome) + ")")
        return
    sys.exit("DRIVER DID NOT LOAD / wrong arch / missing companion: " + describe(outcome))


if __name__ == "__main__":
    main()
