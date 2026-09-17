"""Fixed-size independent-client controls, not retries of the pytest failures."""

import argparse
import json
import os
import pathlib
import shutil
import subprocess
import time

from mssql_python import OperationalError, connect, ddbc_bindings
from mssql_python.connection_string_parser import _ConnectionStringParser

SQL = """
SET NOCOUNT ON;
DECLARE @utc datetime2 = SYSUTCDATETIME();
DECLARE @ticks bigint = (SELECT ms_ticks FROM sys.dm_os_sys_info);
WAITFOR DELAY '00:00:05';
SELECT DATEDIFF_BIG(microsecond, @utc, SYSUTCDATETIME()) AS server_elapsed_us,
       (SELECT ms_ticks FROM sys.dm_os_sys_info) - @ticks AS server_elapsed_ms,
       1 AS completed;
"""


def mssql_trial(connection_string, timeout):
    with connect(connection_string, timeout=10, autocommit=True) as connection:
        connection.timeout = timeout
        with connection.cursor() as cursor:
            ret, value = ddbc_bindings._diagnostic_query_timeout(cursor.hstmt)
            if ret not in (0, 1) or value != timeout:
                raise RuntimeError("Control query-timeout readback mismatch")
            before = time.monotonic_ns()
            try:
                cursor.execute(SQL)
                execute_done = time.monotonic_ns()
                row = cursor.fetchone()
                after = time.monotonic_ns()
                result = {
                    "execute_seconds": (execute_done - before) / 1e9,
                    "fetch_seconds": (after - execute_done) / 1e9,
                    "total_seconds": (after - before) / 1e9,
                    "server_values": list(row) if row is not None else None,
                    "timeout_error": False,
                }
            except OperationalError as exc:
                result = {
                    "total_seconds": (time.monotonic_ns() - before) / 1e9,
                    "error_type": type(exc).__name__,
                    "timeout_error": "timeout" in str(exc).lower(),
                }
            return {**result, "effective_query_timeout": value}


def sqlcmd_trial(params, timeout, docker):
    env = os.environ.copy()
    password = params.get("pwd", params.get("password"))
    if password:
        env["SQLCMDPASSWORD"] = password
    if docker:
        command = [
            "docker",
            "exec",
            "--env",
            "SQLCMDPASSWORD",
            "sqlserver",
            "/opt/mssql-tools18/bin/sqlcmd",
            "-S",
            "localhost",
            "-U",
            "SA",
            "-C",
        ]
    else:
        executable = shutil.which("sqlcmd")
        if not executable:
            raise RuntimeError("Independent sqlcmd control is unavailable")
        command = [executable, "-S", params["server"]]
        if params.get("uid"):
            command.extend(["-U", params["uid"]])
        else:
            command.append("-E")
        if params.get("trustservercertificate", "").lower() in ("yes", "true"):
            command.append("-C")
    command.extend(["-b", "-l", "10", "-t", str(timeout), "-h", "-1", "-W", "-Q", SQL])
    before = time.monotonic_ns()
    process = subprocess.run(
        command,
        env=env,
        capture_output=True,
        timeout=20,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    text = process.stdout + process.stderr
    # Connection failures can print endpoint information; only numeric output is retained.
    numeric_rows = [
        line.strip()
        for line in process.stdout.splitlines()
        if len(line.split()) == 3 and all(v.lstrip("-").isdigit() for v in line.split())
    ]
    return {
        "total_seconds": (time.monotonic_ns() - before) / 1e9,
        "returncode": process.returncode,
        "timeout_error": "timeout" in text.lower(),
        "server_numeric_rows": numeric_rows,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, type=pathlib.Path)
    parser.add_argument("--docker", action="store_true")
    args = parser.parse_args()
    connection_string = os.environ["DB_CONNECTION_STRING"]
    params = _ConnectionStringParser()._parse(connection_string)
    output = {"planned_trials": 12, "trials": [], "errors": []}
    try:
        with connect(connection_string, timeout=10, autocommit=True) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT CAST(SERVERPROPERTY('ProductVersion') AS varchar(50)), "
                    "CAST(SERVERPROPERTY('Edition') AS varchar(100)), "
                    "(SELECT host_platform FROM sys.dm_os_host_info)"
                )
                output["server_identity"] = list(cursor.fetchone())
        for repetition in range(3):
            for timeout in (0, 2):
                for client in ("mssql-python", "sqlcmd"):
                    result = (
                        mssql_trial(connection_string, timeout)
                        if client == "mssql-python"
                        else sqlcmd_trial(params, timeout, args.docker)
                    )
                    if timeout:
                        matched = result["timeout_error"] and result["total_seconds"] < 4
                    else:
                        matched = (
                            not result["timeout_error"]
                            and 4 <= result["total_seconds"] < 15
                            and (
                                result.get("server_values") is not None
                                if client == "mssql-python"
                                else result["returncode"] == 0
                                and bool(result["server_numeric_rows"])
                            )
                        )
                    output["trials"].append(
                        {
                            "repetition": repetition + 1,
                            "client": client,
                            "timeout": timeout,
                            "matched_original_timing_contract": matched,
                            **result,
                        }
                    )
    except (OSError, RuntimeError, subprocess.TimeoutExpired) as exc:
        output["errors"].append({"phase": "control", "type": type(exc).__name__})
        raise
    finally:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("x", encoding="utf-8") as stream:
            json.dump(output, stream, indent=2)
    if output["errors"] or not all(t["matched_original_timing_contract"] for t in output["trials"]):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
