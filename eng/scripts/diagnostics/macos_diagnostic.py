"""Bounded, opt-in diagnostics; never imported by the normal test suite."""

import argparse
from contextlib import closing
import datetime
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import sys
import time

OUTPUT = Path(os.environ["DIAG_OUTPUT"])
OUTPUT.mkdir(parents=True, exist_ok=True)
SECRETS = [os.environ[k] for k in ("DB_PASSWORD",) if os.environ.get(k)]
QUERY = """
SET NOCOUNT ON;
DECLARE @started datetime2 = SYSUTCDATETIME();
DECLARE @ticks bigint = (SELECT ms_ticks FROM sys.dm_os_sys_info);
WAITFOR DELAY '00:00:05';
SELECT DATEDIFF_BIG(microsecond, @started, SYSUTCDATETIME()),
       ms_ticks - @ticks, @@SPID FROM sys.dm_os_sys_info;
"""


def redact(text):
    for value in SECRETS:
        text = text.replace(value, "[REDACTED]")
    return text


def emit(kind, **values):
    line = redact(
        json.dumps(
            {
                "kind": kind,
                "utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                **values,
            },
            default=str,
        )
    )
    print(line, flush=True)
    with (OUTPUT / "observations.jsonl").open("a", encoding="utf-8") as stream:
        stream.write(line + "\n")


def command(name, args, timeout):
    started = time.monotonic()
    result = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    text = redact(result.stdout + "\n" + result.stderr)
    (OUTPUT / f"{name}.log").write_text(text, encoding="utf-8")
    emit(
        "command",
        name=name,
        returncode=result.returncode,
        elapsed=time.monotonic() - started,
        output=f"{name}.log",
    )
    return result.returncode


def snapshot(label):
    for name, args in (
        ("host", ["sysctl", "hw.ncpu", "hw.memsize", "kern.boottime"]),
        ("memory", ["vm_stat"]),
        ("load", ["uptime"]),
        ("docker-stats", ["docker", "stats", "--no-stream", "sqlserver"]),
        ("docker-state", ["docker", "inspect", "--format", "{{json .State}}", "sqlserver"]),
        ("sql-log", ["docker", "logs", "--tail", "250", "sqlserver"]),
    ):
        command(f"{label}-{name}", args, 45)
    query = (
        "SET NOCOUNT ON; SELECT @@VERSION; "
        "SELECT SYSUTCDATETIME(),ms_ticks,cpu_ticks,cpu_count,"
        "physical_memory_kb,committed_kb,committed_target_kb FROM sys.dm_os_sys_info; "
        "SELECT name,value_in_use FROM sys.configurations WHERE name IN "
        "('query wait (s)','max worker threads','max server memory (MB)'); "
        "SELECT scheduler_id,current_workers_count,active_workers_count,"
        "runnable_tasks_count,work_queue_count FROM sys.dm_os_schedulers "
        "WHERE status='VISIBLE ONLINE';"
    )
    command(
        f"{label}-sql-state",
        [
            "docker",
            "exec",
            "-e",
            "SQLCMDPASSWORD=" + os.environ["DB_PASSWORD"],
            "sqlserver",
            "/opt/mssql-tools18/bin/sqlcmd",
            "-S",
            "localhost",
            "-U",
            "SA",
            "-C",
            "-b",
            "-l",
            "10",
            "-t",
            "20",
            "-Q",
            query,
        ],
        45,
    )


def original_tests(full):
    targets = (
        []
        if full
        else [
            "tests/test_003_connection.py::test_constructor_timeout_does_not_become_query_timeout",
            "tests/test_003_connection.py::test_timeout_long_running_query_with_small_timeout",
            "tests/test_009_pooling.py::test_connection_pooling_speed",
        ]
    )
    failures = 0
    for index in range(1 if full else 10):
        name = f"{'suite' if full else 'targeted'}-{index:02d}"
        command_args = [
            sys.executable,
            "-X",
            "faulthandler",
            "-m",
            "pytest",
            "-v",
            "--capture=tee-sys",
            "--cache-clear",
            f"--junitxml={OUTPUT / (name + '.xml')}",
        ]
        if full:
            command_args.extend(["--cov=.", f"--cov-report=xml:{OUTPUT / 'coverage.xml'}"])
        command_args.extend(targets)
        failures += command(name, command_args, 1200 if full else 180) != 0
    emit("original_tests_complete", full=full, failed_invocations=failures)
    return failures


def delay_probes(driver_name):
    import mssql_python

    if driver_name == "pyodbc":
        import pyodbc

        pyodbc.pooling = False
        factory = lambda: pyodbc.connect(
            os.environ["DB_CONNECTION_STRING"], driver="ODBC Driver 18 for SQL Server", timeout=3
        )
        error_type = pyodbc.Error
        modes = [False]
    else:
        factory = lambda: mssql_python.connect(os.environ["DB_CONNECTION_STRING"], timeout=3)
        error_type = mssql_python.DatabaseError
        modes = [False, True]
    for pool_enabled in modes:
        if driver_name == "mssql_python":
            mssql_python.pooling(enabled=pool_enabled)
        for repetition in range(6):
            for query_timeout in (0, 2):
                with closing(factory()) as conn:
                    conn.timeout = query_timeout
                    with closing(conn.cursor()) as cursor:
                        before = time.monotonic()
                        try:
                            cursor.execute(QUERY)
                            executed = time.monotonic()
                            rows = [tuple(row) for row in cursor.fetchall()]
                            finished = time.monotonic()
                            emit(
                                "delay",
                                driver=driver_name,
                                pooling=pool_enabled,
                                repetition=repetition,
                                requested_timeout=query_timeout,
                                execute_seconds=executed - before,
                                fetch_seconds=finished - executed,
                                total_seconds=finished - before,
                                rows=rows,
                                raw_statement_timeout="not exposed by the current binding",
                            )
                        except error_type as exc:
                            emit(
                                "delay_error",
                                driver=driver_name,
                                pooling=pool_enabled,
                                repetition=repetition,
                                requested_timeout=query_timeout,
                                total_seconds=time.monotonic() - before,
                                error_type=type(exc).__name__,
                                error=str(exc),
                            )
        if driver_name == "mssql_python":
            mssql_python.pooling(enabled=False)


def pooling_probes():
    import mssql_python

    def sample():
        started = time.perf_counter()
        conn = mssql_python.connect(os.environ["DB_CONNECTION_STRING"])
        connected = time.perf_counter()
        conn.close()
        closed = time.perf_counter()
        return {
            "connect": connected - started,
            "close": closed - connected,
            "total": closed - started,
        }

    for order in ((False, True), (True, False)):
        for warmed in (False, True):
            for enabled in order:
                mssql_python.pooling(enabled=False)
                mssql_python.pooling(enabled=enabled, max_size=5, idle_timeout=30)
                if warmed:
                    for _ in range(3):
                        sample()
                samples = [sample() for _ in range(10)]
                emit(
                    "pool_samples",
                    order=order,
                    warmed=warmed,
                    enabled=enabled,
                    samples=samples,
                    median=statistics.median(s["total"] for s in samples),
                )
    mssql_python.pooling(enabled=False)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["suite", "targeted", "probe", "control", "snapshot"])
    mode = parser.parse_args().mode
    emit(
        "runtime",
        mode=mode,
        python=sys.version,
        platform=platform.platform(),
        monotonic=vars(time.get_clock_info("monotonic")),
    )
    if mode == "snapshot":
        snapshot("final")
        return 0
    if mode in ("suite", "targeted"):
        return int(original_tests(mode == "suite") > 0)
    snapshot(mode + "-before")
    delay_probes("pyodbc" if mode == "control" else "mssql_python")
    if mode == "probe":
        pooling_probes()
    snapshot(mode + "-after")
    return 0


if __name__ == "__main__":
    sys.exit(main())
