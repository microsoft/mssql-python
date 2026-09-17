"""Diagnostics-only pytest plugin; never retries tests or changes their assertions."""

import datetime
import hashlib
import json
import os
import pathlib
import platform
import sys
import threading
import time

import pytest

TARGETS = {
    "test_constructor_timeout_does_not_become_query_timeout",
    "test_timeout_long_running_query_with_small_timeout",
}
QUERIES = {
    "WAITFOR DELAY '00:00:05'; SELECT 1",
    "WAITFOR DELAY '00:00:05'",
}
SAMPLE_SQL = """
SELECT SYSUTCDATETIME() AS server_utc, i.ms_ticks, i.cpu_ticks,
       i.cpu_count, i.scheduler_count, i.max_workers_count,
       (SELECT SUM(runnable_tasks_count) FROM sys.dm_os_schedulers
        WHERE status = 'VISIBLE ONLINE') AS runnable_tasks,
       (SELECT SUM(work_queue_count) FROM sys.dm_os_schedulers
        WHERE status = 'VISIBLE ONLINE') AS queued_work,
       (SELECT SUM(waiting_tasks_count) FROM sys.dm_os_wait_stats
        WHERE wait_type = 'THREADPOOL') AS threadpool_wait_count,
       r.status, r.command, r.wait_type, r.wait_time,
       r.total_elapsed_time, r.cpu_time, r.last_wait_type
FROM sys.dm_os_sys_info AS i
OUTER APPLY (
    SELECT status, command, wait_type, wait_time, total_elapsed_time,
           cpu_time, last_wait_type
    FROM sys.dm_exec_requests
    WHERE session_id = ? AND request_id = 0
) AS r
"""


def stamp():
    return {"monotonic_ns": time.monotonic_ns(), "wall_ns": time.time_ns()}


def error(exc):
    # Error text can include server addresses or credentials; retain its type only.
    return {"type": type(exc).__name__}


def emit(path, data):
    with path.open("x", encoding="utf-8") as stream:
        json.dump(data, stream, indent=2, default=str)


class Observer:
    def __init__(self, connection_string):
        self.connection_string = connection_string
        self.target_spid = -1
        self.samples = []
        self.errors = []
        self.stop = threading.Event()
        self.ready = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)

    def run(self):
        from mssql_python import connect
        from mssql_python import ddbc_bindings

        try:
            with connect(self.connection_string, timeout=10, autocommit=True) as connection:
                connection.timeout = 2
                with connection.cursor() as cursor:
                    ret, timeout = ddbc_bindings._diagnostic_query_timeout(cursor.hstmt)
                    if ret not in (0, 1) or timeout != 2:
                        raise RuntimeError("Observer query timeout readback failed")
                    while not self.stop.is_set():
                        before = stamp()
                        cursor.execute(SAMPLE_SQL, self.target_spid)
                        row = cursor.fetchone()
                        if row is None:
                            raise RuntimeError("Observer returned no server sample")
                        self.samples.append(
                            {
                                "before": before,
                                "after": stamp(),
                                "target_spid": self.target_spid,
                                "server": dict(zip((c[0] for c in cursor.description), row)),
                            }
                        )
                        self.ready.set()
                        self.stop.wait(0.15)
        except Exception as exc:
            self.errors.append({"phase": "observer", **error(exc)})
            self.ready.set()

    def start(self):
        self.thread.start()
        if not self.ready.wait(15):
            self.errors.append({"phase": "observer-start", "type": "DeadlineExceeded"})

    def finish(self):
        self.stop.set()
        self.thread.join(5)
        if self.thread.is_alive():
            self.errors.append({"phase": "observer-stop", "type": "DeadlineExceeded"})


def pytest_addoption(parser):
    parser.addoption("--waitfor-probe-dir", required=True)


def pytest_configure(config):
    from mssql_python import ddbc_bindings

    if not callable(getattr(ddbc_bindings, "_diagnostic_query_timeout", None)):
        raise pytest.UsageError("Diagnostics-only native query-timeout getter is required")
    if not os.environ.get("DB_CONNECTION_STRING"):
        raise pytest.UsageError("DB_CONNECTION_STRING must be supplied through the environment")
    config._waitfor_errors = []
    config._waitfor_cases = []
    output = pathlib.Path(config.getoption("--waitfor-probe-dir"))
    output.mkdir(parents=True, exist_ok=True)
    config._waitfor_output = output
    emit(
        output / "client.json",
        {
            "platform": platform.platform(),
            "python": sys.version,
            "source": os.environ.get("BUILD_SOURCEVERSION"),
            "pid": os.getpid(),
            "probe_sha256": hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest(),
            "started": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        },
    )


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_call(item):
    if item.name not in TARGETS:
        yield
        return

    from mssql_python import ddbc_bindings
    from mssql_python.cursor import Cursor

    observer = Observer(os.environ["DB_CONNECTION_STRING"])
    observer.start()
    events = []
    original_connect = item.module.connect
    original_native = ddbc_bindings.DDBCSQLExecDirect
    original_execute = Cursor.execute
    original_fetchval = Cursor.fetchval

    def connect(*args, **kwargs):
        connection = original_connect(*args, **kwargs)
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT @@SPID")
                observer.target_spid = cursor.fetchval()
            events.append(
                {
                    "phase": "connect",
                    "at": stamp(),
                    "spid": observer.target_spid,
                    "login_timeout": kwargs.get("timeout", 0),
                    "python_query_timeout": connection.timeout,
                }
            )
        except Exception as exc:
            observer.errors.append({"phase": "target-identification", **error(exc)})
        return connection

    def native(handle, query):
        if query not in QUERIES:
            return original_native(handle, query)
        event = {"phase": "native-execute", "query": query, "before": stamp()}
        try:
            ret, timeout = ddbc_bindings._diagnostic_query_timeout(handle)
            event.update(attribute_return=ret, effective_query_timeout=timeout)
            if ret not in (0, 1):
                observer.errors.append({"phase": "timeout-readback", "return": ret})
        except Exception as exc:
            observer.errors.append({"phase": "timeout-readback", **error(exc)})
        event["readback_finished"] = stamp()
        try:
            ret = original_native(handle, query)
            event["sql_return"] = ret
            return ret
        finally:
            # No ODBC call here: another call could erase the execution diagnostics.
            event["after"] = stamp()
            events.append(event)

    def execute(cursor, operation, *args, **kwargs):
        if operation not in QUERIES:
            return original_execute(cursor, operation, *args, **kwargs)
        event = {
            "phase": "python-execute",
            "query": operation,
            "before": stamp(),
            "python_cursor_timeout": cursor._timeout,
            "python_connection_timeout": cursor._connection.timeout,
        }
        try:
            return original_execute(cursor, operation, *args, **kwargs)
        except Exception as exc:
            event["error"] = error(exc)
            raise
        finally:
            event["after"] = stamp()
            events.append(event)

    def fetchval(cursor):
        if cursor.last_executed_stmt not in QUERIES:
            return original_fetchval(cursor)
        before = stamp()
        try:
            return original_fetchval(cursor)
        finally:
            events.append({"phase": "fetchval", "before": before, "after": stamp()})

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(item.module, "connect", connect)
        patch.setattr(ddbc_bindings, "DDBCSQLExecDirect", native)
        patch.setattr(Cursor, "execute", execute)
        patch.setattr(Cursor, "fetchval", fetchval)
        before = stamp()
        result = yield
        after = stamp()
    observer.finish()
    if not any(e["phase"] == "native-execute" for e in events):
        observer.errors.append({"phase": "target-execution", "type": "NotObserved"})
    data = {
        "nodeid": item.nodeid,
        "before": before,
        "after": after,
        "original_test_raised": result.excinfo is not None,
        "events": events,
        "samples": observer.samples,
        "diagnostic_errors": observer.errors,
    }
    emit(item.config._waitfor_output / f"{item.name}.json", data)
    item.config._waitfor_errors.extend(observer.errors)
    item.config._waitfor_cases.append(item.nodeid)


def pytest_sessionfinish(session, exitstatus):
    config = session.config
    if not exitstatus and len(config._waitfor_cases) != len(TARGETS):
        config._waitfor_errors.append({"phase": "case-coverage", "type": "MissingTarget"})
    emit(
        config._waitfor_output / "session.json",
        {
            "original_exit_status": int(exitstatus),
            "observed_cases": config._waitfor_cases,
            "diagnostic_errors": config._waitfor_errors,
        },
    )
    if config._waitfor_errors and not exitstatus:
        session.exitstatus = pytest.ExitCode.INTERNAL_ERROR
