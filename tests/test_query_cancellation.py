# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.
"""Bounded subprocess tests for synchronous cancellation and connection recovery."""

import ctypes
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import uuid

import pytest

import mssql_python
from mssql_python import ddbc_bindings
from mssql_python.exceptions import DataError, NotSupportedError, OperationalError, ProgrammingError


def _run(name, *args):
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import runpy,sys; runpy.run_path(sys.argv[1])[sys.argv[2]](*sys.argv[3:])",
            str(Path(__file__).resolve()),
            name,
            *args,
        ],
        capture_output=True,
        text=True,
        timeout=50,
    )
    assert result.returncode == 0, (result.stdout, result.stderr)


def _connection(**kwargs):
    return mssql_python.connect(os.environ["DB_CONNECTION_STRING"], **kwargs)


def _worker(function):
    errors = []
    done = threading.Event()

    def run():
        try:
            function()
        except BaseException as error:
            errors.append(error)
        finally:
            done.set()

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread, done, errors


def _blocked(control, session, blocker=None):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        row = control.execute(
            "select wait_type, blocking_session_id from sys.dm_exec_requests "
            "where session_id = ?",
            session,
        ).fetchone()
        if row and (
            (blocker is None and row[0] == "WAITFOR")
            or (blocker is not None and row[1] == blocker and row[0].startswith("LCK_M_"))
        ):
            return
        time.sleep(0.01)
    raise AssertionError("Expected server-side blocked request was not observed")


@pytest.mark.parametrize("prepared", ["direct", "prepared", "dae", "executemany"])
@pytest.mark.parametrize("pooling", ["on", "off"])
def test_cancel_active_execute(prepared, pooling):
    _run("_execute_probe", prepared, pooling)


@pytest.mark.parametrize("operation", ["execute", "fetchall", "connection", "catalog"])
def test_native_query_guard_preserves_protocol_without_python_owner(operation):
    from mssql_python._query_operation import QueryOperation, QueryConnectionState
    from mssql_python import _query_operation

    if not isinstance(_query_operation._NATIVE_QUERY_RUNTIME, tuple):
        pytest.skip("Requires supported CPython query policy watchers")
    calls = []
    owners = {
        QueryOperation.__init__.__code__,
        QueryOperation.__enter__.__code__,
        QueryOperation._end.__code__,
        QueryConnectionState.__init__.__code__,
        QueryConnectionState.__enter__.__code__,
    }
    with _connection() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT 1, 2").fetchall()
        catalog = type(cursor).tables.__wrapped__.__code__
        rejected = []

        def observe(frame, event, argument):
            if event == "call":
                if frame.f_code in owners:
                    calls.append(frame.f_code)
                if operation == "catalog" and frame.f_code is catalog:
                    with pytest.raises(mssql_python.NotSupportedError):
                        cursor.cancel()
                    rejected.append(True)

        previous = sys.getprofile()
        sys.setprofile(observe)
        try:
            if operation == "execute":
                assert cursor.execute("SELECT 3, 4").fetchone()[0] == 3
            elif operation == "fetchall":
                cursor.execute("SELECT 3, 4")
                assert cursor.fetchall()[0][0] == 3
            elif operation == "connection":
                connection.commit()
            else:
                cursor.tables(table="__cancel_test_missing_table__").fetchall()
        finally:
            sys.setprofile(previous)
        assert not calls
        assert rejected == ([True] if operation == "catalog" else [])
        assert cursor._query_slot.active is None
        assert not connection._query_state.busy
        assert cursor.execute("SELECT 5").fetchone()[0] == 5


@pytest.mark.parametrize("method", ["fetchone", "fetchmany", "fetchall"])
def test_grouped_scalar_reads_preserve_mixed_values_and_chunk_boundaries(method):
    scalar_types = ("int", "smallint", "bigint", "real", "float")
    columns = [
        f"CAST({index if index % 3 else 'NULL'} AS {scalar_types[index % 5]})"
        for index in range(20)
    ]
    columns.extend(("CAST('text' AS nvarchar(10))", "CAST(-9 AS int)", "CAST(NULL AS bigint)"))
    expected = tuple(None if index % 3 == 0 else index for index in range(20)) + (
        "text",
        -9,
        None,
    )
    with _connection() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT " + ",".join(columns) + " FROM (VALUES (1), (2), (3)) AS source(n)")
        if method == "fetchone":
            rows = [cursor.fetchone() for _ in range(3)]
        elif method == "fetchmany":
            rows = [cursor.fetchmany(1)[0] for _ in range(3)]
        else:
            rows = cursor.fetchall()
        assert [tuple(row) for row in rows] == [expected] * 3
        assert cursor.fetchone() is None
        assert not cursor.messages


@pytest.mark.parametrize(
    "change",
    ["factory", "closed-check", "connection", "connection-class", "keywords", "method-code"],
)
def test_native_query_guard_falls_back_for_python_overrides(change, monkeypatch):
    from mssql_python._query_operation import QueryOperation

    with _connection() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT 1").fetchall()
        calls = []
        original_factory = cursor._query_operation
        original_check = cursor._check_closed

        def factory(*, cancellable=True):
            calls.append("factory")
            return original_factory(cancellable=cancellable)

        def check():
            calls.append("check")
            return original_check()

        def replacement(self, *args, **kwargs):
            self.__dict__["_guard_test_replacement"] = True
            return self

        original_closed = type(connection).closed.fget

        def closed(self):
            calls.append("closed-property")
            return original_closed(self)

        with monkeypatch.context() as patch:
            if change == "factory":
                patch.setattr(cursor, "_query_operation", factory)
            elif change == "closed-check":
                patch.setattr(cursor, "_check_closed", check)
            elif change == "connection":
                patch.setitem(cursor.__dict__, "connection", connection)
            elif change == "connection-class":
                patch.setattr(type(connection), "closed", property(closed))
            elif change == "keywords":
                patch.setitem(type(cursor)._reset_cursor.__kwdefaults__, "checked", True)
            else:
                patch.setattr(type(cursor).execute.__wrapped__, "__code__", replacement.__code__)
            previous = sys.getprofile()

            def observe(frame, event, argument):
                if event == "call" and frame.f_code is QueryOperation.__init__.__code__:
                    calls.append("python-owner")

            sys.setprofile(observe)
            try:
                assert cursor.execute("SELECT 2") is cursor
            finally:
                sys.setprofile(previous)
            assert "python-owner" in calls
            if change == "factory":
                assert "factory" in calls
            elif change == "closed-check":
                assert "check" in calls
            elif change == "method-code":
                assert cursor.__dict__.pop("_guard_test_replacement")
            elif change == "connection-class":
                assert "closed-property" in calls
            assert not connection._query_state.busy
        assert cursor.execute("SELECT 3").fetchone()[0] == 3


def _execute_probe(prepared, pooling):
    mssql_python.pooling(enabled=pooling == "on")
    with _connection(autocommit=True) as connection, _connection(autocommit=True) as observer:
        with (
            connection.cursor() as cursor,
            connection.cursor() as sibling,
            observer.cursor() as control,
        ):
            session = cursor.execute("select @@spid").fetchone()[0]
            if prepared == "direct":
                execute = lambda: cursor.execute("WAITFOR DELAY '00:00:20'; SELECT 1")
            elif prepared == "executemany":
                execute = lambda: cursor.executemany("WAITFOR DELAY ?; SELECT 1", [("00:00:20",)])
            else:
                # The same prepared statement is reused with new parameter values.
                query = "declare @payload nvarchar(max) = ?; WAITFOR DELAY ?; SELECT 1"
                cursor.execute(query, "warm", "00:00:00").fetchall()
                payload = "x" * 100000 if prepared == "dae" else None
                execute = lambda: cursor.execute(query, payload, "00:00:20")
            thread, done, errors = _worker(execute)
            _blocked(control, session)
            old_operation = cursor._query_slot.active
            for competing in (
                cursor.close,
                connection.close,
                connection.commit,
                connection.rollback,
                connection.cursor,
                lambda: sibling.execute("select 2"),
                lambda: cursor.execute("select 2"),
            ):
                with pytest.raises(ProgrammingError, match="operation in progress"):
                    competing()
            assert not sibling.closed and not connection.closed and not cursor.closed
            cancel_threads = [_worker(cursor.cancel), _worker(cursor.cancel)]
            for canceller, cancelled, cancel_errors in cancel_threads:
                assert cancelled.wait(5), "cancel request did not return"
                canceller.join(1)
                assert not cancel_errors, cancel_errors
            assert done.wait(5), "execute did not stop after cancellation"
            thread.join(1)
            assert len(errors) == 1 and isinstance(errors[0], OperationalError), errors
            assert errors[0].driver_error == "Operation canceled", errors
            assert cursor.description is None and cursor.rowcount == -1
            cursor.execute("select 99")
            # A saved late request cannot cancel the replacement statement or unread results.
            old_operation.cancel()
            cursor.cancel()
            assert cursor.fetchone()[0] == 99
            assert sibling.execute("select 100").fetchone()[0] == 100
        connection.close()
    with _connection() as reused:
        assert reused.cursor().execute("select 101").fetchone()[0] == 101


@pytest.mark.parametrize(
    "method", ["fetchone", "fetchmany", "fetchmany1", "fetchall", "fetchval", "next"]
)
@pytest.mark.parametrize("lob", ["scalar", "lob"])
def test_cancel_blocked_row_fetch(method, lob):
    _run("_fetch_probe", method, lob)


def _fetch_probe(method, lob):
    table = "##cancel_" + uuid.uuid4().hex
    with _connection(autocommit=True) as locker, _connection(autocommit=True) as connection:
        with locker.cursor() as control, connection.cursor() as cursor:
            payload_type = "varchar(max)" if lob == "lob" else "char(4000)"
            control.execute(f"create table {table} (id int primary key, payload {payload_type})")
            control.execute(
                f"insert into {table} select top (4096) "
                "row_number() over (order by (select null)), replicate('x', 4000) "
                "from sys.all_objects a cross join sys.all_objects b"
            )
            locker_id = control.execute("select @@spid").fetchone()[0]
            session = cursor.execute("select @@spid").fetchone()[0]
            locker.autocommit = False
            control.execute(f"update {table} with (rowlock) set payload='locked' where id=4096")
            cursor.execute("set lock_timeout 20000")
            cursor.execute(
                f"select id,payload from {table} with (readcommittedlock,rowlock,index(1)) "
                "order by id option(maxdop 1)"
            )
            cursor.fetchone()
            cursor.fetchone()

            def consume():
                if method == "fetchall":
                    cursor.fetchall()
                else:
                    while True:
                        if method == "next":
                            next(cursor)
                        elif method in ("fetchmany", "fetchmany1"):
                            assert cursor.fetchmany(1 if method == "fetchmany1" else 64)
                        else:
                            assert getattr(cursor, method)() is not None

            thread, done, errors = _worker(consume)
            try:
                _blocked(control, session, locker_id)
                cursor.cancel()
                assert done.wait(5), "blocked fetch did not terminate"
                thread.join(1)
                assert len(errors) == 1 and isinstance(errors[0], OperationalError), errors
                assert errors[0].driver_error == "Operation canceled"
                assert cursor.execute("select 99").fetchone()[0] == 99
            finally:
                locker.rollback()
                thread.join(5)
                assert not thread.is_alive()
                control.execute(f"drop table {table}")


def test_cancel_preserves_idle_results_and_closed_contract(cursor):
    cursor.execute("select 1 union all select 2")
    cursor.cancel()
    assert len(cursor.fetchall()) == 2
    cursor.cancel()
    with _connection() as connection:
        closed = connection.cursor()
        closed.close()
        with pytest.raises(ProgrammingError, match="closed"):
            closed.cancel()


def test_cancel_does_not_rollback_transaction():
    _run("_transaction_probe")


def _transaction_probe():
    with _connection(autocommit=False) as connection, _connection(autocommit=True) as observer:
        with connection.cursor() as cursor, observer.cursor() as control:
            cursor.execute("create table #cancel_transaction(value int)")
            connection.commit()
            cursor.execute("insert into #cancel_transaction values(1)")
            session = cursor.execute("select @@spid").fetchone()[0]
            thread, done, errors = _worker(lambda: cursor.execute("WAITFOR DELAY '00:00:20'"))
            _blocked(control, session)
            cursor.cancel()
            assert done.wait(5)
            thread.join(1)
            assert len(errors) == 1 and isinstance(errors[0], OperationalError)
            assert cursor.execute("select @@trancount").fetchone()[0] > 0
            connection.rollback()
            assert cursor.execute("select count(*) from #cancel_transaction").fetchone()[0] == 0


def _pointers():
    if not hasattr(ddbc_bindings.module, "_arrow_test_pointer_addresses"):
        if os.environ.get("MSSQL_ARROW_TEST_INTERPOSITION", "").upper() in ("1", "ON"):
            pytest.fail("Requested native cancellation controls were not built")
        pytest.skip("Requires compiled native cancellation controls")
    return {
        key: ctypes.c_void_p.from_address(value)
        for key, value in ddbc_bindings.module._arrow_test_pointer_addresses.items()
    }


@pytest.mark.parametrize(
    "outcome",
    [
        "cancel",
        "warning",
        "mixed",
        "disconnected",
        "mixed_disconnected",
        "failed",
        "cleanup_failed",
        "invalid",
        "unavailable",
    ],
)
@pytest.mark.parametrize("target", ["fetch", "scroll", "getdata", "grouped"])
def test_cancel_native_completion_fence(outcome, target):
    _pointers()
    _run("_fence_probe", outcome, target)


def test_native_cancellation_recovery_allocation_failure_retires_connection():
    _pointers()
    if ddbc_bindings._fetch_observer is None:
        pytest.skip("Requires supported CPython watchers")
    _run("_native_recovery_allocation_probe")


def test_grouped_scalar_cancel_prevents_later_column_calls():
    _pointers()
    _run("_grouped_scalar_boundary_probe")


def _grouped_scalar_boundary_probe():
    pointers = _pointers()
    callback = ctypes.WINFUNCTYPE if os.name == "nt" else ctypes.CFUNCTYPE
    getdata_type = callback(
        ctypes.c_short,
        ctypes.c_void_p,
        ctypes.c_ushort,
        ctypes.c_short,
        ctypes.c_void_p,
        ctypes.c_ssize_t,
        ctypes.c_void_p,
    )
    cancel_type = callback(ctypes.c_short, ctypes.c_void_p)
    with _connection() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT 1, 2, 3, 4 FROM (VALUES (1), (2)) AS source(n)")
        assert tuple(cursor.fetchone()) == (1, 2, 3, 4)
        original_getdata = pointers["SQLGetData_ptr"].value
        original_cancel = pointers["SQLCancel_ptr"].value
        getdata = getdata_type(original_getdata)
        reads = []

        @getdata_type
        def cancel_after_second_column(*args):
            reads.append(args[1])
            result = getdata(*args)
            if args[1] == 2:
                cursor.cancel()
            return result

        @cancel_type
        def cancel(_):
            return 0

        profiling = ddbc_bindings.profiling
        profiling.reset()
        profiling.enable()
        pointers["SQLGetData_ptr"].value = ctypes.cast(
            cancel_after_second_column, ctypes.c_void_p
        ).value
        pointers["SQLCancel_ptr"].value = ctypes.cast(cancel, ctypes.c_void_p).value
        try:
            with pytest.raises(OperationalError, match="Operation canceled"):
                cursor.fetchone()
        finally:
            pointers["SQLGetData_ptr"].value = original_getdata
            pointers["SQLCancel_ptr"].value = original_cancel
            profiling.disable()
        assert reads == [1, 2]
        assert profiling.get_stats()["ddbc::SQLGetData::scalar_group"]["calls"] == 1
        assert cursor._query_slot.active is None
        assert not connection._query_state.busy
        assert cursor.execute("SELECT 99").fetchone()[0] == 99


def _native_recovery_allocation_probe():
    from mssql_python import _query_operation

    pointers = _pointers()
    callback = ctypes.WINFUNCTYPE if os.name == "nt" else ctypes.CFUNCTYPE
    getdata_type = callback(
        ctypes.c_short,
        ctypes.c_void_p,
        ctypes.c_ushort,
        ctypes.c_short,
        ctypes.c_void_p,
        ctypes.c_ssize_t,
        ctypes.c_void_p,
    )
    cancel_type = callback(ctypes.c_short, ctypes.c_void_p)

    class AllocationFailure:
        @staticmethod
        def __new__(cls):
            raise MemoryError("injected native recovery allocation failure")

    with _connection(autocommit=True) as connection, connection.cursor() as cursor:
        cursor.execute("SELECT n FROM (VALUES (1), (2), (3)) AS v(n) ORDER BY n")
        assert cursor.fetchone()[0] == 1
        assert cursor.fetchone()[0] == 2
        original_getdata = pointers["SQLGetData_ptr"].value
        original_cancel = pointers["SQLCancel_ptr"].value
        getdata = getdata_type(original_getdata)
        profiling = ddbc_bindings.profiling
        profiling.reset()
        profiling.enable()
        with pytest.MonkeyPatch.context() as patch:

            @getdata_type
            def cancel_after_getdata(*args):
                result = getdata(*args)
                patch.setattr(_query_operation, "object", AllocationFailure, raising=False)
                cursor.cancel()
                return result

            @cancel_type
            def cancel(_):
                return 0

            pointers["SQLGetData_ptr"].value = ctypes.cast(
                cancel_after_getdata, ctypes.c_void_p
            ).value
            pointers["SQLCancel_ptr"].value = ctypes.cast(cancel, ctypes.c_void_p).value
            try:
                with pytest.raises(OperationalError, match="Operation canceled"):
                    cursor.fetchone()
            finally:
                pointers["SQLGetData_ptr"].value = original_getdata
                pointers["SQLCancel_ptr"].value = original_cancel
                profiling.disable()
        assert profiling.get_stats()["ddbc::NativeCursor::fetch"]["calls"] == 1
        assert connection.closed and cursor.closed
        assert not connection._query_state.busy
        assert cursor._query_slot.active is None


def _fence_probe(outcome, target="fetch"):
    pointers = _pointers()
    callback = ctypes.WINFUNCTYPE if os.name == "nt" else ctypes.CFUNCTYPE
    call = callback(ctypes.c_short, ctypes.c_void_p)
    targets = {
        "fetch": ("SQLFetch_ptr", call),
        "scroll": (
            "SQLFetchScroll_ptr",
            callback(ctypes.c_short, ctypes.c_void_p, ctypes.c_short, ctypes.c_ssize_t),
        ),
        "getdata": (
            "SQLGetData_ptr",
            callback(
                ctypes.c_short,
                ctypes.c_void_p,
                ctypes.c_ushort,
                ctypes.c_short,
                ctypes.c_void_p,
                ctypes.c_ssize_t,
                ctypes.c_void_p,
            ),
        ),
    }
    targets["grouped"] = targets["getdata"]
    diag_call = callback(
        ctypes.c_short,
        ctypes.c_short,
        ctypes.c_void_p,
        ctypes.c_short,
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_short,
        ctypes.c_void_p,
    )
    entered, cancelling, release, completed = (threading.Event() for _ in range(4))
    violations = []
    with _connection(autocommit=True) as connection:
        with connection.cursor() as cursor:
            physical_id = cursor.execute(
                "select convert(varchar(36), connection_id) from sys.dm_exec_connections "
                "where session_id=@@spid"
            ).fetchone()[0]
            if target == "grouped":
                cursor.execute("SELECT 1, 2, 3 FROM (VALUES (1), (2)) AS source(n)")
                assert tuple(cursor.fetchone()) == (1, 2, 3)
            else:
                cursor.execute("select 1")
            original = {key: pointer.value for key, pointer in pointers.items()}

            @targets[target][1]
            def fetch(*_):
                entered.set()
                if not cancelling.wait(5):
                    violations.append("cancel never entered")
                return 1 if outcome == "warning" else (-2 if outcome == "invalid" else -1)

            @call
            def cancel(_):
                cancelling.set()
                if not release.wait(5):
                    violations.append("cancel fence was never released")
                completed.set()
                return -1 if outcome == "failed" else 0

            @diag_call
            def diag(_kind, _handle, record, state, native, message, capacity, length):
                if not completed.is_set():
                    violations.append("diagnostics raced SQLCancel")
                states = {
                    "mixed": ["HY008", "22003"],
                    "disconnected": ["HY008", "08S01"],
                    "mixed_disconnected": ["HY008", "22003", "08S01"],
                }.get(outcome, ["HY008"])
                if record > len(states):
                    return 100
                ctypes.memmove(state, (states[record - 1] + "\0").encode("utf-16le"), 12)
                ctypes.cast(native, ctypes.POINTER(ctypes.c_int))[0] = 0
                ctypes.cast(length, ctypes.POINTER(ctypes.c_short))[0] = 0
                if capacity:
                    ctypes.memset(message, 0, 2)
                return 0

            free_type = callback(ctypes.c_short, ctypes.c_short, ctypes.c_void_p)
            real_free = free_type(original["SQLFreeHandle_ptr"])
            free_failures = []

            @free_type
            def free(kind, handle):
                if outcome == "cleanup_failed" and kind == 3 and not free_failures:
                    free_failures.append(handle)
                    return -1
                return real_free(kind, handle)

            pointers[targets[target][0]].value = ctypes.cast(fetch, ctypes.c_void_p).value
            pointers["SQLCancel_ptr"].value = (
                None if outcome == "unavailable" else ctypes.cast(cancel, ctypes.c_void_p).value
            )
            pointers["SQLGetDiagRec_ptr"].value = ctypes.cast(diag, ctypes.c_void_p).value
            pointers["SQLFreeHandle_ptr"].value = ctypes.cast(free, ctypes.c_void_p).value
            worker, done, errors = _worker(
                (lambda: cursor.fetchmany(4)) if target == "scroll" else cursor.fetchone
            )
            assert entered.wait(5)
            try:
                if outcome == "unavailable":
                    with pytest.raises(NotSupportedError):
                        cursor.cancel()
                    # Let the fake fetch return; this was not accepted cancellation.
                    completed.set()
                    cancelling.set()
                else:
                    canceller, cancel_done, cancel_errors = _worker(cursor.cancel)
                    assert cancelling.wait(5)
                    assert not done.wait(0.1), "worker escaped the cancellation fence"
                    with pytest.raises(ProgrammingError):
                        connection.close()
                    release.set()
                    assert cancel_done.wait(5)
                    canceller.join(1)
                    assert bool(cancel_errors) == (outcome == "failed"), cancel_errors
                assert done.wait(5)
                worker.join(1)
                expected = (
                    DataError if outcome in ("mixed", "mixed_disconnected") else OperationalError
                )
                assert len(errors) == 1 and isinstance(errors[0], expected), errors
                if outcome == "disconnected":
                    assert errors[0].driver_error == "Communication link failure"
                elif outcome in ("mixed", "mixed_disconnected"):
                    assert errors[0].driver_error == "Numeric value out of range"
                assert not violations, violations
            finally:
                release.set()
                cancelling.set()
                worker.join(5)
                for key, pointer in pointers.items():
                    pointer.value = original[key]
            if outcome in (
                "failed",
                "cleanup_failed",
                "invalid",
                "disconnected",
                "mixed_disconnected",
            ):
                assert connection.closed
                if outcome == "cleanup_failed":
                    assert len(free_failures) == 1
                with _connection() as replacement:
                    new_id = (
                        replacement.cursor()
                        .execute(
                            "select convert(varchar(36), connection_id) "
                            "from sys.dm_exec_connections where session_id=@@spid"
                        )
                        .fetchone()[0]
                    )
                    assert new_id != physical_id
            else:
                assert cursor.execute("select 99").fetchone()[0] == 99


@pytest.mark.parametrize("stage", ["prepare", "describe", "param_data", "put_data"])
def test_cancel_stops_multistep_execution(stage):
    _pointers()
    _run("_execution_boundary_probe", stage)


def _execution_boundary_probe(stage):
    pointers = _pointers()
    callback = ctypes.WINFUNCTYPE if os.name == "nt" else ctypes.CFUNCTYPE
    pointer = ctypes.c_void_p
    short = ctypes.c_short
    signatures = {
        "prepare": callback(short, pointer, pointer, ctypes.c_int),
        "describe": callback(short, pointer, ctypes.c_ushort, pointer, pointer, pointer, pointer),
        "execute": callback(short, pointer),
        "param_data": callback(short, pointer, pointer),
        "put_data": callback(short, pointer, pointer, ctypes.c_ssize_t),
    }
    keys = dict(
        prepare="SQLPrepare_ptr",
        describe="SQLDescribeParam_ptr",
        execute="SQLExecute_ptr",
        param_data="SQLParamData_ptr",
        put_data="SQLPutData_ptr",
    )
    entered, release = threading.Event(), threading.Event()
    counts = dict.fromkeys(keys, 0)
    callbacks = []
    with _connection(autocommit=True) as connection:
        with connection.cursor() as cursor:
            original = {key: pointers[key].value for key in keys.values()}

            def wrap(name):
                real = signatures[name](original[keys[name]])

                @signatures[name]
                def invoke(*args):
                    counts[name] += 1
                    result = real(*args)
                    if name == stage and counts[name] == 1:
                        entered.set()
                        if not release.wait(5):
                            return -1
                    return result

                callbacks.append(invoke)
                pointers[keys[name]].value = ctypes.cast(invoke, pointer).value

            for name in keys:
                wrap(name)
            value = None if stage == "describe" else "x" * 100000
            worker, done, errors = _worker(lambda: cursor.execute("select ?", value))
            try:
                assert entered.wait(5), (stage, counts)
                cursor.cancel()
                release.set()
                assert done.wait(5)
                worker.join(1)
                assert len(errors) == 1 and isinstance(errors[0], OperationalError), errors
                assert errors[0].driver_error == "Operation canceled"
                if stage in ("prepare", "describe"):
                    assert counts["execute"] == 0, counts
                elif stage == "param_data":
                    assert counts["put_data"] == 0, counts
                else:
                    assert counts["put_data"] == 1, counts
            finally:
                release.set()
                worker.join(5)
                assert not worker.is_alive()
                for key, value in original.items():
                    pointers[key].value = value
            assert cursor.execute("select 99").fetchone()[0] == 99


def test_cancel_during_python_preparation_and_stale_request(cursor, monkeypatch):
    original = ddbc_bindings.DDBCSQLExecDirect
    requests = []

    def before_native(*args):
        requests.append(cursor._query_slot.active)
        cursor.cancel()
        return original(*args)

    monkeypatch.setattr(ddbc_bindings, "DDBCSQLExecDirect", before_native)
    with pytest.raises(OperationalError, match="Operation canceled"):
        cursor.execute("select 1")
    monkeypatch.setattr(ddbc_bindings, "DDBCSQLExecDirect", original)
    cursor.execute("select 2")
    requests[0].cancel()
    assert cursor.fetchone()[0] == 2


def test_cancel_during_unsupported_operation(cursor):
    with cursor._query_operation(cancellable=False):
        with pytest.raises(NotSupportedError):
            cursor.cancel()
        with pytest.raises(ProgrammingError):
            cursor.close()
    assert cursor.execute("select 3").fetchone()[0] == 3


def test_cancel_repeated_prepared_execution():
    _run("_repeat_probe")


def test_cancel_active_nextset():
    _run("_nextset_probe")


def _nextset_probe():
    with _connection(autocommit=True) as connection, _connection(autocommit=True) as observer:
        with connection.cursor() as cursor, observer.cursor() as control:
            session = cursor.execute("select @@spid").fetchone()[0]
            cursor.execute(
                "SELECT TOP (4096) 1, REPLICATE('x', 4000) FROM sys.all_objects a "
                "CROSS JOIN sys.all_objects b; WAITFOR DELAY '00:00:20'; SELECT 2"
            )
            assert cursor.fetchone()[0] == 1
            worker, done, errors = _worker(cursor.nextset)
            deadline = time.monotonic() + 5
            while cursor._query_slot.active is None and time.monotonic() < deadline:
                time.sleep(0.01)
            assert cursor._query_slot.active is not None
            _blocked(control, session)
            cursor.cancel()
            assert done.wait(5)
            worker.join(1)
            assert len(errors) == 1 and isinstance(errors[0], OperationalError), errors
            assert cursor.execute("select 99").fetchone()[0] == 99


@pytest.mark.parametrize("autocommit", ["on", "off"])
def test_cancel_executemany_partial_transaction(autocommit):
    _run("_batch_transaction_probe", autocommit)


def _batch_transaction_probe(autocommit):
    with _connection(autocommit=True) as connection, _connection(autocommit=True) as observer:
        with connection.cursor() as cursor, observer.cursor() as control:
            cursor.execute("SET NOCOUNT ON; CREATE TABLE #cancel_batch (id int)")
            session = cursor.execute("select @@spid").fetchone()[0]
            connection.autocommit = autocommit == "on"
            worker, done, errors = _worker(
                lambda: cursor.executemany(
                    "WAITFOR DELAY ?; INSERT INTO #cancel_batch VALUES (?)",
                    [("00:00:00", 1), ("00:00:20", 2), ("00:00:00", 3)],
                )
            )
            _blocked(control, session)
            cursor.cancel()
            assert done.wait(5)
            worker.join(1)
            assert len(errors) == 1 and isinstance(errors[0], OperationalError), errors
            assert [
                row[0] for row in cursor.execute("SELECT id FROM #cancel_batch").fetchall()
            ] == [1]
            if autocommit == "off":
                assert cursor.execute("SELECT @@TRANCOUNT").fetchone()[0] > 0
                connection.rollback()
                assert not cursor.execute("SELECT id FROM #cancel_batch").fetchall()


def test_stale_arrow_reader_close_cannot_reach_active_execute():
    pytest.importorskip("pyarrow")
    _run("_arrow_close_probe")


def _arrow_close_probe():
    with _connection(autocommit=True) as connection, _connection(autocommit=True) as observer:
        with connection.cursor() as cursor, observer.cursor() as control:
            session = cursor.execute("select @@spid").fetchone()[0]
            query = "WAITFOR DELAY ?; SELECT 1"
            cursor.execute(query, "00:00:00")
            reader = cursor.arrow_reader(batch_size=1)
            worker, done, errors = _worker(lambda: cursor.execute(query, "00:00:20"))
            _blocked(control, session)
            reader.close()
            assert not done.is_set(), "stale reader close reached new work"
            _blocked(control, session)
            cursor.cancel()
            assert done.wait(5)
            worker.join(1)
            assert len(errors) == 1 and isinstance(errors[0], OperationalError), errors
            cursor.execute("select 99")
            reader.close()
            assert cursor.fetchone()[0] == 99


@pytest.mark.parametrize("operation_failed", [False, True])
def test_cancel_recovery_preserves_primary_error(monkeypatch, operation_failed):
    with _connection() as connection, connection.cursor() as cursor:
        native = connection._conn

        class FailingDiscard:
            def discard(self):
                native.discard()
                raise RuntimeError("discard failed")

        def fail_reset(**_):
            raise RuntimeError("statement reset failed")

        monkeypatch.setattr(cursor, "_reset_cursor", fail_reset)
        connection._conn = FailingDiscard()
        expected = DataError if operation_failed else RuntimeError
        message = "primary operation error" if operation_failed else "statement reset failed"
        with pytest.raises(expected, match=message):
            with cursor._query_operation() as operation:
                operation.cancel()
                if operation_failed:
                    raise DataError("primary operation error", "")
        assert connection.closed and connection._conn is None
        assert cursor.closed


def test_cancel_at_iteration_eof_completion(monkeypatch):
    from mssql_python._query_operation import QueryOperation

    with _connection() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT 1")
        assert cursor.fetchone()[0] == 1
        end = QueryOperation._end

        def late_cancel(operation, failed):
            operation.cancel()
            return end(operation, failed)

        with monkeypatch.context() as patch:
            patch.setattr(QueryOperation, "_end", late_cancel)
            with pytest.raises(OperationalError, match="Operation canceled"):
                next(cursor)
        assert cursor.execute("SELECT 99").fetchone()[0] == 99


def _repeat_probe():
    with _connection(autocommit=True) as connection, _connection(autocommit=True) as observer:
        with connection.cursor() as cursor, observer.cursor() as control:
            session = cursor.execute("select @@spid").fetchone()[0]
            old_operation = None
            for _ in range(8):
                worker, done, errors = _worker(
                    lambda: cursor.execute("WAITFOR DELAY ?; SELECT 1", "00:00:20")
                )
                _blocked(control, session)
                if old_operation is not None:
                    old_operation.cancel()
                    assert not done.is_set(), "stale cancellation reached new work"
                old_operation = cursor._query_slot.active
                cursor.cancel()
                assert done.wait(5)
                worker.join(1)
                assert len(errors) == 1 and isinstance(errors[0], OperationalError)
                assert cursor.execute("select 99").fetchone()[0] == 99


@pytest.mark.parametrize("stage", ["finish", "reset"])
@pytest.mark.parametrize("interrupt", ["KeyboardInterrupt", "SystemExit"])
def test_interrupted_cancellation_recovery_retires_connection(stage, interrupt):
    _run("_interrupted_recovery_probe", stage, interrupt)


def _interrupted_recovery_probe(stage, interrupt):
    import builtins

    interruption = getattr(builtins, interrupt)
    with _connection(autocommit=True) as connection:
        with connection.cursor() as cursor, connection.cursor() as sibling:
            identity_query = (
                "select convert(varchar(36), connection_id) from sys.dm_exec_connections "
                "where session_id=@@spid"
            )
            physical_id = cursor.execute(identity_query).fetchone()[0]
            cursor.execute("select 1")
            with pytest.MonkeyPatch.context() as patch:
                if stage == "reset":

                    def reset(**_):
                        raise interruption("interrupted recovery")

                    patch.setattr(cursor, "_reset_cursor", reset)
                try:
                    with cursor._query_operation() as operation:
                        operation.cancel()
                        if stage == "finish":
                            native = operation.native

                            class InterruptedFinish:
                                def finish(self):
                                    native.finish()
                                    raise interruption("interrupted recovery")

                            operation.native = InterruptedFinish()
                except interruption:
                    pass
                else:
                    raise AssertionError("Recovery interruption was swallowed")
            assert connection.closed, "interrupted recovery left a reusable connection"
            assert cursor.closed and sibling.closed
            assert not connection._query_state.busy
            with _connection() as replacement:
                assert replacement.cursor().execute(identity_query).fetchone()[0] != physical_id


@pytest.mark.parametrize("target", ["fetch", "execute"])
def test_cancel_before_native_driver_entry(target):
    _pointers()
    _run("_driver_entry_probe", target)


def _driver_entry_probe(target):
    pointers = _pointers()
    callback = ctypes.WINFUNCTYPE if os.name == "nt" else ctypes.CFUNCTYPE
    call = callback(ctypes.c_short, ctypes.c_void_p)
    key = "SQLFetch_ptr" if target == "fetch" else "SQLExecute_ptr"
    entered, release = threading.Event(), threading.Event()
    with _connection(autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute("select ?", 1)
            original = pointers[key].value
            real = call(original)

            @call
            def at_entry(handle):
                entered.set()
                if not release.wait(5):
                    return -1
                return real(handle)

            pointers[key].value = ctypes.cast(at_entry, ctypes.c_void_p).value
            worker, done, errors = _worker(
                cursor.fetchone if target == "fetch" else lambda: cursor.execute("select ?", 2)
            )
            try:
                assert entered.wait(5)
                cursor.cancel()
                release.set()
                assert done.wait(5)
                worker.join(1)
                assert len(errors) == 1 and isinstance(errors[0], OperationalError), errors
                assert errors[0].driver_error == "Operation canceled", errors
            finally:
                release.set()
                worker.join(5)
                pointers[key].value = original
            assert cursor.execute("select 99").fetchone()[0] == 99


@pytest.mark.parametrize("fails", ["no", "interrupt"])
def test_reentrant_converter_cancellation_restores_outer_operation(fails):
    _run("_reentrant_converter_probe", fails)


def _reentrant_converter_probe(fails):
    from mssql_python.constants import ConstantsDDBC as C

    with _connection() as outer, _connection() as inner:
        with outer.cursor() as cursor, inner.cursor() as nested:
            observed = []

            def converter(value):
                assert nested.execute("select 7").fetchone()[0] == 7
                with pytest.raises(ProgrammingError, match="operation in progress"):
                    cursor.execute("select 3")
                cursor.cancel()
                observed.append(value)
                if fails == "interrupt":
                    raise KeyboardInterrupt("converter primary error")
                return value

            outer.add_output_converter(C.SQL_INTEGER.value, converter)
            cursor.execute("select 1 union all select 2")
            expected = KeyboardInterrupt if fails == "interrupt" else OperationalError
            with pytest.raises(expected):
                cursor.fetchall()
            assert observed
            outer.clear_output_converters()
            assert cursor.execute("select 99").fetchone()[0] == 99
            assert nested.execute("select 100").fetchone()[0] == 100


def test_gc_during_operation_admission_does_not_deadlock():
    _run("_admission_gc_probe")


def _admission_gc_probe():
    import faulthandler
    import gc
    import weakref

    faulthandler.dump_traceback_later(5, exit=True)
    gc.disable()
    try:
        with _connection() as connection, connection.cursor() as cursor:
            old = connection.cursor()
            old.cycle = old
            watched = weakref.ref(old)
            del old
            construct = ddbc_bindings._StatementOperation

            def collect(statement):
                gc.collect()
                return construct(statement)

            with pytest.MonkeyPatch.context() as patch:
                patch.setattr(ddbc_bindings, "_StatementOperation", collect)
                assert cursor.execute("select 99").fetchone()[0] == 99
            assert watched() is None
    finally:
        faulthandler.cancel_dump_traceback_later()
        gc.enable()


@pytest.mark.parametrize("request_mode", ["callback", "thread"])
def test_cancel_during_operation_token_construction(request_mode):
    _run("_token_construction_probe", request_mode)


def _token_construction_probe(request_mode):
    import faulthandler

    faulthandler.dump_traceback_later(5, exit=True)
    try:
        with _connection() as connection, connection.cursor() as cursor:
            cursor.execute("CREATE TABLE #admission_cancel (id int)")
            construct = ddbc_bindings._StatementOperation

            def cancel_before_construct(statement):
                if request_mode == "callback":
                    cursor.cancel()
                else:
                    canceller, done, errors = _worker(cursor.cancel)
                    assert done.wait(3), "admission lock held across native allocation"
                    canceller.join(1)
                    assert not errors, errors
                return construct(statement)

            with pytest.MonkeyPatch.context() as patch:
                patch.setattr(ddbc_bindings, "_StatementOperation", cancel_before_construct)
                with pytest.raises(OperationalError, match="Operation canceled"):
                    cursor.execute("INSERT INTO #admission_cancel VALUES (1)")
            assert not connection._query_state.busy
            assert cursor._query_slot.active is None
            assert cursor.execute("SELECT COUNT(*) FROM #admission_cancel").fetchone()[0] == 0
            assert cursor.execute("select 99").fetchone()[0] == 99
    finally:
        faulthandler.cancel_dump_traceback_later()


def test_uncancelled_sequence_error_is_not_normalized():
    with _connection() as connection, connection.cursor() as cursor:
        cursor.execute("select 1")
        cursor.hstmt._close_cursor()
        with pytest.raises(ProgrammingError):
            cursor.fetchone()
        assert cursor.execute("select 99").fetchone()[0] == 99


def test_operation_allocation_failure_preserves_idle_result(monkeypatch):
    with _connection() as connection, connection.cursor() as cursor:
        cursor.execute("select 99")

        def fail(_):
            raise MemoryError("native operation allocation failed")

        with monkeypatch.context() as patch:
            patch.setattr(ddbc_bindings, "_StatementOperation", fail)
            with pytest.raises(MemoryError):
                cursor.execute("select 1")
        assert not connection._query_state.busy and cursor._query_slot.active is None
        assert cursor.fetchone()[0] == 99


@pytest.mark.parametrize("pooling", ["on", "off"])
def test_cancel_storm_with_competing_cursors(pooling):
    _run("_cancel_storm_probe", pooling)


def _cancel_storm_probe(pooling):
    mssql_python.pooling(enabled=pooling == "on")
    with _connection(autocommit=True) as connection, _connection(autocommit=True) as observer:
        with observer.cursor() as control:
            cursors = [connection.cursor() for _ in range(8)]
            session = cursors[0].execute("select @@spid").fetchone()[0]
            try:
                for _ in range(8):
                    start = threading.Barrier(len(cursors), timeout=5)

                    def execute(cursor):
                        start.wait()
                        cursor.execute("WAITFOR DELAY '00:00:20'; SELECT 1")

                    workers = [_worker(lambda cursor=cursor: execute(cursor)) for cursor in cursors]
                    _blocked(control, session)
                    owners = [cursor for cursor in cursors if cursor._query_slot.active is not None]
                    assert len(owners) == 1
                    owner = owners[0]
                    for cursor in cursors:
                        if cursor is not owner:
                            cursor.cancel()
                    _blocked(control, session)
                    requests = [_worker(owner.cancel) for _ in range(8)]
                    for thread, done, errors in requests:
                        assert done.wait(5)
                        thread.join(1)
                        assert not errors, errors
                    failures = []
                    for thread, done, errors in workers:
                        assert done.wait(5)
                        thread.join(1)
                        assert len(errors) == 1, errors
                        failures.extend(errors)
                    assert sum(isinstance(error, OperationalError) for error in failures) == 1
                    assert sum(isinstance(error, ProgrammingError) for error in failures) == 7
                    assert owner.execute("select 99").fetchone()[0] == 99
            finally:
                for cursor in cursors:
                    cursor.close()
