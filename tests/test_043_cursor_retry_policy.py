"""
Tests for statement scope retry on Cursor.execute(), the second part of
https://github.com/microsoft/mssql-python/issues/682.

No test here needs a server. A bare Cursor is built the way test_004_cursor.py builds one, on a
fake connection that hands out fake statement handles, and the native execute and diagnostic
functions on mssql_python.cursor.ddbc_bindings are replaced with fakes that fail a chosen number
of times with a chosen SQLSTATE. The retry module's sleep and random seams are replaced so
nothing sleeps and every delay sequence is asserted exactly. Neither the db_connection nor the
cursor fixture is requested, so the file runs with DB_CONNECTION_STRING unset.
"""

import decimal
import gc
import logging
import uuid
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import mssql_python.cursor
import mssql_python.logging
import mssql_python.retry
from mssql_python import RetryPolicy
from mssql_python.connection import Connection
from mssql_python.constants import ConstantsDDBC
from mssql_python.cursor import MAX_INLINE_CHAR, Cursor
from mssql_python.exceptions import OperationalError, ProgrammingError
from mssql_python.retry import DEFAULT_RETRIABLE_SQLSTATES

DRIVER_PREFIX = "[Microsoft][ODBC Driver 18 for SQL Server]"
SQL_SUCCESS = 0
SQL_ERROR = -1
THE_SEVEN = ("HYT00", "HYT01", "08001", "08S01", "08007", "40001", "40003")
MESSAGES = {
    "HYT00": "Query timeout expired",
    "HYT01": "Connection timeout expired",
    "08S01": "Communication link failure",
    "40001": "Transaction (Process ID 52) was deadlocked on lock resources with another process "
    "and has been chosen as the deadlock victim. Rerun the transaction.",
    "40003": "Statement completion unknown",
    "42000": "Incorrect syntax near 'SELEC'.",
}


class FakeHandle:
    """Stand in for a native statement handle. Records whether it was freed."""

    def __init__(self, number):
        self.number = number
        self.freed = False

    def free(self):
        self.freed = True

    def _close_cursor(self):
        pass


class FakeConnection:
    """Just enough of Connection for execute(): a native connection that hands out statement
    handles, the autocommit property and the encoding settings execute() reads."""

    def __init__(self, autocommit=True):
        self.handles = []
        self._conn = SimpleNamespace(alloc_statement_handle=self._alloc)
        self._cursors = set()
        self._native_uuid = None
        self._autocommit = autocommit
        self.autocommit_error = None
        self.autocommit_reads = 0
        # Number of the handle whose allocation fails, as it would on a dead link; None for
        # never.
        self.fail_alloc_at = None

    def _alloc(self):
        number = len(self.handles) + 1
        if number == self.fail_alloc_at:
            raise RuntimeError("SQLSTATE:08S01:Communication link failure")
        handle = FakeHandle(number)
        self.handles.append(handle)
        return handle

    @property
    def autocommit(self):
        self.autocommit_reads += 1
        if self.autocommit_error is not None:
            raise self.autocommit_error
        return self._autocommit

    def getencoding(self):
        return {"encoding": "utf-16le", "ctype": ConstantsDDBC.SQL_WCHAR.value}


class FakeNative:
    """Stand in for the native execute and diagnostic functions used by execute().

    Both execute entry points fail ``failures`` times with ``sqlstate`` and then succeed. Every
    call records the handle, the parameter list object, a snapshot of the values in it, the
    input sizes, the is_stmt_prepared list object and the prepare flag, so a test can assert
    exactly what each attempt was given. Like the native type detector, the fake rewrites the
    Decimal and UUID slots of the list it was handed in place, so a list reused on a later
    attempt would show up as str values in that attempt's snapshot. Like the native execute, it
    refuses a statement that is neither prepared nor asked to be, and marks the statement
    prepared before running it. A parameterless statement goes to the direct entry point, which
    never prepares. The diagnostic fake records the handle of every read.
    """

    def __init__(self, failures=0, sqlstate="40001"):
        self.failures = failures
        self.sqlstate = sqlstate
        self.calls = []
        self.check_error_calls = 0
        self.check_error_handles = []
        # Index, counting from 1, of the call that raises instead of returning; None for never.
        self.raise_on_call = None

    def install(self, monkeypatch):
        bindings = mssql_python.cursor.ddbc_bindings
        monkeypatch.setattr(bindings, "DDBCSQLExecute", self.execute)
        monkeypatch.setattr(bindings, "DDBCSQLExecDirect", self.exec_direct)
        monkeypatch.setattr(bindings, "DDBCSQLCheckError", self.check_error)
        monkeypatch.setattr(bindings, "DDBCSQLResetStmt", lambda hstmt: SQL_SUCCESS)
        monkeypatch.setattr(bindings, "DDBCSQLRowCount", lambda hstmt: 1)
        monkeypatch.setattr(bindings, "DDBCSQLDescribeCol", lambda hstmt, metadata: None)
        return self

    def execute(
        self, hstmt, operation, params, input_sizes, is_stmt_prepared, use_prepare, encoding
    ):
        self._record(hstmt, params, input_sizes, is_stmt_prepared, use_prepare)
        for i, value in enumerate(params):
            if isinstance(value, (decimal.Decimal, uuid.UUID)):
                params[i] = str(value)
        if not is_stmt_prepared[0]:
            if not use_prepare:
                raise RuntimeError("Cannot execute unprepared statement")
            is_stmt_prepared[0] = True
        return self._outcome()

    def exec_direct(self, hstmt, operation):
        self._record(hstmt, [], None, None, None)
        return self._outcome()

    def _record(self, hstmt, params, input_sizes, is_stmt_prepared, use_prepare):
        self.calls.append(
            SimpleNamespace(
                hstmt=hstmt,
                params=params,
                values=list(params),
                input_sizes=input_sizes,
                is_stmt_prepared=is_stmt_prepared,
                use_prepare=use_prepare,
            )
        )
        if len(self.calls) == self.raise_on_call:
            raise RuntimeError("native layer raised")

    def _outcome(self):
        return SQL_ERROR if len(self.calls) <= self.failures else SQL_SUCCESS

    def check_error(self, handle_type, handle, ret):
        self.check_error_calls += 1
        self.check_error_handles.append(handle)
        return SimpleNamespace(
            sqlState=self.sqlstate, ddbcErrorMsg=DRIVER_PREFIX + MESSAGES[self.sqlstate]
        )


def make_cursor(connection=None, retry_policy=None):
    """Build a Cursor without __init__, as test_004_cursor.py does, with what execute() reads.

    The retry policy attribute is set only when a policy is given, so a cursor built here
    without one also proves that execute() tolerates the attribute being absent.
    """
    connection = FakeConnection() if connection is None else connection
    cur = Cursor.__new__(Cursor)
    cur.closed = False
    cur._connection = connection
    cur._timeout = 0
    cur._inputsizes = None
    cur.hstmt = connection._conn.alloc_statement_handle()
    cur.is_stmt_prepared = [False]
    cur.messages = []
    cur.last_executed_stmt = ""
    cur.description = None
    cur.rowcount = -1
    cur._clear_rownumber()
    if retry_policy is not None:
        cur._retry_policy = retry_policy
    return cur


def connection_stub():
    """A Connection shaped mock with the attributes cursor() and Cursor.__init__ read."""
    conn = MagicMock(spec=Connection)
    conn._closed = False
    conn._timeout = 0
    conn._cursors = set()
    conn._native_uuid = None
    conn._converters_generation = 0
    conn._decoding_generation = 0
    settings = {"encoding": "utf-16le", "ctype": ConstantsDDBC.SQL_WCHAR.value}
    conn.getencoding.return_value = settings
    conn.getdecoding.return_value = settings
    conn._conn = MagicMock()
    return conn


def retry_lines(driver_log, level=None):
    """The log lines statement retry writes, optionally only those at one level."""
    messages = [(record.levelno, record.getMessage()) for record in driver_log.records]
    return [
        msg
        for levelno, msg in messages
        if ("Statement attempt" in msg or "Statement failed" in msg)
        and (level is None or levelno == level)
    ]


@pytest.fixture(autouse=True)
def sleeps(monkeypatch):
    """Replace the retry module's sleep with a recorder so no test ever waits."""
    recorded = []
    monkeypatch.setattr(mssql_python.retry, "_sleep", recorded.append)
    return recorded


@pytest.fixture(autouse=True)
def steady_random(monkeypatch):
    """Pin the jitter source so no delay can depend on the random source."""
    monkeypatch.setattr(mssql_python.retry, "_random", lambda: 0.5)


@pytest.fixture
def native(monkeypatch):
    """Install a FakeNative in place of the native execute and diagnostic functions."""
    return FakeNative().install(monkeypatch)


@pytest.fixture
def driver_log(caplog, monkeypatch):
    """Capture what the driver logger emits, down to debug level, for one test.

    The driver logger does not propagate, so caplog's handler goes on it directly. It also
    drops anything below warning on a cached level, so that level is lowered for the test.
    """
    gc.collect()
    monkeypatch.setattr(mssql_python.logging.logger, "_cached_level", logging.DEBUG)
    with caplog.at_level(logging.DEBUG, logger="mssql_python"):
        mssql_python.logging.logger.addHandler(caplog.handler)
        try:
            yield caplog
        finally:
            mssql_python.logging.logger.removeHandler(caplog.handler)


@pytest.fixture
def raised(monkeypatch):
    """Record every exception check_error raises inside execute(), to assert identity later."""
    real_check_error = mssql_python.cursor.check_error
    seen = []

    def recording_check_error(*args):
        try:
            real_check_error(*args)
        except Exception as e:
            seen.append(e)
            raise

    monkeypatch.setattr(mssql_python.cursor, "check_error", recording_check_error)
    return seen


def test_no_policy_makes_a_single_attempt_and_raises_as_before(native, sleeps, driver_log):
    native.failures = 1
    native.sqlstate = "HYT00"
    cur = make_cursor()
    assert not hasattr(cur, "_retry_policy")
    with pytest.raises(OperationalError) as exc_info:
        cur.execute("SELECT 1")
    assert len(native.calls) == 1
    # Only check_error reads the diagnostics; without a policy there is no second read.
    assert native.check_error_calls == 1
    assert type(exc_info.value) is OperationalError
    assert exc_info.value.driver_error == "Timeout expired"
    assert "Query timeout expired" in exc_info.value.ddbc_error
    assert sleeps == []
    warnings = [r.getMessage() for r in driver_log.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1 and "Execute failed, resetting cursor" in warnings[0]
    assert retry_lines(driver_log) == []
    assert cur._connection.autocommit_reads == 0
    # The failed handle was released and a fresh one allocated, as before.
    assert cur._connection.handles[0].freed
    assert cur.hstmt is cur._connection.handles[1]


def test_policy_with_no_failure_is_a_single_attempt_without_a_diagnostic_read(native, sleeps):
    cur = make_cursor(retry_policy=RetryPolicy(max_attempts=3, jitter=False))
    assert cur.execute("SELECT 1") is cur
    assert len(native.calls) == 1
    assert native.check_error_calls == 0
    assert sleeps == []


def test_policy_of_one_attempt_adds_no_diagnostic_read(native, sleeps, driver_log):
    native.failures = 1
    cur = make_cursor(retry_policy=RetryPolicy(max_attempts=1))
    with pytest.raises(OperationalError):
        cur.execute("SELECT 1")
    assert len(native.calls) == 1
    assert native.check_error_calls == 1
    assert cur._connection.autocommit_reads == 0
    assert retry_lines(driver_log) == []


def test_policy_on_cursor_retries_a_parameterless_statement(native, sleeps):
    native.failures = 2
    cur = make_cursor(retry_policy=RetryPolicy(max_attempts=3, jitter=False))
    cur.execute("DELETE FROM queue WHERE done = 1")
    assert len(native.calls) == 3
    assert sleeps == [1.0, 2.0]
    # A parameterless statement goes to the direct entry point on every attempt, on the handle
    # current at the time.
    assert all(call.is_stmt_prepared is None for call in native.calls)
    handles = cur._connection.handles
    assert [call.hstmt for call in native.calls] == handles[:3]
    assert handles[0].freed and handles[1].freed and not handles[2].freed
    assert cur.hstmt is handles[2]
    assert cur.is_stmt_prepared == [False]
    assert cur.last_executed_stmt == "DELETE FROM queue WHERE done = 1"
    # Autocommit is read before each delay and again after it.
    assert cur._connection.autocommit_reads == 4


def test_retry_after_plan_reuse_prepares_again_on_the_fresh_handle(native, sleeps):
    cur = make_cursor(retry_policy=RetryPolicy(max_attempts=2, jitter=False))
    sql = "UPDATE t SET n = n + 1 WHERE id = ?"
    cur.execute(sql, 7)
    assert [call.use_prepare for call in native.calls] == [True]
    # The same SQL again reuses the prepared plan, so the first attempt asks for no prepare.
    # It fails; the retry runs on a fresh handle, where the plan is gone, so it prepares.
    native.failures = 2
    cur.execute(sql, 7)
    assert [call.use_prepare for call in native.calls] == [True, False, True]
    first_handle, retry_handle = native.calls[1].hstmt, native.calls[2].hstmt
    assert first_handle is not retry_handle and first_handle.freed
    assert native.calls[2].is_stmt_prepared is cur.is_stmt_prepared
    assert cur.is_stmt_prepared == [True]
    assert sleeps == [1.0]


def test_each_attempt_binds_a_fresh_copy_of_the_original_parameters(native, sleeps):
    native.failures = 2
    amount = decimal.Decimal("100.50")
    row_id = uuid.uuid4()
    cur = make_cursor(retry_policy=RetryPolicy(max_attempts=3, jitter=False))
    cur.execute("UPDATE t SET amount = ? WHERE id = ?", amount, row_id)
    assert len(native.calls) == 3
    assert sleeps == [1.0, 2.0]
    for call in native.calls:
        assert call.values[0] is amount and call.values[1] is row_id
        # The fake rewrote the list it was given, as the native detector does, so a reused
        # list would have arrived here holding these strings instead of the originals.
        assert call.params == [str(amount), str(row_id)]
    assert len({id(call.params) for call in native.calls}) == 3


def test_input_sizes_reach_every_attempt_and_are_cleared_after(native, sleeps):
    native.failures = 1
    cur = make_cursor(retry_policy=RetryPolicy(max_attempts=2, jitter=False))
    cur.setinputsizes([(ConstantsDDBC.SQL_DECIMAL.value, 18, 4)])
    sizes = cur._inputsizes
    cur.execute("INSERT INTO t (amount) VALUES (?)", decimal.Decimal("100.50"))
    assert [call.input_sizes for call in native.calls] == [sizes, sizes]
    assert native.calls[1].input_sizes is sizes
    # Cleared once, after the statement finally succeeded, as for any execute().
    assert cur._inputsizes is None


def test_autocommit_off_raises_at_once_and_says_why(native, sleeps, driver_log):
    native.failures = 1
    connection = FakeConnection(autocommit=False)
    cur = make_cursor(connection, RetryPolicy(max_attempts=3, jitter=False))
    with pytest.raises(OperationalError) as exc_info:
        cur.execute("UPDATE t SET n = n + 1")
    assert len(native.calls) == 1
    assert sleeps == []
    assert exc_info.value.driver_error == "Serialization failure"
    assert connection.autocommit_reads == 1
    debug = retry_lines(driver_log, logging.DEBUG)
    assert len(debug) == 1
    assert "attempt 1 of 3" in debug[0] and "40001" in debug[0]
    assert "autocommit is off" in debug[0]
    assert retry_lines(driver_log, logging.WARNING) == []


@pytest.mark.parametrize("value", [1, "on", MagicMock()], ids=["one", "text", "mock"])
def test_autocommit_must_read_exactly_true(native, sleeps, driver_log, value):
    # Fail closed: only the bool True allows a retry, not any value that merely tests true.
    native.failures = 1
    connection = FakeConnection(autocommit=value)
    cur = make_cursor(connection, RetryPolicy(max_attempts=3, jitter=False))
    with pytest.raises(OperationalError):
        cur.execute("SELECT 1")
    assert len(native.calls) == 1
    assert sleeps == []
    assert connection.autocommit_reads == 1
    debug = retry_lines(driver_log, logging.DEBUG)
    assert len(debug) == 1 and "autocommit is off" in debug[0]
    assert retry_lines(driver_log, logging.WARNING) == []


@pytest.mark.parametrize("sqlstate", ["08S01", "40003", "HYT01"])
def test_connection_lost_codes_are_not_retried_on_the_same_connection(
    native, sleeps, driver_log, sqlstate
):
    policy = RetryPolicy(max_attempts=3, jitter=False)
    assert policy.is_retriable(sqlstate)
    native.failures = 1
    native.sqlstate = sqlstate
    connection = FakeConnection()
    cur = make_cursor(connection, policy)
    with pytest.raises(OperationalError) as exc_info:
        cur.execute("SELECT 1")
    assert len(native.calls) == 1
    assert sleeps == []
    assert (
        exc_info.value.driver_error
        == {
            "08S01": "Communication link failure",
            "40003": "Statement completion unknown",
            "HYT01": "Connection timeout expired",
        }[sqlstate]
    )
    # Refused before the autocommit read, a native call that a dead link could not answer.
    assert connection.autocommit_reads == 0
    debug = retry_lines(driver_log, logging.DEBUG)
    assert len(debug) == 1 and "never retried on the same connection" in debug[0]


@pytest.mark.parametrize("sqlstate", THE_SEVEN)
def test_is_retriable_on_same_connection_for_each_default_code(sqlstate):
    policy = RetryPolicy()
    expected = sqlstate in ("HYT00", "40001")
    assert policy.is_retriable(sqlstate)
    assert policy.is_retriable_on_same_connection(sqlstate) is expected
    assert policy.is_retriable_on_same_connection(sqlstate.lower()) is expected


def test_is_retriable_on_same_connection_with_a_custom_set():
    policy = RetryPolicy(retriable_sqlstates={"08004", "40001", "42000"})
    assert policy.is_retriable("08004")
    assert not policy.is_retriable_on_same_connection("08004")
    assert policy.is_retriable_on_same_connection("40001")
    assert policy.is_retriable_on_same_connection("42000")
    # Not in the custom set, so not retriable at either scope.
    assert not policy.is_retriable_on_same_connection("HYT00")
    for sqlstate in (None, "", "08S0", "40001x"):
        assert not policy.is_retriable_on_same_connection(sqlstate)
    # Of the default set, exactly HYT00 and 40001 survive the same connection filter.
    survivors = {
        code
        for code in DEFAULT_RETRIABLE_SQLSTATES
        if RetryPolicy().is_retriable_on_same_connection(code)
    }
    assert survivors == {"HYT00", "40001"}


def test_policy_does_not_retry_a_code_it_does_not_list(native, sleeps, driver_log):
    native.failures = 1
    native.sqlstate = "42000"
    cur = make_cursor(retry_policy=RetryPolicy(max_attempts=3, jitter=False))
    with pytest.raises(ProgrammingError):
        cur.execute("SELEC 1")
    assert len(native.calls) == 1
    assert sleeps == []
    assert cur._connection.autocommit_reads == 0
    assert retry_lines(driver_log) == []


def test_exhaustion_raises_the_mapped_type_after_the_last_attempt(native, sleeps, driver_log):
    native.failures = 3
    cur = make_cursor(retry_policy=RetryPolicy(max_attempts=3, jitter=False))
    with pytest.raises(OperationalError) as exc_info:
        cur.execute("SELECT 1")
    assert len(native.calls) == 3
    assert sleeps == [1.0, 2.0]
    assert type(exc_info.value) is OperationalError
    assert exc_info.value.driver_error == "Serialization failure"
    assert "deadlock victim" in exc_info.value.ddbc_error
    # Each failure is read twice: once for the SQLSTATE, once by check_error as before, both
    # on the handle that failed and before _reset_cursor replaces it.
    handles = cur._connection.handles
    assert native.check_error_handles == [
        handles[0],
        handles[0],
        handles[1],
        handles[1],
        handles[2],
        handles[2],
    ]
    warnings = retry_lines(driver_log, logging.WARNING)
    assert len(warnings) == 3
    assert "attempt 1 of 3" in warnings[0] and "1.00 seconds" in warnings[0]
    assert "attempt 2 of 3" in warnings[1] and "2.00 seconds" in warnings[1]
    assert "attempt 3 of 3" in warnings[2] and "attempt limit" in warnings[2]
    assert "40001" in warnings[2]
    # Every failed handle was released and the cursor holds a fresh one.
    assert len(handles) == 4
    assert all(handle.freed for handle in handles[:3]) and cur.hstmt is handles[3]


def test_a_different_failure_after_a_retry_logs_the_give_up(
    native, sleeps, driver_log, monkeypatch
):
    native.failures = 2
    cur = make_cursor(retry_policy=RetryPolicy(max_attempts=3, jitter=False))

    def syntax_error_after_first_sleep(delay):
        sleeps.append(delay)
        native.sqlstate = "42000"

    monkeypatch.setattr(mssql_python.retry, "_sleep", syntax_error_after_first_sleep)
    with pytest.raises(ProgrammingError):
        cur.execute("SELECT 1")
    assert len(native.calls) == 2
    assert sleeps == [1.0]
    warnings = retry_lines(driver_log, logging.WARNING)
    assert len(warnings) == 2
    assert "attempt 2 of 3" in warnings[1] and "42000" in warnings[1]
    assert "does not list the SQLSTATE" in warnings[1]


def test_native_error_on_a_retry_keeps_its_type_and_logs_the_give_up(native, sleeps, driver_log):
    native.failures = 1
    native.raise_on_call = 2
    cur = make_cursor(retry_policy=RetryPolicy(max_attempts=3, jitter=False))
    with pytest.raises(RuntimeError, match="native layer raised"):
        cur.execute("SELECT 1")
    assert len(native.calls) == 2
    assert sleeps == [1.0]
    warnings = retry_lines(driver_log, logging.WARNING)
    assert len(warnings) == 2
    assert "attempt 2 of 3" in warnings[1] and "SQLSTATE none" in warnings[1]
    assert "RuntimeError" in warnings[1]


def test_native_error_on_the_first_attempt_is_raised_as_before(native, sleeps, driver_log):
    native.raise_on_call = 1
    cur = make_cursor(retry_policy=RetryPolicy(max_attempts=3, jitter=False))
    with pytest.raises(RuntimeError, match="native layer raised"):
        cur.execute("SELECT 1")
    assert len(native.calls) == 1
    assert sleeps == []
    assert retry_lines(driver_log) == []
    # Not routed through check_error, so the handle is kept, exactly as without a policy.
    assert cur.hstmt is cur._connection.handles[0]


def test_failed_sqlstate_read_is_not_retried(native, sleeps, raised, monkeypatch):
    native.failures = 1
    reads = []

    def first_read_fails(handle_type, handle, ret):
        reads.append(handle)
        if len(reads) == 1:
            raise RuntimeError("diagnostics unavailable")
        return native.check_error(handle_type, handle, ret)

    monkeypatch.setattr(mssql_python.cursor.ddbc_bindings, "DDBCSQLCheckError", first_read_fails)
    cur = make_cursor(retry_policy=RetryPolicy(max_attempts=3, jitter=False))
    with pytest.raises(OperationalError) as exc_info:
        cur.execute("SELECT 1")
    # The statement's own error, read by check_error as before, is what surfaces.
    assert exc_info.value is raised[0]
    assert exc_info.value.driver_error == "Serialization failure"
    assert len(native.calls) == 1
    assert len(reads) == 2
    assert sleeps == []


def test_jittered_delays_are_used_when_retrying(native, sleeps, monkeypatch):
    monkeypatch.setattr(mssql_python.retry, "_random", lambda: 0.25)
    native.failures = 2
    cur = make_cursor(retry_policy=RetryPolicy(max_attempts=3))
    cur.execute("SELECT 1")
    assert sleeps == [0.25, 0.5]


def test_cursor_closed_during_the_delay_stops_the_loop(
    native, sleeps, driver_log, raised, monkeypatch
):
    native.failures = 1
    cur = make_cursor(retry_policy=RetryPolicy(max_attempts=3, jitter=False))

    def close_while_sleeping(delay):
        sleeps.append(delay)
        cur.close()

    monkeypatch.setattr(mssql_python.retry, "_sleep", close_while_sleeping)
    with pytest.raises(OperationalError) as exc_info:
        cur.execute("SELECT 1")
    assert len(native.calls) == 1
    assert sleeps == [1.0]
    assert cur.closed and cur.hstmt is None
    assert exc_info.value is raised[0]
    # The retry was announced, so the refusal after the delay is a warning too.
    warnings = retry_lines(driver_log, logging.WARNING)
    assert len(warnings) == 2
    assert "the cursor was closed" in warnings[1] and "after the delay" in warnings[1]


def test_autocommit_turned_off_during_the_delay_stops_the_loop(
    native, sleeps, driver_log, raised, monkeypatch
):
    native.failures = 1
    connection = FakeConnection()
    cur = make_cursor(connection, RetryPolicy(max_attempts=3, jitter=False))

    def disable_autocommit_while_sleeping(delay):
        sleeps.append(delay)
        connection._autocommit = False

    monkeypatch.setattr(mssql_python.retry, "_sleep", disable_autocommit_while_sleeping)
    with pytest.raises(OperationalError) as exc_info:
        cur.execute("UPDATE t SET n = n + 1")
    assert len(native.calls) == 1
    assert sleeps == [1.0]
    assert exc_info.value is raised[0]
    # Read once before the delay, when it was still on, and once after it.
    assert connection.autocommit_reads == 2
    assert not cur.closed
    warnings = retry_lines(driver_log, logging.WARNING)
    assert len(warnings) == 2
    assert "autocommit is off" in warnings[1] and "after the delay" in warnings[1]


def test_reset_cursor_failure_after_a_failed_attempt_propagates_unwrapped(
    native, sleeps, driver_log, raised
):
    native.failures = 1
    connection = FakeConnection()
    # The failed attempt ran on handle 1; _reset_cursor releases it and allocates handle 2
    # before the retry decision, and here that allocation fails as it would on a dead link.
    connection.fail_alloc_at = 2
    cur = make_cursor(connection, RetryPolicy(max_attempts=3, jitter=False))
    with pytest.raises(RuntimeError) as exc_info:
        cur.execute("SELECT 1")
    assert "Communication link failure" in str(exc_info.value)
    # Raised while the statement's own failure was being handled, not wrapped in it.
    assert exc_info.value.__context__ is raised[0]
    assert len(native.calls) == 1
    assert sleeps == []
    assert connection.autocommit_reads == 0
    assert retry_lines(driver_log) == []
    assert connection.handles[0].freed and cur.hstmt is None


def test_executemany_row_by_row_fallback_follows_the_cursor_policy(native, sleeps):
    native.failures = 1
    cur = make_cursor(retry_policy=RetryPolicy(max_attempts=2, jitter=False))
    # A string past the inline limit is sent as data at execution, which makes executemany()
    # run the rows one at a time through execute(); that call carries the cursor's policy like
    # any other, so the first row's failure is retried.
    big = "x" * (MAX_INLINE_CHAR + 1)
    cur.executemany("INSERT INTO notes (body) VALUES (?)", [(big,), ("short",)])
    assert len(native.calls) == 3
    assert sleeps == [1.0]
    assert [call.values for call in native.calls] == [[big], [big], ["short"]]


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(
            OperationalError(driver_error="Connection operation failed", ddbc_error=""),
            id="mapped",
        ),
        pytest.param(RuntimeError("SQLSTATE:08003:Connection not open"), id="native"),
    ],
)
def test_autocommit_read_failure_fails_closed(native, sleeps, driver_log, raised, error):
    native.failures = 1
    connection = FakeConnection()
    connection.autocommit_error = error
    cur = make_cursor(connection, RetryPolicy(max_attempts=3, jitter=False))
    with pytest.raises(OperationalError) as exc_info:
        cur.execute("SELECT 1")
    # The statement's own failure surfaces, not the error from the guardrail read.
    assert exc_info.value is raised[0]
    assert exc_info.value.driver_error == "Serialization failure"
    assert len(native.calls) == 1
    assert sleeps == []
    assert connection.autocommit_reads == 1
    debug = retry_lines(driver_log, logging.DEBUG)
    assert len(debug) == 1 and "autocommit state could not be read" in debug[0]


def test_execute_retry_policy_applies_to_that_call_only(native, sleeps):
    native.failures = 1
    cur = make_cursor()
    cur.execute("SELECT 1", retry_policy=RetryPolicy(max_attempts=2, jitter=False))
    assert len(native.calls) == 2
    assert sleeps == [1.0]
    assert not hasattr(cur, "_retry_policy")
    # The next call without the override is back to a single attempt.
    native.calls.clear()
    native.failures = 1
    with pytest.raises(OperationalError):
        cur.execute("SELECT 1")
    assert len(native.calls) == 1
    assert sleeps == [1.0]


def test_execute_retry_policy_overrides_the_cursor_policy(native, sleeps):
    native.failures = 1
    cursor_policy = RetryPolicy(max_attempts=3, jitter=False)
    cur = make_cursor(retry_policy=cursor_policy)
    with pytest.raises(OperationalError):
        cur.execute("SELECT 1", retry_policy=RetryPolicy(max_attempts=1))
    assert len(native.calls) == 1
    assert sleeps == []
    assert cur._retry_policy is cursor_policy


def test_connection_cursor_passes_the_policy_to_the_cursor():
    policy = RetryPolicy(max_attempts=2)
    conn = connection_stub()
    conn._retry_policy = RetryPolicy(max_attempts=5)
    cursor = Connection.cursor(conn, retry_policy=policy)
    try:
        assert isinstance(cursor, Cursor)
        assert cursor._retry_policy is policy
        assert cursor in conn._cursors
    finally:
        cursor.close()


def test_cursor_does_not_inherit_the_connection_policy(native, sleeps):
    conn = connection_stub()
    conn._retry_policy = RetryPolicy(max_attempts=5, jitter=False)
    cursor = Connection.cursor(conn)
    try:
        assert cursor._retry_policy is None
        # Behaviourally too: a transient failure on such a cursor is a single attempt.
        native.failures = 1
        with pytest.raises(OperationalError):
            cursor.execute("SELECT 1")
        assert len(native.calls) == 1
        assert sleeps == []
    finally:
        cursor.close()
    direct = Cursor(conn)
    try:
        assert direct._retry_policy is None
    finally:
        direct.close()


@pytest.mark.parametrize("bad", ["nope", {"max_attempts": 3}, 3])
def test_wrong_retry_policy_type_is_rejected(native, sleeps, bad):
    conn = connection_stub()
    # The match pins the driver's own check: without it an unexpected keyword would also
    # raise TypeError and the test would pass on a build without this feature.
    with pytest.raises(TypeError, match="RetryPolicy instance or None"):
        Connection.cursor(conn, retry_policy=bad)
    assert conn._cursors == set()
    assert conn._conn.alloc_statement_handle.call_count == 0
    cur = make_cursor()
    with pytest.raises(TypeError, match="RetryPolicy instance or None"):
        cur.execute("SELECT 1", retry_policy=bad)
    assert native.calls == []
    assert sleeps == []
    assert not cur.closed


def test_cursor_rejected_for_its_policy_closes_cleanly():
    conn = connection_stub()
    cur = Cursor.__new__(Cursor)
    with pytest.raises(TypeError, match="RetryPolicy instance or None"):
        cur.__init__(conn, retry_policy="nope")
    # The close() invariant was set before the check, so the half built cursor releases
    # nothing and its cleanup cannot fail on a missing attribute.
    assert cur.hstmt is None
    cur.close()
    assert cur.closed
    assert conn._conn.alloc_statement_handle.call_count == 0


def test_retry_log_lines_omit_the_sql_text_and_parameter_values(native, sleeps, driver_log):
    native.failures = 3
    cur = make_cursor(retry_policy=RetryPolicy(max_attempts=3, jitter=False))
    with pytest.raises(OperationalError):
        cur.execute("UPDATE accounts SET secret_note = ? WHERE owner = ?", "S3CR3T", "owner_7")
    lines = retry_lines(driver_log)
    # Two retries and the give up after them, all at warning level.
    assert len(lines) == 3
    for line in lines:
        assert "accounts" not in line and "secret_note" not in line
        assert "S3CR3T" not in line and "owner_7" not in line
        assert "40001" in line
