"""
Tests for the optional retry policy on connect(), added for
https://github.com/microsoft/mssql-python/issues/682.

No test here needs a server. The native connection constructor is replaced with a fake that
fails a chosen number of times, and the retry module's sleep and random seams are replaced so
nothing sleeps and every delay sequence is asserted exactly. Neither the db_connection nor the
cursor fixture is requested, so the file runs with DB_CONNECTION_STRING unset.
"""

import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import mssql_python
import mssql_python.connection
import mssql_python.logging
import mssql_python.retry
from mssql_python import Connection, RetryPolicy, connect
from mssql_python.exceptions import OperationalError, ProgrammingError
from mssql_python.retry import DEFAULT_RETRIABLE_SQLSTATES

CONN_STR = "Server=testserver;Database=mydb;Trusted_Connection=yes;"
DRIVER_PREFIX = "[Microsoft][ODBC Driver 18 for SQL Server]"
LINK_FAILURE = "SQLSTATE:08S01:" + DRIVER_PREFIX + "Communication link failure"
LOGIN_FAILURE = "SQLSTATE:28000:" + DRIVER_PREFIX + "Login failed for user 'baduser'."
THE_SEVEN = ("HYT00", "HYT01", "08001", "08S01", "08007", "40001", "40003")


class FakeNativeConnection:
    """Stand in for ddbc_bindings.Connection that fails a set number of times, then succeeds.

    Every call records its positional arguments, so a test can assert how many attempts were
    made and that each attempt received exactly the same inputs.
    """

    def __init__(self, failures=0, message=LINK_FAILURE):
        self.failures = failures
        self.message = message
        self.calls = []

    def __call__(self, *args, **kwargs):
        # Snapshot any dict argument, so a mutation between attempts shows up as a difference
        # between recorded calls instead of the same object being compared with itself.
        self.calls.append(tuple(dict(arg) if isinstance(arg, dict) else arg for arg in args))
        if len(self.calls) <= self.failures:
            raise RuntimeError(self.message)
        native = MagicMock()
        native.get_autocommit.return_value = False
        return native


class CountingTokenProvider:
    """Minimal token_provider whose get_token() counts how often a token is requested."""

    def __init__(self):
        self.calls = 0

    def get_token(self, scope):
        self.calls += 1
        return SimpleNamespace(token="header.payload.signature", expires_on=None)


class RecordingHandler(logging.Handler):
    """Collects the formatted messages the driver logger emits."""

    def __init__(self):
        super().__init__()
        self.messages = []

    def emit(self, record):
        self.messages.append((record.levelno, record.getMessage()))


@pytest.fixture(autouse=True)
def sleeps(monkeypatch):
    """Replace the retry module's sleep with a recorder so no test ever waits."""
    recorded = []
    monkeypatch.setattr(mssql_python.retry, "_sleep", recorded.append)
    return recorded


@pytest.fixture
def native(monkeypatch):
    """Install a FakeNativeConnection in place of the pybind constructor."""
    fake = FakeNativeConnection()
    monkeypatch.setattr(mssql_python.connection.ddbc_bindings, "Connection", fake)
    return fake


@pytest.fixture
def driver_log():
    """Attach a recording handler to the driver logger for the duration of a test.

    The underlying stdlib logger sits at CRITICAL until setup_logging() is called, so its level
    is lowered to WARNING here and restored afterwards; nothing else about logging is changed.
    """
    stdlib_logger = logging.getLogger("mssql_python")
    previous_level = stdlib_logger.level
    stdlib_logger.setLevel(logging.WARNING)
    handler = RecordingHandler()
    mssql_python.logging.logger.addHandler(handler)
    try:
        yield handler
    finally:
        mssql_python.logging.logger.removeHandler(handler)
        stdlib_logger.setLevel(previous_level)


def test_no_policy_makes_a_single_attempt_and_raises_as_before(native, sleeps):
    native.failures = 1
    with pytest.raises(OperationalError) as exc_info:
        connect(CONN_STR)
    assert len(native.calls) == 1
    assert sleeps == []
    assert exc_info.value.driver_error == "Communication link failure"
    assert "Communication link failure" in exc_info.value.ddbc_error
    assert not isinstance(exc_info.value, RuntimeError)


def test_no_policy_is_stored_as_none(native):
    conn = connect(CONN_STR)
    assert conn._retry_policy is None
    assert len(native.calls) == 1


def test_policy_retries_transient_failure_until_success(native, sleeps):
    native.failures = 2
    policy = RetryPolicy(max_attempts=3, jitter=False)
    conn = connect(CONN_STR, retry_policy=policy)
    assert len(native.calls) == 3
    assert sleeps == [1.0, 2.0]
    assert conn._retry_policy is policy
    # Every attempt is made with the same connection string, attributes, pool key and factory.
    assert all(call == native.calls[0] for call in native.calls)


def test_policy_does_not_retry_permanent_failure(native, sleeps):
    native.failures = 1
    native.message = LOGIN_FAILURE
    with pytest.raises(OperationalError) as exc_info:
        connect(CONN_STR, retry_policy=RetryPolicy(max_attempts=3, jitter=False))
    assert len(native.calls) == 1
    assert sleeps == []
    assert exc_info.value.driver_error == "Invalid authorization specification"


def test_policy_exhausts_attempts_and_raises_the_mapped_type(native, sleeps):
    native.failures = 3
    with pytest.raises(OperationalError) as exc_info:
        connect(CONN_STR, retry_policy=RetryPolicy(max_attempts=3, jitter=False))
    assert len(native.calls) == 3
    assert sleeps == [1.0, 2.0]
    assert type(exc_info.value) is OperationalError
    assert exc_info.value.driver_error == "Communication link failure"
    assert "Communication link failure" in exc_info.value.ddbc_error
    assert not isinstance(exc_info.value, RuntimeError)


@pytest.mark.parametrize(
    "message",
    [
        pytest.param(DRIVER_PREFIX + "Connection handle not allocated", id="no_prefix"),
        pytest.param("SQLSTATE::" + DRIVER_PREFIX + "Invalid handle!", id="empty_code"),
    ],
)
def test_policy_does_not_retry_error_without_a_sqlstate(native, sleeps, message):
    native.failures = 1
    native.message = message
    with pytest.raises(OperationalError) as exc_info:
        connect(CONN_STR, retry_policy=RetryPolicy(max_attempts=3, jitter=False))
    assert len(native.calls) == 1
    assert sleeps == []
    assert exc_info.value.driver_error == "Connection operation failed"


def test_default_set_is_exactly_the_seven_transient_codes():
    assert DEFAULT_RETRIABLE_SQLSTATES == frozenset(THE_SEVEN)
    assert RetryPolicy().retriable_sqlstates is DEFAULT_RETRIABLE_SQLSTATES


@pytest.mark.parametrize("sqlstate", THE_SEVEN)
def test_default_policy_retries_each_transient_sqlstate(native, sleeps, sqlstate):
    assert RetryPolicy().is_retriable(sqlstate)
    assert RetryPolicy().is_retriable(sqlstate.lower())
    native.failures = 1
    native.message = "SQLSTATE:" + sqlstate + ":" + DRIVER_PREFIX + "transient failure"
    connect(CONN_STR, retry_policy=RetryPolicy(max_attempts=2, jitter=False))
    assert len(native.calls) == 2
    assert sleeps == [1.0]


@pytest.mark.parametrize(
    "sqlstate, expected",
    [
        ("08004", OperationalError),
        ("28000", OperationalError),
        ("42000", ProgrammingError),
    ],
)
def test_default_policy_does_not_retry_permanent_sqlstate(native, sleeps, sqlstate, expected):
    assert not RetryPolicy().is_retriable(sqlstate)
    native.failures = 1
    native.message = "SQLSTATE:" + sqlstate + ":" + DRIVER_PREFIX + "permanent failure"
    with pytest.raises(expected):
        connect(CONN_STR, retry_policy=RetryPolicy(max_attempts=3, jitter=False))
    assert len(native.calls) == 1
    assert sleeps == []


@pytest.mark.parametrize("sqlstate", [None, "", "08S0", "08S011"])
def test_is_retriable_rejects_missing_or_malformed_codes(sqlstate):
    assert not RetryPolicy().is_retriable(sqlstate)


def test_custom_sqlstates_replace_the_default_set(native, sleeps):
    policy = RetryPolicy(max_attempts=2, jitter=False, retriable_sqlstates={"28000"})
    assert policy.retriable_sqlstates == frozenset({"28000"})
    assert policy.is_retriable("28000")
    assert not policy.is_retriable("08S01")
    native.failures = 1
    native.message = LOGIN_FAILURE
    connect(CONN_STR, retry_policy=policy)
    assert len(native.calls) == 2
    assert sleeps == [1.0]


def test_custom_sqlstates_do_not_retry_a_default_code(native, sleeps):
    policy = RetryPolicy(max_attempts=2, jitter=False, retriable_sqlstates={"28000"})
    native.failures = 1
    with pytest.raises(OperationalError) as exc_info:
        connect(CONN_STR, retry_policy=policy)
    assert len(native.calls) == 1
    assert sleeps == []
    assert exc_info.value.driver_error == "Communication link failure"


def test_custom_sqlstates_are_upper_cased_and_accept_any_iterable():
    policy = RetryPolicy(retriable_sqlstates=["08s01", "hyt00"])
    assert policy.retriable_sqlstates == frozenset({"08S01", "HYT00"})
    assert RetryPolicy(retriable_sqlstates=()).retriable_sqlstates == frozenset()


def test_exponential_delay_doubles_and_is_capped():
    policy = RetryPolicy(max_attempts=6, base_delay=1.0, max_delay=5.0, jitter=False)
    assert [policy.compute_delay(n) for n in range(1, 6)] == [1.0, 2.0, 4.0, 5.0, 5.0]


def test_exponential_delay_with_a_huge_attempt_number_stays_at_the_cap():
    policy = RetryPolicy(jitter=False)
    assert policy.compute_delay(5000) == 30.0
    assert RetryPolicy(base_delay=0.0, jitter=False).compute_delay(5000) == 0.0


def test_fixed_delay_is_constant():
    policy = RetryPolicy(backoff="fixed", base_delay=0.25, max_delay=5.0, jitter=False)
    assert [policy.compute_delay(n) for n in range(1, 5)] == [0.25, 0.25, 0.25, 0.25]


def test_jitter_scales_the_delay_and_never_exceeds_the_cap(monkeypatch):
    policy = RetryPolicy(base_delay=1.0, max_delay=5.0, jitter=True)
    monkeypatch.setattr(mssql_python.retry, "_random", lambda: 0.0)
    assert [policy.compute_delay(n) for n in (1, 2, 3)] == [0.5, 1.0, 2.0]
    monkeypatch.setattr(mssql_python.retry, "_random", lambda: 1.0)
    assert [policy.compute_delay(n) for n in (1, 2, 3, 4)] == [1.5, 3.0, 5.0, 5.0]


def test_jittered_delays_are_used_when_retrying(native, sleeps, monkeypatch):
    monkeypatch.setattr(mssql_python.retry, "_random", lambda: 0.0)
    native.failures = 2
    connect(CONN_STR, retry_policy=RetryPolicy(max_attempts=3))
    assert sleeps == [0.5, 1.0]


@pytest.mark.parametrize("attempt", [0, -1, 1.0, True])
def test_compute_delay_rejects_an_invalid_attempt_number(attempt):
    with pytest.raises(ValueError):
        RetryPolicy().compute_delay(attempt)


def test_default_settings_match_the_issue_proposal():
    policy = RetryPolicy()
    assert policy.max_attempts == 3
    assert policy.backoff == "exponential"
    assert policy.base_delay == 1.0
    assert policy.max_delay == 30.0
    assert policy.jitter is True
    assert policy.retriable_sqlstates == DEFAULT_RETRIABLE_SQLSTATES
    assert repr(policy).startswith("RetryPolicy(max_attempts=3, backoff='exponential'")
    assert "08S01" in repr(policy)


@pytest.mark.parametrize(
    "name, value",
    [
        ("max_attempts", 0),
        ("backoff", "fixed"),
        ("base_delay", 2.0),
        ("max_delay", 60.0),
        ("jitter", False),
        ("retriable_sqlstates", frozenset({"28000"})),
    ],
)
def test_policy_settings_cannot_be_changed_after_construction(name, value):
    policy = RetryPolicy()
    with pytest.raises(AttributeError):
        setattr(policy, name, value)
    assert getattr(policy, name) == getattr(RetryPolicy(), name)


def test_single_attempt_policy_never_retries(native, sleeps):
    native.failures = 1
    with pytest.raises(OperationalError):
        connect(CONN_STR, retry_policy=RetryPolicy(max_attempts=1))
    assert len(native.calls) == 1
    assert sleeps == []


@pytest.mark.parametrize(
    "kwargs",
    [
        pytest.param({"max_attempts": 0}, id="max_attempts_zero"),
        pytest.param({"max_attempts": True}, id="max_attempts_bool"),
        pytest.param({"max_attempts": 2.0}, id="max_attempts_float"),
        pytest.param({"backoff": "linear"}, id="backoff_linear"),
        pytest.param({"base_delay": -1.0}, id="base_delay_negative"),
        pytest.param({"base_delay": float("nan")}, id="base_delay_nan"),
        pytest.param({"base_delay": 2.0, "max_delay": 1.0}, id="max_delay_below_base"),
        pytest.param({"max_delay": float("inf")}, id="max_delay_infinite"),
        pytest.param({"jitter": 1}, id="jitter_not_bool"),
        pytest.param({"retriable_sqlstates": ["08S0"]}, id="sqlstate_four_chars"),
        pytest.param({"retriable_sqlstates": "08S01"}, id="sqlstate_bare_string"),
        pytest.param({"retriable_sqlstates": [8001]}, id="sqlstate_not_a_string"),
    ],
)
def test_invalid_settings_raise_value_error(kwargs):
    with pytest.raises(ValueError):
        RetryPolicy(**kwargs)


def test_connect_rejects_a_value_that_is_not_a_policy(native, sleeps):
    with pytest.raises(TypeError):
        connect(CONN_STR, retry_policy="nope")
    with pytest.raises(TypeError):
        Connection(CONN_STR, retry_policy={"max_attempts": 3})
    assert native.calls == []
    assert sleeps == []


def test_connect_passes_the_policy_through_to_the_connection(native):
    policy = RetryPolicy(max_attempts=2)
    conn = connect(CONN_STR, retry_policy=policy)
    assert conn._retry_policy is policy
    assert len(native.calls) == 1


def test_token_is_acquired_once_across_attempts(native, sleeps):
    native.failures = 2
    provider = CountingTokenProvider()
    connect(
        "Server=testserver;Database=mydb;",
        token_provider=provider,
        retry_policy=RetryPolicy(max_attempts=3, jitter=False),
    )
    assert len(native.calls) == 3
    assert provider.calls == 1
    assert sleeps == [1.0, 2.0]


def test_retry_log_lines_name_the_attempt_and_omit_the_connection_string(
    native, sleeps, driver_log
):
    native.failures = 3
    with pytest.raises(OperationalError):
        connect(CONN_STR, retry_policy=RetryPolicy(max_attempts=3, jitter=False))
    warnings = [msg for level, msg in driver_log.messages if level == logging.WARNING]
    errors = [msg for level, msg in driver_log.messages if level == logging.ERROR]
    assert len(warnings) == 2
    assert "attempt 1 of 3" in warnings[0] and "08S01" in warnings[0]
    assert "attempt 2 of 3" in warnings[1] and "2.00 seconds" in warnings[1]
    # The final failure logs only the one error line _raise_connection_error has always written.
    assert len(errors) == 1
    assert "Connection attempt" not in errors[0]
    retry_lines = [msg for _, msg in driver_log.messages if "Connection attempt" in msg]
    assert len(retry_lines) == 2
    for message in retry_lines:
        assert "testserver" not in message
        assert "Trusted_Connection" not in message


def test_no_policy_adds_no_extra_log_lines(native, driver_log):
    native.failures = 1
    with pytest.raises(OperationalError):
        connect(CONN_STR)
    assert [msg for level, msg in driver_log.messages if level == logging.WARNING] == []
    # Only the one error line _raise_connection_error has always written.
    errors = [msg for level, msg in driver_log.messages if level == logging.ERROR]
    assert len(errors) == 1
    assert "Connection attempt" not in errors[0]


def test_retry_policy_is_exported_from_the_package():
    assert mssql_python.RetryPolicy is RetryPolicy
    assert "RetryPolicy" in mssql_python.__all__
