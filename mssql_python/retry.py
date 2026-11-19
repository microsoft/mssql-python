"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
This module defines the RetryPolicy class, which describes how connect() retries a connection
attempt, and how Cursor.execute() retries a statement on the same connection, when the failure
carries a transient SQLSTATE.
"""

import random
import time
from typing import FrozenSet, Iterable, Optional

# Seams for tests. Both are looked up on this module at call time, so a test can replace them
# and assert on the exact delay sequence without sleeping or depending on the random source.
_sleep = time.sleep
_random = random.random

# SQLSTATEs the driver treats as transient at connect time. These are the seven transient codes
# from the retry logic page for the driver on Microsoft Learn
# (https://learn.microsoft.com/sql/connect/python/mssql-python/retry-logic), applied here to
# the connect attempt: HYT00 and HYT01 (a timeout), 08001, 08S01 and 08007 (the link could not
# be established or was lost), 40001 (serialization failure) and 40003 (statement completion
# unknown). 08004, "Server rejected the connection", is deliberately excluded: the server
# answered and refused, so the same request is not going to be accepted on the next try.
DEFAULT_RETRIABLE_SQLSTATES: FrozenSet[str] = frozenset(
    {"HYT00", "HYT01", "08001", "08S01", "08007", "40001", "40003"}
)

# Transient codes that a statement retry on the same connection can never turn into a success,
# so RetryPolicy.is_retriable_on_same_connection() refuses them even when the policy lists them.
# HYT01 is the connection timeout, not the query timeout (HYT00): the server did not answer a
# request on the link at all, which points at the connection rather than the statement, so
# the statement is not run on that connection again. 40003 is raised when the connection fails
# while a statement is in flight: the driver has no way to know whether the statement ran, so
# running it again could repeat work the server already did. Class 08 codes (the link itself
# failed) are refused by prefix in the same method rather than listed here. All of these stay
# retriable at connect scope, where a retry opens a fresh connection instead of reusing a
# broken one.
_NEVER_RETRIED_ON_SAME_CONNECTION: FrozenSet[str] = frozenset({"HYT01", "40003"})

_BACKOFF_STRATEGIES = ("exponential", "fixed")
_SQLSTATE_LENGTH = 5

# Upper bound for base_delay and max_delay: one day. It keeps every wait well inside the range
# time.sleep accepts on all supported platforms (about 49.7 days on Windows and about 3.2 years
# on macOS under Python 3.10), so a policy that validates can never fail inside the retry loop.
_MAX_DELAY_SECONDS = 86400.0


def _is_valid_delay(value: object) -> bool:
    """Return True for an int or float, not a bool, from zero to ``_MAX_DELAY_SECONDS``.

    NaN and infinity fail the range comparison, and an int too large for a float is compared
    exactly, so neither needs a separate check.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    return 0 <= value <= _MAX_DELAY_SECONDS


def _normalize_sqlstates(codes: Optional[Iterable[str]]) -> FrozenSet[str]:
    """Validate and upper case a caller supplied set of SQLSTATE codes.

    Args:
        codes (iterable of str, optional): SQLSTATE codes, or None for the driver default set.

    Returns:
        FrozenSet[str]: The upper cased codes, or ``DEFAULT_RETRIABLE_SQLSTATES`` for None.

    Raises:
        ValueError: If ``codes`` is a single string or not iterable, or any code is not a
            string of exactly five ASCII letters or digits.
    """
    if codes is None:
        return DEFAULT_RETRIABLE_SQLSTATES
    if isinstance(codes, (str, bytes)):
        raise ValueError(
            "retriable_sqlstates must be an iterable of SQLSTATE strings, not a single string"
        )
    # Only iter() sits in the try, so a TypeError raised inside a caller's generator is not
    # relabelled as a bad setting.
    try:
        iterator = iter(codes)
    except TypeError:
        raise ValueError(
            "retriable_sqlstates must be an iterable of SQLSTATE strings, "
            f"got {type(codes).__name__}"
        ) from None
    normalized = set()
    for code in iterator:
        # Checked on the original string: ASCII only means upper() cannot change the length,
        # and letters or digits are the only characters a driver SQLSTATE is parsed from.
        if (
            not isinstance(code, str)
            or len(code) != _SQLSTATE_LENGTH
            or not (code.isascii() and code.isalnum())
        ):
            raise ValueError(
                f"each SQLSTATE must be {_SQLSTATE_LENGTH} ASCII letters or digits, got {code!r}"
            )
        normalized.add(code.upper())
    return frozenset(normalized)


class RetryPolicy:
    """Describes how the driver retries an operation that fails with a transient error.

    A policy is optional and works at two scopes. Passed as ``retry_policy=`` to ``connect()``
    or ``Connection()`` it retries the native connect: when that raises with a SQLSTATE in
    ``retriable_sqlstates``, the driver waits for ``compute_delay(attempt)`` seconds and tries
    again, up to ``max_attempts`` tries in total. Passed to ``Connection.cursor()`` or to a
    single ``Cursor.execute()`` call it retries the statement on the same connection, from its
    beginning, under the narrower rule of ``is_retriable_on_same_connection()`` and only while
    the connection is in autocommit mode; ``Cursor.execute`` documents what such a retry does
    and does not guarantee. A policy given to ``connect()`` never applies to the cursors of
    that connection. Without a policy each operation makes a single attempt, and any failure
    that is not retried is raised at once, as the same exception type it has always been.

    Every setting is validated once in ``__init__`` and exposed through a property with no
    setter, so an instance cannot be changed after construction and the same policy can be
    shared by any number of connections and cursors.

    Attributes:
        max_attempts (int): Total number of tries, including the first. 1 means never retry.
        backoff (str): "exponential" doubles the delay after each failed attempt, "fixed"
            waits ``base_delay`` every time.
        base_delay (float): Delay in seconds before the second attempt, before jitter.
        max_delay (float): Upper bound in seconds for any single delay, jitter included.
        jitter (bool): When True each delay is scaled by a factor drawn uniformly from
            [0, 1), so many clients do not reconnect in lockstep. The delay can be shorter than
            ``base_delay`` and can be zero.
        retriable_sqlstates (frozenset): The SQLSTATE codes that are retried, uppercased and
            each exactly five ASCII letters or digits. Defaults to
            ``DEFAULT_RETRIABLE_SQLSTATES``; a custom set replaces the default entirely rather
            than extending it.

    Example:
        >>> import mssql_python as ms
        >>> policy = ms.RetryPolicy(max_attempts=5, base_delay=0.5, max_delay=10.0)
        >>> conn = ms.connect("Server=myserver;Database=mydb", retry_policy=policy)
        >>> cursor = conn.cursor(retry_policy=policy)  # statement retry needs autocommit on
    """

    def __init__(
        self,
        max_attempts: int = 3,
        backoff: str = "exponential",
        base_delay: float = 1.0,
        max_delay: float = 30.0,
        jitter: bool = True,
        retriable_sqlstates: Optional[Iterable[str]] = None,
    ) -> None:
        """Validate the settings and build the policy.

        Args:
            max_attempts (int): Total number of tries including the first; at least 1.
            backoff (str): "exponential" or "fixed".
            base_delay (float): Seconds to wait before the second attempt, before jitter;
                zero to 86400 (one day).
            max_delay (float): Cap in seconds for every delay; at least ``base_delay`` and at
                most 86400.
            jitter (bool): Scale each delay down by a random factor in [0, 1), so the wait can
                be anywhere between zero and the backoff delay.
            retriable_sqlstates (iterable of str, optional): SQLSTATE codes to retry. None
                selects ``DEFAULT_RETRIABLE_SQLSTATES``. Codes are upper cased.

        Raises:
            ValueError: If any setting is out of range or of the wrong type.
        """
        if isinstance(max_attempts, bool) or not isinstance(max_attempts, int):
            raise ValueError("max_attempts must be an integer of at least 1")
        if max_attempts < 1:
            raise ValueError("max_attempts must be an integer of at least 1")
        if backoff not in _BACKOFF_STRATEGIES:
            raise ValueError("backoff must be one of 'exponential' or 'fixed'")
        if not _is_valid_delay(base_delay):
            raise ValueError("base_delay must be a number of seconds from 0 to 86400")
        if not _is_valid_delay(max_delay) or max_delay < base_delay:
            raise ValueError("max_delay must be a number of seconds from base_delay to 86400")
        if not isinstance(jitter, bool):
            raise ValueError("jitter must be True or False")

        self._max_attempts: int = max_attempts
        self._backoff: str = backoff
        self._base_delay: float = float(base_delay)
        self._max_delay: float = float(max_delay)
        self._jitter: bool = jitter
        self._retriable_sqlstates: FrozenSet[str] = _normalize_sqlstates(retriable_sqlstates)

    @property
    def max_attempts(self) -> int:
        """Total number of tries, including the first."""
        return self._max_attempts

    @property
    def backoff(self) -> str:
        """Backoff strategy, "exponential" or "fixed"."""
        return self._backoff

    @property
    def base_delay(self) -> float:
        """Delay in seconds before the second attempt, before jitter."""
        return self._base_delay

    @property
    def max_delay(self) -> float:
        """Upper bound in seconds for any single delay, jitter included."""
        return self._max_delay

    @property
    def jitter(self) -> bool:
        """Whether each delay is scaled down by a random factor in [0, 1)."""
        return self._jitter

    @property
    def retriable_sqlstates(self) -> FrozenSet[str]:
        """The SQLSTATE codes this policy retries."""
        return self._retriable_sqlstates

    def is_retriable(self, sqlstate: Optional[str]) -> bool:
        """Return True when ``sqlstate`` is one of the codes this policy retries.

        Args:
            sqlstate (str, optional): SQLSTATE code from the failed attempt, or None when the
                failure carried no SQLSTATE. None is never retriable.

        Returns:
            bool: True only when the upper cased code is in ``retriable_sqlstates``.
        """
        if not isinstance(sqlstate, str):
            return False
        return sqlstate.upper() in self.retriable_sqlstates

    def is_retriable_on_same_connection(self, sqlstate: Optional[str]) -> bool:
        """Return True when a statement may run again on the connection it failed on.

        This is the test ``Cursor.execute()`` applies. It is ``is_retriable(sqlstate)`` narrowed
        by the codes a retry on the same connection can never satisfy: every class 08 code,
        since the link itself failed and only a fresh connection (the ``connect()`` scope) can
        help, plus ``HYT01`` (connection timeout) and ``40003`` (statement completion unknown).
        Those codes are refused here even when the policy lists them.

        Args:
            sqlstate (str, optional): SQLSTATE code from the failed statement, or None when the
                failure carried no SQLSTATE. None is never retriable.

        Returns:
            bool: True only when ``is_retriable(sqlstate)`` is True and the upper cased code is
                neither a class 08 code nor ``HYT01`` or ``40003``.
        """
        if not isinstance(sqlstate, str) or not self.is_retriable(sqlstate):
            return False
        code = sqlstate.upper()
        return not (code.startswith("08") or code in _NEVER_RETRIED_ON_SAME_CONNECTION)

    def compute_delay(self, attempt: int) -> float:
        """Return how long to wait, in seconds, after a failed attempt.

        Args:
            attempt (int): Index, counting from 1, of the attempt that just failed, so the
                delay before the second attempt is ``compute_delay(1)``.

        Returns:
            float: Seconds to wait, never negative and never above ``max_delay``. With jitter
                on, the capped delay is scaled by a random factor in [0, 1), so zero is possible.

        Raises:
            ValueError: If ``attempt`` is less than 1.
        """
        if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 1:
            raise ValueError("attempt must be an integer of at least 1")
        delay = self.base_delay
        if self.backoff == "exponential":
            # Double once per failed attempt and stop as soon as the cap is reached, so a large
            # attempt number can never overflow the way a direct power of two would.
            doublings = attempt - 1
            while doublings > 0 and 0.0 < delay < self.max_delay:
                delay *= 2.0
                doublings -= 1
        delay = min(delay, self.max_delay)
        if self.jitter:
            # Full jitter: scale down by a factor in [0, 1) rather than around the delay. Scaling
            # around it meant every draw at or above the midpoint clamped to max_delay, so once
            # backoff reached the cap about half of all clients waited the identical amount.
            delay *= _random()
        return delay

    def __repr__(self) -> str:
        """Return a constructor style representation of the policy."""
        return (
            f"RetryPolicy(max_attempts={self.max_attempts!r}, backoff={self.backoff!r}, "
            f"base_delay={self.base_delay!r}, max_delay={self.max_delay!r}, "
            f"jitter={self.jitter!r}, retriable_sqlstates={sorted(self.retriable_sqlstates)!r})"
        )
