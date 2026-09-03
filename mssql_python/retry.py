"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
This module defines the RetryPolicy class, which describes how connect() retries a connection
attempt that fails with a transient error.
"""

import math
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

_BACKOFF_STRATEGIES = ("exponential", "fixed")
_SQLSTATE_LENGTH = 5


def _is_finite_number(value: object) -> bool:
    """Return True for a finite int or float that is not a bool."""
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _normalize_sqlstates(codes: Optional[Iterable[str]]) -> FrozenSet[str]:
    """Validate and upper case a caller supplied set of SQLSTATE codes.

    Args:
        codes (iterable of str, optional): SQLSTATE codes, or None for the driver default set.

    Returns:
        FrozenSet[str]: The upper cased codes, or ``DEFAULT_RETRIABLE_SQLSTATES`` for None.

    Raises:
        ValueError: If ``codes`` is a single string, or any code is not a string of exactly
            five characters.
    """
    if codes is None:
        return DEFAULT_RETRIABLE_SQLSTATES
    if isinstance(codes, (str, bytes)):
        raise ValueError(
            "retriable_sqlstates must be an iterable of SQLSTATE strings, not a single string"
        )
    normalized = set()
    for code in codes:
        if not isinstance(code, str) or len(code) != _SQLSTATE_LENGTH:
            raise ValueError(
                f"each SQLSTATE must be a string of exactly {_SQLSTATE_LENGTH} characters, "
                f"got {code!r}"
            )
        normalized.add(code.upper())
    return frozenset(normalized)


class RetryPolicy:
    """Describes how ``connect()`` retries a connection attempt that fails with a transient error.

    A policy is optional: ``connect()`` and ``Connection()`` make a single attempt unless one is
    passed as ``retry_policy=``. When the native connect raises with a SQLSTATE in
    ``retriable_sqlstates``, the driver waits for ``compute_delay(attempt)`` seconds and tries
    again, up to ``max_attempts`` tries in total. Any other failure is raised at once, as the
    same exception type it has always been.

    Every setting is validated once in ``__init__`` and exposed through a property with no
    setter, so an instance cannot be changed after construction and the same policy can be
    shared by any number of connections.

    Attributes:
        max_attempts (int): Total number of tries, including the first. 1 means never retry.
        backoff (str): "exponential" doubles the delay after each failed attempt, "fixed"
            waits ``base_delay`` every time.
        base_delay (float): Delay in seconds before the second attempt.
        max_delay (float): Upper bound in seconds for any single delay, jitter included.
        jitter (bool): When True each delay is scaled by a factor drawn uniformly from
            [0.5, 1.5) so that many clients do not reconnect in lockstep.
        retriable_sqlstates (frozenset): The SQLSTATE codes that are retried, uppercased and
            each exactly five characters. Defaults to ``DEFAULT_RETRIABLE_SQLSTATES``; a custom
            set replaces the default entirely rather than extending it.

    Example:
        >>> import mssql_python as ms
        >>> policy = ms.RetryPolicy(max_attempts=5, base_delay=0.5, max_delay=10.0)
        >>> conn = ms.connect("Server=myserver;Database=mydb", retry_policy=policy)
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
            base_delay (float): Seconds to wait before the second attempt; zero or more.
            max_delay (float): Cap in seconds for every delay; at least ``base_delay``.
            jitter (bool): Scale each delay by a random factor in [0.5, 1.5).
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
        if not _is_finite_number(base_delay) or base_delay < 0:
            raise ValueError("base_delay must be a finite number of zero or more seconds")
        if not _is_finite_number(max_delay) or max_delay < base_delay:
            raise ValueError("max_delay must be a finite number of at least base_delay seconds")
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
        """Delay in seconds before the second attempt."""
        return self._base_delay

    @property
    def max_delay(self) -> float:
        """Upper bound in seconds for any single delay, jitter included."""
        return self._max_delay

    @property
    def jitter(self) -> bool:
        """Whether each delay is scaled by a random factor in [0.5, 1.5)."""
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

    def compute_delay(self, attempt: int) -> float:
        """Return how long to wait, in seconds, after a failed attempt.

        Args:
            attempt (int): Index, counting from 1, of the attempt that just failed, so the
                delay before the second attempt is ``compute_delay(1)``.

        Returns:
            float: Seconds to wait, never negative and never above ``max_delay``.

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
            delay = min(delay * (0.5 + _random()), self.max_delay)
        return delay

    def __repr__(self) -> str:
        """Return a constructor style representation of the policy."""
        return (
            f"RetryPolicy(max_attempts={self.max_attempts!r}, backoff={self.backoff!r}, "
            f"base_delay={self.base_delay!r}, max_delay={self.max_delay!r}, "
            f"jitter={self.jitter!r}, retriable_sqlstates={sorted(self.retriable_sqlstates)!r})"
        )
