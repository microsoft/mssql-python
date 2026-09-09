# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.
"""
Lightweight phase-level profiling for the Python layer.

Usage in cursor.py:
    from mssql_python.perf_timer import perf_phase

    with perf_phase("py::execute::param_type_detection"):
        ...

Control from profiler script:
    from mssql_python.perf_timer import enable, disable, get_stats, reset

Stats dict matches the C++ profiling format so both layers can be
printed with the same reporter. Entries use a "py::" prefix to
distinguish from C++ timers.
"""

import threading
import time
from contextlib import contextmanager
from typing import NamedTuple


class _Counter(NamedTuple):
    calls: int
    total_ns: int
    min_ns: int
    max_ns: int


class _Event(NamedTuple):
    name: str
    start_ns: int
    duration_ns: int


_enabled = False
_lock = threading.RLock()
_local = threading.local()
_window_start_ns = 0
_stats: dict[str, _Counter] = {}
_timeline: list[_Event] = []
_timeline_enabled = False
_epoch_ns: int = 0


@contextmanager
def _bookkeeping():
    # GC can run SQL-cleanup finalizers during our own allocations. Suppress only
    # recursive samples on this thread, not the cleanup or ordinary nested phases.
    depth = getattr(_local, "depth", 0)
    _local.depth = depth + 1
    try:
        yield
    finally:
        _local.depth = depth


def enable():
    global _enabled, _window_start_ns
    with _bookkeeping():
        release = _lock.release
        _lock.acquire()
        try:
            _window_start_ns = time.perf_counter_ns()
            _enabled = True
        finally:
            release()


def disable():
    global _enabled
    with _bookkeeping():
        release = _lock.release
        _lock.acquire()
        try:
            _enabled = False
        finally:
            release()


def is_enabled() -> bool:
    return _enabled


def reset():
    global _window_start_ns, _stats, _timeline
    with _bookkeeping():
        stats, timeline = {}, []
        release = _lock.release
        _lock.acquire()
        try:
            _window_start_ns = time.perf_counter_ns()
            _stats = stats
            _timeline = timeline
        finally:
            release()


def reset_stats_only():
    global _window_start_ns, _stats
    with _bookkeeping():
        stats = {}
        release = _lock.release
        _lock.acquire()
        try:
            _window_start_ns = time.perf_counter_ns()
            _stats = stats
        finally:
            release()


def enable_timeline():
    global _timeline_enabled, _epoch_ns, _timeline
    # Clear any previously recorded events when (re)setting the epoch, so every
    # event in _timeline shares the current epoch. Otherwise a second
    # enable_timeline() without an intervening reset() would leave stale events
    # whose offsets were computed from an older epoch, corrupting the sort.
    with _bookkeeping():
        timeline = []
        release = _lock.release
        _lock.acquire()
        try:
            _timeline = timeline
            _epoch_ns = time.perf_counter_ns()
            _timeline_enabled = True
        finally:
            release()


def disable_timeline():
    global _timeline_enabled
    with _bookkeeping():
        release = _lock.release
        _lock.acquire()
        try:
            _timeline_enabled = False
        finally:
            release()


def get_timeline() -> list[dict]:
    with _bookkeeping():
        # Built-in container copies hold the GIL on supported CPython builds.
        # Immutable entries stay stable; no profiler lock surrounds GC allocations.
        snapshot = _timeline.copy()
        return [
            {
                "name": ev.name,
                "start_us": ev.start_ns // 1000,
                "duration_us": ev.duration_ns // 1000,
            }
            for ev in snapshot
        ]


def get_stats() -> dict:
    with _bookkeeping():
        snapshot = _stats.copy()
        out = {}
        for name, s in snapshot.items():
            # Keep fractional microseconds when converting accumulated samples.
            out[name] = {
                "calls": s.calls,
                "total_us": s.total_ns / 1000.0,
                "min_us": s.min_ns / 1000.0,
                "max_us": s.max_ns / 1000.0,
            }
        return out


class _NullPhase:
    """No-op context manager returned by perf_phase when profiling is disabled.

    A single shared instance is reused so the disabled path costs only a function
    call plus two slot method calls, avoiding the generator + _GeneratorContextManager
    allocation of an @contextmanager that every instrumented call site would
    otherwise pay even when profiling is off.
    """

    __slots__ = ()

    def __enter__(self):
        return None

    def __exit__(self, *exc):
        return False


class _Phase:
    """Times one phase and records it on exit (used only when enabled).

    Recording happens in __exit__, which always runs even if the wrapped block
    raises, so an exception can't silently drop the sample and desync the Python
    call counts from the C++ ones.
    """

    __slots__ = ("_name", "_t0")

    def __init__(self, name: str):
        self._name = name

    def __enter__(self):
        self._t0 = perf_start()
        return None

    def __exit__(self, *exc):
        perf_stop(self._name, self._t0)
        return False


_NULL_PHASE = _NullPhase()


def perf_phase(name: str):
    if not _enabled:
        return _NULL_PHASE
    return _Phase(name)


def perf_start() -> int:
    if not _enabled:
        return 0
    return time.perf_counter_ns()


def perf_stop(name: str, t0: int):
    # t0 == 0 means perf_start() ran while disabled (or was never called); a
    # falsy start has no valid interval, so record nothing rather than a bogus
    # "now - 0" duration.
    if not _enabled or not t0:
        return
    _record(name, time.perf_counter_ns() - t0, t0)


def _record(name: str, elapsed: int, start_ns: int = 0):
    if getattr(_local, "depth", 0):
        return
    with _bookkeeping():
        # Prebind the no-argument release: RLock.__exit__ and method lookup can
        # allocate before unlocking, which is unsafe around GC finalizers.
        release = _lock.release
        while True:
            _lock.acquire()
            try:
                if not _enabled or (start_ns and start_ns < _window_start_ns):
                    return
                stats = _stats
                entry = stats.get(name)
                window = _window_start_ns
                timeline = _timeline
                epoch = _epoch_ns
                record_event = _timeline_enabled and start_ns and start_ns >= epoch
            finally:
                release()

            # Allocate before locking: GC may run a finalizer that waits for SQL
            # on another thread, which must be able to finish its own recording.
            updated = (
                _Counter(1, elapsed, elapsed, elapsed)
                if entry is None
                else _Counter(
                    entry.calls + 1,
                    entry.total_ns + elapsed,
                    min(entry.min_ns, elapsed),
                    max(entry.max_ns, elapsed),
                )
            )
            event = _Event(name, start_ns - epoch, elapsed) if record_event else None

            _lock.acquire()
            try:
                # Revalidate after allocations and any callbacks they triggered.
                if not _enabled or window != _window_start_ns or stats is not _stats:
                    return
                if stats.get(name) is not entry:
                    continue
                stats[name] = updated
                # A timeline-only restart must not discard valid aggregate samples.
                if event is not None and _timeline_enabled and timeline is _timeline:
                    timeline.append(event)
                return
            finally:
                release()
