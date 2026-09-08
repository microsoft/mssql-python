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

import time

_enabled = False
_stats: dict[str, dict] = {}
_timeline: list[dict] = []
_timeline_enabled = False
_epoch_ns: int = 0


def enable():
    global _enabled
    _enabled = True


def disable():
    global _enabled
    _enabled = False


def is_enabled() -> bool:
    return _enabled


def reset():
    _stats.clear()
    _timeline.clear()


def reset_stats_only():
    _stats.clear()


def enable_timeline():
    global _timeline_enabled, _epoch_ns
    # Clear any previously recorded events when (re)setting the epoch, so every
    # event in _timeline shares the current epoch. Otherwise a second
    # enable_timeline() without an intervening reset() would leave stale events
    # whose offsets were computed from an older epoch, corrupting the sort.
    _timeline.clear()
    _epoch_ns = time.perf_counter_ns()
    _timeline_enabled = True


def disable_timeline():
    global _timeline_enabled
    _timeline_enabled = False


def get_timeline() -> list[dict]:
    return [
        {
            "name": ev["name"],
            "start_us": ev["start_ns"] // 1000,
            "duration_us": ev["duration_ns"] // 1000,
        }
        for ev in _timeline
    ]


def get_stats() -> dict:
    out = {}
    for name, s in _stats.items():
        out[name] = {
            "calls": s["calls"],
            "total_us": s["total_ns"] // 1000,
            "min_us": s["min_ns"] // 1000,
            "max_us": s["max_ns"] // 1000,
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
        self._t0 = time.perf_counter_ns()
        return None

    def __exit__(self, *exc):
        _record(self._name, time.perf_counter_ns() - self._t0, self._t0)
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
    entry = _stats.get(name)
    if entry is None:
        _stats[name] = {
            "calls": 1,
            "total_ns": elapsed,
            "min_ns": elapsed,
            "max_ns": elapsed,
        }
    else:
        entry["calls"] += 1
        entry["total_ns"] += elapsed
        if elapsed < entry["min_ns"]:
            entry["min_ns"] = elapsed
        if elapsed > entry["max_ns"]:
            entry["max_ns"] = elapsed

    if _timeline_enabled and start_ns:
        _timeline.append(
            {
                "name": name,
                "start_ns": start_ns - _epoch_ns,
                "duration_ns": elapsed,
            }
        )
