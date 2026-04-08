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
from contextlib import contextmanager

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
    _timeline_enabled = True
    _epoch_ns = time.perf_counter_ns()


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


@contextmanager
def perf_phase(name: str):
    if not _enabled:
        yield
        return
    t0 = time.perf_counter_ns()
    yield
    elapsed = time.perf_counter_ns() - t0
    _record(name, elapsed, t0)


def perf_start() -> int:
    if not _enabled:
        return 0
    return time.perf_counter_ns()


def perf_stop(name: str, t0: int):
    if not _enabled:
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
