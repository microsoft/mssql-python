"""
Tests for the internal performance profiler.

Two layers are exercised:
- Python layer: mssql_python.perf_timer (perf_phase / perf_start / perf_stop,
  enable/disable, stats, timeline).
- C++ layer: the mssql_python.ddbc_bindings.profiling submodule backed by
  performance_counter.hpp.

The profiler is internal development tooling and is a no-op unless enabled.
Every test here resets and disables both layers on teardown so profiling state
never leaks into the rest of the suite.
"""

import os
import time

import pytest

from mssql_python import perf_timer

try:
    import mssql_python.ddbc_bindings as ddbc

    CPP_PROFILING = hasattr(ddbc, "profiling")
except ImportError:
    ddbc = None
    CPP_PROFILING = False


@pytest.fixture(autouse=True)
def _clean_profiling_state():
    """Guarantee profiling is off and empty before and after each test."""
    perf_timer.disable()
    perf_timer.disable_timeline()
    perf_timer.reset()
    if CPP_PROFILING:
        ddbc.profiling.disable()
        ddbc.profiling.disable_timeline()
        ddbc.profiling.reset()
    yield
    perf_timer.disable()
    perf_timer.disable_timeline()
    perf_timer.reset()
    if CPP_PROFILING:
        ddbc.profiling.disable()
        ddbc.profiling.disable_timeline()
        ddbc.profiling.reset()


# ---------------------------------------------------------------------------
# Python layer: mssql_python.perf_timer
# ---------------------------------------------------------------------------


def test_disabled_by_default_and_toggle():
    assert perf_timer.is_enabled() is False
    perf_timer.enable()
    assert perf_timer.is_enabled() is True
    perf_timer.disable()
    assert perf_timer.is_enabled() is False


def test_perf_phase_is_noop_when_disabled():
    with perf_timer.perf_phase("py::test::noop"):
        pass
    assert perf_timer.get_stats() == {}


def test_perf_phase_records_when_enabled():
    perf_timer.enable()
    with perf_timer.perf_phase("py::test::phase"):
        time.sleep(0.002)
    stats = perf_timer.get_stats()
    assert "py::test::phase" in stats
    entry = stats["py::test::phase"]
    assert set(entry.keys()) == {"calls", "total_us", "min_us", "max_us"}
    assert entry["calls"] == 1
    assert entry["total_us"] > 0
    assert entry["min_us"] <= entry["max_us"]


def test_perf_phase_aggregates_multiple_calls():
    perf_timer.enable()
    for _ in range(3):
        with perf_timer.perf_phase("py::test::loop"):
            time.sleep(0.001)
    entry = perf_timer.get_stats()["py::test::loop"]
    assert entry["calls"] == 3


def test_perf_start_stop_pairs():
    perf_timer.enable()
    t0 = perf_timer.perf_start()
    assert t0 > 0
    time.sleep(0.001)
    perf_timer.perf_stop("py::test::manual", t0)
    assert perf_timer.get_stats()["py::test::manual"]["calls"] == 1


def test_perf_start_stop_noop_when_disabled():
    t0 = perf_timer.perf_start()
    assert t0 == 0
    perf_timer.perf_stop("py::test::manual_disabled", t0)
    assert perf_timer.get_stats() == {}


def test_reset_stats_only_keeps_timeline():
    perf_timer.enable()
    perf_timer.enable_timeline()
    with perf_timer.perf_phase("py::test::keep_timeline"):
        time.sleep(0.001)
    assert perf_timer.get_stats() != {}
    assert perf_timer.get_timeline() != []
    perf_timer.reset_stats_only()
    assert perf_timer.get_stats() == {}
    # timeline survives reset_stats_only
    assert perf_timer.get_timeline() != []


def test_reset_clears_everything():
    perf_timer.enable()
    perf_timer.enable_timeline()
    with perf_timer.perf_phase("py::test::clear_all"):
        time.sleep(0.001)
    perf_timer.reset()
    assert perf_timer.get_stats() == {}
    assert perf_timer.get_timeline() == []


def test_timeline_event_shape():
    perf_timer.enable()
    perf_timer.enable_timeline()
    with perf_timer.perf_phase("py::test::timeline"):
        time.sleep(0.001)
    timeline = perf_timer.get_timeline()
    assert len(timeline) == 1
    ev = timeline[0]
    assert set(ev.keys()) == {"name", "start_us", "duration_us"}
    assert ev["name"] == "py::test::timeline"
    assert ev["duration_us"] >= 0


def test_timeline_not_recorded_when_timeline_disabled():
    perf_timer.enable()
    # timeline explicitly disabled
    with perf_timer.perf_phase("py::test::no_timeline"):
        time.sleep(0.001)
    assert perf_timer.get_stats() != {}
    assert perf_timer.get_timeline() == []


# ---------------------------------------------------------------------------
# C++ layer: ddbc_bindings.profiling submodule
# ---------------------------------------------------------------------------

_CONN_STR = os.getenv("DB_CONNECTION_STRING")
_needs_cpp = pytest.mark.skipif(
    not CPP_PROFILING, reason="ddbc_bindings.profiling submodule not available"
)
_needs_db = pytest.mark.skipif(not _CONN_STR, reason="DB_CONNECTION_STRING not set")


@_needs_cpp
def test_cpp_profiling_toggle():
    assert ddbc.profiling.is_enabled() is False
    ddbc.profiling.enable()
    assert ddbc.profiling.is_enabled() is True
    ddbc.profiling.disable()
    assert ddbc.profiling.is_enabled() is False


@_needs_cpp
def test_cpp_get_stats_empty_when_reset():
    ddbc.profiling.reset()
    assert ddbc.profiling.get_stats() == {}
    assert ddbc.profiling.get_timeline() == []


@_needs_cpp
@_needs_db
def test_cpp_profiling_captures_query():
    import mssql_python

    ddbc.profiling.enable()
    conn = mssql_python.connect(_CONN_STR)
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchall()
        cur.close()
    finally:
        conn.close()

    stats = ddbc.profiling.get_stats()
    assert len(stats) > 0
    # every C++ timer name carries the ddbc:: prefix
    assert all(name.startswith("ddbc::") for name in stats)
    sample = next(iter(stats.values()))
    assert {"calls", "total_us", "min_us", "max_us", "avg_us", "platform"}.issubset(sample.keys())
    assert sample["calls"] >= 1


@_needs_cpp
@_needs_db
def test_cpp_timeline_captures_events():
    import mssql_python

    ddbc.profiling.enable()
    ddbc.profiling.enable_timeline()
    conn = mssql_python.connect(_CONN_STR)
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchall()
        cur.close()
    finally:
        conn.close()

    timeline = ddbc.profiling.get_timeline()
    assert len(timeline) > 0
    ev = timeline[0]
    assert set(ev.keys()) == {"name", "start_us", "duration_us"}


@_needs_cpp
@_needs_db
def test_cpp_reset_stats_only_keeps_timeline():
    import mssql_python

    ddbc.profiling.enable()
    ddbc.profiling.enable_timeline()
    conn = mssql_python.connect(_CONN_STR)
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchall()
        cur.close()
    finally:
        conn.close()

    assert ddbc.profiling.get_stats() != {}
    assert ddbc.profiling.get_timeline() != []
    ddbc.profiling.reset_stats_only()
    assert ddbc.profiling.get_stats() == {}
    assert ddbc.profiling.get_timeline() != []
