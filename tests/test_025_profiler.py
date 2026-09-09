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
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from importlib.util import find_spec
from unittest.mock import MagicMock

import pytest

from mssql_python import perf_timer

try:
    import mssql_python.ddbc_bindings as ddbc

    CPP_PROFILING = hasattr(ddbc, "profiling")
except ImportError:
    ddbc = None
    CPP_PROFILING = False

_needs_profiler = pytest.mark.skipif(
    find_spec("profiler") is None, reason="dev-only profiler package is not installed"
)


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


def test_submicrosecond_samples_accumulate_without_truncation():
    """Regression: each sample is accumulated in nanoseconds and converted to us
    only at get_stats(). Five 300 ns samples must sum to 1.5 us, not truncate to
    zero the way per-sample us rounding did."""
    perf_timer.enable()
    perf_timer.reset()
    for _ in range(5):
        perf_timer._record("py::test::subus", 300)  # 300 ns, well under 1 us
    entry = perf_timer.get_stats()["py::test::subus"]
    assert entry["calls"] == 5
    assert entry["total_us"] == 1.5  # 5 * 300 ns = 1500 ns = 1.5 us
    assert entry["min_us"] == 0.3  # a single sub-us sample survives as fractional us
    assert entry["max_us"] == 0.3


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


@pytest.mark.parametrize("manual", [False, True])
@pytest.mark.parametrize("restart", [False, True])
def test_timeline_epoch_change_keeps_stats_but_drops_old_span(monkeypatch, manual, restart):
    ticks = iter(range(1_000_000, 20_000_000, 1_000_000))
    monkeypatch.setattr(perf_timer.time, "perf_counter_ns", lambda: next(ticks))
    perf_timer.enable()
    if restart:
        perf_timer.enable_timeline()
    timer = perf_timer.perf_start() if manual else perf_timer.perf_phase("old")
    if not manual:
        timer.__enter__()
    perf_timer.enable_timeline()
    if manual:
        perf_timer.perf_stop("old", timer)
    else:
        timer.__exit__(None, None, None)
    assert perf_timer.get_stats()["old"]["calls"] == 1
    assert perf_timer.get_timeline() == []
    with perf_timer.perf_phase("new"):
        pass
    assert [ev["name"] for ev in perf_timer.get_timeline()] == ["new"]
    assert perf_timer.get_timeline()[0]["start_us"] >= 0


@pytest.mark.parametrize("boundary", ["disable", "enable", "reset", "reset_stats_only"])
@pytest.mark.parametrize("manual", [False, True])
def test_inflight_python_sample_cannot_cross_window(boundary, manual):
    perf_timer.enable()
    timer = perf_timer.perf_start() if manual else perf_timer.perf_phase("old")
    if not manual:
        timer.__enter__()
    getattr(perf_timer, boundary)()
    if manual:
        perf_timer.perf_stop("old", timer)
    else:
        timer.__exit__(None, None, None)
    assert perf_timer.get_stats() == {}
    perf_timer.enable()
    with perf_timer.perf_phase("new"):
        pass
    assert set(perf_timer.get_stats()) == {"new"}


def test_phase_created_before_disable_does_not_start_after_disable():
    perf_timer.enable()
    phase = perf_timer.perf_phase("late")
    perf_timer.disable()
    with phase:
        perf_timer.enable()
    assert perf_timer.get_stats() == {}


def test_zero_start_is_ignored_after_enabling():
    start = perf_timer.perf_start()
    perf_timer.enable()
    perf_timer.perf_stop("invalid", start)
    assert perf_timer.get_stats() == {}


@pytest.mark.parametrize("boundary", ["enable_timeline", "reset"])
def test_python_boundary_while_another_thread_is_in_phase(boundary):
    started = threading.Event()
    finish = threading.Event()
    perf_timer.enable()
    perf_timer.enable_timeline()

    def worker():
        with perf_timer.perf_phase("old"):
            started.set()
            assert finish.wait(5)

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(worker)
        try:
            assert started.wait(5)
            getattr(perf_timer, boundary)()
        finally:
            finish.set()
        future.result(timeout=5)
    assert perf_timer.get_timeline() == []
    assert bool(perf_timer.get_stats()) == (boundary == "enable_timeline")


# ---------------------------------------------------------------------------
# C++ layer: ddbc_bindings.profiling submodule
# ---------------------------------------------------------------------------

_CONN_STR = os.getenv("DB_CONNECTION_STRING")
_needs_cpp = pytest.mark.skipif(
    not CPP_PROFILING, reason="ddbc_bindings.profiling submodule not available"
)
_needs_db = pytest.mark.skipif(not _CONN_STR, reason="DB_CONNECTION_STRING not set")


@_needs_db
@pytest.mark.parametrize("phase", ["param_type_detection", "param_conversion"])
def test_executemany_records_failed_parameter_phase(phase, monkeypatch):
    import mssql_python

    conn = mssql_python.connect(_CONN_STR)
    try:
        with conn.cursor() as cursor:
            if phase == "param_type_detection":

                def fail_detection(_column):
                    raise ValueError("type detection failed")

                monkeypatch.setattr(cursor, "_compute_column_type", fail_detection)
                message = "type detection failed"
            else:
                cursor.setinputsizes([(mssql_python.SQL_DECIMAL, 18, 4)])
                message = "Failed to convert parameter to Decimal"
            perf_timer.enable()
            with pytest.raises(ValueError, match=message):
                cursor.executemany("SELECT ?", [("invalid decimal",)])
            assert perf_timer.get_stats()[f"py::executemany::{phase}"]["calls"] == 1
    finally:
        perf_timer.disable()
        conn.close()


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


# ---------------------------------------------------------------------------
# Profiler measurement-window isolation (profiler/core.py _ProfilingContext)
# ---------------------------------------------------------------------------


@_needs_profiler
@_needs_cpp
def test_context_collect_disables_profiling():
    """collect() must end the window: after it, profiling is off in both layers."""
    from profiler.core import _ProfilingContext

    ctx = _ProfilingContext()
    ctx.enable()
    assert perf_timer.is_enabled() is True
    assert ddbc.profiling.is_enabled() is True
    with perf_timer.perf_phase("py::window::work"):
        pass
    cpp, py = ctx.collect()
    # window recorded its work...
    assert "py::window::work" in py
    # ...and profiling is now OFF so nothing after this point is counted.
    assert perf_timer.is_enabled() is False
    assert ddbc.profiling.is_enabled() is False


@_needs_profiler
@_needs_cpp
def test_context_enable_timeline_arg_survives_collect():
    """Turning timeline on via enable(timeline=True) — without set_timeline() —
    must still make collect() preserve timeline events for collect_timeline().
    Regression: collect() used to consult only _timeline_mode, so an enable-arg
    request was silently dropped and the events were cleared."""
    from profiler.core import _ProfilingContext

    ctx = _ProfilingContext()
    ctx.enable(timeline=True)  # request timeline via the arg, not set_timeline()
    with perf_timer.perf_phase("py::tl::work"):
        pass
    ctx.collect()  # must keep timeline events (reset_stats_only, not reset)
    _, py_tl = ctx.collect_timeline()
    assert any(
        ev["name"] == "py::tl::work" for ev in py_tl
    ), f"timeline events were cleared on collect(): {py_tl}"


@_needs_profiler
@_needs_cpp
def test_context_windows_do_not_leak_into_each_other():
    """Work done between two windows must not appear in the next window's stats."""
    from profiler.core import _ProfilingContext

    ctx = _ProfilingContext()

    # Window 1: does real work.
    ctx.enable()
    with perf_timer.perf_phase("py::w1::work"):
        pass
    ctx.collect()

    # Between windows (profiling is off now): this must NOT be recorded.
    with perf_timer.perf_phase("py::between::leak"):
        pass

    # Window 2: enable() resets, and we collect immediately with no work.
    ctx.enable()
    cpp2, py2 = ctx.collect()
    assert py2 == {}, f"window 2 leaked stats from between windows: {py2}"
    assert "py::between::leak" not in py2


@_needs_profiler
@_needs_cpp
def test_context_collect_stops_inflight_python_phase():
    from profiler.core import _ProfilingContext

    ctx = _ProfilingContext()
    ctx.enable(timeline=True)
    with perf_timer.perf_phase("old"):
        ctx.collect()
    assert perf_timer.get_stats() == {}
    assert ctx.collect_timeline() == ([], [])


@_needs_profiler
@_needs_cpp
def test_context_can_turn_timeline_off_between_windows():
    from profiler.core import _ProfilingContext

    ctx = _ProfilingContext()
    ctx.enable(timeline=True)
    ctx.collect()
    ctx.set_timeline(False)
    ctx.enable()
    with perf_timer.perf_phase("stats_only"):
        pass
    _, stats = ctx.collect()
    assert stats["stats_only"]["calls"] == 1
    assert ctx.collect_timeline() == ([], [])


@_needs_profiler
@pytest.mark.parametrize(
    "name,kwargs",
    [
        ("execute_insert", {"table": "#unused", "count": 1}),
        ("executemany", {"table": "#unused", "row_count": 1}),
        ("commit_rollback", {"count": 1}),
        ("insertmanyvalues", {"rows_per_batch": 1, "total_rows": 1}),
    ],
)
@pytest.mark.parametrize("rollback_fails", [False, True])
def test_scenario_failure_closes_cursor_even_if_rollback_fails(name, kwargs, rollback_fails):
    from profiler import scenarios

    conn = MagicMock()
    cursor = conn.cursor.return_value
    cursor.__enter__.return_value = cursor
    error = RuntimeError("query failed")
    cursor.execute.side_effect = error
    cursor.executemany.side_effect = error
    if rollback_fails:
        conn.rollback.side_effect = RuntimeError("rollback failed")
    ctx = MagicMock()
    expected = "rollback failed" if rollback_fails else "query failed"
    with pytest.raises(RuntimeError, match=expected):
        getattr(scenarios, name)(conn, ctx=ctx, **kwargs)
    conn.rollback.assert_called_once()
    cursor.__exit__.assert_called_once()
    ctx.disable.assert_called_once()


@_needs_profiler
def test_setup_failure_releases_cursor_and_transaction():
    from profiler.scenarios import setup_test_data

    conn = MagicMock()
    cursor = conn.cursor.return_value
    cursor.__enter__.return_value = cursor
    cursor.executemany.side_effect = RuntimeError("setup failed")
    with pytest.raises(RuntimeError, match="setup failed"):
        setup_test_data(conn, row_count=1)
    conn.rollback.assert_called_once()
    cursor.__exit__.assert_called_once()


@_needs_profiler
def test_connect_scenario_closes_connection_if_collection_fails(monkeypatch):
    from profiler.scenarios import connect

    conn = MagicMock()
    monkeypatch.setattr("mssql_python.connect", lambda _: conn)
    ctx = MagicMock()
    ctx.collect.side_effect = RuntimeError("collection failed")
    with pytest.raises(RuntimeError, match="collection failed"):
        connect("Server=localhost", ctx)
    conn.close.assert_called_once()
    ctx.disable.assert_called_once()


@_needs_profiler
def test_connect_scenario_disables_profiling_if_connection_fails(monkeypatch):
    from profiler.scenarios import connect

    def fail_connect(_):
        raise RuntimeError("connection failed")

    monkeypatch.setattr("mssql_python.connect", fail_connect)
    ctx = MagicMock()
    with pytest.raises(RuntimeError, match="connection failed"):
        connect("Server=localhost", ctx)
    ctx.disable.assert_called_once()
    ctx.collect.assert_not_called()


@_needs_profiler
@_needs_cpp
@_needs_db
@pytest.mark.parametrize("name", ["execute_insert", "executemany"])
def test_failed_insert_scenario_rolls_back_partial_work(name):
    import mssql_python
    from profiler import scenarios
    from profiler.core import _ProfilingContext

    conn = mssql_python.connect(_CONN_STR)
    ctx = _ProfilingContext()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                scenarios._CREATE_TABLE.replace("int_col INT,", "int_col INT CHECK (int_col = 0),")
            )
        conn.commit()
        kwargs = {"count": 2} if name == "execute_insert" else {"row_count": 2}
        with pytest.raises(mssql_python.DatabaseError):
            getattr(scenarios, name)(conn, "#perf_test", ctx, **kwargs)
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM #perf_test")
            assert cursor.fetchone()[0] == 0
        assert not perf_timer.is_enabled()
        assert not ddbc.profiling.is_enabled()
    finally:
        ctx.disable()
        conn.close()


@_needs_profiler
@_needs_cpp
def test_context_disable_turns_everything_off():
    from profiler.core import _ProfilingContext

    ctx = _ProfilingContext()
    ctx.enable(timeline=True)
    ctx.disable()
    assert perf_timer.is_enabled() is False
    assert ddbc.profiling.is_enabled() is False


@_needs_profiler
@_needs_cpp
@_needs_db
def test_run_script_disables_profiling_even_when_script_raises(tmp_path):
    """A user script that raises must not leave profiling enabled."""
    from profiler.core import Profiler

    bad = tmp_path / "boom.py"
    bad.write_text("cursor.execute('SELECT 1')\nraise RuntimeError('boom')\n")

    p = Profiler(_CONN_STR)
    try:
        with pytest.raises(RuntimeError):
            p.run_script(str(bad))
        # window must be closed despite the exception
        assert perf_timer.is_enabled() is False
        assert ddbc.profiling.is_enabled() is False
    finally:
        p.close()


def test_enable_timeline_clears_stale_events():
    """A second enable_timeline() must not leave events from the previous epoch."""
    perf_timer.enable()
    perf_timer.enable_timeline()
    with perf_timer.perf_phase("py::epoch1::evt"):
        pass
    assert len(perf_timer.get_timeline()) == 1
    # Re-arm timeline without an explicit reset(): stale events must be dropped
    # so all remaining events share the new epoch.
    perf_timer.enable_timeline()
    assert perf_timer.get_timeline() == []
    with perf_timer.perf_phase("py::epoch2::evt"):
        pass
    tl = perf_timer.get_timeline()
    assert len(tl) == 1
    assert tl[0]["name"] == "py::epoch2::evt"


def test_perf_phase_records_even_when_body_raises():
    """__exit__ must record the sample even if the wrapped block raises, so the
    Python call counts don't silently desync from the C++ ones."""
    perf_timer.enable()
    with pytest.raises(ValueError):
        with perf_timer.perf_phase("py::raises::evt"):
            raise ValueError("boom")
    stats = perf_timer.get_stats()
    assert "py::raises::evt" in stats
    assert stats["py::raises::evt"]["calls"] == 1


def test_perf_phase_disabled_returns_shared_noop():
    """Disabled perf_phase returns the shared singleton (no per-call allocation)."""
    assert perf_timer.is_enabled() is False
    a = perf_timer.perf_phase("py::x")
    b = perf_timer.perf_phase("py::y")
    assert a is b  # same shared _NULL_PHASE instance
    with a:
        pass
    assert perf_timer.get_stats() == {}


@_needs_profiler
@_needs_cpp
@_needs_db
def test_run_script_bad_syntax_still_cleans_up(tmp_path):
    """A script that fails to COMPILE must still disable profiling (and not leak)."""
    from profiler.core import Profiler

    bad = tmp_path / "syntax.py"
    bad.write_text("this is not valid python !!!\n")

    p = Profiler(_CONN_STR)
    try:
        with pytest.raises(SyntaxError):
            p.run_script(str(bad))
        assert perf_timer.is_enabled() is False
        assert ddbc.profiling.is_enabled() is False
    finally:
        p.close()


@_needs_profiler
@_needs_cpp
@_needs_db
def test_run_script_wall_time_reflects_exec_not_io(tmp_path):
    """wall_ms should track the script's execution (a known sleep), which only
    holds because the timer starts after read+compile. If read/compile were
    inside the window the assertion would still pass, so we also bound it from
    above to catch gross inflation."""
    from profiler.core import Profiler

    # Script sleeps a known amount; that sleep must show up in wall_ms.
    script = tmp_path / "sleeper.py"
    script.write_text("import time\ntime.sleep(0.20)\n")

    p = Profiler(_CONN_STR)
    try:
        result = p.run_script(str(script))
        # Lower bound: the 200 ms sleep must be measured (window covers exec).
        assert result["wall_ms"] >= 180
        # Upper bound: not grossly inflated beyond the sleep (a few hundred ms
        # of slack for interpreter overhead, never seconds of I/O).
        assert result["wall_ms"] < 1000
    finally:
        p.close()
