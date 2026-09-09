# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.
"""
Profiler core — orchestrates scenarios, collects stats from both layers.

    from profiler import Profiler

    p = Profiler("Server=localhost,1433;UID=sa;Pwd=...;Encrypt=no;TrustServerCertificate=yes;")
    p.run()              # all scenarios
    p.run("fetchall")    # one scenario
"""

from __future__ import annotations

import os
import platform

from profiler.reporter import print_stats, print_timeline
from profiler.scenarios import SCENARIOS, setup_test_data


class _ProfilingContext:
    """Thin wrapper that enables/disables/collects from both C++ and Python profiling."""

    def __init__(self):
        from mssql_python import ddbc_bindings, perf_timer

        if not hasattr(ddbc_bindings, "profiling"):
            raise RuntimeError(
                "Native profiling is not available in this build. The C++ extension "
                "was built without profiling instrumentation. Rebuild it from the "
                "mssql_python/pybind directory with ENABLE_PROFILING=1 (e.g. "
                "`cd mssql_python/pybind && ENABLE_PROFILING=1 bash build.sh`, or "
                "`cd mssql_python\\pybind` then `set ENABLE_PROFILING=1` and "
                "`build.bat` on Windows) before running the profiler."
            )
        self._cpp = ddbc_bindings.profiling
        self._py = perf_timer
        self._timeline_mode = False

    def set_timeline(self, on: bool):
        self._timeline_mode = on

    def enable(self, timeline: bool = False):
        # Start a clean measurement window. Reset first so anything that leaked
        # between windows (teardown of the previous scenario, test-data setup, a
        # scenario's own pre-enable cursor.execute) is discarded and only the work
        # between this enable() and the matching collect() is ever counted.
        # Persist the timeline decision so collect() (which preserves timeline
        # events only when _timeline_mode is set) can never disagree with how
        # timeline recording was turned on here.
        if timeline:
            self._timeline_mode = True
        self._cpp.reset()
        self._py.reset()
        # Set the timeline epoch BEFORE enabling profiling. Otherwise a timer that
        # starts in the gap between enable() and enable_timeline() (native timers
        # run with the GIL released) would finish with start_ < epoch_ and record a
        # negative timeline offset, breaking ordering and indentation.
        if self._timeline_mode:
            self._cpp.enable_timeline()
            self._py.enable_timeline()
        self._cpp.enable()
        self._py.enable()

    def collect(self) -> tuple[dict, dict]:
        # End the measurement window: snapshot, then turn profiling OFF so nothing
        # outside a window (commit/close, inter-scenario setup) gets recorded.
        cpp = self._cpp.get_stats()
        py = self._py.get_stats()
        self._cpp.disable()
        self._py.disable()
        if self._timeline_mode:
            # Keep timeline events for the subsequent collect_timeline(); clearing
            # only aggregate counters. Recording is already gated off by disable().
            self._cpp.reset_stats_only()
            self._py.reset_stats_only()
        else:
            self._cpp.reset()
            self._py.reset()
        return cpp, py

    def collect_timeline(self) -> tuple[list, list]:
        cpp_tl = self._cpp.get_timeline()
        py_tl = self._py.get_timeline()
        self._cpp.reset()
        self._py.reset()
        return cpp_tl, py_tl

    def disable(self):
        # Fully turn profiling off (both aggregate and timeline recording).
        self._cpp.disable()
        self._py.disable()
        self._cpp.disable_timeline()
        self._py.disable_timeline()

    def disable_timeline(self):
        self._cpp.disable_timeline()
        self._py.disable_timeline()


class Profiler:
    def __init__(self, conn_str: str | None = None, timeline: bool = False):
        self.conn_str = conn_str or os.getenv("DB_CONNECTION_STRING")
        if not self.conn_str:
            raise ValueError(
                "Connection string required. Pass it directly or set DB_CONNECTION_STRING."
            )
        self._ctx = _ProfilingContext()
        self._timeline = timeline
        self._ctx.set_timeline(timeline)
        self._conn = None
        self._table = None
        self._results: list[dict] = []

    def _ensure_connection(self):
        if self._conn is None:
            from mssql_python import connect

            self._conn = connect(self.conn_str)
            self._conn.autocommit = False

    def _ensure_test_data(self):
        if self._table is None:
            self._ensure_connection()
            print("Setting up test data...", end=" ", flush=True)
            self._table = setup_test_data(self._conn)
            # No drain needed: each scenario's enable() resets before measuring,
            # so any stats generated by setup are discarded at the window start.
            print("Done", flush=True)

    def run(self, *scenario_names: str) -> list[dict]:
        names = list(scenario_names) if scenario_names else list(SCENARIOS.keys())
        unknown = set(names) - set(SCENARIOS.keys())
        if unknown:
            raise ValueError(f"Unknown scenarios: {unknown}. Available: {list(SCENARIOS.keys())}")

        self._print_header()
        results = []

        for i, name in enumerate(names, 1):
            fn, needs_table = SCENARIOS[name]
            print(f"\n{'#' * 100}")
            print(f"# {i}. {name.upper()}")
            print(f"{'#' * 100}")

            # Build args based on what the scenario function needs. Wrap the call
            # so a scenario that raises can never leave profiling enabled and bleed
            # into the next scenario — one guard here covers all scenarios (and any
            # future ones) instead of a try/finally in every scenario body.
            try:
                if name == "connect":
                    result = fn(self.conn_str, self._ctx)
                elif name == "insertmanyvalues":
                    self._ensure_connection()
                    result = fn(self._conn, self._ctx)
                elif name == "commit_rollback":
                    self._ensure_connection()
                    result = fn(self._conn, self._ctx)
                elif needs_table:
                    self._ensure_test_data()
                    result = fn(self._conn, self._table, self._ctx)
                else:
                    self._ensure_connection()
                    result = fn(self._conn, self._ctx)
            finally:
                self._ctx.disable()

            # Collect timeline if enabled
            if self._timeline:
                cpp_tl, py_tl = self._ctx.collect_timeline()
                self._ctx.disable_timeline()
                result["cpp_timeline"] = cpp_tl
                result["py_timeline"] = py_tl

            # Print result
            detail = result.get("detail", "")
            if detail:
                print(f"\n  {detail}, Wall clock: {result['wall_ms']:.1f}ms")
            else:
                print(f"\n  Wall clock: {result['wall_ms']:.1f}ms")

            if self._timeline:
                print_timeline(
                    result.get("cpp_timeline"), result.get("py_timeline"), result["title"]
                )
            else:
                print_stats(result["cpp"], result["py"], result["title"])

            results.append(result)

        self._results = results
        self._print_footer()
        return results

    def run_script(self, script_path: str) -> dict:
        """Run a user-supplied .py script and report whatever timers it hits.

        The script gets `conn` (a live Connection) and `cursor` (a fresh Cursor)
        injected into its namespace.
        """
        import time
        from pathlib import Path

        path = Path(script_path)
        if not path.is_file():
            raise FileNotFoundError(f"Script not found: {script_path}")

        self._ensure_connection()
        cursor = self._conn.cursor()

        self._print_header()
        print(f"\n{'#' * 100}")
        print(f"# CUSTOM: {path.name}")
        print(f"{'#' * 100}")

        ns = {
            "conn": self._conn,
            "cursor": cursor,
            "__name__": "__main__",
            "__file__": str(path),
        }

        try:
            # compile() is inside the guard so a SyntaxError in the user script
            # still closes the cursor via the finally below.
            code = compile(path.read_text(), str(path), "exec")
            self._ctx.enable(timeline=self._timeline)
            # Start the wall-clock only after enable(), so file read and compile
            # (which the profiling counters don't see) aren't charged to the script.
            t0 = time.perf_counter()
            exec(code, ns)  # noqa: S102
            wall_ms = (time.perf_counter() - t0) * 1000
            cpp, py = self._ctx.collect()
            if self._timeline:
                cpp_tl, py_tl = self._ctx.collect_timeline()
                self._ctx.disable_timeline()
        finally:
            # Always end the window and close the cursor, even if compile()/exec()
            # raised, so profiling state and the cursor never leak into a later run.
            self._ctx.disable()
            cursor.close()

        result = {
            "title": f"CUSTOM: {path.name}",
            "wall_ms": wall_ms,
            "cpp": cpp,
            "py": py,
        }

        if self._timeline:
            result["cpp_timeline"] = cpp_tl
            result["py_timeline"] = py_tl

        print(f"\n  Wall clock: {wall_ms:.1f}ms")
        if self._timeline:
            print_timeline(result.get("cpp_timeline"), result.get("py_timeline"), result["title"])
        else:
            print_stats(cpp, py, result["title"])
        self._print_footer()
        return result

    def close(self):
        # Always turn profiling off so a programmatic caller doesn't leave the
        # process-wide counters enabled after using the Profiler.
        self._ctx.disable()
        if self._conn:
            self._conn.close()
            self._conn = None
            self._table = None

    def _print_header(self):
        print("=" * 100)
        print("mssql-python profiler")
        print("=" * 100)
        print(f"Platform: {platform.system()} {platform.release()} ({platform.machine()})")
        print(f"Python:   {platform.python_version()}")

    def _print_footer(self):
        print(f"\n{'=' * 100}")
        print("PROFILING COMPLETE")
        print("=" * 100)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
