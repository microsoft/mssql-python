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
import sys

from profiler.reporter import print_stats, print_timeline
from profiler.scenarios import SCENARIOS, setup_test_data


class _ProfilingContext:
    """Thin wrapper that enables/disables/collects from both C++ and Python profiling."""

    def __init__(self):
        from mssql_python import ddbc_bindings, perf_timer

        self._cpp = ddbc_bindings.profiling
        self._py = perf_timer
        self._timeline_mode = False

    def set_timeline(self, on: bool):
        self._timeline_mode = on

    def enable(self, timeline: bool = False):
        self._cpp.enable()
        self._py.enable()
        if timeline or self._timeline_mode:
            self._cpp.enable_timeline()
            self._py.enable_timeline()

    def collect(self) -> tuple[dict, dict]:
        cpp = self._cpp.get_stats()
        py = self._py.get_stats()
        if self._timeline_mode:
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
            # Drain any stats leaked from setup
            self._ctx.enable()
            self._ctx.collect()
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

            # Build args based on what the scenario function needs
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

        ns = {"conn": self._conn, "cursor": cursor}
        code = compile(path.read_text(), str(path), "exec")

        self._ctx.enable(timeline=self._timeline)
        t0 = time.perf_counter()
        exec(code, ns)  # noqa: S102
        wall_ms = (time.perf_counter() - t0) * 1000
        cpp, py = self._ctx.collect()

        result = {
            "title": f"CUSTOM: {path.name}",
            "wall_ms": wall_ms,
            "cpp": cpp,
            "py": py,
        }

        if self._timeline:
            cpp_tl, py_tl = self._ctx.collect_timeline()
            self._ctx.disable_timeline()
            result["cpp_timeline"] = cpp_tl
            result["py_timeline"] = py_tl

        cursor.close()

        print(f"\n  Wall clock: {wall_ms:.1f}ms")
        if self._timeline:
            print_timeline(result.get("cpp_timeline"), result.get("py_timeline"), result["title"])
        else:
            print_stats(cpp, py, result["title"])
        self._print_footer()
        return result

    def close(self):
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
