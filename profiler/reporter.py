# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.
"""Stats collection and reporting — merges Python (py::) and C++ timer data."""

from __future__ import annotations


def merge_stats(cpp_stats: dict | None, py_stats: dict | None) -> dict:
    merged = {}
    for name, s in (cpp_stats or {}).items():
        merged[name] = s
    for name, s in (py_stats or {}).items():
        merged[name] = s
    return merged


def merge_timeline(cpp_timeline: list | None, py_timeline: list | None) -> list[dict]:
    events = list(cpp_timeline or []) + list(py_timeline or [])
    events.sort(key=lambda e: e["start_us"])
    return events


def format_stats_table(stats: dict, title: str) -> str:
    if not stats:
        return f"\n{title}: No data collected"

    lines = [
        "",
        "=" * 100,
        title,
        "=" * 100,
        f"  {'Function':<55} {'Calls':>8} {'Total(ms)':>12} "
        f"{'Avg(us)':>12} {'Min(us)':>10} {'Max(us)':>10}",
        f"  {'-' * 107}",
    ]
    for name, s in sorted(stats.items(), key=lambda x: x[1]["total_us"], reverse=True):
        total_ms = s["total_us"] / 1000.0
        avg_us = s["total_us"] / s["calls"] if s["calls"] > 0 else 0
        min_us = 0 if s["min_us"] > 1e15 else s["min_us"]
        lines.append(
            f"  {name:<55} {s['calls']:>8} {total_ms:>12.3f} "
            f"{avg_us:>12.1f} {min_us:>10.1f} {s['max_us']:>10.1f}"
        )
    return "\n".join(lines)


def format_timeline(events: list[dict], title: str) -> str:
    if not events:
        return f"\n{title}: No timeline events"

    lines = [
        "",
        "=" * 110,
        f"TIMELINE: {title}",
        "=" * 110,
        f"  {'Start(ms)':>10}  {'Dur(ms)':>10}  {'End(ms)':>10}  {'Function'}",
        f"  {'-' * 104}",
    ]

    # Build a simple nesting stack based on time overlap
    stack: list[tuple[int, int]] = []  # (start_us, end_us) of active spans
    for ev in events:
        s = ev["start_us"]
        d = ev["duration_us"]
        e = s + d

        # Pop spans that don't fully contain this event
        while stack and stack[-1][1] < e:
            stack.pop()

        depth = len(stack)
        indent = "  " * depth
        start_ms = s / 1000.0
        dur_ms = d / 1000.0
        end_ms = e / 1000.0

        lines.append(f"  {start_ms:>10.3f}  {dur_ms:>10.3f}  {end_ms:>10.3f}  {indent}{ev['name']}")

        stack.append((s, e))

    return "\n".join(lines)


def print_stats(cpp_stats: dict | None, py_stats: dict | None, title: str):
    print(format_stats_table(merge_stats(cpp_stats, py_stats), title))


def print_timeline(cpp_timeline: list | None, py_timeline: list | None, title: str):
    print(format_timeline(merge_timeline(cpp_timeline, py_timeline), title))
