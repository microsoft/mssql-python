"""Validate bounded profiler data and render an advisory, per-platform comparison."""

import argparse
import html
import json
import math
from pathlib import Path
import re
import statistics

LEGS = ("Windows-SQL2022", "Windows-SQL2025", "macOS-SQL2022", "macOS-SQL2025", "Linux-SQL2022")
CASES = (
    "connect",
    "select",
    "insert",
    "executemany",
    "fetchall",
    "fetchone",
    "fetchmany",
    "commit_rollback",
    "arrow",
    "insertmanyvalues",
    "fetchmany_100",
    "fetchmany_10000",
    "prepared_qmark",
    "prepared_named",
    "legacy_insertmany",
    "setinputsizes",
    "join_aggregation",
    "large_fetch",
    "fetch_1_2m",
    "cte",
)
MAX_BYTES = 8 * 1024 * 1024
MARKER = "<!-- mssql-python-profiler-ci -->"
THRESHOLD = 0.20
MIN_DELTA_MS = 1.0


def number(value, maximum=1e12):
    if type(value) not in (float, int) or not 0 <= value <= maximum:
        raise ValueError("Invalid performance measurement")
    return value


def text(value, limit=160):
    if not isinstance(value, str) or not value or len(value) > limit:
        raise ValueError("Invalid performance label")
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError("Control character in performance label")
    return value


def validate(report, build_id=None, head=None, source=None, base=None):
    if not isinstance(report, dict) or report.get("schema_version") != 1:
        raise ValueError("Unsupported report schema")
    if report.get("leg") not in LEGS or report.get("status") not in ("complete", "incomplete"):
        raise ValueError("Invalid report status or leg")
    for key, expected in (
        ("build_id", build_id),
        ("head_commit", head),
        ("source_commit", source),
        ("base_commit", base),
    ):
        if expected is not None and report.get(key) != expected:
            raise ValueError(f"Report provenance mismatch: {key}")
    for key in ("head_commit", "source_commit", "base_commit"):
        if not re.fullmatch(r"[0-9a-f]{40}", report.get(key, "")):
            raise ValueError("Invalid commit identity")
    if not re.fullmatch(r"[0-9a-f]{64}", report.get("suite_hash", "")):
        raise ValueError("Invalid workload identity")
    samples = report.get("samples")
    if type(samples) is not int or not 3 <= samples <= 15:
        raise ValueError("Insufficient or excessive samples")
    if type(report.get("warmups")) is not int or not 1 <= report["warmups"] <= 3:
        raise ValueError("Invalid warmup count")
    pairs = report.get("pairs")
    if not isinstance(pairs, list) or len(pairs) > samples:
        raise ValueError("Invalid sample pairs")
    if report["status"] == "incomplete":
        return report
    if len(pairs) != samples:
        raise ValueError("Incomplete sample pairs")
    environment = None
    work = {}
    for pair in pairs:
        if not isinstance(pair, dict) or set(pair) != {"base", "candidate"}:
            raise ValueError("Invalid paired sample")
        for side in ("base", "candidate"):
            sample = pair[side]
            env = sample["environment"]
            if not isinstance(env, dict) or set(env) != {
                "os",
                "architecture",
                "python",
                "sql_version",
            }:
                raise ValueError("Invalid environment")
            for value in env.values():
                text(value)
            expected_os, sql = report["leg"].split("-")
            if env["os"] != {"macOS": "Darwin"}.get(expected_os, expected_os):
                raise ValueError("Artifact platform does not match its leg")
            if not env["sql_version"].startswith({"SQL2022": "16.", "SQL2025": "17."}[sql]):
                raise ValueError("Artifact SQL version does not match its leg")
            if environment is not None and environment != env:
                raise ValueError("Environment changed between measurements")
            environment = env
            if set(sample["scenarios"]) != set(CASES):
                raise ValueError("Scenario set incomplete or changed")
            for name, scenario in sample["scenarios"].items():
                number(scenario["wall_ms"])
                if scenario["wall_ms"] <= 0:
                    raise ValueError("Zero workload time")
                identity = text(scenario["work"])
                if name in work and work[name] != identity:
                    raise ValueError(f"Workload changed for {name}")
                work[name] = identity
                for layer in ("cpp", "py"):
                    stats = scenario[layer]
                    if (
                        not isinstance(stats, dict)
                        or len(stats) > 300
                        or (layer == "cpp" and not stats)
                    ):
                        raise ValueError("Missing or oversized profiling data")
                    for label, counter in stats.items():
                        text(label)
                        if not label.startswith("ddbc::" if layer == "cpp" else "py::"):
                            raise ValueError("Invalid phase prefix")
                        calls = counter["calls"]
                        if type(calls) is not int or not 1 <= calls <= 100_000_000:
                            raise ValueError("Invalid call count")
                        for field in ("total_us", "min_us", "max_us"):
                            number(counter[field])
                        if not counter["min_us"] <= counter["max_us"] <= counter["total_us"]:
                            raise ValueError("Inconsistent phase totals")
    return report


def comparisons(report):
    """Do not add inclusive phase totals together or treat them as wall-clock time."""
    output = []
    for name in CASES:
        base = [pair["base"]["scenarios"][name] for pair in report["pairs"]]
        candidate = [pair["candidate"]["scenarios"][name] for pair in report["pairs"]]
        ratios = [new["wall_ms"] / old["wall_ms"] for old, new in zip(base, candidate)]
        old = statistics.median(s["wall_ms"] for s in base)
        new = statistics.median(s["wall_ms"] for s in candidate)
        ratio = statistics.median(ratios)
        # Requiring 80% of paired samples to agree avoids flagging one noisy pass.
        agrees = sum(r > 1 + THRESHOLD for r in ratios) >= math.ceil(len(ratios) * 0.8)
        status = (
            "regression"
            if ratio > 1 + THRESHOLD and new - old >= MIN_DELTA_MS and agrees
            else ("noisy" if ratio > 1 + THRESHOLD and new - old >= MIN_DELTA_MS else "ok")
        )
        phases = []
        changed_counts = []
        for layer in ("cpp", "py"):
            labels = set().union(*(s[layer] for s in base + candidate))
            for label in labels:
                before = [s[layer].get(label) for s in base]
                after = [s[layer].get(label) for s in candidate]
                if not all(before) or not all(after):
                    changed_counts.append(f"{label} (added, removed, or intermittent)")
                    continue
                before_calls = statistics.median(s["calls"] for s in before)
                after_calls = statistics.median(s["calls"] for s in after)
                if before_calls != after_calls:
                    changed_counts.append(f"{label} ({before_calls:g} -> {after_calls:g} calls)")
                delta = (
                    statistics.median(s["total_us"] for s in after)
                    - statistics.median(s["total_us"] for s in before)
                ) / 1000
                if delta > 0:
                    phases.append((delta, label))
        output.append(
            dict(
                name=name,
                base_ms=old,
                candidate_ms=new,
                change_pct=(ratio - 1) * 100,
                status=status,
                phases=sorted(phases, reverse=True)[:3],
                counts=sorted(changed_counts)[:3],
            )
        )
    return output


def escape(value):
    value = html.escape(value, quote=True)
    for char in "\\|`[]()*_~@":
        value = value.replace(char, f"&#{ord(char)};")
    return value


def render(reports, head, build_id, issues=()):
    url = f"https://dev.azure.com/sqlclientdrivers/public/_build/results?buildId={build_id}"
    lines = [
        MARKER,
        "## Profiler performance report",
        f"Head `{head}` | [ADO build {build_id}]({url})",
        "",
        "Advisory base vs PR-merge comparison. Both revisions are profiling-enabled, "
        "measured on the same agent/database with alternating order and discarded warmups.",
        "Flags require >20% paired median slowdown, >=1 ms added time, and 80% of pairs agreeing. "
        "These are signals to investigate, not production-wheel latency guarantees.",
        "",
    ]
    by_leg = {r["leg"]: r for r in reports}
    for leg in LEGS:
        report = by_leg.get(leg)
        if report is None or report["status"] != "complete":
            lines.append(f"**{leg}: incomplete/unavailable. No regression verdict.**")
            continue
        rows = comparisons(report)
        env = report["pairs"][0]["base"]["environment"]
        flags = [r for r in rows if r["status"] == "regression"]
        noisy = sum(r["status"] == "noisy" for r in rows)
        lines += [
            "",
            f"### {leg}",
            f"Base `{report['base_commit'][:12]}` -> merge `{report['source_commit'][:12]}`; "
            f"Python {escape(env['python'])}, {escape(env['architecture'])}, "
            f"SQL {escape(env['sql_version'])}; {report['samples']} pairs.",
            f"**{len(flags)} regression signals, {noisy} noisy comparisons.**",
            "<details><summary>All scenarios and phase diagnostics</summary>",
            "",
            "| Scenario | Base ms | PR ms | Paired change | Result |",
            "|---|---:|---:|---:|---|",
        ]
        for row in rows:
            lines.append(
                f"| {row['name']} | {row['base_ms']:.3f} | {row['candidate_ms']:.3f} | "
                f"{row['change_pct']:+.1f}% | {row['status']} |"
            )
        for row in rows:
            if row["status"] != "ok" or row["counts"]:
                detail = "; ".join(
                    f"{escape(label)} +{delta:.3f} ms" for delta, label in row["phases"]
                )
                counts = "; ".join(escape(label) for label in row["counts"])
                lines.append(
                    f"\n**{row['name']}**: {detail or 'no positive phase delta'}."
                    + (f" Call changes: {counts}." if counts else "")
                )
        lines += [
            "",
            "Phase times are inclusive diagnostics, not additive wall-clock components.",
            "</details>",
        ]
    if issues:
        lines += [
            "",
            "Some artifacts were missing or rejected: " + ", ".join(escape(x) for x in issues),
        ]
    lines += [
        "",
        "Raw samples and build logs are attached to the ADO run as `profiler-*` artifacts.",
    ]
    body = "\n".join(lines)
    if len(body) > 60000:
        raise ValueError("Performance comment exceeds its size budget")
    return body


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reports", nargs="+", type=Path)
    args = parser.parse_args()
    reports = [validate(json.loads(path.read_text(encoding="utf-8"))) for path in args.reports]
    print(render(reports, reports[0]["head_commit"], reports[0]["build_id"]))


if __name__ == "__main__":
    main()
