"""Validate bounded profiler data and render an advisory, per-platform comparison."""

import argparse
import hashlib
import html
import json
import math
from pathlib import Path
import re
import statistics

LEGS = ("Windows-SQL2022", "Windows-SQL2025", "macOS-SQL2022", "macOS-SQL2025", "Linux-SQL2022")
TASK_NAMES = {
    "connect": "Connection opening",
    "select": "SELECT queries",
    "insert": "Row insertion",
    "executemany": "Executemany inserts",
    "fetchall": "Fetch-all queries",
    "fetchone": "Row-by-row fetching",
    "fetchmany": "Batched row fetching",
    "commit_rollback": "Transaction commit and rollback",
    "arrow": "Arrow row fetching",
    "insertmanyvalues": "100,000-row insertion",
    "fetchmany_100": "Row fetching in batches of 100",
    "fetchmany_10000": "Row fetching in batches of 10,000",
    "prepared_qmark": "Repeated positional queries",
    "prepared_named": "Repeated named-parameter queries",
    "legacy_insertmany": "Legacy 100,000-row insertion",
    "setinputsizes": "Insertion with explicit input sizes",
    "join_aggregation": "Joined aggregation queries",
    "large_fetch": "Large joined-result fetching",
    "fetch_1_2m": "1.2-million-row fetching",
    "cte": "Common table expression queries",
}
CASES = tuple(TASK_NAMES)
MAX_BYTES = 8 * 1024 * 1024
MARKER = "<!-- mssql-python-profiler-ci -->"
THRESHOLD = 0.20
MIN_DELTA_MS = 1.0


def suite_paths(root):
    root = Path(root)
    return [
        root / "eng/pipelines/pr-validation-pipeline.yml",
        root / "eng/profiler_benchmarks/__init__.py",
        root / "eng/profiler_benchmarks/controller.py",
        root / "eng/profiler_benchmarks/report.py",
        root / "eng/profiler_benchmarks/workloads.py",
        *sorted((root / "profiler").glob("*.py")),
    ]


def suite_hash(root):
    digest = hashlib.sha256()
    for file in suite_paths(root):
        digest.update(file.name.encode())
        digest.update(file.read_bytes().replace(b"\r\n", b"\n"))
    return digest.hexdigest()


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


def validate(report, build_id=None, head=None, source=None, base=None, suite=None):
    if not isinstance(report, dict) or report.get("schema_version") != 1:
        raise ValueError("Unsupported report schema")
    if report.get("leg") not in LEGS or report.get("status") not in ("complete", "incomplete"):
        raise ValueError("Invalid report status or leg")
    if type(report.get("build_id")) is not int or report["build_id"] < 0:
        raise ValueError("Invalid build_id")
    for key, expected in (
        ("build_id", build_id),
        ("head_commit", head),
        ("source_commit", source),
        ("base_commit", base),
        ("suite_hash", suite),
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
            if not isinstance(sample, dict):
                raise ValueError("Invalid sample")
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
            scenarios = sample["scenarios"]
            if not isinstance(scenarios, dict):
                raise ValueError("Invalid scenarios object")
            if set(scenarios) != set(CASES):
                raise ValueError("Scenario set incomplete or changed")
            for name, scenario in scenarios.items():
                if not isinstance(scenario, dict):
                    raise ValueError("Invalid scenario")
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
                        if not isinstance(counter, dict):
                            raise ValueError("Invalid phase counter")
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


def environment_name(leg):
    operating_system, sql = leg.split("-")
    return f"{operating_system} / SQL Server {sql.removeprefix('SQL')}"


def issue_reason(leg, issues):
    prefix = leg + " ("
    for issue in issues:
        if issue.startswith(prefix) and issue.endswith(")"):
            return issue[len(prefix) : -1]
    global_issues = [issue for issue in issues if not any(issue.startswith(x + " (") for x in LEGS)]
    return global_issues[0] if global_issues else "incomplete benchmark"


def render(reports, head, build_id, issues=()):
    url = f"https://dev.azure.com/sqlclientdrivers/public/_build/results?buildId={build_id}"
    by_leg = {r["leg"]: r for r in reports}
    completed = {
        leg: (report, comparisons(report))
        for leg in LEGS
        if (report := by_leg.get(leg)) is not None and report["status"] == "complete"
    }
    regressions = [
        (leg, row)
        for leg, (_, rows) in completed.items()
        for row in rows
        if row["status"] == "regression"
    ]
    noisy = [
        (leg, row)
        for leg, (_, rows) in completed.items()
        for row in rows
        if row["status"] == "noisy"
    ]
    missing = len(LEGS) - len(completed)

    if len(regressions) == 1:
        leg, row = regressions[0]
        opening = (
            f"This PR consistently slows {TASK_NAMES[row['name']].lower()} on "
            f"{environment_name(leg)} by {row['change_pct']:.1f}%."
        )
    elif regressions:
        tasks = len({row["name"] for _, row in regressions})
        environments = len({leg for leg, _ in regressions})
        opening = (
            f"This PR has {len(regressions)} consistent slowdown signals across "
            f"{tasks} database tasks and {environments} environments."
        )
    elif noisy:
        if len(noisy) == 1:
            leg, row = noisy[0]
            opening = (
                f"{TASK_NAMES[row['name']]} was slower on {environment_name(leg)}, "
                "but the repeated comparisons were inconsistent."
            )
        else:
            tasks = len({row["name"] for _, row in noisy})
            environments = len({leg for leg, _ in noisy})
            opening = (
                f"No consistent slowdowns detected. {len(noisy)} inconsistent comparisons "
                f"need review across {tasks} database tasks and {environments} environments."
            )
    elif not completed:
        opening = (
            "Performance could not be assessed because no environment produced a complete result."
        )
    elif not missing:
        opening = f"No consistent slowdowns detected across all {len(LEGS)} environments."
    else:
        completed_label = "environment" if len(completed) == 1 else "environments"
        missing_label = "environment" if missing == 1 else "environments"
        opening = (
            f"No consistent slowdowns in the {len(completed)} completed {completed_label}. "
            f"No result is available for {missing} {missing_label}."
        )

    lines = [MARKER, "## PR Performance Report", "", f"**{opening}**", ""]
    highlighted = regressions or noisy
    if highlighted:
        if not regressions:
            lines += ["Inconsistent slowdowns to review:", ""]
        lines += [
            "| Environment | Affected task | Before | After | Change |",
            "|---|---|---:|---:|---:|",
        ]
        for leg, row in highlighted:
            lines.append(
                f"| {environment_name(leg)} | {TASK_NAMES[row['name']]} | "
                f"{row['base_ms']:.3f} ms | {row['candidate_ms']:.3f} ms | "
                f"{row['change_pct']:+.1f}% |"
            )
        lines.append("")
    if regressions:
        lines.append(
            "The largest recorded phase increases for these tasks are shown below. "
            "Phase timings are supporting evidence, not root-cause proof."
        )
        if noisy:
            lines.append(
                f"{len(noisy)} additional inconsistent slowdown"
                f"{'s' if len(noisy) != 1 else ''} also need review."
            )
        lines.append("")

    lines += [
        f"**Coverage:** {len(completed)} of {len(LEGS)} environments completed. "
        "Advisory result; does not block merging.",
        "",
        "| Environment | Status |",
        "|---|---|",
    ]
    for leg in LEGS:
        report = by_leg.get(leg)
        status = (
            "Completed"
            if leg in completed
            else f"No result available ({escape(issue_reason(leg, issues))})"
        )
        lines.append(f"| {environment_name(leg)} | {status} |")

    lines += [
        "",
        "<details>",
        "<summary>Affected phases and call counts</summary>",
        "",
        "Phase times are inclusive diagnostics and must not be added together. "
        "They identify where measured time changed, not why it changed.",
    ]
    diagnostics = 0
    for leg, (_, rows) in completed.items():
        relevant = [row for row in rows if row["status"] != "ok" or row["counts"]]
        if not relevant:
            continue
        lines += ["", f"### {environment_name(leg)}"]
        for row in relevant:
            diagnostics += 1
            phases = "; ".join(f"{escape(label)} +{delta:.3f} ms" for delta, label in row["phases"])
            counts = "; ".join(escape(label) for label in row["counts"])
            detail = phases or "no positive phase delta"
            if counts:
                detail += f". Call changes: {counts}"
            lines.append(f"**{TASK_NAMES[row['name']]}:** {detail}.")
    if not diagnostics:
        lines += ["", "No affected phases or call-count changes were recorded."]
    lines += [
        "",
        "</details>",
        "",
        "<details>",
        "<summary>All database tasks and timings</summary>",
    ]

    for leg, (report, rows) in completed.items():
        lines += [
            "",
            f"### {environment_name(leg)}",
            "| Database task | Before | After | Paired change | Result |",
            "|---|---:|---:|---:|---|",
        ]
        for row in rows:
            result = {
                "regression": "consistent slowdown",
                "noisy": "inconsistent slowdown",
                "ok": "no signal",
            }[row["status"]]
            lines.append(
                f"| {TASK_NAMES[row['name']]} | {row['base_ms']:.3f} ms | "
                f"{row['candidate_ms']:.3f} ms | {row['change_pct']:+.1f}% | {result} |"
            )
    lines += [
        "",
        "</details>",
        "",
        "<details>",
        "<summary>Build, commits and measurement details</summary>",
        "",
    ]
    lines += [
        f"[ADO build {build_id}]({url})",
        "",
        f"PR head: `{head}`",
    ]
    if completed:
        first = next(iter(completed.values()))[0]
        lines += [
            f"Base: `{first['base_commit']}`",
            f"Measured merge: `{first['source_commit']}`",
            "",
        ]
        for leg, (report, _) in completed.items():
            env = report["pairs"][0]["base"]["environment"]
            lines.append(
                f"- {environment_name(leg)}: Python {escape(env['python'])}, "
                f"{escape(env['architecture'])}, SQL {escape(env['sql_version'])}; "
                f"{report['samples']} paired comparisons and {report['warmups']} warmup."
            )
    lines += [
        "",
        "A consistent slowdown requires more than 20% median paired slowdown, at least "
        "1 ms between the median runtimes, and at least 80% of pairs exceeding the "
        "relative threshold. An inconsistent slowdown crosses the first two thresholds "
        "without enough pair agreement.",
        "",
        "The displayed change is the median of paired before-and-after ratios. It is not "
        "recalculated from the two displayed median runtimes.",
    ]
    if issues:
        lines += ["", "Unavailable or rejected data: " + ", ".join(escape(x) for x in issues)]
    lines += [
        "",
        "Both revisions use profiling-enabled builds on the same agent and database, "
        "with alternating order and discarded warmups. Results are diagnostic and do "
        "not represent production-wheel latency.",
        "",
        "Raw samples and logs are attached to the ADO run as `profiler-*` artifacts.",
        "",
        "</details>",
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
    first = reports[0]
    for report in reports[1:]:
        validate(
            report,
            build_id=first["build_id"],
            head=first["head_commit"],
            source=first["source_commit"],
            base=first["base_commit"],
            suite=first["suite_hash"],
        )
    print(render(reports, first["head_commit"], first["build_id"]))


if __name__ == "__main__":
    main()
