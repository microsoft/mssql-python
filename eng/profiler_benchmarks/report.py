"""Validate bounded profiler data and render an advisory, per-platform comparison."""

import argparse
from dataclasses import dataclass
import html
import io
import json
import math
from pathlib import Path, PurePosixPath
import re
import stat
import statistics
import zipfile
import zlib

# Hosted macOS plus Colima produced false regressions on a documentation-only
# control PR. Routine reports use stable Ubuntu measurements as the Unix signal.
LEGS = ("Linux-SQL2022", "Linux-SQL2025")
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
    "lob_varchar_256k_fetchall": "256 KiB VARCHAR(MAX) / fetchall()",
    "scalar_fetchval": "10,000 scalar values / fetchval() (debug disabled)",
}
CASES = tuple(TASK_NAMES)
MODES = ("diagnostic", "latency", "route")
FETCH_CASES = tuple(
    f"{shape}_{method}"
    for shape in ("numeric", "mixed")
    for method in ("fetchone", "fetchmany", "fetchval")
)
TASK_NAMES.update(
    {
        name: f"1,000 {name.split('_')[0]} rows / "
        + ("fetchmany(1)" if name.endswith("fetchmany") else name.split("_")[1] + "()")
        for name in FETCH_CASES
    }
)


def measurement_mode(report):
    version = report.get("schema_version")
    if version == 1 and "mode" not in report:
        return "diagnostic"
    if version == 2 and report.get("mode") in ("latency", "route"):
        return report["mode"]
    raise ValueError("Unsupported measurement schema or mode")


def cases_for(report):
    return CASES if measurement_mode(report) == "diagnostic" else FETCH_CASES


MAX_BYTES = 8 * 1024 * 1024
MAX_COMMENT_CHARS = 60000
MAX_DIAGNOSTIC_ROWS = 20
MAX_FINGERPRINT_TASKS = 4
MARKER = "<!-- mssql-python-profiler-ci -->"
THRESHOLD = 0.20
MIN_DELTA_MS = 1.0


@dataclass(frozen=True)
class AssessmentEvidence:
    build: dict
    head: str
    base: str
    merge_commit: dict
    base_commit: dict


def artifact_report(raw):
    """Read exactly one bounded JSON member; never extract or execute artifact files."""
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        members = archive.infolist()
        if len(members) > 200 or sum(member.file_size for member in members) > 64 * 1024 * 1024:
            raise ValueError("Oversized artifact")
        reports = [
            member for member in members if PurePosixPath(member.filename).name == "report.json"
        ]
        if len(reports) != 1:
            raise ValueError("Expected exactly one report.json")
        member = reports[0]
        path = PurePosixPath(member.filename)
        if (
            path.is_absolute()
            or ".." in path.parts
            or "\\" in member.filename
            or stat.S_ISLNK(member.external_attr >> 16)
            or member.file_size > MAX_BYTES
        ):
            raise ValueError("Invalid report member")
        if member.flag_bits & 1:
            raise ValueError("Encrypted performance artifacts are unsupported")
        return json.loads(archive.read(member).decode("utf-8"))


def unavailable(reason):
    return (
        f"{MARKER}\n## PR Performance Report\n\n"
        f"**Performance could not be assessed.**\n\n{reason} No result is available."
    )


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
    try:
        return _validate(report, build_id, head, source, base)
    except KeyError as error:
        raise ValueError(f"Missing performance report field: {error.args[0]}") from error


def _validate(report, build_id=None, head=None, source=None, base=None):
    if not isinstance(report, dict):
        raise ValueError("Unsupported report schema")
    mode = measurement_mode(report)
    if report.get("leg") not in LEGS or report.get("status") not in ("complete", "incomplete"):
        raise ValueError("Invalid report status or leg")
    if type(report.get("build_id")) is not int or report["build_id"] < 0:
        raise ValueError("Invalid build_id")
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
    samples = report.get("samples")
    if type(samples) is not int or not 3 <= samples <= 15:
        raise ValueError("Insufficient or excessive samples")
    if type(report.get("warmups")) is not int or not 1 <= report["warmups"] <= 3:
        raise ValueError("Invalid warmup count")
    pairs = report.get("pairs")
    if not isinstance(pairs, list) or len(pairs) > samples:
        raise ValueError("Invalid sample pairs")
    validate_python_sources(report)
    if report["status"] == "incomplete":
        if "python_sources" in report:
            text(report.get("unavailable_reason"), limit=240)
        return validate_samples(report)
    if len(pairs) != samples:
        raise ValueError("Incomplete sample pairs")
    return validate_samples(report)


def validate_samples(report, selected_cases=None):
    mode = measurement_mode(report)
    pairs = report["pairs"]
    cases = cases_for(report) if selected_cases is None else selected_cases
    if not cases or set(cases) - set(cases_for(report)):
        raise ValueError("Invalid selected scenarios")
    environment = None
    work = {}
    provenance = {}
    for pair in pairs:
        if not isinstance(pair, dict) or set(pair) != {"base", "candidate"}:
            raise ValueError("Invalid paired sample")
        for side in ("base", "candidate"):
            sample = pair[side]
            if not isinstance(sample, dict):
                raise ValueError("Invalid sample")
            if mode != "diagnostic" or "python_sources" in report or "provenance" in sample:
                if sample.get("mode") != mode or sample.get("status") != "complete":
                    raise ValueError("Incomplete or mismatched measurement mode")
                native_identity = validate_native_identity(sample, report, side, mode)
                if side in provenance and provenance[side] != native_identity:
                    raise ValueError("Native identity changed between samples")
                provenance[side] = native_identity
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
            if env["os"] != expected_os:
                raise ValueError("Artifact platform does not match its leg")
            if not env["sql_version"].startswith({"SQL2022": "16.", "SQL2025": "17."}[sql]):
                raise ValueError("Artifact SQL version does not match its leg")
            if environment is not None and environment != env:
                raise ValueError("Environment changed between measurements")
            environment = env
            scenarios = sample["scenarios"]
            if not isinstance(scenarios, dict):
                raise ValueError("Invalid scenarios object")
            if set(scenarios) != set(cases):
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
                if mode != "diagnostic":
                    if scenario["py"] or (mode == "latency" and scenario["cpp"]):
                        raise ValueError("Recording contaminated the measurement mode")
                    shape, method = name.split("_")
                    if scenario["work"] != f"Rows: 1000; shape: {shape}; API: {method}; EOF: 1":
                        raise ValueError("Single-row workload identity mismatch")
                    if mode == "route":
                        timer = (
                            "ddbc::FetchMany_wrap"
                            if method == "fetchmany"
                            else "ddbc::FetchOne_wrap"
                        )
                        stats = scenario["cpp"]
                        if not isinstance(stats, dict) or not isinstance(stats.get(timer), dict):
                            raise ValueError("Missing native fetch route evidence")
                        if stats[timer].get("calls") != 1001:
                            raise ValueError("Native fetch route count mismatch")
                        constructor = stats.get("ddbc::FetchRow::construct_row", {})
                        if not isinstance(constructor, dict) or constructor.get("calls", 0) != (
                            expected_constructors(native_identity, method)
                        ):
                            raise ValueError("Native constructor route count mismatch")
                for layer in ("cpp", "py"):
                    stats = scenario[layer]
                    if (
                        not isinstance(stats, dict)
                        or len(stats) > 300
                        or (layer == "cpp" and mode != "latency" and not stats)
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


def validate_row_route(route, guarded=None):
    if (
        type(route) is not dict
        or set(route) != {"version", "methods"}
        or type(route["version"]) is not int
        or route["version"] != 1
    ):
        raise ValueError("Unsupported Python row route version or shape")
    methods = route["methods"]
    if (
        type(methods) is not dict
        or set(methods) != {"fetchone", "fetchmany", "fetchval"}
        or any(type(value) is not bool for value in methods.values())
    ):
        raise ValueError("Invalid Python row route methods")
    values = tuple(methods[name] for name in ("fetchone", "fetchmany", "fetchval"))
    if values not in ((False, False, False), (True, True, True), (False, True, False)):
        raise ValueError("Unsupported Python row route policy")
    if guarded is not None and (type(guarded) is not bool or (any(values) and not guarded)):
        raise ValueError("Native binding contradicts Python row route")
    return methods


def expected_constructors(identity, method):
    if "row_route" not in identity:
        return 1000 if identity["guarded_row"] else 0
    return 1000 if validate_row_route(identity["row_route"], identity["guarded_row"])[method] else 0


def route_description(report, method):
    descriptions = []
    for side in ("base", "candidate"):
        identity = report["pairs"][0][side]["provenance"]
        descriptions.append(
            f"{side}: binding={identity['guarded_row']}, default {method} native Row="
            f"{bool(expected_constructors(identity, method))}"
        )
    return "; ".join(descriptions) + "."


def validate_python_sources(report):
    if "python_sources" not in report:
        return
    sources = report["python_sources"]
    if type(sources) is not dict or set(sources) != {"base", "candidate"}:
        raise ValueError("Missing Python source anchors")
    for side, source in sources.items():
        if type(source) is not dict or set(source) != {
            "source_commit",
            "python_cursor_sha256",
            "row_route",
        }:
            raise ValueError("Invalid Python source anchor")
        if source["source_commit"] != report["base_commit" if side == "base" else "source_commit"]:
            raise ValueError("Python source revision mismatch")
        if not isinstance(source["python_cursor_sha256"], str) or not re.fullmatch(
            r"[0-9a-f]{64}", source["python_cursor_sha256"]
        ):
            raise ValueError("Invalid Python cursor digest")
        validate_row_route(source["row_route"])


def validate_native_identity(sample, report, side, mode):
    identity = sample.get("provenance")
    legacy = {"source_commit", "native_file", "native_sha256", "native_profiling", "guarded_row"}
    modern = legacy | {"python_cursor_sha256", "row_route"}
    if not isinstance(identity, dict) or set(identity) not in (legacy, modern):
        raise ValueError("Missing native measurement identity")
    if (set(identity) == modern) != ("python_sources" in report):
        raise ValueError("Missing Python source anchors or worker route fields")
    if set(identity) == modern:
        validate_python_sources(report)
        validate_row_route(identity["row_route"], identity["guarded_row"])
        anchor = {
            key: identity[key] for key in ("source_commit", "python_cursor_sha256", "row_route")
        }
        if anchor != report["python_sources"][side]:
            raise ValueError("Python source-policy drift")
    if identity["source_commit"] != report["base_commit" if side == "base" else "source_commit"]:
        raise ValueError("Worker revision mismatch")
    text(identity["native_file"], limit=4096)
    if not isinstance(identity["native_sha256"], str) or not re.fullmatch(
        r"[0-9a-f]{64}", identity["native_sha256"]
    ):
        raise ValueError("Invalid native binary digest")
    if (
        identity["native_profiling"] is not (mode != "latency")
        or type(identity["guarded_row"]) is not bool
    ):
        raise ValueError("Invalid native measurement configuration")
    return identity


def validate_ci_mode(report, mode):
    if measurement_mode(report) != mode:
        raise ValueError("CI measurement mode mismatch")
    validate(report)
    if report["status"] == "incomplete":
        text(report["unavailable_reason"], limit=240)
        validate_samples(report)
    identities = {}
    for pair in report["pairs"]:
        for side, sample in pair.items():
            if sample.get("status") != "complete" or sample.get("mode") != mode:
                raise ValueError("Incomplete or mismatched worker mode")
            identity = validate_native_identity(sample, report, side, mode)
            if side in identities and identities[side] != identity:
                raise ValueError("Native identity changed between samples")
            identities[side] = identity
    return report


def has_ci_bundle(report):
    return isinstance(report, dict) and (
        "measurement_bundle_version" in report or "fetch_measurements" in report
    )


def validate_ci_header(report, build_id=None, head=None, source=None, base=None):
    if (
        type(report.get("measurement_bundle_version")) is not int
        or report["measurement_bundle_version"] != 1
    ):
        raise ValueError("Unsupported CI measurement bundle version")
    if measurement_mode(report) != "diagnostic":
        raise ValueError("CI bundle root must be the diagnostic report")
    header = dict(report, status="incomplete", pairs=[])
    header.pop("python_sources", None)
    validate(header, build_id, head, source, base)
    if report["samples"] != 5 or report["warmups"] != 1:
        raise ValueError("CI bundle requires five pairs and one warmup")
    children = report.get("fetch_measurements")
    if not isinstance(children, dict) or set(children) - {"latency", "route"}:
        raise ValueError("Invalid CI measurement children")
    return report


def ci_mode_reports(report):
    validate_ci_header(report)
    valid, errors = {}, {}
    for mode in ("latency", "route", "diagnostic"):
        item = report if mode == "diagnostic" else report["fetch_measurements"].get(mode)
        try:
            if not isinstance(item, dict):
                raise ValueError("Missing mode")
            for key in (
                "build_id",
                "head_commit",
                "source_commit",
                "base_commit",
                "leg",
                "samples",
                "warmups",
            ):
                if item.get(key) != report[key]:
                    raise ValueError("Mode provenance mismatch: " + key)
            validate_ci_mode(item, mode)
            valid[mode] = item
            if item["status"] != "complete":
                errors[mode] = item["unavailable_reason"]
        except (KeyError, TypeError, ValueError) as error:
            errors[mode] = "Invalid mode data: " + str(error)[:180]
    on = [valid.get(mode) for mode in ("route", "diagnostic")]
    if all(item is not None and item["pairs"] for item in on):
        if any(
            on[0]["pairs"][0][side][key] != on[1]["pairs"][0][side][key]
            for side in ("base", "candidate")
            for key in ("provenance", "environment")
        ):
            for mode in ("route", "diagnostic"):
                valid.pop(mode)
                errors[mode] = "Shared ON binary/environment identity mismatch"
    anchored = [(mode, item) for mode, item in valid.items() if "python_sources" in item]
    if anchored:
        reference = next((item for mode, item in anchored if mode == "latency"), anchored[0][1])
        for mode, item in list(valid.items()):
            if item.get("python_sources") != reference["python_sources"] and (
                item["pairs"] or "python_sources" in item
            ):
                valid.pop(mode)
                errors[mode] = "Python source identity differs across modes"
    latency = valid.get("latency")
    if latency is not None and latency["pairs"]:
        for mode in ("route", "diagnostic"):
            item = valid.get(mode)
            if (
                item is not None
                and item["pairs"]
                and item["pairs"][0]["base"]["environment"]
                != latency["pairs"][0]["base"]["environment"]
            ):
                valid.pop(mode)
                errors[mode] = "Environment differs from the latency comparison"
    return valid, errors


def render_ci_reports(reports, head, build_id, issues=()):
    diagnostics = []
    diagnostic_issues = list(issues)
    seen = set()
    for report in reports:
        leg = report["leg"]
        if leg in seen:
            raise ValueError("Duplicate performance report leg")
        seen.add(leg)
        if has_ci_bundle(report):
            valid, errors = ci_mode_reports(report)
        else:
            validate(report)
            valid = {measurement_mode(report): report}
            errors = {}
        diagnostic = valid.get("diagnostic")
        if diagnostic is not None and diagnostic["status"] == "complete":
            diagnostics.append(diagnostic)
        else:
            reason = errors.get("diagnostic", "missing or incomplete diagnostic measurement")
            diagnostic_issues.append(leg + " (" + reason + ")")
    body = render(diagnostics, head, build_id, diagnostic_issues)
    artifact_note = "Raw samples and logs are attached to the ADO run as `profiler-*` artifacts."
    body = body.replace(
        artifact_note,
        "This headline uses the original 22-task profiling-enabled diagnostics; separate "
        "OFF/OFF latency and ON/OFF route measurements, when available, are retained in the raw artifacts "
        "and are not headline inputs.\n\n" + artifact_note,
        1,
    )
    if len(body) > MAX_COMMENT_CHARS:
        raise ValueError("CI performance comment exceeds its bounded size")
    return body


def assess(evidence, artifact_urls, load_artifact, issues=()):
    issues = list(issues)
    try:
        if (
            not isinstance(evidence.build, dict)
            or not isinstance(evidence.merge_commit, dict)
            or not isinstance(evidence.base_commit, dict)
        ):
            raise ValueError
        build_id = evidence.build.get("id")
        source = evidence.build.get("sourceVersion")
        if (
            type(build_id) is not int
            or build_id <= 0
            or not re.fullmatch(r"[0-9a-f]{40}", source or "")
            or not re.fullmatch(r"[0-9a-f]{40}", evidence.head)
            or not re.fullmatch(r"[0-9a-f]{40}", evidence.base)
            or evidence.merge_commit.get("sha") != source
            or evidence.base_commit.get("sha") != evidence.base
            or [parent["sha"] for parent in evidence.merge_commit["parents"]]
            != [evidence.base, evidence.head]
        ):
            raise ValueError
    except (KeyError, TypeError, ValueError):
        return unavailable("Build provenance validation failed.")

    # Match coverage's trust boundary: select the exact PR-head build and treat
    # its bounded artifacts as data without requiring an identical producer tree.
    reports = []
    ci_requested = False
    for leg, url in artifact_urls.items():
        try:
            report = artifact_report(load_artifact(url))
            ci_requested = ci_requested or has_ci_bundle(report)
            validator = validate_ci_header if has_ci_bundle(report) else validate
            validator(report, build_id, evidence.head, source, evidence.base)
            if report["leg"] != leg:
                raise ValueError("Artifact leg mismatch")
            reports.append(report)
        except (
            KeyError,
            RecursionError,
            TypeError,
            ValueError,
            zipfile.BadZipFile,
            zlib.error,
        ):
            issues.append(leg + " (invalid artifact)")

    try:
        renderer = render_ci_reports if ci_requested else render
        return renderer(reports, evidence.head, build_id, issues)
    except ValueError:
        return unavailable("Performance report rendering failed.")


def comparisons(report):
    """Do not add inclusive phase totals together or treat them as wall-clock time."""
    output = []
    for name in cases_for(report):
        base = [pair["base"]["scenarios"][name] for pair in report["pairs"]]
        candidate = [pair["candidate"]["scenarios"][name] for pair in report["pairs"]]
        ratios = [new["wall_ms"] / old["wall_ms"] for old, new in zip(base, candidate)]
        old = statistics.median(s["wall_ms"] for s in base)
        new = statistics.median(s["wall_ms"] for s in candidate)
        ratio = statistics.median(ratios)
        # Requiring 80% of paired samples to agree avoids flagging one noisy pass.
        agrees = sum(r > 1 + THRESHOLD for r in ratios) >= math.ceil(len(ratios) * 0.8)
        improves = sum(r < 1 - THRESHOLD for r in ratios) >= math.ceil(len(ratios) * 0.8)
        status = (
            "regression"
            if ratio > 1 + THRESHOLD and new - old >= MIN_DELTA_MS and agrees
            else (
                "improvement"
                if ratio < 1 - THRESHOLD and old - new >= MIN_DELTA_MS and improves
                else ("noisy" if ratio > 1 + THRESHOLD and new - old >= MIN_DELTA_MS else "ok")
            )
        )
        phase_deltas = []
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
                if delta:
                    phase_deltas.append((delta, label))
        phases = (
            sorted((item for item in phase_deltas if item[0] < 0))[:3]
            if status == "improvement"
            else sorted((item for item in phase_deltas if item[0] > 0), reverse=True)[:3]
        )
        output.append(
            dict(
                name=name,
                base_ms=old,
                candidate_ms=new,
                change_pct=(ratio - 1) * 100,
                ratio=ratio,
                ratio_min=min(ratios),
                ratio_max=max(ratios),
                status=status,
                phases=phases,
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
    operating_system = {"Linux": "Unix"}.get(operating_system, operating_system)
    return f"{operating_system} / SQL Server {sql.removeprefix('SQL')}"


def issue_reason(leg, issues):
    prefix = leg + " ("
    for issue in issues:
        if issue.startswith(prefix) and issue.endswith(")"):
            return issue[len(prefix) : -1]
    global_issues = [issue for issue in issues if not any(issue.startswith(x + " (") for x in LEGS)]
    return global_issues[0] if global_issues else "incomplete benchmark"


def render(reports, head, build_id, issues=(), default_mode="diagnostic"):
    url = f"https://dev.azure.com/sqlclientdrivers/public/_build/results?buildId={build_id}"
    modes = {measurement_mode(r) for r in reports}
    if len(modes) > 1:
        raise ValueError("Cannot combine different measurement modes")
    mode = next(iter(modes), default_mode)
    by_leg = {r["leg"]: r for r in reports}
    if len(by_leg) != len(reports):
        raise ValueError("Duplicate performance report leg")
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
    improvements = [
        (leg, row)
        for leg, (_, rows) in completed.items()
        for row in rows
        if row["status"] == "improvement"
    ]
    noisy = [
        (leg, row)
        for leg, (_, rows) in completed.items()
        for row in rows
        if row["status"] == "noisy"
    ]
    missing = len(LEGS) - len(completed)

    highlighted = [
        (leg, row) for leg, (_, rows) in completed.items() for row in rows if row["status"] != "ok"
    ]
    if regressions:
        tasks = len({row["name"] for _, row in regressions})
        environments = len({leg for leg, _ in regressions})
        opening = (
            f"{tasks} database task{'s' if tasks != 1 else ''} consistently slowed down across "
            f"{environments} measured environment{'s' if environments != 1 else ''}."
        )
        verdict = "⚠️ Performance regression detected"
    elif noisy:
        tasks = len({row["name"] for _, row in noisy})
        environments = len({leg for leg, _ in noisy})
        opening = (
            f"{tasks} database task{'s' if tasks != 1 else ''} produced inconsistent slowdown "
            f"signals across {environments} measured environment"
            f"{'s' if environments != 1 else ''}."
        )
        verdict = "🔍 Performance needs review"
    elif improvements:
        tasks = len({row["name"] for _, row in improvements})
        environments = len({leg for leg, _ in improvements})
        opening = (
            f"{tasks} database task{'s' if tasks != 1 else ''} consistently improved across "
            f"{environments} measured environment{'s' if environments != 1 else ''}. "
            "No consistent slowdowns were detected."
        )
        verdict = "✅ Performance improved"
    elif not completed:
        opening = (
            "Performance could not be assessed because no environment produced a complete result."
        )
        verdict = "⛔ Performance unavailable"
    elif not missing:
        opening = f"No consistent slowdowns detected across all {len(LEGS)} environments."
        verdict = "✅ No regression detected"
    else:
        completed_label = "environment" if len(completed) == 1 else "environments"
        missing_label = "environment" if missing == 1 else "environments"
        opening = (
            f"No consistent slowdowns in the {len(completed)} completed {completed_label}. "
            f"No result is available for {missing} {missing_label}."
        )
        verdict = "✅ No regression detected"

    if mode == "route" and completed:
        verdict = "Native route attribution (instrumented)"
        opening = (
            "Instrumented paired timings below describe the verified native route, not production latency. "
            + opening
        )
    improvement_tasks = len({row["name"] for _, row in improvements})
    regression_tasks = len({row["name"] for _, row in regressions})
    lines = [
        MARKER,
        "## PR Performance Report",
        "",
        f"### {verdict}",
        "",
        f"**{opening}**",
        "",
        f"<kbd>{improvement_tasks} IMPROVEMENT"
        f"{'S' if improvement_tasks != 1 else ''}</kbd> "
        f"<kbd>{regression_tasks} SLOWDOWN"
        f"{'S' if regression_tasks != 1 else ''}</kbd> "
        f"<kbd>{len(completed)}/{len(LEGS)} ENVIRONMENTS</kbd>",
        "",
    ]
    if mode != "diagnostic":
        lines += [
            "**Measurement:** "
            + (
                "native instrumentation OFF / Python phases OFF; controlled fetch-loop latency."
                if mode == "latency"
                else "native instrumentation ON / Python phases OFF; route attribution, not production latency."
            ),
            "",
        ]
    if noisy:
        noisy_tasks = len({row["name"] for _, row in noisy})
        lines += [
            f"<kbd>{noisy_tasks} INCONSISTENT SLOWDOWN" f"{'S' if noisy_tasks != 1 else ''}</kbd>",
            "",
        ]
    affected_tasks = [
        name for name in TASK_NAMES if any(row["name"] == name for _, row in highlighted)
    ]
    if highlighted and len(affected_tasks) <= MAX_FINGERPRINT_TASKS:
        affected_legs = [leg for leg in LEGS if any(item_leg == leg for item_leg, _ in highlighted)]
        by_signal = {(leg, row["name"]): row for leg, row in highlighted}
        lines += [
            "### Signal fingerprint",
            "",
            "| Database task | "
            + " | ".join(environment_name(leg) for leg in affected_legs)
            + " |",
            "|---|" + "|".join("---:" for _ in affected_legs) + "|",
        ]
        for name in affected_tasks:
            cells = []
            for leg in affected_legs:
                row = by_signal.get((leg, name))
                if row is None:
                    cells.append("No signal")
                elif row["status"] == "improvement":
                    cells.append(f"**{abs(row['change_pct']):.1f}% faster**")
                elif row["status"] == "regression":
                    cells.append(f"**{abs(row['change_pct']):.1f}% slower**")
                else:
                    cells.append(f"**{abs(row['change_pct']):.1f}% inconsistent**")
            lines.append(f"| {escape(TASK_NAMES[name])} | " + " | ".join(cells) + " |")
        lines.append("")
    if regressions:
        lines.append(
            "Paired fetch-loop timings are shown below. Phase attribution is unavailable in latency mode."
            if mode == "latency"
            else "The largest recorded phase increases for these tasks are shown below. "
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
    ]
    unavailable_legs = [
        f"{environment_name(leg)} ({escape(issue_reason(leg, issues))})"
        for leg in LEGS
        if leg not in completed
    ]
    if unavailable_legs:
        lines += ["", "Unavailable: " + "; ".join(unavailable_legs) + "."]

    if highlighted:
        lines += [
            "",
            "<details>",
            "<summary><b>Measured timings</b></summary>",
            "",
            "| Environment | Database task | Before | After | Change |",
            "|---|---|---:|---:|---:|",
        ]
        for leg, row in highlighted:
            lines.append(
                f"| {environment_name(leg)} | {TASK_NAMES[row['name']]} | "
                f"{row['base_ms']:.3f} ms | {row['candidate_ms']:.3f} ms | "
                f"**{row['change_pct']:+.1f}%** |"
            )
        lines += ["", "</details>"]

    diagnostics_start = len(lines)
    lines += [
        "",
        "<details>",
        "<summary><b>Performance diagnostics</b></summary>",
        "",
        "Phase times are inclusive diagnostics and must not be added together. "
        "They identify where measured time changed, not why it changed.",
    ]
    diagnostics = 0
    total_diagnostics = 0
    for leg, (_, rows) in completed.items():
        relevant = [row for row in rows if row["status"] != "ok" or row["counts"]]
        total_diagnostics += len(relevant)
        visible = relevant[: max(0, MAX_DIAGNOSTIC_ROWS - diagnostics)]
        if not visible:
            continue
        lines += ["", f"### {environment_name(leg)}"]
        for row in visible:
            diagnostics += 1
            phases = "; ".join(f"{escape(label)} {delta:+.3f} ms" for delta, label in row["phases"])
            counts = "; ".join(escape(label) for label in row["counts"])
            detail = phases or (
                "phase attribution unavailable (recording OFF)"
                if mode == "latency"
                else "no measured phase delta"
            )
            if counts:
                detail += f". Call changes: {counts}"
            lines.append(f"**{TASK_NAMES[row['name']]}:** {detail}.")
    if not diagnostics:
        lines += ["", "No affected phases or call-count changes were recorded."]
    elif total_diagnostics > diagnostics:
        lines += [
            "",
            f"{total_diagnostics - diagnostics} additional diagnostic rows are available "
            "in the raw ADO artifacts.",
        ]
    lines += [
        "",
        "</details>",
    ]
    diagnostics_end = len(lines)
    lines += [
        "",
        "<details>",
        "<summary><b>All database tasks and timings</b></summary>",
    ]

    for leg, (report, rows) in completed.items():
        lines += [
            "",
            f"### {environment_name(leg)}",
            (
                "| Database task | Before | After | Paired change | Result |"
                if mode == "diagnostic"
                else "| Database task | Before | After | Paired change | Ratio median [min, max] | Result |"
            ),
            "|---|---:|---:|---:|---|" if mode == "diagnostic" else "|---|---:|---:|---:|---:|---|",
        ]
        for row in rows:
            result = {
                "regression": "consistent slowdown",
                "improvement": "consistent improvement",
                "noisy": "inconsistent slowdown",
                "ok": "no signal",
            }[row["status"]]
            lines.append(
                f"| {TASK_NAMES[row['name']]} | {row['base_ms']:.3f} ms | "
                f"{row['candidate_ms']:.3f} ms | {row['change_pct']:+.1f}% | "
                + (
                    f"{row['ratio']:.3f} [{row['ratio_min']:.3f}, {row['ratio_max']:.3f}] | "
                    if mode != "diagnostic"
                    else ""
                )
                + f"{result} |"
            )
    lines += [
        "",
        "</details>",
        "",
        "<details>",
        "<summary><b>Build and measurement details</b></summary>",
        "",
    ]
    lines += [
        (
            f"[ADO build {build_id}]({url})"
            if build_id or mode == "diagnostic"
            else "Local paired comparison (no ADO build)"
        ),
        "",
        f"PR head: `{head}`",
    ]
    if completed:
        first = next(iter(completed.values()))[0]
        lines += [
            f"Base: `{first['base_commit']}`",
            f"Measured {'merge' if mode == 'diagnostic' else 'source'}: `{first['source_commit']}`",
            "",
        ]
        for leg, (report, _) in completed.items():
            env = report["pairs"][0]["base"]["environment"]
            lines.append(
                f"- {environment_name(leg)}: Python {escape(env['python'])}, "
                f"{escape(env['architecture'])}, SQL {escape(env['sql_version'])}; "
                f"{report['samples']} paired comparisons and {report['warmups']} warmup."
            )
            if mode != "diagnostic":
                for side in ("base", "candidate"):
                    identity = report["pairs"][0][side]["provenance"]
                    lines.append(
                        f"- {environment_name(leg)} {side} native SHA256: `{identity['native_sha256']}`; "
                        f"guarded Row entry available: {identity['guarded_row']}. "
                        + (
                            "Default native Row routes: "
                            + ", ".join(
                                f"{method}={enabled}"
                                for method, enabled in identity["row_route"]["methods"].items()
                            )
                            if "row_route" in identity
                            else "Historical all-or-none route contract."
                        )
                    )
    lines += [
        "",
        "A consistent change requires more than 20% median paired movement, at least "
        "1 ms between the median runtimes, and at least 80% of pairs exceeding the "
        "relative threshold in the same direction. A slowdown without enough pair "
        "agreement is reported as inconsistent.",
        "",
        "The displayed change is the median of paired before-and-after ratios. It is not "
        "recalculated from the two displayed median runtimes.",
    ]
    if issues:
        lines += ["", "Unavailable or rejected data: " + ", ".join(escape(x) for x in issues)]
    lines += [
        "",
        (
            "Both revisions use native-instrumentation-OFF builds with Python phases OFF on "
            "the same agent and database, alternating order and discarded warmups. This is "
            "controlled fetch-loop latency, not a customer-production or pyodbc comparison."
            if mode == "latency"
            else "Both revisions use profiling-enabled builds on the same agent and database, "
            "with alternating order and discarded warmups. Results are diagnostic and do "
            "not represent production-wheel latency."
        ),
        "",
        "Raw samples and logs are attached to the ADO run as `profiler-*` artifacts.",
        "",
        "</details>",
    ]
    body = "\n".join(lines)
    if len(body) > MAX_COMMENT_CHARS:
        lines[diagnostics_start:diagnostics_end] = [
            "",
            "<details>",
            "<summary><b>Performance diagnostics</b></summary>",
            "",
            f"{total_diagnostics} diagnostic rows are available in the raw ADO artifacts.",
            "",
            "</details>",
        ]
        body = "\n".join(lines)
    if len(body) > MAX_COMMENT_CHARS:
        raise ValueError("Performance comment exceeds its size budget")
    return body


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reports", nargs="+", type=Path)
    args = parser.parse_args()
    reports = [json.loads(path.read_text(encoding="utf-8")) for path in args.reports]
    for report in reports:
        (validate_ci_header if has_ci_bundle(report) else validate)(report)
    first = reports[0]
    for report in reports[1:]:
        (validate_ci_header if has_ci_bundle(report) else validate)(
            report,
            build_id=first["build_id"],
            head=first["head_commit"],
            source=first["source_commit"],
            base=first["base_commit"],
        )
    renderer = render_ci_reports if any(has_ci_bundle(report) for report in reports) else render
    print(renderer(reports, first["head_commit"], first["build_id"]))


if __name__ == "__main__":
    main()
