"""Contract tests for paired performance comparisons and data-only PR reporting."""

import copy
from http.client import IncompleteRead
import importlib.util
import io
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tarfile
import time
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock
from urllib.error import URLError
import zipfile
import zlib

import pytest

ROOT = Path(__file__).resolve().parents[1]
if not (ROOT / ".github/scripts/post_profiler_comment.py").is_file():
    pytest.skip("CI reporting tools are not installed in driver wheels", allow_module_level=True)

from eng.profiler_benchmarks import controller
from eng.profiler_benchmarks import report as reporting
from eng.profiler_benchmarks import workloads as benchmark_workloads


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, ROOT / path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


publisher = load("post_profiler_comment", ".github/scripts/post_profiler_comment.py")
extractor = load("extract_coverage_artifact", ".github/scripts/extract_coverage_artifact.py")


def ado_build(**values):
    build = dict(
        id=42,
        status="completed",
        result="failed",
        definition={"id": 2128},
        repository={"id": "microsoft/mssql-python"},
        sourceBranch="refs/pull/123/merge",
        sourceVersion="b" * 40,
        triggerInfo={"pr.number": "123", "pr.sourceSha": "c" * 40},
    )
    build.update(values)
    return build


def pr_topology(head="c" * 40, base="a" * 40, merge_base=None, state="open", merged=False):
    def response(path):
        if path.startswith("pulls/"):
            return {
                "state": state,
                "merged": merged,
                "head": {"sha": head},
                "base": {"sha": base},
            }
        if path.startswith("git/commits/"):
            commit_sha = path.removeprefix("git/commits/")
            source = commit_sha == "b" * 40
            return {
                "sha": commit_sha,
                "parents": [{"sha": merge_base or base}, {"sha": head}] if source else [],
                "tree": {"sha": ("d" if source else "e") * 40},
            }
        raise AssertionError(f"Unexpected GitHub path: {path}")

    return response


@pytest.fixture
def report():
    def sample(scale):
        counter = dict(calls=1, total_us=1000, min_us=1000, max_us=1000)
        return dict(
            environment=dict(
                os="Linux", architecture="x86_64", python="3.13.7", sql_version="16.0"
            ),
            scenarios={
                name: dict(
                    wall_ms=10 * scale, work="Rows: 100", cpp={"ddbc::query": counter}, py={}
                )
                for name in reporting.CASES
            },
        )

    return dict(
        schema_version=1,
        status="complete",
        leg="Linux-SQL2022",
        base_commit="a" * 40,
        source_commit="b" * 40,
        head_commit="c" * 40,
        build_id=42,
        samples=5,
        warmups=1,
        pairs=[dict(base=sample(1), candidate=sample(1.3)) for _ in range(5)],
    )


def test_consistent_slowdown_is_advisory_regression(report):
    reporting.validate(report, 42, "c" * 40, "b" * 40, "a" * 40)
    rows = reporting.comparisons(report)
    assert all(
        row["status"] == "regression" and row["change_pct"] == pytest.approx(30) for row in rows
    )
    body = reporting.render([report], "c" * 40, 42)
    assert "### ⚠️ Performance regression detected" in body
    assert f"{len(reporting.CASES)} database tasks consistently slowed down" in body
    assert "| Unix / SQL Server 2022 | Connection opening |" in body
    assert "Unavailable: Unix / SQL Server 2025 (incomplete benchmark)." in body
    assert body.index("consistently slowed down") < body.index(
        "<summary><b>Build and measurement details</b></summary>"
    )


def test_noisy_slowdown_and_submillisecond_change_are_not_regressions(report):
    for pair in report["pairs"][:2]:
        pair["candidate"]["scenarios"]["select"]["wall_ms"] = 8
    report["pairs"][0]["candidate"]["scenarios"]["connect"]["wall_ms"] = 1000
    assert reporting.comparisons(report)[1]["status"] == "noisy"
    for pair in report["pairs"]:
        pair["base"]["scenarios"]["insert"]["wall_ms"] = 0.1
        pair["candidate"]["scenarios"]["insert"]["wall_ms"] = 0.2
    assert reporting.comparisons(report)[2]["status"] == "ok"


def test_phase_call_changes_are_reported_without_summing_nested_totals(report):
    for pair in report["pairs"]:
        pair["candidate"]["scenarios"]["select"]["cpp"] = {
            "ddbc::query": dict(calls=2, total_us=5000, min_us=2000, max_us=3000),
        }
    row = reporting.comparisons(report)[1]
    assert row["counts"] == ["ddbc::query (1 -> 2 calls)"]
    assert row["phases"] == [(4.0, "ddbc::query")]


@pytest.mark.parametrize("case", ["nan", "missing", "environment", "work", "few", "prefix", "zero"])
def test_reject_invalid_or_incomparable_data(report, case):
    sample = report["pairs"][0]["candidate"]
    if case == "nan":
        sample["scenarios"]["select"]["wall_ms"] = float("nan")
    elif case == "missing":
        del sample["scenarios"]["select"]
    elif case == "environment":
        sample["environment"]["sql_version"] = "other"
    elif case == "work":
        sample["scenarios"]["select"]["work"] = "Rows: 200"
    elif case == "few":
        report["pairs"].pop()
    elif case == "zero":
        sample["scenarios"]["select"]["wall_ms"] = 0
    elif case == "prefix":
        sample["scenarios"]["select"]["cpp"] = {
            "not-native": sample["scenarios"]["select"]["cpp"]["ddbc::query"]
        }
    with pytest.raises(ValueError):
        reporting.validate(report)


@pytest.mark.parametrize("field", ["environment", "scenarios", "cpp", "calls"])
def test_missing_report_fields_are_normalized_to_value_error(report, field):
    sample = report["pairs"][0]["base"]
    if field in ("environment", "scenarios"):
        del sample[field]
    elif field == "cpp":
        del sample["scenarios"]["select"][field]
    else:
        del sample["scenarios"]["select"]["cpp"]["ddbc::query"][field]
    with pytest.raises(ValueError, match="Missing performance report field"):
        reporting.validate(report)


def test_reject_wrong_commit_and_preserve_incomplete_status(report):
    with pytest.raises(ValueError, match="provenance"):
        reporting.validate(report, head="e" * 40)
    report["status"] = "incomplete"
    report["pairs"] = []
    reporting.validate(report)
    assert "Performance could not be assessed" in reporting.render([report], "c" * 40, 42)


@pytest.mark.parametrize("build_id", [None, True, -1])
def test_reject_invalid_build_id(report, build_id):
    report["build_id"] = build_id
    with pytest.raises(ValueError, match="build_id"):
        reporting.validate(report)


def set_leg(report, leg):
    report = copy.deepcopy(report)
    report["leg"] = leg
    operating_system, sql = leg.split("-")
    for pair in report["pairs"]:
        for sample in pair.values():
            sample["environment"]["os"] = operating_system
            sample["environment"]["sql_version"] = "16.0" if sql == "SQL2022" else "17.0"
    return report


@pytest.mark.parametrize(
    "key,value",
    [
        ("build_id", 43),
        ("head_commit", "e" * 40),
        ("source_commit", "e" * 40),
        ("base_commit", "e" * 40),
    ],
)
def test_standalone_report_rejects_mixed_provenance(report, tmp_path, monkeypatch, key, value):
    first = tmp_path / "linux.json"
    second = tmp_path / "linux-2025.json"
    first.write_text(json.dumps(report), encoding="utf-8")
    other = set_leg(report, "Linux-SQL2025")
    other[key] = value
    second.write_text(json.dumps(other), encoding="utf-8")
    monkeypatch.setattr(sys, "argv", ["report", str(first), str(second)])
    with pytest.raises(ValueError, match=key):
        reporting.main()


def test_render_rejects_duplicate_legs(report):
    with pytest.raises(ValueError, match="Duplicate"):
        reporting.render([report, copy.deepcopy(report)], "c" * 40, 42)


def test_render_bounds_schema_valid_diagnostics(report):
    reports = [set_leg(copy.deepcopy(report), leg) for leg in reporting.LEGS]
    labels = ["ddbc::" + str(index) + "_" * 152 for index in range(3)]
    for item in reports:
        for pair in item["pairs"]:
            for name in reporting.CASES:
                for side, calls in (("base", 1), ("candidate", 2)):
                    pair[side]["scenarios"][name]["cpp"] = {
                        label: dict(calls=calls, total_us=2000, min_us=1000, max_us=1000)
                        for label in labels
                    }
        reporting.validate(item)
    body = reporting.render(reports, "c" * 40, 42)
    assert len(body) <= 60000
    extra = len(reporting.CASES) * len(reporting.LEGS) - reporting.MAX_DIAGNOSTIC_ROWS
    total = len(reporting.CASES) * len(reporting.LEGS)
    assert (
        f"{extra} additional diagnostic rows are available in the raw ADO artifacts" in body
        or f"{total} diagnostic rows are available in the raw ADO artifacts" in body
    )
    assert "<summary><b>All database tasks and timings</b></summary>" in body
    assert "<summary><b>Build and measurement details</b></summary>" in body


@pytest.mark.parametrize("invalid", ["source commit", "base commit"])
def test_assessment_binds_all_evidence_to_authenticated_commits(invalid):
    evidence = reporting.AssessmentEvidence(
        build=ado_build(),
        head="c" * 40,
        base="a" * 40,
        merge_commit={
            "sha": "b" * 40,
            "parents": [{"sha": "a" * 40}, {"sha": "c" * 40}],
            "tree": {"sha": "d" * 40},
        },
        base_commit={"sha": "a" * 40, "tree": {"sha": "e" * 40}},
    )
    if invalid == "source commit":
        evidence.merge_commit["sha"] = "f" * 40
    elif invalid == "base commit":
        evidence.base_commit["sha"] = "f" * 40
    body = reporting.assess(evidence, {}, lambda url: pytest.fail("must not download"))
    assert "Performance could not be assessed" in body
    assert "Build provenance validation failed" in body


def clear_slowdowns(report):
    for pair in report["pairs"]:
        for name in reporting.CASES:
            pair["candidate"]["scenarios"][name]["wall_ms"] = pair["base"]["scenarios"][name][
                "wall_ms"
            ]
    return report


def test_impact_summary_handles_single_inconsistent_and_complete_clean_results(report):
    clean = clear_slowdowns(copy.deepcopy(report))
    for pair, scale in zip(clean["pairs"], (1.3, 1.3, 1.3, 0.8, 0.8)):
        pair["candidate"]["scenarios"]["fetchone"]["wall_ms"] *= scale
    noisy = reporting.render([clean], "c" * 40, 42)
    assert "### 🔍 Performance needs review" in noisy
    assert "1 database task produced inconsistent slowdown signals" in noisy
    assert "<kbd>1 INCONSISTENT SLOWDOWN</kbd>" in noisy

    complete = [set_leg(clear_slowdowns(copy.deepcopy(report)), leg) for leg in reporting.LEGS]
    clean_body = reporting.render(complete, "c" * 40, 42)
    assert "### ✅ No regression detected" in clean_body
    assert "**No consistent slowdowns detected across all 2 environments.**" in clean_body
    assert "**Coverage:** 2 of 2 environments completed." in clean_body


def test_impact_summary_handles_single_regression_partial_and_no_results(report):
    single = clear_slowdowns(copy.deepcopy(report))
    for pair in single["pairs"]:
        pair["candidate"]["scenarios"]["fetchone"]["wall_ms"] *= 1.3
    body = reporting.render([single], "c" * 40, 42)
    assert "### ⚠️ Performance regression detected" in body
    assert "**1 database task consistently slowed down across 1 measured environment.**" in body
    assert "<summary><b>Performance diagnostics</b></summary>" in body
    assert "<summary><b>All database tasks and timings</b></summary>" in body
    assert "<summary><b>Build and measurement details</b></summary>" in body
    assert "median of paired before-and-after ratios" in body

    partial = reporting.render(
        [clear_slowdowns(copy.deepcopy(report))],
        "c" * 40,
        42,
        ["Linux-SQL2025 (missing)"],
    )
    assert "No consistent slowdowns in the 1 completed environment." in partial
    assert "No result is available for 1 environment." in partial
    assert "Unavailable: Unix / SQL Server 2025 (missing)." in partial
    assert "pending" not in partial.lower()

    unavailable = reporting.render([], "c" * 40, 42, ["Linux-SQL2022 (invalid artifact)"])
    assert "Performance could not be assessed" in unavailable
    assert "No consistent slowdowns" not in unavailable


def test_impact_summary_reports_consistent_improvements(report):
    reports = [set_leg(clear_slowdowns(copy.deepcopy(report)), leg) for leg in reporting.LEGS]
    for item, scales in zip(reports, ((0.7, 0.6), (0.72, 0.61))):
        for pair in item["pairs"]:
            pair["candidate"]["scenarios"]["fetchall"]["wall_ms"] *= scales[0]
            pair["candidate"]["scenarios"]["setinputsizes"]["wall_ms"] *= scales[1]
            pair["candidate"]["scenarios"]["fetchall"]["cpp"]["ddbc::query"] = dict(
                calls=1, total_us=500, min_us=500, max_us=500
            )
    rows = reporting.comparisons(reports[0])
    assert rows[4]["status"] == "improvement"
    assert rows[4]["phases"] == [(-0.5, "ddbc::query")]
    body = reporting.render(reports, "c" * 40, 42)
    assert "### ✅ Performance improved" in body
    assert (
        "**2 database tasks consistently improved across 2 measured environments. "
        "No consistent slowdowns were detected.**"
    ) in body
    assert "<kbd>2 IMPROVEMENTS</kbd> <kbd>0 SLOWDOWNS</kbd> <kbd>2/2 ENVIRONMENTS</kbd>" in body
    assert "| Fetch-all queries | **30.0% faster** | **28.0% faster** |" in body
    assert (
        "| Insertion with explicit input sizes | **40.0% faster** | " "**39.0% faster** |"
    ) in body
    assert "Spread" not in body
    assert "<summary><b>Measured timings</b></summary>" in body
    assert "| Fetch-all queries |" in body and "| consistent improvement |" in body
    assert "ddbc::query -0.500 ms" in body


def test_regression_headline_keeps_precedence_over_improvement(report):
    mixed = clear_slowdowns(copy.deepcopy(report))
    for pair in mixed["pairs"]:
        pair["candidate"]["scenarios"]["fetchall"]["wall_ms"] *= 0.7
        pair["candidate"]["scenarios"]["fetchone"]["wall_ms"] *= 1.3
    body = reporting.render([mixed], "c" * 40, 42)
    assert "### ⚠️ Performance regression detected" in body
    assert "<kbd>1 IMPROVEMENT</kbd> <kbd>1 SLOWDOWN</kbd>" in body


def test_inconsistent_slowdown_keeps_precedence_over_improvement(report):
    mixed = clear_slowdowns(copy.deepcopy(report))
    for pair in mixed["pairs"]:
        pair["candidate"]["scenarios"]["fetchall"]["wall_ms"] *= 0.7
    for pair, scale in zip(mixed["pairs"], (1.3, 1.3, 1.3, 0.8, 0.8)):
        pair["candidate"]["scenarios"]["fetchone"]["wall_ms"] *= scale
    body = reporting.render([mixed], "c" * 40, 42)
    assert "### 🔍 Performance needs review" in body
    assert "<kbd>1 IMPROVEMENT</kbd> <kbd>0 SLOWDOWNS</kbd>" in body
    assert "<kbd>1 INCONSISTENT SLOWDOWN</kbd>" in body


@pytest.mark.parametrize(
    "path",
    [
        ("pairs", 0, "candidate"),
        ("pairs", 0, "candidate", "environment"),
        ("pairs", 0, "candidate", "scenarios"),
        ("pairs", 0, "candidate", "scenarios", "select"),
        ("pairs", 0, "candidate", "scenarios", "select", "cpp"),
        ("pairs", 0, "candidate", "scenarios", "select", "cpp", "ddbc::query"),
    ],
)
@pytest.mark.parametrize("as_list", [False, True])
def test_reject_non_object_sample_containers(report, path, as_list):
    parent = report
    for key in path[:-1]:
        parent = parent[key]
    key = path[-1]
    parent[key] = list(parent[key]) if as_list else None
    with pytest.raises(ValueError):
        reporting.validate(report)


def zip_data(entries):
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as archive:
        for name, data in entries:
            archive.writestr(name, data)
    return out.getvalue()


@pytest.mark.parametrize(
    "kind,member,data",
    [
        ("html", "Code Coverage Report_1/index.html", b"<html>coverage</html>"),
        ("xml", "unified-coverage/coverage.xml", b"<coverage />"),
    ],
)
def test_coverage_artifact_reader_copies_only_expected_report(tmp_path, kind, member, data):
    archive = tmp_path / "coverage.zip"
    archive.write_bytes(
        zip_data(
            [
                (".github/actions/post-coverage-comment/action.yml", "malicious"),
                ("../outside.txt", "escape"),
                (member, data),
            ]
        )
    )
    output = tmp_path / f"report.{kind}"
    extractor.copy_report(archive, output, kind)
    assert output.read_bytes() == data
    assert not (tmp_path.parent / "outside.txt").exists()
    assert not (tmp_path / ".github").exists()


@pytest.mark.parametrize("second,valid", [("<coverage />", True), ("<different />", False)])
def test_coverage_artifact_reader_accepts_only_identical_duplicate_reports(tmp_path, second, valid):
    archive = tmp_path / "coverage.zip"
    output = tmp_path / "coverage.xml"
    archive.write_bytes(
        zip_data(
            [
                ("first/coverage.xml", "<coverage />"),
                ("second/coverage.xml", second),
            ]
        )
    )
    if valid:
        extractor.copy_report(archive, output, "xml")
        assert output.read_text() == "<coverage />"
    else:
        with pytest.raises(ValueError, match="Conflicting"):
            extractor.copy_report(archive, output, "xml")


def test_coverage_artifact_reader_rejects_oversized_or_unrelated_archives(tmp_path):
    archive = tmp_path / "coverage.zip"
    archive.write_bytes(zip_data([("test-results.xml", "<tests />")]))
    with pytest.raises(ValueError, match="No coverage xml"):
        extractor.copy_report(archive, tmp_path / "coverage.xml", "xml")
    with archive.open("wb") as stream:
        stream.seek(extractor.MAX_ARCHIVE_BYTES)
        stream.write(b"x")
    with pytest.raises(ValueError, match="archive exceeds"):
        extractor.copy_report(archive, tmp_path / "coverage.xml", "xml")
    archive.write_bytes(zip_data([("coverage.xml/", b"")]))
    with pytest.raises(ValueError, match="No coverage xml"):
        extractor.copy_report(archive, tmp_path / "coverage.xml", "xml")
    directory = zipfile.ZipInfo("coverage.xml")
    directory.create_system = 3
    directory.external_attr = 0o40755 << 16
    archive.write_bytes(zip_data([(directory, b"")]))
    with pytest.raises(ValueError, match="No coverage xml"):
        extractor.copy_report(archive, tmp_path / "coverage.xml", "xml")


def test_artifact_read_never_extracts_paths(report):
    raw = json.dumps(report)
    assert (
        reporting.artifact_report(zip_data([("profiler-Linux-SQL2022/report.json", raw)])) == report
    )
    for entries in [
        [("../report.json", raw)],
        [("/report.json", raw)],
        [("a/report.json", raw), ("b/report.json", raw)],
        [("logs.txt", "no report")],
    ]:
        with pytest.raises(ValueError):
            reporting.artifact_report(zip_data(entries))


def test_untrusted_labels_cannot_inject_links_mentions_or_markdown():
    assert reporting.escape("[click](https://example.com) @everyone | `code`") == (
        "&#91;click&#93;&#40;https://example.com&#41; &#64;everyone &#124; &#96;code&#96;"
    )
    assert not publisher.allowed_url("https://example.com/artifact")
    assert not publisher.allowed_url("http://dev.azure.com/artifact")
    assert not publisher.allowed_url("https://dev.azure.com@evil.example/artifact")
    assert publisher.allowed_url("https://dev.azure.com/sqlclientdrivers/public/")
    assert publisher.allowed_url(
        "https://artprodcus3.artifacts.visualstudio.com/A1/_apis/artifact/"
    )
    assert not publisher.allowed_url("https://artifacts.visualstudio.com.evil.example/artifact")


@pytest.mark.parametrize("error", [IncompleteRead(b"partial"), ConnectionResetError("reset")])
def test_incomplete_http_response_is_normalized_for_terminal_fallback(monkeypatch, error):
    opener = MagicMock()
    opener.open.return_value.__enter__.return_value.read.side_effect = error
    monkeypatch.setattr(publisher, "build_opener", lambda *args: opener)
    with pytest.raises(URLError, match="Incomplete HTTP response"):
        publisher.fetch("https://api.github.com/repos/microsoft/mssql-python")


def test_build_selection_requires_exact_pr_head():
    build = ado_build()
    assert publisher.find_build([build], 123, "c" * 40) is build
    for key in ("pr.number", "pr.sourceSha"):
        bad = copy.deepcopy(build)
        bad["triggerInfo"][key] = "different"
        assert publisher.find_build([bad], 123, "c" * 40) is None


def test_publisher_does_not_post_stale_head(monkeypatch):
    calls = []

    def api(path, **kwargs):
        calls.append((path, kwargs))
        if path.startswith("pulls/"):
            return {"state": "open", "head": {"sha": "new-head"}}
        return []

    monkeypatch.setattr(publisher, "github", api)
    publisher.publish(123, "old-head", "anything")
    assert all(not kwargs for _, kwargs in calls)
    assert not any(path == "issues/123/comments" and kwargs for path, kwargs in calls)


def test_publisher_can_finalize_exact_head_after_merge(monkeypatch):
    calls = []

    def api(path, **kwargs):
        calls.append((path, kwargs))
        if path.startswith("pulls/"):
            return {
                "state": "closed",
                "merged": True,
                "head": {"sha": "head"},
                "base": {"sha": "base"},
            }
        if path.startswith("issues/") and "comments" in path:
            return [
                {
                    "id": 42,
                    "user": {"login": "github-actions[bot]"},
                    "body": publisher.pending_message("head"),
                }
            ]
        return {}

    monkeypatch.setattr(publisher, "github", api)
    publisher.publish(123, "head", "final", "base")
    assert ("issues/comments/42", {"method": "PATCH", "data": {"body": "final"}}) in calls


def test_pending_rerun_preserves_completed_report_for_same_head(monkeypatch):
    calls = []
    completed = reporting.MARKER + "\nfinal\n\nPR head: `head`"

    def api(path, **kwargs):
        calls.append((path, kwargs))
        if path.startswith("pulls/"):
            return {
                "state": "open",
                "head": {"sha": "head"},
                "base": {"sha": "base"},
            }
        if path.startswith("issues/") and "comments" in path:
            return [
                {
                    "id": 42,
                    "user": {"login": "github-actions[bot]"},
                    "body": completed,
                }
            ]
        return {}

    monkeypatch.setattr(publisher, "github", api)
    publisher.publish(1, "head", publisher.pending_message("head"), "base")
    assert not any(kwargs for path, kwargs in calls if path == "issues/comments/42")


def test_publisher_finalizes_pending_comment_when_pr_is_abandoned(monkeypatch):
    posted = []
    monkeypatch.setattr(
        publisher, "publish", lambda number, head, body, base=None: posted.append(body)
    )
    monkeypatch.setattr(publisher, "github", pr_topology(state="closed"))
    publisher.run(123, "c" * 40, 1)
    assert len(posted) == 2
    assert "Pull request closed before assessment completed" in posted[-1]


def test_publisher_preserves_completed_report_when_pr_is_abandoned(monkeypatch):
    calls = []

    def api(path, **kwargs):
        calls.append((path, kwargs))
        if path.startswith("pulls/"):
            return {
                "state": "closed",
                "merged": False,
                "head": {"sha": "head"},
                "base": {"sha": "base"},
            }
        if path.startswith("issues/") and "comments" in path:
            return [
                {
                    "id": 42,
                    "user": {"login": "github-actions[bot]"},
                    "body": "final report",
                }
            ]
        return {}

    monkeypatch.setattr(publisher, "github", api)
    publisher.publish(1, "head", "new report", "base")
    assert not any(kwargs for path, kwargs in calls if path == "issues/comments/42")


def test_publisher_retries_transient_comment_failures(monkeypatch):
    publish = MagicMock(side_effect=[URLError("temporary"), None])
    sleeps = []
    monkeypatch.setattr(publisher, "publish", publish)
    monkeypatch.setattr(publisher.time, "sleep", sleeps.append)
    publisher.publish_with_retry(123, "a" * 40, "body")
    assert publish.call_count == 2
    assert sleeps == [5]


def test_revisions_use_exact_first_parent(monkeypatch):
    calls = []

    def git(*args):
        calls.append(args)
        return "b" * 40 if len(calls) == 1 else "a" * 40

    monkeypatch.setattr(controller, "git", git)
    assert controller.resolve_revisions(None, "HEAD") == ("a" * 40, "b" * 40)
    assert calls[1][-1] == "b" * 40 + "^1^{commit}"


@pytest.mark.parametrize("name", ["safe.txt", "../outside.txt", "C:/outside.txt"])
def test_checkout_is_safe_and_compatible_with_python_310(tmp_path, monkeypatch, name):
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w") as tar:
        member = tarfile.TarInfo(name)
        member.size = 4
        tar.addfile(member, io.BytesIO(b"data"))
    archive.seek(0)

    class Archive:
        def __enter__(self):
            return archive

        def __exit__(self, *args):
            return None

    monkeypatch.setattr(controller.sys, "version_info", (3, 10))
    monkeypatch.setattr(controller.tempfile, "TemporaryFile", Archive)
    monkeypatch.setattr(controller.subprocess, "run", lambda *a, **kw: None)
    if name == "safe.txt":
        controller.checkout("a" * 40, tmp_path)
        assert (tmp_path / name).read_bytes() == b"data"
    else:
        with pytest.raises(ValueError, match="Unsafe"):
            controller.checkout("a" * 40, tmp_path)
        assert not (tmp_path.parent / "outside.txt").exists()


def test_report_cases_match_the_executed_workload_registry():
    _, workloads = controller.load_suite()
    assert tuple(workloads.registry()) == reporting.CASES
    assert len(reporting.CASES) == 22
    assert workloads.registry()["scalar_fetchval"] == (workloads.scalar_fetchval, True)
    assert [name for name in reporting.CASES if name.startswith("lob_")] == [
        "lob_varchar_256k_fetchall",
    ]


def test_scalar_fetchval_workload_times_only_fetch_and_reaches_eof(monkeypatch, report):
    monkeypatch.setattr("mssql_python.logging.logger", SimpleNamespace(is_debug_enabled=False))
    cursor = MagicMock()
    cursor.fetchval.side_effect = [*range(10_000), None]
    cursor.messages = []
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    context = MagicMock()
    context.collect.return_value = ({}, {})

    def clock():
        cursor.execute.assert_called_once_with(
            "SELECT TOP (10000) int_col FROM #perf_test ORDER BY id"
        )
        context.enable.assert_called_once()
        context.collect.assert_not_called()
        assert cursor.fetchval.call_count in (0, 10_001)
        return 1.1 if cursor.fetchval.call_count else 1.0

    monkeypatch.setattr(benchmark_workloads.time, "perf_counter", clock)
    result = benchmark_workloads.scalar_fetchval(connection, "#perf_test", context)
    assert result["wall_ms"] == pytest.approx(100)
    assert result["detail"] == "Rows: 10000; type: int; API: fetchval; debug: disabled"
    assert cursor.fetchval.call_count == 10_001
    cursor.fetchone.assert_not_called()
    cursor.fetchall.assert_not_called()
    cursor.fetchmany.assert_not_called()
    context.collect.assert_called_once()
    context.disable.assert_called_once()
    connection.cursor.return_value.__exit__.assert_called_once()
    assert reporting.TASK_NAMES["scalar_fetchval"] in reporting.render([report], "c" * 40, 42)


@pytest.mark.parametrize(
    "problem", ("wrong-value", "wrong-type", "missing", "extra", "warning", "error", "debug")
)
def test_scalar_fetchval_workload_rejects_invalid_measurements(monkeypatch, problem):
    monkeypatch.setattr(
        "mssql_python.logging.logger", SimpleNamespace(is_debug_enabled=problem == "debug")
    )
    values = [*range(10_000), None]
    cursor = MagicMock()
    cursor.messages = []
    if problem == "wrong-value":
        values[0] = -1
    elif problem == "wrong-type":
        values[0] = 0.0
    elif problem == "missing":
        values[0] = None
    elif problem == "extra":
        values[-1] = 10_000
    elif problem == "warning":
        cursor.messages = [("01000", "unexpected")]
    cursor.fetchval.side_effect = RuntimeError("fetch failed") if problem == "error" else values
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    context = MagicMock()
    context.collect.return_value = ({}, {})
    error = {"debug": ValueError, "error": RuntimeError}.get(problem, AssertionError)
    with pytest.raises(error):
        benchmark_workloads.scalar_fetchval(connection, "#perf_test", context)
    if problem == "debug":
        connection.cursor.assert_not_called()
        context.enable.assert_not_called()
    else:
        context.disable.assert_called_once()
        connection.cursor.return_value.__exit__.assert_called_once()


def test_lob_workload_validates_payload_and_times_only_fetch(monkeypatch):
    size = 256 * 1024
    expected = "x" * size
    cursor = MagicMock()
    cursor.fetchall.return_value = [(expected,)]
    cursor.messages = []
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    context = MagicMock()
    context.collect.return_value = ({}, {})

    def enable():
        cursor.execute.assert_called_once()
        cursor.fetchall.assert_not_called()

    context.enable.side_effect = enable
    monkeypatch.setattr(benchmark_workloads.time, "perf_counter", MagicMock(side_effect=[1, 1.1]))
    result = benchmark_workloads.lob_fetch(connection, context)
    assert "(MAX)" in cursor.execute.call_args.args[0]
    assert result["wall_ms"] == pytest.approx(100)
    assert result["detail"] == f"Rows: 1; type: varchar; payload bytes: {size}; API: fetchall"
    cursor.fetchall.assert_called_once_with()
    cursor.fetchone.assert_not_called()
    cursor.fetchmany.assert_not_called()
    context.collect.assert_called_once()
    context.disable.assert_called_once()


@pytest.mark.parametrize(
    "problem", ("truncated", "wrong-type", "missing", "extra", "warning", "error")
)
def test_lob_workload_rejects_invalid_results_and_always_disables(problem):
    cursor = MagicMock()
    cursor.fetchall.return_value = [("x" * 262144,)]
    cursor.messages = []
    if problem == "truncated":
        cursor.fetchall.return_value = [("x" * 262143,)]
    elif problem == "wrong-type":
        cursor.fetchall.return_value = [(b"x" * 262144,)]
    elif problem == "missing":
        cursor.fetchall.return_value = []
    elif problem == "extra":
        cursor.fetchall.return_value *= 2
    elif problem == "warning":
        cursor.messages = [("01000", "unexpected")]
    else:
        cursor.fetchall.side_effect = RuntimeError("fetch failed")
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    context = MagicMock()
    context.collect.return_value = ({}, {})
    with pytest.raises(RuntimeError if problem == "error" else AssertionError):
        benchmark_workloads.lob_fetch(connection, context)
    context.disable.assert_called_once()


def test_query_workload_executes_and_collects(monkeypatch):
    cursor = MagicMock()
    cursor.fetchall.return_value = [(1,), (2,)]
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    context = MagicMock()
    context.collect.return_value = ({"cpp": {}}, {"py": {}})
    monkeypatch.setattr(benchmark_workloads.time, "perf_counter", MagicMock(side_effect=[1, 1.1]))
    result = benchmark_workloads.query(connection, context, "SELECT 1")
    cursor.execute.assert_called_once_with("SELECT 1")
    assert result["detail"] == "Rows: 2"
    context.enable.assert_called_once()
    context.disable.assert_called_once()


@pytest.mark.parametrize("named", [False, True])
def test_parameter_workload_executes_both_binding_forms(named):
    cursor = MagicMock()
    cursor.fetchone.side_effect = [(value,) for value in range(100)]
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    context = MagicMock()
    context.collect.return_value = ({}, {})
    result = benchmark_workloads.parameter_execution(connection, context, named=named)
    expected = ("SELECT %(value)s", {"value": 0}) if named else ("SELECT ?", (0,))
    assert cursor.execute.call_args_list[0].args == expected
    assert cursor.execute.call_count == 100
    assert result["detail"] == "Rows: 100"
    context.disable.assert_called_once()


@pytest.mark.parametrize("input_sizes", [False, True])
def test_legacy_insert_workload_executes_both_variants(input_sizes):
    cursor = MagicMock()
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    context = MagicMock()
    context.collect.return_value = ({}, {})
    result = benchmark_workloads.legacy_insertmany(connection, context, input_sizes=input_sizes)
    assert cursor.execute.call_count == 101
    assert cursor.setinputsizes.call_count == (100 if input_sizes else 0)
    assert result["detail"] == "Rows: 100000"
    connection.rollback.assert_called_once()
    context.disable.assert_called_once()


@pytest.mark.parametrize("fail", [False, True])
def test_worker_checkpoints_completed_and_active_scenarios(tmp_path, monkeypatch, capsys, fail):
    output = tmp_path / "base-0.json"
    result = dict(wall_ms=10.0, cpp={"ddbc::run": {}}, py={}, detail="Rows: 10")
    profiler = MagicMock()
    profiler.__enter__.return_value = profiler
    profiler._conn.cursor.return_value.__enter__.return_value.fetchone.return_value = ("16.0",)

    def run(name):
        partial = json.loads(output.read_text())
        assert partial["active_scenario"] == name
        if name == "second":
            assert set(partial["scenarios"]) == {"first"}
            if fail:
                raise RuntimeError("workload failure")
        return [result]

    profiler.run.side_effect = run
    core = SimpleNamespace(Profiler=lambda: profiler)
    workloads = SimpleNamespace(registry=lambda: {"first": None, "second": None})
    monkeypatch.setattr(controller, "check_build", lambda *a, **kw: None)
    monkeypatch.setattr(controller, "load_suite", lambda: (core, workloads))
    args = SimpleNamespace(source_root=tmp_path, scenarios=None, output=output)
    if fail:
        with pytest.raises(RuntimeError, match="workload failure"):
            controller.worker(args)
        assert json.loads(output.read_text())["active_scenario"] == "second"
    else:
        controller.worker(args)
        final = json.loads(output.read_text())
        assert final["environment"]["sql_version"] == "16.0"
        assert set(final["scenarios"]) == {"first", "second"}
        assert "active_scenario" not in final
    assert "Starting scenario: second" in capsys.readouterr().out
    profiler.__exit__.assert_called_once()


def test_measure_timeout_retains_partial_results_and_log(tmp_path, monkeypatch):
    output = tmp_path / "base-0.json"
    output.write_text('{"stale": true}')

    def timeout(command, **kwargs):
        assert not output.exists()
        assert command[1] == "-u"
        assert kwargs["timeout"] == 3
        output.write_text('{"status":"running","active_scenario":"fetchone"}')
        kwargs["stdout"].write("Starting scenario: fetchone\n")
        raise subprocess.TimeoutExpired(command, 3)

    monkeypatch.setattr(controller.subprocess, "run", timeout)
    with pytest.raises(subprocess.TimeoutExpired):
        controller.measure(tmp_path, output, ["fetchone"], timeout=3)
    assert json.loads(output.read_text())["active_scenario"] == "fetchone"
    assert "Starting scenario: fetchone" in output.with_suffix(".log").read_text()


@pytest.mark.skipif(os.name == "nt", reason="exercises POSIX process groups")
def test_build_timeout_terminates_descendants(tmp_path, monkeypatch):
    pybind = tmp_path / "mssql_python/pybind"
    pybind.mkdir(parents=True)
    pid_file = tmp_path / "descendant.pid"
    monkeypatch.setenv("DESCENDANT_PID", str(pid_file))
    (pybind / "build.sh").write_text(
        "#!/usr/bin/env bash\n"
        f'"{sys.executable}" -c "import time; time.sleep(60)" &\n'
        'echo "$!" > "$DESCENDANT_PID"\n'
        "wait\n",
        encoding="utf-8",
    )

    descendant = None
    try:
        with pytest.raises(subprocess.TimeoutExpired):
            controller.build(tmp_path, tmp_path / "build.log", timeout=1)
        descendant = int(pid_file.read_text(encoding="utf-8"))
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                os.kill(descendant, 0)
            except ProcessLookupError:
                break
            stat = Path(f"/proc/{descendant}/stat")
            if stat.is_file() and stat.read_text(encoding="utf-8").split()[2] == "Z":
                break
            time.sleep(0.05)
        else:
            pytest.fail("build descendant survived timeout cleanup")
    finally:
        if descendant is not None:
            try:
                os.kill(descendant, signal.SIGKILL)
            except ProcessLookupError:
                pass


def test_windows_process_tree_cleanup_uses_taskkill(monkeypatch):
    process = MagicMock(pid=123)
    taskkill = MagicMock(return_value=subprocess.CompletedProcess([], 0, ""))
    monkeypatch.setattr(controller, "WINDOWS", True)
    monkeypatch.setattr(controller.subprocess, "run", taskkill)
    controller.terminate_process_tree(process)
    taskkill.assert_called_once_with(
        ["taskkill", "/PID", "123", "/T", "/F"],
        capture_output=True,
        text=True,
    )
    process.wait.assert_called_once_with(timeout=5)


def test_windows_process_tree_cleanup_accepts_already_exited_process(monkeypatch):
    process = MagicMock(pid=123)
    process.poll.return_value = 0
    monkeypatch.setattr(controller, "WINDOWS", True)
    monkeypatch.setattr(
        controller.subprocess,
        "run",
        MagicMock(return_value=subprocess.CompletedProcess([], 128, "", "not found")),
    )
    controller.terminate_process_tree(process)
    process.kill.assert_not_called()


def test_overall_budget_caps_build_and_worker_time(monkeypatch):
    monkeypatch.setattr(controller.time, "monotonic", lambda: 100)
    assert controller.remaining(110, controller.WORKER_TIMEOUT) == 10
    assert controller.remaining(1000, 60) == 60
    with pytest.raises(TimeoutError, match="overall"):
        controller.remaining(100, controller.WORKER_TIMEOUT)


@pytest.mark.parametrize("scenarios,status", [(None, "complete"), (["select"], "incomplete")])
def test_full_sample_budget_fits_slow_hosted_workers(
    report, tmp_path, monkeypatch, scenarios, status
):
    # Run 174385 completed workers in 169-285s. Budget twelve five-minute
    # passes plus the full base-build/preflight allowance, not just measured pairs.
    clock = [0]
    measured = []
    monkeypatch.setattr(controller.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(controller, "resolve_revisions", lambda *a: ("a" * 40, "b" * 40))
    monkeypatch.setattr(controller, "git", lambda *a: "b" * 40)
    monkeypatch.setattr(controller, "checkout", lambda *a: None)
    monkeypatch.setenv("BUILD_BUILDID", "42")
    monkeypatch.setenv("SYSTEM_PULLREQUEST_SOURCECOMMITID", "c" * 40)

    def build(path, log, timeout):
        assert timeout >= 900
        clock[0] += 900

    def preflight(command, **kwargs):
        assert command[1:3] == ["-m", "eng.profiler_benchmarks.controller"]
        assert "--check-build" in command and kwargs["timeout"] == 60
        clock[0] += 60

    def measure(path, output, scenarios, timeout):
        assert scenarios == args.scenarios
        if timeout < 300:
            raise subprocess.TimeoutExpired("hosted worker replay", timeout)
        clock[0] += 300
        measured.append(output.name)
        return copy.deepcopy(report["pairs"][0]["base"])

    monkeypatch.setattr(controller, "build", build)
    monkeypatch.setattr(controller.subprocess, "run", preflight)
    monkeypatch.setattr(controller, "measure", measure)
    args = SimpleNamespace(
        base=None,
        candidate="HEAD",
        output=tmp_path,
        leg=report["leg"],
        samples=5,
        warmups=1,
        reuse_candidate=True,
        scenarios=scenarios,
    )
    controller.run(args)
    result = reporting.validate(json.loads((tmp_path / "report.json").read_text()))
    assert result["status"] == status and len(result["pairs"]) == 5
    assert measured == [
        f"{side}-{index}.json"
        for index in range(6)
        for side in (("base", "candidate") if index % 2 == 0 else ("candidate", "base"))
    ]
    assert clock[0] == 76 * 60
    assert clock[0] < controller.BENCHMARK_TIMEOUT


def test_ci_deadlines_include_setup_queueing_and_publication():
    pipeline = (ROOT / "eng/pipelines/pr-validation-pipeline.yml").read_text(encoding="utf-8")
    for job in ("PytestOnLinux",):
        section = pipeline.split(f"- job: {job}\n", 1)[1].split("\n- job:", 1)[0]
        job_minutes = int(re.search(r"^  timeoutInMinutes: (\d+)$", section, re.M)[1])
        benchmark_step = section.split(
            "python -m eng.profiler_benchmarks.controller --reuse-candidate", 1
        )[1]
        step_minutes = int(re.search(r"^    timeoutInMinutes: (\d+)$", benchmark_step, re.M)[1])
        assert step_minutes * 60 >= controller.BENCHMARK_TIMEOUT + 10 * 60
        assert job_minutes >= step_minutes + 60
        assert publisher.WAIT_MINUTES >= job_minutes + 60
    workflow = (ROOT / ".github/workflows/pr-profiler-report.yml").read_text(encoding="utf-8")
    workflow_minutes = int(re.search(r"timeout-minutes: (\d+)", workflow)[1])
    assert workflow_minutes >= publisher.WAIT_MINUTES + 10
    assert controller.LOCAL_BENCHMARK_TIMEOUT >= (
        2 * 15 * 60 + 2 * (5 + 1) * controller.WORKER_TIMEOUT
    )


def test_unix_profiler_step_does_not_put_database_password_on_command_line():
    pipeline = (ROOT / "eng/pipelines/pr-validation-pipeline.yml").read_text(encoding="utf-8")
    benchmark = pipeline.split("# Run Unix performance benchmarks on Ubuntu", 1)[1]
    benchmark = benchmark.split("displayName: 'Compare profiling builds", 1)[0]
    assert "-e DB_PASSWORD \\" in benchmark
    assert "Pwd=$(DB_PASSWORD)" not in benchmark
    assert "Pwd=$DB_PASSWORD" in benchmark


def test_build_check_rejects_foreign_provider_and_enabled_recording(tmp_path, monkeypatch):
    native = SimpleNamespace(
        __file__=str(tmp_path / "binding.so"), profiling=SimpleNamespace(is_enabled=lambda: False)
    )
    timer = SimpleNamespace(is_enabled=lambda: False)
    package = SimpleNamespace(
        __file__=str(tmp_path / "mssql_python/__init__.py"), ddbc_bindings=native, perf_timer=timer
    )
    provider = SimpleNamespace(__file__=str(tmp_path / "mssql_python_odbc/__init__.py"))
    monkeypatch.setitem(sys.modules, "mssql_python", package)
    monkeypatch.setitem(sys.modules, "mssql_python_odbc", provider)
    monkeypatch.setattr(sys, "path", sys.path[:])
    controller.check_build(tmp_path, True)
    provider.__file__ = str(tmp_path.parent / "candidate-provider/__init__.py")
    with pytest.raises(RuntimeError, match="outside"):
        controller.check_build(tmp_path, True)
    provider.__file__ = str(tmp_path / "provider/__init__.py")
    timer.is_enabled = lambda: True
    with pytest.raises(RuntimeError, match="recording OFF"):
        controller.check_build(tmp_path, True)


def test_head_moving_while_listing_comments_prevents_publish(monkeypatch):
    calls = []
    reads = 0

    def api(path, **kwargs):
        nonlocal reads
        calls.append((path, kwargs))
        assert not kwargs, "No write allowed after head moved"
        if path.startswith("pulls/"):
            reads += 1
            return {"state": "open", "head": {"sha": "old" if reads == 1 else "new"}}
        return []

    monkeypatch.setattr(publisher, "github", api)
    publisher.publish(1, "old", "data")
    assert reads == 2 and len(calls) == 3


def test_head_moving_before_write_supersedes_unchanged_pending_comment(monkeypatch):
    calls = []
    reads = 0

    def api(path, **kwargs):
        nonlocal reads
        calls.append((path, kwargs))
        if path.startswith("pulls/"):
            reads += 1
            return {
                "state": "open",
                "head": {"sha": "head" if reads == 1 else "new-head"},
                "base": {"sha": "base"},
            }
        if path == "issues/comments/42" and not kwargs:
            return {"id": 42, "body": publisher.pending_message("head")}
        if path.startswith("issues/") and "comments" in path:
            return [
                {
                    "id": 42,
                    "user": {"login": "github-actions[bot]"},
                    "body": publisher.pending_message("head"),
                }
            ]
        return {}

    monkeypatch.setattr(publisher, "github", api)
    publisher.publish(1, "head", "normal report", "base")
    writes = [
        kwargs["data"]["body"] for path, kwargs in calls if path == "issues/comments/42" and kwargs
    ]
    assert len(writes) == 1 and "Performance assessment superseded" in writes[0]


def test_base_moving_while_listing_comments_prevents_publish(monkeypatch):
    calls = []
    reads = 0

    def api(path, **kwargs):
        nonlocal reads
        calls.append((path, kwargs))
        assert not kwargs, "No write allowed after base moved"
        if path.startswith("pulls/"):
            reads += 1
            return {
                "state": "open",
                "head": {"sha": "head"},
                "base": {"sha": "base" if reads == 1 else "new-base"},
            }
        return []

    monkeypatch.setattr(publisher, "github", api)
    publisher.publish(1, "head", "data", "base")
    assert reads == 2 and len(calls) == 3


def test_abandoned_while_listing_comments_replaces_pending_with_terminal_state(monkeypatch):
    calls = []
    reads = 0

    def api(path, **kwargs):
        nonlocal reads
        calls.append((path, kwargs))
        if path.startswith("pulls/"):
            reads += 1
            return {
                "state": "open" if reads == 1 else "closed",
                "merged": False,
                "head": {"sha": "head"},
                "base": {"sha": "base"},
            }
        if path == "issues/comments/42" and not kwargs:
            return {"id": 42, "body": publisher.pending_message("head")}
        if path.startswith("issues/") and "comments" in path:
            return [
                {
                    "id": 42,
                    "user": {"login": "github-actions[bot]"},
                    "body": publisher.pending_message("head"),
                }
            ]
        return {}

    monkeypatch.setattr(publisher, "github", api)
    publisher.publish(1, "head", "normal report", "base")
    writes = [
        kwargs["data"]["body"] for path, kwargs in calls if path == "issues/comments/42" and kwargs
    ]
    assert len(writes) == 1 and "Pull request closed before assessment completed" in writes[0]


def test_abandoned_after_comment_write_is_immediately_terminalized(monkeypatch):
    calls = []
    reads = 0

    def api(path, **kwargs):
        nonlocal reads
        calls.append((path, kwargs))
        if path.startswith("pulls/"):
            reads += 1
            return {
                "state": "open" if reads < 3 else "closed",
                "merged": False,
                "head": {"sha": "head"},
                "base": {"sha": "base"},
            }
        if path.startswith("issues/") and "comments" in path:
            return [
                {
                    "id": 42,
                    "user": {"login": "github-actions[bot]"},
                    "body": publisher.pending_message("head"),
                }
            ]
        return {}

    monkeypatch.setattr(publisher, "github", api)
    publisher.publish(1, "head", "normal report", "base")
    writes = [kwargs["data"]["body"] for path, kwargs in calls if path == "issues/comments/42"]
    assert writes[0] == "normal report"
    assert "Pull request closed before assessment completed" in writes[1]


def test_head_change_after_comment_write_supersedes_only_unchanged_body(monkeypatch):
    calls = []
    reads = 0

    def api(path, **kwargs):
        nonlocal reads
        calls.append((path, kwargs))
        if path.startswith("pulls/"):
            reads += 1
            return {
                "state": "open",
                "head": {"sha": "head" if reads < 3 else "new-head"},
                "base": {"sha": "base"},
            }
        if path == "issues/comments/42" and not kwargs:
            return {"id": 42, "body": "normal report"}
        if path.startswith("issues/") and "comments" in path:
            return [
                {
                    "id": 42,
                    "user": {"login": "github-actions[bot]"},
                    "body": publisher.pending_message("head"),
                }
            ]
        return {}

    monkeypatch.setattr(publisher, "github", api)
    publisher.publish(1, "head", "normal report", "base")
    writes = [
        kwargs["data"]["body"] for path, kwargs in calls if path == "issues/comments/42" and kwargs
    ]
    assert writes[0] == "normal report"
    assert "Performance assessment superseded" in writes[1]


@pytest.mark.parametrize(
    "corrupt",
    [
        None,
        "zip",
        "timeout",
        "scenarios",
        "base",
        "provenance",
        "recursion",
        "deflate",
        "delayed",
    ],
)
def test_publisher_renders_validated_artifact_and_marks_missing_legs(report, monkeypatch, corrupt):
    posted = []
    linux_2025 = set_leg(report, "Linux-SQL2025")
    if corrupt == "scenarios":
        report["pairs"][0]["candidate"]["scenarios"] = list(reporting.CASES)
    data = {
        "Linux-SQL2025": zip_data([("report.json", json.dumps(linux_2025))]),
        "Linux-SQL2022": (
            b"invalid ZIP"
            if corrupt == "zip"
            else zip_data(
                [
                    (
                        "report.json",
                        (
                            "[" * 2000 + "0" + "]" * 2000
                            if corrupt == "recursion"
                            else json.dumps(report)
                        ),
                    )
                ]
            )
        ),
    }
    build = ado_build()
    if corrupt == "provenance":
        del build["sourceVersion"]
    artifacts = [
        {
            "name": "profiler-" + leg,
            "resource": {"downloadUrl": "https://dev.azure.com/" + leg},
        }
        for leg in data
    ]
    artifact_responses = [[], artifacts] if corrupt == "delayed" else [artifacts]
    clock = [0]

    def api(url):
        if "/artifacts?" not in url:
            return {"value": [build]}
        response = (
            artifact_responses.pop(0) if len(artifact_responses) > 1 else artifact_responses[0]
        )
        return {"value": response}

    monkeypatch.setattr(
        publisher, "publish", lambda number, head, body, base=None: posted.append(body)
    )
    monkeypatch.setattr(
        publisher,
        "github",
        pr_topology(base="e" * 40, merge_base="a" * 40) if corrupt == "base" else pr_topology(),
    )
    monkeypatch.setattr(publisher, "api", api)

    def fetch(url, **kwargs):
        leg = url.rsplit("/", 1)[-1]
        if corrupt == "timeout" and leg == "Linux-SQL2022":
            raise TimeoutError("timed out")
        return data[leg]

    monkeypatch.setattr(publisher, "fetch", fetch)
    if corrupt == "deflate":
        artifact_report = reporting.artifact_report

        def corrupt_deflate(raw):
            if raw == data["Linux-SQL2022"]:
                raise zlib.error("corrupt deflate stream")
            return artifact_report(raw)

        monkeypatch.setattr(reporting, "artifact_report", corrupt_deflate)
    monkeypatch.setattr(publisher.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        publisher.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds)
    )
    publisher.run(123, "c" * 40, 4)
    assert len(posted) == 2
    assert posted[0].startswith(reporting.MARKER)
    if corrupt in ("base", "provenance"):
        assert "Build provenance validation failed" in posted[1]
        return
    if corrupt in ("zip", "timeout", "scenarios", "recursion", "deflate"):
        assert "### Unix / SQL Server 2025" in posted[1]
        assert reporting.escape("Linux-SQL2022 (invalid artifact)") in posted[1]
        assert "Unavailable: Unix / SQL Server 2022 (invalid artifact)." in posted[1]
        assert (
            posted[1].count(f"{len(reporting.CASES)} database tasks consistently slowed down") == 1
        )
    else:
        assert "**Coverage:** 2 of 2 environments completed." in posted[1]
        assert "### Unix / SQL Server 2022" in posted[1]
        assert "### Unix / SQL Server 2025" in posted[1]
        assert (
            posted[1].count(f"{len(reporting.CASES)} database tasks consistently slowed down") == 1
        )


def test_publisher_waits_for_newer_run_after_exact_head_build_is_canceled(report, monkeypatch):
    canceled = ado_build(id=41, result="canceled")
    replacement = {**canceled, "id": 42, "result": "failed"}
    builds = [[canceled], [replacement]]
    posted = []
    clock = [0]

    def api(url):
        if "/builds?" in url:
            return {"value": builds.pop(0) if len(builds) > 1 else builds[0]}
        return {"value": []}

    monkeypatch.setattr(publisher, "api", api)
    monkeypatch.setattr(publisher, "github", pr_topology())
    monkeypatch.setattr(
        publisher, "publish", lambda number, head, body, base=None: posted.append(body)
    )
    monkeypatch.setattr(publisher.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        publisher.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds)
    )
    publisher.run(123, "c" * 40, 4)
    assert clock[0] == 150
    assert len(posted) == 2
    assert "buildId=42" in posted[1]


def test_publisher_ignores_cancelling_build_artifacts_and_uses_replacement(monkeypatch):
    posted = []
    clock = [0]
    cancelling = ado_build(id=41, status="cancelling", result=None)
    replacement = ado_build(id=42, status="inProgress", result=None)
    builds = [[cancelling], [replacement]]
    artifacts = [
        {"name": "profiler-" + leg, "resource": {"downloadUrl": "https://dev.azure.com/" + leg}}
        for leg in reporting.LEGS
    ]

    def api(url):
        if "/builds?" in url:
            return {"value": builds.pop(0) if len(builds) > 1 else builds[0]}
        return {"value": artifacts}

    monkeypatch.setattr(publisher, "api", api)
    monkeypatch.setattr(publisher, "github", pr_topology())
    monkeypatch.setattr(
        publisher, "publish", lambda number, head, body, base=None: posted.append(body)
    )
    monkeypatch.setattr(
        reporting, "assess", lambda evidence, *args: f"buildId={evidence.build['id']}"
    )
    monkeypatch.setattr(publisher.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        publisher.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds)
    )
    publisher.run(123, "c" * 40, 4)
    assert clock[0] == 30
    assert posted[-1] == "buildId=42"


def test_publisher_restarts_artifact_grace_when_completed_build_resumes(monkeypatch):
    posted = []
    clock = [0]
    builds = [
        ado_build(),
        ado_build(status="inProgress", result=None),
        ado_build(),
    ]
    artifacts = [
        {
            "name": "profiler-Linux-SQL2022",
            "resource": {"downloadUrl": "https://dev.azure.com/Linux-SQL2022"},
        }
    ]

    def api(url):
        if "/builds?" in url:
            return {"value": [builds.pop(0) if len(builds) > 1 else builds[0]]}
        return {"value": artifacts}

    monkeypatch.setattr(publisher, "api", api)
    monkeypatch.setattr(publisher, "github", pr_topology())
    monkeypatch.setattr(
        publisher, "publish", lambda number, head, body, base=None: posted.append(body)
    )
    monkeypatch.setattr(reporting, "assess", lambda *args: "partial report")
    monkeypatch.setattr(publisher.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        publisher.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds)
    )
    publisher.run(123, "c" * 40, 2)
    assert clock[0] == 180
    assert posted[-1] == "partial report"


@pytest.mark.parametrize("result", [None, "unknown"])
def test_publisher_rejects_unsupported_completed_results(monkeypatch, result):
    posted = []
    build = ado_build(result=result)

    def api(url):
        assert "/builds?" in url, "Unsupported builds must not query artifacts"
        return {"value": [build]}

    monkeypatch.setattr(publisher, "api", api)
    monkeypatch.setattr(publisher, "github", pr_topology())
    monkeypatch.setattr(
        publisher, "publish", lambda number, head, body, base=None: posted.append(body)
    )
    publisher.run(123, "c" * 40, 1)
    assert len(posted) == 2
    assert "unsupported result" in posted[1]


@pytest.mark.parametrize("status", [None, "notStarted", "inProgress"])
def test_publisher_deadline_finishes_with_terminal_comment(monkeypatch, status):
    posted = []
    clock = [0]
    build = ado_build(status=status, result=None)

    def github(path):
        return pr_topology()(path)

    def api(url):
        if "/builds?" in url:
            return {"value": [] if status is None else [build]}
        return {"value": []}

    def sleep(seconds):
        clock[0] += seconds

    monkeypatch.setattr(publisher.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(publisher.time, "sleep", sleep)
    monkeypatch.setattr(publisher, "github", github)
    monkeypatch.setattr(publisher, "api", api)
    monkeypatch.setattr(
        publisher, "publish", lambda number, head, body, base=None: posted.append(body)
    )
    publisher.run(123, "c" * 40, 1)
    assert clock[0] == 60 and len(posted) == 2
    assert "Performance assessment pending" in posted[0]
    assert "Performance assessment pending" not in posted[1]
    assert "Performance could not be assessed" in posted[1]


def test_publisher_retries_transient_polling_failures_before_finalizing(monkeypatch):
    posted = []
    clock = [0]
    responses = iter((URLError("temporary"), ValueError("bad JSON"), {"value": []}))
    monkeypatch.setattr(
        publisher, "publish", lambda number, head, body, base=None: posted.append(body)
    )
    monkeypatch.setattr(publisher, "github", pr_topology())
    monkeypatch.setattr(publisher, "api", lambda url: next(responses))
    monkeypatch.setattr(publisher.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        publisher.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds)
    )
    publisher.run(123, "c" * 40, 1)
    assert clock[0] == 60
    assert len(posted) == 2
    assert "Performance could not be assessed" in posted[1]


def test_publisher_bounds_consecutive_artifact_service_failures(monkeypatch):
    posted = []
    clock = [0]

    def api(url):
        if "/artifacts?" in url:
            raise URLError("temporary")
        return {"value": [ado_build(status="inProgress", result=None)]}

    monkeypatch.setattr(publisher, "api", api)
    monkeypatch.setattr(publisher, "github", pr_topology())
    monkeypatch.setattr(
        publisher, "publish", lambda number, head, body, base=None: posted.append(body)
    )
    monkeypatch.setattr(publisher.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        publisher.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds)
    )
    publisher.run(123, "c" * 40, 10)
    assert clock[0] == 120
    assert "Performance data services failed repeatedly" in posted[-1]


def test_publisher_retries_malformed_pr_and_artifact_responses(monkeypatch):
    posted = []
    clock = [0]
    prs = iter(({}, pr_topology()("pulls/123")))
    monkeypatch.setattr(
        publisher, "publish", lambda number, head, body, base=None: posted.append(body)
    )
    monkeypatch.setattr(publisher, "github", lambda path: next(prs))
    monkeypatch.setattr(publisher, "api", lambda url: {"value": []})
    monkeypatch.setattr(publisher.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        publisher.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds)
    )
    publisher.run(123, "c" * 40, 1)
    assert len(posted) == 2 and "Performance could not be assessed" in posted[1]
    with pytest.raises(ValueError, match="artifact list"):
        publisher.artifact_items({"value": [None]})
    with pytest.raises(ValueError, match="build list"):
        publisher.build_items({"value": [{}]})
    malformed = ado_build()
    malformed["repository"]["id"] = None
    with pytest.raises(ValueError, match="build list"):
        publisher.build_items({"value": [malformed]})


def test_artifact_polling_uses_remaining_publication_budget(monkeypatch):
    posted = []
    clock = [0]
    build = ado_build(status="inProgress", result=None)
    artifacts = [
        {"name": "profiler-" + leg, "resource": {"downloadUrl": "https://dev.azure.com/" + leg}}
        for leg in reporting.LEGS
    ]
    artifact_responses = iter([[]] * 5 + [artifacts])

    def api(url):
        return {"value": next(artifact_responses)} if "/artifacts?" in url else {"value": [build]}

    monkeypatch.setattr(publisher, "api", api)
    monkeypatch.setattr(publisher, "github", pr_topology())
    monkeypatch.setattr(
        publisher, "publish", lambda number, head, body, base=None: posted.append(body)
    )
    monkeypatch.setattr(reporting, "assess", lambda *args: "final report")
    monkeypatch.setattr(publisher.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        publisher.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds)
    )
    publisher.run(123, "c" * 40, 4)
    assert clock[0] == 150
    assert posted == [
        publisher.pending_message("c" * 40),
        "final report",
    ]


def test_publisher_finishes_after_merge_before_aggregate_build(monkeypatch):
    posted = []
    build = ado_build(status="inProgress", result=None)
    artifacts = [
        {"name": "profiler-" + leg, "resource": {"downloadUrl": "https://dev.azure.com/" + leg}}
        for leg in reporting.LEGS
    ]

    def api(url):
        return {"value": artifacts} if "/artifacts?" in url else {"value": [build]}

    monkeypatch.setattr(publisher, "api", api)
    monkeypatch.setattr(publisher, "github", pr_topology(state="closed", merged=True))
    monkeypatch.setattr(
        publisher, "publish", lambda number, head, body, base=None: posted.append(body)
    )
    monkeypatch.setattr(reporting, "assess", lambda *args: "final report")
    sleeps = []
    monkeypatch.setattr(publisher.time, "sleep", sleeps.append)
    publisher.run(123, "c" * 40, 4)
    assert sleeps == []
    assert posted[-1] == "final report"


def test_publisher_waits_for_usable_artifact_urls(monkeypatch):
    posted = []
    build = ado_build(status="inProgress", result=None)
    valid = [
        {"name": "profiler-" + leg, "resource": {"downloadUrl": "https://dev.azure.com/" + leg}}
        for leg in reporting.LEGS
    ]
    invalid = copy.deepcopy(valid)
    invalid[0]["resource"]["downloadUrl"] = ""
    responses = [invalid, valid]

    def api(url):
        if "/artifacts?" in url:
            return {"value": responses.pop(0)}
        return {"value": [build]}

    monkeypatch.setattr(publisher, "api", api)
    monkeypatch.setattr(publisher, "github", pr_topology())
    monkeypatch.setattr(
        publisher, "publish", lambda number, head, body, base=None: posted.append(body)
    )
    monkeypatch.setattr(reporting, "assess", lambda *args: "final report")
    sleeps = []
    monkeypatch.setattr(publisher.time, "sleep", sleeps.append)
    publisher.run(123, "c" * 40, 4)
    assert sleeps == [30]
    assert posted[-1] == "final report"


def test_completed_build_publishes_partial_result_after_artifact_grace(monkeypatch):
    posted = []
    clock = [0]
    build = ado_build()
    artifacts = [
        {
            "name": "profiler-Linux-SQL2022",
            "resource": {"downloadUrl": "https://dev.azure.com/Linux-SQL2022"},
        }
    ]

    def api(url):
        return {"value": artifacts} if "/artifacts?" in url else {"value": [build]}

    monkeypatch.setattr(publisher, "api", api)
    monkeypatch.setattr(publisher, "github", pr_topology())
    monkeypatch.setattr(
        publisher, "publish", lambda number, head, body, base=None: posted.append(body)
    )
    monkeypatch.setattr(reporting, "assess", lambda *args: "partial report")
    monkeypatch.setattr(publisher.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        publisher.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds)
    )
    publisher.run(123, "c" * 40, 1)
    assert clock[0] == publisher.ARTIFACT_GRACE_SECONDS
    assert posted[-1] == "partial report"


def test_deadline_does_not_assess_partial_running_build(monkeypatch):
    posted = []
    clock = [0]
    build = ado_build(status="inProgress", result=None)
    artifacts = [
        {
            "name": "profiler-Linux-SQL2022",
            "resource": {"downloadUrl": "https://dev.azure.com/Linux-SQL2022"},
        }
    ]

    def api(url):
        return {"value": artifacts} if "/artifacts?" in url else {"value": [build]}

    monkeypatch.setattr(publisher, "api", api)
    monkeypatch.setattr(publisher, "github", pr_topology())
    monkeypatch.setattr(
        publisher, "publish", lambda number, head, body, base=None: posted.append(body)
    )
    monkeypatch.setattr(
        reporting, "assess", lambda *args: pytest.fail("running partial build must not assess")
    )
    monkeypatch.setattr(publisher.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        publisher.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds)
    )
    publisher.run(123, "c" * 40, 1)
    assert "did not become ready within the 1-minute wait" in posted[-1]


def test_artifact_symlink_and_oversized_json_are_rejected():
    symlink = zipfile.ZipInfo("report.json")
    symlink.create_system = 3
    symlink.external_attr = 0o120777 << 16
    with pytest.raises(ValueError, match="Invalid report"):
        reporting.artifact_report(zip_data([(symlink, "{}")]))
    with pytest.raises(ValueError, match="Invalid report"):
        reporting.artifact_report(zip_data([("report.json", " " * (reporting.MAX_BYTES + 1))]))


@pytest.mark.parametrize("environment", [{"os": "Windows"}, {"sql_version": "17.0"}])
def test_report_leg_must_match_measured_environment(report, environment):
    for pair in report["pairs"]:
        for sample in pair.values():
            sample["environment"].update(environment)
    with pytest.raises(ValueError, match="leg"):
        reporting.validate(report)


def test_ci_reuses_profiling_builds_without_changing_release_defaults():
    pipeline = (ROOT / "eng/pipelines/pr-validation-pipeline.yml").read_text(encoding="utf-8")
    assert "benchmarks/perf-benchmarking.py" not in pipeline
    assert pipeline.count("python -m eng.profiler_benchmarks.controller --reuse-candidate") == 2
    profiler_conditions = re.findall(
        r"displayName: '(?:Compare profiling builds[^']*|Publish paired profiler measurements)'\n"
        r"    condition: ([^\n]+)",
        pipeline,
    )
    assert len(profiler_conditions) == 4
    assert profiler_conditions.count("false") == 2
    assert all(
        "eq(variables['Build.Reason'], 'PullRequest')" in condition
        for condition in profiler_conditions
        if condition != "false"
    )
    for release in (ROOT / "OneBranchPipelines").rglob("*.yml"):
        assert "ENABLE_PROFILING" not in release.read_text(encoding="utf-8")
    windows = pipeline.split("- job: pytestonwindows\n", 1)[1].split("\n- job:", 1)[0]
    assert "##vso[task.setvariable" not in windows
    assert windows.count("condition: false") >= 5
    assert "ArtifactName: 'ddbc_bindings'" in windows
    assert "Hosted Windows timings varied more than the regression threshold" in windows
    macos = pipeline.split("- job: PytestOnMacOS\n", 1)[1].split("\n- job:", 1)[0]
    assert "timeoutInMinutes: 90" in macos
    assert "ENABLE_PROFILING" not in macos
    assert "--reuse-candidate" not in macos
    assert "AdventureWorks2022" not in macos
    assert "profiler-macOS" not in macos
    assert "Routine PR profiling excludes hosted macOS" in macos
    linux = pipeline.split("- job: PytestOnLinux\n", 1)[1].split("\n- job:", 1)[0]
    assert 'if [ "$(Build.Reason)" = "PullRequest" ] &&' in linux
    assert '[[ "$(distroName)" =~ ^Ubuntu(-SQL2025)?$ ]]' in linux
    assert 'if [ "$PROFILER_BUILD" = "1" ]; then' in linux
    assert "python -m eng.profiler_benchmarks.controller --check-build on" in linux
    assert "profilerLeg: 'Linux-SQL2022'" in linux
    assert "profilerLeg: 'Linux-SQL2025'" in linux
    assert '--leg "$(profilerLeg)"' in linux
    assert "artifact: profiler-$(profilerLeg)" in linux
    benchmark = linux.split("# Run Unix performance benchmarks on Ubuntu with", 1)[1]
    assert "-e BUILD_BUILDID \\" in benchmark
    assert "BUILD_BUILDID: $(Build.BuildId)" in benchmark
    assert "git config --global --add safe.directory /workspace" in benchmark
    assert "apt-get install -y --reinstall libodbcinst2" in benchmark
    assert benchmark.index("apt-get install -y --reinstall libodbcinst2") < benchmark.index(
        "ACCEPT_EULA=Y apt-get install"
    )
    assert "libodbc1 " not in benchmark and "odbcinst1debian2" not in benchmark


def test_profiler_documentation_preserves_standalone_benchmarks_and_failed_build_contract():
    benchmarks = (ROOT / "benchmarks/README.md").read_text(encoding="utf-8")
    assert "perf-benchmarking.py" in benchmarks
    assert "Profiler benchmark comparisons" in benchmarks
    contract = (ROOT / "eng/profiler_benchmarks/README.md").read_text(encoding="utf-8")
    assert "failed aggregate build can still publish" in contract


def test_comment_workflow_separates_same_repo_and_fork_trust():
    workflow = (ROOT / ".github/workflows/pr-profiler-report.yml").read_text(encoding="utf-8")
    assert "pull_request:" in workflow
    assert "pull_request_target:" in workflow
    assert (
        "profiler-report-${{ github.event.pull_request.number }}-${{ github.event_name }}"
        in workflow
    )
    assert "github.event.pull_request.head.repo.full_name == github.repository" in workflow
    assert "github.event.pull_request.head.repo.full_name != github.repository" in workflow
    assert (
        "github.event_name == 'pull_request' && github.event.pull_request.head.sha || "
        "github.event.pull_request.base.sha"
    ) in workflow
    assert "persist-credentials: false" in workflow
    assert "actions/checkout@11d5960a326750d5838078e36cf38b85af677262" in workflow
    assert "actions/setup-python@a26af69be951a213d495a4c3e4e4022e16d87065" in workflow
    coverage = (ROOT / ".github/workflows/pr-code-coverage.yml").read_text(encoding="utf-8")
    assert coverage.count("extract_coverage_artifact.py") == 2
    assert coverage.count("--max-filesize 268435456") == 2
    assert "unzip -o" not in coverage
    assert '-o "$COVERAGE_ARCHIVE"' in coverage
    assert '-o "$COVERAGE_XML_ARCHIVE"' in coverage
    assert "-o coverage-report.zip" not in coverage
    assert "-o coverage-artifacts.zip" not in coverage
    assert coverage.count("._links.web.href") == 1
    assert (
        'ADO_URL="https://dev.azure.com/sqlclientdrivers/public/_build/results?buildId=$BUILD_ID"'
        in coverage
    )
    assert 'cp "$COVERAGE_XML"' not in coverage
    assert 'diff-cover "$COVERAGE_XML"' in coverage
    assert "COVERAGE_XML: ${{ runner.temp }}/coverage.xml" in coverage
    assert (
        "head.ref" not in workflow
        and "head.sha }}" not in workflow.split("ref:", 1)[1].split("persist", 1)[0]
    )


class TestSingleRowMeasurementModes(unittest.TestCase):
    """Pure fake-boundary contracts; these do not qualify native timing."""

    def sample_report(self, mode="latency", ratio=0.9):
        def counter(calls):
            return dict(calls=calls, total_us=10000, min_us=1, max_us=100)

        def sample(side):
            guarded = side == "candidate"
            cases = {}
            for name in reporting.FETCH_CASES:
                shape, method = name.split("_")
                cpp = {}
                if mode == "route":
                    timer = (
                        "ddbc::FetchMany_wrap" if method == "fetchmany" else "ddbc::FetchOne_wrap"
                    )
                    cpp[timer] = counter(1001)
                    if guarded:
                        cpp["ddbc::FetchRow::construct_row"] = counter(1000)
                cases[name] = dict(
                    wall_ms=100 * (ratio if guarded else 1),
                    cpp=cpp,
                    py={},
                    work=f"Rows: 1000; shape: {shape}; API: {method}; EOF: 1",
                )
            return dict(
                status="complete",
                mode=mode,
                provenance=dict(
                    source_commit=("b" if guarded else "a") * 40,
                    native_file="/source/native.so",
                    native_sha256=("d" if guarded else "e") * 64,
                    native_profiling=mode == "route",
                    guarded_row=guarded,
                ),
                environment=dict(
                    os="Linux", architecture="x86_64", python="3.13.7", sql_version="16.0"
                ),
                scenarios=cases,
            )

        return dict(
            schema_version=2,
            mode=mode,
            status="complete",
            leg="Linux-SQL2022",
            base_commit="a" * 40,
            source_commit="b" * 40,
            head_commit="c" * 40,
            build_id=42,
            samples=5,
            warmups=1,
            pairs=[dict(base=sample("base"), candidate=sample("candidate")) for _ in range(5)],
        )

    def source_anchor(self, source_root, revision):
        return dict(
            source_commit=revision,
            python_cursor_sha256=("1" if revision == "a" * 40 else "2") * 64,
            row_route=dict(
                version=1,
                methods=dict(fetchone=False, fetchmany=revision != "a" * 40, fetchval=False),
            ),
        )

    def new_sample_report(self, mode="latency", ratio=1.1):
        report = self.sample_report(mode, ratio)
        report["python_sources"] = {
            side: self.source_anchor(
                None, report["base_commit" if side == "base" else "source_commit"]
            )
            for side in ("base", "candidate")
        }
        for pair in report["pairs"]:
            for side, sample in pair.items():
                sample["provenance"].update(copy.deepcopy(report["python_sources"][side]))
                if mode == "route":
                    for name, case in sample["scenarios"].items():
                        if not name.endswith("fetchmany"):
                            case["cpp"].pop("ddbc::FetchRow::construct_row", None)
        return report

    def test_modes_validate_and_keep_subthreshold_changes_visible(self):
        self.assertEqual(tuple(benchmark_workloads.single_row_registry()), reporting.FETCH_CASES)
        self.assertEqual(len(reporting.CASES), 22)
        for mode in ("latency", "route"):
            for ratio in (0.9, 1.1):
                with self.subTest(mode=mode, ratio=ratio):
                    report = self.sample_report(mode, ratio)
                    reporting.validate(report)
                    self.assertTrue(
                        all(row["status"] == "ok" for row in reporting.comparisons(report))
                    )
                    body = reporting.render([report], "c" * 40, 42)
                    self.assertIn(f"{(ratio - 1) * 100:+.1f}%", body)
                    self.assertIn("Python phases OFF", body)
                    self.assertIn("All database tasks and timings", body)
                    self.assertIn(f"{ratio:.3f} [{ratio:.3f}, {ratio:.3f}]", body)
                    self.assertIn("1,000 mixed rows / fetchmany(1)", body)
                    if mode == "route":
                        self.assertIn("not production latency", body)

    def test_threshold_policy_is_unchanged(self):
        self.assertEqual((reporting.THRESHOLD, reporting.MIN_DELTA_MS), (0.20, 1.0))
        for ratio, expected in (
            (0.8, "ok"),
            (0.79, "improvement"),
            (1.2, "ok"),
            (1.21, "regression"),
        ):
            with self.subTest(ratio=ratio):
                self.assertEqual(
                    reporting.comparisons(self.sample_report(ratio=ratio))[0]["status"], expected
                )
        report = self.sample_report(ratio=1.3)
        for pair in report["pairs"][:2]:
            pair["candidate"]["scenarios"]["numeric_fetchone"]["wall_ms"] = 100
        self.assertEqual(reporting.comparisons(report)[0]["status"], "noisy")
        report["pairs"][1]["candidate"]["scenarios"]["numeric_fetchone"]["wall_ms"] = 130
        self.assertEqual(reporting.comparisons(report)[0]["status"], "regression")
        for pair in report["pairs"]:
            pair["base"]["scenarios"]["numeric_fetchone"]["wall_ms"] = 1
            pair["candidate"]["scenarios"]["numeric_fetchone"]["wall_ms"] = 1.5
        self.assertEqual(reporting.comparisons(report)[0]["status"], "ok")

    def test_schema_rejects_mismatched_or_missing_evidence(self):
        mutations = (
            lambda sample: sample.update(mode="diagnostic"),
            lambda sample: sample.update(status="running"),
            lambda sample: sample.pop("provenance"),
            lambda sample: sample["provenance"].update(native_profiling=True),
            lambda sample: sample["provenance"].update(source_commit="f" * 40),
            lambda sample: sample["provenance"].update(native_sha256="invalid"),
            lambda sample: sample["scenarios"]["mixed_fetchval"].update(py={"py::fetch": {}}),
            lambda sample: sample["scenarios"]["mixed_fetchval"].update(cpp={"ddbc::fetch": {}}),
            lambda sample: sample["scenarios"].pop("numeric_fetchmany"),
            lambda sample: sample["scenarios"]["mixed_fetchval"].update(wall_ms=0),
            lambda sample: sample["scenarios"]["mixed_fetchval"].update(work="Rows: 1"),
        )
        for index, mutate in enumerate(mutations):
            with self.subTest(mutation=index):
                report = self.sample_report()
                mutate(report["pairs"][0]["candidate"])
                with self.assertRaises(ValueError):
                    reporting.validate(report)
        report = self.sample_report()
        report["pairs"][1]["candidate"]["provenance"]["native_sha256"] = "f" * 64
        with self.assertRaisesRegex(ValueError, "changed between samples"):
            reporting.validate(report)
        with self.assertRaisesRegex(ValueError, "different measurement modes"):
            reporting.render([self.sample_report(), self.sample_report("route")], "c" * 40, 42)

    def test_route_requires_actual_counts_not_only_timers(self):
        for label, calls in (("ddbc::FetchOne_wrap", 1000), ("ddbc::FetchRow::construct_row", 0)):
            with self.subTest(label=label):
                report = self.sample_report("route")
                report["pairs"][0]["candidate"]["scenarios"]["numeric_fetchone"]["cpp"][label][
                    "calls"
                ] = calls
                with self.assertRaises(ValueError):
                    reporting.validate(report)
        report = self.sample_report("route")
        report["pairs"][0]["base"]["scenarios"]["mixed_fetchval"]["cpp"] = {}
        with self.assertRaises(ValueError):
            reporting.validate(report)

    def test_incomplete_measurement_is_unavailable_not_zero(self):
        for mode in ("latency", "route"):
            report = self.sample_report(mode)
            report.update(status="incomplete", pairs=[])
            reporting.validate(report)
            body = reporting.render([report], "c" * 40, 42)
            self.assertIn("Performance unavailable", body)
            self.assertNotIn("0.000 ms", body)

    def test_context_never_enables_python_and_rejects_contamination(self):
        with self.assertRaises(ValueError):
            controller._FetchContext("latency", MagicMock(), MagicMock())
        for mode in ("latency", "route"):
            with self.subTest(mode=mode):
                native = MagicMock() if mode == "route" else None
                python = MagicMock()
                python.is_enabled.return_value = False
                python.get_stats.return_value = {}
                if native is not None:
                    native.get_stats.return_value = {"timer": 1}
                ctx = controller._FetchContext(mode, native, python)
                ctx.enable()
                python.enable.assert_not_called()
                python.enable_timeline.assert_not_called()
                python.reset.assert_called_once()
                cpp, py = ctx.collect()
                self.assertEqual(py, {})
                self.assertEqual(cpp, {"timer": 1} if native is not None else {})
                python.is_enabled.return_value = True
                with self.assertRaisesRegex(RuntimeError, "became enabled"):
                    ctx.collect()
                python.is_enabled.return_value = False
                python.get_stats.return_value = {"py::unexpected": {}}
                with self.assertRaisesRegex(RuntimeError, "contaminate"):
                    ctx.collect()
                ctx.disable()
                if native is not None:
                    native.enable.assert_called_once()
                    native.disable_timeline.assert_called()

    def test_six_workloads_time_only_fetch_and_validate_full_results(self):
        from unittest.mock import patch

        for name, workload in benchmark_workloads.single_row_registry().items():
            with self.subTest(name=name):
                shape, method = name.split("_")
                cursor = MagicMock()
                cursor.messages = []
                rows = [(n, n + 10 if shape == "numeric" else "text") for n in range(1000)]
                values = (
                    list(range(1000))
                    if method == "fetchval"
                    else [[row] for row in rows] if method == "fetchmany" else rows
                )
                fetch = getattr(cursor, method)
                fetch.side_effect = values + ([[]] if method == "fetchmany" else [None])
                conn = MagicMock()
                conn.cursor.return_value.__enter__.return_value = cursor
                ctx = MagicMock()
                ctx.collect.return_value = ({}, {})

                def clock():
                    cursor.execute.assert_called_once()
                    ctx.enable.assert_called_once()
                    ctx.collect.assert_not_called()
                    self.assertIn(fetch.call_count, (0, 1001))
                    return 2.0 if fetch.call_count else 1.0

                package = SimpleNamespace(logger=SimpleNamespace(is_debug_enabled=False))
                with (
                    patch.dict(sys.modules, {"mssql_python.logging": package}),
                    patch.object(benchmark_workloads.time, "perf_counter", clock),
                ):
                    result = workload(conn, ctx)
                self.assertEqual(result["wall_ms"], 1000)
                self.assertEqual(fetch.call_count, 1001)
                if method == "fetchmany":
                    self.assertTrue(
                        all(
                            call.args == (1,) and type(call.args[0]) is int
                            for call in fetch.call_args_list
                        )
                    )
                for other in {"fetchone", "fetchmany", "fetchval"} - {method}:
                    getattr(cursor, other).assert_not_called()
                ctx.disable.assert_called_once()
                conn.cursor.return_value.__exit__.assert_called_once()

    def test_workloads_fail_closed_and_disable_context(self):
        from unittest.mock import patch

        for failure in ("type", "value", "eof", "warning", "fetch", "enable", "debug"):
            with self.subTest(failure=failure):
                cursor = MagicMock()
                cursor.messages = [("01000", "warning")] if failure == "warning" else []
                rows = [(n, "text") for n in range(1000)]
                if failure == "type":
                    rows[0] = (0.0, "text")
                if failure == "value":
                    rows[0] = (0, "wrong")
                cursor.fetchone.side_effect = (
                    RuntimeError("fetch")
                    if failure == "fetch"
                    else rows + ([rows[0]] if failure == "eof" else [None])
                )
                conn = MagicMock()
                conn.cursor.return_value.__enter__.return_value = cursor
                ctx = MagicMock()
                ctx.collect.return_value = ({}, {})
                if failure == "enable":
                    ctx.enable.side_effect = RuntimeError("enable")
                package = SimpleNamespace(
                    logger=SimpleNamespace(is_debug_enabled=failure == "debug")
                )
                with patch.dict(sys.modules, {"mssql_python.logging": package}):
                    with self.assertRaises((AssertionError, RuntimeError, ValueError)):
                        benchmark_workloads.single_row_fetch(conn, ctx, "fetchone", "mixed")
                if failure == "debug":
                    conn.cursor.assert_not_called()
                else:
                    ctx.disable.assert_called_once()
                    conn.cursor.return_value.__exit__.assert_called_once()

    def test_off_build_and_worker_mode_are_explicit(self):
        from unittest.mock import patch

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            process = MagicMock()
            process.wait.return_value = 0
            with patch.object(controller.subprocess, "Popen", return_value=process) as start:
                controller.build(path, path / "build.log", profiling=False)
                self.assertEqual(start.call_args.kwargs["env"]["ENABLE_PROFILING"], "0")
            with patch.object(controller.subprocess, "run") as run:
                output = path / "sample.json"
                run.side_effect = lambda *a, **k: output.write_text('{"status":"complete"}')
                controller.measure(path, output, None, mode="latency", revision="a" * 40)
                command = run.call_args.args[0]
                self.assertEqual(command[command.index("--mode") + 1], "latency")
                self.assertEqual(command[command.index("--revision") + 1], "a" * 40)
            with patch.object(controller, "resolve_revisions") as resolve:
                with self.assertRaisesRegex(ValueError, "cannot reuse"):
                    controller.run(SimpleNamespace(mode="latency", reuse_candidate=True))
                resolve.assert_not_called()

    def test_worker_provenance_route_counts_and_failure_checkpoint(self):
        from unittest.mock import patch

        for mode, failure in (
            ("latency", None),
            ("route", None),
            ("route", "counts"),
            ("latency", "close"),
        ):
            with (
                self.subTest(mode=mode, failure=failure),
                tempfile.TemporaryDirectory() as directory,
            ):
                root = Path(directory)
                source = root / "mssql_python" / "cursor.py"
                source.parent.mkdir()
                source.write_bytes((ROOT / "mssql_python" / "cursor.py").read_bytes())
                cursor_module = SimpleNamespace(__file__=str(source))
                native_file = root / "native.so"
                native_file.write_bytes(b"fake binary identity; never loaded")
                args = SimpleNamespace(
                    source_root=root, revision="b" * 40, scenarios=None, output=root / "sample.json"
                )
                native = SimpleNamespace(
                    module=SimpleNamespace(__file__=str(native_file)), DDBCSQLFetchRow=object()
                )
                if mode == "route":
                    native.profiling = MagicMock()
                python = MagicMock()
                conn = MagicMock()
                conn.__enter__.return_value = conn
                if failure == "close":
                    conn.__exit__.side_effect = RuntimeError("connection close failed")
                conn.cursor.return_value.__enter__.return_value.fetchone.return_value = ("16.0",)
                package = SimpleNamespace(
                    ddbc_bindings=native,
                    perf_timer=python,
                    cursor=cursor_module,
                    connect=MagicMock(return_value=conn),
                )
                sample = self.new_sample_report(mode)["pairs"][0]["candidate"]

                def workload(connection, ctx):
                    ctx.enable()
                    # Measurement results here are fake; native count validation is real.
                    name = json.loads(args.output.read_text())["active_scenario"]
                    result = copy.deepcopy(sample["scenarios"][name])
                    result["detail"] = result.pop("work")
                    if failure == "counts":
                        result["cpp"]["ddbc::FetchRow::construct_row"] = dict(calls=999)
                    ctx.disable()
                    return result

                cases = {name: workload for name in reporting.FETCH_CASES}
                with (
                    patch.dict(
                        sys.modules, {"mssql_python": package, "mssql_python.cursor": cursor_module}
                    ),
                    patch.dict(os.environ, {"DB_CONNECTION_STRING": "test-only"}),
                    patch.object(controller, "check_build") as check,
                    patch.object(controller.platform, "system", return_value="Linux"),
                    patch.object(controller.platform, "machine", return_value="x86_64"),
                    patch.object(controller.platform, "python_version", return_value="3.13.7"),
                    patch.object(benchmark_workloads, "single_row_registry", return_value=cases),
                ):
                    if failure == "counts":
                        with self.assertRaisesRegex(RuntimeError, "route was not established"):
                            controller.fetch_worker(args, mode)
                    elif failure == "close":
                        with self.assertRaisesRegex(RuntimeError, "connection close failed"):
                            controller.fetch_worker(args, mode)
                    else:
                        controller.fetch_worker(args, mode)
                check.assert_called_once_with(root, profiling=mode == "route")
                result = json.loads(args.output.read_text())
                self.assertEqual(result["status"], "running" if failure else "complete")
                self.assertEqual(result["provenance"]["native_profiling"], mode == "route")
                self.assertEqual(len(result["provenance"]["native_sha256"]), 64)
                self.assertEqual(result["provenance"]["source_commit"], "b" * 40)
                self.assertEqual(len(result["scenarios"]), 0 if failure == "counts" else 6)
                python.enable.assert_not_called()
                if mode == "route":
                    native.profiling.disable.assert_called()

    def test_controller_pairs_use_selected_mode_and_fail_incomplete(self):
        from unittest.mock import patch

        for mode, fail in (("latency", False), ("route", False), ("latency", True)):
            with self.subTest(mode=mode, fail=fail), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                args = SimpleNamespace(
                    base="a" * 40,
                    candidate="b" * 40,
                    output=root,
                    mode=mode,
                    reuse_candidate=False,
                    leg="Linux-SQL2022",
                    samples=3,
                    warmups=1,
                    scenarios=None,
                )
                sample = self.new_sample_report(mode)["pairs"][0]
                calls = []

                def measure(path, output, scenarios, timeout, **options):
                    calls.append((path.name, options))
                    if fail:
                        raise RuntimeError("missing measurement")
                    return copy.deepcopy(sample[path.name])

                with (
                    patch.object(
                        controller, "resolve_revisions", return_value=("a" * 40, "b" * 40)
                    ),
                    patch.object(controller, "checkout"),
                    patch.object(
                        controller, "python_source_identity", side_effect=self.source_anchor
                    ),
                    patch.object(controller, "build") as build,
                    patch.object(controller, "measure", side_effect=measure),
                ):
                    if fail:
                        with self.assertRaisesRegex(RuntimeError, "missing measurement"):
                            controller.run(args)
                    else:
                        controller.run(args)
                self.assertEqual(build.call_count, 2)
                self.assertTrue(
                    all(
                        call.kwargs == {"profiling": mode != "latency"}
                        for call in build.call_args_list
                    )
                )
                for side, options in calls:
                    self.assertEqual(
                        options, dict(mode=mode, revision=("a" if side == "base" else "b") * 40)
                    )
                result = reporting.validate(json.loads((root / "report.json").read_text()))
                self.assertEqual(result["status"], "incomplete" if fail else "complete")
                if not fail:
                    self.assertEqual(
                        [side for side, _ in calls], ["base", "candidate", "candidate", "base"] * 2
                    )
                    self.assertEqual(len(result["pairs"]), 3)

    def test_legacy_diagnostic_schema_still_requires_native_samples(self):
        report = self.sample_report()
        report.pop("mode")
        report["schema_version"] = 1
        for pair in report["pairs"]:
            for sample in pair.values():
                sample.pop("mode")
                sample.pop("provenance")
                sample["scenarios"] = {
                    name: dict(
                        wall_ms=100,
                        work="Rows: 1",
                        cpp={"ddbc::query": dict(calls=1, total_us=10, min_us=10, max_us=10)},
                        py={},
                    )
                    for name in reporting.CASES
                }
        reporting.validate(report)
        self.assertEqual(len(reporting.comparisons(report)), 22)
        self.assertIn("profiling-enabled builds", reporting.render([report], "c" * 40, 42))
        report["pairs"][0]["base"]["scenarios"]["fetchone"]["cpp"] = {}
        with self.assertRaisesRegex(ValueError, "profiling data"):
            reporting.validate(report)

    def sample_bundle(self, modern=False):
        factory = self.new_sample_report if modern else self.sample_report
        latency = factory("latency")
        route = factory("route")
        diagnostic = copy.deepcopy(route)
        diagnostic.pop("mode")
        diagnostic["schema_version"] = 1
        for pair in diagnostic["pairs"]:
            for sample in pair.values():
                sample["mode"] = "diagnostic"
                cell = sample["scenarios"]["numeric_fetchone"]
                sample["scenarios"] = {name: copy.deepcopy(cell) for name in reporting.CASES}
        diagnostic.update(
            measurement_bundle_version=1, fetch_measurements=dict(latency=latency, route=route)
        )
        return diagnostic

    def test_ci_bundle_all_mode_outcomes_restore_diagnostic_verdicts(self):
        for bits in range(8):
            with self.subTest(completeness=bits):
                bundle = self.sample_bundle()
                for index, mode in enumerate(("latency", "route", "diagnostic")):
                    report = bundle if mode == "diagnostic" else bundle["fetch_measurements"][mode]
                    if not bits & (1 << index):
                        report.update(
                            status="incomplete",
                            pairs=[],
                            unavailable_reason="Not started: budget exhausted",
                        )
                valid, reasons = reporting.ci_mode_reports(bundle)
                self.assertEqual(len(valid), 3)
                self.assertEqual(len(reasons), 3 - bits.bit_count())
                body = reporting.render_ci_reports([bundle], "c" * 40, 42)
                self.assertIn("original 22-task profiling-enabled diagnostics", body)
                self.assertIn("not headline inputs", body)
                self.assertIn("when available", body)
                self.assertIn("do not represent production-wheel latency", body)
                self.assertNotIn("Primary verdict:", body)
                self.assertNotIn("### Measurement availability", body)
                self.assertNotIn("<summary>Native route attribution", body)
                self.assertEqual("Performance unavailable" in body, not bool(bits & 4))
                self.assertNotIn("Performance improved", body)
                self.assertLessEqual(len(body), reporting.MAX_COMMENT_CHARS)
                headings = [
                    "<summary><b>Performance diagnostics</b></summary>",
                    "<summary><b>All database tasks and timings</b></summary>",
                    "<summary><b>Build and measurement details</b></summary>",
                ]
                positions = [body.index(heading) for heading in headings]
                self.assertEqual(positions, sorted(positions))
                if bits & 4:
                    self.assertIn(
                        "| Database task | Before | After | Paired change | Result |", body
                    )
                    table = body.split(headings[1], 1)[1].split("</details>", 1)[0]
                    for name in reporting.CASES:
                        self.assertIn("| " + reporting.TASK_NAMES[name] + " |", table)
                    self.assertNotIn("Ratio median", table)
                else:
                    self.assertIn("Not started: budget exhausted", body)

    def test_ci_restored_headline_does_not_substitute_other_modes(self):
        for diagnostic_ratio, latency_ratio in ((0.6, 1.5), (1.5, 0.6)):
            with self.subTest(diagnostic_ratio=diagnostic_ratio):
                bundle = self.sample_bundle()
                for report, ratio in (
                    (bundle, diagnostic_ratio),
                    (bundle["fetch_measurements"]["latency"], latency_ratio),
                ):
                    for pair in report["pairs"]:
                        for name, cell in pair["candidate"]["scenarios"].items():
                            cell["wall_ms"] = pair["base"]["scenarios"][name]["wall_ms"] * ratio
                body = reporting.render_ci_reports([bundle], "c" * 40, 42)
                self.assertEqual("Performance improved" in body, diagnostic_ratio < 1)
                self.assertEqual("Performance regression detected" in body, diagnostic_ratio > 1)
                self.assertIn("not headline inputs", body)
                self.assertIn("when available", body)
                bundle["pairs"][0]["candidate"]["scenarios"].pop("fetchone")
                body = reporting.render_ci_reports([bundle], "c" * 40, 42)
                self.assertIn("Performance unavailable", body)
                self.assertIn("Invalid mode data", body)
                self.assertNotIn("Performance improved", body)

    def test_ci_bundle_rejects_bad_siblings_and_shared_on_drift(self):
        for defect in ("missing", "mode", "commit", "counter", "identity", "environment"):
            with self.subTest(defect=defect):
                bundle = self.sample_bundle()
                route = bundle["fetch_measurements"]["route"]
                if defect == "missing":
                    bundle["fetch_measurements"].pop("route")
                elif defect == "mode":
                    route["mode"] = "latency"
                elif defect == "commit":
                    route["source_commit"] = "f" * 40
                elif defect == "counter":
                    route["pairs"][0]["candidate"]["scenarios"]["mixed_fetchmany"]["cpp"] = {}
                elif defect == "identity":
                    for pair in route["pairs"]:
                        pair["candidate"]["provenance"]["native_sha256"] = "f" * 64
                else:
                    for pair in route["pairs"]:
                        for sample in pair.values():
                            sample["environment"]["python"] = "3.12.0"
                valid, reasons = reporting.ci_mode_reports(bundle)
                self.assertIn("latency", valid)
                self.assertNotIn("route", valid)
                self.assertIn("route", reasons)
                if defect in ("identity", "environment"):
                    self.assertNotIn("diagnostic", valid)
                body = reporting.render_ci_reports([bundle], "c" * 40, 42)
                self.assertIn("Unavailable:", body)
                self.assertEqual("Performance unavailable" in body, "diagnostic" not in valid)
                if "diagnostic" in valid:
                    self.assertIn("| Row-by-row fetching |", body)
                else:
                    self.assertIn(reasons["diagnostic"], body)
        bundle = self.sample_bundle()
        for pair in bundle["pairs"]:
            for side in ("base", "candidate"):
                for cell in pair[side]["scenarios"].values():
                    cell["cpp"] = {
                        "ddbc::"
                        + str(n)
                        + "_"
                        * 150: dict(
                            calls=1 if side == "base" else 2,
                            total_us=100 if side == "base" else 200,
                            min_us=1,
                            max_us=10,
                        )
                        for n in range(3)
                    }
        body = reporting.render_ci_reports([bundle], "c" * 40, 42)
        self.assertLessEqual(len(body), reporting.MAX_COMMENT_CHARS)
        self.assertIn("raw artifact", body)
        for version in (None, True, 2):
            bundle = self.sample_bundle()
            bundle["measurement_bundle_version"] = version
            with self.assertRaises(ValueError):
                reporting.validate_ci_header(bundle)
        bundle = self.sample_bundle()
        bundle.pop("measurement_bundle_version")
        with self.assertRaises(ValueError):
            reporting.validate_ci_header(bundle)

    def test_ci_bundle_existing_artifact_and_assessment_preserve_trust(self):
        bundle = self.sample_bundle()
        reporting.validate(bundle)  # The legacy schema-1 diagnostic root still works.

        def archive(value, duplicate=False):
            buf = io.BytesIO()
            with zipfile.ZipFile(buf, "w") as output:
                output.writestr("profiler-Linux-SQL2022/report.json", json.dumps(value))
                output.writestr("profiler-Linux-SQL2022/route-base-0.json", "{}")
                if duplicate:
                    output.writestr("route/report.json", "{}")
            return buf.getvalue()

        self.assertEqual(reporting.artifact_report(archive(bundle)), bundle)
        with self.assertRaisesRegex(ValueError, "exactly one"):
            reporting.artifact_report(archive(bundle, True))
        evidence = reporting.AssessmentEvidence(
            build=dict(id=42, sourceVersion="b" * 40),
            head="c" * 40,
            base="a" * 40,
            merge_commit=dict(sha="b" * 40, parents=[dict(sha="a" * 40), dict(sha="c" * 40)]),
            base_commit=dict(sha="a" * 40),
        )
        body = reporting.assess(
            evidence, {"Linux-SQL2022": "fake-artifact"}, lambda url: archive(bundle)
        )
        self.assertIn("original 22-task profiling-enabled diagnostics", body)
        self.assertIn("| Row-by-row fetching |", body)
        bundle["fetch_measurements"]["latency"].update(
            status="incomplete", pairs=[], unavailable_reason="OFF build failed"
        )
        body = reporting.assess(
            evidence, {"Linux-SQL2022": "fake-artifact"}, lambda url: archive(bundle)
        )
        self.assertNotIn("Performance unavailable", body)
        self.assertIn("| Row-by-row fetching |", body)
        bundle.update(status="incomplete", pairs=[], unavailable_reason="Diagnostic worker failed")
        body = reporting.assess(
            evidence, {"Linux-SQL2022": "fake-artifact"}, lambda url: archive(bundle)
        )
        self.assertIn("Performance unavailable", body)
        self.assertIn("Diagnostic worker failed", body)
        bundle["head_commit"] = "f" * 40
        body = reporting.assess(
            evidence, {"Linux-SQL2022": "fake-artifact"}, lambda url: archive(bundle)
        )
        self.assertIn("Performance unavailable", body)
        self.assertNotIn("| Row-by-row fetching |", body)

    def test_ci_orchestration_counts_order_reuse_and_budget_exhaustion(self):
        from unittest.mock import patch

        for slow in (False, True):
            with self.subTest(slow=slow), tempfile.TemporaryDirectory() as directory:
                args = SimpleNamespace(
                    output=Path(directory),
                    mode="diagnostic",
                    base="a" * 40,
                    candidate="b" * 40,
                    reuse_candidate=True,
                    scenarios=None,
                    samples=5,
                    warmups=1,
                    leg="Linux-SQL2022",
                )
                clock, builds, workers, archives = [0], [], [], []
                bundle = self.sample_bundle(modern=True)
                samples = {
                    "diagnostic": bundle["pairs"][0],
                    **{
                        mode: report["pairs"][0]
                        for mode, report in bundle["fetch_measurements"].items()
                    },
                }

                def build(path, log, timeout, profiling=True):
                    builds.append((path, profiling))
                    clock[0] += 900 if slow else 1

                def measure(path, output, scenarios, timeout, **options):
                    mode = options["mode"]
                    side = "base" if options["revision"] == "a" * 40 else "candidate"
                    workers.append((mode, side, path, options["isolated"]))
                    duration = (360 if mode == "diagnostic" else 30) if slow else 1
                    clock[0] += min(timeout, duration)
                    if duration > timeout:
                        raise subprocess.TimeoutExpired("fake-worker", timeout)
                    return copy.deepcopy(samples[mode][side])

                with (
                    patch.object(controller.time, "monotonic", side_effect=lambda: clock[0]),
                    patch.object(
                        controller, "resolve_revisions", return_value=("a" * 40, "b" * 40)
                    ),
                    patch.object(controller, "git", return_value="b" * 40),
                    patch.object(
                        controller, "run_process", side_effect=lambda *a, **k: archives.append(a[0])
                    ),
                    patch.object(
                        controller, "python_source_identity", side_effect=self.source_anchor
                    ),
                    patch.object(controller, "build", side_effect=build),
                    patch.object(controller, "measure", side_effect=measure),
                    patch.dict(
                        os.environ,
                        {"BUILD_BUILDID": "42", "SYSTEM_PULLREQUEST_SOURCECOMMITID": "c" * 40},
                    ),
                ):
                    if slow:
                        with self.assertRaisesRegex(RuntimeError, "incomplete"):
                            controller.run_ci_report(args)
                    else:
                        controller.run_ci_report(args)
                result = json.loads((args.output / "report.json").read_text())
                self.assertEqual([flag for _, flag in builds], [False, False, True])
                self.assertEqual(len(archives), 3)
                self.assertTrue(all("--archive-source" in command for command in archives))
                self.assertTrue(all(isolated for _, _, _, isolated in workers))
                self.assertEqual(
                    [mode for mode, *_ in workers[:24]], ["latency"] * 12 + ["route"] * 12
                )
                for mode in ("latency", "route"):
                    selected = [(side, path) for kind, side, path, _ in workers if kind == mode]
                    self.assertEqual(
                        [side for side, _ in selected],
                        ["base", "candidate", "candidate", "base"] * 3,
                    )
                on_paths = {
                    (mode, side): path for mode, side, path, _ in workers if mode != "latency"
                }
                for side in ("base", "candidate"):
                    self.assertEqual(on_paths["route", side], on_paths["diagnostic", side])
                valid, reasons = reporting.ci_mode_reports(result)
                self.assertEqual(valid["latency"]["status"], "complete")
                self.assertEqual(valid["route"]["status"], "complete")
                self.assertEqual(result["status"], "incomplete" if slow else "complete")
                self.assertLessEqual(
                    clock[0], controller.BENCHMARK_TIMEOUT - controller.CI_FINISH_RESERVE
                )
                if slow:
                    self.assertIn("Timeout", reasons["diagnostic"])
                    self.assertLess(len(workers), 36)
                else:
                    self.assertEqual(len(workers), 36)
                    self.assertEqual(
                        sum(22 if mode == "diagnostic" else 6 for mode, *_ in workers), 408
                    )
                    self.assertTrue(all(len(report["pairs"]) == 5 for report in valid.values()))
                self.assertFalse((args.output / "report.tmp").exists())

    def test_ci_failures_do_not_retry_or_fabricate_siblings(self):
        from unittest.mock import patch

        for failure in (
            "off-build",
            "on-build",
            "latency",
            "route",
            "diagnostic",
            "identity",
            "exhausted",
        ):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                args = SimpleNamespace(
                    output=Path(directory),
                    mode="diagnostic",
                    base="a" * 40,
                    candidate="b" * 40,
                    reuse_candidate=True,
                    scenarios=None,
                    samples=5,
                    warmups=1,
                    leg="Linux-SQL2022",
                )
                clock, workers, builds = [0], [], []
                bundle = self.sample_bundle(modern=True)
                samples = {
                    "diagnostic": bundle["pairs"][0],
                    **{
                        mode: report["pairs"][0]
                        for mode, report in bundle["fetch_measurements"].items()
                    },
                }

                def build(path, log, timeout, profiling=True):
                    builds.append(path.name)
                    if failure == ("on-build" if profiling else "off-build"):
                        raise subprocess.CalledProcessError(1, "fake-build")
                    if failure == "exhausted":
                        clock[0] = controller.BENCHMARK_TIMEOUT

                def measure(path, output, scenarios, timeout, **options):
                    mode = options["mode"]
                    workers.append(mode)
                    if mode == failure:
                        raise subprocess.CalledProcessError(1, "fake-worker")
                    side = "base" if options["revision"] == "a" * 40 else "candidate"
                    result = copy.deepcopy(samples[mode][side])
                    if failure == "identity" and mode == "diagnostic":
                        result["provenance"]["native_sha256"] = "f" * 64
                    return result

                with (
                    patch.object(controller.time, "monotonic", side_effect=lambda: clock[0]),
                    patch.object(
                        controller, "resolve_revisions", return_value=("a" * 40, "b" * 40)
                    ),
                    patch.object(controller, "git", return_value="b" * 40),
                    patch.object(controller, "run_process"),
                    patch.object(
                        controller, "python_source_identity", side_effect=self.source_anchor
                    ),
                    patch.object(controller, "build", side_effect=build),
                    patch.object(controller, "measure", side_effect=measure),
                    patch.dict(
                        os.environ,
                        {"BUILD_BUILDID": "42", "SYSTEM_PULLREQUEST_SOURCECOMMITID": "c" * 40},
                    ),
                ):
                    with self.assertRaisesRegex(RuntimeError, "incomplete"):
                        controller.run_ci_report(args)
                result = json.loads((args.output / "report.json").read_text())
                valid, reasons = reporting.ci_mode_reports(result)
                self.assertTrue(reasons)
                self.assertEqual(len(builds), len(set(builds)))
                self.assertLessEqual(len(workers), 36)
                if failure in ("latency", "route", "diagnostic"):
                    self.assertEqual(workers.count(failure), 1)
                if failure in ("off-build", "latency"):
                    self.assertEqual(valid["route"]["status"], "complete")
                    self.assertEqual(valid["diagnostic"]["status"], "complete")
                elif failure in ("on-build", "route", "diagnostic", "identity"):
                    self.assertEqual(valid["latency"]["status"], "complete")
                if failure == "on-build":
                    self.assertNotIn("diagnostic", workers)
                    self.assertIn("Shared ON build unavailable", reasons["diagnostic"])
                if failure == "exhausted":
                    self.assertEqual(workers, [])
                for mode, reason in reasons.items():
                    self.assertTrue(reason)
                    self.assertEqual(valid[mode]["status"], "incomplete")

    def test_ci_atomic_write_preserves_last_report_and_cleanup_aborts(self):
        from unittest.mock import patch

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "report.json"
            controller.write_report(output, {"status": "incomplete"})
            original = output.read_bytes()
            with self.assertRaises(ValueError):
                controller.write_report(output, {"wall_ms": float("nan")})
            self.assertEqual(output.read_bytes(), original)
            process = MagicMock()
            process.wait.side_effect = subprocess.TimeoutExpired("fake-process", 1)
            with (
                patch.object(controller.subprocess, "Popen", return_value=process),
                patch.object(
                    controller, "terminate_process_tree", side_effect=OSError("cannot reap")
                ) as reap,
            ):
                with self.assertRaises(controller.ProcessCleanupError):
                    controller.run_process(["fake-worker"], root / "worker.log", 1)
                reap.assert_called_once_with(process)
            args = SimpleNamespace(
                output=root,
                mode="diagnostic",
                base="a" * 40,
                candidate="b" * 40,
                reuse_candidate=True,
                scenarios=None,
                samples=5,
                warmups=1,
                leg="Linux-SQL2022",
            )
            retained = root / "retained-build-root"
            retained.mkdir()
            with (
                patch.object(controller, "resolve_revisions", return_value=("a" * 40, "b" * 40)),
                patch.object(controller, "git", return_value="b" * 40),
                patch.object(controller.tempfile, "mkdtemp", return_value=str(retained)),
                patch.object(
                    controller,
                    "run_process",
                    side_effect=controller.ProcessCleanupError("cannot reap"),
                ) as launch,
                patch.object(controller.shutil, "rmtree") as remove,
                patch.dict(os.environ, {"SYSTEM_PULLREQUEST_SOURCECOMMITID": "c" * 40}),
            ):
                with self.assertRaises(controller.ProcessCleanupError):
                    controller.run_ci_report(args)
                remove.assert_not_called()
                self.assertEqual(launch.call_count, 1)
            result = json.loads(output.read_text())
            self.assertEqual(result["cleanup_required"], str(retained))
            self.assertEqual(result["fetch_measurements"]["latency"]["status"], "incomplete")
            self.assertIn("cleanup", result["fetch_measurements"]["route"]["unavailable_reason"])

    def test_ci_source_wiring_keeps_matrix_deadlines_and_trust(self):
        pipeline = (ROOT / "eng/pipelines/pr-validation-pipeline.yml").read_text(encoding="utf-8")
        self.assertEqual(pipeline.count("--ci-report"), 1)
        linux = pipeline.split("- job: PytestOnLinux\n", 1)[1].split("\n- job:", 1)[0]
        self.assertIn('--reuse-candidate --ci-report --leg "$(profilerLeg)"', linux)
        self.assertIn("timeoutInMinutes: 100", linux)
        self.assertIn("timeoutInMinutes: 160", linux)
        self.assertIn("artifact: profiler-$(profilerLeg)", linux)
        self.assertIn("eq(variables['distroName'], 'Ubuntu-SQL2025')", linux)
        windows = pipeline.split("# Hosted Windows timings", 1)[1].split("- job:", 1)[0]
        self.assertIn("condition: false", windows)
        self.assertNotIn("--ci-report", windows)
        self.assertEqual(
            (
                controller.BENCHMARK_TIMEOUT,
                controller.LOCAL_BENCHMARK_TIMEOUT,
                controller.WORKER_TIMEOUT,
                controller.FETCH_WORKER_TIMEOUT,
            ),
            (5400, 6300, 360, 30),
        )
        workflow = (ROOT / ".github/workflows/pr-profiler-report.yml").read_text(encoding="utf-8")
        self.assertIn("timeout-minutes: 230", workflow)
        self.assertIn("github.event.pull_request.base.sha", workflow)

    def test_ci_diagnostic_worker_attests_actual_binary_after_cleanup(self):
        from unittest.mock import patch

        for fails in (False, True):
            with self.subTest(cleanup_failure=fails), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                source = root / "mssql_python" / "cursor.py"
                source.parent.mkdir()
                source.write_bytes((ROOT / "mssql_python" / "cursor.py").read_bytes())
                cursor_module = SimpleNamespace(__file__=str(source))
                binary = root / "native.so"
                binary.write_bytes(b"fake-not-imported")
                native = SimpleNamespace(
                    module=SimpleNamespace(__file__=str(binary)), DDBCSQLFetchRow=object()
                )
                manager = MagicMock()
                profiler = manager.__enter__.return_value
                profiler.run.return_value = [
                    dict(
                        wall_ms=1,
                        cpp={"ddbc::query": dict(calls=1, total_us=1, min_us=1, max_us=1)},
                        py={},
                        detail="Rows: 1",
                    )
                ]
                profiler._conn.cursor.return_value.__enter__.return_value.fetchone.return_value = (
                    "16.0",
                )
                if fails:
                    manager.__exit__.side_effect = RuntimeError("profiler cleanup failed")
                core = SimpleNamespace(Profiler=MagicMock(return_value=manager))
                suite = SimpleNamespace(registry=lambda: {"connect": (object(), False)})
                args = SimpleNamespace(
                    source_root=root,
                    mode="diagnostic",
                    revision="b" * 40,
                    scenarios=None,
                    output=root / "diagnostic.json",
                )
                with (
                    patch.dict(
                        sys.modules,
                        {
                            "mssql_python": SimpleNamespace(
                                ddbc_bindings=native, cursor=cursor_module
                            ),
                            "mssql_python.cursor": cursor_module,
                        },
                    ),
                    patch.object(controller, "check_build") as check,
                    patch.object(controller.platform, "system", return_value="Linux"),
                    patch.object(controller.platform, "machine", return_value="x86_64"),
                    patch.object(controller.platform, "python_version", return_value="3.13.7"),
                    patch.object(controller, "load_suite", return_value=(core, suite)),
                ):
                    if fails:
                        with self.assertRaisesRegex(RuntimeError, "profiler cleanup failed"):
                            controller.worker(args)
                    else:
                        controller.worker(args)
                check.assert_called_once_with(root, profiling=True)
                sample = json.loads(args.output.read_text())
                self.assertEqual(sample["status"], "running" if fails else "complete")
                if not fails:
                    self.assertEqual(sample["mode"], "diagnostic")
                    self.assertEqual(sample["provenance"]["native_file"], str(binary.resolve()))
                    self.assertEqual(sample["provenance"]["source_commit"], "b" * 40)
                    self.assertTrue(sample["provenance"]["native_profiling"])
                    self.assertEqual(len(sample["provenance"]["native_sha256"]), 64)
                manager.__exit__.assert_called_once()

    def test_ci_rejects_scope_changes_before_work(self):
        from unittest.mock import patch

        for change in (
            {"samples": 3},
            {"warmups": 2},
            {"scenarios": ["numeric_fetchone"]},
            {"mode": "route"},
            {"reuse_candidate": False},
        ):
            args = SimpleNamespace(
                samples=5, warmups=1, scenarios=None, mode="diagnostic", reuse_candidate=True
            )
            args.__dict__.update(change)
            with (
                self.subTest(change=change),
                patch.object(controller, "resolve_revisions") as resolve,
            ):
                with self.assertRaises(ValueError):
                    controller.run_ci_report(args)
                resolve.assert_not_called()

        command = [
            "controller",
            "--reuse-candidate",
            "--ci-report",
            "--leg",
            "Linux-SQL2022",
            "--output",
            "unused-fake-output",
        ]
        with (
            patch.object(sys, "argv", command),
            patch.object(controller, "run_ci_report") as run,
            patch.object(controller, "run") as legacy,
        ):
            controller.main()
            self.assertTrue(run.call_args.args[0].ci_report)
            legacy.assert_not_called()
        with (
            patch.object(sys, "argv", command + ["--worker"]),
            patch.object(controller, "worker") as worker,
        ):
            with self.assertRaises(SystemExit):
                controller.main()
            worker.assert_not_called()

    def test_ci_reused_source_and_filesystem_cleanup_fail_closed(self):
        from unittest.mock import patch

        with patch.object(controller, "git", return_value="b" * 40) as git:
            controller.verify_reused_source(controller.ROOT, "b" * 40)
            self.assertEqual(git.call_count, 2)
            self.assertIn("--exit-code", git.call_args.args)
        with patch.object(controller, "git", return_value="a" * 40):
            with self.assertRaisesRegex(ValueError, "revision changed"):
                controller.verify_reused_source(controller.ROOT, "b" * 40)
        with patch.object(
            controller, "git", side_effect=["b" * 40, subprocess.CalledProcessError(1, "git diff")]
        ):
            with self.assertRaises(subprocess.CalledProcessError):
                controller.verify_reused_source(controller.ROOT, "b" * 40)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            retained = root / "build-root"
            retained.mkdir()
            args = SimpleNamespace(
                output=root,
                mode="diagnostic",
                base="a" * 40,
                candidate="b" * 40,
                reuse_candidate=True,
                scenarios=None,
                samples=5,
                warmups=1,
                leg="Linux-SQL2022",
            )
            with (
                patch.object(controller, "resolve_revisions", return_value=("a" * 40, "b" * 40)),
                patch.object(controller, "git", return_value="b" * 40),
                patch.object(controller.tempfile, "mkdtemp", return_value=str(retained)),
                patch.object(
                    controller,
                    "run_process",
                    side_effect=subprocess.CalledProcessError(1, "fake-archive"),
                ),
                patch.object(
                    controller.shutil, "rmtree", side_effect=OSError("cannot remove root")
                ),
                patch.dict(os.environ, {"SYSTEM_PULLREQUEST_SOURCECOMMITID": "c" * 40}),
            ):
                with self.assertRaisesRegex(
                    controller.ProcessCleanupError, "build-directory cleanup"
                ):
                    controller.run_ci_report(args)
            result = json.loads((root / "report.json").read_text())
            self.assertEqual(result["status"], "incomplete")
            self.assertEqual(result["cleanup_required"], str(retained))
            self.assertIn("cleanup failed", result["unavailable_reason"])

    def test_ci_existing_publisher_restores_diagnostics_without_sibling_fallback(self):
        from unittest.mock import patch

        for modern, diagnostic_available in (
            (False, True),
            (False, False),
            (True, True),
            (True, False),
        ):
            with self.subTest(modern=modern, diagnostic_available=diagnostic_available):
                bundle = self.sample_bundle(modern=modern)
                if not diagnostic_available:
                    bundle.update(
                        status="incomplete", pairs=[], unavailable_reason="Diagnostic worker failed"
                    )
                data = {}
                for leg in reporting.LEGS:
                    value = set_leg(bundle, leg)
                    value["fetch_measurements"] = {
                        mode: set_leg(child, leg)
                        for mode, child in value["fetch_measurements"].items()
                    }
                    data[leg] = zip_data([("report.json", json.dumps(value))])
                artifacts = [
                    {
                        "name": "profiler-" + leg,
                        "resource": {"downloadUrl": "https://dev.azure.com/" + leg},
                    }
                    for leg in data
                ]
                posted, clock = [], [0]
                with (
                    patch.object(
                        publisher,
                        "publish",
                        side_effect=lambda number, head, body, base=None: posted.append(body),
                    ),
                    patch.object(publisher, "github", side_effect=pr_topology()),
                    patch.object(
                        publisher,
                        "api",
                        side_effect=lambda url: {
                            "value": artifacts if "/artifacts?" in url else [ado_build()]
                        },
                    ),
                    patch.object(
                        publisher,
                        "fetch",
                        side_effect=lambda url, **kwargs: data[url.rsplit("/", 1)[-1]],
                    ),
                    patch.object(publisher.time, "monotonic", side_effect=lambda: clock[0]),
                    patch.object(
                        publisher.time,
                        "sleep",
                        side_effect=lambda seconds: clock.__setitem__(0, clock[0] + seconds),
                    ),
                ):
                    publisher.run(123, "c" * 40, 4)
                self.assertEqual(len(posted), 2)
                body = posted[-1]
                self.assertEqual(body.count(reporting.MARKER), 1)
                self.assertEqual(body.count("## PR Performance Report"), 1)
                self.assertIn("original 22-task profiling-enabled diagnostics", body)
                self.assertIn("not headline inputs", body)
                self.assertIn("when available", body)
                self.assertNotIn("Primary verdict:", body)
                self.assertEqual("Performance unavailable" in body, not diagnostic_available)
                if not diagnostic_available:
                    self.assertIn("Diagnostic worker failed", body)
                    self.assertNotIn("Performance improved", body)

    def test_ci_finalization_deadline_is_checked_after_validation_and_write(self):
        from unittest.mock import patch

        for delayed in (None, "validation", "write"):
            with self.subTest(delayed=delayed), tempfile.TemporaryDirectory() as directory:
                args = SimpleNamespace(
                    output=Path(directory),
                    mode="diagnostic",
                    base="a" * 40,
                    candidate="b" * 40,
                    reuse_candidate=True,
                    scenarios=None,
                    samples=5,
                    warmups=1,
                    leg="Linux-SQL2022",
                )
                bundle = self.sample_bundle(modern=True)
                samples = {
                    "diagnostic": bundle["pairs"][0],
                    **{
                        mode: report["pairs"][0]
                        for mode, report in bundle["fetch_measurements"].items()
                    },
                }
                clock, cleaned, final_writes = [0], [False], [0]
                original_remove = controller.shutil.rmtree
                original_validate = controller.ci_mode_reports
                original_write = controller.write_report

                def remove(path):
                    original_remove(path)
                    cleaned[0] = True

                def validate(value):
                    result = original_validate(value)
                    if cleaned[0] and delayed == "validation":
                        clock[0] = controller.BENCHMARK_TIMEOUT + 1
                    return result

                def write(path, value):
                    original_write(path, value)
                    if cleaned[0]:
                        final_writes[0] += 1
                        if delayed == "write":
                            clock[0] = controller.BENCHMARK_TIMEOUT + final_writes[0]
                        elif delayed is None:
                            clock[0] = controller.BENCHMARK_TIMEOUT

                def measure(path, output, scenarios, timeout, **options):
                    side = "base" if options["revision"] == "a" * 40 else "candidate"
                    return copy.deepcopy(samples[options["mode"]][side])

                error = None
                with (
                    patch.object(controller.time, "monotonic", side_effect=lambda: clock[0]),
                    patch.object(
                        controller, "resolve_revisions", return_value=("a" * 40, "b" * 40)
                    ),
                    patch.object(controller, "git", return_value="b" * 40),
                    patch.object(controller, "run_process"),
                    patch.object(
                        controller, "python_source_identity", side_effect=self.source_anchor
                    ),
                    patch.object(controller, "build"),
                    patch.object(controller, "measure", side_effect=measure),
                    patch.object(controller.shutil, "rmtree", side_effect=remove),
                    patch.object(controller, "ci_mode_reports", side_effect=validate),
                    patch.object(controller, "write_report", side_effect=write),
                    patch.dict(
                        os.environ,
                        {"BUILD_BUILDID": "42", "SYSTEM_PULLREQUEST_SOURCECOMMITID": "c" * 40},
                    ),
                ):
                    try:
                        controller.run_ci_report(args)
                    except RuntimeError as failure:
                        error = str(failure)
                saved = json.loads((args.output / "report.json").read_text())
                self.assertTrue(cleaned[0])
                self.assertEqual(final_writes[0], 2 if delayed else 1)
                self.assertEqual(saved["status"], "incomplete" if delayed else "complete")
                self.assertEqual(len(saved["pairs"]), 5)
                for mode in ("latency", "route"):
                    self.assertEqual(saved["fetch_measurements"][mode]["status"], "complete")
                    self.assertEqual(len(saved["fetch_measurements"][mode]["pairs"]), 5)
                if delayed:
                    self.assertIsNotNone(error)
                    self.assertIn("incomplete", error)
                    self.assertIn("finish deadline", saved["unavailable_reason"])
                else:
                    self.assertIsNone(error)

    def test_route_source_recognizer_exact_bytes_and_rejects_drift(self):
        import ast

        source = (ROOT / "mssql_python" / "cursor.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        declaration = next(
            node
            for node in tree.body
            if isinstance(node, ast.Assign)
            and any(
                isinstance(t, ast.Name) and t.id == "_DEFAULT_NATIVE_ROW_ROUTE"
                for t in node.targets
            )
        )
        literal = ast.get_source_segment(source, declaration)
        route = {"version": 1, "methods": {"fetchone": False, "fetchmany": True, "fetchval": False}}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "mssql_python" / "cursor.py"
            target.parent.mkdir()
            hashes = []
            for text in (source, source.replace("\n", "\r\n"), "\n\n" + source):
                target.write_bytes(text.encode())
                observed = controller.python_source_identity(root, "b" * 40)
                self.assertEqual(observed["row_route"], route)
                hashes.append(observed["python_cursor_sha256"])
            self.assertEqual(hashes[0], hashes[1])
            self.assertNotEqual(hashes[0], hashes[2])
            mutations = [
                source.replace(literal, ""),
                source + "\n" + literal,
                source.replace(literal, "_DEFAULT_NATIVE_ROW_ROUTE: dict = " + repr(route)),
                source.replace(literal, "ALIAS = _DEFAULT_NATIVE_ROW_ROUTE = " + repr(route)),
                source.replace(literal, "_DEFAULT_NATIVE_ROW_ROUTE = dict(version=1)"),
                source.replace(
                    "    def fetchone(", "    @unreviewed_dispatch_wrapper\n    def fetchone("
                ),
                source.replace("    def fetchmany(", "    @staticmethod\n    def fetchmany("),
                source.replace("    def fetchval(", "    @property\n    def fetchval("),
                source.replace("    def fetchone(", "    async def fetchone("),
                source.replace(
                    "    def fetchone(", "    def fetchone(self): pass\n\n    def fetchone("
                ),
                source + "\nclass Cursor: pass\n",
                source.replace("class Cursor:", "@unreviewed_dispatch_wrapper\nclass Cursor:"),
                source.replace(
                    "        Fetch the next row of a query result set.",
                    "        Unknown changed method.",
                ),
                source + "\nprobe = _DEFAULT_NATIVE_ROW_ROUTE\n",
                "invalid Python syntax !",
            ]
            for bad in (
                {"version": True, "methods": route["methods"]},
                {"version": 2, "methods": route["methods"]},
                {"methods": route["methods"]},
                {"version": 1, "methods": {"fetchone": 0, "fetchmany": True, "fetchval": False}},
                {"version": 1, "methods": {"fetchone": True, "fetchmany": True, "fetchval": True}},
                {"version": 1, "methods": {"fetchone": False, "fetchmany": True}},
            ):
                mutations.append(
                    source.replace(literal, "_DEFAULT_NATIVE_ROW_ROUTE = " + repr(bad))
                )
            mutations.append(
                source.replace(
                    literal,
                    "_DEFAULT_NATIVE_ROW_ROUTE = {'version':1,'version':1,'methods':"
                    + repr(route["methods"])
                    + "}",
                )
            )
            mutations.append(
                source.replace(
                    literal,
                    "_DEFAULT_NATIVE_ROW_ROUTE = {'version':1,'methods':{'fetchone':False,'fetchone':False,'fetchmany':True,'fetchval':False}}",
                )
            )
            cursor_class = next(
                node
                for node in tree.body
                if isinstance(node, ast.ClassDef) and node.name == "Cursor"
            )
            lines = source.splitlines(keepends=True)
            for method in ("fetchone", "fetchmany", "fetchval"):
                for binding in (
                    f"{method} = replacement",
                    f"{method}: object = replacement",
                    f"{method}: object",
                    f"alias = {method} = replacement",
                    f"{method}, alias = replacements",
                    f"{method} += replacement",
                    f"del {method}",
                    f"import replacement as {method}",
                    f"from replacement import value as {method}",
                    f"from replacement import {method}",
                    f"for {method} in replacements: pass",
                    f"with replacement as {method}: pass",
                    f"if flag: {method} = replacement",
                    f"({method} := replacement)",
                    f"def helper(self, value=({method} := replacement)): pass",
                    f"def helper(self) -> ({method} := replacement): pass",
                    f"async def helper(self) -> ({method} := replacement): pass",
                    f"def helper(self, value: ({method} := replacement)): pass",
                    f"async def helper(self, value: ({method} := replacement)): pass",
                    f"helper = lambda value=({method} := replacement): value",
                    f"class Helper(({method} := replacement)): pass",
                    f"try: pass\nexcept Exception as {method}: pass",
                    f"match replacement:\n    case {{'value': {method}}}: pass",
                    f"class {method}: pass",
                    f"if flag:\n    def {method}(self): pass",
                ):
                    insertion = (
                        "\n" + "\n".join("    " + line for line in binding.splitlines()) + "\n"
                    )
                    mutations.append(
                        "".join(lines[: cursor_class.end_lineno])
                        + insertion
                        + "".join(lines[cursor_class.end_lineno :])
                    )
            for index, text in enumerate(mutations):
                with self.subTest(mutation=index):
                    self.assertNotEqual(text, source)
                    target.write_bytes(text.encode())
                    with self.assertRaises(ValueError):
                        controller.python_source_identity(root, "b" * 40)

    def test_versioned_route_standalone_and_fully_populated_bundles(self):
        for modern in (False, True):
            bundle = self.sample_bundle(modern=modern)
            for mode in ("latency", "route"):
                report = bundle["fetch_measurements"][mode]
                reporting.validate(report)
                self.assertIn("Python phases OFF", reporting.render([report], "c" * 40, 42))
            reporting.validate(bundle)
            valid, errors = reporting.ci_mode_reports(bundle)
            self.assertEqual(set(valid), set(reporting.MODES))
            self.assertEqual(errors, {})
            body = reporting.render_ci_reports([bundle], "c" * 40, 42)
            self.assertIn("original 22-task profiling-enabled diagnostics", body)
            route = valid["route"]
            for method in ("fetchone", "fetchmany", "fetchval"):
                for side in ("base", "candidate"):
                    identity = route["pairs"][0][side]["provenance"]
                    expected = (
                        1000 if side == "candidate" and (not modern or method == "fetchmany") else 0
                    )
                    self.assertEqual(reporting.expected_constructors(identity, method), expected)
            if modern:
                self.assertIn(
                    "binding=True, default fetchone native Row=False",
                    reporting.route_description(route, "fetchone"),
                )
            for policy in ((True, True, True), (False, True, False)) if modern else ():
                # A future base chooses its own policy; candidate role does not select it.
                future = copy.deepcopy(bundle)
                for mode in reporting.MODES:
                    report = future if mode == "diagnostic" else future["fetch_measurements"][mode]
                    anchor = report["python_sources"]["base"]
                    anchor["row_route"]["methods"] = dict(
                        zip(("fetchone", "fetchmany", "fetchval"), policy)
                    )
                    for pair in report["pairs"]:
                        sample = pair["base"]
                        sample["provenance"].update(copy.deepcopy(anchor), guarded_row=True)
                        if mode == "route":
                            for name, case in sample["scenarios"].items():
                                if anchor["row_route"]["methods"][name.split("_")[1]]:
                                    case["cpp"]["ddbc::FetchRow::construct_row"] = dict(
                                        calls=1000, total_us=10000, min_us=1, max_us=100
                                    )
                self.assertEqual(reporting.ci_mode_reports(future)[1], {})
                self.assertIn(
                    "original 22-task profiling-enabled diagnostics",
                    reporting.render_ci_reports([future], "c" * 40, 42),
                )

    def test_versioned_route_rejects_bad_identity_policy_counts_and_partial_data(self):
        for mutation in (
            lambda r: r.pop("python_sources"),
            lambda r: r["python_sources"].pop("base"),
            lambda r: r["python_sources"]["candidate"].update(source_commit="f" * 40),
            lambda r: r["python_sources"]["candidate"].update(python_cursor_sha256="bad"),
            lambda r: r["pairs"][0]["candidate"]["provenance"].pop("row_route"),
            lambda r: r["pairs"][0]["candidate"]["provenance"].pop("python_cursor_sha256"),
            lambda r: r["pairs"][0]["candidate"]["provenance"].update(
                python_cursor_sha256="f" * 64
            ),
            lambda r: r["pairs"][0]["candidate"]["provenance"].update(guarded_row=False),
            lambda r: r["pairs"][0]["candidate"]["provenance"]["row_route"].update(version=True),
            lambda r: r["pairs"][0]["candidate"]["provenance"]["row_route"].update(version=2),
            lambda r: r["pairs"][0]["candidate"]["provenance"]["row_route"].pop("version"),
            lambda r: r["pairs"][0]["candidate"]["provenance"]["row_route"]["methods"].pop(
                "fetchval"
            ),
            lambda r: r["pairs"][0]["candidate"]["provenance"]["row_route"]["methods"].update(
                fetchone=0
            ),
            lambda r: r["pairs"][0]["candidate"]["provenance"]["row_route"]["methods"].update(
                fetchval=True
            ),
            lambda r: r["pairs"][0]["candidate"]["scenarios"]["numeric_fetchone"]["cpp"].update(
                {"ddbc::FetchRow::construct_row": dict(calls=1000)}
            ),
            lambda r: r["pairs"][0]["candidate"]["scenarios"]["mixed_fetchmany"]["cpp"].pop(
                "ddbc::FetchRow::construct_row"
            ),
            lambda r: r["pairs"][0]["candidate"]["scenarios"]["mixed_fetchone"].update(cpp={}),
            lambda r: r["pairs"][0]["candidate"].update(status="running"),
            lambda r: r["pairs"][1]["candidate"]["provenance"].update(native_sha256="f" * 64),
        ):
            for partial in (False, True):
                with self.subTest(mutation=mutation, partial=partial):
                    report = self.new_sample_report("route")
                    if partial:
                        report.update(
                            status="incomplete",
                            pairs=report["pairs"][:2],
                            unavailable_reason="Deadline before next pair",
                        )
                    mutation(report)
                    with self.assertRaises((ValueError, KeyError)):
                        reporting.validate(report)
        report = self.new_sample_report("route")
        report.update(
            status="incomplete",
            pairs=report["pairs"][:2],
            unavailable_reason="Deadline before next pair",
        )
        reporting.validate(report)
        self.assertIn("Performance unavailable", reporting.render([report], "c" * 40, 42))

    def test_versioned_route_partial_siblings_and_off_on_identity(self):
        for defect in (
            "native",
            "python",
            "missing-anchor",
            "diagnostic-anchor",
            "off-environment",
        ):
            bundle = self.sample_bundle(modern=True)
            route = bundle["fetch_measurements"]["route"]
            route.update(
                status="incomplete", pairs=route["pairs"][:2], unavailable_reason="Deadline"
            )
            if defect == "native":
                for pair in route["pairs"]:
                    pair["candidate"]["provenance"]["native_sha256"] = "f" * 64
            elif defect == "missing-anchor":
                route.pop("python_sources")
            elif defect == "diagnostic-anchor":
                bundle["python_sources"]["candidate"]["python_cursor_sha256"] = "f" * 64
            elif defect == "off-environment":
                for item in (route, bundle):
                    for pair in item["pairs"]:
                        for sample in pair.values():
                            sample["environment"]["python"] = "3.12.0"
            else:
                route["python_sources"]["candidate"]["python_cursor_sha256"] = "f" * 64
                for pair in route["pairs"]:
                    pair["candidate"]["provenance"]["python_cursor_sha256"] = "f" * 64
            valid, errors = reporting.ci_mode_reports(bundle)
            self.assertIn("latency", valid)
            self.assertIn("diagnostic" if defect == "diagnostic-anchor" else "route", errors)
            self.assertIn(
                "original 22-task profiling-enabled diagnostics",
                reporting.render_ci_reports([bundle], "c" * 40, 42),
            )

    def test_versioned_controller_rejects_warmup_drift_and_keeps_subset_incomplete(self):
        from unittest.mock import patch

        for defect in (None, "warmup", "native-drift", "source-drift", "subset"):
            with self.subTest(defect=defect), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                args = SimpleNamespace(
                    base="a" * 40,
                    candidate="b" * 40,
                    output=root,
                    mode="route",
                    reuse_candidate=False,
                    leg="Linux-SQL2022",
                    samples=3,
                    warmups=1,
                    scenarios=["mixed_fetchmany"] if defect == "subset" else None,
                )
                calls = []
                samples = self.new_sample_report("route")["pairs"][0]

                def measure(path, output, scenarios, timeout, **options):
                    result = copy.deepcopy(samples[path.name])
                    calls.append(path.name)
                    if defect == "warmup" and len(calls) == 1:
                        result["scenarios"]["mixed_fetchmany"]["cpp"] = {}
                    if len(calls) > 2 and path.name == "base":
                        if defect == "native-drift":
                            result["provenance"]["native_sha256"] = "f" * 64
                        if defect == "source-drift":
                            result["provenance"]["python_cursor_sha256"] = "f" * 64
                    if scenarios:
                        result["scenarios"] = {
                            name: result["scenarios"][name] for name in scenarios
                        }
                    return result

                with (
                    patch.object(
                        controller, "resolve_revisions", return_value=("a" * 40, "b" * 40)
                    ),
                    patch.object(controller, "checkout"),
                    patch.object(controller, "build"),
                    patch.object(
                        controller, "python_source_identity", side_effect=self.source_anchor
                    ),
                    patch.object(controller, "measure", side_effect=measure),
                ):
                    if defect in (None, "subset"):
                        controller.run(args)
                    else:
                        with self.assertRaises(ValueError):
                            controller.run(args)
                saved = json.loads((root / "report.json").read_text())
                self.assertEqual(saved["status"], "complete" if defect is None else "incomplete")
                if defect not in (None, "subset"):
                    self.assertEqual(saved["pairs"], [])
                    self.assertLessEqual(len(calls), 4)
                if defect == "subset":
                    self.assertEqual(len(saved["pairs"]), 3)
                    self.assertEqual(
                        set(saved["pairs"][0]["base"]["scenarios"]), {"mixed_fetchmany"}
                    )
                    with self.assertRaises(ValueError):
                        reporting.validate(saved)
