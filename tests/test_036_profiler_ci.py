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


def pr_topology(head="c" * 40, base="a" * 40, merge_base=None):
    def response(path):
        if path.startswith("pulls/"):
            return {"state": "open", "head": {"sha": head}, "base": {"sha": base}}
        if path.startswith("git/commits/"):
            commit_sha = path.removeprefix("git/commits/")
            source = commit_sha == "b" * 40
            return {
                "sha": commit_sha,
                "parents": [{"sha": merge_base or base}, {"sha": head}] if source else [],
                "tree": {"sha": ("d" if source else "e") * 40},
            }
        if path.startswith("git/trees/"):
            tree_sha = path.removeprefix("git/trees/").split("?", 1)[0]
            return {
                "sha": tree_sha,
                "truncated": False,
                "tree": [
                    {
                        "path": file.relative_to(ROOT).as_posix(),
                        "type": "blob",
                        "sha": f"{index + 1:040x}",
                    }
                    for index, file in enumerate(reporting.suite_paths(ROOT))
                ],
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
        suite_hash="d" * 64,
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
    assert "20 consistent slowdown signals" in body
    assert "| Unix / SQL Server 2022 | Connection opening |" in body
    assert "| Windows / SQL Server 2022 | No result available" in body
    assert body.index("consistent slowdown signals") < body.index(
        "<summary>Build, commits and measurement details</summary>"
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
        ("suite_hash", "e" * 64),
    ],
)
def test_standalone_report_rejects_mixed_provenance(report, tmp_path, monkeypatch, key, value):
    first = tmp_path / "linux.json"
    second = tmp_path / "windows.json"
    first.write_text(json.dumps(report), encoding="utf-8")
    other = set_leg(report, "Windows-SQL2022")
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
    assert "60 diagnostic rows are available in the raw ADO artifacts" in body
    assert "<summary>All database tasks and timings</summary>" in body
    assert "<summary>Build, commits and measurement details</summary>" in body


@pytest.mark.parametrize("invalid", ["source commit", "base commit", "source tree"])
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
        source_tree={"sha": "d" * 40, "truncated": False, "tree": []},
        base_tree={"sha": "e" * 40, "truncated": False, "tree": []},
        trusted_root=ROOT,
    )
    if invalid == "source commit":
        evidence.merge_commit["sha"] = "f" * 40
    elif invalid == "base commit":
        evidence.base_commit["sha"] = "f" * 40
    else:
        evidence = reporting.AssessmentEvidence(**{**evidence.__dict__, "source_tree": []})
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
    assert (
        "**Row-by-row fetching was slower on Unix / SQL Server 2022, "
        "but the repeated comparisons were inconsistent.**"
    ) in noisy
    assert "Inconsistent slowdowns to review:" in noisy

    complete = [set_leg(clear_slowdowns(copy.deepcopy(report)), leg) for leg in reporting.LEGS]
    clean_body = reporting.render(complete, "c" * 40, 42)
    assert "**No consistent slowdowns detected across all 3 environments.**" in clean_body
    assert "**Coverage:** 3 of 3 environments completed." in clean_body


def test_impact_summary_handles_single_regression_partial_and_no_results(report):
    single = clear_slowdowns(copy.deepcopy(report))
    for pair in single["pairs"]:
        pair["candidate"]["scenarios"]["fetchone"]["wall_ms"] *= 1.3
    body = reporting.render([single], "c" * 40, 42)
    assert (
        "**This PR consistently slows row-by-row fetching on Unix / SQL Server 2022 " "by 30.0%.**"
    ) in body
    assert "<summary>Affected phases and call counts</summary>" in body
    assert "<summary>All database tasks and timings</summary>" in body
    assert "<summary>Build, commits and measurement details</summary>" in body
    assert "median of paired before-and-after ratios" in body

    partial = reporting.render(
        [clear_slowdowns(copy.deepcopy(report))],
        "c" * 40,
        42,
        ["Windows-SQL2022 (missing)"],
    )
    assert "No consistent slowdowns in the 1 completed environment." in partial
    assert "No result is available for 2 environments." in partial
    assert "| Windows / SQL Server 2022 | No result available (missing) |" in partial
    assert "pending" not in partial.lower()

    unavailable = reporting.render([], "c" * 40, 42, ["Linux-SQL2022 (invalid artifact)"])
    assert "Performance could not be assessed" in unavailable
    assert "No consistent slowdowns" not in unavailable


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
        return {"state": "open", "head": {"sha": "new-head"}}

    monkeypatch.setattr(publisher, "github", api)
    publisher.publish(123, "old-head", "anything")
    assert len(calls) == 1 and calls[0][1] == {}


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
    assert ROOT / "eng/profiler_benchmarks/__init__.py" in reporting.suite_paths(ROOT)
    assert ROOT / "eng/profiler_benchmarks/report.py" in reporting.suite_paths(ROOT)
    assert ROOT / "eng/pipelines/pr-validation-pipeline.yml" in reporting.suite_paths(ROOT)
    assert ROOT / "eng/scripts/setup_sql_container.py" in reporting.suite_paths(ROOT)
    assert ROOT / "requirements.txt" in reporting.suite_paths(ROOT)


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


def test_suite_blobs_require_complete_authenticated_tree():
    expected = [path.relative_to(ROOT).as_posix() for path in reporting.suite_paths(ROOT)]
    tree = {
        "truncated": False,
        "tree": [
            {"path": path, "type": "blob", "sha": f"{index + 1:040x}"}
            for index, path in enumerate(expected)
        ],
    }
    assert set(reporting.suite_blobs(tree, ROOT)) == set(expected)
    tree["tree"].pop()
    with pytest.raises(ValueError, match="missing"):
        reporting.suite_blobs(tree, ROOT)
    tree["tree"].append(None)
    with pytest.raises(ValueError, match="Incomplete"):
        reporting.suite_blobs(tree, ROOT)


def test_publisher_finishes_unavailable_when_checked_suite_file_moves(monkeypatch):
    posted = []
    build = ado_build()
    monkeypatch.setattr(
        publisher, "publish", lambda number, head, body, base=None: posted.append(body)
    )
    monkeypatch.setattr(publisher, "github", pr_topology())
    artifacts = [
        {"name": "profiler-" + leg, "resource": {"downloadUrl": "https://dev.azure.com/" + leg}}
        for leg in reporting.LEGS
    ]
    monkeypatch.setattr(
        publisher,
        "api",
        lambda url: {"value": artifacts if "/artifacts?" in url else [build]},
    )
    monkeypatch.setattr(
        reporting,
        "suite_blobs",
        MagicMock(side_effect=ValueError("Benchmark suite missing from commit tree")),
    )
    publisher.run(123, "c" * 40, 1)
    assert len(posted) == 2
    assert "Performance assessment pending" in posted[0]
    assert "Performance could not be assessed" in posted[1]
    assert "required file changed" in posted[1]


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
    for job in ("pytestonwindows", "PytestOnLinux"):
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


@pytest.mark.parametrize(
    "corrupt",
    [
        None,
        "zip",
        "timeout",
        "scenarios",
        "suite",
        "source",
        "base",
        "provenance",
        "recursion",
        "deflate",
        "delayed",
    ],
)
def test_publisher_renders_validated_artifact_and_marks_missing_legs(report, monkeypatch, corrupt):
    posted = []
    windows = copy.deepcopy(report)
    windows["leg"] = "Windows-SQL2022"
    for pair in windows["pairs"]:
        for sample in pair.values():
            sample["environment"]["os"] = "Windows"
    if corrupt == "scenarios":
        report["pairs"][0]["candidate"]["scenarios"] = list(reporting.CASES)
    elif corrupt == "suite":
        report["suite_hash"] = "e" * 64
    data = {
        "Windows-SQL2022": zip_data([("report.json", json.dumps(windows))]),
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
    monkeypatch.setattr(reporting, "suite_hash", lambda root: "d" * 64)
    suite_versions = iter(({"suite": "source"}, {"suite": "base"}))
    monkeypatch.setattr(
        reporting,
        "suite_blobs",
        lambda *args: next(suite_versions) if corrupt == "source" else {"suite": "same"},
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
    publisher.run(123, "c" * 40, 1)
    assert len(posted) == 2
    assert posted[0].startswith(reporting.MARKER)
    if corrupt in ("base", "provenance"):
        assert "Build provenance validation failed" in posted[1]
        return
    assert "| Windows / SQL Server 2025 | No result available" in posted[1]
    if corrupt in ("suite", "source"):
        assert "workload version differs from trusted base" in posted[1]
        assert "consistent slowdown signals" not in posted[1]
    elif corrupt in ("zip", "timeout", "scenarios", "recursion", "deflate"):
        assert "### Windows / SQL Server 2022" in posted[1]
        assert reporting.escape("Linux-SQL2022 (invalid artifact)") in posted[1]
        assert "| Unix / SQL Server 2022 | No result available (invalid artifact) |" in posted[1]
        assert posted[1].count("20 consistent slowdown signals") == 1
    else:
        assert "### Windows / SQL Server 2022" in posted[1]
        assert posted[1].count("40 consistent slowdown signals") == 1


def test_publisher_waits_for_newer_run_after_exact_head_build_is_canceled(report, monkeypatch):
    canceled = ado_build(id=41, result="canceled")
    replacement = {**canceled, "id": 42, "result": "failed"}
    builds = iter(([canceled], [replacement]))
    posted = []
    clock = [0]

    def api(url):
        return {"value": next(builds)} if "/builds?" in url else {"value": []}

    monkeypatch.setattr(publisher, "api", api)
    monkeypatch.setattr(publisher, "github", pr_topology())
    monkeypatch.setattr(
        publisher, "publish", lambda number, head, body, base=None: posted.append(body)
    )
    monkeypatch.setattr(reporting, "suite_blobs", lambda *args: {"suite": "same"})
    monkeypatch.setattr(publisher.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        publisher.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds)
    )
    publisher.run(123, "c" * 40, 4)
    assert clock[0] == 240
    assert len(posted) == 2
    assert "buildId=42" in posted[1]


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
def test_publisher_deadline_finishes_without_reading_unfinished_build_metadata(monkeypatch, status):
    posted = []
    clock = [0]
    build = ado_build(status=status, sourceVersion=None)

    def github(path):
        assert path == "pulls/123", "Unfinished builds must not query merge topology"
        return {"state": "open", "head": {"sha": "c" * 40}, "base": {"sha": "a" * 40}}

    def api(url):
        assert "/builds?" in url, "Unfinished builds must not query artifacts"
        return {"value": [] if status is None else [build]}

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
    assert "1-minute wait" in posted[1]
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
    build = ado_build()
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
        publisher.HEADER
        + "**Performance assessment pending.**\n\n"
        + f"Waiting for the matching performance run for head `{'c' * 40}`.",
        "final report",
    ]


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
    assert all(
        "eq(variables['Build.Reason'], 'PullRequest')" in condition
        for condition in profiler_conditions
    )
    for release in (ROOT / "OneBranchPipelines").rglob("*.yml"):
        assert "ENABLE_PROFILING" not in release.read_text(encoding="utf-8")
    windows = pipeline.split("- job: pytestonwindows\n", 1)[1].split("\n- job:", 1)[0]
    assert "##vso[task.setvariable" not in windows
    assert "ENABLE_PROFILING: 1" in windows
    assert "ArtifactName: 'ddbc_bindings-profiling-$(sqlVersion)'" in windows
    assert "ArtifactName: 'ddbc_bindings'" in windows
    assert (
        "condition: and(succeeded(), eq(variables['Build.Reason'], 'PullRequest'), "
        "ne(variables['sqlVersion'], 'LocalDB'))"
    ) in windows
    assert (
        "condition: and(succeeded(), or(ne(variables['Build.Reason'], 'PullRequest'), "
        "eq(variables['sqlVersion'], 'LocalDB')))"
    ) in windows
    macos = pipeline.split("- job: PytestOnMacOS\n", 1)[1].split("\n- job:", 1)[0]
    assert "timeoutInMinutes: 90" in macos
    assert "ENABLE_PROFILING" not in macos
    assert "--reuse-candidate" not in macos
    assert "AdventureWorks2022" not in macos
    assert "profiler-macOS" not in macos
    assert "Routine PR profiling excludes hosted macOS" in macos
    linux = pipeline.split("- job: PytestOnLinux\n", 1)[1].split("\n- job:", 1)[0]
    assert (
        'if [ "$(Build.Reason)" = "PullRequest" ] && [ "$(distroName)" = "Ubuntu" ]; then' in linux
    )
    assert 'if [ "$PROFILER_BUILD" = "1" ]; then' in linux
    assert "python -m eng.profiler_benchmarks.controller --check-build on" in linux
    assert "--leg Linux-SQL2022" in linux
    assert "artifact: profiler-Linux-SQL2022" in linux
    benchmark = linux.split("# Run Unix performance benchmarks on Ubuntu", 1)[1]
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


def test_comment_workflow_executes_only_trusted_base_code():
    workflow = (ROOT / ".github/workflows/pr-profiler-report.yml").read_text(encoding="utf-8")
    assert "pull_request_target:" in workflow
    assert "ref: ${{ github.event.pull_request.base.sha }}" in workflow
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
