"""Contract tests for paired performance comparisons and data-only PR reporting."""

import copy
import importlib.util
import io
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import zipfile

import pytest

ROOT = Path(__file__).resolve().parents[1]
if not (ROOT / ".github/scripts/post_profiler_comment.py").is_file():
    pytest.skip("CI reporting tools are not installed in driver wheels", allow_module_level=True)


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, ROOT / path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


reporting = load("profiler_report", "benchmarks/profiler_report.py")
controller = load("profiler_ci", "benchmarks/profiler_ci.py")
sys.modules["profiler_report"] = reporting
publisher = load("post_profiler_comment", ".github/scripts/post_profiler_comment.py")


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
    assert "20 regression signals" in body
    assert "incomplete/unavailable" in body  # missing platforms never read as green


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


def test_reject_wrong_commit_and_preserve_incomplete_status(report):
    with pytest.raises(ValueError, match="provenance"):
        reporting.validate(report, head="e" * 40)
    report["status"] = "incomplete"
    report["pairs"] = []
    reporting.validate(report)
    assert "No regression verdict" in reporting.render([report], "c" * 40, 42)


def zip_data(entries):
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as archive:
        for name, data in entries:
            archive.writestr(name, data)
    return out.getvalue()


def test_artifact_read_never_extracts_paths(report):
    raw = json.dumps(report)
    assert (
        publisher.artifact_report(zip_data([("profiler-Linux-SQL2022/report.json", raw)])) == report
    )
    for entries in [
        [("../report.json", raw)],
        [("/report.json", raw)],
        [("a/report.json", raw), ("b/report.json", raw)],
        [("logs.txt", "no report")],
    ]:
        with pytest.raises(ValueError):
            publisher.artifact_report(zip_data(entries))


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


def test_build_selection_requires_exact_pr_head():
    build = dict(
        id=42,
        definition={"id": 2128},
        repository={"id": "microsoft/mssql-python"},
        sourceBranch="refs/pull/123/merge",
        triggerInfo={"pr.number": "123", "pr.sourceSha": "c" * 40},
    )
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


def test_revisions_use_exact_first_parent(monkeypatch):
    calls = []

    def git(*args):
        calls.append(args)
        return "b" * 40 if len(calls) == 1 else "a" * 40

    monkeypatch.setattr(controller, "git", git)
    assert controller.resolve_revisions(None, "HEAD") == ("a" * 40, "b" * 40)
    assert calls[1][-1] == "b" * 40 + "^1^{commit}"


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


@pytest.mark.parametrize("corrupt", [False, True])
def test_publisher_renders_validated_artifact_and_marks_missing_legs(report, monkeypatch, corrupt):
    posted = []
    build = dict(
        id=42,
        status="completed",
        definition={"id": 2128},
        repository={"id": "microsoft/mssql-python"},
        sourceBranch="refs/pull/123/merge",
        sourceVersion="b" * 40,
        triggerInfo={"pr.number": "123", "pr.sourceSha": "c" * 40},
    )
    monkeypatch.setattr(publisher, "publish", lambda number, head, body: posted.append(body))
    monkeypatch.setattr(
        publisher,
        "github",
        lambda path: (
            {"state": "open", "head": {"sha": "c" * 40}}
            if path.startswith("pulls/")
            else {"parents": [{"sha": "a" * 40}, {"sha": "c" * 40}]}
        ),
    )
    monkeypatch.setattr(
        publisher,
        "api",
        lambda url: (
            {
                "value": [
                    {
                        "name": "profiler-Linux-SQL2022",
                        "resource": {"downloadUrl": "https://dev.azure.com/artifact"},
                    }
                ]
            }
            if "/artifacts?" in url
            else {"value": [build]}
        ),
    )
    raw = b"invalid ZIP" if corrupt else zip_data([("report.json", json.dumps(report))])
    monkeypatch.setattr(publisher, "fetch", lambda *args, **kwargs: raw)
    publisher.run(123, "c" * 40, 1)
    assert len(posted) == 2
    assert posted[0].startswith(reporting.MARKER)
    assert "Windows-SQL2022: incomplete/unavailable" in posted[1]
    if corrupt:
        assert reporting.escape("Linux-SQL2022 (invalid artifact)") in posted[1]
        assert "regression signals" not in posted[1]
    else:
        assert "20 regression signals" in posted[1]


def test_artifact_symlink_and_oversized_json_are_rejected():
    symlink = zipfile.ZipInfo("report.json")
    symlink.create_system = 3
    symlink.external_attr = 0o120777 << 16
    with pytest.raises(ValueError, match="Invalid report"):
        publisher.artifact_report(zip_data([(symlink, "{}")]))
    with pytest.raises(ValueError, match="Invalid report"):
        publisher.artifact_report(zip_data([("report.json", " " * (reporting.MAX_BYTES + 1))]))


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
    assert pipeline.count("python benchmarks/profiler_ci.py --reuse-candidate") == 3
    assert "profilerBuild: '0'" in pipeline  # LocalDB still exercises the normal build
    assert "ddbc_bindings-profiling-SQL2022" in pipeline
    assert "ddbc_bindings-profiling-SQL2025" in pipeline
    assert "ENABLE_PROFILING=1 ./build.sh" in pipeline
    for release in (ROOT / "OneBranchPipelines").rglob("*.yml"):
        assert "ENABLE_PROFILING" not in release.read_text(encoding="utf-8")
    windows = pipeline.split("- job: pytestonwindows\n", 1)[1].split("\n- job:", 1)[0]
    assert "profilerCheck: 'off'" in windows.split("LocalDB_Python314:", 1)[1].split("steps:", 1)[0]
    assert windows.split("steps:", 1)[0].count("profilerCheck: 'on'") == 2
    linux = pipeline.split("- job: PytestOnLinux\n", 1)[1].split("\n- job:", 1)[0]
    benchmark = linux.split("# Run performance benchmarks on Ubuntu", 1)[1]
    assert '-e BUILD_BUILDID="$(Build.BuildId)"' in benchmark
    assert "git config --global --add safe.directory /workspace" in benchmark
    assert "apt-get install -y --reinstall libodbcinst2" in benchmark
    assert "libodbc1 " not in benchmark and "odbcinst1debian2" not in benchmark


def test_comment_workflow_executes_only_trusted_base_code():
    workflow = (ROOT / ".github/workflows/pr-profiler-report.yml").read_text(encoding="utf-8")
    assert "pull_request_target:" in workflow
    assert "ref: ${{ github.event.pull_request.base.sha }}" in workflow
    assert "persist-credentials: false" in workflow
    assert (
        "head.ref" not in workflow
        and "head.sha }}" not in workflow.split("ref:", 1)[1].split("persist", 1)[0]
    )
