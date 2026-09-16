"""Contract tests for paired performance comparisons and data-only PR reporting."""

import copy
import importlib.util
import io
import json
from pathlib import Path
import re
import subprocess
import sys
import tarfile
from types import SimpleNamespace
from unittest.mock import MagicMock
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
    for job in ("pytestonwindows", "PytestOnMacOS", "PytestOnLinux"):
        section = pipeline.split(f"- job: {job}\n", 1)[1].split("\n- job:", 1)[0]
        job_minutes = int(re.search(r"^  timeoutInMinutes: (\d+)$", section, re.M)[1])
        benchmark_step = section.split("python benchmarks/profiler_ci.py --reuse-candidate", 1)[1]
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


def test_linux_profiler_step_does_not_put_database_password_on_command_line():
    pipeline = (ROOT / "eng/pipelines/pr-validation-pipeline.yml").read_text(encoding="utf-8")
    benchmark = pipeline.split("# Run performance benchmarks on Ubuntu", 1)[1]
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


@pytest.mark.parametrize("corrupt", [None, "zip", "scenarios"])
def test_publisher_renders_validated_artifact_and_marks_missing_legs(report, monkeypatch, corrupt):
    posted = []
    windows = copy.deepcopy(report)
    windows["leg"] = "Windows-SQL2022"
    for pair in windows["pairs"]:
        for sample in pair.values():
            sample["environment"]["os"] = "Windows"
    if corrupt == "scenarios":
        report["pairs"][0]["candidate"]["scenarios"] = list(reporting.CASES)
    data = {
        "Windows-SQL2022": zip_data([("report.json", json.dumps(windows))]),
        "Linux-SQL2022": (
            b"invalid ZIP" if corrupt == "zip" else zip_data([("report.json", json.dumps(report))])
        ),
    }
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
                        "name": "profiler-" + leg,
                        "resource": {"downloadUrl": "https://dev.azure.com/" + leg},
                    }
                    for leg in data
                ]
            }
            if "/artifacts?" in url
            else {"value": [build]}
        ),
    )
    monkeypatch.setattr(publisher, "fetch", lambda url, **kw: data[url.rsplit("/", 1)[-1]])
    publisher.run(123, "c" * 40, 1)
    assert len(posted) == 2
    assert posted[0].startswith(reporting.MARKER)
    assert "### Windows-SQL2022" in posted[1]
    assert "macOS-SQL2022: incomplete/unavailable" in posted[1]
    if corrupt:
        assert reporting.escape("Linux-SQL2022 (invalid artifact)") in posted[1]
        assert "Linux-SQL2022: incomplete/unavailable" in posted[1]
        assert posted[1].count("20 regression signals") == 1
    else:
        assert posted[1].count("20 regression signals") == 2


@pytest.mark.parametrize("status", [None, "notStarted", "inProgress"])
def test_publisher_deadline_finishes_without_reading_unfinished_build_metadata(monkeypatch, status):
    posted = []
    clock = [0]
    build = dict(
        id=42,
        status=status,
        sourceVersion=None,
        definition={"id": 2128},
        repository={"id": "microsoft/mssql-python"},
        sourceBranch="refs/pull/123/merge",
        triggerInfo={"pr.number": "123", "pr.sourceSha": "c" * 40},
    )

    def github(path):
        assert path == "pulls/123", "Unfinished builds must not query merge topology"
        return {"state": "open", "head": {"sha": "c" * 40}}

    def api(url):
        assert "/builds?" in url, "Unfinished builds must not query artifacts"
        return {"value": [] if status is None else [build]}

    def sleep(seconds):
        clock[0] += seconds

    monkeypatch.setattr(publisher.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(publisher.time, "sleep", sleep)
    monkeypatch.setattr(publisher, "github", github)
    monkeypatch.setattr(publisher, "api", api)
    monkeypatch.setattr(publisher, "publish", lambda number, head, body: posted.append(body))
    publisher.run(123, "c" * 40, 1)
    assert clock[0] == 60 and len(posted) == 2
    assert "Awaiting" in posted[0] and "Awaiting" not in posted[1]
    assert "1-minute wait" in posted[1] and "incomplete" in posted[1]
    assert "No regression verdict" in posted[1]


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
    assert "-e BUILD_BUILDID \\" in benchmark
    assert "BUILD_BUILDID: $(Build.BuildId)" in benchmark
    assert "git config --global --add safe.directory /workspace" in benchmark
    assert "apt-get install -y --reinstall libodbcinst2" in benchmark
    assert benchmark.index("apt-get install -y --reinstall libodbcinst2") < benchmark.index(
        "ACCEPT_EULA=Y apt-get install"
    )
    assert "libodbc1 " not in benchmark and "odbcinst1debian2" not in benchmark


def test_comment_workflow_executes_only_trusted_base_code():
    workflow = (ROOT / ".github/workflows/pr-profiler-report.yml").read_text(encoding="utf-8")
    assert "pull_request_target:" in workflow
    assert "ref: ${{ github.event.pull_request.base.sha }}" in workflow
    assert "persist-credentials: false" in workflow
    assert "actions/checkout@11d5960a326750d5838078e36cf38b85af677262" in workflow
    assert "actions/setup-python@a26af69be951a213d495a4c3e4e4022e16d87065" in workflow
    assert (
        "head.ref" not in workflow
        and "head.sha }}" not in workflow.split("ref:", 1)[1].split("persist", 1)[0]
    )
