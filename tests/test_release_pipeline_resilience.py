"""Source-only pipeline contracts; no driver imports or SQL Server required."""

import importlib.util
import os
from pathlib import Path
import shutil
import subprocess
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
HELPER = ROOT / "eng/scripts/start_colima.py"
if not HELPER.is_file():
    pytest.skip("Requires the complete pipeline source checkout", allow_module_level=True)

spec = importlib.util.spec_from_file_location("start_colima", HELPER)
colima = importlib.util.module_from_spec(spec)
spec.loader.exec_module(colima)


@pytest.mark.parametrize("failure", [None, "first", "both", "context", "daemon", "cleanup"])
def test_colima_bounded_startup_and_verified_daemon(monkeypatch, failure):
    calls, diagnostics = [], []
    starts = 0

    def run(command, timeout):
        nonlocal starts
        calls.append((command, timeout))
        if command[:2] == ["colima", "start"]:
            starts += 1
            return failure not in {"both", "cleanup"} and not (failure == "first" and starts == 1)
        if command[:3] == ["docker", "context", "use"]:
            return failure != "context"
        if command[-1] == "info":
            return failure != "daemon"
        if command[:2] == ["colima", "stop"]:
            return failure != "cleanup"
        return True

    monkeypatch.setattr(colima, "run", run)
    monkeypatch.setattr(colima, "diagnostics", lambda: diagnostics.append(True))
    monkeypatch.setattr(colima.time, "sleep", lambda _: None)
    if failure in {"both", "context", "daemon", "cleanup"}:
        with pytest.raises(RuntimeError):
            colima.start()
    else:
        colima.start()
        assert calls[-1][0] == ["docker", "--context", "colima", "info"]
    assert starts == (1 if failure in {None, "cleanup"} else 2)
    assert len([c for c, _ in calls if c[-1] == "info"]) <= 24
    assert len([c for c, _ in calls if c[:2] == ["colima", "delete"]]) <= 1
    assert all(0 < timeout <= 600 for _, timeout in calls)
    if failure:
        assert diagnostics


def test_colima_command_timeout_terminates_only_its_process_group(monkeypatch):
    class Process:
        pid = 12345

        def __init__(self):
            self.waits = []

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def wait(self, timeout=None):
            self.waits.append(timeout)
            if len(self.waits) <= 2:
                raise colima.subprocess.TimeoutExpired("fixture", timeout)
            return -9

    process, signals = Process(), []

    def popen(command, **kwargs):
        assert kwargs == {"start_new_session": True}
        return process

    monkeypatch.setattr(colima.subprocess, "Popen", popen)
    monkeypatch.setattr(
        colima.os, "killpg", lambda pid, sig: signals.append((pid, sig)), raising=False
    )
    monkeypatch.setattr(colima.signal, "SIGKILL", 9, raising=False)
    assert colima.run(["colima", "start"], 600) is False
    assert signals == [(12345, colima.signal.SIGTERM), (12345, 9)]
    assert process.waits == [600, 5, None]


def test_linux_default_matrix_and_shards_cover_every_python_once():
    # PyYAML is a tooling-test dependency, not a new driver/runtime dependency.
    yaml = pytest.importorskip("yaml")
    pipeline = yaml.safe_load(
        (ROOT / "OneBranchPipelines/build-release-package-pipeline.yml").read_text()
    )
    parameters = {p["name"]: p for p in pipeline["parameters"]}
    configs = parameters["linuxConfigs"]["default"]
    assert len(configs) == 6
    expected = [f"cp3{minor}" for minor in range(10, 16)]
    for tag in ("manylinux_2_28", "musllinux"):
        for arch in ("x86_64", "aarch64"):
            group = [c for c in configs if (c["tag"], c["arch"]) == (tag, arch)]
            assert sorted(p for c in group for p in c["pythonBins"].split()) == expected
            assert len(group) == (2 if arch == "aarch64" else 1)
            if arch == "aarch64":
                assert all(len(c["pythonBins"].split()) == 3 for c in group)
    assert sum(len(c["pythonBins"].split()) for c in configs) == 24
    assert parameters["linuxBuildParallelLevel"]["default"] == "1"
    assert parameters["runSdlTasks"]["default"] is True
    assert parameters["signWindowsBinaries"]["default"] is True


def test_odbc_consumers_wait_for_scanned_producer_and_keep_release_interface():
    pipeline = (ROOT / "OneBranchPipelines/build-release-package-pipeline.yml").read_text()
    assert pipeline.count("odbcArtifactName: 'drop_ODBC_BuildAll_BuildWheel'") == 3
    assert pipeline.count("- ODBC_BuildAll") == 4  # Three consumers + compatibility.
    assert pipeline.count("- RuntimeWheels") == 4  # Three consumers + consolidation.
    odbc = (ROOT / "OneBranchPipelines/stages/build-odbc-all-stage.yml").read_text()
    assert "if ($wheelCount -ne 7)" in odbc
    assert "Assert each ODBC wheel is platform-correct" in odbc
    assert "EsrpMalwareScanning@6" in odbc
    compatibility = (
        ROOT / "OneBranchPipelines/jobs/consolidate-odbc-artifacts-job.yml"
    ).read_text()
    assert "artifact: 'drop_ConsolidateOdbc_ConsolidateArtifacts'" in compatibility
    consolidate = (ROOT / "OneBranchPipelines/jobs/consolidate-artifacts-job.yml").read_text()
    assert "--verify-only" in consolidate
    assert 'if [ "$WHEEL_COUNT" -ne 41 ]' in consolidate


@pytest.fixture
def linux_job():
    yaml = pytest.importorskip("yaml")
    stage = yaml.safe_load(
        (ROOT / "OneBranchPipelines/stages/build-linux-single-stage.yml").read_text()
    )
    return stage["stages"][0]["jobs"][0]


def test_linux_cache_reaches_both_container_build_and_test_pip(linux_job):
    variables = {v["name"]: v["value"] for v in linux_job["variables"]}
    cache = next(s for s in linux_job["steps"] if s.get("task") == "Cache@2")
    assert cache["inputs"]["path"] == "$(PIP_CACHE_DIR)"
    assert variables["PIP_CACHE_DIR"] == "$(Pipeline.Workspace)/.pip-cache"
    key = cache["inputs"]["key"]
    for token in (
        '"$(Agent.OS)"',
        '"${{ parameters.linuxTag }}"',
        '"${{ parameters.arch }}"',
        '"${{ parameters.pythonBins }}"',
        "eng/requirements-build-linux.txt",
        "eng/requirements-test-linux.txt",
    ):
        assert token in key
    scripts = "\n".join(s.get("script", "") for s in linux_job["steps"])
    assert 'mkdir -p "$(PIP_CACHE_DIR)"' in scripts
    assert '-v "$(PIP_CACHE_DIR):/pip-cache"' in scripts
    assert scripts.count("-e PIP_CACHE_DIR=/pip-cache/$PYBIN") == 2
    assert scripts.count("--require-hashes -r /workspace/eng/requirements-build-linux.txt") == 2
    assert scripts.count("--require-hashes -r /workspace/eng/requirements-test-linux.txt") == 2


@pytest.mark.parametrize("failure", ["", "install", "conversion", "fidelity"])
def test_manylinux_requires_working_cp949_conversion(linux_job, tmp_path, failure):
    bash = shutil.which("bash")
    if not bash:
        pytest.skip("Bash is required to exercise the pipeline script")
    step = next(
        s for s in linux_job["steps"] if s.get("displayName") == "Install system build dependencies"
    )
    script = step["script"].split("# ODBC requires CP949", 1)[1]
    script = script.split('docker exec "$CONTAINER" bash -lc ', 2)[1].strip()
    script = script[1 : script.rfind("'")]
    prefix = r"""
dnf() {
  if [ "$*" != "-y install glibc-gconv-extra" ]; then return 37; fi
  if [ "$FAILURE" = install ]; then return 19; fi
}
iconv() {
  cat >/dev/null
  if [ "$FAILURE" = conversion ]; then return 23; fi
  if [ "$FAILURE" = fidelity ]; then printf incorrect; return 0; fi
  printf "\xec\x95\x88\xeb\x85\x95\xed\x95\x98\xec\x84\xb8\xec\x9a\x94"
}
"""
    result = subprocess.run(
        [bash, "-c", prefix + script],
        cwd=tmp_path,
        env={**os.environ, "FAILURE": failure},
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == {"": 0, "install": 19, "conversion": 23, "fidelity": 1}[failure]
    assert ("CP949 conversion verified" in result.stdout) == (not failure)


def test_windows_cache_isolated_by_python_architecture_and_lock():
    yaml = pytest.importorskip("yaml")
    stage = yaml.safe_load(
        (ROOT / "OneBranchPipelines/stages/build-windows-single-stage.yml").read_text()
    )
    job = stage["stages"][0]["jobs"][0]
    cache = next(s for s in job["steps"] if s.get("task") == "Cache@2")
    assert cache["inputs"]["path"] == "$(PIP_CACHE_DIR)"
    assert job["variables"]["PIP_CACHE_DIR"] == r"$(Pipeline.Workspace)\.pip-cache"
    for token in (
        '"$(Agent.OS)"',
        '"$(Agent.OSArchitecture)"',
        '"${{ parameters.pythonVersion }}"',
        '"${{ parameters.architecture }}"',
        "eng/requirements-build-windows.txt",
    ):
        assert token in cache["inputs"]["key"]
    install = next(s for s in job["steps"] if s.get("displayName") == "Install Python dependencies")
    assert "--require-hashes -r eng/requirements-build-windows.txt" in install["powershell"]
    assert job["steps"].index(cache) < job["steps"].index(install)


@pytest.mark.parametrize("failure,status", [("", 0), ("setup", 19), ("inspect", 23), ("empty", 1)])
def test_linux_sql_setup_failure_never_publishes_an_address(linux_job, tmp_path, failure, status):
    bash = shutil.which("bash")
    if not bash:
        pytest.skip("Bash is required to exercise the pipeline script")
    step = next(
        s
        for s in linux_job["steps"]
        if s.get("displayName") == "Start SQL Server container for testing"
    )
    cleanup = next(
        s for s in linux_job["steps"] if s.get("displayName") == "Clean up Linux SQL container"
    )
    assert cleanup["condition"] == "always()"
    assert cleanup["timeoutInMinutes"] * 60 > 115
    assert step["timeoutInMinutes"] * 60 > 1260
    for task in (step, cleanup):
        assert '--owner "$(Build.BuildId).$(System.JobId)"' in task["script"]
        assert '--name "sqlserver-$(LINUX_TAG)-$(ARCH)"' in task["script"]
        assert "setup_sql_container.py" in task["script"]
    assert "--cleanup" in cleanup["script"]
    assert "--image mcr.microsoft.com/mssql/server:2022-latest" in step["script"]
    scripts = "\n".join(s.get("script", "") for s in linux_job["steps"])
    assert "docker stop build-$(LINUX_TAG)-$(ARCH) sqlserver-" not in scripts
    script = step["script"]
    for token, value in {
        "$(LINUX_TAG)": "manylinux_2_28",
        "$(ARCH)": "x86_64",
        "$(Build.BuildId)": "1",
        "$(System.JobId)": "fixture",
    }.items():
        script = script.replace(token, value)
    prefix = r"""
python3() {
  if [ "$FAILURE" = setup ]; then return 19; fi
}
docker() {
  if [ "$FAILURE" = inspect ]; then return 23; fi
  if [ "$FAILURE" != empty ]; then echo 172.17.0.2; fi
}
"""
    result = subprocess.run(
        [bash, "-c", prefix + script + "\necho complete\n"],
        cwd=tmp_path,
        env={**os.environ, "FAILURE": failure},
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == status, result.stdout + result.stderr
    if failure:
        assert "task.setvariable" not in result.stdout
        assert "complete" not in result.stdout
    else:
        assert "##vso[task.setvariable variable=SQL_IP]172.17.0.2" in result.stdout


@pytest.fixture
def macos_job():
    yaml = pytest.importorskip("yaml")
    stage = yaml.safe_load(
        (ROOT / "OneBranchPipelines/stages/build-macos-single-stage.yml").read_text()
    )
    return stage["stages"][0]["jobs"][0]


def test_macos_cache_and_cleanup_keep_release_isolation(macos_job):
    variables = {v["name"]: v["value"] for v in macos_job["variables"]}
    assert variables["DOCKER_CONTEXT"] == "colima"
    cache = next(s for s in macos_job["steps"] if s.get("task") == "Cache@2")
    assert cache["inputs"]["path"] == "$(PIP_CACHE_DIR)"
    assert variables["PIP_CACHE_DIR"] == "$(Pipeline.Workspace)/.pip-cache"
    assert cache["inputs"]["key"].endswith("eng/requirements-build-macos.txt")
    for key in (cache["inputs"]["key"], cache["inputs"]["restoreKeys"]):
        assert '"$(Agent.OS)"' in key
        assert '"$(Agent.OSArchitecture)"' in key
        assert '"${{ parameters.pythonVersion }}"' in key
    cleanup = next(
        s for s in macos_job["steps"] if s.get("displayName") == "Clean up macOS SQL container"
    )
    assert cleanup["condition"] == "always()"
    assert '--owner "$(Build.BuildId).$(System.JobId)"' in cleanup["script"]
    assert "--cleanup --colima" in cleanup["script"]


@pytest.mark.parametrize("failure", ["", "first", "both", "cancel", "cleanup", "daemon"])
def test_shared_sql_helper_preserves_bounded_release_colima_retry(monkeypatch, failure):
    from eng.scripts import setup_sql_container as sql_setup

    args = sql_setup.arguments(
        [
            "--name",
            "sqlserver",
            "--owner",
            "fixture",
            "--image",
            "mcr.microsoft.com/mssql/server:2022-latest",
            "--colima",
            "--retry-colima",
        ]
    )
    setup = sql_setup.SqlSetup(args, "dummy")
    calls = []

    def command(args, timeout=15, **kwargs):
        calls.append((args, timeout))
        starts = sum(c[:2] == ["colima", "start"] for c, _ in calls)
        if args[:2] == ["colima", "start"]:
            if failure == "cancel":
                raise sql_setup.Cancelled(15)
            if failure in {"both", "cleanup"} or failure == "first" and starts == 1:
                raise sql_setup.SetupFailure("fixture startup failure")
        if args[-1] == "info" and failure == "daemon":
            raise sql_setup.SetupFailure("fixture daemon failure")
        if args[:2] == ["colima", "stop"] and failure == "cleanup":
            raise sql_setup.SetupFailure("fixture cleanup failure")
        return SimpleNamespace(output="")

    monkeypatch.setattr(setup, "command", command)
    if failure == "cancel":
        with pytest.raises(sql_setup.Cancelled):
            setup.prepare_runtime()
    elif failure in {"both", "cleanup", "daemon"}:
        with pytest.raises(sql_setup.SetupFailure):
            setup.prepare_runtime()
    else:
        setup.prepare_runtime()
    assert sum(c[:2] == ["colima", "start"] for c, _ in calls) == (
        2 if failure in {"first", "both", "daemon"} else 1
    )
    assert sum(c[:2] == ["colima", "delete"] for c, _ in calls) <= 1
    assert all(0 < timeout <= 600 for _, timeout in calls)


def test_colima_retry_does_not_change_default_pr_or_linux_setup(monkeypatch):
    from eng.scripts import setup_sql_container as sql_setup

    base = [
        "--name",
        "sqlserver",
        "--owner",
        "fixture",
        "--image",
        "mcr.microsoft.com/mssql/server:2022-latest",
    ]
    for flags in ([], ["--colima"]):
        args = sql_setup.arguments(base + flags)
        assert not args.retry_colima
        setup = sql_setup.SqlSetup(args, "dummy")
        calls = []

        def command(*args, **kwargs):
            calls.append(args)
            raise sql_setup.SetupFailure("fixture startup failure")

        monkeypatch.setattr(setup, "command", command)
        if flags:
            with pytest.raises(sql_setup.SetupFailure):
                setup.prepare_runtime()
            assert len(calls) == 1
        else:
            setup.prepare_runtime()
            assert not calls
    with pytest.raises(sql_setup.SetupFailure, match="requires --colima setup"):
        sql_setup.arguments(base + ["--retry-colima"])


@pytest.mark.parametrize(
    "failure,expected_status", [("", 0), ("sql", 19), ("pip", 23), ("build", 29)]
)
def test_macos_overlaps_setup_and_propagates_failures(
    macos_job, tmp_path, failure, expected_status
):
    bash = shutil.which("bash")
    if not bash:
        pytest.skip("Bash is required to exercise the pipeline script")
    step = next(
        s
        for s in macos_job["steps"]
        if s.get("displayName") == "Build universal2 bindings alongside SQL setup"
    )
    script = step["script"]
    assert "--require-hashes -r eng/requirements-build-macos.txt" in script
    assert "setup_sql_container.py" in script
    assert "--image mcr.microsoft.com/mssql/server:2022-latest --colima --retry-colima" in script
    for token, value in {
        "$(Agent.TempDirectory)": ".",
        "$(Build.SourcesDirectory)": ".",
        "$(System.JobId)": "fixture",
        "$(Build.BuildId)": "1",
        "${{ parameters.pythonVersion }}": "3.13",
    }.items():
        script = script.replace(token, value)
    build_dir = tmp_path / "mssql_python" / "pybind"
    build_dir.mkdir(parents=True)
    (build_dir / "build.sh").write_text(
        "#!/bin/bash\n"
        "cd ../..\n"
        "echo build-start >> events\n"
        "touch build-started\n"
        'if [ "$FAILURE" = build ]; then exit 29; fi\n',
        encoding="utf-8",
    )
    (build_dir / "build.sh").chmod(0o755)
    prefix = r"""
python3() {
  case " $* " in
    *" --cleanup "*) echo cleanup >> events; return 0 ;;
  esac
  trap 'echo setup-cancelled >> events; exit 143' TERM
  echo setup-start >> events
  touch setup-started
  for i in {1..100}; do
    if [ -f build-started ]; then break; fi
    sleep 0.02
  done
  if [ ! -f build-started ]; then return 31; fi
  if [ "$FAILURE" = sql ]; then return 19; fi
  echo setup-ready >> events
}
python() {
  if [ "$*" = "-m pip install --require-hashes -r eng/requirements-build-macos.txt" ]; then
    for i in {1..100}; do
      if [ -f setup-started ]; then break; fi
      sleep 0.02
    done
    if [ ! -f setup-started ]; then return 37; fi
    echo dependencies >> events
    if [ "$FAILURE" = pip ]; then return 23; fi
  fi
}
"""
    result = subprocess.run(
        [bash, "-c", prefix + script + "\necho complete >> events\n"],
        cwd=tmp_path,
        env={**os.environ, "FAILURE": failure},
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == expected_status, result.stdout + result.stderr
    events = (tmp_path / "events").read_text().splitlines()
    assert events.index("setup-start") < events.index("dependencies")
    assert not list(tmp_path.glob("sql-setup-*.log"))
    if failure:
        assert "complete" not in events
        assert "cleanup" in events
    else:
        assert events.index("dependencies") < events.index("build-start")
        assert events.index("setup-ready") < events.index("complete")
        assert "cleanup" not in events
