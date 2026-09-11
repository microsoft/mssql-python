import os
from pathlib import Path
import re
import subprocess
import textwrap

import pytest

ROOT = Path(__file__).resolve().parents[1]
PIPELINE = ROOT / "eng" / "pipelines" / "pr-validation-pipeline.yml"
if not PIPELINE.is_file():
    pytest.skip("SQL pipeline contracts require a source checkout", allow_module_level=True)

JOBS = {
    "PytestOnMacOS": ("sqlserver", False),
    "PytestOnLinux": ("sqlserver-$(distroName)", True),
    "PytestOnLinux_ARM64": ("sqlserver-$(distroName)-$(archName)", True),
    "PytestOnLinux_RHEL9": ("sqlserver-rhel9", True),
    "PytestOnLinux_RHEL9_ARM64": ("sqlserver-rhel9-arm64", True),
    "PytestOnLinux_Alpine": ("sqlserver-alpine", True),
    "PytestOnLinux_Alpine_ARM64": ("sqlserver-alpine-arm64", True),
    "CodeCoverageReport": ("sqlserver", False),
}


def section(name):
    text = PIPELINE.read_text(encoding="utf-8")
    return re.search(r"^- job: " + name + r"\n.*?(?=^- job: |\Z)", text, re.M | re.S)[0]


def script_steps(name):
    return re.findall(r"^  - script: \|\n.*?(?=^  - |\Z)", section(name), re.M | re.S)


@pytest.mark.parametrize("name, configuration", JOBS.items())
def test_all_sql_setup_and_cleanup_paths_are_wired(name, configuration):
    container, database = configuration
    steps = script_steps(name)
    setup = next(step for step in steps if "setup_sql_container.py" in step and "--image" in step)
    assert container in setup
    assert ("--database TestDB" in setup) == database
    assert "$(Build.BuildId).$(System.JobId)" in setup
    assert "DB_PASSWORD: $(DB_PASSWORD)" in setup
    assert "continueOnError" not in setup and "retryCountOnTaskFailure" not in setup
    assert "MSSQL_SA_PASSWORD=" not in setup and "-P " not in setup
    cleanup = [
        step
        for step in steps
        if "setup_sql_container.py" in step and "--cleanup" in step and "--image" not in step
    ]
    assert len(cleanup) == 1
    assert "always()" in cleanup[0]
    assert "timeoutInMinutes: 3" in cleanup[0]
    assert "test-container" not in cleanup[0]
    assert container in cleanup[0]
    if name != "PytestOnMacOS":
        assert "timeoutInMinutes: 22" in setup


def test_azuresql_skips_local_sql_and_preserves_matrix():
    linux = section("PytestOnLinux")
    assert "Ubuntu_AzureSQL:" in linux
    assert "if ne(variables['AZURE_CONNECTION_STRING'], '')" in linux
    setup = next(step for step in script_steps("PytestOnLinux") if "--image" in step)
    assert "condition: and(succeeded(), eq(variables['useAzureSQL'], 'false'))" in setup
    cleanup = next(step for step in script_steps("PytestOnLinux") if "--cleanup" in step)
    assert "condition: and(always(), eq(variables['useAzureSQL'], 'false'))" in cleanup
    assert "AZURE_CONNECTION_STRING" in linux


def test_no_new_build_test_retries_or_sql_architecture_changes():
    pipeline = PIPELINE.read_text(encoding="utf-8")
    assert pipeline.count("retryCountOnTaskFailure:") == 3
    for name in ("PytestOnLinux_ARM64", "PytestOnLinux_RHEL9_ARM64", "PytestOnLinux_Alpine_ARM64"):
        steps = [step for step in script_steps(name) if "retryCountOnTaskFailure:" in step]
        assert len(steps) == 1
        assert "retryCountOnTaskFailure: 2" in steps[0]
        assert "build.sh" in steps[0]
        assert "setup_sql_container.py" not in steps[0] and "pytest" not in steps[0]
    helper = (ROOT / "eng" / "scripts" / "setup_sql_container.py").read_text(encoding="utf-8")
    assert '"linux/amd64"' in helper
    assert "linux/arm64" not in helper
    assert "Config.Env" not in helper and "prune" not in helper
    for name in JOBS:
        for step in script_steps(name):
            if "python -m pytest" in step:
                assert "retryCountOnTaskFailure" not in step
                assert "continueOnError" not in step
                assert "--cleanup" not in step


def test_mac_overlap_and_failure_wait_remain_separate_from_tests():
    mac = section("PytestOnMacOS")
    assert "DOCKER_CONTEXT: colima" in mac
    assert "timeoutInMinutes: 90" in mac
    assert "SQL2022:" in mac and "SQL2025:" in mac
    setup = next(step for step in script_steps("PytestOnMacOS") if "--image" in step)
    assert '> "$SQL_LOG" 2>&1 &' in setup
    assert setup.count("./build.sh") == 1
    assert setup.count("pip install -r requirements.txt") == 1
    assert (
        setup.index("SQL_PID=$!") < setup.index("./build.sh") < setup.rindex('wait "$SQL_PID" ||')
    )
    assert 'exit "$SQL_STATUS"' in setup
    assert "trap finish_setup EXIT" in setup
    assert "pytest" not in setup
    assert "/tmp/sql_setup.log" not in setup


@pytest.mark.skipif(os.name != "posix", reason="Hosted Unix Bash supervision contract")
@pytest.mark.parametrize("sql_status, build_status", [(0, 0), (17, 0), (0, 27)])
def test_mac_script_builds_once_and_gates_dependents(tmp_path, sql_status, build_status):
    step = next(step for step in script_steps("PytestOnMacOS") if "--image" in step)
    script = textwrap.dedent(step.split("    displayName:", 1)[0].split("\n", 1)[1])
    replacements = {
        "$(Agent.TempDirectory)": str(tmp_path),
        "$(Build.SourcesDirectory)": str(tmp_path),
        "$(System.JobId)": "test-job",
        "$(Build.BuildId)": "1",
        "$(sqlServerImage)": "mcr.microsoft.com/mssql/server:2025-latest",
    }
    for before, after in replacements.items():
        script = script.replace(before, after)
    bindir = tmp_path / "bin"
    bindir.mkdir()
    events = tmp_path / "events"
    builddir = tmp_path / "mssql_python" / "pybind"
    builddir.mkdir(parents=True)
    build = builddir / "build.sh"
    build.write_text('#!/bin/sh\necho build >> "$EVENTS"\nexit "$BUILD_STATUS"\n', encoding="utf-8")
    build.chmod(0o700)
    for name in ("python", "pip"):
        file = bindir / name
        file.write_text('#!/bin/sh\necho dependency >> "$EVENTS"\n', encoding="utf-8")
        file.chmod(0o700)
    python3 = bindir / "python3"
    python3.write_text(
        "#!/bin/sh\n"
        'case "$*" in\n'
        '  *--cleanup*) echo cleanup >> "$EVENTS"; exit 0 ;;\n'
        "esac\n"
        'echo sql >> "$EVENTS"\n'
        'exit "$SQL_STATUS_TEST"\n',
        encoding="utf-8",
    )
    python3.chmod(0o700)
    env = {
        **os.environ,
        "PATH": str(bindir) + os.pathsep + os.environ["PATH"],
        "EVENTS": str(events),
        "SQL_STATUS_TEST": str(sql_status),
        "BUILD_STATUS": str(build_status),
    }
    script_file = tmp_path / "step.sh"
    script_file.write_text(script, encoding="utf-8")
    result = subprocess.run(
        ["bash", str(script_file)],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=15,
    )
    actions = events.read_text(encoding="utf-8").splitlines()
    assert result.returncode == (build_status or sql_status), result.stdout + result.stderr
    assert actions.count("build") == 1
    assert actions.count("dependency") == 2
    assert actions.count("sql") <= 1
    assert actions.count("cleanup") == (1 if result.returncode else 0)
    if result.returncode == 0:
        assert actions.count("sql") == 1
