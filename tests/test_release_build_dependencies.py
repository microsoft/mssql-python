import re
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml
from packaging.requirements import Requirement

ROOT = Path(__file__).parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "refresh-build-dependencies.yml"
RELEASE_PIPELINE = ROOT / "OneBranchPipelines" / "stages" / "build-linux-single-stage.yml"


def _compile_command(workflow, output):
    commands = [
        line.strip()
        for line in workflow.splitlines()
        if "uv pip compile" in line and output in line
    ]
    assert (
        len(commands) == 1
    ), f"Expected exactly one compile command for {output}, found {len(commands)}: {commands}"
    return commands[0]


def _platform_matrix_item(workflow, name):
    compile_platform = workflow.split("  compile-platform:", maxsplit=1)[1]
    matrix = compile_platform.split("\n    steps:", maxsplit=1)[0]
    marker = f"          - name: {name}\n"
    items = matrix.split(marker)
    assert len(items) == 2, f"Expected exactly one {name} matrix item"
    return items[1].split("\n          - name:", maxsplit=1)[0]


def test_lock_generation_preserves_python_markers():
    workflow = WORKFLOW.read_text(encoding="utf-8")

    for output in (
        "eng/requirements-build-linux.txt",
        "eng/requirements-test-linux.txt",
    ):
        assert "--universal" in _compile_command(workflow, output)

    for name, universal in (
        ("macos", "--universal"),
        ("windows", "--universal"),
        ("odbc", '""'),
    ):
        item = _platform_matrix_item(workflow, name)
        values = [line.strip() for line in item.splitlines() if "universal:" in line]
        assert values == [f"universal: {universal}"]

    assert "${{ matrix.universal }}" in _compile_command(workflow, "matrix.output")


def test_asyncio_backport_is_only_installed_below_python_311():
    affected_locks = (
        "requirements-build-windows.txt",
        "requirements-build-macos.txt",
        "requirements-test-linux.txt",
    )

    for lock_name in affected_locks:
        lock = (ROOT / "eng" / lock_name).read_text(encoding="utf-8")
        requirement = next(
            line for line in lock.splitlines() if line.startswith("backports-asyncio-runner==")
        )
        assert re.fullmatch(
            r"backports-asyncio-runner==[^ ;]+ ; python_(?:full_)?version < ['\"]3\.11['\"] \\",
            requirement,
        )


def test_isolated_wheel_suite_excludes_release_repository_contracts():
    pipeline = RELEASE_PIPELINE.read_text(encoding="utf-8")

    assert pipeline.count("--ignore=tests/test_release_build_dependencies.py") == 2


def test_windows_release_lock_supplies_arrow_for_every_python():
    lock = (ROOT / "eng" / "requirements-build-windows.txt").read_text(encoding="utf-8")
    requirements = [
        Requirement(line.rstrip(" \\"))
        for line in lock.splitlines()
        if line.startswith("pyarrow==")
    ]
    for minor in range(10, 16):
        environment = {
            "python_version": f"3.{minor}",
            "python_full_version": f"3.{minor}.0",
        }
        selected = [
            requirement
            for requirement in requirements
            if requirement.marker is None or requirement.marker.evaluate(environment)
        ]
        assert len(selected) == 1
        assert selected[0].specifier.contains("25.0.1" if minor < 15 else "26.0.0")


@pytest.mark.parametrize("fixture_index", [0, 1], ids=["manylinux", "musllinux"])
def test_relocated_linux_fixture_runs_public_contracts(tmp_path_factory, fixture_index):
    tmp_path = tmp_path_factory.mktemp("lf")
    pipeline = RELEASE_PIPELINE.read_text(encoding="utf-8")
    fixtures = pipeline.split("# Step 7: Setup test environment")[1:]
    assert len(fixtures) == 2
    fixture = fixtures[fixture_index].split("# Step 8:", maxsplit=1)[0]
    for line in fixture.splitlines():
        if not line.strip().startswith("cp "):
            continue
        arguments = shlex.split(line.strip().split(" ||", maxsplit=1)[0].rstrip(";"))
        sources = arguments[2:-1] if arguments[1] == "-r" else arguments[1:-1]
        destination = arguments[-1]
        assert destination.startswith("$TEST_DIR/")
        target = tmp_path / destination.removeprefix("$TEST_DIR/")
        target.mkdir(parents=True, exist_ok=True)
        for source in sources:
            assert source.startswith("/workspace/")
            path = ROOT / source.removeprefix("/workspace/")
            if path.is_dir():
                directory = target if source.endswith("/.") else target / path.name
                shutil.copytree(
                    path,
                    directory,
                    dirs_exist_ok=True,
                    ignore=shutil.ignore_patterns("__pycache__"),
                )
            else:
                shutil.copy2(path, target / path.name)
    for package in ("mssql_python", "mssql_python_odbc", "mssql_py_core"):
        assert not (tmp_path / package).exists(), f"Fixture shadows the installed {package}"
    # Supply only the installed provider's metadata, not importable driver source.
    provider_init = ROOT / "mssql_python_odbc" / "__init__.py"
    if provider_init.is_file():
        provider = provider_init.read_text(encoding="utf-8")
        version = re.search(r'__version__\s*=\s*["\']([^"\']+)["\']', provider).group(1)
    else:
        from importlib.metadata import version as distribution_version

        version = distribution_version("mssql-python-odbc")
    metadata = tmp_path / f"mssql_python_odbc-{version}.dist-info"
    metadata.mkdir()
    (metadata / "METADATA").write_text(
        f"Metadata-Version: 2.1\nName: mssql-python-odbc\nVersion: {version}\n",
        encoding="utf-8",
    )
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "--noconftest",
            "-q",
            "--tb=short",
            "--basetemp",
            str(tmp_path / "p"),
            "tests/test_027_conda_release_metadata.py",
            "tests/test_029_bundled_binary_audit.py",
            "tests/test_030_pe_machine_assert.py",
            "tests/test_034_conda_verify_cwd.py",
            "tests/test_036_conda_provenance.py",
            "tests/test_040_rs_runtime_selection.py",
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_python_315_source_builds_preserve_stable_binary_only_matrix():
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    job = workflow["jobs"]["validate-locks"]
    matrix = {item["name"]: item for item in job["strategy"]["matrix"]["include"]}

    for name in ("Linux build", "Linux test", "macOS", "Windows build host"):
        stable = matrix[name]
        assert stable["versions"].split() == ["3.10", "3.11", "3.12", "3.13", "3.14"]
        assert stable.get("allowSourceBuilds", False) is False

        preview = matrix[f"{name} Python 3.15"]
        assert preview["versions"] == "3.15"
        assert preview["allowSourceBuilds"] is True
        for field in ("artifact", "lock", "platforms"):
            assert preview[field] == stable[field]

    step = next(step for step in job["steps"] if "ALLOW_SOURCE_BUILDS" in step.get("env", {}))
    assert step["env"]["ALLOW_SOURCE_BUILDS"] == "${{ matrix.allowSourceBuilds || false }}"
    assert "binary_args=(--only-binary :all:)" in step["run"]
    assert 'if [ "$ALLOW_SOURCE_BUILDS" = true ]; then binary_args=(); fi' in step["run"]
    assert '"${binary_args[@]}"' in step["run"]
    assert "--require-hashes" in step["run"]
