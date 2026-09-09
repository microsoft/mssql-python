import shutil
import subprocess
import sys
import tarfile
import zipfile
from email.parser import Parser
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parents[1]
VALIDATOR = REPO_ROOT / "OneBranchPipelines" / "scripts" / "validate-rs-wheel-dependency.ps1"


def _copy_source(destination):
    shutil.copytree(
        REPO_ROOT,
        destination,
        ignore=shutil.ignore_patterns(".git", ".venv", "build", "dist", "*.egg-info"),
    )


@pytest.fixture(scope="module", name="built_wheel")
def _built_wheel(tmp_path_factory):
    root = tmp_path_factory.mktemp("rs-package-wheel")
    source = root / "source"
    _copy_source(source)

    fake_core = source / "mssql_py_core"
    fake_core.mkdir()
    (fake_core / "__init__.py").write_text("", encoding="ascii")
    extension = ".pyd" if sys.platform == "win32" else ".so"
    (fake_core / f"mssql_py_core.cp310-test{extension}").write_bytes(b"not-a-real-extension")

    dist = root / "dist"
    subprocess.run(
        [sys.executable, "setup.py", "bdist_wheel", "--dist-dir", str(dist)],
        cwd=source,
        check=True,
        capture_output=True,
        text=True,
    )

    wheels = list(dist.glob("mssql_python-*.whl"))
    assert len(wheels) == 1
    return wheels[0]


def _powershell():
    executable = shutil.which("pwsh") or shutil.which("powershell")
    if executable is None:
        pytest.skip("PowerShell is required to exercise the release validator")
    return executable


def _run_validator(wheel, root, version="0.1.0"):
    wheels_dir = root / "wheels"
    wheels_dir.mkdir()
    staged_wheel = wheels_dir / wheel.name
    shutil.copy2(wheel, staged_wheel)
    version_file = root / "mssql-python-rs.version"
    version_file.write_text(version, encoding="ascii")
    result = subprocess.run(
        [
            _powershell(),
            "-NoProfile",
            "-File",
            str(VALIDATOR),
            "-WheelsDir",
            str(wheels_dir),
            "-VersionFile",
            str(version_file),
        ],
        capture_output=True,
        check=False,
        text=True,
    )
    return result, staged_wheel


def test_wheel_depends_on_rs_distribution_without_vendoring_core(built_wheel):
    with zipfile.ZipFile(built_wheel) as wheel:
        names = wheel.namelist()
        metadata_names = [name for name in names if name.endswith(".dist-info/METADATA")]
        assert len(metadata_names) == 1
        metadata = Parser().parsestr(wheel.read(metadata_names[0]).decode("utf-8"))

    requirements = metadata.get_all("Requires-Dist", [])
    assert requirements.count("mssql-python-rs==0.1.0") == 1
    assert not any(name.startswith(("mssql_py_core/", "mssql_py_core.libs/")) for name in names)


def test_sdist_contains_rs_dependency_version_source(tmp_path):
    source = tmp_path / "source"
    _copy_source(source)
    dist = tmp_path / "dist"
    subprocess.run(
        [sys.executable, "setup.py", "sdist", "--dist-dir", str(dist)],
        cwd=source,
        check=True,
        capture_output=True,
        text=True,
    )

    archives = list(dist.glob("mssql_python-*.tar.gz"))
    assert len(archives) == 1
    with tarfile.open(archives[0], "r:gz") as archive:
        version_files = [
            name
            for name in archive.getnames()
            if name.endswith("/eng/versions/mssql-python-rs.version")
        ]
    assert len(version_files) == 1


def test_release_validator_accepts_clean_wheel(built_wheel, tmp_path):
    result, _ = _run_validator(built_wheel, tmp_path)
    assert result.returncode == 0, result.stderr


def test_release_validator_rejects_wrong_dependency_version(built_wheel, tmp_path):
    result, _ = _run_validator(built_wheel, tmp_path, version="0.1.1")
    assert result.returncode != 0
    assert "declare exactly mssql-python-rs==0.1.1" in result.stderr


def test_release_validator_rejects_vendored_core(built_wheel, tmp_path):
    result, staged_wheel = _run_validator(built_wheel, tmp_path)
    assert result.returncode == 0, result.stderr

    with zipfile.ZipFile(staged_wheel, "a") as wheel:
        wheel.writestr("mssql_py_core/__init__.py", "")

    result = subprocess.run(
        [
            _powershell(),
            "-NoProfile",
            "-File",
            str(VALIDATOR),
            "-WheelsDir",
            str(staged_wheel.parent),
            "-VersionFile",
            str(tmp_path / "mssql-python-rs.version"),
        ],
        capture_output=True,
        check=False,
        text=True,
    )
    assert result.returncode != 0
    assert "vendors mssql_py_core files" in result.stderr
