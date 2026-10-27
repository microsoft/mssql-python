import json
import runpy
import subprocess
import sys
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
if not (ROOT / "eng/scripts/mssql_python_rs.py").is_file() or not (ROOT / "setup.py").is_file():
    pytest.skip(
        "Runtime selection contracts require a complete source checkout", allow_module_level=True
    )

from eng.scripts import install_mssql_python_rs as installer
from eng.scripts import mssql_python_rs as runtime


@pytest.fixture(autouse=True)
def clear_override(monkeypatch):
    monkeypatch.delenv(runtime.VERSION_ENV, raising=False)


def _pin(tmp_path, value="0.3.0"):
    path = tmp_path / "runtime.version"
    path.write_text(value, encoding="ascii")
    return path


@pytest.mark.parametrize("override", [None, "", "0.4.0", "0.4.0.dev20261028187000"])
def test_resolves_exact_release_or_override(tmp_path, monkeypatch, override):
    if override is not None:
        monkeypatch.setenv(runtime.VERSION_ENV, override)
    selected = runtime.resolve_runtime(_pin(tmp_path))
    assert selected.version == (override or "0.3.0")
    assert selected.version_override == bool(override)
    assert selected.index_url == (
        runtime.DEV_INDEX if override and ".dev" in override else runtime.DEFAULT_INDEX
    )


@pytest.mark.parametrize(
    "version",
    ["latest", ">=0.3.0", "0.3", "0.3.0;echo bad", "0.3.0rc1", "0.3.0.dev", "0.3.0+local"],
)
def test_rejects_non_exact_or_unsupported_override(tmp_path, monkeypatch, version):
    monkeypatch.setenv(runtime.VERSION_ENV, version)
    with pytest.raises(ValueError, match="Invalid MSSQL_PYTHON_RS_VERSION"):
        runtime.resolve_runtime(_pin(tmp_path))


@pytest.mark.parametrize("version", ["", "0.4.0.dev1", "0.4.0rc1", ">=0.3.0"])
def test_committed_pin_must_be_a_release(tmp_path, version):
    with pytest.raises(ValueError, match="exact released"):
        runtime.resolve_runtime(_pin(tmp_path, version))


@pytest.mark.parametrize("override", [None, "0.4.0.dev20261028187000"])
def test_setup_metadata_uses_same_effective_runtime(monkeypatch, override):
    if override:
        monkeypatch.setenv(runtime.VERSION_ENV, override)
    captured = {}
    monkeypatch.chdir(ROOT)
    monkeypatch.setattr("setuptools.setup", lambda **kwargs: captured.update(kwargs))
    runpy.run_path(str(ROOT / "setup.py"))
    selected = runtime.resolve_runtime()
    assert f"mssql-python-rs=={selected.version}" in captured["install_requires"]


@pytest.mark.parametrize("override", [None, "0.4.0.dev20261028187000"])
def test_generated_package_metadata_matches_selection(tmp_path, monkeypatch, override):
    if override:
        monkeypatch.setenv(runtime.VERSION_ENV, override)
    subprocess.run(
        [sys.executable, "setup.py", "egg_info", "--egg-base", str(tmp_path)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    metadata = tmp_path / "mssql_python.egg-info"
    requirements = (metadata / "requires.txt").read_text(encoding="utf-8").splitlines()
    assert f"mssql-python-rs=={runtime.resolve_runtime().version}" in requirements
    sources = (metadata / "SOURCES.txt").read_text(encoding="utf-8")
    assert "eng/scripts/mssql_python_rs.py" in sources
    assert "eng/versions/mssql-python-rs.version" in sources


def test_verify_rejects_runtime_replaced_by_later_pip_install(monkeypatch):
    monkeypatch.setattr(runtime.importlib.metadata, "version", lambda _name: "0.3.0")
    with pytest.raises(ValueError, match="expected 0.4.0.dev1"):
        runtime.verify_installed(runtime.Runtime("0.4.0.dev1", runtime.DEV_INDEX, True))


def test_verify_imports_native_module(monkeypatch):
    monkeypatch.setattr(runtime.importlib.metadata, "version", lambda _name: "0.3.0")
    imports = []

    def import_module(name):
        imports.append(name)
        return SimpleNamespace(__file__="test-native-module")

    monkeypatch.setattr(runtime.importlib, "import_module", import_module)
    runtime.verify_installed(runtime.Runtime("0.3.0", runtime.DEFAULT_INDEX, False))
    assert imports == ["mssql_py_core"]


@pytest.mark.parametrize("cross_arch", [False, True])
def test_installer_selects_index_and_preserves_cross_arch_behavior(
    tmp_path, monkeypatch, cross_arch
):
    monkeypatch.setenv(runtime.VERSION_ENV, "0.4.0.dev1")
    monkeypatch.setenv("PIP_EXTRA_INDEX_URL", "https://wrong.example/simple/")
    monkeypatch.setenv("PIP_CERT", "corporate-ca.pem")
    monkeypatch.setattr(installer.platform, "machine", lambda: "AMD64")
    monkeypatch.setattr(installer.sys, "platform", "win32")
    commands = []
    verified = []

    def run(command, *, env, check):
        assert check is True
        assert "PIP_EXTRA_INDEX_URL" not in env
        assert env["PIP_CERT"] == "corporate-ca.pem"
        commands.append(command)
        if "download" in command:
            target = Path(command[command.index("--dest") + 1])
            name = "mssql_python_rs-0.4.0.dev1-cp310-abi3-win_arm64.whl"
            with zipfile.ZipFile(target / name, "w") as wheel:
                wheel.writestr(
                    "mssql_python_rs-0.4.0.dev1.dist-info/METADATA",
                    "Name: mssql-python-rs\nVersion: 0.4.0.dev1\n",
                )

    monkeypatch.setattr(installer.subprocess, "run", run)
    monkeypatch.setattr(installer, "verify_installed", verified.append)
    installer.install_runtime("arm64" if cross_arch else "", tmp_path / "downloads")
    download = commands[0]
    assert download[download.index("--index-url") + 1] == runtime.DEV_INDEX
    assert download[-1] == "mssql-python-rs==0.4.0.dev1"
    if cross_arch:
        assert download[download.index("--platform") + 1] == "win_arm64"
        assert len(commands) == 1
        assert verified == []
    else:
        assert len(commands) == 2
        assert "--no-deps" in commands[1]
        assert verified[0].version == "0.4.0.dev1"
    assert not list((tmp_path / "downloads").iterdir())


def test_installer_does_not_fall_back_after_download_failure(monkeypatch):
    commands = []

    def run(command, **kwargs):
        commands.append(command)
        raise subprocess.CalledProcessError(1, command)

    monkeypatch.setattr(installer.subprocess, "run", run)
    with pytest.raises(subprocess.CalledProcessError):
        installer.install_runtime()
    assert len(commands) == 1


def test_resolver_cli_writes_effective_artifact_pin(tmp_path, monkeypatch):
    monkeypatch.setenv(runtime.VERSION_ENV, "0.4.0.dev1")
    destination = tmp_path / "mssql-python-rs.version"
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "eng/scripts/mssql_python_rs.py"),
            "--write-version-file",
            str(destination),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert destination.read_text(encoding="ascii") == "0.4.0.dev1\n"
    assert json.loads(result.stdout)["version_override"] is True
