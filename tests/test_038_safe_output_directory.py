import importlib.util
import os
import subprocess
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_MODULE_PATH = _REPO_ROOT / "eng" / "scripts" / "mssql_python_build_safety.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("safe_output_directory_under_test", _MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _directory_link(link: Path, target: Path) -> None:
    if os.name == "nt":
        subprocess.run(
            ["cmd", "/c", "mklink", "/J", str(link), str(target)],
            check=True,
            capture_output=True,
        )
    else:
        link.symlink_to(target, target_is_directory=True)


def test_rejects_current_working_directory_and_ancestors(tmp_path, monkeypatch):
    module = _load_module()
    working_directory = tmp_path / "checkout" / "subdirectory"
    working_directory.mkdir(parents=True)
    monkeypatch.chdir(working_directory)

    for unsafe in (working_directory, working_directory.parent, tmp_path):
        with pytest.raises(ValueError, match="current working directory|ancestors"):
            module.resolve_safe_output_directory(unsafe)


def test_rejects_link_or_junction_in_path(tmp_path):
    module = _load_module()
    target = tmp_path / "unrelated"
    target.mkdir()
    link = tmp_path / "link"
    _directory_link(link, target)

    with pytest.raises(ValueError, match="symbolic link|junction"):
        module.resolve_safe_output_directory(link / "output")


def test_allows_normal_directory_outside_working_tree(tmp_path):
    module = _load_module()
    working_directory = tmp_path / "checkout"
    working_directory.mkdir()
    output = tmp_path / "artifacts" / "wheels"

    assert module.resolve_safe_output_directory(output, cwd=working_directory) == output.resolve()


def test_installers_use_shared_output_directory_guard():
    shell_installer = (_REPO_ROOT / "eng" / "scripts" / "install-mssql-py-core.sh").read_text(
        encoding="utf-8"
    )
    powershell_installer = (_REPO_ROOT / "eng" / "scripts" / "install-mssql-py-core.ps1").read_text(
        encoding="utf-8"
    )

    assert "mssql_python_build_safety.py" in shell_installer
    assert "mssql_python_build_safety.py" in powershell_installer
