"""Regression test for build_conda_packages.py ``verify()`` -- the whole point of the PR.

``verify()`` must run its ``python -c "import mssql_python"`` subprocesses from a NEUTRAL
working directory. For ``python -c``, ``sys.path[0]`` is ``''`` (the process cwd), so when the
ADO agent's cwd is the checkout root -- which contains the un-built ``mssql_python/`` and
``mssql_python_odbc/`` SOURCE trees -- the import resolves the SOURCE package (``ImportError:
No ddbc_bindings module found``) instead of the conda-INSTALLED one the gate is meant to
validate. The fix is a ``verify()`` wrapper that ``os.chdir``s to the per-leg build dir (the
Python equivalent of the ``cd`` the two deleted shell scripts did before their imports), so
every verify subprocess inherits the neutral cwd.

The orchestrator otherwise has no tests, which is how the source-shadowing regression slipped
in; this asserts the invariant so the whole class stays closed. It loads the orchestrator as a
standalone module (no compiled extension needed) and runs under ``--noconftest``.
"""

import importlib.util
import os
import subprocess
import sys
import types
from pathlib import Path

import pytest

_ORCH_PATH = (
    Path(__file__).resolve().parent.parent
    / "OneBranchPipelines"
    / "scripts"
    / "build_conda_packages.py"
)

pytestmark = pytest.mark.skipif(
    not _ORCH_PATH.exists(), reason=f"orchestrator not present ({_ORCH_PATH})"
)


def _load_orchestrator():
    """Import build_conda_packages.py by path (stdlib-only; no ddbc_bindings needed)."""
    spec = importlib.util.spec_from_file_location("build_conda_packages_under_test", _ORCH_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_verify_runs_imports_from_neutral_workdir(tmp_path, monkeypatch):
    """Capture the cwd at every subprocess call and assert the ``import mssql_python`` probes
    ran from the passed workdir (not the inherited checkout-root cwd), and that the original
    cwd is restored afterward."""
    mod = _load_orchestrator()
    calls = []

    def _fake_run(cmd, *args, **kwargs):
        # Record the cwd EFFECTIVE at call time (verify() os.chdir's, it does not pass cwd=).
        calls.append((list(cmd), os.getcwd()))
        return types.SimpleNamespace(returncode=0, stdout="")

    monkeypatch.setattr(
        mod,
        "subprocess",
        types.SimpleNamespace(run=_fake_run, PIPE=subprocess.PIPE, STDOUT=subprocess.STDOUT),
    )

    workdir = tmp_path / "conda-bld" / "linux-64"
    workdir.mkdir(parents=True)
    start_cwd = os.getcwd()

    mod.verify(
        "conda",
        str(tmp_path / "chan"),
        str(tmp_path / "recipe"),
        ["3.11"],
        "1.2.3",
        "",  # native target (no cross-skip)
        {},  # env: no CONDA_ASSERT_PREFIX_REACHABLE -> the ldd reachability gate self-skips
        str(workdir),
    )

    # cwd must be restored regardless of how verify() exits.
    assert os.getcwd() == start_cwd

    import_calls = [
        (cmd, cwd)
        for cmd, cwd in calls
        if "-c" in cmd and any("mssql_python" in str(a) for a in cmd)
    ]
    assert import_calls, "verify() never issued an `import mssql_python` probe"
    for cmd, cwd in import_calls:
        assert os.path.realpath(cwd) == os.path.realpath(str(workdir)), (
            f"import probe ran from {cwd!r}, not the neutral workdir {str(workdir)!r} -- the "
            f"repo source tree would shadow the conda-installed package"
        )


def test_verify_restores_cwd_when_the_phase_fails(tmp_path, monkeypatch):
    """The wrapper's ``finally`` must restore the original cwd even when the verify phase raises
    (a failed subprocess -> _die, or any exception) -- otherwise a failing leg would strand the
    process in the build dir and corrupt the later stage() step's relative paths. The happy-path
    test proves the chdir; this proves the restore survives the failure path."""
    mod = _load_orchestrator()

    def _raising_run(cmd, *args, **kwargs):
        raise RuntimeError("boom: subprocess failed")

    monkeypatch.setattr(
        mod,
        "subprocess",
        types.SimpleNamespace(run=_raising_run, PIPE=subprocess.PIPE, STDOUT=subprocess.STDOUT),
    )

    workdir = tmp_path / "conda-bld" / "linux-64"
    workdir.mkdir(parents=True)
    start_cwd = os.getcwd()

    with pytest.raises(RuntimeError):
        mod.verify(
            "conda",
            str(tmp_path / "chan"),
            str(tmp_path / "recipe"),
            ["3.11"],
            "1.2.3",
            "",
            {},
            str(workdir),
        )

    assert os.getcwd() == start_cwd, "verify() did not restore cwd after a failing phase"
