"""Regression tests for build_conda_packages.py orchestration invariants.

``verify()`` must run its ``python -c "import mssql_python"`` subprocesses from a NEUTRAL
working directory. For ``python -c``, ``sys.path[0]`` is ``''`` (the process cwd), so when the
ADO agent's cwd is the checkout root -- which contains the un-built ``mssql_python/`` and
``mssql_python_odbc/`` SOURCE trees -- the import resolves the SOURCE package (``ImportError:
No ddbc_bindings module found``) instead of the conda-INSTALLED one the gate is meant to
validate. The fix is a ``verify()`` wrapper that ``os.chdir``s to the per-leg build dir (the
Python equivalent of the ``cd`` the two deleted shell scripts did before their imports), so
every verify subprocess inherits the neutral cwd.

The tests also enforce exact-one ODBC wheel selection and blocking win-arm64 environment
creation. They load the orchestrator as a standalone module (no compiled extension needed)
and run under ``--noconftest``.
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
_PIPELINE_PATH = _ORCH_PATH.parent.parent / "conda-build-pipeline.yml"
_CONSOLIDATE_JOB_PATH = _ORCH_PATH.parent.parent / "jobs" / "consolidate-conda-artifacts-job.yml"

pytestmark = pytest.mark.skipif(
    not _ORCH_PATH.exists(), reason=f"orchestrator not present ({_ORCH_PATH})"
)


def _load_orchestrator():
    """Import build_conda_packages.py by path (stdlib-only; no ddbc_bindings needed)."""
    spec = importlib.util.spec_from_file_location("build_conda_packages_under_test", _ORCH_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_best_effort_consolidation_runs_after_upstream_failure():
    pipeline = _PIPELINE_PATH.read_text(encoding="utf-8")
    for stage_name in ("CondaWin64", "CondaMacOS", "CondaLinux"):
        producer = pipeline.split(f"- stage: {stage_name}", 1)[1]
        assert "dependsOn: ValidateWheelProvenance" in producer.split("jobs:", 1)[0]

    stage = pipeline.split("- stage: ConsolidateConda", 1)[1]
    dependencies = stage.split("jobs:", 1)[0]
    for stage_name in ("CondaWin64", "CondaMacOS", "CondaLinux"):
        assert f"- {stage_name}" in dependencies
    assert "condition: succeededOrFailed()" in stage.split("jobs:", 1)[0]

    mac_stage = pipeline.split("- stage: CondaMacOS", 1)[1].split("- stage: CondaLinux", 1)[0]
    mac_publish = mac_stage.split("displayName: 'Publish macOS conda artifact'", 1)[1]
    assert "condition: succeededOrFailed()" in mac_publish.split("inputs:", 1)[0]

    job = _CONSOLIDATE_JOB_PATH.read_text(encoding="utf-8")
    consolidate = job.split("- job: ConsolidateArtifacts", 1)[1]
    assert "condition: succeededOrFailed()" in consolidate.split("pool:", 1)[0]


def test_official_builds_require_main_wheel_provenance():
    pipeline = _PIPELINE_PATH.read_text(encoding="utf-8")
    resource = pipeline.split("- pipeline: buildPipeline", 1)[1].split("extends:", 1)[0]
    assert "branch: main" in resource

    gate = pipeline.split("- stage: ValidateWheelProvenance", 1)[1].split("- stage: CondaWin64", 1)[
        0
    ]
    assert '[[ -z "${WHEEL_SOURCE_BRANCH:-}" ]]' in gate
    assert 'case "$ONEBRANCH_TYPE" in' in gate
    assert "Official)" in gate
    assert "NonOfficial) ;;" in gate
    assert '[[ "$WHEEL_SOURCE_BRANCH" != "refs/heads/main" ]]' in gate
    assert "unknown OneBranch type" in gate
    assert "ONEBRANCH_TYPE: ${{ variables.effectiveOneBranchType }}" in gate
    assert "WHEEL_SOURCE_BRANCH: $(resources.pipeline.buildPipeline.sourceBranch)" in gate

    for stage_name in ("CondaWin64", "CondaMacOS", "CondaLinux"):
        producer = pipeline.split(f"- stage: {stage_name}", 1)[1]
        assert "dependsOn: ValidateWheelProvenance" in producer.split("jobs:", 1)[0]


def test_windows_pool_demand_is_indented_under_demands_key():
    pipeline = _PIPELINE_PATH.read_text(encoding="utf-8")
    windows_stage = pipeline.split("- stage: CondaWin64", 1)[1].split("- stage: CondaMacOS", 1)[0]
    assert (
        "              demands:\n"
        "                - imageOverride -equals PYTHON-1ES-MMS2022\n" in windows_stage
    )


@pytest.mark.parametrize(
    ("target_subdir", "cross_build", "host", "expected"),
    [
        ("linux-aarch64", True, "x86_64", True),
        ("linux-aarch64", True, "arm64", False),
        ("linux-aarch64", False, "x86_64", False),
        ("win-arm64", True, "x86_64", False),
        ("osx-arm64", True, "x86_64", False),
        ("", False, "x86_64", False),
    ],
)
def test_is_emulated_cross_only_classifies_linux_qemu(
    target_subdir, cross_build, host, expected, monkeypatch, capsys
):
    mod = _load_orchestrator()
    monkeypatch.setattr(mod.platform, "machine", lambda: host)

    assert mod._is_emulated_cross(target_subdir, cross_build) is expected
    assert ("QEMU" in capsys.readouterr().out) is expected


@pytest.mark.parametrize(("cross_build", "should_fail"), [(False, True), (True, False)])
def test_arm_target_execution_skip_requires_cross_build(
    cross_build, should_fail, tmp_path, monkeypatch
):
    mod = _load_orchestrator()

    def _fake_run(cmd, *args, **kwargs):
        command = list(cmd)
        target_python_probe = command[-2:] == ["-c", "import sys"]
        return types.SimpleNamespace(
            returncode=17 if target_python_probe else 0,
            stdout="target Python cannot execute" if target_python_probe else "",
        )

    monkeypatch.setattr(
        mod,
        "subprocess",
        types.SimpleNamespace(run=_fake_run, PIPE=subprocess.PIPE, STDOUT=subprocess.STDOUT),
    )

    if should_fail:
        with pytest.raises(SystemExit):
            mod._verify_impl(
                "conda",
                str(tmp_path / "channel"),
                str(tmp_path / "recipe"),
                ["3.13"],
                "1.2.3",
                "osx-arm64",
                cross_build,
                {},
            )
    else:
        mod._verify_impl(
            "conda",
            str(tmp_path / "channel"),
            str(tmp_path / "recipe"),
            ["3.13"],
            "1.2.3",
            "osx-arm64",
            cross_build,
            {},
        )


@pytest.mark.parametrize("target_subdir", ["win-arm64", "osx-arm64"])
def test_non_qemu_cross_driver_probe_failure_is_blocking(target_subdir, tmp_path, monkeypatch):
    mod = _load_orchestrator()
    probe_calls = []

    def _fake_run(cmd, *args, **kwargs):
        command = list(cmd)
        is_probe = any(str(arg).endswith("driver_load_probe.py") for arg in command)
        if is_probe:
            probe_calls.append(command)
        return types.SimpleNamespace(returncode=17 if is_probe else 0, stdout="")

    monkeypatch.setattr(mod.platform, "machine", lambda: "x86_64")
    monkeypatch.setattr(
        mod,
        "subprocess",
        types.SimpleNamespace(run=_fake_run, PIPE=subprocess.PIPE, STDOUT=subprocess.STDOUT),
    )

    with pytest.raises(SystemExit) as exc_info:
        mod._verify_impl(
            "conda",
            str(tmp_path / "chan"),
            str(tmp_path),
            ["3.13"],
            "1.2.3",
            target_subdir,
            True,
            {},
        )

    assert exc_info.value.code == 1
    assert len(probe_calls) == 1


def _run_reachability_helper_failure(monkeypatch, failing_marker):
    mod = _load_orchestrator()

    def _fake_run(cmd, *args, **kwargs):
        command = " ".join(str(arg) for arg in cmd)
        if failing_marker in command:
            return types.SimpleNamespace(returncode=17, stdout="specific helper failure")
        if "CONDA_PREFIX" in command:
            return types.SimpleNamespace(returncode=0, stdout="/tmp/verify-prefix\n")
        return types.SimpleNamespace(returncode=0, stdout="")

    monkeypatch.setattr(mod.sys, "platform", "linux")
    monkeypatch.setattr(
        mod,
        "subprocess",
        types.SimpleNamespace(run=_fake_run, PIPE=subprocess.PIPE, STDOUT=subprocess.STDOUT),
    )

    with pytest.raises(SystemExit) as exc_info:
        mod._reachability_gate(
            "conda",
            "verify_linux_313",
            "3.13",
            False,
            {"CONDA_ASSERT_PREFIX_REACHABLE": "1"},
        )
    assert exc_info.value.code == 1


def test_reachability_gate_reports_prefix_helper_failure(monkeypatch, capsys):
    _run_reachability_helper_failure(monkeypatch, "CONDA_PREFIX")
    error = capsys.readouterr().err
    assert "failed to read CONDA_PREFIX/sys.prefix" in error
    assert "specific helper failure" in error


def test_reachability_gate_reports_driver_locator_failure(monkeypatch, capsys):
    _run_reachability_helper_failure(monkeypatch, "mssql_python_odbc")
    error = capsys.readouterr().err
    assert "failed to locate driver path" in error
    assert "specific helper failure" in error


def test_reachability_gate_rejects_empty_prefix_output(monkeypatch, capsys):
    mod = _load_orchestrator()

    def _fake_run(_cmd, *args, **kwargs):
        return types.SimpleNamespace(returncode=0, stdout="")

    monkeypatch.setattr(mod.sys, "platform", "linux")
    monkeypatch.setattr(
        mod,
        "subprocess",
        types.SimpleNamespace(run=_fake_run, PIPE=subprocess.PIPE, STDOUT=subprocess.STDOUT),
    )

    with pytest.raises(SystemExit):
        mod._reachability_gate(
            "conda",
            "verify_linux_313",
            "3.13",
            False,
            {"CONDA_ASSERT_PREFIX_REACHABLE": "1"},
        )
    assert "empty CONDA_PREFIX/sys.prefix" in capsys.readouterr().err


def test_verify_reports_conda_list_failure(monkeypatch, capsys, tmp_path):
    mod = _load_orchestrator()

    def _fake_run(cmd, *args, **kwargs):
        command = list(cmd)
        is_conda_list = command[1:2] == ["list"]
        return types.SimpleNamespace(
            returncode=17 if is_conda_list else 0,
            stdout="specific conda list failure" if is_conda_list else "",
        )

    monkeypatch.setattr(
        mod,
        "subprocess",
        types.SimpleNamespace(run=_fake_run, PIPE=subprocess.PIPE, STDOUT=subprocess.STDOUT),
    )

    with pytest.raises(SystemExit):
        mod._verify_impl(
            "conda",
            str(tmp_path / "channel"),
            str(tmp_path),
            ["3.13"],
            "1.2.3",
            "",
            False,
            {},
        )
    error = capsys.readouterr().err
    assert "failed to list resolved dependencies" in error
    assert "specific conda list failure" in error


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
        "linux-64",
        False,
        {},  # env: no CONDA_ASSERT_PREFIX_REACHABLE -> the ldd reachability gate self-skips
        str(workdir),
    )

    # cwd must be restored regardless of how verify() exits.
    assert os.getcwd() == start_cwd

    import_calls = [
        (cmd, cwd)
        for cmd, cwd in calls
        if "-c" in cmd and any("mssql_python" in str(a) or "mssql_py_core" in str(a) for a in cmd)
    ]
    assert import_calls, "verify() never issued an `import mssql_python` probe"
    for cmd, cwd in import_calls:
        assert os.path.realpath(cwd) == os.path.realpath(str(workdir)), (
            f"import probe ran from {cwd!r}, not the neutral workdir {str(workdir)!r} -- the "
            f"repo source tree would shadow the conda-installed package"
        )
    codes = [cmd[-1] for cmd, _ in import_calls]
    assert codes[0] == mod._core_probe()
    assert codes.count(mod._core_probe()) == 1
    assert "import mssql_python" not in codes[0]


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
            "linux-64",
            False,
            {},
            str(workdir),
        )

    assert os.getcwd() == start_cwd, "verify() did not restore cwd after a failing phase"


def test_make_verify_channel_returns_encoded_file_uri(tmp_path):
    mod = _load_orchestrator()
    output_dir = tmp_path / "output #1" / "linux-64"
    output_dir.mkdir(parents=True)
    bld = tmp_path / "bld"
    bld.mkdir()
    (bld / "repodata.json").write_text("{}", encoding="ascii")

    channel = mod.make_verify_channel(str(output_dir), str(bld))
    channel_path = output_dir.parent / "verifychan_linux_64"

    assert channel == channel_path.resolve().as_uri()
    assert "%20" in channel
    assert "%23" in channel
    assert (channel_path / "repodata.json").read_text(encoding="ascii") == "{}"


def test_main_routes_native_effective_subdir_without_cross_target(tmp_path, monkeypatch):
    mod = _load_orchestrator()
    calls = {}

    monkeypatch.setattr(mod, "gather_wheels", lambda *_args: ("1.2.3", "18.6.2"))
    monkeypatch.setattr(mod, "find_or_install_conda", lambda _output_dir: "conda")
    monkeypatch.setattr(mod, "run", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(mod, "create_builder_env", lambda _conda: "conda_builder")
    monkeypatch.setattr(mod, "detect_pythons", lambda *_args: ["3.13"])

    def _build_env(_mssql_ver, _odbc_ver, _links, cross_target_subdir):
        calls["build_env"] = cross_target_subdir
        return {}

    def _build_packages(_conda, _builder, _recipe, _pyvers, _bld, target, _env):
        calls["build"] = target

    def _audit_packages(_conda, _builder, _recipe, _bld, target, _env):
        calls["audit"] = target

    def _verify(
        _conda,
        _channel,
        _recipe,
        _pyvers,
        _version,
        target,
        cross_build,
        _env,
        _workdir,
    ):
        calls["verify"] = (target, cross_build)

    def _stage(_bld, _stage_dir, target):
        calls["stage"] = target

    monkeypatch.setattr(mod, "build_env", _build_env)
    monkeypatch.setattr(mod, "build_packages", _build_packages)
    monkeypatch.setattr(mod, "audit_packages", _audit_packages)
    monkeypatch.setattr(mod, "make_verify_channel", lambda *_args: "file:///channel")
    monkeypatch.setattr(mod, "verify", _verify)
    monkeypatch.setattr(mod, "stage", _stage)

    result = mod.main(
        [
            "--mssql-wheel-dir",
            str(tmp_path / "wheels"),
            "--odbc-wheel-dir",
            str(tmp_path / "odbc"),
            "--odbc-wheel-filter",
            "*.whl",
            "--recipe-root",
            str(tmp_path / "recipe"),
            "--output-dir",
            str(tmp_path / "output"),
            "--stage-dir",
            str(tmp_path / "stage"),
            "--conda-subdir",
            "linux-64",
        ]
    )

    assert result == 0
    assert calls == {
        "build_env": "",
        "build": "linux-64",
        "audit": "linux-64",
        "verify": ("linux-64", False),
        "stage": "linux-64",
    }


def test_build_env_clears_ambient_subdir_for_native_build(monkeypatch):
    mod = _load_orchestrator()
    monkeypatch.setenv("CONDA_SUBDIR", "win-arm64")

    env = mod.build_env("1.2.3", "18.6.2", "wheels", "")

    assert "CONDA_SUBDIR" not in env


def test_build_env_sets_subdir_for_cross_build(monkeypatch):
    mod = _load_orchestrator()
    monkeypatch.setenv("CONDA_SUBDIR", "win-64")

    env = mod.build_env("1.2.3", "18.6.2", "wheels", "osx-arm64")

    assert env["CONDA_SUBDIR"] == "osx-arm64"


def test_win_arm64_real_environment_create_failure_is_blocking(tmp_path, monkeypatch):
    """A successful solve does not prove package extraction/linking succeeds."""
    mod = _load_orchestrator()
    calls = []

    def _fake_run(cmd, *args, **kwargs):
        command = list(cmd)
        calls.append(command)
        is_real_create = command[1:3] == ["create", "-y"]
        return types.SimpleNamespace(returncode=17 if is_real_create else 0, stdout="")

    monkeypatch.setattr(
        mod,
        "subprocess",
        types.SimpleNamespace(run=_fake_run, PIPE=subprocess.PIPE, STDOUT=subprocess.STDOUT),
    )

    with pytest.raises(SystemExit) as exc_info:
        mod._verify_impl(
            "conda",
            str(tmp_path / "chan"),
            str(tmp_path / "recipe"),
            ["3.11"],
            "1.2.3",
            "win-arm64",
            True,
            {},
        )

    assert exc_info.value.code == 1
    assert any("--dry-run" in command for command in calls)
    assert any(command[1:3] == ["create", "-y"] for command in calls)
    assert not any(command[1:3] == ["run", "-n"] for command in calls)


def _wheel_inputs(tmp_path, odbc_names):
    mssql_dir = tmp_path / "mssql"
    odbc_dir = tmp_path / "odbc"
    links = tmp_path / "links"
    mssql_dir.mkdir()
    odbc_dir.mkdir()
    (mssql_dir / "mssql_python-1.2.3-cp313-cp313-win_amd64.whl").write_bytes(b"mssql")
    for name in odbc_names:
        (odbc_dir / name).write_bytes(b"odbc")
    return mssql_dir, odbc_dir, links


def test_gather_wheels_accepts_exactly_one_odbc_match(tmp_path):
    mod = _load_orchestrator()
    mssql_dir, odbc_dir, links = _wheel_inputs(
        tmp_path, ["mssql_python_odbc-18.6.2-py3-none-win_amd64.whl"]
    )
    (mssql_dir / "mssql_python-1.2.3-cp312-cp312-win_amd64.whl").write_bytes(b"mssql")

    versions = mod.gather_wheels(
        str(mssql_dir), "mssql_python-*.whl", str(odbc_dir), "*.whl", str(links)
    )

    assert versions == ("1.2.3", "18.6.2")
    assert sorted(path.name for path in links.iterdir()) == [
        "mssql_python-1.2.3-cp312-cp312-win_amd64.whl",
        "mssql_python-1.2.3-cp313-cp313-win_amd64.whl",
        "mssql_python_odbc-18.6.2-py3-none-win_amd64.whl",
    ]


def test_gather_wheels_rejects_mixed_mssql_python_versions(tmp_path, capsys):
    mod = _load_orchestrator()
    mssql_dir, odbc_dir, links = _wheel_inputs(
        tmp_path, ["mssql_python_odbc-18.6.2-py3-none-win_amd64.whl"]
    )
    (mssql_dir / "mssql_python-9.9.9-cp312-cp312-win_amd64.whl").write_bytes(b"mssql")

    with pytest.raises(SystemExit):
        mod.gather_wheels(str(mssql_dir), "mssql_python-*.whl", str(odbc_dir), "*.whl", str(links))

    error = capsys.readouterr().err
    assert "inconsistent versions" in error
    assert "1.2.3" in error
    assert "9.9.9" in error


def test_gather_wheels_rejects_no_odbc_match(tmp_path):
    mod = _load_orchestrator()
    mssql_dir, odbc_dir, links = _wheel_inputs(tmp_path, [])

    with pytest.raises(SystemExit):
        mod.gather_wheels(str(mssql_dir), "mssql_python-*.whl", str(odbc_dir), "*.whl", str(links))


def test_gather_wheels_rejects_multiple_odbc_matches(tmp_path):
    mod = _load_orchestrator()
    mssql_dir, odbc_dir, links = _wheel_inputs(
        tmp_path,
        [
            "mssql_python_odbc-18.6.2-py3-none-win_amd64.whl",
            "mssql_python_odbc-18.6.3-py3-none-win_amd64.whl",
        ],
    )

    with pytest.raises(SystemExit):
        mod.gather_wheels(str(mssql_dir), "mssql_python-*.whl", str(odbc_dir), "*.whl", str(links))


@pytest.mark.parametrize(
    ("target_subdir", "expected_channels"),
    [
        ("", ["microsoft", "conda-forge"]),
        ("win-64", ["microsoft", "conda-forge"]),
        ("osx-arm64", ["microsoft", "conda-forge"]),
        ("linux-aarch64", ["microsoft", "conda-forge"]),
        ("win-arm64", ["defaults", "microsoft", "conda-forge"]),
    ],
)
def test_conda_build_uses_only_explicit_channels(
    target_subdir, expected_channels, tmp_path, monkeypatch
):
    mod = _load_orchestrator()
    calls = []
    croot = tmp_path / "croot"
    croot.mkdir()
    (croot / "stale").write_text("stale", encoding="ascii")

    def _capture_run(command, **_kwargs):
        calls.append(list(command))

    monkeypatch.setattr(mod, "run", _capture_run)

    mod.build_packages(
        "conda",
        "conda_builder",
        str(tmp_path / "recipe"),
        ["3.13"],
        str(tmp_path / "conda-bld"),
        target_subdir,
        {},
    )

    assert len(calls) == 1
    command = calls[0]
    assert "--override-channels" in command
    assert command[command.index("--croot") + 1] == str(croot)
    assert not croot.exists()
    assert [command[index + 1] for index, arg in enumerate(command) if arg == "-c"] == (
        expected_channels
    )


@pytest.mark.parametrize("state", ["native", "pure-python", "foreign"])
def test_core_probe_requires_native_extension_from_installed_prefix(state, tmp_path, monkeypatch):
    mod = _load_orchestrator()
    prefix = tmp_path / "prefix"
    monkeypatch.setattr(sys, "prefix", str(prefix))
    package = types.ModuleType("mssql_py_core")
    package.__file__ = str(prefix / "mssql_py_core" / "__init__.py")
    monkeypatch.setitem(sys.modules, "mssql_py_core", package)
    if state != "pure-python":
        native = types.ModuleType("mssql_py_core.mssql_py_core")
        native.__file__ = str((tmp_path / "foreign" if state == "foreign" else prefix) / "core.pyd")
        native.__loader__ = importlib.machinery.ExtensionFileLoader(
            native.__name__, native.__file__
        )
        monkeypatch.setitem(sys.modules, native.__name__, native)
    if state == "native":
        exec(mod._core_probe(), {})
    else:
        with pytest.raises(AssertionError, match="native extension|outside installed prefix"):
            exec(mod._core_probe(), {})


def test_core_failure_blocks_api_preload(tmp_path, monkeypatch):
    mod = _load_orchestrator()
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append(list(cmd))
        return types.SimpleNamespace(returncode=17 if mod._core_probe() in cmd else 0, stdout="")

    monkeypatch.setattr(
        mod,
        "subprocess",
        types.SimpleNamespace(run=fake_run, PIPE=subprocess.PIPE, STDOUT=subprocess.STDOUT),
    )
    with pytest.raises(SystemExit):
        mod._verify_impl(
            "conda", "channel", str(tmp_path), ["3.12"], "1.14.0", "linux-64", False, {}
        )
    assert any(mod._core_probe() in cmd for cmd in calls)
    assert not any("BINDING_OK" in str(cmd) for cmd in calls)


@pytest.mark.parametrize("subdir", ["win-64", "win-arm64"])
def test_both_windows_targets_run_native_audit(subdir, monkeypatch):
    mod = _load_orchestrator()
    calls = []
    monkeypatch.setattr(mod, "run", lambda cmd, **kwargs: calls.append(cmd))
    mod.audit_packages(
        "conda", "builder", str(_ORCH_PATH.parents[2] / "conda"), "output", subdir, {}
    )
    pe_calls = [
        cmd for cmd in calls if any(str(arg).endswith("assert_pe_machine.py") for arg in cmd)
    ]
    assert len(pe_calls) == 1
    assert pe_calls[0][-2:] == ["--subdir", subdir]


def test_build_does_not_automatically_accept_channel_terms(monkeypatch):
    mod = _load_orchestrator()
    monkeypatch.delenv("CONDA_PLUGINS_AUTO_ACCEPT_TOS", raising=False)
    assert "CONDA_PLUGINS_AUTO_ACCEPT_TOS" not in mod.build_env(
        "1.14.0", "18.6.2.1", "wheels", "win-arm64"
    )


@pytest.mark.parametrize("subdir", ["win-64", "win-arm64"])
def test_main_routes_effective_target_to_native_audit(subdir, tmp_path, monkeypatch):
    mod = _load_orchestrator()
    targets = []
    monkeypatch.setattr(mod, "gather_wheels", lambda *args: ("1.14.0", "18.6.2.1"))
    monkeypatch.setattr(mod, "find_or_install_conda", lambda *args: "conda")
    monkeypatch.setattr(mod, "create_builder_env", lambda *args: "builder")
    monkeypatch.setattr(mod, "detect_pythons", lambda *args: ["3.12"])
    monkeypatch.setattr(mod, "make_verify_channel", lambda *args: "channel")
    for name in ("run", "build_packages", "verify", "stage"):
        monkeypatch.setattr(mod, name, lambda *args, **kwargs: None)
    monkeypatch.setattr(mod, "audit_packages", lambda *args: targets.append(args[-2]))
    args = [
        "--mssql-wheel-dir",
        str(tmp_path / "wheels"),
        "--odbc-wheel-dir",
        str(tmp_path / "wheels"),
        "--odbc-wheel-filter",
        "*.whl",
        "--recipe-root",
        str(_ORCH_PATH.parents[2] / "conda"),
        "--output-dir",
        str(tmp_path / "out"),
        "--stage-dir",
        str(tmp_path / "stage"),
        "--conda-subdir",
        subdir,
    ]
    if subdir == "win-arm64":
        args += ["--conda-target-subdir", subdir]
    assert mod.main(args) == 0
    assert targets == [subdir]
