"""Regression tests for eng.conda_tools build orchestration invariants.

``verify()`` must run its ``python -c "import mssql_python"`` subprocesses from a NEUTRAL
working directory. For ``python -c``, ``sys.path[0]`` is ``''`` (the process cwd), so when the
ADO agent's cwd is the checkout root -- which contains the un-built ``mssql_python/`` and
``mssql_python_odbc/`` SOURCE trees -- the import resolves the SOURCE package (``ImportError:
No ddbc_bindings module found``) instead of the conda-INSTALLED one the gate is meant to
validate. The fix is a ``verify()`` wrapper that ``os.chdir``s to the per-leg build dir (the
Python equivalent of the ``cd`` the two deleted shell scripts did before their imports), so
every verify subprocess inherits the neutral cwd.

The tests also enforce exact-one ODBC wheel selection and blocking win-arm64 environment
creation. They import the internal tooling modules (no compiled extension needed)
and run under ``--noconftest``.
"""

import importlib.machinery
import os
import subprocess
import sys
import types
import zipfile
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
_TOOLS_DIR = _ROOT / "eng" / "conda_tools"

if not _TOOLS_DIR.is_dir():
    pytest.skip(
        f"Conda tooling source not present ({_TOOLS_DIR}); skipping source-only build tests",
        allow_module_level=True,
    )

from eng.conda_tools import __main__ as cli
from eng.conda_tools import archive, build, contracts, environment, verify
from test_030_pe_machine_assert import _fake_pe


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
    monkeypatch.setattr(verify.platform, "machine", lambda: host)

    assert verify._is_emulated_cross(target_subdir, cross_build) is expected
    assert ("QEMU" in capsys.readouterr().out) is expected


@pytest.mark.parametrize(("cross_build", "should_fail"), [(False, True), (True, False)])
def test_arm_target_execution_skip_requires_cross_build(
    cross_build, should_fail, tmp_path, monkeypatch
):

    def _fake_run(cmd, *args, **kwargs):
        command = list(cmd)
        target_python_probe = command[-2:] == ["-c", "import sys"]
        return types.SimpleNamespace(
            returncode=17 if target_python_probe else 0,
            stdout="target Python cannot execute" if target_python_probe else "",
        )

    monkeypatch.setattr(
        environment,
        "subprocess",
        types.SimpleNamespace(run=_fake_run, PIPE=subprocess.PIPE, STDOUT=subprocess.STDOUT),
    )

    if should_fail:
        with pytest.raises(SystemExit):
            verify._verify_impl(
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
        verify._verify_impl(
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
    probe_calls = []

    def _fake_run(cmd, *args, **kwargs):
        command = list(cmd)
        is_probe = any(str(arg).endswith("driver_load_probe.py") for arg in command)
        if is_probe:
            probe_calls.append(command)
        return types.SimpleNamespace(returncode=17 if is_probe else 0, stdout="")

    monkeypatch.setattr(verify.platform, "machine", lambda: "x86_64")
    monkeypatch.setattr(
        environment,
        "subprocess",
        types.SimpleNamespace(run=_fake_run, PIPE=subprocess.PIPE, STDOUT=subprocess.STDOUT),
    )

    with pytest.raises(SystemExit) as exc_info:
        verify._verify_impl(
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

    def _fake_run(cmd, *args, **kwargs):
        command = " ".join(str(arg) for arg in cmd)
        if failing_marker in command:
            return types.SimpleNamespace(returncode=17, stdout="specific helper failure")
        if "CONDA_PREFIX" in command:
            return types.SimpleNamespace(returncode=0, stdout="/tmp/verify-prefix\n")
        return types.SimpleNamespace(returncode=0, stdout="")

    monkeypatch.setattr(verify.sys, "platform", "linux")
    monkeypatch.setattr(
        environment,
        "subprocess",
        types.SimpleNamespace(run=_fake_run, PIPE=subprocess.PIPE, STDOUT=subprocess.STDOUT),
    )

    with pytest.raises(SystemExit) as exc_info:
        verify._reachability_gate(
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

    def _fake_run(_cmd, *args, **kwargs):
        return types.SimpleNamespace(returncode=0, stdout="")

    monkeypatch.setattr(verify.sys, "platform", "linux")
    monkeypatch.setattr(
        environment,
        "subprocess",
        types.SimpleNamespace(run=_fake_run, PIPE=subprocess.PIPE, STDOUT=subprocess.STDOUT),
    )

    with pytest.raises(SystemExit):
        verify._reachability_gate(
            "conda",
            "verify_linux_313",
            "3.13",
            False,
            {"CONDA_ASSERT_PREFIX_REACHABLE": "1"},
        )
    assert "empty CONDA_PREFIX/sys.prefix" in capsys.readouterr().err


def test_verify_reports_conda_list_failure(monkeypatch, capsys, tmp_path):

    def _fake_run(cmd, *args, **kwargs):
        command = list(cmd)
        is_conda_list = command[1:2] == ["list"]
        return types.SimpleNamespace(
            returncode=17 if is_conda_list else 0,
            stdout="specific conda list failure" if is_conda_list else "",
        )

    monkeypatch.setattr(
        environment,
        "subprocess",
        types.SimpleNamespace(run=_fake_run, PIPE=subprocess.PIPE, STDOUT=subprocess.STDOUT),
    )

    with pytest.raises(SystemExit):
        verify._verify_impl(
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
    calls = []

    def _fake_run(cmd, *args, **kwargs):
        # Record the cwd EFFECTIVE at call time (verify() os.chdir's, it does not pass cwd=).
        calls.append((list(cmd), os.getcwd()))
        return types.SimpleNamespace(returncode=0, stdout="")

    monkeypatch.setattr(
        environment,
        "subprocess",
        types.SimpleNamespace(run=_fake_run, PIPE=subprocess.PIPE, STDOUT=subprocess.STDOUT),
    )

    workdir = tmp_path / "conda-bld" / "linux-64"
    workdir.mkdir(parents=True)
    start_cwd = os.getcwd()

    verify.verify(
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
    assert codes[0] == verify._core_probe()
    assert codes.count(verify._core_probe()) == 1
    assert "import mssql_python" not in codes[0]
    driver_calls = [
        (cmd, cwd)
        for cmd, cwd in calls
        if any(str(arg).endswith("driver_load_probe.py") for arg in cmd)
    ]
    assert len(driver_calls) == 1
    command, cwd = driver_calls[0]
    assert command[-1] == str(tmp_path / "eng" / "conda_tools" / "driver_load_probe.py")
    assert os.path.realpath(cwd) == os.path.realpath(str(workdir))


def test_verify_restores_cwd_when_the_phase_fails(tmp_path, monkeypatch):
    """The wrapper's ``finally`` must restore the original cwd even when the verify phase raises
    (a failed subprocess -> _die, or any exception) -- otherwise a failing leg would strand the
    process in the build dir and corrupt the later stage() step's relative paths. The happy-path
    test proves the chdir; this proves the restore survives the failure path."""

    def _raising_run(cmd, *args, **kwargs):
        raise RuntimeError("boom: subprocess failed")

    monkeypatch.setattr(
        environment,
        "subprocess",
        types.SimpleNamespace(run=_raising_run, PIPE=subprocess.PIPE, STDOUT=subprocess.STDOUT),
    )

    workdir = tmp_path / "conda-bld" / "linux-64"
    workdir.mkdir(parents=True)
    start_cwd = os.getcwd()

    with pytest.raises(RuntimeError):
        verify.verify(
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
    output_dir = tmp_path / "output #1" / "linux-64"
    output_dir.mkdir(parents=True)
    bld = tmp_path / "bld"
    bld.mkdir()
    (bld / "repodata.json").write_text("{}", encoding="ascii")

    channel = verify.make_verify_channel(str(output_dir), str(bld))
    channel_path = output_dir.parent / "verifychan_linux_64"

    assert channel == channel_path.resolve().as_uri()
    assert "%20" in channel
    assert "%23" in channel
    assert (channel_path / "repodata.json").read_text(encoding="ascii") == "{}"


def test_main_routes_native_effective_subdir_without_cross_target(tmp_path, monkeypatch):
    calls = {}

    monkeypatch.setattr(build, "gather_wheels", lambda *_args: ("1.2.3", "18.6.2", None))
    monkeypatch.setattr(environment, "find_or_install_conda", lambda _output_dir: "conda")
    monkeypatch.setattr(environment, "run", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(environment, "create_builder_env", lambda _conda: "conda_builder")
    monkeypatch.setattr(build, "detect_pythons", lambda *_args: ["3.13"])

    def _build_env(_mssql_ver, _odbc_ver, _links, cross_target_subdir, _rs_ver):
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

    monkeypatch.setattr(environment, "build_env", _build_env)
    monkeypatch.setattr(build, "build_packages", _build_packages)
    monkeypatch.setattr(build, "audit_packages", _audit_packages)
    monkeypatch.setattr(verify, "make_verify_channel", lambda *_args: "file:///channel")
    monkeypatch.setattr(verify, "verify", _verify)
    monkeypatch.setattr(build, "stage", _stage)

    result = cli.main(
        [
            "build",
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
    monkeypatch.setenv("CONDA_SUBDIR", "win-arm64")

    env = environment.build_env("1.2.3", "18.6.2", "wheels", "")

    assert "CONDA_SUBDIR" not in env


def test_build_env_sets_subdir_for_cross_build(monkeypatch):
    monkeypatch.setenv("CONDA_SUBDIR", "win-64")

    env = environment.build_env("1.2.3", "18.6.2", "wheels", "osx-arm64")

    assert env["CONDA_SUBDIR"] == "osx-arm64"


def test_recipe_requires_explicit_wheel_version():
    jinja2 = pytest.importorskip("jinja2", reason="Conda recipe rendering requires Jinja2")
    recipe = _ROOT / "conda" / "mssql-python" / "meta.yaml"
    template = jinja2.Environment(undefined=jinja2.StrictUndefined).from_string(
        recipe.read_text(encoding="utf-8")
    )
    env = environment.build_env("1.2.3", "18.6.2", "wheels", "")
    assert 'version: "1.2.3"' in template.render(environ=env)

    del env["MSSQL_PYTHON_VERSION"]
    with pytest.raises(jinja2.UndefinedError, match="MSSQL_PYTHON_VERSION"):
        template.render(environ=env)


def test_win_arm64_real_environment_create_failure_is_blocking(tmp_path, monkeypatch):
    """A successful solve does not prove package extraction/linking succeeds."""
    calls = []

    def _fake_run(cmd, *args, **kwargs):
        command = list(cmd)
        calls.append(command)
        is_real_create = command[1:3] == ["create", "-y"]
        return types.SimpleNamespace(returncode=17 if is_real_create else 0, stdout="")

    monkeypatch.setattr(
        environment,
        "subprocess",
        types.SimpleNamespace(run=_fake_run, PIPE=subprocess.PIPE, STDOUT=subprocess.STDOUT),
    )

    with pytest.raises(SystemExit) as exc_info:
        verify._verify_impl(
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


def _write_wheel(
    path, metadata=None, *, metadata_count=1, payload=None, wheel_tag=None, record_members=None
):
    distribution, version = path.name.split("-")[:2]
    python_tag, abi, platform_tag = path.stem.rsplit("-", 3)[1:]
    if metadata is None:
        metadata = f"Metadata-Version: 2.1\nName: {distribution}\nVersion: {version}\n"
        if distribution == "mssql_python":
            metadata += (
                "Requires-Dist: mssql-python-odbc==18.6.2\n"
                'Requires-Dist: pyarrow>=14; extra == "pyarrow"\n'
                "Requires-Dist: mssql-python-odbc-helper>=1\n"
            )
    if payload is None:
        payload = {}
        if distribution in ("mssql_python", "mssql_python_rs"):
            native = _fake_pe(0xAA64 if platform_tag == "win_arm64" else 0x8664)
            payload = {
                "mssql_py_core/__init__.py": b"",
                f"mssql_py_core/mssql_py_core.{python_tag}-{platform_tag}.pyd": bytes(native),
            }
            if distribution == "mssql_python_rs":
                arch = "arm64" if platform_tag == "win_arm64" else "x64"
                payload[f"mssql_py_core/libs/windows/{arch}/mssqlodbc.dll"] = bytes(native)
    info = f"{distribution}-{version}.dist-info"
    with zipfile.ZipFile(path, "w") as wheel:
        for name, data in payload.items():
            wheel.writestr(name, data)
        for _ in range(metadata_count):
            wheel.writestr(f"{info}/METADATA", metadata)
        wheel.writestr(
            f"{info}/WHEEL",
            f"Wheel-Version: 1.0\nTag: {wheel_tag or f'{python_tag}-{abi}-{platform_tag}'}\n",
        )
        wheel.writestr(
            f"{info}/RECORD",
            "".join(
                f"{name},,\n"
                for name in (
                    record_members
                    if record_members is not None
                    else [*payload, f"{info}/METADATA", f"{info}/WHEEL"]
                )
            ),
        )
    return metadata


def _wheel_inputs(tmp_path, odbc_names, binding_tags=("cp313-cp313-win_amd64",)):
    mssql_dir = tmp_path / "mssql"
    odbc_dir = tmp_path / "odbc"
    links = tmp_path / "links"
    mssql_dir.mkdir()
    odbc_dir.mkdir()
    for tag in binding_tags:
        _write_wheel(mssql_dir / f"mssql_python-1.2.3-{tag}.whl")
    for name in odbc_names:
        _write_wheel(odbc_dir / name)
    return mssql_dir, odbc_dir, links


@pytest.mark.parametrize(
    "requirement",
    [
        "mssql-python-odbc==18.6.2",
        "MSSQL_PYTHON_ODBC ( == 18.6.2 )",
        "mssql.python.odbc ==18.6.2",
    ],
)
def test_gather_wheels_accepts_exactly_one_odbc_match(tmp_path, requirement):
    mssql_dir, odbc_dir, links = _wheel_inputs(
        tmp_path, ["mssql_python_odbc-18.6.2-py3-none-win_amd64.whl"]
    )
    wheel = mssql_dir / "mssql_python-1.2.3-cp312-cp312-win_amd64.whl"
    metadata = _write_wheel(wheel).replace("Name: mssql_python", "Name: MSSQL.Python")
    _write_wheel(wheel, metadata.replace("mssql-python-odbc==18.6.2", requirement))

    versions = build.gather_wheels(
        str(mssql_dir), "mssql_python-*.whl", str(odbc_dir), "*.whl", str(links)
    )

    assert versions == ("1.2.3", "18.6.2", None)
    assert sorted(path.name for path in links.iterdir()) == [
        "mssql_python-1.2.3-cp312-cp312-win_amd64.whl",
        "mssql_python-1.2.3-cp313-cp313-win_amd64.whl",
        "mssql_python_odbc-18.6.2-py3-none-win_amd64.whl",
    ]


def test_gather_wheels_rejects_mixed_mssql_python_versions(tmp_path, capsys):
    mssql_dir, odbc_dir, links = _wheel_inputs(
        tmp_path, ["mssql_python_odbc-18.6.2-py3-none-win_amd64.whl"]
    )
    _write_wheel(mssql_dir / "mssql_python-9.9.9-cp312-cp312-win_amd64.whl")

    with pytest.raises(SystemExit):
        build.gather_wheels(
            str(mssql_dir), "mssql_python-*.whl", str(odbc_dir), "*.whl", str(links)
        )

    error = capsys.readouterr().err
    assert "inconsistent versions" in error
    assert "1.2.3" in error
    assert "9.9.9" in error


def test_gather_wheels_rejects_no_odbc_match(tmp_path):
    mssql_dir, odbc_dir, links = _wheel_inputs(tmp_path, [])

    with pytest.raises(SystemExit):
        build.gather_wheels(
            str(mssql_dir), "mssql_python-*.whl", str(odbc_dir), "*.whl", str(links)
        )
    assert not list(links.iterdir())


def test_gather_wheels_rejects_multiple_odbc_matches(tmp_path):
    mssql_dir, odbc_dir, links = _wheel_inputs(
        tmp_path,
        [
            "mssql_python_odbc-18.6.2-py3-none-win_amd64.whl",
            "mssql_python_odbc-18.6.3-py3-none-win_amd64.whl",
        ],
    )

    with pytest.raises(SystemExit):
        build.gather_wheels(
            str(mssql_dir), "mssql_python-*.whl", str(odbc_dir), "*.whl", str(links)
        )
    assert not list(links.iterdir())


@pytest.mark.parametrize(
    ("package", "problem"),
    [
        ("mssql", "missing-metadata"),
        ("odbc", "duplicate-metadata"),
        ("mssql", "missing-name"),
        ("mssql", "duplicate-name"),
        ("odbc", "wrong-name"),
        ("odbc", "missing-version"),
        ("odbc", "duplicate-version"),
        ("mssql", "wrong-version"),
        ("mssql", "bad-zip"),
    ],
)
def test_gather_wheels_rejects_invalid_metadata_before_copy(tmp_path, capsys, package, problem):
    mssql_dir, odbc_dir, links = _wheel_inputs(
        tmp_path, ["mssql_python_odbc-18.6.2-py3-none-win_amd64.whl"]
    )
    wheel = (
        mssql_dir / "mssql_python-1.2.3-cp312-cp312-win_amd64.whl"
        if package == "mssql"
        else next(odbc_dir.glob("*.whl"))
    )
    metadata = _write_wheel(wheel)
    if problem == "missing-metadata":
        _write_wheel(wheel, metadata_count=0)
    elif problem == "duplicate-metadata":
        with pytest.warns(UserWarning, match="Duplicate name"):
            _write_wheel(wheel, metadata_count=2)
    elif problem == "bad-zip":
        wheel.write_bytes(b"not a wheel ZIP")
    else:
        field = "Name" if problem.endswith("name") else "Version"
        line = next(
            line for line in metadata.splitlines(keepends=True) if line.startswith(field + ":")
        )
        replacement = f"{field}: {'unrelated-package' if field == 'Name' else '9.9.9'}\n"
        if problem.startswith("missing"):
            replacement = ""
        elif problem.startswith("duplicate"):
            replacement = line + line
        _write_wheel(wheel, metadata.replace(line, replacement))

    with pytest.raises(SystemExit) as error:
        build.gather_wheels(
            str(mssql_dir), "mssql_python-*.whl", str(odbc_dir), "*.whl", str(links)
        )
    assert error.value.code == 1
    assert wheel.name in capsys.readouterr().err
    assert not list(links.iterdir())


@pytest.mark.parametrize(
    "requirements",
    [
        [],
        ["mssql-python-odbc-helper==18.6.2"],
        ["mssql-python-odbc==18.6.3"],
        ["mssql-python-odbc>=18.6.2"],
        ["mssql-python-odbc==18.6.*"],
        ["mssql-python-odbc===18.6.2"],
        ['mssql-python-odbc==18.6.2; python_version >= "3.10"'],
        ["mssql-python-odbc[extra]==18.6.2"],
        ["mssql-python-odbc (==18.6.2"],
        ["mssql-python-odbc==18.6.2", "mssql-python-odbc==18.6.3"],
        ["mssql-python-odbc==18.6.2,<19"],
        ["mssql-python-odbc @ https://example.invalid/driver.whl"],
    ],
)
def test_gather_wheels_requires_exact_actual_odbc_pair(tmp_path, capsys, requirements):
    mssql_dir, odbc_dir, links = _wheel_inputs(
        tmp_path, ["mssql_python_odbc-18.6.2-py3-none-win_amd64.whl"]
    )
    wheel = next(mssql_dir.glob("*.whl"))
    _write_wheel(
        wheel,
        "Metadata-Version: 2.1\nName: mssql-python\nVersion: 1.2.3\n"
        + "".join(f"Requires-Dist: {requirement}\n" for requirement in requirements),
    )
    with pytest.raises(SystemExit) as error:
        build.gather_wheels(
            str(mssql_dir), "mssql_python-*.whl", str(odbc_dir), "*.whl", str(links)
        )
    assert error.value.code == 1
    assert "one unconditional exact mssql-python-odbc==18.6.2" in capsys.readouterr().err
    assert not list(links.iterdir())


def _rs_inputs(tmp_path, platform="win_amd64", python_tags=("cp313",)):
    code, odbc, links = _wheel_inputs(
        tmp_path,
        [f"mssql_python_odbc-18.6.2-py3-none-{platform}.whl"],
        [f"{tag}-{tag}-{platform}" for tag in python_tags],
    )
    for binding in code.glob("*.whl"):
        metadata = _write_wheel(binding) + "Requires-Dist: mssql-python-rs==0.1.0\n"
        _write_wheel(binding, metadata, payload={})
    rs = tmp_path / "rs"
    rs.mkdir()
    for tag in python_tags:
        _write_wheel(rs / f"mssql_python_rs-0.1.0-{tag}-{tag}-{platform}.whl")
    return code, odbc, links, rs


@pytest.mark.parametrize(
    ("platform", "subdir", "matches"),
    [
        ("win_amd64", "win-64", True),
        ("win_arm64", "win-arm64", True),
        ("win_amd64", "win-arm64", False),
        ("manylinux_2_28_x86_64", "linux-64", True),
        ("manylinux_2_34_aarch64", "linux-aarch64", True),
        ("manylinux2014_x86_64.manylinux_2_28_x86_64", "linux-64", True),
        ("manylinux_2_28_x86_64", "linux-aarch64", False),
        ("musllinux_1_2_x86_64", "linux-64", False),
        ("macosx_12_0_universal2", "osx-64", True),
        ("macosx_12_0_universal2", "osx-arm64", True),
        ("macosx_15_0_arm64", "osx-arm64", True),
        ("macosx_15_0_x86_64", "osx-arm64", False),
    ],
)
def test_binding_selection_uses_target_platform_tags(platform, subdir, matches):
    wheel = f"mssql_python-1.2.3-cp313-cp313-{platform}.whl"
    assert contracts.binding_wheel_matches_target(wheel, subdir, ["3.13"]) is matches
    assert not contracts.binding_wheel_matches_target(wheel, subdir, ["3.12"])


@pytest.mark.parametrize("state", ["valid", "missing-rs", "mismatched-rs"])
def test_build_filters_consolidated_bindings_before_rs_resolution(
    tmp_path, monkeypatch, capsys, state
):
    tags = ("cp312", "cp313", "cp314")
    code, odbc, _, rs = _rs_inputs(tmp_path, "win_arm64", tags)
    for suffix in (
        "1.2.3-cp310-cp310-win_arm64",
        "1.2.3-cp311-cp311-win_arm64",
        "1.2.3-cp313-cp313-win_amd64",
        "9.9.9-cp313-cp313-manylinux_2_28_x86_64",
    ):
        _write_wheel(code / f"mssql_python-{suffix}.whl", "irrelevant metadata", payload={})
    selected_rs = rs / "mssql_python_rs-0.1.0-cp313-cp313-win_arm64.whl"
    if state == "missing-rs":
        selected_rs.unlink()
    elif state == "mismatched-rs":
        _write_wheel(selected_rs, "Name: mssql-python-rs\nVersion: 0.2.0\n")
    bootstrap_calls = []

    def stop_before_conda(output_dir):
        bootstrap_calls.append(output_dir)
        raise RuntimeError("input selection completed")

    monkeypatch.setattr(environment, "find_or_install_conda", stop_before_conda)
    with pytest.raises(RuntimeError if state == "valid" else SystemExit) as error:
        cli.main(
            [
                "build",
                "--mssql-wheel-dir",
                str(code),
                "--odbc-wheel-dir",
                str(odbc),
                "--odbc-wheel-filter",
                "*.whl",
                "--rs-wheel-dir",
                str(rs),
                "--recipe-root",
                str(_ROOT / "conda"),
                "--output-dir",
                str(tmp_path / "out"),
                "--stage-dir",
                str(tmp_path / "stage"),
                "--conda-subdir",
                "win-64",
                "--conda-target-subdir",
                "win-arm64",
                "--python-versions",
                "3.12, 3.13,3.14",
            ]
        )
    links = tmp_path / "out" / "win-64" / "wheels"
    if state != "valid":
        assert error.value.code == 1
        assert bootstrap_calls == []
        assert not list(links.iterdir())
        expected = "cp313 win-arm64; found []" if state == "missing-rs" else "METADATA Version"
        assert expected in capsys.readouterr().err
        return
    assert str(error.value) == "input selection completed"
    assert bootstrap_calls == [str(tmp_path / "out" / "win-64")]
    assert {wheel.name for wheel in links.glob("mssql_python-*.whl")} == {
        f"mssql_python-1.2.3-{tag}-{tag}-win_arm64.whl" for tag in tags
    }
    assert {wheel.name for wheel in links.glob("mssql_python_rs-*.whl")} == {
        f"mssql_python_rs-0.1.0-{tag}-{tag}-win_arm64.whl" for tag in tags
    }
    for tag in tags:
        assert (links / f"rs-wheel-{tag}.txt").read_text().strip() == (
            f"mssql_python_rs-0.1.0-{tag}-{tag}-win_arm64.whl"
        )
    assert not (links / "rs-wheel-cp310.txt").exists()
    assert build.detect_pythons(str(links), "") == ["3.12", "3.13", "3.14"]


@pytest.mark.parametrize(("subdir", "python_versions"), [("win-arm64", "3.13"), ("win-64", "3.12")])
def test_binding_selection_fails_when_no_requested_inputs_match(
    tmp_path, capsys, subdir, python_versions
):
    code, odbc, links, rs = _rs_inputs(tmp_path)
    with pytest.raises(SystemExit) as error:
        build.gather_wheels(
            str(code),
            "*.whl",
            str(odbc),
            "*.whl",
            str(links),
            str(rs),
            None,
            subdir,
            python_versions,
        )
    assert error.value.code == 1
    assert "no mssql-python wheels match target" in capsys.readouterr().err
    assert not list(links.iterdir())


def test_binding_selection_reports_malformed_candidate(tmp_path, capsys):
    assert not contracts.binding_wheel_matches_target(
        "mssql_python-1.2.3-cp313invalid-cp313-win_arm64.whl", "win-64", ["3.13"]
    )
    code, odbc, links, rs = _rs_inputs(tmp_path)
    (code / "mssql_python-1.2.3-cp313invalid-cp313-win_amd64.whl").write_bytes(b"invalid wheel")
    with pytest.raises(SystemExit) as error:
        build.gather_wheels(
            str(code), "*.whl", str(odbc), "*.whl", str(links), str(rs), None, "win-64", "3.13"
        )
    assert error.value.code == 1
    assert "invalid binding wheel Python tag" in capsys.readouterr().err
    assert not list(links.iterdir())


@pytest.mark.parametrize("abi3", [False, True])
def test_gather_selects_real_target_and_preserves_whole_rs_wheel(tmp_path, abi3):
    code, odbc, links, rs = _rs_inputs(tmp_path)
    _write_wheel(
        code / "mssql_python-9.9.9-cp310-cp310-win_arm64.whl", "foreign metadata", payload={}
    )
    selected = next(rs.glob("*.whl"))
    if abi3:
        with zipfile.ZipFile(selected) as wheel:
            payload = {
                name: wheel.read(name)
                for name in wheel.namelist()
                if name.startswith("mssql_py_core/")
            }
        core = next(name for name in payload if name.endswith(".pyd"))
        payload["mssql_py_core/mssql_py_core.pyd"] = payload.pop(core)
        selected.unlink()
        selected = rs / "mssql_python_rs-0.1.0-cp310-abi3-win_amd64.whl"
        _write_wheel(selected, payload=payload)
    _write_wheel(rs / "mssql_python_rs-0.1.0-cp313-cp313-win_arm64.whl")
    _write_wheel(rs / "mssql_python_rs-0.1.0-cp312-cp312-win_amd64.whl")
    assertion = tmp_path / "mssql-python-rs.version"
    assertion.write_text("0.1.0\n")
    result = build.gather_wheels(
        str(code), "*.whl", str(odbc), "*.whl", str(links), str(rs), str(assertion), "win-64"
    )
    assert result == ("1.2.3", "18.6.2", "0.1.0")
    assert (links / selected.name).read_bytes() == selected.read_bytes()
    assert list(links.glob("mssql_python_rs-*.whl")) == [links / selected.name]
    assert (links / "rs-wheel-cp313.txt").read_text().strip() == selected.name
    assert build.detect_pythons(str(links), "") == ["3.13"]
    env = environment.build_env(*result[:2], str(links), "win-arm64", result[2])
    assert env["MSSQL_RS_VERSION"] == "0.1.0"
    assert env["CONDA_SUBDIR"] == "win-arm64"


@pytest.mark.parametrize(
    "declarations",
    [
        ["mssql-python-rs>=0.1.0"],
        ["mssql-python-rs==0.1.*"],
        ["mssql-python-rs==0.1.0; python_version >= '3.10'"],
        ["mssql-python-rs[extra]==0.1.0"],
        ["mssql-python-rs==0.1.0", "mssql-python-rs==0.1.0"],
        ["mssql-python-rs===0.1.0"],
        ["mssql-python-rs @ https://example.invalid/core.whl"],
    ],
)
def test_malformed_rs_declarations_never_become_legacy(declarations):
    metadata = archive.parse_distribution_metadata(
        (
            "Name: mssql-python\nVersion: 1.2.3\n"
            + "".join(f"Requires-Dist: {value}\n" for value in declarations)
        ).encode()
    )
    core = ["mssql_py_core/__init__.py", "mssql_py_core/mssql_py_core.pyd"]
    with pytest.raises(ValueError, match="unconditional exact mssql-python-rs"):
        contracts.binding_rs_version(metadata, core, core)


@pytest.mark.parametrize(
    "state",
    [
        "missing-wheel",
        "wrong-name",
        "wrong-version",
        "wrong-wheel-tag",
        "missing-init",
        "missing-core",
        "missing-private-library",
        "unowned-core",
        "wrong-arch",
        "wrong-core-tag",
        "binding-owns-core",
        "mixed-profiles",
    ],
)
def test_rs_inputs_fail_before_any_wheel_is_staged(tmp_path, state):
    code, odbc, links, rs = _rs_inputs(tmp_path)
    wheel = next(rs.glob("*.whl"))
    with zipfile.ZipFile(wheel) as source:
        payload = {
            name: source.read(name)
            for name in source.namelist()
            if name.startswith("mssql_py_core/")
        }
        metadata = source.read("mssql_python_rs-0.1.0.dist-info/METADATA").decode()
    core = next(name for name in payload if name.endswith(".pyd"))
    options = {}
    if state == "wrong-name":
        metadata = metadata.replace("Name: mssql_python_rs", "Name: another-package")
    elif state == "wrong-version":
        metadata = metadata.replace("Version: 0.1.0", "Version: 0.2.0")
    elif state == "wrong-wheel-tag":
        options["wheel_tag"] = "cp312-cp312-win_amd64"
    elif state == "missing-init":
        del payload["mssql_py_core/__init__.py"]
    elif state == "missing-core":
        del payload[core]
    elif state == "missing-private-library":
        del payload["mssql_py_core/libs/windows/x64/mssqlodbc.dll"]
    elif state == "unowned-core":
        options["record_members"] = [name for name in payload if name != core]
    elif state == "wrong-arch":
        payload[core] = _fake_pe(0xAA64)
    elif state == "wrong-core-tag":
        payload[core.replace("cp313", "cp312")] = payload.pop(core)
    elif state == "binding-owns-core":
        binding = next(code.glob("*.whl"))
        _write_wheel(binding, _write_wheel(binding) + "Requires-Dist: mssql-python-rs==0.1.0\n")
    elif state == "mixed-profiles":
        _write_wheel(code / "mssql_python-1.2.3-cp312-cp312-win_amd64.whl")
    _write_wheel(wheel, metadata, payload=payload, **options)
    if state == "missing-wheel":
        wheel.unlink()
    requested = "3.12,3.13" if state == "mixed-profiles" else "3.13"
    with pytest.raises(SystemExit) as error:
        build.gather_wheels(
            str(code), "*.whl", str(odbc), "*.whl", str(links), str(rs), None, "win-64", requested
        )
    assert error.value.code == 1
    assert not list(links.iterdir())


def test_source_rs_assertion_rejects_a_same_version_legacy_binding(tmp_path, capsys):
    code, odbc, links = _wheel_inputs(tmp_path, ["mssql_python_odbc-18.6.2-py3-none-win_amd64.whl"])
    assertion = tmp_path / "mssql-python-rs.version"
    assertion.write_text("0.1.0")
    with pytest.raises(SystemExit):
        build.gather_wheels(
            str(code), "*.whl", str(odbc), "*.whl", str(links), None, str(assertion), "win-64"
        )
    assert "producer source" in capsys.readouterr().err
    assert not list(links.iterdir())


def test_rs_linux_selection_rejects_the_openssl_1_1_manylinux_variant(tmp_path):
    _write_wheel(tmp_path / "mssql_python_rs-0.1.0-cp313-cp313-manylinux_2_28_x86_64.whl")
    with pytest.raises(ValueError, match="expected exactly one"):
        build._select_rs_wheel(str(tmp_path), "0.1.0", "cp313", "linux-64")


@pytest.mark.parametrize("record", [b"missing-columns\n", b"name,,\nname,,\n", b"\xff,,\n"])
def test_malformed_record_is_an_explicit_error(record):
    with pytest.raises(ValueError):
        archive.parse_record_members(record)


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
    calls = []
    croot = tmp_path / "croot"
    croot.mkdir()
    (croot / "stale").write_text("stale", encoding="ascii")

    def _capture_run(command, **_kwargs):
        calls.append(list(command))

    monkeypatch.setattr(environment, "run", _capture_run)

    build.build_packages(
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
        exec(verify._core_probe(), {})
    else:
        with pytest.raises(AssertionError, match="native extension|outside installed prefix"):
            exec(verify._core_probe(), {})


def test_core_failure_blocks_api_preload(tmp_path, monkeypatch):
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append(list(cmd))
        return types.SimpleNamespace(returncode=17 if verify._core_probe() in cmd else 0, stdout="")

    monkeypatch.setattr(
        environment,
        "subprocess",
        types.SimpleNamespace(run=fake_run, PIPE=subprocess.PIPE, STDOUT=subprocess.STDOUT),
    )
    with pytest.raises(SystemExit):
        verify._verify_impl(
            "conda", "channel", str(tmp_path), ["3.12"], "1.14.0", "linux-64", False, {}
        )
    assert any(verify._core_probe() in cmd for cmd in calls)
    assert not any("BINDING_OK" in str(cmd) for cmd in calls)


@pytest.mark.parametrize("subdir", ["win-64", "win-arm64"])
def test_both_windows_targets_run_native_audit(subdir, monkeypatch):
    calls = []
    monkeypatch.setattr(environment, "run", lambda cmd, **kwargs: calls.append(cmd))
    build.audit_packages("conda", "builder", str(_ROOT / "conda"), "output", subdir, {})
    pe_calls = [cmd for cmd in calls if cmd[4:8] == ["python", "-m", "eng.conda_tools", "pe"]]
    assert len(pe_calls) == 1
    assert pe_calls[0][-2:] == ["--subdir", subdir]


@pytest.mark.parametrize("inherited", [None, "true"])
def test_build_does_not_automatically_accept_channel_terms(monkeypatch, inherited):
    if inherited is None:
        monkeypatch.delenv("CONDA_PLUGINS_AUTO_ACCEPT_TOS", raising=False)
    else:
        monkeypatch.setenv("CONDA_PLUGINS_AUTO_ACCEPT_TOS", inherited)
    assert "CONDA_PLUGINS_AUTO_ACCEPT_TOS" not in environment.build_env(
        "1.14.0", "18.6.2.1", "wheels", "win-arm64"
    )
    assert os.environ.get("CONDA_PLUGINS_AUTO_ACCEPT_TOS") == inherited


@pytest.mark.parametrize("subdir", ["win-64", "win-arm64"])
def test_main_routes_effective_target_to_native_audit(subdir, tmp_path, monkeypatch):
    targets = []
    monkeypatch.setattr(build, "gather_wheels", lambda *args: ("1.14.0", "18.6.2.1", None))
    monkeypatch.setattr(environment, "find_or_install_conda", lambda *args: "conda")
    monkeypatch.setattr(environment, "create_builder_env", lambda *args: "builder")
    monkeypatch.setattr(build, "detect_pythons", lambda *args: ["3.12"])
    monkeypatch.setattr(verify, "make_verify_channel", lambda *args: "channel")
    for owner, name in (
        (environment, "run"),
        (build, "build_packages"),
        (verify, "verify"),
        (build, "stage"),
    ):
        monkeypatch.setattr(owner, name, lambda *args, **kwargs: None)
    monkeypatch.setattr(build, "audit_packages", lambda *args: targets.append(args[-2]))
    args = [
        "--mssql-wheel-dir",
        str(tmp_path / "wheels"),
        "--odbc-wheel-dir",
        str(tmp_path / "wheels"),
        "--odbc-wheel-filter",
        "*.whl",
        "--recipe-root",
        str(_ROOT / "conda"),
        "--output-dir",
        str(tmp_path / "out"),
        "--stage-dir",
        str(tmp_path / "stage"),
        "--conda-subdir",
        subdir,
    ]
    if subdir == "win-arm64":
        args += ["--conda-target-subdir", subdir]
    assert cli.main(["build", *args]) == 0
    assert targets == [subdir]


def test_build_audit_reports_missing_source_checkout(tmp_path, monkeypatch, capsys):
    calls = []
    monkeypatch.setattr(environment, "run", lambda *args, **kwargs: calls.append(args))
    with pytest.raises(SystemExit) as error:
        build.audit_packages("conda", "builder", str(tmp_path / "conda"), "output", "win-64", {})
    assert error.value.code == 1
    assert (
        "--recipe-root must point to the source checkout's conda directory"
        in capsys.readouterr().err
    )
    assert calls == []


def test_build_module_help():
    result = subprocess.run(
        [sys.executable, "-m", "eng.conda_tools", "build", "--help"],
        cwd=_ROOT,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "--mssql-wheel-dir" in result.stdout
    assert "--conda-target-subdir" in result.stdout
