# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Source-only contracts for runtime selection and the pre-ESRP release gates."""

import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
from types import SimpleNamespace
import uuid
import zipfile

import pytest

ROOT = Path(__file__).resolve().parents[1]
PIPELINES = ROOT / "OneBranchPipelines"
SOURCE_PREREQUISITES = (
    "OneBranchPipelines/scripts/validate-release-versions.ps1",
    "eng/scripts/mssql_python_rs.py",
    "eng/scripts/resolve_pr_rs_runtime.py",
    "eng/conda_tools/archive.py",
    "eng/conda_tools/contracts.py",
)
if not all((ROOT / relative).is_file() for relative in SOURCE_PREREQUISITES):
    pytest.skip("Pipeline contracts require a complete source checkout", allow_module_level=True)

import yaml

VERSION_ENV = "MSSQL_PYTHON_RS_VERSION"
ENTRY_POINTS = (
    "eng/pipelines/pr-validation-pipeline.yml",
    "eng/pipelines/mssql-odbc-daily-validation-pipeline.yml",
    "OneBranchPipelines/build-release-package-pipeline.yml",
    "OneBranchPipelines/stress-test-pipeline.yml",
)


@pytest.mark.parametrize("filename", ENTRY_POINTS)
def test_pipeline_override_is_explicit_and_defaults_to_empty(filename):
    source = (ROOT / filename).read_text(encoding="utf-8")
    assert re.search(r"name: rsVersionOverride[\s\S]*?default: ''", source)
    assert VERSION_ENV in source
    assert "${{ parameters.rsVersionOverride }}" in source
    assert "$(rsVersionOverride)" not in source
    assert "$(MSSQL_PYTHON_RS_VERSION)" not in source


def test_pr_validation_resolves_nightly_once_and_shares_it_across_all_jobs():
    pipeline = yaml.safe_load((ROOT / ENTRY_POINTS[0]).read_text(encoding="utf-8"))
    assert VERSION_ENV not in pipeline["variables"]
    jobs = pipeline["jobs"]
    selectors = [job for job in jobs if job["job"] == "SelectRsRuntime"]
    assert len(selectors) == 1
    selector = selectors[0]
    selection_steps = [
        step
        for job in jobs
        for step in job["steps"]
        if "resolve_pr_rs_runtime.py" in step.get("script", "")
    ]
    assert len(selection_steps) == 1
    selection = selection_steps[0]
    assert selection in selector["steps"]
    assert selection["name"] == "select"
    assert selection["env"][VERSION_ENV] == "${{ parameters.rsVersionOverride }}"
    assert "--output-dir" in selection["script"]
    artifacts = [
        step["inputs"]
        for step in selector["steps"]
        if step.get("task") == "PublishPipelineArtifact@1"
    ]
    assert artifacts == [
        {
            "targetPath": "$(Build.ArtifactStagingDirectory)/rs-runtime",
            "artifact": "rs-runtime-selection",
        }
    ]
    consumers = [job for job in jobs if job is not selector]
    assert len(consumers) == 10
    for job in consumers:
        assert job["dependsOn"] == "SelectRsRuntime", job["job"]
        assert job["variables"][VERSION_ENV] == (
            "$[ dependencies.SelectRsRuntime.outputs['select.version'] ]"
        ), job["job"]
        assert "condition" not in job, "A failed selection must block all consumers"


def test_official_entry_points_keep_stable_defaults_without_nightly_resolution():
    for path in (
        PIPELINES / "build-release-package-pipeline.yml",
        PIPELINES / "official-release-pipeline.yml",
        PIPELINES / "conda-release-pipeline.yml",
    ):
        source = path.read_text(encoding="utf-8")
        assert "resolve_pr_rs_runtime.py" not in source, path
        assert "SelectRsRuntime" not in source, path
    pipeline = yaml.safe_load(
        (PIPELINES / "build-release-package-pipeline.yml").read_text(encoding="utf-8")
    )
    override = next(p for p in pipeline["parameters"] if p["name"] == "rsVersionOverride")
    assert override["default"] == ""


def test_modified_pipeline_yaml_parses():
    for folder in (ROOT / "eng/pipelines", PIPELINES):
        for path in folder.rglob("*.yml"):
            assert isinstance(yaml.safe_load(path.read_text(encoding="utf-8")), dict), path


def test_conda_pipeline_no_longer_passes_removed_transport_provenance():
    for path in (
        PIPELINES / "conda-release-pipeline.yml",
        PIPELINES / "steps/conda-publish-step.yml",
        PIPELINES / "steps/conda-release-step.yml",
    ):
        source = path.read_text(encoding="utf-8")
        for obsolete in ("rsTransportVersion", "RS_TRANSPORT_VERSION", "--rs-transport-version"):
            assert obsolete not in source, path
        assert "releaseVersions" in source
        assert "mssqlPythonVersion" in source


def test_every_runtime_container_inherits_version_without_shell_interpolation():
    for filename in ENTRY_POINTS[:2] + ("OneBranchPipelines/stages/build-linux-single-stage.yml",):
        source = (ROOT / filename).read_text(encoding="utf-8")
        runs = [
            line
            for line in source.splitlines()
            if not line.lstrip().startswith("#")
            and "docker run " in line
            and ("--name test-container" in line or "--name build-" in line)
        ]
        if "daily" in filename:
            runs = [line for line in source.splitlines() if "docker run " in line]
        assert runs, filename
        assert all("-e MSSQL_PYTHON_RS_VERSION " in line for line in runs)
        assert not re.search(r"-e MSSQL_PYTHON_RS_VERSION=", source)
    installer = (ROOT / "eng/pipelines/steps/install-mssql-py-core.yml").read_text(encoding="utf-8")
    assert "docker exec -e MSSQL_PYTHON_RS_VERSION " in installer


def test_consolidation_stages_effective_receipt_version_not_source_pin():
    source = (PIPELINES / "jobs/consolidate-artifacts-job.yml").read_text(encoding="utf-8")
    assert "receipt['distribution_version']" in source
    assert "dist/transport.json" in source
    assert "filePath: OneBranchPipelines/scripts/validate-rs-wheel-dependency.ps1" in source
    assert "mssql-python-rs-nuget.version" not in source
    assert 'cp "$(Build.SourcesDirectory)/eng/versions/mssql-python-rs.version"' not in source
    linux = (PIPELINES / "stages/build-linux-single-stage.yml").read_text(encoding="utf-8")
    assert "mssql-python-rs-nuget.version" not in linux


def test_stress_uses_selected_build_and_checks_effective_runtime_metadata():
    source = (PIPELINES / "stress-test-pipeline.yml").read_text(encoding="utf-8")
    assert source.count("pipelineId: '$(resources.pipeline.buildPipeline.runID)'") == 4
    assert (
        source.count("filePath: OneBranchPipelines/scripts/validate-rs-wheel-dependency.ps1") == 2
    )
    assert source.count("'--no-index', '--no-deps'") == 2
    assert source.count("'mssql-python-rs=='+resolve_runtime().version") == 2


@pytest.fixture(params=("StressTestWindows", "StressTestLinux"))
def stress_job(request):
    pipeline = yaml.safe_load((PIPELINES / "stress-test-pipeline.yml").read_text(encoding="utf-8"))
    return next(job for job in pipeline["jobs"] if job["job"] == request.param)


@pytest.mark.parametrize(
    "missing",
    (
        "eng/scripts/mssql_python_rs.py",
        "eng/scripts/resolve_pr_rs_runtime.py",
        "eng/conda_tools/archive.py",
        "eng/conda_tools/contracts.py",
    ),
)
def test_pipeline_contracts_skip_incomplete_wheel_fixture(tmp_path, missing):
    shutil.copytree(PIPELINES, tmp_path / "OneBranchPipelines")
    for relative in SOURCE_PREREQUISITES[1:]:
        if relative != missing:
            path = tmp_path / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.touch()
    copied_test = tmp_path / "tests" / Path(__file__).name
    copied_test.parent.mkdir()
    shutil.copy2(__file__, copied_test)
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "--noconftest",
            "--collect-only",
            "-q",
            "-rs",
            str(copied_test),
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 5, result.stdout + result.stderr
    assert "complete source checkout" in result.stdout
    assert "SKIPPED [1]" in result.stdout


def test_pipeline_contracts_require_yaml_in_complete_checkout():
    command = (
        "import sys, pytest; sys.modules['yaml'] = None; "
        "raise SystemExit(pytest.main("
        + repr(["--noconftest", "--collect-only", "-q", str(Path(__file__).resolve())])
        + "))"
    )
    result = subprocess.run(
        [sys.executable, "-c", command],
        cwd=ROOT,
        env=dict(os.environ, PYTEST_DISABLE_PLUGIN_AUTOLOAD="1"),
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 2, result.stdout + result.stderr
    assert "ModuleNotFoundError" in result.stdout
    assert "yaml" in result.stdout


@pytest.mark.parametrize("shadow", (None, "mssql_python_odbc", "mssql_python"))
def test_stress_checks_import_provenance_before_binding_load(stress_job, tmp_path, shadow):
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    installed = tmp_path / "checkout-site-packages"
    for name in ("mssql_python_odbc", "mssql_python"):
        package = installed / name
        package.mkdir(parents=True)
        (package / "__init__.py").write_text(
            "print('BINDING_IMPORTED')\n" if name == "mssql_python" else "",
            encoding="utf-8",
        )
    if shadow:
        package = checkout / shadow
        package.mkdir()
        (package / "__init__.py").write_text(
            "print('BINDING_IMPORTED')\n" if shadow == "mssql_python" else "",
            encoding="utf-8",
        )
    step = next(
        step
        for step in stress_job["steps"]
        if step.get("displayName") == "Ensure tests use installed wheel"
    )
    script = step.get("script", step.get("powershell"))
    command = next(line.strip() for line in script.splitlines() if 'python -c "' in line)
    code = command.removeprefix('python -c "').removesuffix('"')
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=checkout,
        env=dict(
            os.environ,
            PYTHONPATH=str(installed),
            BUILD_SOURCESDIRECTORY=str(checkout),
        ),
        capture_output=True,
        text=True,
        timeout=30,
    )
    if shadow:
        assert result.returncode != 0, result.stdout
        assert f"{shadow} resolved inside checkout:" in result.stderr
        if shadow == "mssql_python_odbc":
            assert "BINDING_IMPORTED" not in result.stdout
    else:
        assert result.returncode == 0, result.stderr
        assert str(installed) in result.stdout
        assert "BINDING_IMPORTED" in result.stdout
    renaming = script[: script.index('python -c "')]
    assert "mssql_python_odbc" in renaming
    assert "mssql_python" in renaming
    assert "set -e" in script if "script" in step else "$ErrorActionPreference" in script


def _stress_wheel(folder, name, version, requirements=()):
    folder.mkdir(parents=True, exist_ok=True)
    stem = f"{name.replace('-', '_')}-{version}"
    path = folder / f"{stem}-py3-none-any.whl"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(
            f"{stem}.dist-info/METADATA",
            f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n"
            + "".join(f"Requires-Dist: {requirement}\n" for requirement in requirements),
        )
        archive.writestr(
            f"{stem}.dist-info/WHEEL",
            "Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
        )
        archive.writestr(f"{stem}.dist-info/RECORD", "")
    return path


def _stress_install_command(stress_job, workspace, monkeypatch):
    from eng.scripts import mssql_python_rs

    calls = []
    monkeypatch.setattr(subprocess, "check_call", calls.append)
    monkeypatch.setattr(
        mssql_python_rs, "resolve_runtime", lambda: SimpleNamespace(version="0.3.1.dev123")
    )
    step = next(
        step
        for step in stress_job["steps"]
        if step.get("displayName") == "Install pre-built mssql-python wheel"
    )
    script = step["script"]
    command = next(line.strip() for line in script.splitlines() if 'python -c "' in line)
    code = command.removeprefix('python -c "').removesuffix('"')
    source_path = (
        str(workspace) if stress_job["job"] == "StressTestWindows" else workspace.as_posix()
    )
    code = code.replace("$(Build.SourcesDirectory)", source_path).replace(
        "$(Pipeline.Workspace)", source_path
    )
    if os.name != "nt":
        code = code.replace("\\", "/")
    exec(compile(code, "<stress-install>", "exec"), {})
    assert len(calls) == 1
    assert script.index("python -c ") < script.index("mssql_python_rs.py --verify")
    return calls[0]


def test_stress_downloads_odbc_from_same_selected_build(stress_job):
    downloads = {
        step["inputs"]["artifactName"]: step["inputs"]
        for step in stress_job["steps"]
        if step.get("task") == "DownloadPipelineArtifact@2"
    }
    binding = downloads["drop_Consolidate_ConsolidateArtifacts"]
    odbc = downloads["drop_ConsolidateOdbc_ConsolidateArtifacts"]
    for key in ("buildType", "project", "definition", "buildVersionToDownload", "pipelineId"):
        assert binding[key] == odbc[key]
    assert odbc["buildType"] == odbc["buildVersionToDownload"] == "specific"
    assert odbc["pipelineId"] == "$(resources.pipeline.buildPipeline.runID)"
    assert odbc["targetPath"].replace("\\", "/") == "$(Pipeline.Workspace)/odbc-wheels"


@pytest.mark.parametrize("odbc_available", (True, False))
def test_stress_installs_metadata_pinned_odbc_offline(
    stress_job, workspace, monkeypatch, odbc_available
):
    _stress_wheel(
        workspace / "wheels/dist",
        "mssql-python",
        "9.8.7",
        ("mssql-python-odbc==8.7.6", "mssql-python-rs==0.3.1.dev123"),
    )
    odbc_dir = workspace / "odbc-wheels/dist"
    _stress_wheel(odbc_dir, "mssql-python-odbc", "99.0.0")
    if odbc_available:
        _stress_wheel(odbc_dir, "mssql-python-odbc", "8.7.6")
    _stress_wheel(workspace / "mssql-python-rs-wheels", "mssql-python-rs", "0.3.1.dev123")
    command = _stress_install_command(stress_job, workspace, monkeypatch)
    assert "mssql-python-odbc==8.7.6" in command
    assert "mssql-python-rs==0.3.1.dev123" in command
    assert "--no-index" in command and "--no-deps" in command
    links = [command[index + 1] for index, value in enumerate(command) if value == "--find-links"]
    assert str(odbc_dir) in [str(Path(link)) for link in links]
    result = subprocess.run(
        [*command, "--dry-run", "--ignore-installed"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert (result.returncode == 0) == odbc_available, result.stdout + result.stderr
    if odbc_available:
        assert "mssql-python-odbc-8.7.6" in result.stdout
    else:
        assert "mssql-python-odbc==8.7.6" in result.stderr


@pytest.mark.parametrize(
    "requirements",
    (
        (),
        ("mssql-python-odbc>=8.7.6",),
        ("mssql-python-odbc==8.7.*",),
        ("mssql-python-odbc==8.7.6; sys_platform == 'win32'",),
        ("mssql-python-odbc==8.7.6", "mssql-python-odbc==8.7.5"),
    ),
)
def test_stress_rejects_missing_or_nonexact_odbc_pin(
    stress_job, workspace, monkeypatch, requirements
):
    _stress_wheel(workspace / "wheels/dist", "mssql-python", "9.8.7", requirements)
    with pytest.raises((ValueError, TypeError)):
        _stress_install_command(stress_job, workspace, monkeypatch)


def test_stress_rejects_inconsistent_binding_odbc_pins(stress_job, workspace, monkeypatch):
    for version, pin in (("9.8.7", "8.7.6"), ("9.8.8", "8.7.5")):
        _stress_wheel(
            workspace / "wheels/dist", "mssql-python", version, (f"mssql-python-odbc=={pin}",)
        )
    with pytest.raises(ValueError):
        _stress_install_command(stress_job, workspace, monkeypatch)


def test_coverage_selects_python_before_dependencies_native_build_and_runtime():
    pipeline = yaml.safe_load((ROOT / ENTRY_POINTS[0]).read_text(encoding="utf-8"))
    job = next(job for job in pipeline["jobs"] if job["job"] == "CodeCoverageReport")
    steps = job["steps"]
    selectors = [
        index for index, step in enumerate(steps) if step.get("task") == "UsePythonVersion@0"
    ]
    assert len(selectors) == 1
    selected = selectors[0]
    assert steps[selected]["inputs"]["versionSpec"] == "3.12"
    assert steps[selected]["inputs"]["addToPath"] is True
    for required in ("pip install", "./build.sh codecov", "./generate_codecov.sh"):
        assert any(required in step.get("script", "") for step in steps), required
    assert any(step.get("template") == "steps/install-mssql-py-core.yml" for step in steps)
    for index, step in enumerate(steps):
        uses_python = re.search(
            r"\b(?:python\d*|pip\d*)\b|(?:build|generate_codecov)\.sh",
            step.get("script", ""),
        )
        if uses_python or step.get("template") == "steps/install-mssql-py-core.yml":
            assert selected < index, step


def test_runtime_is_verified_after_dependency_setup_before_testing():
    pr = (ROOT / ENTRY_POINTS[0]).read_text(encoding="utf-8")
    test_commands = [
        line
        for line in pr.splitlines()
        if not line.lstrip().startswith("#") and "python -m pytest " in line
    ]
    assert test_commands
    assert all("mssql_python_rs.py --verify && python -m pytest " in line for line in test_commands)
    daily = (ROOT / ENTRY_POINTS[1]).read_text(encoding="utf-8")
    assert "python eng/scripts/mssql_python_rs.py --verify || exit 2" in daily
    linux = (PIPELINES / "stages/build-linux-single-stage.yml").read_text(encoding="utf-8")
    assert (
        linux.count(
            '$PY -m pip install -q "$WHEEL";\n                    $PY /workspace/eng/scripts/mssql_python_rs.py --verify;'
        )
        == 2
    )
    for platform in ("windows", "macos"):
        source = (PIPELINES / f"stages/build-{platform}-single-stage.yml").read_text(
            encoding="utf-8"
        )
        verify = source.index("python eng/scripts/mssql_python_rs.py --verify")
        assert verify < source.index("python -m pytest -v")
    stress = (PIPELINES / "stress-test-pipeline.yml").read_text(encoding="utf-8")
    assert stress.count("python eng/scripts/mssql_python_rs.py --verify") == 2


@pytest.mark.parametrize("platform", ("windows", "macos", "linux"))
def test_official_build_guards_run_before_runtime_installation(platform):
    source = (PIPELINES / f"stages/build-{platform}-single-stage.yml").read_text(encoding="utf-8")
    guard = source.index("validate-release-versions.ps1")
    assert source.index("if eq(parameters.oneBranchType, 'Official')") < guard
    assert "arguments: '-BuildOnly'" in source
    assert guard < source.index("install-mssql-py-core")


@pytest.mark.parametrize("pipeline", ("official", "dummy"))
def test_release_validates_selected_artifact_before_esrp(pipeline):
    source = (PIPELINES / f"{pipeline}-release-pipeline.yml").read_text(encoding="utf-8")
    guard = source.index(
        "filePath: '$(Build.SourcesDirectory)\\OneBranchPipelines\\scripts\\validate-release-versions.ps1'"
    )
    metadata = source.index("validate-rs-wheel-dependency.ps1")
    assert guard < metadata
    assert source[guard:metadata].count(r"artifacts\dist\transport.json") == 1
    assert source.index("dependsOn: PrepAndValidate", metadata) > metadata


@pytest.fixture
def workspace():
    path = ROOT / f".pipeline-contract-{uuid.uuid4().hex}"
    path.mkdir()
    try:
        yield path
    finally:
        shutil.rmtree(path)


def _receipt(version="0.3.0"):
    return {
        "distribution_version": version,
        "index_url": "https://pypi.org/simple/",
        "version_override": False,
        "wheel_sha256": {"mssql_python_rs-0.3.0-py3-none-any.whl": "a" * 64},
    }


def _guard(workspace, *, version="0.3.0", receipt=None, override="", build_only=False, public=True):
    pwsh = shutil.which("pwsh")
    if not pwsh:
        pytest.skip("PowerShell Core is not available")
    (workspace / "runtime.version").write_text(version)
    if receipt is not None:
        (workspace / "transport.json").write_text(json.dumps(receipt))
    env = dict(os.environ)
    env.update(
        {
            VERSION_ENV: override,
            "GUARD": str(PIPELINES / "scripts/validate-release-versions.ps1"),
            "VERSION_FILE": str(workspace / "runtime.version"),
            "PUBLIC_VERSION": version if public else "9.9.9",
            "BUILD_ONLY": str(build_only).lower(),
        }
    )
    command = """
        $ErrorActionPreference = 'Stop'
        function Invoke-RestMethod {
            param($Uri, $Method)
            if ($Uri -cne "https://pypi.org/pypi/mssql-python-rs/$env:PUBLIC_VERSION/json") {
                throw 'No exact release on public PyPI'
            }
            return @{
                info = @{ version = $env:PUBLIC_VERSION; name = 'mssql-python-rs' }
                urls = @(@{ packagetype = 'bdist_wheel'; yanked = $false })
            }
        }
        & $env:GUARD -VersionFile $env:VERSION_FILE -BuildOnly:($env:BUILD_ONLY -eq 'true')
    """
    return subprocess.run(
        [pwsh, "-NoProfile", "-NonInteractive", "-Command", command],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )


@pytest.mark.parametrize("override", ("0.3.0", "0.3.1.dev123"))
def test_official_build_rejects_all_explicit_overrides(workspace, override):
    result = _guard(workspace, override=override, build_only=True)
    assert result.returncode != 0
    assert "forbid" in result.stderr


@pytest.mark.parametrize("version", ("0.3.1.dev123", "0.3.1rc1", "", ">=0.3.0"))
def test_official_build_rejects_non_release_pins(workspace, version):
    assert _guard(workspace, version=version, build_only=True).returncode != 0


def test_official_build_accepts_committed_release_without_network_or_receipt(workspace):
    result = _guard(workspace, build_only=True, public=False)
    assert result.returncode == 0, result.stderr


def test_release_requires_receipt_and_exact_public_pypi_release(workspace):
    assert _guard(workspace).returncode != 0
    result = _guard(workspace, receipt=_receipt(), public=False)
    assert result.returncode != 0
    result = _guard(workspace, receipt=_receipt())
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    "field,value",
    (
        ("version_override", True),
        ("version_override", "false"),
        ("distribution_version", "0.3.1"),
        ("index_url", "https://pkgs.dev.azure.com/example/simple/"),
        ("wheel_sha256", {}),
        ("wheel_sha256", {"test.whl": "bad"}),
    ),
)
def test_release_rejects_override_or_inconsistent_receipt(workspace, field, value):
    receipt = _receipt()
    receipt[field] = value
    assert _guard(workspace, receipt=receipt).returncode != 0


@pytest.mark.parametrize("dependency", ("0.3.0", "0.3.1.dev123"))
def test_wheel_metadata_must_match_effective_staged_version(workspace, dependency):
    pwsh = shutil.which("pwsh")
    if not pwsh:
        pytest.skip("PowerShell Core is not available")
    (workspace / "runtime.version").write_text("0.3.1.dev123")
    (workspace / "transport.json").write_text(json.dumps(_receipt("0.3.1.dev123")))
    with zipfile.ZipFile(workspace / "mssql_python-1.0.0-py3-none-any.whl", "w") as archive:
        archive.writestr(
            "mssql_python-1.0.0.dist-info/METADATA",
            f"Name: mssql-python\nVersion: 1.0.0\nRequires-Dist: mssql-python-rs=={dependency}\n",
        )
    result = subprocess.run(
        [
            pwsh,
            "-NoProfile",
            "-NonInteractive",
            "-File",
            str(PIPELINES / "scripts/validate-rs-wheel-dependency.ps1"),
            "-WheelsDir",
            str(workspace),
            "-VersionFile",
            str(workspace / "runtime.version"),
            "-ReceiptFile",
            str(workspace / "transport.json"),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert (result.returncode == 0) == (dependency == "0.3.1.dev123"), result.stderr
