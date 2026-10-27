# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Source-only contracts for runtime selection and the pre-ESRP release gates."""

import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import uuid
import zipfile

import pytest

ROOT = Path(__file__).resolve().parents[1]
PIPELINES = ROOT / "OneBranchPipelines"
if not (PIPELINES / "scripts/validate-release-versions.ps1").is_file():
    pytest.skip("Pipeline contracts require a complete source checkout", allow_module_level=True)

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


def test_modified_pipeline_yaml_parses():
    yaml = pytest.importorskip("yaml")
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
    assert source.count("pipelineId: '$(resources.pipeline.buildPipeline.runID)'") == 2
    assert (
        source.count("filePath: OneBranchPipelines/scripts/validate-rs-wheel-dependency.ps1") == 2
    )
    assert source.count("'--no-index', '--no-deps'") == 2
    assert source.count("'mssql-python-rs=='+resolve_runtime().version") == 2


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
