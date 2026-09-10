"""Unit tests for the metadata-based conda release gate.

``conda/validate_conda_release.py`` reads each package's authoritative
``info/index.json`` and enforces: real-subdir == folder, allowed subdirs, the
full (subdir x Python) matrix, and exact versions for the self-contained
``mssql-python`` package (which vendors the ODBC payload -- no companion). These
tests exercise the pure ``validate()`` logic with synthetic package records (no
real ``.conda`` needed) plus one optional round-trip through the metadata reader.
"""

import importlib.util
import io
import json
import sys
import tarfile
import types
from pathlib import Path

import pytest

_MODULE_PATH = Path(__file__).resolve().parent.parent / "conda" / "validate_conda_release.py"
_ROOT = _MODULE_PATH.parent.parent
_PROMOTER_PATH = _ROOT / "conda" / "promote_conda_release.py"
_PUBLISH_STEP_PATH = _ROOT / "OneBranchPipelines" / "steps" / "conda-publish-step.yml"
_RELEASE_STEP_PATH = _ROOT / "OneBranchPipelines" / "steps" / "conda-release-step.yml"
_RELEASE_PIPELINE_PATH = _ROOT / "OneBranchPipelines" / "conda-release-pipeline.yml"
_README_PATH = _ROOT / "README.md"

# The conda/ sources are not shipped inside the built wheel, so the installed-wheel
# test leg copies only tests/ into an isolated dir. Skip the whole module (rather than
# erroring at collection) when the conda source it exercises is absent.
if not _MODULE_PATH.is_file():
    pytest.skip(
        f"conda source not present ({_MODULE_PATH}); skipping conda release metadata tests",
        allow_module_level=True,
    )


import yaml


def _load_module():
    spec = importlib.util.spec_from_file_location("validate_conda_release_under_test", _MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


vcr = _load_module()


def _load_promoter():
    inserted = str(_PROMOTER_PATH.parent)
    sys.path.insert(0, inserted)
    try:
        spec = importlib.util.spec_from_file_location(
            "promote_conda_release_under_test", _PROMOTER_PATH
        )
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        return module
    finally:
        sys.path.remove(inserted)


promoter = _load_promoter()


@pytest.fixture(autouse=True)
def _mock_publication_guard(monkeypatch):
    monkeypatch.setattr(promoter, "require_publication_lock", lambda *_scope: None)


_REQUIRED = ["win-64", "osx-64", "osx-arm64", "linux-64", "linux-aarch64"]
_ALLOWED = ["win-64", "win-arm64", "osx-64", "osx-arm64", "linux-64", "linux-aarch64"]
_PYTHONS = ["3.10", "3.11", "3.12", "3.13", "3.14"]
_MP_VER = "1.13.0"


@pytest.mark.parametrize(
    "source_present", [False, True], ids=["wheel-tests-only", "source-checkout"]
)
def test_collection_without_pyyaml(source_present, tmp_path, monkeypatch):
    original_import = __import__
    yaml_imports = []

    def without_yaml(name, *args, **kwargs):
        if name == "yaml" or name.startswith("yaml."):
            yaml_imports.append(name)
            raise ModuleNotFoundError("No module named 'yaml'", name=name)
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr("builtins.__import__", without_yaml)
    spec = importlib.util.spec_from_file_location("conda_metadata_without_yaml", __file__)
    module = importlib.util.module_from_spec(spec)
    if not source_present:
        module.__file__ = str(tmp_path / "tests" / Path(__file__).name)

    expected_error = ModuleNotFoundError if source_present else pytest.skip.Exception
    expected_message = "No module named 'yaml'" if source_present else "conda source not present"
    with pytest.raises(
        (ModuleNotFoundError, pytest.skip.Exception), match=expected_message
    ) as error:
        spec.loader.exec_module(module)
    assert error.type is expected_error
    assert yaml_imports == (["yaml"] if source_present else [])


def _binding(subdir, py, folder=None, version=_MP_VER):
    return {
        "folder": folder or subdir,
        "subdir": subdir,
        "name": "mssql-python",
        "version": version,
        "build": f"py{py.replace('.', '')}_0",
        "python": py,
    }


def _healthy_set():
    """A complete release: the self-contained mssql-python package for every
    (required subdir x Python)."""
    pkgs = []
    for sub in _REQUIRED:
        for py in _PYTHONS:
            pkgs.append(_binding(sub, py))
    return pkgs


def _run(pkgs, expected_versions=None):
    return vcr.validate(
        pkgs,
        required_subdirs=_REQUIRED,
        allowed_subdirs=_ALLOWED,
        expected_pythons=_PYTHONS,
        expected_versions=(
            expected_versions if expected_versions is not None else {"mssql-python": _MP_VER}
        ),
    )


def test_healthy_set_passes():
    assert _run(_healthy_set()) == []


def test_mislabeled_subdir_fails():
    pkgs = _healthy_set()
    # An osx-64 package physically staged into the osx-arm64 folder.
    pkgs.append(_binding("osx-64", "3.12", folder="osx-arm64"))
    errors = _run(pkgs)
    assert any("MISLABELED" in e for e in errors)


def test_missing_python_variant_on_win64_fails():
    # This is the exact 8e7f217f regression: drop a win-64 binding; presence-pairing
    # against the single companion used to pass, metadata matrix must now fail.
    pkgs = [p for p in _healthy_set() if not (p["subdir"] == "win-64" and p["python"] == "3.12")]
    errors = _run(pkgs)
    assert any("win-64" in e and "INCOMPLETE" in e and "3.12" in e for e in errors)


def test_stray_companion_package_fails():
    # The self-contained model ships ONLY mssql-python; a stray companion package
    # (the old separate mssql-python-odbc) must now be rejected as unexpected.
    pkgs = _healthy_set()
    pkgs.append(
        {
            "folder": "linux-64",
            "subdir": "linux-64",
            "name": "mssql-python-odbc",
            "version": "18.6.2.1",
            "build": "0",
            "python": "",
        }
    )
    errors = _run(pkgs)
    assert any("unexpected package name" in e and "mssql-python-odbc" in e for e in errors)


def test_unexpected_subdir_fails():
    pkgs = _healthy_set()
    pkgs.append(_binding("linux-ppc64le", "3.12"))
    errors = _run(pkgs)
    assert any("linux-ppc64le" in e and "allowed" in e for e in errors)


def test_version_mismatch_fails():
    pkgs = _healthy_set()
    pkgs.append(_binding("linux-64", "3.14", version="9.9.9"))  # stray wrong-version binding
    # remove the correct 3.14 to avoid duplicate-python noise masking the version check
    pkgs = [
        p
        for p in pkgs
        if not (p["subdir"] == "linux-64" and p["python"] == "3.14" and p["version"] == _MP_VER)
    ]
    errors = _run(pkgs)
    assert any("version" in e.lower() for e in errors)


def test_missing_package_version_fails_without_expected_version():
    packages = _healthy_set()
    packages[0]["version"] = ""

    errors = _run(packages, expected_versions={})

    assert any("package version is missing" in error for error in errors)


def test_multiple_versions_same_package_fails():
    pkgs = _healthy_set()
    pkgs.append(_binding("linux-64", "3.10", version="1.12.0", folder="linux-64"))
    errors = _run(pkgs, expected_versions={})  # no expected -> consistency check must still fail
    assert any("multiple versions" in e for e in errors)


def test_missing_required_subdir_fails():
    pkgs = [p for p in _healthy_set() if p["subdir"] != "linux-aarch64"]
    errors = _run(pkgs)
    assert any("linux-aarch64" in e and "MISSING" in e for e in errors)


def test_duplicate_package_fails():
    # The identical package staged twice (same name/version/subdir/python) -- e.g. a
    # leg's package collected twice from a shared output dir. A set-based matrix
    # check would silently absorb it; the gate must reject the duplicate outright so
    # it can never mask a genuinely missing variant.
    pkgs = _healthy_set()
    pkgs.append(_binding("linux-64", "3.12"))  # exact duplicate of an existing entry
    errors = _run(pkgs)
    assert any("DUPLICATE" in e and "linux-64" in e and "3.12" in e for e in errors)


def test_present_allowed_subdir_partial_matrix_fails():
    # win-arm64 is ALLOWED but not REQUIRED. If it shows up only partially built it
    # must still fail the gate, else a half-finished allowed subdir slips to publish
    # simply because it is not in the required set.
    pkgs = _healthy_set()
    pkgs.append(_binding("win-arm64", "3.10"))  # only one of five Pythons
    errors = _run(pkgs)
    assert any("win-arm64" in e and "INCOMPLETE" in e for e in errors)


def test_win_arm64_reduced_matrix_passes_with_override():
    # win-arm64 legitimately ships only 3.12-3.14 (Anaconda `defaults` has no
    # cryptography/pyodbc for 3.10/3.11). With the per-subdir override it must PASS.
    pkgs = _healthy_set()
    for py in ["3.12", "3.13", "3.14"]:
        pkgs.append(_binding("win-arm64", py))
    errors = vcr.validate(
        pkgs,
        required_subdirs=_REQUIRED,
        allowed_subdirs=_ALLOWED,
        expected_pythons=_PYTHONS,
        expected_versions={"mssql-python": _MP_VER},
        subdir_pythons={"win-arm64": ["3.12", "3.13", "3.14"]},
    )
    assert errors == []


def test_win_arm64_reduced_matrix_still_fails_when_incomplete():
    # Even with the reduced expectation, a missing 3.13 must still fail.
    pkgs = _healthy_set()
    for py in ["3.12", "3.14"]:
        pkgs.append(_binding("win-arm64", py))
    errors = vcr.validate(
        pkgs,
        required_subdirs=_REQUIRED,
        allowed_subdirs=_ALLOWED,
        expected_pythons=_PYTHONS,
        expected_versions={"mssql-python": _MP_VER},
        subdir_pythons={"win-arm64": ["3.12", "3.13", "3.14"]},
    )
    assert any("win-arm64" in e and "INCOMPLETE" in e and "3.13" in e for e in errors)


def test_extra_unsupported_python_fails():
    # got == expected (not just expected subset of got): an EXTRA python beyond the expected
    # set (e.g. a win-arm64 3.10 that slipped in) must fail even though the expected 3.12-3.14
    # are all present.
    pkgs = _healthy_set()
    for py in ["3.12", "3.13", "3.14"]:
        pkgs.append(_binding("win-arm64", py))
    pkgs.append(_binding("win-arm64", "3.10"))  # unsupported extra
    errors = vcr.validate(
        pkgs,
        required_subdirs=_REQUIRED,
        allowed_subdirs=_ALLOWED,
        expected_pythons=_PYTHONS,
        expected_versions={"mssql-python": _MP_VER},
        subdir_pythons={"win-arm64": ["3.12", "3.13", "3.14"]},
    )
    assert any("win-arm64" in e and "UNSUPPORTED" in e and "3.10" in e for e in errors)


def test_v1_required_includes_win_arm64_passes():
    # v1 parity: win-arm64 is REQUIRED. A complete set (win-arm64 at 3.12-3.14 via the
    # per-subdir override) passes.
    required = ["win-64", "win-arm64", "osx-64", "osx-arm64", "linux-64", "linux-aarch64"]
    pkgs = _healthy_set()
    for py in ["3.12", "3.13", "3.14"]:
        pkgs.append(_binding("win-arm64", py))
    errors = vcr.validate(
        pkgs,
        required_subdirs=required,
        allowed_subdirs=_ALLOWED,
        expected_pythons=_PYTHONS,
        expected_versions={"mssql-python": _MP_VER},
        subdir_pythons={"win-arm64": ["3.12", "3.13", "3.14"]},
    )
    assert errors == []


def test_v1_missing_win_arm64_now_fails():
    # With win-arm64 REQUIRED, a set that omits it must now fail (it silently passed before).
    required = ["win-64", "win-arm64", "osx-64", "osx-arm64", "linux-64", "linux-aarch64"]
    errors = vcr.validate(
        _healthy_set(),  # the original required parity, MINUS win-arm64
        required_subdirs=required,
        allowed_subdirs=_ALLOWED,
        expected_pythons=_PYTHONS,
        expected_versions={"mssql-python": _MP_VER},
        subdir_pythons={"win-arm64": ["3.12", "3.13", "3.14"]},
    )
    assert any("win-arm64" in e and "MISSING" in e for e in errors)


def test_default_subdir_pythons_reduces_win_arm64():
    # The shipped CLI default carries the win-arm64 reduction so the pipeline needs
    # no extra flag; parsing it yields the expected mapping.
    assert vcr._parse_subdir_pythons(vcr._DEFAULT_SUBDIR_PYTHONS) == {
        "win-arm64": ["3.12", "3.13", "3.14"]
    }


def test_parse_subdir_pythons():
    assert vcr._parse_subdir_pythons("") == {}
    assert vcr._parse_subdir_pythons("a=3.10;b=3.11,3.12") == {
        "a": ["3.10"],
        "b": ["3.11", "3.12"],
    }


@pytest.mark.parametrize("value", ["win-arm64", "=3.12", "win-arm64="])
def test_parse_subdir_pythons_rejects_malformed_policy(value):
    with pytest.raises(ValueError, match="invalid subdir Python override"):
        vcr._parse_subdir_pythons(value)


def test_release_policy_cannot_disable_required_matrix():
    packages = _healthy_set()

    assert any(
        "required_subdirs" in error for error in vcr.validate(packages, [], _ALLOWED, _PYTHONS)
    )
    assert any(
        "expected_pythons" in error for error in vcr.validate(packages, _REQUIRED, _ALLOWED, [])
    )
    assert any(
        "absent from allowed" in error
        for error in vcr.validate(packages, _REQUIRED, ["win-64"], _PYTHONS)
    )
    assert any(
        "must not be empty" in error
        for error in vcr.validate(
            packages,
            _REQUIRED,
            _ALLOWED,
            _PYTHONS,
            subdir_pythons={"win-64": []},
        )
    )


def test_python_tag_from_index():
    assert vcr.python_tag_from_index({"build": "py311_0"}) == "3.11"
    assert vcr.python_tag_from_index({"build": "py310h1a2b3c_0"}) == "3.10"
    assert (
        vcr.python_tag_from_index({"build": "0", "depends": ["python 3.12.* *_cpython"]}) == "3.12"
    )
    assert vcr.python_tag_from_index({"build": "0"}) == ""


def _zstd_available():
    try:
        from compression import zstd  # noqa: F401  # py3.14+

        return True
    except Exception:
        try:
            import zstandard  # noqa: F401

            return True
        except Exception:
            return False


@pytest.mark.skipif(not _zstd_available(), reason="no zstandard backend available")
def test_read_index_json_roundtrip(tmp_path):
    import zipfile

    index = {"name": "mssql-python", "version": _MP_VER, "build": "py312_0", "subdir": "win-64"}
    # Build info/index.json -> tar -> zstd -> .conda zip, then read it back.
    tar_buf = io.BytesIO()
    with tarfile.open(fileobj=tar_buf, mode="w") as tf:
        data = json.dumps(index).encode()
        ti = tarfile.TarInfo("info/index.json")
        ti.size = len(data)
        tf.addfile(ti, io.BytesIO(data))
    try:
        from compression import zstd  # py3.14+

        compressed = zstd.compress(tar_buf.getvalue())
    except Exception:
        import zstandard

        compressed = zstandard.ZstdCompressor().compress(tar_buf.getvalue())

    conda_path = tmp_path / "mssql-python-1.13.0-py312_0.conda"
    with zipfile.ZipFile(conda_path, "w") as zf:
        zf.writestr("info-mssql-python-1.13.0-py312_0.tar.zst", compressed)

    got = vcr.read_index_json(str(conda_path))
    assert got["subdir"] == "win-64"
    assert vcr.python_tag_from_index(got) == "3.12"


@pytest.mark.skipif(not _zstd_available(), reason="no zstandard backend available")
def test_read_index_json_rejects_multiple_info_payloads(tmp_path):
    import zipfile

    conda_path = tmp_path / "ambiguous.conda"
    with zipfile.ZipFile(conda_path, "w") as archive:
        archive.writestr("info-first.tar.zst", b"first")
        archive.writestr("info-second.tar.zst", b"second")

    with pytest.raises(ValueError, match="exactly one info-.*found 2"):
        vcr.read_index_json(str(conda_path))


def test_index_json_must_be_an_object():
    with pytest.raises(ValueError, match="must contain a JSON object"):
        vcr._require_index_object([], "package.conda")


@pytest.mark.parametrize("value", [None, 123, "", " 1.13.0", "1.13.0 "])
def test_required_index_fields_must_be_trimmed_strings(value):
    with pytest.raises(ValueError, match="must be a non-empty, trimmed string"):
        vcr._required_index_string({"version": value}, "version", "package.conda")


def test_stdlib_zstd_data_error_does_not_fall_back(monkeypatch):
    class CorruptFrameError(Exception):
        pass

    compression = types.ModuleType("compression")
    compression.zstd = types.SimpleNamespace(
        decompress=lambda _raw: (_ for _ in ()).throw(CorruptFrameError("corrupt frame"))
    )
    fallback = types.ModuleType("zstandard")
    fallback.ZstdDecompressor = lambda: (_ for _ in ()).throw(
        AssertionError("third-party fallback must not run after a data error")
    )
    monkeypatch.setitem(sys.modules, "compression", compression)
    monkeypatch.setitem(sys.modules, "zstandard", fallback)

    with pytest.raises(CorruptFrameError, match="corrupt frame"):
        vcr._zstd_decompress(b"not zstd")


def test_zstd_missing_backends_raise_clear_error(monkeypatch):
    real_import = __import__

    def _missing_backends(name, *args, **kwargs):
        if name in {"compression", "zstandard"}:
            raise ImportError(f"blocked {name}")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr("builtins.__import__", _missing_backends)

    with pytest.raises(RuntimeError, match="reading .conda.*requires"):
        vcr._zstd_decompress(b"data")


def test_publish_parameters_are_quoted_and_passed_via_environment():
    publish = _PUBLISH_STEP_PATH.read_text(encoding="utf-8")

    for name, parameter in (
        ("CONDA_CHANNEL", "condaChannel"),
        ("CONDA_LABEL", "condaLabel"),
        ("REQUIRED_SUBDIRS", "requiredSubdirs"),
        ("ALLOWED_SUBDIRS", "allowedSubdirs"),
        ("PYTHON_VERSIONS", "pythonVersions"),
    ):
        assert f"{name}: '${{{{ parameters.{parameter} }}}}'" in publish

    assert "$required = $env:REQUIRED_SUBDIRS.Split(',')" in publish
    assert "$allowed  = $env:ALLOWED_SUBDIRS.Split(',')" in publish
    assert "IsNullOrWhiteSpace($env:CONDA_LABEL)" in publish
    assert "IsNullOrWhiteSpace($env:REQUIRED_SUBDIRS)" in publish
    assert "IsNullOrWhiteSpace($env:ALLOWED_SUBDIRS)" in publish
    assert "IsNullOrWhiteSpace($env:PYTHON_VERSIONS)" in publish
    assert "$required.Count -eq 0" in publish
    assert "$allowed.Count -eq 0" in publish
    assert "$required = '${{ parameters.requiredSubdirs }}'" not in publish
    assert "$allowed  = '${{ parameters.allowedSubdirs }}'" not in publish


def test_release_pipeline_uses_resource_identity_and_gates_production_provenance():
    pipeline = _RELEASE_PIPELINE_PATH.read_text(encoding="utf-8")
    release = _RELEASE_STEP_PATH.read_text(encoding="utf-8")
    publish = _PUBLISH_STEP_PATH.read_text(encoding="utf-8")

    assert "branch: main" in pipeline
    assert pipeline.count("- checkout: self") == 2
    assert "default: 'microsoft'\n    values:\n      - 'microsoft'" in pipeline
    assert "default: 'main'\n    values:\n      - 'main'" in pipeline
    assert "CONDA_BUILD_PIPELINE_ID: $(resources.pipeline.buildPipeline.pipelineID)" in pipeline
    assert "CONDA_BUILD_RUN_ID: $(resources.pipeline.buildPipeline.runID)" in pipeline
    assert "CONDA_BUILD_SOURCE_BRANCH: $(resources.pipeline.buildPipeline.sourceBranch)" in pipeline
    assert "CONDA_BUILD_SOURCE_COMMIT: $(resources.pipeline.buildPipeline.sourceCommit)" in pipeline
    assert "RELEASE_SOURCE_BRANCH: $(Build.SourceBranch)" in pipeline
    assert "conda/validate_conda_provenance.py" in pipeline
    assert "Recorded producer/wheel provenance verification failed" in pipeline
    assert "condaBuildDefinitionId" not in pipeline
    assert "definition: $(resources.pipeline.buildPipeline.pipelineID)" in release
    assert "definition: $(resources.pipeline.buildPipeline.pipelineID)" in publish
    assert "buildDefinitionId" not in release
    assert "buildDefinitionId" not in publish


def test_protected_group_and_publisher_are_absent_from_validate_only_path():
    pipeline = yaml.safe_load(_RELEASE_PIPELINE_PATH.read_text(encoding="utf-8"))
    stage = pipeline["extends"]["parameters"]["stages"][0]
    production = "${{ if eq(parameters.publishToConda, true) }}"
    assert stage[production] == {
        "lockBehavior": "sequential",
        "variables": [{"group": "Anaconda Publishing"}],
    }
    assert not any("group" in variable for variable in pipeline["variables"])
    assert stage["jobs"][0]["job"] == "ValidateConda"
    assert stage["jobs"][1][production][0]["job"] == "PublishConda"
    validation_steps = stage["jobs"][0]["steps"]
    assert "ANACONDA_API_TOKEN" not in json.dumps(validation_steps)
    assert "validate_conda_publication_lock.py" not in json.dumps(validation_steps)
    assert "Get-FileHash -LiteralPath $p.FullName -Algorithm SHA256" in json.dumps(validation_steps)


def test_release_boundary_runs_all_platform_audits_with_pinned_dependency():
    release = _RELEASE_STEP_PATH.read_text(encoding="utf-8")

    assert '"zstandard==0.23.0"' in release
    assert "--only-binary=:all:" in release
    assert "pip --isolated install" in release
    assert "https://packagefeedproxy.microsoft.io/pypi/simple/" in release
    for script in (
        "audit_bundled_binaries.py",
        "assert_pe_machine.py",
        "assert_macho_arch.py",
    ):
        assert script in release
    assert "--mssql-python-odbc-version" not in release
    assert "odbcVersion" not in release


def test_release_and_publish_template_defaults_stay_aligned():
    release = yaml.safe_load(_RELEASE_STEP_PATH.read_text(encoding="utf-8"))
    publish = yaml.safe_load(_PUBLISH_STEP_PATH.read_text(encoding="utf-8"))

    def defaults(document):
        return {parameter["name"]: parameter.get("default") for parameter in document["parameters"]}

    release_defaults = defaults(release)
    publish_defaults = defaults(publish)
    for name in (
        "condaArtifactName",
        "requiredSubdirs",
        "allowedSubdirs",
        "pythonVersions",
        "pythonVersion",
    ):
        assert publish_defaults[name] == release_defaults[name]


def test_publish_uses_pinned_client_and_fail_closed_promotion_helper():
    publish = _PUBLISH_STEP_PATH.read_text(encoding="utf-8")

    assert '"anaconda-client==1.14.1"' in publish
    assert '"zstandard==0.23.0"' in publish
    assert "--only-binary=:all:" in publish
    assert "pip --isolated install" in publish
    assert "https://packagefeedproxy.microsoft.io/pypi/simple/" in publish
    assert "m.version('anaconda-client') == '1.14.1'" in publish
    assert "m.version('zstandard') == '0.23.0'" in publish
    assert "ANACONDA_CLIENT_FORCE_STANDALONE: '1'" in publish
    assert "python -m binstar_client.scripts.cli upload --help" in publish
    assert "$output = @(& python -m binstar_client.scripts.cli @argsList 2>&1)" in publish
    assert "$exitCode = $LASTEXITCODE" in publish
    assert "if ($exitCode -eq 0)" in publish
    assert "if ($attempt -lt 3)" in publish
    assert "anaconda -V" not in publish
    assert "anaconda --version" not in publish
    assert "& anaconda" not in publish
    assert "pip install --upgrade pip" not in publish
    assert "promote_conda_release.py" in publish
    assert publish.index("validate_conda_publication_lock.py") < publish.index("==== Stage: upload")
    assert "validate_conda_release.py" in publish
    assert "Credential-boundary Conda release metadata validation failed" in publish
    assert "--check-local-only" in publish
    assert publish.index("validate_conda_release.py") < publish.index("--check-local-only")
    assert publish.index("--check-local-only") < publish.index("==== Stage: upload")
    assert "BINSTAR_CONFIG_DIR:" in publish
    assert "url: https://api.anaconda.org" in publish
    assert "ssl_verify: true" in publish
    assert "get_config()" in publish
    assert "@('upload', '--user'" in publish
    assert "@('--at'" not in publish
    assert "--expected-version" in publish
    assert "anaconda show" not in publish
    assert "anaconda move" not in publish
    assert "NOTE: anaconda show exposed no sha256" not in publish


def test_release_pool_demand_is_nested_under_demands():
    pipeline = _RELEASE_PIPELINE_PATH.read_text(encoding="utf-8")
    assert (
        "              demands:\n"
        "                - imageOverride -equals PYTHON-1ES-MMS2022\n" in pipeline
    )


def test_readme_documents_windows_arm64_defaults_channel():
    readme = _README_PATH.read_text(encoding="utf-8")
    assert "# Windows x64, macOS, and Linux" in readme
    assert (
        "conda install -c microsoft -c conda-forge --strict-channel-priority "
        "--override-channels mssql-python" in readme
    )
    assert "# Windows ARM64" in readme
    assert "conda install -c microsoft -c defaults --override-channels mssql-python" in readme
    assert "brew install openssl" in readme
    assert "does not load OpenSSL from the Conda environment" in readme


class _FakeAnacondaApi:
    def __init__(self, distributions):
        self.distributions = distributions
        self.calls = []
        self.fail_add_after_apply = None
        self.fail_remove_after_apply = None

    def distribution(self, owner, package, version, basename):
        self.calls.append(("distribution", owner, package, version, basename))
        metadata = self.distributions[basename]
        return {**metadata, "labels": list(metadata["labels"])}

    def add_channel(self, label, owner, *, package, version, filename):
        self.calls.append(("add", label, owner, package, version, filename))
        labels = self.distributions[filename]["labels"]
        if label not in labels:
            labels.append(label)
        if filename == self.fail_add_after_apply:
            raise RuntimeError("simulated add failure after server update")

    def remove_channel(self, label, owner, *, package, version, filename):
        self.calls.append(("remove", label, owner, package, version, filename))
        labels = self.distributions[filename]["labels"]
        if label in labels:
            labels.remove(label)
        if (label, filename) == self.fail_remove_after_apply:
            raise RuntimeError("simulated remove failure after server update")


def _distribution(subdir, filename, version="1.13.0"):
    return promoter.Distribution(
        path=Path(filename),
        package="mssql-python",
        version=version,
        basename=f"{subdir}/{filename}",
        sha256=(filename.encode().hex() + "0" * 64)[:64],
    )


def _api_for(distributions, labels=("staging",)):
    return _FakeAnacondaApi(
        {
            distribution.basename: {
                "basename": distribution.basename,
                "sha256": distribution.sha256,
                "labels": list(labels),
            }
            for distribution in distributions
        }
    )


def test_verify_distribution_uses_full_subdir_basename_and_requires_sha_and_label():
    distribution = _distribution("win-64", "mssql-python-1.13.0-py312_0.conda")
    api = _api_for([distribution])

    labels = promoter.verify_distribution(api, "microsoft", distribution, required_label="staging")

    assert labels == {"staging"}
    assert api.calls[-1][-1] == distribution.basename

    api.distributions[distribution.basename]["sha256"] = "bad"
    with pytest.raises(RuntimeError, match="no valid SHA-256"):
        promoter.verify_distribution(api, "microsoft", distribution)


def test_promote_verifies_all_files_then_cleans_staging_label():
    distributions = [
        _distribution("win-64", "mssql-python-1.13.0-py312_0.conda"),
        _distribution("linux-64", "mssql-python-1.13.0-py313_0.conda"),
    ]
    api = _api_for(distributions)

    promoter.promote(
        api,
        "microsoft",
        "staging",
        "main",
        "1.13.0",
        distributions,
        verify_attempts=1,
        delay_seconds=0,
    )

    assert all(api.distributions[item.basename]["labels"] == ["main"] for item in distributions)
    assert [call[0] for call in api.calls].count("add") == 2
    assert [call[0] for call in api.calls].count("remove") == 2


def test_promote_is_idempotent_after_partial_staging_cleanup():
    first = _distribution("win-64", "mssql-python-1.13.0-py312_0.conda")
    second = _distribution("linux-64", "mssql-python-1.13.0-py313_0.conda")
    api = _api_for([first, second], labels=("staging", "main"))
    api.distributions[first.basename]["labels"] = ["main"]

    promoter.promote(
        api,
        "microsoft",
        "staging",
        "main",
        "1.13.0",
        [first, second],
        verify_attempts=1,
        delay_seconds=0,
    )

    assert api.distributions[first.basename]["labels"] == ["main"]
    assert api.distributions[second.basename]["labels"] == ["main"]
    assert not any(call[0] == "add" for call in api.calls)


def test_promote_recovers_matching_file_left_on_old_staging_label():
    distribution = _distribution("linux-64", "mssql-python-1.13.0-py313_0.conda")
    api = _api_for([distribution], labels=("main_staging_old",))

    promoter.promote(
        api,
        "microsoft",
        "main_staging_new",
        "main",
        "1.13.0",
        [distribution],
        verify_attempts=1,
        delay_seconds=0,
    )

    assert api.distributions[distribution.basename]["labels"] == ["main_staging_old", "main"]
    add_labels = [call[1] for call in api.calls if call[0] == "add"]
    assert add_labels == ["main_staging_new", "main"]


def test_promote_accepts_ambiguous_add_failure_when_label_landed():
    first = _distribution("win-64", "mssql-python-1.13.0-py312_0.conda")
    second = _distribution("linux-64", "mssql-python-1.13.0-py313_0.conda")
    api = _api_for([first, second])
    api.fail_add_after_apply = second.basename

    promoter.promote(
        api,
        "microsoft",
        "staging",
        "main",
        "1.13.0",
        [first, second],
        verify_attempts=1,
        delay_seconds=0,
    )

    assert all(api.distributions[item.basename]["labels"] == ["main"] for item in (first, second))


def test_promote_accepts_ambiguous_staging_cleanup_when_label_was_removed():
    distribution = _distribution("linux-64", "mssql-python-1.13.0-py313_0.conda")
    api = _api_for([distribution])
    api.fail_remove_after_apply = ("staging", distribution.basename)

    promoter.promote(
        api,
        "microsoft",
        "staging",
        "main",
        "1.13.0",
        [distribution],
        verify_attempts=1,
        delay_seconds=0,
    )

    assert api.distributions[distribution.basename]["labels"] == ["main"]


@pytest.mark.parametrize("rollback_reply_lost", [False, True])
def test_promote_rolls_back_partial_label_promotion(rollback_reply_lost):
    first = _distribution("win-64", "mssql-python-1.13.0-py312_0.conda")
    second = _distribution("linux-64", "mssql-python-1.13.0-py313_0.conda")
    api = _api_for([first, second])
    api.fail_add_after_apply = second.basename
    if rollback_reply_lost:
        api.fail_remove_after_apply = ("main", second.basename)

    original_distribution = api.distribution
    hide_target_once = {second.basename}

    def fail_second_target_verification(owner, package, version, basename):
        metadata = original_distribution(owner, package, version, basename)
        if basename in hide_target_once and "main" in metadata["labels"]:
            hide_target_once.remove(basename)
            metadata["labels"].remove("main")
        return metadata

    api.distribution = fail_second_target_verification

    with pytest.raises(RuntimeError, match="rollback of newly added target labels was attempted"):
        promoter.promote(
            api,
            "microsoft",
            "staging",
            "main",
            "1.13.0",
            [first, second],
            verify_attempts=1,
            delay_seconds=0,
        )

    assert all(
        api.distributions[item.basename]["labels"] == ["staging"] for item in (first, second)
    )


def test_rollback_never_removes_unattempted_or_preexisting_target_labels():
    previous = _distribution("win-64", "previous.conda")
    failed = _distribution("linux-64", "failed.conda")
    untouched = _distribution("osx-64", "untouched.conda")
    api = _api_for([previous, failed, untouched])
    api.distributions[previous.basename]["labels"].append("main")
    original_distribution = api.distribution
    hide_target_once = {failed.basename}

    def hide_failed_add(owner, package, version, basename):
        metadata = original_distribution(owner, package, version, basename)
        if basename in hide_target_once and "main" in metadata["labels"]:
            hide_target_once.remove(basename)
            metadata["labels"].remove("main")
        return metadata

    api.distribution = hide_failed_add
    with pytest.raises(RuntimeError, match="rollback"):
        promoter.promote(
            api,
            "microsoft",
            "staging",
            "main",
            "1.13.0",
            [previous, failed, untouched],
            verify_attempts=1,
            delay_seconds=0,
        )

    removed = [call[-1] for call in api.calls if call[:2] == ("remove", "main")]
    assert removed == [failed.basename]
    assert api.distributions[previous.basename]["labels"] == ["staging", "main"]
    assert api.distributions[untouched.basename]["labels"] == ["staging"]


def test_publication_guard_rejects_before_initial_snapshot(monkeypatch):
    distribution = _distribution("win-64", "package.conda")
    api = _api_for([distribution])

    def no_lock(*_scope):
        raise RuntimeError("No exclusive publication lock")

    monkeypatch.setattr(promoter, "require_publication_lock", no_lock)
    with pytest.raises(RuntimeError, match="No exclusive publication lock"):
        promoter.promote(api, "microsoft", "staging", "main", "1.13.0", [distribution])
    assert api.calls == []


@pytest.mark.parametrize("next_version", ["1.13.0", "1.14.0"])
def test_simulated_stage_lock_spans_snapshot_rollback_and_next_publisher(monkeypatch, next_version):
    import threading

    stage_lock = threading.Lock()
    distribution = _distribution("win-64", "package.conda")
    api = _api_for([distribution], labels=("stage_a", "stage_b"))
    original_read = api.distribution
    fail_a_once = [True]
    verified_scopes = []

    def guard(*scope):
        assert stage_lock.locked()
        verified_scopes.append(scope)

    def read_under_stage_lock(owner, package, version, basename):
        # Model the server's protected-stage boundary, not a production local lock.
        # A second stage cannot snapshot while the first promotes/rolls back/cleans up.
        assert not stage_lock.acquire(blocking=False)
        metadata = original_read(owner, package, version, basename)
        if fail_a_once[0] and "main" in metadata["labels"]:
            fail_a_once[0] = False
            metadata["labels"].remove("main")
        return metadata

    monkeypatch.setattr(promoter, "require_publication_lock", guard)
    api.distribution = read_under_stage_lock
    with stage_lock:
        with pytest.raises(RuntimeError, match="rollback"):
            promoter.promote(
                api,
                "microsoft",
                "stage_a",
                "main",
                "1.13.0",
                [distribution],
                verify_attempts=1,
                delay_seconds=0,
            )
    assert "main" not in api.distributions[distribution.basename]["labels"]
    next_distribution = distribution
    if next_version != "1.13.0":
        next_distribution = _distribution("win-64", "next.conda", version=next_version)
        api.distributions.update(_api_for([next_distribution], labels=("stage_b",)).distributions)
    with stage_lock:
        promoter.promote(
            api,
            "microsoft",
            "stage_b",
            "main",
            next_version,
            [next_distribution],
            verify_attempts=1,
            delay_seconds=0,
        )
    expected = ["stage_a", "main"] if next_distribution is distribution else ["main"]
    assert api.distributions[next_distribution.basename]["labels"] == expected
    assert verified_scopes == [("microsoft", "mssql-python", "main")] * 2


def test_promote_rejects_wrong_release_version_before_api_mutation():
    distribution = _distribution("linux-64", "mssql-python-9.9.9-py313_0.conda", version="9.9.9")
    api = _api_for([distribution])

    with pytest.raises(ValueError, match="do not match expected"):
        promoter.promote(
            api,
            "microsoft",
            "staging",
            "main",
            "1.13.0",
            [distribution],
            verify_attempts=1,
            delay_seconds=0,
        )

    assert api.calls == []


@pytest.mark.parametrize(
    "field,value,message",
    [
        ("sha256", None, "no valid SHA-256"),
        ("sha256", "0" * 64, "SHA-256 mismatch"),
        ("basename", "same.conda", "basename"),
        ("basename", "../win-64/same.conda", "basename"),
        ("labels", None, "labels list"),
        ("labels", "main", "labels list"),
        ("labels", [123], "labels list"),
        ("labels", ["main_staging_1"], "missing required label"),
    ],
)
def test_remote_identity_checksum_and_labels_fail_closed(field, value, message):
    distribution = _distribution("win-64", "same.conda")
    api = _api_for([distribution])
    metadata = api.distributions[distribution.basename]
    metadata[field] = value
    api.distribution = lambda *_args: metadata
    with pytest.raises(RuntimeError, match=message):
        promoter.verify_distribution(api, "microsoft", distribution, required_label="main")


def test_same_filename_on_different_platforms_is_not_a_duplicate():
    distributions = [
        _distribution("win-64", "same.conda"),
        _distribution("linux-64", "same.conda"),
    ]
    api = _api_for(distributions)
    promoter.promote(
        api,
        "microsoft",
        "staging",
        "main",
        "1.13.0",
        distributions,
        verify_attempts=1,
        delay_seconds=0,
    )
    assert {call[-1] for call in api.calls} == {"win-64/same.conda", "linux-64/same.conda"}


@pytest.mark.parametrize(
    "owner,staging,target",
    [
        ("../org", "staging", "main"),
        ("microsoft", "", "main"),
        ("microsoft", "staging", "../main"),
        ("microsoft", "main", "main"),
    ],
)
def test_invalid_publication_scope_fails_before_any_remote_read(owner, staging, target):
    distribution = _distribution("win-64", "package.conda")
    api = _api_for([distribution])
    with pytest.raises(ValueError):
        promoter.promote(api, owner, staging, target, "1.13.0", [distribution])
    assert api.calls == []


def test_empty_and_duplicate_promotion_inputs_are_rejected():
    distribution = _distribution("win-64", "package.conda")
    with pytest.raises(ValueError, match="No Conda distributions"):
        promoter.validate_release_input("1.13.0", [])
    with pytest.raises(ValueError, match="Duplicate distribution"):
        promoter.validate_release_input("1.13.0", [distribution, distribution])


def test_partial_upload_cannot_start_public_label_promotion():
    first = _distribution("win-64", "first.conda")
    absent = _distribution("linux-64", "absent.conda")
    api = _api_for([first])
    with pytest.raises(KeyError):
        promoter.promote(
            api,
            "microsoft",
            "staging",
            "main",
            "1.13.0",
            [first, absent],
            verify_attempts=1,
            delay_seconds=0,
        )
    assert not any(call[0] == "add" for call in api.calls)


@pytest.mark.parametrize("success_on_last_attempt", [True, False])
def test_eventual_consistency_retry_count_and_delay_are_bounded(
    monkeypatch, success_on_last_attempt
):
    attempts, delays = [], []
    monkeypatch.setattr(promoter.time, "sleep", delays.append)

    def verify():
        attempts.append(1)
        if success_on_last_attempt and len(attempts) == 3:
            return {"main"}
        raise TimeoutError("metadata request timed out")

    if success_on_last_attempt:
        assert promoter._verify_with_retry(verify, "metadata", attempts=3, delay_seconds=5) == {
            "main"
        }
    else:
        with pytest.raises(TimeoutError):
            promoter._verify_with_retry(verify, "metadata", attempts=3, delay_seconds=5)
    assert len(attempts) == 3
    assert delays == [5, 5]


def test_rollback_failure_is_reported_and_does_not_erase_prior_good_membership():
    previous = _distribution("win-64", "previous.conda")
    failed = _distribution("linux-64", "failed.conda")
    api = _api_for([previous, failed])
    api.distributions[previous.basename]["labels"].append("main")
    original_read = api.distribution
    hide_target_once = {failed.basename}

    def fail_verification(owner, package, version, basename):
        metadata = original_read(owner, package, version, basename)
        if basename in hide_target_once and "main" in metadata["labels"]:
            hide_target_once.remove(basename)
            metadata["labels"].remove("main")
        return metadata

    def fail_remove(*_args, **_kwargs):
        raise TimeoutError("rollback did not reach server")

    api.distribution = fail_verification
    api.remove_channel = fail_remove
    with pytest.raises(RuntimeError, match="Rollback errors.*rollback did not reach server"):
        promoter.promote(
            api,
            "microsoft",
            "staging",
            "main",
            "1.13.0",
            [previous, failed],
            verify_attempts=1,
            delay_seconds=0,
        )
    assert "main" in api.distributions[previous.basename]["labels"]
    assert "main" in api.distributions[failed.basename]["labels"]


def test_interrupted_promotion_is_recoverable_but_not_atomic():
    first = _distribution("win-64", "first.conda")
    second = _distribution("linux-64", "second.conda")
    api = _api_for([first, second])
    original_add = api.add_channel

    def interrupt_after_add(*args, **kwargs):
        original_add(*args, **kwargs)
        raise KeyboardInterrupt("simulated process interruption")

    api.add_channel = interrupt_after_add
    with pytest.raises(KeyboardInterrupt):
        promoter.promote(
            api,
            "microsoft",
            "staging",
            "main",
            "1.13.0",
            [first, second],
            verify_attempts=1,
            delay_seconds=0,
        )
    assert "main" in api.distributions[first.basename]["labels"]
    assert "main" not in api.distributions[second.basename]["labels"]
    api.add_channel = original_add
    promoter.promote(
        api,
        "microsoft",
        "staging",
        "main",
        "1.13.0",
        [first, second],
        verify_attempts=1,
        delay_seconds=0,
    )
    assert all(api.distributions[item.basename]["labels"] == ["main"] for item in (first, second))


def _write_release_archive(tmp_path, **overrides):
    directory = tmp_path / "win-64"
    directory.mkdir(exist_ok=True)
    path = directory / "mssql-python-1.13.0-py312_0.tar.bz2"
    index = {
        "name": "mssql-python",
        "version": "1.13.0",
        "build": "py312_0",
        "subdir": "win-64",
        "depends": ["python >=3.12,<3.13.0a0"],
        **overrides,
    }
    data = json.dumps(index).encode()
    with tarfile.open(path, "w:bz2") as archive:
        member = tarfile.TarInfo("info/index.json")
        member.size = len(data)
        archive.addfile(member, io.BytesIO(data))
    return path


@pytest.mark.parametrize(
    "metadata",
    [{"name": "unexpected"}, {"version": ""}, {"subdir": "../win-64"}],
)
def test_untrusted_local_metadata_is_rejected(tmp_path, metadata):
    path = _write_release_archive(tmp_path, **metadata)
    with pytest.raises(ValueError):
        promoter.distribution_from_path(path)


def test_local_only_cli_needs_neither_token_nor_publication_guard(tmp_path, monkeypatch, capsys):
    path = _write_release_archive(tmp_path)
    monkeypatch.delenv("ANACONDA_API_TOKEN", raising=False)
    monkeypatch.delenv("SYSTEM_ACCESSTOKEN", raising=False)
    monkeypatch.setattr(
        promoter,
        "require_publication_lock",
        lambda *_args: pytest.fail("local-only must not inspect production controls"),
    )
    assert (
        promoter.main(
            [
                "--owner",
                "microsoft",
                "--staging-label",
                "local",
                "--target-label",
                "main",
                "--expected-version",
                "1.13.0",
                "--check-local-only",
                str(path),
            ]
        )
        == 0
    )
    assert "LOCAL_RELEASE_INPUT_OK" in capsys.readouterr().out


def test_promotion_cli_uses_bounded_api_requests(tmp_path, monkeypatch):
    path = _write_release_archive(tmp_path)
    requests = []
    api = types.SimpleNamespace(
        session=types.SimpleNamespace(request=lambda *args, **kwargs: requests.append(kwargs))
    )
    client = types.ModuleType("binstar_client")
    utils = types.ModuleType("binstar_client.utils")
    utils.get_server_api = lambda **_kwargs: api
    monkeypatch.setitem(sys.modules, "binstar_client", client)
    monkeypatch.setitem(sys.modules, "binstar_client.utils", utils)
    monkeypatch.setattr(
        promoter,
        "promote",
        lambda api, *_args: api.session.request("GET", "https://api.anaconda.org/example"),
    )
    assert (
        promoter.main(
            [
                "--owner",
                "microsoft",
                "--staging-label",
                "staging",
                "--target-label",
                "main",
                "--expected-version",
                "1.13.0",
                str(path),
            ]
        )
        == 0
    )
    assert requests == [{"timeout": (15, 60)}]


def test_metadata_cli_reads_real_archive_and_enforces_requested_matrix(tmp_path, capsys):
    _write_release_archive(tmp_path)
    assert vcr.main(["--root", str(tmp_path), "--mssql-python-version", "1.13.0"]) == 1
    assert "MISSING" in capsys.readouterr().err
    assert (
        vcr.main(
            [
                "--root",
                str(tmp_path),
                "--required-subdirs",
                "win-64",
                "--allowed-subdirs",
                "win-64",
                "--pythons",
                "3.12",
                "--mssql-python-version",
                "1.13.0",
            ]
        )
        == 0
    )
    assert "metadata-validated" in capsys.readouterr().out


def test_metadata_cli_rejects_empty_and_25_of_28_default_release(tmp_path, monkeypatch, capsys):
    assert vcr.main(["--root", str(tmp_path)]) == 1
    assert "no conda packages" in capsys.readouterr().err
    monkeypatch.setattr(vcr, "collect_packages", lambda _root: _healthy_set())
    assert vcr.main(["--root", str(tmp_path)]) == 1
    assert "win-arm64" in capsys.readouterr().err


def test_staging_recovery_accepts_reply_lost_after_server_mutation():
    distribution = _distribution("win-64", "package.conda")
    api = _api_for([distribution], labels=("old_staging",))
    api.fail_add_after_apply = distribution.basename
    promoter.promote(
        api,
        "microsoft",
        "new_staging",
        "main",
        "1.13.0",
        [distribution],
        verify_attempts=1,
        delay_seconds=0,
    )
    assert api.distributions[distribution.basename]["labels"] == ["old_staging", "main"]


def test_failed_staging_cleanup_preserves_successful_publication():
    distribution = _distribution("win-64", "package.conda")
    api = _api_for([distribution])

    def timeout_before_remove(*_args, **_kwargs):
        raise TimeoutError("cleanup did not reach server")

    api.remove_channel = timeout_before_remove
    with pytest.raises(RuntimeError, match="Failed to remove staging label"):
        promoter.promote(
            api,
            "microsoft",
            "staging",
            "main",
            "1.13.0",
            [distribution],
            verify_attempts=1,
            delay_seconds=0,
        )
    assert api.distributions[distribution.basename]["labels"] == ["staging", "main"]


def test_missing_local_archive_and_version_are_rejected(tmp_path):
    with pytest.raises(ValueError, match="does not exist"):
        promoter.distribution_from_path(tmp_path / "missing.conda")
    with pytest.raises(ValueError, match="Expected mssql-python version"):
        promoter.validate_release_input("", [])


@pytest.mark.parametrize("extension", [".conda", ".tar.bz2"])
@pytest.mark.parametrize("index_kind", ["missing", "duplicate", "symlink"])
def test_index_member_must_be_unique_regular_file(tmp_path, monkeypatch, extension, index_kind):
    import zipfile

    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w" if extension == ".conda" else "w:bz2") as archive:
        for _ in range(2 if index_kind == "duplicate" else 1):
            member = tarfile.TarInfo("other.json" if index_kind == "missing" else "info/index.json")
            if index_kind == "symlink":
                member.type = tarfile.SYMTYPE
                member.linkname = "../../outside.json"
            archive.addfile(member)
    path = tmp_path / ("package" + extension)
    if extension == ".conda":
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr("info-package.tar.zst", buffer.getvalue())
        monkeypatch.setattr(vcr, "_zstd_decompress", lambda raw: raw)
    else:
        path.write_bytes(buffer.getvalue())
    with pytest.raises(ValueError, match="exactly one regular info/index.json"):
        vcr.read_index_json(str(path))
