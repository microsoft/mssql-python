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
import tarfile
from pathlib import Path

import pytest

_MODULE_PATH = Path(__file__).resolve().parent.parent / "conda" / "validate_conda_release.py"

# The conda/ sources are not shipped inside the built wheel, so the installed-wheel
# test leg copies only tests/ into an isolated dir. Skip the whole module (rather than
# erroring at collection) when the conda source it exercises is absent.
if not _MODULE_PATH.is_file():
    pytest.skip(
        f"conda source not present ({_MODULE_PATH}); skipping conda release metadata tests",
        allow_module_level=True,
    )


def _load_module():
    spec = importlib.util.spec_from_file_location("validate_conda_release_under_test", _MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


vcr = _load_module()

_REQUIRED = ["win-64", "osx-64", "osx-arm64", "linux-64", "linux-aarch64"]
_ALLOWED = ["win-64", "win-arm64", "osx-64", "osx-arm64", "linux-64", "linux-aarch64"]
_PYTHONS = ["3.10", "3.11", "3.12", "3.13", "3.14"]
_MP_VER = "1.13.0"


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
