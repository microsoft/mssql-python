"""Unit tests for the release-time re-solve helper (eng/scripts/conda_resolve_check.py).

Exercises the pure logic -- per-subdir channel selection, the ``conda create --dry-run``
command construction, and target enumeration from a package tree -- without invoking conda.
"""

import importlib.util
import io
import json
import tarfile
import zipfile
from pathlib import Path

import pytest

_MODULE_PATH = Path(__file__).resolve().parent.parent / "eng" / "scripts" / "conda_resolve_check.py"

if not _MODULE_PATH.is_file():
    pytest.skip(
        f"conda_resolve_check.py not present ({_MODULE_PATH}); skipping re-solve tests",
        allow_module_level=True,
    )


def _load_module():
    spec = importlib.util.spec_from_file_location("conda_resolve_check_under_test", _MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


crc = _load_module()


def test_channels_win_arm64_uses_defaults_no_strict():
    ch = crc.channels_for("win-arm64")
    assert ch == ["-c", "microsoft", "-c", "defaults"]
    assert "--strict-channel-priority" not in ch


def test_channels_other_subdirs_use_conda_forge_strict():
    for sub in ("win-64", "linux-64", "osx-arm64"):
        ch = crc.channels_for(sub)
        assert ch == ["-c", "microsoft", "-c", "conda-forge", "--strict-channel-priority"]


def test_build_solve_cmd_shape():
    cmd = crc.build_solve_cmd("conda", "linux-64", "3.12", "1.13.0", "file:///chan")
    assert cmd[:4] == ["conda", "create", "--dry-run", "--yes"]
    assert "--platform" in cmd and cmd[cmd.index("--platform") + 1] == "linux-64"
    # Local channel is FIRST -c so the freshly built package is authoritative.
    first_c = cmd.index("-c")
    assert cmd[first_c + 1] == "file:///chan"
    assert "--override-channels" in cmd
    assert "python=3.12" in cmd
    assert "mssql-python=1.13.0" in cmd  # the REAL package, pinned to the exact version


def test_build_solve_cmd_win_arm64_channels():
    cmd = crc.build_solve_cmd("conda", "win-arm64", "3.13", "1.13.0", "file:///chan")
    assert "defaults" in cmd
    assert "--strict-channel-priority" not in cmd


def test_python_tag_from_index():
    assert crc.python_tag_from_index({"build": "py312_0"}) == "3.12"
    assert crc.python_tag_from_index({"build": "py310h505dd55_0"}) == "3.10"
    assert crc.python_tag_from_index({"build": "0", "depends": ["python 3.14.* *_cp314"]}) == "3.14"
    assert crc.python_tag_from_index({"build": "0"}) == ""


def test_as_file_url():
    url = crc._as_file_url("/tmp/chan")
    assert url.startswith("file:///")
    assert url.endswith("/tmp/chan")


def _zstd_available():
    try:
        from compression import zstd  # noqa: F401

        return True
    except Exception:
        try:
            import zstandard  # noqa: F401

            return True
        except Exception:
            return False


def _zstd_compress(raw: bytes) -> bytes:
    try:
        from compression import zstd

        return zstd.compress(raw)
    except Exception:
        import zstandard

        return zstandard.ZstdCompressor().compress(raw)


def _make_conda(path, name, subdir, build):
    index = {"name": name, "version": "1.13.0", "build": build, "subdir": subdir}
    idx = json.dumps(index).encode()
    info_buf = io.BytesIO()
    with tarfile.open(fileobj=info_buf, mode="w") as tf:
        ti = tarfile.TarInfo("info/index.json")
        ti.size = len(idx)
        tf.addfile(ti, io.BytesIO(idx))
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr(f"info-{name}-1.13.0-{build}.tar.zst", _zstd_compress(info_buf.getvalue()))


@pytest.mark.skipif(not _zstd_available(), reason="no zstandard backend available")
def test_enumerate_targets_dedups_and_ignores_non_binding(tmp_path):
    (tmp_path / "linux-64").mkdir()
    (tmp_path / "win-arm64").mkdir()
    _make_conda(tmp_path / "linux-64" / "a.conda", "mssql-python", "linux-64", "py312h1_0")
    _make_conda(tmp_path / "linux-64" / "b.conda", "mssql-python", "linux-64", "py313h1_0")
    _make_conda(tmp_path / "win-arm64" / "c.conda", "mssql-python", "win-arm64", "py314_0")
    # A stray non-binding package must be ignored.
    _make_conda(tmp_path / "linux-64" / "d.conda", "some-other-pkg", "linux-64", "py312_0")

    targets = crc.enumerate_targets(str(tmp_path))
    assert targets == [("linux-64", "3.12"), ("linux-64", "3.13"), ("win-arm64", "3.14")]
