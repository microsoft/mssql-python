"""Unit tests for the Windows PE machine-type assert (eng/scripts/assert_pe_machine.py).

The win-arm64 conda package is cross-built on x64 where the arm64 Python can't run, so
the arch is otherwise trusted from the wheel filename. ``assert_pe_machine`` reads the
PE COFF Machine field of every vendored ``.pyd``/``.dll`` and fails if it does not match
the package subdir. These tests exercise the pure PE parser plus a ``.conda`` round-trip.
"""

import importlib.util
import io
import json
import struct
import tarfile
import zipfile
from pathlib import Path

import pytest

_MODULE_PATH = Path(__file__).resolve().parent.parent / "eng" / "scripts" / "assert_pe_machine.py"

if not _MODULE_PATH.is_file():
    pytest.skip(
        f"assert_pe_machine.py not present ({_MODULE_PATH}); skipping PE assert tests",
        allow_module_level=True,
    )


def _load_module():
    spec = importlib.util.spec_from_file_location("assert_pe_machine_under_test", _MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


ape = _load_module()

_ARM64 = 0xAA64
_AMD64 = 0x8664


def _fake_pe(machine: int) -> bytes:
    """A minimal but structurally valid PE: MZ -> e_lfanew -> 'PE\\0\\0' -> Machine."""
    buf = bytearray(b"\x00" * 0x100)
    buf[0:2] = b"MZ"
    e_lfanew = 0x80
    struct.pack_into("<I", buf, 0x3C, e_lfanew)
    buf[e_lfanew : e_lfanew + 4] = b"PE\x00\x00"
    struct.pack_into("<H", buf, e_lfanew + 4, machine)
    return bytes(buf)


def test_pe_machine_parses_arch():
    assert ape.pe_machine(_fake_pe(_ARM64)) == _ARM64
    assert ape.pe_machine(_fake_pe(_AMD64)) == _AMD64


def test_pe_machine_rejects_non_pe():
    assert ape.pe_machine(b"not a pe file at all, no MZ header") is None
    assert ape.pe_machine(b"MZ") is None  # too short
    # MZ present but the PE signature does not resolve.
    bad = bytearray(b"\x00" * 0x100)
    bad[0:2] = b"MZ"
    struct.pack_into("<I", bad, 0x3C, 0x80)  # e_lfanew points at zeros (no 'PE\0\0')
    assert ape.pe_machine(bytes(bad)) is None


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


def _zstd_compress(raw: bytes) -> bytes:
    try:
        from compression import zstd  # py3.14+

        return zstd.compress(raw)
    except Exception:
        import zstandard

        return zstandard.ZstdCompressor().compress(raw)


def _make_conda(tmp_path, subdir, payload):
    """Build a minimal .conda (info-*.tar.zst + pkg-*.tar.zst) with the given payload files."""
    name = "mssql-python-1.13.0-py312_0"

    pkg_buf = io.BytesIO()
    with tarfile.open(fileobj=pkg_buf, mode="w") as tf:
        for arc, data in payload.items():
            ti = tarfile.TarInfo(arc)
            ti.size = len(data)
            tf.addfile(ti, io.BytesIO(data))

    index = {
        "name": "mssql-python",
        "version": "1.13.0",
        "build": "py312_0",
        "subdir": subdir,
    }
    idx = json.dumps(index).encode()
    info_buf = io.BytesIO()
    with tarfile.open(fileobj=info_buf, mode="w") as tf:
        ti = tarfile.TarInfo("info/index.json")
        ti.size = len(idx)
        tf.addfile(ti, io.BytesIO(idx))

    conda_path = tmp_path / f"{name}.conda"
    with zipfile.ZipFile(conda_path, "w") as zf:
        zf.writestr(f"pkg-{name}.tar.zst", _zstd_compress(pkg_buf.getvalue()))
        zf.writestr(f"info-{name}.tar.zst", _zstd_compress(info_buf.getvalue()))
    return str(conda_path)


@pytest.mark.skipif(not _zstd_available(), reason="no zstandard backend available")
def test_win_arm64_arm64_binaries_pass(tmp_path):
    p = _make_conda(
        tmp_path,
        "win-arm64",
        {
            "Lib/site-packages/mssql_python/ddbc_bindings.cp312-arm64.pyd": _fake_pe(_ARM64),
            "Lib/site-packages/mssql_python_odbc/libs/windows/arm64/msodbcsql18.dll": _fake_pe(
                _ARM64
            ),
        },
    )
    assert ape.audit_package(p) == []


@pytest.mark.skipif(not _zstd_available(), reason="no zstandard backend available")
def test_win_arm64_x64_binary_fails(tmp_path):
    # The exact bug this guard exists for: an x64 .pyd inside a win-arm64 package.
    p = _make_conda(
        tmp_path,
        "win-arm64",
        {
            "Lib/site-packages/mssql_python/ddbc_bindings.cp312-arm64.pyd": _fake_pe(_AMD64),
        },
    )
    errors = ape.audit_package(p)
    assert any("amd64" in e and "arm64" in e for e in errors)


@pytest.mark.skipif(not _zstd_available(), reason="no zstandard backend available")
def test_win_package_without_native_fails(tmp_path):
    p = _make_conda(
        tmp_path,
        "win-arm64",
        {"Lib/site-packages/mssql_python/__init__.py": b"# pure python, no native binary\n"},
    )
    errors = ape.audit_package(p)
    assert any("no .pyd/.dll" in e for e in errors)


@pytest.mark.skipif(not _zstd_available(), reason="no zstandard backend available")
def test_win_missing_driver_dll_fails(tmp_path):
    # Correct-arch binding present, but the vendored ODBC driver DLLs are missing.
    p = _make_conda(
        tmp_path,
        "win-arm64",
        {"Lib/site-packages/mssql_python/ddbc_bindings.cp312-arm64.pyd": _fake_pe(_ARM64)},
    )
    errors = ape.audit_package(p)
    assert any("driver DLL" in e for e in errors)


@pytest.mark.skipif(not _zstd_available(), reason="no zstandard backend available")
def test_win_missing_binding_fails(tmp_path):
    # Correct-arch driver DLL present, but the native binding .pyd is missing.
    p = _make_conda(
        tmp_path,
        "win-arm64",
        {
            "Lib/site-packages/mssql_python_odbc/libs/windows/arm64/msodbcsql18.dll": _fake_pe(
                _ARM64
            )
        },
    )
    errors = ape.audit_package(p)
    assert any("native binding" in e for e in errors)


@pytest.mark.skipif(not _zstd_available(), reason="no zstandard backend available")
def test_non_windows_package_skipped(tmp_path):
    # A linux-64 package has no PE payload -> skipped clean (not failed).
    p = _make_conda(
        tmp_path,
        "linux-64",
        {"lib/python3.12/site-packages/mssql_python/_core.so": b"\x7fELF fake"},
    )
    assert ape.audit_package(p) == []
