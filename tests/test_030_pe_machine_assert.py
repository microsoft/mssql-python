"""Unit tests for the Windows PE machine-type assert (eng/scripts/assert_pe_machine.py).

The win-arm64 conda package is cross-built on x64 where the arm64 Python can't run, so
the arch is otherwise trusted from the wheel filename. ``assert_pe_machine`` reads the
PE COFF Machine field of every vendored ``.pyd``/``.dll`` and fails if it does not match
the package subdir. These tests exercise the pure PE parser plus a ``.conda`` round-trip.
"""

import importlib.util
import io
import json
import shutil
import struct
import subprocess
import sys
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
    # assert_pe_machine.py imports its sibling _conda_pkg; put eng/scripts on sys.path so the
    # by-path load here resolves it (a direct `python <script>` run gets this for free).
    inserted = str(_MODULE_PATH.parent)
    sys.path.insert(0, inserted)
    try:
        spec = importlib.util.spec_from_file_location("assert_pe_machine_under_test", _MODULE_PATH)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    finally:
        # Don't leak eng/scripts onto sys.path for the rest of the session.
        if inserted in sys.path:
            sys.path.remove(inserted)
    return module


ape = _load_module()

_ARM64 = 0xAA64
_AMD64 = 0x8664
_CORE_INIT = "Lib/site-packages/mssql_py_core/__init__.py"


def _fake_pe(machine: int) -> bytes:
    """A minimal PE32+ with one complete section table entry and in-range raw byte."""
    buf = bytearray(b"\x00" * 0x200)
    buf[0:2] = b"MZ"
    e_lfanew = 0x80
    struct.pack_into("<I", buf, 0x3C, e_lfanew)
    buf[e_lfanew : e_lfanew + 4] = b"PE\x00\x00"
    struct.pack_into("<H", buf, e_lfanew + 4, machine)
    struct.pack_into("<H", buf, e_lfanew + 6, 1)  # NumberOfSections
    optional_size = 0xF0
    struct.pack_into("<H", buf, e_lfanew + 20, optional_size)
    struct.pack_into("<H", buf, e_lfanew + 24, 0x20B)  # PE32+
    section_offset = e_lfanew + 24 + optional_size
    struct.pack_into("<II", buf, section_offset + 16, 1, 0x1F0)
    buf[0x1F0] = 1
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


def test_pe_machine_rejects_header_only_pe():
    header_only = bytearray(b"\x00" * 0x100)
    header_only[0:2] = b"MZ"
    struct.pack_into("<I", header_only, 0x3C, 0x80)
    header_only[0x80:0x84] = b"PE\x00\x00"
    struct.pack_into("<H", header_only, 0x84, _ARM64)
    assert ape.pe_machine(bytes(header_only)) is None


def test_pe_machine_rejects_out_of_range_section_data():
    invalid = bytearray(_fake_pe(_ARM64))
    section_offset = 0x80 + 24 + 0xF0
    struct.pack_into("<II", invalid, section_offset + 16, 32, len(invalid) - 1)
    assert ape.pe_machine(bytes(invalid)) is None


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


def test_zstd_backend_is_available_for_conda_audit_tests():
    assert _zstd_available(), (
        "reading synthetic .conda packages requires Python 3.14+ compression.zstd or the "
        "declared zstandard test dependency"
    )


def test_wheel_retains_normal_and_stable_abi_core_extensions(tmp_path):
    shutil.copy2(_MODULE_PATH.parents[2] / "setup.py", tmp_path / "setup.py")
    sources = {
        "PyPI_Description.md": "Packaging fixture",
        "mssql_python/__init__.py": "",
        "mssql_python_odbc/__init__.py": '__version__ = "18.6.2.1"\n',
        "mssql_py_core/__init__.py": "from .mssql_py_core import *\n",
    }
    for relative, content in sources.items():
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    extensions = (
        "mssql_py_core.cp312-win_arm64.pyd",
        "mssql_py_core.cpython-312-x86_64-linux-gnu.so",
        "mssql_py_core.pyd",
        "mssql_py_core.abi3.so",
    )
    for name in extensions:
        (tmp_path / "mssql_py_core" / name).write_bytes(b"native payload fixture")

    result = subprocess.run(
        [sys.executable, "setup.py", "--quiet", "bdist_wheel"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    wheels = list((tmp_path / "dist").glob("*.whl"))
    assert len(wheels) == 1
    with zipfile.ZipFile(wheels[0]) as wheel:
        for name in extensions:
            assert wheel.read(f"mssql_py_core/{name}") == b"native payload fixture"


def _make_conda(tmp_path, subdir, payload, depends=None):
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
        "depends": depends if depends is not None else ["python_abi 3.12.* *_cp312"],
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
            _CORE_INIT: b"from .mssql_py_core import *\n",
            "Lib/site-packages/mssql_python/ddbc_bindings.cp312-arm64.pyd": _fake_pe(_ARM64),
            "Lib/site-packages/mssql_py_core/mssql_py_core.cp312-win_arm64.pyd": _fake_pe(_ARM64),
            "Lib/site-packages/mssql_python_odbc/libs/windows/arm64/msodbcsql18.dll": _fake_pe(
                _ARM64
            ),
            "Lib/site-packages/mssql_python_odbc/libs/windows/arm64/mssql-auth.dll": _fake_pe(
                _ARM64
            ),
        },
    )
    assert ape.audit_package(p) == []


@pytest.mark.parametrize(
    "state",
    [
        "valid",
        "missing",
        "missing-init",
        "wrong-arch",
        "wrong-tag",
        "abi3",
        "missing-abi",
        "wrong-abi",
    ],
)
def test_required_core_contract(tmp_path, state):
    pin = "python_abi 3.12.* *_cp312"
    # The observed defaults CP312 host supplied only the Python range, not an ABI export.
    depends = ["vc14_runtime", "python >=3.12,<3.13.0a0", "azure-identity >=1.12.0"]
    if state != "missing-abi":
        depends.append(pin if state != "wrong-abi" else "python_abi 3.12.* *_cp313")
    core = "Lib/site-packages/mssql_py_core/mssql_py_core.cp312-win_arm64.pyd"
    payload = {
        _CORE_INIT: b"from .mssql_py_core import *\n",
        "Lib/site-packages/mssql_python/ddbc_bindings.cp312-arm64.pyd": _fake_pe(_ARM64),
        core: _fake_pe(_ARM64),
        "Lib/site-packages/mssql_python_odbc/libs/windows/arm64/msodbcsql18.dll": _fake_pe(_ARM64),
        "Lib/site-packages/mssql_python_odbc/libs/windows/arm64/mssql-auth.dll": _fake_pe(_ARM64),
    }
    if state == "missing":
        del payload[core]
    elif state == "missing-init":
        del payload[_CORE_INIT]
    elif state == "wrong-arch":
        payload[core] = _fake_pe(_AMD64)
    elif state == "wrong-tag":
        payload[core.replace("312", "311")] = payload.pop(core)
    elif state == "abi3":
        payload[core.replace(".cp312-win_arm64", "")] = payload.pop(core)
    errors = ape.audit_package(_make_conda(tmp_path, "win-arm64", payload, depends=depends))
    if state in ("valid", "abi3"):
        assert errors == []
    elif state in ("missing-abi", "wrong-abi"):
        assert any("matching normal CPython python_abi pin" in error for error in errors)
    else:
        assert any("mssql_py_core" in error for error in errors)


@pytest.mark.skipif(not _zstd_available(), reason="no zstandard backend available")
def test_win_arm64_driver_dlls_in_x64_directory_fail_presence_gate(tmp_path):
    # Correct ARM64 machine fields do not help when the loader searches windows/arm64,
    # while both required DLLs were misplaced under windows/x64.
    p = _make_conda(
        tmp_path,
        "win-arm64",
        {
            "Lib/site-packages/mssql_python/ddbc_bindings.cp312-arm64.pyd": _fake_pe(_ARM64),
            "Lib/site-packages/mssql_python_odbc/libs/windows/x64/msodbcsql18.dll": _fake_pe(
                _ARM64
            ),
            "Lib/site-packages/mssql_python_odbc/libs/windows/x64/mssql-auth.dll": _fake_pe(_ARM64),
        },
    )

    errors = ape.audit_package(p)

    assert any("windows/arm64/msodbcsql18" in error for error in errors)
    assert any("windows/arm64/mssql-auth" in error for error in errors)


@pytest.mark.skipif(not _zstd_available(), reason="no zstandard backend available")
def test_win_missing_auth_dll_fails(tmp_path):
    # Binding + core driver present, but mssql-auth.dll missing -> the loader THROWS at connect,
    # so the presence gate (win-arm64's only check) must fail.
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
    errors = ape.audit_package(p)
    assert any("mssql-auth" in e for e in errors)


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
def test_win_support_dll_only_fails(tmp_path):
    # Binding + mssql-auth present, but the CORE driver msodbcsql18*.dll is missing -> the
    # presence gate must still fail on the core driver (win-arm64's sole check).
    p = _make_conda(
        tmp_path,
        "win-arm64",
        {
            "Lib/site-packages/mssql_python/ddbc_bindings.cp312-arm64.pyd": _fake_pe(_ARM64),
            "Lib/site-packages/mssql_python_odbc/libs/windows/arm64/mssql-auth.dll": _fake_pe(
                _ARM64
            ),
        },
    )
    errors = ape.audit_package(p)
    assert any("driver DLL" in e for e in errors)


@pytest.mark.skipif(not _zstd_available(), reason="no zstandard backend available")
def test_non_windows_package_skipped(tmp_path):
    # A linux-64 package has no PE payload -> skipped clean (not failed).
    p = _make_conda(
        tmp_path,
        "linux-64",
        {"lib/python3.12/site-packages/mssql_python/_core.so": b"\x7fELF fake"},
    )
    assert ape.audit_package(p) == []


@pytest.mark.skipif(not _zstd_available(), reason="no zstandard backend available")
def test_win_conda_missing_pkg_payload_fails(tmp_path):
    # A .conda with info/index.json but NO pkg-*.tar.zst payload must FAIL (not silently pass
    # with zero members): iter_payload_members raises, and audit_package -> a violation.
    name = "mssql-python-1.13.0-py312_0"
    index = {
        "name": "mssql-python",
        "version": "1.13.0",
        "build": "py312_0",
        "subdir": "win-arm64",
    }
    info_buf = io.BytesIO()
    with tarfile.open(fileobj=info_buf, mode="w") as tf:
        idx = json.dumps(index).encode()
        ti = tarfile.TarInfo("info/index.json")
        ti.size = len(idx)
        tf.addfile(ti, io.BytesIO(idx))
    conda_path = tmp_path / f"{name}.conda"
    with zipfile.ZipFile(conda_path, "w") as zf:
        zf.writestr(f"info-{name}.tar.zst", _zstd_compress(info_buf.getvalue()))
    errors = ape.audit_package(str(conda_path))
    assert any("pkg-" in e for e in errors)
