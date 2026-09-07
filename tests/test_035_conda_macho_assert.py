"""Arch-slice tests for eng/scripts/assert_macho_arch.py (the macOS twin of test_030).

The osx-arm64 conda package is CROSS-built on an Intel agent where the arm64 slice cannot
run, so the build-time runtime import is skipped and the package's arch is otherwise trusted
from the universal2 wheel tag. This asserts the static Mach-O check catches a mislabeled/thin
(x86_64-only) binding inside an osx-arm64 package -- the exact gap for the osx legs -- while
accepting the real wheel layout with separate thin arm64 and x86_64 driver directories.
"""

import importlib.util
import io
import json
import struct
import sys
import tarfile
import zipfile
from pathlib import Path

import pytest

_MODULE_PATH = Path(__file__).resolve().parent.parent / "eng" / "scripts" / "assert_macho_arch.py"

if not _MODULE_PATH.exists():
    pytest.skip(
        f"macho arch assert not present ({_MODULE_PATH}); skipping",
        allow_module_level=True,
    )


def _load_module():
    # assert_macho_arch.py imports its sibling _conda_pkg; put eng/scripts on sys.path so the
    # by-path load here resolves it (a direct `python <script>` run gets this for free).
    inserted = str(_MODULE_PATH.parent)
    sys.path.insert(0, inserted)
    try:
        spec = importlib.util.spec_from_file_location("assert_macho_arch_under_test", _MODULE_PATH)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    finally:
        # Don't leak eng/scripts onto sys.path for the rest of the session.
        if inserted in sys.path:
            sys.path.remove(inserted)
    return module


mac = _load_module()

_X86_64 = 0x01000007
_ARM64 = 0x0100000C


def _fake_macho_thin(cputype: int) -> bytes:
    """A minimal 64-bit little-endian Mach-O with one complete LC_UUID command."""
    header = struct.pack("<IIIIIIII", 0xFEEDFACF, cputype, 0, 2, 1, 24, 0, 0)
    load_command = struct.pack("<II", 0x1B, 24) + b"\x00" * 16
    return header + load_command


def _fake_macho_fat(cputypes) -> bytes:
    """A minimal valid universal binary with a thin Mach-O body for every declared slice."""
    slices = [_fake_macho_thin(cputype) for cputype in cputypes]
    table_size = 8 + 20 * len(slices)
    entries = bytearray()
    bodies = bytearray()
    offset = table_size
    for cputype, body in zip(cputypes, slices):
        # fat_arch: cputype, cpusubtype, offset, size, align (all big-endian, 20 bytes).
        entries += struct.pack(">IIIII", cputype, 0, offset, len(body), 0)
        bodies += body
        offset += len(body)
    return struct.pack(">II", 0xCAFEBABE, len(slices)) + bytes(entries) + bytes(bodies)


def test_macho_arches_thin():
    assert mac.macho_arches(_fake_macho_thin(_ARM64)) == {"arm64"}
    assert mac.macho_arches(_fake_macho_thin(_X86_64)) == {"x86_64"}


def test_macho_arches_fat_universal2():
    assert mac.macho_arches(_fake_macho_fat([_X86_64, _ARM64])) == {"x86_64", "arm64"}


def test_macho_arches_rejects_non_macho():
    assert mac.macho_arches(b"not a mach-o binary at all") is None
    assert mac.macho_arches(b"\xcf\xfa") is None  # too short


def test_macho_arches_rejects_header_only_thin_binary():
    header_only = struct.pack("<IIIIIIII", 0xFEEDFACF, _ARM64, 0, 2, 1, 24, 0, 0)
    assert mac.macho_arches(header_only) is None


def test_macho_arches_rejects_truncated_fat_table():
    # Claims two slices but contains only one cputype word from the first table entry.
    truncated = struct.pack(">III", 0xCAFEBABE, 2, _ARM64)
    assert mac.macho_arches(truncated) is None


def test_macho_arches_rejects_invalid_fat_slice_range():
    # Complete table, but its slice points beyond the end of the file.
    invalid_range = struct.pack(">IIIIIII", 0xCAFEBABE, 1, _ARM64, 0, 28, 64, 0)
    assert mac.macho_arches(invalid_range) is None


def test_macho_arches_rejects_slice_that_disagrees_with_table():
    data = bytearray(_fake_macho_fat([_ARM64]))
    data[8:12] = struct.pack(">I", _X86_64)
    assert mac.macho_arches(bytes(data)) is None


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

    index = {"name": "mssql-python", "version": "1.13.0", "build": "py312_0", "subdir": subdir}
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


_BINDING = "lib/python3.12/site-packages/mssql_python/ddbc_bindings.cp312-darwin.so"
_DRIVER_ROOT = "lib/python3.12/site-packages/mssql_python_odbc/libs/macos"
_DRIVER_LIBRARIES = (
    "libltdl.7.dylib",
    "libmsodbcsql.18.dylib",
    "libodbc.2.dylib",
    "libodbcinst.2.dylib",
)


def _realistic_payload(binding, arm64=None, x86_64=None):
    """Mirror the wheel's two architecture-specific four-library driver directories."""
    arm64 = arm64 or _fake_macho_thin(_ARM64)
    x86_64 = x86_64 or _fake_macho_thin(_X86_64)
    payload = {_BINDING: binding}
    for library in _DRIVER_LIBRARIES:
        payload[f"{_DRIVER_ROOT}/arm64/lib/{library}"] = arm64
        payload[f"{_DRIVER_ROOT}/x86_64/lib/{library}"] = x86_64
    return payload


@pytest.mark.skipif(not _zstd_available(), reason="no zstandard backend available")
@pytest.mark.parametrize("subdir", ["osx-arm64", "osx-64"])
def test_osx_packages_accept_real_split_driver_layout(tmp_path, subdir):
    # Real shipped case: universal2 binding plus thin arm64 AND x86_64 driver trees.
    p = _make_conda(
        tmp_path,
        subdir,
        _realistic_payload(_fake_macho_fat([_X86_64, _ARM64])),
    )
    assert mac.audit_package(p) == []


@pytest.mark.skipif(not _zstd_available(), reason="no zstandard backend available")
def test_osx_arm64_accepts_thin_arm64_binding(tmp_path):
    p = _make_conda(
        tmp_path,
        "osx-arm64",
        _realistic_payload(_fake_macho_thin(_ARM64)),
    )
    assert mac.audit_package(p) == []


@pytest.mark.skipif(not _zstd_available(), reason="no zstandard backend available")
def test_osx_arm64_x86_64_only_binding_fails(tmp_path):
    # The exact binding bug this guard exists for: x86_64-only inside an osx-arm64 package.
    p = _make_conda(
        tmp_path,
        "osx-arm64",
        _realistic_payload(_fake_macho_thin(_X86_64)),
    )
    errors = mac.audit_package(p)
    assert any("x86_64" in e and "arm64" in e for e in errors)


@pytest.mark.skipif(not _zstd_available(), reason="no zstandard backend available")
def test_driver_binary_must_match_its_arch_directory(tmp_path):
    p = _make_conda(
        tmp_path,
        "osx-arm64",
        _realistic_payload(
            _fake_macho_fat([_X86_64, _ARM64]),
            arm64=_fake_macho_thin(_X86_64),
        ),
    )
    errors = mac.audit_package(p)
    assert len([error for error in errors if "required 'arm64' slice" in error]) == 4


@pytest.mark.skipif(not _zstd_available(), reason="no zstandard backend available")
def test_osx_arm64_missing_driver_fails(tmp_path):
    # The other driver tree does not satisfy the target-tree presence gate.
    payload = {_BINDING: _fake_macho_fat([_X86_64, _ARM64])}
    for library in _DRIVER_LIBRARIES:
        payload[f"{_DRIVER_ROOT}/x86_64/lib/{library}"] = _fake_macho_thin(_X86_64)
    p = _make_conda(tmp_path, "osx-arm64", payload)
    errors = mac.audit_package(p)
    assert any("no vendored ODBC driver for 'arm64'" in error for error in errors)


@pytest.mark.skipif(not _zstd_available(), reason="no zstandard backend available")
def test_osx_arm64_driver_outside_runtime_lib_directory_fails(tmp_path):
    payload = _realistic_payload(_fake_macho_fat([_X86_64, _ARM64]))
    driver = payload.pop(f"{_DRIVER_ROOT}/arm64/lib/libmsodbcsql.18.dylib")
    payload[f"{_DRIVER_ROOT}/arm64/libmsodbcsql.18.dylib"] = driver

    errors = mac.audit_package(_make_conda(tmp_path, "osx-arm64", payload))

    assert any("no vendored ODBC driver for 'arm64'" in error for error in errors)


@pytest.mark.skipif(not _zstd_available(), reason="no zstandard backend available")
def test_package_rejects_truncated_fat_binding(tmp_path):
    truncated = struct.pack(">III", 0xCAFEBABE, 2, _ARM64)
    p = _make_conda(
        tmp_path,
        "osx-arm64",
        _realistic_payload(truncated),
    )
    errors = mac.audit_package(p)
    assert any(_BINDING in error and "not a valid, complete Mach-O" in error for error in errors)


@pytest.mark.skipif(not _zstd_available(), reason="no zstandard backend available")
def test_non_osx_subdir_is_skipped(tmp_path):
    # A win-64 package has no _SUBDIR_ARCH entry -> skipped, not failed.
    p = _make_conda(tmp_path, "win-64", {_BINDING: _fake_macho_thin(_ARM64)})
    assert mac.audit_package(p) == []
