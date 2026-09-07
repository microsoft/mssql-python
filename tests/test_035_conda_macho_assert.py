"""Arch-slice tests for eng/scripts/assert_macho_arch.py (the macOS twin of test_030).

The osx-arm64 conda package is CROSS-built on an Intel agent where the arm64 slice cannot
run, so the build-time runtime import is skipped and the package's arch is otherwise trusted
from the universal2 wheel tag. This asserts the static Mach-O check catches a mislabeled/thin
(x86_64-only) binary inside an osx-arm64 package -- the exact gap for the osx legs.
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
    """A minimal 64-bit little-endian thin Mach-O: MH_MAGIC_64 then the cputype word."""
    buf = bytearray(b"\x00" * 0x40)
    struct.pack_into("<I", buf, 0, 0xFEEDFACF)  # MH_MAGIC_64 (little-endian on disk)
    struct.pack_into("<I", buf, 4, cputype)
    return bytes(buf)


def _fake_macho_fat(cputypes) -> bytes:
    """A minimal universal (FAT_MAGIC) binary listing the given slices, like universal2."""
    buf = bytearray()
    buf += struct.pack(">I", 0xCAFEBABE)  # FAT_MAGIC (fat header is big-endian on disk)
    buf += struct.pack(">I", len(cputypes))  # nfat_arch
    for ct in cputypes:
        # fat_arch: cputype, cpusubtype, offset, size, align (all big-endian, 20 bytes).
        buf += struct.pack(">IIIII", ct, 0, 0, 0, 0)
    return bytes(buf)


def test_macho_arches_thin():
    assert mac.macho_arches(_fake_macho_thin(_ARM64)) == {"arm64"}
    assert mac.macho_arches(_fake_macho_thin(_X86_64)) == {"x86_64"}


def test_macho_arches_fat_universal2():
    assert mac.macho_arches(_fake_macho_fat([_X86_64, _ARM64])) == {"x86_64", "arm64"}


def test_macho_arches_rejects_non_macho():
    assert mac.macho_arches(b"not a mach-o binary at all") is None
    assert mac.macho_arches(b"\xcf\xfa") is None  # too short


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
_DRIVER = "lib/python3.12/site-packages/mssql_python_odbc/libs/macos/lib/libmsodbcsql.18.dylib"


@pytest.mark.skipif(not _zstd_available(), reason="no zstandard backend available")
def test_osx_arm64_universal2_binaries_pass(tmp_path):
    # Real shipped case: universal2 (both slices) binding + driver in an osx-arm64 package.
    p = _make_conda(
        tmp_path,
        "osx-arm64",
        {
            _BINDING: _fake_macho_fat([_X86_64, _ARM64]),
            _DRIVER: _fake_macho_fat([_X86_64, _ARM64]),
        },
    )
    assert mac.audit_package(p) == []


@pytest.mark.skipif(not _zstd_available(), reason="no zstandard backend available")
def test_osx_arm64_thin_arm64_binaries_pass(tmp_path):
    p = _make_conda(
        tmp_path,
        "osx-arm64",
        {_BINDING: _fake_macho_thin(_ARM64), _DRIVER: _fake_macho_thin(_ARM64)},
    )
    assert mac.audit_package(p) == []


@pytest.mark.skipif(not _zstd_available(), reason="no zstandard backend available")
def test_osx_arm64_x86_64_only_binary_fails(tmp_path):
    # The exact bug this guard exists for: a thin x86_64 binary inside an osx-arm64 package.
    p = _make_conda(
        tmp_path,
        "osx-arm64",
        {_BINDING: _fake_macho_thin(_X86_64), _DRIVER: _fake_macho_thin(_ARM64)},
    )
    errors = mac.audit_package(p)
    assert any("x86_64" in e and "arm64" in e for e in errors)


@pytest.mark.skipif(not _zstd_available(), reason="no zstandard backend available")
def test_osx_arm64_missing_driver_fails(tmp_path):
    # Binding present but no vendored driver dylib -> the presence gate must fail.
    p = _make_conda(tmp_path, "osx-arm64", {_BINDING: _fake_macho_fat([_X86_64, _ARM64])})
    errors = mac.audit_package(p)
    assert any("libmsodbcsql" in e or "ODBC driver" in e for e in errors)


@pytest.mark.skipif(not _zstd_available(), reason="no zstandard backend available")
def test_osx_64_x86_64_binaries_pass(tmp_path):
    p = _make_conda(
        tmp_path,
        "osx-64",
        {_BINDING: _fake_macho_fat([_X86_64, _ARM64]), _DRIVER: _fake_macho_thin(_X86_64)},
    )
    assert mac.audit_package(p) == []


@pytest.mark.skipif(not _zstd_available(), reason="no zstandard backend available")
def test_non_osx_subdir_is_skipped(tmp_path):
    # A win-64 package has no _SUBDIR_ARCH entry -> skipped, not failed.
    p = _make_conda(tmp_path, "win-64", {_BINDING: _fake_macho_thin(_ARM64)})
    assert mac.audit_package(p) == []
