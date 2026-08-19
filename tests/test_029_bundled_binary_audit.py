"""Unit tests for the masking-immune RUNPATH audit (eng/scripts/audit_bundled_binaries.py).

The audit reads the ELF RUNPATH bytes of the vendored Linux ODBC binaries in each
built conda package and asserts the #563 relative ``$ORIGIN`` climb is present, no
absolute RPATH entry exists, and no krb5/openssl/libltdl is vendored (those must be
declared conda deps). These tests craft minimal ELF64 blobs + ``.tar.bz2`` conda
packages (no zstd backend needed) and exercise the pure logic.
"""

import importlib.util
import io
import json
import struct
import tarfile
from pathlib import Path

import pytest

_MODULE_PATH = (
    Path(__file__).resolve().parent.parent / "eng" / "scripts" / "audit_bundled_binaries.py"
)

# The eng/ sources are not shipped inside the built wheel; the installed-wheel test
# leg copies only tests/. Skip the whole module when the audited source is absent.
if not _MODULE_PATH.is_file():
    pytest.skip(
        f"audit source not present ({_MODULE_PATH}); skipping RUNPATH audit tests",
        allow_module_level=True,
    )


def _load_module():
    spec = importlib.util.spec_from_file_location("audit_bundled_binaries_under_test", _MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


audit = _load_module()


def _make_elf64(runpath, needed="libodbcinst.so.2"):
    """Build a minimal, self-consistent ELF64-LE with a DT_RUNPATH + DT_NEEDED."""
    dynstr = b"\x00"
    rp_off = len(dynstr)
    dynstr += runpath.encode() + b"\x00"
    nd_off = len(dynstr)
    dynstr += needed.encode() + b"\x00"

    dynamic = b""
    dynamic += struct.pack("<qQ", audit._DT_RUNPATH, rp_off)
    dynamic += struct.pack("<qQ", audit._DT_NEEDED, nd_off)
    dynamic += struct.pack("<qQ", 0, 0)  # DT_NULL

    ehdr_size = 64
    dynstr_off = ehdr_size
    dynamic_off = dynstr_off + len(dynstr)
    shoff = dynamic_off + len(dynamic)

    def shdr(sh_type, sh_offset, sh_size, sh_link, sh_entsize):
        return struct.pack(
            "<IIQQQQIIQQ", 0, sh_type, 0, 0, sh_offset, sh_size, sh_link, 0, 0, sh_entsize
        )

    sh_null = shdr(0, 0, 0, 0, 0)
    sh_dynstr = shdr(3, dynstr_off, len(dynstr), 0, 0)  # SHT_STRTAB
    sh_dynamic = shdr(
        audit._SHT_DYNAMIC, dynamic_off, len(dynamic), 1, 16
    )  # link -> .dynstr (idx 1)
    shdrs = sh_null + sh_dynstr + sh_dynamic

    e_ident = b"\x7fELF" + bytes([2, 1, 1, 0]) + b"\x00" * 8  # 64-bit, LE, v1
    ehdr = e_ident + struct.pack(
        "<HHIQQQIHHHHHH",
        3,  # e_type ET_DYN
        62,  # e_machine x86-64
        1,  # e_version
        0,  # e_entry
        0,  # e_phoff
        shoff,  # e_shoff
        0,  # e_flags
        64,  # e_ehsize
        0,  # e_phentsize
        0,  # e_phnum
        64,  # e_shentsize
        3,  # e_shnum
        0,  # e_shstrndx
    )
    assert len(ehdr) == ehdr_size
    return ehdr + dynstr + dynamic + shdrs


_LIBDIR = "lib/python3.12/site-packages/mssql_python_odbc/libs/linux/debian_ubuntu/x86_64/lib"
_CLIMB = "$ORIGIN:$ORIGIN/../../../../../../../.."


def _make_pkg(tmp_path, runpath, subdir="linux-64", vendored=None):
    """Write a minimal .tar.bz2 conda package with two ELF driver binaries."""
    p = tmp_path / "mssql-python-1.13.0-py312_0.tar.bz2"
    with tarfile.open(p, "w:bz2") as tf:

        def add(name, data):
            ti = tarfile.TarInfo(name)
            ti.size = len(data)
            tf.addfile(ti, io.BytesIO(data))

        add(
            "info/index.json",
            json.dumps(
                {
                    "name": "mssql-python",
                    "version": "1.13.0",
                    "build": "py312_0",
                    "subdir": subdir,
                }
            ).encode(),
        )
        add(f"{_LIBDIR}/libmsodbcsql-18.6.so.2.1", _make_elf64(runpath, needed="libkrb5.so.3"))
        add(f"{_LIBDIR}/libodbcinst.so.2", _make_elf64(runpath, needed="libltdl.so.7"))
        if vendored:
            add(f"{_LIBDIR}/{vendored}", b"\x7fELF fake-vendored")
    return str(p)


def test_elf_dynamic_reads_runpath_and_needed():
    data = _make_elf64(_CLIMB, needed="libodbcinst.so.2")
    dyn = audit.elf_dynamic(data, (audit._DT_RUNPATH, audit._DT_NEEDED))
    assert dyn[audit._DT_RUNPATH] == [_CLIMB]
    assert "libodbcinst.so.2" in dyn[audit._DT_NEEDED]


def test_has_relative_climb():
    assert audit._has_relative_climb(["$ORIGIN:$ORIGIN/../../.."])
    assert not audit._has_relative_climb(["$ORIGIN"])  # bare origin, no climb
    assert not audit._has_relative_climb([])  # no runpath at all
    assert not audit._has_relative_climb(["/usr/lib"])  # absolute only


def test_absolute_entries():
    assert audit._absolute_entries(["$ORIGIN:/usr/lib"]) == ["/usr/lib"]
    assert audit._absolute_entries(["$ORIGIN:$ORIGIN/../.."]) == []


def test_audit_passes_with_climb(tmp_path):
    assert audit.audit_package(_make_pkg(tmp_path, _CLIMB)) == []


def test_audit_fails_without_climb(tmp_path):
    errors = audit.audit_package(_make_pkg(tmp_path, "$ORIGIN"))  # bare, no climb
    assert any("climb" in e for e in errors)


def test_audit_fails_on_absolute_rpath(tmp_path):
    errors = audit.audit_package(_make_pkg(tmp_path, "$ORIGIN:/opt/lib"))
    assert any("ABSOLUTE" in e for e in errors)


def test_audit_fails_on_vendored_crypto(tmp_path):
    errors = audit.audit_package(_make_pkg(tmp_path, _CLIMB, vendored="libkrb5.so.3"))
    assert any("vendors" in e for e in errors)


def test_audit_skips_non_linux(tmp_path):
    # win/osx packages carry no governed ELF payload -> clean no-op regardless of
    # what the (irrelevant) crafted payload contains.
    assert audit.audit_package(_make_pkg(tmp_path, "$ORIGIN", subdir="win-64")) == []
