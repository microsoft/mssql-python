"""Unit tests for the masking-immune RUNPATH audit (eng/scripts/audit_bundled_binaries.py).

The audit reads the ELF ``PT_DYNAMIC`` program header of the vendored Linux ODBC
binaries and asserts the EXACT ``$ORIGIN`` climb to ``$PREFIX/lib``, the effective
RUNPATH (DT_RUNPATH beats DT_RPATH), declared conda deps, and DT_NEEDED. These tests
craft minimal ELF64 blobs (with real program headers) + ``.tar.bz2`` conda packages
(no zstd backend needed) and exercise the pure logic.
"""

import importlib.util
import io
import json
import struct
import sys
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
    # audit_bundled_binaries.py imports its sibling _conda_pkg; put eng/scripts on sys.path so
    # the by-path load here resolves it (a direct `python <script>` run gets this for free).
    sys.path.insert(0, str(_MODULE_PATH.parent))
    spec = importlib.util.spec_from_file_location("audit_bundled_binaries_under_test", _MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


audit = _load_module()


def _make_elf64(runpath=None, rpath=None, needed=(), machine=62):
    """Build a minimal, self-consistent ELF64-LE with real program headers.

    Emits a PT_LOAD (vaddr == file offset, covering the whole file) + a PT_DYNAMIC,
    so the audit's loader-style PT_DYNAMIC + PT_LOAD vaddr mapping resolves the
    strings. ``runpath``/``rpath`` are optional; ``needed`` is a list of sonames.
    ``machine`` is the ELF e_machine (62 x86-64 / 183 aarch64).
    """
    ehdr_size = 64
    phdr_size = 56
    nph = 2
    phoff = ehdr_size
    dynstr_off = phoff + nph * phdr_size  # dynstr immediately follows the phdrs

    dynstr = b"\x00"

    def add_str(s):
        nonlocal dynstr
        off = len(dynstr)
        dynstr += s.encode() + b"\x00"
        return off

    rp_rel = add_str(runpath) if runpath is not None else None
    rpath_rel = add_str(rpath) if rpath is not None else None
    need_rels = [add_str(n) for n in needed]

    dynamic_off = dynstr_off + len(dynstr)
    dyn = b""
    if rp_rel is not None:
        dyn += struct.pack("<qQ", audit._DT_RUNPATH, rp_rel)
    if rpath_rel is not None:
        dyn += struct.pack("<qQ", audit._DT_RPATH, rpath_rel)
    for nr in need_rels:
        dyn += struct.pack("<qQ", audit._DT_NEEDED, nr)
    dyn += struct.pack("<qQ", audit._DT_STRTAB, dynstr_off)  # vaddr == offset (PT_LOAD v=0)
    dyn += struct.pack("<qQ", 0, 0)  # DT_NULL

    total = dynamic_off + len(dyn)

    # PT_LOAD: type, flags, offset, vaddr, paddr, filesz, memsz, align
    ph_load = struct.pack("<IIQQQQQQ", audit._PT_LOAD, 5, 0, 0, 0, total, total, 0x1000)
    ph_dyn = struct.pack(
        "<IIQQQQQQ",
        audit._PT_DYNAMIC,
        6,
        dynamic_off,
        dynamic_off,
        dynamic_off,
        len(dyn),
        len(dyn),
        8,
    )

    e_ident = b"\x7fELF" + bytes([2, 1, 1, 0]) + b"\x00" * 8  # 64-bit, LE, v1
    ehdr = e_ident + struct.pack(
        "<HHIQQQIHHHHHH",
        3,  # e_type ET_DYN
        machine,  # e_machine (62 x86-64 / 183 aarch64)
        1,  # e_version
        0,  # e_entry
        phoff,  # e_phoff
        0,  # e_shoff (unused: audit reads program headers)
        0,  # e_flags
        64,  # e_ehsize
        phdr_size,  # e_phentsize
        nph,  # e_phnum
        0,  # e_shentsize
        0,  # e_shnum
        0,  # e_shstrndx
    )
    assert len(ehdr) == ehdr_size
    return ehdr + ph_load + ph_dyn + dynstr + dyn


_LIBDIR = "lib/python3.12/site-packages/mssql_python_odbc/libs/linux/debian_ubuntu/x86_64/lib"
# The exact climb the audit computes from _LIBDIR to package-root lib (8 levels up).
_CLIMB_ENTRY = "$ORIGIN/../../../../../../../.."
_GOOD_RUNPATH = "$ORIGIN:" + _CLIMB_ENTRY
_DRIVER_NEEDED = ["libkrb5.so.3", "libgssapi_krb5.so.2", "libodbcinst.so.2"]
_INST_NEEDED = ["libltdl.so.7"]
_GOOD_DEPENDS = ["python", "azure-identity", "krb5", "libtool", "openssl >=3,<4"]


def _make_pkg(
    tmp_path,
    runpath=_GOOD_RUNPATH,
    rpath=None,
    subdir="linux-64",
    vendored=None,
    depends=None,
    driver_needed=None,
    inst_needed=None,
    machine=62,
):
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
                    "depends": _GOOD_DEPENDS if depends is None else depends,
                }
            ).encode(),
        )
        add(
            f"{_LIBDIR}/libmsodbcsql-18.6.so.2.1",
            _make_elf64(
                runpath=runpath,
                rpath=rpath,
                needed=_DRIVER_NEEDED if driver_needed is None else driver_needed,
                machine=machine,
            ),
        )
        add(
            f"{_LIBDIR}/libodbcinst.so.2",
            _make_elf64(
                runpath=runpath,
                rpath=rpath,
                needed=_INST_NEEDED if inst_needed is None else inst_needed,
                machine=machine,
            ),
        )
        if vendored:
            add(f"{_LIBDIR}/{vendored}", b"\x7fELF fake-vendored")
    return str(p)


# --- low-level parser -------------------------------------------------------


def test_elf_dynamic_pt_parse():
    data = _make_elf64(runpath=_GOOD_RUNPATH, needed=["libkrb5.so.3", "libodbcinst.so.2"])
    dyn = audit.elf_dynamic(data)
    assert dyn["runpath"] == _GOOD_RUNPATH
    assert dyn["rpath"] is None
    assert "libkrb5.so.3" in dyn["needed"] and "libodbcinst.so.2" in dyn["needed"]


def test_effective_runpath_prefers_runpath_over_rpath():
    # DT_RUNPATH present -> loader ignores DT_RPATH.
    dyn = audit.elf_dynamic(_make_elf64(runpath="$ORIGIN", rpath=_GOOD_RUNPATH))
    assert audit.effective_runpath(dyn) == "$ORIGIN"
    # Only DT_RPATH present -> that is the effective one.
    dyn2 = audit.elf_dynamic(_make_elf64(rpath=_GOOD_RUNPATH))
    assert audit.effective_runpath(dyn2) == _GOOD_RUNPATH


def test_expected_climb_entry_is_exact():
    member = f"{_LIBDIR}/libmsodbcsql-18.6.so.2.1"
    assert audit.expected_climb_entry(member) == _CLIMB_ENTRY


# --- audit_package: the happy path -----------------------------------------


def test_audit_passes_with_exact_climb(tmp_path):
    assert audit.audit_package(_make_pkg(tmp_path)) == []


# --- N1: wrong climb variants must all FAIL --------------------------------


def test_audit_fails_wrong_depth_too_short(tmp_path):
    errors = audit.audit_package(_make_pkg(tmp_path, runpath="$ORIGIN:$ORIGIN/.."))
    assert any("exact climb entry" in e for e in errors)


def test_audit_fails_overshoot(tmp_path):
    over = "$ORIGIN:$ORIGIN/../../../../../../../../.."  # one level too many
    errors = audit.audit_package(_make_pkg(tmp_path, runpath=over))
    assert any("exact climb entry" in e for e in errors)


def test_audit_fails_decoy_rpath_behind_bad_runpath(tmp_path):
    # Good climb hidden in DT_RPATH, but DT_RUNPATH (which the loader uses) is bare.
    errors = audit.audit_package(_make_pkg(tmp_path, runpath="$ORIGIN", rpath=_GOOD_RUNPATH))
    assert any("exact climb entry" in e for e in errors)


def test_audit_fails_malformed_originator(tmp_path):
    bad = "$ORIGINATOR/../../../../../../../.."  # startswith('$ORIGIN') but wrong token
    errors = audit.audit_package(_make_pkg(tmp_path, runpath=bad))
    assert any("exact climb entry" in e for e in errors)


def test_audit_fails_on_absolute_rpath(tmp_path):
    errors = audit.audit_package(_make_pkg(tmp_path, runpath="$ORIGIN:/opt/lib"))
    assert any("ABSOLUTE" in e for e in errors)


def test_audit_fails_empty_runpath_entry(tmp_path):
    # A trailing ':' (empty entry = current-directory search) must FAIL, even though the
    # NON-empty entries are exactly {$ORIGIN, climb} that _entries() would otherwise accept.
    errors = audit.audit_package(_make_pkg(tmp_path, runpath=_GOOD_RUNPATH + ":"))
    assert any("EMPTY entry" in e for e in errors)


def test_audit_fails_missing_bare_origin(tmp_path):
    # Climb entry present but bare $ORIGIN dropped -> the driver can no longer resolve
    # its co-located sibling libodbcinst.so.2 even though $PREFIX/lib is reachable.
    errors = audit.audit_package(_make_pkg(tmp_path, runpath=_CLIMB_ENTRY))
    assert any("bare '$ORIGIN'" in e for e in errors)


# --- N2: declared deps + DT_NEEDED -----------------------------------------


def test_audit_fails_missing_declared_krb5(tmp_path):
    depends = ["python", "azure-identity", "libtool", "openssl >=3,<4"]  # no krb5
    errors = audit.audit_package(_make_pkg(tmp_path, depends=depends))
    assert any("missing 'krb5'" in e for e in errors)


def test_audit_fails_missing_declared_libtool(tmp_path):
    depends = ["python", "azure-identity", "krb5", "openssl >=3,<4"]  # no libtool
    errors = audit.audit_package(_make_pkg(tmp_path, depends=depends))
    assert any("missing 'libtool'" in e for e in errors)


def test_audit_fails_missing_declared_openssl(tmp_path):
    depends = ["python", "azure-identity", "krb5", "libtool"]  # no openssl
    errors = audit.audit_package(_make_pkg(tmp_path, depends=depends))
    assert any("missing 'openssl'" in e for e in errors)


def test_audit_fails_openssl_not_range_pinned(tmp_path):
    # openssl present but not pinned to the Driver-18 ABI range (>=3,<4).
    depends = ["python", "azure-identity", "krb5", "libtool", "openssl"]
    errors = audit.audit_package(_make_pkg(tmp_path, depends=depends))
    assert any("range-pinned" in e for e in errors)


def test_audit_fails_openssl_loose_upper_bound(tmp_path):
    # '<40' merely CONTAINS the substring '<4' but admits openssl 4..39 -> must FAIL
    # (the substring heuristic this replaced would have false-passed here).
    depends = ["python", "azure-identity", "krb5", "libtool", "openssl >=3,<40"]
    errors = audit.audit_package(_make_pkg(tmp_path, depends=depends))
    assert any("range-pinned" in e for e in errors)


def test_audit_passes_openssl_alpha_upper_bound(tmp_path):
    # conda's canonical exclusive upper bound is '<4.0a0'; the proper parse must accept it.
    depends = ["python", "azure-identity", "krb5", "libtool", "openssl >=3,<4.0a0"]
    errors = audit.audit_package(_make_pkg(tmp_path, depends=depends))
    assert not any("range-pinned" in e for e in errors)


def test_audit_fails_openssl_upper_admits_4x(tmp_path):
    # '<=4', '<4.1', '<4.0.1' each admit some openssl 4.x -> must FAIL (looser than the pin).
    for spec in ("openssl >=3,<=4", "openssl >=3,<4.1", "openssl >=3,<4.0.1"):
        depends = ["python", "azure-identity", "krb5", "libtool", spec]
        errors = audit.audit_package(_make_pkg(tmp_path, depends=depends))
        assert any("range-pinned" in e for e in errors), spec


def test_audit_passes_openssl_exclusive_4_variants(tmp_path):
    # '<4', '<4.0', '<4.0.0' all exclude every openssl 4.x and are accepted.
    for spec in ("openssl >=3,<4", "openssl >=3,<4.0", "openssl >=3,<4.0.0"):
        depends = ["python", "azure-identity", "krb5", "libtool", spec]
        errors = audit.audit_package(_make_pkg(tmp_path, depends=depends))
        assert not any("range-pinned" in e for e in errors), spec


def test_audit_fails_openssl_or_group_or_garbage_clause(tmp_path):
    # A conda OR-group ('>=3|>=1'), a garbage clause, or a prefix-parse spelling ('<4garbage')
    # must all FAIL CLOSED -- the allowlist admits only canonical bound spellings.
    for spec in ("openssl >=3|>=1,<4", "openssl >=3,foo,<4", "openssl >=3,<4garbage"):
        depends = ["python", "azure-identity", "krb5", "libtool", spec]
        errors = audit.audit_package(_make_pkg(tmp_path, depends=depends))
        assert any("range-pinned" in e for e in errors), spec


def test_audit_fails_driver_missing_from_one_subdir(tmp_path):
    # debian_ubuntu is complete, but rhel ships only libodbcinst (driver dropped). A
    # package-global count would pass since debian_ubuntu supplies a driver; per-subdir
    # presence must catch the rhel gap.
    rhel_lib = "lib/python3.12/site-packages/mssql_python_odbc/libs/linux/rhel/x86_64/lib"
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
                    "subdir": "linux-64",
                    "depends": _GOOD_DEPENDS,
                }
            ).encode(),
        )
        add(
            f"{_LIBDIR}/libmsodbcsql-18.6.so.2.1", _make_elf64(_GOOD_RUNPATH, needed=_DRIVER_NEEDED)
        )
        add(f"{_LIBDIR}/libodbcinst.so.2", _make_elf64(_GOOD_RUNPATH, needed=_INST_NEEDED))
        # rhel: libodbcinst only -- the driver is missing from this subdir.
        add(f"{rhel_lib}/libodbcinst.so.2", _make_elf64(_GOOD_RUNPATH, needed=_INST_NEEDED))
    errors = audit.audit_package(str(p))
    assert any("rhel" in e and "libmsodbcsql" in e for e in errors)


def test_audit_fails_driver_lost_needed(tmp_path):
    # Driver stopped NEEDing libgssapi_krb5 -> declared krb5 dep is now moot.
    errors = audit.audit_package(
        _make_pkg(tmp_path, driver_needed=["libkrb5.so.3", "libodbcinst.so.2"])
    )
    assert any("libgssapi_krb5" in e and "no longer NEED" in e for e in errors)


def test_audit_fails_needed_substring_impostor(tmp_path):
    # libkrb5support.so.0 must NOT satisfy the required libkrb5.so DT_NEEDED -- the bare
    # substring 'libkrb5' would have (the '.so' anchor closes that hole).
    errors = audit.audit_package(
        _make_pkg(
            tmp_path,
            driver_needed=["libkrb5support.so.0", "libgssapi_krb5.so.2", "libodbcinst.so.2"],
        )
    )
    assert any("libkrb5.so" in e and "no longer NEED" in e for e in errors)


def test_audit_fails_odbcinst_lost_libltdl(tmp_path):
    errors = audit.audit_package(_make_pkg(tmp_path, inst_needed=["libc.so.6"]))
    assert any("libltdl" in e and "no longer NEED" in e for e in errors)


def test_audit_allows_musl_variant_without_libltdl(tmp_path):
    # The alpine/musl libodbcinst NEEDs libc.musl* and statically links ltdl, so the
    # glibc libltdl DT_NEEDED requirement must NOT fail it. Package has a complete glibc
    # debian_ubuntu variant plus an alpine/musl variant.
    alpine_lib = "lib/python3.12/site-packages/mssql_python_odbc/libs/linux/alpine/x86_64/lib"
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
                    "subdir": "linux-64",
                    "depends": _GOOD_DEPENDS,
                }
            ).encode(),
        )
        # glibc debian_ubuntu (complete: NEEDs libltdl/krb5).
        add(
            f"{_LIBDIR}/libmsodbcsql-18.6.so.2.1", _make_elf64(_GOOD_RUNPATH, needed=_DRIVER_NEEDED)
        )
        add(f"{_LIBDIR}/libodbcinst.so.2", _make_elf64(_GOOD_RUNPATH, needed=_INST_NEEDED))
        # alpine/musl: driver + inst link libc.musl and do NOT NEED libltdl.
        add(
            f"{alpine_lib}/libmsodbcsql-18.6.so.2.1",
            _make_elf64(
                _GOOD_RUNPATH,
                needed=[
                    "libodbcinst.so.2",
                    "libkrb5.so.3",
                    "libgssapi_krb5.so.2",
                    "libc.musl-x86_64.so.1",
                ],
            ),
        )
        add(
            f"{alpine_lib}/libodbcinst.so.2",
            _make_elf64(_GOOD_RUNPATH, needed=["libc.musl-x86_64.so.1"]),
        )
    assert audit.audit_package(str(p)) == []


# --- vendoring + malformed + non-linux -------------------------------------


def test_audit_fails_on_vendored_crypto(tmp_path):
    errors = audit.audit_package(_make_pkg(tmp_path, vendored="libkrb5.so.3"))
    assert any("vendors" in e for e in errors)


def test_audit_fails_malformed_package(tmp_path):
    # A .tar.bz2 with no info/index.json must FAIL (never silently skipped as non-Linux).
    p = tmp_path / "broken-1.0-0.tar.bz2"
    with tarfile.open(p, "w:bz2") as tf:
        data = b"not an index"
        ti = tarfile.TarInfo("some/other/file")
        ti.size = len(data)
        tf.addfile(ti, io.BytesIO(data))
    errors = audit.audit_package(str(p))
    assert any("unreadable/malformed" in e for e in errors)


def test_audit_skips_non_linux(tmp_path):
    assert audit.audit_package(_make_pkg(tmp_path, subdir="win-64")) == []


# --- arch gate (e_machine vs conda subdir) ---------------------------------


def test_audit_fails_wrong_arch(tmp_path):
    # x86-64 ELFs (e_machine=62) mislabeled inside a linux-aarch64 package must FAIL the
    # arch gate -- the emulated aarch64 leg's best-effort runtime probe would not catch it.
    errors = audit.audit_package(_make_pkg(tmp_path, subdir="linux-aarch64"))
    assert any("does not match" in e and "aarch64" in e for e in errors)


def test_audit_passes_matching_arch_aarch64(tmp_path):
    # aarch64 ELFs (e_machine=183) in a linux-aarch64 package satisfy the arch gate.
    assert audit.audit_package(_make_pkg(tmp_path, subdir="linux-aarch64", machine=183)) == []


def test_elf_machine_reads_arch():
    assert audit.elf_machine(_make_elf64()) == 62
    assert audit.elf_machine(_make_elf64(machine=183)) == 183
    assert audit.elf_machine(b"not an elf") is None


def test_audit_fails_unknown_linux_subdir(tmp_path):
    # A linux subdir with no e_machine mapping (e.g. a future linux-ppc64le) must FAIL CLOSED,
    # not silently skip the architecture gate this audit exists to enforce.
    errors = audit.audit_package(_make_pkg(tmp_path, subdir="linux-ppc64le"))
    assert any("unrecognized Linux subdir" in e for e in errors)
