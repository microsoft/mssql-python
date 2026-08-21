#!/usr/bin/env python3
"""Masking-immune audit of the vendored Linux ODBC binaries in built conda packages.

The #563 reachability fix lives in the ELF RUNPATH of the vendored driver, not in a
comment or a runtime probe. A runtime ``ldd``/import check can PASS on any host that
happens to carry a system ``krb5``/``libltdl`` -- the driver silently binds the
system copy and a wrong/missing conda climb goes unnoticed (exactly what the full CI
agents hide). This audit is immune to that masking: it reads the ELF bytes straight
out of each built ``.conda`` payload -- via the ``PT_DYNAMIC`` program header the
*loader itself* uses -- and asserts, statically and exactly:

  * ``libmsodbcsql*`` and ``libodbcinst.so.2`` carry the EXACT relative ``$ORIGIN``
    climb that lands on the package-root ``lib`` (== ``$PREFIX/lib``), computed from
    each binary's own location -- not a substring, not "any ``..``". A too-short,
    overshooting, or ``$ORIGINATOR`` climb FAILS;
  * that climb entry appears in the EFFECTIVE RUNPATH: the loader honours
    ``DT_RUNPATH`` and IGNORES ``DT_RPATH`` when ``DT_RUNPATH`` is present, so a good
    ``DT_RPATH`` decoy behind a bad ``DT_RUNPATH`` FAILS;
  * no absolute RPATH entry exists (stay relocatable);
  * the run deps that SERVICE the driver -- ``krb5``, ``libtool`` (libltdl provider),
    ``openssl`` -- are DECLARED in ``info/index.json`` ``depends`` (deleting a dep
    from ``meta.yaml`` must fail here, not just be masked at runtime), and the driver
    still ``DT_NEEDED``s ``libkrb5``/``libgssapi_krb5``/``libodbcinst`` (and
    ``libodbcinst`` still needs ``libltdl``) so a driver that stopped needing krb5 is
    caught too;
  * no ``krb5``/``openssl``/``libltdl`` is VENDORED inside the payload (they are
    serviced by conda, never bundled).

Non-Linux packages (``win-*`` / ``osx-*``) have no such ELF payload and are skipped.
An unreadable/malformed package FAILS (it is never silently treated as non-Linux).

Exit code 0 = every Linux package is exactly self-contained; non-zero = a violation
was found (blocks the build/release).
"""

from __future__ import annotations

import argparse
import glob
import io
import json
import os
import posixpath
import struct
import sys
import tarfile
import zipfile

# --- ELF constants ---------------------------------------------------------
_DT_NEEDED = 1
_DT_STRTAB = 5
_DT_RPATH = 15
_DT_RUNPATH = 29
_PT_LOAD = 1
_PT_DYNAMIC = 2

# Driver binaries whose RUNPATH must carry the exact $ORIGIN climb.
_DRIVER_PREFIXES = ("libmsodbcsql-", "libmsodbcsql.")
_ODBCINST = "libodbcinst.so.2"

# Libraries that must be serviced by DECLARED conda deps, never vendored into the
# Linux payload (bundling these is the anti-pattern the recipe avoids).
_MUST_NOT_VENDOR = ("libkrb5", "libgssapi", "libssl", "libcrypto", "libltdl")

# conda run-deps that SERVICE the driver's krb5/openssl/libltdl. Missing any means
# the $PREFIX/lib copy the RUNPATH climb points at would not exist -- declaration is
# as load-bearing as the climb itself.
_REQUIRED_DEPS = ("krb5", "libtool", "openssl")

# Expected DT_NEEDED soname substrings, so a driver that silently STOPPED needing
# krb5 (making the declared dep moot) is caught too.
_DRIVER_NEEDED = ("libkrb5", "libgssapi_krb5", "libodbcinst")
_ODBCINST_NEEDED = ("libltdl",)


def _zstd_decompress(raw: bytes) -> bytes:
    """Decompress a zstandard blob, preferring the 3.14+ stdlib backend."""
    try:  # Python 3.14+
        from compression import zstd  # type: ignore

        return zstd.decompress(raw)
    except Exception:
        pass
    import zstandard  # third-party fallback

    return zstandard.ZstdDecompressor().decompress(raw)


def _is_elf(data: bytes) -> bool:
    return len(data) >= 64 and data[:4] == b"\x7fELF"


def elf_dynamic(data: bytes) -> dict:
    """Return ``{'runpath': str|None, 'rpath': str|None, 'needed': [str]}``.

    Parses the ``PT_DYNAMIC`` program header -- the segment the LOADER actually uses
    -- and maps ``DT_STRTAB``'s virtual address to a file offset through the
    ``PT_LOAD`` segments, so this matches the loader's own view rather than a section
    table that a stripped/rewritten binary might not carry. Handles ELF32/ELF64 and
    both endiannesses; the shipped drivers are ELF64-LE.
    """
    out: dict = {"runpath": None, "rpath": None, "needed": []}
    if not _is_elf(data):
        return out
    is64 = data[4] == 2
    en = "<" if data[5] == 1 else ">"

    if is64:
        e_phoff = struct.unpack_from(en + "Q", data, 0x20)[0]
        e_phentsize = struct.unpack_from(en + "H", data, 0x36)[0]
        e_phnum = struct.unpack_from(en + "H", data, 0x38)[0]
    else:
        e_phoff = struct.unpack_from(en + "I", data, 0x1C)[0]
        e_phentsize = struct.unpack_from(en + "H", data, 0x2A)[0]
        e_phnum = struct.unpack_from(en + "H", data, 0x2C)[0]
    if not e_phoff or not e_phnum:
        return out

    loads = []  # (p_vaddr, p_offset, p_filesz)
    dyn = None  # (p_offset, p_filesz)
    for i in range(e_phnum):
        off = e_phoff + i * e_phentsize
        if off + e_phentsize > len(data):
            return out
        p_type = struct.unpack_from(en + "I", data, off)[0]
        if is64:
            p_offset = struct.unpack_from(en + "Q", data, off + 8)[0]
            p_vaddr = struct.unpack_from(en + "Q", data, off + 16)[0]
            p_filesz = struct.unpack_from(en + "Q", data, off + 32)[0]
        else:
            p_offset = struct.unpack_from(en + "I", data, off + 4)[0]
            p_vaddr = struct.unpack_from(en + "I", data, off + 8)[0]
            p_filesz = struct.unpack_from(en + "I", data, off + 16)[0]
        if p_type == _PT_LOAD:
            loads.append((p_vaddr, p_offset, p_filesz))
        elif p_type == _PT_DYNAMIC:
            dyn = (p_offset, p_filesz)
    if dyn is None:
        return out
    dyn_off, dyn_size = dyn

    def vaddr_to_off(vaddr: int):
        for v, o, sz in loads:
            if v <= vaddr < v + sz:
                return vaddr - v + o
        return None

    strtab_vaddr = None
    runpath_rel = None
    rpath_rel = None
    needed_rel: list[int] = []
    entsize = 16 if is64 else 8
    for off in range(dyn_off, dyn_off + dyn_size, entsize):
        if off + entsize > len(data):
            break
        if is64:
            d_tag = struct.unpack_from(en + "q", data, off)[0]
            d_val = struct.unpack_from(en + "Q", data, off + 8)[0]
        else:
            d_tag = struct.unpack_from(en + "i", data, off)[0]
            d_val = struct.unpack_from(en + "I", data, off + 4)[0]
        if d_tag == 0:  # DT_NULL terminates the array
            break
        if d_tag == _DT_STRTAB:
            strtab_vaddr = d_val
        elif d_tag == _DT_RUNPATH:
            runpath_rel = d_val
        elif d_tag == _DT_RPATH:
            rpath_rel = d_val
        elif d_tag == _DT_NEEDED:
            needed_rel.append(d_val)
    if strtab_vaddr is None:
        return out
    strtab_off = vaddr_to_off(strtab_vaddr)
    if strtab_off is None:
        return out

    def read_str(rel: int) -> str:
        pos = strtab_off + rel
        end = data.find(b"\x00", pos)
        return data[pos : (end if end >= 0 else len(data))].decode("utf-8", "replace")

    if runpath_rel is not None:
        out["runpath"] = read_str(runpath_rel)
    if rpath_rel is not None:
        out["rpath"] = read_str(rpath_rel)
    out["needed"] = [read_str(n) for n in needed_rel]
    return out


def effective_runpath(dyn: dict):
    """The loader ignores ``DT_RPATH`` when ``DT_RUNPATH`` is present."""
    return dyn["runpath"] if dyn["runpath"] is not None else dyn["rpath"]


def _entries(runpath) -> list[str]:
    return [e for e in (runpath or "").split(":") if e]


def expected_climb_entry(member_name: str) -> str:
    """Exact ``$ORIGIN/<climb>`` from the member's own dir to package-root ``lib``.

    conda stores python files at ``lib/pythonX.Y/site-packages/...`` and
    ``$PREFIX/lib`` == package-root ``lib``, so the climb is the POSIX relpath from
    the driver's directory to the top-level ``lib`` (never a hard-coded ``../`` count).
    """
    member_dir = posixpath.dirname(member_name)
    climb = posixpath.relpath("lib", member_dir)
    return "$ORIGIN/" + climb


def _iter_payload_members(path: str):
    """Yield ``(member_name, data_bytes)`` for the files in a conda package payload."""
    if path.endswith(".conda"):
        with zipfile.ZipFile(path) as zf:
            pkg_name = next(
                (n for n in zf.namelist() if n.startswith("pkg-") and n.endswith(".tar.zst")),
                None,
            )
            if pkg_name is None:
                return
            blob = _zstd_decompress(zf.read(pkg_name))
        with tarfile.open(fileobj=io.BytesIO(blob)) as tf:
            for m in tf.getmembers():
                if not m.isfile():
                    continue
                f = tf.extractfile(m)
                if f is not None:
                    yield m.name, f.read()
    elif path.endswith(".tar.bz2"):
        with tarfile.open(path, "r:bz2") as tf:
            for m in tf.getmembers():
                if not m.isfile():
                    continue
                f = tf.extractfile(m)
                if f is not None:
                    yield m.name, f.read()


def read_index(path: str) -> dict:
    """Return the package's ``info/index.json`` as a dict.

    RAISES on a malformed/unreadable package -- callers must NOT swallow this into a
    silent "non-Linux, skip" (a truncated Linux package would then slip through).
    """
    if path.endswith(".conda"):
        with zipfile.ZipFile(path) as zf:
            info_name = next(
                (n for n in zf.namelist() if n.startswith("info-") and n.endswith(".tar.zst")),
                None,
            )
            if info_name is None:
                raise ValueError("no info-*.tar.zst member (malformed .conda)")
            blob = _zstd_decompress(zf.read(info_name))
        with tarfile.open(fileobj=io.BytesIO(blob)) as tf:
            member = tf.extractfile("info/index.json")
            if member is None:
                raise ValueError("info/index.json missing")
            return json.load(member)
    if path.endswith(".tar.bz2"):
        with tarfile.open(path, "r:bz2") as tf:
            member = tf.extractfile("info/index.json")
            if member is None:
                raise ValueError("info/index.json missing")
            return json.load(member)
    raise ValueError("unrecognized conda package extension")


def _dep_names(depends) -> set:
    """The package names (first token) of an ``info/index.json`` ``depends`` list."""
    names = set()
    for d in depends or []:
        token = str(d).strip().split()
        if token:
            names.add(token[0])
    return names


def audit_package(path: str) -> list[str]:
    """Return a list of violation strings for one package (empty == clean)."""
    base_name = os.path.basename(path)
    try:
        index = read_index(path)
    except Exception as exc:  # H2: malformed/unreadable must FAIL, never skip.
        return [f"{base_name}: unreadable/malformed package metadata ({exc})."]

    subdir = str(index.get("subdir", ""))
    if not subdir.startswith("linux"):
        print(f"  SKIP (no Linux ELF payload): {base_name} [subdir={subdir or '?'}]")
        return []

    errors: list[str] = []

    # N2a: the run deps that SERVICE the driver's krb5/openssl/libltdl must be declared.
    dep_names = _dep_names(index.get("depends"))
    for req in _REQUIRED_DEPS:
        if req not in dep_names:
            errors.append(
                f"{base_name}: info/index.json depends is missing '{req}' -- the "
                f"$PREFIX/lib copy the RUNPATH climb targets would not exist. "
                f"depends={sorted(dep_names)}"
            )
    # openssl must be RANGE-pinned for Driver 18 (which supports only the OpenSSL
    # 1.1/3.0 ABI; conda-forge has begun shipping openssl 4), not merely present.
    if "openssl" in dep_names:
        spec = next(
            (str(d) for d in (index.get("depends") or []) if str(d).split()[:1] == ["openssl"]),
            "openssl",
        )
        constraint = spec[len("openssl") :].strip()
        if ">=3" not in constraint or "<4" not in constraint:
            errors.append(
                f"{base_name}: openssl dep '{spec}' is not range-pinned '>=3,<4' "
                f"(Driver 18 supports only the OpenSSL 1.1/3.0 ABI)."
            )

    lib_dirs: set = set()
    dirs_with_driver: set = set()
    dirs_with_inst: set = set()
    vendored: list[str] = []

    for name, data in _iter_payload_members(path):
        base = posixpath.basename(name)
        norm = "/" + name
        member_dir = posixpath.dirname(name)
        # Track every driver lib dir (mssql_python_odbc/libs/linux/<distro>/<arch>/lib).
        if "/libs/linux/" in norm and member_dir.endswith("/lib"):
            lib_dirs.add(member_dir)

        # Flag any crypto/krb5/ltdl library vendored into the Linux payload.
        if "/libs/linux/" in norm and any(
            base.startswith(p) and ".so" in base for p in _MUST_NOT_VENDOR
        ):
            vendored.append(name)
            continue

        is_driver = any(base.startswith(p) for p in _DRIVER_PREFIXES)
        is_inst = base == _ODBCINST
        if not (is_driver or is_inst):
            continue
        if not _is_elf(data):
            errors.append(f"{name}: expected an ELF binary but the header is not ELF.")
            continue

        dyn = elf_dynamic(data)
        entries = _entries(effective_runpath(dyn))
        needed = dyn["needed"]
        # musl/alpine variants (NEEDED libc.musl*) link differently -- their libodbcinst
        # statically resolves libltdl, so the glibc DT_NEEDED requirements below do not
        # apply. There is no musl conda subdir (conda Linux is glibc-only); these variants
        # ride along in the payload but are never the conda load target. The climb /
        # presence / no-vendored checks still apply to them.
        is_musl = any("libc.musl" in n for n in needed)
        want = expected_climb_entry(name)

        # Bare $ORIGIN must ALSO be present: it is how the driver resolves its
        # co-located sibling libodbcinst.so.2. Losing it breaks driver-manager loading
        # even when the $PREFIX/lib climb entry is intact.
        if "$ORIGIN" not in entries:
            errors.append(
                f"{name}: effective RUNPATH {entries or '[none]'} lacks bare '$ORIGIN' "
                f"(co-located sibling resolution for libodbcinst.so.2). NEEDED={needed}"
            )
        # N1: the EXACT climb entry must be present in the EFFECTIVE RUNPATH.
        if want not in entries:
            errors.append(
                f"{name}: effective RUNPATH {entries or '[none]'} does not contain the "
                f"exact climb entry '{want}' to $PREFIX/lib (the loader uses DT_RUNPATH "
                f"when present, else DT_RPATH). NEEDED={needed}"
            )
        # Stay relocatable: reject ANY absolute entry.
        abs_entries = [e for e in entries if e.startswith("/")]
        if abs_entries:
            errors.append(
                f"{name}: RUNPATH has ABSOLUTE entries {abs_entries}; must stay "
                f"relocatable (relative $ORIGIN only)."
            )

        # N2b: the expected DT_NEEDED set must still be present (glibc variants only;
        # musl links these statically / differently, and is not a conda target).
        if is_driver:
            dirs_with_driver.add(member_dir)
            if not is_musl:
                for want_need in _DRIVER_NEEDED:
                    if not any(want_need in n for n in needed):
                        errors.append(
                            f"{name}: driver no longer NEEDs '{want_need}*' (NEEDED={needed}); "
                            f"the declared conda dep would go unused and reachability is unproven."
                        )
        if is_inst:
            dirs_with_inst.add(member_dir)
            if not is_musl:
                for want_need in _ODBCINST_NEEDED:
                    if not any(want_need in n for n in needed):
                        errors.append(
                            f"{name}: libodbcinst.so.2 no longer NEEDs '{want_need}*' "
                            f"(NEEDED={needed})."
                        )
        print(f"  {subdir}/{base}: effective RUNPATH={entries} NEEDED={needed}")

    if vendored:
        errors.append(
            f"{base_name}: vendors libraries that must be DECLARED conda deps, not "
            f"bundled: {sorted(vendored)} (krb5/openssl/libltdl are serviced by conda, "
            f"never shipped inside the payload)."
        )
    # Per-subdir presence: EVERY discovered driver lib dir must ship BOTH a driver and
    # libodbcinst.so.2. A package-global count would let a driver missing from ONE
    # distro subdir (alpine/debian_ubuntu/rhel/suse) slip past.
    if not lib_dirs:
        errors.append(
            f"{base_name}: no mssql_python_odbc/libs/linux/*/*/lib directory found in a "
            f"Linux package."
        )
    for d in sorted(lib_dirs):
        if d not in dirs_with_driver:
            errors.append(f"{base_name}: '{d}' has no libmsodbcsql* driver.")
        if d not in dirs_with_inst:
            errors.append(f"{base_name}: '{d}' has no libodbcinst.so.2.")
    return errors


def main(argv: list | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", help="Directory to scan recursively for *.conda / *.tar.bz2.")
    parser.add_argument("packages", nargs="*", help="Explicit package paths to audit.")
    args = parser.parse_args(argv)

    paths = list(args.packages)
    if args.root:
        paths += glob.glob(os.path.join(args.root, "**", "*.conda"), recursive=True)
        paths += glob.glob(os.path.join(args.root, "**", "*.tar.bz2"), recursive=True)
    paths = sorted(set(paths))

    if not paths:
        print(
            "ERROR: no conda packages to audit (pass --root DIR or package paths).", file=sys.stderr
        )
        return 1

    print(f"Auditing RUNPATH self-containment of {len(paths)} conda package(s):")
    all_errors: list[str] = []
    linux_checked = 0
    for p in paths:
        try:
            if str(read_index(p).get("subdir", "")).startswith("linux"):
                linux_checked += 1
        except Exception:
            # A malformed package is a violation, reported by audit_package below.
            pass
        all_errors.extend(audit_package(p))

    if all_errors:
        print("\nRUNPATH audit FAILED:", file=sys.stderr)
        for e in all_errors:
            print(f"  - {e}", file=sys.stderr)
        return 1

    if linux_checked == 0:
        print("\nOK: no Linux packages present; nothing to audit (win/osx have no ELF payload).")
    else:
        print(
            f"\nOK: all {linux_checked} Linux package(s) carry the EXACT $ORIGIN climb, keep "
            f"their krb5/gssapi/libltdl NEEDEDs, declare krb5/libtool/openssl, and vendor no "
            f"crypto (conda services them)."
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
