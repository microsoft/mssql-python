#!/usr/bin/env python3
"""Assert the vendored macOS Mach-O binaries in a built conda package carry the package's arch.

The macOS conda packages are repackaged from a UNIVERSAL2 wheel, so a package's architecture
is otherwise trusted purely from the wheel filename -- and osx-arm64 is CROSS-built on an
Intel agent, where the arm64 slice cannot execute, so the build-time runtime import is skipped.
A mislabeled or thin (single-arch) wheel could therefore ship an x86_64-only binary inside an
osx-arm64 package and nothing would catch it before publish.

This is the macOS twin of eng/scripts/assert_pe_machine.py (Windows PE COFF machine) and
eng/scripts/audit_bundled_binaries.py (Linux ELF RUNPATH): it reads the Mach-O cputype(s)
straight out of every .dylib/.so in the built .conda payload -- enumerating every slice of a
FAT/universal binary, like ``lipo -archs`` -- and asserts the package's arch slice is PRESENT
(osx-arm64 -> arm64, osx-64 -> x86_64). A package missing EITHER the binding (ddbc_bindings*.so)
OR the vendored ODBC driver (libmsodbcsql*.dylib) FAILS.

Exit 0 = every checked package's Mach-O binaries carry the expected arch slice; non-zero = a
mismatch/violation.
"""

from __future__ import annotations

import argparse
import glob
import os
import struct
import sys

from _conda_pkg import iter_payload_members as _iter_payload_members, read_index

# Mach-O cputype (mach/machine.h): the base type OR'd with the 64-bit ABI flag -> lipo name.
_CPU_ARCH_ABI64 = 0x01000000
_CPU_TYPE_X86 = 0x00000007
_CPU_TYPE_ARM = 0x0000000C
_CPU_ARCHES = {
    _CPU_TYPE_X86 | _CPU_ARCH_ABI64: "x86_64",  # 0x01000007
    _CPU_TYPE_ARM | _CPU_ARCH_ABI64: "arm64",  # 0x0100000C
    _CPU_TYPE_X86: "i386",
    _CPU_TYPE_ARM: "arm",
}

# conda subdir -> the arch slice its vendored Mach-O binaries MUST contain.
_SUBDIR_ARCH = {
    "osx-64": "x86_64",
    "osx-arm64": "arm64",
}

_NATIVE_SUFFIXES = (".dylib", ".so")

# Mach-O / fat magics (mach-o/loader.h, mach-o/fat.h). The fat header is ALWAYS big-endian on
# disk; a thin header's cputype word follows the header's own endianness.
_MH_MAGIC = 0xFEEDFACE  # 32-bit thin
_MH_MAGIC_64 = 0xFEEDFACF  # 64-bit thin (x86_64 / arm64)
_FAT_MAGIC = 0xCAFEBABE  # universal (fat_arch entries, 20 bytes each)
_FAT_MAGIC_64 = 0xCAFEBABF  # universal64 (fat_arch_64 entries, 32 bytes each)


def macho_arches(data: bytes):
    """Return the SET of lipo-style arch names in a Mach-O binary (thin OR fat/universal), or
    None if the bytes are not Mach-O. Reads only headers -- no dependency on macOS tooling."""
    if len(data) < 8:
        return None
    be = struct.unpack_from(">I", data, 0)[0]  # fat magic is big-endian on disk
    le = struct.unpack_from("<I", data, 0)[0]
    if be in (_FAT_MAGIC, _FAT_MAGIC_64):
        nfat = struct.unpack_from(">I", data, 4)[0]
        entry = 20 if be == _FAT_MAGIC else 32  # fat_arch vs fat_arch_64
        arches = set()
        off = 8
        for _ in range(nfat):
            if off + 4 > len(data):
                break
            cputype = struct.unpack_from(">I", data, off)[0]  # cputype is fat_arch's first word
            arches.add(_CPU_ARCHES.get(cputype, hex(cputype)))
            off += entry
        return arches or None
    if le in (_MH_MAGIC, _MH_MAGIC_64):  # little-endian thin (Intel / Apple Silicon)
        cputype = struct.unpack_from("<I", data, 4)[0]
    elif be in (_MH_MAGIC, _MH_MAGIC_64):  # big-endian thin (legacy)
        cputype = struct.unpack_from(">I", data, 4)[0]
    else:
        return None
    return {_CPU_ARCHES.get(cputype, hex(cputype))}


def read_subdir(path: str) -> str:
    """Return the package's ``info/index.json`` ``subdir`` (RAISES on malformed package)."""
    return str(read_index(path).get("subdir", ""))


def audit_package(path: str) -> list[str]:
    """Return violation strings for one package (empty == clean / skipped non-macOS)."""
    base_name = os.path.basename(path)
    try:
        subdir = read_subdir(path)
    except Exception as exc:  # malformed must FAIL, never silently skip
        return [f"{base_name}: unreadable/malformed package metadata ({exc})."]

    expected = _SUBDIR_ARCH.get(subdir)
    if expected is None:
        print(f"  SKIP (no macOS Mach-O payload): {base_name} [subdir={subdir or '?'}]")
        return []

    try:
        members = list(_iter_payload_members(path))
    except ValueError as exc:  # malformed payload (e.g. .conda missing pkg-*.tar.zst)
        return [f"{base_name}: unreadable/malformed package payload ({exc})."]

    errors: list[str] = []
    native_seen = 0
    binding_seen = 0
    driver_seen = 0
    for name, data in members:
        low = name.replace("\\", "/").lower()
        if not low.endswith(_NATIVE_SUFFIXES):
            continue
        native_seen += 1
        base_low = os.path.basename(low)
        if "/mssql_python/" in low and "ddbc_bindings" in base_low and low.endswith(".so"):
            binding_seen += 1
        is_driver = base_low.startswith("libmsodbcsql") and low.endswith(".dylib")
        if "/mssql_python_odbc/libs/" in low and is_driver:
            driver_seen += 1
        arches = macho_arches(data)
        if arches is None:
            errors.append(f"{name}: not a valid Mach-O binary (no MH/FAT magic).")
            continue
        if expected not in arches:
            errors.append(
                f"{name}: Mach-O arches {sorted(arches)} do NOT include the required "
                f"'{expected}' slice for subdir '{subdir}'."
            )
        else:
            print(f"  {subdir}/{base_low}: arches={sorted(arches)} (has {expected}) OK")

    # Presence gate (mirror the PE assert): osx-arm64 skips the runtime import, so this static
    # pass IS its arch+presence check. A package with the binding but no driver (or vice versa)
    # must fail here.
    if native_seen == 0:
        errors.append(
            f"{base_name}: no .dylib/.so found in a '{subdir}' package -- the native binding "
            f"(ddbc_bindings*.so) + the vendored ODBC driver (libmsodbcsql*.dylib) must be present."
        )
    else:
        if binding_seen == 0:
            errors.append(
                f"{base_name}: no native binding (mssql_python/ddbc_bindings*.so) found in a "
                f"'{subdir}' package."
            )
        if driver_seen == 0:
            errors.append(
                f"{base_name}: no vendored ODBC driver "
                f"(mssql_python_odbc/libs/**/libmsodbcsql*.dylib) found in a '{subdir}' package."
            )
    return errors


def collect(root: str) -> list[str]:
    return sorted(
        glob.glob(os.path.join(root, "**", "*.conda"), recursive=True)
        + glob.glob(os.path.join(root, "**", "*.tar.bz2"), recursive=True)
    )


def main(argv: list | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, help="Directory to scan recursively.")
    parser.add_argument(
        "--subdir",
        default="",
        help="Only audit packages of this subdir (e.g. osx-arm64). Empty = all osx-* packages.",
    )
    args = parser.parse_args(argv)

    paths = collect(args.root)
    if not paths:
        print(f"ERROR: no conda packages found under {args.root}.", file=sys.stderr)
        return 1

    errors: list[str] = []
    checked = 0
    for path in paths:
        # With --subdir, audit only that subdir's packages (read the authoritative
        # info/index.json, never the filename).
        if args.subdir:
            try:
                if read_subdir(path) != args.subdir:
                    continue
            except Exception as exc:
                errors.append(f"{os.path.basename(path)}: unreadable metadata ({exc}).")
                continue
        checked += 1
        errors.extend(audit_package(path))

    if args.subdir and checked == 0:
        print(f"ERROR: no '{args.subdir}' packages found under {args.root}.", file=sys.stderr)
        return 1

    if errors:
        print("\nMach-O arch-slice assert FAILED:", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        return 1

    print(f"\nOK: all {checked} checked package(s) carry the expected Mach-O arch slice.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
