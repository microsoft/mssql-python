#!/usr/bin/env python3
"""Assert macOS Mach-O binaries in a built conda package match their intended architectures.

The macOS conda packages are repackaged from a UNIVERSAL2 wheel, so a package's architecture
is otherwise trusted purely from the wheel filename -- and osx-arm64 is CROSS-built on an
Intel agent, where the arm64 slice cannot execute, so the build-time runtime import is skipped.
A mislabeled or thin (single-arch) wheel could therefore ship an x86_64-only binary inside an
osx-arm64 package and nothing would catch it before publish.

This is the macOS twin of eng/scripts/assert_pe_machine.py (Windows PE COFF machine) and
eng/scripts/audit_bundled_binaries.py (Linux ELF RUNPATH): it reads the Mach-O cputype(s)
straight out of the binding and vendored driver files in the built .conda payload. The binding
must contain the package's arch slice (osx-arm64 -> arm64, osx-64 -> x86_64). The ODBC wheel
deliberately bundles separate macos/arm64 and macos/x86_64 driver trees, so each tree is checked
against its directory arch and the package target's runtime tree must contain all four required
dylibs. FAT/universal binaries are validated like ``lipo -archs``, including complete tables and
valid slice ranges.

Exit 0 = every checked package satisfies the binding/driver architecture contract; non-zero =
a mismatch/violation.
"""

from __future__ import annotations

import argparse
import glob
import os
import struct
import sys

from _conda_pkg import (
    iter_payload_members as _iter_payload_members,
    read_index,
    validate_native_contract,
)

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

_DRIVER_DIR_ARCH = {
    "arm64": "arm64",
    "x86_64": "x86_64",
}

_NATIVE_SUFFIXES = (".dylib", ".so")
_REQUIRED_DRIVER_LIBRARIES = frozenset(
    {
        "libltdl.7.dylib",
        "libmsodbcsql.18.dylib",
        "libodbc.2.dylib",
        "libodbcinst.2.dylib",
    }
)

# Mach-O / fat magics (mach-o/loader.h, mach-o/fat.h). The fat header is ALWAYS big-endian on
# disk; a thin header's cputype word follows the header's own endianness.
_MH_MAGIC = 0xFEEDFACE  # 32-bit thin
_MH_MAGIC_64 = 0xFEEDFACF  # 64-bit thin (x86_64 / arm64)
_FAT_MAGIC = 0xCAFEBABE  # universal (fat_arch entries, 20 bytes each)
_FAT_MAGIC_64 = 0xCAFEBABF  # universal64 (fat_arch_64 entries, 32 bytes each)


def _thin_arch(data: bytes) -> str | None:
    if len(data) < 8:
        return None
    be = struct.unpack_from(">I", data, 0)[0]
    le = struct.unpack_from("<I", data, 0)[0]
    if le in (_MH_MAGIC, _MH_MAGIC_64):
        endian = "<"
        header_size = 32 if le == _MH_MAGIC_64 else 28
    elif be in (_MH_MAGIC, _MH_MAGIC_64):
        endian = ">"
        header_size = 32 if be == _MH_MAGIC_64 else 28
    else:
        return None
    if len(data) < header_size:
        return None
    ncmds, sizeofcmds = struct.unpack_from(f"{endian}II", data, 16)
    commands_end = header_size + sizeofcmds
    if ncmds == 0 or sizeofcmds < ncmds * 8 or commands_end > len(data):
        return None
    command_offset = header_size
    for _ in range(ncmds):
        if command_offset + 8 > commands_end:
            return None
        command_size = struct.unpack_from(f"{endian}I", data, command_offset + 4)[0]
        if command_size < 8 or command_offset + command_size > commands_end:
            return None
        command_offset += command_size
    if command_offset != commands_end:
        return None
    cputype = struct.unpack_from(f"{endian}I", data, 4)[0]
    return _CPU_ARCHES.get(cputype, hex(cputype))


def macho_arches(data: bytes) -> set[str] | None:
    """Return the SET of lipo-style arch names in a Mach-O binary (thin OR fat/universal), or
    None if the bytes are not Mach-O. Reads only headers -- no dependency on macOS tooling."""
    if len(data) < 8:
        return None
    be = struct.unpack_from(">I", data, 0)[0]  # fat magic is big-endian on disk
    if be in (_FAT_MAGIC, _FAT_MAGIC_64):
        nfat = struct.unpack_from(">I", data, 4)[0]
        entry = 20 if be == _FAT_MAGIC else 32  # fat_arch vs fat_arch_64
        table_end = 8 + nfat * entry
        if nfat == 0 or table_end > len(data):
            return None
        arches = set()
        for index in range(nfat):
            entry_offset = 8 + index * entry
            if be == _FAT_MAGIC:
                cputype, _, slice_offset, slice_size, _ = struct.unpack_from(
                    ">IIIII", data, entry_offset
                )
            else:
                cputype, _, slice_offset, slice_size, _, _ = struct.unpack_from(
                    ">IIQQII", data, entry_offset
                )
            if (
                slice_size == 0
                or slice_offset < table_end
                or slice_offset > len(data)
                or slice_size > len(data) - slice_offset
            ):
                return None
            declared_arch = _CPU_ARCHES.get(cputype, hex(cputype))
            embedded_arch = _thin_arch(data[slice_offset : slice_offset + slice_size])
            if embedded_arch != declared_arch:
                return None
            arches.add(declared_arch)
        return arches
    thin_arch = _thin_arch(data)
    return {thin_arch} if thin_arch is not None else None


def read_subdir(path: str) -> str:
    """Return the package's ``info/index.json`` ``subdir`` (RAISES on malformed package)."""
    return str(read_index(path).get("subdir", ""))


def audit_package(path: str) -> list[str]:
    """Return violation strings for one package (empty == clean / skipped non-macOS)."""
    base_name = os.path.basename(path)
    try:
        index = read_index(path)
        subdir = str(index.get("subdir", ""))
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

    errors = validate_native_contract(members, index)
    binding_seen = 0
    target_driver_libraries: set[str] = set()
    for name, data in members:
        low = name.replace("\\", "/").lower()
        if not low.endswith(_NATIVE_SUFFIXES):
            continue
        base_low = os.path.basename(low)
        required_arch = None
        if "/mssql_python/" in low and base_low.startswith("ddbc_bindings") and low.endswith(".so"):
            binding_seen += 1
            required_arch = expected
        elif "/mssql_py_core/" in low and low.endswith(".so"):
            required_arch = expected
        elif "/mssql_python_odbc/libs/macos/" in low and low.endswith(".dylib"):
            relative = low.split("/mssql_python_odbc/libs/macos/", 1)[1]
            parts = relative.split("/")
            driver_dir = parts[0]
            required_arch = _DRIVER_DIR_ARCH.get(driver_dir)
            if required_arch is None:
                errors.append(f"{name}: unrecognized macOS driver architecture directory.")
                continue
            is_runtime_location = len(parts) == 3 and parts[1] == "lib"
            if required_arch == expected and is_runtime_location:
                target_driver_libraries.add(base_low)
        else:
            continue
        arches = macho_arches(data)
        if arches is None:
            errors.append(f"{name}: not a valid, complete Mach-O binary.")
            continue
        if required_arch not in arches:
            errors.append(
                f"{name}: Mach-O arches {sorted(arches)} do NOT include the required "
                f"'{required_arch}' slice."
            )
        else:
            print(f"  {subdir}/{base_low}: arches={sorted(arches)} (has {required_arch}) OK")

    # Presence gate (mirror the PE assert): osx-arm64 skips the runtime import, so this static
    # pass IS its arch+presence check. A package with the binding but no driver (or vice versa)
    # must fail here.
    if binding_seen == 0:
        errors.append(
            f"{base_name}: no native binding (mssql_python/ddbc_bindings*.so) found in a "
            f"'{subdir}' package."
        )
    missing_driver_libraries = sorted(_REQUIRED_DRIVER_LIBRARIES - target_driver_libraries)
    if missing_driver_libraries:
        errors.append(
            f"{base_name}: no vendored ODBC driver for '{expected}': incomplete runtime in "
            f"mssql_python_odbc/libs/macos/{expected}/lib; missing: "
            f"{', '.join(missing_driver_libraries)}."
        )
    return errors


def collect(root: str) -> list[str]:
    return sorted(
        glob.glob(os.path.join(root, "**", "*.conda"), recursive=True)
        + glob.glob(os.path.join(root, "**", "*.tar.bz2"), recursive=True)
    )


def main(argv: list[str] | None = None) -> int:
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

    print(f"\nOK: all {checked} checked package(s) satisfy the Mach-O architecture contract.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
