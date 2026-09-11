#!/usr/bin/env python3
"""Assert the vendored Windows PE binaries in a built conda package match its arch.

The win-arm64 conda package is CROSS-built on an x64 agent, where the arm64 Python
cannot execute -- so the build-time runtime import is skipped and the package's
architecture would otherwise be trusted purely from the wheel filename. A mislabeled
or mis-built wheel could therefore ship x64 (.pyd/.dll) binaries inside a win-arm64
package and nothing would catch it before publish.

This is the Windows twin of eng/scripts/audit_bundled_binaries.py (which audits the
Linux ELF payload): it reads the PE COFF Machine field straight out of every
.pyd/.dll in the built .conda payload and asserts it matches the package's subdir
(win-arm64 -> ARM64, win-64 -> AMD64). A Windows package missing EITHER the binding
(ddbc_bindings*.pyd) OR the core ODBC driver (msodbcsql18*.dll) FAILS.

Exit 0 = every checked package's PE binaries match; non-zero = a mismatch/violation.
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

# IMAGE_FILE_MACHINE_* (winnt.h): the PE COFF Machine field -> a short name.
_MACHINES = {
    0x8664: "amd64",
    0xAA64: "arm64",
    0x014C: "x86",
    0x01C0: "arm",
    0x01C4: "armnt",
}

# conda subdir -> the ONLY PE machine its vendored .pyd/.dll may carry.
_SUBDIR_MACHINE = {
    "win-64": 0x8664,
    "win-arm64": 0xAA64,
}
_SUBDIR_DRIVER_DIR = {
    "win-64": "x64",
    "win-arm64": "arm64",
}

_NATIVE_SUFFIXES = (".pyd", ".dll")


def pe_machine(data: bytes) -> int | None:
    """Return the PE COFF Machine value (int) for a Windows binary, or None.

    DOS header 'MZ' -> e_lfanew at offset 0x3C -> 'PE\\0\\0' signature -> COFF header,
    whose first 2 bytes are the Machine field (little-endian).
    """
    if len(data) < 0x40 or data[:2] != b"MZ":
        return None
    e_lfanew = struct.unpack_from("<I", data, 0x3C)[0]
    coff_offset = e_lfanew + 4
    if coff_offset + 20 > len(data) or data[e_lfanew:coff_offset] != b"PE\x00\x00":
        return None
    machine, section_count = struct.unpack_from("<HH", data, coff_offset)
    optional_size = struct.unpack_from("<H", data, coff_offset + 16)[0]
    optional_offset = coff_offset + 20
    section_table = optional_offset + optional_size
    if section_count == 0 or optional_size < 2 or section_table + section_count * 40 > len(data):
        return None
    optional_magic = struct.unpack_from("<H", data, optional_offset)[0]
    if optional_magic not in (0x10B, 0x20B):  # PE32 / PE32+
        return None
    for index in range(section_count):
        section_offset = section_table + index * 40
        raw_size, raw_offset = struct.unpack_from("<II", data, section_offset + 16)
        if raw_size and (raw_offset > len(data) or raw_size > len(data) - raw_offset):
            return None
    return machine


def read_subdir(path: str) -> str:
    """Return the package's ``info/index.json`` ``subdir`` (RAISES on malformed package)."""
    return str(read_index(path).get("subdir", ""))


def audit_package(path: str) -> list[str]:
    """Return violation strings for one package (empty == clean / skipped non-Windows)."""
    base_name = os.path.basename(path)
    try:
        index = read_index(path)
        subdir = str(index.get("subdir", ""))
    except Exception as exc:  # malformed must FAIL, never silently skip
        return [f"{base_name}: unreadable/malformed package metadata ({exc})."]

    expected = _SUBDIR_MACHINE.get(subdir)
    if expected is None:
        print(f"  SKIP (no Windows PE payload): {base_name} [subdir={subdir or '?'}]")
        return []
    expected_driver_dir = _SUBDIR_DRIVER_DIR[subdir]

    try:
        members = list(_iter_payload_members(path))
    except ValueError as exc:  # malformed payload (e.g. .conda missing pkg-*.tar.zst)
        return [f"{base_name}: unreadable/malformed package payload ({exc})."]

    errors = validate_native_contract(members, index)
    native_seen = 0
    binding_seen = 0
    driver_dll_seen = 0
    auth_dll_seen = 0
    for name, data in members:
        if not name.lower().endswith(_NATIVE_SUFFIXES):
            continue
        native_seen += 1
        low = name.replace("\\", "/").lower()
        if "/mssql_python/" in low and "ddbc_bindings" in low and low.endswith(".pyd"):
            binding_seen += 1
        # The presence gate requires BOTH the CORE driver (msodbcsql18*.dll) AND its auth
        # companion (mssql-auth*.dll) specifically -- not just any vendored .dll. The loader
        # (ddbc_bindings.cpp) THROWS at connect if mssql-auth.dll is absent, so a package
        # missing it would pass CI (win-arm64 skips the runtime import) yet fail on EVERY
        # connect; a VC++ runtime or other support DLL satisfies neither category.
        if "/mssql_python_odbc/libs/windows/" in low and low.endswith(".dll"):
            base_low = os.path.basename(low)
            runtime_suffix = f"/mssql_python_odbc/libs/windows/{expected_driver_dir}/{base_low}"
            if base_low.startswith("msodbcsql18") and low.endswith(runtime_suffix):
                driver_dll_seen += 1
            elif base_low.startswith("mssql-auth") and low.endswith(runtime_suffix):
                auth_dll_seen += 1
        machine = pe_machine(data)
        if machine is None:
            errors.append(f"{name}: not a valid PE binary (no MZ/PE header).")
            continue
        if machine != expected:
            errors.append(
                f"{name}: PE machine {_MACHINES.get(machine, hex(machine))} "
                f"!= expected {_MACHINES[expected]} for subdir '{subdir}'."
            )
        else:
            print(f"  {subdir}/{os.path.basename(name)}: PE machine={_MACHINES[expected]} OK")

    # Presence: assert BOTH required binary categories independently, not just >=1 native
    # file -- win-arm64 skips the runtime import, so this IS its presence gate. A package
    # with the binding .pyd but missing driver DLLs (or vice versa) must fail here.
    if native_seen == 0:
        errors.append(
            f"{base_name}: no .pyd/.dll found in a '{subdir}' package -- the native binding "
            f"(ddbc_bindings*.pyd) + the vendored ODBC driver DLLs must be present."
        )
    else:
        if binding_seen == 0:
            errors.append(
                f"{base_name}: no native binding (mssql_python/ddbc_bindings*.pyd) found in a "
                f"'{subdir}' package."
            )
        if driver_dll_seen == 0:
            errors.append(
                f"{base_name}: no vendored core ODBC driver DLL "
                f"(mssql_python_odbc/libs/windows/{expected_driver_dir}/msodbcsql18*.dll) "
                f"found in a "
                f"'{subdir}' package."
            )
        if auth_dll_seen == 0:
            errors.append(
                f"{base_name}: no vendored mssql-auth DLL "
                f"(mssql_python_odbc/libs/windows/{expected_driver_dir}/mssql-auth*.dll) found "
                f"in a '{subdir}' package -- the ODBC driver loader THROWS at connect if it "
                f"is absent."
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
        help="Only audit packages of this subdir (e.g. win-arm64). Empty = all win-* packages.",
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
        print("\nPE machine-type assert FAILED:", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        return 1

    print(f"\nOK: all {checked} checked package(s) carry the expected PE machine type.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
