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

from _conda_pkg import iter_payload_members as _iter_payload_members, read_index

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

_NATIVE_SUFFIXES = (".pyd", ".dll")


def pe_machine(data: bytes):
    """Return the PE COFF Machine value (int) for a Windows binary, or None.

    DOS header 'MZ' -> e_lfanew at offset 0x3C -> 'PE\\0\\0' signature -> COFF header,
    whose first 2 bytes are the Machine field (little-endian).
    """
    if len(data) < 0x40 or data[:2] != b"MZ":
        return None
    e_lfanew = struct.unpack_from("<I", data, 0x3C)[0]
    if e_lfanew + 6 > len(data) or data[e_lfanew : e_lfanew + 4] != b"PE\x00\x00":
        return None
    return struct.unpack_from("<H", data, e_lfanew + 4)[0]


def read_subdir(path: str) -> str:
    """Return the package's ``info/index.json`` ``subdir`` (RAISES on malformed package)."""
    return str(read_index(path).get("subdir", ""))


def audit_package(path: str) -> list[str]:
    """Return violation strings for one package (empty == clean / skipped non-Windows)."""
    base_name = os.path.basename(path)
    try:
        subdir = read_subdir(path)
    except Exception as exc:  # malformed must FAIL, never silently skip
        return [f"{base_name}: unreadable/malformed package metadata ({exc})."]

    expected = _SUBDIR_MACHINE.get(subdir)
    if expected is None:
        print(f"  SKIP (no Windows PE payload): {base_name} [subdir={subdir or '?'}]")
        return []

    try:
        members = list(_iter_payload_members(path))
    except ValueError as exc:  # malformed payload (e.g. .conda missing pkg-*.tar.zst)
        return [f"{base_name}: unreadable/malformed package payload ({exc})."]

    errors: list[str] = []
    native_seen = 0
    binding_seen = 0
    driver_dll_seen = 0
    for name, data in members:
        if not name.lower().endswith(_NATIVE_SUFFIXES):
            continue
        native_seen += 1
        low = name.replace("\\", "/").lower()
        if "/mssql_python/" in low and "ddbc_bindings" in low and low.endswith(".pyd"):
            binding_seen += 1
        # The presence gate requires the CORE driver (msodbcsql18*.dll) specifically, not
        # just any vendored .dll: a package shipping only support DLLs (e.g. mssql-auth or a
        # VC++ runtime) with the core driver missing would otherwise pass -- and on win-arm64
        # (runtime import skipped) this is the only check standing between it and publish.
        if (
            "/mssql_python_odbc/libs/windows/" in low
            and os.path.basename(low).startswith("msodbcsql18")
            and low.endswith(".dll")
        ):
            driver_dll_seen += 1
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
                f"(mssql_python_odbc/libs/windows/**/msodbcsql18*.dll) found in a "
                f"'{subdir}' package."
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
