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
(win-arm64 -> ARM64, win-64 -> AMD64). A Windows package with NO native binary FAILS
(the binding's ddbc_bindings*.pyd + the vendored ODBC driver DLLs must be present).

Exit 0 = every checked package's PE binaries match; non-zero = a mismatch/violation.
"""

from __future__ import annotations

import argparse
import glob
import io
import json
import os
import struct
import sys
import tarfile
import zipfile

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


def _zstd_decompress(raw: bytes) -> bytes:
    """Decompress a zstandard blob, preferring the 3.14+ stdlib backend."""
    try:  # Python 3.14+
        from compression import zstd  # type: ignore

        return zstd.decompress(raw)
    except Exception:
        pass
    import zstandard  # third-party fallback

    return zstandard.ZstdDecompressor().decompress(raw)


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


def _iter_payload_members(path: str):
    """Yield ``(member_name, data_bytes)`` for files in a .conda / .tar.bz2 payload."""
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


def read_subdir(path: str) -> str:
    """Return the package's ``info/index.json`` ``subdir`` (RAISES on malformed package)."""
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
            return str(json.load(member).get("subdir", ""))
    if path.endswith(".tar.bz2"):
        with tarfile.open(path, "r:bz2") as tf:
            member = tf.extractfile("info/index.json")
            if member is None:
                raise ValueError("info/index.json missing")
            return str(json.load(member).get("subdir", ""))
    raise ValueError("unrecognized conda package extension")


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

    errors: list[str] = []
    native_seen = 0
    for name, data in _iter_payload_members(path):
        if not name.lower().endswith(_NATIVE_SUFFIXES):
            continue
        native_seen += 1
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

    if native_seen == 0:
        errors.append(
            f"{base_name}: no .pyd/.dll found in a '{subdir}' package -- the native binding "
            f"(ddbc_bindings*.pyd) + the vendored ODBC driver DLLs must be present."
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
