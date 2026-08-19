#!/usr/bin/env python3
"""Masking-immune audit of the vendored Linux ODBC binaries in built conda packages.

The #563 reachability fix lives in the ELF RUNPATH of the vendored driver, not in
the recipe's dependency list. A runtime ``ldd``/import check can PASS on any host
that happens to carry a system ``krb5``/``libltdl`` -- the driver silently binds
the system copy and the missing conda climb goes unnoticed (exactly what the full
CI agents hide). This audit is immune to that masking because it reads the RUNPATH
BYTES straight out of each built ``.conda`` payload and asserts, statically:

  * ``libmsodbcsql*`` and ``libodbcinst.so.2`` carry a PURELY RELATIVE ``$ORIGIN``
    climb (``$ORIGIN`` + at least one ``..`` entry that reaches ``$PREFIX/lib``),
    with NO absolute RPATH entry -- so the declared conda ``krb5``/``openssl``/
    ``libltdl`` in ``$PREFIX/lib`` are actually reachable, location-independently;
  * the driver's ``krb5``/``gssapi`` and ``libodbcinst``'s ``libltdl`` are
    satisfied by DECLARED conda deps -- i.e. those libraries are NOT vendored
    inside the payload (bundling crypto/krb5 is exactly what the recipe must not
    do; it is serviced by conda instead).

Non-Linux packages (``win-*`` / ``osx-*``) have no such ELF payload and are
skipped, so running this on a Windows or macOS leg is a clean no-op.

Exit code 0 = every Linux package is self-contained + relocatable; non-zero = a
violation was found (blocks the build/release).
"""

from __future__ import annotations

import argparse
import glob
import io
import os
import struct
import sys
import tarfile
import zipfile

# --- ELF constants ---------------------------------------------------------
_DT_NEEDED = 1
_DT_RPATH = 15
_DT_RUNPATH = 29
_SHT_DYNAMIC = 6

# Driver binaries whose RUNPATH must carry the $ORIGIN climb.
_DRIVER_PREFIXES = ("libmsodbcsql-", "libmsodbcsql.")
_DRIVER_EXACT = ("libodbcinst.so.2",)

# Libraries that must be serviced by DECLARED conda deps, never vendored into the
# Linux payload (bundling these is the anti-pattern the recipe avoids).
_MUST_NOT_VENDOR = ("libkrb5", "libgssapi", "libssl", "libcrypto", "libltdl")


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
    return len(data) >= 20 and data[:4] == b"\x7fELF"


def elf_dynamic(data: bytes, tags: tuple[int, ...]) -> dict[int, list[str]]:
    """Return ``{tag: [strings]}`` for the requested dynamic tags.

    Parses the ELF via SECTION headers (file offsets -- no vaddr-to-offset mapping
    needed) so it works on a raw in-memory blob extracted from the package. Handles
    ELF32/ELF64 and both endiannesses; the shipped drivers are ELF64-LE.
    """
    result: dict[int, list[str]] = {t: [] for t in tags}
    if not _is_elf(data):
        return result

    is64 = data[4] == 2
    endian = "<" if data[5] == 1 else ">"

    if is64:
        e_shoff = struct.unpack_from(endian + "Q", data, 0x28)[0]
        e_shentsize = struct.unpack_from(endian + "H", data, 0x3A)[0]
        e_shnum = struct.unpack_from(endian + "H", data, 0x3C)[0]
    else:
        e_shoff = struct.unpack_from(endian + "I", data, 0x20)[0]
        e_shentsize = struct.unpack_from(endian + "H", data, 0x2E)[0]
        e_shnum = struct.unpack_from(endian + "H", data, 0x30)[0]
    if not e_shoff or not e_shnum:
        return result

    sections = []  # (sh_type, sh_offset, sh_size, sh_link, sh_entsize)
    for i in range(e_shnum):
        off = e_shoff + i * e_shentsize
        if off + e_shentsize > len(data):
            return result
        sh_type = struct.unpack_from(endian + "I", data, off + 4)[0]
        if is64:
            sh_offset = struct.unpack_from(endian + "Q", data, off + 0x18)[0]
            sh_size = struct.unpack_from(endian + "Q", data, off + 0x20)[0]
            sh_link = struct.unpack_from(endian + "I", data, off + 0x28)[0]
            sh_entsize = struct.unpack_from(endian + "Q", data, off + 0x38)[0]
        else:
            sh_offset = struct.unpack_from(endian + "I", data, off + 0x10)[0]
            sh_size = struct.unpack_from(endian + "I", data, off + 0x14)[0]
            sh_link = struct.unpack_from(endian + "I", data, off + 0x18)[0]
            sh_entsize = struct.unpack_from(endian + "I", data, off + 0x24)[0]
        sections.append((sh_type, sh_offset, sh_size, sh_link, sh_entsize))

    dyn = next((s for s in sections if s[0] == _SHT_DYNAMIC), None)
    if dyn is None:
        return result
    _, dyn_off, dyn_size, dyn_link, dyn_entsize = dyn
    if dyn_link >= len(sections):
        return result
    strtab_off = sections[dyn_link][1]
    strtab_size = sections[dyn_link][2]
    strtab = data[strtab_off : strtab_off + strtab_size]

    def _read_str(pos: int) -> str:
        end = strtab.find(b"\x00", pos)
        return strtab[pos : (end if end >= 0 else len(strtab))].decode("utf-8", "replace")

    entsize = dyn_entsize or (16 if is64 else 8)
    for off in range(dyn_off, dyn_off + dyn_size, entsize):
        if off + entsize > len(data):
            break
        if is64:
            d_tag = struct.unpack_from(endian + "q", data, off)[0]
            d_val = struct.unpack_from(endian + "Q", data, off + 8)[0]
        else:
            d_tag = struct.unpack_from(endian + "i", data, off)[0]
            d_val = struct.unpack_from(endian + "I", data, off + 4)[0]
        if d_tag == 0:  # DT_NULL terminates the array
            break
        if d_tag in result:
            result[d_tag].append(_read_str(d_val))
    return result


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


def _read_subdir(path: str) -> str:
    """Return the package's real ``info/index.json`` subdir (or '')."""
    try:
        if path.endswith(".conda"):
            with zipfile.ZipFile(path) as zf:
                info_name = next(
                    n for n in zf.namelist() if n.startswith("info-") and n.endswith(".tar.zst")
                )
                blob = _zstd_decompress(zf.read(info_name))
            with tarfile.open(fileobj=io.BytesIO(blob)) as tf:
                member = tf.extractfile("info/index.json")
                if member is not None:
                    import json

                    return str(json.load(member).get("subdir", ""))
        elif path.endswith(".tar.bz2"):
            with tarfile.open(path, "r:bz2") as tf:
                member = tf.extractfile("info/index.json")
                if member is not None:
                    import json

                    return str(json.load(member).get("subdir", ""))
    except Exception:
        pass
    return ""


def _is_driver(basename: str) -> bool:
    return basename in _DRIVER_EXACT or any(basename.startswith(p) for p in _DRIVER_PREFIXES)


def _has_relative_climb(runpaths: list[str]) -> bool:
    """True iff the RUNPATH carries ``$ORIGIN`` AND a relative ``..`` climb entry."""
    entries: list[str] = []
    for rp in runpaths:
        entries.extend(e for e in rp.split(":") if e)
    if not any(e == "$ORIGIN" or e.startswith("$ORIGIN") for e in entries):
        return False
    return any("$ORIGIN" in e and ".." in e for e in entries)


def _absolute_entries(runpaths: list[str]) -> list[str]:
    bad: list[str] = []
    for rp in runpaths:
        for e in (x for x in rp.split(":") if x):
            if e.startswith("/"):
                bad.append(e)
    return bad


def audit_package(path: str) -> list[str]:
    """Return a list of violation strings for one package (empty == clean)."""
    subdir = _read_subdir(path)
    # Only Linux packages carry the ELF ODBC payload this audit governs.
    if not subdir.startswith("linux"):
        print(f"  SKIP (no Linux ELF payload): {os.path.basename(path)} [subdir={subdir or '?'}]")
        return []

    errors: list[str] = []
    drivers_seen = 0
    odbcinst_seen = 0
    vendored: list[str] = []

    for name, data in _iter_payload_members(path):
        base = os.path.basename(name)
        # Flag any crypto/krb5/ltdl library vendored into the Linux payload.
        if "/libs/linux/" in ("/" + name) and any(
            base.startswith(p) and (".so" in base) for p in _MUST_NOT_VENDOR
        ):
            vendored.append(name)
            continue
        if not _is_driver(base):
            continue
        if not _is_elf(data):
            continue

        dyn = elf_dynamic(data, (_DT_RUNPATH, _DT_RPATH, _DT_NEEDED))
        runpaths = dyn[_DT_RUNPATH] + dyn[_DT_RPATH]
        needed = dyn[_DT_NEEDED]

        if base.startswith("libmsodbcsql"):
            drivers_seen += 1
        elif base == "libodbcinst.so.2":
            odbcinst_seen += 1

        if not _has_relative_climb(runpaths):
            errors.append(
                f"{name}: RUNPATH {runpaths or '[none]'} lacks the relative "
                f"'$ORIGIN/..' climb to $PREFIX/lib (the #563 reachability fix). "
                f"NEEDED={needed}"
            )
        abs_entries = _absolute_entries(runpaths)
        if abs_entries:
            errors.append(
                f"{name}: RUNPATH has ABSOLUTE entries {abs_entries}; must stay "
                f"relocatable (relative $ORIGIN only)."
            )
        print(f"  {subdir}/{base}: RUNPATH={runpaths} NEEDED={[n for n in needed]}")

    if vendored:
        errors.append(
            f"{os.path.basename(path)}: vendors libraries that must be DECLARED conda "
            f"deps, not bundled: {sorted(vendored)} (krb5/openssl/libltdl are serviced "
            f"by conda, never shipped inside the payload)."
        )
    if drivers_seen == 0:
        errors.append(
            f"{os.path.basename(path)}: no libmsodbcsql* driver found in a Linux package."
        )
    if odbcinst_seen == 0:
        errors.append(f"{os.path.basename(path)}: no libodbcinst.so.2 found in a Linux package.")
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
        subdir = _read_subdir(p)
        if subdir.startswith("linux"):
            linux_checked += 1
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
            f"\nOK: all {linux_checked} Linux package(s) carry the relative $ORIGIN climb "
            f"and vendor no krb5/openssl/libltdl (declared conda deps service them)."
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
