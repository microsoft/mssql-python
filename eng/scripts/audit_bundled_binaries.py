#!/usr/bin/env python3
"""Audit the dynamic-dependency closure of every native binary shipped in an
mssql-python / mssql-python-odbc payload against an explicit allowlist.

This is gate step 3 of the packaging acceptance gate: a link-time audit that a
runtime import alone cannot fully cover, and -- on Intel-only macOS PR
validation -- the arch-slice substitute for the arm64 runtime import that an
Intel agent cannot run.

Policy: every dynamic dependency of every shipped binary must be exactly ONE of
  * BUNDLED  - the depended-on library is shipped in the SAME payload, or
  * BASE     - a core OS / libc / toolchain runtime that is always present and is
               policy-allowed for manylinux / musllinux / macOS / Windows, or
  * DECLARED - a system library we deliberately DO NOT vendor but DECLARE as a
               dependency (openssl, krb5 on Linux; the VC++ runtime on Windows).
Anything else is a VIOLATION and fails the gate (exit 1). The canonical case this
catches is ``libltdl.so.7`` NEEDED by ``libodbcinst.so.2`` but not bundled.

Extra ELF check: any binary that depends on a BUNDLED sibling must carry an
``$ORIGIN`` RUNPATH, otherwise the loader will never find that sibling on a
minimal base (this is precisely the libodbcinst.so.2 fix).

Analyzers are lazy-imported so a leg only needs the analyzer for its own files:
ELF via pyelftools, Mach-O via macholib, PE via pefile.

Usage:
  audit_bundled_binaries.py --payload <dir> [--os auto|linux|macos|windows]
                            [--require-arch x86_64|arm64|aarch64] [--json]

Exit codes: 0 = clean, 1 = one or more violations, 2 = usage / missing analyzer.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass, field

# --------------------------------------------------------------------------- #
# Allowlists (regex, matched case-insensitively against the dependency string).
# BASE = always-present OS/libc/toolchain runtime; DECLARED = deliberately not
# vendored but declared as a runtime dependency.
# --------------------------------------------------------------------------- #
_BASE_LINUX = [
    r"^libc\.so\.\d+$",
    r"^libc\.musl-(x86_64|aarch64)\.so\.\d+$",
    r"^ld-linux-(x86-64|aarch64)\.so\.\d+$",
    r"^ld-musl-(x86_64|aarch64)\.so\.\d+$",
    r"^libm\.so\.\d+$",
    r"^libdl\.so\.\d+$",
    r"^libpthread\.so\.\d+$",
    r"^librt\.so\.\d+$",
    r"^libutil\.so\.\d+$",
    r"^libresolv\.so\.\d+$",
    r"^libgcc_s\.so\.\d+$",
    r"^libstdc\+\+\.so\.\d+$",
]
# OpenSSL is dlopen'd at connect (not a NEEDED), krb5 IS a NEEDED of the driver;
# both are declared (conda run deps + documented system deps), never vendored.
_DECLARED_LINUX = [
    r"^libssl\.so.*$",
    r"^libcrypto\.so.*$",
    r"^libkrb5\.so\.\d+$",
    r"^libgssapi_krb5\.so\.\d+$",
    r"^libk5crypto\.so\.\d+$",
    r"^libkrb5support\.so\.\d+$",
    r"^libcom_err\.so\.\d+$",
    r"^libkeyutils\.so\.\d+$",
]

# macOS: everything under /usr/lib and the system frameworks is a base OS lib.
_BASE_MACOS = [
    r"^/usr/lib/.*$",
    r"^/System/Library/Frameworks/.*$",
    r"^/System/Library/PrivateFrameworks/.*$",
]
_DECLARED_MACOS: list[str] = []  # Kerberos/GSS/SecureTransport are all system frameworks (BASE)

# Windows system DLLs (SChannel/SSPI live here, so TLS + Kerberos need nothing vendored).
_BASE_WINDOWS = [
    r"^kernel32\.dll$",
    r"^kernelbase\.dll$",
    r"^ntdll\.dll$",
    r"^advapi32\.dll$",
    r"^user32\.dll$",
    r"^gdi32\.dll$",
    r"^shell32\.dll$",
    r"^shlwapi\.dll$",
    r"^ole32\.dll$",
    r"^oleaut32\.dll$",
    r"^rpcrt4\.dll$",
    r"^sspicli\.dll$",
    r"^secur32\.dll$",
    r"^crypt32\.dll$",
    r"^bcrypt\.dll$",
    r"^ncrypt\.dll$",
    r"^ws2_32\.dll$",
    r"^mswsock\.dll$",
    r"^winhttp\.dll$",
    r"^wininet\.dll$",
    r"^wldap32\.dll$",
    r"^winmm\.dll$",
    r"^imm32\.dll$",
    r"^uxtheme\.dll$",
    r"^dwmapi\.dll$",
    r"^wtsapi32\.dll$",
    r"^sechost\.dll$",
    r"^cryptbase\.dll$",
    r"^cryptsp\.dll$",
    r"^bcryptprimitives\.dll$",
    r"^profapi\.dll$",
    r"^wintrust\.dll$",
    r"^msasn1\.dll$",
    r"^win32u\.dll$",
    r"^gdi32full\.dll$",
    r"^ucrtbase\.dll$",
    r"^normaliz\.dll$",
    r"^dnsapi\.dll$",
    r"^iphlpapi\.dll$",
    r"^userenv\.dll$",
    r"^netapi32\.dll$",
    r"^version\.dll$",
    r"^msvcrt\.dll$",
    r"^powrprof\.dll$",
    r"^dbghelp\.dll$",
    r"^comctl32\.dll$",
    r"^comdlg32\.dll$",
    r"^setupapi\.dll$",
    r"^cfgmgr32\.dll$",
    r"^api-ms-win-.*\.dll$",
    r"^ext-ms-.*\.dll$",
]
# VC++ runtime is declared via the conda `vc14_runtime` run dep (msvcp140.dll is
# also physically bundled, so it classifies as BUNDLED when present).
_DECLARED_WINDOWS = [
    r"^vcruntime140\.dll$",
    r"^vcruntime140_1\.dll$",
    r"^msvcp140\.dll$",
    r"^concrt140\.dll$",
]

_BINARY_EXT = {".so", ".dylib", ".dll", ".pyd", ".exe", ".bundle"}


def _compile(patterns: list[str]) -> list[re.Pattern]:
    return [re.compile(p, re.IGNORECASE) for p in patterns]


# --------------------------------------------------------------------------- #
# File-type detection by magic.
# --------------------------------------------------------------------------- #
def _magic(path: str) -> str:
    try:
        with open(path, "rb") as fh:
            head = fh.read(4)
    except OSError:
        return "other"
    if head[:4] == b"\x7fELF":
        return "elf"
    if head[:4] in (
        b"\xfe\xed\xfa\xce",
        b"\xfe\xed\xfa\xcf",
        b"\xce\xfa\xed\xfe",
        b"\xcf\xfa\xed\xfe",
        b"\xca\xfe\xba\xbe",
        b"\xbe\xba\xfe\xca",
    ):
        return "macho"
    if head[:2] == b"MZ":
        return "pe"
    return "other"


def _iter_binaries(root: str):
    for dirpath, _dirs, files in os.walk(root):
        for name in files:
            full = os.path.join(dirpath, name)
            ext = os.path.splitext(name)[1].lower()
            kind = _magic(full)
            if kind != "other":
                yield full, kind
            elif ext in _BINARY_EXT:
                # extension says binary but magic did not match -> report as unknown
                yield full, kind


# --------------------------------------------------------------------------- #
# Analyzers (lazy imports).
# --------------------------------------------------------------------------- #
def _elf_info(path: str):
    from elftools.elf.elffile import ELFFile  # lazy

    needed: list[str] = []
    runpath = ""
    machine = ""
    with open(path, "rb") as fh:
        elf = ELFFile(fh)
        machine = elf.get_machine_arch()
        dyn = elf.get_section_by_name(".dynamic")
        if dyn is not None:
            for tag in dyn.iter_tags():
                t = tag.entry.d_tag
                if t == "DT_NEEDED":
                    needed.append(tag.needed)
                elif t == "DT_RUNPATH":
                    runpath = tag.runpath
                elif t == "DT_RPATH" and not runpath:
                    runpath = tag.rpath
    return needed, runpath, machine


def _macho_info(path: str):
    from macholib.MachO import MachO  # lazy
    from macholib.mach_o import (
        LC_LOAD_DYLIB,
        LC_LOAD_WEAK_DYLIB,
        LC_REEXPORT_DYLIB,
        LC_LOAD_UPWARD_DYLIB,
        CPU_TYPE_NAMES,
    )

    dylibs: list[str] = []
    arches: set[str] = set()
    load_types = {LC_LOAD_DYLIB, LC_LOAD_WEAK_DYLIB, LC_REEXPORT_DYLIB, LC_LOAD_UPWARD_DYLIB}
    m = MachO(path)
    for hdr in m.headers:
        arches.add(CPU_TYPE_NAMES.get(hdr.header.cputype, str(hdr.header.cputype)))
        for load_cmd, _cmd, data in hdr.commands:
            if load_cmd.cmd in load_types:
                name = bytes(data).rstrip(b"\x00").decode("utf-8", "replace")
                if name:
                    dylibs.append(name)
    return dylibs, arches


def _pe_info(path: str):
    import pefile  # lazy

    imports: list[str] = []
    machine = ""
    pe = pefile.PE(path, fast_load=True)
    pe.parse_data_directories(directories=[pefile.DIRECTORY_ENTRY["IMAGE_DIRECTORY_ENTRY_IMPORT"]])
    machine = pefile.MACHINE_TYPE.get(pe.FILE_HEADER.Machine, str(pe.FILE_HEADER.Machine))
    for entry in getattr(pe, "DIRECTORY_ENTRY_IMPORT", []) or []:
        dll = entry.dll
        if dll:
            imports.append(dll.decode("utf-8", "replace"))
    pe.close()
    return imports, machine


# --------------------------------------------------------------------------- #
# Classification.
# --------------------------------------------------------------------------- #
@dataclass
class Finding:
    binary: str
    dep: str
    category: str  # BUNDLED | BASE | DECLARED | VIOLATION | RPATH


@dataclass
class Report:
    findings: list[Finding] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def violations(self) -> list[Finding]:
        return [f for f in self.findings if f.category in ("VIOLATION", "RPATH")]


def _basename(dep: str) -> str:
    # macOS install names may be @rpath/... , /usr/lib/... , @loader_path/... ;
    # ELF/PE deps are already basenames.
    return dep.replace("\\", "/").rsplit("/", 1)[-1]


def _classify(
    dep: str, os_name: str, bundled: set[str], base_re: list[re.Pattern], decl_re: list[re.Pattern]
) -> str:
    base = _basename(dep)
    key = base.lower() if os_name == "windows" else base

    # macOS install names that are ABSOLUTE paths resolve by that exact path, NOT by
    # basename. So a non-system absolute path (e.g. a hardcoded /opt/homebrew libltdl)
    # is a VIOLATION even if a same-named file is bundled -- the loader will never use
    # the bundled copy. @rpath/@loader_path/@executable_path resolve relative to the
    # bundle, so those fall through to the basename bundled check below.
    if os_name == "macos" and dep.startswith("/"):
        for rx in base_re:
            if rx.match(dep):
                return "BASE"
        return "VIOLATION"

    if key in bundled:
        return "BUNDLED"
    # macOS: match the FULL install path against base patterns (/usr/lib, frameworks).
    probe_full = dep if os_name == "macos" else base
    for rx in base_re:
        if rx.match(probe_full) or rx.match(base):
            return "BASE"
    for rx in decl_re:
        if rx.match(base):
            return "DECLARED"
    return "VIOLATION"


def audit(payload: str, os_name: str, require_arch: str | None) -> Report:
    rep = Report()
    binaries = list(_iter_binaries(payload))
    if not binaries:
        rep.errors.append(f"no native binaries found under {payload}")
        return rep

    # Bundled set = basenames of every binary present in the payload.
    bundled: set[str] = set()
    for path, _kind in binaries:
        b = os.path.basename(path)
        bundled.add(b.lower() if os_name == "windows" else b)

    base_re = _compile(
        {"linux": _BASE_LINUX, "macos": _BASE_MACOS, "windows": _BASE_WINDOWS}[os_name]
    )
    decl_re = _compile(
        {"linux": _DECLARED_LINUX, "macos": _DECLARED_MACOS, "windows": _DECLARED_WINDOWS}[os_name]
    )

    want_kind = {"linux": "elf", "macos": "macho", "windows": "pe"}[os_name]
    arch_norm = {"aarch64": "arm64"}.get((require_arch or "").lower(), (require_arch or "").lower())

    for path, kind in binaries:
        rel = os.path.relpath(path, payload)
        if kind == "other":
            rep.errors.append(f"{rel}: unrecognized binary format")
            continue
        if kind != want_kind:
            # e.g. a stray PE inside a linux payload -- flag it.
            rep.errors.append(f"{rel}: {kind.upper()} file in a {os_name} payload")
            continue
        try:
            if kind == "elf":
                needed, runpath, _machine = _elf_info(path)
                deps = needed
            elif kind == "macho":
                deps, arches = _macho_info(path)
                runpath = None
                if arch_norm and arch_norm not in {a.lower() for a in arches}:
                    rep.findings.append(Finding(rel, f"<arch slice {arch_norm}>", "VIOLATION"))
            else:  # pe
                deps, _machine = _pe_info(path)
                runpath = None
        except ImportError as exc:
            rep.errors.append(f"missing analyzer for {want_kind}: {exc}")
            return rep
        except Exception as exc:  # noqa: BLE001 - report and continue auditing
            rep.errors.append(f"{rel}: failed to analyze ({exc})")
            continue

        depends_on_bundled = False
        for dep in deps:
            cat = _classify(dep, os_name, bundled, base_re, decl_re)
            if cat == "BUNDLED":
                depends_on_bundled = True
            rep.findings.append(Finding(rel, dep, cat))

        # ELF: a binary that needs a bundled sibling must carry $ORIGIN in RUNPATH,
        # else the loader cannot find it on a minimal base (the libodbcinst fix).
        if kind == "elf" and depends_on_bundled:
            rp = runpath or ""
            if "$ORIGIN" not in rp and "${ORIGIN}" not in rp:
                rep.findings.append(Finding(rel, f"RUNPATH='{rp}' (needs $ORIGIN)", "RPATH"))
    return rep


# --------------------------------------------------------------------------- #
# CLI / reporting.
# --------------------------------------------------------------------------- #
def _detect_os() -> str:
    if sys.platform.startswith("linux"):
        return "linux"
    if sys.platform == "darwin":
        return "macos"
    if os.name == "nt" or sys.platform.startswith("win"):
        return "windows"
    return "linux"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Audit bundled native binary dependencies.")
    ap.add_argument("--payload", required=True, help="root dir of the extracted payload to audit")
    ap.add_argument("--os", choices=["auto", "linux", "macos", "windows"], default="auto")
    ap.add_argument(
        "--require-arch", default=None, help="assert every Mach-O contains this arch slice (macOS)"
    )
    ap.add_argument("--json", action="store_true", help="emit a JSON report")
    args = ap.parse_args(argv)

    os_name = _detect_os() if args.os == "auto" else args.os
    if not os.path.isdir(args.payload):
        print(f"ERROR: not a directory: {args.payload}", file=sys.stderr)
        return 2

    rep = audit(args.payload, os_name, args.require_arch)

    if args.json:
        print(
            json.dumps(
                {
                    "os": os_name,
                    "payload": args.payload,
                    "findings": [f.__dict__ for f in rep.findings],
                    "errors": rep.errors,
                    "violations": len(rep.violations),
                },
                indent=2,
            )
        )
    else:
        width = max([len(f.binary) for f in rep.findings] + [len("BINARY")]) if rep.findings else 6
        print(f"== binary dependency audit ({os_name}, payload={args.payload}) ==")
        print(f"{'BINARY'.ljust(width)}  {'CATEGORY'.ljust(9)}  DEPENDENCY")
        for f in rep.findings:
            print(f"{f.binary.ljust(width)}  {f.category.ljust(9)}  {f.dep}")
        for e in rep.errors:
            print(f"ERROR: {e}", file=sys.stderr)

    if rep.errors:
        # unresolved analysis errors are themselves gate failures
        print(f"AUDIT FAILED: {len(rep.errors)} error(s)", file=sys.stderr)
        return 2 if any("analyzer" in e for e in rep.errors) else 1
    if rep.violations:
        print(
            f"AUDIT FAILED: {len(rep.violations)} violation(s) -- "
            f"undeclared/unbundled dependency or missing $ORIGIN RUNPATH",
            file=sys.stderr,
        )
        return 1
    print("AUDIT PASSED: every dependency is bundled, base, or declared.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
