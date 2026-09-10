#!/usr/bin/env python3
"""Scan the native payload of built wheels, with per-file SARIF coverage checks."""

import argparse
import json
import re
import shutil
import struct
import subprocess
import sys
import zipfile
from pathlib import Path, PurePosixPath
from urllib.parse import unquote, urlsplit

from assert_macho_arch import macho_arches

NATIVE_NAME = re.compile(r"\.(pyd|dll|exe|rll|dylib|so(?:\..+)?)$", re.IGNORECASE)
MACHO_MAGICS = {
    b"\xce\xfa\xed\xfe",
    b"\xcf\xfa\xed\xfe",
    b"\xfe\xed\xfa\xce",
    b"\xfe\xed\xfa\xcf",
    b"\xca\xfe\xba\xbe",
    b"\xca\xfe\xba\xbf",
}


def binary_format(header):
    if header.startswith(b"\x7fELF"):
        return "ELF"
    if header.startswith(b"MZ"):
        return "PE"
    if header[:4] in MACHO_MAGICS:
        return "Mach-O"
    return None


def extract_binaries(wheel, destination):
    """Keep archive paths for adjacent PDB lookup; never extract outside the root."""
    binaries = {}
    with zipfile.ZipFile(wheel) as archive:
        for entry in archive.infolist():
            if entry.is_dir():
                continue
            name = PurePosixPath(entry.filename)
            if (
                name.is_absolute()
                or ".." in name.parts
                or "\\" in entry.filename
                or ":" in entry.filename
            ):
                raise ValueError(f"Invalid wheel member: {entry.filename}")
            with archive.open(entry) as source:
                header = source.read(4)
            kind = binary_format(header)
            if not kind and not NATIVE_NAME.search(name.name) and name.suffix.lower() != ".pdb":
                continue
            if NATIVE_NAME.search(name.name) and not kind:
                raise ValueError(f"Unrecognized native binary: {entry.filename}")
            target = destination.joinpath(*name.parts)
            target.parent.mkdir(parents=True, exist_ok=True)
            with archive.open(entry) as source, target.open("xb") as output:
                shutil.copyfileobj(source, output)
            if kind:
                binaries[target.resolve().as_uri()] = kind
    if not binaries:
        raise ValueError(f"No native binaries in {wheel.name}")
    return binaries


def check_macho_stack(path):
    """Cover every slice, including MH_BUNDLE, which BinSkim BA5002 skips."""
    data = path.read_bytes()
    if not macho_arches(data):
        raise ValueError(f"Malformed Mach-O: {path.name}")
    magic = struct.unpack_from(">I", data)[0]
    offsets = [0]
    if magic in (0xCAFEBABE, 0xCAFEBABF):
        count = struct.unpack_from(">I", data, 4)[0]
        stride, offset_format = (20, ">I") if magic == 0xCAFEBABE else (32, ">Q")
        offsets = [
            struct.unpack_from(offset_format, data, 8 + index * stride + 8)[0]
            for index in range(count)
        ]
    for offset in offsets:
        endian = (
            "<" if data[offset : offset + 4] in (b"\xce\xfa\xed\xfe", b"\xcf\xfa\xed\xfe") else ">"
        )
        flags = struct.unpack_from(endian + "I", data, offset + 24)[0]
        if flags & 0x20000:  # MH_ALLOW_STACK_EXECUTION
            raise ValueError(f"Executable Mach-O stack: {path.name}")


def canonical_uri(uri):
    # BinSkim emits absolute file URIs, including percent-encoded wheel/member names.
    return unquote(uri).casefold() if sys.platform == "win32" else unquote(uri)


def verify_report(report, binaries):
    runs = json.loads(report.read_text(encoding="utf-8-sig")).get("runs", [])
    if not runs:
        raise ValueError("BinSkim produced no runs")
    seen = set()
    evaluated = {}
    errors = []
    for run in runs:
        invocations = run.get("invocations", [])
        if not invocations or any(i.get("executionSuccessful") is not True for i in invocations):
            errors.append("BinSkim did not complete successfully")
        for invocation in invocations:
            for key in ("toolExecutionNotifications", "toolConfigurationNotifications"):
                if any(n.get("level") == "error" for n in invocation.get(key, [])):
                    errors.append("BinSkim reported an analysis error")
        for result in run.get("results", []):
            if result.get("kind", "fail") == "fail" and result.get("level", "warning") == "error":
                errors.append(f"BinSkim {result.get('ruleId', 'unknown')} failed")
            for location in result.get("locations", []):
                artifact = location.get("physicalLocation", {}).get("artifactLocation", {})
                if "index" in artifact and "uri" not in artifact:
                    artifact = run["artifacts"][artifact["index"]]["location"]
                uri = artifact.get("uri", "")
                if uri:
                    seen.add(canonical_uri(uri))
                    if result.get("kind", "fail") in ("pass", "fail"):
                        evaluated.setdefault(canonical_uri(uri), set()).add(result.get("ruleId"))
    missing = {canonical_uri(uri) for uri in binaries} - seen
    if missing:
        errors.append(f"BinSkim omitted {len(missing)} native binary/binaries")
    for uri, kind in binaries.items():
        # These ELF checks apply to shared libraries, not only executables.
        if kind == "ELF" and not {"BA3006", "BA3010", "BA3011"} <= evaluated.get(
            canonical_uri(uri), set()
        ):
            errors.append("BinSkim did not evaluate the required ELF checks")
        if kind == "PE" and not evaluated.get(canonical_uri(uri)):
            errors.append("BinSkim did not evaluate any PE checks")
    if errors:
        raise ValueError("; ".join(errors))


def scan_wheels(wheel_dir, work_dir, binskim, expected_wheels, symbols=None):
    wheels = sorted(wheel_dir.rglob("*.whl"))
    if not wheels:
        raise ValueError("No wheels to scan")
    if len(wheels) != expected_wheels:
        raise ValueError(f"Expected {expected_wheels} wheels, found {len(wheels)}")
    work_dir.mkdir(parents=True, exist_ok=False)
    reports = work_dir / "reports"
    reports.mkdir()
    symbol_dirs = (
        sorted({str(p.parent.resolve()) for p in symbols.rglob("*.pdb")}) if symbols else []
    )
    inventory = []
    failures = []
    for index, wheel in enumerate(wheels):
        destination = work_dir / str(index)
        binaries = extract_binaries(wheel, destination)
        for uri, kind in binaries.items():
            if kind == "Mach-O":
                # URI conversion is only for local files just extracted above.
                local_path = unquote(urlsplit(uri).path)
                if sys.platform == "win32":
                    local_path = local_path.lstrip("/")
                check_macho_stack(Path(local_path))
        report = reports / f"{index}.sarif"
        command = [
            str(binskim),
            "analyze",
            str(destination.resolve() / "*"),
            "--recurse",
            "true",
            "--output",
            str(report.resolve()),
            "--kind",
            "Fail;Pass;NotApplicable",
            "--level",
            "Error;Warning;Note",
            "--trace",
            "TargetsScanned",
            "--quiet",
            "true",
        ]
        if symbol_dirs:
            command += ["--local-symbol-directories", ";".join(symbol_dirs)]
        result = subprocess.run(command, check=False)
        try:
            verify_report(report, binaries)
            if result.returncode:
                raise ValueError(f"BinSkim exit code {result.returncode}")
        except (ValueError, OSError) as error:
            failures.append(f"{wheel.name}: {error}")
        inventory.append({"wheel": wheel.name, "binaries": binaries, "report": report.name})
        print(f"{wheel.name}: {len(binaries)} native binaries")
    (reports / "inventory.json").write_text(json.dumps(inventory, indent=2), encoding="utf-8")
    if failures:
        raise ValueError("\n".join(failures))
    print(f"Scanned {len(wheels)} wheels, {sum(len(i['binaries']) for i in inventory)} binaries")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("wheel_dir", type=Path)
    parser.add_argument("work_dir", type=Path)
    parser.add_argument("binskim", type=Path)
    parser.add_argument("--expected-wheels", type=int, required=True)
    parser.add_argument("--symbols", type=Path)
    args = parser.parse_args()
    try:
        scan_wheels(args.wheel_dir, args.work_dir, args.binskim, args.expected_wheels, args.symbols)
    except (ValueError, OSError, zipfile.BadZipFile) as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    main()
