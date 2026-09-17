"""Shared package discovery/read/filter/validation lifecycle, without CLI output."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Iterable, Iterator

from . import archive, contracts
from .contracts import Format
from .formats import elf, macho, pe


@dataclass
class AuditResult:
    checked: int = 0
    skipped: int = 0
    details: list[str] = field(default_factory=list)
    violations: list[str] = field(default_factory=list)


def discover_packages(root: str | None, packages: Iterable[str] = ()) -> list[str]:
    return sorted(set(packages) | set(archive.collect(root) if root else []))


def _validate_payload(
    path: str, kind: Format, index: dict[str, Any]
) -> tuple[list[str], list[str]]:
    # Retain names and parsed facts, not every member's native payload bytes.
    metadata: dict[str, tuple[archive.DistributionMetadata, str]] = {}
    records: dict[str, list[str]] = {}
    names: list[str] = []

    def members() -> Iterator[tuple[str, bytes]]:
        for name, data in archive.iter_payload_members(path):
            names.append(name)
            if name.endswith(".dist-info/METADATA"):
                facts = archive.parse_distribution_metadata(data)
                distribution = contracts.canonical_distribution_name(facts["name"])
                if distribution in metadata:
                    raise ValueError(f"duplicate installed metadata for {distribution}")
                metadata[distribution] = facts, name
            elif name.endswith(".dist-info/RECORD"):
                records[name] = archive.parse_record_members(data)
            yield name, data

    base = os.path.basename(path)
    if kind == "elf":
        elf_members = [(name, elf.parse(data)) for name, data in members()]
    elif kind == "pe":
        pe_members = [(name, pe.pe_machine(data)) for name, data in members()]
    else:
        macho_members = [(name, macho.macho_arches(data)) for name, data in members()]

    def ownership(
        distribution: str,
    ) -> tuple[archive.DistributionMetadata, list[str], list[str]]:
        facts, member = metadata[distribution]
        root = member.rsplit("/", 2)[0] + "/"
        record = member[: -len("METADATA")] + "RECORD"
        owned = records.get(record, [])
        relative = [name[len(root) :] for name in names if name.startswith(root)]
        return facts, [name for name in relative if name in owned], owned

    rs_required = "mssql-python-rs" in metadata
    if rs_required and "mssql-python" not in metadata:
        raise ValueError("RS distribution is missing its binding distribution metadata")
    if "mssql-python" in metadata:
        binding, files, owned = ownership("mssql-python")
        rs_version = contracts.binding_rs_version(binding, files, owned)
        if rs_version is None and rs_required:
            raise ValueError(
                "historical embedded-core binding must not be combined with an RS distribution"
            )
        if rs_version is not None:
            if not rs_required:
                raise ValueError(
                    f"binding requires mssql-python-rs=={rs_version}, but its metadata is missing"
                )
            rs, files, owned = ownership("mssql-python-rs")
            abi = [dep for dep in index.get("depends", []) if dep.split()[:1] == ["python_abi"]]
            python_tag = abi[0].rsplit("_", 1)[-1] if len(abi) == 1 else ""
            errors = contracts.validate_rs_ownership(
                rs, files, owned, rs_version, python_tag, index["subdir"]
            )
            if errors:
                raise ValueError("; ".join(errors))
    if kind == "elf":
        return contracts.validate_elf(base, index, elf_members, rs_required)
    if kind == "pe":
        return contracts.validate_pe(base, index, pe_members)
    return contracts.validate_macho(base, index, macho_members)


def audit_packages(paths: Iterable[str], kind: Format, subdir: str = "") -> AuditResult:
    result = AuditResult()
    for path in paths:
        base = os.path.basename(path)
        try:
            index = archive.read_index(path)
        except archive.READ_ERRORS as exc:
            if kind != "elf" and subdir:
                result.violations.append(f"{base}: unreadable metadata ({exc}).")
            else:
                result.checked += kind != "elf"
                result.violations.append(f"{base}: unreadable/malformed package metadata ({exc}).")
            continue

        actual_subdir = str(index.get("subdir", ""))
        if subdir and actual_subdir != subdir:
            continue
        # Legacy PE/Mach-O counts include foreign packages when no filter is supplied.
        result.checked += actual_subdir.startswith("linux") if kind == "elf" else 1
        applicable, errors = contracts.target_status(kind, index, base)
        if errors:
            result.violations.extend(errors)
            continue
        if not applicable:
            label = {"elf": "Linux ELF", "pe": "Windows PE", "macho": "macOS Mach-O"}[kind]
            result.skipped += 1
            result.details.append(
                f"  SKIP (no {label} payload): {base} [subdir={actual_subdir or '?'}]"
            )
            continue
        try:
            errors, details = _validate_payload(path, kind, index)
        except archive.READ_ERRORS as exc:
            result.violations.append(f"{base}: unreadable/malformed package payload ({exc}).")
            continue
        result.violations.extend(errors)
        result.details.extend(details)
    return result


def audit_package(path: str, kind: Format) -> AuditResult:
    return audit_packages([path], kind)
