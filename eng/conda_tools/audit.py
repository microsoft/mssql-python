"""Shared package discovery/read/filter/validation lifecycle, without CLI output."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Iterable

from . import archive, contracts
from .formats import Format, elf, macho, pe


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
    members = archive.iter_payload_members(path)
    base = os.path.basename(path)
    if kind == "elf":
        return contracts.validate_elf(
            base, index, [(name, elf.parse(data)) for name, data in members]
        )
    if kind == "pe":
        return contracts.validate_pe(
            base, index, [(name, pe.pe_machine(data)) for name, data in members]
        )
    return contracts.validate_macho(
        base, index, [(name, macho.macho_arches(data)) for name, data in members]
    )


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
