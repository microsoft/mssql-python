"""Conda archive discovery, payload I/O and structural metadata validation."""

from __future__ import annotations

import glob
import csv
import io
import json
import os
import tarfile
import zipfile
from email.parser import BytesParser
from email.policy import default
from pathlib import Path
from typing import Any, Iterator, TypedDict

READ_ERRORS = (
    OSError,
    ValueError,
    KeyError,
    EOFError,
    RuntimeError,
    tarfile.TarError,
    zipfile.BadZipFile,
)


class DistributionMetadata(TypedDict):
    name: str
    version: str
    requires_dist: list[str]


class WheelMetadata(DistributionMetadata):
    members: list[str]
    record_members: list[str]
    tags: list[str]


def parse_distribution_metadata(data: bytes) -> DistributionMetadata:
    """Read installed or wheel METADATA without applying dependency policy."""
    metadata = BytesParser(policy=default).parsebytes(data)
    if metadata.defects:
        raise ValueError(f"malformed METADATA: {metadata.defects}")
    values = {}
    for field in ("Name", "Version"):
        entries = metadata.get_all(field, [])
        if len(entries) != 1 or not str(entries[0]).strip():
            raise ValueError(f"expected exactly one nonempty METADATA {field}.")
        values[field] = str(entries[0]).strip()
    return {
        "name": values["Name"],
        "version": values["Version"],
        "requires_dist": [str(value).strip() for value in metadata.get_all("Requires-Dist", [])],
    }


def parse_record_members(data: bytes) -> list[str]:
    """Read RECORD ownership paths; installed native hashes may change during relocation."""
    try:
        rows = list(csv.reader(io.StringIO(data.decode("utf-8")), strict=True))
    except csv.Error as exc:
        raise ValueError(f"malformed RECORD: {exc}") from exc
    if any(len(row) != 3 or not row[0] for row in rows):
        raise ValueError("RECORD must contain three-column rows with nonempty member paths")
    names = [row[0] for row in rows]
    if len(names) != len(set(names)):
        raise ValueError("RECORD contains duplicate member paths")
    return names


def read_wheel_metadata(path: str | Path) -> WheelMetadata:
    """Return metadata, actual members, declared ownership and wheel tags as facts."""
    with zipfile.ZipFile(path) as wheel:
        names = wheel.namelist()
        entries = [
            name for name in names if name.count("/") == 1 and name.endswith(".dist-info/METADATA")
        ]
        if len(entries) != 1:
            raise ValueError("expected exactly one .dist-info/METADATA entry.")
        metadata = parse_distribution_metadata(wheel.read(entries[0]))
        prefix = entries[0][: -len("METADATA")]
        for member in ("RECORD", "WHEEL"):
            if names.count(prefix + member) != 1:
                raise ValueError(f"expected exactly one {prefix}{member} entry")
        records = parse_record_members(wheel.read(prefix + "RECORD"))
        tags = BytesParser(policy=default).parsebytes(wheel.read(prefix + "WHEEL"))
        return {
            **metadata,
            "members": [entry.filename for entry in wheel.infolist() if not entry.is_dir()],
            "record_members": records,
            "tags": [str(tag).strip() for tag in tags.get_all("Tag", [])],
        }


def zstd_decompress(raw: bytes) -> bytes:
    """Decompress a zstandard blob, preferring the 3.14+ stdlib backend."""
    try:  # Python 3.14+
        from compression import zstd  # type: ignore
    except ImportError:
        try:
            import zstandard  # third-party fallback
        except ImportError as exc:
            raise RuntimeError(
                "Unable to import 'zstandard': reading .conda (.tar.zst) payloads requires "
                "Python 3.14+ with compression.zstd or a working 'zstandard' install "
                "(pip install zstandard)."
            ) from exc
        try:
            return zstandard.ZstdDecompressor().decompress(raw)
        except zstandard.ZstdError as exc:
            raise ValueError(str(exc)) from exc
    try:
        return zstd.decompress(raw)
    except zstd.ZstdError as exc:
        raise ValueError(str(exc)) from exc


def _conda_component(zf: zipfile.ZipFile, component: str) -> zipfile.ZipInfo:
    matches = [
        member
        for member in zf.infolist()
        if member.filename.startswith(f"{component}-") and member.filename.endswith(".tar.zst")
    ]
    if len(matches) != 1:
        raise ValueError(
            f"expected exactly one {component}-*.tar.zst member in .conda archive; "
            f"found {len(matches)}"
        )
    return matches[0]


def iter_payload_members(path: str) -> Iterator[tuple[str, bytes]]:
    """Yield ``(member_name, data_bytes)`` for the files in a ``.conda`` / ``.tar.bz2`` payload."""
    if path.endswith(".conda"):
        with zipfile.ZipFile(path) as zf:
            blob = zstd_decompress(zf.read(_conda_component(zf, "pkg")))
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
    else:
        # Fail CLOSED like read_index -- a caller that gets an unexpected extension must NOT
        # receive a silently-empty iterator (a truncated/renamed package would slip through).
        raise ValueError(f"{path}: unrecognized conda package extension")


def read_index(path: str) -> dict[str, Any]:
    """Return the package's ``info/index.json`` as a dict.

    RAISES on a malformed/unreadable package -- callers must NOT swallow this into a
    silent "non-Linux/non-Windows, skip" (a truncated package would then slip through).
    """
    if path.endswith(".conda"):
        with zipfile.ZipFile(path) as zf:
            blob = zstd_decompress(zf.read(_conda_component(zf, "info")))
        with tarfile.open(fileobj=io.BytesIO(blob)) as tf:
            member = tf.extractfile("info/index.json")
            if member is None:
                raise ValueError("info/index.json missing")
            index = json.load(member)
    elif path.endswith(".tar.bz2"):
        with tarfile.open(path, "r:bz2") as tf:
            member = tf.extractfile("info/index.json")
            if member is None:
                raise ValueError("info/index.json missing")
            index = json.load(member)
    else:
        raise ValueError("unrecognized conda package extension")
    if not isinstance(index, dict):
        raise ValueError("info/index.json must be an object")
    depends = index.get("depends", [])
    if not isinstance(depends, list) or any(not isinstance(dep, str) for dep in depends):
        raise ValueError("info/index.json depends must be a list of dependency strings")
    return index


def collect(root: str) -> list[str]:
    return sorted(
        glob.glob(os.path.join(root, "**", "*.conda"), recursive=True)
        + glob.glob(os.path.join(root, "**", "*.tar.bz2"), recursive=True)
    )
