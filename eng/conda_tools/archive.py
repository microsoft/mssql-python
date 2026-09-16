"""Conda archive discovery, payload I/O and structural metadata validation."""

from __future__ import annotations

import glob
import csv
from contextlib import contextmanager
import io
import json
import os
import tarfile
import zipfile
from email.parser import BytesParser
from email.policy import default
from pathlib import Path
from typing import Any, Callable, Iterator, TypedDict

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
        return {
            **metadata,
            "members": [entry.filename for entry in wheel.infolist() if not entry.is_dir()],
            "record_members": records,
            "tags": parse_wheel_tags(wheel.read(prefix + "WHEEL")),
        }


def parse_wheel_tags(data: bytes) -> list[str]:
    tags = BytesParser(policy=default).parsebytes(data)
    return [str(tag).strip() for tag in tags.get_all("Tag", [])]


def installed_metadata(
    names: list[str], files: dict[str, bytes]
) -> Iterator[tuple[DistributionMetadata, list[str], list[str], str]]:
    """Yield facts, present RECORD-owned paths, RECORD paths and dist-info prefix.

    Installed distributions share site-packages. A binding must not inherit its
    separate provider's ownership just because those files are present beside it.
    """
    record_files = {
        path: parse_record_members(data)
        for path, data in files.items()
        if path.endswith(".dist-info/RECORD")
    }
    for member, data in files.items():
        if not member.endswith(".dist-info/METADATA"):
            continue
        if names.count(member) != 1:
            raise ValueError(f"duplicate installed metadata: {member}")
        facts = parse_distribution_metadata(data)
        root = member.rsplit("/", 2)[0] + "/"
        prefix = member[: -len("METADATA")]
        records = record_files.get(prefix + "RECORD", [])
        owned = set(records)
        present = [
            name[len(root) :]
            for name in names
            if name.startswith(root) and name[len(root) :] in owned
        ]
        yield facts, present, records, prefix


def collect(root: str) -> list[str]:
    return sorted(
        glob.glob(os.path.join(root, "**", "*.conda"), recursive=True)
        + glob.glob(os.path.join(root, "**", "*.tar.bz2"), recursive=True)
    )


def _require_index_object(index: object, path: str) -> dict:
    if not isinstance(index, dict):
        raise ValueError(f"{path}: info/index.json must contain a JSON object")
    return index


def _required_index_string(index: dict, key: str, path: str) -> str:
    value = index.get(key)
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(
            f"{path}: info/index.json field '{key}' must be a non-empty, trimmed string"
        )
    return value


def _validated_package_identity(index: dict, path: str) -> tuple[str, str, str, str]:
    name, version, subdir, build = (
        _required_index_string(index, key, path) for key in ("name", "version", "subdir", "build")
    )
    extension = ".conda" if path.endswith(".conda") else ".tar.bz2"
    canonical_name = f"{name}-{version}-{build}{extension}"
    if Path(path).name != canonical_name:
        raise ValueError(
            f"{path}: must use canonical basename '{canonical_name}': "
            "anaconda-client normalizes upload names from metadata."
        )
    return name, version, subdir, build


def _zstd_decoder(purpose: str) -> tuple[Callable[[bytes], bytes], type[Exception]]:
    try:
        from compression import zstd  # type: ignore
    except ImportError:
        try:
            import zstandard
        except ImportError as exc:
            raise RuntimeError(
                f"Unable to import 'zstandard': reading .conda (.tar.zst) {purpose} requires "
                "Python 3.14+ with compression.zstd or a working 'zstandard' install "
                "(pip install zstandard)."
            ) from exc
        return zstandard.ZstdDecompressor().decompress, zstandard.ZstdError
    return zstd.decompress, zstd.ZstdError


def zstd_decompress(raw: bytes) -> bytes:
    """Normalize native-audit decoding errors."""
    decode, error = _zstd_decoder("payloads")
    try:
        return decode(raw)
    except error as exc:
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


def decompress_index(raw: bytes) -> bytes:
    """Normalize release-index decoding errors without trying another backend."""
    decode, error = _zstd_decoder("metadata")
    try:
        return decode(raw)
    except error as exc:
        raise ValueError(str(exc)) from exc


@contextmanager
def _open_tar(
    path: str,
    select: Callable[[zipfile.ZipFile], str | zipfile.ZipInfo],
    decode: Callable[[bytes], bytes],
) -> Iterator[tarfile.TarFile]:
    if path.endswith(".conda"):
        with zipfile.ZipFile(path) as container:
            blob = decode(container.read(select(container)))
        with tarfile.open(fileobj=io.BytesIO(blob)) as contents:
            yield contents
    elif path.endswith(".tar.bz2"):
        with tarfile.open(path, "r:bz2") as contents:
            yield contents
    else:
        raise ValueError(f"{path}: unrecognized conda package extension")


def iter_payload_members(path: str) -> Iterator[tuple[str, bytes]]:
    """Yield regular payload members after unambiguous component selection."""

    def select(container: zipfile.ZipFile) -> zipfile.ZipInfo:
        return _conda_component(container, "pkg")

    with _open_tar(path, select, zstd_decompress) as contents:
        for member in contents.getmembers():
            if member.isfile():
                source = contents.extractfile(member)
                if source is not None:
                    yield member.name, source.read()


def read_index(path: str) -> dict[str, Any]:
    """Read native-audit metadata without applying the stricter release-container policy."""

    def select(container: zipfile.ZipFile) -> zipfile.ZipInfo:
        return _conda_component(container, "info")

    if not path.endswith((".conda", ".tar.bz2")):
        raise ValueError("unrecognized conda package extension")
    with _open_tar(path, select, zstd_decompress) as contents:
        member = contents.extractfile("info/index.json")
        if member is None:
            raise ValueError("info/index.json missing")
        index = json.load(member)
    if not isinstance(index, dict):
        raise ValueError("info/index.json must be an object")
    depends = index.get("depends", [])
    if not isinstance(depends, list) or any(not isinstance(dep, str) for dep in depends):
        raise ValueError("info/index.json depends must be a list of dependency strings")
    return index


def read_release_index(path: str) -> dict:
    """Require the canonical three-member container and one regular, object-valued index."""

    def select(container: zipfile.ZipFile) -> str:
        names = container.namelist()
        if len(names) != len(set(names)):
            raise ValueError(f"{path}: duplicate ZIP entries are not allowed")
        info_names = [n for n in names if n.startswith("info-") and n.endswith(".tar.zst")]
        if len(info_names) != 1:
            raise ValueError(
                f"{path}: expected exactly one info-*.tar.zst member; found {len(info_names)}"
            )
        pkg_names = [n for n in names if n.startswith("pkg-") and n.endswith(".tar.zst")]
        if len(pkg_names) != 1:
            raise ValueError(
                f"{path}: expected exactly one pkg-*.tar.zst member; found {len(pkg_names)}"
            )
        stem = Path(path).stem
        if set(names) != {"metadata.json", f"info-{stem}.tar.zst", f"pkg-{stem}.tar.zst"}:
            raise ValueError(f"{path}: expected only canonical metadata.json/info/pkg members")
        metadata = json.loads(container.read("metadata.json"))
        if (
            not isinstance(metadata, dict)
            or type(metadata.get("conda_pkg_format_version")) is not int
            or metadata["conda_pkg_format_version"] != 2
        ):
            raise ValueError(f"{path}: metadata.json must declare conda_pkg_format_version 2")
        return info_names[0]

    with _open_tar(path, select, decompress_index) as contents:
        members = [member for member in contents.getmembers() if member.name == "info/index.json"]
        if len(members) != 1 or not members[0].isfile():
            raise ValueError(f"{path}: expected exactly one regular info/index.json member")
        source = contents.extractfile(members[0])
        if source is None:
            raise ValueError(f"{path}: info/index.json is unreadable")
        return _require_index_object(json.load(source), path)
