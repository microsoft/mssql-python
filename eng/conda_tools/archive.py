"""Conda archive discovery, payload I/O and structural metadata validation."""

from __future__ import annotations

import glob
import csv
import bz2
from contextlib import contextmanager
import io
import json
import os
import posixpath
import re
import tarfile
import zipfile
from collections import Counter
from email.parser import BytesParser
from email.policy import default
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Protocol, TypedDict

_CHUNK_BYTES = 64 * 1024
_MAX_ARCHIVE_BYTES = 256 * 1024**2
_MAX_COMPRESSED_BYTES = 256 * 1024**2
_MAX_EXPANDED_BYTES = 1024**3
_MAX_MEMBER_BYTES = 128 * 1024**2
_MAX_PAYLOAD_BYTES = 512 * 1024**2
_MAX_METADATA_BYTES = 8 * 1024**2
_MAX_METADATA_TOTAL = 64 * 1024**2
_MAX_MEMBERS = 10_000
_MAX_ZIP_MEMBERS = 10_000
_MAX_ZIP_DIRECTORY_BYTES = 1024**2
_MAX_ZSTD_WINDOW_BYTES = 64 * 1024**2

READ_ERRORS = (
    OSError,
    ValueError,
    KeyError,
    EOFError,
    RuntimeError,
    tarfile.TarError,
    zipfile.BadZipFile,
)


def _check_limit(size: int, limit: int, description: str) -> None:
    if size < 0 or size > limit:
        raise ValueError(f"Archive limit exceeded: {description} ({size}; limit {limit})")


class _Readable(Protocol):
    def read(self, size: int = -1, /) -> bytes: ...


class _SeekableReader(_Readable, Protocol):
    def seek(self, offset: int, whence: int = 0, /) -> int: ...


class _LimitedReader(io.RawIOBase):
    def __init__(self, source: _Readable, limit: int, description: str):
        self.source = source
        self.remaining = limit
        self.limit = limit
        self.description = description

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            raise ValueError("Archive stream reads must specify a bounded size")
        data = self.source.read(min(size, _CHUNK_BYTES, self.remaining + 1))
        self.remaining -= len(data)
        _check_limit(self.limit - self.remaining, self.limit, self.description)
        return data

    def readinto(self, buffer) -> int:
        data = self.read(len(buffer))
        buffer[: len(data)] = data
        return len(data)


class _ZipInput(io.BufferedReader):
    def read(self, size: int | None = -1) -> bytes:
        if size is None or size < 0:
            size = os.fstat(self.fileno()).st_size - self.tell()
        # ZipFile reads the central directory in one allocation, before exposing entries.
        _check_limit(size, _MAX_ZIP_DIRECTORY_BYTES, "ZIP directory/read bytes")
        return super().read(size)


@contextmanager
def _open_zip(path: str | Path) -> Iterator[zipfile.ZipFile]:
    with _ZipInput(open(path, "rb", buffering=0)) as source:
        _check_limit(os.fstat(source.fileno()).st_size, _MAX_ARCHIVE_BYTES, "archive bytes")
        with zipfile.ZipFile(source) as container:
            _check_limit(len(container.infolist()), _MAX_ZIP_MEMBERS, "ZIP member count")
            for member in container.infolist():
                if member.filename != member.orig_filename:
                    raise ValueError(f"Noncanonical ZIP member name: {member.orig_filename!r}")
                if member.compress_type not in (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED):
                    raise ValueError("Archive ZIP members must use stored or deflate compression")
                _check_limit(member.compress_size, _MAX_COMPRESSED_BYTES, "compressed bytes")
                _check_limit(member.file_size, _MAX_COMPRESSED_BYTES, "ZIP member bytes")
            yield container


def _read_bytes(source: _Readable, limit: int, description: str) -> bytes:
    reader = _LimitedReader(source, limit, description)
    with io.BytesIO() as result:
        while data := reader.read(_CHUNK_BYTES):
            result.write(data)
        return result.getvalue()


def _zip_metadata(container: zipfile.ZipFile, member: str) -> bytes:
    info = container.getinfo(member)
    _check_limit(info.file_size, _MAX_METADATA_BYTES, "metadata bytes")
    with container.open(info) as source:
        return _read_bytes(source, _MAX_METADATA_BYTES, "metadata bytes")


class DistributionMetadata(TypedDict):
    name: str
    version: str
    requires_dist: list[str]


class WheelMetadata(DistributionMetadata):
    members: list[str]
    record_members: list[str]
    tags: list[str]


def canonical_distribution_name(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def metadata_members(members: Iterable[str], field: str) -> list[str]:
    suffix = ".dist-info/" + field.casefold()
    return [
        member
        for member in members
        if posixpath.normpath(member.replace("\\", "/")).casefold().endswith(suffix)
    ]


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
    with _open_zip(path) as wheel:
        names = wheel.namelist()
        entries = metadata_members(names, "METADATA")
        if (
            len(entries) != 1
            or entries[0].count("/") != 1
            or not entries[0].endswith(".dist-info/METADATA")
        ):
            raise ValueError("expected exactly one .dist-info/METADATA entry.")
        metadata = parse_distribution_metadata(_zip_metadata(wheel, entries[0]))
        prefix = entries[0][: -len("METADATA")]
        for member in ("RECORD", "WHEEL"):
            if metadata_members(names, member) != [prefix + member]:
                raise ValueError(f"expected exactly one {prefix}{member} entry")
        records = parse_record_members(_zip_metadata(wheel, prefix + "RECORD"))
        return {
            **metadata,
            "members": [entry.filename for entry in wheel.infolist() if not entry.is_dir()],
            "record_members": records,
            "tags": parse_wheel_tags(_zip_metadata(wheel, prefix + "WHEEL")),
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
    counts = Counter(names)
    candidates = {
        field: metadata_members(names, field) for field in ("METADATA", "RECORD", "WHEEL")
    }
    for field, members in candidates.items():
        seen = set()
        for member in members:
            if posixpath.normpath(member.replace("\\", "/")) != member or not member.endswith(
                ".dist-info/" + field
            ):
                raise ValueError(f"Noncanonical installed {field} member: {member}")
            if counts[member] != 1:
                if field == "METADATA":
                    raise ValueError(f"duplicate installed metadata: {member}")
                raise ValueError(f"expected exactly one installed {field} entry: {member}")
            if member.casefold() in seen:
                raise ValueError(f"Aliased installed {field} entries: {member}")
            seen.add(member.casefold())
    if set(metadata_members(files, "METADATA")) != set(candidates["METADATA"]):
        raise ValueError("Installed METADATA contents differ from the actual payload members")
    distributions = []
    for member in candidates["METADATA"]:
        facts = parse_distribution_metadata(files[member])
        prefix = member[: -len("METADATA")]
        directory = prefix.rstrip("/").rsplit("/", 1)[-1]
        expected = (
            canonical_distribution_name(facts["name"]).replace("-", "_")
            + f"-{facts['version']}.dist-info"
        )
        if directory != expected:
            raise ValueError(f"Noncanonical installed METADATA directory: {member}")
        distributions.append((facts, prefix))
    prefixes = {prefix for _, prefix in distributions}
    for field in ("RECORD", "WHEEL"):
        if any(member[: -len(field)] not in prefixes for member in candidates[field]):
            raise ValueError(f"Unexpected or aliased installed {field} member")
    record_files = {}
    for _, prefix in distributions:
        record = prefix + "RECORD"
        if counts[record] != 1 or record not in files:
            raise ValueError(f"expected exactly one installed RECORD entry: {record}")
        record_files[record] = parse_record_members(files[record])
    for facts, prefix in distributions:
        root = prefix.rstrip("/").rsplit("/", 1)[0] + "/"
        record = prefix + "RECORD"
        records = record_files[record]
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


def _read_exact(source: _Readable, size: int) -> bytes:
    data = source.read(size)
    if len(data) != size:
        raise EOFError("Truncated zstandard frame")
    return data


def _validate_zstd_frames(source: _Readable) -> None:
    """Check frame boundaries because python-zstandard's reader accepts truncated EOF."""
    import zstandard

    reader = _LimitedReader(source, _MAX_COMPRESSED_BYTES, "compressed bytes")
    frames = 0
    while magic := reader.read(4):
        frames += 1
        _check_limit(frames, _MAX_MEMBERS, "zstandard frame count")
        if len(magic) != 4:
            raise EOFError("Truncated zstandard frame")
        number = int.from_bytes(magic, "little")
        if number & 0xFFFFFFF0 == 0x184D2A50:
            remaining = int.from_bytes(_read_exact(reader, 4), "little")
        else:
            if magic != b"\x28\xb5\x2f\xfd":
                raise ValueError("invalid zstd frame magic")
            header = magic + _read_exact(reader, 2)
            header += _read_exact(reader, zstandard.frame_header_size(header) - len(header))
            parameters = zstandard.get_frame_parameters(header)
            _check_limit(parameters.window_size, _MAX_ZSTD_WINDOW_BYTES, "zstandard window bytes")
            if parameters.content_size != zstandard.CONTENTSIZE_UNKNOWN:
                _check_limit(parameters.content_size, _MAX_EXPANDED_BYTES, "expanded bytes")
            while True:
                block = int.from_bytes(_read_exact(reader, 3), "little")
                kind, size = (block >> 1) & 3, block >> 3
                if kind == 3 or size > 128 * 1024:
                    raise ValueError("invalid zstd block header")
                remaining = 1 if kind == 1 else size
                while remaining:
                    chunk = _read_exact(reader, min(remaining, _CHUNK_BYTES))
                    remaining -= len(chunk)
                if block & 1:
                    break
            remaining = 4 if parameters.has_checksum else 0
        while remaining:
            chunk = _read_exact(reader, min(remaining, _CHUNK_BYTES))
            remaining -= len(chunk)
    if not frames:
        raise EOFError("Missing zstandard frame")


@contextmanager
def _zstd_stream(source: _SeekableReader, purpose: str) -> Iterator[_Readable]:
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
        try:
            _validate_zstd_frames(source)
            source.seek(0)
            with io.BufferedReader(
                _LimitedReader(source, _MAX_COMPRESSED_BYTES, "compressed bytes"),
                buffer_size=_CHUNK_BYTES,
            ) as compressed:
                # Tested python-zstandard 0.23/0.25 forward a byte count to libzstd here.
                with zstandard.ZstdDecompressor(
                    max_window_size=_MAX_ZSTD_WINDOW_BYTES
                ).stream_reader(
                    compressed,
                    read_size=_CHUNK_BYTES,
                    read_across_frames=True,
                    closefd=False,
                ) as decoded:
                    yield decoded
        except zstandard.ZstdError as exc:
            raise ValueError(str(exc)) from exc
    else:
        try:
            with zstd.ZstdFile(
                _LimitedReader(source, _MAX_COMPRESSED_BYTES, "compressed bytes"),
                mode="rb",
                options={
                    zstd.DecompressionParameter.window_log_max: _MAX_ZSTD_WINDOW_BYTES.bit_length()
                    - 1
                },
            ) as decoded:
                yield decoded
        except zstd.ZstdError as exc:
            raise ValueError(str(exc)) from exc


def zstd_decompress(raw: bytes) -> bytes:
    """Decode a bounded byte input; archive readers use streaming instead."""
    _check_limit(len(raw), _MAX_COMPRESSED_BYTES, "compressed bytes")
    with _zstd_stream(io.BytesIO(raw), "payloads") as decoded:
        return _read_bytes(decoded, _MAX_EXPANDED_BYTES, "expanded bytes")


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
    """Normalize bounded release-index decoding errors without changing backends."""
    _check_limit(len(raw), _MAX_COMPRESSED_BYTES, "compressed bytes")
    with _zstd_stream(io.BytesIO(raw), "metadata") as decoded:
        return _read_bytes(decoded, _MAX_EXPANDED_BYTES, "expanded bytes")


def _member_limit(member: tarfile.TarInfo) -> tuple[int, str]:
    normalized = posixpath.normpath(member.name.replace("\\", "/")).casefold()
    if (
        normalized.startswith("info/")
        or ".dist-info/" in normalized
        or member.type
        in (
            tarfile.XHDTYPE,
            tarfile.XGLTYPE,
            tarfile.SOLARIS_XHDTYPE,
            tarfile.GNUTYPE_LONGNAME,
            tarfile.GNUTYPE_LONGLINK,
        )
    ):
        return _MAX_METADATA_BYTES, "metadata bytes"
    return _MAX_MEMBER_BYTES, "member bytes"


def _tar_info_type() -> type[tarfile.TarInfo]:
    count, metadata_bytes = 0, 0

    class LimitedTarInfo(tarfile.TarInfo):
        @classmethod
        def _frombuf(
            cls, buf: bytes | bytearray, encoding: str, errors: str, **kwargs: bool
        ) -> LimitedTarInfo:
            nonlocal count, metadata_bytes
            # Recent Python security updates route parsing through _frombuf instead.
            decode = getattr(super(), "_frombuf", super().frombuf)
            member = decode(buf, encoding, errors, **kwargs)
            count += 1
            _check_limit(count, _MAX_MEMBERS, "TAR member count")
            limit, description = _member_limit(member)
            _check_limit(member.size, limit, description)
            if description == "metadata bytes":
                metadata_bytes += member.size
                _check_limit(metadata_bytes, _MAX_METADATA_TOTAL, "cumulative metadata bytes")
            return member

        frombuf = _frombuf

        def _reject_sparse(self, *args):
            raise ValueError("Sparse TAR members are not supported by Conda archive readers")

        # Sparse extension parsing can allocate maps before TarFile returns a member.
        _proc_sparse = _proc_gnusparse_00 = _proc_gnusparse_01 = _proc_gnusparse_10 = _reject_sparse

    return LimitedTarInfo


def _tar_members(contents: tarfile.TarFile) -> Iterator[tarfile.TarInfo]:
    total = 0
    for member in contents:
        _check_limit(member.size, *_member_limit(member))
        total += member.size
        _check_limit(total, _MAX_PAYLOAD_BYTES, "cumulative member bytes")
        yield member


@contextmanager
def _stream_tar(decoded: _Readable) -> Iterator[tarfile.TarFile]:
    reader = _LimitedReader(decoded, _MAX_EXPANDED_BYTES, "expanded bytes")
    with tarfile.open(fileobj=reader, mode="r|", tarinfo=_tar_info_type()) as contents:
        yield contents
        # Account for padding/trailing frames and detect truncated compressor footers.
        while reader.read(_CHUNK_BYTES):
            pass


@contextmanager
def _open_tar(
    path: str,
    select: Callable[[zipfile.ZipFile], str | zipfile.ZipInfo],
) -> Iterator[tarfile.TarFile]:
    if path.endswith(".conda"):
        with _open_zip(path) as container:
            with container.open(select(container)) as source:
                with _zstd_stream(source, "payloads/metadata") as decoded:
                    with _stream_tar(decoded) as contents:
                        yield contents
    elif path.endswith(".tar.bz2"):
        with open(path, "rb") as source:
            _check_limit(os.fstat(source.fileno()).st_size, _MAX_ARCHIVE_BYTES, "archive bytes")
            with bz2.BZ2File(
                _LimitedReader(source, _MAX_COMPRESSED_BYTES, "compressed bytes")
            ) as decoded:
                with _stream_tar(decoded) as contents:
                    yield contents
    else:
        raise ValueError(f"{path}: unrecognized conda package extension")


def iter_payload_members(path: str) -> Iterator[tuple[str, bytes]]:
    """Yield regular payload members after unambiguous component selection."""

    def select(container: zipfile.ZipFile) -> zipfile.ZipInfo:
        return _conda_component(container, "pkg")

    with _open_tar(path, select) as contents:
        for member in _tar_members(contents):
            if member.isfile():
                source = contents.extractfile(member)
                if source is not None:
                    yield member.name, _read_bytes(source, *_member_limit(member))


def _tar_index(contents: tarfile.TarFile, path: str) -> dict:
    data = None
    for member in _tar_members(contents):
        if member.name != "info/index.json":
            continue
        if data is not None or not member.isfile():
            raise ValueError(f"{path}: expected exactly one regular info/index.json member")
        source = contents.extractfile(member)
        if source is None:
            raise ValueError(f"{path}: info/index.json is unreadable")
        data = _read_bytes(source, _MAX_METADATA_BYTES, "metadata bytes")
    if data is None:
        raise ValueError(f"{path}: expected exactly one regular info/index.json member")
    return _require_index_object(json.loads(data), path)


def read_index(path: str) -> dict[str, Any]:
    """Read native-audit metadata without applying the stricter release-container policy."""

    def select(container: zipfile.ZipFile) -> zipfile.ZipInfo:
        return _conda_component(container, "info")

    if not path.endswith((".conda", ".tar.bz2")):
        raise ValueError("unrecognized conda package extension")
    with _open_tar(path, select) as contents:
        index = _tar_index(contents, path)
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
        metadata = json.loads(_zip_metadata(container, "metadata.json"))
        if (
            not isinstance(metadata, dict)
            or type(metadata.get("conda_pkg_format_version")) is not int
            or metadata["conda_pkg_format_version"] != 2
        ):
            raise ValueError(f"{path}: metadata.json must declare conda_pkg_format_version 2")
        return info_names[0]

    with _open_tar(path, select) as contents:
        return _tar_index(contents, path)
