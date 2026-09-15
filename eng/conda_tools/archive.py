"""Conda archive discovery, payload I/O and structural metadata validation."""

from __future__ import annotations

import glob
import io
import json
import os
import tarfile
import zipfile
from typing import Any, Iterator

READ_ERRORS = (
    OSError,
    ValueError,
    KeyError,
    EOFError,
    RuntimeError,
    tarfile.TarError,
    zipfile.BadZipFile,
)


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


def iter_payload_members(path: str) -> Iterator[tuple[str, bytes]]:
    """Yield ``(member_name, data_bytes)`` for the files in a ``.conda`` / ``.tar.bz2`` payload."""
    if path.endswith(".conda"):
        with zipfile.ZipFile(path) as zf:
            pkg_name = next(
                (n for n in zf.namelist() if n.startswith("pkg-") and n.endswith(".tar.zst")),
                None,
            )
            if pkg_name is None:
                raise ValueError(f"{path}: no pkg-*.tar.zst payload found in .conda archive")
            blob = zstd_decompress(zf.read(pkg_name))
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
            info_name = next(
                (n for n in zf.namelist() if n.startswith("info-") and n.endswith(".tar.zst")),
                None,
            )
            if info_name is None:
                raise ValueError("no info-*.tar.zst member (malformed .conda)")
            blob = zstd_decompress(zf.read(info_name))
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
