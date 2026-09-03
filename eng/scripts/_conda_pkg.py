"""Shared ``.conda`` / ``.tar.bz2`` payload readers for the conda binary-audit scripts.

``audit_bundled_binaries.py`` (Linux ELF RUNPATH) and ``assert_pe_machine.py`` (Windows
PE machine) both need to (a) zstd-decompress a ``.conda`` member, (b) iterate the package
payload files, and (c) read ``info/index.json``. Keeping that extraction in ONE place stops
the two validators from drifting as they grow (pylint R0801).

This is a plain sibling module: both scripts are invoked as ``python <path>/<script>.py``,
so their own directory (``eng/scripts``) is on ``sys.path`` and ``import _conda_pkg`` resolves
here; the unit tests that load the scripts by path insert that directory too.
"""

from __future__ import annotations

import io
import json
import tarfile
import zipfile


def zstd_decompress(raw: bytes) -> bytes:
    """Decompress a zstandard blob, preferring the 3.14+ stdlib backend."""
    try:  # Python 3.14+
        from compression import zstd  # type: ignore

        return zstd.decompress(raw)
    except Exception:
        pass
    import zstandard  # third-party fallback

    return zstandard.ZstdDecompressor().decompress(raw)


def iter_payload_members(path: str):
    """Yield ``(member_name, data_bytes)`` for the files in a ``.conda`` / ``.tar.bz2`` payload."""
    if path.endswith(".conda"):
        with zipfile.ZipFile(path) as zf:
            pkg_name = next(
                (n for n in zf.namelist() if n.startswith("pkg-") and n.endswith(".tar.zst")),
                None,
            )
            if pkg_name is None:
                return
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


def read_index(path: str) -> dict:
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
            return json.load(member)
    if path.endswith(".tar.bz2"):
        with tarfile.open(path, "r:bz2") as tf:
            member = tf.extractfile("info/index.json")
            if member is None:
                raise ValueError("info/index.json missing")
            return json.load(member)
    raise ValueError("unrecognized conda package extension")
