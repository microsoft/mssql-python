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
import re
import tarfile
import zipfile
from typing import Any, Iterable, Iterator


def validate_native_contract(
    members: Iterable[tuple[str, bytes]], index: dict[str, Any]
) -> list[str]:
    """Require a target binding, core extension and initializer; platform audits check headers.

    Wheels may include bindings for several Python minors. The core uses Python's
    normal extension loader, including its stable-ABI suffix. These static checks
    do not replace native import/feature qualification.
    """
    pins = [
        d
        for d in index.get("depends", [])
        if isinstance(d, str) and d.split()[:1] == ["python_abi"]
    ]
    abi = re.fullmatch(r"python_abi (3\.\d+)\.\* \*_cp(3\d+)", pins[0]) if len(pins) == 1 else None
    if abi is None or abi[1].replace(".", "") != abi[2]:
        return ["expected a matching normal CPython python_abi pin"]
    subdir = index["subdir"]
    windows = subdir.startswith("win-")
    suffix = "pyd" if windows else "so"
    prefix = "Lib" if windows else f"lib/python{abi[1]}"
    root = f"{prefix}/site-packages/"
    names = [name.replace("\\", "/") for name, _ in members]
    bindings = [
        name
        for name in names
        if re.fullmatch(
            rf"{re.escape(root)}mssql_python/ddbc_bindings\.cp{abi[2]}-[^/]+\.{suffix}", name
        )
    ]
    cores = [
        name
        for name in names
        if re.fullmatch(
            rf"{re.escape(root)}mssql_py_core/mssql_py_core(?:\.[^/]+)?\.{suffix}", name
        )
    ]
    arch = {
        "win-64": "win_amd64",
        "win-arm64": "win_arm64",
        "linux-64": "x86_64-linux-gnu",
        "linux-aarch64": "aarch64-linux-gnu",
        "osx-64": "darwin",
        "osx-arm64": "darwin",
    }[subdir]
    core_names = (
        (f"mssql_py_core.cp{abi[2]}-{arch}.pyd", "mssql_py_core.pyd")
        if windows
        else (f"mssql_py_core.cpython-{abi[2]}-{arch}.so", "mssql_py_core.abi3.so")
    )
    errors = []
    initializer = f"{root}mssql_py_core/__init__.py"
    if names.count(initializer) != 1:
        errors.append(
            f"expected exactly one required {initializer}; found {names.count(initializer)}"
        )
    if len(bindings) != 1:
        errors.append(
            f"expected exactly one normal cp{abi[2]} native binding; found {len(bindings)}"
        )
    if len(cores) != 1:
        errors.append(
            f"expected exactly one required mssql_py_core native extension; found {len(cores)}"
        )
    elif cores[0].rsplit("/", 1)[-1] not in core_names:
        errors.append(f"{cores[0]}: mssql_py_core is incompatible with normal cp{abi[2]} {subdir}")
    return errors


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
        return zstandard.ZstdDecompressor().decompress(raw)
    return zstd.decompress(raw)


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
