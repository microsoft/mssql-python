"""Metadata-based conda release-readiness gate.

The release pipeline must never ship an incomplete conda set. This module reads
the AUTHORITATIVE ``info/index.json`` embedded in every ``.conda`` / ``.tar.bz2``
(never folder names or bare counts) and validates the self-contained
``mssql-python`` package -- which vendors the ODBC Driver 18 payload, so there is
NO separate companion package:

* every package's real ``subdir`` is in the allowed set AND matches its folder
  (catches a mislabeled / mis-stamped leg);
* the only package name is ``mssql-python`` and its version matches the expected
  release version (or, if none supplied, is internally consistent -- one version);
* the full (required-subdir x Python) matrix is complete -- every required
  platform ships a package for every expected Python.

Exit code 0 = release-ready; non-zero = a violation was found (blocks publish).
"""

from __future__ import annotations

import argparse
import io
import json
import re
import sys
import tarfile
import zipfile
from collections import defaultdict

_BINDING_NAME = "mssql-python"

_PY_TAG_RE = re.compile(r"py(\d)(\d{1,2})")
_PY_DEP_RE = re.compile(r"python\s+(\d+)\.(\d+)")


def _zstd_decompress(raw: bytes) -> bytes:
    """Decompress a zstandard blob, preferring the 3.14+ stdlib backend."""
    try:  # Python 3.14+
        from compression import zstd  # type: ignore

        return zstd.decompress(raw)
    except Exception:  # pragma: no cover - exercised via the third-party path
        pass
    import zstandard  # third-party fallback

    return zstandard.ZstdDecompressor().decompress(raw)


def read_index_json(path: str) -> dict:
    """Return the parsed ``info/index.json`` from a ``.conda`` / ``.tar.bz2``."""
    if path.endswith(".conda"):
        with zipfile.ZipFile(path) as zf:
            info_name = next(
                n for n in zf.namelist() if n.startswith("info-") and n.endswith(".tar.zst")
            )
            info_blob = zf.read(info_name)
        with tarfile.open(fileobj=io.BytesIO(_zstd_decompress(info_blob))) as tf:
            member = tf.extractfile("info/index.json")
            if member is None:  # pragma: no cover - malformed package
                raise ValueError(f"{path}: info/index.json missing")
            return json.load(member)
    if path.endswith(".tar.bz2"):
        with tarfile.open(path, "r:bz2") as tf:
            member = tf.extractfile("info/index.json")
            if member is None:  # pragma: no cover - malformed package
                raise ValueError(f"{path}: info/index.json missing")
            return json.load(member)
    raise ValueError(f"{path}: unrecognized conda package extension")


def python_tag_from_index(index: dict) -> str:
    """Extract the ``X.Y`` Python version a package is built for, or ``''``.

    Uses the build string's ``pyXY`` token first (authoritative for conda-build
    Python packages), then falls back to a ``python X.Y`` run dependency. A
    Python-agnostic package (build string ``0``) has neither and returns ``''``.
    """
    match = _PY_TAG_RE.search(str(index.get("build", "")))
    if match:
        return f"{match.group(1)}.{match.group(2)}"
    for dep in index.get("depends", []) or []:
        match = _PY_DEP_RE.match(str(dep))
        if match:
            return f"{match.group(1)}.{match.group(2)}"
    return ""


def validate(
    packages: list[dict],
    required_subdirs: list[str],
    allowed_subdirs: list[str],
    expected_pythons: list[str],
    expected_versions: dict | None = None,
) -> list[str]:
    """Return a list of human-readable violation strings (empty == release-ready).

    ``packages`` is a list of dicts with keys: ``folder`` (staged subdir folder),
    ``subdir`` (real info/index.json subdir), ``name``, ``version``, ``build``,
    ``python`` (``X.Y`` or ``''``).
    """
    errors: list[str] = []
    expected_versions = expected_versions or {}

    # 1. Authoritative subdir must be allowed AND match the folder it was staged in.
    for p in packages:
        ident = f"{p['name']}-{p['version']}-{p['build']}"
        if p["subdir"] not in allowed_subdirs:
            errors.append(
                f"{ident}: real subdir '{p['subdir']}' is not in allowed set {allowed_subdirs}."
            )
        if p["subdir"] != p["folder"]:
            errors.append(
                f"MISLABELED: {ident} is staged in folder '{p['folder']}' but its "
                f"info/index.json subdir is '{p['subdir']}'."
            )

    # 2. Only the self-contained mssql-python package may appear; versions match
    #    expected (or are internally consistent -- one version per package).
    seen_versions: dict = defaultdict(set)
    for p in packages:
        if p["name"] != _BINDING_NAME:
            errors.append(
                f"unexpected package name '{p['name']}' ({p['version']}); the "
                f"self-contained conda package ships only '{_BINDING_NAME}'."
            )
            continue
        seen_versions[p["name"]].add(p["version"])
    for name, versions in seen_versions.items():
        if len(versions) > 1:
            errors.append(
                f"{name}: multiple versions present {sorted(versions)} "
                f"(a release must ship exactly one version per package)."
            )
        exp = expected_versions.get(name)
        if exp is not None:
            for v in versions:
                if v != exp:
                    errors.append(f"{name}: version '{v}' != expected '{exp}'.")

    # 2b. Reject duplicate (name, version, subdir, python) keys. Two packages with
    #     an identical key are never legitimate -- it means one leg's package bled
    #     into another subdir's staging folder (the shared-output-dir hazard) or was
    #     staged twice. The per-subdir matrix check below collapses variants into a
    #     set, so a duplicate would silently MASK a genuinely missing variant; fail
    #     loudly on the duplicate instead.
    key_folders: dict = defaultdict(list)
    for p in packages:
        key_folders[(p["name"], p["version"], p["subdir"], p["python"])].append(p["folder"])
    for (name, version, subdir, python), folders in sorted(key_folders.items()):
        if len(folders) > 1:
            errors.append(
                f"DUPLICATE: {name}-{version} (subdir '{subdir}', python "
                f"'{python or '-'}') appears {len(folders)}x (staged in {sorted(folders)})."
            )

    # Group by the REAL (metadata) subdir, never the folder name.
    by_subdir: dict = defaultdict(list)
    for p in packages:
        by_subdir[p["subdir"]].append(p)

    # 3. Required subdirs must be PRESENT; every present ALLOWED subdir must ship a
    #    COMPLETE per-Python matrix. Validating present-but-not-required subdirs too
    #    (not just the required set) stops a partially built allowed subdir -- e.g. a
    #    half-finished win-arm64 -- from slipping through to publish just because it
    #    is not in the required set.
    for sub in required_subdirs:
        if not by_subdir.get(sub):
            errors.append(f"required subdir '{sub}' is MISSING.")

    for sub in sorted(by_subdir):
        if sub not in allowed_subdirs:
            # Not an allowed subdir: already flagged per-package in step 1. Skip the
            # matrix work so the error set stays focused on the root cause.
            continue
        grp = by_subdir[sub]
        bindings = [p for p in grp if p["name"] == _BINDING_NAME]
        if not bindings:
            errors.append(f"subdir '{sub}': no {_BINDING_NAME} package.")
            continue

        for p in bindings:
            if not p["python"]:
                errors.append(
                    f"{p['name']}-{p['version']}-{p['build']} in '{sub}' has no "
                    f"detectable Python tag (build string should carry pyXY)."
                )
        got_pythons = sorted({p["python"] for p in bindings if p["python"]})
        missing = [py for py in expected_pythons if py not in got_pythons]
        if missing:
            errors.append(
                f"subdir '{sub}': matrix INCOMPLETE -- missing Python {missing} "
                f"(present: {got_pythons or 'none'})."
            )

    return errors


def collect_packages(root: str) -> list[dict]:
    """Read every ``.conda`` / ``.tar.bz2`` under ``root`` into package dicts."""
    import glob
    import os

    paths = sorted(
        glob.glob(os.path.join(root, "**", "*.conda"), recursive=True)
        + glob.glob(os.path.join(root, "**", "*.tar.bz2"), recursive=True)
    )
    packages = []
    for path in paths:
        index = read_index_json(path)
        packages.append(
            {
                "folder": os.path.basename(os.path.dirname(path)),
                "subdir": str(index.get("subdir", "")),
                "name": str(index.get("name", "")),
                "version": str(index.get("version", "")),
                "build": str(index.get("build", "")),
                "python": python_tag_from_index(index),
                "path": path,
            }
        )
    return packages


def _split(value: str) -> list[str]:
    return [x.strip() for x in value.split(",") if x.strip()]


def main(argv: list | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, help="Root of the consolidated conda tree.")
    parser.add_argument(
        "--required-subdirs", default="win-64,osx-64,osx-arm64,linux-64,linux-aarch64"
    )
    parser.add_argument(
        "--allowed-subdirs",
        default="win-64,win-arm64,osx-64,osx-arm64,linux-64,linux-aarch64",
    )
    parser.add_argument("--pythons", default="3.10,3.11,3.12,3.13,3.14")
    parser.add_argument("--mssql-python-version", default=None)
    parser.add_argument("--mssql-python-odbc-version", default=None)
    args = parser.parse_args(argv)

    packages = collect_packages(args.root)
    if not packages:
        print(f"ERROR: no conda packages found under {args.root}.", file=sys.stderr)
        return 1

    expected_versions = {}
    if args.mssql_python_version:
        expected_versions[_BINDING_NAME] = args.mssql_python_version
    # --mssql-python-odbc-version is accepted for back-compat but ignored: the
    # self-contained mssql-python package vendors the ODBC payload, so there is no
    # separate companion package to version.

    print(f"Discovered {len(packages)} conda package(s):")
    for p in sorted(packages, key=lambda x: (x["subdir"], x["name"], x["python"])):
        print(
            f"  {p['subdir']:<14} {p['name']:<18} {p['version']:<12} "
            f"py={p['python'] or '-':<5} build={p['build']}"
        )

    errors = validate(
        packages,
        required_subdirs=_split(args.required_subdirs),
        allowed_subdirs=_split(args.allowed_subdirs),
        expected_pythons=_split(args.pythons),
        expected_versions=expected_versions,
    )

    if errors:
        print("\nConda release readiness FAILED:", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        return 1

    print("\nOK: metadata-validated conda set is release-ready (subdirs, Python matrix, pairing).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
