"""Metadata-based conda release-readiness gate.

The release pipeline must never ship an incomplete or mis-paired conda set. The
earlier gate grouped packages by their *folder name* and counted them, so it
could not catch (a) a package whose real ``subdir`` (in ``info/index.json``)
disagrees with the folder it was staged into, nor (b) a missing Python variant
on a platform -- and after the Windows "companion built once" change the
presence-pairing relaxation let a dropped binding slip through (e.g. 3 of 5
win-64 bindings still looked paired against the single companion).

This module reads the AUTHORITATIVE ``info/index.json`` embedded in every
``.conda`` / ``.tar.bz2`` and validates:

* every package's real ``subdir`` is in the allowed set AND matches its folder
  (catches a mislabeled / mis-stamped leg);
* names are exactly ``mssql-python`` / ``mssql-python-odbc`` and their versions
  match the expected release versions (or, if none supplied, are internally
  consistent -- a single version per package);
* the full (required-subdir x Python) binding matrix is complete -- every
  required platform ships a binding for every expected Python;
* #706 pairing: a binding and its companion ship together (both-or-neither),
  never a companion-only bump; the companion may be a single Python-agnostic
  package serving many bindings (win-64), but where it is per-Python (count > 1)
  it must pair 1:1 with the bindings.

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
_COMPANION_NAME = "mssql-python-odbc"

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
    Python-agnostic package (e.g. the once-built Windows companion, build string
    ``0``) has neither and returns ``''``.
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

    # 2. Names known; versions match expected (or are internally consistent).
    seen_versions: dict = defaultdict(set)
    for p in packages:
        if p["name"] not in (_BINDING_NAME, _COMPANION_NAME):
            errors.append(f"unexpected package name '{p['name']}' ({p['version']}).")
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

    # Group by the REAL (metadata) subdir, never the folder name.
    by_subdir: dict = defaultdict(list)
    for p in packages:
        by_subdir[p["subdir"]].append(p)

    # 3. Required subdirs: present, binding matrix complete, companion paired.
    for sub in required_subdirs:
        grp = by_subdir.get(sub, [])
        if not grp:
            errors.append(f"required subdir '{sub}' is MISSING.")
            continue
        bindings = [p for p in grp if p["name"] == _BINDING_NAME]
        companions = [p for p in grp if p["name"] == _COMPANION_NAME]
        if not bindings:
            errors.append(f"subdir '{sub}': no binding ({_BINDING_NAME}) package.")
        if not companions:
            errors.append(f"subdir '{sub}': no companion ({_COMPANION_NAME}) package.")

        for p in bindings:
            if not p["python"]:
                errors.append(
                    f"binding {p['name']}-{p['version']}-{p['build']} in '{sub}' has no "
                    f"detectable Python tag (build string should carry pyXY)."
                )
        got_pythons = sorted({p["python"] for p in bindings if p["python"]})
        missing = [py for py in expected_pythons if py not in got_pythons]
        if missing:
            errors.append(
                f"subdir '{sub}': binding matrix INCOMPLETE -- missing Python {missing} "
                f"(present: {got_pythons or 'none'})."
            )

        b, c = len(bindings), len(companions)
        if c > 1 and b != c:
            errors.append(
                f"#706 pairing in '{sub}': {b} binding(s) vs {c} per-Python companion(s) "
                f"(a per-Python companion must pair 1:1)."
            )

    # 4. #706 pairing across EVERY real subdir (required or not): both-or-neither.
    for sub, grp in by_subdir.items():
        b = len([p for p in grp if p["name"] == _BINDING_NAME])
        c = len([p for p in grp if p["name"] == _COMPANION_NAME])
        if (b > 0) != (c > 0):
            errors.append(
                f"#706 pairing in '{sub}': binding={b} companion={c} "
                f"(a binding and its companion must ship together)."
            )

    # 5. Global companion-only / binding-only guard (the exact #706 mistake).
    total_b = len([p for p in packages if p["name"] == _BINDING_NAME])
    total_c = len([p for p in packages if p["name"] == _COMPANION_NAME])
    if total_c > 0 and total_b == 0:
        errors.append(f"#706 (global): {total_c} companion package(s) with NO binding.")
    if total_b > 0 and total_c == 0:
        errors.append(f"#706 (global): {total_b} binding package(s) with NO companion.")

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
    if args.mssql_python_odbc_version:
        expected_versions[_COMPANION_NAME] = args.mssql_python_odbc_version

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
