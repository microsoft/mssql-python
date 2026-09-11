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
* the (required-subdir x Python) matrix is complete -- every required platform
  ships a package for every expected Python, honoring any per-subdir Python
  override (e.g. win-arm64 ships only 3.12-3.14).

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
_PY_DEP_RE = re.compile(r"python\s+(?:==?)?(\d+)\.(\d+)(?:\.(?:\d+|\*))?(?:\s+\S+)?")
_PY_RANGE_RE = re.compile(r"python\s+>=\s*(\d+)\.(\d+)(?:\.\d+)?\s*,\s*<\s*(\d+)\.(\d+)(?:\.0a0)?")

# Some subdirs legitimately ship a REDUCED Python matrix. win-arm64's conda
# dependencies (cryptography, pyodbc) are published on Anaconda `defaults` only
# for Python 3.12+, so 3.10/3.11 cannot be built there -- expect 3.12-3.14 only.
_DEFAULT_SUBDIR_PYTHONS = "win-arm64=3.12,3.13,3.14"


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


def _zstd_decompress(raw: bytes) -> bytes:
    """Decompress a zstandard blob, preferring the 3.14+ stdlib backend."""
    try:  # Python 3.14+
        from compression import zstd  # type: ignore
    except ImportError:
        try:
            import zstandard  # third-party fallback
        except ImportError as exc:
            raise RuntimeError(
                "Unable to import 'zstandard': reading .conda (.tar.zst) metadata requires "
                "Python 3.14+ with compression.zstd or a working 'zstandard' install "
                "(pip install zstandard)."
            ) from exc
        return zstandard.ZstdDecompressor().decompress(raw)
    return zstd.decompress(raw)


def read_index_json(path: str) -> dict:
    """Return the parsed ``info/index.json`` from a ``.conda`` / ``.tar.bz2``."""
    if path.endswith(".conda"):
        with zipfile.ZipFile(path) as zf:
            info_names = [
                name
                for name in zf.namelist()
                if name.startswith("info-") and name.endswith(".tar.zst")
            ]
            if len(info_names) != 1:
                raise ValueError(
                    f"{path}: expected exactly one info-*.tar.zst member; "
                    f"found {len(info_names)}"
                )
            info_blob = zf.read(info_names[0])
        with tarfile.open(fileobj=io.BytesIO(_zstd_decompress(info_blob))) as tf:
            index_members = [
                member for member in tf.getmembers() if member.name == "info/index.json"
            ]
            if len(index_members) != 1 or not index_members[0].isfile():
                raise ValueError(f"{path}: expected exactly one regular info/index.json member")
            member = tf.extractfile(index_members[0])
            if member is None:
                raise ValueError(f"{path}: info/index.json is unreadable")
            return _require_index_object(json.load(member), path)
    if path.endswith(".tar.bz2"):
        with tarfile.open(path, "r:bz2") as tf:
            index_members = [
                member for member in tf.getmembers() if member.name == "info/index.json"
            ]
            if len(index_members) != 1 or not index_members[0].isfile():
                raise ValueError(f"{path}: expected exactly one regular info/index.json member")
            member = tf.extractfile(index_members[0])
            if member is None:
                raise ValueError(f"{path}: info/index.json is unreadable")
            return _require_index_object(json.load(member), path)
    raise ValueError(f"{path}: unrecognized conda package extension")


def python_tag_from_index(index: dict) -> str:
    """Extract the ``X.Y`` Python version a package is built for, or ``''``.

    Uses the build string's ``pyXY`` token first (authoritative for conda-build
    Python packages), then falls back to a pinned run dependency or the bounded
    ``python >=X.Y,<X.(Y+1).0a0`` form emitted by conda-build. Unbounded or
    cross-minor ranges do not identify one Python variant and return ``''``.
    """
    match = _PY_TAG_RE.search(str(index.get("build", "")))
    if match:
        return f"{match.group(1)}.{match.group(2)}"
    for dep in index.get("depends", []) or []:
        match = _PY_DEP_RE.fullmatch(str(dep).strip())
        if match:
            return f"{match.group(1)}.{match.group(2)}"
        match = _PY_RANGE_RE.fullmatch(str(dep).strip())
        if match:
            major, minor, upper_major, upper_minor = map(int, match.groups())
            if (upper_major, upper_minor) == (major, minor + 1):
                return f"{major}.{minor}"
    return ""


def validate(
    packages: list[dict],
    required_subdirs: list[str],
    allowed_subdirs: list[str],
    expected_pythons: list[str],
    expected_versions: dict | None = None,
    subdir_pythons: dict | None = None,
) -> list[str]:
    """Return a list of human-readable violation strings (empty == release-ready).

    ``packages`` is a list of dicts with keys: ``folder`` (staged subdir folder),
    ``subdir`` (real info/index.json subdir), ``name``, ``version``, ``build``,
    ``python`` (``X.Y`` or ``''``).

    ``subdir_pythons`` maps a subdir to the Python versions expected FOR THAT
    subdir, overriding ``expected_pythons`` (e.g. win-arm64 ships only 3.12-3.14).
    """
    errors: list[str] = []
    expected_versions = expected_versions or {}
    subdir_pythons = subdir_pythons or {}

    for policy_name, values in (
        ("required_subdirs", required_subdirs),
        ("allowed_subdirs", allowed_subdirs),
        ("expected_pythons", expected_pythons),
    ):
        if not values:
            errors.append(f"release policy '{policy_name}' must not be empty.")
        elif len(values) != len(set(values)):
            errors.append(f"release policy '{policy_name}' contains duplicates: {values}.")
    missing_allowed = sorted(set(required_subdirs) - set(allowed_subdirs))
    if missing_allowed:
        errors.append(f"required subdirs are absent from allowed_subdirs: {missing_allowed}.")
    for subdir, versions in sorted(subdir_pythons.items()):
        if not versions:
            errors.append(f"subdir Python override for '{subdir}' must not be empty.")

    # 1. Authoritative subdir must be allowed AND match the folder it was staged in.
    for p in packages:
        ident = f"{p['name']}-{p['version']}-{p['build']}"
        if not p["version"]:
            errors.append(f"{ident}: package version is missing.")
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
        sub_expected = subdir_pythons.get(sub, expected_pythons)
        got_pythons = sorted({p["python"] for p in bindings if p["python"]})
        missing = [py for py in sub_expected if py not in got_pythons]
        if missing:
            errors.append(
                f"subdir '{sub}': matrix INCOMPLETE -- missing Python {missing} "
                f"(present: {got_pythons or 'none'})."
            )
        # Reject EXTRA pythons too (got == expected, not just expected subset of got): an
        # unsupported build (e.g. a win-arm64 3.10 that slipped in) must never publish.
        extra = [py for py in got_pythons if py not in sub_expected]
        if extra:
            errors.append(
                f"subdir '{sub}': matrix has UNSUPPORTED Python {extra} "
                f"(expected exactly {sub_expected})."
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
        name = _required_index_string(index, "name", path)
        version = _required_index_string(index, "version", path)
        subdir = _required_index_string(index, "subdir", path)
        build = _required_index_string(index, "build", path)
        packages.append(
            {
                "folder": os.path.basename(os.path.dirname(path)),
                "subdir": subdir,
                "name": name,
                "version": version,
                "build": build,
                "python": python_tag_from_index(index),
                "path": path,
            }
        )
    return packages


def _split(value: str) -> list[str]:
    return [x.strip() for x in value.split(",") if x.strip()]


def _parse_subdir_pythons(value: str) -> dict:
    """Parse ``subdir=py,py;subdir2=py,py`` into ``{subdir: [py, ...]}``."""
    result: dict = {}
    for chunk in value.split(";"):
        chunk = chunk.strip()
        if not chunk:
            continue
        subdir, _, pys = chunk.partition("=")
        if not subdir.strip() or not pys.strip():
            raise ValueError(
                f"invalid subdir Python override '{chunk}'; expected subdir=X.Y[,X.Y]."
            )
        subdir = subdir.strip()
        if subdir in result:
            raise ValueError(f"invalid subdir Python override: duplicate subdir '{subdir}'.")
        result[subdir] = _split(pys)
    return result


def main(argv: list | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, help="Root of the consolidated conda tree.")
    parser.add_argument(
        "--required-subdirs",
        default="win-64,win-arm64,osx-64,osx-arm64,linux-64,linux-aarch64",
    )
    parser.add_argument(
        "--allowed-subdirs",
        default="win-64,win-arm64,osx-64,osx-arm64,linux-64,linux-aarch64",
    )
    parser.add_argument("--pythons", default="3.10,3.11,3.12,3.13,3.14")
    parser.add_argument(
        "--subdir-pythons",
        default=_DEFAULT_SUBDIR_PYTHONS,
        help="Per-subdir Python overrides, e.g. 'win-arm64=3.12,3.13,3.14'.",
    )
    parser.add_argument("--mssql-python-version", default=None)
    args = parser.parse_args(argv)

    packages = collect_packages(args.root)
    if not packages:
        print(f"ERROR: no conda packages found under {args.root}.", file=sys.stderr)
        return 1

    expected_versions = {}
    if args.mssql_python_version:
        expected_versions[_BINDING_NAME] = args.mssql_python_version
    subdir_pythons = _parse_subdir_pythons(args.subdir_pythons)

    print(f"Discovered {len(packages)} conda package(s):")
    for p in sorted(packages, key=lambda x: (x["subdir"], x["name"], x["python"])):
        print(
            f"  {p['subdir']:<14} {p['name']:<18} {p['version']:<12} "
            f"py={p['python'] or '-':<5} build={p['build']}"
        )
    if subdir_pythons:
        print(f"Per-subdir Python overrides: {subdir_pythons}")

    errors = validate(
        packages,
        required_subdirs=_split(args.required_subdirs),
        allowed_subdirs=_split(args.allowed_subdirs),
        expected_pythons=_split(args.pythons),
        expected_versions=expected_versions,
        subdir_pythons=subdir_pythons,
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
