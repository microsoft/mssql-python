#!/usr/bin/env python3
"""Release-time dependency RE-SOLVE: catch channel drift before publishing.

The conda-build pipeline proves each package solves AT BUILD TIME. Channels drift (a
dependency can be yanked or repinned) between build and release, so a package that built
cleanly can become uninstallable by the time it is published. This gate stages the
downloaded consolidated conda tree as a LOCAL CHANNEL and re-runs a
``conda create --dry-run`` install of ``mssql-python=<version>`` against the LIVE upstream
channels, per subdir, at release time -- so a drifted graph blocks the publish instead of
breaking the user's ``conda install``.

Solving the REAL package (not just its extracted ``depends``) also exercises the package's
own version / ``run_constrained`` interplay -- the exact class of thing that drifts.
``--dry-run`` never links, runs post-link, or executes the target interpreter, so ALL
subdirs (incl. osx-*/linux-*/win-arm64) are re-solved from a single host via ``--platform``
(set ``CONDA_SUBDIR`` instead if your conda predates ``--platform`` on ``create``).

The conda executable is supplied by the caller (``--conda``) so this stays agnostic to how
conda is provisioned (an approved base image / internal feed -- never a raw download in the
Official publish pipeline).

Exit 0 = every package's graph still resolves; non-zero = drift (blocks publish).
"""

from __future__ import annotations

import argparse
import glob
import io
import json
import os
import re
import subprocess
import sys
import tarfile
import zipfile

_BINDING_NAME = "mssql-python"

_PY_TAG_RE = re.compile(r"py(\d)(\d{1,2})")
_PY_DEP_RE = re.compile(r"python\s+(\d+)\.(\d+)")


def _zstd_decompress(raw: bytes) -> bytes:
    """Decompress a zstandard blob, preferring the 3.14+ stdlib backend."""
    try:  # Python 3.14+
        from compression import zstd  # type: ignore

        return zstd.decompress(raw)
    except Exception:
        pass
    import zstandard  # third-party fallback

    return zstandard.ZstdDecompressor().decompress(raw)


def read_index(path: str) -> dict:
    """Return the package's ``info/index.json`` dict."""
    if path.endswith(".conda"):
        with zipfile.ZipFile(path) as zf:
            info = next(
                (n for n in zf.namelist() if n.startswith("info-") and n.endswith(".tar.zst")),
                None,
            )
            if info is None:
                raise ValueError(f"{path}: no info-*.tar.zst member (malformed .conda)")
            blob = _zstd_decompress(zf.read(info))
        with tarfile.open(fileobj=io.BytesIO(blob)) as tf:
            member = tf.extractfile("info/index.json")
            if member is None:
                raise ValueError(f"{path}: info/index.json missing")
            return json.load(member)
    if path.endswith(".tar.bz2"):
        with tarfile.open(path, "r:bz2") as tf:
            member = tf.extractfile("info/index.json")
            if member is None:
                raise ValueError(f"{path}: info/index.json missing")
            return json.load(member)
    raise ValueError(f"{path}: unrecognized conda package extension")


def python_tag_from_index(index: dict) -> str:
    """Extract the ``X.Y`` Python version a package is built for, or ``''``."""
    match = _PY_TAG_RE.search(str(index.get("build", "")))
    if match:
        return f"{match.group(1)}.{match.group(2)}"
    for dep in index.get("depends", []) or []:
        match = _PY_DEP_RE.match(str(dep))
        if match:
            return f"{match.group(1)}.{match.group(2)}"
    return ""


def channels_for(subdir: str) -> list[str]:
    """Upstream channel args, mirroring build-conda-packages.{sh,ps1}.

    win-arm64 deps (cryptography / vc14_runtime / pyodbc + python) live on Anaconda
    ``defaults``, NOT conda-forge, and legitimately split across microsoft+defaults, so it
    drops ``--strict-channel-priority``. Every other subdir uses the lean ``microsoft``
    channel ahead of conda-forge with strict priority.
    """
    if subdir == "win-arm64":
        return ["-c", "microsoft", "-c", "defaults"]
    return ["-c", "microsoft", "-c", "conda-forge", "--strict-channel-priority"]


def build_solve_cmd(
    conda: str, subdir: str, python: str, version: str, local_channel: str
) -> list[str]:
    """The ``conda create --dry-run`` command that re-solves mssql-python=<version>.

    The local channel comes FIRST (authoritative for the freshly built package); the
    upstream channels supply its dependencies. ``--platform`` targets the subdir so one host
    re-solves every platform (a ``--dry-run`` solve never runs the target interpreter).
    """
    env_name = f"_resolve_{subdir.replace('-', '_')}_{python.replace('.', '')}"
    cmd = [
        conda,
        "create",
        "--dry-run",
        "--yes",
        "-n",
        env_name,
        "--platform",
        subdir,
        "-c",
        local_channel,
    ]
    cmd += channels_for(subdir)
    cmd += ["--override-channels", f"python={python}", f"{_BINDING_NAME}={version}"]
    return cmd


def enumerate_targets(root: str) -> list[tuple[str, str]]:
    """Return sorted unique ``(subdir, python)`` for every mssql-python package under root."""
    paths = sorted(
        glob.glob(os.path.join(root, "**", "*.conda"), recursive=True)
        + glob.glob(os.path.join(root, "**", "*.tar.bz2"), recursive=True)
    )
    targets: set = set()
    for path in paths:
        index = read_index(path)
        if str(index.get("name", "")) != _BINDING_NAME:
            continue
        subdir = str(index.get("subdir", ""))
        python = python_tag_from_index(index)
        if subdir and python:
            targets.add((subdir, python))
    return sorted(targets)


def _as_file_url(path: str) -> str:
    return "file:///" + os.path.abspath(path).replace("\\", "/").lstrip("/")


def index_channel(conda: str, root: str) -> None:
    """Make ``root`` a valid conda channel (per-subdir repodata + a noarch stub).

    A conda channel must carry ``noarch/repodata.json`` even when empty, else
    ``conda create -c file://root`` fails with UnavailableInvalidChannel.
    """
    noarch = os.path.join(root, "noarch")
    os.makedirs(noarch, exist_ok=True)
    repo = os.path.join(noarch, "repodata.json")
    if not os.path.exists(repo):
        with open(repo, "w", encoding="utf-8") as fh:
            fh.write('{"info":{"subdir":"noarch"},"packages":{},"packages.conda":{}}')
    # `conda index` needs the conda-index package (the stage installs it); fall back to the
    # module entry point if the subcommand is absent.
    last = None
    for cmd in (
        [conda, "index", root],
        [conda, "run", "-n", "base", "python", "-m", "conda_index", root],
    ):
        last = subprocess.run(cmd, capture_output=True, text=True)
        if last.returncode == 0:
            return
    raise RuntimeError(
        f"failed to index the local channel at {root}:\n{last.stdout}\n{last.stderr}"
    )


def main(argv: list | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, help="Consolidated conda tree to re-solve.")
    parser.add_argument("--conda", required=True, help="Path to the conda executable.")
    parser.add_argument(
        "--mssql-python-version", required=True, help="Exact mssql-python version to solve."
    )
    args = parser.parse_args(argv)

    if not os.path.isdir(args.root):
        print(f"ERROR: --root '{args.root}' is not a directory.", file=sys.stderr)
        return 1

    targets = enumerate_targets(args.root)
    if not targets:
        print(f"ERROR: no {_BINDING_NAME} packages found under {args.root}.", file=sys.stderr)
        return 1

    print(f"Indexing local channel: {args.root}")
    index_channel(args.conda, args.root)
    local_channel = _as_file_url(args.root)

    print(
        f"Re-solving {len(targets)} (subdir x python) target(s) of "
        f"{_BINDING_NAME}={args.mssql_python_version} against live channels:"
    )
    failures: list = []
    for subdir, python in targets:
        cmd = build_solve_cmd(args.conda, subdir, python, args.mssql_python_version, local_channel)
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode == 0:
            print(f"  OK    {subdir:<14} py{python}")
        else:
            tail = "\n".join((proc.stderr or proc.stdout).strip().splitlines()[-3:])
            print(f"  DRIFT {subdir:<14} py{python} -- solve FAILED", file=sys.stderr)
            failures.append((subdir, python, tail))

    if failures:
        print("\nRelease-time re-solve FAILED (channel drift since build):", file=sys.stderr)
        for subdir, python, tail in failures:
            print(f"  - {subdir} py{python}:\n      {tail}", file=sys.stderr)
        return 1

    print(f"\nOK: every {_BINDING_NAME}={args.mssql_python_version} target still resolves.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
