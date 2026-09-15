"""Conda process execution, discovery, bootstrap and copied build environments."""

from __future__ import annotations

import hashlib
import os
import platform
import re
import shutil
import subprocess
import sys
import urllib.request
from typing import NoReturn

_MINIFORGE_VERSION = os.environ.get("MINIFORGE_VERSION", "26.3.2-3")


def _log(msg: str) -> None:
    print(msg, flush=True)


def _die(msg: str) -> NoReturn:
    print(f"ERROR: {msg}", file=sys.stderr, flush=True)
    sys.exit(1)


def run(
    cmd: list[str],
    *,
    env: dict[str, str] | None = None,
    cwd: str | None = None,
    what: str = "",
) -> None:
    """Run a command, streaming output; raise (exit 1) on a non-zero return -- the
    Assert-LastExit / `set -e` equivalent, but the caller just reads our exit code."""
    _log("+ " + " ".join(str(c) for c in cmd))
    rc = subprocess.run(cmd, env=env, cwd=cwd).returncode
    if rc != 0:
        _die(f"{what or ' '.join(str(c) for c in cmd)} (exit {rc})")


def run_ok(cmd: list[str], *, env: dict[str, str] | None = None, cwd: str | None = None) -> int:
    """Run best-effort: return the exit code instead of dying (the `|| true` equivalent)."""
    _log("+ " + " ".join(str(c) for c in cmd))
    return subprocess.run(cmd, env=env, cwd=cwd).returncode


def run_capture(cmd: list[str], *, env: dict[str, str] | None = None) -> tuple[int, str]:
    """Run and capture (rc, combined-output). Best-effort paths print the output themselves
    so a real failure is diagnosable rather than swallowed."""
    p = subprocess.run(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    return p.returncode, p.stdout


def find_or_install_conda(output_dir: str) -> str:
    on_path = shutil.which("conda")
    if on_path:
        return on_path

    # Reuse Miniforge from a prior run of THIS leg (for example, a retry on a reused agent).
    # Each condaSubdir has its own output_dir/miniforge; avoid reinstalling into an already
    # populated target directory, which would fail.
    forge = os.path.join(output_dir, "miniforge")
    reuse = _conda_exe(forge)
    if os.path.exists(reuse):
        _log(f"=== reusing existing Miniforge3 at {forge} ===")
        return reuse

    _log("=== conda not found on PATH; installing Miniforge3 ===")
    is_win = sys.platform == "win32"
    if is_win:
        installer_name = f"Miniforge3-{_MINIFORGE_VERSION}-Windows-x86_64.exe"
    else:
        osname = "MacOSX" if sys.platform == "darwin" else "Linux"
        arch = platform.machine()
        arch = {"aarch64": "aarch64", "arm64": "arm64", "x86_64": "x86_64"}.get(arch, arch)
        installer_name = f"Miniforge3-{_MINIFORGE_VERSION}-{osname}-{arch}.sh"

    installer = os.path.join(output_dir, installer_name)
    url = (
        "https://github.com/conda-forge/miniforge/releases/download/"
        f"{_MINIFORGE_VERSION}/{installer_name}"
    )
    # Pin Miniforge to a specific release (never `latest`, which floats) and verify its SHA256
    # BEFORE executing. The expected hash is NOT hard-coded: prefer an explicit
    # MINIFORGE_SHA256 pipeline variable (out-of-source, strongest), else the release's own
    # published <installer>.sha256 sidecar. The installer is never executed unverified.
    _log(f"Downloading pinned Miniforge {_MINIFORGE_VERSION}: {url}")
    urllib.request.urlretrieve(url, installer)  # noqa: S310 - pinned https conda-forge release
    expected = os.environ.get("MINIFORGE_SHA256")
    if not expected:
        sidecar = installer + ".sha256"
        urllib.request.urlretrieve(url + ".sha256", sidecar)  # noqa: S310 - same pinned release
        with open(sidecar, "r", encoding="utf-8") as fh:
            m = re.search(r"[0-9a-fA-F]{64}", fh.read())
        expected = m.group(0) if m else None
    if not expected:
        _die(f"could not determine the expected SHA256 for {installer_name}")
    actual = _sha256(installer)
    if actual.lower() != expected.lower():
        _die(f"Miniforge installer SHA256 mismatch: expected '{expected}', got '{actual}'")
    _log(f"Miniforge installer SHA256 verified ({actual}).")

    if is_win:
        # NSIS silent install; /D (target dir) MUST be last and unquoted.
        run(
            [
                installer,
                "/S",
                "/InstallationType=JustMe",
                "/AddToPath=0",
                f"/D={forge}",
            ],
            what="Miniforge NSIS install",
        )
    else:
        # -b batch, -u update/reuse an existing target dir (in case a prior run left a partial).
        run(["bash", installer, "-b", "-u", "-p", forge], what="Miniforge install")

    conda = _conda_exe(forge)
    if not os.path.exists(conda):
        _die(f"conda not available at '{conda}' after install attempt")
    return conda


def _conda_exe(forge: str) -> str:
    return (
        os.path.join(forge, "Scripts", "conda.exe")
        if sys.platform == "win32"
        else os.path.join(forge, "bin", "conda")
    )


def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def create_builder_env(conda: str) -> str:
    """conda-build<26: 26.7.0 crashes in the local packaging phase; the 25.x series is stable.
    A DEDICATED env (not `install -n base`) because a hosted runner's base may be pinned to a
    python no conda-build<26 supports (e.g. 3.14), making a base install UNSOLVABLE; a fresh
    env lets conda pick a supported python. conda-forge only (--override-channels) avoids the
    defaults ToS; zstandard rides along so the RUNPATH audit reads .conda payloads here."""
    env_name = "conda_builder"
    _log(f"=== creating dedicated conda-build env ({env_name}: conda-build<26) ===")
    # Idempotent: a reused agent may already have this env; a pre-existing env makes
    # `conda create` fail. Remove first (best-effort).
    run_ok([conda, "env", "remove", "-y", "-n", env_name])
    run(
        [
            conda,
            "create",
            "-y",
            "-n",
            env_name,
            "-c",
            "conda-forge",
            "--override-channels",
            "conda-build<26",
            "zstandard",
        ],
        what=f"conda create {env_name}",
    )
    return env_name


def build_env(
    mssql_ver: str, odbc_ver: str, links: str, cross_target_subdir: str
) -> dict[str, str]:
    """The environment consumed by the recipe (jinja + build.sh/bld.bat) and by conda-build."""
    env = dict(os.environ)
    env.pop("CONDA_PLUGINS_AUTO_ACCEPT_TOS", None)
    env["WHEELS_DIR"] = links
    env["MSSQL_PYTHON_VERSION"] = mssql_ver
    env["MSSQL_ODBC_VERSION"] = odbc_ver
    if cross_target_subdir:
        # conda-build AND the verify `conda create` honor CONDA_SUBDIR -> the packages are
        # stamped for the target subdir and the import check runs the target Python where the
        # host can execute it (natively / Rosetta 2 / QEMU binfmt).
        env["CONDA_SUBDIR"] = cross_target_subdir
        _log(f"Cross-targeting conda subdir: CONDA_SUBDIR={cross_target_subdir}")
        if cross_target_subdir.endswith("aarch64") and os.path.isdir("/usr/aarch64-linux-gnu"):
            # Emulated aarch64 verify runs under qemu-user; point it at the aarch64 glibc loader.
            env.setdefault("QEMU_LD_PREFIX", "/usr/aarch64-linux-gnu")
            _log(f"Set QEMU_LD_PREFIX={env['QEMU_LD_PREFIX']} for emulated aarch64 verify")
    else:
        env.pop("CONDA_SUBDIR", None)
    return env
