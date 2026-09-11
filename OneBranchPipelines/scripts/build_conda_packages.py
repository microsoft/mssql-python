#!/usr/bin/env python3
"""One cross-platform orchestrator for the conda build+validate leg.

Replaces build-conda-packages.ps1 + build-conda-packages.sh (the same 7-step pipeline
written twice, which had already drifted). conda is Python and every agent has a bootstrap
interpreter, so ONE orchestrator runs on every leg; the platform differences (the Miniforge
installer, the win-arm64 channel profile, the Linux-only reachability gate) are
a handful of branches, not a second 360-line script. Running as a NORMAL process also means
the caller reads the exit code directly -- so the PowerShell ErrorActionPreference flips, the
`2>$null` swallows, and the `cmd /c "exit 0"` reset all disappear.

Pipeline: gather this leg's wheels into a find-links dir -> locate/install Miniforge ->
create a dedicated conda-build env -> build the self-contained mssql-python package (which
VENDORS the ODBC Driver 18 payload) per Python version -> masking-immune platform binary
audit -> solve a fresh env from the freshly built local channel and import + driver-load +
(opt-in) reachability gate -> stage the packages onto the leg artifact.

Cross-builds (CONDA_SUBDIR): linux-aarch64 executes under QEMU binfmt; osx-arm64 and
win-arm64 cannot execute the target Python on their x64 build hosts, so static platform
audits enforce architecture and the runtime import auto-skips. osx-64 runs natively on
the Intel macOS agent.
"""

from __future__ import annotations

import argparse
import glob
import hashlib
import os
import platform
import re
import shutil
import subprocess
import sys
import urllib.request
from pathlib import Path
from typing import NoReturn

_MINIFORGE_VERSION = os.environ.get("MINIFORGE_VERSION", "26.3.2-3")
_KNOWN_SUBDIRS = ("win-64", "win-arm64", "osx-64", "osx-arm64", "linux-64", "linux-aarch64")


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


# ---------------------------------------------------------------------------
# 0. Gather THIS leg's wheels into one find-links dir + derive the versions.
# ---------------------------------------------------------------------------
def gather_wheels(
    mssql_dir: str, mssql_glob: str, odbc_dir: str, odbc_filter: str, links: str
) -> tuple[str, str]:
    """Copy this platform's mssql-python wheel(s) (excluding the odbc package, whose filename
    also starts with mssql_python) + this platform's odbc wheel into ONE find-links dir. The
    dir is CLEARED first so a stale artifact from a reused workdir can never be validated."""
    if os.path.isdir(links):
        shutil.rmtree(links)
    os.makedirs(links, exist_ok=True)

    mssql = sorted(
        w
        for w in glob.glob(os.path.join(mssql_dir, mssql_glob))
        if not os.path.basename(w).startswith("mssql_python_odbc-")
    )
    if not mssql:
        _die(f"no mssql-python wheel matching '{mssql_glob}' in {mssql_dir}")

    mssql_versions_by_wheel = {
        os.path.basename(w): _wheel_version(os.path.basename(w), "mssql_python") for w in mssql
    }
    malformed_mssql = sorted(
        name for name, version in mssql_versions_by_wheel.items() if version is None
    )
    if malformed_mssql:
        _die(f"could not derive a version from mssql-python wheels: {malformed_mssql}")
    mssql_versions = {
        version for version in mssql_versions_by_wheel.values() if version is not None
    }
    if len(mssql_versions) != 1:
        _die("mssql-python wheels contain inconsistent versions: " f"{mssql_versions_by_wheel}")
    mssql_ver = next(iter(mssql_versions))

    for w in mssql:
        shutil.copy2(w, links)

    odbc_matches = sorted(glob.glob(os.path.join(odbc_dir, "**", odbc_filter), recursive=True))
    if not odbc_matches:
        _die(f"no wheel matching '{odbc_filter}' in {odbc_dir}")
    if len(odbc_matches) != 1:
        # An arch-ambiguous filter (e.g. one that matches BOTH x86_64 and arm64 odbc wheels)
        # would silently pick [0] and could vendor the WRONG-arch driver into this leg. Demand
        # an exact single match so the filter is tightened to this leg's arch instead.
        _die(
            f"odbc-wheel-filter '{odbc_filter}' matched {len(odbc_matches)} wheels in {odbc_dir} "
            f"(expected exactly 1): {[os.path.basename(m) for m in odbc_matches]}"
        )
    odbc = odbc_matches[0]
    shutil.copy2(odbc, links)

    _log("find-links wheels:")
    for f in sorted(os.listdir(links)):
        _log(f"  - {f}")

    # Derive versions from the wheel FILENAMES (single source of truth: the ESRP-signed
    # wheels), so the conda package version can NEVER drift from the wheel.
    odbc_ver = _wheel_version(os.path.basename(odbc), "mssql_python_odbc")
    if not odbc_ver:
        _die(f"could not derive a version from ODBC wheel: {os.path.basename(odbc)}")
    _log(f"Derived versions -> mssql-python={mssql_ver}  mssql-python-odbc={odbc_ver}")
    return mssql_ver, odbc_ver


def _wheel_version(name: str, dist: str) -> str | None:
    m = re.match(rf"^{re.escape(dist)}-([^-]+)-", name)
    return m.group(1) if m else None


# ---------------------------------------------------------------------------
# 1. Locate conda, or install a pinned + SHA256-verified Miniforge3 for THIS platform.
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# 2. A dedicated conda-build env (pinned conda-build<26 + zstandard for the audit).
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# 3. Which Python versions to build (auto-detect from the mssql-python wheels, or explicit).
# ---------------------------------------------------------------------------
def detect_pythons(links: str, python_versions: str) -> list[str]:
    if python_versions.strip():
        pyvers = [v.strip() for v in python_versions.split(",") if v.strip()]
    else:
        pyvers = sorted(
            {
                f"3.{m.group(1)}"
                for w in glob.glob(os.path.join(links, "mssql_python-*.whl"))
                if "mssql_python_odbc" not in os.path.basename(w)
                for m in [re.search(r"-cp3(\d+)-", os.path.basename(w))]
                if m
            }
        )
    if not pyvers:
        _die(f"no mssql-python wheels in '{links}' to determine Python versions")
    _log(f"Building conda packages for Python versions: {', '.join(pyvers)}")
    return pyvers


def build_env(
    mssql_ver: str, odbc_ver: str, links: str, cross_target_subdir: str
) -> dict[str, str]:
    """The environment consumed by the recipe (jinja + build.sh/bld.bat) and by conda-build."""
    env = dict(os.environ)
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


# ---------------------------------------------------------------------------
# 4. Build + audit.
# ---------------------------------------------------------------------------
def build_packages(
    conda: str,
    builder: str,
    recipe_root: str,
    pyvers: list[str],
    bld: str,
    target_subdir: str,
    env: dict[str, str],
) -> None:
    recipe = os.path.join(recipe_root, "mssql-python")
    croot = os.path.join(os.path.dirname(os.path.abspath(bld)), "croot")
    if os.path.isdir(croot):
        shutil.rmtree(croot)
    channels = ["microsoft", "conda-forge"]
    if target_subdir == "win-arm64":
        channels.insert(0, "defaults")
    for py in pyvers:
        _log(f"=== [py {py}] build mssql-python (self-contained: vendors the ODBC payload) ===")
        cmd = [
            conda,
            "run",
            "-n",
            builder,
            "conda-build",
            recipe,
            "--python",
            py,
            "--no-test",
            "--no-anaconda-upload",
            "--output-folder",
            bld,
            "--croot",
            croot,
        ]
        for channel in channels:
            cmd += ["-c", channel]
        cmd.append("--override-channels")
        run(cmd, env=env, what=f"conda-build mssql-python (py {py})")

    # A local channel is only valid if it ALSO carries noarch/repodata.json (even empty) --
    # conda-build wrote it only for the built subdir. Create it directly (miniforge has no
    # `conda index` -- it moved to the standalone conda-index package).
    noarch = os.path.join(bld, "noarch")
    os.makedirs(noarch, exist_ok=True)
    repodata = os.path.join(noarch, "repodata.json")
    if not os.path.exists(repodata):
        with open(repodata, "w", encoding="ascii") as fh:
            fh.write('{"info":{"subdir":"noarch"},"packages":{},"packages.conda":{}}')


def audit_packages(
    conda: str,
    builder: str,
    recipe_root: str,
    bld: str,
    target_subdir: str,
    env: dict[str, str],
) -> None:
    eng = os.path.join(os.path.dirname(os.path.abspath(recipe_root)), "eng", "scripts")
    audit = os.path.join(eng, "audit_bundled_binaries.py")
    if not os.path.isfile(audit):
        _die(f"RUNPATH audit script not found at {audit}")
    _log("=== RUNPATH self-containment audit (eng/scripts/audit_bundled_binaries.py) ===")
    run(
        [conda, "run", "-n", builder, "python", audit, "--root", bld],
        env=env,
        what="RUNPATH self-containment audit",
    )
    # Both Windows packages must retain the core; cross builds also rely on static architecture.
    if target_subdir in ("win-64", "win-arm64"):
        pe = os.path.join(eng, "assert_pe_machine.py")
        if not os.path.isfile(pe):
            _die(f"PE machine-type assert script not found at {pe}")
        _log(f"=== {target_subdir} PE machine-type and required native-component assert ===")
        run(
            [conda, "run", "-n", builder, "python", pe, "--root", bld, "--subdir", target_subdir],
            env=env,
            what=f"{target_subdir} PE machine-type assert",
        )
    # osx legs: verify the universal binding contains the target slice and each thin vendored
    # driver dylib matches its architecture-specific directory. osx-arm64 is cross-built on the
    # Intel agent, so this static pass replaces the runtime import as its architecture check.
    if target_subdir in ("osx-64", "osx-arm64"):
        macho = os.path.join(eng, "assert_macho_arch.py")
        if not os.path.isfile(macho):
            _die(f"Mach-O arch assert script not found at {macho}")
        _log(f"=== {target_subdir} Mach-O assert (binding target slice + driver-tree arches) ===")
        run(
            [
                conda,
                "run",
                "-n",
                builder,
                "python",
                macho,
                "--root",
                bld,
                "--subdir",
                target_subdir,
            ],
            env=env,
            what=f"{target_subdir} Mach-O arch-slice assert",
        )


# ---------------------------------------------------------------------------
# 5. Verify: solve a fresh env from the freshly built local channel and import + probe.
# ---------------------------------------------------------------------------
def make_verify_channel(output_dir: str, bld: str) -> str:
    """conda's channel-URL parser STRIPS any path component equal to a known subdir; the
    pipeline isolates each leg under a subdir-named dir, so copy the built channel (+ the
    noarch stub) into a token-FREE per-leg dir conda parses verbatim."""
    leg = os.path.basename(os.path.normpath(output_dir))
    safe = re.sub(r"[^A-Za-z0-9]", "_", leg)
    chan = os.path.join(os.path.dirname(os.path.normpath(output_dir)), f"verifychan_{safe}")
    if os.path.isdir(chan):
        shutil.rmtree(chan)
    shutil.copytree(bld, chan)

    # Prefer an explicit file:// URL for maximum conda compatibility (especially on Windows,
    # where drive-letter paths can be parsed as URL schemes). as_uri() also percent-encodes
    # characters such as spaces and '#', which string concatenation would leave ambiguous.
    chan_url = Path(chan).resolve().as_uri()
    _log(f"verify channel (token-free alias of {bld}): {chan_url}")
    return chan_url


def _is_emulated_cross(target_subdir: str, cross_build: bool) -> bool:
    host = platform.machine().lower()
    if cross_build and target_subdir == "linux-aarch64" and host not in ("aarch64", "arm64"):
        _log(
            f"NOTE: emulated CROSS leg (CONDA_SUBDIR={target_subdir} on {host}); runtime driver "
            f"probes are best-effort under QEMU binfmt, build/audit/import remain blocking."
        )
        return True
    return False


def _import_probe(mod_name: str, ok_label: str) -> str:
    """A `python -c` body that imports mod_name and FAIL-CLOSED asserts it loaded from under
    sys.prefix (the conda env's own site-packages). os.chdir closes CWD shadowing; this also
    catches a stray PYTHONPATH/.pth that could still load the repo source -- proving the
    INSTALLED package, not the checkout. Uses abspath (NOT realpath) so conda's softlink install
    mode -- where the site-packages entry symlinks into the pkgs/ cache OUTSIDE the prefix -- is
    not false-failed: the import PATH stays under the prefix regardless of hard/soft link; only
    the symlink TARGET would not. Then prints ok_label + the installed module path."""
    return (
        f"import os,sys,{mod_name} as m;"
        "f=os.path.normcase(os.path.abspath(m.__file__));"
        "pref=os.path.normcase(os.path.abspath(sys.prefix));"
        f"assert f.startswith(pref+os.sep),{mod_name!r}+' loaded from '+m.__file__+"
        "', not under the conda env '+sys.prefix+' (stray PYTHONPATH/.pth?)';"
        f"print({ok_label!r},m.__file__)"
    )


def _core_probe() -> str:
    return (
        _import_probe("mssql_py_core", "CORE_PACKAGE_OK") + ";import importlib.machinery;"
        "exts=[v for k,v in list(sys.modules.items()) "
        "if (k=='mssql_py_core' or k.startswith('mssql_py_core.')) "
        "and isinstance(getattr(v,'__loader__',None),importlib.machinery.ExtensionFileLoader)];"
        "assert exts,'mssql_py_core did not load its required native extension';"
        "assert all(os.path.normcase(os.path.abspath(v.__file__)).startswith(pref+os.sep) "
        "for v in exts),'core native extension loaded outside installed prefix';"
        "print('CORE_NATIVE_OK',*[v.__file__ for v in exts])"
    )


def verify(
    conda: str,
    chan: str,
    recipe_root: str,
    pyvers: list[str],
    mssql_ver: str,
    target_subdir: str,
    cross_build: bool,
    env: dict[str, str],
    workdir: str,
) -> None:
    """Run the whole verify phase from a NEUTRAL cwd (the per-leg build dir) so a
    `python -c "import mssql_python"` binds the conda-INSTALLED package, not the repo source
    tree that shadows it when the agent's cwd is the checkout root (for `python -c`, sys.path[0]
    is '' = the cwd). This is the Python equivalent of the `cd` the two deleted shell scripts did
    before their verify imports; os.chdir (not a per-call cwd=) so EVERY current and future verify
    subprocess -- including _reachability_gate's -- inherits it, closing the shadow class."""
    old_cwd = os.getcwd()
    os.chdir(workdir)
    try:
        _verify_impl(conda, chan, recipe_root, pyvers, mssql_ver, target_subdir, cross_build, env)
    finally:
        os.chdir(old_cwd)


def _verify_impl(
    conda: str,
    chan: str,
    recipe_root: str,
    pyvers: list[str],
    mssql_ver: str,
    target_subdir: str,
    cross_build: bool,
    env: dict[str, str],
) -> None:
    emulated = _is_emulated_cross(target_subdir, cross_build)
    is_win = sys.platform == "win32"
    for py in pyvers:
        sub = (target_subdir or "native").replace("-", "_")
        name = f"verify_{sub}_{py.replace('.', '')}"
        run_ok([conda, "env", "remove", "-y", "-n", name])
        _log(f"=== [py {py}] create verify env from local channel ===")

        cross_best_effort = cross_build and target_subdir in ("win-arm64", "osx-arm64")
        if target_subdir == "win-arm64":
            # BLOCKING solvability gate: --dry-run resolves the FULL win-arm64 graph on x64
            # (no link / post-link / arm64 exec) -- a pure "installable?" check. win-arm64 deps
            # span microsoft (noarch azure-identity/msal) + Anaconda defaults, so no
            # --strict-channel-priority. Pin the freshly built version so no channel can shadow it.
            run(
                [
                    conda,
                    "create",
                    "--dry-run",
                    "-n",
                    name,
                    "-c",
                    chan,
                    "-c",
                    "microsoft",
                    "-c",
                    "defaults",
                    "--override-channels",
                    f"python={py}",
                    f"mssql-python={mssql_ver}",
                ],
                env=env,
                what=f"win-arm64 --dry-run solve (py {py})",
            )
            run(
                [
                    conda,
                    "create",
                    "-y",
                    "-n",
                    name,
                    "-c",
                    chan,
                    "-c",
                    "microsoft",
                    "-c",
                    "defaults",
                    "--override-channels",
                    f"python={py}",
                    f"mssql-python={mssql_ver}",
                ],
                env=env,
                what=f"win-arm64 conda create verify env (py {py})",
            )
        else:
            run(
                [
                    conda,
                    "create",
                    "-y",
                    "-n",
                    name,
                    "-c",
                    chan,
                    "-c",
                    "microsoft",
                    "-c",
                    "conda-forge",
                    "--strict-channel-priority",
                    "--override-channels",
                    f"python={py}",
                    f"mssql-python={mssql_ver}",
                ],
                env=env,
                what=f"conda create verify env (py {py})",
            )

        # Can the freshly built package's Python EXECUTE on this host?
        rc, out = run_capture([conda, "run", "-n", name, "python", "-c", "import sys"], env=env)
        if rc != 0:
            if cross_best_effort:
                _log(
                    f"=== [py {py}] {target_subdir} cross: target Python not executable on this "
                    f"host; deps SOLVED (blocking), skipping runtime import (arch enforced by "
                    f"the platform's static binary audit). ==="
                )
                _log(out)
                continue
            _die(
                f"[py {py}] target Python for CONDA_SUBDIR={target_subdir or 'native'} is not "
                f"executable on {sys.platform}/{platform.machine()}, and this is NOT an "
                f"arm64 cross-build. Refusing to silently skip validation. Output: {out}"
            )

        # A separate process prevents API/driver preloads from masking core load failures.
        run(
            [conda, "run", "-n", name, "python", "-c", _core_probe()],
            env=env,
            what=f"independent required mssql_py_core load (py {py})",
        )
        _log(f"=== [py {py}] import mssql_python + prove the vendored ODBC payload is present ===")
        run(
            [
                conda,
                "run",
                "-n",
                name,
                "python",
                "-c",
                _import_probe("mssql_python", "BINDING_OK"),
            ],
            env=env,
            what=f"import mssql_python (py {py})",
        )
        run(
            [
                conda,
                "run",
                "-n",
                name,
                "python",
                "-c",
                _import_probe("mssql_python_odbc", "ODBC_PAYLOAD_OK"),
            ],
            env=env,
            what=f"import mssql_python_odbc (py {py})",
        )

        _log(f"=== [py {py}] DB-less driver-load proof (real ODBC driver must load) ===")
        probe = os.path.join(recipe_root, "driver_load_probe.py")
        if emulated:
            if run_ok([conda, "run", "-n", name, "python", probe], env=env) != 0:
                _log(
                    "SKIP (emulated cross under QEMU binfmt): qemu-user cannot initialize the "
                    "native ODBC environment; best-effort on the emulated leg (static RUNPATH "
                    "audit + native + full-arch-emulation legs validate the driver)."
                )
        else:
            run(
                [conda, "run", "-n", name, "python", probe],
                env=env,
                what=f"driver-load proof (py {py})",
            )

        if not is_win:
            _reachability_gate(conda, name, py, emulated, env)

        _log(f"=== [py {py}] confirm resolved dependencies ===")
        rc, out = run_capture([conda, "list", "-n", name], env=env)
        if rc != 0:
            _die(f"[py {py}] failed to list resolved dependencies. Output: {out}")
        for line in out.splitlines():
            if re.search(r"azure-identity|mssql-python|openssl|krb5", line):
                _log(line)


def _reachability_gate(conda: str, name: str, py: str, emulated: bool, env: dict[str, str]) -> None:
    """Linux, opt-in (CONDA_ASSERT_PREFIX_REACHABLE=1): prove the vendored driver binds the
    env's OWN $CONDA_PREFIX/lib krb5/gssapi/libltdl via the $ORIGIN climb, not a system copy."""
    if env.get("CONDA_ASSERT_PREFIX_REACHABLE") != "1" or sys.platform != "linux":
        return
    if emulated:
        _log(
            f"=== [py {py}] reachability gate SKIPPED on the emulated cross leg (qemu-user "
            f"cannot reliably run the aarch64 driver's ldd/env init); the static RUNPATH audit "
            f"is the authoritative $ORIGIN-climb guard. ==="
        )
        return
    _log(
        f"=== [py {py}] minimal-base ldd reachability gate (driver MUST bind CONDA_PREFIX/lib) ==="
    )
    rc, prefix_out = run_capture(
        [
            conda,
            "run",
            "-n",
            name,
            "python",
            "-c",
            "import os,sys; print(os.environ.get('CONDA_PREFIX') or sys.prefix)",
        ],
        env=env,
    )
    if rc != 0:
        _die(
            f"[py {py}] failed to read CONDA_PREFIX/sys.prefix for reachability gate. "
            f"Output: {prefix_out}"
        )
    prefix = prefix_out.strip().splitlines()[-1] if prefix_out.strip() else ""
    if not prefix:
        _die(f"[py {py}] reachability gate returned an empty CONDA_PREFIX/sys.prefix.")
    # Select the SAME driver variant the loader binds (GetDriverPathCpp probes /etc/*-release);
    # a blind glob would grab alphabetically-first 'alpine' (musl) and falsely fail on libltdl.
    sel = (
        "import mssql_python,glob,os,platform;"
        "b=os.path.dirname(mssql_python.__file__);"
        "d=('alpine' if os.path.exists('/etc/alpine-release') else 'rhel' if "
        "(os.path.exists('/etc/redhat-release') or os.path.exists('/etc/centos-release')) else "
        "'suse' if (os.path.exists('/etc/SuSE-release') or os.path.exists('/etc/SUSE-brand')) "
        "else 'debian_ubuntu');"
        "a=('arm64' if platform.machine() in ('aarch64','arm64') else 'x86_64');"
        "m=glob.glob(os.path.join(b,'..','mssql_python_odbc','libs','linux',d,a,'lib',"
        "'libmsodbcsql*'));print(m[0] if m else '')"
    )
    rc, drv_out = run_capture([conda, "run", "-n", name, "python", "-c", sel], env=env)
    if rc != 0:
        _die(f"[py {py}] failed to locate driver path in verify env. Output: {drv_out}")
    drv = drv_out.strip().splitlines()[-1] if drv_out.strip() else ""
    if not drv:
        _die(f"[py {py}] no libmsodbcsql driver found in the verify env; cannot prove reachability")
    inst = os.path.join(os.path.dirname(drv), "libodbcinst.so.2")

    ldd_all = []
    for lib in (drv, inst):
        _log(f"--- ldd {os.path.basename(lib)} ---")
        # Clear inherited LD_LIBRARY_PATH so resolution proves the RUNPATH $ORIGIN climb ALONE
        # reaches $CONDA_PREFIX/lib -- an ambient LD_LIBRARY_PATH could otherwise mask a bad RUNPATH.
        rc, out = run_capture(
            [conda, "run", "-n", name, "env", "-u", "LD_LIBRARY_PATH", "ldd", lib], env=env
        )
        _log(out)
        if rc != 0:
            _die(f"[py {py}] ldd failed on {os.path.basename(lib)}; cannot verify reachability")
        ldd_all.append(out)
    combined = "\n".join(ldd_all)

    reach_fail = False
    for want in ("libkrb5.so", "libgssapi_krb5.so", "libltdl.so"):
        hits = [ln for ln in combined.splitlines() if want in ln]
        if not hits:
            print(f"MISS: required '{want}' absent from ldd output.", file=sys.stderr)
            reach_fail = True
            continue
        n_prefix = n_bad = 0
        for ln in hits:
            m = re.search(r"=>\s+(\S+)", ln)
            resolved = m.group(1) if m else ""
            if prefix and resolved.startswith(prefix + os.sep + "lib" + os.sep):
                n_prefix += 1
                _log(f"OK       {ln.strip()}")
            elif resolved == "":
                n_bad += 1
                print(f"NOTFOUND {ln.strip()}", file=sys.stderr)
            else:
                n_bad += 1
                print(f"SYSTEM   {ln.strip()}", file=sys.stderr)
        if n_bad or n_prefix < 1:
            print(
                f"ERROR: '{want}' did not resolve cleanly from {prefix}/lib "
                f"(prefix={n_prefix}, system/absent={n_bad}).",
                file=sys.stderr,
            )
            reach_fail = True
    if reach_fail:
        _die(
            f"[py {py}] reachability gate FAILED -- a required krb5/gssapi/libltdl bound to "
            f"system or was absent instead of {prefix}/lib"
        )
    _log(f"REACHABILITY_OK (krb5 + gssapi_krb5 + libltdl all bound from {prefix}/lib)")


# ---------------------------------------------------------------------------
# 6. Stage this leg's packages onto the artifact (metadata-matched subdir).
# ---------------------------------------------------------------------------
def stage(bld: str, stage_dir: str, target_subdir: str) -> None:
    """Stage ONLY packages whose conda-build output subdir matches THIS leg's target (the
    bld/<subdir>/ folder name IS the authoritative subdir), so a shared agent (osx-arm64 +
    osx-64) never bleeds one leg's packages into the other's artifact."""
    dest_root = os.path.join(stage_dir, target_subdir)
    os.makedirs(dest_root, exist_ok=True)
    staged = 0
    for ext in ("*.conda", "*.tar.bz2"):
        for p in glob.glob(os.path.join(bld, "**", ext), recursive=True):
            if not os.path.basename(p).startswith("mssql-python"):
                continue
            sub = os.path.basename(os.path.dirname(p))
            if sub != target_subdir:
                _log(f"  skip (subdir '{sub}' != target '{target_subdir}'): {os.path.basename(p)}")
                continue
            shutil.copy2(p, dest_root)
            _log(f"  staged {target_subdir}/{os.path.basename(p)}")
            staged += 1
    if not staged:
        _die(f"no conda packages matching target subdir '{target_subdir}' were produced in {bld}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--mssql-wheel-dir", required=True)
    ap.add_argument("--mssql-wheel-glob", default="mssql_python-*.whl")
    ap.add_argument("--odbc-wheel-dir", required=True)
    ap.add_argument("--odbc-wheel-filter", required=True)
    ap.add_argument("--recipe-root", required=True)
    ap.add_argument("--output-dir", required=True)
    ap.add_argument("--stage-dir", required=True)
    ap.add_argument("--conda-subdir", required=True, help="This leg's subdir (staging + display).")
    ap.add_argument("--conda-target-subdir", default="", help="Cross-target via CONDA_SUBDIR.")
    ap.add_argument("--python-versions", default="")
    args = ap.parse_args(argv)

    if args.conda_subdir not in _KNOWN_SUBDIRS:
        _die(f"--conda-subdir '{args.conda_subdir}' is not a known conda subdir")
    if args.conda_target_subdir and args.conda_target_subdir not in _KNOWN_SUBDIRS:
        _die(f"--conda-target-subdir '{args.conda_target_subdir}' is not a known conda subdir")
    if args.python_versions and not re.fullmatch(
        r"\d+\.\d+(,\d+\.\d+)*", args.python_versions.replace(" ", "")
    ):
        _die(f"--python-versions '{args.python_versions}' must be comma-separated X.Y")

    # verify() os.chdir's to the per-leg build dir, so a RELATIVE --recipe-root would resolve the
    # driver_load_probe against the wrong dir. CI passes an absolute path; abspath makes it robust.
    args.recipe_root = os.path.abspath(args.recipe_root)

    # The subdir used for CONDA_SUBDIR cross-targeting + staging (target overrides the native).
    target = args.conda_target_subdir or args.conda_subdir

    # Per-leg work dir keyed on THIS leg's subdir so two legs on a shared agent (osx-arm64 +
    # osx-64) never collide (matches the old scripts' OUT=<outputDir>/<condaSubdir>).
    output_dir = os.path.join(os.path.abspath(args.output_dir), args.conda_subdir)
    os.makedirs(output_dir, exist_ok=True)
    # Clear a reused bld tree so a stale package from a prior run can never be validated/staged.
    bld = os.path.join(output_dir, "bld")
    if os.path.isdir(bld):
        shutil.rmtree(bld)
    os.makedirs(bld, exist_ok=True)
    links = os.path.join(output_dir, "wheels")

    _log("==================== conda build inputs ====================")
    _log(f"mssqlWheelDir      : {args.mssql_wheel_dir}")
    _log(f"odbcWheelDir       : {args.odbc_wheel_dir}")
    _log(f"recipeRoot         : {args.recipe_root}")
    _log(f"outputDir          : {output_dir}")
    _log(f"stageDir           : {args.stage_dir}")
    _log(f"condaSubdir        : {args.conda_subdir}")
    _log(f"condaTargetSubdir  : {args.conda_target_subdir or '(native)'}")
    _log(f"pythonVersions     : {args.python_versions or '(auto-detect)'}")
    _log("============================================================")

    mssql_ver, odbc_ver = gather_wheels(
        args.mssql_wheel_dir,
        args.mssql_wheel_glob,
        args.odbc_wheel_dir,
        args.odbc_wheel_filter,
        links,
    )
    conda = find_or_install_conda(output_dir)
    _log(f"Using conda: {conda}")
    run([conda, "--version"], what="conda --version")
    builder = create_builder_env(conda)
    pyvers = detect_pythons(links, args.python_versions)
    cross_build = bool(args.conda_target_subdir)
    env = build_env(mssql_ver, odbc_ver, links, args.conda_target_subdir)

    build_packages(conda, builder, args.recipe_root, pyvers, bld, target, env)
    audit_packages(conda, builder, args.recipe_root, bld, target, env)
    chan = make_verify_channel(output_dir, bld)
    verify(conda, chan, args.recipe_root, pyvers, mssql_ver, target, cross_build, env, output_dir)
    stage(bld, args.stage_dir, target)

    _log("CONDA_BUILD_OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
