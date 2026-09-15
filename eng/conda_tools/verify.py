"""Isolated installed-package verification; never import native payloads in this process."""

from __future__ import annotations

import os
import platform
import re
import shutil
import sys
from pathlib import Path

from . import environment


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
    environment._log(f"verify channel (token-free alias of {bld}): {chan_url}")
    return chan_url


def _is_emulated_cross(target_subdir: str, cross_build: bool) -> bool:
    host = platform.machine().lower()
    if cross_build and target_subdir == "linux-aarch64" and host not in ("aarch64", "arm64"):
        environment._log(
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
        environment.run_ok([conda, "env", "remove", "-y", "-n", name])
        environment._log(f"=== [py {py}] create verify env from local channel ===")

        cross_best_effort = cross_build and target_subdir in ("win-arm64", "osx-arm64")
        if target_subdir == "win-arm64":
            # BLOCKING solvability gate: --dry-run resolves the FULL win-arm64 graph on x64
            # (no link / post-link / arm64 exec) -- a pure "installable?" check. win-arm64 deps
            # span microsoft (noarch azure-identity/msal) + Anaconda defaults, so no
            # --strict-channel-priority. Pin the freshly built version so no channel can shadow it.
            environment.run(
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
            environment.run(
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
            environment.run(
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
        rc, out = environment.run_capture(
            [conda, "run", "-n", name, "python", "-c", "import sys"], env=env
        )
        if rc != 0:
            if cross_best_effort:
                environment._log(
                    f"=== [py {py}] {target_subdir} cross: target Python not executable on this "
                    f"host; deps SOLVED (blocking), skipping runtime import (arch enforced by "
                    f"the platform's static binary audit). ==="
                )
                environment._log(out)
                continue
            environment._die(
                f"[py {py}] target Python for CONDA_SUBDIR={target_subdir or 'native'} is not "
                f"executable on {sys.platform}/{platform.machine()}, and this is NOT an "
                f"arm64 cross-build. Refusing to silently skip validation. Output: {out}"
            )

        # A separate process prevents API/driver preloads from masking core load failures.
        environment.run(
            [conda, "run", "-n", name, "python", "-c", _core_probe()],
            env=env,
            what=f"independent required mssql_py_core load (py {py})",
        )
        environment._log(
            f"=== [py {py}] import mssql_python + prove the vendored ODBC payload is present ==="
        )
        environment.run(
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
        environment.run(
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

        environment._log(
            f"=== [py {py}] DB-less driver-load proof (real ODBC driver must load) ==="
        )
        probe = os.path.join(recipe_root, "driver_load_probe.py")
        if emulated:
            if environment.run_ok([conda, "run", "-n", name, "python", probe], env=env) != 0:
                environment._log(
                    "SKIP (emulated cross under QEMU binfmt): qemu-user cannot initialize the "
                    "native ODBC environment; best-effort on the emulated leg (static RUNPATH "
                    "audit + native + full-arch-emulation legs validate the driver)."
                )
        else:
            environment.run(
                [conda, "run", "-n", name, "python", probe],
                env=env,
                what=f"driver-load proof (py {py})",
            )

        if not is_win:
            _reachability_gate(conda, name, py, emulated, env)

        environment._log(f"=== [py {py}] confirm resolved dependencies ===")
        rc, out = environment.run_capture([conda, "list", "-n", name], env=env)
        if rc != 0:
            environment._die(f"[py {py}] failed to list resolved dependencies. Output: {out}")
        for line in out.splitlines():
            if re.search(r"azure-identity|mssql-python|openssl|krb5", line):
                environment._log(line)


def _reachability_gate(conda: str, name: str, py: str, emulated: bool, env: dict[str, str]) -> None:
    """Linux, opt-in (CONDA_ASSERT_PREFIX_REACHABLE=1): prove the vendored driver binds the
    env's OWN $CONDA_PREFIX/lib krb5/gssapi/libltdl via the $ORIGIN climb, not a system copy."""
    if env.get("CONDA_ASSERT_PREFIX_REACHABLE") != "1" or sys.platform != "linux":
        return
    if emulated:
        environment._log(
            f"=== [py {py}] reachability gate SKIPPED on the emulated cross leg (qemu-user "
            f"cannot reliably run the aarch64 driver's ldd/env init); the static RUNPATH audit "
            f"is the authoritative $ORIGIN-climb guard. ==="
        )
        return
    environment._log(
        f"=== [py {py}] minimal-base ldd reachability gate (driver MUST bind CONDA_PREFIX/lib) ==="
    )
    rc, prefix_out = environment.run_capture(
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
        environment._die(
            f"[py {py}] failed to read CONDA_PREFIX/sys.prefix for reachability gate. "
            f"Output: {prefix_out}"
        )
    prefix = prefix_out.strip().splitlines()[-1] if prefix_out.strip() else ""
    if not prefix:
        environment._die(f"[py {py}] reachability gate returned an empty CONDA_PREFIX/sys.prefix.")
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
    rc, drv_out = environment.run_capture([conda, "run", "-n", name, "python", "-c", sel], env=env)
    if rc != 0:
        environment._die(f"[py {py}] failed to locate driver path in verify env. Output: {drv_out}")
    drv = drv_out.strip().splitlines()[-1] if drv_out.strip() else ""
    if not drv:
        environment._die(
            f"[py {py}] no libmsodbcsql driver found in the verify env; cannot prove reachability"
        )
    inst = os.path.join(os.path.dirname(drv), "libodbcinst.so.2")

    ldd_all = []
    for lib in (drv, inst):
        environment._log(f"--- ldd {os.path.basename(lib)} ---")
        # Clear inherited LD_LIBRARY_PATH so resolution proves the RUNPATH $ORIGIN climb ALONE
        # reaches $CONDA_PREFIX/lib -- an ambient LD_LIBRARY_PATH could otherwise mask a bad RUNPATH.
        rc, out = environment.run_capture(
            [conda, "run", "-n", name, "env", "-u", "LD_LIBRARY_PATH", "ldd", lib], env=env
        )
        environment._log(out)
        if rc != 0:
            environment._die(
                f"[py {py}] ldd failed on {os.path.basename(lib)}; cannot verify reachability"
            )
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
                environment._log(f"OK       {ln.strip()}")
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
        environment._die(
            f"[py {py}] reachability gate FAILED -- a required krb5/gssapi/libltdl bound to "
            f"system or was absent instead of {prefix}/lib"
        )
    environment._log(f"REACHABILITY_OK (krb5 + gssapi_krb5 + libltdl all bound from {prefix}/lib)")
