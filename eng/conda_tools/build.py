"""Wheel selection, build sequencing and staging for the combined Conda package."""

from __future__ import annotations

import argparse
import glob
import os
import re
import shutil
import zipfile
from email.parser import BytesParser
from email.policy import default

from . import environment, verify

_KNOWN_SUBDIRS = ("win-64", "win-arm64", "osx-64", "osx-arm64", "linux-64", "linux-aarch64")


def gather_wheels(
    mssql_dir: str, mssql_glob: str, odbc_dir: str, odbc_filter: str, links: str
) -> tuple[str, str]:
    """Copy this platform's mssql-python wheel(s) (excluding the odbc package, whose filename
    also starts with mssql_python) + this platform's odbc wheel into ONE find-links dir. The
    dir is CLEARED first so a stale artifact from a reused workdir can never be validated.
    Check METADATA identities and the exact ODBC dependency pair before copying any wheel."""
    if os.path.isdir(links):
        shutil.rmtree(links)
    os.makedirs(links, exist_ok=True)

    mssql = sorted(
        w
        for w in glob.glob(os.path.join(mssql_dir, mssql_glob))
        if not os.path.basename(w).startswith("mssql_python_odbc-")
    )
    if not mssql:
        environment._die(f"no mssql-python wheel matching '{mssql_glob}' in {mssql_dir}")

    mssql_versions_by_wheel = {
        os.path.basename(w): _wheel_version(os.path.basename(w), "mssql_python") for w in mssql
    }
    malformed_mssql = sorted(
        name for name, version in mssql_versions_by_wheel.items() if version is None
    )
    if malformed_mssql:
        environment._die(f"could not derive a version from mssql-python wheels: {malformed_mssql}")
    mssql_versions = {
        version for version in mssql_versions_by_wheel.values() if version is not None
    }
    if len(mssql_versions) != 1:
        environment._die(
            "mssql-python wheels contain inconsistent versions: " f"{mssql_versions_by_wheel}"
        )
    mssql_ver = next(iter(mssql_versions))

    odbc_matches = sorted(glob.glob(os.path.join(odbc_dir, "**", odbc_filter), recursive=True))
    if not odbc_matches:
        environment._die(f"no wheel matching '{odbc_filter}' in {odbc_dir}")
    if len(odbc_matches) != 1:
        # An arch-ambiguous filter (e.g. one that matches BOTH x86_64 and arm64 odbc wheels)
        # would silently pick [0] and could vendor the WRONG-arch driver into this leg. Demand
        # an exact single match so the filter is tightened to this leg's arch instead.
        environment._die(
            f"odbc-wheel-filter '{odbc_filter}' matched {len(odbc_matches)} wheels in {odbc_dir} "
            f"(expected exactly 1): {[os.path.basename(m) for m in odbc_matches]}"
        )
    odbc = odbc_matches[0]
    odbc_ver = _wheel_version(os.path.basename(odbc), "mssql_python_odbc")
    if not odbc_ver:
        environment._die(f"could not derive a version from ODBC wheel: {os.path.basename(odbc)}")
    _wheel_metadata(odbc, "mssql-python-odbc", odbc_ver)
    for wheel in mssql:
        requirements = _wheel_metadata(wheel, "mssql-python", mssql_ver)
        _validate_odbc_pin(wheel, requirements, odbc_ver)

    # Copy only after every selected wheel agrees with its filename and dependency pair.
    for wheel in [*mssql, odbc]:
        shutil.copy2(wheel, links)
    environment._log("find-links wheels:")
    for name in sorted(os.listdir(links)):
        environment._log(f"  - {name}")
    environment._log(f"Derived versions -> mssql-python={mssql_ver}  mssql-python-odbc={odbc_ver}")
    return mssql_ver, odbc_ver


def _canonical_name(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def _wheel_metadata(path: str, distribution: str, version: str) -> list[str]:
    """Validate the wheel identity and return its declared dependencies without importing it."""
    name = os.path.basename(path)
    try:
        with zipfile.ZipFile(path) as wheel:
            entries = [
                entry
                for entry in wheel.namelist()
                if re.fullmatch(r"[^/]+\.dist-info/METADATA", entry)
            ]
            if len(entries) != 1:
                environment._die(f"{name}: expected exactly one .dist-info/METADATA entry.")
            metadata = BytesParser(policy=default).parsebytes(wheel.read(entries[0]))
    except (OSError, zipfile.BadZipFile, KeyError, RuntimeError, NotImplementedError) as exc:
        environment._die(f"{name}: unreadable wheel metadata ({exc}).")
    for field, expected in (("Name", distribution), ("Version", version)):
        values = metadata.get_all(field, [])
        if len(values) != 1 or not str(values[0]).strip():
            environment._die(f"{name}: expected exactly one nonempty METADATA {field}.")
        actual = str(values[0]).strip()
        matches = (
            _canonical_name(actual) == _canonical_name(expected)
            if field == "Name"
            else actual == expected
        )
        if not matches:
            environment._die(
                f"{name}: METADATA {field} {actual!r} does not match selected wheel {expected!r}."
            )
    return [str(value).strip() for value in metadata.get_all("Requires-Dist", [])]


def _validate_odbc_pin(path: str, requirements: list[str], version: str) -> None:
    pins = []
    for requirement in requirements:
        name = re.match(r"[A-Za-z0-9][A-Za-z0-9._-]*", requirement)
        if name and _canonical_name(name[0]) == "mssql-python-odbc":
            pins.append(requirement[name.end() :].strip())
    match = None
    if len(pins) == 1:
        constraint = pins[0]
        if constraint.startswith("(") and constraint.endswith(")"):
            constraint = constraint[1:-1].strip()
        match = re.fullmatch(r"==\s*([0-9][A-Za-z0-9.!+_]*)", constraint)
    if match is None or match[1] != version:
        environment._die(
            f"{os.path.basename(path)}: expected one unconditional exact "
            f"mssql-python-odbc=={version} requirement; found {pins!r}."
        )


def _wheel_version(name: str, dist: str) -> str | None:
    m = re.match(rf"^{re.escape(dist)}-([^-]+)-", name)
    return m.group(1) if m else None


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
        environment._die(f"no mssql-python wheels in '{links}' to determine Python versions")
    environment._log(f"Building conda packages for Python versions: {', '.join(pyvers)}")
    return pyvers


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
        environment._log(
            f"=== [py {py}] build mssql-python (self-contained: vendors the ODBC payload) ==="
        )
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
        environment.run(cmd, env=env, what=f"conda-build mssql-python (py {py})")

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
    repo_root = os.path.dirname(os.path.abspath(recipe_root))
    module = os.path.join(repo_root, "eng", "conda_tools", "__main__.py")
    if not os.path.isfile(module):
        environment._die(
            f"Conda audit module not found at {module}; "
            "--recipe-root must point to the source checkout's conda directory."
        )
    bld = os.path.abspath(bld)
    command = [conda, "run", "-n", builder, "python", "-m", "eng.conda_tools"]
    environment._log("=== RUNPATH self-containment audit (eng.conda_tools elf) ===")
    environment.run(
        command + ["elf", "--root", bld],
        env=env,
        cwd=repo_root,
        what="RUNPATH self-containment audit",
    )
    # Both Windows packages must retain the core; cross builds also rely on static architecture.
    if target_subdir in ("win-64", "win-arm64"):
        environment._log(
            f"=== {target_subdir} PE machine-type and required native-component assert ==="
        )
        environment.run(
            command + ["pe", "--root", bld, "--subdir", target_subdir],
            env=env,
            cwd=repo_root,
            what=f"{target_subdir} PE machine-type assert",
        )
    # osx legs: verify the universal binding contains the target slice and each thin vendored
    # driver dylib matches its architecture-specific directory. osx-arm64 is cross-built on the
    # Intel agent, so this static pass replaces the runtime import as its architecture check.
    if target_subdir in ("osx-64", "osx-arm64"):
        environment._log(
            f"=== {target_subdir} Mach-O assert (binding target slice + driver-tree arches) ==="
        )
        environment.run(
            command + ["macho", "--root", bld, "--subdir", target_subdir],
            env=env,
            cwd=repo_root,
            what=f"{target_subdir} Mach-O arch-slice assert",
        )


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
                environment._log(
                    f"  skip (subdir '{sub}' != target '{target_subdir}'): {os.path.basename(p)}"
                )
                continue
            shutil.copy2(p, dest_root)
            environment._log(f"  staged {target_subdir}/{os.path.basename(p)}")
            staged += 1
    if not staged:
        environment._die(
            f"no conda packages matching target subdir '{target_subdir}' were produced in {bld}"
        )


def execute(args: argparse.Namespace) -> int:
    if args.conda_subdir not in _KNOWN_SUBDIRS:
        environment._die(f"--conda-subdir '{args.conda_subdir}' is not a known conda subdir")
    if args.conda_target_subdir and args.conda_target_subdir not in _KNOWN_SUBDIRS:
        environment._die(
            f"--conda-target-subdir '{args.conda_target_subdir}' is not a known conda subdir"
        )
    if args.python_versions and not re.fullmatch(
        r"\d+\.\d+(,\d+\.\d+)*", args.python_versions.replace(" ", "")
    ):
        environment._die(f"--python-versions '{args.python_versions}' must be comma-separated X.Y")

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

    environment._log("==================== conda build inputs ====================")
    environment._log(f"mssqlWheelDir      : {args.mssql_wheel_dir}")
    environment._log(f"odbcWheelDir       : {args.odbc_wheel_dir}")
    environment._log(f"recipeRoot         : {args.recipe_root}")
    environment._log(f"outputDir          : {output_dir}")
    environment._log(f"stageDir           : {args.stage_dir}")
    environment._log(f"condaSubdir        : {args.conda_subdir}")
    environment._log(f"condaTargetSubdir  : {args.conda_target_subdir or '(native)'}")
    environment._log(f"pythonVersions     : {args.python_versions or '(auto-detect)'}")
    environment._log("============================================================")

    mssql_ver, odbc_ver = gather_wheels(
        args.mssql_wheel_dir,
        args.mssql_wheel_glob,
        args.odbc_wheel_dir,
        args.odbc_wheel_filter,
        links,
    )
    conda = environment.find_or_install_conda(output_dir)
    environment._log(f"Using conda: {conda}")
    environment.run([conda, "--version"], what="conda --version")
    builder = environment.create_builder_env(conda)
    pyvers = detect_pythons(links, args.python_versions)
    cross_build = bool(args.conda_target_subdir)
    env = environment.build_env(mssql_ver, odbc_ver, links, args.conda_target_subdir)

    build_packages(conda, builder, args.recipe_root, pyvers, bld, target, env)
    audit_packages(conda, builder, args.recipe_root, bld, target, env)
    chan = verify.make_verify_channel(output_dir, bld)
    verify.verify(
        conda, chan, args.recipe_root, pyvers, mssql_ver, target, cross_build, env, output_dir
    )
    stage(bld, args.stage_dir, target)

    environment._log("CONDA_BUILD_OK")
    return 0
