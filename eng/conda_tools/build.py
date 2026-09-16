"""Wheel selection, build sequencing and staging for the combined Conda package."""

from __future__ import annotations

import argparse
import glob
import json
import os
import re
import shutil
import zipfile
from pathlib import Path

from eng.scripts.download_mssql_python_rs_wheels import file_sha256

from . import archive, contracts, environment, verify
from .formats import elf, macho, pe

_KNOWN_SUBDIRS = ("win-64", "win-arm64", "osx-64", "osx-arm64", "linux-64", "linux-aarch64")


def gather_wheels(
    mssql_dir: str,
    mssql_glob: str,
    odbc_dir: str,
    odbc_filter: str,
    links: str,
    rs_dir: str | None = None,
    rs_version_file: str | None = None,
    target_subdir: str = "",
) -> tuple[str, str, str | None]:
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
    rs: set[str] = set()
    rs_metadata: dict[str, archive.WheelMetadata] = {}
    rs_selected: dict[str, str] = {}
    rs_versions: set[str | None] = set()
    try:
        source_rs = None
        if rs_version_file is not None:
            source_rs = Path(rs_version_file).read_text(encoding="ascii").strip()
            if not source_rs:
                raise ValueError(f"empty RS source version assertion: {rs_version_file}")
        _checked_metadata(odbc, "mssql-python-odbc", odbc_ver)
        for wheel in mssql:
            metadata = _checked_metadata(wheel, "mssql-python", mssql_ver)
            try:
                pin = contracts.exact_dependency_pin(metadata["requires_dist"], "mssql-python-odbc")
            except ValueError as exc:
                raise ValueError(
                    f"{wheel}: expected one unconditional exact mssql-python-odbc=={odbc_ver}; "
                    f"{exc}"
                ) from exc
            if pin != odbc_ver:
                raise ValueError(
                    f"{wheel}: expected one unconditional exact mssql-python-odbc=={odbc_ver}; "
                    f"found {pin!r}"
                )
            version = contracts.binding_rs_version(
                metadata, metadata["members"], metadata["record_members"], source_rs
            )
            rs_versions.add(version)
            if version is not None:
                python_tag = os.path.basename(wheel).rsplit("-", 3)[1]
                selected = _select_rs_wheel(rs_dir, version, python_tag, target_subdir)
                if selected not in rs:
                    rs_metadata[selected] = _check_rs_wheel(
                        selected, version, python_tag, target_subdir
                    )
                    rs.add(selected)
                else:
                    facts = rs_metadata[selected]
                    errors = contracts.validate_rs_ownership(
                        facts,
                        facts["members"],
                        facts["record_members"],
                        version,
                        python_tag,
                        target_subdir,
                    )
                    if errors:
                        raise ValueError("; ".join(errors))
                rs_selected[python_tag] = os.path.basename(selected)
        if len(rs_versions) != 1:
            raise ValueError(f"binding wheels mix incompatible RS profiles/versions: {rs_versions}")
        rs_ver = next(iter(rs_versions))
        if rs_dir:
            if rs_ver is None:
                raise ValueError(
                    "RS wheel inputs must not be added to a historical embedded-core binding"
                )
            receipt_path = Path(rs_dir, "transport.json")
            if receipt_path.exists():
                receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
                if not isinstance(receipt, dict) or not isinstance(
                    receipt.get("wheel_sha256"), dict
                ):
                    raise ValueError("RS transport receipt must contain a wheel_sha256 mapping")
                if receipt.get("distribution_version") != rs_ver:
                    raise ValueError("RS transport receipt disagrees with the binding dependency")
                if rs_version_file is not None:
                    pin_file = Path(rs_version_file).with_name("mssql-python-rs-nuget.version")
                    transport = pin_file.read_text(encoding="ascii").strip()
                    if not transport or receipt.get("transport_version") != transport:
                        raise ValueError(
                            "RS transport receipt disagrees with the producer transport pin"
                        )
                for wheel in rs:
                    if receipt["wheel_sha256"].get(Path(wheel).name) != file_sha256(Path(wheel)):
                        raise ValueError(
                            f"RS wheel differs from its producer transport receipt: {wheel}"
                        )
    except archive.READ_ERRORS as exc:
        environment._die(f"invalid wheel inputs: {exc}")

    # Copy only after every selected wheel agrees with its filename and dependency pair.
    for wheel in [*mssql, odbc, *sorted(rs)]:
        shutil.copy2(wheel, links)
    for python_tag, filename in rs_selected.items():
        Path(links, f"rs-wheel-{python_tag}.txt").write_text(
            filename + "\n", encoding="utf-8", newline="\n"
        )
    environment._log("find-links wheels:")
    for name in sorted(os.listdir(links)):
        environment._log(f"  - {name}")
    environment._log(f"Derived versions -> mssql-python={mssql_ver}  mssql-python-odbc={odbc_ver}")
    environment._log(
        f"RS component: mssql-python-rs=={rs_ver}"
        if rs_ver is not None
        else "Historical embedded-core profile; NOT current-source RS qualification."
    )
    return mssql_ver, odbc_ver, rs_ver


def _checked_metadata(path: str, distribution: str, version: str) -> archive.WheelMetadata:
    try:
        metadata = archive.read_wheel_metadata(path)
        errors = contracts.validate_distribution_identity(metadata, distribution, version)
        errors.extend(contracts.validate_wheel_tags(Path(path).name, metadata["tags"]))
        if errors:
            raise ValueError("; ".join(errors))
        return metadata
    except archive.READ_ERRORS as exc:
        raise ValueError(f"{os.path.basename(path)}: {exc}") from exc


def _select_rs_wheel(directory: str | None, version: str, python_tag: str, subdir: str) -> str:
    if (
        not directory
        or subdir not in contracts._RS_PLATFORMS
        or not re.fullmatch(r"cp3\d+", python_tag)
    ):
        raise ValueError(
            "an RS-dependent binding requires --rs-wheel-dir and a normal CPython target"
        )
    matches = []
    for path in sorted(Path(directory).glob(f"mssql_python_rs-{version}-*.whl")):
        if contracts.rs_wheel_matches_target(path.name, python_tag, subdir):
            matches.append(str(path))
    if len(matches) != 1:
        raise ValueError(
            f"expected exactly one mssql-python-rs=={version} wheel for {python_tag} {subdir}; "
            f"found {matches}"
        )
    return matches[0]


def _check_rs_wheel(path: str, version: str, python_tag: str, subdir: str) -> archive.WheelMetadata:
    metadata = _checked_metadata(path, "mssql-python-rs", version)
    errors = contracts.validate_rs_ownership(
        metadata, metadata["members"], metadata["record_members"], version, python_tag, subdir
    )
    with zipfile.ZipFile(path) as wheel:
        for name in metadata["members"]:
            if not name.endswith((".pyd", ".dll", ".so", ".dylib")):
                continue
            data = wheel.read(name)
            if subdir.startswith("win-"):
                errors.extend(contracts.validate_rs_binary(name, subdir, pe.pe_machine(data)))
            elif subdir.startswith("osx-"):
                errors.extend(contracts.validate_rs_binary(name, subdir, macho.macho_arches(data)))
            else:
                errors.extend(contracts.validate_rs_binary(name, subdir, elf.parse(data)))
    if errors:
        raise ValueError(f"{os.path.basename(path)}: {'; '.join(errors)}")
    return metadata


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

    mssql_ver, odbc_ver, rs_ver = gather_wheels(
        args.mssql_wheel_dir,
        args.mssql_wheel_glob,
        args.odbc_wheel_dir,
        args.odbc_wheel_filter,
        links,
        args.rs_wheel_dir,
        args.rs_version_file,
        target,
    )
    conda = environment.find_or_install_conda(output_dir)
    environment._log(f"Using conda: {conda}")
    environment.run([conda, "--version"], what="conda --version")
    builder = environment.create_builder_env(conda)
    pyvers = detect_pythons(links, args.python_versions)
    cross_build = bool(args.conda_target_subdir)
    env = environment.build_env(mssql_ver, odbc_ver, links, args.conda_target_subdir, rs_ver)

    build_packages(conda, builder, args.recipe_root, pyvers, bld, target, env)
    audit_packages(conda, builder, args.recipe_root, bld, target, env)
    chan = verify.make_verify_channel(output_dir, bld)
    verify.verify(
        conda, chan, args.recipe_root, pyvers, mssql_ver, target, cross_build, env, output_dir
    )
    stage(bld, args.stage_dir, target)
    if rs_ver is not None and args.rs_wheel_dir:
        receipt = Path(args.rs_wheel_dir, "transport.json")
        if receipt.is_file():
            shutil.copy2(receipt, Path(args.stage_dir, "rs-transport.json"))

    environment._log("CONDA_BUILD_OK")
    return 0
