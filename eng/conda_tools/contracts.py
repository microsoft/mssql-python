"""Compatibility policy over package metadata, member names and parsed binary facts."""

from __future__ import annotations

import os
import posixpath
import re
from typing import Any, Iterable, Literal, Mapping, Sequence

from .archive import DistributionMetadata, WheelMetadata
from .formats.elf import ElfDynamicInfo, ElfFacts

Format = Literal["elf", "pe", "macho"]

# Driver binaries whose RUNPATH must carry the exact $ORIGIN climb.
_DRIVER_PREFIXES = ("libmsodbcsql-", "libmsodbcsql.")
_ODBCINST = "libodbcinst.so.2"

# Libraries that must be serviced by DECLARED conda deps, never vendored into the
# Linux payload (bundling these is the anti-pattern the recipe avoids).
_MUST_NOT_VENDOR = ("libkrb5", "libgssapi", "libssl", "libcrypto", "libltdl")

# conda run-deps that SERVICE the driver's krb5/openssl/libltdl. Missing any means
# the $PREFIX/lib copy the RUNPATH climb points at would not exist -- declaration is
# as load-bearing as the climb itself.
_REQUIRED_DEPS = ("krb5", "libtool", "openssl")

# Expected DT_NEEDED soname PREFIXES (incl. the '.so' so 'libkrb5support.so' does NOT satisfy
# 'libkrb5.so'), so a driver that silently STOPPED needing krb5 (making the declared dep moot)
# is caught too.
_DRIVER_NEEDED = ("libkrb5.so", "libgssapi_krb5.so", "libodbcinst.so")
_ODBCINST_NEEDED = ("libltdl.so",)

# ELF e_machine architecture ids (ELF header offset 0x12). The conda subdir is the
# authority: every vendored driver/manager ELF must match it, so an x86_64 .so
# mislabeled under a linux-aarch64 package is caught statically (the emulated aarch64
# leg's runtime probe is best-effort and would not). Linux twin of the PE audit.
_EM_X86_64 = 62
_EM_AARCH64 = 183
_SUBDIR_MACHINE = {"linux-64": _EM_X86_64, "linux-aarch64": _EM_AARCH64}
_MACHINE_NAME = {_EM_X86_64: "x86_64", _EM_AARCH64: "aarch64"}
_REQUIRED_DRIVER_TREES = {
    "linux-64": {
        ("alpine", "x86_64"),
        ("debian_ubuntu", "x86_64"),
        ("rhel", "x86_64"),
        ("suse", "x86_64"),
    },
    "linux-aarch64": {
        ("alpine", "arm64"),
        ("debian_ubuntu", "arm64"),
        ("rhel", "arm64"),
    },
}


# IMAGE_FILE_MACHINE_* (winnt.h): the PE COFF Machine field -> a short name.
_PE_MACHINES = {
    0x8664: "amd64",
    0xAA64: "arm64",
    0x014C: "x86",
    0x01C0: "arm",
    0x01C4: "armnt",
}

# conda subdir -> the ONLY PE machine its vendored .pyd/.dll may carry.
_PE_SUBDIR_MACHINE = {
    "win-64": 0x8664,
    "win-arm64": 0xAA64,
}
_PE_DRIVER_DIR = {
    "win-64": "x64",
    "win-arm64": "arm64",
}

_PE_SUFFIXES = (".pyd", ".dll")


# conda subdir -> the arch slice its vendored Mach-O binaries MUST contain.
_MACHO_SUBDIR_ARCH = {
    "osx-64": "x86_64",
    "osx-arm64": "arm64",
}

_MACHO_DRIVER_DIR_ARCH = {
    "arm64": "arm64",
    "x86_64": "x86_64",
}

_MACHO_SUFFIXES = (".dylib", ".so")
_REQUIRED_DRIVER_LIBRARIES = frozenset(
    {
        "libltdl.7.dylib",
        "libmsodbcsql.18.dylib",
        "libodbc.2.dylib",
        "libodbcinst.2.dylib",
    }
)


def canonical_distribution_name(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def validate_distribution_identity(
    metadata: DistributionMetadata, distribution: str, version: str
) -> list[str]:
    errors = []
    for field, actual, expected in (
        ("Name", metadata["name"], distribution),
        ("Version", metadata["version"], version),
    ):
        matches = (
            canonical_distribution_name(actual) == canonical_distribution_name(expected)
            if field == "Name"
            else actual == expected
        )
        if not matches:
            errors.append(
                f"METADATA {field} {actual!r} does not match selected wheel {expected!r}."
            )
    return errors


def validate_wheel_tags(filename: str, tags: Sequence[str]) -> list[str]:
    parts = filename.removesuffix(".whl").rsplit("-", 3)
    if not filename.endswith(".whl") or len(parts) != 4:
        return [f"invalid wheel filename: {filename}"]
    python_tag, abi, platform = parts[1:]
    expected = {
        f"{p}-{a}-{plat}"
        for p in python_tag.split(".")
        for a in abi.split(".")
        for plat in platform.split(".")
    }
    return [] if set(tags) == expected else ["WHEEL tags do not match the selected filename"]


def _wheel_platform_matches_target(platforms: str, subdir: str) -> bool:
    patterns = {
        "win-64": r"win_amd64",
        "win-arm64": r"win_arm64",
        "linux-64": r"(?:linux|manylinux(?:1|2010|2014|_\d+_\d+))_x86_64",
        "linux-aarch64": r"(?:linux|manylinux(?:1|2010|2014|_\d+_\d+))_aarch64",
        "osx-64": r"macosx_\d+_\d+_(?:x86_64|universal2)",
        "osx-arm64": r"macosx_\d+_\d+_(?:arm64|universal2)",
    }
    if subdir not in patterns:
        raise ValueError(f"unknown Conda wheel target: {subdir}")
    return any(re.fullmatch(patterns[subdir], platform) for platform in platforms.split("."))


def binding_wheel_matches_target(
    filename: str, subdir: str, python_versions: Sequence[str] = ()
) -> bool:
    """Select binding filenames before reading metadata; native audits remain authoritative."""
    if not subdir and not python_versions:
        return True
    parts = filename.removesuffix(".whl").rsplit("-", 3)
    if not filename.endswith(".whl") or len(parts) != 4:
        raise ValueError(f"invalid binding wheel filename: {filename}")
    python_tag, abi, platforms = parts[1:]
    if subdir and not _wheel_platform_matches_target(platforms, subdir):
        return False
    if not re.fullmatch(r"cp3\d+", python_tag):
        raise ValueError(f"invalid binding wheel Python tag: {filename}")
    if python_versions and python_tag not in {
        f"cp{version.replace('.', '')}" for version in python_versions
    }:
        return False
    return abi == python_tag


def odbc_wheel_matches_target(filename: str, subdir: str) -> bool:
    parts = filename.removesuffix(".whl").rsplit("-", 3)
    if not filename.endswith(".whl") or len(parts) != 4:
        raise ValueError(f"invalid ODBC wheel filename: {filename}")
    python_tag, abi, platforms = parts[1:]
    return (
        python_tag == "py3" and abi == "none" and _wheel_platform_matches_target(platforms, subdir)
    )


_RS_PLATFORMS = {
    "win-64": "win_amd64",
    "win-arm64": "win_arm64",
    "osx-64": "macosx_15_0_universal2",
    "osx-arm64": "macosx_15_0_universal2",
    "linux-64": "manylinux_2_34_x86_64",
    "linux-aarch64": "manylinux_2_34_aarch64",
}


def rs_wheel_matches_target(filename: str, python_tag: str, subdir: str) -> bool:
    """Use the build's exact normal-CPython/abi3 and platform selection for RS inputs."""
    parts = filename.removesuffix(".whl").rsplit("-", 3)
    if not filename.endswith(".whl") or len(parts) != 4 or not re.fullmatch(r"cp3\d+", python_tag):
        return False
    py, abi, platform = parts[1:]
    compatible_python = py == python_tag and abi == python_tag
    if abi == "abi3" and re.fullmatch(r"cp3\d+", py):
        compatible_python = int(py[2:]) <= int(python_tag[2:])
    return compatible_python and _RS_PLATFORMS.get(subdir) in platform.split(".")


def exact_dependency_pin(requirements: Iterable[str], distribution: str) -> str | None:
    """Only an absent declaration returns None; malformed declarations always fail."""
    distribution = canonical_distribution_name(distribution)
    pins = []
    for requirement in requirements:
        name = re.match(r"[A-Za-z0-9][A-Za-z0-9._-]*", requirement)
        if name and canonical_distribution_name(name[0]) == distribution:
            pins.append(requirement[name.end() :].strip())
    if not pins:
        return None
    match = None
    if len(pins) == 1:
        constraint = pins[0]
        if constraint.startswith("(") and constraint.endswith(")"):
            constraint = constraint[1:-1].strip()
        match = re.fullmatch(r"==\s*([0-9][A-Za-z0-9.!+_]*)", constraint)
    if match is None:
        raise ValueError(
            f"expected one unconditional exact {distribution} requirement; found {pins!r}."
        )
    return match[1]


def owned_core_members(members: Iterable[str], records: Iterable[str]) -> list[str]:
    """Core ownership requires a file to be both declared in RECORD and actually present."""
    actual = set(members)
    return [name for name in records if name in actual and name.startswith("mssql_py_core/")]


def _core_path(member: str, root: str = "") -> str | None:
    # Account for wheel spread paths and aliases on case-insensitive extraction targets.
    path = posixpath.normpath(member.replace("\\", "/")).lstrip("/").casefold()
    if root:
        prefix = posixpath.normpath(root.replace("\\", "/")).lstrip("/").casefold() + "/"
        if not path.startswith(prefix):
            return None
        path = path[len(prefix) :]
    path = re.sub(r"^[^/]+\.data/(?:purelib|platlib)/", "", path)
    return path if path.startswith("mssql_py_core/") else None


def validate_core_ownership(
    members: Iterable[str],
    ownership: Mapping[str, Iterable[str]],
    provider: str,
    *,
    root: str = "",
) -> list[str]:
    """Require every actual core file to belong only to the selected provider's RECORD."""
    core = [path for member in members if (path := _core_path(member, root)) is not None]
    records = {
        owner: {path for member in declared if (path := _core_path(member)) is not None}
        for owner, declared in ownership.items()
    }
    errors = []
    if len(core) != len(set(core)):
        errors.append("duplicate mssql_py_core payload paths")
    for member in sorted(set(core)):
        owners = {owner for owner, paths in records.items() if member in paths}
        if owners != {provider}:
            errors.append(
                f"{member}: mssql_py_core must be owned exclusively by {provider}; "
                f"RECORD owners: {sorted(owners)}"
            )
    return errors


def validate_wheel_core_ownership(metadata: WheelMetadata) -> list[str]:
    distribution = canonical_distribution_name(metadata["name"])
    provider = (
        "mssql-python"
        if distribution == "mssql-python"
        and exact_dependency_pin(metadata["requires_dist"], "mssql-python-rs") is None
        else "mssql-python-rs"
    )
    return validate_core_ownership(
        metadata["members"], {distribution: metadata["record_members"]}, provider
    )


def binding_rs_version(
    metadata: DistributionMetadata,
    members: Iterable[str],
    records: Iterable[str],
    expected_rs_version: str | None = None,
) -> str | None:
    """Classify a binding using its exclusive wheel members or RECORD-owned installed files."""
    names = list(members)
    version = exact_dependency_pin(metadata["requires_dist"], "mssql-python-rs")
    if expected_rs_version is not None and version != expected_rs_version:
        raise ValueError(
            f"binding RS dependency {version!r} does not match producer source "
            f"mssql-python-rs=={expected_rs_version}"
        )
    if version is not None:
        if any(name.startswith("mssql_py_core/") for name in names):
            raise ValueError("RS-dependent binding wheel must not own mssql_py_core")
    else:
        core = owned_core_members(names, records)
        if "mssql_py_core/__init__.py" not in core or not any(
            re.fullmatch(r"mssql_py_core/mssql_py_core(?:\.[^/]+)?\.(?:so|pyd)", name)
            for name in core
        ):
            raise ValueError(
                "binding without an RS dependency must own a present core initializer and "
                "native extension in both payload and RECORD; not a valid historical profile"
            )
    return version


def rs_private_libraries(subdir: str) -> tuple[str, ...]:
    if subdir.startswith("win-"):
        return (f"mssql_py_core/libs/windows/{_PE_DRIVER_DIR[subdir]}/mssqlodbc.dll",)
    if subdir.startswith("osx-"):
        return tuple(
            f"mssql_py_core/libs/macos/{arch}/lib/mssqlodbc.dylib" for arch in ("arm64", "x86_64")
        )
    arch = {"linux-64": "x86_64", "linux-aarch64": "arm64"}[subdir]
    return (f"mssql_py_core/libs/linux/glibc/{arch}/lib/mssqlodbc.so",)


def validate_core_layout(
    names: Iterable[str], python_tag: str, subdir: str, root: str = ""
) -> list[str]:
    """Check wheel-relative core layout; native fact validators separately check its bytes."""
    names = list(names)
    windows = subdir.startswith("win-")
    suffix = "pyd" if windows else "so"
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
        (f"mssql_py_core.{python_tag}-{arch}.pyd", "mssql_py_core.pyd")
        if windows
        else (f"mssql_py_core.cpython-{python_tag[2:]}-{arch}.so", "mssql_py_core.abi3.so")
    )
    errors = []
    initializer = f"{root}mssql_py_core/__init__.py"
    if names.count(initializer) != 1:
        errors.append(
            f"expected exactly one required {initializer}; found {names.count(initializer)}"
        )
    if len(cores) != 1:
        errors.append(
            f"expected exactly one required mssql_py_core native extension; found {len(cores)}"
        )
    elif cores[0].rsplit("/", 1)[-1] not in core_names:
        errors.append(
            f"{cores[0]}: mssql_py_core is incompatible with normal {python_tag} {subdir}"
        )
    return errors


def validate_rs_binary(
    name: str, subdir: str, facts: ElfFacts | int | set[str] | None
) -> list[str]:
    """Check an RS wheel's parsed native facts before staging a cross-target input."""
    if subdir.startswith("win-"):
        if facts != _PE_SUBDIR_MACHINE[subdir]:
            return [f"{name}: RS PE header does not match {subdir}"]
    elif subdir.startswith("osx-"):
        expected = "arm64" if "/arm64/" in name else "x86_64" if "/x86_64/" in name else None
        required = {expected} if expected else {"arm64", "x86_64"}
        if not isinstance(facts, set) or not required <= facts:
            return [f"{name}: RS Mach-O header lacks required slices {sorted(required)}"]
    elif (
        not isinstance(facts, ElfFacts)
        or not facts.header
        or not facts.elf64_le
        or facts.machine != _SUBDIR_MACHINE[subdir]
        or facts.error
        or facts.dynamic is None
    ):
        return [f"{name}: invalid or wrong-architecture RS ELF for {subdir}"]
    else:
        if any(version > (2, 34) for version in facts.dynamic["glibc_required"]):
            return [f"{name}: RS ELF requires a glibc newer than 2.34"]
        if {"libssl.so.1.1", "libcrypto.so.1.1"} & set(facts.dynamic["needed"]):
            return [f"{name}: RS ELF requires OpenSSL 1.1, incompatible with openssl >=3,<4"]
    return []


def validate_rs_ownership(
    metadata: DistributionMetadata,
    members: Iterable[str],
    records: Iterable[str],
    version: str,
    python_tag: str,
    subdir: str,
) -> list[str]:
    owned = owned_core_members(members, records)
    errors = validate_distribution_identity(metadata, "mssql-python-rs", version)
    errors.extend(validate_core_layout(owned, python_tag, subdir))
    for name in rs_private_libraries(subdir):
        if name not in owned:
            errors.append(f"RS package is missing owned private runtime library {name}")
    return errors


def validate_native_contract(names: Iterable[str], index: dict[str, Any]) -> list[str]:
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
    names = [name.replace("\\", "/") for name in names]
    bindings = [
        name
        for name in names
        if re.fullmatch(
            rf"{re.escape(root)}mssql_python/ddbc_bindings\.cp{abi[2]}-[^/]+\.{suffix}", name
        )
    ]
    errors = validate_core_layout(names, f"cp{abi[2]}", subdir, root)
    if len(bindings) != 1:
        errors.append(
            f"expected exactly one normal cp{abi[2]} native binding; found {len(bindings)}"
        )
    return errors


def declared_glibc_floor(index: dict[str, Any]) -> tuple[int, ...]:
    """Require a single explicit minimum; wheel platform tags are not symbol-floor evidence."""
    specs = [
        d for d in index.get("depends", []) if isinstance(d, str) and d.split()[:1] == ["__glibc"]
    ]
    if len(specs) != 1:
        raise ValueError("expected exactly one __glibc >=VERSION dependency")
    match = re.fullmatch(r"__glibc\s+>=(\d+(?:\.\d+)+)", specs[0])
    if match is None:
        raise ValueError(f"unsupported __glibc dependency: {specs[0]!r}")
    return tuple(map(int, match[1].split(".")))


def effective_runpath(dyn: ElfDynamicInfo) -> str | None:
    """The loader ignores ``DT_RPATH`` when ``DT_RUNPATH`` is present."""
    return dyn["runpath"] if dyn["runpath"] is not None else dyn["rpath"]


def _entries(runpath: str | None) -> list[str]:
    return [e for e in (runpath or "").split(":") if e]


def expected_climb_entry(member_name: str) -> str:
    """Exact ``$ORIGIN/<climb>`` from the member's own dir to package-root ``lib``.

    conda stores python files at ``lib/pythonX.Y/site-packages/...`` and
    ``$PREFIX/lib`` == package-root ``lib``, so the climb is the POSIX relpath from
    the driver's directory to the top-level ``lib`` (never a hard-coded ``../`` count).
    """
    member_dir = posixpath.dirname(member_name)
    climb = posixpath.relpath("lib", member_dir)
    return "$ORIGIN/" + climb


def _dep_names(depends: Iterable[Any] | None) -> set[str]:
    """The package names (first token) of an ``info/index.json`` ``depends`` list."""
    names = set()
    for d in depends or []:
        token = str(d).strip().split()
        if token:
            names.add(token[0])
    return names


_OPENSSL_LOWER_OK = frozenset({">=3", ">=3.0", ">=3.0.0"})
_OPENSSL_UPPER_OK = frozenset({"<4", "<4.0", "<4.0.0", "<4.0a0", "<4.0.0a0"})


def _openssl_range_ok(constraint: str) -> bool:
    """True iff an openssl spec pins EXACTLY the Driver-18 ABI range: a ``>=3`` lower AND a
    ``<4`` upper that admits no openssl 4.x, matched against an ALLOWLIST of canonical bound
    spellings. Anything else -- a conda OR-group (``>=3|>=1``), a garbage/unrecognized clause,
    ``<=4``, ``<4.1``, or a spec missing a lower/upper -- FAILS CLOSED. This is a security-
    adjacent pin, so a false-negative is safe (a maintainer widens the allowlist) but a
    false-positive is not.
    """
    if not constraint or "|" in constraint:
        return False
    has_lower = False
    has_upper = False
    for clause in constraint.split(","):
        c = clause.replace(" ", "")
        if not c:
            continue
        if c in _OPENSSL_LOWER_OK:
            has_lower = True
        elif c in _OPENSSL_UPPER_OK:
            has_upper = True
        else:
            return False
    return has_lower and has_upper


def validate_elf(
    base_name: str,
    index: dict[str, Any],
    members: Sequence[tuple[str, ElfFacts]],
    rs_required: bool = False,
) -> tuple[list[str], list[str]]:
    subdir = index["subdir"]
    expected_machine = _SUBDIR_MACHINE[subdir]
    errors: list[str] = []
    details: list[str] = []

    # N2a: the run deps that SERVICE the driver's krb5/openssl/libltdl must be declared.
    dep_names = _dep_names(index.get("depends"))
    for req in _REQUIRED_DEPS:
        if req not in dep_names:
            errors.append(
                f"{base_name}: info/index.json depends is missing '{req}' -- the "
                f"$PREFIX/lib copy the RUNPATH climb targets would not exist. "
                f"depends={sorted(dep_names)}"
            )
    # openssl must be RANGE-pinned for Driver 18 (which supports only the OpenSSL
    # 1.1/3.0 ABI; conda-forge has begun shipping openssl 4), not merely present.
    if "openssl" in dep_names:
        spec = next(
            (str(d) for d in (index.get("depends") or []) if str(d).split()[:1] == ["openssl"]),
            "openssl",
        )
        constraint = spec[len("openssl") :].strip()
        if not _openssl_range_ok(constraint):
            errors.append(
                f"{base_name}: openssl dep '{spec}' is not range-pinned '>=3,<4' "
                f"(Driver 18 supports only the OpenSSL 1.1/3.0 ABI)."
            )

    lib_dirs: set = set()
    driver_trees: set = set()
    dirs_with_driver: set = set()
    dirs_with_inst: set = set()
    vendored: list[str] = []

    errors.extend(validate_native_contract((name for name, _ in members), index))
    try:
        glibc_floor = declared_glibc_floor(index)
    except (TypeError, ValueError) as exc:
        errors.append(f"{base_name}: invalid glibc compatibility metadata: {exc}")
        glibc_floor = None

    for name, fact in members:
        base = posixpath.basename(name)
        norm = "/" + name
        member_dir = posixpath.dirname(name)
        # Track every driver lib dir (mssql_python_odbc/libs/linux/<distro>/<arch>/lib).
        if "/mssql_python_odbc/libs/linux/" in norm and member_dir.endswith("/lib"):
            lib_dirs.add(member_dir)
            relative = norm.split("/libs/linux/", 1)[1]
            parts = relative.split("/")
            if len(parts) >= 3:
                driver_trees.add((parts[0], parts[1]))

        # Flag any crypto/krb5/ltdl library vendored ANYWHERE in the package payload (not only
        # under /libs/linux/): conda services these via DECLARED deps in $PREFIX/lib, so the
        # mssql-python package must never SHIP one -- a copy in a .libs/ dir or a stray
        # $PREFIX/lib .so is reachable by the audited RUNPATH climb and breaks the invariant.
        if any(base.startswith(p) and ".so" in base for p in _MUST_NOT_VENDOR):
            vendored.append(name)
            continue

        is_driver = any(base.startswith(p) for p in _DRIVER_PREFIXES)
        is_inst = base == _ODBCINST
        is_native = fact.magic or base.endswith(".so") or ".so." in base
        if not (is_native or is_driver or is_inst):
            continue
        if not fact.header:
            errors.append(f"{name}: expected an ELF binary but the header is not ELF.")
            continue
        if not fact.elf64_le:
            errors.append(f"{name}: expected an ELF64 little-endian binary for '{subdir}'.")
            continue

        # Architecture gate: the ELF machine MUST match the package's conda subdir, so
        # an x86_64 driver mislabeled under a linux-aarch64 package (which the emulated
        # leg's best-effort runtime probe would not catch) fails here.
        mach = fact.machine
        if mach != expected_machine:
            machine_name = _MACHINE_NAME.get(mach, "unknown") if mach is not None else "unknown"
            errors.append(
                f"{name}: ELF machine {mach} ({machine_name}) does "
                f"not match the '{subdir}' package arch {expected_machine} "
                f"({_MACHINE_NAME[expected_machine]}) -- wrong-arch/mislabeled native binary."
            )

        if fact.error is not None:
            errors.append(f"{name}: invalid ELF dynamic metadata ({fact.error}).")
            continue
        dyn = fact.dynamic
        assert dyn is not None
        for required in dyn["glibc_required"]:
            if glibc_floor is not None:
                width = max(len(required), len(glibc_floor))
                if required + (0,) * (width - len(required)) > glibc_floor + (0,) * (
                    width - len(glibc_floor)
                ):
                    errors.append(
                        f"{name}: requires GLIBC_{'.'.join(map(str, required))} but archive "
                        f"declares __glibc >={'.'.join(map(str, glibc_floor))}."
                    )
        is_rs = rs_required and "/mssql_py_core/" in norm and base.endswith(".so")
        if is_rs:
            errors.extend(validate_rs_binary(name, subdir, fact))
        if not (is_driver or is_inst or is_rs):
            continue
        raw_runpath = effective_runpath(dyn)
        entries = _entries(raw_runpath)
        needed = dyn["needed"]
        # An EMPTY RUNPATH entry (leading/trailing/double ':') is resolved by the loader
        # against the CURRENT directory -- an untrusted-cwd search. _entries() drops empties
        # for the membership checks below, so flag it HERE, else '$ORIGIN:' would pass the
        # exact-{$ORIGIN, climb} check.
        if raw_runpath is not None and "" in raw_runpath.split(":"):
            errors.append(
                f"{name}: effective RUNPATH '{raw_runpath}' contains an EMPTY entry "
                f"(current-directory search); it must be exactly '$ORIGIN:<climb>'. "
                f"NEEDED={needed}"
            )
        # musl/alpine variants (NEEDED libc.musl*) link differently -- their libodbcinst
        # statically resolves libltdl, so the glibc DT_NEEDED requirements below do not
        # apply. There is no musl conda subdir (conda Linux is glibc-only); these variants
        # ride along in the payload but are never the conda load target. The climb /
        # presence / no-vendored checks still apply to them.
        is_musl = any("libc.musl" in n for n in needed)
        want = expected_climb_entry(name)

        # Bare $ORIGIN must ALSO be present: it is how the driver resolves its
        # co-located sibling libodbcinst.so.2. Losing it breaks driver-manager loading
        # even when the $PREFIX/lib climb entry is intact.
        if "$ORIGIN" not in entries:
            errors.append(
                f"{name}: effective RUNPATH {entries or '[none]'} lacks bare '$ORIGIN' "
                f"(co-located sibling resolution for libodbcinst.so.2). NEEDED={needed}"
            )
        # N1: the EXACT climb entry must be present in the EFFECTIVE RUNPATH.
        if want not in entries:
            errors.append(
                f"{name}: effective RUNPATH {entries or '[none]'} does not contain the "
                f"exact climb entry '{want}' to $PREFIX/lib (the loader uses DT_RUNPATH "
                f"when present, else DT_RPATH). NEEDED={needed}"
            )
        # Stay relocatable: reject ANY absolute entry.
        abs_entries = [e for e in entries if e.startswith("/")]
        if abs_entries:
            errors.append(
                f"{name}: RUNPATH has ABSOLUTE entries {abs_entries}; must stay "
                f"relocatable (relative $ORIGIN only)."
            )
        # Defense-in-depth: the RELATIVE entries must be EXACTLY {$ORIGIN, want}. build.sh
        # stamps precisely those two, so any other relative entry (a stray climb / leftover
        # build path) is unexpected and could resolve a lib from an unintended location.
        unexpected_rel = [
            e for e in entries if not e.startswith("/") and e not in ("$ORIGIN", want)
        ]
        if unexpected_rel:
            errors.append(
                f"{name}: RUNPATH has unexpected relative entries {unexpected_rel}; the "
                f"effective RUNPATH must be exactly ['$ORIGIN', '{want}'] (got {entries})."
            )

        # N2b: the expected DT_NEEDED set must still be present (glibc variants only;
        # musl links these statically / differently, and is not a conda target).
        if is_driver:
            dirs_with_driver.add(member_dir)
            if not is_musl:
                for want_need in _DRIVER_NEEDED:
                    if not any(want_need in n for n in needed):
                        errors.append(
                            f"{name}: driver no longer NEEDs '{want_need}*' (NEEDED={needed}); "
                            f"the declared conda dep would go unused and reachability is unproven."
                        )
        if is_inst:
            dirs_with_inst.add(member_dir)
            if not is_musl:
                for want_need in _ODBCINST_NEEDED:
                    if not any(want_need in n for n in needed):
                        errors.append(
                            f"{name}: libodbcinst.so.2 no longer NEEDs '{want_need}*' "
                            f"(NEEDED={needed})."
                        )
        details.append(f"  {subdir}/{base}: effective RUNPATH={entries} NEEDED={needed}")

    if vendored:
        errors.append(
            f"{base_name}: vendors libraries that must be DECLARED conda deps, not "
            f"bundled: {sorted(vendored)} (krb5/openssl/libltdl are serviced by conda, "
            f"never shipped inside the payload)."
        )
    # Require the supported distro inventory for this architecture, then require EVERY
    # discovered driver lib dir to ship BOTH a driver and libodbcinst.so.2. The x86_64
    # ODBC wheel supports alpine/debian_ubuntu/rhel/suse; the arm64 wheel supports
    # alpine/debian_ubuntu/rhel (Microsoft does not ship a SUSE ARM64 driver tree).
    missing_trees = _REQUIRED_DRIVER_TREES[subdir] - driver_trees
    if missing_trees:
        errors.append(
            f"{base_name}: missing required Linux driver trees for '{subdir}': "
            f"{sorted(f'{distro}/{arch}' for distro, arch in missing_trees)}."
        )
    if not lib_dirs:
        errors.append(
            f"{base_name}: no mssql_python_odbc/libs/linux/*/*/lib directory found in a "
            f"Linux package."
        )
    for d in sorted(lib_dirs):
        if d not in dirs_with_driver:
            errors.append(f"{base_name}: '{d}' has no libmsodbcsql* driver.")
        if d not in dirs_with_inst:
            errors.append(f"{base_name}: '{d}' has no libodbcinst.so.2.")
    return errors, details


def validate_pe(
    base_name: str, index: dict[str, Any], members: Sequence[tuple[str, int | None]]
) -> tuple[list[str], list[str]]:
    subdir = str(index.get("subdir", ""))
    expected = _PE_SUBDIR_MACHINE[subdir]
    expected_driver_dir = _PE_DRIVER_DIR[subdir]
    details: list[str] = []
    errors = validate_native_contract((name for name, _ in members), index)
    native_seen = 0
    binding_seen = 0
    driver_dll_seen = 0
    auth_dll_seen = 0
    for name, machine in members:
        if not name.lower().endswith(_PE_SUFFIXES):
            continue
        native_seen += 1
        low = name.replace("\\", "/").lower()
        if "/mssql_python/" in low and "ddbc_bindings" in low and low.endswith(".pyd"):
            binding_seen += 1
        # The presence gate requires BOTH the CORE driver (msodbcsql18*.dll) AND its auth
        # companion (mssql-auth*.dll) specifically -- not just any vendored .dll. The loader
        # (ddbc_bindings.cpp) THROWS at connect if mssql-auth.dll is absent, so a package
        # missing it would pass CI (win-arm64 skips the runtime import) yet fail on EVERY
        # connect; a VC++ runtime or other support DLL satisfies neither category.
        if "/mssql_python_odbc/libs/windows/" in low and low.endswith(".dll"):
            base_low = os.path.basename(low)
            runtime_suffix = f"/mssql_python_odbc/libs/windows/{expected_driver_dir}/{base_low}"
            if base_low.startswith("msodbcsql18") and low.endswith(runtime_suffix):
                driver_dll_seen += 1
            elif base_low.startswith("mssql-auth") and low.endswith(runtime_suffix):
                auth_dll_seen += 1
        if machine is None:
            errors.append(f"{name}: not a valid PE binary (no MZ/PE header).")
            continue
        if machine != expected:
            errors.append(
                f"{name}: PE machine {_PE_MACHINES.get(machine, hex(machine))} "
                f"!= expected {_PE_MACHINES[expected]} for subdir '{subdir}'."
            )
        else:
            details.append(
                f"  {subdir}/{os.path.basename(name)}: PE machine={_PE_MACHINES[expected]} OK"
            )

    # Presence: assert BOTH required binary categories independently, not just >=1 native
    # file -- win-arm64 skips the runtime import, so this IS its presence gate. A package
    # with the binding .pyd but missing driver DLLs (or vice versa) must fail here.
    if native_seen == 0:
        errors.append(
            f"{base_name}: no .pyd/.dll found in a '{subdir}' package -- the native binding "
            f"(ddbc_bindings*.pyd) + the vendored ODBC driver DLLs must be present."
        )
    else:
        if binding_seen == 0:
            errors.append(
                f"{base_name}: no native binding (mssql_python/ddbc_bindings*.pyd) found in a "
                f"'{subdir}' package."
            )
        if driver_dll_seen == 0:
            errors.append(
                f"{base_name}: no vendored core ODBC driver DLL "
                f"(mssql_python_odbc/libs/windows/{expected_driver_dir}/msodbcsql18*.dll) "
                f"found in a "
                f"'{subdir}' package."
            )
        if auth_dll_seen == 0:
            errors.append(
                f"{base_name}: no vendored mssql-auth DLL "
                f"(mssql_python_odbc/libs/windows/{expected_driver_dir}/mssql-auth*.dll) found "
                f"in a '{subdir}' package -- the ODBC driver loader THROWS at connect if it "
                f"is absent."
            )
    return errors, details


def validate_macho(
    base_name: str, index: dict[str, Any], members: Sequence[tuple[str, set[str] | None]]
) -> tuple[list[str], list[str]]:
    subdir = str(index.get("subdir", ""))
    expected = _MACHO_SUBDIR_ARCH[subdir]
    details: list[str] = []
    errors = validate_native_contract((name for name, _ in members), index)
    binding_seen = 0
    target_driver_libraries: set[str] = set()
    for name, arches in members:
        low = name.replace("\\", "/").lower()
        if not low.endswith(_MACHO_SUFFIXES):
            continue
        base_low = os.path.basename(low)
        required_arch = None
        if "/mssql_python/" in low and base_low.startswith("ddbc_bindings") and low.endswith(".so"):
            binding_seen += 1
            required_arch = expected
        elif "/mssql_py_core/" in low and low.endswith(".so"):
            required_arch = expected
        elif any(
            f"/{package}/libs/macos/" in low for package in ("mssql_python_odbc", "mssql_py_core")
        ) and low.endswith(".dylib"):
            relative = low.split("/libs/macos/", 1)[1]
            parts = relative.split("/")
            driver_dir = parts[0]
            required_arch = _MACHO_DRIVER_DIR_ARCH.get(driver_dir)
            if required_arch is None:
                errors.append(f"{name}: unrecognized macOS driver architecture directory.")
                continue
            is_runtime_location = len(parts) == 3 and parts[1] == "lib"
            if (
                "/mssql_python_odbc/libs/macos/" in low
                and required_arch == expected
                and is_runtime_location
            ):
                target_driver_libraries.add(base_low)
        else:
            continue
        if arches is None:
            errors.append(f"{name}: not a valid, complete Mach-O binary.")
            continue
        if required_arch not in arches:
            errors.append(
                f"{name}: Mach-O arches {sorted(arches)} do NOT include the required "
                f"'{required_arch}' slice."
            )
        else:
            details.append(
                f"  {subdir}/{base_low}: arches={sorted(arches)} (has {required_arch}) OK"
            )

    # Presence gate (mirror the PE assert): osx-arm64 skips the runtime import, so this static
    # pass IS its arch+presence check. A package with the binding but no driver (or vice versa)
    # must fail here.
    if binding_seen == 0:
        errors.append(
            f"{base_name}: no native binding (mssql_python/ddbc_bindings*.so) found in a "
            f"'{subdir}' package."
        )
    missing_driver_libraries = sorted(_REQUIRED_DRIVER_LIBRARIES - target_driver_libraries)
    if missing_driver_libraries:
        errors.append(
            f"{base_name}: no vendored ODBC driver for '{expected}': incomplete runtime in "
            f"mssql_python_odbc/libs/macos/{expected}/lib; missing: "
            f"{', '.join(missing_driver_libraries)}."
        )
    return errors, details


def target_status(kind: Format, index: dict[str, Any], base_name: str) -> tuple[bool, list[str]]:
    """Decide applicability before touching payload bytes, preserving platform skip policy."""
    subdir = index.get("subdir")
    if kind == "elf":
        if not isinstance(subdir, str) or not subdir or subdir != subdir.strip():
            return False, [f"{base_name}: info/index.json is missing or invalid 'subdir'."]
        if not subdir.startswith("linux"):
            return False, []
        if subdir not in _SUBDIR_MACHINE:
            return False, [
                f"{base_name}: unrecognized Linux subdir '{subdir}' has no known ELF machine "
                f"mapping -- add it to _SUBDIR_MACHINE so the arch gate can enforce it "
                f"(refusing to skip the architecture check)."
            ]
        return True, []
    if kind == "pe":
        return str(subdir or "") in _PE_SUBDIR_MACHINE, []
    return str(subdir or "") in _MACHO_SUBDIR_ARCH, []
