"""Source-bound component inputs and explicit published-wheel audit controls."""

from __future__ import annotations

import argparse
import ast
import json
import os
from pathlib import Path
from typing import Callable
import re
import subprocess
import sys
from urllib.error import URLError
from urllib.request import urlopen

from eng.scripts.download_mssql_python_rs_wheels import DEFAULT_FEED, PACKAGE_ID, file_sha256

from . import archive, contracts


def _is_sha256(value: object) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def _download_published(
    versions: dict[str, str],
    requirements: list[str],
    wheel_dir: Path,
    requirements_file: Path,
) -> dict[str, dict[str, str]]:
    published = {}
    for name, version in versions.items():
        try:
            with urlopen(f"https://pypi.org/pypi/{name}/{version}/json", timeout=60) as response:
                release = json.load(response)
        except URLError as error:
            raise ValueError(
                f"Cannot fetch published {name}=={version} from PyPI; no fallback is allowed."
            ) from error
        info = release.get("info") if isinstance(release, dict) else None
        urls = release.get("urls") if isinstance(release, dict) else None
        if (
            not isinstance(info, dict)
            or info.get("name") != name
            or info.get("version") != version
            or not isinstance(urls, list)
            or any(not isinstance(item, dict) for item in urls)
        ):
            raise ValueError(f"Invalid published wheel identity or SHA256 for {name}=={version}.")
        wheels = [
            item
            for item in urls
            if item.get("packagetype") == "bdist_wheel" and not item.get("yanked")
        ]
        if not wheels or any(
            not isinstance(item.get("filename"), str)
            or not isinstance(item.get("digests"), dict)
            or not _is_sha256(item["digests"].get("sha256"))
            for item in wheels
        ):
            raise ValueError(f"Invalid published wheel identity or SHA256 for {name}=={version}.")
        hashes = {item["filename"]: item["digests"]["sha256"] for item in wheels}
        if len(hashes) != len(wheels):
            raise ValueError(f"Ambiguous published wheel filenames for {name}=={version}.")
        published[name] = hashes
        requirements.append(
            f"{name}=={version} "
            + " ".join(f"--hash=sha256:{digest}" for digest in sorted(set(hashes.values())))
        )
    requirements_file.write_text("\n".join(requirements) + "\n", encoding="utf-8")
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "--isolated",
            "download",
            "--index-url",
            "https://pypi.org/simple",
            "--no-deps",
            "--only-binary=:all:",
            "--require-hashes",
            "-r",
            str(requirements_file),
            "-d",
            str(wheel_dir),
        ],
        check=True,
    )
    return published


def _published_wheel(
    wheel_dir: Path,
    name: str,
    version: str,
    hashes: dict[str, str],
    python_tag: str,
    subdir: str,
) -> archive.WheelMetadata:
    selected = list(wheel_dir.glob(f"{name.replace('-', '_')}-{version}-*.whl"))
    if len(selected) != 1:
        raise ValueError(f"Missing or ambiguous wheel for {name}=={version}.")
    path = selected[0]
    if hashes.get(path.name) != file_sha256(path):
        raise ValueError(f"Downloaded wheel SHA256 disagrees with published {path.name}.")
    metadata = archive.read_wheel_metadata(path)
    violations = contracts.validate_distribution_identity(metadata, name, version)
    violations.extend(contracts.validate_wheel_tags(path.name, metadata["tags"]))
    violations.extend(contracts.validate_wheel_core_ownership(metadata))
    if name == "mssql-python":
        matches = contracts.binding_wheel_matches_target(
            path.name, subdir, [python_tag.removeprefix("cp")]
        )
    elif name == "mssql-python-odbc":
        matches = contracts.odbc_wheel_matches_target(path.name, subdir)
    else:
        matches = contracts.rs_wheel_matches_target(path.name, python_tag, subdir)
    if not matches:
        violations.append(
            f"Published {name} wheel does not match the requested {python_tag} {subdir} target."
        )
    expected = f"{name.replace('-', '_')}-{version}.dist-info/METADATA"
    if [member for member in metadata["members"] if member.endswith(".dist-info/METADATA")] != [
        expected
    ]:
        violations.append(f"Expected one matching root METADATA in {path.name}.")
    if violations:
        raise ValueError("; ".join(violations))
    return metadata


def fetch_wheels(
    source_versions: dict[str, str],
    wheel_dir: Path,
    requirements_file: Path,
    python_tag: str,
    subdir: str,
) -> dict[str, str]:
    """Fetch exact public inputs; the returned component set identifies the actual profile.

    A published embedded-core wheel is a historical packaging control, not proof of
    the current source's separate-RS contract. Recorded producer inputs do not use
    this public-profile selection.
    """
    if not re.fullmatch(r"cp3\d+", python_tag):
        raise ValueError(f"Expected a normal CPython target tag, not {python_tag!r}.")
    wheel_dir.mkdir(parents=True, exist_ok=True)
    versions = {name: source_versions[name] for name in ("mssql-python", "mssql-python-odbc")}
    requirements: list[str] = []
    hashes = _download_published(versions, requirements, wheel_dir, requirements_file)
    binding = _published_wheel(
        wheel_dir,
        "mssql-python",
        versions["mssql-python"],
        hashes["mssql-python"],
        python_tag,
        subdir,
    )
    _published_wheel(
        wheel_dir,
        "mssql-python-odbc",
        versions["mssql-python-odbc"],
        hashes["mssql-python-odbc"],
        python_tag,
        subdir,
    )
    if (
        contracts.exact_dependency_pin(binding["requires_dist"], "mssql-python-odbc")
        != versions["mssql-python-odbc"]
    ):
        raise ValueError("Binding METADATA must require exactly the selected ODBC wheel version.")
    rs_version = contracts.binding_rs_version(
        binding, binding["members"], binding["record_members"]
    )
    if rs_version is None:
        owned = contracts.owned_core_members(binding["members"], binding["record_members"])
        violations = contracts.validate_core_layout(owned, python_tag, subdir)
    else:
        if source_versions.get("mssql-python-rs") != rs_version:
            raise ValueError(
                "Published binding RS requirement differs from the maintained source pin."
            )
        versions["mssql-python-rs"] = rs_version
        hashes.update(
            _download_published(
                {"mssql-python-rs": rs_version}, requirements, wheel_dir, requirements_file
            )
        )
        rs = _published_wheel(
            wheel_dir, "mssql-python-rs", rs_version, hashes["mssql-python-rs"], python_tag, subdir
        )
        violations = contracts.validate_rs_ownership(
            rs, rs["members"], rs["record_members"], rs_version, python_tag, subdir
        )
    if violations:
        raise ValueError("; ".join(violations))
    if len(list(wheel_dir.glob("*.whl"))) != len(versions):
        raise ValueError("Expected exactly the selected binding, ODBC and required RS wheels.")
    return versions


def validate_installed_inputs(
    path: str,
    python_tag: str,
    subdir: str,
    versions: dict[str, str],
    receipt: dict | None,
) -> None:
    """Bind installed component identities/ownership to verified producer source inputs."""
    names, files = [], {}
    for name, data in archive.iter_payload_members(path):
        names.append(name)
        if ".dist-info/" in name:
            files[name] = data
    if len(names) != len(set(names)):
        raise ValueError(f"{path}: duplicate payload members")
    metadata = {}
    root = (
        "Lib/site-packages/"
        if subdir.startswith("win-")
        else (f"lib/python{python_tag[2]}.{python_tag[3:]}/site-packages/")
    )
    for facts, present, owned, prefix in archive.installed_metadata(names, files):
        name = contracts.canonical_distribution_name(facts["name"])
        if name in metadata:
            raise ValueError(f"{path}: duplicate installed metadata for {name}")
        metadata[name] = facts, present, owned, prefix
        if name in versions:
            expected = root + f"{name.replace('-', '_')}-{versions[name]}.dist-info/"
            errors = contracts.validate_distribution_identity(facts, name, versions[name])
            if (
                prefix != expected
                or prefix + "RECORD" not in files
                or prefix + "WHEEL" not in files
            ):
                errors.append(f"Expected canonical installed METADATA/RECORD/WHEEL for {name}.")
            if errors:
                raise ValueError(f"{path}: " + "; ".join(errors))
    if set(metadata) != set(versions):
        raise ValueError(
            f"{path}: installed components {sorted(metadata)} differ from source {sorted(versions)}"
        )
    binding, present, owned, _ = metadata["mssql-python"]
    if (
        contracts.exact_dependency_pin(binding["requires_dist"], "mssql-python-odbc")
        != versions["mssql-python-odbc"]
    ):
        raise ValueError(f"{path}: binding does not require the source ODBC version")
    rs_version = contracts.binding_rs_version(
        binding, present, owned, versions.get("mssql-python-rs")
    )
    if rs_version != versions.get("mssql-python-rs"):
        raise ValueError(f"{path}: installed binding RS profile differs from producer source")
    errors = contracts.validate_core_ownership(
        names,
        {name: component[2] for name, component in metadata.items()},
        "mssql-python-rs" if rs_version is not None else "mssql-python",
        root=root,
    )
    if errors:
        raise ValueError(f"{path}: " + "; ".join(errors))
    if rs_version is None:
        errors = contracts.validate_core_layout(
            contracts.owned_core_members(present, owned), python_tag, subdir
        )
    else:
        rs, present, owned, prefix = metadata["mssql-python-rs"]
        errors = contracts.validate_rs_ownership(rs, present, owned, rs_version, python_tag, subdir)
        source = files.get(prefix + "conda-wheel-source.txt", b"").decode("utf-8")
        filename = source.removesuffix("\n")
        if receipt is None or source != filename + "\n" or filename not in receipt["wheel_sha256"]:
            errors.append("Selected RS wheel is absent from the verified transport receipt.")
        errors.extend(
            contracts.validate_wheel_tags(
                filename, archive.parse_wheel_tags(files[prefix + "WHEEL"])
            )
        )
        if not contracts.rs_wheel_matches_target(filename, python_tag, subdir):
            errors.append("Selected RS wheel does not match the Conda Python/platform target.")
    if errors:
        raise ValueError(f"{path}: " + "; ".join(errors))


def add_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--wheel-dir", type=Path, required=True)
    parser.add_argument("--requirements-file", type=Path, required=True)
    parser.add_argument(
        "--python-tag",
        required=True,
        help="Expected normal CPython tag (e.g. cp311), not a pip cross-target override.",
    )
    parser.add_argument(
        "--conda-subdir",
        required=True,
        help="Expected native Conda subdir; pip downloads for the executing interpreter/host.",
    )


def fetch_cli(args: argparse.Namespace) -> int:
    try:
        source = read_release_versions(lambda path: Path(path).read_text(encoding="utf-8"))
        versions = fetch_wheels(
            source, args.wheel_dir, args.requirements_file, args.python_tag, args.conda_subdir
        )
        rs_required = "mssql-python-rs" in versions
        if output := os.environ.get("GITHUB_OUTPUT"):
            with open(output, "a", encoding="utf-8") as destination:
                destination.write(f"rsRequired={str(rs_required).lower()}\n")
    except (*archive.READ_ERRORS, subprocess.CalledProcessError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    print("PUBLIC_WHEEL_INPUT_OK: " + json.dumps(versions, sort_keys=True))
    print(
        "Published RS-dependent input contract verified; no installed Conda runtime claim."
        if rs_required
        else "Historical published packaging control; NOT current-source RS qualification."
    )
    return 0


def read_release_versions(read_source: Callable[[str], str]) -> dict[str, str]:
    """Read the literals maintained by the existing wheel release process, without imports."""
    setup_source = read_source("setup.py")
    versions = {}
    for path, field in (
        ("setup.py", "version"),
        ("mssql_python/__init__.py", "__version__"),
        ("mssql_python_odbc/__init__.py", "__version__"),
    ):
        matches = re.findall(
            rf"""(?m)^\s*{field}\s*=\s*['"]([A-Za-z0-9][A-Za-z0-9._-]*)['"]\s*,?\s*(?:#.*)?$""",
            setup_source if path == "setup.py" else read_source(path),
        )
        if len(matches) != 1:
            raise ValueError(f"{path} must contain exactly one literal {field} release version.")
        versions[path] = matches[0]
    if versions["setup.py"] != versions["mssql_python/__init__.py"]:
        raise ValueError(
            "Binding release versions in setup.py and mssql_python/__init__.py differ."
        )
    result = {
        "mssql-python": versions["setup.py"],
        "mssql-python-odbc": versions["mssql_python_odbc/__init__.py"],
    }
    if _source_requires_rs(setup_source):
        path = "eng/versions/mssql-python-rs.version"
        version = read_source(path).strip()
        if not re.fullmatch(r"[0-9][A-Za-z0-9.!+_-]*", version):
            raise ValueError(f"{path} must contain one RS distribution release version.")
        result["mssql-python-rs"] = version
    return result


def _source_requires_rs(setup_source: str) -> bool:
    """Recognize only the explicit maintained dependency contract, never execute setup.py."""
    rs_dependencies = []
    try:
        setup_tree = ast.parse(setup_source)
    except SyntaxError:
        raise ValueError(
            "setup.py is not valid Python; its dependency contract cannot be read."
        ) from None
    declarations = [
        node.value
        for node in ast.walk(setup_tree)
        if isinstance(node, ast.keyword) and node.arg == "install_requires"
    ]
    if len(declarations) != 1:
        raise ValueError("setup.py must declare exactly one explicit install_requires contract.")
    dependencies = declarations[0]
    if not isinstance(dependencies, (ast.List, ast.Tuple)):
        raise ValueError("setup.py install_requires must be an explicit dependency list.")
    for dependency in dependencies.elts:
        prefix = (
            dependency.values[0]
            if isinstance(dependency, ast.JoinedStr) and dependency.values
            else dependency
        )
        if (
            not isinstance(prefix, ast.Constant)
            or not isinstance(prefix.value, str)
            or not prefix.value.strip()
            or (
                isinstance(dependency, ast.JoinedStr)
                and not re.match(r"^[A-Za-z0-9][A-Za-z0-9._-]*[<=>!~]", prefix.value)
            )
        ):
            raise ValueError("setup.py dependencies must have explicit distribution names.")
        if any(
            isinstance(part, ast.Constant)
            and isinstance(part.value, str)
            and re.match(r"(?i)^mssql[-_.]+python[-_.]+rs(?:[<=>!~;\s(\[]|$)", part.value.strip())
            for part in ast.walk(dependency)
        ):
            rs_dependencies.append(dependency)
    if rs_dependencies:
        expected = ast.parse(
            'f"mssql-python-rs=={_read_mssql_python_rs_version()}"', mode="eval"
        ).body
        if len(rs_dependencies) != 1 or ast.dump(rs_dependencies[0]) != ast.dump(expected):
            raise ValueError("setup.py must require the exact maintained RS distribution pin.")
    return bool(rs_dependencies)


def parse_release_versions(raw: str) -> dict[str, str]:
    versions = json.loads(raw)
    required = {"mssql-python", "mssql-python-odbc"}
    if (
        not isinstance(versions, dict)
        or not required.issubset(versions)
        or set(versions) - required - {"mssql-python-rs"}
        or any(
            not isinstance(version, str)
            or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.!+_-]*", version) is None
            for version in versions.values()
        )
    ):
        raise ValueError("Expected exact binding/ODBC and optional RS distribution versions.")
    return versions


def validate_rs_transport(
    receipt: object, versions: dict[str, str], transport_version: str
) -> None:
    """Check the selected producer's receipt against its separately verified source pins."""
    version = versions.get("mssql-python-rs")
    if version is None:
        if receipt is not None or transport_version:
            raise ValueError("An embedded-core producer must not supply an RS transport receipt.")
        return
    if not isinstance(receipt, dict):
        raise ValueError("The RS producer requires its recorded rs-transport.json receipt.")
    expected = {
        "distribution_version": version,
        "transport_version": transport_version,
        "feed_url": DEFAULT_FEED,
        "package_id": PACKAGE_ID,
    }
    if not transport_version or any(receipt.get(key) != value for key, value in expected.items()):
        raise ValueError(
            "RS transport receipt differs from the verified producer's source pins/feed."
        )

    wheels = receipt.get("wheel_sha256")
    if not _is_sha256(receipt.get("package_sha256")) or not isinstance(wheels, dict) or not wheels:
        raise ValueError("RS transport receipt requires its package and wheel SHA256 values.")
    for name, digest in wheels.items():
        if (
            not isinstance(name, str)
            or "/" in name
            or "\\" in name
            or not name.startswith(f"mssql_python_rs-{version}-")
            or not name.endswith(".whl")
            or not _is_sha256(digest)
        ):
            raise ValueError("RS transport receipt contains an invalid wheel identity or SHA256.")
