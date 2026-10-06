#!/usr/bin/env python3
"""Download and stage mssql-python-rs wheels from the internal NuGet transport."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import urllib.request
import zipfile
from xml.etree import ElementTree
from pathlib import Path, PurePosixPath

if __package__:
    from . import mssql_python_build_safety
    from .resolve_nuget_feed import resolve
else:
    import mssql_python_build_safety
    from resolve_nuget_feed import resolve

DEFAULT_FEED = (
    "https://pkgs.dev.azure.com/sqlclientdrivers/public/"
    "_packaging/mssql-rs_Public/nuget/v3/index.json"
)
PACKAGE_ID = "mssql-python-rs-wheels"


def _read_version(version_file: Path) -> str:
    version = version_file.read_text(encoding="ascii").strip()
    if not version:
        raise ValueError(f"Version file is empty: {version_file}")
    return version


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def download_wheels(
    feed_url: str,
    distribution_version_file: Path,
    transport_version_file: Path,
    output_dir: Path,
) -> list[Path]:
    distribution_version = _read_version(distribution_version_file)
    transport_version = _read_version(transport_version_file)

    output_dir = getattr(mssql_python_build_safety, "resolve_safe_output_directory")(output_dir)

    package_base = resolve(feed_url).rstrip("/") + "/"
    normalized_version = transport_version.lower()
    package_url = (
        f"{package_base}{PACKAGE_ID}/{normalized_version}/"
        f"{PACKAGE_ID}.{normalized_version}.nupkg"
    )

    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)
    package_path = output_dir / f"{PACKAGE_ID}.{normalized_version}.nupkg"
    with urllib.request.urlopen(package_url, timeout=120) as response:
        with package_path.open("wb") as destination:
            shutil.copyfileobj(response, destination)

    staged: list[Path] = []
    seen: set[str] = set()
    expected_prefix = f"mssql_python_rs-{distribution_version}-"
    with zipfile.ZipFile(package_path) as package:
        nuspecs = [name for name in package.namelist() if name.endswith(".nuspec")]
        if len(nuspecs) != 1:
            raise ValueError("RS transport must contain exactly one Nuspec")
        metadata = ElementTree.fromstring(package.read(nuspecs[0])).find("{*}metadata")
        if (
            metadata is None
            or metadata.findtext("{*}id") != PACKAGE_ID
            or metadata.findtext("{*}version", "").lower() != normalized_version
        ):
            raise ValueError("RS transport Nuspec identity does not match the pinned package")
        description = metadata.findtext("{*}description", "")
        for entry in package.infolist():
            path = PurePosixPath(entry.filename)
            if len(path.parts) != 2 or path.parts[0] != "wheels" or path.suffix != ".whl":
                continue
            if not path.name.startswith(expected_prefix) or "\\" in path.name:
                raise ValueError(
                    "Unexpected wheel name for mssql-python-rs "
                    f"{distribution_version}: {path.name}"
                )
            if path.name in seen:
                raise ValueError(f"Duplicate wheel filename in NuGet package: {path.name}")
            seen.add(path.name)
            destination = output_dir / path.name
            with package.open(entry) as source, destination.open("wb") as target:
                shutil.copyfileobj(source, target)
            staged.append(destination)

    if not staged:
        raise ValueError(f"No mssql-python-rs {distribution_version} wheels found in {package_url}")
    receipt = {
        "distribution_version": distribution_version,
        "transport_version": transport_version,
        "feed_url": feed_url,
        "package_id": PACKAGE_ID,
        "package_sha256": file_sha256(package_path),
        "nuspec_description": description,
        "wheel_sha256": {path.name: file_sha256(path) for path in sorted(staged)},
    }
    (output_dir / "transport.json").write_text(
        json.dumps(receipt, indent=2) + "\n", encoding="utf-8", newline="\n"
    )
    package_path.unlink()
    return sorted(staged)


def main() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--feed-url", default=DEFAULT_FEED)
    parser.add_argument(
        "--distribution-version-file",
        type=Path,
        default=repo_root / "eng" / "versions" / "mssql-python-rs.version",
    )
    parser.add_argument(
        "--transport-version-file",
        type=Path,
        default=repo_root / "eng" / "versions" / "mssql-python-rs-nuget.version",
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

    wheels = download_wheels(
        args.feed_url,
        args.distribution_version_file,
        args.transport_version_file,
        args.output_dir,
    )
    for wheel in wheels:
        print(wheel)


if __name__ == "__main__":
    main()
