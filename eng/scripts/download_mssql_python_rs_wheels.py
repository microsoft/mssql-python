#!/usr/bin/env python3
"""Download and stage mssql-python-rs wheels from the internal NuGet transport."""

from __future__ import annotations

import argparse
import shutil
import urllib.request
import zipfile
from pathlib import Path, PurePosixPath

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


def download_wheels(
    feed_url: str,
    distribution_version_file: Path,
    transport_version_file: Path,
    output_dir: Path,
) -> list[Path]:
    distribution_version = _read_version(distribution_version_file)
    transport_version = _read_version(transport_version_file)

    output_dir = output_dir.resolve()
    if output_dir == Path(output_dir.anchor):
        raise ValueError(f"Refusing to use a filesystem root as output directory: {output_dir}")
    if output_dir.exists() and not output_dir.is_dir():
        raise ValueError(f"Output directory is a file: {output_dir}")

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
        for entry in package.infolist():
            path = PurePosixPath(entry.filename)
            if len(path.parts) != 2 or path.parts[0] != "wheels" or path.suffix != ".whl":
                continue
            if not path.name.startswith(expected_prefix):
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

    package_path.unlink()
    if not staged:
        raise ValueError(f"No mssql-python-rs {distribution_version} wheels found in {package_url}")
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
