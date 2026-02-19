#!/usr/bin/env python3
"""
Repackage mssql_py_core binaries into mssql-python wheels.

Downloads the mssql-py-core-wheels NuGet package, extracts the matching
mssql_py_core native extension for each mssql-python wheel, and injects
it into the wheel so that mssql_py_core is bundled inside mssql-python.

Usage:
    python repackage-with-mssql-py-core.py --wheel-dir <dir> [--nuget-dir <dir>] [--feed-url <url>]

The NuGet package version is read from eng/versions/mssql-py-core.version.
If --nuget-dir is provided, it skips the download and uses pre-extracted wheels.
"""

import argparse
import csv
import hashlib
import io
import json
import os
import re
import shutil
import sys
import tempfile
import urllib.request
import zipfile
from base64 import urlsafe_b64encode
from pathlib import Path


def parse_wheel_filename(filename: str) -> dict | None:
    """Parse a wheel filename into its components."""
    # Format: {name}-{version}(-{build})?-{python}-{abi}-{platform}.whl
    m = re.match(
        r"^(?P<name>[A-Za-z0-9_]+)-(?P<version>[^-]+)"
        r"(?:-(?P<build>\d[^-]*))?"
        r"-(?P<python>[^-]+)-(?P<abi>[^-]+)-(?P<platform>.+)\.whl$",
        filename,
    )
    return m.groupdict() if m else None


def compute_record_hash(data: bytes) -> str:
    """Compute sha256 hash in RECORD format: sha256=<urlsafe-b64-digest>."""
    digest = hashlib.sha256(data).digest()
    return "sha256=" + urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def find_matching_core_wheel(
    mssql_python_whl: str, core_wheels_dir: Path
) -> Path | None:
    """Find the mssql_py_core wheel matching a mssql-python wheel's platform tags."""
    parsed = parse_wheel_filename(os.path.basename(mssql_python_whl))
    if not parsed:
        return None

    python_tag = parsed["python"]   # e.g., cp313
    platform_tag = parsed["platform"]  # e.g., win_amd64

    # Look for matching core wheel
    pattern = f"mssql_py_core-*-{python_tag}-{python_tag}-{platform_tag}.whl"
    matches = list(core_wheels_dir.glob(pattern))

    if not matches:
        # Try without exact abi match (some wheels use cpXXX for both)
        for whl in core_wheels_dir.glob("mssql_py_core-*.whl"):
            core_parsed = parse_wheel_filename(whl.name)
            if core_parsed and core_parsed["platform"] == platform_tag and core_parsed["python"] == python_tag:
                return whl
        return None

    return matches[0]


def inject_core_into_wheel(
    mssql_python_whl: Path, core_whl: Path, output_dir: Path
) -> Path:
    """
    Inject mssql_py_core files from core_whl into mssql_python_whl.

    Copies the mssql_py_core/ package and mssql_py_core.libs/ (if present)
    into the mssql-python wheel and updates the RECORD file.
    """
    output_path = output_dir / mssql_python_whl.name

    # Read the core wheel contents we want to inject
    inject_files: dict[str, bytes] = {}
    with zipfile.ZipFile(core_whl, "r") as core_zip:
        for entry in core_zip.namelist():
            # Include mssql_py_core/ package files and mssql_py_core.libs/
            if entry.startswith("mssql_py_core/") or entry.startswith("mssql_py_core.libs/"):
                inject_files[entry] = core_zip.read(entry)

    if not inject_files:
        print(f"  WARNING: No mssql_py_core files found in {core_whl.name}")
        shutil.copy2(mssql_python_whl, output_path)
        return output_path

    # Build new wheel with injected files
    with zipfile.ZipFile(mssql_python_whl, "r") as src_zip:
        with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as dst_zip:
            # Find the dist-info directory name for mssql-python
            dist_info_dir = None
            for name in src_zip.namelist():
                if name.endswith("/RECORD"):
                    dist_info_dir = name.rsplit("/", 1)[0]
                    break

            if not dist_info_dir:
                raise ValueError(f"No RECORD found in {mssql_python_whl.name}")

            record_path = f"{dist_info_dir}/RECORD"
            new_record_entries: list[str] = []

            # Copy all existing files (except RECORD, we'll regenerate it)
            for item in src_zip.infolist():
                if item.filename == record_path:
                    # Parse existing RECORD entries (we'll append to them)
                    existing_record = src_zip.read(item.filename).decode("utf-8")
                    for line in existing_record.strip().split("\n"):
                        line = line.strip()
                        if line and not line.startswith(record_path):
                            new_record_entries.append(line)
                    continue

                data = src_zip.read(item.filename)
                dst_zip.writestr(item, data)

            # Inject mssql_py_core files
            for filename, data in inject_files.items():
                dst_zip.writestr(filename, data)
                file_hash = compute_record_hash(data)
                new_record_entries.append(f"{filename},{file_hash},{len(data)}")

            # Write updated RECORD
            new_record_entries.append(f"{record_path},,")
            record_content = "\n".join(new_record_entries) + "\n"
            dst_zip.writestr(record_path, record_content)

    return output_path


def download_nuget_package(
    feed_url: str, package_id: str, version: str, output_dir: Path
) -> Path:
    """Download and extract a NuGet package from an Azure Artifacts feed."""
    print(f"Resolving NuGet feed: {feed_url}")
    with urllib.request.urlopen(feed_url) as resp:
        feed_index = json.loads(resp.read().decode())

    package_base_url = None
    for resource in feed_index.get("resources", []):
        if "PackageBaseAddress" in resource.get("@type", ""):
            package_base_url = resource["@id"]
            break

    if not package_base_url:
        raise RuntimeError("Could not resolve PackageBaseAddress from feed")

    print(f"Package base URL: {package_base_url}")

    version_lower = version.lower()
    pkg_id_lower = package_id.lower()
    nupkg_url = f"{package_base_url}{pkg_id_lower}/{version_lower}/{pkg_id_lower}.{version_lower}.nupkg"

    nupkg_path = output_dir / f"{pkg_id_lower}.{version_lower}.nupkg"
    print(f"Downloading: {nupkg_url}")
    urllib.request.urlretrieve(nupkg_url, nupkg_path)
    size_mb = nupkg_path.stat().st_size / (1024 * 1024)
    print(f"Downloaded: {nupkg_path.name} ({size_mb:.1f} MB)")

    # Extract (NuGet packages are ZIP files)
    extract_dir = output_dir / "extracted"
    with zipfile.ZipFile(nupkg_path, "r") as z:
        z.extractall(extract_dir)

    wheels_dir = extract_dir / "wheels"
    if not wheels_dir.is_dir():
        raise RuntimeError(
            f"No 'wheels' directory in NuGet package. Contents: {list(extract_dir.iterdir())}"
        )

    return wheels_dir


def main():
    parser = argparse.ArgumentParser(
        description="Repackage mssql_py_core binaries into mssql-python wheels"
    )
    parser.add_argument(
        "--wheel-dir",
        required=True,
        help="Directory containing mssql-python wheels to repackage",
    )
    parser.add_argument(
        "--nuget-dir",
        help="Directory containing pre-extracted mssql_py_core wheels (skips download)",
    )
    parser.add_argument(
        "--feed-url",
        default="https://pkgs.dev.azure.com/sqlclientdrivers/public/_packaging/mssql-rs_Public/nuget/v3/index.json",
        help="NuGet v3 feed URL",
    )
    parser.add_argument(
        "--version-file",
        help="Path to mssql-py-core.version file (default: auto-detect from repo)",
    )
    parser.add_argument(
        "--output-dir",
        help="Output directory for repackaged wheels (default: overwrite in place)",
    )
    args = parser.parse_args()

    wheel_dir = Path(args.wheel_dir)
    if not wheel_dir.is_dir():
        print(f"ERROR: Wheel directory not found: {wheel_dir}")
        sys.exit(1)

    # Find mssql-python wheels
    mssql_python_wheels = sorted(wheel_dir.glob("mssql_python-*.whl"))
    if not mssql_python_wheels:
        print(f"ERROR: No mssql_python-*.whl files found in {wheel_dir}")
        sys.exit(1)

    print(f"Found {len(mssql_python_wheels)} mssql-python wheel(s)")

    # Get or download mssql_py_core wheels
    temp_dir = None
    if args.nuget_dir:
        core_wheels_dir = Path(args.nuget_dir)
    else:
        # Find version file
        if args.version_file:
            version_file = Path(args.version_file)
        else:
            # Auto-detect from repo root
            script_dir = Path(__file__).resolve().parent
            version_file = script_dir / ".." / "versions" / "mssql-py-core.version"

        if not version_file.is_file():
            print(f"ERROR: Version file not found: {version_file}")
            sys.exit(1)

        version = version_file.read_text().strip()
        print(f"mssql-py-core version: {version}")

        temp_dir = Path(tempfile.mkdtemp(prefix="mssql-py-core-"))
        core_wheels_dir = download_nuget_package(
            args.feed_url, "mssql-py-core-wheels", version, temp_dir
        )

    print(f"Core wheels directory: {core_wheels_dir}")
    core_wheel_count = len(list(core_wheels_dir.glob("*.whl")))
    print(f"Available mssql_py_core wheels: {core_wheel_count}")

    # Set up output directory
    if args.output_dir:
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
    else:
        # In-place: use temp output, then replace originals
        output_dir = Path(tempfile.mkdtemp(prefix="repackaged-"))

    # Repackage each wheel
    repackaged = 0
    skipped = 0
    for whl in mssql_python_wheels:
        print(f"\nProcessing: {whl.name}")
        core_whl = find_matching_core_wheel(whl.name, core_wheels_dir)

        if core_whl is None:
            parsed = parse_wheel_filename(whl.name)
            platform = parsed["platform"] if parsed else "unknown"
            # musllinux wheels have no matching core wheel yet — skip gracefully
            if "musllinux" in platform:
                print(f"  SKIP: No musllinux mssql_py_core wheel available for {platform}")
                if not args.output_dir:
                    # In-place mode: copy unchanged
                    shutil.copy2(whl, output_dir / whl.name)
                skipped += 1
                continue
            else:
                print(f"  ERROR: No matching mssql_py_core wheel for {whl.name}")
                sys.exit(1)

        print(f"  Matched: {core_whl.name}")
        result = inject_core_into_wheel(whl, core_whl, output_dir)
        print(f"  Repackaged: {result.name}")
        repackaged += 1

    # If in-place mode, replace originals
    if not args.output_dir:
        for repackaged_whl in output_dir.glob("*.whl"):
            dest = wheel_dir / repackaged_whl.name
            shutil.move(str(repackaged_whl), str(dest))
        shutil.rmtree(output_dir, ignore_errors=True)

    # Cleanup temp NuGet download
    if temp_dir:
        shutil.rmtree(temp_dir, ignore_errors=True)

    print(f"\n{'='*50}")
    print(f"Repackaging complete!")
    print(f"  Repackaged: {repackaged} wheel(s)")
    print(f"  Skipped:    {skipped} wheel(s) (no matching core wheel)")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()
