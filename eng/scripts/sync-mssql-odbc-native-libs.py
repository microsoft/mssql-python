#!/usr/bin/env python
"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.

Sync mssql_python_rust_odbc/libs/ from the mssql-odbc-native NuGet package.

Unlike eng/scripts/install-mssql-py-core.*, which fetches only the CURRENT
host's platform binary for local dev/CI, this script is a maintainer/pipeline
operation: it downloads every packaged platform's driver binary at once and
lays them out under mssql_python_rust_odbc/libs/, matching exactly what
GetDriverPathCpp() (mssql_python/pybind/ddbc_bindings.cpp) resolves for the
"mssql-odbc" provider. The resulting libs/ tree is the committed source of
truth for setup_rust_odbc.py (mirroring mssql_python_odbc/libs/) and must be
`git add`ed after running this script.

The mssql-odbc-native package (built by mssql-rs's odbc-native-stages.yml) has
no macOS lane yet, and ships THREE Linux glibc variants; only two are used
here:
  - glibc228_*  (built on manylinux_2_28 / AlmaLinux 8) -> libs/linux/glibc/
  - musl_*      (Alpine)                                -> libs/linux/musl/
  - linux_*     (newer host glibc, for mssql-rs's own distro-container tests)
                is NOT manylinux-portable and is intentionally skipped.

Usage:
    python eng/scripts/sync-mssql-odbc-native-libs.py [--feed-url URL] [--version VERSION]
"""

import argparse
import shutil
import sys
import tempfile
import zipfile
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent

sys.path.insert(0, str(SCRIPT_DIR))
from resolve_nuget_feed import resolve  # noqa: E402

DEFAULT_FEED_URL = "https://pkgs.dev.azure.com/sqlclientdrivers/public/_packaging/mssql-rs_Public/nuget/v3/index.json"
PACKAGE_ID = "mssql-odbc-native"

# NuGet tag inside native/<tag>/<file> -> destination under mssql_python_rust_odbc/libs/.
# Skips linux_x64 / linux_arm64 (see module docstring).
_TAG_TO_DEST = {
    "glibc228_x64": ("mssqlodbc.so", "linux/glibc/x86_64/lib"),
    "glibc228_arm64": ("mssqlodbc.so", "linux/glibc/arm64/lib"),
    "musl_x64": ("mssqlodbc.so", "linux/musl/x86_64/lib"),
    "musl_arm64": ("mssqlodbc.so", "linux/musl/arm64/lib"),
    "windows_x64": ("mssqlodbc.dll", "windows/x64"),
    "windows_arm64": ("mssqlodbc.dll", "windows/arm64"),
}


def _read_version(version_arg: str) -> str:
    if version_arg:
        return version_arg
    version_file = REPO_ROOT / "eng" / "versions" / "mssql-odbc-native.version"
    if not version_file.exists():
        raise SystemExit(f"Version file not found: {version_file}")
    version = version_file.read_text(encoding="utf-8").strip()
    if not version:
        raise SystemExit(f"Version file is empty: {version_file}")
    return version


def _download_nupkg(feed_url: str, version: str, dest_dir: Path) -> Path:
    print(f"Resolving feed: {feed_url}")
    package_base_url = resolve(feed_url)
    version_lower = version.lower()
    nupkg_url = f"{package_base_url}{PACKAGE_ID}/{version_lower}/{PACKAGE_ID}.{version_lower}.nupkg"
    nupkg_path = dest_dir / f"{PACKAGE_ID}.{version_lower}.nupkg"

    print(f"Downloading: {nupkg_url}")
    import urllib.request

    with urllib.request.urlopen(nupkg_url, timeout=120) as resp, open(nupkg_path, "wb") as out:
        shutil.copyfileobj(resp, out)
    size_mb = nupkg_path.stat().st_size / (1024 * 1024)
    print(f"Downloaded: {nupkg_path} ({size_mb:.2f} MB)")
    return nupkg_path


def _sync_libs(extract_dir: Path, libs_dir: Path) -> None:
    native_dir = extract_dir / "native"
    if not native_dir.is_dir():
        raise SystemExit(
            f"No 'native' directory found in NuGet package (extracted to {extract_dir})"
        )

    missing = []
    for tag, (filename, dest_subpath) in _TAG_TO_DEST.items():
        src = native_dir / tag / filename
        if not src.is_file():
            missing.append(tag)
            continue
        dest_dir = libs_dir / dest_subpath
        dest_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest_dir / filename)
        print(f"Synced {tag}: {src} -> {dest_dir / filename}")

    if missing:
        raise SystemExit(f"Missing driver binary for tag(s): {', '.join(missing)}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--feed-url", default=DEFAULT_FEED_URL)
    parser.add_argument(
        "--version", default="", help="Override eng/versions/mssql-odbc-native.version"
    )
    args = parser.parse_args()

    version = _read_version(args.version)
    print(f"mssql-odbc-native version: {version}")

    libs_dir = REPO_ROOT / "mssql_python_rust_odbc" / "libs"

    with tempfile.TemporaryDirectory(prefix="mssql-odbc-native-") as tmp:
        tmp_path = Path(tmp)
        nupkg_path = _download_nupkg(args.feed_url, version, tmp_path)

        extract_dir = tmp_path / "extracted"
        with zipfile.ZipFile(nupkg_path) as zf:
            zf.extractall(extract_dir)

        _sync_libs(extract_dir, libs_dir)

    print("=== mssql_python_rust_odbc/libs/ synced successfully ===")
    print("Review and 'git add mssql_python_rust_odbc/libs/' to commit the updated binaries.")


if __name__ == "__main__":
    main()
