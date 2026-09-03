import os
import re
import sys
from pathlib import Path

from setuptools import setup, find_packages
from setuptools.dist import Distribution
from wheel.bdist_wheel import bdist_wheel

PROJECT_ROOT = Path(__file__).resolve().parent


def _read_odbc_version() -> str:
    """Return the ``mssql-python-odbc`` version -- the single source of truth for
    the ODBC dependency pin.

    Primary source is ``mssql_python_odbc/__init__.py`` in the checkout (the same
    ``__version__`` ``setup_odbc.py`` uses for the wheel version and the
    ``pr-validation`` pipeline reads for the driver version), parsed by regex
    without importing the package.

    Fallback: the multi-platform build pipeline deletes the committed
    ``mssql_python_odbc/`` directory before building the mssql-python wheel (so
    pytest resolves the driver from the installed wheel, not the checkout). In
    that window the ``mssql-python-odbc`` wheel is already pip-installed, so read
    the version from its installed metadata -- which itself came from the same
    ``__init__.py``. Either way it is one version, one place.
    """
    init_file = PROJECT_ROOT / "mssql_python_odbc" / "__init__.py"
    if init_file.is_file():
        text = init_file.read_text(encoding="utf-8")
        match = re.search(r'^__version__\s*=\s*["\']([^"\']+)["\']', text, re.MULTILINE)
        if match:
            return match.group(1)

    # The checkout copy is absent (removed by the wheel-build stage); fall back to
    # the already-installed mssql-python-odbc package metadata.
    from importlib.metadata import version, PackageNotFoundError

    try:
        return version("mssql-python-odbc")
    except PackageNotFoundError:
        raise SystemExit(
            "Could not determine the mssql-python-odbc version: neither "
            f"{init_file} exists nor is the mssql-python-odbc package installed."
        )


def _read_mssql_python_rs_version() -> str:
    """Return the pinned ``mssql-python-rs`` dependency version."""
    version_file = PROJECT_ROOT / "eng" / "versions" / "mssql-python-rs.version"
    if not version_file.is_file():
        raise SystemExit(f"Could not determine the mssql-python-rs version: {version_file} missing.")

    version = version_file.read_text(encoding="utf-8").strip()
    if not version:
        raise SystemExit(f"Could not determine the mssql-python-rs version: {version_file} empty.")
    return version


# Custom distribution to force platform-specific wheel
class BinaryDistribution(Distribution):
    def has_ext_modules(self):
        return True


def get_platform_info():
    """Get platform-specific architecture and platform tag information."""
    if sys.platform.startswith("win"):
        # Get architecture from environment variable or default to x64
        arch = os.environ.get("ARCHITECTURE", "x64")
        # Strip quotes if present
        if isinstance(arch, str):
            arch = arch.strip("\"'")

        # Normalize architecture values
        if arch in ["x86", "win32"]:
            return "x86", "win32"
        elif arch == "arm64":
            return "arm64", "win_arm64"
        else:  # Default to x64/amd64
            return "x64", "win_amd64"

    elif sys.platform.startswith("darwin"):
        # macOS platform - always use universal2
        return "universal2", "macosx_15_0_universal2"

    elif sys.platform.startswith("linux"):
        # Linux platform - use musllinux or manylinux tags based on architecture
        # Get target architecture from environment variable or default to platform machine type
        import platform

        target_arch = os.environ.get("targetArch", platform.machine())

        # Detect libc type
        libc_name, _ = platform.libc_ver()
        is_musl = libc_name == "" or "musl" in libc_name.lower()

        # Allow explicit override via MANYLINUX_TAG env var (defaults to manylinux_2_28)
        manylinux_tag = os.environ.get("MANYLINUX_TAG", "manylinux_2_28")

        if target_arch == "x86_64":
            return "x86_64", f"musllinux_1_2_x86_64" if is_musl else f"{manylinux_tag}_x86_64"
        elif target_arch in ["aarch64", "arm64"]:
            return "aarch64", f"musllinux_1_2_aarch64" if is_musl else f"{manylinux_tag}_aarch64"
        else:
            raise OSError(
                f"Unsupported architecture '{target_arch}' for Linux; expected 'x86_64' or 'aarch64'."
            )


class CustomBdistWheel(bdist_wheel):
    def finalize_options(self):
        # Call the original finalize_options first to initialize self.bdist_dir
        bdist_wheel.finalize_options(self)

        # Get platform info using consolidated function
        arch, platform_tag = get_platform_info()
        self.plat_name = platform_tag
        print(f"Setting wheel platform tag to: {self.plat_name} (arch: {arch})")


# ---------------------------------------------------------------------------
# Package discovery
# ---------------------------------------------------------------------------

# Find all packages in the current directory.
# Exclude mssql_python_odbc: it is shipped exclusively by the standalone
# mssql-python-odbc distribution (see setup_odbc.py) and pulled in via
# install_requires. Shipping it here too would make two distributions own the
# same import directory (install-order file overwrites; uninstall of one can
# remove files the other needs).
packages = find_packages(exclude=["mssql_python_odbc", "mssql_python_odbc.*"])

# Get platform info using consolidated function
arch, platform_tag = get_platform_info()
print(f"Detected architecture: {arch} (platform tag: {platform_tag})")

# ---------------------------------------------------------------------------
# package_data – binaries to include in the wheel
# ---------------------------------------------------------------------------
package_data = {
    "mssql_python": [
        "py.typed",
        "ddbc_bindings.cp*.pyd",
        "ddbc_bindings.cp*.so",
        # msvcp140.dll (VC++ runtime) is copied next to the compiled extension by
        # build.bat; the ODBC driver binaries themselves ship only in the
        # standalone mssql-python-odbc package (see setup_odbc.py).
        "*.dll",
    ],
}

setup(
    name="mssql-python",
    version="1.14.0",
    description="A Python library for interacting with Microsoft SQL Server",
    long_description=open("PyPI_Description.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    author="Microsoft Corporation",
    author_email="mssql-python@microsoft.com",
    url="https://github.com/microsoft/mssql-python",
    packages=packages,
    package_data=package_data,
    include_package_data=True,
    # Requires >= Python 3.10
    python_requires=">=3.10",
    # Add dependencies
    install_requires=[
        "azure-identity>=1.12.0",  # Azure authentication library
        # ODBC Driver 18 binaries (standalone package). The pin is derived from
        # mssql_python_odbc.__version__ (single source of truth) so it can never
        # drift from the published mssql-python-odbc package.
        f"mssql-python-odbc=={_read_odbc_version()}",
        # Rust TDS core package, renamed from mssql_py_core and published by
        # mssql-rs as a normal PyPI dependency instead of re-embedded in this wheel.
        f"mssql-python-rs=={_read_mssql_python_rs_version()}",
    ],
    extras_require={
        "pyarrow": ["pyarrow>=14.0.0"],
    },
    classifiers=[
        "Operating System :: Microsoft :: Windows",
        "Operating System :: MacOS",
        "Operating System :: POSIX :: Linux",
    ],
    zip_safe=False,
    # Force binary distribution
    distclass=BinaryDistribution,
    exclude_package_data={
        "": ["*.yml", "*.yaml"],  # Exclude YML files
    },
    # Register custom commands
    cmdclass={
        "bdist_wheel": CustomBdistWheel,
    },
)
