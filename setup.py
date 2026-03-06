import os
import sys
from pathlib import Path

from setuptools import setup, find_packages
from setuptools.dist import Distribution
from wheel.bdist_wheel import bdist_wheel

PROJECT_ROOT = Path(__file__).resolve().parent


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

        if target_arch == "x86_64":
            return "x86_64", "musllinux_1_2_x86_64" if is_musl else "manylinux_2_28_x86_64"
        elif target_arch in ["aarch64", "arm64"]:
            return "aarch64", "musllinux_1_2_aarch64" if is_musl else "manylinux_2_28_aarch64"
        else:
            raise OSError(
                f"Unsupported architecture '{target_arch}' for Linux; expected 'x86_64' or 'aarch64'."
            )


# ---------------------------------------------------------------------------
# mssql_py_core validation
# ---------------------------------------------------------------------------
def validate_mssql_py_core():
    """Validate that mssql_py_core has been extracted into the project root.

    Expects ``<project_root>/mssql_py_core/`` to contain:
      - ``__init__.py``
      - At least one native extension (``.pyd`` on Windows, ``.so`` on Linux/macOS)

    The extraction is performed by ``eng/scripts/install-mssql-py-core.ps1``
    (Windows) or ``eng/scripts/install-mssql-py-core.sh`` (Linux/macOS)
    and must be run before ``setup.py bdist_wheel``.

    Raises SystemExit if mssql_py_core is missing or invalid.
    """
    core_dir = PROJECT_ROOT / "mssql_py_core"

    if not core_dir.is_dir():
        sys.exit(
            "ERROR: mssql_py_core/ directory not found in project root. "
            "Run eng/scripts/install-mssql-py-core to extract it before building."
        )

    # Check for __init__.py
    if not (core_dir / "__init__.py").is_file():
        sys.exit("ERROR: mssql_py_core/__init__.py not found.")

    # Check for native extension (.pyd on Windows, .so on Linux/macOS)
    ext = ".pyd" if sys.platform.startswith("win") else ".so"
    native_files = list(core_dir.glob(f"mssql_py_core*{ext}"))
    if not native_files:
        sys.exit(
            f"ERROR: No mssql_py_core native extension ({ext}) found "
            f"in mssql_py_core/. Run eng/scripts/install-mssql-py-core to extract it."
        )

    for f in native_files:
        print(f"  Found mssql_py_core native extension: {f.name}")

    print("mssql_py_core validation: OK")


class CustomBdistWheel(bdist_wheel):
    def finalize_options(self):
        # Call the original finalize_options first to initialize self.bdist_dir
        bdist_wheel.finalize_options(self)

        # Get platform info using consolidated function
        arch, platform_tag = get_platform_info()
        self.plat_name = platform_tag
        print(f"Setting wheel platform tag to: {self.plat_name} (arch: {arch})")

    def run(self):
        validate_mssql_py_core()
        bdist_wheel.run(self)


# ---------------------------------------------------------------------------
# Package discovery
# ---------------------------------------------------------------------------

# Find all packages in the current directory
packages = find_packages()

# Get platform info using consolidated function
arch, platform_tag = get_platform_info()
print(f"Detected architecture: {arch} (platform tag: {platform_tag})")

# mssql_py_core is validated inside CustomBdistWheel.run() so that editable
# installs (pip install -e .) and other setup.py commands are not blocked.
if (PROJECT_ROOT / "mssql_py_core").is_dir():
    packages.append("mssql_py_core")

# Add platform-specific packages
if sys.platform.startswith("win"):
    packages.extend(
        [
            f"mssql_python.libs.windows.{arch}",
            f"mssql_python.libs.windows.{arch}.1033",
            f"mssql_python.libs.windows.{arch}.vcredist",
        ]
    )
elif sys.platform.startswith("darwin"):
    packages.extend(
        [
            f"mssql_python.libs.macos",
        ]
    )
elif sys.platform.startswith("linux"):
    packages.extend(
        [
            f"mssql_python.libs.linux",
        ]
    )

# ---------------------------------------------------------------------------
# package_data – binaries to include in the wheel
# ---------------------------------------------------------------------------
package_data = {
    "mssql_python": [
        "py.typed",
        "ddbc_bindings.cp*.pyd",
        "ddbc_bindings.cp*.so",
        "libs/*",
        "libs/**/*",
        "*.dll",
    ],
    "mssql_py_core": [
        "mssql_py_core.cp*.pyd",
        "mssql_py_core.cp*.so",
    ],
}

setup(
    name="mssql-python",
    version="1.4.0",
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
    ],
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
        "mssql_python": [
            "libs/*/vcredist/*",
            "libs/*/vcredist/**/*",  # Exclude vcredist directories, added here since `'libs/*' is already included`
        ],
    },
    # Register custom commands
    cmdclass={
        "bdist_wheel": CustomBdistWheel,
    },
)
