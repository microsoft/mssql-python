"""
Setup script for mssql-python.

This script handles platform-specific wheel building with correct platform tags.
The native extension compilation is handled by the build_ddbc package.

Note: This file is still needed for:
1. Platform-specific package discovery (libs/windows, libs/linux, libs/macos)
2. Custom wheel platform tags (BinaryDistribution, CustomBdistWheel)

For building:
    python -m build_ddbc       # Compile ddbc_bindings only
    python -m build            # Compile + create wheel (recommended)
    pip install -e .           # Editable install with auto-compile
"""

import sys

from setuptools import setup, find_packages
from setuptools.dist import Distribution
from wheel.bdist_wheel import bdist_wheel

from build_ddbc.compiler import get_platform_info


# =============================================================================
# Platform-Specific Package Discovery
# =============================================================================


def get_platform_packages():
    """Get platform-specific package list."""
    packages = find_packages()
    arch, _ = get_platform_info()

    if sys.platform.startswith("win"):
        packages.extend(
            [
                f"mssql_python.libs.windows.{arch}",
                f"mssql_python.libs.windows.{arch}.1033",
                f"mssql_python.libs.windows.{arch}.vcredist",
            ]
        )
    elif sys.platform.startswith("darwin"):
        packages.append("mssql_python.libs.macos")
    elif sys.platform.startswith("linux"):
        packages.append("mssql_python.libs.linux")

    return packages


# =============================================================================
# Custom Distribution (Force Platform-Specific Wheel)
# =============================================================================


class BinaryDistribution(Distribution):
    """Distribution that forces platform-specific wheel creation."""

    def has_ext_modules(self):
        return True


# =============================================================================
# Custom bdist_wheel Command
# =============================================================================


class CustomBdistWheel(bdist_wheel):
    """Custom wheel builder with platform-specific tags."""

    def finalize_options(self):
        bdist_wheel.finalize_options(self)
        arch, platform_tag = get_platform_info()
        self.plat_name = platform_tag
        print(f"[setup.py] Setting wheel platform tag to: {self.plat_name} (arch: {arch})")


# =============================================================================
# Setup Configuration
# =============================================================================

arch, platform_tag = get_platform_info()
print(f"[setup.py] Detected architecture: {arch} (platform tag: {platform_tag})")

setup(
    # Package discovery
    packages=get_platform_packages(),
    # Force binary distribution
    distclass=BinaryDistribution,
    # Register custom commands
    cmdclass={
        "bdist_wheel": CustomBdistWheel,
    },
)
