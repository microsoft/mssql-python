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

import os
import platform
import sys

from setuptools import setup, find_packages
from setuptools.dist import Distribution
from wheel.bdist_wheel import bdist_wheel


def get_platform_info():
    """
    Get platform-specific architecture and platform tag information.

    Note: This is duplicated from build_ddbc.compiler to avoid circular imports
    during fresh installs where build_ddbc isn't available yet.
    """
    if sys.platform.startswith("win"):
        arch = os.environ.get("ARCHITECTURE", "x64")
        if isinstance(arch, str):
            arch = arch.strip("\"'")
        if arch in ["x86", "win32"]:
            return "x86", "win32"
        elif arch == "arm64":
            return "arm64", "win_arm64"
        else:
            return "x64", "win_amd64"
    elif sys.platform.startswith("darwin"):
        return "universal2", "macosx_11_0_universal2"
    elif sys.platform.startswith("linux"):
        target_arch = os.environ.get("targetArch", platform.machine())
        libc_name, _ = platform.libc_ver()
        is_musl = libc_name and "musl" in libc_name.lower()
        if target_arch == "x86_64":
            return "x86_64", "musllinux_1_2_x86_64" if is_musl else "manylinux_2_28_x86_64"
        elif target_arch in ["aarch64", "arm64"]:
            return "aarch64", "musllinux_1_2_aarch64" if is_musl else "manylinux_2_28_aarch64"
        else:
            raise OSError(f"Unsupported architecture '{target_arch}' for Linux")
    raise OSError(f"Unsupported platform: {sys.platform}")


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
