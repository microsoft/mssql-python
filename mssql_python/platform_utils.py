"""
Platform detection utilities for mssql-python.

This module provides platform and architecture detection used by both
the build system (setup.py, build_ddbc) and runtime code.
"""

import glob
import os
import platform
import sys
from typing import Tuple


def get_platform_info() -> Tuple[str, str]:
    """
    Get platform-specific architecture and platform tag information.

    Returns:
        Tuple of (architecture, platform_tag) where platform_tag is a
        PEP 425 compatible wheel platform tag.

    Raises:
        OSError: If the platform or architecture is not supported
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
        return "universal2", "macosx_15_0_universal2"

    elif sys.platform.startswith("linux"):
        target_arch = os.environ.get("targetArch", platform.machine())
        libc_name, _ = platform.libc_ver()

        if not libc_name:
            # Fallback: check for musl linker (Alpine Linux)
            # platform.libc_ver() returns empty string on Alpine
            is_musl = bool(glob.glob("/lib/ld-musl*"))
            if not is_musl:
                print(
                    "[mssql_python] Warning: libc detection failed; defaulting to glibc.",
                    file=sys.stderr,
                )
        else:
            is_musl = "musl" in libc_name.lower()

        if target_arch == "x86_64":
            return "x86_64", "musllinux_1_2_x86_64" if is_musl else "manylinux_2_28_x86_64"
        elif target_arch in ["aarch64", "arm64"]:
            return "aarch64", "musllinux_1_2_aarch64" if is_musl else "manylinux_2_28_aarch64"
        else:
            raise OSError(
                f"Unsupported architecture '{target_arch}' for Linux; "
                "expected 'x86_64' or 'aarch64'."
            )

    raise OSError(f"Unsupported platform: {sys.platform}")
