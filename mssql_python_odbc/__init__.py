"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.

mssql_python_odbc — Microsoft ODBC Driver 18 for SQL Server binaries.

Internal implementation package for ``mssql-python``. It ships the
platform-specific ODBC driver binaries (``msodbcsql18``) and their supporting
libraries so that ``mssql-python`` does not have to bundle them in its own
wheel. It is not meant for direct consumption — install ``mssql-python``
instead, which depends on this package.

The public surface is :func:`get_driver_path`, which returns the absolute path
to the ODBC driver shared library for the current platform/architecture. The
native ``mssql_python.ddbc_bindings`` extension resolves the same location in
C++ (see ``GetOdbcLibsBaseDir`` / ``GetDriverPathCpp``); this Python API mirrors
that logic for tooling, tests, and diagnostics.
"""

import os
import platform
import sys

__all__ = ["get_driver_path", "get_libs_dir", "__version__"]

# Version tracks the bundled Microsoft ODBC Driver 18 for SQL Server release.
__version__ = "18.6.2"

# Driver shared-library file names per platform (must match the names produced
# by the ODBC driver packaging and expected by the native loader).
_DRIVER_FILENAME = {
    "windows": "msodbcsql18.dll",
    "linux": "libmsodbcsql-18.6.so.2.1",
    "macos": "libmsodbcsql.18.dylib",
}


def get_libs_dir() -> str:
    """Return the absolute path to this package's ``libs/`` directory.

    This is the root under which the platform-specific ODBC binaries live
    (``libs/<platform>/<arch>/...``). The parent of this path (the package
    directory) is the base the native loader appends ``libs`` to when resolving
    the driver.
    """
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "libs")


def _detect_arch() -> str:
    """Return the architecture directory name for the current interpreter.

    Mirrors the compile-time detection in ``GetDriverPathCpp``:
      * Windows uses ``x64`` / ``x86`` / ``arm64``.
      * Linux and macOS use ``x86_64`` / ``arm64``.
    """
    machine = platform.machine().lower()

    if sys.platform.startswith("win"):
        if machine in ("amd64", "x86_64"):
            return "x64"
        if machine in ("arm64", "aarch64"):
            return "arm64"
        if machine in ("x86", "i386", "i686"):
            return "x86"
        raise OSError(f"Unsupported Windows architecture: {platform.machine()!r}")

    # Linux / macOS
    if machine in ("x86_64", "amd64"):
        return "x86_64"
    if machine in ("arm64", "aarch64"):
        return "arm64"
    raise OSError(f"Unsupported architecture: {platform.machine()!r}")


def _detect_linux_distro_family() -> str:
    """Return the Linux distro family directory name.

    Mirrors the ``/etc/*-release`` probing in ``GetDriverPathCpp`` so the Python
    and C++ paths agree.
    """
    if os.path.exists("/etc/alpine-release"):
        return "alpine"
    if os.path.exists("/etc/redhat-release") or os.path.exists("/etc/centos-release"):
        return "rhel"
    if os.path.exists("/etc/SuSE-release") or os.path.exists("/etc/SUSE-brand"):
        return "suse"
    return "debian_ubuntu"  # default for Debian/Ubuntu and other glibc distros


def get_driver_path() -> str:
    """Return the absolute path to the ODBC driver shared library.

    Resolves the platform/architecture (and, on Linux, the distro family) and
    returns the full path to ``msodbcsql18`` inside this package.

    Raises:
        OSError: if the platform/architecture is unsupported.
        FileNotFoundError: if the resolved driver file is not present (e.g. the
            package was built without this platform's binaries).
    """
    libs_dir = get_libs_dir()
    arch = _detect_arch()

    if sys.platform.startswith("win"):
        driver_path = os.path.join(libs_dir, "windows", arch, _DRIVER_FILENAME["windows"])
    elif sys.platform.startswith("darwin"):
        driver_path = os.path.join(
            libs_dir, "macos", arch, "lib", _DRIVER_FILENAME["macos"]
        )
    elif sys.platform.startswith("linux"):
        distro = _detect_linux_distro_family()
        driver_path = os.path.join(
            libs_dir, "linux", distro, arch, "lib", _DRIVER_FILENAME["linux"]
        )
    else:
        raise OSError(f"Unsupported platform: {sys.platform!r}")

    if not os.path.isfile(driver_path):
        raise FileNotFoundError(
            f"ODBC driver not found at {driver_path!r}. The mssql-python-odbc "
            f"package may not include binaries for this platform/architecture."
        )

    return driver_path
