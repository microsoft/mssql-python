"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.

Build script for the ``mssql-python-odbc`` package.

This packages the Microsoft ODBC Driver 18 for SQL Server binaries into a
standalone, platform-specific wheel that ``mssql-python`` depends on. Build it
with::

    python setup_odbc.py bdist_wheel

During the transition period the driver binaries still live under
``mssql_python/libs/``. This script copies the current platform's subtree into
``mssql_python_odbc/libs/`` so a wheel can be produced locally. The release
pipeline populates ``libs/`` per-platform and is the source of truth for the
full release matrix.
"""

import os
import re
import shutil
import sys
from pathlib import Path

import setuptools
from setuptools import setup
from setuptools.dist import Distribution
from wheel.bdist_wheel import bdist_wheel

PROJECT_ROOT = Path(__file__).resolve().parent
PACKAGE_NAME = "mssql_python_odbc"
PACKAGE_DIR = PROJECT_ROOT / PACKAGE_NAME
BUNDLED_LIBS_ROOT = PROJECT_ROOT / "mssql_python" / "libs"

# The driver binaries are packaged via the recursive ``libs/**/*`` glob in
# ``package_data`` below, which enumerates files from the copied
# ``mssql_python_odbc/libs/`` subtree on disk (not from version control).
# Support for ``**`` recursive globs in ``package_data`` landed in setuptools
# 62.3.0; with an older setuptools the glob silently matches nothing and the
# wheel ships WITHOUT any driver binaries. Fail fast instead of producing a
# broken-but-quiet wheel.
MIN_SETUPTOOLS = (62, 3, 0)


def _require_min_setuptools() -> None:
    raw = setuptools.__version__
    parts = tuple(int(m) for m in re.findall(r"\d+", raw)[:3])
    parts += (0,) * (3 - len(parts))
    if parts < MIN_SETUPTOOLS:
        raise SystemExit(
            "setup_odbc.py requires setuptools >= "
            f"{'.'.join(map(str, MIN_SETUPTOOLS))} to package the ODBC driver "
            "binaries via the recursive 'libs/**/*' glob; found setuptools "
            f"{raw}. Upgrade with:\n"
            '    python -m pip install --upgrade "setuptools>=62.3.0"'
        )


class BinaryDistribution(Distribution):
    """Force a platform-specific wheel (the package ships native binaries)."""

    def has_ext_modules(self):
        return True


def get_platform_info():
    """Get platform-specific architecture and platform tag information.

    Kept in sync with ``setup.py`` so the ODBC wheel carries the same platform
    tags as the main ``mssql-python`` wheel.
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
        import platform

        target_arch = os.environ.get("targetArch", platform.machine())
        libc_name, _ = platform.libc_ver()
        is_musl = libc_name == "" or "musl" in libc_name.lower()
        manylinux_tag = os.environ.get("MANYLINUX_TAG", "manylinux_2_28")

        if target_arch == "x86_64":
            return "x86_64", "musllinux_1_2_x86_64" if is_musl else f"{manylinux_tag}_x86_64"
        elif target_arch in ["aarch64", "arm64"]:
            return "aarch64", "musllinux_1_2_aarch64" if is_musl else f"{manylinux_tag}_aarch64"
        else:
            raise OSError(
                f"Unsupported architecture '{target_arch}' for Linux; "
                f"expected 'x86_64' or 'aarch64'."
            )

    raise OSError(f"Unsupported platform: {sys.platform!r}")


def _libs_arch(build_arch: str) -> str:
    """Map the build arch from ``get_platform_info`` to the ``libs/`` dir name.

    ``libs/`` uses ``x64``/``x86``/``arm64`` on Windows and
    ``x86_64``/``arm64`` on Linux/macOS.
    """
    if sys.platform.startswith("win"):
        return build_arch  # already x64 / x86 / arm64
    if build_arch in ("x86_64", "amd64"):
        return "x86_64"
    if build_arch in ("aarch64", "arm64"):
        return "arm64"
    return build_arch


def _copytree(src: Path, dst: Path) -> None:
    if src.is_dir():
        shutil.copytree(src, dst, dirs_exist_ok=True)
        print(f"  Copied {src} -> {dst}")


def sync_libs() -> None:
    """Copy the current platform's ODBC libs into ``mssql_python_odbc/libs/``.

    Convenience for local/transition builds while ``mssql_python`` still bundles
    the binaries. If ``mssql_python/libs`` is absent (e.g. the pipeline places
    binaries directly into the package), this is a no-op.
    """
    target_root = PACKAGE_DIR / "libs"
    if not BUNDLED_LIBS_ROOT.is_dir():
        print(f"sync_libs: bundled libs not found at {BUNDLED_LIBS_ROOT}; skipping copy")
        return

    build_arch, _ = get_platform_info()
    arch = _libs_arch(build_arch)

    # Always carry the licensing files.
    _copytree(BUNDLED_LIBS_ROOT / "LICENSING", target_root / "LICENSING")

    if sys.platform.startswith("win"):
        _copytree(BUNDLED_LIBS_ROOT / "windows" / arch, target_root / "windows" / arch)

    elif sys.platform.startswith("darwin"):
        # universal2 wheel serves both architectures.
        for mac_arch in ("arm64", "x86_64"):
            _copytree(BUNDLED_LIBS_ROOT / "macos" / mac_arch, target_root / "macos" / mac_arch)

    elif sys.platform.startswith("linux"):
        # A single Linux wheel serves all distro families for its libc/arch;
        # the driver is selected at runtime via /etc/*-release detection.
        for distro in ("alpine", "debian_ubuntu", "rhel", "suse"):
            _copytree(
                BUNDLED_LIBS_ROOT / "linux" / distro / arch,
                target_root / "linux" / distro / arch,
            )


class CustomBdistWheel(bdist_wheel):
    """Force a platform-specific but Python-agnostic tag and sync libs.

    The package ships only pre-built ODBC driver binaries (data), not a compiled
    Python extension, so one ``py3-none-<platform>`` wheel serves every supported
    Python version (3.10+). The wheel stays platform-specific
    (``root_is_pure = False``) because the binaries differ per OS/arch/libc, but
    the interpreter/ABI tags are forced to ``py3``/``none``. Without this the
    ``BinaryDistribution`` (``has_ext_modules`` -> True) would produce a
    ``cp3XX``-specific tag, forcing a needless per-Python-version build matrix.
    """

    def finalize_options(self):
        bdist_wheel.finalize_options(self)
        arch, platform_tag = get_platform_info()
        self.plat_name = platform_tag
        # Platform-specific (ships native binaries) but not tied to a CPython ABI.
        self.root_is_pure = False
        print(f"Setting wheel platform tag to: {self.plat_name} (arch: {arch})")

    def get_tag(self):
        # Preserve the platform tag from the base implementation but relabel the
        # interpreter/ABI tags as Python-agnostic ("py3"/"none").
        _python, _abi, plat = bdist_wheel.get_tag(self)
        return "py3", "none", plat

    def run(self):
        sync_libs()
        bdist_wheel.run(self)


_require_min_setuptools()

setup(
    name="mssql-python-odbc",
    version="18.6.0",
    description=(
        "Internal implementation package for mssql-python: Microsoft ODBC "
        "Driver 18 for SQL Server binaries. Not intended for direct use."
    ),
    long_description=(
        "Internal implementation package not meant for direct consumption. "
        "Install `mssql-python`, which depends on this package."
    ),
    long_description_content_type="text/plain",
    author="Microsoft Corporation",
    author_email="mssql-python@microsoft.com",
    url="https://github.com/microsoft/mssql-python",
    license="MIT",
    packages=[PACKAGE_NAME],
    # vcredist / VC++ runtime (deliberate divergence from the main wheel's SHAPE,
    # not a behavior change): unlike setup.py we do NOT exclude
    # ``libs/<plat>/vcredist/``. This is intentional and consistent with what the
    # main wheel actually ships:
    #   * The main mssql-python wheel still ships the VC++ runtime
    #     (``msvcp140.dll``) -- build.bat copies it out of
    #     ``libs/windows/<arch>/vcredist/`` to the package root next to the
    #     compiled ``ddbc_bindings`` extension, and setup.py then excludes the
    #     now-duplicate ``vcredist/`` folder. That exclusion is DE-DUPLICATION,
    #     not a licensing/size-driven removal of the runtime.
    #   * This package is pure driver-binary data with no compiled extension, so
    #     there is no package-root ``.pyd`` to relocate the runtime beside. We
    #     therefore keep ``msvcp140.dll`` in its original ``vcredist/`` folder
    #     (with its license text) so the driver binaries and the runtime they
    #     were built against travel together and the wheel stays self-contained.
    # This does not create a Phase-2 runtime skew: the driver's ``msvcp140.dll``
    # dependency is satisfied in-process by the mssql-python extension module
    # (built ``/MD``, which loads ``msvcp140.dll`` from beside the ``.pyd``), so
    # the external-package and bundled-libs paths behave identically. This copy
    # is completeness + attribution, which is also why GetOdbcLibsBaseDir()'s
    # completeness check does not need to verify vcredist.
    package_data={
        PACKAGE_NAME: [
            "libs/*",
            "libs/**/*",
        ],
    },
    include_package_data=True,
    python_requires=">=3.10",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: MacOS",
        "Operating System :: POSIX :: Linux",
    ],
    zip_safe=False,
    distclass=BinaryDistribution,
    cmdclass={
        "bdist_wheel": CustomBdistWheel,
    },
)
