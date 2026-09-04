"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.

Build script for the ``mssql-python-rust-odbc`` package.

This packages the mssql-odbc (Rust) driver binaries, built by
`microsoft/mssql-rs <https://github.com/microsoft/mssql-rs>`_ and published as
the ``mssql-odbc-native`` NuGet package, into a standalone, platform-specific
wheel. ``mssql-python`` depends on this package only when the ``mssql-odbc``
provider is selected (see ``mssql_python.native_provider`` /
``MSSQL_PYTHON_NATIVE_PROVIDER``). Build it with::

    python setup_rust_odbc.py bdist_wheel

The driver binaries live under ``mssql_python_rust_odbc/libs/`` (the committed
source of truth, populated by
``eng/scripts/sync-mssql-odbc-native-libs.py``). Each wheel ships ONLY its own
platform's ``libs/`` subtree (see ``_target_libs_globs``); a single build host
can produce every platform's wheel via ``RUST_ODBC_TARGET_PLATFORM_TAG`` /
``RUST_ODBC_TARGET_ARCH`` (see ``get_platform_info``).
"""

import os
import re
import sys
from pathlib import Path

import setuptools
from setuptools import setup
from setuptools.dist import Distribution
from wheel.bdist_wheel import bdist_wheel

PROJECT_ROOT = Path(__file__).resolve().parent
PACKAGE_NAME = "mssql_python_rust_odbc"
PACKAGE_DIR = PROJECT_ROOT / PACKAGE_NAME

# See setup_odbc.py for why this is required: recursive ``libs/**/*`` globs in
# ``package_data`` need setuptools >= 62.3.0.
MIN_SETUPTOOLS = (62, 3, 0)


def _read_version() -> str:
    """Return ``__version__`` from ``mssql_python_rust_odbc/__init__.py``.

    Single source of truth for the package version: the value is defined once
    in the package's ``__init__.py`` and read here (by regex, without
    importing the package) so the wheel version can never drift from what the
    package reports at runtime.
    """
    init_file = PACKAGE_DIR / "__init__.py"
    text = init_file.read_text(encoding="utf-8")
    match = re.search(r'^__version__\s*=\s*["\']([^"\']+)["\']', text, re.MULTILINE)
    if not match:
        raise SystemExit(f"Could not find __version__ in {init_file}")
    return match.group(1)


def _require_min_setuptools() -> None:
    raw = setuptools.__version__
    parts = tuple(int(m) for m in re.findall(r"\d+", raw)[:3])
    parts += (0,) * (3 - len(parts))
    if parts < MIN_SETUPTOOLS:
        raise SystemExit(
            "setup_rust_odbc.py requires setuptools >= "
            f"{'.'.join(map(str, MIN_SETUPTOOLS))} to package the driver "
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

    Kept in sync with ``setup.py`` / ``setup_odbc.py`` so this wheel carries
    the same platform tags as the rest of the mssql-python distribution.
    """
    # Explicit target override for single-host cross-building (see
    # get_platform_info in setup_odbc.py for the same pattern). Distinct env
    # var names so this script can run alongside setup_odbc.py without either
    # one picking up the other's target.
    explicit_tag = os.environ.get("RUST_ODBC_TARGET_PLATFORM_TAG")
    if explicit_tag:
        target_arch = os.environ.get("RUST_ODBC_TARGET_ARCH", "").strip()
        if not target_arch:
            raise OSError(
                "RUST_ODBC_TARGET_ARCH must be set (non-empty) when "
                "RUST_ODBC_TARGET_PLATFORM_TAG is provided: an empty arch would expand "
                "the libs/ package_data globs to EVERY architecture's subtree and leak "
                "foreign-platform driver binaries into the wheel."
            )
        return target_arch, explicit_tag

    if sys.platform.startswith("win"):
        arch = os.environ.get("ARCHITECTURE", "x64")
        if isinstance(arch, str):
            arch = arch.strip("\"'")
        if arch == "arm64":
            return "arm64", "win_arm64"
        elif arch in ["x86", "win32"]:
            raise OSError(
                "mssql-odbc has no Windows x86 build lane; only x64 and arm64 are " "supported."
            )
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


def _target_libs_globs(platform_tag: str, arch: str) -> list:
    """Return the ``package_data`` globs for exactly ONE target platform's libs.

    The committed ``mssql_python_rust_odbc/libs/`` tree holds every supported
    platform's driver binary. A wheel must ship only its own platform's
    subtree, so we translate the (``platform_tag``, ``arch``) of the wheel
    being built into the minimal set of ``libs/`` globs -- matching exactly
    what ``GetDriverPathCpp`` (mssql_python/pybind/ddbc_bindings.cpp) resolves
    for the ``mssql-odbc`` provider. Combined with ``include_package_data=False``
    this guarantees a Windows wheel never carries Linux binaries (and vice
    versa), whether the build runs on the native OS or is cross-built on a
    single host via the ``RUST_ODBC_TARGET_*`` overrides.
    """
    globs = ["libs/LICENSING"]
    tag = platform_tag.lower()

    def _subtree(root: str) -> None:
        globs.append(f"{root}/*")
        globs.append(f"{root}/**/*")

    if tag.startswith("win"):
        # arch is already the libs dir name on Windows: x64 / arm64.
        _subtree(f"libs/windows/{arch}")
    elif tag.startswith("macos"):
        # The universal2 wheel serves both slices (arm64 + x86_64).
        _subtree("libs/macos")
    elif "musllinux" in tag:
        libs_arch = "arm64" if arch in ("aarch64", "arm64") else "x86_64"
        _subtree(f"libs/linux/musl/{libs_arch}")
    elif "manylinux" in tag:
        # manylinux_2_28 is the broadest-compatibility glibc build (see
        # mssql-rs's build-odbc-glibc228-template.yml); it is the ONLY glibc
        # variant packaged here. mssql-rs also builds a plain 'linux_*'
        # variant against a newer host glibc for its own distro-container
        # testing -- that one is not manylinux-portable and is intentionally
        # not packaged into this wheel.
        libs_arch = "arm64" if arch in ("aarch64", "arm64") else "x86_64"
        _subtree(f"libs/linux/glibc/{libs_arch}")
    else:
        raise OSError(f"Cannot determine libs subtree for platform tag {platform_tag!r}")
    return globs


class CustomBdistWheel(bdist_wheel):
    """Force a platform-specific but Python-agnostic tag.

    The package ships only pre-built driver binaries (data), not a compiled
    Python extension, so one ``py3-none-<platform>`` wheel serves every
    supported Python version (3.10+). See ``setup_odbc.py``'s
    ``CustomBdistWheel`` for the full rationale -- identical here.
    """

    def finalize_options(self):
        bdist_wheel.finalize_options(self)
        arch, platform_tag = get_platform_info()
        self.plat_name = platform_tag
        self.plat_name_supplied = True
        self.root_is_pure = False
        print(f"Setting wheel platform tag to: {self.plat_name} (arch: {arch})")

    def get_tag(self):
        _python, _abi, plat = bdist_wheel.get_tag(self)
        return "py3", "none", plat


_require_min_setuptools()

_TARGET_ARCH, _TARGET_PLATFORM_TAG = get_platform_info()
_LIBS_GLOBS = _target_libs_globs(_TARGET_PLATFORM_TAG, _TARGET_ARCH)
print(f"Rust ODBC wheel target: tag={_TARGET_PLATFORM_TAG} arch={_TARGET_ARCH!r}")
print(f"Rust ODBC libs globs: {_LIBS_GLOBS}")

_LONG_DESCRIPTION = (PROJECT_ROOT / "PyPI_Description_RustODBC.md").read_text(encoding="utf-8")

setup(
    name="mssql-python-rust-odbc",
    version=_read_version(),
    description=(
        "Internal implementation package for mssql-python: mssql-odbc (Rust) "
        "driver binaries. Not intended for direct use."
    ),
    long_description=_LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    author="Microsoft Corporation",
    author_email="mssql-python@microsoft.com",
    url="https://github.com/microsoft/mssql-python",
    license="MIT",
    license_files=[
        "mssql_python_rust_odbc/licenses/MSSQL_ODBC_LICENSE.txt",
    ],
    packages=[PACKAGE_NAME],
    package_data={
        PACKAGE_NAME: _LIBS_GLOBS,
    },
    # include_package_data MUST stay False: the committed libs/ tree holds
    # EVERY platform, so SCM-based inclusion would sweep them all into every
    # wheel. We rely solely on the target-specific _LIBS_GLOBS above.
    include_package_data=False,
    python_requires=">=3.10",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: POSIX :: Linux",
        "Operating System :: MacOS",
    ],
    zip_safe=False,
    distclass=BinaryDistribution,
    cmdclass={
        "bdist_wheel": CustomBdistWheel,
    },
)
