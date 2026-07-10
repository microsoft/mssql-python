"""
Tests for the ODBC driver package split (mssql-python-odbc).

Part of splitting the bundled ODBC driver binaries out of ``mssql-python`` into
the standalone ``mssql-python-odbc`` package. These tests validate the Python
driver-path API and, crucially, that it agrees with the native C++ resolver
(``ddbc_bindings.GetDriverPathCpp``) so the path Python reports is exactly the
one the driver is loaded from at connect time.

Notes:
- ``mssql_python_odbc/libs`` is populated per-platform at build time (and copied
  locally by ``setup_odbc.py`` during the transition). Tests that need the actual
  driver file skip when the libs directory is absent (e.g. a fresh CI checkout
  where the binaries have not been synced).
- The C++ fallback (``GetOdbcLibsBaseDir`` -> bundled ``mssql_python`` libs when
  the external package is not installed) is verified manually rather than here,
  since exercising it requires uninstalling the package mid-run.
"""

import os
import sys

import pytest

# Skip the whole module cleanly if the package is not importable.
mssql_python_odbc = pytest.importorskip("mssql_python_odbc")


EXPECTED_DRIVER_FILENAME = {
    "win32": "msodbcsql18.dll",
    "darwin": "libmsodbcsql.18.dylib",
    "linux": "libmsodbcsql-18.6.so.2.1",
}


def _platform_key():
    if sys.platform.startswith("win"):
        return "win32"
    if sys.platform.startswith("darwin"):
        return "darwin"
    if sys.platform.startswith("linux"):
        return "linux"
    return sys.platform


def _package_dir():
    """Base directory the native loader appends ``libs`` to."""
    return os.path.dirname(os.path.abspath(mssql_python_odbc.__file__))


def _libs_present():
    """True only when this platform's driver binary is actually present.

    Checks for the driver file rather than just the ``libs/`` directory, so a
    stray or empty ``libs/`` in a dev checkout makes the driver tests skip
    cleanly instead of failing. Mirrors the C++ resolver, which falls back to
    the bundled libs unless the external package ships a real driver binary.
    """
    try:
        return os.path.isfile(mssql_python_odbc.get_driver_path())
    except OSError:
        return False


_NO_LIBS_REASON = (
    "ODBC libs not present in package (fresh checkout, or not built/synced for " "this platform)"
)


class TestOdbcPackageMetadata:
    def test_version_is_driver_version(self):
        assert mssql_python_odbc.__version__ == "18.6.2"

    def test_public_api_present(self):
        assert callable(mssql_python_odbc.get_driver_path)
        assert callable(mssql_python_odbc.get_libs_dir)


class TestLibsDir:
    def test_libs_dir_under_package(self):
        libs_dir = mssql_python_odbc.get_libs_dir()
        assert os.path.basename(libs_dir) == "libs"
        assert os.path.normcase(libs_dir).startswith(os.path.normcase(_package_dir()))


class TestArchDetection:
    def test_detect_arch_matches_platform(self):
        arch = mssql_python_odbc._detect_arch()
        if sys.platform.startswith("win"):
            assert arch in ("x64", "x86", "arm64")
        else:
            assert arch in ("x86_64", "arm64")

    @pytest.mark.skipif(
        not sys.platform.startswith("linux"),
        reason="Linux-only distro-family detection",
    )
    def test_detect_linux_distro_family(self):
        distro = mssql_python_odbc._detect_linux_distro_family()
        assert distro in ("alpine", "rhel", "suse", "debian_ubuntu")


class TestDriverPath:
    @pytest.mark.skipif(not _libs_present(), reason=_NO_LIBS_REASON)
    def test_driver_path_layout_and_exists(self):
        driver_path = mssql_python_odbc.get_driver_path()
        # Correct driver filename for this platform.
        assert os.path.basename(driver_path) == EXPECTED_DRIVER_FILENAME[_platform_key()]
        # Resolved under the package's own libs directory.
        assert mssql_python_odbc.get_libs_dir() in driver_path
        # The resolved driver file actually exists on disk.
        assert os.path.isfile(driver_path), f"driver not found at {driver_path}"

    @pytest.mark.skipif(not _libs_present(), reason=_NO_LIBS_REASON)
    def test_driver_path_contains_expected_platform_dir(self):
        driver_path = mssql_python_odbc.get_driver_path().replace("\\", "/")
        expected_segment = {
            "win32": "/libs/windows/",
            "darwin": "/libs/macos/",
            "linux": "/libs/linux/",
        }[_platform_key()]
        assert expected_segment in driver_path


class TestPythonCppParity:
    """The Python ``get_driver_path()`` must resolve to the exact same path the
    native C++ loader (``GetDriverPathCpp``) computes for the same base dir, so
    tooling/tests agree with the driver actually loaded at connect time."""

    @pytest.mark.skipif(not _libs_present(), reason=_NO_LIBS_REASON)
    def test_get_driver_path_matches_cpp(self):
        try:
            from mssql_python import ddbc_bindings
        except Exception as exc:  # native extension not built / driver load failed
            pytest.skip(f"ddbc_bindings unavailable: {exc}")

        get_cpp = getattr(ddbc_bindings, "GetDriverPathCpp", None)
        if get_cpp is None:
            pytest.skip("GetDriverPathCpp not exposed by this build")

        py_path = mssql_python_odbc.get_driver_path()
        cpp_path = get_cpp(_package_dir())

        def norm(p):
            return os.path.normcase(os.path.normpath(p))

        assert norm(py_path) == norm(cpp_path), (
            "Python/C++ driver path mismatch:\n" f"  python={py_path}\n" f"  cpp   ={cpp_path}"
        )
