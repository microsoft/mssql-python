"""
Tests for the build_backend package.

Covers:
- platform_utils: platform detection logic
- hooks: PEP 517/660 build hooks (_is_truthy, build_wheel, build_editable, etc.)
- compiler: find_pybind_dir, compile_ddbc, _run_windows_build, _run_unix_build
- __main__: CLI entry point
"""

import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path


# =============================================================================
# platform_utils tests
# =============================================================================


class TestPlatformUtils:
    """Tests for build_backend.platform_utils module."""

    def test_get_platform_info_returns_tuple(self):
        """Test that get_platform_info returns a tuple of two strings."""
        from build_backend.platform_utils import get_platform_info

        result = get_platform_info()
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert isinstance(result[0], str)  # architecture
        assert isinstance(result[1], str)  # platform_tag

    def test_get_platform_info_current_platform(self):
        """Test get_platform_info on current platform returns valid values."""
        from build_backend.platform_utils import get_platform_info
        import sys

        arch, platform_tag = get_platform_info()

        # Architecture should be non-empty
        assert arch

        # Platform tag should match current platform
        if sys.platform.startswith("win"):
            assert "win" in platform_tag
        elif sys.platform.startswith("darwin"):
            assert "macos" in platform_tag
        elif sys.platform.startswith("linux"):
            assert "linux" in platform_tag

    def test_windows_x64_detection(self):
        """Test Windows x64 platform detection."""
        from build_backend import platform_utils

        with patch.object(platform_utils.sys, "platform", "win32"):
            with patch.object(platform_utils.os.environ, "get", return_value="x64"):
                arch, tag = platform_utils.get_platform_info()
                assert arch == "x64"
                assert tag == "win_amd64"

    def test_windows_x86_detection(self):
        """Test Windows x86 platform detection."""
        from build_backend import platform_utils

        with patch.object(platform_utils.sys, "platform", "win32"):
            with patch.object(platform_utils.os.environ, "get", return_value="x86"):
                arch, tag = platform_utils.get_platform_info()
                assert arch == "x86"
                assert tag == "win32"

    def test_windows_arm64_detection(self):
        """Test Windows ARM64 platform detection."""
        from build_backend import platform_utils

        with patch.object(platform_utils.sys, "platform", "win32"):
            with patch.object(platform_utils.os.environ, "get", return_value="arm64"):
                arch, tag = platform_utils.get_platform_info()
                assert arch == "arm64"
                assert tag == "win_arm64"

    def test_macos_detection(self):
        """Test macOS platform detection."""
        from build_backend import platform_utils

        with patch.object(platform_utils.sys, "platform", "darwin"):
            arch, tag = platform_utils.get_platform_info()
            assert arch == "universal2"
            assert "macosx" in tag
            assert "universal2" in tag

    def test_linux_x86_64_glibc_detection(self):
        """Test Linux x86_64 glibc platform detection."""
        from build_backend import platform_utils

        with (
            patch.object(platform_utils.sys, "platform", "linux"),
            patch.object(platform_utils.os.environ, "get", return_value="x86_64"),
            patch.object(platform_utils.platform, "machine", return_value="x86_64"),
            patch.object(platform_utils.platform, "libc_ver", return_value=("glibc", "2.28")),
        ):
            arch, tag = platform_utils.get_platform_info()
            assert arch == "x86_64"
            assert tag == "manylinux_2_28_x86_64"

    def test_linux_x86_64_musl_detection(self):
        """Test Linux x86_64 musl platform detection."""
        from build_backend import platform_utils

        with (
            patch.object(platform_utils.sys, "platform", "linux"),
            patch.object(platform_utils.os.environ, "get", return_value="x86_64"),
            patch.object(platform_utils.platform, "machine", return_value="x86_64"),
            patch.object(platform_utils.platform, "libc_ver", return_value=("musl", "1.2")),
        ):
            arch, tag = platform_utils.get_platform_info()
            assert arch == "x86_64"
            assert tag == "musllinux_1_2_x86_64"

    def test_linux_aarch64_glibc_detection(self):
        """Test Linux aarch64 glibc platform detection."""
        from build_backend import platform_utils

        with (
            patch.object(platform_utils.sys, "platform", "linux"),
            patch.object(platform_utils.os.environ, "get", return_value="aarch64"),
            patch.object(platform_utils.platform, "machine", return_value="aarch64"),
            patch.object(platform_utils.platform, "libc_ver", return_value=("glibc", "2.28")),
        ):
            arch, tag = platform_utils.get_platform_info()
            assert arch == "aarch64"
            assert tag == "manylinux_2_28_aarch64"

    def test_linux_aarch64_musl_detection(self):
        """Test Linux aarch64 musl platform detection."""
        from build_backend import platform_utils

        with (
            patch.object(platform_utils.sys, "platform", "linux"),
            patch.object(platform_utils.os.environ, "get", return_value="aarch64"),
            patch.object(platform_utils.platform, "machine", return_value="aarch64"),
            patch.object(platform_utils.platform, "libc_ver", return_value=("musl", "1.2")),
        ):
            arch, tag = platform_utils.get_platform_info()
            assert arch == "aarch64"
            assert tag == "musllinux_1_2_aarch64"

    def test_linux_arm64_alias(self):
        """Test Linux arm64 is treated as aarch64."""
        from build_backend import platform_utils

        with (
            patch.object(platform_utils.sys, "platform", "linux"),
            patch.object(platform_utils.os.environ, "get", return_value="arm64"),
            patch.object(platform_utils.platform, "machine", return_value="arm64"),
            patch.object(platform_utils.platform, "libc_ver", return_value=("glibc", "2.28")),
        ):
            arch, tag = platform_utils.get_platform_info()
            assert arch == "aarch64"
            assert tag == "manylinux_2_28_aarch64"

    def test_linux_empty_libc_with_musl_glob(self):
        """Test Linux with empty libc_ver falls back to glob for musl detection."""
        from build_backend import platform_utils

        with (
            patch.object(platform_utils.sys, "platform", "linux"),
            patch.object(platform_utils.os.environ, "get", return_value="x86_64"),
            patch.object(platform_utils.platform, "machine", return_value="x86_64"),
            patch.object(platform_utils.platform, "libc_ver", return_value=("", "")),
            patch.object(platform_utils.glob, "glob", return_value=["/lib/ld-musl-x86_64.so.1"]),
        ):
            arch, tag = platform_utils.get_platform_info()
            assert arch == "x86_64"
            assert tag == "musllinux_1_2_x86_64"

    def test_linux_empty_libc_no_musl_glob(self, capsys):
        """Test Linux with empty libc_ver and no musl glob defaults to glibc."""
        from build_backend import platform_utils

        with (
            patch.object(platform_utils.sys, "platform", "linux"),
            patch.object(platform_utils.os.environ, "get", return_value="x86_64"),
            patch.object(platform_utils.platform, "machine", return_value="x86_64"),
            patch.object(platform_utils.platform, "libc_ver", return_value=("", "")),
            patch.object(platform_utils.glob, "glob", return_value=[]),
        ):
            arch, tag = platform_utils.get_platform_info()
            assert arch == "x86_64"
            assert tag == "manylinux_2_28_x86_64"
            # Check warning was printed
            captured = capsys.readouterr()
            assert "Warning" in captured.err or "warning" in captured.err.lower()

    def test_linux_unsupported_architecture(self):
        """Test Linux with unsupported architecture raises OSError."""
        from build_backend import platform_utils

        with (
            patch.object(platform_utils.sys, "platform", "linux"),
            patch.object(platform_utils.os.environ, "get", return_value="ppc64le"),
            patch.object(platform_utils.platform, "machine", return_value="ppc64le"),
            patch.object(platform_utils.platform, "libc_ver", return_value=("glibc", "2.28")),
        ):
            with pytest.raises(OSError) as exc_info:
                platform_utils.get_platform_info()
            assert "ppc64le" in str(exc_info.value)
            assert "Unsupported architecture" in str(exc_info.value)

    def test_unsupported_platform(self):
        """Test unsupported platform raises OSError."""
        from build_backend import platform_utils

        with patch.object(platform_utils.sys, "platform", "freebsd"):
            with pytest.raises(OSError) as exc_info:
                platform_utils.get_platform_info()
            assert "freebsd" in str(exc_info.value)
            assert "Unsupported platform" in str(exc_info.value)

    def test_windows_strips_quotes_from_arch(self):
        """Test Windows architecture strips surrounding quotes."""
        from build_backend import platform_utils

        with patch.object(platform_utils.sys, "platform", "win32"):
            with patch.object(platform_utils.os.environ, "get", return_value='"x64"'):
                arch, tag = platform_utils.get_platform_info()
                assert arch == "x64"
                assert tag == "win_amd64"

    def test_windows_win32_alias(self):
        """Test Windows win32 is treated as x86."""
        from build_backend import platform_utils

        with patch.object(platform_utils.sys, "platform", "win32"):
            with patch.object(platform_utils.os.environ, "get", return_value="win32"):
                arch, tag = platform_utils.get_platform_info()
                assert arch == "x86"
                assert tag == "win32"


# =============================================================================
# _is_truthy tests
# =============================================================================


class TestIsTruthy:
    """Tests for the _is_truthy config_settings helper."""

    def test_bool_true(self):
        from build_backend.hooks import _is_truthy
        assert _is_truthy(True) is True

    def test_bool_false(self):
        from build_backend.hooks import _is_truthy
        assert _is_truthy(False) is False

    def test_string_true_variants(self):
        from build_backend.hooks import _is_truthy
        for val in ("true", "True", "TRUE", "1", "yes", "Yes", "YES"):
            assert _is_truthy(val) is True, f"Expected True for {val!r}"

    def test_string_false_variants(self):
        from build_backend.hooks import _is_truthy
        for val in ("false", "False", "0", "no", "No", "", "anything"):
            assert _is_truthy(val) is False, f"Expected False for {val!r}"

    def test_non_string_non_bool(self):
        from build_backend.hooks import _is_truthy
        assert _is_truthy(1) is True
        assert _is_truthy(0) is False
        assert _is_truthy([]) is False
        assert _is_truthy([1]) is True


# =============================================================================
# PEP 517 hooks tests
# =============================================================================


class TestBuildWheel:
    """Tests for build_wheel hook."""

    @patch("build_backend.hooks._setuptools_build_wheel", return_value="fake.whl")
    @patch("build_backend.hooks.compile_ddbc")
    def test_build_wheel_compiles_and_delegates(self, mock_compile, mock_st_wheel):
        from build_backend.hooks import build_wheel

        result = build_wheel("/tmp/out")
        mock_compile.assert_called_once_with(arch=None, coverage=False, verbose=True)
        mock_st_wheel.assert_called_once_with("/tmp/out", None, None)
        assert result == "fake.whl"

    @patch("build_backend.hooks._setuptools_build_wheel", return_value="fake.whl")
    @patch("build_backend.hooks.compile_ddbc")
    def test_build_wheel_skip_compile(self, mock_compile, mock_st_wheel):
        from build_backend.hooks import build_wheel

        result = build_wheel("/tmp/out", config_settings={"--skip-ddbc-compile": "true"})
        mock_compile.assert_not_called()
        assert result == "fake.whl"

    @patch("build_backend.hooks._setuptools_build_wheel", return_value="fake.whl")
    @patch("build_backend.hooks.compile_ddbc")
    def test_build_wheel_passes_arch_and_coverage(self, mock_compile, mock_st_wheel):
        from build_backend.hooks import build_wheel

        build_wheel("/tmp/out", config_settings={"--arch": "arm64", "--coverage": "true"})
        mock_compile.assert_called_once_with(arch="arm64", coverage=True, verbose=True)

    @patch("build_backend.hooks.compile_ddbc", side_effect=FileNotFoundError("build.sh not found"))
    def test_build_wheel_file_not_found_raises(self, mock_compile):
        from build_backend.hooks import build_wheel

        with pytest.raises(FileNotFoundError, match="build.sh not found"):
            build_wheel("/tmp/out")

    @patch("build_backend.hooks.compile_ddbc", side_effect=RuntimeError("cmake failed"))
    def test_build_wheel_runtime_error_raises(self, mock_compile):
        from build_backend.hooks import build_wheel

        with pytest.raises(RuntimeError, match="cmake failed"):
            build_wheel("/tmp/out")

    @patch("build_backend.hooks.compile_ddbc", side_effect=OSError("permission denied"))
    def test_build_wheel_os_error_raises(self, mock_compile):
        from build_backend.hooks import build_wheel

        with pytest.raises(OSError, match="permission denied"):
            build_wheel("/tmp/out")


class TestBuildEditable:
    """Tests for build_editable hook (PEP 660)."""

    @patch("build_backend.hooks.compile_ddbc")
    def test_build_editable_compiles_and_delegates(self, mock_compile):
        from build_backend.hooks import build_editable

        mock_st_editable = MagicMock(return_value="fake-editable.whl")
        with patch(
            "build_backend.hooks._setuptools_build_editable",
            mock_st_editable,
            create=True,
        ):
            # Patch the import inside build_editable
            with patch(
                "setuptools.build_meta.build_editable",
                mock_st_editable,
            ):
                result = build_editable("/tmp/out")

        mock_compile.assert_called_once_with(arch=None, coverage=False, verbose=True)
        assert result == "fake-editable.whl"

    @patch("build_backend.hooks.compile_ddbc")
    def test_build_editable_skip_compile(self, mock_compile):
        from build_backend.hooks import build_editable

        mock_st_editable = MagicMock(return_value="fake-editable.whl")
        with patch("setuptools.build_meta.build_editable", mock_st_editable):
            result = build_editable(
                "/tmp/out", config_settings={"--skip-ddbc-compile": "true"}
            )

        mock_compile.assert_not_called()
        assert result == "fake-editable.whl"

    @patch("build_backend.hooks.compile_ddbc")
    def test_build_editable_passes_arch_and_coverage(self, mock_compile):
        from build_backend.hooks import build_editable

        mock_st_editable = MagicMock(return_value="fake-editable.whl")
        with patch("setuptools.build_meta.build_editable", mock_st_editable):
            build_editable(
                "/tmp/out",
                config_settings={"--arch": "x64", "--coverage": "1"},
            )

        mock_compile.assert_called_once_with(arch="x64", coverage=True, verbose=True)

    @patch("build_backend.hooks.compile_ddbc", side_effect=FileNotFoundError("build.sh missing"))
    def test_build_editable_file_not_found_raises(self, mock_compile):
        from build_backend.hooks import build_editable

        with pytest.raises(FileNotFoundError, match="build.sh missing"):
            build_editable("/tmp/out")

    @patch("build_backend.hooks.compile_ddbc", side_effect=RuntimeError("compile error"))
    def test_build_editable_runtime_error_raises(self, mock_compile):
        from build_backend.hooks import build_editable

        with pytest.raises(RuntimeError, match="compile error"):
            build_editable("/tmp/out")

    @patch("build_backend.hooks.compile_ddbc", side_effect=OSError("disk full"))
    def test_build_editable_os_error_raises(self, mock_compile):
        from build_backend.hooks import build_editable

        with pytest.raises(OSError, match="disk full"):
            build_editable("/tmp/out")


class TestBuildSdist:
    """Tests for build_sdist hook."""

    @patch("build_backend.hooks._setuptools_build_sdist", return_value="fake.tar.gz")
    def test_build_sdist_delegates(self, mock_st_sdist):
        from build_backend.hooks import build_sdist

        result = build_sdist("/tmp/out")
        mock_st_sdist.assert_called_once_with("/tmp/out", None)
        assert result == "fake.tar.gz"


class TestRequiresHooks:
    """Tests for get_requires_for_build_* hooks."""

    @patch("build_backend.hooks._get_requires_for_build_wheel", return_value=["setuptools"])
    def test_get_requires_for_build_wheel(self, mock_req):
        from build_backend.hooks import get_requires_for_build_wheel

        result = get_requires_for_build_wheel()
        assert result == ["setuptools"]

    @patch("build_backend.hooks._get_requires_for_build_sdist", return_value=["setuptools"])
    def test_get_requires_for_build_sdist(self, mock_req):
        from build_backend.hooks import get_requires_for_build_sdist

        result = get_requires_for_build_sdist()
        assert result == ["setuptools"]

    @patch("build_backend.hooks._get_requires_for_build_wheel", return_value=["setuptools"])
    def test_get_requires_for_build_editable(self, mock_req):
        from build_backend.hooks import get_requires_for_build_editable

        result = get_requires_for_build_editable()
        assert result == ["setuptools"]

    @patch(
        "build_backend.hooks._prepare_metadata_for_build_wheel",
        return_value="metadata-dir",
    )
    def test_prepare_metadata_for_build_wheel(self, mock_prep):
        from build_backend.hooks import prepare_metadata_for_build_wheel

        result = prepare_metadata_for_build_wheel("/tmp/meta")
        mock_prep.assert_called_once_with("/tmp/meta", None)
        assert result == "metadata-dir"


# =============================================================================
# CLI (__main__) tests
# =============================================================================


class TestCLI:
    """Tests for build_backend CLI entry point."""

    @patch("build_backend.__main__.compile_ddbc")
    @patch("build_backend.__main__.get_platform_info", return_value=("x86_64", "manylinux_2_28_x86_64"))
    def test_main_success(self, mock_plat, mock_compile):
        from build_backend.__main__ import main

        with patch("sys.argv", ["build_backend"]):
            assert main() == 0
        mock_compile.assert_called_once()

    @patch("build_backend.__main__.compile_ddbc", side_effect=FileNotFoundError("no build.sh"))
    @patch("build_backend.__main__.get_platform_info", return_value=("x86_64", "manylinux_2_28_x86_64"))
    def test_main_file_not_found(self, mock_plat, mock_compile):
        from build_backend.__main__ import main

        with patch("sys.argv", ["build_backend"]):
            assert main() == 1

    @patch("build_backend.__main__.compile_ddbc", side_effect=RuntimeError("failed"))
    @patch("build_backend.__main__.get_platform_info", return_value=("x86_64", "manylinux_2_28_x86_64"))
    def test_main_runtime_error(self, mock_plat, mock_compile):
        from build_backend.__main__ import main

        with patch("sys.argv", ["build_backend"]):
            assert main() == 1

    @patch("build_backend.__main__.compile_ddbc")
    @patch("build_backend.__main__.get_platform_info", return_value=("x86_64", "manylinux_2_28_x86_64"))
    def test_main_quiet_suppresses_output(self, mock_plat, mock_compile, capsys):
        from build_backend.__main__ import main

        with patch("sys.argv", ["build_backend", "--quiet"]):
            assert main() == 0
        captured = capsys.readouterr()
        assert "[build_backend]" not in captured.out

    @patch("build_backend.__main__.compile_ddbc")
    @patch("build_backend.__main__.get_platform_info", return_value=("x86_64", "manylinux_2_28_x86_64"))
    def test_main_passes_arch(self, mock_plat, mock_compile):
        from build_backend.__main__ import main

        with patch("sys.argv", ["build_backend", "--arch", "arm64", "--quiet"]):
            main()
        mock_compile.assert_called_once_with(arch="arm64", coverage=False, verbose=False)

    @patch("build_backend.__main__.compile_ddbc")
    @patch("build_backend.__main__.get_platform_info", return_value=("x86_64", "manylinux_2_28_x86_64"))
    def test_main_passes_coverage(self, mock_plat, mock_compile):
        from build_backend.__main__ import main

        with patch("sys.argv", ["build_backend", "--coverage", "--quiet"]):
            main()
        mock_compile.assert_called_once_with(arch=None, coverage=True, verbose=False)


# =============================================================================
# build_editable ImportError branch (hooks.py L148-153)
# =============================================================================


class TestBuildEditableImportError:
    """Test the ImportError fallback when setuptools lacks build_editable."""

    @patch("build_backend.hooks.compile_ddbc")
    def test_build_editable_old_setuptools_raises(self, mock_compile):
        """Older setuptools without build_editable raises RuntimeError."""
        from build_backend.hooks import build_editable

        with patch.dict("sys.modules", {"setuptools.build_meta": MagicMock(spec=[])}):
            # Remove build_editable from the module so the import fails
            with patch(
                "builtins.__import__",
                side_effect=_make_import_blocker("setuptools.build_meta", "build_editable"),
            ):
                with pytest.raises(RuntimeError, match="Editable installs are not supported"):
                    build_editable(
                        "/tmp/out",
                        config_settings={"--skip-ddbc-compile": "true"},
                    )


def _make_import_blocker(module_name, attr_name):
    """Create an __import__ side_effect that blocks a specific from-import."""
    real_import = __builtins__.__import__ if hasattr(__builtins__, "__import__") else __import__

    def blocker(name, *args, **kwargs):
        if name == module_name:
            mod = MagicMock(spec=[])  # spec=[] means no attributes
            # Accessing attr_name will raise AttributeError,
            # which 'from X import Y' turns into ImportError
            return mod
        return real_import(name, *args, **kwargs)

    return blocker


# =============================================================================
# compiler tests
# =============================================================================


class TestFindPybindDir:
    """Tests for find_pybind_dir."""

    def test_find_pybind_dir_relative_to_file(self, tmp_path):
        """Test finding pybind dir relative to compiler.py."""
        from build_backend.compiler import find_pybind_dir

        # The real pybind dir should be found from the repo root
        pybind_dir = find_pybind_dir()
        assert pybind_dir.exists()
        assert (pybind_dir / "build.sh").exists() or (pybind_dir / "build.bat").exists()

    def test_find_pybind_dir_not_found(self, tmp_path):
        """Test FileNotFoundError when pybind dir doesn't exist."""
        from build_backend import compiler

        # Point both search paths to a tmp dir with no pybind content
        fake_parent = tmp_path / "fake"
        fake_parent.mkdir()

        with (
            patch.object(compiler, "__file__", str(fake_parent / "compiler.py")),
            patch.object(compiler.Path, "cwd", return_value=tmp_path),
        ):
            with pytest.raises(FileNotFoundError, match="Could not find"):
                compiler.find_pybind_dir()


class TestCompileDdbc:
    """Tests for compile_ddbc."""

    @patch("build_backend.compiler._run_unix_build", return_value=True)
    @patch("build_backend.compiler.find_pybind_dir", return_value=Path("/fake/pybind"))
    @patch("build_backend.compiler.get_platform_info", return_value=("x86_64", "manylinux_2_28_x86_64"))
    def test_compile_ddbc_linux_default_arch(self, mock_plat, mock_find, mock_unix):
        """compile_ddbc uses get_platform_info when arch is None."""
        from build_backend.compiler import compile_ddbc

        with patch("build_backend.compiler.sys") as mock_sys:
            mock_sys.platform = "linux"
            result = compile_ddbc(arch=None, coverage=False, verbose=True)

        mock_plat.assert_called_once()
        mock_unix.assert_called_once_with(Path("/fake/pybind"), False, True)
        assert result is True

    @patch("build_backend.compiler._run_unix_build", return_value=True)
    @patch("build_backend.compiler.find_pybind_dir", return_value=Path("/fake/pybind"))
    def test_compile_ddbc_linux_explicit_arch(self, mock_find, mock_unix):
        """compile_ddbc skips get_platform_info when arch is provided."""
        from build_backend.compiler import compile_ddbc

        with patch("build_backend.compiler.sys") as mock_sys:
            mock_sys.platform = "linux"
            result = compile_ddbc(arch="aarch64", coverage=True, verbose=False)

        mock_unix.assert_called_once_with(Path("/fake/pybind"), True, False)
        assert result is True

    @patch("build_backend.compiler._run_windows_build", return_value=True)
    @patch("build_backend.compiler.find_pybind_dir", return_value=Path("/fake/pybind"))
    def test_compile_ddbc_windows(self, mock_find, mock_win):
        """compile_ddbc dispatches to _run_windows_build on Windows."""
        from build_backend.compiler import compile_ddbc

        with patch("build_backend.compiler.sys") as mock_sys:
            mock_sys.platform = "win32"
            result = compile_ddbc(arch="x64", coverage=False, verbose=True)

        mock_win.assert_called_once_with(Path("/fake/pybind"), "x64", True)
        assert result is True


class TestRunUnixBuild:
    """Tests for _run_unix_build."""

    def test_unix_build_success(self, tmp_path):
        """Successful build.sh execution."""
        from build_backend.compiler import _run_unix_build

        build_script = tmp_path / "build.sh"
        build_script.write_text("#!/bin/bash\nexit 0\n")
        build_script.chmod(0o755)

        with patch("build_backend.compiler.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            result = _run_unix_build(tmp_path, coverage=False, verbose=True)

        assert result is True
        args = mock_run.call_args
        assert "bash" in args[0][0][0]

    def test_unix_build_with_coverage(self, tmp_path):
        """build.sh receives --coverage flag."""
        from build_backend.compiler import _run_unix_build

        build_script = tmp_path / "build.sh"
        build_script.write_text("#!/bin/bash\nexit 0\n")
        build_script.chmod(0o755)

        with patch("build_backend.compiler.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            _run_unix_build(tmp_path, coverage=True, verbose=True)

        cmd = mock_run.call_args[0][0]
        assert "--coverage" in cmd

    def test_unix_build_failure_verbose(self, tmp_path):
        """build.sh failure raises RuntimeError (verbose mode)."""
        from build_backend.compiler import _run_unix_build

        build_script = tmp_path / "build.sh"
        build_script.write_text("#!/bin/bash\nexit 1\n")
        build_script.chmod(0o755)

        with patch("build_backend.compiler.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1)
            with pytest.raises(RuntimeError, match="build.sh failed"):
                _run_unix_build(tmp_path, coverage=False, verbose=True)

    def test_unix_build_failure_quiet_prints_output(self, tmp_path):
        """build.sh failure in quiet mode prints stdout/stderr."""
        from build_backend.compiler import _run_unix_build

        build_script = tmp_path / "build.sh"
        build_script.write_text("#!/bin/bash\nexit 1\n")
        build_script.chmod(0o755)

        with patch("build_backend.compiler.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=1,
                stdout=b"some output",
                stderr=b"some error",
            )
            with pytest.raises(RuntimeError, match="build.sh failed"):
                _run_unix_build(tmp_path, coverage=False, verbose=False)

    def test_unix_build_script_not_found(self, tmp_path):
        """Missing build.sh raises FileNotFoundError."""
        from build_backend.compiler import _run_unix_build

        with pytest.raises(FileNotFoundError, match="Build script not found"):
            _run_unix_build(tmp_path, coverage=False, verbose=True)


class TestRunWindowsBuild:
    """Tests for _run_windows_build."""

    def test_windows_build_success(self, tmp_path):
        """Successful build.bat execution."""
        from build_backend.compiler import _run_windows_build

        build_script = tmp_path / "build.bat"
        build_script.write_text("@echo off\nexit /b 0\n")

        with patch("build_backend.compiler.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            result = _run_windows_build(tmp_path, arch="x64", verbose=True)

        assert result is True
        cmd = mock_run.call_args[0][0]
        assert "x64" in cmd

    def test_windows_build_failure_verbose(self, tmp_path):
        """build.bat failure raises RuntimeError (verbose mode)."""
        from build_backend.compiler import _run_windows_build

        build_script = tmp_path / "build.bat"
        build_script.write_text("@echo off\nexit /b 1\n")

        with patch("build_backend.compiler.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1)
            with pytest.raises(RuntimeError, match="build.bat failed"):
                _run_windows_build(tmp_path, arch="x64", verbose=True)

    def test_windows_build_failure_quiet_prints_output(self, tmp_path):
        """build.bat failure in quiet mode prints stdout/stderr."""
        from build_backend.compiler import _run_windows_build

        build_script = tmp_path / "build.bat"
        build_script.write_text("@echo off\nexit /b 1\n")

        with patch("build_backend.compiler.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=1,
                stdout=b"build output",
                stderr=b"build error",
            )
            with pytest.raises(RuntimeError, match="build.bat failed"):
                _run_windows_build(tmp_path, arch="x86", verbose=False)

    def test_windows_build_script_not_found(self, tmp_path):
        """Missing build.bat raises FileNotFoundError."""
        from build_backend.compiler import _run_windows_build

        with pytest.raises(FileNotFoundError, match="Build script not found"):
            _run_windows_build(tmp_path, arch="x64", verbose=True)
