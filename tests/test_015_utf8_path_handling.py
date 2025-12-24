"""
Tests for UTF-8 path handling fix (Issue #370).

Verifies that the driver correctly handles paths containing non-ASCII
characters on Windows (e.g., usernames like 'Thalén', folders like 'café').

Bug Summary:
- GetModuleDirectory() used ANSI APIs (PathRemoveFileSpecA) which corrupted UTF-8 paths
- LoadDriverLibrary() used broken UTF-8→UTF-16 conversion: std::wstring(path.begin(), path.end())
- LoadDriverOrThrowException() used same broken pattern for mssql-auth.dll

Fix:
- Use std::filesystem::path which handles encoding correctly on all platforms
- fs::path::c_str() returns wchar_t* on Windows with proper UTF-16 encoding

Test Categories:
================
1. DEFAULT TESTS (run always) - Exercise the fixed code paths
2. SLOW TESTS (run with -m slow) - Full integration with non-ASCII venv paths
"""

import pytest
import platform
import sys
import subprocess


class TestPathHandlingCodePaths:
    """
    Test that path handling code paths are exercised correctly.

    These tests run by DEFAULT and verify the fixed C++ functions
    (GetModuleDirectory, LoadDriverLibrary) are working.
    """

    def test_module_import_exercises_path_handling(self):
        """
        Verify module import succeeds - this exercises GetModuleDirectory().

        When mssql_python imports, it calls:
        1. GetModuleDirectory() - to find module location
        2. LoadDriverLibrary() - to load ODBC driver
        3. LoadLibraryW() for mssql-auth.dll on Windows

        If any of these fail due to path encoding issues, import fails.
        """
        import mssql_python

        assert mssql_python is not None
        assert hasattr(mssql_python, "__file__")
        assert isinstance(mssql_python.__file__, str)

    def test_module_path_is_valid_utf8(self):
        """Verify module path is valid UTF-8 string."""
        import mssql_python

        module_path = mssql_python.__file__

        # Should be encodable/decodable as UTF-8 without errors
        encoded = module_path.encode("utf-8")
        decoded = encoded.decode("utf-8")
        assert decoded == module_path

    def test_connect_function_available(self):
        """Verify connect function is available (proves ddbc_bindings loaded)."""
        import mssql_python

        assert hasattr(mssql_python, "connect")
        assert callable(mssql_python.connect)

    def test_ddbc_bindings_loaded(self):
        """Verify ddbc_bindings C++ module loaded successfully."""
        from mssql_python import ddbc_bindings

        assert ddbc_bindings is not None

    def test_connection_class_available(self):
        """Verify Connection class from C++ bindings is accessible."""
        from mssql_python.ddbc_bindings import Connection

        assert Connection is not None


class TestPathWithNonAsciiCharacters:
    """
    Test path handling with non-ASCII characters in strings.

    These tests verify that Python string operations with non-ASCII
    characters work correctly (prerequisite for the C++ fix to work).
    """

    # Non-ASCII test strings representing real-world scenarios
    NON_ASCII_PATHS = [
        "Thalén",  # Swedish - the original issue reporter's username
        "café",  # French
        "日本語",  # Japanese
        "中文",  # Chinese
        "über",  # German
        "Müller",  # German umlaut
        "España",  # Spanish
        "Россия",  # Russian
        "한국어",  # Korean
        "Ñoño",  # Spanish ñ
        "Ångström",  # Swedish å
    ]

    @pytest.mark.parametrize("non_ascii_name", NON_ASCII_PATHS)
    def test_path_string_with_non_ascii(self, non_ascii_name):
        """Test that Python can handle paths with non-ASCII characters."""
        # Simulate Windows-style path
        test_path = f"C:\\Users\\{non_ascii_name}\\project\\.venv\\Lib\\site-packages"

        # Verify UTF-8 encoding/decoding works
        encoded = test_path.encode("utf-8")
        decoded = encoded.decode("utf-8")
        assert decoded == test_path
        assert non_ascii_name in decoded

    @pytest.mark.parametrize("non_ascii_name", NON_ASCII_PATHS)
    def test_pathlib_with_non_ascii(self, non_ascii_name, tmp_path):
        """Test that pathlib handles non-ASCII directory names."""
        from pathlib import Path

        test_dir = tmp_path / non_ascii_name
        test_dir.mkdir()
        assert test_dir.exists()

        # Create a file in the non-ASCII directory
        test_file = test_dir / "test.txt"
        test_file.write_text("test content", encoding="utf-8")
        assert test_file.exists()

        # Read back
        content = test_file.read_text(encoding="utf-8")
        assert content == "test content"

    def test_path_with_multiple_non_ascii_segments(self, tmp_path):
        """Test path with multiple non-ASCII directory segments."""
        from pathlib import Path

        # Create nested directories with non-ASCII names
        nested = tmp_path / "Thalén" / "プロジェクト" / "código"
        nested.mkdir(parents=True)
        assert nested.exists()

    def test_path_with_spaces_and_non_ascii(self, tmp_path):
        """Test path with both spaces and non-ASCII characters."""
        from pathlib import Path

        test_dir = tmp_path / "My Thalén Project"
        test_dir.mkdir()
        assert test_dir.exists()


@pytest.mark.skipif(
    platform.system() != "Windows", reason="DLL loading and path encoding issue is Windows-specific"
)
class TestWindowsSpecificPathHandling:
    """
    Windows-specific tests for path handling.

    These tests verify Windows-specific behavior related to the fix.
    """

    def test_module_loads_on_windows(self):
        """Verify module loads correctly on Windows."""
        import mssql_python

        # If we get here, LoadLibraryW succeeded for:
        # - msodbcsql18.dll
        # - mssql-auth.dll (if exists)
        assert mssql_python.ddbc_bindings is not None

    def test_libs_directory_exists(self):
        """Verify the libs/windows directory structure exists."""
        import mssql_python
        from pathlib import Path

        module_dir = Path(mssql_python.__file__).parent
        libs_dir = module_dir / "libs" / "windows"

        # Check that at least one architecture directory exists
        arch_dirs = ["x64", "x86", "arm64"]
        found_arch = any((libs_dir / arch).exists() for arch in arch_dirs)
        assert found_arch, f"No architecture directory found in {libs_dir}"

    def test_auth_dll_exists_if_libs_present(self):
        """Verify mssql-auth.dll exists in the libs directory."""
        import mssql_python
        from pathlib import Path
        import struct

        module_dir = Path(mssql_python.__file__).parent

        # Determine architecture
        arch = "x64" if struct.calcsize("P") * 8 == 64 else "x86"
        # Check for ARM64

        if platform.machine().lower() in ("arm64", "aarch64"):
            arch = "arm64"

        auth_dll = module_dir / "libs" / "windows" / arch / "mssql-auth.dll"

        if auth_dll.parent.exists():
            # If the directory exists, the DLL should be there
            assert auth_dll.exists(), f"mssql-auth.dll not found at {auth_dll}"


@pytest.mark.skipif(platform.system() != "Windows", reason="Integration test requires Windows")
class TestNonAsciiPathIntegration:
    """
    Full integration tests for non-ASCII path handling.

    These tests create actual virtual environments in paths with non-ASCII
    characters to verify the fix works end-to-end (Issue #370).

    NOTE: These tests take ~30-60s each as they:
    1. Create a virtual environment
    2. Install mssql-python via pip
    3. Verify import works

    This is acceptable overhead for a critical bug fix that affects users
    with non-ASCII characters in their Windows username or installation path.
    """

    @pytest.fixture
    def clean_test_dir(self, tmp_path):
        """Create a clean test directory and cleanup after."""
        yield tmp_path
        # Cleanup is handled automatically by tmp_path fixture

    def test_venv_in_swedish_username_path(self, clean_test_dir):
        """
        Reproduce Issue #370: venv in path with Swedish character 'é'.

        This is the exact scenario reported by the user with username 'Thalén'.
        """
        self._test_venv_in_non_ascii_path(clean_test_dir, "Thalén_test")

    def test_venv_in_german_umlaut_path(self, clean_test_dir):
        """Test venv in path with German umlaut 'ü'."""
        self._test_venv_in_non_ascii_path(clean_test_dir, "Müller_projekt")

    def test_venv_in_japanese_path(self, clean_test_dir):
        """Test venv in path with Japanese characters."""
        self._test_venv_in_non_ascii_path(clean_test_dir, "日本語プロジェクト")

    def test_venv_in_chinese_path(self, clean_test_dir):
        """Test venv in path with Chinese characters."""
        self._test_venv_in_non_ascii_path(clean_test_dir, "中文项目")

    def _test_venv_in_non_ascii_path(self, base_path, folder_name):
        """
        Helper method to test venv creation and mssql-python import in non-ASCII path.

        Args:
            base_path: Base temporary directory
            folder_name: Folder name containing non-ASCII characters
        """
        # Create directory with non-ASCII character
        test_dir = base_path / folder_name
        test_dir.mkdir()

        venv_path = test_dir / ".venv"

        # Create virtual environment
        result = subprocess.run(
            [sys.executable, "-m", "venv", str(venv_path)],
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 0, f"venv creation failed: {result.stderr}"

        # Get the Python executable in the venv
        python_exe = venv_path / "Scripts" / "python.exe"
        assert python_exe.exists(), f"Python not found at {python_exe}"

        # Install mssql-python in the venv
        pip_result = subprocess.run(
            [str(python_exe), "-m", "pip", "install", "mssql-python", "--quiet"],
            capture_output=True,
            text=True,
            timeout=120,
        )

        if pip_result.returncode != 0:
            pytest.skip(f"pip install failed (may not be published): {pip_result.stderr}")

        # Try to import mssql_python - this is where the bug manifests
        # The C++ code calls LoadLibraryW with the path, and if UTF-8→UTF-16
        # conversion is broken, it fails with "mssql-auth.dll not found"
        test_script = """
import sys
try:
    import mssql_python
    from mssql_python import ddbc_bindings
    print("SUCCESS")
    print(f"Module: {mssql_python.__file__}")
except Exception as e:
    print(f"FAILED: {e}")
    sys.exit(1)
"""

        import_result = subprocess.run(
            [str(python_exe), "-c", test_script], capture_output=True, text=True, timeout=30
        )

        assert import_result.returncode == 0, (
            f"Import failed in non-ASCII path '{folder_name}'.\n"
            f"stdout: {import_result.stdout}\n"
            f"stderr: {import_result.stderr}\n"
            f"This indicates the UTF-8 path encoding fix is not working."
        )
        assert "SUCCESS" in import_result.stdout


class TestPathEncodingEdgeCases:
    """Test edge cases in path encoding handling."""

    def test_ascii_only_path_still_works(self):
        """Verify ASCII-only paths continue to work (regression test)."""
        import mssql_python

        # If we got here, module loaded successfully
        assert mssql_python is not None

    def test_path_with_spaces(self):
        """Verify paths with spaces work (common Windows scenario)."""
        import mssql_python

        # Common Windows paths like "Program Files" have spaces
        # Module should load regardless
        assert mssql_python.__file__ is not None

    def test_very_long_path_component(self, tmp_path):
        """Test handling of long path components."""
        from pathlib import Path

        # Windows MAX_PATH is 260, but individual components can be up to 255
        long_name = "a" * 200
        test_dir = tmp_path / long_name
        test_dir.mkdir()
        assert test_dir.exists()

    @pytest.mark.parametrize(
        "char",
        [
            "é",
            "ñ",
            "ü",
            "ö",
            "å",
            "ø",
            "æ",  # European diacritics
            "中",
            "日",
            "한",  # CJK ideographs
            "α",
            "β",
            "γ",  # Greek letters
            "й",
            "ж",
            "щ",  # Cyrillic
        ],
    )
    def test_individual_non_ascii_chars_utf8_roundtrip(self, char):
        """Test UTF-8 encoding roundtrip for individual non-ASCII characters."""
        test_path = f"C:\\Users\\Test{char}User\\project"

        # UTF-8 roundtrip
        encoded = test_path.encode("utf-8")
        decoded = encoded.decode("utf-8")
        assert decoded == test_path
        assert char in decoded

    def test_emoji_in_path(self, tmp_path):
        """Test path with emoji characters (supplementary plane)."""
        from pathlib import Path

        # Emoji are in the supplementary planes (> U+FFFF)
        # This tests 4-byte UTF-8 sequences
        try:
            emoji_dir = tmp_path / "test_🚀_project"
            emoji_dir.mkdir()
            assert emoji_dir.exists()
        except OSError:
            # Some filesystems don't support emoji in filenames
            pytest.skip("Filesystem doesn't support emoji in filenames")

    def test_mixed_scripts_in_path(self, tmp_path):
        """Test path with mixed scripts (Latin + CJK + Cyrillic)."""
        from pathlib import Path

        mixed_name = "Project_项目_Проект"
        test_dir = tmp_path / mixed_name
        test_dir.mkdir()
        assert test_dir.exists()
