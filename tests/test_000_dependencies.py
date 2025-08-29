"""
Test file for platform-specific dependencies in mssql-python package.
This file tests that all required dependencies are present for the current platform and architecture.
"""

import pytest
import platform
import os
import sys
from pathlib import Path

from mssql_python.ddbc_bindings import normalize_architecture


class DependencyTester:
    """Helper class to test platform-specific dependencies."""
    
    def __init__(self):
        self.platform_name = platform.system().lower()
        self.raw_architecture = platform.machine().lower()
        self.module_dir = self._get_module_directory()
        self.normalized_arch = self._normalize_architecture()
    
    def _get_module_directory(self):
        """Get the mssql_python module directory."""
        try:
            import mssql_python
            module_file = mssql_python.__file__
            return Path(module_file).parent
        except ImportError:
            # Fallback to relative path from tests directory
            return Path(__file__).parent.parent / "mssql_python"
    
    def _normalize_architecture(self):
        """Normalize architecture names for the given platform."""
        arch_lower = self.raw_architecture.lower()
        
        if self.platform_name == "windows":
            arch_map = {
                "win64": "x64", "amd64": "x64", "x64": "x64",
                "win32": "x86", "x86": "x86",
                "arm64": "arm64"
            }
            return arch_map.get(arch_lower, arch_lower)
        
        elif self.platform_name == "darwin":
            # For macOS, we use universal2 for distribution
            return "universal2"
        
        elif self.platform_name == "linux":
            arch_map = {
                "x64": "x86_64", "amd64": "x86_64", "x86_64": "x86_64",
                "arm64": "arm64", "aarch64": "arm64"
            }
            return arch_map.get(arch_lower, arch_lower)
        
        return arch_lower
    
    def _detect_linux_distro(self):
        """Detect Linux distribution for driver path selection."""
        distro_name = "ubuntu"  # default
        '''
        #ifdef __linux__
        if (fs::exists("/etc/alpine-release")) {
            platform = "alpine";
        } else if (fs::exists("/etc/redhat-release") || fs::exists("/etc/centos-release")) {
            platform = "rhel";
        } else if (fs::exists("/etc/SuSE-release") || fs::exists("/etc/SUSE-brand")) {
            platform = "suse";
        } else {
            platform = "ubuntu";
        }

        fs::path driverPath = basePath / "libs" / "linux" / platform / arch / "lib" / "libmsodbcsql-18.5.so.1.1";
        return driverPath.string();
        '''
        try:
            if (Path("/etc/alpine-release").exists()):
                distro_name = "alpine"
            elif (Path("/etc/redhat-release").exists() or Path("/etc/centos-release").exists()):
                distro_name = "rhel"
            elif (Path("/etc/SuSE-release").exists() or Path("/etc/SUSE-brand").exists()):
                distro_name = "suse"
            else:
                distro_name = "ubuntu"  # Default for other Linux distributions
        except Exception:
            pass  # use default
        
        return distro_name
    
    def get_expected_dependencies(self):
        """Get expected dependencies for the current platform and architecture."""
        if self.platform_name == "windows":
            return self._get_windows_dependencies()
        elif self.platform_name == "darwin":
            return self._get_macos_dependencies()
        elif self.platform_name == "linux":
            return self._get_linux_dependencies()
        else:
            return []
    
    def _get_windows_dependencies(self):
        """Get Windows dependencies based on architecture."""
        base_path = self.module_dir / "libs" / "windows" / self.normalized_arch
        
        dependencies = [
            base_path / "msodbcsql18.dll",
            base_path / "msodbcdiag18.dll",
            base_path / "mssql-auth.dll",
            base_path / "vcredist" / "msvcp140.dll",
        ]
        
        return dependencies
    
    def _get_macos_dependencies(self):
        """Get macOS dependencies for both architectures."""
        dependencies = []
        
        # macOS uses universal2 binaries, but we need to check both arch directories
        for arch in ["arm64", "x86_64"]:
            base_path = self.module_dir / "libs" / "macos" / arch / "lib"
            dependencies.extend([
                base_path / "libmsodbcsql.18.dylib",
                base_path / "libodbcinst.2.dylib",
            ])
        
        return dependencies
    
    def _get_linux_dependencies(self):
        """Get Linux dependencies based on distribution and architecture."""
        distro_name = self._detect_linux_distro()
        
        # For Linux, we need to handle the actual runtime architecture
        runtime_arch = self.raw_architecture.lower()
        if runtime_arch in ["x64", "amd64"]:
            runtime_arch = "x86_64"
        elif runtime_arch in ["aarch64"]:
            runtime_arch = "arm64"
        
        base_path = self.module_dir / "libs" / "linux" / distro_name / runtime_arch / "lib"
        
        dependencies = [
            base_path / "libmsodbcsql-18.5.so.1.1",
            base_path / "libodbcinst.so.2",
        ]
        
        return dependencies
    
    def get_expected_python_extension(self):
        """Get expected Python extension module filename."""
        python_version = f"{sys.version_info.major}{sys.version_info.minor}"
        
        if self.platform_name == "windows":
            # Windows architecture mapping for wheel names
            if self.normalized_arch == "x64":
                wheel_arch = "amd64"
            elif self.normalized_arch == "x86":
                wheel_arch = "win32"
            elif self.normalized_arch == "arm64":
                wheel_arch = "arm64"
            else:
                wheel_arch = self.normalized_arch
            
            extension_name = f"ddbc_bindings.cp{python_version}-{wheel_arch}.pyd"
        else:
            # macOS and Linux use .so
            if self.platform_name == "darwin":
                wheel_arch = "universal2"
            else:
                wheel_arch = self.normalized_arch
            
            extension_name = f"ddbc_bindings.cp{python_version}-{wheel_arch}.so"
        
        return self.module_dir / extension_name

    def get_expected_driver_path(self):
        platform_name = platform.system().lower()
        normalized_arch = normalize_architecture(platform_name, self.normalized_arch)

        if platform_name == "windows":
            driver_path = Path(self.module_dir) / "libs" / "windows" / normalized_arch / "msodbcsql18.dll"

        elif platform_name == "darwin":
            driver_path = Path(self.module_dir) / "libs" / "macos" / normalized_arch / "lib" / "libmsodbcsql.18.dylib"

        elif platform_name == "linux":
            distro_name = self._detect_linux_distro()
            driver_path = Path(self.module_dir) / "libs" / "linux" / distro_name / normalized_arch / "lib" / "libmsodbcsql-18.5.so.1.1"

        else:
            raise RuntimeError(f"Unsupported platform: {platform_name}")

        driver_path_str = str(driver_path)

        # Check if file exists
        if not driver_path.exists():
            raise RuntimeError(f"ODBC driver not found at: {driver_path_str}")

        return driver_path_str

# Create global instance for use in tests
dependency_tester = DependencyTester()


class TestPlatformDetection:
    """Test platform and architecture detection."""
    
    def test_platform_detection(self):
        """Test that platform detection works correctly."""
        assert dependency_tester.platform_name in ["windows", "darwin", "linux"], \
            f"Unsupported platform: {dependency_tester.platform_name}"
    
    def test_architecture_detection(self):
        """Test that architecture detection works correctly."""
        if dependency_tester.platform_name == "windows":
            assert dependency_tester.normalized_arch in ["x64", "x86", "arm64"], \
                f"Unsupported Windows architecture: {dependency_tester.normalized_arch}"
        elif dependency_tester.platform_name == "darwin":
            assert dependency_tester.normalized_arch == "universal2", \
                f"macOS should use universal2, got: {dependency_tester.normalized_arch}"
        elif dependency_tester.platform_name == "linux":
            assert dependency_tester.normalized_arch in ["x86_64", "arm64"], \
                f"Unsupported Linux architecture: {dependency_tester.normalized_arch}"
    
    def test_module_directory_exists(self):
        """Test that the mssql_python module directory exists."""
        assert dependency_tester.module_dir.exists(), \
            f"Module directory not found: {dependency_tester.module_dir}"


class TestDependencyFiles:
    """Test that required dependency files exist."""
    
    def test_platform_specific_dependencies(self):
        """Test that all platform-specific dependencies exist."""
        dependencies = dependency_tester.get_expected_dependencies()
        
        missing_dependencies = []
        for dep_path in dependencies:
            if not dep_path.exists():
                missing_dependencies.append(str(dep_path))
        
        assert not missing_dependencies, \
            f"Missing dependencies for {dependency_tester.platform_name} {dependency_tester.normalized_arch}:\n" + \
            "\n".join(missing_dependencies)
    
    def test_python_extension_exists(self):
        """Test that the Python extension module exists."""
        extension_path = dependency_tester.get_expected_python_extension()
        
        assert extension_path.exists(), \
            f"Python extension module not found: {extension_path}"
    
    def test_python_extension_loadable(self):
        """Test that the Python extension module can be loaded."""
        try:
            import mssql_python.ddbc_bindings
            # Test that we can access a basic function
            assert hasattr(mssql_python.ddbc_bindings, 'normalize_architecture')
        except ImportError as e:
            pytest.fail(f"Failed to import ddbc_bindings: {e}")


class TestArchitectureSpecificDependencies:
    """Test architecture-specific dependency requirements."""
    
    @pytest.mark.skipif(dependency_tester.platform_name != "windows", reason="Windows-specific test")
    def test_windows_vcredist_dependency(self):
        """Test that Windows builds include vcredist dependencies."""
        vcredist_path = dependency_tester.module_dir / "libs" / "windows" / dependency_tester.normalized_arch / "vcredist" / "msvcp140.dll"
        
        assert vcredist_path.exists(), \
            f"Windows vcredist dependency not found: {vcredist_path}"
    
    @pytest.mark.skipif(dependency_tester.platform_name != "windows", reason="Windows-specific test")
    def test_windows_auth_dependency(self):
        """Test that Windows builds include authentication library."""
        auth_path = dependency_tester.module_dir / "libs" / "windows" / dependency_tester.normalized_arch / "mssql-auth.dll"
        
        assert auth_path.exists(), \
            f"Windows authentication library not found: {auth_path}"
    
    @pytest.mark.skipif(dependency_tester.platform_name != "darwin", reason="macOS-specific test")
    def test_macos_universal_dependencies(self):
        """Test that macOS builds include dependencies for both architectures."""
        for arch in ["arm64", "x86_64"]:
            base_path = dependency_tester.module_dir / "libs" / "macos" / arch / "lib"
            
            msodbcsql_path = base_path / "libmsodbcsql.18.dylib"
            libodbcinst_path = base_path / "libodbcinst.2.dylib"
            
            assert msodbcsql_path.exists(), \
                f"macOS {arch} ODBC driver not found: {msodbcsql_path}"
            assert libodbcinst_path.exists(), \
                f"macOS {arch} ODBC installer library not found: {libodbcinst_path}"
    
    @pytest.mark.skipif(dependency_tester.platform_name != "linux", reason="Linux-specific test")
    def test_linux_distribution_dependencies(self):
        """Test that Linux builds include distribution-specific dependencies."""
        distro_name = dependency_tester._detect_linux_distro()
        
        # Test that the distribution directory exists
        distro_path = dependency_tester.module_dir / "libs" / "linux" / distro_name
        
        assert distro_path.exists(), \
            f"Linux distribution directory not found: {distro_path}"


class TestDependencyContent:
    """Test that dependency files have expected content/properties."""
    
    def test_dependency_file_sizes(self):
        """Test that dependency files are not empty."""
        dependencies = dependency_tester.get_expected_dependencies()
        
        for dep_path in dependencies:
            if dep_path.exists():
                file_size = dep_path.stat().st_size
                assert file_size > 0, \
                    f"Dependency file is empty: {dep_path}"
    
    def test_python_extension_file_size(self):
        """Test that the Python extension module is not empty."""
        extension_path = dependency_tester.get_expected_python_extension()
        
        if extension_path.exists():
            file_size = extension_path.stat().st_size
            assert file_size > 0, \
                f"Python extension module is empty: {extension_path}"


class TestRuntimeCompatibility:
    """Test runtime compatibility of dependencies."""
    
    def test_python_extension_imports(self):
        """Test that the Python extension can be imported without errors."""
        try:
            # Test basic import
            import mssql_python.ddbc_bindings
            
            # Test that we can access the normalize_architecture function
            from mssql_python.ddbc_bindings import normalize_architecture
            
            # Test that the function works
            result = normalize_architecture("windows", "x64")
            assert result == "x64"
            
        except Exception as e:
            pytest.fail(f"Failed to import or use ddbc_bindings: {e}")

# Print platform information when tests are collected
def pytest_runtest_setup(item):
    """Print platform information before running tests."""
    if item.name == "test_platform_detection":
        print(f"\nRunning dependency tests on:")
        print(f"  Platform: {dependency_tester.platform_name}")
        print(f"  Architecture: {dependency_tester.normalized_arch}")
        print(f"  Raw Architecture: {dependency_tester.raw_architecture}")
        print(f"  Module Directory: {dependency_tester.module_dir}")
        if dependency_tester.platform_name == "linux":
            print(f"  Linux Distribution: {dependency_tester._detect_linux_distro()}")

# Test if ddbc_bindings can be imported (the compiled file is present or not)
def test_ddbc_bindings_import():
    """Test if ddbc_bindings can be imported."""
    try:
        import mssql_python.ddbc_bindings
        assert True, "ddbc_bindings module imported successfully."
    except ImportError as e:
        pytest.fail(f"Failed to import ddbc_bindings: {e}")



def test_get_driver_path_from_ddbc_bindings():
    """Test the GetDriverPathCpp function from ddbc_bindings."""
    try:
        import mssql_python.ddbc_bindings as ddbc
        module_dir = dependency_tester.module_dir

        driver_path = ddbc.GetDriverPathCpp(str(module_dir))

        # The driver path should be same as one returned by the Python function
        expected_path = dependency_tester.get_expected_driver_path()
        assert driver_path == str(expected_path), \
            f"Driver path mismatch: expected {expected_path}, got {driver_path}"
    except Exception as e:
        pytest.fail(f"Failed to call GetDriverPathCpp: {e}")
