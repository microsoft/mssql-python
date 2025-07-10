import os
import sys
from setuptools import setup, find_packages
from setuptools.dist import Distribution
from wheel.bdist_wheel import bdist_wheel

# Custom distribution to force platform-specific wheel
class BinaryDistribution(Distribution):
    def has_ext_modules(self):
        return True

def get_platform_info():
    """Get platform-specific architecture and platform tag information."""
    if sys.platform.startswith('win'):
        # Get architecture from environment variable or default to x64
        arch = os.environ.get('ARCHITECTURE', 'x64')
        # Strip quotes if present
        if isinstance(arch, str):
            arch = arch.strip('"\'')

        # Normalize architecture values
        if arch in ['x86', 'win32']:
            return 'x86', 'win32'
        elif arch == 'arm64':
            return 'arm64', 'win_arm64'
        else:  # Default to x64/amd64
            return 'x64', 'win_amd64'
            
    elif sys.platform.startswith('darwin'):
        # macOS platform - always use universal2
        return 'universal2', 'macosx_15_0_universal2'
        
    elif sys.platform.startswith('linux'):
        # Linux platform - use manylinux2014 tags
        # Use targetArch from environment or fallback to platform.machine()
        import platform
        target_arch = os.environ.get('targetArch', platform.machine())
        
        if target_arch == 'x86_64':
            return 'x86_64', 'manylinux2014_x86_64'
        elif target_arch in ['aarch64', 'arm64']:
            return 'aarch64', 'manylinux2014_aarch64'
        else:
            raise OSError(f"Unsupported architecture '{target_arch}' for Linux; expected 'x86_64' or 'aarch64'.")

# Custom bdist_wheel command to override platform tag
class CustomBdistWheel(bdist_wheel):
    def finalize_options(self):
        # Call the original finalize_options first to initialize self.bdist_dir
        bdist_wheel.finalize_options(self)
        
        # Get platform info using consolidated function
        arch, platform_tag = get_platform_info()
        self.plat_name = platform_tag
        print(f"Setting wheel platform tag to: {self.plat_name} (arch: {arch})")

# Find all packages in the current directory
packages = find_packages()

# Get platform info using consolidated function
arch, platform_tag = get_platform_info()
print(f"Detected architecture: {arch} (platform tag: {platform_tag})")

# Add platform-specific packages
if sys.platform.startswith('win'):
    packages.extend([
        f'mssql_python.libs.windows.{arch}',
        f'mssql_python.libs.windows.{arch}.1033',
        f'mssql_python.libs.windows.{arch}.vcredist'
    ])
elif sys.platform.startswith('darwin'):
    packages.extend([
        f'mssql_python.libs.macos',
    ])
elif sys.platform.startswith('linux'):
    packages.extend([
        f'mssql_python.libs.linux',
    ])

setup(
    name='mssql-python',
    version='0.7.0',
    description='A Python library for interacting with Microsoft SQL Server',
    long_description=open('PyPI_Description.md', encoding='utf-8').read(),
    long_description_content_type='text/markdown',
    author='Microsoft Corporation',
    author_email='pysqldriver@microsoft.com',
    url='https://github.com/microsoft/mssql-python',
    packages=packages,
    package_data={
        # Include PYD and DLL files inside mssql_python, exclude YML files
        'mssql_python': [
            'ddbc_bindings.cp*.pyd',  # Include all PYD files
            'ddbc_bindings.cp*.so',  # Include all SO files
            'libs/*', 
            'libs/**/*', 
            '*.dll'
        ]
    },
    include_package_data=True,
    # Requires >= Python 3.10
    python_requires='>=3.10',
    classifiers=[
        'Operating System :: Microsoft :: Windows',
        'Operating System :: MacOS',
        'Operating System :: POSIX :: Linux',
    ],
    zip_safe=False,
    # Force binary distribution
    distclass=BinaryDistribution,
    exclude_package_data={
        '': ['*.yml', '*.yaml'],  # Exclude YML files
        'mssql_python': [
            'libs/*/vcredist/*', 'libs/*/vcredist/**/*',  # Exclude vcredist directories, added here since `'libs/*' is already included`
        ],
    },
    # Register custom commands
    cmdclass={
        'bdist_wheel': CustomBdistWheel,
    },
)
