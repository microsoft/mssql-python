import os
import sys
from setuptools import setup, find_packages
from setuptools.dist import Distribution

# Custom distribution to force platform-specific wheel
class BinaryDistribution(Distribution):
    def has_ext_modules(self):
        return True

# Custom bdist_wheel command to override platform tag
class CustomBdistWheel(bdist_wheel):
    def finalize_options(self):
        # Call the original finalize_options first to initialize self.bdist_dir
        bdist_wheel.finalize_options(self)
        
        # Override the platform tag with our custom one based on ARCHITECTURE env var
        if sys.platform.startswith('win'):
            # Strip quotes if present
            arch = os.environ.get('ARCHITECTURE', 'x64')
            if isinstance(arch, str):
                arch = arch.strip('"\'')
                
            print(f"Architecture from environment: '{arch}'")
            
            if arch in ['x86', 'win32']:
                self.plat_name = "win32"
                platform_dir = "win32"
            elif arch == 'arm64':
                self.plat_name = "win_arm64"
                platform_dir = "win_arm64"
            else:  # Default to x64/amd64
                self.plat_name = "win_amd64"
                platform_dir = "win_amd64"
            
            # Override the plat_name for the wheel
            print(f"Setting wheel platform tag to: {self.plat_name}")
            
            # Force platform-specific paths if bdist_dir is already set
            if self.bdist_dir and "win-amd64" in self.bdist_dir:
                self.bdist_dir = self.bdist_dir.replace("win-amd64", f"win-{platform_dir}")
                print(f"Using build directory: {self.bdist_dir}")

# Find all packages in the current directory
packages = find_packages()

# Determine the architecture and platform tag for the wheel
if sys.platform.startswith('win'):
    # Get architecture from environment variable or default to x64
    arch = os.environ.get('ARCHITECTURE', 'x64')
    # Strip quotes if present
    if isinstance(arch, str):
        arch = arch.strip('"\'')
    
    # Normalize architecture values
    if arch in ['x86', 'win32']:
        arch = 'x86'
        platform_tag = 'win32'
    elif arch == 'arm64':
        platform_tag = 'win_arm64'
    else:  # Default to x64/amd64
        arch = 'x64'
        platform_tag = 'win_amd64'

    print(f"Detected architecture: {arch} (platform tag: {platform_tag})")

    # Add architecture-specific packages
    packages.extend([
        f'mssql_python.libs.{arch}',
        f'mssql_python.libs.{arch}.1033',
        f'mssql_python.libs.{arch}.vcredist'
    ])
elif sys.platform.startswith('darwin'):
    # macOS platform
    import platform
    arch = os.environ.get('ARCHITECTURE', None)
    
    # Auto-detect architecture if not specified
    if arch is None:
        if platform.machine() == 'arm64':
            arch = 'arm64'
            platform_tag = 'macosx_11_0_arm64'
        else:
            arch = 'x64'
            platform_tag = 'macosx_10_9_x86_64'
    
    # Add architecture-specific packages for macOS
    packages.extend([
        f'mssql_python.libs.{arch}',
        f'mssql_python.libs.{arch}.macos'
    ])
else:
    platform_tag = 'any'  # Fallback

setup(
    name='mssql-python',
    version='0.1.6',
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
