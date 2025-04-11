import os
import sys
from setuptools import setup, find_packages
from setuptools.dist import Distribution

# Custom distribution to force platform-specific wheel
class BinaryDistribution(Distribution):
    def has_ext_modules(self):
        return True

# Determine the platform tag for the wheel
if sys.platform.startswith('win'):
    if os.environ.get('ARCHITECTURE') == 'arm64':
        platform_tag = 'win_arm64'
    elif os.environ.get('ARCHITECTURE') == 'x86' or os.environ.get('ARCHITECTURE') == 'win32':
        platform_tag = 'win32'
    else:  # Default to x64/amd64
        platform_tag = 'win_amd64'
else:
    platform_tag = 'any'  # Fallback

setup(
    name='mssql-python',
    version='0.1.5',
    description='A Python library for interacting with Microsoft SQL Server',
    long_description=open('README.md', encoding='utf-8').read(),
    long_description_content_type='text/markdown',
    author='Microsoft Corporation',
    author_email='pysqldriver@microsoft.com',
    url='https://github.com/microsoft/mssql-python',
    packages=find_packages(),
    package_data={
        # Include PYD and DLL files inside mssql_python, exclude YML files
        'mssql_python': [
            'ddbc_bindings.cp*.pyd',  # Include all PYD files
            'libs/*', 
            'libs/**/*', 
            '*.dll'
        ]
    },
    include_package_data=True,
    # Requires >= Python 3.9
    python_requires='>=3.9',
    zip_safe=False,
    # Force binary distribution
    distclass=BinaryDistribution,
    exclude_package_data={
        '': ['*.yml', '*.yaml'],  # Exclude YML files
        'mssql_python': [
            'libs/*/vcredist/*', 'libs/*/vcredist/**/*',  # Exclude vcredist directories, added here since `'libs/*' is already included`
        ],
    },
)
