import os
import sys
import subprocess
from setuptools import setup, Extension, find_packages
from setuptools.command.build_ext import build_ext

class CMakeExtension(Extension):
    def __init__(self, name, sourcedir=''):
        # No sources; CMake handles the build.
        super().__init__(name, sources=[])
        self.sourcedir = os.path.abspath(sourcedir)

class CMakeBuild(build_ext):
    def run(self):
        # Check if CMake is installed
        try:
            subprocess.check_output(['cmake', '--version'])
        except OSError:
            raise RuntimeError("CMake must be installed to build these extensions.")
        for ext in self.extensions:
            self.build_extension(ext)

    def build_extension(self, ext):
        # Calculate the directory where the final .pyd will be placed (inside mssql_python)
        extdir = os.path.abspath(os.path.dirname(self.get_ext_fullpath(ext.name)))
        cfg = 'Debug' if self.debug else 'Release'
        cmake_args = [
            '-DCMAKE_LIBRARY_OUTPUT_DIRECTORY=' + extdir,
            '-DCMAKE_LIBRARY_OUTPUT_DIRECTORY_RELEASE=' + extdir,
            '-DCMAKE_RUNTIME_OUTPUT_DIRECTORY_RELEASE=' + extdir,
            '-DPYTHON_EXECUTABLE=' + sys.executable,
            '-DCMAKE_BUILD_TYPE=' + cfg
        ]
        build_args = ['--config', cfg]

        if not os.path.exists(self.build_temp):
            os.makedirs(self.build_temp)

        # Configure CMake project
        subprocess.check_call(['cmake', ext.sourcedir] + cmake_args, cwd=self.build_temp)
        # Build the target defined in your CMakeLists.txt
        subprocess.check_call(['cmake', '--build', '.', '--target', 'ddbc_bindings'] + build_args, cwd=self.build_temp)

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
        # Include DLL files inside mssql_python
        'mssql_python': ['libs/*', 'libs/**/*', '*.dll']
    },
    include_package_data=True,
    # Requires Python 3.13
    python_requires='==3.13.*',
    # Naming the extension as mssql_python.ddbc_bindings puts the .pyd directly in mssql_python
    ext_modules=[CMakeExtension('mssql_python.ddbc_bindings', sourcedir='mssql_python/pybind')],
    cmdclass={'build_ext': CMakeBuild},
    zip_safe=False,
)
