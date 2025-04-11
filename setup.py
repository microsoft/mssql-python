import os
from setuptools import setup, find_packages

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
        # Include PYD and DLL files inside mssql_python
        'mssql_python': [
            'ddbc_bindings.cp*.pyd',  # Include all PYD files
            'libs/*', 
            'libs/**/*', 
            '*.dll'
        ]
    },
    include_package_data=True,
    # Requires Python 3.13
    python_requires='==3.13.*',
    zip_safe=False,
)
