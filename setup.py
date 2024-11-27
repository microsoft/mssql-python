from setuptools import setup, find_packages

setup(
    name='mssql-python',
    version='0.1.0',
    description='A Python library for interacting with Microsoft SQL Server',
    author='Microsoft Corporation',
    author_email='pysqldriver@microsoft.com',
    url='https://sqlclientdrivers.visualstudio.com/mssql-python',
    packages=find_packages(),
    install_requires=[],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
)