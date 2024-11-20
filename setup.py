from setuptools import setup, find_packages

setup(
    name='mssql-python',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[],
    author='Microsoft Corporation',
    author_email='pysqldriver@microsoft.com',
    description='A Python package for interacting with MSSQL databases',
    url='https://sqlclientdrivers.visualstudio.com/mssql-python',  # Replace with your project's URL
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
)