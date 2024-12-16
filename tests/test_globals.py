import pytest

# Import global variables from the repository
from mssql_python import apilevel, threadsafety, paramstyle

def test_apilevel():
    # Check if apilevel has the expected value
    assert apilevel == "2.0", "apilevel should be '2.0'"

def test_threadsafety():
    # Check if threadsafety has the expected value
    assert threadsafety == 1, "threadsafety should be 1"

def test_paramstyle():
    # Check if paramstyle has the expected value
    assert paramstyle == "pyformat", "paramstyle should be 'pyformat'"