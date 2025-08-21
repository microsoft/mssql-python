"""
This file contains tests for the global variables in the mssql_python package.
Functions:
- test_apilevel: Check if apilevel has the expected value.
- test_threadsafety: Check if threadsafety has the expected value.
- test_paramstyle: Check if paramstyle has the expected value.
- test_lowercase: Check if lowercase has the expected value.
"""

import pytest

# Import global variables from the repository
from mssql_python import apilevel, threadsafety, paramstyle, lowercase, getDecimalSeparator, setDecimalSeparator

def test_apilevel():
    # Check if apilevel has the expected value
    assert apilevel == "2.0", "apilevel should be '2.0'"

def test_threadsafety():
    # Check if threadsafety has the expected value
    assert threadsafety == 1, "threadsafety should be 1"

def test_paramstyle():
    # Check if paramstyle has the expected value
    assert paramstyle == "qmark", "paramstyle should be 'qmark'"

def test_lowercase():
    # Check if lowercase has the expected default value
    assert lowercase is False, "lowercase should default to False"

def test_decimal_separator():
    """Test decimal separator functionality"""
    
    # Check default value
    assert getDecimalSeparator() == '.', "Default decimal separator should be '.'"
    
    try:
        # Test setting a new value
        setDecimalSeparator(',')
        assert getDecimalSeparator() == ',', "Decimal separator should be ',' after setting"
        
        # Test invalid input
        with pytest.raises(ValueError):
            setDecimalSeparator('too long')
            
        with pytest.raises(ValueError):
            setDecimalSeparator('')
            
        with pytest.raises(ValueError):
            setDecimalSeparator(123)  # Non-string input
    
    finally:
        # Restore default value
        setDecimalSeparator('.')
        assert getDecimalSeparator() == '.', "Decimal separator should be restored to '.'"

