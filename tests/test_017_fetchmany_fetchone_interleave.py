"""
Test for GitHub Issue #427: Segmentation fault when interleaving fetchmany() and fetchone()
https://github.com/microsoft/mssql-python/issues/427
"""
import pytest


def test_fetchmany_then_fetchone_interleave(cursor):
    """
    Test that fetchmany() followed by fetchone() doesn't cause segfault.
    This was the original failing case from issue #427.
    """
    cursor.execute("SELECT 1 UNION SELECT 2")
    
    result1 = cursor.fetchmany(1)
    assert len(result1) == 1
    assert result1[0][0] == 1
    
    # This used to cause segfault on Linux, return None on Windows
    result2 = cursor.fetchone()
    assert result2 is not None
    assert result2[0] == 2


def test_fetchone_then_fetchmany_interleave(cursor):
    """Test that fetchone() followed by fetchmany() works correctly."""
    cursor.execute("SELECT 1 UNION SELECT 2 UNION SELECT 3")
    
    result1 = cursor.fetchone()
    assert result1[0] == 1
    
    result2 = cursor.fetchmany(2)
    assert len(result2) == 2
    assert result2[0][0] == 2
    assert result2[1][0] == 3


def test_multiple_interleaved_fetches(cursor):
    """Test multiple alternating fetchmany() and fetchone() calls."""
    cursor.execute("SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4")
    
    result = cursor.fetchmany(1)
    assert result[0][0] == 1
    
    result = cursor.fetchone()
    assert result[0] == 2
    
    result = cursor.fetchmany(1)
    assert result[0][0] == 3
    
    result = cursor.fetchone()
    assert result[0] == 4
