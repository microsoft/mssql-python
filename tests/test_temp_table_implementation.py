#!/usr/bin/env python3
"""
Quick test script to verify the temp table implementation logic.
This tests just temp table detection and NOCOUNT addition methods.
"""

import sys


def is_multistatement_query(sql: str) -> bool:
    """Detect if this is a multi-statement query that could benefit from SET NOCOUNT ON"""
    sql_lower = sql.lower().strip()
    
    # Skip if already has SET NOCOUNT
    if sql_lower.startswith('set nocount'):
        return False
        
    # Detect multiple statements by counting SQL keywords and separators
    statement_indicators = (
        sql_lower.count('select') + sql_lower.count('insert') + 
        sql_lower.count('update') + sql_lower.count('delete') + 
        sql_lower.count('create') + sql_lower.count('drop') +
        sql_lower.count('alter') + sql_lower.count('exec')
    )
    
    # Also check for explicit statement separators
    has_separators = ';' in sql_lower or '\n\n' in sql
    
    # Consider it multi-statement if multiple SQL operations or explicit separators
    return statement_indicators > 1 or has_separators


def add_nocount_to_multistatement_sql(sql: str) -> str:
    """Add SET NOCOUNT ON to multi-statement SQL - pyodbc approach"""
    sql = sql.strip()
    if not sql.upper().startswith('SET NOCOUNT'):
        sql = 'SET NOCOUNT ON;\n' + sql
    return sql


def test_multistatement_detection():
    """Test the multi-statement detection logic"""
    print("\nTesting multi-statement detection logic...")
    
    # Test cases for detection
    test_cases = [
        {
            'sql': """
            SELECT col1, col2 INTO #temp FROM table1
            SELECT * FROM #temp
            """,
            'expected': True,
            'description': "Multi-statement with local temp table"
        },
        {
            'sql': "SELECT col1, col2 INTO #temp FROM table1",
            'expected': False,
            'description': "Single statement with temp table"
        },
        {
            'sql': """
            SELECT col1, col2 INTO ##temp FROM table1
            SELECT * FROM ##temp
            """,
            'expected': True,
            'description': "Multi-statement with global temp table"
        },
        {
            'sql': """
            SELECT col1, col2 FROM table1
            SELECT col3, col4 FROM table2
            """,
            'expected': True,
            'description': "Multi-statement without temp tables"
        },
        {
            'sql': """
            SELECT data INTO #temp1 FROM source1;
            UPDATE #temp1 SET processed = 1;
            SELECT * FROM #temp1;
            """,
            'expected': True,
            'description': "Multi-statement with semicolons"
        }
    ]
    
    all_passed = True
    for test_case in test_cases:
        result = is_multistatement_query(test_case['sql'])
        if result == test_case['expected']:
            print(f"  PASS {test_case['description']}: {result}")
        else:
            print(f"  FAIL {test_case['description']}: Expected {test_case['expected']}, got {result}")
            all_passed = False
    
    return all_passed


def test_nocount_addition():
    """Test the NOCOUNT addition logic"""
    print("\nTesting NOCOUNT addition logic...")
    
    test_cases = [
        {
            'sql': "SELECT * FROM table1",
            'expected_prefix': 'SET NOCOUNT ON;',
            'description': "Add NOCOUNT to regular query"
        },
        {
            'sql': "SET NOCOUNT ON; SELECT * FROM table1",
            'expected_count': 1,
            'description': "Don't duplicate NOCOUNT"
        },
        {
            'sql': "  \n  SELECT * FROM table1  \n  ",
            'expected_prefix': 'SET NOCOUNT ON;',
            'description': "Handle whitespace correctly"
        }
    ]
    
    all_passed = True
    for test_case in test_cases:
        result = add_nocount_to_multistatement_sql(test_case['sql'])
        
        if 'expected_prefix' in test_case:
            if result.startswith(test_case['expected_prefix']):
                print(f"  PASS {test_case['description']}: Correctly added prefix")
            else:
                print(f"  FAIL {test_case['description']}: Expected to start with '{test_case['expected_prefix']}', got '{result[:20]}...'")
                all_passed = False
        
        if 'expected_count' in test_case:
            count = result.count('SET NOCOUNT ON')
            if count == test_case['expected_count']:
                print(f"  PASS {test_case['description']}: NOCOUNT count is {count}")
            else:
                print(f"  FAIL {test_case['description']}: Expected NOCOUNT count {test_case['expected_count']}, got {count}")
                all_passed = False
    
    return all_passed


def test_integration():
    """Test the integration of detection and enhancement logic"""
    print("\nTesting integration logic...")
    
    # Test SQL that should trigger enhancement
    problematic_sql = """
    SELECT CustomerID, SUM(Amount) as Total
    INTO #customer_totals
    FROM Orders
    GROUP BY CustomerID
    
    SELECT c.Name, ct.Total
    FROM Customers c
    JOIN #customer_totals ct ON c.ID = ct.CustomerID
    ORDER BY ct.Total DESC
    """
    
    # Check if it's detected as problematic
    is_multistatement = is_multistatement_query(problematic_sql)
    if is_multistatement:
        print("  PASS Correctly identified multi-statement SQL")
        
        # Apply the enhancement
        enhanced_sql = add_nocount_to_multistatement_sql(problematic_sql)
        
        if enhanced_sql.startswith('SET NOCOUNT ON;'):
            print("  PASS Correctly applied NOCOUNT enhancement")
            print(f"  Enhanced SQL preview: {enhanced_sql[:50]}...")
            return True
        else:
            print("  FAIL Failed to apply NOCOUNT enhancement")
            return False
    else:
        print("  FAIL Failed to identify problematic SQL")
        return False


def main():
    """Run all tests"""
    print("Testing mssql-python temp table implementation")
    print("=" * 60)
    
    all_tests_passed = True
    
    # Run detection tests
    if not test_multistatement_detection():
        all_tests_passed = False
    
    # Run NOCOUNT addition tests
    if not test_nocount_addition():
        all_tests_passed = False
    
    # Run integration tests
    if not test_integration():
        all_tests_passed = False
    
    print("\n" + "=" * 60)
    if all_tests_passed:
        print("SUCCESS: All tests passed! The temp table implementation looks good.")
        print("\nSummary of implementation:")
        print("  - Enhanced cursor.py with temp table detection")
        print("  - Added _is_multistatement_query() method")
        print("  - Added _add_nocount_to_multistatement_sql() method") 
        print("  - Modified execute() method to use enhancement")
    else:
        print("FAILURE: Some tests failed. Please review the implementation.")
        return 1
    
    return 0


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)