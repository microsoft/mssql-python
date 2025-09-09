"""
Tests for temporary table support in multi-statement execution
"""
import pytest

class TestTempTableSupport:
    """Test cases for temporary table functionality"""
    
    def test_simple_temp_table_creation_and_query(self, cursor):
        """Test basic temp table creation and querying"""
        
        sql = """
        CREATE TABLE #simple_temp (id INT, name VARCHAR(50))
        INSERT INTO #simple_temp VALUES (1, 'Test')
        SELECT * FROM #simple_temp
        """
        
        cursor.execute(sql)
        results = cursor.fetchall()
        
        assert len(results) == 1
        assert results[0][0] == 1
        assert results[0][1] == 'Test'
    
    def test_select_into_temp_table(self, cursor):
        """Test SELECT INTO temp table pattern"""
        
        # First create a source table for the test
        cursor.execute("CREATE TABLE #source (id INT, value VARCHAR(10))")
        cursor.execute("INSERT INTO #source VALUES (1, 'data1'), (2, 'data2')")
        
        sql = """
        SELECT id, value INTO #target FROM #source WHERE id > 0
        SELECT COUNT(*) FROM #target
        """
        
        cursor.execute(sql)
        result = cursor.fetchone()
        
        assert result[0] == 2
    
    def test_complex_temp_table_query(self, cursor):
        """Test the actual production query pattern"""
        
        # Simplified version of the production query
        sql = """
        IF OBJECT_ID('tempdb..#TempTest') IS NOT NULL DROP TABLE #TempTest
        
        SELECT 
            'Test' as category,
            1 as count_val
        INTO #TempTest
        
        SELECT category, count_val, count_val * 2 as doubled
        FROM #TempTest
        """
        
        cursor.execute(sql)
        results = cursor.fetchall()
        
        assert len(results) == 1
        assert results[0][0] == 'Test'
        assert results[0][1] == 1
        assert results[0][2] == 2
    
    def test_temp_table_with_parameters(self, cursor):
        """Test temp tables work with parameterized queries"""
        
        sql = """
        CREATE TABLE #param_test (id INT, active BIT)
        INSERT INTO #param_test VALUES (1, 1), (2, 0), (3, 1)
        SELECT COUNT(*) FROM #param_test WHERE active = ?
        """
        
        cursor.execute(sql, 1)
        result = cursor.fetchone()
        
        assert result[0] == 2
    
    def test_multiple_temp_tables(self, cursor):
        """Test multiple temp tables in the same query"""
        
        sql = """
        CREATE TABLE #temp1 (id INT)
        CREATE TABLE #temp2 (name VARCHAR(50))
        INSERT INTO #temp1 VALUES (1), (2)
        INSERT INTO #temp2 VALUES ('First'), ('Second')
        SELECT t1.id, t2.name FROM #temp1 t1 CROSS JOIN #temp2 t2
        """
        
        cursor.execute(sql)
        results = cursor.fetchall()
        
        # Should have 4 results (2 x 2 cross join)
        assert len(results) == 4
    
    def test_regular_query_unchanged(self, cursor):
        """Ensure non-temp-table queries work as before"""
        
        # This should not trigger temp table handling
        cursor.execute("SELECT 1 as test_value")
        result = cursor.fetchone()
        
        assert result[0] == 1
    
    def test_global_temp_table_ignored(self, cursor):
        """Global temp tables (##) should not trigger the enhancement"""
        
        sql = """
        SELECT 1 as id INTO ##global_temp
        SELECT * FROM ##global_temp
        DROP TABLE ##global_temp
        """
        
        cursor.execute(sql)
        result = cursor.fetchone()
        
        assert result[0] == 1
    
    def test_single_select_into_ignored(self, cursor):
        """Single SELECT INTO without multiple statements should not trigger enhancement"""
        
        # Single statement should not trigger temp table handling
        cursor.execute("SELECT 1 as id INTO #single_temp")
        
        # Clean up
        cursor.execute("DROP TABLE #single_temp")
    
    def test_production_query_pattern(self, cursor):
        """Test a realistic production query pattern with joins"""
        
        sql = """
        IF OBJECT_ID('tempdb..#TempEdi') IS NOT NULL DROP TABLE #TempEdi
        
        -- Create some test data first
        CREATE TABLE #TestOrders (OrderID INT, CustomerID INT, Amount DECIMAL(10,2))
        CREATE TABLE #TestCustomers (CustomerID INT, CustomerName VARCHAR(50))
        INSERT INTO #TestOrders VALUES (1, 100, 250.00), (2, 101, 150.00), (3, 100, 300.00)
        INSERT INTO #TestCustomers VALUES (100, 'Customer A'), (101, 'Customer B')
        
        -- Main query pattern similar to production
        SELECT 
            c.CustomerName as customer_name,
            SUM(o.Amount) as total_amount,
            COUNT(*) as order_count
        INTO #TempEdi
        FROM #TestOrders o
        LEFT JOIN #TestCustomers c ON o.CustomerID = c.CustomerID
        GROUP BY c.CustomerName

        -- Final result query
        SELECT customer_name, total_amount, order_count
        FROM #TempEdi
        ORDER BY total_amount DESC
        """
        
        cursor.execute(sql)
        results = cursor.fetchall()
        
        assert len(results) == 2
        # Should be ordered by total_amount DESC
        assert results[0][1] == 550.00  # Customer A: 250 + 300
        assert results[1][1] == 150.00  # Customer B: 150


class TestTempTableDetection:
    """Test the temp table detection logic itself"""
    
    def test_detection_method_exists(self, cursor):
        """Test that the detection methods exist"""
        assert hasattr(cursor, '_is_multistatement_query')
        assert hasattr(cursor, '_add_nocount_to_multistatement_sql')
    
    def test_temp_table_detection(self, cursor):
        """Test the multi-statement detection logic directly"""
        
        # Should detect multi-statement queries
        sql_with_temp = """
        SELECT col1, col2 INTO #temp FROM table1
        SELECT * FROM #temp
        """
        assert cursor._is_multistatement_query(sql_with_temp)
        
        # Should not detect (single statement)
        sql_single = "SELECT col1, col2 INTO #temp FROM table1"
        assert not cursor._is_multistatement_query(sql_single)
        
        # Should detect (multiple statements even without temp tables)
        sql_multi = """
        SELECT col1, col2 FROM table1
        SELECT col3, col4 FROM table2
        """
        assert cursor._is_multistatement_query(sql_multi)
        
        # Should detect CREATE TABLE multi-statement
        sql_create = """
        CREATE TABLE #temp (id INT)
        INSERT INTO #temp VALUES (1)
        SELECT * FROM #temp
        """
        assert cursor._is_multistatement_query(sql_create)
    
    def test_nocount_addition(self, cursor):
        """Test the NOCOUNT addition logic"""
        
        sql_without_nocount = "SELECT * FROM table1"
        result = cursor._add_nocount_to_multistatement_sql(sql_without_nocount)
        assert result.startswith('SET NOCOUNT ON;')
        
        # Should not add if already present
        sql_with_nocount = "SET NOCOUNT ON; SELECT * FROM table1"
        result = cursor._add_nocount_to_multistatement_sql(sql_with_nocount)
        assert result.count('SET NOCOUNT ON') == 1


class TestTempTableBehaviorComparison:
    """Test before/after behavior comparison"""
    
    def test_before_fix_simulation(self, cursor):
        """
        Test what would happen without the fix (for documentation purposes)
        This test validates that our fix is working by ensuring temp tables work
        """
        
        # This pattern would previously fail silently (return empty results)
        # With our fix, it should work correctly
        sql = """
        CREATE TABLE #before_fix_test (id INT, value VARCHAR(10))
        INSERT INTO #before_fix_test VALUES (1, 'works')
        SELECT value FROM #before_fix_test WHERE id = 1
        """
        
        cursor.execute(sql)
        result = cursor.fetchone()
        
        # With the fix, this should return the expected result
        assert result is not None
        assert result[0] == 'works'
    
    def test_after_fix_behavior(self, cursor):
        """Test that the fix enables the expected behavior"""
        
        # Complex multi-statement query that should work with the fix
        sql = """
        IF OBJECT_ID('tempdb..#FixTest') IS NOT NULL DROP TABLE #FixTest
        
        SELECT 
            1 as test_id,
            'success' as status
        INTO #FixTest
        
        SELECT 
            test_id,
            status,
            CASE WHEN status = 'success' THEN 'PASSED' ELSE 'FAILED' END as result
        FROM #FixTest
        """
        
        cursor.execute(sql)
        result = cursor.fetchone()
        
        assert result is not None
        assert result[0] == 1
        assert result[1] == 'success'
        assert result[2] == 'PASSED'


if __name__ == '__main__':
    pytest.main([__file__])