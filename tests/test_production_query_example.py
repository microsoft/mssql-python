"""
Test the specific production query example from the contribution plan
"""
import pytest


class TestProductionQueryExample:
    """Test the specific production query that was failing before the fix"""
    
    def test_production_query_pattern_simplified(self, cursor):
        """
        Test a simplified version of the production query to verify the fix works.
        The original query was too complex with external database references,
        so this creates a similar pattern with the same temp table logic.
        """
        
        # Create mock tables to simulate the production environment
        setup_sql = """
        -- Mock the various tables referenced in the original query
        CREATE TABLE #MockPalesuDati (
            Palikna_ID VARCHAR(50),
            piepr_sk INT,
            OrderNum VARCHAR(100),
            bsid VARCHAR(50),
            group_id INT,
            RawDataID_group_id INT,
            paliktna_id INT,
            konusa_id INT
        )
        
        CREATE TABLE #MockRawDataIds (
            group_id INT,
            RawDataID INT
        )
        
        CREATE TABLE #MockOrderRawData (
            id INT,
            OrderNum VARCHAR(100)
        )
        
        CREATE TABLE #MockPalikni (
            ID INT,
            Palikna_ID VARCHAR(50)
        )
        
        -- Insert test data
        INSERT INTO #MockPalesuDati VALUES 
            ('PAL001', 10, 'ORD001-01', 'BS001', 1, 1, 1, 7),
            ('PAL002', 15, 'ORD002-01', 'BS002', 2, 2, 2, 7),
            (NULL, 5, 'ORD003-01', 'BS003', 3, 3, 3, 7)
            
        INSERT INTO #MockRawDataIds VALUES (1, 101), (2, 102), (3, 103)
        INSERT INTO #MockOrderRawData VALUES (101, 'ORD001-01'), (102, 'ORD002-01'), (103, 'ORD003-01')
        INSERT INTO #MockPalikni VALUES (1, 'PAL001'), (2, 'PAL002'), (3, 'PAL003')
        """
        cursor.execute(setup_sql)
        
        # Now test the production query pattern (simplified)
        production_query = """
        -- This mirrors the structure of the original failing query
        IF OBJECT_ID('tempdb..#TempEdi') IS NOT NULL DROP TABLE #TempEdi
        
        SELECT 
            COALESCE(d.Palikna_ID, N'Nav norādīts') as pal_bsid,
            SUM(a.piepr_sk) as piepr_sk,
            LEFT(a.OrderNum, LEN(a.OrderNum) - 2) as pse,
            a.bsid,
            a.group_id
        INTO #TempEdi
        FROM #MockPalesuDati a
        LEFT JOIN #MockRawDataIds b ON a.RawDataID_group_id = b.group_id
        LEFT JOIN #MockOrderRawData c ON b.RawDataID = c.id
        LEFT JOIN #MockPalikni d ON a.paliktna_id = d.ID
        WHERE a.konusa_id = 7
        GROUP BY COALESCE(d.Palikna_ID, N'Nav norādīts'), LEFT(a.OrderNum, LEN(a.OrderNum) - 2), a.bsid, a.group_id

        -- Second part of the query that uses the temp table
        SELECT 
            te.pal_bsid,
            te.piepr_sk,
            te.pse,
            te.bsid,
            te.group_id,
            'TEST_RESULT' as test_status
        FROM #TempEdi te
        ORDER BY te.bsid
        """
        
        # Execute the production query pattern
        cursor.execute(production_query)
        results = cursor.fetchall()
        
        # Verify we get results (previously this would return empty)
        assert len(results) > 0, "Production query should return results with temp table fix"
        
        # Verify the structure and content
        for row in results:
            assert len(row) == 6, "Should have 6 columns in result"
            assert row[5] == 'TEST_RESULT', "Last column should be test status"
            assert row[0] is not None, "pal_bsid should not be None"
    
    def test_multistatement_with_complex_temp_operations(self, cursor):
        """Test complex temp table operations that would fail without the fix"""
        
        complex_query = """
        -- Complex temp table scenario
        IF OBJECT_ID('tempdb..#ComplexTemp') IS NOT NULL DROP TABLE #ComplexTemp
        
        -- Step 1: Create temp table with aggregated data
        SELECT 
            'Category_' + CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS VARCHAR(10)) as category,
            ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) * 100 as amount,
            GETDATE() as created_date
        INTO #ComplexTemp
        FROM sys.objects
        WHERE type = 'U'
        
        -- Step 2: Update the temp table (this would fail without session persistence)
        UPDATE #ComplexTemp 
        SET amount = amount * 1.1 
        WHERE category LIKE 'Category_%'
        
        -- Step 3: Select from the updated temp table
        SELECT 
            category,
            amount,
            CASE 
                WHEN amount > 500 THEN 'HIGH'
                WHEN amount > 200 THEN 'MEDIUM'
                ELSE 'LOW'
            END as amount_category,
            created_date
        FROM #ComplexTemp
        ORDER BY amount DESC
        """
        
        cursor.execute(complex_query)
        results = cursor.fetchall()
        
        # Should get results without errors
        assert isinstance(results, list), "Should return a list of results"
        
        # If there are results, verify structure
        if len(results) > 0:
            assert len(results[0]) == 4, "Should have 4 columns"
            # Verify that amounts were updated (multiplied by 1.1)
            for row in results:
                # Amount should be a multiple of 110 (100 * 1.1)
                assert row[1] % 110 == 0, f"Amount {row[1]} should be a multiple of 110"
    
    def test_nested_temp_table_operations(self, cursor):
        """Test nested operations with temp tables"""
        
        nested_query = """
        -- Create initial temp table
        SELECT 1 as level, 'root' as node_type, 0 as parent_id INTO #Hierarchy
        
        -- Add more levels to the hierarchy
        INSERT INTO #Hierarchy 
        SELECT 2, 'child', 1 FROM #Hierarchy WHERE level = 1
        
        INSERT INTO #Hierarchy 
        SELECT 3, 'grandchild', 2 FROM #Hierarchy WHERE level = 2
        
        -- Create summary temp table from the hierarchy
        SELECT 
            level,
            COUNT(*) as node_count,
            STRING_AGG(node_type, ', ') as node_types
        INTO #Summary
        FROM #Hierarchy
        GROUP BY level
        
        -- Final query joining both temp tables
        SELECT 
            h.level,
            h.node_type,
            s.node_count,
            s.node_types
        FROM #Hierarchy h
        JOIN #Summary s ON h.level = s.level
        ORDER BY h.level, h.node_type
        """
        
        cursor.execute(nested_query)
        results = cursor.fetchall()
        
        # Verify we get the expected hierarchical structure
        assert len(results) >= 3, "Should have at least 3 rows (root, child, grandchild levels)"
        
        # Check that we have different levels
        levels = [row[0] for row in results]
        assert 1 in levels, "Should have level 1 (root)"
        assert 2 in levels, "Should have level 2 (child)"
        assert 3 in levels, "Should have level 3 (grandchild)"


if __name__ == '__main__':
    pytest.main([__file__])