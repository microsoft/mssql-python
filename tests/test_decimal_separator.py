import pytest
import mssql_python
import os
from decimal import Decimal

# Helper to get connection
def get_connection():
    server = os.getenv("TEST_SERVER", "localhost")
    database = os.getenv("TEST_DATABASE", "master")
    username = os.getenv("TEST_USERNAME", "sa")
    password = os.getenv("TEST_PASSWORD", "yourStrong(!)Password")
    driver = os.getenv("TEST_DRIVER", "ODBC Driver 17 for SQL Server")
    
    conn_str = f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes"
    return mssql_python.connect(conn_str)

@pytest.mark.skipif(not os.getenv("TEST_SERVER"), reason="TEST_SERVER not set")
def test_decimal_separator_bug():
    """
    Test that fetchall() dealing with DECIMALS works correctly even when 
    setDecimalSeparator is set to something other than '.'
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # Create a temp table
        cursor.execute("CREATE TABLE #TestDecimal (Val DECIMAL(10, 2))")
        cursor.execute("INSERT INTO #TestDecimal VALUES (1234.56)")
        cursor.execute("INSERT INTO #TestDecimal VALUES (78.90)")
        conn.commit()

        # Set custom separator
        mssql_python.setDecimalSeparator(",")
        
        # Test fetchall
        cursor.execute("SELECT Val FROM #TestDecimal ORDER BY Val")
        rows = cursor.fetchall()
        
        # Verify fetchall results
        assert len(rows) == 2, f"Expected 2 rows, got {len(rows)}"
        assert isinstance(rows[0][0], Decimal), f"Expected Decimal, got {type(rows[0][0])}"
        assert rows[0][0] == Decimal("78.90"), f"Expected 78.90, got {rows[0][0]}"
        assert rows[1][0] == Decimal("1234.56"), f"Expected 1234.56, got {rows[1][0]}"
        
        # Verify fetchmany
        cursor.execute("SELECT Val FROM #TestDecimal ORDER BY Val")
        batch = cursor.fetchmany(2)
        assert len(batch) == 2
        assert batch[1][0] == Decimal("1234.56")

        # Verify fetchone behavior is consistent
        cursor.execute("SELECT CAST(99.99 AS DECIMAL(10,2))")
        val = cursor.fetchone()[0]
        assert isinstance(val, Decimal)
        assert val == Decimal("99.99")

    finally:
        # Reset separator to default just in case
        mssql_python.setDecimalSeparator(".")
        cursor.execute("DROP TABLE #TestDecimal")
        conn.close()

if __name__ == "__main__":
    # Allow running as a script too
    try:
        test_decimal_separator_bug()
        print("Test PASSED")
    except Exception as e:
        print(f"Test FAILED: {e}")
