"""
Tests for MONEY and SMALLMONEY type handling.

Validates that Python Decimal values are correctly bound and round-tripped
through MONEY, SMALLMONEY, and DECIMAL columns with proper precision handling.

Key implementation detail: MONEY-range Decimals use string binding (SQL_VARCHAR)
because SQL_NUMERIC binding fails with ODBC "Numeric value out of range" error.
String binding preserves full precision and SQL Server handles conversion.
"""

import pytest
from decimal import Decimal

# MONEY/SMALLMONEY range constants
MONEY_MIN = Decimal("-922337203685477.5808")
MONEY_MAX = Decimal("922337203685477.5807")
SMALLMONEY_MIN = Decimal("-214748.3648")
SMALLMONEY_MAX = Decimal("214748.3647")


class TestMoneyDecimalBinding:
    """Test Decimal -> MONEY column binding."""

    def test_money_positive_value(self, db_connection):
        """Test positive Decimal value inserted into MONEY column."""
        cursor = db_connection.cursor()
        val = Decimal("12345.6789")
        cursor.execute("CREATE TABLE #t_money_pos (val MONEY)")
        cursor.execute("INSERT INTO #t_money_pos VALUES (?)", (val,))
        cursor.execute("SELECT val FROM #t_money_pos")
        result = cursor.fetchone()[0]
        assert result == val
        cursor.execute("DROP TABLE #t_money_pos")
        cursor.close()

    def test_money_negative_value(self, db_connection):
        """Test negative Decimal value inserted into MONEY column."""
        cursor = db_connection.cursor()
        val = Decimal("-9999.9999")
        cursor.execute("CREATE TABLE #t_money_neg (val MONEY)")
        cursor.execute("INSERT INTO #t_money_neg VALUES (?)", (val,))
        cursor.execute("SELECT val FROM #t_money_neg")
        result = cursor.fetchone()[0]
        assert result == val
        cursor.execute("DROP TABLE #t_money_neg")
        cursor.close()

    def test_money_zero(self, db_connection):
        """Test zero value inserted into MONEY column."""
        cursor = db_connection.cursor()
        val = Decimal("0.0000")
        cursor.execute("CREATE TABLE #t_money_zero (val MONEY)")
        cursor.execute("INSERT INTO #t_money_zero VALUES (?)", (val,))
        cursor.execute("SELECT val FROM #t_money_zero")
        result = cursor.fetchone()[0]
        assert result == Decimal("0.0000")
        cursor.execute("DROP TABLE #t_money_zero")
        cursor.close()

    def test_money_max_value(self, db_connection):
        """Test maximum MONEY value."""
        cursor = db_connection.cursor()
        cursor.execute("CREATE TABLE #t_money_max (val MONEY)")
        cursor.execute("INSERT INTO #t_money_max VALUES (?)", (MONEY_MAX,))
        cursor.execute("SELECT val FROM #t_money_max")
        result = cursor.fetchone()[0]
        assert result == MONEY_MAX
        cursor.execute("DROP TABLE #t_money_max")
        cursor.close()

    def test_money_min_value(self, db_connection):
        """Test minimum MONEY value."""
        cursor = db_connection.cursor()
        cursor.execute("CREATE TABLE #t_money_min (val MONEY)")
        cursor.execute("INSERT INTO #t_money_min VALUES (?)", (MONEY_MIN,))
        cursor.execute("SELECT val FROM #t_money_min")
        result = cursor.fetchone()[0]
        assert result == MONEY_MIN
        cursor.execute("DROP TABLE #t_money_min")
        cursor.close()

    def test_money_null(self, db_connection):
        """Test NULL value inserted into MONEY column."""
        cursor = db_connection.cursor()
        cursor.execute("CREATE TABLE #t_money_null (val MONEY)")
        cursor.execute("INSERT INTO #t_money_null VALUES (?)", (None,))
        cursor.execute("SELECT val FROM #t_money_null")
        result = cursor.fetchone()[0]
        assert result is None
        cursor.execute("DROP TABLE #t_money_null")
        cursor.close()


class TestSmallmoneyDecimalBinding:
    """Test Decimal -> SMALLMONEY column binding."""

    def test_smallmoney_positive_value(self, db_connection):
        """Test positive Decimal value inserted into SMALLMONEY column."""
        cursor = db_connection.cursor()
        val = Decimal("1234.5678")
        cursor.execute("CREATE TABLE #t_sm_pos (val SMALLMONEY)")
        cursor.execute("INSERT INTO #t_sm_pos VALUES (?)", (val,))
        cursor.execute("SELECT val FROM #t_sm_pos")
        result = cursor.fetchone()[0]
        assert result == val
        cursor.execute("DROP TABLE #t_sm_pos")
        cursor.close()

    def test_smallmoney_negative_value(self, db_connection):
        """Test negative Decimal value inserted into SMALLMONEY column."""
        cursor = db_connection.cursor()
        val = Decimal("-999.1234")
        cursor.execute("CREATE TABLE #t_sm_neg (val SMALLMONEY)")
        cursor.execute("INSERT INTO #t_sm_neg VALUES (?)", (val,))
        cursor.execute("SELECT val FROM #t_sm_neg")
        result = cursor.fetchone()[0]
        assert result == val
        cursor.execute("DROP TABLE #t_sm_neg")
        cursor.close()

    def test_smallmoney_max_value(self, db_connection):
        """Test maximum SMALLMONEY value."""
        cursor = db_connection.cursor()
        cursor.execute("CREATE TABLE #t_sm_max (val SMALLMONEY)")
        cursor.execute("INSERT INTO #t_sm_max VALUES (?)", (SMALLMONEY_MAX,))
        cursor.execute("SELECT val FROM #t_sm_max")
        result = cursor.fetchone()[0]
        assert result == SMALLMONEY_MAX
        cursor.execute("DROP TABLE #t_sm_max")
        cursor.close()

    def test_smallmoney_min_value(self, db_connection):
        """Test minimum SMALLMONEY value."""
        cursor = db_connection.cursor()
        cursor.execute("CREATE TABLE #t_sm_min (val SMALLMONEY)")
        cursor.execute("INSERT INTO #t_sm_min VALUES (?)", (SMALLMONEY_MIN,))
        cursor.execute("SELECT val FROM #t_sm_min")
        result = cursor.fetchone()[0]
        assert result == SMALLMONEY_MIN
        cursor.execute("DROP TABLE #t_sm_min")
        cursor.close()

    def test_smallmoney_null(self, db_connection):
        """Test NULL value inserted into SMALLMONEY column."""
        cursor = db_connection.cursor()
        cursor.execute("CREATE TABLE #t_sm_null (val SMALLMONEY)")
        cursor.execute("INSERT INTO #t_sm_null VALUES (?)", (None,))
        cursor.execute("SELECT val FROM #t_sm_null")
        result = cursor.fetchone()[0]
        assert result is None
        cursor.execute("DROP TABLE #t_sm_null")
        cursor.close()


class TestDecimalColumnWithMoneyRangeValue:
    """Test MONEY-range Decimal values -> DECIMAL column (uses string binding)."""

    def test_decimal_column_money_range_value(self, db_connection):
        """Test MONEY-range Decimal inserted into DECIMAL column preserves precision."""
        cursor = db_connection.cursor()
        # Value in MONEY range but with more than 4 decimal places
        val = Decimal("100.123456789012345678")
        cursor.execute("CREATE TABLE #t_dec_range (val DECIMAL(38,20))")
        cursor.execute("INSERT INTO #t_dec_range VALUES (?)", (val,))
        cursor.execute("SELECT val FROM #t_dec_range")
        result = cursor.fetchone()[0]
        # String binding via format(param, "f") preserves all decimals
        assert result == val
        cursor.execute("DROP TABLE #t_dec_range")
        cursor.close()

    def test_decimal_column_truncation_by_sql_server(self, db_connection):
        """Test that SQL Server truncates to column precision (not driver)."""
        cursor = db_connection.cursor()
        val = Decimal("100.123456789")  # 9 decimal places
        cursor.execute("CREATE TABLE #t_dec_trunc (val DECIMAL(10,4))")  # Only 4 decimal places
        cursor.execute("INSERT INTO #t_dec_trunc VALUES (?)", (val,))
        cursor.execute("SELECT val FROM #t_dec_trunc")
        result = cursor.fetchone()[0]
        # SQL Server truncates to scale 4
        assert result == Decimal("100.1235")  # Rounded by SQL Server
        cursor.execute("DROP TABLE #t_dec_trunc")
        cursor.close()


class TestDecimalColumnOutsideMoneyRange:
    """Test Decimal values outside MONEY range -> DECIMAL column (uses SQL_NUMERIC)."""

    def test_decimal_above_money_max(self, db_connection):
        """Test Decimal larger than MONEY_MAX uses SQL_NUMERIC binding."""
        cursor = db_connection.cursor()
        val = Decimal("999999999999999.99")  # Above MONEY_MAX
        cursor.execute("CREATE TABLE #t_dec_above (val DECIMAL(20,2))")
        cursor.execute("INSERT INTO #t_dec_above VALUES (?)", (val,))
        cursor.execute("SELECT val FROM #t_dec_above")
        result = cursor.fetchone()[0]
        assert result == val
        cursor.execute("DROP TABLE #t_dec_above")
        cursor.close()

    def test_decimal_below_money_min(self, db_connection):
        """Test Decimal smaller than MONEY_MIN uses SQL_NUMERIC binding."""
        cursor = db_connection.cursor()
        val = Decimal("-999999999999999.99")  # Below MONEY_MIN
        cursor.execute("CREATE TABLE #t_dec_below (val DECIMAL(20,2))")
        cursor.execute("INSERT INTO #t_dec_below VALUES (?)", (val,))
        cursor.execute("SELECT val FROM #t_dec_below")
        result = cursor.fetchone()[0]
        assert result == val
        cursor.execute("DROP TABLE #t_dec_below")
        cursor.close()


class TestBoundaryEdgeCases:
    """Test boundary cases between SMALLMONEY and MONEY ranges."""

    def test_just_outside_smallmoney_to_money(self, db_connection):
        """Test value just outside SMALLMONEY range works in MONEY column."""
        cursor = db_connection.cursor()
        val = Decimal("214748.3648")  # Just above SMALLMONEY_MAX
        cursor.execute("CREATE TABLE #t_bound_out (val MONEY)")
        cursor.execute("INSERT INTO #t_bound_out VALUES (?)", (val,))
        cursor.execute("SELECT val FROM #t_bound_out")
        result = cursor.fetchone()[0]
        assert result == val
        cursor.execute("DROP TABLE #t_bound_out")
        cursor.close()

    def test_outside_smallmoney_fails_for_smallmoney_column(self, db_connection):
        """Test value outside SMALLMONEY range fails for SMALLMONEY column."""
        cursor = db_connection.cursor()
        val = Decimal("214748.3648")  # Just above SMALLMONEY_MAX
        cursor.execute("CREATE TABLE #t_bound_fail (val SMALLMONEY)")
        with pytest.raises(Exception):
            cursor.execute("INSERT INTO #t_bound_fail VALUES (?)", (val,))
        cursor.execute("DROP TABLE IF EXISTS #t_bound_fail")
        cursor.close()

    def test_outside_money_fails_for_money_column(self, db_connection):
        """Test value outside MONEY range fails for MONEY column."""
        cursor = db_connection.cursor()
        val = Decimal("922337203685477.5808")  # Just above MONEY_MAX
        cursor.execute("CREATE TABLE #t_bound_mfail (val MONEY)")
        with pytest.raises(Exception):
            cursor.execute("INSERT INTO #t_bound_mfail VALUES (?)", (val,))
        cursor.execute("DROP TABLE IF EXISTS #t_bound_mfail")
        cursor.close()

    def test_large_money_value(self, db_connection):
        """Test large value well within MONEY range but outside SMALLMONEY."""
        cursor = db_connection.cursor()
        val = Decimal("500000000000.1234")
        cursor.execute("CREATE TABLE #t_large (val MONEY)")
        cursor.execute("INSERT INTO #t_large VALUES (?)", (val,))
        cursor.execute("SELECT val FROM #t_large")
        result = cursor.fetchone()[0]
        assert result == val
        cursor.execute("DROP TABLE #t_large")
        cursor.close()


class TestPythonNumericTypesToMoney:
    """Test Python float and int -> MONEY column."""

    def test_python_float_to_money(self, db_connection):
        """Test Python float inserted into MONEY column."""
        cursor = db_connection.cursor()
        val = 123.4567
        cursor.execute("CREATE TABLE #t_float (val MONEY)")
        cursor.execute("INSERT INTO #t_float VALUES (?)", (val,))
        cursor.execute("SELECT val FROM #t_float")
        result = cursor.fetchone()[0]
        # Float may have precision issues, compare as float
        assert float(result) == pytest.approx(val, rel=1e-4)
        cursor.execute("DROP TABLE #t_float")
        cursor.close()

    def test_python_int_to_money(self, db_connection):
        """Test Python int inserted into MONEY column."""
        cursor = db_connection.cursor()
        val = 12345
        cursor.execute("CREATE TABLE #t_int (val MONEY)")
        cursor.execute("INSERT INTO #t_int VALUES (?)", (val,))
        cursor.execute("SELECT val FROM #t_int")
        result = cursor.fetchone()[0]
        assert result == Decimal("12345.0000")
        cursor.execute("DROP TABLE #t_int")
        cursor.close()


class TestStringBindingPreservesPrecision:
    """Verify that string binding via format(param, 'f') preserves all decimal places."""

    def test_format_preserves_20_decimals(self):
        """Test that format(Decimal, 'f') preserves high precision."""
        # This tests the implementation detail that string binding preserves precision
        val = Decimal("100.12345678901234567890")
        formatted = format(val, "f")
        assert formatted == "100.12345678901234567890"


class TestExecutemanyMoneySmallmoney:
    """Test executemany with MONEY and SMALLMONEY types."""

    def test_executemany_money_smallmoney(self, db_connection):
        """Test inserting multiple rows with executemany."""
        cursor = db_connection.cursor()
        cursor.execute("""
            CREATE TABLE #t_execmany (
                id INT IDENTITY PRIMARY KEY,
                m MONEY,
                sm SMALLMONEY
            )
        """)

        test_data = [
            (Decimal("12345.6789"), Decimal("987.6543")),
            (Decimal("0.0001"), Decimal("0.0100")),
            (None, Decimal("42.4200")),
            (Decimal("-1000.9900"), None),
        ]

        cursor.executemany("INSERT INTO #t_execmany (m, sm) VALUES (?, ?)", test_data)

        cursor.execute("SELECT m, sm FROM #t_execmany ORDER BY id")
        results = cursor.fetchall()
        assert len(results) == len(test_data)

        for row, expected in zip(results, test_data):
            for val, exp_val in zip(row, expected):
                if exp_val is None:
                    assert val is None
                else:
                    assert val == exp_val
                    assert isinstance(val, Decimal)

        cursor.execute("DROP TABLE #t_execmany")
        cursor.close()


class TestInvalidInputHandling:
    """Test that invalid inputs raise appropriate errors."""

    def test_invalid_string_to_money(self, db_connection):
        """Test that invalid string input raises error."""
        cursor = db_connection.cursor()
        cursor.execute("CREATE TABLE #t_invalid (val MONEY)")
        with pytest.raises(Exception):
            cursor.execute("INSERT INTO #t_invalid VALUES (?)", ("invalid_string",))
        cursor.execute("DROP TABLE IF EXISTS #t_invalid")
        cursor.close()

    def test_below_money_min_fails(self, db_connection):
        """Test value below MONEY_MIN raises error."""
        cursor = db_connection.cursor()
        val = Decimal("-922337203685477.5809")  # Just below MONEY_MIN
        cursor.execute("CREATE TABLE #t_below_min (val MONEY)")
        with pytest.raises(Exception):
            cursor.execute("INSERT INTO #t_below_min VALUES (?)", (val,))
        cursor.execute("DROP TABLE IF EXISTS #t_below_min")
        cursor.close()

    def test_below_smallmoney_min_fails(self, db_connection):
        """Test value below SMALLMONEY_MIN raises error."""
        cursor = db_connection.cursor()
        val = Decimal("-214748.3649")  # Just below SMALLMONEY_MIN
        cursor.execute("CREATE TABLE #t_below_sm_min (val SMALLMONEY)")
        with pytest.raises(Exception):
            cursor.execute("INSERT INTO #t_below_sm_min VALUES (?)", (val,))
        cursor.execute("DROP TABLE IF EXISTS #t_below_sm_min")
        cursor.close()


class TestMixedNullScenarios:
    """Test mixed NULL scenarios for MONEY and SMALLMONEY."""

    def test_money_value_smallmoney_null(self, db_connection):
        """Test MONEY has value while SMALLMONEY is NULL."""
        cursor = db_connection.cursor()
        cursor.execute("CREATE TABLE #t_mixed1 (m MONEY, sm SMALLMONEY)")
        cursor.execute("INSERT INTO #t_mixed1 VALUES (?, ?)", (Decimal("123.4500"), None))
        cursor.execute("SELECT m, sm FROM #t_mixed1")
        row = cursor.fetchone()
        assert row[0] == Decimal("123.4500")
        assert row[1] is None
        cursor.execute("DROP TABLE #t_mixed1")
        cursor.close()

    def test_money_null_smallmoney_value(self, db_connection):
        """Test MONEY is NULL while SMALLMONEY has value."""
        cursor = db_connection.cursor()
        cursor.execute("CREATE TABLE #t_mixed2 (m MONEY, sm SMALLMONEY)")
        cursor.execute("INSERT INTO #t_mixed2 VALUES (?, ?)", (None, Decimal("67.8900")))
        cursor.execute("SELECT m, sm FROM #t_mixed2")
        row = cursor.fetchone()
        assert row[0] is None
        assert row[1] == Decimal("67.8900")
        cursor.execute("DROP TABLE #t_mixed2")
        cursor.close()

    def test_both_null(self, db_connection):
        """Test both MONEY and SMALLMONEY are NULL."""
        cursor = db_connection.cursor()
        cursor.execute("CREATE TABLE #t_mixed3 (m MONEY, sm SMALLMONEY)")
        cursor.execute("INSERT INTO #t_mixed3 VALUES (?, ?)", (None, None))
        cursor.execute("SELECT m, sm FROM #t_mixed3")
        row = cursor.fetchone()
        assert row[0] is None
        assert row[1] is None
        cursor.execute("DROP TABLE #t_mixed3")
        cursor.close()
