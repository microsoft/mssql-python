"""
Tests for MONEY and SMALLMONEY type handling.

Validates that Python Decimal values are correctly bound and round-tripped
through MONEY, SMALLMONEY, and DECIMAL columns with proper precision handling.

Key implementation detail: on the execute() path every finite Decimal binds as
SQL_NUMERIC using its own precision and scale, regardless of value. Binding no longer
depends on whether the value falls in the MONEY/SMALLMONEY range, so an in-range value
compared against a smaller numeric column returns no match instead of a varchar->numeric
overflow (GH-740). executemany still string-binds Decimals (SQL_VARCHAR) to preserve
scale-38 precision (GH-503), so that path is unchanged here.
"""

import pytest
from decimal import Decimal

# MONEY/SMALLMONEY range constants
MONEY_MIN = Decimal("-922337203685477.5808")
MONEY_MAX = Decimal("922337203685477.5807")
SMALLMONEY_MIN = Decimal("-214748.3648")
SMALLMONEY_MAX = Decimal("214748.3647")


def drop_table_if_exists(cursor, table_name):
    """Drop the table if it exists."""
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    except Exception:
        pass  # Ignore errors during cleanup


# =============================================================================
# MONEY Decimal Binding Tests
# =============================================================================


def test_money_positive_value(cursor, db_connection):
    """Test positive Decimal value inserted into MONEY column."""
    table_name = "#pytest_money_pos"
    try:
        drop_table_if_exists(cursor, table_name)
        cursor.execute(f"CREATE TABLE {table_name} (val MONEY)")
        db_connection.commit()

        val = Decimal("12345.6789")
        cursor.execute(f"INSERT INTO {table_name} VALUES (?)", (val,))
        db_connection.commit()

        cursor.execute(f"SELECT val FROM {table_name}")
        result = cursor.fetchone()[0]
        assert result == val
    finally:
        drop_table_if_exists(cursor, table_name)
        db_connection.commit()


def test_money_negative_value(cursor, db_connection):
    """Test negative Decimal value inserted into MONEY column."""
    table_name = "#pytest_money_neg"
    try:
        drop_table_if_exists(cursor, table_name)
        cursor.execute(f"CREATE TABLE {table_name} (val MONEY)")
        db_connection.commit()

        val = Decimal("-9999.9999")
        cursor.execute(f"INSERT INTO {table_name} VALUES (?)", (val,))
        db_connection.commit()

        cursor.execute(f"SELECT val FROM {table_name}")
        result = cursor.fetchone()[0]
        assert result == val
    finally:
        drop_table_if_exists(cursor, table_name)
        db_connection.commit()


def test_money_zero(cursor, db_connection):
    """Test zero value inserted into MONEY column."""
    table_name = "#pytest_money_zero"
    try:
        drop_table_if_exists(cursor, table_name)
        cursor.execute(f"CREATE TABLE {table_name} (val MONEY)")
        db_connection.commit()

        val = Decimal("0.0000")
        cursor.execute(f"INSERT INTO {table_name} VALUES (?)", (val,))
        db_connection.commit()

        cursor.execute(f"SELECT val FROM {table_name}")
        result = cursor.fetchone()[0]
        assert result == Decimal("0.0000")
    finally:
        drop_table_if_exists(cursor, table_name)
        db_connection.commit()


def test_money_max_value(cursor, db_connection):
    """Test maximum MONEY value."""
    table_name = "#pytest_money_max"
    try:
        drop_table_if_exists(cursor, table_name)
        cursor.execute(f"CREATE TABLE {table_name} (val MONEY)")
        db_connection.commit()

        cursor.execute(f"INSERT INTO {table_name} VALUES (?)", (MONEY_MAX,))
        db_connection.commit()

        cursor.execute(f"SELECT val FROM {table_name}")
        result = cursor.fetchone()[0]
        assert result == MONEY_MAX
    finally:
        drop_table_if_exists(cursor, table_name)
        db_connection.commit()


def test_money_min_value(cursor, db_connection):
    """Test minimum MONEY value."""
    table_name = "#pytest_money_min"
    try:
        drop_table_if_exists(cursor, table_name)
        cursor.execute(f"CREATE TABLE {table_name} (val MONEY)")
        db_connection.commit()

        cursor.execute(f"INSERT INTO {table_name} VALUES (?)", (MONEY_MIN,))
        db_connection.commit()

        cursor.execute(f"SELECT val FROM {table_name}")
        result = cursor.fetchone()[0]
        assert result == MONEY_MIN
    finally:
        drop_table_if_exists(cursor, table_name)
        db_connection.commit()


def test_money_null(cursor, db_connection):
    """Test NULL value inserted into MONEY column."""
    table_name = "#pytest_money_null"
    try:
        drop_table_if_exists(cursor, table_name)
        cursor.execute(f"CREATE TABLE {table_name} (val MONEY)")
        db_connection.commit()

        cursor.execute(f"INSERT INTO {table_name} VALUES (?)", (None,))
        db_connection.commit()

        cursor.execute(f"SELECT val FROM {table_name}")
        result = cursor.fetchone()[0]
        assert result is None
    finally:
        drop_table_if_exists(cursor, table_name)
        db_connection.commit()


# =============================================================================
# SMALLMONEY Decimal Binding Tests
# =============================================================================


def test_smallmoney_positive_value(cursor, db_connection):
    """Test positive Decimal value inserted into SMALLMONEY column."""
    table_name = "#pytest_smallmoney_pos"
    try:
        drop_table_if_exists(cursor, table_name)
        cursor.execute(f"CREATE TABLE {table_name} (val SMALLMONEY)")
        db_connection.commit()

        val = Decimal("1234.5678")
        cursor.execute(f"INSERT INTO {table_name} VALUES (?)", (val,))
        db_connection.commit()

        cursor.execute(f"SELECT val FROM {table_name}")
        result = cursor.fetchone()[0]
        assert result == val
    finally:
        drop_table_if_exists(cursor, table_name)
        db_connection.commit()


def test_smallmoney_negative_value(cursor, db_connection):
    """Test negative Decimal value inserted into SMALLMONEY column."""
    table_name = "#pytest_smallmoney_neg"
    try:
        drop_table_if_exists(cursor, table_name)
        cursor.execute(f"CREATE TABLE {table_name} (val SMALLMONEY)")
        db_connection.commit()

        val = Decimal("-999.1234")
        cursor.execute(f"INSERT INTO {table_name} VALUES (?)", (val,))
        db_connection.commit()

        cursor.execute(f"SELECT val FROM {table_name}")
        result = cursor.fetchone()[0]
        assert result == val
    finally:
        drop_table_if_exists(cursor, table_name)
        db_connection.commit()


def test_smallmoney_max_value(cursor, db_connection):
    """Test maximum SMALLMONEY value."""
    table_name = "#pytest_smallmoney_max"
    try:
        drop_table_if_exists(cursor, table_name)
        cursor.execute(f"CREATE TABLE {table_name} (val SMALLMONEY)")
        db_connection.commit()

        cursor.execute(f"INSERT INTO {table_name} VALUES (?)", (SMALLMONEY_MAX,))
        db_connection.commit()

        cursor.execute(f"SELECT val FROM {table_name}")
        result = cursor.fetchone()[0]
        assert result == SMALLMONEY_MAX
    finally:
        drop_table_if_exists(cursor, table_name)
        db_connection.commit()


def test_smallmoney_min_value(cursor, db_connection):
    """Test minimum SMALLMONEY value."""
    table_name = "#pytest_smallmoney_min"
    try:
        drop_table_if_exists(cursor, table_name)
        cursor.execute(f"CREATE TABLE {table_name} (val SMALLMONEY)")
        db_connection.commit()

        cursor.execute(f"INSERT INTO {table_name} VALUES (?)", (SMALLMONEY_MIN,))
        db_connection.commit()

        cursor.execute(f"SELECT val FROM {table_name}")
        result = cursor.fetchone()[0]
        assert result == SMALLMONEY_MIN
    finally:
        drop_table_if_exists(cursor, table_name)
        db_connection.commit()


def test_smallmoney_null(cursor, db_connection):
    """Test NULL value inserted into SMALLMONEY column."""
    table_name = "#pytest_smallmoney_null"
    try:
        drop_table_if_exists(cursor, table_name)
        cursor.execute(f"CREATE TABLE {table_name} (val SMALLMONEY)")
        db_connection.commit()

        cursor.execute(f"INSERT INTO {table_name} VALUES (?)", (None,))
        db_connection.commit()

        cursor.execute(f"SELECT val FROM {table_name}")
        result = cursor.fetchone()[0]
        assert result is None
    finally:
        drop_table_if_exists(cursor, table_name)
        db_connection.commit()


# =============================================================================
# DECIMAL Column with MONEY-range Values (String Binding Path)
# =============================================================================


def test_decimal_column_money_range_value(cursor, db_connection):
    """Test MONEY-range Decimal inserted into DECIMAL column preserves precision."""
    table_name = "#pytest_money_dec_range"
    try:
        drop_table_if_exists(cursor, table_name)
        cursor.execute(f"CREATE TABLE {table_name} (val DECIMAL(38,20))")
        db_connection.commit()

        # Value in MONEY range but with more than 4 decimal places
        val = Decimal("100.123456789012345678")
        cursor.execute(f"INSERT INTO {table_name} VALUES (?)", (val,))
        db_connection.commit()

        cursor.execute(f"SELECT val FROM {table_name}")
        result = cursor.fetchone()[0]
        # String binding via format(param, "f") preserves all decimals
        assert result == val
    finally:
        drop_table_if_exists(cursor, table_name)
        db_connection.commit()


def test_decimal_column_rounding_by_sql_server(cursor, db_connection):
    """Test that SQL Server rounds to column precision (not driver)."""
    table_name = "#pytest_money_dec_round"
    try:
        drop_table_if_exists(cursor, table_name)
        cursor.execute(f"CREATE TABLE {table_name} (val DECIMAL(10,4))")  # Only 4 decimal places
        db_connection.commit()

        val = Decimal("100.123456789")  # 9 decimal places
        cursor.execute(f"INSERT INTO {table_name} VALUES (?)", (val,))
        db_connection.commit()

        cursor.execute(f"SELECT val FROM {table_name}")
        result = cursor.fetchone()[0]
        # SQL Server rounds to scale 4
        assert result == Decimal("100.1235")
    finally:
        drop_table_if_exists(cursor, table_name)
        db_connection.commit()


# =============================================================================
# DECIMAL Column Outside MONEY Range (SQL_NUMERIC Binding Path)
# =============================================================================


def test_decimal_above_money_max(cursor, db_connection):
    """Test Decimal larger than MONEY_MAX uses SQL_NUMERIC binding."""
    table_name = "#pytest_money_dec_above"
    try:
        drop_table_if_exists(cursor, table_name)
        cursor.execute(f"CREATE TABLE {table_name} (val DECIMAL(20,2))")
        db_connection.commit()

        val = Decimal("999999999999999.99")  # Above MONEY_MAX
        cursor.execute(f"INSERT INTO {table_name} VALUES (?)", (val,))
        db_connection.commit()

        cursor.execute(f"SELECT val FROM {table_name}")
        result = cursor.fetchone()[0]
        assert result == val
    finally:
        drop_table_if_exists(cursor, table_name)
        db_connection.commit()


def test_decimal_below_money_min(cursor, db_connection):
    """Test Decimal smaller than MONEY_MIN uses SQL_NUMERIC binding."""
    table_name = "#pytest_money_dec_below"
    try:
        drop_table_if_exists(cursor, table_name)
        cursor.execute(f"CREATE TABLE {table_name} (val DECIMAL(20,2))")
        db_connection.commit()

        val = Decimal("-999999999999999.99")  # Below MONEY_MIN
        cursor.execute(f"INSERT INTO {table_name} VALUES (?)", (val,))
        db_connection.commit()

        cursor.execute(f"SELECT val FROM {table_name}")
        result = cursor.fetchone()[0]
        assert result == val
    finally:
        drop_table_if_exists(cursor, table_name)
        db_connection.commit()


# =============================================================================
# Boundary Edge Cases
# =============================================================================


def test_just_outside_smallmoney_to_money(cursor, db_connection):
    """Test value just outside SMALLMONEY range works in MONEY column."""
    table_name = "#pytest_money_bound_out"
    try:
        drop_table_if_exists(cursor, table_name)
        cursor.execute(f"CREATE TABLE {table_name} (val MONEY)")
        db_connection.commit()

        val = Decimal("214748.3648")  # Just above SMALLMONEY_MAX
        cursor.execute(f"INSERT INTO {table_name} VALUES (?)", (val,))
        db_connection.commit()

        cursor.execute(f"SELECT val FROM {table_name}")
        result = cursor.fetchone()[0]
        assert result == val
    finally:
        drop_table_if_exists(cursor, table_name)
        db_connection.commit()


def test_outside_smallmoney_fails_for_smallmoney_column(cursor, db_connection):
    """Test value outside SMALLMONEY range fails for SMALLMONEY column."""
    table_name = "#pytest_money_bound_fail"
    try:
        drop_table_if_exists(cursor, table_name)
        cursor.execute(f"CREATE TABLE {table_name} (val SMALLMONEY)")
        db_connection.commit()

        val = Decimal("214748.3648")  # Just above SMALLMONEY_MAX
        with pytest.raises(Exception):
            cursor.execute(f"INSERT INTO {table_name} VALUES (?)", (val,))
    finally:
        drop_table_if_exists(cursor, table_name)
        db_connection.commit()


def test_outside_money_fails_for_money_column(cursor, db_connection):
    """Test value outside MONEY range fails for MONEY column."""
    table_name = "#pytest_money_bound_mfail"
    try:
        drop_table_if_exists(cursor, table_name)
        cursor.execute(f"CREATE TABLE {table_name} (val MONEY)")
        db_connection.commit()

        val = Decimal("922337203685477.5808")  # Just above MONEY_MAX
        with pytest.raises(Exception):
            cursor.execute(f"INSERT INTO {table_name} VALUES (?)", (val,))
    finally:
        drop_table_if_exists(cursor, table_name)
        db_connection.commit()


def test_large_money_value(cursor, db_connection):
    """Test large value well within MONEY range but outside SMALLMONEY."""
    table_name = "#pytest_money_large"
    try:
        drop_table_if_exists(cursor, table_name)
        cursor.execute(f"CREATE TABLE {table_name} (val MONEY)")
        db_connection.commit()

        val = Decimal("500000000000.1234")
        cursor.execute(f"INSERT INTO {table_name} VALUES (?)", (val,))
        db_connection.commit()

        cursor.execute(f"SELECT val FROM {table_name}")
        result = cursor.fetchone()[0]
        assert result == val
    finally:
        drop_table_if_exists(cursor, table_name)
        db_connection.commit()


# =============================================================================
# Python Numeric Types to MONEY
# =============================================================================


def test_python_float_to_money(cursor, db_connection):
    """Test Python float inserted into MONEY column."""
    table_name = "#pytest_money_float"
    try:
        drop_table_if_exists(cursor, table_name)
        cursor.execute(f"CREATE TABLE {table_name} (val MONEY)")
        db_connection.commit()

        val = 123.4567
        cursor.execute(f"INSERT INTO {table_name} VALUES (?)", (val,))
        db_connection.commit()

        cursor.execute(f"SELECT val FROM {table_name}")
        result = cursor.fetchone()[0]
        # Float may have precision issues, compare as float
        assert float(result) == pytest.approx(val, rel=1e-4)
    finally:
        drop_table_if_exists(cursor, table_name)
        db_connection.commit()


def test_python_int_to_money(cursor, db_connection):
    """Test Python int inserted into MONEY column."""
    table_name = "#pytest_money_int"
    try:
        drop_table_if_exists(cursor, table_name)
        cursor.execute(f"CREATE TABLE {table_name} (val MONEY)")
        db_connection.commit()

        val = 12345
        cursor.execute(f"INSERT INTO {table_name} VALUES (?)", (val,))
        db_connection.commit()

        cursor.execute(f"SELECT val FROM {table_name}")
        result = cursor.fetchone()[0]
        assert result == Decimal("12345.0000")
    finally:
        drop_table_if_exists(cursor, table_name)
        db_connection.commit()


# =============================================================================
# String Binding Precision Verification
# =============================================================================


def test_format_preserves_20_decimals():
    """Test that format(Decimal, 'f') preserves high precision."""
    # This tests the implementation detail that string binding preserves precision
    val = Decimal("100.12345678901234567890")
    formatted = format(val, "f")
    assert formatted == "100.12345678901234567890"


# =============================================================================
# Executemany Tests
# =============================================================================


def test_executemany_money_smallmoney(cursor, db_connection):
    """Test inserting multiple rows with executemany."""
    table_name = "#pytest_money_execmany"
    try:
        drop_table_if_exists(cursor, table_name)
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                id INT IDENTITY PRIMARY KEY,
                m MONEY,
                sm SMALLMONEY
            )
        """)
        db_connection.commit()

        test_data = [
            (Decimal("12345.6789"), Decimal("987.6543")),
            (Decimal("0.0001"), Decimal("0.0100")),
            (None, Decimal("42.4200")),
            (Decimal("-1000.9900"), None),
        ]

        cursor.executemany(f"INSERT INTO {table_name} (m, sm) VALUES (?, ?)", test_data)
        db_connection.commit()

        cursor.execute(f"SELECT m, sm FROM {table_name} ORDER BY id")
        results = cursor.fetchall()
        assert len(results) == len(test_data)

        for row, expected in zip(results, test_data):
            for val, exp_val in zip(row, expected):
                if exp_val is None:
                    assert val is None
                else:
                    assert val == exp_val
                    assert isinstance(val, Decimal)
    finally:
        drop_table_if_exists(cursor, table_name)
        db_connection.commit()


# =============================================================================
# Invalid Input Handling
# =============================================================================


def test_invalid_string_to_money(cursor, db_connection):
    """Test that invalid string input raises error."""
    table_name = "#pytest_money_invalid"
    try:
        drop_table_if_exists(cursor, table_name)
        cursor.execute(f"CREATE TABLE {table_name} (val MONEY)")
        db_connection.commit()

        with pytest.raises(Exception):
            cursor.execute(f"INSERT INTO {table_name} VALUES (?)", ("invalid_string",))
    finally:
        drop_table_if_exists(cursor, table_name)
        db_connection.commit()


def test_below_money_min_fails(cursor, db_connection):
    """Test value below MONEY_MIN raises error."""
    table_name = "#pytest_money_below_min"
    try:
        drop_table_if_exists(cursor, table_name)
        cursor.execute(f"CREATE TABLE {table_name} (val MONEY)")
        db_connection.commit()

        val = Decimal("-922337203685477.5809")  # Just below MONEY_MIN
        with pytest.raises(Exception):
            cursor.execute(f"INSERT INTO {table_name} VALUES (?)", (val,))
    finally:
        drop_table_if_exists(cursor, table_name)
        db_connection.commit()


def test_below_smallmoney_min_fails(cursor, db_connection):
    """Test value below SMALLMONEY_MIN raises error."""
    table_name = "#pytest_money_below_sm_min"
    try:
        drop_table_if_exists(cursor, table_name)
        cursor.execute(f"CREATE TABLE {table_name} (val SMALLMONEY)")
        db_connection.commit()

        val = Decimal("-214748.3649")  # Just below SMALLMONEY_MIN
        with pytest.raises(Exception):
            cursor.execute(f"INSERT INTO {table_name} VALUES (?)", (val,))
    finally:
        drop_table_if_exists(cursor, table_name)
        db_connection.commit()


# =============================================================================
# Mixed NULL Scenarios
# =============================================================================


def test_money_value_smallmoney_null(cursor, db_connection):
    """Test MONEY has value while SMALLMONEY is NULL."""
    table_name = "#pytest_money_mixed1"
    try:
        drop_table_if_exists(cursor, table_name)
        cursor.execute(f"CREATE TABLE {table_name} (m MONEY, sm SMALLMONEY)")
        db_connection.commit()

        cursor.execute(f"INSERT INTO {table_name} VALUES (?, ?)", (Decimal("123.4500"), None))
        db_connection.commit()

        cursor.execute(f"SELECT m, sm FROM {table_name}")
        row = cursor.fetchone()
        assert row[0] == Decimal("123.4500")
        assert row[1] is None
    finally:
        drop_table_if_exists(cursor, table_name)
        db_connection.commit()


def test_money_null_smallmoney_value(cursor, db_connection):
    """Test MONEY is NULL while SMALLMONEY has value."""
    table_name = "#pytest_money_mixed2"
    try:
        drop_table_if_exists(cursor, table_name)
        cursor.execute(f"CREATE TABLE {table_name} (m MONEY, sm SMALLMONEY)")
        db_connection.commit()

        cursor.execute(f"INSERT INTO {table_name} VALUES (?, ?)", (None, Decimal("67.8900")))
        db_connection.commit()

        cursor.execute(f"SELECT m, sm FROM {table_name}")
        row = cursor.fetchone()
        assert row[0] is None
        assert row[1] == Decimal("67.8900")
    finally:
        drop_table_if_exists(cursor, table_name)
        db_connection.commit()


def test_both_null(cursor, db_connection):
    """Test both MONEY and SMALLMONEY are NULL."""
    table_name = "#pytest_money_mixed3"
    try:
        drop_table_if_exists(cursor, table_name)
        cursor.execute(f"CREATE TABLE {table_name} (m MONEY, sm SMALLMONEY)")
        db_connection.commit()

        cursor.execute(f"INSERT INTO {table_name} VALUES (?, ?)", (None, None))
        db_connection.commit()

        cursor.execute(f"SELECT m, sm FROM {table_name}")
        row = cursor.fetchone()
        assert row[0] is None
        assert row[1] is None
    finally:
        drop_table_if_exists(cursor, table_name)
        db_connection.commit()


# =============================================================================
# GH-740: in-range Decimal must bind as SQL_NUMERIC, not VARCHAR
# =============================================================================


def test_gh740_in_range_decimal_numeric_comparison_no_overflow(cursor, db_connection):
    """A money-range Decimal compared against a smaller numeric column must not raise.

    Before the fix the value was bound as VARCHAR, so SQL Server did a
    varchar->numeric conversion that overflowed instead of simply not matching.
    """
    table_name = "#pytest_gh740_cmp"
    try:
        drop_table_if_exists(cursor, table_name)
        cursor.execute(f"CREATE TABLE {table_name} (v numeric(5,2))")  # max 999.99
        cursor.execute(f"INSERT INTO {table_name} VALUES (?)", [Decimal("12.34")])
        db_connection.commit()

        # Both probes sit inside the MONEY range but exceed numeric(5,2); they must
        # return no rows rather than overflow.
        cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE v = ?", [Decimal("12345.6789")])
        assert cursor.fetchone()[0] == 0
        cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE v = ?", [Decimal("300000.00")])
        assert cursor.fetchone()[0] == 0
        # The matching value still matches.
        cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE v = ?", [Decimal("12.34")])
        assert cursor.fetchone()[0] == 1
    finally:
        drop_table_if_exists(cursor, table_name)
        db_connection.commit()


def test_gh740_numeric_param_not_in_first_position(cursor, db_connection):
    """A numeric param at position 2+ must not corrupt the parameter bound before it.

    Guards the descriptor-record fix: the APD record number was hardcoded to 1, so a
    numeric param at any later position wrote its type/precision/scale/data-ptr onto
    record 1, clobbering the FIRST parameter's binding as collateral. Putting a non-null
    value first pins that collateral corruption - the first value must round-trip intact,
    not just the numeric's own value. A NULL first would mask it (record 1 held no data).
    """
    table_name = "#pytest_gh740_pos"
    try:
        drop_table_if_exists(cursor, table_name)
        cursor.execute(f"CREATE TABLE {table_name} (a int, b varchar(10), c numeric(6,4))")
        cursor.execute(
            f"INSERT INTO {table_name} VALUES (?, ?, ?)",
            [12345, "keep", Decimal("67.8900")],
        )
        db_connection.commit()

        cursor.execute(f"SELECT a, b, c FROM {table_name}")
        row = cursor.fetchone()
        assert row[0] == 12345  # first param intact despite the later numeric
        assert row[1] == "keep"
        assert row[2] == Decimal("67.8900")
    finally:
        drop_table_if_exists(cursor, table_name)
        db_connection.commit()


def test_gh740_multiple_numerics_with_null_between(cursor, db_connection):
    """Multiple SQL_NUMERIC params with differing scales and a NULL between them."""
    table_name = "#pytest_gh740_multi"
    try:
        drop_table_if_exists(cursor, table_name)
        cursor.execute(f"CREATE TABLE {table_name} (a numeric(10,4), b int, c numeric(8,2))")
        cursor.execute(
            f"INSERT INTO {table_name} VALUES (?, ?, ?)",
            [Decimal("1.2300"), None, Decimal("999999.99")],
        )
        db_connection.commit()

        cursor.execute(f"SELECT a, b, c FROM {table_name}")
        row = cursor.fetchone()
        assert row[0] == Decimal("1.2300")
        assert row[1] is None
        assert row[2] == Decimal("999999.99")
    finally:
        drop_table_if_exists(cursor, table_name)
        db_connection.commit()


def test_gh740_money_boundary_still_round_trips(cursor, db_connection):
    """MONEY/SMALLMONEY boundary values still insert exactly after the binding change."""
    for coltype, value in [
        ("MONEY", Decimal("922337203685477.5807")),
        ("MONEY", Decimal("-922337203685477.5808")),
        ("SMALLMONEY", Decimal("214748.3647")),
        ("SMALLMONEY", Decimal("-214748.3648")),
    ]:
        table_name = "#pytest_gh740_bound"
        try:
            drop_table_if_exists(cursor, table_name)
            cursor.execute(f"CREATE TABLE {table_name} (v {coltype})")
            cursor.execute(f"INSERT INTO {table_name} VALUES (?)", [value])
            db_connection.commit()
            cursor.execute(f"SELECT v FROM {table_name}")
            assert cursor.fetchone()[0] == value
        finally:
            drop_table_if_exists(cursor, table_name)
            db_connection.commit()


def test_gh740_same_statement_changing_precision(cursor, db_connection):
    """Re-executing the same statement with Decimals of different precision/scale works."""
    table_name = "#pytest_gh740_reexec"
    try:
        drop_table_if_exists(cursor, table_name)
        cursor.execute(f"CREATE TABLE {table_name} (v numeric(20,6))")
        for value in [Decimal("1.5"), Decimal("123456.789012"), Decimal("0.000001")]:
            cursor.execute(f"INSERT INTO {table_name} VALUES (?)", [value])
        db_connection.commit()

        cursor.execute(f"SELECT v FROM {table_name} ORDER BY v")
        rows = [r[0] for r in cursor.fetchall()]
        assert rows == [Decimal("0.000001"), Decimal("1.500000"), Decimal("123456.789012")]
    finally:
        drop_table_if_exists(cursor, table_name)
        db_connection.commit()
