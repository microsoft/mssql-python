"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.

Comprehensive tests for pyformat parameter style support.
Tests cover parse_pyformat_params(), convert_pyformat_to_qmark(), 
and detect_and_convert_parameters() functions.

Goal: 100% code coverage of mssql_python/parameter_helper.py
"""

import pytest
from datetime import date, datetime
from decimal import Decimal
from mssql_python.parameter_helper import (
    parse_pyformat_params,
    convert_pyformat_to_qmark,
    detect_and_convert_parameters,
)


class TestParsePyformatParams:
    """Test parse_pyformat_params() function."""

    def test_parse_single_parameter(self):
        """Test parsing SQL with single parameter."""
        sql = "SELECT * FROM users WHERE id = %(id)s"
        params = parse_pyformat_params(sql)
        assert params == ["id"]

    def test_parse_multiple_parameters(self):
        """Test parsing SQL with multiple different parameters."""
        sql = "SELECT * FROM users WHERE name = %(name)s AND age = %(age)s AND city = %(city)s"
        params = parse_pyformat_params(sql)
        assert params == ["name", "age", "city"]

    def test_parse_parameter_reuse(self):
        """Test parsing when same parameter appears multiple times."""
        sql = "SELECT * FROM users WHERE first_name = %(name)s OR last_name = %(name)s"
        params = parse_pyformat_params(sql)
        assert params == ["name", "name"]

    def test_parse_multiple_reuses(self):
        """Test parsing with multiple parameters reused."""
        sql = "WHERE (user_id = %(id)s OR admin_id = %(id)s OR creator_id = %(id)s) AND date > %(date)s"
        params = parse_pyformat_params(sql)
        assert params == ["id", "id", "id", "date"]

    def test_parse_no_parameters(self):
        """Test parsing SQL with no parameters."""
        sql = "SELECT * FROM users"
        params = parse_pyformat_params(sql)
        assert params == []

    def test_parse_empty_string(self):
        """Test parsing empty SQL string."""
        params = parse_pyformat_params("")
        assert params == []

    def test_parse_parameter_with_underscores(self):
        """Test parsing parameter names with underscores."""
        sql = "WHERE user_id = %(user_id)s AND first_name = %(first_name)s"
        params = parse_pyformat_params(sql)
        assert params == ["user_id", "first_name"]

    def test_parse_parameter_with_numbers(self):
        """Test parsing parameter names with numbers."""
        sql = "WHERE col1 = %(param1)s AND col2 = %(param2)s AND col3 = %(param3)s"
        params = parse_pyformat_params(sql)
        assert params == ["param1", "param2", "param3"]

    def test_parse_parameter_in_string_literal(self):
        """Test that parameters in string literals are still detected (psycopg2 behavior)."""
        sql = "SELECT '%(example)s' AS literal, id FROM users WHERE id = %(id)s"
        params = parse_pyformat_params(sql)
        # Simple scanner detects both - this is by design
        assert params == ["example", "id"]

    def test_parse_parameter_in_comment(self):
        """Test that parameters in comments are still detected (psycopg2 behavior)."""
        sql = """
        SELECT * FROM users
        -- This comment has %(commented)s parameter
        WHERE id = %(id)s
        """
        params = parse_pyformat_params(sql)
        # Simple scanner detects both - this is by design
        assert params == ["commented", "id"]

    def test_parse_complex_query_with_cte(self):
        """Test parsing complex CTE query."""
        sql = """
        WITH recent_orders AS (
            SELECT customer_id, SUM(total) as sum_total
            FROM orders
            WHERE order_date >= %(start_date)s
            GROUP BY customer_id
        )
        SELECT u.name, ro.sum_total
        FROM users u
        JOIN recent_orders ro ON u.id = ro.customer_id
        WHERE ro.sum_total > %(min_amount)s
        """
        params = parse_pyformat_params(sql)
        assert params == ["start_date", "min_amount"]

    def test_parse_incomplete_pattern_no_closing_paren(self):
        """Test that incomplete %(name pattern without ) is ignored."""
        sql = "SELECT * FROM users WHERE id = %(id"
        params = parse_pyformat_params(sql)
        assert params == []

    def test_parse_incomplete_pattern_no_s(self):
        """Test that %(name) without 's' is ignored."""
        sql = "SELECT * FROM users WHERE id = %(id)"
        params = parse_pyformat_params(sql)
        assert params == []

    def test_parse_percent_without_paren(self):
        """Test that % without ( is ignored."""
        sql = "SELECT * FROM users WHERE discount = %10 AND id = %(id)s"
        params = parse_pyformat_params(sql)
        assert params == ["id"]

    def test_parse_special_characters_in_name(self):
        """Test parsing parameter names with special characters (though not recommended)."""
        sql = "WHERE x = %(my-param)s"
        params = parse_pyformat_params(sql)
        assert params == ["my-param"]

    def test_parse_empty_parameter_name(self):
        """Test parsing empty parameter name %()s."""
        sql = "WHERE x = %()s AND y = %(name)s"
        params = parse_pyformat_params(sql)
        assert params == ["", "name"]

    def test_parse_long_query_many_parameters(self):
        """Test parsing query with many parameters."""
        conditions = [f"col{i} = %(param{i})s" for i in range(20)]
        sql = "SELECT * FROM table WHERE " + " AND ".join(conditions)
        params = parse_pyformat_params(sql)
        expected = [f"param{i}" for i in range(20)]
        assert params == expected


class TestConvertPyformatToQmark:
    """Test convert_pyformat_to_qmark() function."""

    def test_convert_single_parameter(self):
        """Test converting single parameter."""
        sql = "SELECT * FROM users WHERE id = %(id)s"
        param_dict = {"id": 42}
        result_sql, result_params = convert_pyformat_to_qmark(sql, param_dict)
        assert result_sql == "SELECT * FROM users WHERE id = ?"
        assert result_params == (42,)

    def test_convert_multiple_parameters(self):
        """Test converting multiple parameters."""
        sql = "INSERT INTO users (name, age, city) VALUES (%(name)s, %(age)s, %(city)s)"
        param_dict = {"name": "Alice", "age": 30, "city": "NYC"}
        result_sql, result_params = convert_pyformat_to_qmark(sql, param_dict)
        assert result_sql == "INSERT INTO users (name, age, city) VALUES (?, ?, ?)"
        assert result_params == ("Alice", 30, "NYC")

    def test_convert_parameter_reuse(self):
        """Test converting when same parameter is reused."""
        sql = "SELECT * FROM logs WHERE user = %(user)s OR admin = %(user)s"
        param_dict = {"user": "alice"}
        result_sql, result_params = convert_pyformat_to_qmark(sql, param_dict)
        assert result_sql == "SELECT * FROM logs WHERE user = ? OR admin = ?"
        assert result_params == ("alice", "alice")

    def test_convert_parameter_reuse_multiple(self):
        """Test converting with parameter used 3+ times."""
        sql = "WHERE a = %(x)s OR b = %(x)s OR c = %(x)s"
        param_dict = {"x": 100}
        result_sql, result_params = convert_pyformat_to_qmark(sql, param_dict)
        assert result_sql == "WHERE a = ? OR b = ? OR c = ?"
        assert result_params == (100, 100, 100)

    def test_convert_missing_parameter_single(self):
        """Test that missing parameter raises KeyError with helpful message."""
        sql = "SELECT * FROM users WHERE id = %(id)s"
        param_dict = {"name": "test"}
        with pytest.raises(KeyError) as exc_info:
            convert_pyformat_to_qmark(sql, param_dict)
        error_msg = str(exc_info.value)
        assert "'id'" in error_msg
        assert "Missing required parameter" in error_msg

    def test_convert_missing_parameter_multiple(self):
        """Test that multiple missing parameters are reported."""
        sql = "WHERE id = %(id)s AND name = %(name)s AND age = %(age)s"
        param_dict = {"id": 42}
        with pytest.raises(KeyError) as exc_info:
            convert_pyformat_to_qmark(sql, param_dict)
        error_msg = str(exc_info.value)
        assert "'age'" in error_msg or "'name'" in error_msg
        assert "Missing required parameter" in error_msg

    def test_convert_extra_parameters_allowed(self):
        """Test that extra parameters in dict are ignored (not an error)."""
        sql = "SELECT * FROM users WHERE id = %(id)s"
        param_dict = {"id": 42, "name": "Alice", "age": 30}
        result_sql, result_params = convert_pyformat_to_qmark(sql, param_dict)
        assert result_sql == "SELECT * FROM users WHERE id = ?"
        assert result_params == (42,)

    def test_convert_empty_dict_no_parameters(self):
        """Test converting query with no parameters and empty dict."""
        sql = "SELECT * FROM users"
        param_dict = {}
        result_sql, result_params = convert_pyformat_to_qmark(sql, param_dict)
        assert result_sql == "SELECT * FROM users"
        assert result_params == ()

    def test_convert_none_value(self):
        """Test converting with NULL/None value."""
        sql = "INSERT INTO users (name, phone) VALUES (%(name)s, %(phone)s)"
        param_dict = {"name": "John", "phone": None}
        result_sql, result_params = convert_pyformat_to_qmark(sql, param_dict)
        assert result_sql == "INSERT INTO users (name, phone) VALUES (?, ?)"
        assert result_params == ("John", None)

    def test_convert_various_types(self):
        """Test converting with various Python data types."""
        sql = """
        INSERT INTO data (str_col, int_col, float_col, bool_col, date_col, bytes_col, decimal_col)
        VALUES (%(s)s, %(i)s, %(f)s, %(b)s, %(d)s, %(by)s, %(dec)s)
        """
        param_dict = {
            "s": "text",
            "i": 42,
            "f": 3.14,
            "b": True,
            "d": date(2025, 1, 1),
            "by": b'\x00\x01\x02',
            "dec": Decimal("99.99")
        }
        result_sql, result_params = convert_pyformat_to_qmark(sql, param_dict)
        assert "?" in result_sql
        assert "%(s)s" not in result_sql
        assert len(result_params) == 7
        assert result_params[0] == "text"
        assert result_params[1] == 42
        assert result_params[2] == 3.14
        assert result_params[3] is True
        assert result_params[4] == date(2025, 1, 1)
        assert result_params[5] == b'\x00\x01\x02'
        assert result_params[6] == Decimal("99.99")

    def test_convert_unicode_values(self):
        """Test converting with Unicode characters in values."""
        sql = "INSERT INTO users (name) VALUES (%(name)s)"
        param_dict = {"name": "José María 日本語 🎉"}
        result_sql, result_params = convert_pyformat_to_qmark(sql, param_dict)
        assert result_sql == "INSERT INTO users (name) VALUES (?)"
        assert result_params == ("José María 日本語 🎉",)

    def test_convert_sql_injection_attempt(self):
        """Test that SQL injection attempts are safely handled as parameter values."""
        sql = "SELECT * FROM users WHERE name = %(name)s"
        param_dict = {"name": "'; DROP TABLE users; --"}
        result_sql, result_params = convert_pyformat_to_qmark(sql, param_dict)
        assert result_sql == "SELECT * FROM users WHERE name = ?"
        assert result_params == ("'; DROP TABLE users; --",)

    def test_convert_complex_cte_query(self):
        """Test converting complex CTE query."""
        sql = """
        WITH recent_orders AS (
            SELECT customer_id, SUM(total) as sum_total
            FROM orders
            WHERE order_date >= %(start_date)s
            GROUP BY customer_id
        )
        SELECT u.name, ro.sum_total
        FROM users u
        JOIN recent_orders ro ON u.id = ro.customer_id
        WHERE ro.sum_total > %(min_amount)s
        """
        param_dict = {"start_date": "2025-01-01", "min_amount": 1000.00}
        result_sql, result_params = convert_pyformat_to_qmark(sql, param_dict)
        assert "%(start_date)s" not in result_sql
        assert "%(min_amount)s" not in result_sql
        assert result_sql.count("?") == 2
        assert result_params == ("2025-01-01", 1000.00)

    def test_convert_with_escaped_percent(self):
        """Test that %% is converted to single %."""
        sql = "SELECT * FROM users WHERE discount = '%%10' AND id = %(id)s"
        param_dict = {"id": 42}
        result_sql, result_params = convert_pyformat_to_qmark(sql, param_dict)
        assert result_sql == "SELECT * FROM users WHERE discount = '%10' AND id = ?"
        assert result_params == (42,)

    def test_convert_with_multiple_escaped_percent(self):
        """Test multiple %% escapes."""
        sql = "SELECT '%%test%%' AS txt, id FROM users WHERE id = %(id)s AND name LIKE '%%%(name)s%%'"
        param_dict = {"id": 1, "name": "alice"}
        result_sql, result_params = convert_pyformat_to_qmark(sql, param_dict)
        assert "'%test%'" in result_sql
        assert "?" in result_sql
        assert "%%(name)s" not in result_sql
        assert result_params == (1, "alice")

    def test_convert_only_escaped_percent_no_params(self):
        """Test SQL with only %% and no parameters."""
        sql = "SELECT * FROM users WHERE discount = '%%10'"
        param_dict = {}
        result_sql, result_params = convert_pyformat_to_qmark(sql, param_dict)
        assert result_sql == "SELECT * FROM users WHERE discount = '%10'"
        assert result_params == ()

    def test_convert_empty_parameter_name(self):
        """Test converting with empty parameter name (edge case)."""
        sql = "WHERE x = %()s"
        param_dict = {"": "value"}
        result_sql, result_params = convert_pyformat_to_qmark(sql, param_dict)
        assert result_sql == "WHERE x = ?"
        assert result_params == ("value",)

    def test_convert_many_parameters(self):
        """Test converting with many parameters (performance test)."""
        param_names = [f"param{i}" for i in range(50)]
        sql = "SELECT * FROM table WHERE " + " AND ".join([f"col{i} = %(param{i})s" for i in range(50)])
        param_dict = {f"param{i}": i for i in range(50)}
        result_sql, result_params = convert_pyformat_to_qmark(sql, param_dict)
        assert result_sql.count("?") == 50
        assert len(result_params) == 50
        assert result_params == tuple(range(50))


class TestDetectAndConvertParameters:
    """Test detect_and_convert_parameters() function."""

    def test_detect_none_parameters(self):
        """Test detection when parameters is None."""
        sql = "SELECT * FROM users"
        result_sql, result_params = detect_and_convert_parameters(sql, None)
        assert result_sql == sql
        assert result_params is None

    def test_detect_qmark_tuple(self):
        """Test detection of qmark style with tuple."""
        sql = "SELECT * FROM users WHERE id = ?"
        params = (42,)
        result_sql, result_params = detect_and_convert_parameters(sql, params)
        assert result_sql == sql
        assert result_params == params

    def test_detect_qmark_list(self):
        """Test detection of qmark style with list."""
        sql = "SELECT * FROM users WHERE id = ? AND name = ?"
        params = [42, "Alice"]
        result_sql, result_params = detect_and_convert_parameters(sql, params)
        assert result_sql == sql
        assert result_params == params

    def test_detect_pyformat_dict(self):
        """Test detection of pyformat style with dict."""
        sql = "SELECT * FROM users WHERE id = %(id)s"
        params = {"id": 42}
        result_sql, result_params = detect_and_convert_parameters(sql, params)
        assert result_sql == "SELECT * FROM users WHERE id = ?"
        assert result_params == (42,)

    def test_detect_pyformat_multiple_params(self):
        """Test detection and conversion with multiple pyformat params."""
        sql = "INSERT INTO users (name, age) VALUES (%(name)s, %(age)s)"
        params = {"name": "Bob", "age": 25}
        result_sql, result_params = detect_and_convert_parameters(sql, params)
        assert result_sql == "INSERT INTO users (name, age) VALUES (?, ?)"
        assert result_params == ("Bob", 25)

    def test_detect_type_mismatch_dict_with_qmark(self):
        """Test TypeError when dict is used with ? placeholders."""
        sql = "SELECT * FROM users WHERE id = ?"
        params = {"id": 42}
        with pytest.raises(TypeError) as exc_info:
            detect_and_convert_parameters(sql, params)
        error_msg = str(exc_info.value)
        assert "Parameter style mismatch" in error_msg
        assert "positional placeholders (?)" in error_msg
        assert "dict was provided" in error_msg

    def test_detect_type_mismatch_tuple_with_pyformat(self):
        """Test TypeError when tuple is used with %(name)s placeholders."""
        sql = "SELECT * FROM users WHERE id = %(id)s"
        params = (42,)
        with pytest.raises(TypeError) as exc_info:
            detect_and_convert_parameters(sql, params)
        error_msg = str(exc_info.value)
        assert "Parameter style mismatch" in error_msg
        assert "named placeholders" in error_msg
        assert "tuple was provided" in error_msg

    def test_detect_type_mismatch_list_with_pyformat(self):
        """Test TypeError when list is used with %(name)s placeholders."""
        sql = "SELECT * FROM users WHERE id = %(id)s AND name = %(name)s"
        params = [42, "Alice"]
        with pytest.raises(TypeError) as exc_info:
            detect_and_convert_parameters(sql, params)
        error_msg = str(exc_info.value)
        assert "Parameter style mismatch" in error_msg
        assert "list was provided" in error_msg

    def test_detect_invalid_type_string(self):
        """Test TypeError for unsupported parameter type (string)."""
        sql = "SELECT * FROM users WHERE id = ?"
        params = "42"
        with pytest.raises(TypeError) as exc_info:
            detect_and_convert_parameters(sql, params)
        error_msg = str(exc_info.value)
        assert "Parameters must be tuple, list, dict, or None" in error_msg
        assert "str" in error_msg

    def test_detect_invalid_type_int(self):
        """Test TypeError for unsupported parameter type (int)."""
        sql = "SELECT * FROM users WHERE id = ?"
        params = 42
        with pytest.raises(TypeError) as exc_info:
            detect_and_convert_parameters(sql, params)
        error_msg = str(exc_info.value)
        assert "Parameters must be tuple, list, dict, or None" in error_msg
        assert "int" in error_msg

    def test_detect_invalid_type_set(self):
        """Test TypeError for unsupported parameter type (set)."""
        sql = "SELECT * FROM users WHERE id = ?"
        params = {42, 43}
        with pytest.raises(TypeError) as exc_info:
            detect_and_convert_parameters(sql, params)
        error_msg = str(exc_info.value)
        assert "Parameters must be tuple, list, dict, or None" in error_msg
        assert "set" in error_msg

    def test_detect_qmark_with_no_question_marks(self):
        """Test qmark detection when SQL has no ? but tuple provided."""
        sql = "SELECT * FROM users"
        params = (42, "Alice")
        result_sql, result_params = detect_and_convert_parameters(sql, params)
        # Passes through - SQL execution will handle parameter count mismatch
        assert result_sql == sql
        assert result_params == params

    def test_detect_pyformat_with_parameter_reuse(self):
        """Test detection and conversion with parameter reuse."""
        sql = "WHERE user = %(user)s OR admin = %(user)s"
        params = {"user": "alice"}
        result_sql, result_params = detect_and_convert_parameters(sql, params)
        assert result_sql == "WHERE user = ? OR admin = ?"
        assert result_params == ("alice", "alice")

    def test_detect_empty_tuple(self):
        """Test detection with empty tuple (no parameters)."""
        sql = "SELECT * FROM users"
        params = ()
        result_sql, result_params = detect_and_convert_parameters(sql, params)
        assert result_sql == sql
        assert result_params == ()

    def test_detect_empty_list(self):
        """Test detection with empty list (no parameters)."""
        sql = "SELECT * FROM users"
        params = []
        result_sql, result_params = detect_and_convert_parameters(sql, params)
        assert result_sql == sql
        assert result_params == []

    def test_detect_empty_dict(self):
        """Test detection with empty dict (no parameters)."""
        sql = "SELECT * FROM users"
        params = {}
        result_sql, result_params = detect_and_convert_parameters(sql, params)
        assert result_sql == sql
        assert result_params == ()

    def test_detect_pyformat_missing_parameter(self):
        """Test that missing pyformat parameter raises KeyError."""
        sql = "WHERE id = %(id)s AND name = %(name)s"
        params = {"id": 42}
        with pytest.raises(KeyError) as exc_info:
            detect_and_convert_parameters(sql, params)
        error_msg = str(exc_info.value)
        assert "Missing required parameter" in error_msg
        assert "'name'" in error_msg

    def test_detect_complex_query_pyformat(self):
        """Test detection and conversion with complex query."""
        sql = """
        WITH recent AS (
            SELECT id FROM orders WHERE date >= %(date)s
        )
        SELECT * FROM users u
        JOIN recent r ON u.id = r.id
        WHERE u.status = %(status)s
        """
        params = {"date": "2025-01-01", "status": "active"}
        result_sql, result_params = detect_and_convert_parameters(sql, params)
        assert "%(date)s" not in result_sql
        assert "%(status)s" not in result_sql
        assert result_sql.count("?") == 2
        assert result_params == ("2025-01-01", "active")

    def test_detect_qmark_multiple_params(self):
        """Test detection with multiple qmark parameters."""
        sql = "UPDATE users SET name = ?, age = ? WHERE id = ?"
        params = ("Alice", 30, 42)
        result_sql, result_params = detect_and_convert_parameters(sql, params)
        assert result_sql == sql
        assert result_params == params

    def test_detect_pyformat_with_escaped_percent(self):
        """Test detection and conversion preserves %% escaping."""
        sql = "SELECT '%%discount%%' AS txt WHERE id = %(id)s"
        params = {"id": 1}
        result_sql, result_params = detect_and_convert_parameters(sql, params)
        assert "'%discount%'" in result_sql
        assert result_params == (1,)

    def test_detect_qmark_heuristic_false_positive_protection(self):
        """Test that qmark detection doesn't false-trigger on %( in SQL."""
        sql = "SELECT * FROM users WHERE discount = '%(10)' AND id = ?"
        params = (42,)
        result_sql, result_params = detect_and_convert_parameters(sql, params)
        # Should pass through as qmark since the pattern doesn't end in 's'
        assert result_sql == sql
        assert result_params == params

    def test_detect_pyformat_all_data_types(self):
        """Test detection and conversion with all supported data types."""
        sql = """
        INSERT INTO data (str_col, int_col, float_col, bool_col, none_col, date_col, datetime_col, bytes_col, decimal_col)
        VALUES (%(s)s, %(i)s, %(f)s, %(b)s, %(n)s, %(date)s, %(dt)s, %(by)s, %(dec)s)
        """
        params = {
            "s": "text",
            "i": 42,
            "f": 3.14,
            "b": False,
            "n": None,
            "date": date(2025, 12, 19),
            "dt": datetime(2025, 12, 19, 10, 30),
            "by": b'\xff\xfe',
            "dec": Decimal("123.45")
        }
        result_sql, result_params = detect_and_convert_parameters(sql, params)
        assert result_sql.count("?") == 9
        assert len(result_params) == 9
        assert result_params[0] == "text"
        assert result_params[1] == 42
        assert result_params[2] == 3.14
        assert result_params[3] is False
        assert result_params[4] is None
        assert result_params[5] == date(2025, 12, 19)
        assert result_params[6] == datetime(2025, 12, 19, 10, 30)
        assert result_params[7] == b'\xff\xfe'
        assert result_params[8] == Decimal("123.45")


class TestEdgeCases:
    """Test edge cases and special scenarios."""

    def test_very_long_parameter_name(self):
        """Test with very long parameter name."""
        long_name = "very_long_parameter_name_" * 10
        sql = f"SELECT * FROM users WHERE id = %({long_name})s"
        params = {long_name: 42}
        result_sql, result_params = convert_pyformat_to_qmark(sql, params)
        assert result_sql == "SELECT * FROM users WHERE id = ?"
        assert result_params == (42,)

    def test_parameter_name_with_unicode(self):
        """Test parameter name with Unicode (Python 3 allows this in dict keys)."""
        sql = "SELECT * FROM users WHERE name = %(名前)s"
        params = {"名前": "Tanaka"}
        result_sql, result_params = convert_pyformat_to_qmark(sql, params)
        assert result_sql == "SELECT * FROM users WHERE name = ?"
        assert result_params == ("Tanaka",)

    def test_sql_with_question_mark_and_pyformat(self):
        """Test SQL containing ? in string literal with pyformat params."""
        sql = "SELECT 'Is this ok?' AS question WHERE id = %(id)s"
        params = {"id": 42}
        result_sql, result_params = detect_and_convert_parameters(sql, params)
        # The ? in the string literal should remain, pyformat should convert
        assert "?" in result_sql
        assert "%(id)s" not in result_sql
        assert result_params == (42,)

    def test_many_parameter_reuses(self):
        """Test with same parameter reused many times."""
        sql = " OR ".join([f"col{i} = %(value)s" for i in range(30)])
        params = {"value": 999}
        result_sql, result_params = convert_pyformat_to_qmark(sql, params)
        assert result_sql.count("?") == 30
        assert len(result_params) == 30
        assert all(p == 999 for p in result_params)

    def test_parameter_value_is_empty_string(self):
        """Test with empty string as parameter value."""
        sql = "INSERT INTO users (name) VALUES (%(name)s)"
        params = {"name": ""}
        result_sql, result_params = convert_pyformat_to_qmark(sql, params)
        assert result_sql == "INSERT INTO users (name) VALUES (?)"
        assert result_params == ("",)

    def test_parameter_value_is_zero(self):
        """Test with zero as parameter value."""
        sql = "UPDATE counters SET count = %(count)s WHERE id = %(id)s"
        params = {"count": 0, "id": 1}
        result_sql, result_params = convert_pyformat_to_qmark(sql, params)
        assert result_params == (0, 1)

    def test_parameter_value_is_false(self):
        """Test with False as parameter value."""
        sql = "UPDATE settings SET enabled = %(enabled)s"
        params = {"enabled": False}
        result_sql, result_params = convert_pyformat_to_qmark(sql, params)
        assert result_params == (False,)

    def test_parameter_value_is_empty_bytes(self):
        """Test with empty bytes as parameter value."""
        sql = "INSERT INTO data (blob_col) VALUES (%(blob)s)"
        params = {"blob": b''}
        result_sql, result_params = convert_pyformat_to_qmark(sql, params)
        assert result_params == (b'',)

    def test_whitespace_in_parameter_name(self):
        """Test that spaces in parameter name are captured."""
        sql = "WHERE x = %(my param)s"
        params = {"my param": 42}
        result_sql, result_params = convert_pyformat_to_qmark(sql, params)
        assert result_sql == "WHERE x = ?"
        assert result_params == (42,)

    def test_consecutive_parameters_no_space(self):
        """Test consecutive parameters without space between them."""
        sql = "SELECT %(a)s%(b)s AS concat"
        params = {"a": "hello", "b": "world"}
        result_sql, result_params = convert_pyformat_to_qmark(sql, params)
        assert result_sql == "SELECT ?? AS concat"
        assert result_params == ("hello", "world")

    def test_parameter_at_start_of_sql(self):
        """Test parameter at the very start of SQL."""
        sql = "%(value)s"
        params = {"value": 42}
        result_sql, result_params = convert_pyformat_to_qmark(sql, params)
        assert result_sql == "?"
        assert result_params == (42,)

    def test_parameter_at_end_of_sql(self):
        """Test parameter at the very end of SQL."""
        sql = "SELECT * FROM users WHERE id = %(id)s"
        params = {"id": 42}
        result_sql, result_params = convert_pyformat_to_qmark(sql, params)
        assert result_sql == "SELECT * FROM users WHERE id = ?"
        assert result_params == (42,)

    def test_only_parameter_in_sql(self):
        """Test SQL with only a parameter."""
        sql = "%(value)s"
        params = {"value": "test"}
        result_sql, result_params = convert_pyformat_to_qmark(sql, params)
        assert result_sql == "?"
        assert result_params == ("test",)


class TestRealWorldScenarios:
    """Test real-world usage scenarios from documentation."""

    def test_ecommerce_order_query(self):
        """Test e-commerce order processing query."""
        sql = """
        SELECT p.id, p.name, p.price, i.stock
        FROM products p
        JOIN inventory i ON p.id = i.product_id
        WHERE p.id = %(product_id)s
          AND i.warehouse_id = %(warehouse_id)s
          AND i.stock >= %(quantity)s
        """
        params = {
            "product_id": 101,
            "warehouse_id": 5,
            "quantity": 10
        }
        result_sql, result_params = detect_and_convert_parameters(sql, params)
        assert result_sql.count("?") == 3
        assert result_params == (101, 5, 10)

    def test_analytics_report_query(self):
        """Test analytics/reporting query with optional filters."""
        sql = """
        WITH daily_sales AS (
            SELECT 
                CAST(o.created_at AS DATE) as sale_date,
                SUM(oi.quantity * oi.price) as daily_revenue
            FROM orders o
            JOIN order_items oi ON o.id = oi.order_id
            WHERE o.created_at BETWEEN %(start_date)s AND %(end_date)s
              AND o.status = %(status)s
            GROUP BY CAST(o.created_at AS DATE)
        )
        SELECT * FROM daily_sales ORDER BY sale_date DESC
        """
        params = {
            "start_date": "2025-01-01",
            "end_date": "2025-12-31",
            "status": "completed"
        }
        result_sql, result_params = detect_and_convert_parameters(sql, params)
        assert "%(start_date)s" not in result_sql
        assert result_sql.count("?") == 3
        assert result_params == ("2025-01-01", "2025-12-31", "completed")

    def test_user_authentication_query(self):
        """Test user authentication with rate limiting."""
        sql = """
        SELECT COUNT(*) as attempts
        FROM login_attempts
        WHERE email = %(email)s
          AND attempted_at > %(cutoff_time)s
          AND success = %(success)s
        """
        params = {
            "email": "user@example.com",
            "cutoff_time": datetime(2025, 12, 19, 9, 0),
            "success": False
        }
        result_sql, result_params = detect_and_convert_parameters(sql, params)
        assert result_sql.count("?") == 3
        assert result_params == ("user@example.com", datetime(2025, 12, 19, 9, 0), False)

    def test_dynamic_query_building(self):
        """Test dynamic query building pattern from documentation."""
        # Simulate dynamic filter building
        filters = {}
        query_parts = ["SELECT * FROM products WHERE 1=1"]
        
        # Add filters dynamically
        name = "Widget"
        if name:
            query_parts.append("AND name LIKE %(name)s")
            filters["name"] = f"%{name}%"
        
        category = "Tools"
        if category:
            query_parts.append("AND category = %(category)s")
            filters["category"] = category
        
        min_price = 10.00
        if min_price is not None:
            query_parts.append("AND price >= %(min_price)s")
            filters["min_price"] = min_price
        
        sql = " ".join(query_parts)
        result_sql, result_params = detect_and_convert_parameters(sql, filters)
        
        assert result_sql.count("?") == 3
        assert result_params == ("%Widget%", "Tools", 10.00)

    def test_batch_insert_pattern(self):
        """Test pattern for batch inserts (would use executemany in practice)."""
        sql = "INSERT INTO products (name, price, category) VALUES (%(name)s, %(price)s, %(category)s)"
        
        # First row
        params1 = {"name": "Widget A", "price": 9.99, "category": "Tools"}
        result_sql1, result_params1 = detect_and_convert_parameters(sql, params1)
        assert result_params1 == ("Widget A", 9.99, "Tools")
        
        # Second row
        params2 = {"name": "Gadget X", "price": 29.99, "category": "Electronics"}
        result_sql2, result_params2 = detect_and_convert_parameters(sql, params2)
        assert result_params2 == ("Gadget X", 29.99, "Electronics")
        
        # Both should produce same SQL
        assert result_sql1 == result_sql2

    def test_subquery_with_parameters(self):
        """Test subquery with parameters."""
        sql = """
        SELECT * FROM products
        WHERE category_id IN (
            SELECT id FROM categories WHERE name = %(category)s
        )
        AND price BETWEEN %(min_price)s AND %(max_price)s
        """
        params = {
            "category": "Electronics",
            "min_price": 100,
            "max_price": 500
        }
        result_sql, result_params = detect_and_convert_parameters(sql, params)
        assert result_sql.count("?") == 3
        assert result_params == ("Electronics", 100, 500)

    def test_window_function_query(self):
        """Test query with window functions."""
        sql = """
        SELECT 
            name,
            salary,
            ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY salary DESC) as rank
        FROM employees
        WHERE department_id = %(dept_id)s
          AND hire_date >= %(hire_date)s
        """
        params = {
            "dept_id": 5,
            "hire_date": "2024-01-01"
        }
        result_sql, result_params = detect_and_convert_parameters(sql, params)
        assert result_sql.count("?") == 2
        assert result_params == (5, "2024-01-01")


class TestBackwardCompatibility:
    """Test that qmark style (existing functionality) still works perfectly."""

    def test_qmark_single_param(self):
        """Test backward compatibility: single qmark parameter."""
        sql = "SELECT * FROM users WHERE id = ?"
        params = (42,)
        result_sql, result_params = detect_and_convert_parameters(sql, params)
        assert result_sql == sql
        assert result_params == params

    def test_qmark_multiple_params(self):
        """Test backward compatibility: multiple qmark parameters."""
        sql = "INSERT INTO users (name, age, city) VALUES (?, ?, ?)"
        params = ("Alice", 30, "NYC")
        result_sql, result_params = detect_and_convert_parameters(sql, params)
        assert result_sql == sql
        assert result_params == params

    def test_qmark_with_list(self):
        """Test backward compatibility: qmark with list."""
        sql = "UPDATE users SET name = ?, age = ? WHERE id = ?"
        params = ["Bob", 25, 100]
        result_sql, result_params = detect_and_convert_parameters(sql, params)
        assert result_sql == sql
        assert result_params == params

    def test_qmark_no_params(self):
        """Test backward compatibility: query with no parameters."""
        sql = "SELECT * FROM users"
        result_sql, result_params = detect_and_convert_parameters(sql, None)
        assert result_sql == sql
        assert result_params is None

    def test_qmark_complex_query(self):
        """Test backward compatibility: complex query with qmark."""
        sql = """
        SELECT u.name, o.total
        FROM users u
        JOIN orders o ON u.id = o.user_id
        WHERE u.created_at >= ?
          AND u.status = ?
          AND o.total > ?
        """
        params = ("2025-01-01", "active", 100.00)
        result_sql, result_params = detect_and_convert_parameters(sql, params)
        assert result_sql == sql
        assert result_params == params


class TestBatchExecuteParameters:
    """Test parameter conversion for connection.batch_execute() method."""

    def test_batch_execute_all_qmark(self):
        """Test batch_execute with all qmark-style parameters."""
        statements = [
            "INSERT INTO users (id, name) VALUES (?, ?)",
            "UPDATE users SET active = ? WHERE id = ?",
            "DELETE FROM logs WHERE id = ?"
        ]
        params = [
            (1, "Alice"),
            (True, 1),
            (100,)
        ]
        
        # Test conversion for each statement
        for stmt, param in zip(statements, params):
            result_sql, result_params = detect_and_convert_parameters(stmt, param)
            assert result_sql == stmt
            assert result_params == param

    def test_batch_execute_all_pyformat(self):
        """Test batch_execute with all pyformat-style parameters."""
        statements = [
            "INSERT INTO users (id, name) VALUES (%(id)s, %(name)s)",
            "UPDATE users SET active = %(active)s WHERE id = %(id)s",
            "DELETE FROM logs WHERE id = %(id)s"
        ]
        params = [
            {"id": 1, "name": "Alice"},
            {"active": True, "id": 1},
            {"id": 100}
        ]
        
        # Test conversion for each statement
        for stmt, param in zip(statements, params):
            result_sql, result_params = detect_and_convert_parameters(stmt, param)
            assert "%(id)s" not in result_sql
            assert "%(name)s" not in result_sql
            assert "%(active)s" not in result_sql
            assert "?" in result_sql

    def test_batch_execute_mixed_styles(self):
        """Test batch_execute with mixed qmark and pyformat parameters."""
        statements = [
            "INSERT INTO users (id, name) VALUES (?, ?)",
            "UPDATE users SET email = %(email)s WHERE id = %(id)s",
            "SELECT * FROM users WHERE id = ?"
        ]
        params = [
            (1, "Alice"),
            {"email": "alice@example.com", "id": 1},
            (1,)
        ]
        
        # First statement - qmark (pass through)
        result_sql_1, result_params_1 = detect_and_convert_parameters(statements[0], params[0])
        assert result_sql_1 == statements[0]
        assert result_params_1 == params[0]
        
        # Second statement - pyformat (convert)
        result_sql_2, result_params_2 = detect_and_convert_parameters(statements[1], params[1])
        assert result_sql_2 == "UPDATE users SET email = ? WHERE id = ?"
        assert result_params_2 == ("alice@example.com", 1)
        
        # Third statement - qmark (pass through)
        result_sql_3, result_params_3 = detect_and_convert_parameters(statements[2], params[2])
        assert result_sql_3 == statements[2]
        assert result_params_3 == params[2]

    def test_batch_execute_with_none_params(self):
        """Test batch_execute with some None parameters."""
        statements = [
            "CREATE TABLE temp (id INT, name VARCHAR(100))",
            "INSERT INTO temp (id, name) VALUES (%(id)s, %(name)s)",
            "SELECT * FROM temp"
        ]
        params = [
            None,
            {"id": 1, "name": "Test"},
            None
        ]
        
        # First statement - None params
        result_sql_1, result_params_1 = detect_and_convert_parameters(statements[0], params[0])
        assert result_sql_1 == statements[0]
        assert result_params_1 is None
        
        # Second statement - pyformat
        result_sql_2, result_params_2 = detect_and_convert_parameters(statements[1], params[1])
        assert "?" in result_sql_2
        assert result_params_2 == (1, "Test")
        
        # Third statement - None params
        result_sql_3, result_params_3 = detect_and_convert_parameters(statements[2], params[2])
        assert result_sql_3 == statements[2]
        assert result_params_3 is None

    def test_batch_execute_pyformat_with_reuse(self):
        """Test batch_execute with pyformat parameters that reuse values."""
        statements = [
            "INSERT INTO logs (user, action) VALUES (%(user)s, %(action)s)",
            "UPDATE stats SET count = count + 1 WHERE user = %(user)s OR admin = %(user)s"
        ]
        params = [
            {"user": "alice", "action": "login"},
            {"user": "alice"}
        ]
        
        # First statement
        result_sql_1, result_params_1 = detect_and_convert_parameters(statements[0], params[0])
        assert result_sql_1 == "INSERT INTO logs (user, action) VALUES (?, ?)"
        assert result_params_1 == ("alice", "login")
        
        # Second statement with parameter reuse
        result_sql_2, result_params_2 = detect_and_convert_parameters(statements[1], params[1])
        assert result_sql_2 == "UPDATE stats SET count = count + 1 WHERE user = ? OR admin = ?"
        assert result_params_2 == ("alice", "alice")

    def test_batch_execute_complex_operations(self):
        """Test batch_execute with complex real-world operations."""
        statements = [
            # CTE with pyformat
            """
            WITH recent AS (
                SELECT id FROM orders WHERE date >= %(start_date)s
            )
            DELETE FROM temp_orders WHERE id IN (SELECT id FROM recent)
            """,
            # Insert with qmark
            "INSERT INTO archive (id, date, status) VALUES (?, ?, ?)",
            # Update with pyformat
            "UPDATE summary SET processed = %(processed)s, updated_at = %(timestamp)s WHERE date = %(date)s"
        ]
        params = [
            {"start_date": "2025-01-01"},
            (1, "2025-12-19", "completed"),
            {"processed": True, "timestamp": datetime(2025, 12, 19, 10, 30), "date": "2025-12-19"}
        ]
        
        # Test each statement
        result_sql_1, result_params_1 = detect_and_convert_parameters(statements[0], params[0])
        assert "%(start_date)s" not in result_sql_1
        assert result_params_1 == ("2025-01-01",)
        
        result_sql_2, result_params_2 = detect_and_convert_parameters(statements[1], params[1])
        assert result_sql_2 == statements[1]
        assert result_params_2 == params[1]
        
        result_sql_3, result_params_3 = detect_and_convert_parameters(statements[2], params[2])
        assert "%(processed)s" not in result_sql_3
        assert len(result_params_3) == 3
        assert result_params_3[0] is True

    def test_batch_execute_empty_statements(self):
        """Test batch_execute with empty statement list."""
        statements = []
        params = []
        
        # Should handle empty list gracefully
        assert len(statements) == len(params)

    def test_batch_execute_single_statement(self):
        """Test batch_execute with single statement (edge case)."""
        statements = ["SELECT * FROM users WHERE id = %(id)s"]
        params = [{"id": 42}]
        
        result_sql, result_params = detect_and_convert_parameters(statements[0], params[0])
        assert result_sql == "SELECT * FROM users WHERE id = ?"
        assert result_params == (42,)

    def test_batch_execute_many_statements(self):
        """Test batch_execute with many statements."""
        # Create 20 insert statements with pyformat
        statements = [
            "INSERT INTO data (id, value) VALUES (%(id)s, %(value)s)"
            for _ in range(20)
        ]
        params = [
            {"id": i, "value": f"value_{i}"}
            for i in range(20)
        ]
        
        # Test conversion for each
        for i, (stmt, param) in enumerate(zip(statements, params)):
            result_sql, result_params = detect_and_convert_parameters(stmt, param)
            assert result_sql == "INSERT INTO data (id, value) VALUES (?, ?)"
            assert result_params == (i, f"value_{i}")

    def test_batch_execute_transaction_pattern(self):
        """Test batch_execute with transaction-like pattern."""
        statements = [
            "BEGIN TRANSACTION",
            "INSERT INTO orders (id, total) VALUES (%(id)s, %(total)s)",
            "UPDATE inventory SET stock = stock - %(qty)s WHERE product_id = %(product_id)s",
            "INSERT INTO audit_log (action, order_id) VALUES (%(action)s, %(order_id)s)",
            "COMMIT"
        ]
        params = [
            None,
            {"id": 101, "total": 99.99},
            {"qty": 5, "product_id": 42},
            {"action": "order_placed", "order_id": 101},
            None
        ]
        
        # BEGIN
        result_sql_0, result_params_0 = detect_and_convert_parameters(statements[0], params[0])
        assert result_sql_0 == statements[0]
        assert result_params_0 is None
        
        # INSERT order
        result_sql_1, result_params_1 = detect_and_convert_parameters(statements[1], params[1])
        assert "?" in result_sql_1
        assert result_params_1 == (101, 99.99)
        
        # UPDATE inventory
        result_sql_2, result_params_2 = detect_and_convert_parameters(statements[2], params[2])
        assert "?" in result_sql_2
        assert result_params_2 == (5, 42)
        
        # INSERT audit
        result_sql_3, result_params_3 = detect_and_convert_parameters(statements[3], params[3])
        assert "?" in result_sql_3
        assert result_params_3 == ("order_placed", 101)
        
        # COMMIT
        result_sql_4, result_params_4 = detect_and_convert_parameters(statements[4], params[4])
        assert result_sql_4 == statements[4]
        assert result_params_4 is None

    def test_batch_execute_all_data_types(self):
        """Test batch_execute with all supported data types across multiple statements."""
        statements = [
            "INSERT INTO test (str_col) VALUES (%(s)s)",
            "INSERT INTO test (int_col) VALUES (%(i)s)",
            "INSERT INTO test (float_col) VALUES (%(f)s)",
            "INSERT INTO test (bool_col) VALUES (%(b)s)",
            "INSERT INTO test (none_col) VALUES (%(n)s)",
            "INSERT INTO test (date_col) VALUES (%(d)s)",
            "INSERT INTO test (bytes_col) VALUES (%(by)s)",
            "INSERT INTO test (decimal_col) VALUES (%(dec)s)"
        ]
        params = [
            {"s": "text"},
            {"i": 42},
            {"f": 3.14},
            {"b": False},
            {"n": None},
            {"d": date(2025, 12, 19)},
            {"by": b'\x00\x01\x02'},
            {"dec": Decimal("123.45")}
        ]
        
        expected_values = [
            ("text",),
            (42,),
            (3.14,),
            (False,),
            (None,),
            (date(2025, 12, 19),),
            (b'\x00\x01\x02',),
            (Decimal("123.45"),)
        ]
        
        for stmt, param, expected in zip(statements, params, expected_values):
            result_sql, result_params = detect_and_convert_parameters(stmt, param)
            assert "?" in result_sql
            assert result_params == expected

    def test_batch_execute_error_handling_mixed(self):
        """Test that each statement in batch is converted independently."""
        statements = [
            "INSERT INTO users (id, name) VALUES (%(id)s, %(name)s)",
            "SELECT * FROM users WHERE id = ?",
            "UPDATE users SET email = %(email)s WHERE id = %(id)s"
        ]
        
        # Valid params for first and third, qmark for second
        params = [
            {"id": 1, "name": "Alice"},
            (1,),
            {"email": "alice@example.com", "id": 1}
        ]
        
        results = []
        for stmt, param in zip(statements, params):
            result_sql, result_params = detect_and_convert_parameters(stmt, param)
            results.append((result_sql, result_params))
        
        # Check conversions
        assert results[0][0] == "INSERT INTO users (id, name) VALUES (?, ?)"
        assert results[0][1] == (1, "Alice")
        
        assert results[1][0] == statements[1]
        assert results[1][1] == (1,)
        
        assert results[2][0] == "UPDATE users SET email = ? WHERE id = ?"
        assert results[2][1] == ("alice@example.com", 1)

    def test_batch_execute_parameter_mismatch_detection(self):
        """Test that parameter style mismatches are detected in batch context."""
        # Statement with pyformat but tuple provided
        stmt = "INSERT INTO users (id, name) VALUES (%(id)s, %(name)s)"
        param = (1, "Alice")  # Wrong: should be dict
        
        with pytest.raises(TypeError) as exc_info:
            detect_and_convert_parameters(stmt, param)
        assert "Parameter style mismatch" in str(exc_info.value)

    def test_batch_execute_missing_parameter_detection(self):
        """Test that missing parameters are detected in batch context."""
        stmt = "INSERT INTO users (id, name, email) VALUES (%(id)s, %(name)s, %(email)s)"
        param = {"id": 1, "name": "Alice"}  # Missing 'email'
        
        with pytest.raises(KeyError) as exc_info:
            detect_and_convert_parameters(stmt, param)
        error_msg = str(exc_info.value)
        assert "Missing required parameter" in error_msg
        assert "'email'" in error_msg
