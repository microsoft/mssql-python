"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.

Unit tests for _ConnectionStringParser (internal).
"""

import pytest
from mssql_python.connection_string_parser import _ConnectionStringParser, ConnectionStringParseError
from mssql_python.connection_string_allowlist import ConnectionStringAllowList


class TestConnectionStringParser:
    """Unit tests for _ConnectionStringParser."""
    
    def test_parse_empty_string(self):
        """Test parsing an empty string returns empty dict."""
        parser = _ConnectionStringParser()
        result = parser.parse("")
        assert result == {}
    
    def test_parse_whitespace_only(self):
        """Test parsing whitespace-only connection string."""
        parser = _ConnectionStringParser()
        result = parser.parse("   \t  ")
        assert result == {}
    
    def test_parse_simple_params(self):
        """Test parsing simple key=value pairs."""
        parser = _ConnectionStringParser()
        result = parser.parse("Server=localhost;Database=mydb")
        assert result == {
            'server': 'localhost',
            'database': 'mydb'
        }
    
    def test_parse_single_param(self):
        """Test parsing a single parameter."""
        parser = _ConnectionStringParser()
        result = parser.parse("Server=localhost")
        assert result == {'server': 'localhost'}
    
    def test_parse_trailing_semicolon(self):
        """Test parsing with trailing semicolon."""
        parser = _ConnectionStringParser()
        result = parser.parse("Server=localhost;")
        assert result == {'server': 'localhost'}
    
    def test_parse_multiple_semicolons(self):
        """Test parsing with multiple consecutive semicolons."""
        parser = _ConnectionStringParser()
        result = parser.parse("Server=localhost;;Database=mydb")
        assert result == {'server': 'localhost', 'database': 'mydb'}
    
    def test_parse_braced_value_with_semicolon(self):
        """Test parsing braced values containing semicolons."""
        parser = _ConnectionStringParser()
        result = parser.parse("Server={;local;host};Database=mydb")
        assert result == {
            'server': ';local;host',
            'database': 'mydb'
        }
    
    def test_parse_braced_value_with_escaped_right_brace(self):
        """Test parsing braced values with escaped }}."""
        parser = _ConnectionStringParser()
        result = parser.parse("PWD={p}}w{{d}")
        assert result == {'pwd': 'p}w{d'}
    
    def test_parse_braced_value_with_all_escapes(self):
        """Test parsing braced values with both {{ and }} escapes."""
        parser = _ConnectionStringParser()
        result = parser.parse("Value={test}}{{escape}")
        assert result == {'value': 'test}{escape'}
    
    def test_parse_empty_value(self):
        """Test parsing parameter with empty value."""
        parser = _ConnectionStringParser()
        result = parser.parse("Server=;Database=mydb")
        assert result == {'server': '', 'database': 'mydb'}
    
    def test_parse_empty_braced_value(self):
        """Test parsing parameter with empty braced value."""
        parser = _ConnectionStringParser()
        result = parser.parse("Server={};Database=mydb")
        assert result == {'server': '', 'database': 'mydb'}
    
    def test_parse_whitespace_around_key(self):
        """Test parsing with whitespace around keys."""
        parser = _ConnectionStringParser()
        result = parser.parse("  Server  =localhost;  Database  =mydb")
        assert result == {'server': 'localhost', 'database': 'mydb'}
    
    def test_parse_whitespace_in_simple_value(self):
        """Test parsing simple value with trailing whitespace."""
        parser = _ConnectionStringParser()
        result = parser.parse("Server=localhost   ;Database=mydb")
        assert result == {'server': 'localhost', 'database': 'mydb'}
    
    def test_parse_case_insensitive_keys(self):
        """Test that keys are normalized to lowercase."""
        parser = _ConnectionStringParser()
        result = parser.parse("SERVER=localhost;DatABase=mydb")
        assert result == {'server': 'localhost', 'database': 'mydb'}
    
    def test_parse_special_chars_in_simple_value(self):
        """Test parsing simple values with special characters (not ; { })."""
        parser = _ConnectionStringParser()
        result = parser.parse("Server=server:1433;User=domain\\user")
        assert result == {'server': 'server:1433', 'user': 'domain\\user'}
    
    def test_parse_complex_connection_string(self):
        """Test parsing a complex realistic connection string."""
        parser = _ConnectionStringParser()
        conn_str = "Server=tcp:server.database.windows.net,1433;Database=mydb;UID=user@server;PWD={p@ss;w}}rd};Encrypt=yes"
        result = parser.parse(conn_str)
        assert result == {
            'server': 'tcp:server.database.windows.net,1433',
            'database': 'mydb',
            'uid': 'user@server',
            'pwd': 'p@ss;w}rd',  # }} escapes to single }
            'encrypt': 'yes'
        }
    
    def test_parse_driver_parameter(self):
        """Test parsing Driver parameter with braced value."""
        parser = _ConnectionStringParser()
        result = parser.parse("Driver={ODBC Driver 18 for SQL Server};Server=localhost")
        assert result == {
            'driver': 'ODBC Driver 18 for SQL Server',
            'server': 'localhost'
        }
    
    def test_parse_braced_value_with_left_brace(self):
        """Test parsing braced value containing unescaped single {."""
        parser = _ConnectionStringParser()
        result = parser.parse("Value={test{value}")
        assert result == {'value': 'test{value'}
    
    def test_parse_braced_value_double_left_brace(self):
        """Test parsing braced value with escaped {{ (left brace)."""
        parser = _ConnectionStringParser()
        result = parser.parse("Value={test{{value}")
        assert result == {'value': 'test{value'}
    
    def test_parse_unicode_characters(self):
        """Test parsing values with unicode characters."""
        parser = _ConnectionStringParser()
        result = parser.parse("Database=数据库;Server=сервер")
        assert result == {'database': '数据库', 'server': 'сервер'}
    
    def test_parse_equals_in_braced_value(self):
        """Test parsing braced value containing equals sign."""
        parser = _ConnectionStringParser()
        result = parser.parse("Value={key=value}")
        assert result == {'value': 'key=value'}


class TestConnectionStringParserErrors:
    """Test error handling in ConnectionStringParser."""
    
    def test_error_duplicate_keys(self):
        """Test that duplicate keys raise an error."""
        parser = _ConnectionStringParser()
        with pytest.raises(ConnectionStringParseError) as exc_info:
            parser.parse("Server=first;Server=second;Server=third")
        
        assert "Duplicate keyword 'server'" in str(exc_info.value)
        assert len(exc_info.value.errors) == 2  # Two duplicates (second and third)
    
    def test_error_incomplete_specification_no_equals(self):
        """Test that keyword without '=' raises an error."""
        parser = _ConnectionStringParser()
        with pytest.raises(ConnectionStringParseError) as exc_info:
            parser.parse("Server;Database=mydb")
        
        assert "Incomplete specification" in str(exc_info.value)
        assert "'server'" in str(exc_info.value).lower()
    
    def test_error_incomplete_specification_trailing(self):
        """Test that trailing keyword without value raises an error."""
        parser = _ConnectionStringParser()
        with pytest.raises(ConnectionStringParseError) as exc_info:
            parser.parse("Server=localhost;Database")
        
        assert "Incomplete specification" in str(exc_info.value)
        assert "'database'" in str(exc_info.value).lower()
    
    def test_error_empty_key(self):
        """Test that empty keyword raises an error."""
        parser = _ConnectionStringParser()
        with pytest.raises(ConnectionStringParseError) as exc_info:
            parser.parse("=value;Server=localhost")
        
        assert "Empty keyword" in str(exc_info.value)
    
    def test_error_unclosed_braced_value(self):
        """Test that unclosed braces raise an error."""
        parser = _ConnectionStringParser()
        with pytest.raises(ConnectionStringParseError) as exc_info:
            parser.parse("PWD={unclosed;Server=localhost")
        
        assert "Unclosed braced value" in str(exc_info.value)
    
    def test_error_multiple_issues_collected(self):
        """Test that multiple errors are collected and reported together."""
        parser = _ConnectionStringParser()
        with pytest.raises(ConnectionStringParseError) as exc_info:
            parser.parse("Server=first;InvalidEntry;Server=second;Database")
        
        # Should have: incomplete spec for InvalidEntry, duplicate Server, incomplete spec for Database
        assert len(exc_info.value.errors) >= 3
        assert "Incomplete specification" in str(exc_info.value)
        assert "Duplicate keyword" in str(exc_info.value)
    
    def test_error_unknown_keyword_with_allowlist(self):
        """Test that unknown keywords are flagged when allowlist is provided."""
        allowlist = ConnectionStringAllowList()
        parser = _ConnectionStringParser(allowlist=allowlist)
        
        with pytest.raises(ConnectionStringParseError) as exc_info:
            parser.parse("Server=localhost;UnknownParam=value")
        
        assert "Unknown keyword 'unknownparam'" in str(exc_info.value)
    
    def test_error_multiple_unknown_keywords(self):
        """Test that multiple unknown keywords are all flagged."""
        allowlist = ConnectionStringAllowList()
        parser = _ConnectionStringParser(allowlist=allowlist)
        
        with pytest.raises(ConnectionStringParseError) as exc_info:
            parser.parse("Server=localhost;Unknown1=val1;Database=mydb;Unknown2=val2")
        
        errors_str = str(exc_info.value)
        assert "Unknown keyword 'unknown1'" in errors_str
        assert "Unknown keyword 'unknown2'" in errors_str
    
    def test_error_combined_unknown_and_duplicate(self):
        """Test that unknown keywords and duplicates are both flagged."""
        allowlist = ConnectionStringAllowList()
        parser = _ConnectionStringParser(allowlist=allowlist)
        
        with pytest.raises(ConnectionStringParseError) as exc_info:
            parser.parse("Server=first;UnknownParam=value;Server=second")
        
        errors_str = str(exc_info.value)
        assert "Unknown keyword 'unknownparam'" in errors_str
        assert "Duplicate keyword 'server'" in errors_str
    
    def test_valid_with_allowlist(self):
        """Test that valid keywords pass when allowlist is provided."""
        allowlist = ConnectionStringAllowList()
        parser = _ConnectionStringParser(allowlist=allowlist)
        
        # These are all valid keywords in the allowlist
        result = parser.parse("Server=localhost;Database=mydb;UID=user;PWD=pass")
        assert result == {
            'server': 'localhost',
            'database': 'mydb',
            'uid': 'user',
            'pwd': 'pass'
        }
    
    def test_no_validation_without_allowlist(self):
        """Test that unknown keywords are allowed when no allowlist is provided."""
        parser = _ConnectionStringParser()  # No allowlist
        
        # Should parse successfully even with unknown keywords
        result = parser.parse("Server=localhost;MadeUpKeyword=value")
        assert result == {
            'server': 'localhost',
            'madeupkeyword': 'value'
        }


class TestConnectionStringParserEdgeCases:
    """Test edge cases and boundary conditions."""
    
    def test_error_all_duplicates(self):
        """Test string with only duplicates."""
        parser = _ConnectionStringParser()
        with pytest.raises(ConnectionStringParseError) as exc_info:
            parser.parse("Server=a;Server=b;Server=c")
        
        # First occurrence is kept, other two are duplicates
        assert len(exc_info.value.errors) == 2
    
    def test_error_mixed_valid_and_errors(self):
        """Test that valid params are parsed even when errors exist."""
        parser = _ConnectionStringParser()
        with pytest.raises(ConnectionStringParseError) as exc_info:
            parser.parse("Server=localhost;BadEntry;Database=mydb;Server=dup")
        
        # Should detect incomplete and duplicate
        assert len(exc_info.value.errors) >= 2
    
    def test_normalization_still_works(self):
        """Test that key normalization to lowercase still works."""
        parser = _ConnectionStringParser()
        result = parser.parse("SERVER=srv;DaTaBaSe=db")
        assert result == {'server': 'srv', 'database': 'db'}
    
    def test_error_duplicate_after_normalization(self):
        """Test that duplicates are detected after normalization."""
        parser = _ConnectionStringParser()
        with pytest.raises(ConnectionStringParseError) as exc_info:
            parser.parse("Server=first;SERVER=second")
        
        assert "Duplicate keyword 'server'" in str(exc_info.value)
