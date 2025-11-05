"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.

Unit tests for _ConnectionStringParser (internal).
"""

import pytest
from mssql_python.connection_string_parser import _ConnectionStringParser, ConnectionStringParseError
from mssql_python.constants import _ConnectionStringAllowList


class TestConnectionStringParser:
    """Unit tests for _ConnectionStringParser."""
    
    def test_parse_empty_string(self):
        """Test parsing an empty string returns empty dict."""
        parser = _ConnectionStringParser()
        result = parser._parse("")
        assert result == {}
    
    def test_parse_whitespace_only(self):
        """Test parsing whitespace-only connection string."""
        parser = _ConnectionStringParser()
        result = parser._parse("   \t  ")
        assert result == {}
    
    def test_parse_simple_params(self):
        """Test parsing simple key=value pairs."""
        parser = _ConnectionStringParser()
        result = parser._parse("Server=localhost;Database=mydb")
        assert result == {
            'server': 'localhost',
            'database': 'mydb'
        }
    
    def test_parse_single_param(self):
        """Test parsing a single parameter."""
        parser = _ConnectionStringParser()
        result = parser._parse("Server=localhost")
        assert result == {'server': 'localhost'}
    
    def test_parse_trailing_semicolon(self):
        """Test parsing with trailing semicolon."""
        parser = _ConnectionStringParser()
        result = parser._parse("Server=localhost;")
        assert result == {'server': 'localhost'}
    
    def test_parse_multiple_semicolons(self):
        """Test parsing with multiple consecutive semicolons."""
        parser = _ConnectionStringParser()
        result = parser._parse("Server=localhost;;Database=mydb")
        assert result == {'server': 'localhost', 'database': 'mydb'}
    
    def test_parse_braced_value_with_semicolon(self):
        """Test parsing braced values containing semicolons."""
        parser = _ConnectionStringParser()
        result = parser._parse("Server={;local;host};Database=mydb")
        assert result == {
            'server': ';local;host',
            'database': 'mydb'
        }
    
    def test_parse_braced_value_with_escaped_right_brace(self):
        """Test parsing braced values with escaped }}."""
        parser = _ConnectionStringParser()
        result = parser._parse("PWD={p}}w{{d}")
        assert result == {'pwd': 'p}w{d'}
    
    def test_parse_braced_value_with_all_escapes(self):
        """Test parsing braced values with both {{ and }} escapes."""
        parser = _ConnectionStringParser()
        result = parser._parse("Value={test}}{{escape}")
        assert result == {'value': 'test}{escape'}
    
    def test_parse_empty_value(self):
        """Test that empty value raises error."""
        parser = _ConnectionStringParser()
        with pytest.raises(ConnectionStringParseError) as exc_info:
            parser._parse("Server=;Database=mydb")
        assert "Empty value for keyword 'server'" in str(exc_info.value)
    
    def test_parse_empty_braced_value(self):
        """Test that empty braced value raises error."""
        parser = _ConnectionStringParser()
        with pytest.raises(ConnectionStringParseError) as exc_info:
            parser._parse("Server={};Database=mydb")
        assert "Empty value for keyword 'server'" in str(exc_info.value)
    
    def test_parse_whitespace_around_key(self):
        """Test parsing with whitespace around keys."""
        parser = _ConnectionStringParser()
        result = parser._parse("  Server  =localhost;  Database  =mydb")
        assert result == {'server': 'localhost', 'database': 'mydb'}
    
    def test_parse_whitespace_in_simple_value(self):
        """Test parsing simple value with trailing whitespace."""
        parser = _ConnectionStringParser()
        result = parser._parse("Server=localhost   ;Database=mydb")
        assert result == {'server': 'localhost', 'database': 'mydb'}
    
    def test_parse_case_insensitive_keys(self):
        """Test that keys are normalized to lowercase."""
        parser = _ConnectionStringParser()
        result = parser._parse("SERVER=localhost;DatABase=mydb")
        assert result == {'server': 'localhost', 'database': 'mydb'}
    
    def test_parse_special_chars_in_simple_value(self):
        """Test parsing simple values with special characters (not ; { })."""
        parser = _ConnectionStringParser()
        result = parser._parse("Server=server:1433;User=domain\\user")
        assert result == {'server': 'server:1433', 'user': 'domain\\user'}
    
    def test_parse_complex_connection_string(self):
        """Test parsing a complex realistic connection string."""
        parser = _ConnectionStringParser()
        conn_str = "Server=tcp:server.database.windows.net,1433;Database=mydb;UID=user@server;PWD={p@ss;w}}rd};Encrypt=yes"
        result = parser._parse(conn_str)
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
        result = parser._parse("Driver={ODBC Driver 18 for SQL Server};Server=localhost")
        assert result == {
            'driver': 'ODBC Driver 18 for SQL Server',
            'server': 'localhost'
        }
    
    def test_parse_braced_value_with_left_brace(self):
        """Test parsing braced value containing unescaped single {."""
        parser = _ConnectionStringParser()
        result = parser._parse("Value={test{value}")
        assert result == {'value': 'test{value'}
    
    def test_parse_braced_value_double_left_brace(self):
        """Test parsing braced value with escaped {{ (left brace)."""
        parser = _ConnectionStringParser()
        result = parser._parse("Value={test{{value}")
        assert result == {'value': 'test{value'}
    
    def test_parse_unicode_characters(self):
        """Test parsing values with unicode characters."""
        parser = _ConnectionStringParser()
        result = parser._parse("Database=数据库;Server=сервер")
        assert result == {'database': '数据库', 'server': 'сервер'}
    
    def test_parse_equals_in_braced_value(self):
        """Test parsing braced value containing equals sign."""
        parser = _ConnectionStringParser()
        result = parser._parse("Value={key=value}")
        assert result == {'value': 'key=value'}
    
    def test_parse_special_characters_in_values(self):
        """Test parsing values with various special characters."""
        parser = _ConnectionStringParser()
        
        # Numbers, hyphens, underscores in values
        result = parser._parse("Server=server-123_test;Port=1433")
        assert result == {'server': 'server-123_test', 'port': '1433'}
        
        # Dots, colons, commas in values
        result = parser._parse("Server=server.domain.com:1433,1434")
        assert result == {'server': 'server.domain.com:1433,1434'}
        
        # At signs, slashes in values
        result = parser._parse("UID=user@domain.com;Path=/var/data")
        assert result == {'uid': 'user@domain.com', 'path': '/var/data'}
        
        # Backslashes (common in Windows paths and domain users)
        result = parser._parse("User=DOMAIN\\username;Path=C:\\temp")
        assert result == {'user': 'DOMAIN\\username', 'path': 'C:\\temp'}
    
    def test_parse_special_characters_in_braced_values(self):
        """Test parsing braced values with special characters that would otherwise be delimiters."""
        parser = _ConnectionStringParser()
        
        # Semicolons in braced values
        result = parser._parse("PWD={pass;word;123};Server=localhost")
        assert result == {'pwd': 'pass;word;123', 'server': 'localhost'}
        
        # Equals signs in braced values
        result = parser._parse("ConnectString={Key1=Value1;Key2=Value2}")
        assert result == {'connectstring': 'Key1=Value1;Key2=Value2'}
        
        # Multiple special chars including braces
        result = parser._parse("Token={Bearer: abc123; Expires={{2024-01-01}}}")
        assert result == {'token': 'Bearer: abc123; Expires={2024-01-01}'}
    
    def test_parse_numbers_and_symbols_in_passwords(self):
        """Test parsing passwords with various numbers and symbols."""
        parser = _ConnectionStringParser()
        
        # Common password characters without braces
        result = parser._parse("Server=localhost;PWD=Pass123!@#")
        assert result == {'server': 'localhost', 'pwd': 'Pass123!@#'}
        
        # Special symbols that require bracing
        result = parser._parse("PWD={P@ss;w0rd!};Server=srv")
        assert result == {'pwd': 'P@ss;w0rd!', 'server': 'srv'}
        
        # Complex password with multiple special chars
        result = parser._parse("PWD={P@$$w0rd!#123%;^&*()}")
        assert result == {'pwd': 'P@$$w0rd!#123%;^&*()'}
    
    def test_parse_emoji_and_extended_unicode(self):
        """Test parsing values with emoji and extended unicode characters."""
        parser = _ConnectionStringParser()
        
        # Emoji in values
        result = parser._parse("Description={Test 🚀 Database};Status=✓")
        assert result == {'description': 'Test 🚀 Database', 'status': '✓'}
        
        # Various unicode scripts
        result = parser._parse("Name=مرحبا;Title=こんにちは;Info=안녕하세요")
        assert result == {'name': 'مرحبا', 'title': 'こんにちは', 'info': '안녕하세요'}
    
    def test_parse_whitespace_characters(self):
        """Test parsing values with various whitespace characters."""
        parser = _ConnectionStringParser()
        
        # Spaces in braced values (preserved)
        result = parser._parse("Name={John Doe};Title={Senior Engineer}")
        assert result == {'name': 'John Doe', 'title': 'Senior Engineer'}
        
        # Tabs in braced values
        result = parser._parse("Data={value1\tvalue2\tvalue3}")
        assert result == {'data': 'value1\tvalue2\tvalue3'}
    
    def test_parse_url_encoded_characters(self):
        """Test parsing values that look like URL encoding."""
        parser = _ConnectionStringParser()
        
        # Values with percent signs and hex-like patterns
        result = parser._parse("Value=test%20value;Percent=100%")
        assert result == {'value': 'test%20value', 'percent': '100%'}
        
        # URL-like connection strings
        result = parser._parse("Server=https://api.example.com/v1;Key=abc-123-def")
        assert result == {'server': 'https://api.example.com/v1', 'key': 'abc-123-def'}


class TestConnectionStringParserErrors:
    """Test error handling in ConnectionStringParser."""
    
    def test_error_duplicate_keys(self):
        """Test that duplicate keys raise an error."""
        parser = _ConnectionStringParser()
        with pytest.raises(ConnectionStringParseError) as exc_info:
            parser._parse("Server=first;Server=second;Server=third")
        
        assert "Duplicate keyword 'server'" in str(exc_info.value)
        assert len(exc_info.value.errors) == 2  # Two duplicates (second and third)
    
    def test_error_incomplete_specification_no_equals(self):
        """Test that keyword without '=' raises an error."""
        parser = _ConnectionStringParser()
        with pytest.raises(ConnectionStringParseError) as exc_info:
            parser._parse("Server;Database=mydb")
        
        assert "Incomplete specification" in str(exc_info.value)
        assert "'server'" in str(exc_info.value).lower()
    
    def test_error_incomplete_specification_trailing(self):
        """Test that trailing keyword without value raises an error."""
        parser = _ConnectionStringParser()
        with pytest.raises(ConnectionStringParseError) as exc_info:
            parser._parse("Server=localhost;Database")
        
        assert "Incomplete specification" in str(exc_info.value)
        assert "'database'" in str(exc_info.value).lower()
    
    def test_error_empty_key(self):
        """Test that empty keyword raises an error."""
        parser = _ConnectionStringParser()
        with pytest.raises(ConnectionStringParseError) as exc_info:
            parser._parse("=value;Server=localhost")
        
        assert "Empty keyword" in str(exc_info.value)
    
    def test_error_unclosed_braced_value(self):
        """Test that unclosed braces raise an error."""
        parser = _ConnectionStringParser()
        with pytest.raises(ConnectionStringParseError) as exc_info:
            parser._parse("PWD={unclosed;Server=localhost")
        
        assert "Unclosed braced value" in str(exc_info.value)
    
    def test_error_multiple_empty_values(self):
        """Test that multiple empty values are all collected as errors."""
        parser = _ConnectionStringParser()
        with pytest.raises(ConnectionStringParseError) as exc_info:
            parser._parse("Server=;Database=;UID=user;PWD=")
        
        # Should have 3 errors for empty values
        errors = exc_info.value.errors
        assert len(errors) >= 3
        assert any("Empty value for keyword 'server'" in err for err in errors)
        assert any("Empty value for keyword 'database'" in err for err in errors)
        assert any("Empty value for keyword 'pwd'" in err for err in errors)
    
    def test_error_multiple_issues_collected(self):
        """Test that multiple different types of errors are collected and reported together."""
        parser = _ConnectionStringParser()
        with pytest.raises(ConnectionStringParseError) as exc_info:
            # Multiple error types: incomplete spec, duplicate, empty value, empty key
            parser._parse("Server=first;InvalidEntry;Server=second;Database=;=value;WhatIsThis")
        
        # Should have: incomplete spec for InvalidEntry, duplicate Server, empty Database value, empty key
        errors = exc_info.value.errors
        assert len(errors) >= 4
        
        errors_str = str(exc_info.value)
        assert "Incomplete specification" in errors_str
        assert "Duplicate keyword" in errors_str
        assert "Empty value for keyword 'database'" in errors_str
        assert "Empty keyword" in errors_str
    
    def test_error_unknown_keyword_with_allowlist(self):
        """Test that unknown keywords are flagged when allowlist is provided."""
        allowlist = _ConnectionStringAllowList()
        parser = _ConnectionStringParser(allowlist=allowlist)
        
        with pytest.raises(ConnectionStringParseError) as exc_info:
            parser._parse("Server=localhost;UnknownParam=value")
        
        assert "Unknown keyword 'unknownparam'" in str(exc_info.value)
    
    def test_error_multiple_unknown_keywords(self):
        """Test that multiple unknown keywords are all flagged."""
        allowlist = _ConnectionStringAllowList()
        parser = _ConnectionStringParser(allowlist=allowlist)
        
        with pytest.raises(ConnectionStringParseError) as exc_info:
            parser._parse("Server=localhost;Unknown1=val1;Database=mydb;Unknown2=val2")
        
        errors_str = str(exc_info.value)
        assert "Unknown keyword 'unknown1'" in errors_str
        assert "Unknown keyword 'unknown2'" in errors_str
    
    def test_error_combined_unknown_and_duplicate(self):
        """Test that unknown keywords and duplicates are both flagged."""
        allowlist = _ConnectionStringAllowList()
        parser = _ConnectionStringParser(allowlist=allowlist)
        
        with pytest.raises(ConnectionStringParseError) as exc_info:
            parser._parse("Server=first;UnknownParam=value;Server=second")
        
        errors_str = str(exc_info.value)
        assert "Unknown keyword 'unknownparam'" in errors_str
        assert "Duplicate keyword 'server'" in errors_str
    
    def test_valid_with_allowlist(self):
        """Test that valid keywords pass when allowlist is provided."""
        allowlist = _ConnectionStringAllowList()
        parser = _ConnectionStringParser(allowlist=allowlist)
        
        # These are all valid keywords in the allowlist
        result = parser._parse("Server=localhost;Database=mydb;UID=user;PWD=pass")
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
        result = parser._parse("Server=localhost;MadeUpKeyword=value")
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
            parser._parse("Server=a;Server=b;Server=c")
        
        # First occurrence is kept, other two are duplicates
        assert len(exc_info.value.errors) == 2
    
    def test_error_mixed_valid_and_errors(self):
        """Test that valid params are parsed even when errors exist."""
        parser = _ConnectionStringParser()
        with pytest.raises(ConnectionStringParseError) as exc_info:
            parser._parse("Server=localhost;BadEntry;Database=mydb;Server=dup")
        
        # Should detect incomplete and duplicate
        assert len(exc_info.value.errors) >= 2
    
    def test_normalization_still_works(self):
        """Test that key normalization to lowercase still works."""
        parser = _ConnectionStringParser()
        result = parser._parse("SERVER=srv;DaTaBaSe=db")
        assert result == {'server': 'srv', 'database': 'db'}
    
    def test_error_duplicate_after_normalization(self):
        """Test that duplicates are detected after normalization."""
        parser = _ConnectionStringParser()
        with pytest.raises(ConnectionStringParseError) as exc_info:
            parser._parse("Server=first;SERVER=second")
        
        assert "Duplicate keyword 'server'" in str(exc_info.value)
    
    def test_empty_value_edge_cases(self):
        """Test that empty values are treated as errors."""
        parser = _ConnectionStringParser()
        
        # Empty value after = with trailing semicolon
        with pytest.raises(ConnectionStringParseError) as exc_info:
            parser._parse("Server=localhost;Database=")
        assert "Empty value for keyword 'database'" in str(exc_info.value)
        
        # Empty value at end of string (no trailing semicolon)
        with pytest.raises(ConnectionStringParseError) as exc_info:
            parser._parse("Server=localhost;Database=")
        assert "Empty value for keyword 'database'" in str(exc_info.value)
        
        # Value with only whitespace is treated as empty after strip
        with pytest.raises(ConnectionStringParseError) as exc_info:
            parser._parse("Server=localhost;Database=   ")
        assert "Empty value for keyword 'database'" in str(exc_info.value)
    
    def test_incomplete_entry_recovery(self):
        """Test that parser can recover from incomplete entries and continue parsing."""
        parser = _ConnectionStringParser()
        with pytest.raises(ConnectionStringParseError) as exc_info:
            # Incomplete entry followed by valid entry
            parser._parse("Server;Database=mydb;UID=user")
        
        # Should have error about incomplete 'Server'
        errors = exc_info.value.errors
        assert any('Server' in err and 'Incomplete specification' in err for err in errors)
