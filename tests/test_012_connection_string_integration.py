"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.

Integration tests for connection string allow-list feature.

These tests verify end-to-end behavior of the parser, filter, and builder pipeline.
"""

import pytest
import os
from unittest.mock import patch, MagicMock
from mssql_python.connection_string_parser import _ConnectionStringParser, ConnectionStringParseError
from mssql_python.connection_string_allowlist import ConnectionStringAllowList
from mssql_python.connection_string_builder import _ConnectionStringBuilder
from mssql_python import connect
from mssql_python.connection import Connection
from mssql_python.exceptions import DatabaseError, InterfaceError


class TestConnectionStringIntegration:
    """Integration tests for the complete connection string flow."""
    
    def test_parse_filter_build_simple(self):
        """Test complete flow with simple parameters."""
        # Parse
        parser = _ConnectionStringParser()
        parsed = parser.parse("Server=localhost;Database=mydb;Encrypt=yes")
        
        # Filter
        filtered = ConnectionStringAllowList.filter_params(parsed, warn_rejected=False)
        
        # Build
        builder = _ConnectionStringBuilder(filtered)
        builder.add_param('Driver', 'ODBC Driver 18 for SQL Server')
        builder.add_param('APP', 'MSSQL-Python')
        result = builder.build()
        
        # Verify
        assert 'Driver={ODBC Driver 18 for SQL Server}' in result
        assert 'Server=localhost' in result
        assert 'Database=mydb' in result
        assert 'Encrypt=yes' in result
        assert 'APP=MSSQL-Python' in result
    
    def test_parse_filter_build_with_unsupported_param(self):
        """Test that unsupported parameters are flagged as errors with allowlist."""
        # Parse with allowlist
        allowlist = ConnectionStringAllowList()
        parser = _ConnectionStringParser(allowlist=allowlist)
        
        # Should raise error for unknown keyword
        with pytest.raises(ConnectionStringParseError) as exc_info:
            parser.parse("Server=localhost;Database=mydb;UnsupportedParam=value")
        
        assert "Unknown keyword 'unsupportedparam'" in str(exc_info.value)
    
    def test_parse_filter_build_with_braced_values(self):
        """Test complete flow with braced values and special characters."""
        # Parse
        parser = _ConnectionStringParser()
        parsed = parser.parse("Server={local;host};PWD={p@ss;w}}rd}")
        
        # Filter
        filtered = ConnectionStringAllowList.filter_params(parsed, warn_rejected=False)
        
        # Build
        builder = _ConnectionStringBuilder(filtered)
        builder.add_param('Driver', 'ODBC Driver 18 for SQL Server')
        result = builder.build()
        
        # Verify - values with special chars should be re-escaped
        assert 'Driver={ODBC Driver 18 for SQL Server}' in result
        assert 'Server={local;host}' in result
        assert 'Pwd={p@ss;w}}rd}' in result or 'PWD={p@ss;w}}rd}' in result
    
    def test_parse_filter_build_synonym_normalization(self):
        """Test that parameter synonyms are normalized."""
        # Parse
        parser = _ConnectionStringParser()
        parsed = parser.parse("address=server1;user=testuser;initial catalog=testdb")
        
        # Filter (normalizes synonyms)
        filtered = ConnectionStringAllowList.filter_params(parsed, warn_rejected=False)
        
        # Build
        builder = _ConnectionStringBuilder(filtered)
        builder.add_param('Driver', 'ODBC Driver 18 for SQL Server')
        result = builder.build()
        
        # Verify - should use canonical names
        assert 'Server=server1' in result
        assert 'Uid=testuser' in result
        assert 'Database=testdb' in result
        # Original names should not appear
        assert 'address' not in result.lower()
        assert 'user=' not in result.lower()
        assert 'initial catalog' not in result.lower()
    
    def test_parse_filter_build_driver_and_app_reserved(self):
        """Test that Driver and APP in connection string raise errors."""
        # Parser should reject Driver and APP as reserved keywords
        allowlist = ConnectionStringAllowList()
        parser = _ConnectionStringParser(allowlist=allowlist)
        
        # Test with APP
        with pytest.raises(ConnectionStringParseError) as exc_info:
            parser.parse("APP=UserApp;Server=localhost")
        error_lower = str(exc_info.value).lower()
        assert "reserved keyword" in error_lower
        assert "'app'" in error_lower
        
        # Test with Driver
        with pytest.raises(ConnectionStringParseError) as exc_info:
            parser.parse("Driver={Some Other Driver};Server=localhost")
        error_lower = str(exc_info.value).lower()
        assert "reserved keyword" in error_lower
        assert "'driver'" in error_lower
        
        # Test with both
        with pytest.raises(ConnectionStringParseError) as exc_info:
            parser.parse("Driver={Some Other Driver};APP=UserApp;Server=localhost")
        error_str = str(exc_info.value).lower()
        assert "reserved keyword" in error_str
        # Should have errors for both
        assert len(exc_info.value.errors) == 2
    
    def test_parse_filter_build_empty_input(self):
        """Test complete flow with empty input."""
        # Parse
        parser = _ConnectionStringParser()
        parsed = parser.parse("")
        
        # Filter
        filtered = ConnectionStringAllowList.filter_params(parsed, warn_rejected=False)
        
        # Build
        builder = _ConnectionStringBuilder(filtered)
        builder.add_param('Driver', 'ODBC Driver 18 for SQL Server')
        result = builder.build()
        
        # Verify - should only have Driver
        assert result == 'Driver={ODBC Driver 18 for SQL Server}'
    
    def test_parse_filter_build_complex_realistic(self):
        """Test complete flow with complex realistic connection string."""
        # Parse
        parser = _ConnectionStringParser()
        conn_str = "Server=tcp:server.database.windows.net,1433;Database=mydb;UID=user@server;PWD={P@ss;w}}rd};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30"
        parsed = parser.parse(conn_str)
        
        # Filter
        filtered = ConnectionStringAllowList.filter_params(parsed, warn_rejected=False)
        
        # Build
        builder = _ConnectionStringBuilder(filtered)
        builder.add_param('Driver', 'ODBC Driver 18 for SQL Server')
        builder.add_param('APP', 'MSSQL-Python')
        result = builder.build()
        
        # Verify key parameters are present
        assert 'Driver={ODBC Driver 18 for SQL Server}' in result
        assert 'Server=tcp:server.database.windows.net,1433' in result
        assert 'Database=mydb' in result
        assert 'Uid=user@server' in result
        assert 'Pwd={P@ss;w}}rd}' in result or 'PWD={P@ss;w}}rd}' in result
        assert 'Encrypt=yes' in result
        assert 'TrustServerCertificate=no' in result
        assert 'Connection Timeout=30' in result
        assert 'APP=MSSQL-Python' in result
    
    def test_parse_error_incomplete_specification(self):
        """Test that incomplete specifications raise errors."""
        parser = _ConnectionStringParser()
        
        # Incomplete specification raises error
        with pytest.raises(ConnectionStringParseError) as exc_info:
            parser.parse("Server localhost;Database=mydb")
        
        assert "Incomplete specification" in str(exc_info.value)
        assert "'server localhost'" in str(exc_info.value).lower()
    
    def test_parse_error_unclosed_brace(self):
        """Test that unclosed braces raise errors."""
        parser = _ConnectionStringParser()
        
        # Unclosed brace raises error
        with pytest.raises(ConnectionStringParseError) as exc_info:
            parser.parse("PWD={unclosed;Server=localhost")
        
        assert "Unclosed braced value" in str(exc_info.value)
    
    def test_parse_error_duplicate_keywords(self):
        """Test that duplicate keywords raise errors."""
        parser = _ConnectionStringParser()
        
        # Duplicate keywords raise error
        with pytest.raises(ConnectionStringParseError) as exc_info:
            parser.parse("Server=first;Server=second")
        
        assert "Duplicate keyword 'server'" in str(exc_info.value)
    
    def test_round_trip_preserves_values(self):
        """Test that parsing and rebuilding preserves parameter values."""
        original_params = {
            'server': 'localhost:1433',
            'database': 'TestDB',
            'uid': 'testuser',
            'pwd': 'Test@123',
            'encrypt': 'yes'
        }
        
        # Filter
        filtered = ConnectionStringAllowList.filter_params(original_params, warn_rejected=False)
        
        # Build
        builder = _ConnectionStringBuilder(filtered)
        builder.add_param('Driver', 'ODBC Driver 18 for SQL Server')
        result = builder.build()
        
        # Parse back
        parser = _ConnectionStringParser()
        parsed = parser.parse(result)
        
        # Verify values are preserved (keys are normalized to lowercase in parsing)
        assert parsed['server'] == 'localhost:1433'
        assert parsed['database'] == 'TestDB'
        assert parsed['uid'] == 'testuser'
        assert parsed['pwd'] == 'Test@123'
        assert parsed['encrypt'] == 'yes'
        assert parsed['driver'] == 'ODBC Driver 18 for SQL Server'
    
    def test_builder_escaping_is_correct(self):
        """Test that builder correctly escapes special characters."""
        builder = _ConnectionStringBuilder()
        builder.add_param('Server', 'local;host')
        builder.add_param('PWD', 'p}w{d')
        builder.add_param('Value', 'test;{value}')
        result = builder.build()
        
        # Parse back to verify escaping worked
        parser = _ConnectionStringParser()
        parsed = parser.parse(result)
        
        assert parsed['server'] == 'local;host'
        assert parsed['pwd'] == 'p}w{d'
        assert parsed['value'] == 'test;{value}'
    
    def test_multiple_errors_collected(self):
        """Test that multiple errors are collected and reported together."""
        parser = _ConnectionStringParser()
        
        # Multiple errors: incomplete spec, duplicate
        with pytest.raises(ConnectionStringParseError) as exc_info:
            parser.parse("Server=first;InvalidEntry;Server=second;Database")
        
        # Should have multiple errors
        assert len(exc_info.value.errors) >= 3
        assert "Incomplete specification" in str(exc_info.value)
        assert "Duplicate keyword" in str(exc_info.value)
    
    def test_parser_without_allowlist_accepts_unknown(self):
        """Test that parser without allowlist accepts unknown keywords."""
        parser = _ConnectionStringParser()  # No allowlist
        
        # Should parse successfully even with unknown keywords
        result = parser.parse("Server=localhost;MadeUpKeyword=value")
        assert result == {
            'server': 'localhost',
            'madeupkeyword': 'value'
        }
    
    def test_parser_with_allowlist_rejects_unknown(self):
        """Test that parser with allowlist rejects unknown keywords."""
        allowlist = ConnectionStringAllowList()
        parser = _ConnectionStringParser(allowlist=allowlist)
        
        # Should raise error for unknown keyword
        with pytest.raises(ConnectionStringParseError) as exc_info:
            parser.parse("Server=localhost;MadeUpKeyword=value")
        
        assert "Unknown keyword 'madeupkeyword'" in str(exc_info.value)


class TestConnectAPIIntegration:
    """Integration tests for the connect() API with connection string validation."""
    
    def test_connect_with_unknown_keyword_raises_error(self):
        """Test that connect() raises error for unknown keywords."""
        # connect() uses allowlist validation internally
        with pytest.raises(ConnectionStringParseError) as exc_info:
            connect("Server=localhost;Database=test;UnknownKeyword=value")
        
        assert "Unknown keyword 'unknownkeyword'" in str(exc_info.value)
    
    def test_connect_with_duplicate_keywords_raises_error(self):
        """Test that connect() raises error for duplicate keywords."""
        with pytest.raises(ConnectionStringParseError) as exc_info:
            connect("Server=first;Server=second;Database=test")
        
        assert "Duplicate keyword 'server'" in str(exc_info.value)
    
    def test_connect_with_incomplete_specification_raises_error(self):
        """Test that connect() raises error for incomplete specifications."""
        with pytest.raises(ConnectionStringParseError) as exc_info:
            connect("Server localhost;Database=test")
        
        assert "Incomplete specification" in str(exc_info.value)
    
    def test_connect_with_unclosed_brace_raises_error(self):
        """Test that connect() raises error for unclosed braces."""
        with pytest.raises(ConnectionStringParseError) as exc_info:
            connect("PWD={unclosed;Server=localhost")
        
        assert "Unclosed braced value" in str(exc_info.value)
    
    def test_connect_with_multiple_errors_collected(self):
        """Test that connect() collects multiple errors."""
        with pytest.raises(ConnectionStringParseError) as exc_info:
            connect("Server=first;InvalidEntry;Server=second;Database")
        
        # Should have multiple errors
        assert len(exc_info.value.errors) >= 3
        error_str = str(exc_info.value)
        assert "Incomplete specification" in error_str
        assert "Duplicate keyword" in error_str
    
    @patch('mssql_python.connection.ddbc_bindings.Connection')
    def test_connect_kwargs_override_connection_string(self, mock_ddbc_conn):
        """Test that kwargs override connection string parameters."""
        # Mock the underlying ODBC connection
        mock_ddbc_conn.return_value = MagicMock()
        
        conn = connect("Server=original;Database=originaldb", 
                      Server="overridden", 
                      Database="overriddendb")
        
        # Verify the override worked
        assert "overridden" in conn.connection_str.lower()
        assert "overriddendb" in conn.connection_str.lower()
        # Original values should not be in the final connection string
        assert "original" not in conn.connection_str.lower() or "originaldb" not in conn.connection_str.lower()
        
        conn.close()
    
    @patch('mssql_python.connection.ddbc_bindings.Connection')
    def test_connect_app_parameter_in_connection_string_raises_error(self, mock_ddbc_conn):
        """Test that APP parameter in connection string raises ConnectionStringParseError."""
        # Mock the underlying ODBC connection
        mock_ddbc_conn.return_value = MagicMock()
        
        # User tries to set APP in connection string - should raise error
        with pytest.raises(ConnectionStringParseError) as exc_info:
            connect("Server=localhost;APP=UserApp;Database=test")
        
        # Verify error message
        error_lower = str(exc_info.value).lower()
        assert "reserved keyword" in error_lower
        assert "'app'" in error_lower
        assert "controlled by the driver" in error_lower
    
    @patch('mssql_python.connection.ddbc_bindings.Connection')
    def test_connect_app_parameter_in_kwargs_raises_error(self, mock_ddbc_conn):
        """Test that APP parameter in kwargs raises ValueError."""
        # Mock the underlying ODBC connection
        mock_ddbc_conn.return_value = MagicMock()
        
        # User tries to set APP via kwargs - should raise ValueError
        with pytest.raises(ValueError) as exc_info:
            connect("Server=localhost;Database=test", APP="UserApp")
        
        assert "reserved and controlled by the driver" in str(exc_info.value)
        assert "APP" in str(exc_info.value) or "app" in str(exc_info.value).lower()
    
    @patch('mssql_python.connection.ddbc_bindings.Connection')
    def test_connect_driver_parameter_in_connection_string_raises_error(self, mock_ddbc_conn):
        """Test that Driver parameter in connection string raises ConnectionStringParseError."""
        # Mock the underlying ODBC connection
        mock_ddbc_conn.return_value = MagicMock()
        
        # User tries to set Driver in connection string - should raise error
        with pytest.raises(ConnectionStringParseError) as exc_info:
            connect("Server=localhost;Driver={Some Other Driver};Database=test")
        
        # Verify error message
        error_lower = str(exc_info.value).lower()
        assert "reserved keyword" in error_lower
        assert "'driver'" in error_lower
        assert "controlled by the driver" in error_lower
    
    @patch('mssql_python.connection.ddbc_bindings.Connection')
    def test_connect_driver_parameter_in_kwargs_raises_error(self, mock_ddbc_conn):
        """Test that Driver parameter in kwargs raises ValueError."""
        # Mock the underlying ODBC connection
        mock_ddbc_conn.return_value = MagicMock()
        
        # User tries to set Driver via kwargs - should raise ValueError
        with pytest.raises(ValueError) as exc_info:
            connect("Server=localhost;Database=test", Driver="Some Other Driver")
        
        assert "reserved and controlled by the driver" in str(exc_info.value)
        assert "Driver" in str(exc_info.value)
    
    @patch('mssql_python.connection.ddbc_bindings.Connection')
    def test_connect_synonym_normalization(self, mock_ddbc_conn):
        """Test that connect() normalizes parameter synonyms."""
        # Mock the underlying ODBC connection
        mock_ddbc_conn.return_value = MagicMock()
        
        conn = connect("address=server1;user=testuser;initial catalog=testdb")
        
        # Synonyms should be normalized to canonical names
        conn_str_lower = conn.connection_str.lower()
        assert "server=server1" in conn_str_lower
        assert "uid=testuser" in conn_str_lower
        assert "database=testdb" in conn_str_lower
        # Original names should not appear
        assert "address=" not in conn_str_lower
        assert "user=" not in conn_str_lower
        assert "initial catalog=" not in conn_str_lower
        
        conn.close()
    
    @patch('mssql_python.connection.ddbc_bindings.Connection')
    def test_connect_kwargs_unknown_parameter_warned(self, mock_ddbc_conn):
        """Test that unknown kwargs are warned about but don't raise errors during parsing."""
        # Mock the underlying ODBC connection
        mock_ddbc_conn.return_value = MagicMock()
        
        # Unknown kwargs are filtered out with a warning, but don't cause parse errors
        # because kwargs bypass the parser's allowlist validation
        conn = connect("Server=localhost", Database="test", UnknownParam="value")
        
        # UnknownParam should be filtered out (warned but not included)
        conn_str_lower = conn.connection_str.lower()
        assert "database=test" in conn_str_lower
        assert "unknownparam" not in conn_str_lower
        
        conn.close()
    
    @patch('mssql_python.connection.ddbc_bindings.Connection')
    def test_connect_empty_connection_string(self, mock_ddbc_conn):
        """Test that connect() works with empty connection string and kwargs."""
        # Mock the underlying ODBC connection
        mock_ddbc_conn.return_value = MagicMock()
        
        conn = connect("", Server="localhost", Database="test")
        
        # Should have Server and Database from kwargs
        conn_str_lower = conn.connection_str.lower()
        assert "server=localhost" in conn_str_lower
        assert "database=test" in conn_str_lower
        assert "driver=" in conn_str_lower  # Driver is always added
        assert "app=mssql-python" in conn_str_lower  # APP is always added
        
        conn.close()
    
    @patch('mssql_python.connection.ddbc_bindings.Connection')
    def test_connect_special_characters_in_values(self, mock_ddbc_conn):
        """Test that connect() properly handles special characters in parameter values."""
        # Mock the underlying ODBC connection
        mock_ddbc_conn.return_value = MagicMock()
        
        conn = connect("Server={local;host};PWD={p@ss;w}}rd};Database=test")
        
        # Special characters should be preserved through parsing and building
        # The connection string should properly escape them
        assert "local;host" in conn.connection_str or "{local;host}" in conn.connection_str
        assert "p@ss;w}rd" in conn.connection_str or "{p@ss;w}}rd}" in conn.connection_str
        
        conn.close()
    
    @pytest.mark.skipif(not os.getenv('DB_CONNECTION_STRING'), 
                        reason="Requires database connection string")
    def test_connect_with_real_database(self, conn_str):
        """Test that connect() works with a real database connection."""
        # This test only runs if DB_CONNECTION_STRING is set
        conn = connect(conn_str)
        assert conn is not None
        
        # Verify connection string has required parameters
        assert "Driver=" in conn.connection_str or "driver=" in conn.connection_str
        assert "APP=MSSQL-Python" in conn.connection_str or "app=mssql-python" in conn.connection_str.lower()
        
        # Test basic query execution
        cursor = conn.cursor()
        cursor.execute("SELECT 1 AS test")
        row = cursor.fetchone()
        assert row[0] == 1
        cursor.close()
        
        conn.close()
    
    @pytest.mark.skipif(not os.getenv('DB_CONNECTION_STRING'), 
                        reason="Requires database connection string")
    def test_connect_kwargs_override_with_real_database(self, conn_str):
        """Test that kwargs override works with a real database connection."""
        # Parse the original connection string to extract server
        parser = _ConnectionStringParser()
        original_params = parser.parse(conn_str)
        
        # Get the server from original connection for reconnection
        server = original_params.get('server', 'localhost')
        
        # Create connection with overridden autocommit
        conn = connect(conn_str, autocommit=True)
        
        # Verify connection works and autocommit is set
        assert conn.autocommit == True
        
        # Verify connection string still has all required params
        assert "Driver=" in conn.connection_str or "driver=" in conn.connection_str
        assert "APP=MSSQL-Python" in conn.connection_str or "app=mssql-python" in conn.connection_str.lower()
        
        conn.close()
    
    @pytest.mark.skipif(not os.getenv('DB_CONNECTION_STRING'), 
                        reason="Requires database connection string")
    def test_connect_reserved_params_in_connection_string_raise_error(self, conn_str):
        """Test that reserved params (Driver, APP) in connection string raise error."""
        # Try to add Driver to connection string - should raise error
        with pytest.raises(ConnectionStringParseError) as exc_info:
            test_conn_str = conn_str + ";Driver={User Driver}"
            connect(test_conn_str)
        assert "reserved keyword" in str(exc_info.value).lower()
        assert "driver" in str(exc_info.value).lower()
        
        # Try to add APP to connection string - should raise error
        with pytest.raises(ConnectionStringParseError) as exc_info:
            test_conn_str = conn_str + ";APP=UserApp"
            connect(test_conn_str)
        assert "reserved keyword" in str(exc_info.value).lower()
        assert "app" in str(exc_info.value).lower()
        
        # Try Application Name synonym
        with pytest.raises(ConnectionStringParseError) as exc_info:
            test_conn_str = conn_str + ";Application Name=UserApp"
            connect(test_conn_str)
        assert "reserved keyword" in str(exc_info.value).lower()
    
    @pytest.mark.skipif(not os.getenv('DB_CONNECTION_STRING'), 
                        reason="Requires database connection string")
    def test_connect_reserved_params_in_kwargs_raise_error(self, conn_str):
        """Test that reserved params (Driver, APP) in kwargs raise ValueError."""
        # Try to override Driver via kwargs - should raise ValueError
        with pytest.raises(ValueError) as exc_info:
            connect(conn_str, Driver="User Driver")
        assert "reserved and controlled by the driver" in str(exc_info.value)
        
        # Try to override APP via kwargs - should raise ValueError
        with pytest.raises(ValueError) as exc_info:
            connect(conn_str, APP="UserApp")
        assert "reserved and controlled by the driver" in str(exc_info.value)
    
    @pytest.mark.skipif(not os.getenv('DB_CONNECTION_STRING'), 
                        reason="Requires database connection string")
    def test_app_name_received_by_sql_server(self, conn_str):
        """Test that SQL Server receives the driver-controlled APP name 'MSSQL-Python'."""
        # Connect to SQL Server
        conn = connect(conn_str)
        
        try:
            # Query SQL Server to get the application name it received
            cursor = conn.cursor()
            cursor.execute("SELECT APP_NAME() AS app_name")
            row = cursor.fetchone()
            cursor.close()
            
            # Verify SQL Server received the driver-controlled application name
            assert row is not None, "Failed to get APP_NAME() from SQL Server"
            app_name_received = row[0]
            
            # SQL Server should have received 'MSSQL-Python', not any user-provided value
            assert app_name_received == 'MSSQL-Python', \
                f"Expected SQL Server to receive 'MSSQL-Python', but got '{app_name_received}'"
            
            print(f"\n✓ SQL Server correctly received APP_NAME: '{app_name_received}'")
        finally:
            conn.close()
    
    @pytest.mark.skipif(not os.getenv('DB_CONNECTION_STRING'), 
                        reason="Requires database connection string")
    def test_app_name_in_connection_string_raises_error(self, conn_str):
        """Test that APP in connection string raises ConnectionStringParseError."""
        # Connection strings with APP parameter should now raise an error (not silently filter)
        
        # Try to add APP to connection string
        test_conn_str = conn_str + ";APP=UserDefinedApp"
        
        # Should raise ConnectionStringParseError
        with pytest.raises(ConnectionStringParseError) as exc_info:
            connect(test_conn_str)
        
        error_lower = str(exc_info.value).lower()
        assert "reserved keyword" in error_lower
        assert "'app'" in error_lower
        assert "controlled by the driver" in error_lower
        
        print("\n✓ APP in connection string correctly raised ConnectionStringParseError")
    
    @pytest.mark.skipif(not os.getenv('DB_CONNECTION_STRING'), 
                        reason="Requires database connection string")
    def test_app_name_in_kwargs_rejected_before_sql_server(self, conn_str):
        """Test that APP in kwargs raises ValueError before even attempting to connect to SQL Server."""
        # Unlike connection strings (which are silently filtered), kwargs with APP should raise an error
        # This prevents the connection attempt entirely
        
        with pytest.raises(ValueError) as exc_info:
            connect(conn_str, APP="UserDefinedApp")
        
        assert "reserved and controlled by the driver" in str(exc_info.value)
        assert "APP" in str(exc_info.value) or "app" in str(exc_info.value).lower()
        
        print("\n✓ APP in kwargs correctly raised ValueError before connecting to SQL Server")






