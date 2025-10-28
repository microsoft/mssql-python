"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.

Unit tests for ConnectionStringAllowList.
"""

import pytest
from mssql_python.connection_string_allowlist import ConnectionStringAllowList


class TestConnectionStringAllowList:
    """Unit tests for ConnectionStringAllowList."""
    
    def test_normalize_key_server(self):
        """Test normalization of 'server' and its synonyms."""
        assert ConnectionStringAllowList.normalize_key('server') == 'Server'
        assert ConnectionStringAllowList.normalize_key('SERVER') == 'Server'
        assert ConnectionStringAllowList.normalize_key('Server') == 'Server'
        assert ConnectionStringAllowList.normalize_key('address') == 'Server'
        assert ConnectionStringAllowList.normalize_key('addr') == 'Server'
        assert ConnectionStringAllowList.normalize_key('network address') == 'Server'
    
    def test_normalize_key_authentication(self):
        """Test normalization of authentication parameters."""
        assert ConnectionStringAllowList.normalize_key('uid') == 'Uid'
        assert ConnectionStringAllowList.normalize_key('user id') == 'Uid'
        assert ConnectionStringAllowList.normalize_key('user') == 'Uid'
        assert ConnectionStringAllowList.normalize_key('pwd') == 'Pwd'
        assert ConnectionStringAllowList.normalize_key('password') == 'Pwd'
    
    def test_normalize_key_database(self):
        """Test normalization of database parameters."""
        assert ConnectionStringAllowList.normalize_key('database') == 'Database'
        assert ConnectionStringAllowList.normalize_key('initial catalog') == 'Database'
    
    def test_normalize_key_encryption(self):
        """Test normalization of encryption parameters."""
        assert ConnectionStringAllowList.normalize_key('encrypt') == 'Encrypt'
        assert ConnectionStringAllowList.normalize_key('trustservercertificate') == 'TrustServerCertificate'
        assert ConnectionStringAllowList.normalize_key('trust server certificate') == 'TrustServerCertificate'
    
    def test_normalize_key_timeout(self):
        """Test normalization of timeout parameters."""
        assert ConnectionStringAllowList.normalize_key('connection timeout') == 'Connection Timeout'
        assert ConnectionStringAllowList.normalize_key('connect timeout') == 'Connection Timeout'
        assert ConnectionStringAllowList.normalize_key('timeout') == 'Connection Timeout'
        assert ConnectionStringAllowList.normalize_key('login timeout') == 'Login Timeout'
    
    def test_normalize_key_mars(self):
        """Test that MARS parameters are not in the allowlist."""
        assert ConnectionStringAllowList.normalize_key('mars_connection') is None
        assert ConnectionStringAllowList.normalize_key('mars connection') is None
        assert ConnectionStringAllowList.normalize_key('multipleactiveresultsets') is None
    
    def test_normalize_key_app(self):
        """Test normalization of APP parameters."""
        assert ConnectionStringAllowList.normalize_key('app') == 'APP'
        assert ConnectionStringAllowList.normalize_key('application name') == 'APP'
    
    def test_normalize_key_driver(self):
        """Test normalization of Driver parameter."""
        assert ConnectionStringAllowList.normalize_key('driver') == 'Driver'
        assert ConnectionStringAllowList.normalize_key('DRIVER') == 'Driver'
    
    def test_normalize_key_not_allowed(self):
        """Test normalization of disallowed keys returns None."""
        assert ConnectionStringAllowList.normalize_key('BadParam') is None
        assert ConnectionStringAllowList.normalize_key('UnsupportedParameter') is None
        assert ConnectionStringAllowList.normalize_key('RandomKey') is None
    
    def test_normalize_key_whitespace(self):
        """Test normalization handles whitespace."""
        assert ConnectionStringAllowList.normalize_key('  server  ') == 'Server'
        assert ConnectionStringAllowList.normalize_key(' uid ') == 'Uid'
    
    def test_filter_params_allows_good_params(self):
        """Test filtering allows known parameters."""
        params = {'server': 'localhost', 'database': 'mydb', 'encrypt': 'yes'}
        filtered = ConnectionStringAllowList.filter_params(params, warn_rejected=False)
        assert 'Server' in filtered
        assert 'Database' in filtered
        assert 'Encrypt' in filtered
        assert filtered['Server'] == 'localhost'
        assert filtered['Database'] == 'mydb'
        assert filtered['Encrypt'] == 'yes'
    
    def test_filter_params_rejects_bad_params(self):
        """Test filtering rejects unknown parameters."""
        params = {'server': 'localhost', 'badparam': 'value', 'anotherbad': 'test'}
        filtered = ConnectionStringAllowList.filter_params(params, warn_rejected=False)
        assert 'Server' in filtered
        assert 'badparam' not in filtered
        assert 'anotherbad' not in filtered
    
    def test_filter_params_normalizes_keys(self):
        """Test filtering normalizes parameter keys."""
        params = {'server': 'localhost', 'uid': 'user', 'pwd': 'pass'}
        filtered = ConnectionStringAllowList.filter_params(params, warn_rejected=False)
        assert 'Server' in filtered
        assert 'Uid' in filtered
        assert 'Pwd' in filtered
        assert 'server' not in filtered  # Original key should not be present
    
    def test_filter_params_handles_synonyms(self):
        """Test filtering handles parameter synonyms correctly."""
        params = {
            'address': 'server1',
            'user': 'testuser',
            'initial catalog': 'testdb',
            'connection timeout': '30'
        }
        filtered = ConnectionStringAllowList.filter_params(params, warn_rejected=False)
        assert filtered['Server'] == 'server1'
        assert filtered['Uid'] == 'testuser'
        assert filtered['Database'] == 'testdb'
        assert filtered['Connection Timeout'] == '30'
    
    def test_filter_params_empty_dict(self):
        """Test filtering empty parameter dictionary."""
        filtered = ConnectionStringAllowList.filter_params({}, warn_rejected=False)
        assert filtered == {}
    
    def test_filter_params_removes_driver(self):
        """Test that Driver parameter is filtered out (controlled by driver)."""
        params = {'driver': '{Some Driver}', 'server': 'localhost'}
        filtered = ConnectionStringAllowList.filter_params(params, warn_rejected=False)
        assert 'Driver' not in filtered
        assert 'Server' in filtered
    
    def test_filter_params_removes_app(self):
        """Test that APP parameter is filtered out (controlled by driver)."""
        params = {'app': 'MyApp', 'server': 'localhost'}
        filtered = ConnectionStringAllowList.filter_params(params, warn_rejected=False)
        assert 'APP' not in filtered
        assert 'Server' in filtered
    
    def test_filter_params_mixed_case_keys(self):
        """Test filtering with mixed case keys."""
        params = {'SERVER': 'localhost', 'DataBase': 'mydb', 'EncRypt': 'yes'}
        filtered = ConnectionStringAllowList.filter_params(params, warn_rejected=False)
        assert 'Server' in filtered
        assert 'Database' in filtered
        assert 'Encrypt' in filtered
    
    def test_filter_params_preserves_values(self):
        """Test that filtering preserves original values unchanged."""
        params = {
            'server': 'localhost:1433',
            'database': 'MyDatabase',
            'pwd': 'P@ssw0rd!123'
        }
        filtered = ConnectionStringAllowList.filter_params(params, warn_rejected=False)
        assert filtered['Server'] == 'localhost:1433'
        assert filtered['Database'] == 'MyDatabase'
        assert filtered['Pwd'] == 'P@ssw0rd!123'
    
    def test_filter_params_application_intent(self):
        """Test filtering application intent parameters."""
        params = {'applicationintent': 'ReadOnly', 'application intent': 'ReadWrite'}
        filtered = ConnectionStringAllowList.filter_params(params, warn_rejected=False)
        # Last one wins (application intent → ReadWrite)
        assert filtered['ApplicationIntent'] == 'ReadWrite'
    
    def test_filter_params_failover_partner(self):
        """Test filtering failover partner parameters."""
        params = {'failover partner': 'backup.server.com'}
        filtered = ConnectionStringAllowList.filter_params(params, warn_rejected=False)
        assert filtered['Failover_Partner'] == 'backup.server.com'
    
    def test_filter_params_column_encryption(self):
        """Test that column encryption parameter is not in the allowlist."""
        params = {'columnencryption': 'Enabled', 'column encryption': 'Disabled'}
        filtered = ConnectionStringAllowList.filter_params(params, warn_rejected=False)
        # Column encryption is not in the allowlist, so it should be filtered out
        assert 'ColumnEncryption' not in filtered
        assert len(filtered) == 0
    
    def test_filter_params_multisubnetfailover(self):
        """Test filtering multi-subnet failover parameters."""
        params = {'multisubnetfailover': 'yes', 'multi subnet failover': 'no'}
        filtered = ConnectionStringAllowList.filter_params(params, warn_rejected=False)
        # Last one wins
        assert filtered['MultiSubnetFailover'] == 'no'
