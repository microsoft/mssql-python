"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.

Unit tests for _ConnectionStringAllowList.
"""

import pytest
from mssql_python.connection_string_allowlist import _ConnectionStringAllowList


class Test_ConnectionStringAllowList:
    """Unit tests for _ConnectionStringAllowList."""
    
    def test_normalize_key_server(self):
        """Test normalization of 'server' and related address parameters."""
        # server, address, and addr are all synonyms that map to 'Server'
        assert _ConnectionStringAllowList.normalize_key('server') == 'Server'
        assert _ConnectionStringAllowList.normalize_key('SERVER') == 'Server'
        assert _ConnectionStringAllowList.normalize_key('Server') == 'Server'
        assert _ConnectionStringAllowList.normalize_key('address') == 'Server'
        assert _ConnectionStringAllowList.normalize_key('ADDRESS') == 'Server'
        assert _ConnectionStringAllowList.normalize_key('addr') == 'Server'
        assert _ConnectionStringAllowList.normalize_key('ADDR') == 'Server'
    
    def test_normalize_key_authentication(self):
        """Test normalization of authentication parameters."""
        assert _ConnectionStringAllowList.normalize_key('uid') == 'UID'
        assert _ConnectionStringAllowList.normalize_key('UID') == 'UID'
        assert _ConnectionStringAllowList.normalize_key('pwd') == 'PWD'
        assert _ConnectionStringAllowList.normalize_key('PWD') == 'PWD'
        assert _ConnectionStringAllowList.normalize_key('authentication') == 'Authentication'
        assert _ConnectionStringAllowList.normalize_key('trusted_connection') == 'Trusted_Connection'
    
    def test_normalize_key_database(self):
        """Test normalization of database parameter."""
        assert _ConnectionStringAllowList.normalize_key('database') == 'Database'
        assert _ConnectionStringAllowList.normalize_key('DATABASE') == 'Database'
        # 'initial catalog' is not in the restricted allowlist
        assert _ConnectionStringAllowList.normalize_key('initial catalog') is None
    
    def test_normalize_key_encryption(self):
        """Test normalization of encryption parameters."""
        assert _ConnectionStringAllowList.normalize_key('encrypt') == 'Encrypt'
        assert _ConnectionStringAllowList.normalize_key('trustservercertificate') == 'TrustServerCertificate'
        assert _ConnectionStringAllowList.normalize_key('hostnameincertificate') == 'HostnameInCertificate'
        assert _ConnectionStringAllowList.normalize_key('servercertificate') == 'ServerCertificate'
    def test_normalize_key_connection_params(self):
        """Test normalization of connection behavior parameters."""
        assert _ConnectionStringAllowList.normalize_key('connectretrycount') == 'ConnectRetryCount'
        assert _ConnectionStringAllowList.normalize_key('connectretryinterval') == 'ConnectRetryInterval'
        assert _ConnectionStringAllowList.normalize_key('multisubnetfailover') == 'MultiSubnetFailover'
        assert _ConnectionStringAllowList.normalize_key('applicationintent') == 'ApplicationIntent'
        assert _ConnectionStringAllowList.normalize_key('keepalive') == 'KeepAlive'
        assert _ConnectionStringAllowList.normalize_key('keepaliveinterval') == 'KeepAliveInterval'
        assert _ConnectionStringAllowList.normalize_key('ipaddresspreference') == 'IpAddressPreference'
        # Timeout parameters not in restricted allowlist
        assert _ConnectionStringAllowList.normalize_key('connection timeout') is None
        assert _ConnectionStringAllowList.normalize_key('login timeout') is None
        assert _ConnectionStringAllowList.normalize_key('connect timeout') is None
        assert _ConnectionStringAllowList.normalize_key('timeout') is None
    
    def test_normalize_key_mars(self):
        """Test that MARS parameters are not in the allowlist."""
        assert _ConnectionStringAllowList.normalize_key('mars_connection') is None
        assert _ConnectionStringAllowList.normalize_key('mars connection') is None
        assert _ConnectionStringAllowList.normalize_key('multipleactiveresultsets') is None
    
    def test_normalize_key_app(self):
        """Test normalization of APP parameter."""
        assert _ConnectionStringAllowList.normalize_key('app') == 'APP'
        assert _ConnectionStringAllowList.normalize_key('APP') == 'APP'
        # 'application name' is not in restricted allowlist
        assert _ConnectionStringAllowList.normalize_key('application name') is None
    
    def test_normalize_key_driver(self):
        """Test normalization of Driver parameter."""
        assert _ConnectionStringAllowList.normalize_key('driver') == 'Driver'
        assert _ConnectionStringAllowList.normalize_key('DRIVER') == 'Driver'
    
    def test_normalize_key_not_allowed(self):
        """Test normalization of disallowed keys returns None."""
        assert _ConnectionStringAllowList.normalize_key('BadParam') is None
        assert _ConnectionStringAllowList.normalize_key('UnsupportedParameter') is None
        assert _ConnectionStringAllowList.normalize_key('RandomKey') is None
    
    def test_normalize_key_whitespace(self):
        """Test normalization handles whitespace."""
        assert _ConnectionStringAllowList.normalize_key('  server  ') == 'Server'
        assert _ConnectionStringAllowList.normalize_key(' uid ') == 'UID'
        assert _ConnectionStringAllowList.normalize_key('  database  ') == 'Database'
    
    def test__normalize_params_allows_good_params(self):
        """Test filtering allows known parameters."""
        params = {'server': 'localhost', 'database': 'mydb', 'encrypt': 'yes'}
        filtered = _ConnectionStringAllowList._normalize_params(params, warn_rejected=False)
        assert 'Server' in filtered
        assert 'Database' in filtered
        assert 'Encrypt' in filtered
        assert filtered['Server'] == 'localhost'
        assert filtered['Database'] == 'mydb'
        assert filtered['Encrypt'] == 'yes'
    
    def test__normalize_params_rejects_bad_params(self):
        """Test filtering rejects unknown parameters."""
        params = {'server': 'localhost', 'badparam': 'value', 'anotherbad': 'test'}
        filtered = _ConnectionStringAllowList._normalize_params(params, warn_rejected=False)
        assert 'Server' in filtered
        assert 'badparam' not in filtered
        assert 'anotherbad' not in filtered
    
    def test__normalize_params_normalizes_keys(self):
        """Test filtering normalizes parameter keys."""
        params = {'server': 'localhost', 'uid': 'user', 'pwd': 'pass'}
        filtered = _ConnectionStringAllowList._normalize_params(params, warn_rejected=False)
        assert 'Server' in filtered
        assert 'UID' in filtered
        assert 'PWD' in filtered
        assert 'server' not in filtered  # Original key should not be present
    
    def test__normalize_params_handles_address_variants(self):
        """Test filtering handles address/addr/server as synonyms."""
        params = {
            'address': 'addr1',
            'addr': 'addr2',
            'server': 'server1'
        }
        filtered = _ConnectionStringAllowList._normalize_params(params, warn_rejected=False)
        # All three are synonyms that map to 'Server', last one wins
        assert filtered['Server'] == 'server1'
        assert 'Address' not in filtered
        assert 'Addr' not in filtered
    
    def test__normalize_params_empty_dict(self):
        """Test filtering empty parameter dictionary."""
        filtered = _ConnectionStringAllowList._normalize_params({}, warn_rejected=False)
        assert filtered == {}
    
    def test__normalize_params_removes_driver(self):
        """Test that Driver parameter is filtered out (controlled by driver)."""
        params = {'driver': '{Some Driver}', 'server': 'localhost'}
        filtered = _ConnectionStringAllowList._normalize_params(params, warn_rejected=False)
        assert 'Driver' not in filtered
        assert 'Server' in filtered
    
    def test__normalize_params_removes_app(self):
        """Test that APP parameter is filtered out (controlled by driver)."""
        params = {'app': 'MyApp', 'server': 'localhost'}
        filtered = _ConnectionStringAllowList._normalize_params(params, warn_rejected=False)
        assert 'APP' not in filtered
        assert 'Server' in filtered
    
    def test__normalize_params_mixed_case_keys(self):
        """Test filtering with mixed case keys."""
        params = {'SERVER': 'localhost', 'DataBase': 'mydb', 'EncRypt': 'yes'}
        filtered = _ConnectionStringAllowList._normalize_params(params, warn_rejected=False)
        assert 'Server' in filtered
        assert 'Database' in filtered
        assert 'Encrypt' in filtered
    
    def test__normalize_params_preserves_values(self):
        """Test that filtering preserves original values unchanged."""
        params = {
            'server': 'localhost:1433',
            'database': 'MyDatabase',
            'pwd': 'P@ssw0rd!123'
        }
        filtered = _ConnectionStringAllowList._normalize_params(params, warn_rejected=False)
        assert filtered['Server'] == 'localhost:1433'
        assert filtered['Database'] == 'MyDatabase'
        assert filtered['PWD'] == 'P@ssw0rd!123'
    
    def test__normalize_params_application_intent(self):
        """Test filtering application intent parameters."""
        # Only 'applicationintent' (no spaces) is in the allowlist
        params = {'applicationintent': 'ReadOnly', 'application intent': 'ReadWrite'}
        filtered = _ConnectionStringAllowList._normalize_params(params, warn_rejected=False)
        # 'application intent' with space is rejected, only compact form accepted
        assert filtered['ApplicationIntent'] == 'ReadOnly'
        assert len(filtered) == 1
    
    def test__normalize_params_failover_partner(self):
        """Test that failover partner is not in the restricted allowlist."""
        params = {'failover partner': 'backup.server.com', 'failoverpartner': 'backup2.com'}
        filtered = _ConnectionStringAllowList._normalize_params(params, warn_rejected=False)
        # Failover_Partner is not in the restricted allowlist
        assert 'Failover_Partner' not in filtered
        assert 'FailoverPartner' not in filtered
        assert len(filtered) == 0
    
    def test__normalize_params_column_encryption(self):
        """Test that column encryption parameter is not in the allowlist."""
        params = {'columnencryption': 'Enabled', 'column encryption': 'Disabled'}
        filtered = _ConnectionStringAllowList._normalize_params(params, warn_rejected=False)
        # Column encryption is not in the allowlist, so it should be filtered out
        assert 'ColumnEncryption' not in filtered
        assert len(filtered) == 0
    
    def test__normalize_params_multisubnetfailover(self):
        """Test filtering multi-subnet failover parameters."""
        # Only 'multisubnetfailover' (no spaces) is in the allowlist
        params = {'multisubnetfailover': 'yes', 'multi subnet failover': 'no'}
        filtered = _ConnectionStringAllowList._normalize_params(params, warn_rejected=False)
        # 'multi subnet failover' with spaces is rejected
        assert filtered['MultiSubnetFailover'] == 'yes'
        assert len(filtered) == 1
