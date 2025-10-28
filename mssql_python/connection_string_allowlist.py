"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.

Connection string parameter allow-list for mssql-python.

Manages allowed connection string parameters and handles parameter
normalization, synonym mapping, and filtering.
"""

from typing import Dict, Optional


class ConnectionStringAllowList:
    """
    Manages the allow-list of permitted connection string parameters.
    
    This class implements a deliberate allow-list approach to exposing
    connection string parameters, enabling:
    - Incremental ODBC parity while maintaining backward compatibility
    - Forward compatibility with future driver enhancements
    - Simplified API by normalizing parameter synonyms
    """
    
    # Core connection parameters with synonym mapping
    # Maps lowercase parameter names to their canonical form
    ALLOWED_PARAMS = {
        # Server identification
        'server': 'Server',
        'host': 'Server',  # Common synonym
        'address': 'Server',
        'addr': 'Server',
        'network address': 'Server',
        
        # Authentication
        'uid': 'Uid',
        'user id': 'Uid',
        'user': 'Uid',
        'pwd': 'Pwd',
        'password': 'Pwd',
        'authentication': 'Authentication',
        'trusted_connection': 'Trusted_Connection',
        
        # Database
        'database': 'Database',
        'initial catalog': 'Database',
        
        # Driver (always controlled by mssql-python)
        'driver': 'Driver',
        
        # Application name (always controlled by mssql-python)
        'app': 'APP',
        'application name': 'APP',
        
        # Encryption
        'encrypt': 'Encrypt',
        'trustservercertificate': 'TrustServerCertificate',
        'trust_server_certificate': 'TrustServerCertificate',  # Python-style underscore synonym
        'trust server certificate': 'TrustServerCertificate',
        'hostnameincertificate': 'HostNameInCertificate',
        
        # Connection behavior
        'connection timeout': 'Connection Timeout',
        'connect timeout': 'Connection Timeout',
        'timeout': 'Connection Timeout',
        'login timeout': 'Login Timeout',
        'multisubnetfailover': 'MultiSubnetFailover',
        'multi subnet failover': 'MultiSubnetFailover',
        'applicationintent': 'ApplicationIntent',
        'application intent': 'ApplicationIntent',
        
        # Failover
        'failover partner': 'Failover_Partner',
        'failoverpartner': 'Failover_Partner',
        
        # Packet size
        'packet size': 'Packet Size',
        'packetsize': 'Packet Size',
    }
    
    @classmethod
    def normalize_key(cls, key: str) -> Optional[str]:
        """
        Normalize a parameter key to its canonical form.
        
        Args:
            key: Parameter key from connection string (case-insensitive)
            
        Returns:
            Canonical parameter name if allowed, None otherwise
            
        Examples:
            >>> ConnectionStringAllowList.normalize_key('SERVER')
            'Server'
            >>> ConnectionStringAllowList.normalize_key('user')
            'Uid'
            >>> ConnectionStringAllowList.normalize_key('UnsupportedParam')
            None
        """
        key_lower = key.lower().strip()
        return cls.ALLOWED_PARAMS.get(key_lower)
    
    @classmethod
    def filter_params(cls, params: Dict[str, str], warn_rejected: bool = True) -> Dict[str, str]:
        """
        Filter parameters against the allow-list.
        
        Args:
            params: Dictionary of connection string parameters (keys should be lowercase)
            warn_rejected: Whether to log warnings for rejected parameters
            
        Returns:
            Dictionary containing only allowed parameters with normalized keys
            
        Note:
            Driver and APP parameters are filtered here but will be set by
            the driver in _construct_connection_string to maintain control.
        """
        # Import here to avoid circular dependency issues
        try:
            from mssql_python.logging_config import get_logger
            from mssql_python.helpers import sanitize_user_input
            logger = get_logger()
        except ImportError:
            logger = None
            sanitize_user_input = lambda x: str(x)[:50]  # Simple fallback
        
        filtered = {}
        rejected = []
        
        for key, value in params.items():
            normalized_key = cls.normalize_key(key)
            
            if normalized_key:
                # Skip Driver and APP - these are controlled by the driver
                if normalized_key in ('Driver', 'APP'):
                    continue
                    
                # Parameter is allowed
                filtered[normalized_key] = value
            else:
                # Parameter is not in allow-list
                rejected.append(key)
                if warn_rejected and logger:
                    safe_key = sanitize_user_input(key)
                    logger.warning(
                        f"Connection string parameter '{safe_key}' is not in the allow-list and will be ignored"
                    )
        
        return filtered
