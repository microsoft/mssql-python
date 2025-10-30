"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.

Connection string parameter allow-list for mssql-python.

Manages allowed connection string parameters and handles parameter
normalization, synonym mapping, and filtering.
"""

from typing import Dict, Optional


# Import RESERVED_PARAMETERS from parser module to maintain single source of truth
def _get_reserved_parameters():
    """Lazy import to avoid circular dependency."""
    from mssql_python.connection_string_parser import RESERVED_PARAMETERS
    return RESERVED_PARAMETERS


class _ConnectionStringAllowList:
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
    # Based on ODBC Driver 18 for SQL Server supported parameters
    ALLOWED_PARAMS = {
        # Server identification - addr, address, and server are synonyms
        'server': 'Server',
        'address': 'Server',
        'addr': 'Server',
        
        # Authentication
        'uid': 'UID',
        'pwd': 'PWD',
        'authentication': 'Authentication',
        'trusted_connection': 'Trusted_Connection',
        
        # Database
        'database': 'Database',
        
        # Driver (always controlled by mssql-python)
        'driver': 'Driver',
        
        # Application name (always controlled by mssql-python)
        'app': 'APP',
        
        # Encryption and Security
        'encrypt': 'Encrypt',
        'trustservercertificate': 'TrustServerCertificate',
        'trust_server_certificate': 'TrustServerCertificate',  # Snake_case synonym
        'hostnameincertificate': 'HostnameInCertificate',  # v18.0+
        'servercertificate': 'ServerCertificate',  # v18.1+
        'serverspn': 'ServerSPN',
        
        # Connection behavior
        'multisubnetfailover': 'MultiSubnetFailover',
        'applicationintent': 'ApplicationIntent',
        'connectretrycount': 'ConnectRetryCount',
        'connectretryinterval': 'ConnectRetryInterval',
        
        # Keep-Alive (v17.4+)
        'keepalive': 'KeepAlive',
        'keepaliveinterval': 'KeepAliveInterval',
        
        # IP Address Preference (v18.1+)
        'ipaddresspreference': 'IpAddressPreference',

        'packet size': 'Packet Size',
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
            >>> _ConnectionStringAllowList.normalize_key('SERVER')
            'Server'
            >>> _ConnectionStringAllowList.normalize_key('user')
            'Uid'
            >>> _ConnectionStringAllowList.normalize_key('UnsupportedParam')
            None
        """
        key_lower = key.lower().strip()
        return cls.ALLOWED_PARAMS.get(key_lower)
    
    @classmethod
    def _normalize_params(cls, params: Dict[str, str], warn_rejected: bool = True) -> Dict[str, str]:
        """
        Normalize and filter parameters against the allow-list (internal use only).
        
        This method performs several operations:
        - Normalizes parameter names (e.g., addr/address → Server, uid → UID)
        - Filters out parameters not in the allow-list
        - Removes reserved parameters (Driver, APP)
        - Deduplicates via normalized keys
        
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

        # The rejected list should ideally be empty when used in the normal connection
        # flow, since the parser validates against the allowlist first and raises
        # errors for unknown parameters. This filtering is primarily a safety net.
        rejected = []
        
        reserved_params = _get_reserved_parameters()
        
        for key, value in params.items():
            normalized_key = cls.normalize_key(key)
            
            if normalized_key:
                # Skip Driver and APP - these are controlled by the driver
                if normalized_key in reserved_params:
                    continue
                    
                # Parameter is allowed
                filtered[normalized_key] = value
            else:
                # Parameter is not in allow-list
                # Note: In normal flow, this should be empty since parser validates first
                rejected.append(key)
        
        # Log all rejected parameters together if any were found
        if rejected and warn_rejected and logger:
            safe_keys = [sanitize_user_input(key) for key in rejected]
            logger.warning(
                f"Connection string parameters not in allow-list and will be ignored: {', '.join(safe_keys)}"
            )
        
        return filtered
