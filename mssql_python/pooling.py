"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
This module provides connection pooling functionality for the mssql_python package.
"""
import atexit
import threading
from typing import Dict

from mssql_python import ddbc_bindings


class PoolingManager:
    """
    Manages connection pooling for the mssql_python package.
    
    This class provides thread-safe connection pooling functionality using the 
    underlying DDBC bindings. It follows a singleton pattern with class-level
    state management.
    """
    _enabled: bool = False
    _initialized: bool = False
    _pools_closed: bool = False  # Track if pools have been closed
    _lock: threading.Lock = threading.Lock()
    _config: Dict[str, int] = {"max_size": 100, "idle_timeout": 600}

    @classmethod
    def enable(cls, max_size: int = 100, idle_timeout: int = 600) -> None:
        """
        Enable connection pooling with specified parameters.
        
        Args:
            max_size: Maximum number of connections in the pool (default: 100)
            idle_timeout: Timeout in seconds for idle connections (default: 600)
            
        Raises:
            ValueError: If parameters are invalid (max_size <= 0 or idle_timeout < 0)
        """
        with cls._lock:
            if cls._enabled:
                return

            if max_size <= 0 or idle_timeout < 0:
                raise ValueError("Invalid pooling parameters")

            ddbc_bindings.enable_pooling(max_size, idle_timeout)
            cls._config["max_size"] = max_size
            cls._config["idle_timeout"] = idle_timeout
            cls._enabled = True
            cls._initialized = True

    @classmethod
    def disable(cls) -> None:
        """
        Disable connection pooling and clean up resources.
        
        This method safely disables pooling and closes existing connections.
        It can be called multiple times safely.
        """
        with cls._lock:
            if (
                cls._enabled and not cls._pools_closed
            ):  # Only cleanup if enabled and not already closed
                ddbc_bindings.close_pooling()
            cls._pools_closed = True
            cls._enabled = False
            cls._initialized = True

    @classmethod
    def is_enabled(cls) -> bool:
        """
        Check if connection pooling is currently enabled.
        
        Returns:
            bool: True if pooling is enabled, False otherwise
        """
        return cls._enabled

    @classmethod
    def is_initialized(cls) -> bool:
        """
        Check if the pooling manager has been initialized.
        
        Returns:
            bool: True if initialized (either enabled or disabled), False otherwise
        """
        return cls._initialized

    @classmethod
    def _reset_for_testing(cls) -> None:
        """Reset pooling state - for testing purposes only"""
        with cls._lock:
            cls._enabled = False
            cls._initialized = False
            cls._pools_closed = False


@atexit.register
def shutdown_pooling():
    """
    Shutdown pooling during application exit.
    
    This function is registered with atexit to ensure proper cleanup of
    connection pools when the application terminates.
    """
    with PoolingManager._lock:
        if PoolingManager._enabled and not PoolingManager._pools_closed:
            ddbc_bindings.close_pooling()
            PoolingManager._pools_closed = True
