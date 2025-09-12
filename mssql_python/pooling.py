"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
This module provides a connection pooling manager for efficient database connections.
"""
import atexit
import threading
from mssql_python import ddbc_bindings

class PoolingManager:
    """
    Manages connection pooling for the application.

    This class provides methods to enable, disable, and
    check the status of connection pooling.
    """
    _enabled = False
    _initialized = False
    _lock = threading.Lock()
    _config = {"max_size": 100, "idle_timeout": 600}

    @classmethod
    def enable(cls, max_size: int = 100, idle_timeout: int = 600) -> None:
        """
        Enable connection pooling with the specified parameters.

        Args:
            max_size (int): The maximum number of connections in the pool.
            idle_timeout (int): The idle timeout for connections in the pool (in seconds).
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
        Disable connection pooling.
        """
        with cls._lock:
            cls._enabled = False
            cls._initialized = True

    @classmethod
    def is_enabled(cls) -> bool:
        """
        Check if connection pooling is enabled.
        """
        return cls._enabled

    @classmethod
    def is_initialized(cls) -> bool:
        """
        Check if connection pooling is initialized.
        """
        return cls._initialized


@atexit.register
def shutdown_pooling():
    """
    Shutdown the connection pooling manager.
    """
    if PoolingManager.is_enabled():
        ddbc_bindings.close_pooling()
