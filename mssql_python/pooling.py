# mssql_python/pooling.py
import atexit
from typing import Dict
from mssql_python import ddbc_bindings
import threading


class PoolingManager:
    _enabled: bool = False
    _initialized: bool = False
    _pools_closed: bool = False  # Track if pools have been closed
    _lock: threading.Lock = threading.Lock()
    _config: Dict[str, int] = {"max_size": 100, "idle_timeout": 600}

    @classmethod
    def enable(cls, max_size: int = 100, idle_timeout: int = 600) -> None:
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
        return cls._enabled

    @classmethod
    def is_initialized(cls) -> bool:
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
    with PoolingManager._lock:
        if PoolingManager._enabled and not PoolingManager._pools_closed:
            ddbc_bindings.close_pooling()
            PoolingManager._pools_closed = True
