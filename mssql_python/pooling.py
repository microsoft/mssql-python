# mssql_python/pooling.py
from mssql_python import ddbc_bindings
import threading

class PoolingManager:
    _enabled = False
    _lock = threading.Lock()
    _config = {
        "max_size": 100,
        "idle_timeout": 600
    }

    @classmethod
    def enable(cls, max_size=100, idle_timeout=600):
        with cls._lock:
            if cls._enabled:
                return

            if max_size <= 0 or idle_timeout < 0:
                raise ValueError("Invalid pooling parameters")

            ddbc_bindings.enable_pooling(max_size, idle_timeout)
            cls._config["max_size"] = max_size
            cls._config["idle_timeout"] = idle_timeout
            cls._enabled = True

    @classmethod
    def is_enabled(cls):
        return cls._enabled
