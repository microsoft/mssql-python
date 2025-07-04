# mssql_python/pooling.py
import atexit
from mssql_python import ddbc_bindings
import threading

class PoolingManager:
    _enabled = False
    _initialized = False 
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
            cls._initialized = True

    @classmethod
    def disable(cls):
        with cls._lock:
            cls._enabled = False
            cls._initialized = True

    @classmethod
    def is_enabled(cls):
        return cls._enabled

    @classmethod
    def is_initialized(cls):
        return cls._initialized
    
@atexit.register
def shutdown_pooling():
    if PoolingManager.is_enabled():
        ddbc_bindings.close_pooling()
