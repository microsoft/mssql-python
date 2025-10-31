"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.

Enhanced logging module for mssql_python with JDBC-style logging levels.
This module provides fine-grained logging control with zero overhead when disabled.
"""

import logging
from logging.handlers import RotatingFileHandler
import os
import threading
import datetime
import re
from typing import Optional


# Define custom log levels (JDBC-style)
# In Python logging: LOWER number = MORE detailed, HIGHER number = LESS detailed
# JDBC hierarchy (most to least detailed): FINEST < FINER < FINE < INFO < WARNING < ERROR < CRITICAL
FINEST = 5   # Ultra-detailed trace (most detailed, below DEBUG=10)
FINER = 15   # Very detailed diagnostics (between DEBUG=10 and INFO=20)
FINE = 25    # General diagnostics (between INFO=20 and WARNING=30)

# Register custom level names
logging.addLevelName(FINEST, 'FINEST')
logging.addLevelName(FINER, 'FINER')
logging.addLevelName(FINE, 'FINE')


class MSSQLLogger:
    """
    Singleton logger for mssql_python with JDBC-style logging levels.
    
    Features:
    - Custom levels: FINE (25), FINER (15), FINEST (5)
    - Automatic file rotation (512MB, 5 backups)
    - Password sanitization
    - Trace ID generation (PID_ThreadID_Counter format)
    - Thread-safe operation
    - Zero overhead when disabled (level check only)
    """
    
    _instance: Optional['MSSQLLogger'] = None
    _lock = threading.Lock()
    
    def __new__(cls) -> 'MSSQLLogger':
        """Ensure singleton pattern"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(MSSQLLogger, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize the logger (only once)"""
        # Skip if already initialized
        if hasattr(self, '_initialized'):
            return
        
        self._initialized = True
        
        # Create the underlying Python logger
        self._logger = logging.getLogger('mssql_python')
        self._logger.setLevel(logging.CRITICAL)  # Disabled by default
        self._logger.propagate = False  # Don't propagate to root logger
        
        # Trace ID counter (thread-safe)
        self._trace_counter = 0
        self._trace_lock = threading.Lock()
        
        # Setup file handler
        self._log_file = self._setup_file_handler()
    
    def _setup_file_handler(self) -> str:
        """
        Setup rotating file handler for logging.
        
        Returns:
            str: Path to the log file
        """
        # Clear any existing handlers
        if self._logger.handlers:
            self._logger.handlers.clear()
        
        # Create log file in current working directory (not package directory)
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        pid = os.getpid()
        log_file = os.path.join(
            os.getcwd(),
            f"mssql_python_trace_{timestamp}_{pid}.log"
        )
        
        # Create rotating file handler (512MB, 5 backups)
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=512 * 1024 * 1024,  # 512MB
            backupCount=5
        )
        
        # Set formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
        )
        file_handler.setFormatter(formatter)
        
        # Add handler to logger
        self._logger.addHandler(file_handler)
        
        return log_file
    
    @staticmethod
    def _sanitize_message(msg: str) -> str:
        """
        Sanitize sensitive information from log messages.
        
        Removes:
        - PWD=...
        - Password=...
        - TOKEN=...
        - Authorization: Bearer ...
        
        Args:
            msg: The message to sanitize
            
        Returns:
            str: Sanitized message with credentials replaced by ***
        """
        # Pattern to match various credential formats
        patterns = [
            (r'(PWD|Password|pwd|password)\s*=\s*[^;,\s]+', r'\1=***'),
            (r'(TOKEN|Token|token)\s*=\s*[^;,\s]+', r'\1=***'),
            (r'(Authorization:\s*Bearer\s+)[^\s;,]+', r'\1***'),
            (r'(ApiKey|API_KEY|api_key)\s*=\s*[^;,\s]+', r'\1=***'),
        ]
        
        sanitized = msg
        for pattern, replacement in patterns:
            sanitized = re.sub(pattern, replacement, sanitized)
        
        return sanitized
    
    def generate_trace_id(self, prefix: str = "") -> str:
        """
        Generate a unique trace ID for correlating log messages.
        
        Format: PID_ThreadID_Counter or Prefix_PID_ThreadID_Counter
        Example: 12345_67890_1 or Connection_12345_67890_1
        
        Args:
            prefix: Optional prefix for the trace ID (e.g., "Connection", "Cursor")
            
        Returns:
            str: Unique trace ID
        """
        with self._trace_lock:
            self._trace_counter += 1
            counter = self._trace_counter
        
        pid = os.getpid()
        thread_id = threading.get_ident()
        
        if prefix:
            return f"{prefix}_{pid}_{thread_id}_{counter}"
        return f"{pid}_{thread_id}_{counter}"
    
    def _log(self, level: int, msg: str, *args, **kwargs):
        """
        Internal logging method with sanitization.
        
        Args:
            level: Log level (FINE, FINER, FINEST, etc.)
            msg: Message format string
            *args: Arguments for message formatting
            **kwargs: Additional keyword arguments
        """
        # Fast level check (zero overhead if disabled)
        if not self._logger.isEnabledFor(level):
            return
        
        # Sanitize message
        sanitized_msg = self._sanitize_message(msg)
        
        # Log the message
        self._logger.log(level, sanitized_msg, *args, **kwargs)
    
    # Convenience methods for each level
    
    def finest(self, msg: str, *args, **kwargs):
        """Log at FINEST level (most detailed)"""
        self._log(FINEST, f"[Python] {msg}", *args, **kwargs)
    
    def finer(self, msg: str, *args, **kwargs):
        """Log at FINER level (detailed)"""
        self._log(FINER, f"[Python] {msg}", *args, **kwargs)
    
    def fine(self, msg: str, *args, **kwargs):
        """Log at FINE level (standard diagnostics)"""
        self._log(FINE, f"[Python] {msg}", *args, **kwargs)
    
    def debug(self, msg: str, *args, **kwargs):
        """Log at DEBUG level (alias for compatibility)"""
        self._log(logging.DEBUG, f"[Python] {msg}", *args, **kwargs)
    
    def info(self, msg: str, *args, **kwargs):
        """Log at INFO level"""
        self._log(logging.INFO, f"[Python] {msg}", *args, **kwargs)
    
    def warning(self, msg: str, *args, **kwargs):
        """Log at WARNING level"""
        self._log(logging.WARNING, f"[Python] {msg}", *args, **kwargs)
    
    def error(self, msg: str, *args, **kwargs):
        """Log at ERROR level"""
        self._log(logging.ERROR, f"[Python] {msg}", *args, **kwargs)
    
    def critical(self, msg: str, *args, **kwargs):
        """Log at CRITICAL level"""
        self._log(logging.CRITICAL, f"[Python] {msg}", *args, **kwargs)
    
    def log(self, level: int, msg: str, *args, **kwargs):
        """Log a message at the specified level"""
        self._log(level, f"[Python] {msg}", *args, **kwargs)
    
    # Level control
    
    def setLevel(self, level: int):
        """
        Set the logging level.
        
        Args:
            level: Logging level (FINEST, FINER, FINE, logging.INFO, etc.)
                   Use logging.CRITICAL to disable all logging
        """
        self._logger.setLevel(level)
        
        # Notify C++ bridge of level change
        self._notify_cpp_level_change(level)
    
    def getLevel(self) -> int:
        """
        Get the current logging level.
        
        Returns:
            int: Current log level
        """
        return self._logger.level
    
    def isEnabledFor(self, level: int) -> bool:
        """
        Check if a given log level is enabled.
        
        Args:
            level: Log level to check
            
        Returns:
            bool: True if the level is enabled
        """
        return self._logger.isEnabledFor(level)
    
    # Handler management
    
    def addHandler(self, handler: logging.Handler):
        """Add a handler to the logger"""
        self._logger.addHandler(handler)
    
    def removeHandler(self, handler: logging.Handler):
        """Remove a handler from the logger"""
        self._logger.removeHandler(handler)
    
    @property
    def handlers(self) -> list:
        """Get list of handlers attached to the logger"""
        return self._logger.handlers
    
    def reset_handlers(self):
        """
        Reset/recreate file handler.
        Useful when log file has been deleted or needs to be recreated.
        """
        # Close existing handlers
        for handler in self._logger.handlers[:]:
            handler.close()
            self._logger.removeHandler(handler)
        
        # Recreate file handler
        self._log_file = self._setup_file_handler()
    
    def _notify_cpp_level_change(self, level: int):
        """
        Notify C++ bridge that log level has changed.
        This updates the cached level in C++ for fast checks.
        
        Args:
            level: New log level
        """
        try:
            # Import here to avoid circular dependency
            from . import ddbc_bindings
            if hasattr(ddbc_bindings, 'update_log_level'):
                ddbc_bindings.update_log_level(level)
        except (ImportError, AttributeError):
            # C++ bindings not available or not yet initialized
            pass
    
    # Properties
    
    @property
    def log_file(self) -> str:
        """Get the current log file path"""
        return self._log_file
    
    @property
    def level(self) -> int:
        """Get the current logging level"""
        return self._logger.level


# Create singleton instance
logger = MSSQLLogger()


# Backward compatibility function (deprecated)
def setup_logging(mode: str = 'file', log_level: int = logging.DEBUG):
    """
    DEPRECATED: Use logger.setLevel() instead.
    
    This function is provided for backward compatibility only.
    New code should use: logger.setLevel(FINE)
    
    Args:
        mode: Ignored (always logs to file)
        log_level: Logging level (maps to closest FINE/FINER/FINEST)
    """
    # Map old levels to new levels
    if log_level <= FINEST:
        logger.setLevel(FINEST)
    elif log_level <= FINER:
        logger.setLevel(FINER)
    elif log_level <= FINE:
        logger.setLevel(FINE)
    else:
        logger.setLevel(log_level)
    
    return logger


def get_logger():
    """
    DEPRECATED: Use 'from mssql_python.logging import logger' instead.
    
    Returns:
        MSSQLLogger: The logger instance
    """
    return logger
