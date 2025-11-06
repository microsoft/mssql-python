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
import contextvars
from typing import Optional


# Single DEBUG level - all or nothing philosophy
# If you need logging, you need to see everything
DEBUG = logging.DEBUG        # 10

# Output destination constants
STDOUT = 'stdout'  # Log to stdout only
FILE = 'file'      # Log to file only (default)
BOTH = 'both'      # Log to both file and stdout

# Module-level context variable for trace IDs (thread-safe, async-safe)
_trace_id_var = contextvars.ContextVar('trace_id', default=None)


class TraceIDFilter(logging.Filter):
    """Filter that adds trace_id to all log records."""
    
    def filter(self, record):
        """Add trace_id attribute to log record."""
        trace_id = _trace_id_var.get()
        record.trace_id = trace_id if trace_id else '-'
        return True




class MSSQLLogger:
    """
    Singleton logger for mssql_python with single DEBUG level.
    
    Philosophy: All or nothing - if you enable logging, you see EVERYTHING.
    Logging is a troubleshooting tool, not a production feature.
    
    Features:
    - Single DEBUG level (no categorization)
    - Automatic file rotation (512MB, 5 backups)
    - Password sanitization
    - Trace ID support with contextvars (automatic propagation)
    - Thread-safe operation
    - Zero overhead when disabled (level check only)
    
    ⚠️ Performance Warning: Logging adds ~2-5% overhead. Only enable when troubleshooting.
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
        
        # Add trace ID filter (injects trace_id into every log record)
        self._logger.addFilter(TraceIDFilter())
        
        # Trace ID counter (thread-safe)
        self._trace_counter = 0
        self._trace_lock = threading.Lock()
        
        # Output mode and handlers
        self._output_mode = FILE  # Default to file only
        self._file_handler = None
        self._stdout_handler = None
        self._log_file = None
        self._custom_log_path = None  # Custom log file path (if specified)
        self._handlers_initialized = False
        
        # Don't setup handlers yet - do it lazily when setLevel is called
        # This prevents creating log files when user changes output mode before enabling logging
    
    def _setup_handlers(self):
        """
        Setup handlers based on output mode.
        Creates file handler and/or stdout handler as needed.
        """
        # Clear any existing handlers
        if self._logger.handlers:
            for handler in self._logger.handlers[:]:
                handler.close()
                self._logger.removeHandler(handler)
        
        self._file_handler = None
        self._stdout_handler = None
        
        # Create formatter (same for all handlers)
        formatter = logging.Formatter(
            '%(asctime)s [%(trace_id)s] - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
        )
        
        # Setup file handler if needed
        if self._output_mode in (FILE, BOTH):
            # Use custom path or auto-generate
            if self._custom_log_path:
                self._log_file = self._custom_log_path
                # Ensure directory exists for custom path
                log_dir = os.path.dirname(self._custom_log_path)
                if log_dir and not os.path.exists(log_dir):
                    os.makedirs(log_dir, exist_ok=True)
            else:
                # Create log file in mssql_python_logs folder
                log_dir = os.path.join(os.getcwd(), "mssql_python_logs")
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir, exist_ok=True)
                
                timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                pid = os.getpid()
                self._log_file = os.path.join(
                    log_dir,
                    f"mssql_python_trace_{timestamp}_{pid}.log"
                )
            
            # Create rotating file handler (512MB, 5 backups)
            self._file_handler = RotatingFileHandler(
                self._log_file,
                maxBytes=512 * 1024 * 1024,  # 512MB
                backupCount=5
            )
            self._file_handler.setFormatter(formatter)
            self._logger.addHandler(self._file_handler)
        else:
            # No file logging - clear the log file path
            self._log_file = None
        
        # Setup stdout handler if needed
        if self._output_mode in (STDOUT, BOTH):
            import sys
            self._stdout_handler = logging.StreamHandler(sys.stdout)
            self._stdout_handler.setFormatter(formatter)
            self._logger.addHandler(self._stdout_handler)
    
    def _reconfigure_handlers(self):
        """
        Reconfigure handlers when output mode changes.
        Closes existing handlers and creates new ones based on current output mode.
        """
        self._setup_handlers()
    
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
    
    def generate_trace_id(self, prefix: str = "TRACE") -> str:
        """
        Generate a unique trace ID for correlating log messages.
        
        Format: PREFIX-PID-ThreadID-Counter
        Examples: 
            CONN-12345-67890-1
            CURS-12345-67890-2
        
        Args:
            prefix: Prefix for the trace ID (e.g., "CONN", "CURS", "TRACE")
            
        Returns:
            str: Unique trace ID in format PREFIX-PID-ThreadID-Counter
        """
        with self._trace_lock:
            self._trace_counter += 1
            counter = self._trace_counter
        
        pid = os.getpid()
        thread_id = threading.get_ident()
        
        return f"{prefix}-{pid}-{thread_id}-{counter}"
    
    def set_trace_id(self, trace_id: str):
        """
        Set the trace ID for the current context.
        
        This uses contextvars, so the trace ID automatically propagates to:
        - Child threads created within this context
        - Async tasks spawned from this context
        - All log calls made within this context
        
        Args:
            trace_id: Trace ID to set (typically from generate_trace_id())
        
        Example:
            trace_id = logger.generate_trace_id("CONN")
            logger.set_trace_id(trace_id)
            logger.debug("Connection opened")  # Includes trace ID automatically
        """
        _trace_id_var.set(trace_id)
    
    def get_trace_id(self) -> Optional[str]:
        """
        Get the trace ID for the current context.
        
        Returns:
            str or None: Current trace ID, or None if not set
        """
        return _trace_id_var.get()
    
    def clear_trace_id(self):
        """
        Clear the trace ID for the current context.
        
        Typically called when closing a connection/cursor to avoid
        trace ID leaking to subsequent operations.
        """
        _trace_id_var.set(None)
    
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
        
        # Format message with args if provided
        if args:
            msg = msg % args
        
        # Sanitize message
        sanitized_msg = self._sanitize_message(msg)
        
        # Log the message (no args since already formatted)
        self._logger.log(level, sanitized_msg, **kwargs)
    
    # Convenience methods for logging
    
    def debug(self, msg: str, *args, **kwargs):
        """Log at DEBUG level (all diagnostic messages)"""
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
    
    def _setLevel(self, level: int, output: Optional[str] = None, log_file_path: Optional[str] = None):
        """
        Internal method to set logging level (use setup_logging() instead).
        
        Args:
            level: Logging level (typically DEBUG)
            output: Optional output mode (FILE, STDOUT, BOTH)
            log_file_path: Optional custom path for log file
        
        Raises:
            ValueError: If output mode is invalid
        """
        # Validate and set output mode if specified
        if output is not None:
            if output not in (FILE, STDOUT, BOTH):
                raise ValueError(
                    f"Invalid output mode: {output}. "
                    f"Must be one of: {FILE}, {STDOUT}, {BOTH}"
                )
            self._output_mode = output
        
        # Store custom log file path if provided
        if log_file_path is not None:
            self._custom_log_path = log_file_path
        
        # Setup handlers if not yet initialized or if output mode/path changed
        if not self._handlers_initialized or output is not None or log_file_path is not None:
            self._setup_handlers()
            self._handlers_initialized = True
        
        # Set level
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
        Reset/recreate handlers.
        Useful when log file has been deleted or needs to be recreated.
        """
        self._setup_handlers()
    
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
    def output(self) -> str:
        """Get the current output mode"""
        return self._output_mode
    
    @output.setter
    def output(self, mode: str):
        """
        Set the output mode.
        
        Args:
            mode: Output mode (FILE, STDOUT, or BOTH)
        
        Raises:
            ValueError: If mode is not a valid OutputMode value
        """
        if mode not in (FILE, STDOUT, BOTH):
            raise ValueError(
                f"Invalid output mode: {mode}. "
                f"Must be one of: {FILE}, {STDOUT}, {BOTH}"
            )
        self._output_mode = mode
        
        # Only reconfigure if handlers were already initialized
        if self._handlers_initialized:
            self._reconfigure_handlers()
    
    @property
    def log_file(self) -> Optional[str]:
        """Get the current log file path (None if file output is disabled)"""
        return self._log_file
    
    @property
    def level(self) -> int:
        """Get the current logging level"""
        return self._logger.level


# ============================================================================
# Module-level exports (Primary API)
# ============================================================================

# Singleton logger instance
logger = MSSQLLogger()

# ============================================================================
# Primary API - setup_logging()
# ============================================================================

def setup_logging(output: str = 'file', log_file_path: Optional[str] = None):
    """
    Enable DEBUG logging for troubleshooting.
    
    ⚠️ PERFORMANCE WARNING: Logging adds ~2-5% overhead.
    Only enable when investigating issues. Do NOT enable in production without reason.
    
    Philosophy: All or nothing - if you need logging, you need to see EVERYTHING.
    Logging is a troubleshooting tool, not a production monitoring solution.
    
    Args:
        output: Where to send logs (default: 'file')
                Options: 'file', 'stdout', 'both'
        log_file_path: Optional custom path for log file
                      If not specified, auto-generates in ./mssql_python_logs/
    
    Examples:
        import mssql_python
        
        # File only (default, in mssql_python_logs folder)
        mssql_python.setup_logging()
        
        # Stdout only (for CI/CD)
        mssql_python.setup_logging(output='stdout')
        
        # Both file and stdout (for development)
        mssql_python.setup_logging(output='both')
        
        # Custom log file path
        mssql_python.setup_logging(log_file_path="/var/log/myapp.log")
        
        # Custom path with both outputs
        mssql_python.setup_logging(output='both', log_file_path="/tmp/debug.log")
    
    Future Enhancement:
        For performance analysis, use the universal profiler (coming soon)
        instead of logging. Logging is not designed for performance measurement.
    """
    logger._setLevel(logging.DEBUG, output, log_file_path)
    return logger
