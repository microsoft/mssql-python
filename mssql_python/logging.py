"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.

Enhanced logging module for mssql_python with JDBC-style logging levels.
This module provides fine-grained logging control with zero overhead when disabled.
"""

import logging
from logging.handlers import RotatingFileHandler
import os
import sys
import threading
import datetime
import re
import platform
import atexit
from typing import Optional


# Single DEBUG level - all or nothing philosophy
# If you need logging, you need to see everything
DEBUG = logging.DEBUG  # 10

# Output destination constants
STDOUT = "stdout"  # Log to stdout only
FILE = "file"  # Log to file only (default)
BOTH = "both"  # Log to both file and stdout

# Allowed log file extensions
ALLOWED_LOG_EXTENSIONS = {".txt", ".log", ".csv"}


class ThreadIDFilter(logging.Filter):
    """Filter that adds thread_id to all log records."""

    def filter(self, record):
        """Add thread_id (OS native) attribute to log record."""
        # Use OS native thread ID for debugging compatibility
        try:
            thread_id = threading.get_native_id()
        except AttributeError:
            # Fallback for Python < 3.8
            thread_id = threading.current_thread().ident
        record.thread_id = thread_id
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

    _instance: Optional["MSSQLLogger"] = None
    _lock = threading.Lock()
    _init_lock = threading.Lock()  # Separate lock for initialization

    def __new__(cls) -> "MSSQLLogger":
        """Ensure singleton pattern"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(MSSQLLogger, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        """Initialize the logger (only once) - thread-safe"""
        # Use separate lock for initialization check to prevent race condition
        # This ensures hasattr check and assignment are atomic
        with self._init_lock:
            # Skip if already initialized
            if hasattr(self, "_initialized"):
                return

            self._initialized = True

        # Create the underlying Python logger
        self._logger = logging.getLogger("mssql_python")
        self._logger.setLevel(logging.CRITICAL)  # Disabled by default
        self._logger.propagate = False  # Don't propagate to root logger

        # Add trace ID filter (injects thread_id into every log record)
        self._logger.addFilter(ThreadIDFilter())

        # Output mode and handlers
        self._output_mode = FILE  # Default to file only
        self._file_handler = None
        self._stdout_handler = None
        self._log_file = None
        self._custom_log_path = None  # Custom log file path (if specified)
        self._handlers_initialized = False
        self._handler_lock = threading.RLock()  # Reentrant lock for handler operations
        self._cleanup_registered = False  # Track if atexit cleanup is registered

        # Don't setup handlers yet - do it lazily when setLevel is called
        # This prevents creating log files when user changes output mode before enabling logging

    def _setup_handlers(self):
        """
        Setup handlers based on output mode.
        Creates file handler and/or stdout handler as needed.
        Thread-safe: Protects against concurrent handler removal during logging.
        """
        # Lock prevents race condition where one thread logs while another removes handlers
        with self._handler_lock:
            # Acquire locks on all existing handlers before closing
            # This ensures no thread is mid-write when we close
            old_handlers = self._logger.handlers[:]
            for handler in old_handlers:
                handler.acquire()

            try:
                # Flush and close each handler while holding its lock
                for handler in old_handlers:
                    try:
                        handler.flush()  # Flush BEFORE close
                    except:
                        pass  # Ignore flush errors
                    handler.close()
                    self._logger.removeHandler(handler)
            finally:
                # Release locks on old handlers
                for handler in old_handlers:
                    try:
                        handler.release()
                    except:
                        pass  # Handler might already be closed

            self._file_handler = None
            self._stdout_handler = None

        # Create CSV formatter
        # Custom formatter to extract source from message and format as CSV
        class CSVFormatter(logging.Formatter):
            def format(self, record):
                # Extract source from message (e.g., [Python] or [DDBC])
                msg = record.getMessage()
                if msg.startswith("[") and "]" in msg:
                    end_bracket = msg.index("]")
                    source = msg[1:end_bracket]
                    message = msg[end_bracket + 2 :].strip()  # Skip '] '
                else:
                    source = "Unknown"
                    message = msg

                # Format timestamp with milliseconds using period separator
                timestamp = self.formatTime(record, "%Y-%m-%d %H:%M:%S")
                timestamp_with_ms = f"{timestamp}.{int(record.msecs):03d}"

                # Get thread ID
                thread_id = getattr(record, "thread_id", 0)

                # Build CSV row
                location = f"{record.filename}:{record.lineno}"
                csv_row = f"{timestamp_with_ms}, {thread_id}, {record.levelname}, {location}, {source}, {message}"

                return csv_row

        formatter = CSVFormatter()

        # Override format to use milliseconds with period separator
        formatter.default_msec_format = "%s.%03d"

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

                timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
                pid = os.getpid()
                self._log_file = os.path.join(log_dir, f"mssql_python_trace_{timestamp}_{pid}.log")

            # Create rotating file handler (512MB, 5 backups)
            # Use UTF-8 encoding for unicode support on all platforms
            self._file_handler = RotatingFileHandler(
                self._log_file, maxBytes=512 * 1024 * 1024, backupCount=5, encoding="utf-8"  # 512MB
            )
            self._file_handler.setFormatter(formatter)
            self._logger.addHandler(self._file_handler)

            # Write CSV header to new log file
            self._write_log_header()
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

    def _cleanup_handlers(self):
        """
        Cleanup all handlers on process exit.
        Registered with atexit to ensure proper file handle cleanup.

        Thread-safe: Protects against concurrent logging during cleanup.

        Note on RotatingFileHandler:
            - File rotation (at 512MB) is already thread-safe
            - doRollover() is called within emit() which holds handler.lock
            - No additional synchronization needed for rotation
        """
        with self._handler_lock:
            handlers = self._logger.handlers[:]
            for handler in handlers:
                handler.acquire()

            try:
                for handler in handlers:
                    try:
                        handler.flush()
                        handler.close()
                    except:
                        pass  # Ignore errors during cleanup
                    self._logger.removeHandler(handler)
            finally:
                for handler in handlers:
                    try:
                        handler.release()
                    except:
                        pass

    def _validate_log_file_extension(self, file_path: str) -> None:
        """
        Validate that the log file has an allowed extension.

        Args:
            file_path: Path to the log file

        Raises:
            ValueError: If the file extension is not allowed
        """
        _, ext = os.path.splitext(file_path)
        ext_lower = ext.lower()

        if ext_lower not in ALLOWED_LOG_EXTENSIONS:
            allowed = ", ".join(sorted(ALLOWED_LOG_EXTENSIONS))
            raise ValueError(
                f"Invalid log file extension '{ext}'. " f"Allowed extensions: {allowed}"
            )

    def _write_log_header(self):
        """
        Write CSV header and metadata to the log file.
        Called once when log file is created.
        """
        if not self._log_file or not self._file_handler:
            return

        try:
            # Get script name from sys.argv or __main__
            script_name = os.path.basename(sys.argv[0]) if sys.argv else "<interactive>"

            # Get Python version
            python_version = platform.python_version()

            # Get driver version (try to import from package)
            try:
                from mssql_python import __version__

                driver_version = __version__
            except:
                driver_version = "unknown"

            # Get current time
            start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Get PID
            pid = os.getpid()

            # Get OS info
            os_info = platform.platform()

            # Build header comment line
            header_line = f"# MSSQL-Python Driver Log | Script: {script_name} | PID: {pid} | Log Level: DEBUG | Python: {python_version} | Driver: {driver_version} | Start: {start_time} | OS: {os_info}\n"

            # CSV column headers
            csv_header = "Timestamp, ThreadID, Level, Location, Source, Message\n"

            # Write directly to file (bypass formatter)
            with open(self._log_file, "a") as f:
                f.write(header_line)
                f.write(csv_header)

        except Exception as e:
            # Notify on stderr so user knows why header is missing
            try:
                sys.stderr.write(
                    f"[MSSQL-Python] Warning: Failed to write log header to {self._log_file}: {type(e).__name__}\n"
                )
                sys.stderr.flush()
            except:
                pass  # Even stderr notification failed
            # Don't crash - logging continues without header

    def _log(self, level: int, msg: str, add_prefix: bool = True, *args, **kwargs):
        """
        Internal logging method with exception safety.

        Args:
            level: Log level (DEBUG, INFO, WARNING, ERROR)
            msg: Message format string
            add_prefix: Whether to add [Python] prefix (default True)
            *args: Arguments for message formatting
            **kwargs: Additional keyword arguments

        Note:
            Callers are responsible for sanitizing sensitive data (passwords,
            tokens, etc.) before logging. Use helpers.sanitize_connection_string()
            for connection strings.

        Exception Safety:
            NEVER crashes the application. Catches all exceptions:
            - TypeError/ValueError: Bad format string or args
            - IOError/OSError: Disk full, permission denied
            - UnicodeEncodeError: Encoding issues

            On critical failures (ERROR level), attempts stderr fallback.
            All other failures are silently ignored to prevent app crashes.
        """
        try:
            # Fast level check (zero overhead if disabled)
            if not self._logger.isEnabledFor(level):
                return

            # Add prefix if requested (only after level check)
            if add_prefix:
                msg = f"[Python] {msg}"

            # Format message with args if provided
            if args:
                msg = msg % args

            # Log the message (no args since already formatted)
            self._logger.log(level, msg, **kwargs)
        except Exception:
            # Last resort: Try stderr fallback for any logging failure
            # This helps diagnose critical issues (disk full, permission denied, etc.)
            try:
                import sys

                level_name = logging.getLevelName(level)
                sys.stderr.write(
                    f"[MSSQL-Python Logging Failed - {level_name}] {msg if 'msg' in locals() else 'Unable to format message'}\n"
                )
                sys.stderr.flush()
            except:
                pass  # Even stderr failed - give up silently

    # Convenience methods for logging

    def debug(self, msg: str, *args, **kwargs):
        """Log at DEBUG level (all diagnostic messages)"""
        self._log(logging.DEBUG, msg, True, *args, **kwargs)

    def info(self, msg: str, *args, **kwargs):
        """Log at INFO level"""
        self._log(logging.INFO, msg, True, *args, **kwargs)

    def warning(self, msg: str, *args, **kwargs):
        """Log at WARNING level"""
        self._log(logging.WARNING, msg, True, *args, **kwargs)

    def error(self, msg: str, *args, **kwargs):
        """Log at ERROR level"""
        self._log(logging.ERROR, msg, True, *args, **kwargs)

    # Level control

    def _setLevel(
        self, level: int, output: Optional[str] = None, log_file_path: Optional[str] = None
    ):
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
                    f"Invalid output mode: {output}. " f"Must be one of: {FILE}, {STDOUT}, {BOTH}"
                )
            self._output_mode = output

        # Store custom log file path if provided
        if log_file_path is not None:
            self._validate_log_file_extension(log_file_path)
            self._custom_log_path = log_file_path

        # Setup handlers if not yet initialized or if output mode/path changed
        # Handler setup is protected by _handler_lock inside _setup_handlers()
        if not self._handlers_initialized or output is not None or log_file_path is not None:
            self._setup_handlers()
            self._handlers_initialized = True

            # Register atexit cleanup on first handler setup
            if not self._cleanup_registered:
                atexit.register(self._cleanup_handlers)
                self._cleanup_registered = True

        # Set level (atomic operation, no lock needed)
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
        """Add a handler to the logger (thread-safe)"""
        with self._handler_lock:
            self._logger.addHandler(handler)

    def removeHandler(self, handler: logging.Handler):
        """Remove a handler from the logger (thread-safe)"""
        with self._handler_lock:
            self._logger.removeHandler(handler)

    @property
    def handlers(self) -> list:
        """Get list of handlers attached to the logger (thread-safe)"""
        with self._handler_lock:
            return self._logger.handlers[:]  # Return copy to prevent external modification

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

            if hasattr(ddbc_bindings, "update_log_level"):
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
                f"Invalid output mode: {mode}. " f"Must be one of: {FILE}, {STDOUT}, {BOTH}"
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

# Expose the underlying Python logger for use in application code
# This allows applications to access the same logger used by the driver
# Usage: from mssql_python.logging import driver_logger
driver_logger = logger._logger

# ============================================================================
# Primary API - setup_logging()
# ============================================================================


def setup_logging(output: str = "file", log_file_path: Optional[str] = None):
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
                      Must have extension: .txt, .log, or .csv
                      If not specified, auto-generates in ./mssql_python_logs/

    Examples:
        import mssql_python

        # File only (default, in mssql_python_logs folder)
        mssql_python.setup_logging()

        # Stdout only (for CI/CD)
        mssql_python.setup_logging(output='stdout')

        # Both file and stdout (for development)
        mssql_python.setup_logging(output='both')

        # Custom log file path (must use .txt, .log, or .csv extension)
        mssql_python.setup_logging(log_file_path="/var/log/myapp.log")
        mssql_python.setup_logging(log_file_path="/tmp/debug.txt")
        mssql_python.setup_logging(log_file_path="/tmp/data.csv")

        # Custom path with both outputs
        mssql_python.setup_logging(output='both', log_file_path="/tmp/debug.log")

    Future Enhancement:
        For performance analysis, use the universal profiler (coming soon)
        instead of logging. Logging is not designed for performance measurement.
    """
    logger._setLevel(logging.DEBUG, output, log_file_path)
    return logger
