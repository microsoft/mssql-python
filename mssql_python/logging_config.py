"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
This module provides logging configuration for the mssql_python package.
"""

import logging
from logging.handlers import RotatingFileHandler
import os
import sys
import datetime


class LoggingManager:
    """
    Singleton class to manage logging configuration for the mssql_python package.
    This class provides a centralized way to manage logging configuration and replaces
    the previous approach using global variables.
    """
    _instance = None
    _initialized = False
    _logger = None
    _log_file = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(LoggingManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            self._initialized = True
            self._enabled = False
    
    @classmethod
    def is_logging_enabled(cls):
        """Class method to check if logging is enabled for backward compatibility"""
        if cls._instance is None:
            return False
        return cls._instance._enabled
    
    @property
    def enabled(self):
        """Check if logging is enabled"""
        return self._enabled
    
    @property
    def log_file(self):
        """Get the current log file path"""
        return self._log_file
    
    def setup(self, mode="file", log_level=logging.DEBUG):
        """
        Set up logging configuration.

        This method configures the logging settings for the application.
        It sets the log level, format, and log file location.

        Args:
            mode (str): The logging mode ('file' or 'stdout').
            log_level (int): The logging level (default: logging.DEBUG).
        """
        # Enable logging
        self._enabled = True

        # Create a logger for mssql_python module
        # Use a consistent logger name to ensure we're using the same logger throughout
        self._logger = logging.getLogger("mssql_python")
        self._logger.setLevel(log_level)
        
        # Configure the root logger to ensure all messages are captured
        root_logger = logging.getLogger()
        root_logger.setLevel(log_level)
        
        # Make sure the logger propagates to the root logger
        self._logger.propagate = True
        
        # Clear any existing handlers to avoid duplicates during re-initialization
        if self._logger.handlers:
            self._logger.handlers.clear()

        # Construct the path to the log file
        # Directory for log files - currentdir/logs
        current_dir = os.path.dirname(os.path.abspath(__file__))
        log_dir = os.path.join(current_dir, 'logs')
        # exist_ok=True allows the directory to be created if it doesn't exist
        os.makedirs(log_dir, exist_ok=True)
        
        # Generate timestamp-based filename for better sorting and organization
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        self._log_file = os.path.join(log_dir, f'mssql_python_trace_{timestamp}_{os.getpid()}.log')

        # Create a log handler to log to driver specific file
        # By default we only want to log to a file, max size 500MB, and keep 5 backups
        file_handler = RotatingFileHandler(self._log_file, maxBytes=512*1024*1024, backupCount=5)
        file_handler.setLevel(log_level)
        
        # Create a custom formatter that adds [Python Layer log] prefix only to non-DDBC messages
        class PythonLayerFormatter(logging.Formatter):
            def format(self, record):
                message = record.getMessage()
                # Don't add [Python Layer log] prefix if the message already has [DDBC Bindings log] or [Python Layer log]
                if "[DDBC Bindings log]" not in message and "[Python Layer log]" not in message:
                    # Create a copy of the record to avoid modifying the original
                    new_record = logging.makeLogRecord(record.__dict__)
                    new_record.msg = f"[Python Layer log] {record.msg}"
                    return super().format(new_record)
                return super().format(record)
        
        # Use our custom formatter
        formatter = PythonLayerFormatter('%(asctime)s - %(levelname)s - %(filename)s - %(message)s')
        file_handler.setFormatter(formatter)
        self._logger.addHandler(file_handler)

        if mode == 'stdout':
            # If the mode is stdout, then we want to log to the console as well
            stdout_handler = logging.StreamHandler(sys.stdout)
            stdout_handler.setLevel(log_level)
            # Use the same smart formatter
            stdout_handler.setFormatter(formatter)
            self._logger.addHandler(stdout_handler)
        elif mode != 'file':
            raise ValueError(f'Invalid logging mode: {mode}')
        
        return self._logger
    
    def get_logger(self):
        """
        Get the logger instance.

        Returns:
            logging.Logger: The logger instance, or None if logging is not enabled.
        """
        if not self.enabled:
            # If logging is not enabled, return None
            return None
        return self._logger


# Create a singleton instance
_manager = LoggingManager()

def setup_logging(mode="file", log_level=logging.DEBUG):
    """
    Set up logging configuration.
    
    This is a wrapper around the LoggingManager.setup method for backward compatibility.
    
    Args:
        mode (str): The logging mode ('file' or 'stdout').
        log_level (int): The logging level (default: logging.DEBUG).
    """
    return _manager.setup(mode, log_level)

def get_logger():
    """
    Get the logger instance.
    
    This is a wrapper around the LoggingManager.get_logger method for backward compatibility.

    Returns:
        logging.Logger: The logger instance.
    """
    return _manager.get_logger()