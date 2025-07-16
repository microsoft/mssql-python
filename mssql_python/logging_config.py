"""
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
This module provides logging configuration for the mssql_python package.
"""

import logging
from logging.handlers import RotatingFileHandler
import os
import sys

ENABLE_LOGGING = False


def setup_logging(mode="file", log_level=logging.DEBUG):
    """
    Set up logging configuration.

    This method configures the logging settings for the application.
    It sets the log level, format, and log file location.

    Args:
        mode (str): The logging mode ('file' or 'stdout').
        log_level (int): The logging level (default: logging.DEBUG).
    """
    global ENABLE_LOGGING
    ENABLE_LOGGING = True

    # Create a logger for mssql_python module
    logger = logging.getLogger(__name__)
    logger.setLevel(log_level)

    # Construct the path to the log file
    # TODO: Use a different dir to dump log file
    current_dir = os.path.dirname(os.path.abspath(__file__))
    log_file = os.path.join(current_dir, f'mssql_python_trace_{os.getpid()}.log')

    # Create a log handler to log to driver specific file
    # By default we only want to log to a file, max size 500MB, and keep 5 backups
    # TODO: Rotate files based on time too? Ex: everyday
    file_handler = RotatingFileHandler(log_file, maxBytes=512*1024*1024, backupCount=5)
    file_handler.setLevel(log_level)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    if mode == 'stdout':
        # If the mode is stdout, then we want to log to the console as well
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setLevel(log_level)
        stdout_handler.setFormatter(formatter)
        logger.addHandler(stdout_handler)
    elif mode != 'file':
        raise ValueError(f'Invalid logging mode: {mode}')

def get_logger():
    """
    Get the logger instance.

    Returns:
        logging.Logger: The logger instance.
    """
    if not ENABLE_LOGGING:
        return None
    return logging.getLogger(__name__)
