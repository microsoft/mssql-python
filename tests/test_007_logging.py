import logging
import os
import pytest
from mssql_python.logging_config import setup_logging, get_logger, ENABLE_LOGGING

def get_log_file_path():
    repo_root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    pid = os.getpid()
    log_file = os.path.join(repo_root_dir, "mssql_python", f"mssql_python_trace_{pid}.log")
    return log_file

@pytest.fixture
def cleanup_logger():
    """Cleanup logger & log files before and after each test"""
    def cleanup():
        logger = get_logger()
        if logger is not None:
            logger.handlers.clear()
        log_file_path = get_log_file_path()
        if os.path.exists(log_file_path):
            os.remove(log_file_path)
        ENABLE_LOGGING = False
    # Perform cleanup before the test
    cleanup()
    yield
    # Perform cleanup after the test
    cleanup()

def test_no_logging(cleanup_logger):
    """Test that logging is off by default"""
    try:
        logger = get_logger()
        assert logger is None
        assert ENABLE_LOGGING == False
    except Exception as e:
        pytest.fail(f"Logging not off by default. Error: {e}")

def test_setup_logging(cleanup_logger):
    """Test if logging is set up correctly"""
    try:
        setup_logging() # This must enable logging
        logger = get_logger()
        assert logger is not None
        assert logger == logging.getLogger('mssql_python.logging_config')
        assert logger.level == logging.DEBUG  # DEBUG level
    except Exception as e:
        pytest.fail(f"Logging setup failed: {e}")

def test_logging_in_file_mode(cleanup_logger):
    """Test if logging works correctly in file mode"""
    try:
        setup_logging()
        logger = get_logger()
        assert logger is not None
        # Log a test message
        test_message = "Testing file logging mode"
        logger.info(test_message)
        # Check if the log file is created and contains the test message
        log_file_path = get_log_file_path()
        assert os.path.exists(log_file_path), "Log file not created"
        # open the log file and check its content
        with open(log_file_path, 'r') as f:
            log_content = f.read()
        assert test_message in log_content, "Log message not found in log file"
    except Exception as e:
        pytest.fail(f"Logging in file mode failed: {e}")

def test_logging_in_stdout_mode(cleanup_logger, capsys):
    """Test if logging works correctly in stdout mode"""
    try:
        setup_logging('stdout')
        logger = get_logger()
        assert logger is not None
        # Log a test message
        test_message = "Testing file + stdout logging mode"
        logger.info(test_message)
        # Check if the log file is created and contains the test message
        log_file_path = get_log_file_path()
        assert os.path.exists(log_file_path), "Log file not created in file+stdout mode"
        with open(log_file_path, 'r') as f:
            log_content = f.read()
        assert test_message in log_content, "Log message not found in log file"
        # Check if the message is printed to stdout
        captured_stdout = capsys.readouterr().out
        assert test_message in captured_stdout, "Log message not found in stdout"
    except Exception as e:
        pytest.fail(f"Logging in stdout mode failed: {e}")