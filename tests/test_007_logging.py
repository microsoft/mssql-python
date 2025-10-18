import logging
import os
import pytest
import glob
from mssql_python.logging_config import setup_logging, get_logger, LoggingManager


def get_log_file_path():
    # Get the LoggingManager singleton instance
    manager = LoggingManager()
    # If logging is enabled, return the actual log file path
    if manager.enabled and manager.log_file:
        return manager.log_file
    # For fallback/cleanup, try to find existing log files in the logs directory
    repo_root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    log_dir = os.path.join(repo_root_dir, "mssql_python", "logs")
    os.makedirs(log_dir, exist_ok=True)

    # Try to find existing log files
    log_files = glob.glob(os.path.join(log_dir, "mssql_python_trace_*.log"))
    if log_files:
        # Return the most recently created log file
        return max(log_files, key=os.path.getctime)

    # Fallback to default pattern
    pid = os.getpid()
    return os.path.join(log_dir, f"mssql_python_trace_{pid}.log")


@pytest.fixture
def cleanup_logger():
    """Cleanup logger & log files before and after each test"""

    def cleanup():
        # Get the LoggingManager singleton instance
        manager = LoggingManager()
        logger = get_logger()
        if logger is not None:
            logger.handlers.clear()

        # Try to remove the actual log file if it exists
        try:
            log_file_path = get_log_file_path()
            if os.path.exists(log_file_path):
                os.remove(log_file_path)
        except:
            pass  # Ignore errors during cleanup

        # Reset the LoggingManager instance
        manager._enabled = False
        manager._initialized = False
        manager._logger = None
        manager._log_file = None

    # Perform cleanup before the test
    cleanup()
    yield
    # Perform cleanup after the test
    cleanup()


def test_no_logging(cleanup_logger):
    """Test that logging is off by default"""
    try:
        # Get the LoggingManager singleton instance
        manager = LoggingManager()
        logger = get_logger()
        assert logger is None
        assert manager.enabled == False
    except Exception as e:
        pytest.fail(f"Logging not off by default. Error: {e}")


def test_setup_logging(cleanup_logger):
    """Test if logging is set up correctly"""
    try:
        setup_logging()  # This must enable logging
        logger = get_logger()
        assert logger is not None
        # Fix: Check for the correct logger name
        assert logger == logging.getLogger("mssql_python")
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
        with open(log_file_path, "r") as f:
            log_content = f.read()
        assert test_message in log_content, "Log message not found in log file"
    except Exception as e:
        pytest.fail(f"Logging in file mode failed: {e}")


def test_logging_in_stdout_mode(cleanup_logger, capsys):
    """Test if logging works correctly in stdout mode"""
    try:
        setup_logging("stdout")
        logger = get_logger()
        assert logger is not None
        # Log a test message
        test_message = "Testing file + stdout logging mode"
        logger.info(test_message)
        # Check if the log file is created and contains the test message
        log_file_path = get_log_file_path()
        assert os.path.exists(log_file_path), "Log file not created in file+stdout mode"
        with open(log_file_path, "r") as f:
            log_content = f.read()
        assert test_message in log_content, "Log message not found in log file"
        # Check if the message is printed to stdout
        captured_stdout = capsys.readouterr().out
        assert test_message in captured_stdout, "Log message not found in stdout"
    except Exception as e:
        pytest.fail(f"Logging in stdout mode failed: {e}")


def test_python_layer_prefix(cleanup_logger):
    """Test that Python layer logs have the correct prefix"""
    try:
        setup_logging()
        logger = get_logger()
        assert logger is not None

        # Log a test message
        test_message = "This is a Python layer test message"
        logger.info(test_message)

        # Check if the log file contains the message with [Python Layer log] prefix
        log_file_path = get_log_file_path()
        with open(log_file_path, "r") as f:
            log_content = f.read()

        # The logged message should have the Python Layer prefix
        assert "[Python Layer log]" in log_content, "Python Layer log prefix not found"
        assert test_message in log_content, "Test message not found in log file"
    except Exception as e:
        pytest.fail(f"Python layer prefix test failed: {e}")


def test_different_log_levels(cleanup_logger):
    """Test that different log levels work correctly"""
    try:
        setup_logging()
        logger = get_logger()
        assert logger is not None

        # Log messages at different levels
        debug_msg = "This is a DEBUG message"
        info_msg = "This is an INFO message"
        warning_msg = "This is a WARNING message"
        error_msg = "This is an ERROR message"

        logger.debug(debug_msg)
        logger.info(info_msg)
        logger.warning(warning_msg)
        logger.error(error_msg)

        # Check if the log file contains all messages
        log_file_path = get_log_file_path()
        with open(log_file_path, "r") as f:
            log_content = f.read()

        assert debug_msg in log_content, "DEBUG message not found in log file"
        assert info_msg in log_content, "INFO message not found in log file"
        assert warning_msg in log_content, "WARNING message not found in log file"
        assert error_msg in log_content, "ERROR message not found in log file"

        # Also check for level indicators in the log
        assert "DEBUG" in log_content, "DEBUG level not found in log file"
        assert "INFO" in log_content, "INFO level not found in log file"
        assert "WARNING" in log_content, "WARNING level not found in log file"
        assert "ERROR" in log_content, "ERROR level not found in log file"
    except Exception as e:
        pytest.fail(f"Log levels test failed: {e}")


def test_singleton_behavior(cleanup_logger):
    """Test that LoggingManager behaves as a singleton"""
    try:
        # Create multiple instances of LoggingManager
        manager1 = LoggingManager()
        manager2 = LoggingManager()

        # They should be the same instance
        assert manager1 is manager2, "LoggingManager instances are not the same"

        # Enable logging through one instance
        manager1._enabled = True

        # The other instance should reflect this change
        assert manager2.enabled == True, "Singleton state not shared between instances"

        # Reset for cleanup
        manager1._enabled = False
    except Exception as e:
        pytest.fail(f"Singleton behavior test failed: {e}")


def test_timestamp_in_log_filename(cleanup_logger):
    """Test that log filenames include timestamps"""
    try:
        setup_logging()

        # Get the log file path
        log_file_path = get_log_file_path()
        filename = os.path.basename(log_file_path)

        # Extract parts of the filename
        parts = filename.split("_")

        # The filename should follow the pattern: mssql_python_trace_YYYYMMDD_HHMMSS_PID.log
        # Fix: Account for the fact that "mssql_python" contains an underscore
        assert parts[0] == "mssql", "Incorrect filename prefix part 1"
        assert parts[1] == "python", "Incorrect filename prefix part 2"
        assert parts[2] == "trace", "Incorrect filename part"

        # Check date format (YYYYMMDD)
        date_part = parts[3]
        assert (
            len(date_part) == 8 and date_part.isdigit()
        ), "Date format incorrect in filename"

        # Check time format (HHMMSS)
        time_part = parts[4]
        assert (
            len(time_part) == 6 and time_part.isdigit()
        ), "Time format incorrect in filename"

        # Process ID should be the last part before .log
        pid_part = parts[5].split(".")[0]
        assert pid_part.isdigit(), "Process ID not found in filename"
    except Exception as e:
        pytest.fail(f"Timestamp in filename test failed: {e}")


def test_invalid_logging_mode(cleanup_logger):
    """Test that invalid logging modes raise ValueError (Lines 130-138)."""
    from mssql_python.logging_config import LoggingManager
    
    # Test invalid mode "invalid" - should trigger line 134
    manager = LoggingManager()
    with pytest.raises(ValueError, match="Invalid logging mode: invalid"):
        manager.setup(mode="invalid")
    
    # Test another invalid mode "console" - should also trigger line 134
    with pytest.raises(ValueError, match="Invalid logging mode: console"):
        manager.setup(mode="console")
    
    # Test invalid mode "both" - should also trigger line 134
    with pytest.raises(ValueError, match="Invalid logging mode: both"):
        manager.setup(mode="both")
    
    # Test empty string mode - should trigger line 134
    with pytest.raises(ValueError, match="Invalid logging mode: "):
        manager.setup(mode="")
    
    # Test None as mode (will become string "None") - should trigger line 134
    with pytest.raises(ValueError, match="Invalid logging mode: None"):
        manager.setup(mode=str(None))


def test_valid_logging_modes_for_comparison(cleanup_logger):
    """Test that valid logging modes work correctly for comparison."""
    from mssql_python.logging_config import LoggingManager
    
    # Test valid mode "file" - should not raise exception
    manager = LoggingManager()
    try:
        logger = manager.setup(mode="file")
        assert logger is not None
        assert manager.enabled is True
    except ValueError:
        pytest.fail("Valid mode 'file' should not raise ValueError")
    
    # Reset manager for next test
    manager._enabled = False
    manager._initialized = False
    manager._logger = None
    manager._log_file = None
    
    # Test valid mode "stdout" - should not raise exception
    try:
        logger = manager.setup(mode="stdout")
        assert logger is not None
        assert manager.enabled is True
    except ValueError:
        pytest.fail("Valid mode 'stdout' should not raise ValueError")


def test_logging_mode_validation_error_message_format(cleanup_logger):
    """Test that the error message format for invalid modes is correct."""
    from mssql_python.logging_config import LoggingManager
    
    manager = LoggingManager()
    
    # Test the exact error message format from line 134
    invalid_modes = ["invalid", "debug", "console", "stderr", "syslog"]
    
    for invalid_mode in invalid_modes:
        with pytest.raises(ValueError) as exc_info:
            manager.setup(mode=invalid_mode)
        
        # Verify the error message format matches line 134
        expected_message = f"Invalid logging mode: {invalid_mode}"
        assert str(exc_info.value) == expected_message
        
        # Reset manager state for next iteration
        manager._enabled = False
        manager._initialized = False
        manager._logger = None
        manager._log_file = None
