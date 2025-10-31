import logging
import os
import pytest
import glob
from mssql_python.logging import logger, FINE, FINER, FINEST, setup_logging, get_logger


def get_log_file_path():
    """Get the current log file path from the logger"""
    # The new logger always has a log_file property
    return logger.log_file


@pytest.fixture
def cleanup_logger():
    """Cleanup logger & log files before and after each test"""

    def cleanup():
        # Disable logging by setting level to CRITICAL
        logger.setLevel(logging.CRITICAL)
        
        # Remove old log file if it exists
        try:
            log_file_path = get_log_file_path()
            if os.path.exists(log_file_path):
                os.remove(log_file_path)
        except:
            pass  # Ignore errors during cleanup
        
        # Reset handlers to create a new log file
        logger.reset_handlers()

    # Perform cleanup before the test
    cleanup()
    yield
    # Perform cleanup after the test
    cleanup()


def test_logging_disabled_by_default(cleanup_logger):
    """Test that logging is disabled by default (level=CRITICAL)"""
    try:
        # By default, logger should be at CRITICAL level (effectively disabled)
        assert logger.getLevel() == logging.CRITICAL
        assert not logger.isEnabledFor(FINE)
        assert not logger.isEnabledFor(FINER)
        assert not logger.isEnabledFor(FINEST)
    except Exception as e:
        pytest.fail(f"Logging not disabled by default. Error: {e}")


def test_enable_logging_fine(cleanup_logger):
    """Test enabling logging at FINE level"""
    try:
        logger.setLevel(FINE)
        assert logger.getLevel() == FINE
        assert logger.isEnabledFor(FINE)
        assert not logger.isEnabledFor(FINER)  # FINER is more detailed, should be disabled
        assert not logger.isEnabledFor(FINEST)  # FINEST is most detailed, should be disabled
    except Exception as e:
        pytest.fail(f"Failed to enable FINE logging: {e}")


def test_enable_logging_finer(cleanup_logger):
    """Test enabling logging at FINER level"""
    try:
        logger.setLevel(FINER)
        assert logger.getLevel() == FINER
        assert logger.isEnabledFor(FINE)  # FINE is less detailed, should be enabled
        assert logger.isEnabledFor(FINER)
        assert not logger.isEnabledFor(FINEST)  # FINEST is more detailed, should be disabled
    except Exception as e:
        pytest.fail(f"Failed to enable FINER logging: {e}")


def test_enable_logging_finest(cleanup_logger):
    """Test enabling logging at FINEST level"""
    try:
        logger.setLevel(FINEST)
        assert logger.getLevel() == FINEST
        assert logger.isEnabledFor(FINE)
        assert logger.isEnabledFor(FINER)
        assert logger.isEnabledFor(FINEST)  # All levels enabled
    except Exception as e:
        pytest.fail(f"Failed to enable FINEST logging: {e}")


def test_logging_to_file(cleanup_logger):
    """Test if logging works correctly in file mode"""
    try:
        # Set to FINEST to capture both FINE and INFO messages
        logger.setLevel(FINEST)
        
        # Log test messages at different levels
        test_message_fine = "Testing FINE level logging"
        test_message_info = "Testing INFO level logging"
        
        logger.fine(test_message_fine)
        logger.info(test_message_info)
        
        # Check if the log file is created and contains the test messages
        log_file_path = get_log_file_path()
        assert os.path.exists(log_file_path), "Log file not created"
        
        # Open the log file and check its content
        with open(log_file_path, "r") as f:
            log_content = f.read()
        
        assert test_message_fine in log_content, "FINE message not found in log file"
        assert test_message_info in log_content, "INFO message not found in log file"
        assert "[Python]" in log_content, "Python prefix not found in log file"
    except Exception as e:
        pytest.fail(f"Logging to file failed: {e}")


def test_password_sanitization(cleanup_logger):
    """Test that passwords are sanitized in log messages"""
    try:
        # Set to FINEST to ensure FINE messages are logged
        logger.setLevel(FINEST)
        
        # Log a message with a password
        test_message = "Connection string: Server=localhost;PWD=secret123;Database=test"
        logger.fine(test_message)
        
        # Check if the log file contains the sanitized message
        log_file_path = get_log_file_path()
        with open(log_file_path, "r") as f:
            log_content = f.read()
        
        assert "PWD=***" in log_content, "Password not sanitized in log file"
        assert "secret123" not in log_content, "Password leaked in log file"
    except Exception as e:
        pytest.fail(f"Password sanitization test failed: {e}")


def test_trace_id_generation(cleanup_logger):
    """Test that trace IDs are generated correctly"""
    try:
        # Generate trace IDs
        trace_id1 = logger.generate_trace_id()
        trace_id2 = logger.generate_trace_id("Connection")
        trace_id3 = logger.generate_trace_id()
        
        # Check format: PID_ThreadID_Counter
        import re
        pattern = r'^\d+_\d+_\d+$'
        assert re.match(pattern, trace_id1), f"Trace ID format invalid: {trace_id1}"
        
        # Check format with prefix: Prefix_PID_ThreadID_Counter
        pattern_with_prefix = r'^Connection_\d+_\d+_\d+$'
        assert re.match(pattern_with_prefix, trace_id2), f"Trace ID with prefix format invalid: {trace_id2}"
        
        # Check that trace IDs are unique (counter increments)
        assert trace_id1 != trace_id3, "Trace IDs should be unique"
    except Exception as e:
        pytest.fail(f"Trace ID generation test failed: {e}")


def test_log_file_location(cleanup_logger):
    """Test that log file is created in current working directory"""
    try:
        logger.setLevel(FINE)
        logger.fine("Test message")
        
        log_file_path = get_log_file_path()
        
        # Log file should be in current working directory, not package directory
        cwd = os.getcwd()
        assert log_file_path.startswith(cwd), f"Log file not in CWD: {log_file_path}"
        
        # Check filename format: mssql_python_trace_YYYYMMDD_HHMMSS_PID.log
        import re
        filename = os.path.basename(log_file_path)
        pattern = r'^mssql_python_trace_\d{8}_\d{6}_\d+\.log$'
        assert re.match(pattern, filename), f"Log filename format invalid: {filename}"
    except Exception as e:
        pytest.fail(f"Log file location test failed: {e}")


def test_different_log_levels(cleanup_logger):
    """Test that different log levels work correctly"""
    try:
        logger.setLevel(FINEST)  # Enable all levels
        
        # Log messages at different levels
        finest_msg = "This is a FINEST message"
        finer_msg = "This is a FINER message"
        fine_msg = "This is a FINE message"
        info_msg = "This is an INFO message"
        warning_msg = "This is a WARNING message"
        error_msg = "This is an ERROR message"
        
        logger.finest(finest_msg)
        logger.finer(finer_msg)
        logger.fine(fine_msg)
        logger.info(info_msg)
        logger.warning(warning_msg)
        logger.error(error_msg)
        
        # Check if the log file contains all messages
        log_file_path = get_log_file_path()
        with open(log_file_path, "r") as f:
            log_content = f.read()
        
        assert finest_msg in log_content, "FINEST message not found in log file"
        assert finer_msg in log_content, "FINER message not found in log file"
        assert fine_msg in log_content, "FINE message not found in log file"
        assert info_msg in log_content, "INFO message not found in log file"
        assert warning_msg in log_content, "WARNING message not found in log file"
        assert error_msg in log_content, "ERROR message not found in log file"
        
        # Check for level indicators in the log
        assert "FINEST" in log_content, "FINEST level not found in log file"
        assert "FINER" in log_content, "FINER level not found in log file"
        assert "FINE" in log_content, "FINE level not found in log file"
        assert "INFO" in log_content, "INFO level not found in log file"
        assert "WARNING" in log_content, "WARNING level not found in log file"
        assert "ERROR" in log_content, "ERROR level not found in log file"
    except Exception as e:
        pytest.fail(f"Log levels test failed: {e}")


def test_backward_compatibility_setup_logging(cleanup_logger):
    """Test that deprecated setup_logging() function still works"""
    try:
        # The old setup_logging() should still work for backward compatibility
        setup_logging('file', logging.DEBUG)
        
        # Logger should be enabled
        assert logger.isEnabledFor(FINE)
        
        # Test logging works
        test_message = "Testing backward compatibility"
        logger.info(test_message)
        
        log_file_path = get_log_file_path()
        with open(log_file_path, "r") as f:
            log_content = f.read()
        
        assert test_message in log_content
    except Exception as e:
        pytest.fail(f"Backward compatibility test failed: {e}")


def test_singleton_behavior(cleanup_logger):
    """Test that logger behaves as a module-level singleton"""
    try:
        # Import logger multiple times
        from mssql_python.logging import logger as logger1
        from mssql_python.logging import logger as logger2

        # They should be the same instance
        assert logger1 is logger2, "Logger instances are not the same"

        # Enable logging through one instance
        logger1.setLevel(logging.DEBUG)

        # The other instance should reflect this change
        assert logger2.level == logging.DEBUG, "Logger state not shared between instances"

        # Reset for cleanup
        logger1.setLevel(logging.NOTSET)
    except Exception as e:
        pytest.fail(f"Singleton behavior test failed: {e}")


def test_timestamp_in_log_filename(cleanup_logger):
    """Test that log filenames include timestamp and PID"""
    from mssql_python.logging import logger
    try:
        # Enable logging
        logger.setLevel(logging.DEBUG)
        logger.debug("Test message to create log file")

        # Get the log file path
        log_file_path = get_log_file_path()
        assert log_file_path is not None, "No log file found"
        
        filename = os.path.basename(log_file_path)

        # The filename should follow the pattern: mssql_python_trace_YYYYMMDD_HHMMSS_PID.log
        # Example: mssql_python_trace_20251031_102517_90898.log
        assert filename.startswith("mssql_python_trace_"), "Incorrect filename prefix"
        assert filename.endswith(".log"), "Incorrect filename suffix"

        # Extract the parts between prefix and suffix
        middle_part = filename[len("mssql_python_trace_"):-len(".log")]
        parts = middle_part.split("_")

        # Should have exactly 3 parts: YYYYMMDD, HHMMSS, PID
        assert len(parts) == 3, f"Expected 3 parts in filename, got {len(parts)}: {parts}"

        # Validate parts
        date_part, time_part, pid_part = parts
        assert len(date_part) == 8 and date_part.isdigit(), f"Date part '{date_part}' is not valid (expected YYYYMMDD)"
        assert len(time_part) == 6 and time_part.isdigit(), f"Time part '{time_part}' is not valid (expected HHMMSS)"
        assert pid_part.isdigit(), f"PID part '{pid_part}' is not numeric"

        # PID should match current process ID
        assert int(pid_part) == os.getpid(), "PID in filename doesn't match current process"
    except Exception as e:
        pytest.fail(f"Timestamp in filename test failed: {e}")


def test_invalid_logging_level(cleanup_logger):
    """Test that invalid logging levels are handled correctly."""
    from mssql_python.logging import logger

    # Test invalid level type - should raise TypeError or ValueError
    with pytest.raises((TypeError, ValueError)):
        logger.setLevel("invalid_level")

    # Test negative level - Python logging allows this but we can test boundaries
    try:
        logger.setLevel(-1)
        # If it doesn't raise, verify it's set
        assert logger.level == -1 or logger.level >= 0
    except (TypeError, ValueError):
        pass  # Some implementations may reject negative levels

    # Test extremely high level
    try:
        logger.setLevel(999999)
        assert logger.level == 999999
    except (TypeError, ValueError):
        pass  # Some implementations may have max levels


def test_valid_logging_levels_for_comparison(cleanup_logger):
    """Test that valid logging levels work correctly."""
    from mssql_python.logging import logger, FINE, FINER, FINEST

    # Test standard Python levels
    valid_levels = [
        logging.DEBUG,
        logging.INFO,
        logging.WARNING,
        logging.ERROR,
        logging.CRITICAL,
    ]
    
    for level in valid_levels:
        try:
            logger.setLevel(level)
            assert logger.level == level, f"Level {level} not set correctly"
        except Exception as e:
            pytest.fail(f"Valid level {level} should not raise exception: {e}")

    # Test custom JDBC-style levels
    custom_levels = [FINEST, FINER, FINE]
    for level in custom_levels:
        try:
            logger.setLevel(level)
            assert logger.level == level, f"Custom level {level} not set correctly"
        except Exception as e:
            pytest.fail(f"Valid custom level {level} should not raise exception: {e}")

    # Reset
    logger.setLevel(logging.NOTSET)


def test_logging_level_hierarchy(cleanup_logger):
    """Test that logging level hierarchy works correctly."""
    from mssql_python.logging import logger, FINE, FINER, FINEST
    import io

    # Create a string buffer to capture log output
    log_buffer = io.StringIO()
    handler = logging.StreamHandler(log_buffer)
    handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
    logger.addHandler(handler)

    try:
        # Set level to INFO - should only show INFO and above
        logger.setLevel(logging.INFO)
        
        logger.debug("Debug message")  # Should NOT appear
        logger.info("Info message")     # Should appear
        logger.warning("Warning message")  # Should appear
        
        output = log_buffer.getvalue()
        assert "Debug message" not in output, "Debug message should not appear at INFO level"
        assert "Info message" in output, "Info message should appear at INFO level"
        assert "Warning message" in output, "Warning message should appear at INFO level"

        # Clear buffer
        log_buffer.truncate(0)
        log_buffer.seek(0)

        # Set to FINEST - should show everything
        logger.setLevel(FINEST)
        logger.log(FINEST, "Finest message")
        logger.log(FINER, "Finer message")
        logger.log(FINE, "Fine message")
        logger.debug("Debug message")
        
        output = log_buffer.getvalue()
        assert "Finest message" in output, "Finest message should appear at FINEST level"
        assert "Finer message" in output, "Finer message should appear at FINEST level"
        assert "Fine message" in output, "Fine message should appear at FINEST level"
        assert "Debug message" in output, "Debug message should appear at FINEST level"

    finally:
        logger.removeHandler(handler)
        logger.setLevel(logging.NOTSET)
